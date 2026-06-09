/* Ring — native C connector implementation. See include/impulse_ring.h.
 *
 * Pure C11 + POSIX. The protocol (shm segments, ring buffers, futex wakeup,
 * frames, Avro datums, control messages) is implemented natively here against
 * spec/; nothing binds to the Rust core. Cross-process atomicity uses C11/GCC
 * __atomic builtins; blocking uses the Linux futex syscall directly.
 */
#define _GNU_SOURCE
#include "impulse_ring.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <linux/futex.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

/* ====================================================================== */
/* Wire constants (must match spec/wire-format.md)                        */
/* ====================================================================== */

#define SHM_DIR "/dev/shm"
#define CONTROL_NAME "/impulse-ring.ctl.v1"
#define SUBMISSION_BASE 64u
#define REPLY_CAP (1u << 16)

/* Ring header field offsets (relative to ring base). */
#define R_MAGIC 0
#define R_CAP 4
#define R_HEAD 64
#define R_PRODLOCK 72
#define R_SPACE_SEQ 76
#define R_TAIL 128
#define R_DATA_SEQ 136
#define RING_HEADER 192u
#define RING_MAGIC 0x474e4952u /* "RING" */

/* Frame header. */
#define FRAME_MAGIC 0x5249u /* "IR" */
#define WIRE_VERSION 1
#define FRAME_HEADER 16u

/* Control schema fingerprints (from spec/schemas/FINGERPRINTS.md). */
#define FP_REGISTER 0xfc723bd7afcffc02ull
#define FP_REGISTER_REPLY 0x3af224883f4dec77ull
#define FP_UNREGISTER 0x82cdc2b7ebd36a6aull
#define FP_PUBLISH 0xf271a4c7a09fcdd7ull
#define FP_PUBLISH_REPLY 0x055439d7c0240142ull
#define FP_LIST 0xa5035910e842f4e6ull
#define FP_CHANNEL_LIST 0xa1048915e5931da2ull
#define FP_SUBSCRIBE 0x8ebc74e247531cffull
#define FP_SUBSCRIBE_REPLY 0x83b6f56ef3d10c31ull
#define FP_EXPOSE 0xa1acec8abc87f374ull
#define FP_EXPOSE_REPLY 0x94bb3fe569f8abf8ull
#define FP_LOOKUP 0xe732b44c32d796fcull
#define FP_LOOKUP_REPLY 0x5c9c6cbc1f3d26b2ull
#define FP_HEARTBEAT 0xa5073570fc81a3eaull
#define FP_RPC_REQUEST 0xe88f548e4f540ca4ull
#define FP_RPC_RESPONSE 0x5c4e149239ad5d24ull

/* Broker status codes (proto::status). */
#define ST_OK 0
#define ST_NOT_FOUND 1
#define ST_DENIED 2
#define ST_MISMATCH 3
#define ST_INTERNAL 4
#define ST_EXISTS 5

/* ====================================================================== */
/* Growable byte buffer                                                   */
/* ====================================================================== */

typedef struct {
  uint8_t *data;
  size_t len, cap;
} buf;

static void buf_init(buf *b) {
  b->data = NULL;
  b->len = b->cap = 0;
}
static int buf_reserve(buf *b, size_t extra) {
  if (b->len + extra <= b->cap)
    return 0;
  size_t ncap = b->cap ? b->cap * 2 : 64;
  while (ncap < b->len + extra)
    ncap *= 2;
  uint8_t *nd = (uint8_t *)realloc(b->data, ncap);
  if (!nd)
    return -1;
  b->data = nd;
  b->cap = ncap;
  return 0;
}
static void buf_push(buf *b, const void *p, size_t n) {
  if (buf_reserve(b, n))
    return;
  memcpy(b->data + b->len, p, n);
  b->len += n;
}
static void buf_byte(buf *b, uint8_t v) {
  buf_push(b, &v, 1);
}
static void buf_free(buf *b) {
  free(b->data);
  b->data = NULL;
  b->len = b->cap = 0;
}

/* ====================================================================== */
/* Avro primitives                                                        */
/* ====================================================================== */

struct ir_avro_w {
  buf b;
};
struct ir_avro_r {
  const uint8_t *p;
  size_t len, pos;
};

static void avro_put_varint(buf *b, uint64_t v) {
  while (v & ~0x7fULL) {
    buf_byte(b, (uint8_t)((v & 0x7f) | 0x80));
    v >>= 7;
  }
  buf_byte(b, (uint8_t)v);
}
static void avro_put_long(buf *b, int64_t v) {
  uint64_t zz = ((uint64_t)v << 1) ^ (uint64_t)(v >> 63);
  avro_put_varint(b, zz);
}
static void avro_put_str(buf *b, const char *s, size_t n) {
  avro_put_long(b, (int64_t)n);
  buf_push(b, s, n);
}

static int avro_get_varint(ir_avro_r *r, uint64_t *out) {
  uint64_t v = 0;
  int shift = 0;
  while (r->pos < r->len) {
    uint8_t byte = r->p[r->pos++];
    v |= (uint64_t)(byte & 0x7f) << shift;
    if (!(byte & 0x80)) {
      *out = v;
      return 0;
    }
    shift += 7;
    if (shift > 63)
      break;
  }
  return -1;
}
static int64_t avro_get_long_r(ir_avro_r *r) {
  uint64_t zz = 0;
  if (avro_get_varint(r, &zz))
    return 0;
  return (int64_t)(zz >> 1) ^ -(int64_t)(zz & 1);
}

ir_avro_w *ir_avro_w_new(void) {
  ir_avro_w *w = (ir_avro_w *)calloc(1, sizeof(*w));
  if (w)
    buf_init(&w->b);
  return w;
}
void ir_avro_w_free(ir_avro_w *w) {
  if (!w)
    return;
  buf_free(&w->b);
  free(w);
}
void ir_avro_put_null(ir_avro_w *w) {
  (void)w;
}
void ir_avro_put_boolean(ir_avro_w *w, int v) {
  buf_byte(&w->b, v ? 1 : 0);
}
void ir_avro_put_int(ir_avro_w *w, int32_t v) {
  avro_put_long(&w->b, v);
}
void ir_avro_put_long(ir_avro_w *w, int64_t v) {
  avro_put_long(&w->b, v);
}
void ir_avro_put_float(ir_avro_w *w, float v) {
  uint8_t t[4];
  memcpy(t, &v, 4);
  buf_push(&w->b, t, 4);
}
void ir_avro_put_double(ir_avro_w *w, double v) {
  uint8_t t[8];
  memcpy(t, &v, 8);
  buf_push(&w->b, t, 8);
}
void ir_avro_put_string(ir_avro_w *w, const char *s) {
  avro_put_str(&w->b, s, strlen(s));
}
void ir_avro_put_bytes(ir_avro_w *w, const uint8_t *b, size_t n) {
  avro_put_long(&w->b, (int64_t)n);
  buf_push(&w->b, b, n);
}
void ir_avro_array_start(ir_avro_w *w, int64_t count) {
  avro_put_long(&w->b, count);
}
void ir_avro_array_end(ir_avro_w *w) {
  avro_put_long(&w->b, 0);
}
const uint8_t *ir_avro_w_bytes(const ir_avro_w *w, size_t *len) {
  *len = w->b.len;
  return w->b.data;
}
uint8_t *ir_avro_w_take(ir_avro_w *w, size_t *len) {
  uint8_t *out = (uint8_t *)malloc(w->b.len ? w->b.len : 1);
  if (!out)
    return NULL;
  memcpy(out, w->b.data, w->b.len);
  *len = w->b.len;
  return out;
}

ir_avro_r *ir_avro_r_new(const uint8_t *data, size_t len) {
  ir_avro_r *r = (ir_avro_r *)calloc(1, sizeof(*r));
  if (r) {
    r->p = data;
    r->len = len;
    r->pos = 0;
  }
  return r;
}
void ir_avro_r_free(ir_avro_r *r) {
  free(r);
}
int ir_avro_get_boolean(ir_avro_r *r) {
  return r->pos < r->len ? r->p[r->pos++] : 0;
}
int32_t ir_avro_get_int(ir_avro_r *r) {
  return (int32_t)avro_get_long_r(r);
}
int64_t ir_avro_get_long(ir_avro_r *r) {
  return avro_get_long_r(r);
}
float ir_avro_get_float(ir_avro_r *r) {
  float v = 0;
  if (r->pos + 4 <= r->len) {
    memcpy(&v, r->p + r->pos, 4);
    r->pos += 4;
  }
  return v;
}
double ir_avro_get_double(ir_avro_r *r) {
  double v = 0;
  if (r->pos + 8 <= r->len) {
    memcpy(&v, r->p + r->pos, 8);
    r->pos += 8;
  }
  return v;
}
char *ir_avro_get_string(ir_avro_r *r) {
  int64_t n = avro_get_long_r(r);
  if (n < 0 || r->pos + (size_t)n > r->len)
    n = 0;
  char *s = (char *)malloc((size_t)n + 1);
  if (!s)
    return NULL;
  memcpy(s, r->p + r->pos, (size_t)n);
  s[n] = 0;
  r->pos += (size_t)n;
  return s;
}
/* Reads bytes into a heap buffer; returns length, sets *out (caller frees). */
static int64_t avro_get_bytes_r(ir_avro_r *r, uint8_t **out) {
  int64_t n = avro_get_long_r(r);
  if (n < 0 || r->pos + (size_t)n > r->len)
    n = 0;
  uint8_t *b = (uint8_t *)malloc((size_t)n ? (size_t)n : 1);
  if (!b)
    return -1;
  memcpy(b, r->p + r->pos, (size_t)n);
  r->pos += (size_t)n;
  *out = b;
  return n;
}
int64_t ir_avro_get_array_count(ir_avro_r *r) {
  return avro_get_long_r(r);
}

/* Peek the first Avro long of a body without consuming a reader. */
static int64_t avro_peek_long(const uint8_t *p, size_t len) {
  ir_avro_r r = {p, len, 0};
  return avro_get_long_r(&r);
}

/* ====================================================================== */
/* futex                                                                  */
/* ====================================================================== */

static int futex_wait(uint32_t *addr, uint32_t expected, int timeout_ms) {
  struct timespec ts, *tp = NULL;
  if (timeout_ms >= 0) {
    ts.tv_sec = timeout_ms / 1000;
    ts.tv_nsec = (long)(timeout_ms % 1000) * 1000000L;
    tp = &ts;
  }
  return (int)syscall(SYS_futex, addr, FUTEX_WAIT, expected, tp, NULL, 0);
}
static int futex_wake(uint32_t *addr, int n) {
  return (int)syscall(SYS_futex, addr, FUTEX_WAKE, n, NULL, NULL, 0);
}

/* ====================================================================== */
/* Shared-memory segments                                                 */
/* ====================================================================== */

typedef struct {
  void *addr;
  size_t len;
  char name[256]; /* POSIX name, e.g. /impulse-ring.ctl.v1 */
  int owns_unlink;
} segment;

static void path_for(const char *name, char *out, size_t outn) {
  snprintf(out, outn, "%s%s", SHM_DIR, name);
}

static int seg_open(const char *name, segment *s) {
  char path[512];
  path_for(name, path, sizeof path);
  int fd = open(path, O_RDWR);
  if (fd < 0)
    return -1;
  struct stat st;
  if (fstat(fd, &st) < 0 || st.st_size == 0) {
    close(fd);
    return -1;
  }
  void *p = mmap(NULL, (size_t)st.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  close(fd);
  if (p == MAP_FAILED)
    return -1;
  s->addr = p;
  s->len = (size_t)st.st_size;
  snprintf(s->name, sizeof s->name, "%s", name);
  s->owns_unlink = 0;
  return 0;
}

static int seg_create(const char *name, size_t size, segment *s) {
  char path[512];
  path_for(name, path, sizeof path);
  int fd = open(path, O_CREAT | O_RDWR, 0600);
  if (fd < 0)
    return -1;
  if (ftruncate(fd, (off_t)size) < 0) {
    close(fd);
    return -1;
  }
  void *p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  close(fd);
  if (p == MAP_FAILED)
    return -1;
  s->addr = p;
  s->len = size;
  snprintf(s->name, sizeof s->name, "%s", name);
  s->owns_unlink = 1;
  return 0;
}

static void seg_close(segment *s) {
  if (!s->addr)
    return;
  munmap(s->addr, s->len);
  if (s->owns_unlink) {
    char path[512];
    path_for(s->name, path, sizeof path);
    unlink(path);
  }
  s->addr = NULL;
}

/* ====================================================================== */
/* Ring buffer                                                            */
/* ====================================================================== */

typedef struct {
  uint8_t *base; /* points at ring header inside a mapping */
  uint32_t cap;
  uint64_t mask;
} ring;

static inline uint32_t *r_u32(ring *r, size_t off) {
  return (uint32_t *)(r->base + off);
}
static inline uint64_t *r_u64(ring *r, size_t off) {
  return (uint64_t *)(r->base + off);
}
static inline uint8_t *r_data(ring *r) {
  return r->base + RING_HEADER;
}

static int ring_attach(ring *r, uint8_t *base) {
  uint32_t magic = __atomic_load_n((uint32_t *)(base + R_MAGIC), __ATOMIC_ACQUIRE);
  if (magic != RING_MAGIC)
    return -1;
  uint32_t cap = __atomic_load_n((uint32_t *)(base + R_CAP), __ATOMIC_RELAXED);
  if (cap == 0 || (cap & (cap - 1)) != 0)
    return -1;
  r->base = base;
  r->cap = cap;
  r->mask = cap - 1;
  return 0;
}

static void ring_format(ring *r, uint8_t *base, uint32_t cap) {
  r->base = base;
  r->cap = cap;
  r->mask = cap - 1;
  __atomic_store_n(r_u64(r, R_HEAD), 0, __ATOMIC_RELAXED);
  __atomic_store_n(r_u64(r, R_TAIL), 0, __ATOMIC_RELAXED);
  __atomic_store_n(r_u32(r, R_PRODLOCK), 0, __ATOMIC_RELAXED);
  __atomic_store_n(r_u32(r, R_SPACE_SEQ), 0, __ATOMIC_RELAXED);
  __atomic_store_n(r_u32(r, R_DATA_SEQ), 0, __ATOMIC_RELAXED);
  __atomic_store_n(r_u32(r, R_CAP), cap, __ATOMIC_RELAXED);
  __atomic_store_n(r_u32(r, R_MAGIC), RING_MAGIC, __ATOMIC_RELEASE);
}

static void ring_write_wrapped(ring *r, uint64_t pos, const uint8_t *src, size_t n) {
  size_t idx = (size_t)(pos & r->mask);
  size_t first = r->cap - idx < n ? r->cap - idx : n;
  memcpy(r_data(r) + idx, src, first);
  if (first < n)
    memcpy(r_data(r), src + first, n - first);
}
static void ring_read_wrapped(ring *r, uint64_t pos, uint8_t *dst, size_t n) {
  size_t idx = (size_t)(pos & r->mask);
  size_t first = r->cap - idx < n ? r->cap - idx : n;
  memcpy(dst, r_data(r) + idx, first);
  if (first < n)
    memcpy(dst + first, r_data(r), n - first);
}

/* 3-state futex mutex on the producer side. */
static void ring_lock(ring *r) {
  uint32_t *lock = r_u32(r, R_PRODLOCK);
  uint32_t expected = 0;
  if (__atomic_compare_exchange_n(lock, &expected, 1, 0, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED))
    return;
  while (__atomic_exchange_n(lock, 2, __ATOMIC_ACQUIRE) != 0)
    futex_wait(lock, 2, 50);
}
static void ring_unlock(ring *r) {
  uint32_t *lock = r_u32(r, R_PRODLOCK);
  if (__atomic_exchange_n(lock, 0, __ATOMIC_RELEASE) == 2)
    futex_wake(lock, 1);
}

static int ring_push_locked(ring *r, const uint8_t *data, size_t len) {
  size_t need = 4 + len;
  if (need > r->cap)
    return -1;
  uint64_t head = __atomic_load_n(r_u64(r, R_HEAD), __ATOMIC_RELAXED);
  uint64_t tail = __atomic_load_n(r_u64(r, R_TAIL), __ATOMIC_ACQUIRE);
  if ((size_t)(r->cap - (head - tail)) < need)
    return 1; /* full */
  uint32_t l = (uint32_t)len;
  uint8_t lb[4] = {(uint8_t)l, (uint8_t)(l >> 8), (uint8_t)(l >> 16), (uint8_t)(l >> 24)};
  ring_write_wrapped(r, head, lb, 4);
  ring_write_wrapped(r, head + 4, data, len);
  __atomic_store_n(r_u64(r, R_HEAD), head + need, __ATOMIC_RELEASE);
  return 0;
}

static int ring_push(ring *r, const uint8_t *data, size_t len, int timeout_ms) {
  struct timespec start;
  clock_gettime(CLOCK_MONOTONIC, &start);
  for (;;) {
    ring_lock(r);
    int rc = ring_push_locked(r, data, len);
    if (rc == 0) {
      ring_unlock(r);
      __atomic_fetch_add(r_u32(r, R_DATA_SEQ), 1, __ATOMIC_RELEASE);
      futex_wake(r_u32(r, R_DATA_SEQ), 1);
      return 0;
    }
    if (rc < 0) {
      ring_unlock(r);
      return -1;
    }
    uint32_t seen = __atomic_load_n(r_u32(r, R_SPACE_SEQ), __ATOMIC_ACQUIRE);
    ring_unlock(r);
    if (timeout_ms >= 0) {
      struct timespec now;
      clock_gettime(CLOCK_MONOTONIC, &now);
      long elapsed = (now.tv_sec - start.tv_sec) * 1000 + (now.tv_nsec - start.tv_nsec) / 1000000;
      if (elapsed >= timeout_ms)
        return IR_ERR_TIMEOUT;
      futex_wait(r_u32(r, R_SPACE_SEQ), seen, (int)(timeout_ms - elapsed));
    } else {
      futex_wait(r_u32(r, R_SPACE_SEQ), seen, 50);
    }
  }
}

/* Pop one record into a heap buffer. Returns length, or 0 if empty. */
static size_t ring_try_pop(ring *r, uint8_t **out) {
  uint64_t tail = __atomic_load_n(r_u64(r, R_TAIL), __ATOMIC_RELAXED);
  uint64_t head = __atomic_load_n(r_u64(r, R_HEAD), __ATOMIC_ACQUIRE);
  if (head == tail)
    return 0;
  uint8_t lb[4];
  ring_read_wrapped(r, tail, lb, 4);
  size_t len = (size_t)lb[0] | ((size_t)lb[1] << 8) | ((size_t)lb[2] << 16) | ((size_t)lb[3] << 24);
  uint8_t *p = (uint8_t *)malloc(len ? len : 1);
  if (!p)
    return 0;
  ring_read_wrapped(r, tail + 4, p, len);
  __atomic_store_n(r_u64(r, R_TAIL), tail + 4 + len, __ATOMIC_RELEASE);
  __atomic_fetch_add(r_u32(r, R_SPACE_SEQ), 1, __ATOMIC_RELEASE);
  futex_wake(r_u32(r, R_SPACE_SEQ), INT_MAX);
  *out = p;
  return len;
}

static size_t ring_pop_blocking(ring *r, int timeout_ms, uint8_t **out) {
  struct timespec start;
  clock_gettime(CLOCK_MONOTONIC, &start);
  for (;;) {
    for (int i = 0; i < 256; i++) {
      size_t n = ring_try_pop(r, out);
      if (n)
        return n;
    }
    uint32_t seen = __atomic_load_n(r_u32(r, R_DATA_SEQ), __ATOMIC_ACQUIRE);
    size_t n = ring_try_pop(r, out);
    if (n)
      return n;
    if (timeout_ms >= 0) {
      struct timespec now;
      clock_gettime(CLOCK_MONOTONIC, &now);
      long elapsed = (now.tv_sec - start.tv_sec) * 1000 + (now.tv_nsec - start.tv_nsec) / 1000000;
      if (elapsed >= timeout_ms)
        return 0;
      futex_wait(r_u32(r, R_DATA_SEQ), seen, (int)(timeout_ms - elapsed));
    } else {
      futex_wait(r_u32(r, R_DATA_SEQ), seen, 100);
    }
  }
}

/* ====================================================================== */
/* Frame                                                                  */
/* ====================================================================== */

static uint8_t *frame_encode(uint64_t fp, const uint8_t *body, size_t body_len, size_t *out_len) {
  size_t n = FRAME_HEADER + body_len;
  uint8_t *f = (uint8_t *)malloc(n);
  if (!f)
    return NULL;
  f[0] = (uint8_t)(FRAME_MAGIC & 0xff);
  f[1] = (uint8_t)(FRAME_MAGIC >> 8);
  f[2] = WIRE_VERSION;
  f[3] = 0;
  for (int i = 0; i < 8; i++)
    f[4 + i] = (uint8_t)(fp >> (8 * i));
  uint32_t bl = (uint32_t)body_len;
  f[12] = (uint8_t)bl;
  f[13] = (uint8_t)(bl >> 8);
  f[14] = (uint8_t)(bl >> 16);
  f[15] = (uint8_t)(bl >> 24);
  memcpy(f + FRAME_HEADER, body, body_len);
  *out_len = n;
  return f;
}

/* Parse a frame in place: returns 0 and sets fp + body pointer/len. */
static int frame_decode(const uint8_t *f, size_t len, uint64_t *fp, const uint8_t **body, size_t *body_len) {
  if (len < FRAME_HEADER)
    return -1;
  if ((f[0] | (f[1] << 8)) != FRAME_MAGIC)
    return -1;
  if (f[2] != WIRE_VERSION)
    return -1;
  uint64_t v = 0;
  for (int i = 0; i < 8; i++)
    v |= (uint64_t)f[4 + i] << (8 * i);
  uint32_t bl = (uint32_t)f[12] | ((uint32_t)f[13] << 8) | ((uint32_t)f[14] << 16) | ((uint32_t)f[15] << 24);
  if (FRAME_HEADER + bl > len)
    return -1;
  *fp = v;
  *body = f + FRAME_HEADER;
  *body_len = bl;
  return 0;
}

/* ====================================================================== */
/* Pending correlation slots                                              */
/* ====================================================================== */

typedef struct slot {
  int64_t corr;
  pthread_mutex_t m;
  pthread_cond_t cv;
  int done;
  uint64_t fp;
  uint8_t *body;
  size_t body_len;
  struct slot *next;
} slot;

/* ====================================================================== */
/* Connection                                                             */
/* ====================================================================== */

typedef struct service {
  pthread_t thread;
  ring req_ring;
  segment arena;
  uint64_t req_fp, resp_fp;
  ir_handler handler;
  void *user;
  volatile int *running;
  /* cache of opened caller reply segments */
  struct caller_seg *cache;
  struct service *next;
} service;

typedef struct caller_seg {
  char name[256];
  segment seg;
  ring ring;
  struct caller_seg *next;
} caller_seg;

struct ir_conn {
  char app_name[128];
  segment ctl;
  ring submission;
  segment reply;
  ring reply_ring;
  int64_t client_id;
  pthread_t dispatcher;
  volatile int running;
  pthread_mutex_t pend_lock;
  slot *pend;
  pthread_mutex_t svc_lock;
  service *services;
  pthread_mutex_t err_lock;
  char err[256];
};

struct ir_publisher {
  segment arena;
  ring ring;
  uint64_t schema_fp;
};
struct ir_subscriber {
  segment arena;
  ring ring;
  uint64_t schema_fp;
};

static void set_err(ir_conn *c, const char *fmt, ...) {
  if (!c)
    return;
  pthread_mutex_lock(&c->err_lock);
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(c->err, sizeof c->err, fmt, ap);
  va_end(ap);
  pthread_mutex_unlock(&c->err_lock);
}

const char *ir_last_error(ir_conn *c) {
  return c ? c->err : "no connection";
}
void ir_free(void *p) {
  free(p);
}

static int64_t rand_i64(void) {
  uint64_t v = 0;
  int fd = open("/dev/urandom", O_RDONLY);
  if (fd >= 0) {
    ssize_t got = read(fd, &v, sizeof v);
    close(fd);
    if (got == (ssize_t)sizeof v)
      return (int64_t)v;
  }
  /* fallback */
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (int64_t)(ts.tv_nsec ^ ((uint64_t)ts.tv_sec << 20) ^ (uint64_t)(uintptr_t)&v);
}

/* ---- slot management ---- */
static slot *slot_register(ir_conn *c, int64_t corr) {
  slot *s = (slot *)calloc(1, sizeof(*s));
  if (!s)
    return NULL;
  s->corr = corr;
  pthread_mutex_init(&s->m, NULL);
  pthread_cond_init(&s->cv, NULL);
  pthread_mutex_lock(&c->pend_lock);
  s->next = c->pend;
  c->pend = s;
  pthread_mutex_unlock(&c->pend_lock);
  return s;
}
static slot *slot_take(ir_conn *c, int64_t corr) {
  pthread_mutex_lock(&c->pend_lock);
  slot **pp = &c->pend;
  while (*pp) {
    if ((*pp)->corr == corr) {
      slot *s = *pp;
      *pp = s->next;
      pthread_mutex_unlock(&c->pend_lock);
      return s;
    }
    pp = &(*pp)->next;
  }
  pthread_mutex_unlock(&c->pend_lock);
  return NULL;
}
static void slot_free(slot *s) {
  if (!s)
    return;
  free(s->body);
  pthread_mutex_destroy(&s->m);
  pthread_cond_destroy(&s->cv);
  free(s);
}
/* Wait for a reply on a slot. Returns 0 and fills fp/body, or IR_ERR_TIMEOUT. */
static int slot_wait(slot *s, int timeout_ms, uint64_t *fp, uint8_t **body, size_t *len) {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  ts.tv_sec += timeout_ms / 1000;
  ts.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
  if (ts.tv_nsec >= 1000000000L) {
    ts.tv_sec++;
    ts.tv_nsec -= 1000000000L;
  }
  pthread_mutex_lock(&s->m);
  int rc = 0;
  while (!s->done && rc == 0)
    rc = pthread_cond_timedwait(&s->cv, &s->m, &ts);
  int ret;
  if (s->done) {
    *fp = s->fp;
    *body = s->body;
    *len = s->body_len;
    s->body = NULL; /* hand ownership to caller */
    ret = 0;
  } else {
    ret = IR_ERR_TIMEOUT;
  }
  pthread_mutex_unlock(&s->m);
  return ret;
}

/* ---- dispatcher ---- */
static void *dispatcher_main(void *arg) {
  ir_conn *c = (ir_conn *)arg;
  while (c->running) {
    uint8_t *rec = NULL;
    size_t n = ring_pop_blocking(&c->reply_ring, 100, &rec);
    if (!n)
      continue;
    uint64_t fp;
    const uint8_t *body;
    size_t body_len;
    if (frame_decode(rec, n, &fp, &body, &body_len) == 0) {
      int64_t corr = avro_peek_long(body, body_len);
      slot *s = slot_take(c, corr);
      if (s) {
        pthread_mutex_lock(&s->m);
        s->fp = fp;
        s->body = (uint8_t *)malloc(body_len ? body_len : 1);
        memcpy(s->body, body, body_len);
        s->body_len = body_len;
        s->done = 1;
        pthread_cond_signal(&s->cv);
        pthread_mutex_unlock(&s->m);
      }
    }
    free(rec);
  }
  return NULL;
}

/* ---- control request/response helper ---- */
/* Sends a framed control body on the submission ring and waits for the reply.
 * Returns 0 with reply (fp/body owned by caller) or a negative code. */
static int control_call(ir_conn *c, uint64_t fp, const uint8_t *body, size_t body_len, int64_t corr, int timeout_ms,
                        uint64_t *rfp, uint8_t **rbody, size_t *rlen) {
  slot *s = slot_register(c, corr);
  if (!s)
    return IR_ERR;
  size_t flen;
  uint8_t *frame = frame_encode(fp, body, body_len, &flen);
  if (!frame) {
    slot_take(c, corr);
    slot_free(s);
    return IR_ERR;
  }
  int rc = ring_push(&c->submission, frame, flen, 2000);
  free(frame);
  if (rc != 0) {
    slot_take(c, corr);
    slot_free(s);
    return IR_ERR;
  }
  rc = slot_wait(s, timeout_ms, rfp, rbody, rlen);
  if (rc != 0)
    slot_take(c, corr); /* remove if still pending */
  slot_free(s);
  return rc;
}

/* ====================================================================== */
/* Public API                                                             */
/* ====================================================================== */

ir_conn *ir_connect(const char *app_name) {
  ir_conn *c = (ir_conn *)calloc(1, sizeof(*c));
  if (!c)
    return NULL;
  snprintf(c->app_name, sizeof c->app_name, "%s", app_name ? app_name : "");
  pthread_mutex_init(&c->pend_lock, NULL);
  pthread_mutex_init(&c->svc_lock, NULL);
  pthread_mutex_init(&c->err_lock, NULL);
  c->running = 1;

  if (seg_open(CONTROL_NAME, &c->ctl) != 0) {
    set_err(c, "cannot open control segment (is impulsed running?)");
    goto fail;
  }
  /* validate control magic "IMPRING\0" */
  uint64_t ctl_magic;
  memcpy(&ctl_magic, c->ctl.addr, 8);
  if (memcmp(c->ctl.addr, "IMPRING\0", 8) != 0) {
    set_err(c, "control magic mismatch");
    goto fail;
  }
  if (ring_attach(&c->submission, (uint8_t *)c->ctl.addr + SUBMISSION_BASE) != 0) {
    set_err(c, "cannot attach submission ring");
    goto fail;
  }

  /* create our reply segment, named by a random nonce */
  int64_t nonce = rand_i64();
  char reply_name[256];
  snprintf(reply_name, sizeof reply_name, "/impulse-ring.cli.%llu.v1", (unsigned long long)(uint64_t)nonce);
  if (seg_create(reply_name, RING_HEADER + REPLY_CAP, &c->reply) != 0) {
    set_err(c, "cannot create reply segment");
    goto fail;
  }
  ring_format(&c->reply_ring, (uint8_t *)c->reply.addr, REPLY_CAP);

  if (pthread_create(&c->dispatcher, NULL, dispatcher_main, c) != 0) {
    set_err(c, "cannot start dispatcher");
    goto fail;
  }

  /* Register */
  int64_t corr = rand_i64();
  ir_avro_w *w = ir_avro_w_new();
  ir_avro_put_long(w, corr);
  ir_avro_put_string(w, c->app_name);
  ir_avro_put_long(w, nonce);
  ir_avro_put_string(w, reply_name);
  ir_avro_put_long(w, 1000);
  size_t blen;
  const uint8_t *body = ir_avro_w_bytes(w, &blen);
  uint64_t rfp;
  uint8_t *rbody;
  size_t rlen;
  int rc = control_call(c, FP_REGISTER, body, blen, corr, 5000, &rfp, &rbody, &rlen);
  ir_avro_w_free(w);
  if (rc != 0) {
    set_err(c, "register timed out");
    goto fail;
  }
  /* RegisterReply: correlation_id, client_id, status, message */
  ir_avro_r *r = ir_avro_r_new(rbody, rlen);
  (void)ir_avro_get_long(r); /* corr */
  int64_t client_id = ir_avro_get_long(r);
  int32_t status = ir_avro_get_int(r);
  char *msg = ir_avro_get_string(r);
  ir_avro_r_free(r);
  free(rbody);
  if (status != ST_OK) {
    set_err(c, "register rejected: %s", msg ? msg : "");
    free(msg);
    goto fail;
  }
  free(msg);
  c->client_id = client_id;
  return c;

fail:
  /* tear down whatever started */
  c->running = 0;
  if (c->dispatcher)
    pthread_join(c->dispatcher, NULL);
  seg_close(&c->reply);
  seg_close(&c->ctl);
  {
    /* keep error retrievable: return NULL but free */
    char tmp[256];
    snprintf(tmp, sizeof tmp, "%s", c->err);
    fprintf(stderr, "ir_connect: %s\n", tmp);
  }
  pthread_mutex_destroy(&c->pend_lock);
  pthread_mutex_destroy(&c->svc_lock);
  pthread_mutex_destroy(&c->err_lock);
  free(c);
  return NULL;
}

void ir_disconnect(ir_conn *c) {
  if (!c)
    return;
  /* best-effort unregister */
  int64_t corr = rand_i64();
  ir_avro_w *w = ir_avro_w_new();
  ir_avro_put_long(w, corr);
  ir_avro_put_long(w, c->client_id);
  size_t blen;
  const uint8_t *body = ir_avro_w_bytes(w, &blen);
  size_t flen;
  uint8_t *frame = frame_encode(FP_UNREGISTER, body, blen, &flen);
  if (frame) {
    ring_push(&c->submission, frame, flen, 200);
    free(frame);
  }
  ir_avro_w_free(w);

  c->running = 0;
  pthread_join(c->dispatcher, NULL);

  /* stop + join services */
  pthread_mutex_lock(&c->svc_lock);
  service *sv = c->services;
  pthread_mutex_unlock(&c->svc_lock);
  while (sv) {
    pthread_join(sv->thread, NULL);
    caller_seg *cs = sv->cache;
    while (cs) {
      caller_seg *nx = cs->next;
      seg_close(&cs->seg);
      free(cs);
      cs = nx;
    }
    seg_close(&sv->arena);
    service *nx = sv->next;
    free(sv);
    sv = nx;
  }

  /* free any leftover pending slots */
  slot *s = c->pend;
  while (s) {
    slot *nx = s->next;
    slot_free(s);
    s = nx;
  }

  seg_close(&c->reply);
  seg_close(&c->ctl);
  pthread_mutex_destroy(&c->pend_lock);
  pthread_mutex_destroy(&c->svc_lock);
  pthread_mutex_destroy(&c->err_lock);
  free(c);
}

ir_publisher *ir_publish_channel(ir_conn *c, const char *name, const char *schema_json, const char *key) {
  int64_t corr = rand_i64();
  ir_avro_w *w = ir_avro_w_new();
  ir_avro_put_long(w, corr);
  ir_avro_put_long(w, c->client_id);
  ir_avro_put_string(w, name);
  ir_avro_put_string(w, schema_json);
  ir_avro_put_string(w, key ? key : "");
  size_t blen;
  const uint8_t *body = ir_avro_w_bytes(w, &blen);
  uint64_t rfp;
  uint8_t *rbody;
  size_t rlen;
  int rc = control_call(c, FP_PUBLISH, body, blen, corr, 5000, &rfp, &rbody, &rlen);
  ir_avro_w_free(w);
  if (rc != 0) {
    set_err(c, "publish timed out");
    return NULL;
  }
  /* PublishReply: corr, channel_id, schema_fp, arena, status, message */
  ir_avro_r *r = ir_avro_r_new(rbody, rlen);
  (void)ir_avro_get_long(r);
  (void)ir_avro_get_long(r); /* channel_id */
  int64_t schema_fp = ir_avro_get_long(r);
  char *arena = ir_avro_get_string(r);
  int32_t status = ir_avro_get_int(r);
  char *msg = ir_avro_get_string(r);
  ir_avro_r_free(r);
  free(rbody);
  if (status != ST_OK) {
    set_err(c, "publish failed: %s", msg ? msg : "");
    free(arena);
    free(msg);
    return NULL;
  }
  free(msg);
  ir_publisher *p = (ir_publisher *)calloc(1, sizeof(*p));
  if (seg_open(arena, &p->arena) != 0 || ring_attach(&p->ring, (uint8_t *)p->arena.addr) != 0) {
    set_err(c, "cannot map channel arena");
    free(arena);
    free(p);
    return NULL;
  }
  free(arena);
  p->schema_fp = (uint64_t)schema_fp;
  return p;
}

int ir_publish(ir_publisher *p, const uint8_t *avro_body, size_t len) {
  size_t flen;
  uint8_t *frame = frame_encode(p->schema_fp, avro_body, len, &flen);
  if (!frame)
    return IR_ERR;
  int rc = ring_push(&p->ring, frame, flen, 1000);
  free(frame);
  return rc == 0 ? IR_OK : (rc == IR_ERR_TIMEOUT ? IR_ERR_TIMEOUT : IR_ERR);
}

void ir_publisher_free(ir_publisher *p) {
  if (!p)
    return;
  seg_close(&p->arena);
  free(p);
}

int ir_list_channels(ir_conn *c, ir_channel_info *out, size_t max, size_t *count) {
  int64_t corr = rand_i64();
  ir_avro_w *w = ir_avro_w_new();
  ir_avro_put_long(w, corr);
  ir_avro_put_long(w, c->client_id);
  size_t blen;
  const uint8_t *body = ir_avro_w_bytes(w, &blen);
  uint64_t rfp;
  uint8_t *rbody;
  size_t rlen;
  int rc = control_call(c, FP_LIST, body, blen, corr, 5000, &rfp, &rbody, &rlen);
  ir_avro_w_free(w);
  if (rc != 0) {
    set_err(c, "list timed out");
    return IR_ERR_TIMEOUT;
  }
  ir_avro_r *r = ir_avro_r_new(rbody, rlen);
  (void)ir_avro_get_long(r); /* corr */
  size_t n = 0;
  int64_t block;
  while ((block = ir_avro_get_array_count(r)) != 0) {
    if (block < 0)
      block = -block; /* negative count => block-with-size form */
    for (int64_t i = 0; i < block; i++) {
      int64_t cid = ir_avro_get_long(r);
      char *nm = ir_avro_get_string(r);
      char *owner = ir_avro_get_string(r);
      int64_t fp = ir_avro_get_long(r);
      int reqk = ir_avro_get_boolean(r);
      if (n < max) {
        out[n].channel_id = cid;
        snprintf(out[n].name, sizeof out[n].name, "%s", nm ? nm : "");
        snprintf(out[n].owner_app, sizeof out[n].owner_app, "%s", owner ? owner : "");
        out[n].schema_fp = (uint64_t)fp;
        out[n].requires_key = reqk;
      }
      free(nm);
      free(owner);
      n++;
    }
  }
  ir_avro_r_free(r);
  free(rbody);
  *count = n;
  return IR_OK;
}

ir_subscriber *ir_subscribe(ir_conn *c, int64_t channel_id, const char *key) {
  int64_t corr = rand_i64();
  ir_avro_w *w = ir_avro_w_new();
  ir_avro_put_long(w, corr);
  ir_avro_put_long(w, c->client_id);
  ir_avro_put_long(w, channel_id);
  ir_avro_put_string(w, key ? key : "");
  ir_avro_put_long(w, 0); /* expected_fp = 0: broker computes/owns fingerprints */
  size_t blen;
  const uint8_t *body = ir_avro_w_bytes(w, &blen);
  uint64_t rfp;
  uint8_t *rbody;
  size_t rlen;
  int rc = control_call(c, FP_SUBSCRIBE, body, blen, corr, 5000, &rfp, &rbody, &rlen);
  ir_avro_w_free(w);
  if (rc != 0) {
    set_err(c, "subscribe timed out");
    return NULL;
  }
  /* SubscribeReply: corr, arena, schema_fp, status, message */
  ir_avro_r *r = ir_avro_r_new(rbody, rlen);
  (void)ir_avro_get_long(r);
  char *arena = ir_avro_get_string(r);
  int64_t schema_fp = ir_avro_get_long(r);
  int32_t status = ir_avro_get_int(r);
  char *msg = ir_avro_get_string(r);
  ir_avro_r_free(r);
  free(rbody);
  if (status != ST_OK) {
    set_err(c, "subscribe failed: %s", msg ? msg : "");
    free(arena);
    free(msg);
    return NULL;
  }
  free(msg);
  ir_subscriber *s = (ir_subscriber *)calloc(1, sizeof(*s));
  if (seg_open(arena, &s->arena) != 0 || ring_attach(&s->ring, (uint8_t *)s->arena.addr) != 0) {
    set_err(c, "cannot map channel arena");
    free(arena);
    free(s);
    return NULL;
  }
  free(arena);
  s->schema_fp = (uint64_t)schema_fp;
  return s;
}

int ir_recv(ir_subscriber *s, int timeout_ms, uint8_t **body, size_t *len) {
  uint8_t *rec = NULL;
  size_t n = ring_pop_blocking(&s->ring, timeout_ms, &rec);
  if (!n)
    return 0;
  uint64_t fp;
  const uint8_t *b;
  size_t bl;
  if (frame_decode(rec, n, &fp, &b, &bl) != 0) {
    free(rec);
    return IR_ERR;
  }
  if (fp != s->schema_fp) {
    free(rec);
    return IR_ERR_MISMATCH;
  }
  uint8_t *out = (uint8_t *)malloc(bl ? bl : 1);
  memcpy(out, b, bl);
  free(rec);
  *body = out;
  *len = bl;
  return 1;
}

void ir_subscriber_free(ir_subscriber *s) {
  if (!s)
    return;
  seg_close(&s->arena);
  free(s);
}
uint64_t ir_subscriber_schema_fp(const ir_subscriber *s) {
  return s->schema_fp;
}

/* ---- service ---- */
static ring *caller_ring(service *sv, const char *name) {
  caller_seg *cs = sv->cache;
  while (cs) {
    if (strcmp(cs->name, name) == 0)
      return &cs->ring;
    cs = cs->next;
  }
  cs = (caller_seg *)calloc(1, sizeof(*cs));
  snprintf(cs->name, sizeof cs->name, "%s", name);
  if (seg_open(name, &cs->seg) != 0 || ring_attach(&cs->ring, (uint8_t *)cs->seg.addr) != 0) {
    free(cs);
    return NULL;
  }
  cs->next = sv->cache;
  sv->cache = cs;
  return &cs->ring;
}

static void *service_main(void *arg) {
  service *sv = (service *)arg;
  while (*sv->running) {
    uint8_t *rec = NULL;
    size_t n = ring_pop_blocking(&sv->req_ring, 100, &rec);
    if (!n)
      continue;
    uint64_t fp;
    const uint8_t *body;
    size_t body_len;
    if (frame_decode(rec, n, &fp, &body, &body_len) != 0) {
      free(rec);
      continue;
    }
    /* RpcRequest: corr, caller_id, reply_segment, arg_fp, args(bytes) */
    ir_avro_r *r = ir_avro_r_new(body, body_len);
    int64_t corr = ir_avro_get_long(r);
    (void)ir_avro_get_long(r); /* caller_id */
    char *reply_seg = ir_avro_get_string(r);
    int64_t arg_fp = ir_avro_get_long(r);
    uint8_t *args = NULL;
    int64_t args_len = avro_get_bytes_r(r, &args);
    ir_avro_r_free(r);

    int32_t status = ST_OK;
    const char *emsg = "";
    uint8_t *result = NULL;
    size_t result_len = 0;
    if ((uint64_t)arg_fp != sv->req_fp) {
      status = ST_MISMATCH;
      emsg = "request schema mismatch";
    } else if (sv->handler(args, (size_t)args_len, &result, &result_len, sv->user) != 0) {
      status = ST_INTERNAL;
      emsg = "handler error";
    }
    free(args);

    /* RpcResponse: corr, status, message, result_fp, result(bytes) */
    ir_avro_w *w = ir_avro_w_new();
    ir_avro_put_long(w, corr);
    ir_avro_put_int(w, status);
    ir_avro_put_string(w, emsg);
    ir_avro_put_long(w, status == ST_OK ? (int64_t)sv->resp_fp : 0);
    ir_avro_put_bytes(w, result ? result : (const uint8_t *)"", result_len);
    size_t wlen;
    const uint8_t *wbody = ir_avro_w_bytes(w, &wlen);
    size_t flen;
    uint8_t *frame = frame_encode(FP_RPC_RESPONSE, wbody, wlen, &flen);
    ring *rr = caller_ring(sv, reply_seg);
    if (rr && frame)
      ring_push(rr, frame, flen, 2000);
    free(frame);
    ir_avro_w_free(w);
    free(result);
    free(reply_seg);
    free(rec);
  }
  return NULL;
}

int ir_expose_function(ir_conn *c, const char *name, const char *req_schema_json, const char *resp_schema_json,
                       const char *key, ir_handler handler, void *user) {
  int64_t corr = rand_i64();
  ir_avro_w *w = ir_avro_w_new();
  ir_avro_put_long(w, corr);
  ir_avro_put_long(w, c->client_id);
  ir_avro_put_string(w, name);
  ir_avro_put_string(w, req_schema_json);
  ir_avro_put_string(w, resp_schema_json);
  ir_avro_put_string(w, key ? key : "");
  size_t blen;
  const uint8_t *body = ir_avro_w_bytes(w, &blen);
  uint64_t rfp;
  uint8_t *rbody;
  size_t rlen;
  int rc = control_call(c, FP_EXPOSE, body, blen, corr, 5000, &rfp, &rbody, &rlen);
  ir_avro_w_free(w);
  if (rc != 0) {
    set_err(c, "expose timed out");
    return IR_ERR_TIMEOUT;
  }
  /* ExposeReply: corr, fn_id, req_fp, resp_fp, req_arena, status, message */
  ir_avro_r *r = ir_avro_r_new(rbody, rlen);
  (void)ir_avro_get_long(r);
  (void)ir_avro_get_long(r); /* fn_id */
  int64_t req_fp = ir_avro_get_long(r);
  int64_t resp_fp = ir_avro_get_long(r);
  char *req_arena = ir_avro_get_string(r);
  int32_t status = ir_avro_get_int(r);
  char *msg = ir_avro_get_string(r);
  ir_avro_r_free(r);
  free(rbody);
  if (status != ST_OK) {
    set_err(c, "expose failed: %s", msg ? msg : "");
    free(req_arena);
    free(msg);
    return IR_ERR;
  }
  free(msg);
  service *sv = (service *)calloc(1, sizeof(*sv));
  if (seg_open(req_arena, &sv->arena) != 0 || ring_attach(&sv->req_ring, (uint8_t *)sv->arena.addr) != 0) {
    set_err(c, "cannot map function arena");
    free(req_arena);
    free(sv);
    return IR_ERR;
  }
  free(req_arena);
  sv->req_fp = (uint64_t)req_fp;
  sv->resp_fp = (uint64_t)resp_fp;
  sv->handler = handler;
  sv->user = user;
  sv->running = &c->running;
  pthread_mutex_lock(&c->svc_lock);
  sv->next = c->services;
  c->services = sv;
  pthread_mutex_unlock(&c->svc_lock);
  if (pthread_create(&sv->thread, NULL, service_main, sv) != 0) {
    set_err(c, "cannot start service thread");
    return IR_ERR;
  }
  return IR_OK;
}

int ir_call(ir_conn *c, const char *fn_name, const char *key, const uint8_t *req, size_t req_len, int timeout_ms,
            uint8_t **resp, size_t *resp_len) {
  /* 1. Lookup the function (broker returns fingerprints + arena). */
  int64_t corr = rand_i64();
  ir_avro_w *w = ir_avro_w_new();
  ir_avro_put_long(w, corr);
  ir_avro_put_long(w, c->client_id);
  ir_avro_put_string(w, fn_name);
  ir_avro_put_string(w, key ? key : "");
  size_t blen;
  const uint8_t *body = ir_avro_w_bytes(w, &blen);
  uint64_t rfp;
  uint8_t *rbody;
  size_t rlen;
  int rc = control_call(c, FP_LOOKUP, body, blen, corr, 5000, &rfp, &rbody, &rlen);
  ir_avro_w_free(w);
  if (rc != 0) {
    set_err(c, "lookup timed out");
    return IR_ERR_TIMEOUT;
  }
  /* LookupReply: corr, fn_id, req_fp, resp_fp, req_arena, status, message */
  ir_avro_r *r = ir_avro_r_new(rbody, rlen);
  (void)ir_avro_get_long(r);
  (void)ir_avro_get_long(r); /* fn_id */
  int64_t req_fp = ir_avro_get_long(r);
  int64_t resp_fp = ir_avro_get_long(r);
  char *req_arena = ir_avro_get_string(r);
  int32_t status = ir_avro_get_int(r);
  char *msg = ir_avro_get_string(r);
  ir_avro_r_free(r);
  free(rbody);
  if (status != ST_OK) {
    int code = (status == ST_DENIED) ? IR_ERR_DENIED : (status == ST_NOT_FOUND) ? IR_ERR_NOTFOUND : IR_ERR;
    set_err(c, "lookup failed: %s", msg ? msg : "");
    free(req_arena);
    free(msg);
    return code;
  }
  free(msg);

  /* 2. Open the function's request arena and place an RpcRequest. */
  segment arena;
  ring fnring;
  if (seg_open(req_arena, &arena) != 0 || ring_attach(&fnring, (uint8_t *)arena.addr) != 0) {
    set_err(c, "cannot map function arena");
    free(req_arena);
    return IR_ERR;
  }
  free(req_arena);

  int64_t rpc_corr = rand_i64();
  slot *s = slot_register(c, rpc_corr);
  ir_avro_w *rw = ir_avro_w_new();
  ir_avro_put_long(rw, rpc_corr);
  ir_avro_put_long(rw, c->client_id);
  ir_avro_put_string(rw, c->reply.name);
  ir_avro_put_long(rw, req_fp); /* arg_fp = broker-derived request fp */
  ir_avro_put_bytes(rw, req, req_len);
  size_t rwlen;
  const uint8_t *rwbody = ir_avro_w_bytes(rw, &rwlen);
  size_t flen;
  uint8_t *frame = frame_encode(FP_RPC_REQUEST, rwbody, rwlen, &flen);
  int prc = frame ? ring_push(&fnring, frame, flen, 2000) : IR_ERR;
  free(frame);
  ir_avro_w_free(rw);
  if (prc != 0) {
    slot_take(c, rpc_corr);
    slot_free(s);
    seg_close(&arena);
    set_err(c, "function request ring full");
    return IR_ERR;
  }

  /* 3. Await the response. */
  uint64_t resfp;
  uint8_t *res;
  size_t reslen;
  rc = slot_wait(s, timeout_ms, &resfp, &res, &reslen);
  if (rc != 0)
    slot_take(c, rpc_corr);
  slot_free(s);
  seg_close(&arena);
  if (rc != 0) {
    set_err(c, "rpc timed out");
    return IR_ERR_TIMEOUT;
  }
  /* RpcResponse: corr, status, message, result_fp, result(bytes) */
  ir_avro_r *rr = ir_avro_r_new(res, reslen);
  (void)ir_avro_get_long(rr);
  int32_t rstatus = ir_avro_get_int(rr);
  char *rmsg = ir_avro_get_string(rr);
  int64_t result_fp = ir_avro_get_long(rr);
  uint8_t *result = NULL;
  int64_t result_len = avro_get_bytes_r(rr, &result);
  ir_avro_r_free(rr);
  free(res);
  if (rstatus != ST_OK) {
    int code = (rstatus == ST_MISMATCH) ? IR_ERR_MISMATCH : IR_ERR;
    set_err(c, "remote error: %s", rmsg ? rmsg : "");
    free(rmsg);
    free(result);
    return code;
  }
  free(rmsg);
  if ((uint64_t)result_fp != (uint64_t)resp_fp) {
    free(result);
    set_err(c, "response schema mismatch");
    return IR_ERR_MISMATCH;
  }
  *resp = result;
  *resp_len = (size_t)result_len;
  return IR_OK;
}
