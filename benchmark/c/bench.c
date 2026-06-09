/* Ring relay benchmark — C node. See benchmark/README.md.
 *
 * Usage: bench_c <index> <num_services> <laps>
 * Node `index` subscribes to bench-<index-1> and publishes bench-<index>, all
 * gated by a hard-coded key. Node 0 coordinates timing; the rest relay.
 */
#include "impulse_ring.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define KEY "ring-bench-key"
#define SCHEMA                                                                                                          \
  "{\"type\":\"record\",\"name\":\"BenchToken\",\"namespace\":\"ring.bench\",\"fields\":["                              \
  "{\"name\":\"lap\",\"type\":\"long\"},{\"name\":\"start_nanos\",\"type\":\"long\"},"                                  \
  "{\"name\":\"elapsed_ns\",\"type\":\"long\"},{\"name\":\"stop\",\"type\":\"boolean\"}]}"

static int64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static uint8_t *encode_token(int64_t lap, int64_t start, int64_t elapsed, int stop, size_t *len) {
  ir_avro_w *w = ir_avro_w_new();
  ir_avro_put_long(w, lap);
  ir_avro_put_long(w, start);
  ir_avro_put_long(w, elapsed);
  ir_avro_put_boolean(w, stop);
  uint8_t *out = ir_avro_w_take(w, len);
  ir_avro_w_free(w);
  return out;
}

/* Find a channel id by name; returns -1 if absent. */
static int64_t find_channel(ir_conn *c, const char *name) {
  ir_channel_info chans[64];
  size_t n = 0;
  ir_list_channels(c, chans, 64, &n);
  for (size_t i = 0; i < n; i++)
    if (strcmp(chans[i].name, name) == 0)
      return chans[i].channel_id;
  return -1;
}

static int count_present(ir_conn *c, int n) {
  ir_channel_info chans[64];
  size_t got = 0;
  ir_list_channels(c, chans, 64, &got);
  int present = 0;
  for (int i = 0; i < n; i++) {
    char name[32];
    snprintf(name, sizeof name, "bench-%d", i);
    for (size_t j = 0; j < got; j++)
      if (strcmp(chans[j].name, name) == 0) {
        present++;
        break;
      }
  }
  return present;
}

int main(int argc, char **argv) {
  if (argc < 4) {
    fprintf(stderr, "usage: bench_c <index> <num_services> <laps>\n");
    return 2;
  }
  int index = atoi(argv[1]);
  int n = atoi(argv[2]);
  long long laps = atoll(argv[3]);

  char app[32];
  snprintf(app, sizeof app, "bench-%d", index);
  ir_conn *conn = ir_connect(app);
  if (!conn) {
    fprintf(stderr, "connect failed (broker up?)\n");
    return 1;
  }
  ir_publisher *pub = ir_publish_channel(conn, app, SCHEMA, KEY);
  if (!pub) {
    fprintf(stderr, "publish_channel failed\n");
    return 1;
  }

  char prev[32];
  snprintf(prev, sizeof prev, "bench-%d", (index + n - 1) % n);
  int64_t cid = -1;
  while ((cid = find_channel(conn, prev)) < 0)
    usleep(20000);
  ir_subscriber *sub = ir_subscribe(conn, cid, KEY);
  if (!sub) {
    fprintf(stderr, "subscribe failed\n");
    return 1;
  }

  if (index == 0) {
    while (count_present(conn, n) < n)
      usleep(20000);
    usleep(300000);

    int64_t t0 = now_ns();
    size_t blen = 0;
    uint8_t *b = encode_token(0, t0, 0, 0, &blen);
    ir_publish(pub, b, blen);
    ir_free(b);

    for (;;) {
      uint8_t *body = NULL;
      size_t len = 0;
      if (ir_recv(sub, 30000, &body, &len) != 1)
        break;
      ir_avro_r *r = ir_avro_r_new(body, len);
      int64_t lap = ir_avro_get_long(r) + 1;
      ir_avro_r_free(r);
      ir_free(body);
      if (lap >= laps) {
        int64_t elapsed = now_ns() - t0;
        size_t sl = 0;
        uint8_t *s = encode_token(lap, t0, elapsed, 1, &sl);
        ir_publish(pub, s, sl);
        ir_free(s);
        double secs = (double)elapsed / 1e9;
        printf("ring-bench(c): %lld laps across %d services in %.3fs | %.0f laps/s | %lld ns/lap | %lld ns/hop\n", laps,
               n, secs, (double)laps / secs, elapsed / laps, elapsed / (laps * n));
        usleep(300000);
        break;
      }
      size_t nl = 0;
      uint8_t *nb = encode_token(lap, t0, 0, 0, &nl);
      ir_publish(pub, nb, nl);
      ir_free(nb);
    }
  } else {
    for (;;) {
      uint8_t *body = NULL;
      size_t len = 0;
      if (ir_recv(sub, 30000, &body, &len) != 1)
        break;
      ir_avro_r *r = ir_avro_r_new(body, len);
      ir_avro_get_long(r); /* lap */
      ir_avro_get_long(r); /* start */
      ir_avro_get_long(r); /* elapsed */
      int stop = ir_avro_get_boolean(r);
      ir_avro_r_free(r);
      ir_publish(pub, body, len); /* forward the same bytes */
      ir_free(body);
      if (stop)
        break;
    }
  }

  ir_subscriber_free(sub);
  ir_publisher_free(pub);
  ir_disconnect(conn);
  return 0;
}
