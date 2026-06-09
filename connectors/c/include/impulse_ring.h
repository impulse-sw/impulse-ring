/* Ring — native C connector (by Impulse).
 *
 * A socket-free, shared-memory IPC connector that speaks the Ring wire protocol
 * directly (see spec/). It registers an application, publishes/subscribes to
 * key-gated channels, exposes functions, and calls remote functions — all over
 * POSIX shared memory, with Apache Avro payloads.
 *
 * Linux only (Tier 0: arm64/amd64). Requires the `impulsed` broker to be
 * running. Per the project's fingerprint policy, schema fingerprints are
 * computed by the broker; this connector sends schema JSON and uses the
 * fingerprints the broker returns.
 *
 * All `_body`/`_resp` byte buffers returned to the caller are heap-allocated and
 * must be released with ir_free().
 */
#ifndef IMPULSE_RING_H
#define IMPULSE_RING_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ---- error codes (negative returns) ---- */
#define IR_OK 0
#define IR_ERR (-1)          /* generic/IO error            */
#define IR_ERR_TIMEOUT (-2)  /* operation timed out         */
#define IR_ERR_DENIED (-3)   /* access key rejected         */
#define IR_ERR_MISMATCH (-4) /* schema fingerprint mismatch */
#define IR_ERR_NOTFOUND (-5) /* channel/function not found  */
#define IR_ERR_NOBROKER (-6) /* broker not running          */

typedef struct ir_conn ir_conn;
typedef struct ir_publisher ir_publisher;
typedef struct ir_subscriber ir_subscriber;

/* Metadata for one channel returned by ir_list_channels. */
typedef struct {
  int64_t channel_id;
  char name[256];
  char owner_app[256];
  uint64_t schema_fp;
  int requires_key;
} ir_channel_info;

/* Function handler: decode `req`/`req_len`, produce a heap-allocated Avro body
 * in `*resp`/`*resp_len` (ownership passes to the connector, which frees it).
 * Return 0 on success, non-zero to signal a remote error. */
typedef int (*ir_handler)(const uint8_t *req, size_t req_len, uint8_t **resp, size_t *resp_len, void *user);

/* ---- connection lifecycle ---- */

/* Connect to the broker and register `app_name`. Returns NULL on failure. */
ir_conn *ir_connect(const char *app_name);
void ir_disconnect(ir_conn *c);

/* Last human-readable error for the connection (never NULL). */
const char *ir_last_error(ir_conn *c);

/* Release a buffer handed back by ir_recv / ir_call. */
void ir_free(void *p);

/* ---- channels ---- */

/* Publish a channel. `key` may be NULL for a public channel. NULL on failure. */
ir_publisher *ir_publish_channel(ir_conn *c, const char *name, const char *schema_json, const char *key);

/* Publish one Avro-encoded message. Returns IR_OK or a negative code. */
int ir_publish(ir_publisher *p, const uint8_t *avro_body, size_t len);
void ir_publisher_free(ir_publisher *p);

/* List channels into `out` (capacity `max`); writes the count to `*count`. */
int ir_list_channels(ir_conn *c, ir_channel_info *out, size_t max, size_t *count);

/* Subscribe to a channel by id. `key` may be NULL. NULL on failure. */
ir_subscriber *ir_subscribe(ir_conn *c, int64_t channel_id, const char *key);

/* Receive the next message. Returns 1 (got a message; `*body`/`*len` set and
 * owned by caller), 0 (timeout), or a negative error code. */
int ir_recv(ir_subscriber *s, int timeout_ms, uint8_t **body, size_t *len);
void ir_subscriber_free(ir_subscriber *s);

/* The channel's schema fingerprint as reported by the broker. */
uint64_t ir_subscriber_schema_fp(const ir_subscriber *s);

/* ---- functions / RPC ---- */

/* Expose a function served by `handler` on a background thread. */
int ir_expose_function(ir_conn *c, const char *name, const char *req_schema_json, const char *resp_schema_json,
                       const char *key, ir_handler handler, void *user);

/* Call a remote function and block for the response. On success returns IR_OK
 * with `*resp`/`*resp_len` set (owned by caller). */
int ir_call(ir_conn *c, const char *fn_name, const char *key, const uint8_t *req, size_t req_len, int timeout_ms,
            uint8_t **resp, size_t *resp_len);

/* ---- minimal Avro datum writer (build message bodies) ---- */

typedef struct ir_avro_w ir_avro_w;
ir_avro_w *ir_avro_w_new(void);
void ir_avro_w_free(ir_avro_w *w);
void ir_avro_put_null(ir_avro_w *w);
void ir_avro_put_boolean(ir_avro_w *w, int v);
void ir_avro_put_int(ir_avro_w *w, int32_t v);
void ir_avro_put_long(ir_avro_w *w, int64_t v);
void ir_avro_put_float(ir_avro_w *w, float v);
void ir_avro_put_double(ir_avro_w *w, double v);
void ir_avro_put_string(ir_avro_w *w, const char *s);
void ir_avro_put_bytes(ir_avro_w *w, const uint8_t *b, size_t n);
/* Arrays are written as a single block: call ir_avro_array_start(count) before
 * the `count` items, then ir_avro_array_end(). */
void ir_avro_array_start(ir_avro_w *w, int64_t count);
void ir_avro_array_end(ir_avro_w *w);
/* Borrow the accumulated bytes (valid until the writer is freed/modified). */
const uint8_t *ir_avro_w_bytes(const ir_avro_w *w, size_t *len);
/* Detach a heap copy of the bytes (caller frees via ir_free). */
uint8_t *ir_avro_w_take(ir_avro_w *w, size_t *len);

/* ---- minimal Avro datum reader (decode message bodies) ---- */

typedef struct ir_avro_r ir_avro_r;
ir_avro_r *ir_avro_r_new(const uint8_t *data, size_t len);
void ir_avro_r_free(ir_avro_r *r);
int ir_avro_get_boolean(ir_avro_r *r);
int32_t ir_avro_get_int(ir_avro_r *r);
int64_t ir_avro_get_long(ir_avro_r *r);
float ir_avro_get_float(ir_avro_r *r);
double ir_avro_get_double(ir_avro_r *r);
/* Returns a heap-allocated NUL-terminated string (caller frees via ir_free). */
char *ir_avro_get_string(ir_avro_r *r);
/* Reads a long block count (positive). For multi-block arrays call repeatedly
 * until it returns 0. */
int64_t ir_avro_get_array_count(ir_avro_r *r);

#ifdef __cplusplus
}
#endif

#endif /* IMPULSE_RING_H */
