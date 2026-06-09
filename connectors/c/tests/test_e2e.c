/* C connector end-to-end test against a running `impulsed` broker.
 *
 * Drives two C clients through the bus: one publishes a channel and exposes a
 * function; the other lists, subscribes, receives, and calls — plus the
 * negative access-control paths. Exits 0 on success, non-zero on failure.
 *
 * The broker is started/stopped by the harness (see run_tests.sh).
 */
#include "impulse_ring.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define METRIC_SCHEMA                                                                                                  \
  "{\"type\":\"record\",\"name\":\"Metric\",\"namespace\":\"ring.examples\","                                          \
  "\"fields\":[{\"name\":\"name\",\"type\":\"string\"},"                                                               \
  "{\"name\":\"value\",\"type\":\"double\"}]}"
#define ADD_REQ_SCHEMA                                                                                                 \
  "{\"type\":\"record\",\"name\":\"AddReq\",\"namespace\":\"ring.examples\","                                          \
  "\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"long\"}]}"
#define ADD_RESP_SCHEMA                                                                                                \
  "{\"type\":\"record\",\"name\":\"AddResp\",\"namespace\":\"ring.examples\","                                         \
  "\"fields\":[{\"name\":\"sum\",\"type\":\"long\"}]}"

static int failures = 0;
#define CHECK(cond, msg)                                                                                               \
  do {                                                                                                                 \
    if (!(cond)) {                                                                                                     \
      fprintf(stderr, "FAIL: %s\n", msg);                                                                              \
      failures++;                                                                                                      \
    } else {                                                                                                           \
      fprintf(stderr, "ok: %s\n", msg);                                                                                \
    }                                                                                                                  \
  } while (0)

/* add(a,b) handler: decode AddReq, encode AddResp{sum}. */
static int add_handler(const uint8_t *req, size_t req_len, uint8_t **resp, size_t *resp_len, void *user) {
  (void)user;
  ir_avro_r *r = ir_avro_r_new(req, req_len);
  int64_t a = ir_avro_get_long(r);
  int64_t b = ir_avro_get_long(r);
  ir_avro_r_free(r);
  ir_avro_w *w = ir_avro_w_new();
  ir_avro_put_long(w, a + b);
  *resp = ir_avro_w_take(w, resp_len);
  ir_avro_w_free(w);
  return 0;
}

int main(void) {
  ir_conn *svc = ir_connect("c-svc");
  if (!svc) {
    fprintf(stderr, "FAIL: connect svc\n");
    return 1;
  }

  /* publish a channel + one message */
  ir_publisher *pub = ir_publish_channel(svc, "c-metrics", METRIC_SCHEMA, "chan-key");
  CHECK(pub != NULL, "publish channel");
  if (pub) {
    ir_avro_w *w = ir_avro_w_new();
    ir_avro_put_string(w, "cpu");
    ir_avro_put_double(w, 0.75);
    size_t blen;
    const uint8_t *b = ir_avro_w_bytes(w, &blen);
    CHECK(ir_publish(pub, b, blen) == IR_OK, "publish message");
    ir_avro_w_free(w);
  }

  /* expose add() */
  CHECK(ir_expose_function(svc, "c-add", ADD_REQ_SCHEMA, ADD_RESP_SCHEMA, "fn-key", add_handler, NULL) == IR_OK,
        "expose function");

  ir_conn *cli = ir_connect("c-cli");
  if (!cli) {
    fprintf(stderr, "FAIL: connect cli\n");
    return 1;
  }

  /* list channels, find ours */
  ir_channel_info chans[32];
  size_t n = 0;
  ir_list_channels(cli, chans, 32, &n);
  int64_t cid = -1;
  int requires_key = 0;
  for (size_t i = 0; i < n; i++) {
    if (strcmp(chans[i].name, "c-metrics") == 0) {
      cid = chans[i].channel_id;
      requires_key = chans[i].requires_key;
    }
  }
  CHECK(cid >= 0, "channel listed");
  CHECK(requires_key == 1, "channel requires key");

  /* wrong key denied */
  ir_subscriber *bad = ir_subscribe(cli, cid, "wrong");
  CHECK(bad == NULL, "subscribe wrong key denied");

  /* correct key + receive */
  ir_subscriber *sub = ir_subscribe(cli, cid, "chan-key");
  CHECK(sub != NULL, "subscribe ok");
  if (sub) {
    uint8_t *body = NULL;
    size_t len = 0;
    int got = ir_recv(sub, 2000, &body, &len);
    CHECK(got == 1, "received message");
    if (got == 1) {
      ir_avro_r *r = ir_avro_r_new(body, len);
      char *name = ir_avro_get_string(r);
      double value = ir_avro_get_double(r);
      ir_avro_r_free(r);
      CHECK(name && strcmp(name, "cpu") == 0, "metric name == cpu");
      CHECK(value > 0.74 && value < 0.76, "metric value == 0.75");
      free(name);
      ir_free(body);
    }
    ir_subscriber_free(sub);
  }

  /* call add(7,35) == 42 */
  ir_avro_w *aw = ir_avro_w_new();
  ir_avro_put_long(aw, 7);
  ir_avro_put_long(aw, 35);
  size_t alen;
  const uint8_t *ab = ir_avro_w_bytes(aw, &alen);
  uint8_t *resp = NULL;
  size_t resp_len = 0;
  int crc = ir_call(cli, "c-add", "fn-key", ab, alen, 5000, &resp, &resp_len);
  CHECK(crc == IR_OK, "rpc call ok");
  if (crc == IR_OK) {
    ir_avro_r *r = ir_avro_r_new(resp, resp_len);
    int64_t sum = ir_avro_get_long(r);
    ir_avro_r_free(r);
    CHECK(sum == 42, "add(7,35) == 42");
    ir_free(resp);
  }
  ir_avro_w_free(aw);

  /* wrong function key denied */
  ir_avro_w *aw2 = ir_avro_w_new();
  ir_avro_put_long(aw2, 1);
  ir_avro_put_long(aw2, 1);
  const uint8_t *ab2 = ir_avro_w_bytes(aw2, &alen);
  uint8_t *resp2 = NULL;
  size_t rl2 = 0;
  int crc2 = ir_call(cli, "c-add", "nope", ab2, alen, 2000, &resp2, &rl2);
  CHECK(crc2 == IR_ERR_DENIED, "rpc wrong key denied");
  ir_avro_w_free(aw2);

  ir_publisher_free(pub);
  ir_disconnect(cli);
  ir_disconnect(svc);

  fprintf(stderr, "\n%s (%d failures)\n", failures ? "FAILED" : "PASSED", failures);
  return failures ? 1 : 0;
}
