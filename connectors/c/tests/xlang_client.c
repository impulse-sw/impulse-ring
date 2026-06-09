/* Cross-language client: subscribes to a channel published by the Rust peer and
 * calls a function exposed by it, proving C<->Rust data-plane Avro interop.
 * Expects the broker and the `peer` Rust example to be running. Exits 0 on ok.
 */
#include "impulse_ring.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define MUL_REQ                                                                                                        \
  "{\"type\":\"record\",\"name\":\"MulReq\",\"namespace\":\"ring.xlang\","                                             \
  "\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"long\"}]}"

static int failures = 0;
#define CHECK(c, m)                                                                                                    \
  do {                                                                                                                 \
    if (!(c)) {                                                                                                        \
      fprintf(stderr, "FAIL: %s\n", m);                                                                                \
      failures++;                                                                                                      \
    } else                                                                                                             \
      fprintf(stderr, "ok: %s\n", m);                                                                                  \
  } while (0)

int main(void) {
  ir_conn *c = ir_connect("c-xlang");
  if (!c) {
    fprintf(stderr, "FAIL: connect (broker/peer up?)\n");
    return 1;
  }

  /* find the Rust-published channel */
  ir_channel_info chans[32];
  size_t n = 0;
  int64_t cid = -1;
  for (int tries = 0; tries < 50 && cid < 0; tries++) {
    ir_list_channels(c, chans, 32, &n);
    for (size_t i = 0; i < n; i++)
      if (strcmp(chans[i].name, "rmetrics") == 0)
        cid = chans[i].channel_id;
    if (cid < 0)
      usleep(100000);
  }
  CHECK(cid >= 0, "found rust channel 'rmetrics'");

  if (cid >= 0) {
    ir_subscriber *s = ir_subscribe(c, cid, NULL);
    CHECK(s != NULL, "subscribed to rust channel");
    if (s) {
      uint8_t *body = NULL;
      size_t len = 0;
      int got = ir_recv(s, 3000, &body, &len);
      CHECK(got == 1, "received rust-published message");
      if (got == 1) {
        ir_avro_r *r = ir_avro_r_new(body, len);
        char *name = ir_avro_get_string(r);
        double v = ir_avro_get_double(r);
        ir_avro_r_free(r);
        CHECK(name && strcmp(name, "temp") == 0, "rust metric name == temp");
        CHECK(v > 21.4 && v < 21.6, "rust metric value == 21.5");
        free(name);
        ir_free(body);
      }
      ir_subscriber_free(s);
    }
  }

  /* call the Rust-exposed function rmul(6,7) == 42 */
  ir_avro_w *w = ir_avro_w_new();
  ir_avro_put_long(w, 6);
  ir_avro_put_long(w, 7);
  size_t blen;
  const uint8_t *b = ir_avro_w_bytes(w, &blen);
  uint8_t *resp = NULL;
  size_t rl = 0;
  int rc = ir_call(c, "rmul", NULL, b, blen, 5000, &resp, &rl);
  CHECK(rc == IR_OK, "called rust function rmul");
  if (rc == IR_OK) {
    ir_avro_r *r = ir_avro_r_new(resp, rl);
    int64_t product = ir_avro_get_long(r);
    ir_avro_r_free(r);
    CHECK(product == 42, "rmul(6,7) == 42");
    ir_free(resp);
  }
  ir_avro_w_free(w);

  ir_disconnect(c);
  fprintf(stderr, "\n%s (%d failures)\n", failures ? "FAILED" : "PASSED", failures);
  return failures ? 1 : 0;
}
