/* Minimal demo of the C connector. Start the broker first:
 *   cargo run -p impulsed
 * then:
 *   ./ir_demo
 */
#include "impulse_ring.h"

#include <stdio.h>
#include <stdlib.h>

#define ADD_REQ "{\"type\":\"record\",\"name\":\"AddReq\",\"fields\":[" \
                "{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"long\"}]}"
#define ADD_RESP "{\"type\":\"record\",\"name\":\"AddResp\",\"fields\":[" \
                 "{\"name\":\"sum\",\"type\":\"long\"}]}"

static int add(const uint8_t *req, size_t n, uint8_t **resp, size_t *rn, void *u) {
    (void)u;
    ir_avro_r *r = ir_avro_r_new(req, n);
    int64_t a = ir_avro_get_long(r), b = ir_avro_get_long(r);
    ir_avro_r_free(r);
    ir_avro_w *w = ir_avro_w_new();
    ir_avro_put_long(w, a + b);
    *resp = ir_avro_w_take(w, rn);
    ir_avro_w_free(w);
    return 0;
}

int main(void) {
    ir_conn *svc = ir_connect("c-demo-svc");
    if (!svc) {
        fprintf(stderr, "broker not running?\n");
        return 1;
    }
    ir_expose_function(svc, "add", ADD_REQ, ADD_RESP, NULL, add, NULL);

    ir_conn *cli = ir_connect("c-demo-cli");
    ir_avro_w *w = ir_avro_w_new();
    ir_avro_put_long(w, 20);
    ir_avro_put_long(w, 22);
    size_t n;
    const uint8_t *b = ir_avro_w_bytes(w, &n);
    uint8_t *resp = NULL;
    size_t rn = 0;
    if (ir_call(cli, "add", NULL, b, n, 5000, &resp, &rn) == IR_OK) {
        ir_avro_r *r = ir_avro_r_new(resp, rn);
        printf("add(20, 22) = %lld\n", (long long)ir_avro_get_long(r));
        ir_avro_r_free(r);
        ir_free(resp);
    } else {
        fprintf(stderr, "call failed: %s\n", ir_last_error(cli));
    }
    ir_avro_w_free(w);
    ir_disconnect(cli);
    ir_disconnect(svc);
    return 0;
}
