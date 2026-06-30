/* Broker-restart recovery test for the C connector.
 *
 * Unlike the other tests, this one *manages its own broker*: it spawns impulsed,
 * connects a service (exposing `add`) and a client, then SIGKILLs the broker and
 * starts a fresh one (a new shared-memory generation with a new epoch) and
 * asserts the existing connections transparently reconnect — the function is
 * re-exposed and the client's RPC keeps working without rebuilding any handle.
 *
 * The broker binary path comes from IMPULSED_BIN (default ./target/release/impulsed).
 */
#include "impulse_ring.h"
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static const char *broker_bin(void) {
  const char *b = getenv("IMPULSED_BIN");
  return b ? b : "./target/release/impulsed";
}

static void msleep(int ms) {
  struct timespec ts = {ms / 1000, (long)(ms % 1000) * 1000000L};
  nanosleep(&ts, NULL);
}

static pid_t start_broker(void) {
  pid_t pid = fork();
  if (pid == 0) {
    /* silence the broker's stderr logging */
    if (!freopen("/dev/null", "w", stderr)) { /* ignore: best-effort */
    }
    execl(broker_bin(), broker_bin(), (char *)NULL);
    _exit(127);
  }
  /* wait for the control segment to appear */
  for (int i = 0; i < 200; i++) {
    if (access("/dev/shm/impulse-ring.ctl.v1", F_OK) == 0) {
      msleep(50);
      return pid;
    }
    msleep(20);
  }
  fprintf(stderr, "broker did not come up (IMPULSED_BIN=%s)\n", broker_bin());
  exit(2);
}

static void kill_broker(pid_t pid) {
  kill(pid, SIGKILL);
  waitpid(pid, NULL, 0);
}

/* AddReq{a,b} as Avro: two longs. AddResp{sum}: one long. */
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

static const char *ADD_REQ = "{\"type\":\"record\",\"name\":\"AddReq\",\"namespace\":\"ring.test\","
                             "\"fields\":[{\"name\":\"a\",\"type\":\"long\"},{\"name\":\"b\",\"type\":\"long\"}]}";
static const char *ADD_RESP = "{\"type\":\"record\",\"name\":\"AddResp\",\"namespace\":\"ring.test\","
                              "\"fields\":[{\"name\":\"sum\",\"type\":\"long\"}]}";

static int call_add(ir_conn *c, int64_t a, int64_t b, int64_t *sum) {
  ir_avro_w *w = ir_avro_w_new();
  ir_avro_put_long(w, a);
  ir_avro_put_long(w, b);
  size_t blen;
  uint8_t *body = ir_avro_w_take(w, &blen);
  ir_avro_w_free(w);
  uint8_t *resp = NULL;
  size_t resp_len = 0;
  int rc = ir_call(c, "add", "fn-key", body, blen, 3000, &resp, &resp_len);
  free(body);
  if (rc != IR_OK)
    return rc;
  ir_avro_r *r = ir_avro_r_new(resp, resp_len);
  *sum = ir_avro_get_long(r);
  ir_avro_r_free(r);
  ir_free(resp);
  return IR_OK;
}

static int failures = 0;
#define CHECK(cond, msg)                                                                                               \
  do {                                                                                                                 \
    if (cond) {                                                                                                        \
      printf("ok: %s\n", msg);                                                                                         \
    } else {                                                                                                           \
      printf("FAIL: %s\n", msg);                                                                                       \
      failures++;                                                                                                      \
    }                                                                                                                  \
  } while (0)

int main(void) {
  pid_t broker = start_broker();

  ir_conn *svc = ir_connect("c-svc-restart");
  CHECK(svc != NULL, "service connected");
  if (!svc)
    return 1;
  int er = ir_expose_function(svc, "add", ADD_REQ, ADD_RESP, "fn-key", add_handler, NULL);
  CHECK(er == IR_OK, "function exposed");

  ir_conn *cli = ir_connect("c-cli-restart");
  CHECK(cli != NULL, "client connected");
  if (!cli)
    return 1;
  int64_t epoch_before = ir_broker_epoch(cli);

  int64_t sum = 0;
  CHECK(call_add(cli, 7, 35, &sum) == IR_OK && sum == 42, "rpc before restart == 42");

  /* Restart the broker. */
  kill_broker(broker);
  pid_t broker2 = start_broker();

  /* Retry until the connection reconnects and the function is re-exposed. */
  int ok = 0;
  for (int i = 0; i < 100; i++) {
    if (call_add(cli, 20, 22, &sum) == IR_OK && sum == 42) {
      ok = 1;
      break;
    }
    msleep(100);
  }
  CHECK(ok, "rpc recovered after restart == 42");
  CHECK(ir_broker_epoch(cli) != epoch_before, "epoch advanced after restart");

  ir_disconnect(cli);
  ir_disconnect(svc);
  kill_broker(broker2);

  printf("\n%s (%d failures)\n", failures ? "FAILED" : "PASSED", failures);
  return failures ? 1 : 0;
}
