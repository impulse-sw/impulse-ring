// C++ connector end-to-end test against a running `impulsed` broker. Drives two
// clients through register -> publish/subscribe (key-gated) -> RPC, plus the
// negative access-control paths. Exits 0 on success.
#include "impulse_ring.hpp"

#include <cmath>
#include <iostream>
#include <string>

namespace ir = impulse_ring;

static int failures = 0;
static void check(bool ok, const std::string &msg) {
  std::cerr << (ok ? "ok: " : "FAIL: ") << msg << "\n";
  if (!ok)
    failures++;
}

static const char *METRIC = R"({"type":"record","name":"Metric","namespace":"ring.examples",
  "fields":[{"name":"name","type":"string"},{"name":"value","type":"double"}]})";
static const char *ADD_REQ = R"({"type":"record","name":"AddReq","namespace":"ring.examples",
  "fields":[{"name":"a","type":"long"},{"name":"b","type":"long"}]})";
static const char *ADD_RESP = R"({"type":"record","name":"AddResp","namespace":"ring.examples",
  "fields":[{"name":"sum","type":"long"}]})";

int main() {
  try {
    ir::Connection svc("cpp-svc");
    auto pub = svc.publish_channel("cpp-metrics", METRIC, "chan-key");
    ir::AvroWriter m;
    m.put_string("cpu").put_double(0.75);
    pub.publish(m.bytes());
    check(true, "publish channel + message");

    svc.expose_function(
        "cpp-add", ADD_REQ, ADD_RESP,
        [](const uint8_t *req, size_t n) {
          ir::AvroReader r(req, n);
          int64_t a = r.get_long(), b = r.get_long();
          ir::AvroWriter w;
          w.put_long(a + b);
          return w.bytes();
        },
        "fn-key");
    check(true, "expose function");

    ir::Connection cli("cpp-cli");
    int64_t cid = -1;
    bool requires_key = false;
    for (const auto &c : cli.list_channels()) {
      if (c.name == "cpp-metrics") {
        cid = c.channel_id;
        requires_key = c.requires_key;
      }
    }
    check(cid >= 0, "channel listed");
    check(requires_key, "channel requires key");

    bool denied = false;
    try {
      cli.subscribe(cid, "wrong");
    } catch (const ir::Error &) {
      denied = true;
    }
    check(denied, "subscribe wrong key denied");

    auto sub = cli.subscribe(cid, "chan-key");
    auto body = sub.recv(2000);
    check(body.has_value(), "received message");
    if (body) {
      ir::AvroReader r(*body);
      check(r.get_string() == "cpu", "metric name == cpu");
      check(std::abs(r.get_double() - 0.75) < 1e-9, "metric value == 0.75");
    }

    ir::AvroWriter args;
    args.put_long(7).put_long(35);
    ir::Bytes resp = cli.call("cpp-add", args.bytes(), "fn-key");
    check(ir::AvroReader(resp).get_long() == 42, "add(7,35) == 42");

    bool call_denied = false;
    try {
      cli.call("cpp-add", args.bytes(), "nope", 2000);
    } catch (const ir::Error &) {
      call_denied = true;
    }
    check(call_denied, "call wrong key denied");
  } catch (const ir::Error &e) {
    std::cerr << "FAIL: unexpected error: " << e.what() << "\n";
    failures++;
  }

  std::cerr << "\n" << (failures ? "FAILED" : "PASSED") << " (" << failures << " failures)\n";
  return failures ? 1 : 0;
}
