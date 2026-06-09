// Demo of the C++ connector. Start the broker first:
//   cargo run -p impulsed
//   ./irpp_demo
#include "impulse_ring.hpp"

#include <iostream>

namespace ir = impulse_ring;

static const char *ADD_REQ = R"({"type":"record","name":"AddReq","fields":[
  {"name":"a","type":"long"},{"name":"b","type":"long"}]})";
static const char *ADD_RESP = R"({"type":"record","name":"AddResp","fields":[
  {"name":"sum","type":"long"}]})";

int main() {
  try {
    ir::Connection svc("cpp-demo-svc");
    svc.expose_function("add", ADD_REQ, ADD_RESP, [](const uint8_t *req, size_t n) {
      ir::AvroReader r(req, n);
      int64_t a = r.get_long(), b = r.get_long();
      ir::AvroWriter w;
      w.put_long(a + b);
      return w.bytes();
    });

    ir::Connection cli("cpp-demo-cli");
    ir::AvroWriter args;
    args.put_long(20).put_long(22);
    ir::Bytes resp = cli.call("add", args.bytes());
    std::cout << "add(20, 22) = " << ir::AvroReader(resp).get_long() << "\n";
  } catch (const ir::Error &e) {
    std::cerr << "error: " << e.what() << "\n";
    return 1;
  }
  return 0;
}
