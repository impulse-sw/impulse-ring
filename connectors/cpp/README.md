# Ring — C++ connector

A thin, **header-only C++ wrapper** over the native C connector. It does **not**
reimplement the protocol — it reuses the proven C core (which already exposes an
`extern "C"` ABI) and adds idiomatic ergonomics: RAII, `std::string` /
`std::vector` / `std::function`, `std::optional`, and exceptions.

> Why not a separate native C++ implementation? It would duplicate the wire
> protocol for no benefit — C++ can call the C connector directly. The wrapper is
> the right amount of C++.

Requires C++17; Linux only (Tier 0: arm64/amd64).

## Build

```sh
cmake -S connectors/cpp -B connectors/cpp/build -DCMAKE_BUILD_TYPE=Release
cmake --build connectors/cpp/build
# -> build/irpp_demo, build/irpp_test_e2e (the C core is compiled in)
```

To use the header in your own project, add `connectors/cpp/include` and
`connectors/c/include` to your include path and link the C connector library (or
compile `connectors/c/src/impulse_ring.c` with `-D_GNU_SOURCE -lpthread`).

## Use

```cpp
#include "impulse_ring.hpp"
namespace ir = impulse_ring;

ir::Connection conn("my-cpp-service");

// publish (build the Avro body with the writer)
auto pub = conn.publish_channel("metrics", METRIC_SCHEMA, "secret");
ir::AvroWriter m; m.put_string("cpu").put_double(0.75);
pub.publish(m.bytes());

// subscribe (broker checks the key; "" means public)
auto sub = conn.subscribe(channel_id, "secret");
if (auto body = sub.recv(1000)) {
  ir::AvroReader r(*body);
  auto name = r.get_string(); auto value = r.get_double();
}

// expose a function and call it
conn.expose_function("add", REQ, RESP, [](const uint8_t* req, size_t n) {
  ir::AvroReader r(req, n);
  ir::AvroWriter w; w.put_long(r.get_long() + r.get_long());
  return w.bytes();
});
ir::AvroWriter args; args.put_long(7).put_long(35);
ir::Bytes resp = conn.call("add", args.bytes());
```

Unlike the FFI-based TS connector, C++ **can** expose functions: the C service
thread calls the C++ handler directly (it is all native code).

See [`examples/demo.cpp`](examples/demo.cpp) and the header
[`include/impulse_ring.hpp`](include/impulse_ring.hpp).

## Tests

```sh
cargo build -p impulsed
./target/debug/impulsed &
./connectors/cpp/build/irpp_test_e2e   # register/pub-sub/RPC + negative ACL
```
