# Ring — C connector

A **native** C connector for Ring. It implements the wire protocol from
[`spec/`](../../spec/) directly (shared memory, ring buffers, futex wakeup,
frames, Avro datums, control messages) — it does **not** bind to the Rust core.
Pure C11 + POSIX; Linux only (Tier 0: arm64/amd64).

## Build

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
# -> build/libimpulse_ring.a, build/libimpulse_ring.so, build/ir_demo
```

## Use

```c
#include "impulse_ring.h"

ir_conn *c = ir_connect("my-c-service");

/* publish: build an Avro body with the datum writer */
ir_publisher *p = ir_publish_channel(c, "metrics", METRIC_SCHEMA, "key");
ir_avro_w *w = ir_avro_w_new();
ir_avro_put_string(w, "cpu");
ir_avro_put_double(w, 0.75);
size_t n; const uint8_t *b = ir_avro_w_bytes(w, &n);
ir_publish(p, b, n);
ir_avro_w_free(w);

/* call a remote function */
uint8_t *resp; size_t rn;
ir_call(c, "add", NULL, req, req_len, 5000, &resp, &rn);
ir_free(resp);

ir_disconnect(c);
```

See [`examples/demo.c`](examples/demo.c) for a full publish/subscribe + RPC flow,
and the public API in [`include/impulse_ring.h`](include/impulse_ring.h).

## CI & tests

CI (see `.depl/config.yaml`) lints the C sources (`clang-format` at 2-space/120
plus `clang-tidy`) and runs the C example against a live broker in the
`connector-examples` pipeline. To run things locally:

```sh
# build broker + Rust peer (for the cross-language test) and the C library
cargo build -p impulsed --example peer -p impulse-ring-connector
cmake -S connectors/c -B connectors/c/build && cmake --build connectors/c/build

./target/debug/impulsed &                 # start the broker
./connectors/c/build/ir_test_e2e          # C self-test (register/pub-sub/RPC + ACL)
./target/debug/examples/peer &            # Rust peer for the cross-language test
./connectors/c/build/ir_xlang             # C subscribes to / calls Rust
```

1. **C self-test** (`tests/test_e2e.c`): two C clients do register →
   publish/subscribe (key-gated) → RPC, plus negative ACL cases.
2. **Cross-language** (`tests/xlang_client.c` + the Rust `peer` example): a C
   client subscribes to a channel **published by Rust** and calls a function
   **exposed by Rust**, proving C↔Rust Avro data-plane interop.

## Notes & limits

- **Fingerprints are computed by the broker** (per project policy): the connector
  sends schema JSON and uses the fingerprints the broker returns. `subscribe`
  sends `expected_fp = 0`; incoming frames are still validated at runtime against
  the channel fingerprint the broker reported.
- The included Avro datum reader/writer covers the primitive types, `bytes`, and
  single-block arrays — enough for record payloads. Extend as needed.
- Returned `body`/`resp` buffers are heap-allocated; free them with `ir_free`.
