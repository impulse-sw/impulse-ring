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

### Ring sizes

Every ring on the bus has a fixed size, and each is sized by the side that knows
what has to fit through it. The sizes are readable from both ends, so a limit
derived from one is a limit on the ring the two sides actually share — not a
constant each hopes the other kept to.

```c
/* A channel's arena is sized by its publisher: it knows how big its messages
 * are. The broker clamps to [256 KiB, 128 MiB] and rounds up to a power of two,
 * and ir_publisher_capacity reports what it actually allocated — a message
 * bigger than that can never be published, so cut a stream by this. */
ir_publisher *p = ir_publish_channel_with_arena(c, "files", SCHEMA, NULL, 8u << 20);
uint64_t chunk = ir_publisher_capacity(p) / 3;  /* three in flight keeps it moving */

/* A connection's reply ring is sized by the caller: every reply arrives through
 * that one ring (control replies and RPC responses alike), so it is the ceiling
 * on an answer. Raised to the default (64 KiB) if smaller, capped at 128 MiB,
 * rounded up to a power of two. */
ir_conn *cli = ir_connect_with_reply_cap("my-c-client", 4u << 20);
uint64_t reply = ir_reply_capacity(cli);

/* A function's request arena is sized by the service exposing it, and read by
 * the caller that writes into it (one control round-trip, so remember it). */
uint64_t fn_cap = 0;
ir_function_arena_capacity(cli, "upload", NULL, &fn_cap);
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
