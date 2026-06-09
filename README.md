# Ring

**Ring** is a cross-language IPC bus by **Impulse**: data channels and async
function calls between processes on one machine over **pure POSIX shared
memory** — no HTTP, no sockets, no kernel round-trips on the data path. Every
message and every function argument/result is **Apache Avro** (binary,
versioned, schema-bearing), and interface compatibility is enforced by
**CRC-64-AVRO schema fingerprints**.

It exists to replace two things on a single host:

- **Cross-language FFI** that otherwise collapses to C bindings.
- **Local REST / gRPC / JSON-RPC** between microservices, which needlessly copy
  data user-space → kernel → user-space. Ring keeps payloads in shared memory.

Tier 0 target: **Linux only**, arm64 + amd64.

## Status — Milestone 1

This milestone delivers the broker, the shared core, and the **Rust** connector,
with a full end-to-end path validated by an integration test:

- ✅ `impulsed` broker: owns the control segment + data arenas, registers apps,
  routes the control plane, garbage-collects shared memory on crash/shutdown.
- ✅ `impulse-core`: shared-memory segments, ring buffers (futex wakeup, adaptive
  spin), Avro framing + fingerprinting, control protocol records.
- ✅ `impulse-connector` (Rust): register, publish/subscribe (key-gated,
  fingerprint-checked), expose functions, async RPC `call`.
- ✅ Byte-for-byte wire spec in [`SPEC/`](SPEC/) — the contract for native
  connectors in other languages.

### Milestone 2 — native connectors (in progress)

Other-language connectors are implemented **natively** against `SPEC/` (no
binding to the Rust core). Per project policy, schema fingerprints are computed
by the broker, so connectors send schema JSON and use the returned fingerprints.

- ✅ **C** connector — [`connectors/c/`](connectors/c/): pure C11 + POSIX, with a
  self-test and a C↔Rust cross-language data-plane test.
- ⏳ **Python** connector — next.
- ⏳ Go, C++, JS/TS — later.

## Workspace layout

```
crates/
  impulse-core/        shared mechanics (shm, ring, futex, frame, avro, proto, control)
  impulsed/            the broker daemon (+ E2E test in tests/)
  impulse-connector/   the Rust connector (+ runnable demo in examples/)
SPEC/
  wire-format.md       normative byte layout (segments, rings, frames, Avro)
  bootstrap.md         socket-free discovery & handshake
  schemas/             control.avsc, FINGERPRINTS.md, example user schemas
connectors/
  c/                   native C connector (lib + header + tests)
```

## Quickstart

```sh
# Terminal 1 — start the broker
cargo run -p impulsed

# Terminal 2 — run the demo connector (publish/subscribe + RPC)
cargo run -p impulse-connector --example demo
```

Using the connector from Rust:

```rust
use impulse_connector::Connection;
use std::time::Duration;

let conn = Connection::connect("my-service")?;

// Publish a channel (Avro schema is the contract; key gates subscribers).
let pubr = conn.publish_channel("metrics", METRIC_SCHEMA, Some("secret"))?;
pubr.publish(&metric)?;

// Subscribe (broker checks key + schema fingerprint).
let sub = conn.subscribe(channel_id, Some("secret"), METRIC_SCHEMA)?;
let m: Metric = sub.recv(Duration::from_secs(1))?.unwrap();

// Expose and call functions.
conn.expose_function::<Req, Resp, _>("add", REQ_SCHEMA, RESP_SCHEMA, None, |r| handle(r))?;
let resp: Resp = conn.call_blocking("add", None, &req, REQ_SCHEMA, RESP_SCHEMA, Duration::from_secs(5))?;
```

## Build & test

```sh
cargo test --workspace      # unit + integration (spawns the real broker)
cargo clippy --workspace --all-targets
cargo fmt --check
```

Regenerate the control schemas / fingerprints after a protocol change:

```sh
cargo run -p impulse-core --example dump_schemas      > SPEC/schemas/control.avsc
cargo run -p impulse-core --example dump_schemas fps  > SPEC/schemas/FINGERPRINTS.md
```

## Design notes & known limits (M1)

- Rings are **MPSC-safe** via a futex mutex on the producer side; a lock-free
  SPSC fast path is a planned, wire-compatible optimization.
- One subscriber per channel; channel fan-out is a later milestone.
- Heartbeat reaping is lenient; access keys use salted SHA-256 (argon2 later).

See [`SPEC/wire-format.md`](SPEC/wire-format.md) for the full contract.

## License

MIT — see [LICENSE](LICENSE).
