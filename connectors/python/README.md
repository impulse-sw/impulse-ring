# Ring — Python connector

A **native** Python connector for Ring. The entire wire protocol (shared memory,
ring buffers, frames, Avro datums, control plane) is implemented in pure Python
against [`SPEC/`](../../SPEC/). The only native code is a tiny C extension,
`_ring_native`, providing cross-process **atomics + futex** — primitives Python's
standard library does not offer and a shared-memory ring fundamentally needs.

Pure Python + one small extension; Linux only (Tier 0: arm64/amd64).

## Build & install

```sh
python3 setup.py build_ext --inplace   # builds _ring_native in place
# or: pip install .
```

## Use

```python
from impulse_ring import Connection, avro

conn = Connection("my-py-service")

# publish (build the Avro body with the datum encoder)
pub = conn.publish_channel("metrics", METRIC_SCHEMA, key="secret")
e = avro.Encoder(); e.put_string("cpu"); e.put_double(0.75)
pub.publish(e.getvalue())

# subscribe (broker checks the key)
sub = conn.subscribe(channel_id, key="secret")
body = sub.recv(1000)
d = avro.Decoder(body); name = d.get_string(); value = d.get_double()

# expose a function: handler(req_body: bytes) -> resp_body: bytes
def add(req):
    d = avro.Decoder(req)
    out = avro.Encoder(); out.put_long(d.get_long() + d.get_long())
    return out.getvalue()
conn.expose_function("add", REQ_SCHEMA, RESP_SCHEMA, add)
# size the request arena (bytes; 0 = broker default, clamped to
# [256 KiB, 128 MiB] and rounded up to a power of two):
conn.expose_function("upload", REQ_SCHEMA, RESP_SCHEMA, add, req_arena_cap=4 * 1024 * 1024)

# call (blocking) or call_async (returns a concurrent.futures.Future)
e = avro.Encoder(); e.put_long(7); e.put_long(35)
resp = conn.call("add", e.getvalue(), timeout=5.0)
fut = conn.call_async("add", e.getvalue())   # fut.result() -> bytes

conn.close()
```

### Ring sizes

Every ring on the bus has a fixed size, and each is sized by the side that knows
what has to fit through it. The sizes are readable from both ends, so a limit
derived from one is a limit on the ring the two sides actually share — not a
constant each hopes the other kept to.

```python
# A channel's arena is sized by its publisher: it knows how big its messages are.
# The broker clamps to [256 KiB, 128 MiB] and rounds up to a power of two, and
# `capacity` reports what it actually allocated — a message bigger than that can
# never be published, so cut a stream into messages by this.
pub = conn.publish_channel("files", SCHEMA, arena_cap=8 * 1024 * 1024)
chunk = pub.capacity // 3          # three messages in flight keeps it moving
sub = conn.subscribe(channel_id)   # the subscriber reads the same header
sub.capacity

# A connection's reply ring is sized by the caller: every reply arrives through
# that one ring (control replies and RPC responses alike), so it is the ceiling
# on an answer. Raised to the default if smaller, capped at 128 MiB, rounded up.
cli = Connection("my-py-client", reply_cap=4 * 1024 * 1024)
cli.reply_capacity

# A function's request arena is sized by the service exposing it, and read by the
# caller that writes into it (one control round-trip, so remember the answer).
cli.function_arena_capacity("upload")
```

## CI & tests

CI (see `.depl/config.yaml`) lints with `ruff` (`uvx ruff check` + `ruff format
--check`, line-length 120) and runs the Python example against a live broker in
the `connector-examples` pipeline. Locally:

```sh
# build broker + Rust peer, then the native extension
cargo build -p impulsed --example peer -p impulse-ring-connector
cd connectors/python && python3 setup.py build_ext --inplace

python3 tests/test_e2e.py   # self-contained: spawns the broker + peer itself
```

The end-to-end test runs:

1. **Python self-test**: two Python clients do register → publish/subscribe
   (key-gated) → RPC, plus negative ACL cases.
2. **Cross-language**: a Python client subscribes to a channel **published by
   Rust** and calls a function **exposed by Rust**, proving Python↔Rust Avro
   data-plane interop.

## Notes & limits

- **Fingerprints are computed by the broker** (project policy): the connector
  sends schema JSON and uses the fingerprints the broker returns. `subscribe`
  sends `expected_fp = 0`; incoming frames are validated at runtime.
- Fingerprints travel inside Avro bodies as **signed `long`**; the connector
  keeps them signed end-to-end (a fingerprint with the high bit set is a
  negative `long`, not an out-of-range unsigned value).
- The Avro codec covers the primitives, `bytes`, and single-block arrays —
  enough for record payloads. Extend as needed.
