# Ring — TypeScript / JavaScript connector

JavaScript can't mmap shared memory or do cross-process atomics/futex in pure JS,
so this connector binds the native **C** connector (`libimpulse_ring.so`) through
**Bun's built-in FFI** (`bun:ffi`). The Avro codec is pure TypeScript
([`src/avro.ts`](src/avro.ts)); the transport (shm rings, frames, control plane)
is the proven C core.

Requires **[Bun](https://bun.sh)**; Linux only (Tier 0: arm64/amd64).

## Scope

Supported (the client surface — the common JS use cases):

- `connect`, `publishChannel` + `publish`, `listChannels`, `subscribe` + `recv`,
  `call`.

**Not** supported via the FFI binding: *exposing* a function (being an RPC
server). The C connector runs an exposed function's handler on its own thread and
would have to call **synchronously** back into the single-threaded JS VM, which
Bun's FFI can't do safely. Expose functions from Rust/C/C++/Go/Python instead;
TS/JS remains a first-class **caller** and **subscriber**.

## Build the native library

```sh
cmake -S connectors/c -B connectors/c/build -DCMAKE_BUILD_TYPE=Release
cmake --build connectors/c/build --target impulse_ring_shared
# -> connectors/c/build/libimpulse_ring.so   (override the path with IMPULSE_RING_LIB)
```

## Use

```ts
import { Connection, Encoder, Decoder } from "./src/index";

const conn = new Connection("my-ts-service");

// publish
const pub = conn.publishChannel("metrics", METRIC_SCHEMA, "secret");
pub.publish(new Encoder().putString("cpu").putDouble(0.75).bytes_());

// subscribe ("" key means public)
const sub = conn.subscribe(channelId, "secret");
const body = sub.recv(1000);
if (body) { const d = new Decoder(body); const name = d.string(); const value = d.double(); }

// call a function exposed by another service
const req = new Encoder().putLong(7).putLong(35).bytes_();
const resp = conn.call("add", req);
const sum = new Decoder(resp).long(); // bigint
```

### Ring sizes

Every ring on the bus has a fixed size, and each is sized by the side that knows
what has to fit through it. The sizes are readable from both ends, so a limit
derived from one is a limit on the ring the two sides actually share — not a
constant each hopes the other kept to.

```ts
// A channel's arena is sized by its publisher: it knows how big its messages
// are. The broker clamps to [256 KiB, 128 MiB] and rounds up to a power of two,
// and capacity() reports what it actually allocated — a message bigger than that
// can never be published, so cut a stream into messages by this.
const pub = conn.publishChannel("files", SCHEMA, "", 8 * 1024 * 1024);
const chunk = Math.floor(pub.capacity() / 3); // three in flight keeps it moving
const sub = conn.subscribe(channelId);        // reads the same ring header
sub.capacity();

// A connection's reply ring is sized by the caller: every reply arrives through
// that one ring (control replies and RPC responses alike), so it is the ceiling
// on an answer. Raised to the default (64 KiB) if smaller, capped at 128 MiB.
const cli = new Connection("my-ts-client", 4 * 1024 * 1024);
cli.replyCapacity();

// A function's request arena is sized by the service exposing it, and read by
// the caller that writes into it (one control round-trip, so remember it).
cli.functionArenaCapacity("upload");
```

Run the demo / tests with Bun:

```sh
cargo run -p impulsed &                              # broker
bun run connectors/ts/examples/demo.ts               # publish + list

# cross-language test (TS subscribes to / calls the Rust peer)
cargo build -p impulsed --example peer -p impulse-ring-connector
cd connectors/ts && bun test
```

## Notes

- **Fingerprints are computed by the broker**; `subscribe` sends `expected_fp = 0`
  and incoming frames are validated at runtime by the C core.
- Fingerprints inside Avro bodies are signed `long`; the codec exposes them as
  `bigint`.
- The Avro codec covers the primitives, `bytes`, and single-block arrays.
