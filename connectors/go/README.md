# Ring — Go connector

A **native** Go connector for Ring. It implements the wire protocol from
[`spec/`](../../spec/) directly (shared memory, ring buffers, futex wakeup,
frames, Avro datums, control plane) — it does **not** bind to the Rust core, and
uses **no cgo**: cross-process atomics are `sync/atomic` on the mmap'd memory and
blocking uses the Linux `futex` syscall.

Pure Go (stdlib only); Linux only (Tier 0: arm64/amd64).

## Use

```go
import ring "github.com/goidago/impulse-ring/connectors/go"

conn, _ := ring.Connect("my-go-service")
defer conn.Close()

// publish (build the Avro body with the datum encoder)
pub, _ := conn.PublishChannel("metrics", metricSchema, "secret")
e := ring.NewEncoder(); e.PutString("cpu"); e.PutDouble(0.75)
pub.Publish(e.Bytes())

// subscribe (broker checks the key); "" means no key
sub, _ := conn.Subscribe(channelID, "secret")
body, _ := sub.Recv(1000)
d := ring.NewDecoder(body); name := d.String(); value := d.Double()

// expose a function: handler(req []byte) (resp []byte, err error)
conn.ExposeFunction("add", reqSchema, respSchema, "", func(req []byte) ([]byte, error) {
	d := ring.NewDecoder(req)
	o := ring.NewEncoder(); o.PutLong(d.Long() + d.Long())
	return o.Bytes(), nil
})
// ...or size the request arena (bytes; 0 = broker default, clamped to
// [256 KiB, 128 MiB] and rounded up to a power of two):
conn.ExposeFunctionWithArena("upload", reqSchema, respSchema, "", 4*1024*1024, handler)

// call (blocking)
a := ring.NewEncoder(); a.PutLong(7); a.PutLong(35)
resp, _ := conn.Call("add", "", a.Bytes(), 5000)
```

See [`examples/demo`](examples/demo) for a full publish/subscribe + RPC flow.

## Tests

The tests build the broker + Rust `peer`, then run a self-test and a Go↔Rust
cross-language test:

```sh
cargo build -p impulsed --example peer -p impulse-ring-connector
cd connectors/go && go test ./...
```

`TestSelfFlow` covers register → publish/subscribe (key-gated) → RPC plus the
negative ACL paths; `TestCrossLanguage` subscribes to a channel published by Rust
and calls a function it exposes.

## Notes & limits

- **Fingerprints are computed by the broker** (project policy): the connector
  sends schema JSON and uses the fingerprints the broker returns. `Subscribe`
  sends `expected_fp = 0`; incoming frames are validated at runtime.
- Fingerprints inside Avro bodies are signed `long` and kept signed end-to-end.
- The Avro codec covers the primitives, `bytes`, and single-block arrays — enough
  for record payloads.
- `Publisher`/`Subscriber` hold an mmap; call their `Close` when done.
