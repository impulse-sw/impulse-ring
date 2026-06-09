# Authoring Avro schemas for Ring channels and functions

Every payload on the bus is **Apache Avro** binary. You describe the shape of
your data with an Avro **schema** (JSON), and you give:

- **one schema per channel** — the type of each message, and
- **two schemas per function** — the request type and the response type.

The broker computes each schema's **CRC-64-AVRO fingerprint** and uses it to
guarantee both sides agree on the structure. This page is a short, practical
guide; the example schemas live in [`schemas/examples/`](schemas/examples/).

## A schema is an Avro record

Use a `record` with named, typed `fields`:

```json
{
  "type": "record",
  "name": "Metric",
  "namespace": "myapp.telemetry",
  "fields": [
    { "name": "name",  "type": "string" },
    { "name": "value", "type": "double" }
  ]
}
```

- **`name`** + **`namespace`** identify the record. Pick a stable namespace per
  app (e.g. `myapp.telemetry`) so names don't collide across services.
- **`fields`** are ordered (see "Field order matters" below).

## Supported types

The native connectors (C, C++, Go, Python, TS) ship a **minimal positional
codec** covering:

| Avro type | Notes |
|-----------|-------|
| `null` | empty |
| `boolean` | 1 byte |
| `int`, `long` | zig-zag varint (both encode the same way) |
| `float`, `double` | little-endian IEEE-754 |
| `string`, `bytes` | length-prefixed |
| `record` | fields encoded in declaration order |
| `array` | one block: count, then items (`array_start`/`array_end`) |

> The broker (Rust `apache-avro`) understands the full Avro spec, but the
> connectors' built-in encoders/decoders currently cover the subset above. Stick
> to these types for portable messages, or extend a connector's codec if you need
> `enum` / `map` / `union` / `fixed`.

## Channels (messages)

The channel's schema is the message type. Publish with the schema JSON; the
broker returns the fingerprint and the publisher frames every message with it.

```rust
// Rust
let pubr = conn.publish_channel("metrics", METRIC_SCHEMA, Some("key"))?;
pubr.publish(&Metric { name: "cpu".into(), value: 0.75 })?;
```

A subscriber decodes each message with the **same** schema. (Rust uses
`serde`-derived structs; C/Go/Python/TS read fields **in order** with the Avro
reader — see below.)

## Functions (request + response)

A function has two schemas — request and response — both records:

```json
// AddReq
{ "type":"record","name":"AddReq","namespace":"myapp.math",
  "fields":[ {"name":"a","type":"long"}, {"name":"b","type":"long"} ] }
// AddResp
{ "type":"record","name":"AddResp","namespace":"myapp.math",
  "fields":[ {"name":"sum","type":"long"} ] }
```

Expose it (the callee), and call it (the caller) with the same two schemas:

```rust
conn.expose_function::<AddReq, AddResp, _>("add", ADD_REQ, ADD_RESP, None,
    |r| AddResp { sum: r.a + r.b })?;
let r: AddResp = conn.call_blocking("add", None, &AddReq{a:7,b:35}, ADD_REQ, ADD_RESP, dur)?;
```

The broker checks that the caller's request fingerprint matches the callee's, and
the callee tags its response with the response fingerprint — so a mismatched
interface fails fast instead of corrupting data.

## Field order matters

The connectors' minimal codecs are **positional**: they write and read fields in
the exact order the schema declares them. So when you hand-encode/decode a body,
follow the field order. For the `AddReq` above:

```c   // C / C++
ir_avro_put_long(w, a);   // field "a"
ir_avro_put_long(w, b);   // field "b"
```
```go  // Go
e.PutLong(a); e.PutLong(b)
```
```python  # Python
e.put_long(a); e.put_long(b)
```
```ts   // TypeScript
new Encoder().putLong(a).putLong(b)
```

Decode in the same order. (Rust's `serde` derive maps by field name, but the
field order in the struct should still match the schema.)

## Versioning & compatibility

- The fingerprint is over the schema's **canonical form**, so whitespace and
  field *documentation* don't affect it, but **field names, types, and order
  do**. Any of those changes produces a new fingerprint.
- Two services interoperate only when their fingerprints match. To evolve a
  message, publish a **new channel** (e.g. `metrics.v2`) or a **new function
  name** rather than silently changing a schema in place.
- Need the exact bytes? See [`wire-format.md`](wire-format.md); the fixed
  control-plane fingerprints are in [`schemas/FINGERPRINTS.md`](schemas/FINGERPRINTS.md).

## Checklist

1. Model your data as an Avro `record` with a stable `name` + `namespace`.
2. Use only the [supported types](#supported-types) for portability.
3. One schema for a channel; two (request, response) for a function.
4. Encode/decode fields **in declaration order**.
5. Treat a schema change as a new channel/function (fingerprints are exact).
