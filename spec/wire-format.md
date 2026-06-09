# Ring — Wire Format Specification (v1)

> **Ring** is the shared-memory IPC product by **Impulse**. This document is the
> normative, byte-for-byte contract. Native connectors in other languages
> (Go, C/C++, Python, JS/TS) MUST implement exactly what is described here to
> interoperate with the Rust broker (`impulsed`) and the Rust connector.

Tier 0 platform: **Linux only**, arm64 and amd64. All multi-byte integers are
**little-endian**. The reference implementation lives in the `impulse-core`
crate (`shm.rs`, `ring.rs`, `frame.rs`, `control.rs`, `proto.rs`).

---

## 1. Transport: POSIX shared memory

All communication is through POSIX shared-memory segments (`shm_open` +
`mmap`, `MAP_SHARED`). No sockets, no `memfd` fd-passing (that would require
`SCM_RIGHTS` over a socket). Segments surface as files under `/dev/shm`.

Segment names (POSIX names start with `/`):

| Purpose              | Name pattern                          |
|----------------------|---------------------------------------|
| Control segment      | `/impulse-ring.ctl.v1`                |
| Per-client reply     | `/impulse-ring.cli.<nonce>.v1`        |
| Channel data arena   | `/impulse-ring.arena.<channel_id>.v1` |
| Function request arena | `/impulse-ring.fn.<fn_id>.v1`       |

`<nonce>` is a client-chosen random `u64` (decimal). `<channel_id>` / `<fn_id>`
are broker-assigned decimal ids.

---

## 2. Ring buffer

A ring occupies a 64-byte-aligned `base` within a segment. It carries
**length-prefixed records** and may wrap; readers/writers wrap with a two-part
copy. Header layout (offsets relative to `base`, all fields LE):

| Offset | Size | Field        | Notes                                            |
|-------:|-----:|--------------|--------------------------------------------------|
| 0      | 4    | `magic`      | `0x474E4952` ("RING")                            |
| 4      | 4    | `capacity`   | data-region size, **power of two**               |
| 64     | 8    | `head`       | producer write cursor, monotonic `u64`           |
| 72     | 4    | `prod_lock`  | futex mutex guarding producers (MPSC)            |
| 76     | 4    | `space_seq`  | futex word; producers park here for free space   |
| 128    | 8    | `tail`       | consumer read cursor, monotonic `u64`            |
| 136    | 4    | `data_seq`   | futex word; consumer parks here for data         |
| 192    | N    | data region  | `N = capacity` bytes                             |

`RING_HEADER = 192` bytes (three cache lines). Producer/consumer fields are on
separate cache lines to avoid false sharing.

**Record format** within the data region: `[u32 len][len bytes payload]`. Index
into the data region is `cursor & (capacity - 1)`. Free space is
`capacity - (head - tail)`; a record of `4 + len` bytes is rejected
(backpressure) if it does not fit.

### 2.1 Memory ordering (required)

* Producer writes the payload, then publishes `head` with a **Release** store.
* Consumer loads `head` with **Acquire** before reading the payload.
* Consumer frees space by publishing `tail` with a **Release** store.
* Producer loads `tail` with **Acquire** before reusing space.

This is mandatory for correctness on weakly-ordered arm64.

### 2.2 Concurrency

* **Producers** are serialized by `prod_lock`, a 3-state futex mutex
  (0 = free, 1 = locked, 2 = locked-contended). This makes every ring MPSC-safe.
  (A lock-free SPSC fast path is a planned optimization; it MUST remain wire
  compatible.)
* **Single consumer**, lock-free on the read side.

### 2.3 Wakeup (futex)

Linux `futex` on a plain 32-bit word in the header (shared, non-private):

* After publishing a record, a producer does
  `data_seq += 1; FUTEX_WAKE(data_seq, 1)`.
* A consumer that finds the ring empty does an adaptive spin, then
  `FUTEX_WAIT(data_seq, observed_value, timeout)`.
* After freeing space, a consumer does
  `space_seq += 1; FUTEX_WAKE(space_seq, MAX)`; a blocked producer waits on
  `space_seq` symmetrically.

---

## 3. Frame

Every record payload is a self-describing **frame** wrapping an Avro body:

| Offset | Size | Field       | Notes                                     |
|-------:|-----:|-------------|-------------------------------------------|
| 0      | 2    | `magic`     | `0x5249` ("IR")                           |
| 2      | 1    | `wire_ver`  | `1`                                       |
| 3      | 1    | `flags`     | reserved, `0`                             |
| 4      | 8    | `schema_fp` | CRC-64-AVRO Rabin fingerprint (LE)        |
| 12     | 4    | `body_len`  | Avro body length (LE)                     |
| 16     | N    | `body`      | Avro binary datum (no container header)   |

`FRAME_HEADER = 16` bytes.

---

## 4. Avro & fingerprints

All bodies are **Apache Avro binary** datums (single-object style, no file
container). Schema identity and interface compatibility use the **CRC-64-AVRO
Rabin fingerprint** of the schema's Parsing Canonical Form, transported as a
little-endian `u64`.

* A subscriber/caller presents the fingerprint it expects; the broker compares
  it to the publisher/function's registered fingerprint. Mismatch ⇒ hard error
  (`ERR_SCHEMA_MISMATCH`).
* Because fingerprints are `u64` but Avro `long` is signed `i64`, fingerprints
  inside control records are stored as `i64` via a lossless bit reinterpret.

See `SPEC/schemas/control.avsc` for the control-plane record schemas. They are
fixed; every implementation computes identical fingerprints offline, so control
schemas are never exchanged at runtime — the frame `schema_fp` identifies the
record type.

---

## 5. Control segment superblock

The control segment begins with a superblock, then the submission ring at
offset 64:

| Offset | Size | Field        | Notes                          |
|-------:|-----:|--------------|--------------------------------|
| 0      | 8    | `magic`      | `"IMPRING\0"` as LE u64        |
| 8      | 4    | `version`    | `1`                            |
| 12     | 4    | `broker_pid` |                                |
| 16     | 8    | `epoch`      | broker start time (ns), bumps each run |
| 64     | …    | submission ring (clients → broker, MPSC) |

See `bootstrap.md` for the connection handshake.
