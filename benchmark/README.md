# Cross-language ring relay benchmark

This benchmark measures Ring's end-to-end hop latency across **every supported
language at once**. Six bench nodes — one per language — form a ring and pass a
token around it:

```
0: Rust  →  1: C  →  2: C++  →  3: Go  →  4: Python  →  5: TS  →  (back to 0)
```

Each node subscribes to its predecessor's channel and publishes its own. All
channels are gated by the same **hard-coded key** (`ring-bench-key`), so the ring
also exercises the access-control path. One full circuit of the token is a
**lap**.

## The token

A small Avro record circulates (see [authoring-schemas](../spec/authoring-schemas.md)):

```json
{ "type":"record","name":"BenchToken","namespace":"ring.bench","fields":[
  { "name":"lap",         "type":"long" },
  { "name":"start_nanos", "type":"long" },
  { "name":"elapsed_ns",  "type":"long" },
  { "name":"stop",        "type":"boolean" } ] }
```

- **`lap`** — the lap counter, incremented by node 0 each full circuit.
- **`start_nanos` / `elapsed_ns`** — the coordinator's start time and the total
  elapsed time (filled on the final lap).
- **`stop`** — set on the last lap; every node forwards it and then exits.

## How it runs

Node 0 (Rust) is the **coordinator**:

1. It publishes its channel, then waits until **all** nodes' channels exist —
   i.e. every service has started ("первый проход ждёт запуск всех сервисов").
2. It starts the clock, injects the token, and counts laps.
3. On lap `LAPS` it stamps `elapsed_ns`, sets `stop`, sends the token around once
   more so every relay shuts down, and prints the result.

Relay nodes (C, C++, Go, Python, TS) just forward each token and exit when the
stop token passes through.

## Run it

```sh
benchmark/run.sh             # default: 1,000,000 laps
benchmark/run.sh 50000       # or pass a lap count
BENCH_LAPS=50000 benchmark/run.sh
```

The script builds the broker and a bench node in each language, wires the ring,
runs it, and tears everything down. It prints, e.g.:

```
ring-bench: 1000000 laps across 6 services in 147.9s | 6761 laps/s | 147911 ns/lap | 24651 ns/hop
```

- **ns/lap** — time for the token to visit all six languages once.
- **ns/hop** — average per-language hop (publish + cross-process futex wakeup +
  the next runtime receiving). This is dominated by wakeup latency and the
  slower runtimes (Python, TS); a same-language Rust↔Rust ring is much faster.

## CI

The `benchmark` pipeline in [`.depl/config.yaml`](../.depl/config.yaml) runs this
with a reduced lap count (`BENCH_LAPS=2000`) as a smoke check that the whole ring
still works across all languages. Use `benchmark/run.sh` for the real 1M-lap
measurement.
