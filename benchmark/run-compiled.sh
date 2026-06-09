#!/usr/bin/env bash
# Cross-language ring relay benchmark. Builds the broker and a bench node in each
# supported language, wires them into a ring (0:rust -> 1:c -> 2:cpp -> 3:go ->
# 4:python -> 5:ts -> 0), circulates a token, and reports how long N laps take.
#
# Usage: benchmark/run.sh [laps]   (or BENCH_LAPS=... benchmark/run.sh)
# Default is 1,000,000 laps; pass a smaller number for a quick run.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/.." && pwd)"
cd "$root"

LAPS="${BENCH_LAPS:-${1:-1000000}}"
N=4
echo "==> building (laps=$LAPS, services=$N)"

cargo build --release -p impulsed -p ring-bench >/dev/null
cmake -S connectors/c -B connectors/c/build -DCMAKE_BUILD_TYPE=Release >/dev/null
cmake --build connectors/c/build --target impulse_ring_shared >/dev/null
# Compile the C core once (as C) and link it into both the C and C++ nodes.
gcc -O2 -Iconnectors/c/include -D_GNU_SOURCE -c connectors/c/src/impulse_ring.c -o /tmp/ring_core.o
gcc -O2 -Iconnectors/c/include -D_GNU_SOURCE benchmark/c/bench.c /tmp/ring_core.o \
  -lpthread -o /tmp/ring_bench_c
g++ -O2 -std=c++17 -Iconnectors/c/include -Iconnectors/cpp/include \
  benchmark/cpp/bench.cpp /tmp/ring_core.o -lpthread -o /tmp/ring_bench_cpp
( cd benchmark/go && go build -o /tmp/ring_bench_go . )
( cd connectors/python && python3 setup.py build_ext --inplace >/dev/null 2>&1 )

pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  [[ -n "${BROKER:-}" ]] && kill -TERM "$BROKER" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

rm -f /dev/shm/impulse-ring.* 2>/dev/null || true
echo "==> starting broker"
./target/release/impulsed >/tmp/ring_bench_broker.log 2>&1 & BROKER=$!
sleep 0.5

echo "==> launching relays (c, cpp, go)"
/tmp/ring_bench_c   1 $N "$LAPS" &                              pids+=($!)
/tmp/ring_bench_cpp 2 $N "$LAPS" &                              pids+=($!)
/tmp/ring_bench_go  3 $N "$LAPS" &                              pids+=($!)

echo "==> running coordinator (rust, index 0)"
./target/release/ring-bench 0 $N "$LAPS"

# Let the stop token finish propagating, then tidy up.
sleep 0.5
echo "==> done"
