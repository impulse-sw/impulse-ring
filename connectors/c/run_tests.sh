#!/usr/bin/env bash
# Build and run the C connector tests against the real Rust broker, including a
# cross-language (C <-> Rust) data-plane test. Run from the repo root or here.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"

echo "==> building broker + rust peer"
( cd "$root" && cargo build -p impulsed --example peer -p impulse-connector )

echo "==> building C connector"
cmake -S "$here" -B "$here/build" -DCMAKE_BUILD_TYPE=Release >/dev/null
cmake --build "$here/build" >/dev/null

cleanup() {
  [[ -n "${PEER:-}" ]] && kill "$PEER" 2>/dev/null || true
  [[ -n "${BROKER:-}" ]] && kill -TERM "$BROKER" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

rm -f /dev/shm/impulse-ring.* 2>/dev/null || true

echo "==> starting broker"
"$root/target/debug/impulsed" & BROKER=$!
sleep 0.5

echo "==> C self-test (C <-> broker <-> C)"
"$here/build/ir_test_e2e"

echo "==> starting rust peer"
"$root/target/debug/examples/peer" & PEER=$!
sleep 0.5

echo "==> cross-language test (C <-> Rust)"
"$here/build/ir_xlang"

echo "==> stopping peer + broker"
kill "$PEER" 2>/dev/null || true; PEER=
kill -TERM "$BROKER" 2>/dev/null || true; wait "$BROKER" 2>/dev/null || true; BROKER=

leftovers="$(ls /dev/shm 2>/dev/null | grep impulse-ring || true)"
if [[ -n "$leftovers" ]]; then
  echo "FAIL: leaked shared memory: $leftovers"; exit 1
fi
echo "==> ALL C CONNECTOR TESTS PASSED"
