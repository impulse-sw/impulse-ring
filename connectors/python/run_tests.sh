#!/usr/bin/env bash
# Build the broker, the Rust peer, and the native extension, then run the
# Python connector end-to-end test (self-test + cross-language).
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"

echo "==> building broker + rust peer"
( cd "$root" && cargo build -p impulsed --example peer -p impulse-ring-connector )

echo "==> building native extension (_ring_native)"
( cd "$here" && python3 setup.py build_ext --inplace >/dev/null )

rm -f /dev/shm/impulse-ring.* 2>/dev/null || true

echo "==> running python e2e"
python3 "$here/tests/test_e2e.py"

leftovers="$(ls /dev/shm 2>/dev/null | grep impulse-ring || true)"
if [[ -n "$leftovers" ]]; then
  echo "FAIL: leaked shared memory: $leftovers"; exit 1
fi
echo "==> PYTHON CONNECTOR TESTS PASSED"
