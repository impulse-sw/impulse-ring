"""Ring relay benchmark — Python node.

Usage: python3 bench.py <index> <num_services> <laps>
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "connectors", "python"))

from impulse_ring import Connection, avro  # noqa: E402

KEY = "ring-bench-key"
SCHEMA = (
    '{"type":"record","name":"BenchToken","namespace":"ring.bench","fields":['
    '{"name":"lap","type":"long"},{"name":"start_nanos","type":"long"},'
    '{"name":"elapsed_ns","type":"long"},{"name":"stop","type":"boolean"}]}'
)


def encode(lap, start, elapsed, stop):
    e = avro.Encoder()
    e.put_long(lap)
    e.put_long(start)
    e.put_long(elapsed)
    e.put_boolean(stop)
    return e.getvalue()


def find_channel(conn, name):
    for c in conn.list_channels():
        if c.name == name:
            return c.channel_id
    return None


def present(conn, n):
    names = {c.name for c in conn.list_channels()}
    return sum(1 for i in range(n) if f"bench-{i}" in names)


def main():
    if len(sys.argv) < 4:
        print("usage: bench.py <index> <num_services> <laps>", file=sys.stderr)
        return 2
    index, n, laps = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
    self_name = f"bench-{index}"

    conn = Connection(self_name)
    pub = conn.publish_channel(self_name, SCHEMA, key=KEY)

    prev = f"bench-{(index + n - 1) % n}"
    cid = None
    while cid is None:
        cid = find_channel(conn, prev)
        if cid is None:
            time.sleep(0.02)
    sub = conn.subscribe(cid, key=KEY)

    if index == 0:
        while present(conn, n) < n:
            time.sleep(0.02)
        time.sleep(0.3)

        t0 = time.monotonic_ns()
        pub.publish(encode(0, t0, 0, False))
        while True:
            body = sub.recv(30000)
            if body is None:
                break
            lap = avro.Decoder(body).get_long() + 1
            if lap >= laps:
                elapsed = time.monotonic_ns() - t0
                pub.publish(encode(lap, t0, elapsed, True))
                secs = elapsed / 1e9
                print(
                    f"ring-bench(python): {laps} laps across {n} services in {secs:.3f}s | "
                    f"{laps / secs:.0f} laps/s | {elapsed // laps} ns/lap | {elapsed // (laps * n)} ns/hop"
                )
                time.sleep(0.3)
                break
            pub.publish(encode(lap, t0, 0, False))
    else:
        while True:
            body = sub.recv(30000)
            if body is None:
                break
            d = avro.Decoder(body)
            d.get_long()
            d.get_long()
            d.get_long()
            stop = d.get_boolean()
            pub.publish(body)  # forward the same bytes
            if stop:
                break

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
