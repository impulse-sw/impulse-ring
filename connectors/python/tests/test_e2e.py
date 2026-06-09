"""End-to-end test for the Python connector against a live `impulsed` broker.

Runs a Python self-test (register -> publish/subscribe -> RPC, plus negative ACL
cases) and a cross-language test (subscribe to a channel published by the Rust
`peer` and call a function it exposes), proving Python<->Rust Avro interop.

Run via ../run_tests.sh, which builds the broker, peer, and the native extension.
"""

import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "connectors", "python"))

from impulse_ring import Connection, RingError, avro  # noqa: E402

METRIC = (
    '{"type":"record","name":"Metric","namespace":"ring.examples",'
    '"fields":[{"name":"name","type":"string"},{"name":"value","type":"double"}]}'
)
ADD_REQ = (
    '{"type":"record","name":"AddReq","namespace":"ring.examples",'
    '"fields":[{"name":"a","type":"long"},{"name":"b","type":"long"}]}'
)
ADD_RESP = (
    '{"type":"record","name":"AddResp","namespace":"ring.examples",'
    '"fields":[{"name":"sum","type":"long"}]}'
)

failures = 0


def check(cond, msg):
    global failures
    if cond:
        print("ok:", msg)
    else:
        print("FAIL:", msg)
        failures += 1


def metric_body(name, value):
    e = avro.Encoder()
    e.put_string(name)
    e.put_double(value)
    return e.getvalue()


def add_handler(req_body):
    d = avro.Decoder(req_body)
    a = d.get_long()
    b = d.get_long()
    e = avro.Encoder()
    e.put_long(a + b)
    return e.getvalue()


def wait_for_control(timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists("/dev/shm/impulse-ring.ctl.v1"):
            time.sleep(0.1)
            return
        time.sleep(0.02)
    raise RuntimeError("broker did not come up")


def python_self_test():
    svc = Connection("py-svc")
    pub = svc.publish_channel("py-metrics", METRIC, key="chan-key")
    pub.publish(metric_body("cpu", 0.75))
    svc.expose_function("py-add", ADD_REQ, ADD_RESP, add_handler, key="fn-key")

    cli = Connection("py-cli")
    chans = cli.list_channels()
    info = next((c for c in chans if c.name == "py-metrics"), None)
    check(info is not None, "channel listed")
    check(info and info.requires_key, "channel requires key")

    # wrong key denied
    try:
        cli.subscribe(info.channel_id, key="wrong")
        check(False, "subscribe wrong key denied")
    except RingError:
        check(True, "subscribe wrong key denied")

    sub = cli.subscribe(info.channel_id, key="chan-key")
    body = sub.recv(2000)
    check(body is not None, "received message")
    if body is not None:
        d = avro.Decoder(body)
        check(d.get_string() == "cpu", "metric name == cpu")
        check(abs(d.get_double() - 0.75) < 1e-9, "metric value == 0.75")

    e = avro.Encoder()
    e.put_long(7)
    e.put_long(35)
    resp = cli.call("py-add", e.getvalue(), key="fn-key", timeout=5.0)
    check(avro.Decoder(resp).get_long() == 42, "add(7,35) == 42")

    # wrong function key denied
    try:
        cli.call("py-add", e.getvalue(), key="nope", timeout=2.0)
        check(False, "rpc wrong key denied")
    except RingError:
        check(True, "rpc wrong key denied")

    cli.close()
    svc.close()


def cross_language_test():
    """Subscribe to a Rust-published channel and call a Rust-exposed function."""
    peer = subprocess.Popen([os.path.join(ROOT, "target", "debug", "examples", "peer")])
    try:
        time.sleep(0.6)
        c = Connection("py-xlang")
        cid = None
        for _ in range(50):
            for ch in c.list_channels():
                if ch.name == "rmetrics":
                    cid = ch.channel_id
            if cid is not None:
                break
            time.sleep(0.1)
        check(cid is not None, "found rust channel 'rmetrics'")

        if cid is not None:
            sub = c.subscribe(cid)
            body = sub.recv(3000)
            check(body is not None, "received rust-published message")
            if body is not None:
                d = avro.Decoder(body)
                check(d.get_string() == "temp", "rust metric name == temp")
                check(abs(d.get_double() - 21.5) < 1e-9, "rust metric value == 21.5")

        e = avro.Encoder()
        e.put_long(6)
        e.put_long(7)
        resp = c.call("rmul", e.getvalue(), timeout=5.0)
        check(avro.Decoder(resp).get_long() == 42, "rmul(6,7) == 42")
        c.close()
    finally:
        peer.terminate()
        peer.wait()


def main():
    broker = subprocess.Popen([os.path.join(ROOT, "target", "debug", "impulsed")])
    try:
        wait_for_control()
        print("== python self-test ==")
        python_self_test()
        print("== cross-language (python <-> rust) ==")
        cross_language_test()
    finally:
        broker.terminate()
        broker.wait()

    print("\n%s (%d failures)" % ("FAILED" if failures else "PASSED", failures))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
