"""Broker-restart recovery test for the Python connector.

Manages its own `impulsed`: connects a service (exposing `add`) and a client,
SIGKILLs the broker and starts a fresh one (a new shared-memory generation with
a new epoch), and asserts the existing connections transparently reconnect — the
function is re-exposed and the client's RPC keeps working without rebuilding any
handle.
"""

import os
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "connectors", "python"))

from impulse_ring import Connection, avro  # noqa: E402

ADD_REQ = (
    '{"type":"record","name":"AddReq","namespace":"ring.test",'
    '"fields":[{"name":"a","type":"long"},{"name":"b","type":"long"}]}'
)
ADD_RESP = '{"type":"record","name":"AddResp","namespace":"ring.test","fields":[{"name":"sum","type":"long"}]}'

failures = 0


def check(cond, msg):
    global failures
    if cond:
        print("ok:", msg)
    else:
        print("FAIL:", msg)
        failures += 1


def add_handler(req_body):
    d = avro.Decoder(req_body)
    a, b = d.get_long(), d.get_long()
    e = avro.Encoder()
    e.put_long(a + b)
    return e.getvalue()


def start_broker():
    p = subprocess.Popen([os.path.join(ROOT, "target", "debug", "impulsed")])
    deadline = time.time() + 5.0
    while time.time() < deadline:
        if os.path.exists("/dev/shm/impulse-ring.ctl.v1"):
            time.sleep(0.1)
            return p
        time.sleep(0.02)
    raise RuntimeError("broker did not come up")


def kill_broker(p):
    p.send_signal(signal.SIGKILL)
    p.wait()


def call_add(c, a, b):
    e = avro.Encoder()
    e.put_long(a)
    e.put_long(b)
    return avro.Decoder(c.call("add", e.getvalue(), key="fn-key", timeout=3.0)).get_long()


def main():
    broker = start_broker()
    svc = Connection("py-svc-restart")
    svc.expose_function("add", ADD_REQ, ADD_RESP, add_handler, key="fn-key")
    cli = Connection("py-cli-restart")
    epoch_before = cli.broker_epoch

    check(call_add(cli, 7, 35) == 42, "rpc before restart == 42")

    # Restart the broker.
    kill_broker(broker)
    broker2 = start_broker()
    try:
        ok = False
        for _ in range(100):
            try:
                if call_add(cli, 20, 22) == 42:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(0.1)
        check(ok, "rpc recovered after restart == 42")
        check(cli.broker_epoch != epoch_before, "epoch advanced after restart")
        cli.close()
        svc.close()
    finally:
        kill_broker(broker2)

    print(f"\n{'FAILED' if failures else 'PASSED'} ({failures} failures)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
