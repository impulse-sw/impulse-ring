"""Demo of the Python connector. Start the broker first:

cargo run -p impulsed
python3 connectors/python/examples/demo.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from impulse_ring import Connection, avro  # noqa: E402

METRIC = (
    '{"type":"record","name":"Metric","namespace":"ring.examples",'
    '"fields":[{"name":"name","type":"string"},{"name":"value","type":"double"}]}'
)
ADD_REQ = (
    '{"type":"record","name":"AddReq","namespace":"ring.examples",'
    '"fields":[{"name":"a","type":"long"},{"name":"b","type":"long"}]}'
)
ADD_RESP = '{"type":"record","name":"AddResp","namespace":"ring.examples","fields":[{"name":"sum","type":"long"}]}'


def add(req_body):
    d = avro.Decoder(req_body)
    e = avro.Encoder()
    e.put_long(d.get_long() + d.get_long())
    return e.getvalue()


def main():
    svc = Connection("py-demo-svc")
    pub = svc.publish_channel("py-demo-metrics", METRIC)
    e = avro.Encoder()
    e.put_string("cpu")
    e.put_double(0.5)
    pub.publish(e.getvalue())
    svc.expose_function("py-demo-add", ADD_REQ, ADD_RESP, add)

    cli = Connection("py-demo-cli")
    for c in cli.list_channels():
        print(f"channel: {c.name} (requires_key={c.requires_key})")
    a = avro.Encoder()
    a.put_long(20)
    a.put_long(22)
    resp = cli.call("py-demo-add", a.getvalue(), timeout=5.0)
    print("add(20, 22) =", avro.Decoder(resp).get_long())

    cli.close()
    svc.close()


if __name__ == "__main__":
    main()
