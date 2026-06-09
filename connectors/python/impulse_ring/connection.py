"""The Ring connection API for Python.

Register an app, publish/subscribe to key-gated channels, expose functions, and
call remote functions (sync or via a future) — all over shared memory. Payloads
are Apache Avro; build/parse them with :mod:`impulse_ring.avro`.

Per project policy, schema fingerprints are computed by the broker: we send
schema JSON and use the fingerprints the broker returns.
"""

import secrets
import threading
from collections import namedtuple
from concurrent.futures import Future, TimeoutError as FTimeout

from . import avro, frame, proto, shm
from .ring import Ring, ring_bytes

_U64 = (1 << 64) - 1

ChannelInfo = namedtuple("ChannelInfo", "channel_id name owner_app schema_fp requires_key")


class RingError(Exception):
    """A Ring protocol or transport error."""

    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status


def _new_id() -> int:
    """A positive 63-bit id (clean as a signed Avro long)."""
    return secrets.randbits(63)


class Publisher:
    def __init__(self, ring: Ring, schema_fp: int):
        self._ring = ring
        self.schema_fp = schema_fp & _U64

    def publish(self, avro_body: bytes):
        if not self._ring.push_blocking(frame.encode(self.schema_fp, avro_body), 1000):
            raise RingError("channel full (slow subscriber)")


class Subscriber:
    def __init__(self, ring: Ring, schema_fp: int):
        self._ring = ring
        self.schema_fp = schema_fp & _U64

    def recv(self, timeout_ms: int = 1000):
        """Return the next message body, or None on timeout."""
        rec = self._ring.pop_blocking(timeout_ms)
        if rec is None:
            return None
        fp, body = frame.decode(rec)
        if fp != self.schema_fp:
            raise RingError(f"message schema mismatch: {fp:#x} != {self.schema_fp:#x}")
        return body


class Connection:
    def __init__(self, app_name: str):
        self._ctl = shm.open_segment(proto.CONTROL_NAME)
        if bytes(self._ctl[0:8]) != proto.CTL_MAGIC:
            raise RingError("control magic mismatch (is impulsed running?)")
        self._sub = Ring.attach(self._ctl, proto.SUBMISSION_BASE)

        self._nonce = _new_id()
        self._reply_name = f"/impulse-ring.cli.{self._nonce}.v1"
        self._reply_mm = shm.create(self._reply_name, ring_bytes(proto.REPLY_CAP))
        self._reply_ring = Ring.format(self._reply_mm, 0, proto.REPLY_CAP)

        self._pending = {}
        self._lock = threading.Lock()
        self._running = True
        self._client_id = 0
        self._services = []

        self._disp = threading.Thread(target=self._dispatch, daemon=True)
        self._disp.start()
        self._register(app_name)

    # ---- lifecycle ----
    def _register(self, app_name):
        corr = _new_id()
        e = avro.Encoder()
        e.put_long(corr)
        e.put_string(app_name)
        e.put_long(self._nonce)
        e.put_string(self._reply_name)
        e.put_long(1000)
        _, body = self._control_call(proto.FP_REGISTER, e.getvalue(), corr, 5.0)
        d = avro.Decoder(body)
        d.get_long()
        client_id = d.get_long()
        status = d.get_int()
        msg = d.get_string()
        if status != proto.ST_OK:
            raise RingError(f"register rejected: {msg}", status)
        self._client_id = client_id

    def close(self):
        if not self._running:
            return
        try:
            corr = _new_id()
            e = avro.Encoder()
            e.put_long(corr)
            e.put_long(self._client_id)
            self._sub.push_blocking(frame.encode(proto.FP_UNREGISTER, e.getvalue()), 200)
        except Exception:
            pass
        self._running = False
        self._disp.join(timeout=2.0)
        for t in self._services:
            t.join(timeout=2.0)
        shm.unlink(self._reply_name)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ---- dispatcher + control round-trips ----
    def _dispatch(self):
        while self._running:
            rec = self._reply_ring.pop_blocking(100)
            if rec is None:
                continue
            try:
                fp, body = frame.decode(rec)
            except ValueError:
                continue
            corr = avro.peek_long(body)
            with self._lock:
                fut = self._pending.pop(corr, None)
            if fut is not None and not fut.done():
                fut.set_result((fp, body))

    def _control_call(self, fp, body, corr, timeout):
        fut = Future()
        with self._lock:
            self._pending[corr] = fut
        if not self._sub.push_blocking(frame.encode(fp, body), 2000):
            with self._lock:
                self._pending.pop(corr, None)
            raise RingError("submission ring full (broker stuck?)")
        try:
            return fut.result(timeout)
        except FTimeout:
            with self._lock:
                self._pending.pop(corr, None)
            raise RingError("timed out waiting for broker reply")

    # ---- channels ----
    def publish_channel(self, name, schema_json, key=None) -> Publisher:
        corr = _new_id()
        e = avro.Encoder()
        e.put_long(corr)
        e.put_long(self._client_id)
        e.put_string(name)
        e.put_string(schema_json)
        e.put_string(key or "")
        _, body = self._control_call(proto.FP_PUBLISH, e.getvalue(), corr, 5.0)
        d = avro.Decoder(body)
        d.get_long()
        d.get_long()  # channel_id
        schema_fp = d.get_long()
        arena = d.get_string()
        status = d.get_int()
        msg = d.get_string()
        if status != proto.ST_OK:
            raise RingError(f"publish failed: {msg}", status)
        mm = shm.open_segment(arena)
        return Publisher(Ring.attach(mm, 0), schema_fp)

    def list_channels(self):
        corr = _new_id()
        e = avro.Encoder()
        e.put_long(corr)
        e.put_long(self._client_id)
        _, body = self._control_call(proto.FP_LIST, e.getvalue(), corr, 5.0)
        d = avro.Decoder(body)
        d.get_long()
        out = []
        count = d.get_array_count()
        while count != 0:
            if count < 0:
                count = -count
                d.get_long()  # block byte-size (unused)
            for _ in range(count):
                out.append(
                    ChannelInfo(
                        channel_id=d.get_long(),
                        name=d.get_string(),
                        owner_app=d.get_string(),
                        schema_fp=d.get_long() & _U64,
                        requires_key=d.get_boolean(),
                    )
                )
            count = d.get_array_count()
        return out

    def subscribe(self, channel_id, key=None) -> Subscriber:
        corr = _new_id()
        e = avro.Encoder()
        e.put_long(corr)
        e.put_long(self._client_id)
        e.put_long(channel_id)
        e.put_string(key or "")
        e.put_long(0)  # expected_fp = 0: broker owns fingerprints
        _, body = self._control_call(proto.FP_SUBSCRIBE, e.getvalue(), corr, 5.0)
        d = avro.Decoder(body)
        d.get_long()
        arena = d.get_string()
        schema_fp = d.get_long()
        status = d.get_int()
        msg = d.get_string()
        if status != proto.ST_OK:
            raise RingError(f"subscribe failed: {msg}", status)
        mm = shm.open_segment(arena)
        return Subscriber(Ring.attach(mm, 0), schema_fp)

    # ---- functions / RPC ----
    def expose_function(self, name, req_schema_json, resp_schema_json, handler, key=None):
        """`handler(req_body: bytes) -> resp_body: bytes` runs on a thread."""
        corr = _new_id()
        e = avro.Encoder()
        e.put_long(corr)
        e.put_long(self._client_id)
        e.put_string(name)
        e.put_string(req_schema_json)
        e.put_string(resp_schema_json)
        e.put_string(key or "")
        _, body = self._control_call(proto.FP_EXPOSE, e.getvalue(), corr, 5.0)
        d = avro.Decoder(body)
        d.get_long()
        d.get_long()  # fn_id
        req_fp = d.get_long()  # kept signed: re-encoded as an Avro long below
        resp_fp = d.get_long()
        arena = d.get_string()
        status = d.get_int()
        msg = d.get_string()
        if status != proto.ST_OK:
            raise RingError(f"expose failed: {msg}", status)
        mm = shm.open_segment(arena)
        req_ring = Ring.attach(mm, 0)
        t = threading.Thread(
            target=self._serve, args=(req_ring, req_fp, resp_fp, handler), daemon=True
        )
        t.start()
        self._services.append(t)

    def _serve(self, req_ring, req_fp, resp_fp, handler):
        reply_cache = {}
        while self._running:
            rec = req_ring.pop_blocking(100)
            if rec is None:
                continue
            try:
                _, body = frame.decode(rec)
            except ValueError:
                continue
            d = avro.Decoder(body)
            corr = d.get_long()
            d.get_long()  # caller_id
            reply_seg = d.get_string()
            arg_fp = d.get_long()  # signed Avro long
            args = d.get_bytes()

            status, result = proto.ST_OK, b""
            if arg_fp != req_fp:
                status = proto.ST_MISMATCH
            else:
                try:
                    result = handler(args)
                except Exception:
                    status = proto.ST_INTERNAL

            e = avro.Encoder()
            e.put_long(corr)
            e.put_int(status)
            e.put_string("")
            e.put_long(resp_fp if status == proto.ST_OK else 0)
            e.put_bytes(result if status == proto.ST_OK else b"")
            rr = reply_cache.get(reply_seg)
            if rr is None:
                try:
                    rr = Ring.attach(shm.open_segment(reply_seg), 0)
                    reply_cache[reply_seg] = rr
                except OSError:
                    continue
            rr.push_blocking(frame.encode(proto.FP_RPC_RESPONSE, e.getvalue()), 2000)

    def call_async(self, fn_name, req_body, key=None) -> Future:
        """Send an RPC and return a Future resolving to the response body."""
        # Lookup the function (broker returns fingerprints + arena).
        corr = _new_id()
        e = avro.Encoder()
        e.put_long(corr)
        e.put_long(self._client_id)
        e.put_string(fn_name)
        e.put_string(key or "")
        _, body = self._control_call(proto.FP_LOOKUP, e.getvalue(), corr, 5.0)
        d = avro.Decoder(body)
        d.get_long()
        d.get_long()  # fn_id
        req_fp = d.get_long()  # signed: forwarded as the RpcRequest arg_fp
        resp_fp = d.get_long()
        arena = d.get_string()
        status = d.get_int()
        msg = d.get_string()
        if status != proto.ST_OK:
            raise RingError(f"lookup failed: {msg}", status)

        # Place the request on the function's arena.
        fn_ring = Ring.attach(shm.open_segment(arena), 0)
        rpc_corr = _new_id()
        inner = Future()
        with self._lock:
            self._pending[rpc_corr] = inner
        e = avro.Encoder()
        e.put_long(rpc_corr)
        e.put_long(self._client_id)
        e.put_string(self._reply_name)
        e.put_long(req_fp)  # arg_fp = broker-derived request fp
        e.put_bytes(req_body)
        if not fn_ring.push_blocking(frame.encode(proto.FP_RPC_REQUEST, e.getvalue()), 2000):
            with self._lock:
                self._pending.pop(rpc_corr, None)
            raise RingError("function request ring full")

        out = Future()

        def _done(f):
            try:
                _fp, rbody = f.result()
                dd = avro.Decoder(rbody)
                dd.get_long()
                rstatus = dd.get_int()
                rmsg = dd.get_string()
                result_fp = dd.get_long()  # signed Avro long
                result = dd.get_bytes()
                if rstatus != proto.ST_OK:
                    out.set_exception(RingError(f"remote error: {rmsg}", rstatus))
                elif result_fp != resp_fp:
                    out.set_exception(RingError("response schema mismatch"))
                else:
                    out.set_result(result)
            except Exception as ex:  # noqa: BLE001
                out.set_exception(ex)

        inner.add_done_callback(_done)
        return out

    def call(self, fn_name, req_body, key=None, timeout=5.0) -> bytes:
        """Blocking RPC call returning the response body."""
        fut = self.call_async(fn_name, req_body, key)
        try:
            return fut.result(timeout)
        except FTimeout:
            raise RingError("rpc call timed out")
