"""The Ring connection API for Python.

Register an app, publish/subscribe to key-gated channels, expose functions, and
call remote functions (sync or via a future) — all over shared memory. Payloads
are Apache Avro; build/parse them with :mod:`impulse_ring.avro`.

Per project policy, schema fingerprints are computed by the broker: we send
schema JSON and use the fingerprints the broker returns.

If the ``impulsed`` broker restarts (recreating its shared memory with a fresh
epoch), a connection detects it — proactively via a background watcher, or
lazily when a call stops being answered — and transparently reconnects:
re-registers under the same name and replays the channels it published and the
functions it exposed, so live :class:`Publisher` and RPC services keep working.
Subscribers are not auto-replayed (re-subscribe by resolving the channel by name
again).
"""

import os
import secrets
import struct
import threading
import time
from collections import namedtuple
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FTimeout

from . import avro, frame, proto, shm
from .ring import Ring, ring_bytes

_U64 = (1 << 64) - 1

ChannelInfo = namedtuple("ChannelInfo", "channel_id name owner_app schema_fp requires_key")

# How often the watcher polls the control epoch for a restart (seconds).
_WATCH_INTERVAL = 0.25


class RingError(Exception):
    """A Ring protocol or transport error."""

    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status


def _new_id() -> int:
    """A positive 63-bit id (clean as a signed Avro long)."""
    return secrets.randbits(63)


def _broker_unreachable(err: "RingError") -> bool:
    """Whether an error means the broker stopped answering."""
    s = str(err)
    return "timed out" in s or "submission ring full" in s


def _live_broker_epoch() -> int:
    """Open the control segment freshly and read the live broker epoch (the
    cached mapping still points at the unlinked pre-restart segment). Raises
    ``OSError``/``RingError`` if the broker is currently unreachable."""
    mm = shm.open_segment(proto.CONTROL_NAME)
    try:
        if bytes(mm[0:8]) != proto.CTL_MAGIC:
            raise RingError("control magic mismatch")
        return struct.unpack_from("<q", mm, proto.CTL_OFF_EPOCH)[0]
    finally:
        mm.close()


class _ChanReg:
    """A published channel, tracked so it can be replayed after a restart."""

    def __init__(self, name, schema_json, key, ring, schema_fp):
        self.lock = threading.Lock()
        self.name = name
        self.schema_json = schema_json
        self.key = key
        self.ring = ring
        self.schema_fp = schema_fp & _U64


class _SvcReg:
    """An exposed function, tracked so it can be replayed after a restart."""

    def __init__(self, name, req_schema, resp_schema, key, arena_cap, handler, req_ring, req_fp, resp_fp):
        self.lock = threading.Lock()
        self.name = name
        self.req_schema = req_schema
        self.resp_schema = resp_schema
        self.key = key
        self.arena_cap = arena_cap
        self.handler = handler
        self.req_ring = req_ring
        self.req_fp = req_fp
        self.resp_fp = resp_fp


class Publisher:
    def __init__(self, conn, reg: _ChanReg):
        self._conn = conn
        self._reg = reg

    @property
    def schema_fp(self):
        with self._reg.lock:
            return self._reg.schema_fp

    def publish(self, avro_body: bytes):
        with self._reg.lock:
            ring = self._reg.ring
            fp = self._reg.schema_fp
        if not ring.push_blocking(frame.encode(fp, avro_body), 1000):
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
        self._app_name = app_name
        # Transport (current broker generation), guarded by _tx_lock.
        self._tx_lock = threading.Lock()
        self._ctl = None
        self._sub = None
        self._reply_mm = None
        self._reply_ring = None
        self._reply_name = None
        self._nonce = 0
        self._epoch = 0
        self._client_id = 0
        self._retired = []  # mmaps kept mapped until close (see _reconnect)

        self._pending = {}
        self._lock = threading.Lock()
        self._running = True
        self._auto_reconnect = True
        self._reconnect_lock = threading.Lock()

        self._reg_lock = threading.Lock()
        self._channels = []  # _ChanReg
        self._services = []  # _SvcReg
        self._svc_threads = []

        self._bootstrap()
        self._disp = threading.Thread(target=self._dispatch, daemon=True)
        self._disp.start()
        self._watcher = threading.Thread(target=self._watch, daemon=True)
        self._watcher.start()
        self._register()

    # ---- transport / bootstrap ----
    def _bootstrap(self):
        ctl = shm.open_segment(proto.CONTROL_NAME)
        if bytes(ctl[0:8]) != proto.CTL_MAGIC:
            ctl.close()
            raise RingError("control magic mismatch (is impulsed running?)")
        sub = Ring.attach(ctl, proto.SUBMISSION_BASE)
        epoch = struct.unpack_from("<q", ctl, proto.CTL_OFF_EPOCH)[0]
        nonce = _new_id()
        reply_name = f"/impulse-ring.cli.{nonce}.v1"
        reply_mm = shm.create(reply_name, ring_bytes(proto.REPLY_CAP))
        reply_ring = Ring.format(reply_mm, 0, proto.REPLY_CAP)
        with self._tx_lock:
            if self._ctl is not None:
                self._retired.append(self._ctl)
            if self._reply_mm is not None:
                self._retired.append(self._reply_mm)
            self._ctl = ctl
            self._sub = sub
            self._reply_mm = reply_mm
            self._reply_ring = reply_ring
            self._reply_name = reply_name
            self._nonce = nonce
            self._epoch = epoch
            self._client_id = 0

    def _client_id_tx(self):
        with self._tx_lock:
            return self._client_id

    def _reply_name_tx(self):
        with self._tx_lock:
            return self._reply_name

    @property
    def broker_epoch(self):
        """The broker epoch this connection is attached under (changes on a restart)."""
        with self._tx_lock:
            return self._epoch

    def broker_restarted(self):
        """Whether impulsed restarted (or is unreachable) since this connection attached."""
        try:
            return _live_broker_epoch() != self.broker_epoch
        except OSError:
            return True

    def set_auto_reconnect(self, on: bool):
        """Enable/disable transparent reconnect on a detected restart (default: on)."""
        self._auto_reconnect = bool(on)

    # ---- lifecycle ----
    def _register(self):
        corr = _new_id()
        with self._tx_lock:
            nonce = self._nonce
            reply_name = self._reply_name
        e = avro.Encoder()
        e.put_long(corr)
        e.put_string(self._app_name)
        e.put_long(nonce)
        e.put_string(reply_name)
        e.put_long(1000)
        # pid: lets the broker reclaim our names if we die without unregistering.
        e.put_long(os.getpid())
        _, body = self._control_call(proto.FP_REGISTER, e.getvalue(), corr, 5.0)
        d = avro.Decoder(body)
        d.get_long()
        client_id = d.get_long()
        status = d.get_int()
        msg = d.get_string()
        if status != proto.ST_OK:
            raise RingError(f"register rejected: {msg}", status)
        with self._tx_lock:
            self._client_id = client_id

    def close(self):
        if not self._running:
            return
        try:
            corr = _new_id()
            e = avro.Encoder()
            e.put_long(corr)
            e.put_long(self._client_id_tx())
            self._submit(frame.encode(proto.FP_UNREGISTER, e.getvalue()), 200)
        except Exception:
            pass
        self._running = False
        self._disp.join(timeout=2.0)
        self._watcher.join(timeout=2.0)
        for t in self._svc_threads:
            t.join(timeout=2.0)
        # Safe now that all background threads have stopped reading any ring.
        for mm in self._retired:
            try:
                mm.close()
            except Exception:
                pass
        if self._reply_name:
            shm.unlink(self._reply_name)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ---- dispatcher + watcher + control round-trips ----
    def _dispatch(self):
        while self._running:
            with self._tx_lock:
                reply_ring = self._reply_ring
            rec = reply_ring.pop_blocking(100)
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

    def _watch(self):
        while self._running:
            slept = 0.0
            while slept < _WATCH_INTERVAL and self._running:
                time.sleep(0.01)
                slept += 0.01
            if not self._running or not self._auto_reconnect:
                continue
            with self._tx_lock:
                observed = self._epoch
            try:
                live = _live_broker_epoch()
            except OSError:
                continue
            if live != observed:
                try:
                    self._reconnect(observed)
                except Exception:
                    pass

    def _submit(self, framed, timeout_ms):
        with self._tx_lock:
            sub = self._sub
        return sub.push_blocking(framed, timeout_ms)

    def _control_call(self, fp, body, corr, timeout):
        fut = Future()
        with self._lock:
            self._pending[corr] = fut
        if not self._submit(frame.encode(fp, body), 2000):
            with self._lock:
                self._pending.pop(corr, None)
            raise RingError("submission ring full (broker stuck?)")
        try:
            return fut.result(timeout)
        except FTimeout:
            with self._lock:
                self._pending.pop(corr, None)
            raise RingError("timed out waiting for broker reply") from None

    # ---- reconnect / replay ----
    def _reconnect(self, observed):
        with self._reconnect_lock:
            with self._tx_lock:
                if self._epoch != observed:
                    return  # someone else reconnected
            self._bootstrap()
            # Detach pending futures bound to the dead broker; their waiters time out.
            with self._lock:
                self._pending = {}
            self._register()
            self._replay()

    def _replay(self):
        with self._reg_lock:
            chans = list(self._channels)
            svcs = list(self._services)
        for cr in chans:
            try:
                ring, fp = self._do_publish(cr.name, cr.schema_json, cr.key)
            except RingError:
                continue
            with cr.lock:
                cr.ring = ring
                cr.schema_fp = fp & _U64
        for sv in svcs:
            try:
                ring, req_fp, resp_fp = self._do_expose(sv.name, sv.req_schema, sv.resp_schema, sv.key, sv.arena_cap)
            except RingError:
                continue
            with sv.lock:
                sv.req_ring = ring
                sv.req_fp = req_fp
                sv.resp_fp = resp_fp

    def _with_reconnect(self, op):
        try:
            return op()
        except RingError as err:
            if not self._auto_reconnect or not _broker_unreachable(err):
                raise
            with self._tx_lock:
                observed = self._epoch
            try:
                live = _live_broker_epoch()
            except OSError:
                raise err from None
            if live == observed:
                raise
            self._reconnect(observed)
            return op()

    # ---- channels ----
    def _do_publish(self, name, schema_json, key):
        corr = _new_id()
        e = avro.Encoder()
        e.put_long(corr)
        e.put_long(self._client_id_tx())
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
        return Ring.attach(mm, 0), schema_fp

    def publish_channel(self, name, schema_json, key=None) -> Publisher:
        ring, schema_fp = self._with_reconnect(lambda: self._do_publish(name, schema_json, key))
        reg = _ChanReg(name, schema_json, key, ring, schema_fp)
        with self._reg_lock:
            self._channels.append(reg)
        return Publisher(self, reg)

    def list_channels(self):
        def op():
            corr = _new_id()
            e = avro.Encoder()
            e.put_long(corr)
            e.put_long(self._client_id_tx())
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

        return self._with_reconnect(op)

    def subscribe(self, channel_id, key=None) -> Subscriber:
        def op():
            corr = _new_id()
            e = avro.Encoder()
            e.put_long(corr)
            e.put_long(self._client_id_tx())
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

        return self._with_reconnect(op)

    # ---- functions / RPC ----
    def _do_expose(self, name, req_schema_json, resp_schema_json, key, req_arena_cap):
        corr = _new_id()
        e = avro.Encoder()
        e.put_long(corr)
        e.put_long(self._client_id_tx())
        e.put_string(name)
        e.put_string(req_schema_json)
        e.put_string(resp_schema_json)
        e.put_string(key or "")
        e.put_long(req_arena_cap)
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
        return Ring.attach(mm, 0), req_fp, resp_fp

    def expose_function(self, name, req_schema_json, resp_schema_json, handler, key=None, req_arena_cap=0):
        """`handler(req_body: bytes) -> resp_body: bytes` runs on a thread.

        `req_arena_cap` requests a request-arena capacity in bytes (0 = broker
        default). The broker clamps it to [256 KiB, 128 MiB] and rounds it up to
        a power of two.
        """
        req_ring, req_fp, resp_fp = self._with_reconnect(
            lambda: self._do_expose(name, req_schema_json, resp_schema_json, key, req_arena_cap)
        )
        sv = _SvcReg(name, req_schema_json, resp_schema_json, key, req_arena_cap, handler, req_ring, req_fp, resp_fp)
        with self._reg_lock:
            self._services.append(sv)
        t = threading.Thread(target=self._serve, args=(sv,), daemon=True)
        t.start()
        self._svc_threads.append(t)

    def _serve(self, sv: _SvcReg):
        reply_cache = {}
        while self._running:
            # Snapshot the request ring + fingerprints under the lock so a
            # reconnect's re-expose (which swaps the arena) is picked up.
            with sv.lock:
                req_ring = sv.req_ring
                req_fp = sv.req_fp
                resp_fp = sv.resp_fp
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
                    result = sv.handler(args)
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
        """Send an RPC and return a Future resolving to the response body.

        Note: the returned future is not auto-retried across a broker restart
        that happens after the request was placed; use :meth:`call` for that.
        """
        # Lookup the function (broker returns fingerprints + arena).
        corr = _new_id()
        e = avro.Encoder()
        e.put_long(corr)
        e.put_long(self._client_id_tx())
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
        e.put_long(self._client_id_tx())
        e.put_string(self._reply_name_tx())
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
        """Blocking RPC call returning the response body.

        The whole lookup -> send -> await round-trip is retried once if impulsed
        is restarted underneath it (when auto-reconnect is enabled).
        """

        def op():
            fut = self.call_async(fn_name, req_body, key)
            try:
                return fut.result(timeout)
            except FTimeout:
                raise RingError("rpc call timed out") from None

        return self._with_reconnect(op)
