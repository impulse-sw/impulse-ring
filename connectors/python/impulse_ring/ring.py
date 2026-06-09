"""Shared-memory ring buffer (see SPEC/wire-format.md section 2).

Pure Python on top of the `_ring_native` atomics/futex helper. MPSC-safe via the
3-state futex mutex on the producer side; single consumer on the read side.
Length-prefixed records, wrap handled with a two-part copy.
"""

import time

from . import _ring_native as nat

# Field offsets (relative to ring base).
R_MAGIC = 0
R_CAP = 4
R_HEAD = 64
R_PRODLOCK = 72
R_SPACE_SEQ = 76
R_TAIL = 128
R_DATA_SEQ = 136
RING_HEADER = 192
RING_MAGIC = 0x474E4952  # "RING"
_U64 = (1 << 64) - 1
_INT_MAX = 0x7FFFFFFF
_SPIN = 256


def ring_bytes(capacity: int) -> int:
    return RING_HEADER + capacity


class Ring:
    def __init__(self, mm, base: int, cap: int):
        self.mm = mm
        self.base = base
        self.cap = cap
        self.mask = cap - 1
        self.data_off = base + RING_HEADER

    @classmethod
    def format(cls, mm, base: int, cap: int) -> "Ring":
        assert cap & (cap - 1) == 0, "capacity must be power of two"
        r = cls(mm, base, cap)
        nat.store_u64(mm, base + R_HEAD, 0)
        nat.store_u64(mm, base + R_TAIL, 0)
        nat.store_u32(mm, base + R_PRODLOCK, 0)
        nat.store_u32(mm, base + R_SPACE_SEQ, 0)
        nat.store_u32(mm, base + R_DATA_SEQ, 0)
        nat.store_u32(mm, base + R_CAP, cap)
        nat.store_u32(mm, base + R_MAGIC, RING_MAGIC)  # released last
        return r

    @classmethod
    def attach(cls, mm, base: int) -> "Ring":
        if nat.load_u32(mm, base + R_MAGIC) != RING_MAGIC:
            raise ValueError("ring magic mismatch / not formatted")
        cap = nat.load_u32(mm, base + R_CAP)
        if cap == 0 or (cap & (cap - 1)) != 0:
            raise ValueError("ring capacity invalid")
        return cls(mm, base, cap)

    # ---- data region copies (with wrap) ----
    def _write(self, pos: int, data: bytes):
        idx = pos & self.mask
        first = min(len(data), self.cap - idx)
        self.mm[self.data_off + idx : self.data_off + idx + first] = data[:first]
        if first < len(data):
            rest = len(data) - first
            self.mm[self.data_off : self.data_off + rest] = data[first:]

    def _read(self, pos: int, n: int) -> bytes:
        idx = pos & self.mask
        first = min(n, self.cap - idx)
        out = bytearray(self.mm[self.data_off + idx : self.data_off + idx + first])
        if first < n:
            out += self.mm[self.data_off : self.data_off + (n - first)]
        return bytes(out)

    # ---- producer 3-state futex mutex ----
    def _lock(self):
        if nat.cas_u32(self.mm, self.base + R_PRODLOCK, 0, 1):
            return
        while nat.swap_u32(self.mm, self.base + R_PRODLOCK, 2) != 0:
            nat.futex_wait(self.mm, self.base + R_PRODLOCK, 2, 50)

    def _unlock(self):
        if nat.swap_u32(self.mm, self.base + R_PRODLOCK, 0) == 2:
            nat.futex_wake(self.mm, self.base + R_PRODLOCK, 1)

    def _push_locked(self, data: bytes) -> int:
        need = 4 + len(data)
        if need > self.cap:
            return -1
        head = nat.load_u64(self.mm, self.base + R_HEAD)
        tail = nat.load_u64(self.mm, self.base + R_TAIL)
        used = (head - tail) & _U64
        if self.cap - used < need:
            return 1  # full
        self._write(head, len(data).to_bytes(4, "little"))
        self._write(head + 4, data)
        nat.store_u64(self.mm, self.base + R_HEAD, (head + need) & _U64)
        return 0

    def try_push(self, data: bytes) -> bool:
        self._lock()
        rc = self._push_locked(data)
        self._unlock()
        if rc == 0:
            nat.fetch_add_u32(self.mm, self.base + R_DATA_SEQ, 1)
            nat.futex_wake(self.mm, self.base + R_DATA_SEQ, 1)
            return True
        return False

    def push_blocking(self, data: bytes, timeout_ms: int = -1) -> bool:
        start = time.monotonic()
        while True:
            self._lock()
            rc = self._push_locked(data)
            if rc == 0:
                self._unlock()
                nat.fetch_add_u32(self.mm, self.base + R_DATA_SEQ, 1)
                nat.futex_wake(self.mm, self.base + R_DATA_SEQ, 1)
                return True
            if rc < 0:
                self._unlock()
                return False
            seen = nat.load_u32(self.mm, self.base + R_SPACE_SEQ)
            self._unlock()
            if timeout_ms >= 0:
                elapsed = int((time.monotonic() - start) * 1000)
                if elapsed >= timeout_ms:
                    return False
                nat.futex_wait(self.mm, self.base + R_SPACE_SEQ, seen, timeout_ms - elapsed)
            else:
                nat.futex_wait(self.mm, self.base + R_SPACE_SEQ, seen, 50)

    def try_pop(self):
        tail = nat.load_u64(self.mm, self.base + R_TAIL)
        head = nat.load_u64(self.mm, self.base + R_HEAD)
        if head == tail:
            return None
        length = int.from_bytes(self._read(tail, 4), "little")
        payload = self._read(tail + 4, length)
        nat.store_u64(self.mm, self.base + R_TAIL, (tail + 4 + length) & _U64)
        nat.fetch_add_u32(self.mm, self.base + R_SPACE_SEQ, 1)
        nat.futex_wake(self.mm, self.base + R_SPACE_SEQ, _INT_MAX)
        return payload

    def pop_blocking(self, timeout_ms: int = -1):
        start = time.monotonic()
        while True:
            for _ in range(_SPIN):
                v = self.try_pop()
                if v is not None:
                    return v
            seen = nat.load_u32(self.mm, self.base + R_DATA_SEQ)
            v = self.try_pop()
            if v is not None:
                return v
            if timeout_ms >= 0:
                elapsed = int((time.monotonic() - start) * 1000)
                if elapsed >= timeout_ms:
                    return None
                nat.futex_wait(self.mm, self.base + R_DATA_SEQ, seen, timeout_ms - elapsed)
            else:
                nat.futex_wait(self.mm, self.base + R_DATA_SEQ, seen, 100)
