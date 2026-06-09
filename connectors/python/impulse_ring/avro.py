"""Minimal Apache Avro binary codec (the subset Ring needs).

Pure Python. Covers null/boolean/int/long/float/double/string/bytes and
single-block arrays — enough for record payloads and the control protocol.
Encoding matches the Avro spec, so it interoperates byte-for-byte with the
Rust broker's apache-avro and the C connector.
"""

import struct

_U64 = (1 << 64) - 1


class Encoder:
    """Accumulates an Avro binary datum."""

    def __init__(self):
        self.buf = bytearray()

    def _varint(self, zz: int):
        while zz & ~0x7F:
            self.buf.append((zz & 0x7F) | 0x80)
            zz >>= 7
        self.buf.append(zz & 0x7F)

    def put_long(self, v: int):
        self._varint(((v << 1) ^ (v >> 63)) & _U64)

    # Avro int and long share the same zig-zag varint encoding.
    put_int = put_long

    def put_boolean(self, v: bool):
        self.buf.append(1 if v else 0)

    def put_float(self, v: float):
        self.buf += struct.pack("<f", v)

    def put_double(self, v: float):
        self.buf += struct.pack("<d", v)

    def put_bytes(self, b: bytes):
        self.put_long(len(b))
        self.buf += b

    def put_string(self, s: str):
        self.put_bytes(s.encode("utf-8"))

    def array_start(self, count: int):
        self.put_long(count)

    def array_end(self):
        self.put_long(0)

    def getvalue(self) -> bytes:
        return bytes(self.buf)


class Decoder:
    """Reads an Avro binary datum from `data`."""

    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def _varint(self) -> int:
        result = 0
        shift = 0
        while True:
            b = self.data[self.pos]
            self.pos += 1
            result |= (b & 0x7F) << shift
            if not (b & 0x80):
                return result
            shift += 7

    def get_long(self) -> int:
        n = self._varint()
        return (n >> 1) ^ -(n & 1)

    get_int = get_long

    def get_boolean(self) -> bool:
        b = self.data[self.pos]
        self.pos += 1
        return b != 0

    def get_float(self) -> float:
        v = struct.unpack_from("<f", self.data, self.pos)[0]
        self.pos += 4
        return v

    def get_double(self) -> float:
        v = struct.unpack_from("<d", self.data, self.pos)[0]
        self.pos += 8
        return v

    def get_bytes(self) -> bytes:
        n = self.get_long()
        b = bytes(self.data[self.pos : self.pos + n])
        self.pos += n
        return b

    def get_string(self) -> str:
        return self.get_bytes().decode("utf-8")

    def get_array_count(self) -> int:
        """Block count; negative means a byte-size follows (rare). 0 ends."""
        return self.get_long()


def peek_long(data: bytes) -> int:
    """Read the first Avro long without consuming a Decoder."""
    return Decoder(data).get_long()
