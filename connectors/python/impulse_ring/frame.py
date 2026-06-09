"""Self-describing wire frame (see SPEC/wire-format.md section 3)."""

import struct

FRAME_MAGIC = 0x5249  # "IR"
WIRE_VERSION = 1
FRAME_HEADER = 16


def encode(schema_fp: int, body: bytes, flags: int = 0) -> bytes:
    return (
        struct.pack("<HBB", FRAME_MAGIC, WIRE_VERSION, flags)
        + struct.pack("<Q", schema_fp & ((1 << 64) - 1))
        + struct.pack("<I", len(body))
        + body
    )


def decode(buf: bytes):
    """Return (schema_fp, body) or raise ValueError."""
    if len(buf) < FRAME_HEADER:
        raise ValueError("frame shorter than header")
    magic, ver, _flags = struct.unpack_from("<HBB", buf, 0)
    if magic != FRAME_MAGIC:
        raise ValueError("bad frame magic")
    if ver != WIRE_VERSION:
        raise ValueError(f"unsupported wire version {ver}")
    (schema_fp,) = struct.unpack_from("<Q", buf, 4)
    (body_len,) = struct.unpack_from("<I", buf, 12)
    if FRAME_HEADER + body_len > len(buf):
        raise ValueError("frame body truncated")
    return schema_fp, bytes(buf[FRAME_HEADER : FRAME_HEADER + body_len])
