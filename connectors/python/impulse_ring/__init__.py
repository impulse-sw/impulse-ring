"""Ring — native Python connector (by Impulse).

Pure-Python implementation of the Ring wire protocol (shared memory, rings,
frames, Avro, control plane) against SPEC/. The only native code is the tiny
``_ring_native`` extension providing cross-process atomics + futex, which
Python's stdlib cannot offer.

Example::

    from impulse_ring import Connection, avro
    conn = Connection("my-service")
    for ch in conn.list_channels():
        print(ch.name)
"""

from . import avro
from .avro import Decoder, Encoder
from .connection import ChannelInfo, Connection, Publisher, RingError, Subscriber

__all__ = [
    "Connection",
    "Publisher",
    "Subscriber",
    "ChannelInfo",
    "RingError",
    "Encoder",
    "Decoder",
    "avro",
]
