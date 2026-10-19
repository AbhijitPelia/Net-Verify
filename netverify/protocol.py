"""Wire format for the messaging protocol.

Packet layout (network byte order):

    magic   2 bytes   b"NV"
    type    1 byte    1 = MSG, 2 = ACK
    seq     4 bytes   sequence number chosen by the client
    length  2 bytes   payload length in bytes
    crc32   4 bytes   checksum over type + seq + payload
    payload `length` bytes
"""

import struct
import zlib

MAGIC = b"NV"
TYPE_MSG = 1
TYPE_ACK = 2
MAX_PAYLOAD = 1024

HEADER = struct.Struct("!2sBIHI")


class ProtocolError(ValueError):
    """Raised when a packet cannot be encoded or fails validation on decode."""


def _checksum(type_: int, seq: int, payload: bytes) -> int:
    return zlib.crc32(struct.pack("!BI", type_, seq) + payload) & 0xFFFFFFFF


def encode(type_: int, seq: int, payload: bytes = b"") -> bytes:
    if len(payload) > MAX_PAYLOAD:
        raise ProtocolError(f"payload of {len(payload)} bytes exceeds {MAX_PAYLOAD}")
    header = HEADER.pack(MAGIC, type_, seq, len(payload), _checksum(type_, seq, payload))
    return header + payload


def decode(data: bytes):
    """Return (type, seq, payload) or raise ProtocolError if the packet is invalid."""
    if len(data) < HEADER.size:
        raise ProtocolError("packet shorter than header")
    magic, type_, seq, length, crc = HEADER.unpack_from(data)
    if magic != MAGIC:
        raise ProtocolError("bad magic")
    payload = data[HEADER.size:]
    if len(payload) != length:
        raise ProtocolError("length mismatch")
    if crc != _checksum(type_, seq, payload):
        raise ProtocolError("checksum mismatch")
    if type_ not in (TYPE_MSG, TYPE_ACK):
        raise ProtocolError("unknown packet type")
    return type_, seq, payload
