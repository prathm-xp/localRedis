"""CRC-64 as used in the RDB file trailer.

Redis stores a CRC-64 of the whole file in its last eight bytes. The variant is
the Jones polynomial with reflected input and output (catalogued as
CRC-64/REDIS): polynomial ``0xad93d23594c935a9``, initial value 0, no final
XOR. Its published check value - the CRC of ``b"123456789"`` - is
``0xe9c6d914c4b8d9ca``, which :data:`CHECK_VALUE` records and the test suite
asserts against.

A stored checksum of zero means "not checksummed" and disables verification.
This server always writes a real checksum, and verifies a non-zero one on load.
"""

from __future__ import annotations

from typing import Final

__all__ = ["CHECK_VALUE", "POLYNOMIAL", "crc64"]

POLYNOMIAL: Final = 0xAD93D23594C935A9
CHECK_VALUE: Final = 0xE9C6D914C4B8D9CA

_MASK: Final = 0xFFFFFFFFFFFFFFFF


def _reflect64(value: int) -> int:
    return int(format(value, "064b")[::-1], 2)


def _build_table() -> tuple[int, ...]:
    """Byte-wise lookup table for the reflected algorithm."""
    reflected_poly = _reflect64(POLYNOMIAL)
    table = []
    for index in range(256):
        crc = index
        for _ in range(8):
            crc = (crc >> 1) ^ reflected_poly if crc & 1 else crc >> 1
        table.append(crc & _MASK)
    return tuple(table)


_TABLE: Final = _build_table()


def crc64(data: bytes, crc: int = 0) -> int:
    """CRC-64 of ``data``, optionally continuing from a previous ``crc``."""
    for byte in data:
        crc = _TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc & _MASK
