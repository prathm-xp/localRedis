"""RDB format constants and the length/string codecs both directions share.

An RDB file is:

.. code-block:: text

    "REDIS" <4-digit version>
    (0xFA <string> <string>)*        auxiliary metadata
    (0xFE <length>                   select database
     0xFB <length> <length>          optional size hint
     entry*)*
    0xFF
    <8-byte CRC-64>                  zero means "not checksummed"

and an entry is an optional expiry opcode, a one-byte value type, a
string-encoded key and the value.

The length encoding is the part worth reading carefully: the top two bits of
the first byte select between a 6-bit length, a 14-bit length, a 32- or 64-bit
length, and an *escape* into special string encodings - small integers stored
as integers, and LZF-compressed strings.
"""

from __future__ import annotations

import struct
from typing import Final

from pyredis.errors import RdbError
from pyredis.persistence.lzf import lzf_decompress

__all__ = [
    "AUX",
    "EOF",
    "EXPIRETIME_MS",
    "EXPIRETIME_SECONDS",
    "IDLE",
    "MAGIC",
    "RESIZE_DB",
    "SELECT_DB",
    "TYPE_LIST",
    "TYPE_STRING",
    "TYPE_ZSET_2",
    "VERSION",
    "ByteCursor",
    "encode_length",
    "encode_string",
]

MAGIC: Final = b"REDIS"
#: Version 11 is what Redis 7.x writes. The reader accepts anything <= this.
VERSION: Final = b"0011"

# Opcodes, which are value-type bytes that no real type uses.
IDLE: Final = 0xF8
FREQ: Final = 0xF9
AUX: Final = 0xFA
RESIZE_DB: Final = 0xFB
EXPIRETIME_MS: Final = 0xFC
EXPIRETIME_SECONDS: Final = 0xFD
SELECT_DB: Final = 0xFE
EOF: Final = 0xFF

# Value types this implementation reads and writes.
TYPE_STRING: Final = 0
TYPE_LIST: Final = 1
TYPE_ZSET_2: Final = 5

#: Value types that exist in the format but are not implemented here. Mapping
#: them to names turns "unsupported" into an error a user can act on.
KNOWN_UNSUPPORTED_TYPES: Final[dict[int, str]] = {
    2: "set",
    3: "zset (pre-2.8 double-as-string encoding)",
    4: "hash",
    6: "module",
    7: "module-2",
    9: "hash (zipmap)",
    10: "list (ziplist)",
    11: "set (intset)",
    12: "zset (ziplist)",
    13: "hash (ziplist)",
    14: "list (quicklist)",
    15: "stream (listpacks)",
    16: "hash (listpack)",
    17: "zset (listpack)",
    18: "list (quicklist v2)",
    19: "stream (listpacks v2)",
    20: "set (listpack)",
    21: "stream (listpacks v3)",
}

# Length-encoding selectors, taken from the top two bits of the first byte.
_LEN_6BIT: Final = 0x00
_LEN_14BIT: Final = 0x40
_LEN_WIDE: Final = 0x80
_LEN_SPECIAL: Final = 0xC0

_WIDE_32BIT: Final = 0x80
_WIDE_64BIT: Final = 0x81

# Special string encodings signalled by the 0xC0 selector.
_ENC_INT8: Final = 0
_ENC_INT16: Final = 1
_ENC_INT32: Final = 2
_ENC_LZF: Final = 3

#: zset2 stores scores as a raw little-endian IEEE-754 double.
_DOUBLE: Final = struct.Struct("<d")


class ByteCursor:
    """A bounds-checked read cursor over an in-memory RDB image.

    RDB files are small enough to read whole, and doing so keeps the parser
    free of partial-read handling. Every read validates its bounds, so a
    truncated or corrupt file raises :class:`~pyredis.errors.RdbError` rather
    than producing nonsense.
    """

    __slots__ = ("_data", "_pos")

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0

    @property
    def position(self) -> int:
        return self._pos

    @property
    def at_end(self) -> bool:
        return self._pos >= len(self._data)

    def read(self, count: int) -> bytes:
        end = self._pos + count
        if end > len(self._data):
            raise RdbError(
                f"unexpected end of file: wanted {count} bytes at offset {self._pos}, "
                f"only {len(self._data) - self._pos} remain"
            )
        chunk = self._data[self._pos : end]
        self._pos = end
        return chunk

    def read_byte(self) -> int:
        return self.read(1)[0]

    def peek_byte(self) -> int:
        if self.at_end:
            raise RdbError("unexpected end of file while peeking")
        return self._data[self._pos]

    def read_length(self) -> tuple[int, bool]:
        """Read a length.

        Returns the value and whether it was a *special encoding* selector, in
        which case the value is the encoding id rather than a byte count.
        """
        first = self.read_byte()
        selector = first & 0xC0

        if selector == _LEN_6BIT:
            return first & 0x3F, False
        if selector == _LEN_14BIT:
            return ((first & 0x3F) << 8) | self.read_byte(), False
        if selector == _LEN_SPECIAL:
            return first & 0x3F, True
        if first == _WIDE_32BIT:
            return int.from_bytes(self.read(4), "big"), False
        if first == _WIDE_64BIT:
            return int.from_bytes(self.read(8), "big"), False
        raise RdbError(f"invalid length encoding byte 0x{first:02x} at offset {self._pos - 1}")

    def read_plain_length(self) -> int:
        """Read a length that must not be a special encoding."""
        value, is_special = self.read_length()
        if is_special:
            raise RdbError("unexpected special encoding where a plain length was required")
        return value

    def read_string(self) -> bytes:
        """Read a string, resolving integer and LZF-compressed encodings."""
        length, is_special = self.read_length()
        if not is_special:
            return self.read(length)

        if length == _ENC_INT8:
            return b"%d" % int.from_bytes(self.read(1), "little", signed=True)
        if length == _ENC_INT16:
            return b"%d" % int.from_bytes(self.read(2), "little", signed=True)
        if length == _ENC_INT32:
            return b"%d" % int.from_bytes(self.read(4), "little", signed=True)
        if length == _ENC_LZF:
            compressed_length = self.read_plain_length()
            uncompressed_length = self.read_plain_length()
            return lzf_decompress(self.read(compressed_length), uncompressed_length)
        raise RdbError(f"unknown special string encoding {length}")

    def read_binary_double(self) -> float:
        """Read the 8-byte little-endian double used by the zset2 encoding."""
        return float(_DOUBLE.unpack(self.read(8))[0])


def encode_length(value: int) -> bytes:
    """Encode a length using the narrowest representation that fits."""
    if value < 0:
        raise ValueError("length cannot be negative")
    if value < 1 << 6:
        return bytes([_LEN_6BIT | value])
    if value < 1 << 14:
        return bytes([_LEN_14BIT | (value >> 8), value & 0xFF])
    if value < 1 << 32:
        return bytes([_WIDE_32BIT]) + value.to_bytes(4, "big")
    return bytes([_WIDE_64BIT]) + value.to_bytes(8, "big")


def encode_string(value: bytes) -> bytes:
    """Encode a string as a length followed by its bytes.

    Integer and LZF encodings are deliberately not produced: they only save
    space, the reader handles them, and leaving them out keeps the writer
    simple and its output trivially inspectable.
    """
    return encode_length(len(value)) + value
