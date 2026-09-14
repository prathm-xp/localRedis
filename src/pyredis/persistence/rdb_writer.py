"""Writing RDB snapshots.

The output is a valid RDB v11 file: real ``redis-server`` will load one, and
so will the reader in this package. Values are written in the plain encodings
(no integer packing, no LZF, no listpacks) because the space saving is not
worth the extra code paths here, and the format makes every one of those
choices optional.

**Streams are not written.** The listpack encoding Redis uses for streams is
substantially more involved than the rest of the format, and implementing only
half of it would produce files that look valid and lose data silently. Instead
stream keys are skipped and reported, both in the log and in the return value.
See ``docs/LIMITATIONS.md``.

Snapshots are written to a temporary file and renamed into place, so a crash
mid-write cannot leave a truncated file where the old good one was.
"""

from __future__ import annotations

import logging
import os
import struct
from collections import deque
from dataclasses import dataclass
from pathlib import Path

from pyredis import __version__
from pyredis.datastore import SortedSet, Store, Stream
from pyredis.persistence.crc64 import crc64
from pyredis.persistence.format import (
    AUX,
    EOF,
    EXPIRETIME_MS,
    MAGIC,
    RESIZE_DB,
    SELECT_DB,
    TYPE_LIST,
    TYPE_STRING,
    TYPE_ZSET_2,
    VERSION,
    encode_length,
    encode_string,
)

__all__ = ["SnapshotResult", "dump_rdb", "dump_rdb_bytes"]

logger = logging.getLogger(__name__)

#: zset2 stores scores as a raw little-endian IEEE-754 double.
_DOUBLE = struct.Struct("<d")


@dataclass(frozen=True, slots=True)
class SnapshotResult:
    """What a snapshot actually captured."""

    written_keys: int
    skipped_keys: int
    size_bytes: int


def dump_rdb_bytes(store: Store) -> tuple[bytes, SnapshotResult]:
    """Serialize ``store`` to an RDB image."""
    out = bytearray()
    out += MAGIC + VERSION
    out += _aux(b"pyredis-ver", __version__.encode())
    out += _aux(b"redis-bits", b"64")

    entries = [(key, value, deadline) for key, value, deadline in store.items()]
    serializable = [entry for entry in entries if _type_byte(entry[1]) is not None]
    skipped = len(entries) - len(serializable)

    out.append(SELECT_DB)
    out += encode_length(0)
    out.append(RESIZE_DB)
    out += encode_length(len(serializable))
    out += encode_length(sum(1 for _, _, deadline in serializable if deadline is not None))

    for key, value, deadline in serializable:
        if deadline is not None:
            out.append(EXPIRETIME_MS)
            out += deadline.to_bytes(8, "little")
        type_byte = _type_byte(value)
        assert type_byte is not None  # filtered above
        out.append(type_byte)
        out += encode_string(key)
        out += _encode_value(value)

    out.append(EOF)
    out += crc64(bytes(out)).to_bytes(8, "little")

    if skipped:
        logger.warning(
            "snapshot skipped %d key(s) of unsupported type (streams are not persisted)", skipped
        )
    return bytes(out), SnapshotResult(len(serializable), skipped, len(out))


def dump_rdb(store: Store, path: Path) -> SnapshotResult:
    """Write ``store`` to ``path`` atomically."""
    payload, result = dump_rdb_bytes(store)
    temporary = path.with_name(f"{path.name}.tmp-{os.getpid()}")
    try:
        temporary.write_bytes(payload)
        temporary.replace(path)
    except OSError:
        temporary.unlink(missing_ok=True)
        raise
    logger.info("wrote %d keys (%d bytes) to %s", result.written_keys, result.size_bytes, path)
    return result


def _aux(key: bytes, value: bytes) -> bytes:
    return bytes([AUX]) + encode_string(key) + encode_string(value)


def _type_byte(value: object) -> int | None:
    """The RDB type byte for a value, or ``None`` if it cannot be serialized."""
    if isinstance(value, bytes):
        return TYPE_STRING
    if isinstance(value, deque):
        return TYPE_LIST
    if isinstance(value, SortedSet):
        return TYPE_ZSET_2
    if isinstance(value, Stream):
        return None
    raise AssertionError(f"unregistered value type {type(value).__name__}")


def _encode_value(value: object) -> bytes:
    if isinstance(value, bytes):
        return encode_string(value)

    if isinstance(value, deque):
        out = bytearray(encode_length(len(value)))
        for item in value:
            out += encode_string(item)
        return bytes(out)

    if isinstance(value, SortedSet):
        out = bytearray(encode_length(len(value)))
        for score, member in value.items():
            out += encode_string(member)
            out += _DOUBLE.pack(score)
        return bytes(out)

    raise AssertionError(f"no encoder for {type(value).__name__}")
