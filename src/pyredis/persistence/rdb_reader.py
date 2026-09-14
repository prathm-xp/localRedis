"""Reading RDB snapshots.

Handles files written by this server and the subset of real Redis files whose
values use the encodings implemented here (see
:data:`~pyredis.persistence.format.KNOWN_UNSUPPORTED_TYPES` for what is not).
Unsupported encodings raise :class:`~pyredis.errors.RdbError` naming the type,
rather than silently loading a partial dataset.

The whole file is read into memory before parsing. RDB snapshots are bounded by
the dataset they describe, and the alternative - a resumable parser - would add
complexity that only matters for datasets this server is not built for.
"""

from __future__ import annotations

import logging
from collections import deque
from pathlib import Path

from pyredis.datastore import SortedSet, Store
from pyredis.errors import RdbError
from pyredis.persistence.crc64 import crc64
from pyredis.persistence.format import (
    AUX,
    EOF,
    EXPIRETIME_MS,
    EXPIRETIME_SECONDS,
    FREQ,
    IDLE,
    KNOWN_UNSUPPORTED_TYPES,
    MAGIC,
    RESIZE_DB,
    SELECT_DB,
    TYPE_LIST,
    TYPE_STRING,
    TYPE_ZSET_2,
    VERSION,
    ByteCursor,
)

__all__ = ["load_rdb", "load_rdb_bytes"]

logger = logging.getLogger(__name__)

#: Only database 0 is implemented, so keys from other databases are skipped.
SUPPORTED_DB = 0


def load_rdb(path: Path, store: Store) -> int:
    """Load ``path`` into ``store`` and return the number of keys loaded."""
    return load_rdb_bytes(path.read_bytes(), store)


def load_rdb_bytes(data: bytes, store: Store) -> int:
    """Load an in-memory RDB image, as received during a full resync."""
    cursor = ByteCursor(data)
    _read_header(cursor)

    loaded = 0
    skipped_other_db = 0
    database = SUPPORTED_DB
    expires_at_ms: int | None = None

    while True:
        opcode = cursor.read_byte()

        if opcode == EOF:
            break
        if opcode == AUX:
            aux_key = cursor.read_string()
            aux_value = cursor.read_string()
            logger.debug(
                "rdb metadata %s=%s",
                aux_key.decode(errors="replace"),
                aux_value.decode(errors="replace"),
            )
            continue
        if opcode == SELECT_DB:
            database = cursor.read_plain_length()
            continue
        if opcode == RESIZE_DB:
            cursor.read_plain_length()  # keyspace size hint
            cursor.read_plain_length()  # expires size hint
            continue
        if opcode == EXPIRETIME_MS:
            expires_at_ms = int.from_bytes(cursor.read(8), "little")
            continue
        if opcode == EXPIRETIME_SECONDS:
            expires_at_ms = int.from_bytes(cursor.read(4), "little") * 1000
            continue
        if opcode == IDLE:
            cursor.read_plain_length()
            continue
        if opcode == FREQ:
            cursor.read_byte()
            continue

        key = cursor.read_string()
        value = _read_value(cursor, opcode)
        if database == SUPPORTED_DB:
            store.load(key, value, expires_at_ms)
            loaded += 1
        else:
            skipped_other_db += 1
        expires_at_ms = None

    _verify_checksum(data, cursor)

    if skipped_other_db:
        logger.warning(
            "skipped %d keys from databases other than %d; only one database is supported",
            skipped_other_db,
            SUPPORTED_DB,
        )
    return loaded


def _read_header(cursor: ByteCursor) -> None:
    magic = cursor.read(5)
    if magic != MAGIC:
        raise RdbError(f"not an RDB file: expected magic {MAGIC!r}, got {magic!r}")
    raw_version = cursor.read(4)
    try:
        version = int(raw_version)
    except ValueError:
        raise RdbError(f"malformed RDB version {raw_version!r}") from None
    if version > int(VERSION):
        raise RdbError(f"RDB version {version} is newer than the supported version {int(VERSION)}")


def _read_value(cursor: ByteCursor, value_type: int) -> object:
    if value_type == TYPE_STRING:
        return cursor.read_string()

    if value_type == TYPE_LIST:
        count = cursor.read_plain_length()
        return deque(cursor.read_string() for _ in range(count))

    if value_type == TYPE_ZSET_2:
        count = cursor.read_plain_length()
        zset = SortedSet()
        for _ in range(count):
            member = cursor.read_string()
            zset.add(member, cursor.read_binary_double())
        return zset

    name = KNOWN_UNSUPPORTED_TYPES.get(value_type)
    if name is not None:
        raise RdbError(
            f"RDB value type {value_type} ({name}) is not supported by this implementation"
        )
    raise RdbError(f"unknown RDB value type {value_type} at offset {cursor.position}")


def _verify_checksum(data: bytes, cursor: ByteCursor) -> None:
    """Check the 8-byte CRC-64 trailer, if the file has one.

    A stored checksum of zero means the writer disabled checksumming, which the
    format permits.
    """
    body_length = cursor.position  # everything up to and including the EOF opcode
    remaining = len(data) - body_length
    if remaining == 0:
        logger.debug("RDB image has no checksum trailer")
        return
    if remaining < 8:
        raise RdbError(f"truncated checksum: {remaining} bytes after the EOF opcode")

    stored = int.from_bytes(data[body_length : body_length + 8], "little")
    if stored == 0:
        logger.debug("RDB checksum is zero; verification disabled by the writer")
        return
    actual = crc64(data[:body_length])
    if stored != actual:
        raise RdbError(
            f"RDB checksum mismatch: file says 0x{stored:016x}, computed 0x{actual:016x}"
        )
