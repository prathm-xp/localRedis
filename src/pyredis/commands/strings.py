"""String commands: SET, GET, the counters and the small accessors."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyredis.commands.base import CommandSet
from pyredis.commands.parsing import check_int64, parse_int
from pyredis.datastore import normalize_range, require_type
from pyredis.errors import CommandError, syntax_error
from pyredis.protocol import OK, Reply

if TYPE_CHECKING:  # pragma: no cover
    from pyredis.connection import Connection

commands = CommandSet()

__all__ = ["commands"]


@dataclass(slots=True)
class SetOptions:
    """Parsed modifiers of the ``SET`` command."""

    expires_at_ms: int | None = None
    keep_ttl: bool = False
    only_if_absent: bool = False
    only_if_present: bool = False
    return_old_value: bool = False


def _parse_set_options(args: list[bytes], now_ms: int) -> SetOptions:
    """Parse ``SET key value [options]``.

    All four expiry spellings are normalised to one absolute millisecond
    deadline here, so the rest of the command - and the value propagated to
    replicas - deals with a single representation.
    """
    options = SetOptions()
    index = 3

    while index < len(args):
        modifier = args[index].upper()

        if modifier in (b"EX", b"PX", b"EXAT", b"PXAT"):
            if index + 1 >= len(args):
                raise syntax_error()
            if options.expires_at_ms is not None or options.keep_ttl:
                raise syntax_error()
            amount = parse_int(args[index + 1])
            if modifier in (b"EX", b"PX") and amount <= 0:
                raise CommandError("ERR invalid expire time in 'set' command")
            scale = 1000 if modifier in (b"EX", b"EXAT") else 1
            base = now_ms if modifier in (b"EX", b"PX") else 0
            options.expires_at_ms = base + amount * scale
            index += 2
            continue

        if modifier == b"KEEPTTL":
            if options.expires_at_ms is not None:
                raise syntax_error()
            options.keep_ttl = True
        elif modifier == b"NX":
            if options.only_if_present:
                raise syntax_error()
            options.only_if_absent = True
        elif modifier == b"XX":
            if options.only_if_absent:
                raise syntax_error()
            options.only_if_present = True
        elif modifier == b"GET":
            options.return_old_value = True
        else:
            raise syntax_error()
        index += 1

    return options


@commands.add("SET", arity=-3, is_write=True)
def set_(connection: Connection, args: list[bytes]) -> Reply:
    """``SET key value [EX s|PX ms|EXAT ts|PXAT ts] [NX|XX] [KEEPTTL] [GET]``.

    Propagated as ``SET key value PXAT <deadline>``: a relative expiry replayed
    on a replica would start its clock when the replica applied it, leaving the
    two copies with different deadlines. ``NX``/``XX`` are dropped from the
    propagated form because the condition has already been evaluated here, and
    re-evaluating it against the replica's state could reject a write the master
    accepted.
    """
    store = connection.server.store
    key, value = args[1], args[2]
    options = _parse_set_options(args, store.clock())

    previous = store.get(key)
    previous_value: bytes | None = None
    if options.return_old_value and previous is not None:
        previous_value = require_type(previous, bytes)

    if (options.only_if_absent and previous is not None) or (
        options.only_if_present and previous is None
    ):
        return previous_value if options.return_old_value else None

    store.set(key, value, expires_at_ms=options.expires_at_ms, keep_ttl=options.keep_ttl)

    propagated: list[bytes] = [b"SET", key, value]
    if options.expires_at_ms is not None:
        propagated += [b"PXAT", b"%d" % options.expires_at_ms]
    elif options.keep_ttl:
        propagated.append(b"KEEPTTL")
    connection.rewrite_propagation(*propagated)

    return previous_value if options.return_old_value else OK


@commands.add("SETNX", arity=3, is_write=True)
def setnx(connection: Connection, args: list[bytes]) -> Reply:
    """``SETNX key value``. 1 if the key was created."""
    store = connection.server.store
    if store.get(args[1]) is not None:
        return 0
    store.set(args[1], args[2])
    return 1


@commands.add("GET", arity=2)
def get(connection: Connection, args: list[bytes]) -> Reply:
    """``GET key``. Nil for a missing key; ``WRONGTYPE`` for a non-string."""
    value = connection.server.store.get(args[1])
    if value is None:
        connection.server.stats.keyspace_misses += 1
        return None
    connection.server.stats.keyspace_hits += 1
    return require_type(value, bytes)


@commands.add("GETDEL", arity=2, is_write=True)
def getdel(connection: Connection, args: list[bytes]) -> Reply:
    """``GETDEL key``. Returns the value and removes the key atomically."""
    store = connection.server.store
    value = store.get(args[1])
    if value is None:
        return None
    result = require_type(value, bytes)
    store.delete(args[1])
    connection.rewrite_propagation(b"DEL", args[1])
    return result


@commands.add("MGET", arity=-2)
def mget(connection: Connection, args: list[bytes]) -> Reply:
    """``MGET key [key ...]``. Non-string values report nil rather than erroring."""
    store = connection.server.store
    out: list[Reply] = []
    for key in args[1:]:
        value = store.get(key)
        out.append(value if isinstance(value, bytes) else None)
    return out


@commands.add("MSET", arity=-3, is_write=True)
def mset(connection: Connection, args: list[bytes]) -> Reply:
    """``MSET key value [key value ...]``."""
    pairs = args[1:]
    if len(pairs) % 2 != 0:
        raise CommandError("ERR wrong number of arguments for 'mset' command")
    store = connection.server.store
    for index in range(0, len(pairs), 2):
        store.set(pairs[index], pairs[index + 1])
    return OK


@commands.add("APPEND", arity=3, is_write=True)
def append(connection: Connection, args: list[bytes]) -> Reply:
    """``APPEND key value``. Returns the new length."""
    store = connection.server.store
    key = args[1]
    existing = store.get(key)
    current = require_type(existing, bytes) if existing is not None else b""
    updated = current + args[2]
    store.set(key, updated, keep_ttl=True)
    return len(updated)


@commands.add("STRLEN", arity=2)
def strlen(connection: Connection, args: list[bytes]) -> Reply:
    """``STRLEN key``. 0 for a missing key."""
    value = connection.server.store.get(args[1])
    return 0 if value is None else len(require_type(value, bytes))


@commands.add("GETRANGE", arity=4)
def getrange(connection: Connection, args: list[bytes]) -> Reply:
    """``GETRANGE key start end``. Inclusive, negative indexes count from the end."""
    value = connection.server.store.get(args[1])
    if value is None:
        return b""
    data = require_type(value, bytes)
    start, end = normalize_range(parse_int(args[2]), parse_int(args[3]), len(data))
    if start > end:
        return b""
    return data[start : end + 1]


@commands.add("INCR", arity=2, is_write=True)
def incr(connection: Connection, args: list[bytes]) -> Reply:
    """``INCR key``."""
    return _increment_by(connection, args[1], 1)


@commands.add("DECR", arity=2, is_write=True)
def decr(connection: Connection, args: list[bytes]) -> Reply:
    """``DECR key``."""
    return _increment_by(connection, args[1], -1)


@commands.add("INCRBY", arity=3, is_write=True)
def incrby(connection: Connection, args: list[bytes]) -> Reply:
    """``INCRBY key increment``."""
    return _increment_by(connection, args[1], parse_int(args[2]))


@commands.add("DECRBY", arity=3, is_write=True)
def decrby(connection: Connection, args: list[bytes]) -> Reply:
    """``DECRBY key decrement``."""
    return _increment_by(connection, args[1], -parse_int(args[2]))


def _increment_by(connection: Connection, key: bytes, delta: int) -> int:
    """Shared body of the four counter commands.

    A missing key starts at zero. Redis stores counters as strings and reparses
    them on every increment, so a value that is not a valid 64-bit integer -
    including one with padding, like ``" 1"`` - is an error rather than a
    silent coercion. Overflow is reported too: these are 64-bit counters, not
    Python's arbitrary-precision integers.
    """
    store = connection.server.store
    existing = store.get(key)

    if existing is None:
        current = 0
    else:
        current = parse_int(require_type(existing, bytes))

    updated = current + delta
    try:
        check_int64(updated)
    except CommandError:
        raise CommandError("ERR increment or decrement would overflow") from None

    store.set(key, b"%d" % updated, keep_ttl=True)
    return updated
