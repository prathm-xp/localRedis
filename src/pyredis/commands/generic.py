"""Type-agnostic key commands: DEL, EXISTS, KEYS, TYPE, TTL and the expiry family."""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

from pyredis.commands.base import CommandSet
from pyredis.commands.parsing import parse_int
from pyredis.errors import CommandError, syntax_error
from pyredis.globmatch import glob_match
from pyredis.protocol import OK, Reply, SimpleString

if TYPE_CHECKING:  # pragma: no cover
    from pyredis.connection import Connection

commands = CommandSet()

#: Seconds-resolution commands multiply by this to reach the store's millisecond deadlines.
_MS_PER_SECOND = 1000


@commands.add("DEL", arity=-2, is_write=True)
def delete(connection: Connection, args: list[bytes]) -> Reply:
    """``DEL key [key ...]``. Returns how many of the keys existed."""
    store = connection.server.store
    return sum(1 for key in args[1:] if store.delete(key))


@commands.add("UNLINK", arity=-2, is_write=True)
def unlink(connection: Connection, args: list[bytes]) -> Reply:
    """``UNLINK key [key ...]``.

    Redis unlinks on a background thread. Here the work is the same dictionary
    removal ``DEL`` performs, so this is an alias - the distinction only matters
    for a multi-threaded implementation.
    """
    return delete(connection, args)


@commands.add("EXISTS", arity=-2)
def exists(connection: Connection, args: list[bytes]) -> Reply:
    """``EXISTS key [key ...]``. Counts each occurrence, so repeats count twice."""
    store = connection.server.store
    return sum(1 for key in args[1:] if store.get(key) is not None)


@commands.add("TYPE", arity=2)
def type_(connection: Connection, args: list[bytes]) -> Reply:
    """``TYPE key``. ``none`` when the key does not exist."""
    return SimpleString(connection.server.store.type_name(args[1]))


@commands.add("KEYS", arity=2)
def keys(connection: Connection, args: list[bytes]) -> Reply:
    """``KEYS pattern``.

    O(n) over the keyspace, exactly as in Redis, and just as unsuitable for
    production use on a large dataset.
    """
    pattern = args[1]
    return [key for key in connection.server.store.all_keys() if glob_match(pattern, key)]


@commands.add("RANDOMKEY", arity=1)
def randomkey(connection: Connection, args: list[bytes]) -> Reply:
    """``RANDOMKEY``. A random live key, or nil when the keyspace is empty."""
    live = connection.server.store.all_keys()
    return random.choice(live) if live else None


@commands.add("RENAME", arity=3, is_write=True)
def rename(connection: Connection, args: list[bytes]) -> Reply:
    """``RENAME source destination``. Carries the TTL across, as Redis does."""
    store = connection.server.store
    source, destination = args[1], args[2]
    value = store.get(source)
    if value is None:
        raise CommandError("ERR no such key")
    deadline = store.expire_at_ms(source)
    store.delete(source)
    store.set(destination, value, expires_at_ms=deadline)
    return OK


@commands.add("TTL", arity=2)
def ttl(connection: Connection, args: list[bytes]) -> Reply:
    """``TTL key``. Remaining lifetime in seconds, rounded up."""
    remaining = connection.server.store.pttl_ms(args[1])
    if remaining < 0:
        return remaining  # -1 (no expiry) or -2 (no key)
    # Redis rounds up so a key with 1ms left reports 1s rather than 0s.
    return (remaining + _MS_PER_SECOND - 1) // _MS_PER_SECOND


@commands.add("PTTL", arity=2)
def pttl(connection: Connection, args: list[bytes]) -> Reply:
    """``PTTL key``. Remaining lifetime in milliseconds."""
    return connection.server.store.pttl_ms(args[1])


@commands.add("PERSIST", arity=2, is_write=True)
def persist(connection: Connection, args: list[bytes]) -> Reply:
    """``PERSIST key``. Removes the TTL; 1 if there was one to remove."""
    return 1 if connection.server.store.persist(args[1]) else 0


@commands.add("EXPIRE", arity=-3, is_write=True)
def expire(connection: Connection, args: list[bytes]) -> Reply:
    """``EXPIRE key seconds``. Relative, seconds."""
    return _apply_expiry(connection, args, multiplier=_MS_PER_SECOND, absolute=False)


@commands.add("PEXPIRE", arity=-3, is_write=True)
def pexpire(connection: Connection, args: list[bytes]) -> Reply:
    """``PEXPIRE key milliseconds``. Relative, milliseconds."""
    return _apply_expiry(connection, args, multiplier=1, absolute=False)


@commands.add("EXPIREAT", arity=-3, is_write=True)
def expireat(connection: Connection, args: list[bytes]) -> Reply:
    """``EXPIREAT key unix-time-seconds``. Absolute, seconds."""
    return _apply_expiry(connection, args, multiplier=_MS_PER_SECOND, absolute=True)


@commands.add("PEXPIREAT", arity=-3, is_write=True)
def pexpireat(connection: Connection, args: list[bytes]) -> Reply:
    """``PEXPIREAT key unix-time-milliseconds``. Absolute, milliseconds."""
    return _apply_expiry(connection, args, multiplier=1, absolute=True)


def _apply_expiry(
    connection: Connection, args: list[bytes], *, multiplier: int, absolute: bool
) -> Reply:
    """Shared body of the four expiry commands.

    A deadline in the past deletes the key immediately, which is what Redis
    does and is the only behaviour consistent with "the key is gone once the
    deadline passes".

    Whichever form the client used, the propagated command is ``PEXPIREAT``
    with an absolute deadline, so a replica cannot drift from its master by the
    network latency between them.
    """
    if len(args) != 3:
        # The NX/XX/GT/LT modifiers of Redis 7 are not implemented.
        raise syntax_error()

    store = connection.server.store
    key = args[1]
    amount = parse_int(args[2])
    deadline = amount * multiplier if absolute else store.clock() + amount * multiplier

    if store.get(key) is None:
        return 0

    if deadline <= store.clock():
        store.delete(key)
        connection.rewrite_propagation(b"DEL", key)
        return 1

    store.set_expiry_at(key, deadline)
    connection.rewrite_propagation(b"PEXPIREAT", key, b"%d" % deadline)
    return 1


__all__ = ["commands"]
