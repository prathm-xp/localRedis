"""List commands, including the blocking pops.

Lists are ``collections.deque``, which gives O(1) push and pop at both ends -
the operations Redis lists are actually used for. ``LINDEX`` and ``LRANGE`` are
O(n) as a result, matching Redis's own complexity for the same commands.

``BLPOP``/``BRPOP`` are where the concurrency model shows through: the handler
is a coroutine that awaits the keyspace touch signal instead of blocking, so a
waiting client costs one coroutine rather than the whole server.
"""

from __future__ import annotations

import time
from collections import deque
from itertools import islice
from typing import TYPE_CHECKING

from pyredis.commands.base import CommandSet
from pyredis.commands.parsing import parse_int, parse_timeout_seconds
from pyredis.datastore import RedisList, normalize_range, require_type
from pyredis.errors import CommandError, syntax_error
from pyredis.protocol import NULL_ARRAY, OK, Reply

if TYPE_CHECKING:  # pragma: no cover
    from pyredis.connection import Connection

commands = CommandSet()

__all__ = ["commands"]


def _get_list(connection: Connection, key: bytes) -> RedisList | None:
    """Fetch a list, raising ``WRONGTYPE`` if the key holds something else."""
    value = connection.server.store.get(key)
    if value is None:
        return None
    return require_type(value, deque)


def _get_list_or_none(connection: Connection, key: bytes) -> RedisList | None:
    """Like :func:`_get_list`, but treats a wrong-typed value as "not a list".

    Used by the blocking pops after the first attempt; see :func:`_blocking_pop`.
    """
    value = connection.server.store.get(key)
    return value if isinstance(value, deque) else None


def _drop_if_empty(connection: Connection, key: bytes, target: RedisList) -> None:
    """Delete an emptied list. Redis has no empty collections."""
    store = connection.server.store
    if target:
        store.touch(key)
    else:
        store.delete(key)


@commands.add("RPUSH", arity=-3, is_write=True)
def rpush(connection: Connection, args: list[bytes]) -> Reply:
    """``RPUSH key element [element ...]``. Returns the new length."""
    return _push(connection, args, to_left=False)


@commands.add("LPUSH", arity=-3, is_write=True)
def lpush(connection: Connection, args: list[bytes]) -> Reply:
    """``LPUSH key element [element ...]``.

    Elements are inserted one at a time, so ``LPUSH k a b`` leaves ``b`` at the
    head - the reversal surprises people, and it is the documented behaviour.
    """
    return _push(connection, args, to_left=True)


def _push(connection: Connection, args: list[bytes], *, to_left: bool) -> int:
    store = connection.server.store
    key = args[1]
    existing = _get_list(connection, key)
    target = existing if existing is not None else deque()

    for element in args[2:]:
        if to_left:
            target.appendleft(element)
        else:
            target.append(element)

    if existing is None:
        store.set(key, target, keep_ttl=True)  # `set` touches the key for us
    else:
        store.touch(key)  # an in-place append is invisible to the store
    return len(target)


@commands.add("LLEN", arity=2)
def llen(connection: Connection, args: list[bytes]) -> Reply:
    """``LLEN key``. 0 for a missing key."""
    target = _get_list(connection, args[1])
    return 0 if target is None else len(target)


@commands.add("LRANGE", arity=4)
def lrange(connection: Connection, args: list[bytes]) -> Reply:
    """``LRANGE key start stop``. Inclusive; negative indexes count from the end."""
    target = _get_list(connection, args[1])
    if target is None:
        return []
    start, stop = normalize_range(parse_int(args[2]), parse_int(args[3]), len(target))
    if start > stop:
        return []
    # deque does not support slicing; islice avoids copying the skipped prefix.
    return list(islice(target, start, stop + 1))


@commands.add("LINDEX", arity=3)
def lindex(connection: Connection, args: list[bytes]) -> Reply:
    """``LINDEX key index``. Nil when the index is out of range."""
    target = _get_list(connection, args[1])
    if target is None:
        return None
    try:
        return target[parse_int(args[2])]
    except IndexError:
        return None


@commands.add("LSET", arity=4, is_write=True)
def lset(connection: Connection, args: list[bytes]) -> Reply:
    """``LSET key index element``."""
    target = _get_list(connection, args[1])
    if target is None:
        raise CommandError("ERR no such key")
    try:
        target[parse_int(args[2])] = args[3]
    except IndexError:
        raise CommandError("ERR index out of range") from None
    connection.server.store.touch(args[1])
    return OK


@commands.add("LPOP", arity=-2, is_write=True)
def lpop(connection: Connection, args: list[bytes]) -> Reply:
    """``LPOP key [count]``."""
    return _pop(connection, args, from_left=True)


@commands.add("RPOP", arity=-2, is_write=True)
def rpop(connection: Connection, args: list[bytes]) -> Reply:
    """``RPOP key [count]``."""
    return _pop(connection, args, from_left=False)


def _pop(connection: Connection, args: list[bytes], *, from_left: bool) -> Reply:
    """Shared body of LPOP/RPOP.

    The reply *shape* depends on whether a count was given: without one the
    reply is a bulk string or nil, with one it is an array or a null array.
    Clients distinguish the two, so the difference is preserved exactly.
    """
    if len(args) > 3:
        raise syntax_error()

    count: int | None = None
    if len(args) == 3:
        count = parse_int(args[2])
        if count < 0:
            raise CommandError("ERR value is out of range, must be positive")

    key = args[1]
    target = _get_list(connection, key)
    if target is None:
        return NULL_ARRAY if count is not None else None

    popper = target.popleft if from_left else target.pop
    taken = [popper() for _ in range(min(count if count is not None else 1, len(target)))]
    _drop_if_empty(connection, key, target)

    if count is None:
        return taken[0] if taken else None
    return taken


@commands.add("BLPOP", arity=-3, is_write=True)
async def blpop(connection: Connection, args: list[bytes]) -> Reply:
    """``BLPOP key [key ...] timeout``."""
    return await _blocking_pop(connection, args, from_left=True)


@commands.add("BRPOP", arity=-3, is_write=True)
async def brpop(connection: Connection, args: list[bytes]) -> Reply:
    """``BRPOP key [key ...] timeout``."""
    return await _blocking_pop(connection, args, from_left=False)


async def _blocking_pop(connection: Connection, args: list[bytes], *, from_left: bool) -> Reply:
    """Pop from the first non-empty key, waiting until one has an element.

    The retry loop is deliberate. Being woken only means *something* touched
    one of the keys, not that an element is still there - another blocked
    client may have taken it first. On a spurious wake-up the remaining timeout
    is recomputed from the original deadline rather than restarted, so a client
    that asked to wait one second waits one second in total.

    Propagation is rewritten to the pop that actually happened: replaying
    ``BLPOP`` on a replica would block the replication stream.

    The ``first_pass`` distinction matches Redis: a key that *already* holds a
    non-list value is an immediate ``WRONGTYPE``, but once a client is blocked
    it is only ever served by a list. A string appearing on a watched key
    leaves the client blocked rather than failing it.
    """
    keys = args[1:-1]
    timeout = parse_timeout_seconds(args[-1])
    deadline = None if timeout is None else time.monotonic() + timeout
    first_pass = True

    while True:
        for key in keys:
            target = (
                _get_list(connection, key) if first_pass else _get_list_or_none(connection, key)
            )
            if target:
                element = target.popleft() if from_left else target.pop()
                _drop_if_empty(connection, key, target)
                connection.rewrite_propagation(b"LPOP" if from_left else b"RPOP", key)
                return [key, element]
        first_pass = False

        remaining = None
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                connection.suppress_propagation()
                return NULL_ARRAY

        if not await connection.server.blocking.wait_for_any(list(keys), remaining):
            connection.suppress_propagation()
            return NULL_ARRAY
