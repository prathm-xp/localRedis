"""Sorted-set commands: ZADD, ZSCORE, ZRANK, ZRANGE, ZCARD, ZREM, ZINCRBY."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyredis.commands.base import CommandSet
from pyredis.commands.parsing import parse_float, parse_int
from pyredis.datastore import SortedSet, normalize_range, require_type
from pyredis.errors import CommandError, syntax_error
from pyredis.protocol import Reply

if TYPE_CHECKING:  # pragma: no cover
    from pyredis.connection import Connection

commands = CommandSet()

__all__ = ["commands"]


def _get_zset(connection: Connection, key: bytes) -> SortedSet | None:
    value = connection.server.store.get(key)
    if value is None:
        return None
    return require_type(value, SortedSet)


def format_score(score: float) -> bytes:
    """Render a score the way Redis does.

    Redis prints scores with ``%.17g`` and strips a trailing ``.0``, so a score
    of 1.0 comes back as ``"1"`` rather than ``"1.0"``. Clients that compare
    score strings depend on it.
    """
    if score == float("inf"):
        return b"inf"
    if score == float("-inf"):
        return b"-inf"
    if score.is_integer() and abs(score) < 2**53:
        return b"%d" % int(score)
    return repr(score).encode()


@commands.add("ZADD", arity=-4, is_write=True)
def zadd(connection: Connection, args: list[bytes]) -> Reply:
    """``ZADD key [NX|XX] [CH] score member [score member ...]``.

    Returns the number of members added, or - with ``CH`` - the number added or
    changed.
    """
    store = connection.server.store
    key = args[1]

    index = 2
    only_if_absent = only_if_present = report_changed = False
    while index < len(args):
        option = args[index].upper()
        if option == b"NX":
            only_if_absent = True
        elif option == b"XX":
            only_if_present = True
        elif option == b"CH":
            report_changed = True
        else:
            break
        index += 1
    if only_if_absent and only_if_present:
        raise CommandError("ERR XX and NX options at the same time are not compatible")

    pairs = args[index:]
    if not pairs or len(pairs) % 2 != 0:
        raise syntax_error()

    # Parse every score before mutating anything: ZADD is all-or-nothing, so a
    # bad score in the middle must not leave the first members applied.
    scored = [(parse_float(pairs[i]), pairs[i + 1]) for i in range(0, len(pairs), 2)]

    zset = _get_zset(connection, key)
    created = zset is None
    if zset is None:
        zset = SortedSet()

    added = changed = 0
    for score, member in scored:
        exists = member in zset
        if (only_if_absent and exists) or (only_if_present and not exists):
            continue
        previous = zset.score(member)
        if zset.add(member, score):
            added += 1
        elif previous != score:
            changed += 1

    if created:
        if len(zset) == 0:
            # NX/XX filtered everything out; do not leave an empty key behind.
            return 0
        store.set(key, zset, keep_ttl=True)
    elif added or changed:
        # Only signal a real change: a no-op ZADD should not wake waiters.
        store.touch(key)

    return added + changed if report_changed else added


@commands.add("ZCARD", arity=2)
def zcard(connection: Connection, args: list[bytes]) -> Reply:
    """``ZCARD key``. 0 for a missing key."""
    zset = _get_zset(connection, args[1])
    return 0 if zset is None else len(zset)


@commands.add("ZSCORE", arity=3)
def zscore(connection: Connection, args: list[bytes]) -> Reply:
    """``ZSCORE key member``. Nil when the member or the key is missing."""
    zset = _get_zset(connection, args[1])
    if zset is None:
        return None
    score = zset.score(args[2])
    return None if score is None else format_score(score)


@commands.add("ZRANK", arity=3)
def zrank(connection: Connection, args: list[bytes]) -> Reply:
    """``ZRANK key member``. Zero-based index in ascending score order."""
    return _rank(connection, args, reverse=False)


@commands.add("ZREVRANK", arity=3)
def zrevrank(connection: Connection, args: list[bytes]) -> Reply:
    """``ZREVRANK key member``. Zero-based index in descending score order."""
    return _rank(connection, args, reverse=True)


def _rank(connection: Connection, args: list[bytes], *, reverse: bool) -> Reply:
    zset = _get_zset(connection, args[1])
    if zset is None:
        return None
    rank = zset.rank(args[2])
    if rank is None:
        return None
    return len(zset) - 1 - rank if reverse else rank


@commands.add("ZRANGE", arity=-4)
def zrange(connection: Connection, args: list[bytes]) -> Reply:
    """``ZRANGE key start stop [WITHSCORES]``.

    Index-based only. The ``BYSCORE``/``BYLEX``/``REV``/``LIMIT`` forms added in
    Redis 6.2 are not implemented; see ``docs/LIMITATIONS.md``.
    """
    with_scores = _parse_withscores(args, first_option=4)
    zset = _get_zset(connection, args[1])
    if zset is None:
        return []
    selected = zset.range(parse_int(args[2]), parse_int(args[3]))
    return _render(selected, with_scores)


@commands.add("ZREVRANGE", arity=-4)
def zrevrange(connection: Connection, args: list[bytes]) -> Reply:
    """``ZREVRANGE key start stop [WITHSCORES]``. Indexes count from the top."""
    with_scores = _parse_withscores(args, first_option=4)
    zset = _get_zset(connection, args[1])
    if zset is None:
        return []
    descending = zset.items()[::-1]
    start, stop = normalize_range(parse_int(args[2]), parse_int(args[3]), len(descending))
    if start > stop:
        return []
    return _render(descending[start : stop + 1], with_scores)


def _parse_withscores(args: list[bytes], *, first_option: int) -> bool:
    extra = args[first_option:]
    if not extra:
        return False
    if len(extra) == 1 and extra[0].upper() == b"WITHSCORES":
        return True
    raise syntax_error()


def _render(items: list[tuple[float, bytes]], with_scores: bool) -> list[Reply]:
    if not with_scores:
        return [member for _, member in items]
    out: list[Reply] = []
    for score, member in items:
        out.append(member)
        out.append(format_score(score))
    return out


@commands.add("ZREM", arity=-3, is_write=True)
def zrem(connection: Connection, args: list[bytes]) -> Reply:
    """``ZREM key member [member ...]``. Returns how many were removed."""
    store = connection.server.store
    key = args[1]
    zset = _get_zset(connection, key)
    if zset is None:
        return 0

    removed = sum(1 for member in args[2:] if zset.remove(member))
    if len(zset) == 0:
        store.delete(key)
    elif removed:
        store.touch(key)
    return removed


@commands.add("ZINCRBY", arity=4, is_write=True)
def zincrby(connection: Connection, args: list[bytes]) -> Reply:
    """``ZINCRBY key increment member``. Returns the new score."""
    store = connection.server.store
    key, member = args[1], args[3]
    delta = parse_float(args[2])

    zset = _get_zset(connection, key)
    created = zset is None
    if zset is None:
        zset = SortedSet()

    current = zset.score(member)
    updated = (current if current is not None else 0.0) + delta
    if updated != updated:  # NaN, from adding +inf to -inf
        raise CommandError("ERR resulting score is not a number (NaN)")

    zset.add(member, updated)
    if created:
        store.set(key, zset, keep_ttl=True)
    else:
        store.touch(key)
    return format_score(updated)
