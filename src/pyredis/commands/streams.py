"""Stream commands: XADD, XLEN, XRANGE, XREVRANGE and XREAD.

Streams are the one data type here with a non-obvious identity model. Every
entry has an ID of ``<milliseconds>-<sequence>`` that must be strictly greater
than the previous one, which is what lets a consumer say "everything after this
point" without any server-side cursor. ``XADD`` therefore has three ID forms -
fully explicit, ``<ms>-*`` and ``*`` - and the interesting cases are the ones
where the clock does not cooperate: two entries in the same millisecond, or a
clock that went backwards. :class:`~pyredis.datastore.Stream` handles both by
falling back to incrementing the sequence.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from pyredis.commands.base import CommandSet
from pyredis.commands.parsing import parse_int
from pyredis.datastore import (
    MAX_SEQUENCE,
    MAX_STREAM_ID,
    MIN_STREAM_ID,
    Stream,
    StreamEntry,
    StreamId,
    parse_stream_id,
    require_type,
)
from pyredis.errors import CommandError, syntax_error, wrong_number_of_arguments
from pyredis.protocol import NULL_ARRAY, Reply

if TYPE_CHECKING:  # pragma: no cover
    from pyredis.connection import Connection

commands = CommandSet()

__all__ = ["commands"]

#: ``XREAD``'s special "everything added from now on" ID.
_LAST_ID = b"$"


def _get_stream(connection: Connection, key: bytes) -> Stream | None:
    """Fetch a stream, raising ``WRONGTYPE`` if the key holds something else."""
    value = connection.server.store.get(key)
    if value is None:
        return None
    return require_type(value, Stream)


def _get_stream_or_none(connection: Connection, key: bytes) -> Stream | None:
    """Like :func:`_get_stream`, but treats a wrong-typed value as absent.

    Used by ``XREAD`` after it has blocked: as with ``BLPOP``, a client that is
    already waiting is only ever served by a value of the right type.
    """
    value = connection.server.store.get(key)
    return value if isinstance(value, Stream) else None


def _render(entry: StreamEntry) -> list[Reply]:
    return [entry.id.encode(), list(entry.fields)]


def _render_all(entries: list[StreamEntry]) -> list[Reply]:
    return [_render(entry) for entry in entries]


@commands.add("XADD", arity=-5, is_write=True)
def xadd(connection: Connection, args: list[bytes]) -> Reply:
    """``XADD key <*|id> field value [field value ...]``.

    Propagated with the generated ID made explicit. Sending ``*`` to a replica
    would have it generate an ID from its own clock and its own last entry, and
    the two copies would diverge on the very first entry.
    """
    store = connection.server.store
    key, raw_id = args[1], args[2]
    fields = args[3:]
    if len(fields) % 2 != 0:
        raise wrong_number_of_arguments("xadd")

    stream = _get_stream(connection, key)
    created = stream is None
    if stream is None:
        stream = Stream()

    entry_id = _resolve_add_id(stream, raw_id, connection)
    stream.append(entry_id, list(fields))

    if created:
        store.set(key, stream, keep_ttl=True)
    else:
        store.touch(key)

    connection.rewrite_propagation(b"XADD", key, entry_id.encode(), *fields)
    return entry_id.encode()


def _resolve_add_id(stream: Stream, raw_id: bytes, connection: Connection) -> StreamId:
    """Turn ``*``, ``<ms>-*`` or an explicit ID into a concrete :class:`StreamId`."""
    if raw_id == b"*":
        return stream.next_auto_id(connection.server.store.clock())
    if raw_id.endswith(b"-*"):
        try:
            milliseconds = int(raw_id[:-2])
        except ValueError:
            raise CommandError(
                "ERR Invalid stream ID specified as stream command argument"
            ) from None
        if milliseconds < 0:
            raise CommandError("ERR Invalid stream ID specified as stream command argument")
        return stream.next_sequence_for(milliseconds)
    return parse_stream_id(raw_id, default_seq=0)


@commands.add("XLEN", arity=2)
def xlen(connection: Connection, args: list[bytes]) -> Reply:
    """``XLEN key``. 0 for a missing key."""
    stream = _get_stream(connection, args[1])
    return 0 if stream is None else len(stream)


@commands.add("XRANGE", arity=-4)
def xrange(connection: Connection, args: list[bytes]) -> Reply:
    """``XRANGE key start end [COUNT n]``.

    ``-`` and ``+`` are the open bounds, a bare ``5`` widens to ``5-0`` at the
    start and ``5-<max>`` at the end, and a ``(`` prefix makes a bound
    exclusive.
    """
    return _range(connection, args, reverse=False)


@commands.add("XREVRANGE", arity=-4)
def xrevrange(connection: Connection, args: list[bytes]) -> Reply:
    """``XREVRANGE key end start [COUNT n]``. Same as ``XRANGE`` reversed."""
    return _range(connection, args, reverse=True)


def _range(connection: Connection, args: list[bytes], *, reverse: bool) -> Reply:
    count = _parse_count(args, first_option=4)
    low_arg, high_arg = (args[3], args[2]) if reverse else (args[2], args[3])

    start = _parse_range_bound(low_arg, is_start=True)
    end = _parse_range_bound(high_arg, is_start=False)

    stream = _get_stream(connection, args[1])
    if stream is None:
        return []

    entries = stream.range(start, end)
    if reverse:
        entries = entries[::-1]
    if count is not None:
        entries = entries[:count]
    return _render_all(entries)


def _parse_range_bound(raw: bytes, *, is_start: bool) -> StreamId:
    """Parse one bound of an ``XRANGE``.

    An exclusive bound is expressed by nudging it one ID inwards, which keeps
    the range query itself inclusive and simple.
    """
    exclusive = raw.startswith(b"(")
    if exclusive:
        raw = raw[1:]

    if raw == b"-":
        bound = MIN_STREAM_ID
    elif raw == b"+":
        bound = MAX_STREAM_ID
    else:
        bound = parse_stream_id(raw, default_seq=0 if is_start else MAX_SEQUENCE)

    if not exclusive:
        return bound
    if is_start:
        return bound.next()
    if bound.seq > 0:
        return StreamId(bound.ms, bound.seq - 1)
    if bound.ms == 0:
        raise CommandError("ERR invalid range item")
    return StreamId(bound.ms - 1, MAX_SEQUENCE)


def _parse_count(args: list[bytes], *, first_option: int) -> int | None:
    """Parse a trailing ``COUNT n``, the only option these commands accept."""
    extra = args[first_option:]
    if not extra:
        return None
    if len(extra) == 2 and extra[0].upper() == b"COUNT":
        return max(parse_int(extra[1]), 0)
    raise syntax_error()


@commands.add("XREAD", arity=-4)
async def xread(connection: Connection, args: list[bytes]) -> Reply:
    """``XREAD [COUNT n] [BLOCK ms] STREAMS key [key ...] id [id ...]``.

    ``BLOCK 0`` waits indefinitely. The ``$`` ID means "entries added after this
    call started", which is resolved to each stream's current last ID *before*
    waiting - resolving it afterwards would silently drop anything that arrived
    in between.
    """
    count, block_ms, keys, raw_ids = _parse_xread(args)

    start_ids = [
        _resolve_read_id(connection, key, raw) for key, raw in zip(keys, raw_ids, strict=True)
    ]

    result = _collect_new_entries(connection, keys, start_ids, count, strict=True)
    if result or block_ms is None:
        return result or NULL_ARRAY

    deadline = None if block_ms == 0 else time.monotonic() + block_ms / 1000
    while True:
        remaining = None
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return NULL_ARRAY

        if not await connection.server.blocking.wait_for_any(list(keys), remaining):
            return NULL_ARRAY

        result = _collect_new_entries(connection, keys, start_ids, count, strict=False)
        if result:
            return result


def _parse_xread(args: list[bytes]) -> tuple[int | None, int | None, list[bytes], list[bytes]]:
    count: int | None = None
    block_ms: int | None = None
    index = 1

    while index < len(args):
        option = args[index].upper()
        if option == b"COUNT" and index + 1 < len(args):
            count = max(parse_int(args[index + 1]), 0)
            index += 2
        elif option == b"BLOCK" and index + 1 < len(args):
            block_ms = parse_int(args[index + 1])
            if block_ms < 0:
                raise CommandError("ERR timeout is negative")
            index += 2
        elif option == b"STREAMS":
            index += 1
            break
        else:
            raise syntax_error()
    else:
        raise CommandError(
            "ERR syntax error, STREAMS option must be specified and provide the same "
            "number of keys and IDs"
        )

    rest = args[index:]
    if not rest or len(rest) % 2 != 0:
        raise CommandError(
            "ERR Unbalanced XREAD list of streams: for each stream key an ID or "
            "'$' must be specified."
        )
    half = len(rest) // 2
    return count, block_ms, rest[:half], rest[half:]


def _resolve_read_id(connection: Connection, key: bytes, raw: bytes) -> StreamId:
    if raw == _LAST_ID:
        stream = _get_stream(connection, key)
        return stream.last_id if stream is not None else MIN_STREAM_ID
    return parse_stream_id(raw, default_seq=0)


def _collect_new_entries(
    connection: Connection,
    keys: list[bytes],
    start_ids: list[StreamId],
    count: int | None,
    *,
    strict: bool,
) -> list[Reply]:
    """Entries strictly after each stream's start ID, skipping streams with none.

    ``strict`` selects the wrong-type behaviour: an error on the first attempt,
    silently skipped once the client is blocked.
    """
    lookup = _get_stream if strict else _get_stream_or_none
    out: list[Reply] = []
    for key, start in zip(keys, start_ids, strict=True):
        stream = lookup(connection, key)
        if stream is None:
            continue
        entries = stream.after(start, count)
        if entries:
            out.append([key, _render_all(entries)])
    return out
