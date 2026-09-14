"""Value types held in the keyspace.

Two of the four supported types are plain Python objects:

* **string** -> ``bytes``
* **list**   -> ``collections.deque[bytes]`` (O(1) push/pop at both ends)

The other two need real structure and live here:

* **stream** -> :class:`Stream`
* **zset**   -> :class:`SortedSet`

Both are pure data structures: they validate their own invariants and raise
:class:`~pyredis.errors.CommandError` with the exact Redis message when those
invariants are violated, but they know nothing about connections, the wire
protocol, or expiry.
"""

from __future__ import annotations

import bisect
from collections import deque
from typing import Final, NamedTuple

from pyredis.errors import CommandError

__all__ = [
    "MAX_SEQUENCE",
    "MAX_STREAM_ID",
    "MIN_STREAM_ID",
    "RedisList",
    "SortedSet",
    "Stream",
    "StreamEntry",
    "StreamId",
]

#: Stream IDs are a pair of unsigned 64-bit integers.
MAX_SEQUENCE: Final = 2**64 - 1

RedisList = deque[bytes]


class StreamId(NamedTuple):
    """A stream entry ID: milliseconds since the epoch plus a sequence number.

    Being a ``NamedTuple`` gives correct ordering for free - tuple comparison is
    exactly the ``(ms, seq)`` ordering streams require.
    """

    ms: int
    seq: int

    def __str__(self) -> str:
        return f"{self.ms}-{self.seq}"

    def encode(self) -> bytes:
        return b"%d-%d" % (self.ms, self.seq)

    def next(self) -> StreamId:
        """The smallest ID strictly greater than this one."""
        if self.seq < MAX_SEQUENCE:
            return StreamId(self.ms, self.seq + 1)
        return StreamId(self.ms + 1, 0)


MIN_STREAM_ID: Final = StreamId(0, 0)
MAX_STREAM_ID: Final = StreamId(MAX_SEQUENCE, MAX_SEQUENCE)


class StreamEntry(NamedTuple):
    """One stream entry: an ID and a flat field/value list, as Redis stores it."""

    id: StreamId
    fields: list[bytes]


def parse_stream_id(raw: bytes, *, default_seq: int) -> StreamId:
    """Parse an explicit ``ms`` or ``ms-seq`` ID.

    ``default_seq`` supplies the sequence number when the caller omits it,
    which is how ``XRANGE`` widens a partial ID: the start bound defaults to
    sequence 0 and the end bound to the maximum sequence.

    Raises:
        CommandError: if the ID is not well formed.
    """
    try:
        if b"-" in raw:
            ms_part, _, seq_part = raw.partition(b"-")
            stream_id = StreamId(int(ms_part), int(seq_part))
        else:
            stream_id = StreamId(int(raw), default_seq)
    except ValueError:
        raise CommandError("ERR Invalid stream ID specified as stream command argument") from None

    if stream_id.ms < 0 or stream_id.seq < 0:
        raise CommandError("ERR Invalid stream ID specified as stream command argument")
    return stream_id


class Stream:
    """An append-only log of entries with strictly increasing IDs.

    Entries are stored in a list that is append-ordered by construction, so
    range queries use binary search rather than a scan.
    """

    __slots__ = ("_entries", "_last_id")

    def __init__(self) -> None:
        self._entries: list[StreamEntry] = []
        self._last_id = MIN_STREAM_ID

    def __len__(self) -> int:
        return len(self._entries)

    @property
    def last_id(self) -> StreamId:
        """The highest ID ever added, which is ``0-0`` for an empty stream."""
        return self._last_id

    @property
    def entries(self) -> list[StreamEntry]:
        return self._entries

    def append(self, stream_id: StreamId, fields: list[bytes]) -> StreamId:
        """Append an entry, enforcing the monotonic-ID invariant.

        Raises:
            CommandError: if the ID is ``0-0`` or not greater than the last ID.
        """
        if stream_id <= MIN_STREAM_ID:
            raise CommandError("ERR The ID specified in XADD must be greater than 0-0")
        if self._entries and stream_id <= self._last_id:
            raise CommandError(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            )
        self._entries.append(StreamEntry(stream_id, fields))
        self._last_id = stream_id
        return stream_id

    def next_auto_id(self, now_ms: int) -> StreamId:
        """The ID for a fully auto-generated (``*``) entry."""
        if now_ms > self._last_id.ms:
            return StreamId(now_ms, 0)
        # Clock is behind or equal to the last entry: keep the timestamp and
        # bump the sequence so IDs stay strictly increasing.
        return self._last_id.next()

    def next_sequence_for(self, ms: int) -> StreamId:
        """The ID for a partially auto-generated (``<ms>-*``) entry."""
        if self._entries and ms == self._last_id.ms:
            if self._last_id.seq >= MAX_SEQUENCE:
                raise CommandError(
                    "ERR The ID specified in XADD is equal or smaller than "
                    "the target stream top item"
                )
            return StreamId(ms, self._last_id.seq + 1)
        # Redis seeds sequence 1 for the 0-* case because 0-0 is reserved.
        return StreamId(ms, 1 if ms == 0 else 0)

    def range(self, start: StreamId, end: StreamId) -> list[StreamEntry]:
        """Entries with ``start <= id <= end``, in ascending ID order."""
        if start > end:
            return []
        low = bisect.bisect_left(self._entries, start, key=lambda entry: entry.id)
        high = bisect.bisect_right(self._entries, end, key=lambda entry: entry.id)
        return self._entries[low:high]

    def after(self, exclusive_start: StreamId, count: int | None = None) -> list[StreamEntry]:
        """Entries with ``id > exclusive_start``, optionally capped at ``count``."""
        low = bisect.bisect_right(self._entries, exclusive_start, key=lambda entry: entry.id)
        selected = self._entries[low:]
        if count is not None:
            selected = selected[:count]
        return selected


class SortedSet:
    """A set of members ordered by ``(score, member)``.

    Backed by a dict for O(1) score lookup plus a sorted list of
    ``(score, member)`` pairs for ordered access. Lookups and rank queries are
    O(log n) via binary search; insertion and removal are O(n) because of the
    list shift. Real Redis uses a skip list to make those O(log n) as well -
    see ``docs/ARCHITECTURE.md`` for why the simpler structure was chosen here.
    """

    __slots__ = ("_ordered", "_scores")

    def __init__(self) -> None:
        self._scores: dict[bytes, float] = {}
        self._ordered: list[tuple[float, bytes]] = []

    def __len__(self) -> int:
        return len(self._scores)

    def __contains__(self, member: bytes) -> bool:
        return member in self._scores

    def add(self, member: bytes, score: float) -> bool:
        """Insert or update ``member``. Returns ``True`` if it was newly added."""
        existing = self._scores.get(member)
        if existing is not None:
            if existing == score:
                return False
            self._ordered.pop(bisect.bisect_left(self._ordered, (existing, member)))
        self._scores[member] = score
        bisect.insort(self._ordered, (score, member))
        return existing is None

    def remove(self, member: bytes) -> bool:
        """Remove ``member``. Returns ``True`` if it was present."""
        score = self._scores.pop(member, None)
        if score is None:
            return False
        self._ordered.pop(bisect.bisect_left(self._ordered, (score, member)))
        return True

    def score(self, member: bytes) -> float | None:
        return self._scores.get(member)

    def rank(self, member: bytes) -> int | None:
        """Zero-based index of ``member`` in ascending order, or ``None``."""
        score = self._scores.get(member)
        if score is None:
            return None
        return bisect.bisect_left(self._ordered, (score, member))

    def range(self, start: int, stop: int) -> list[tuple[float, bytes]]:
        """Members in ``[start, stop]`` using Redis's inclusive, negative-aware indexes."""
        start, stop = normalize_range(start, stop, len(self._ordered))
        if start > stop:
            return []
        return self._ordered[start : stop + 1]

    def items(self) -> list[tuple[float, bytes]]:
        """All ``(score, member)`` pairs in ascending order."""
        return list(self._ordered)


def normalize_range(start: int, stop: int, length: int) -> tuple[int, int]:
    """Convert Redis's inclusive, possibly-negative range to list indexes.

    Negative indexes count back from the end; out-of-range bounds are clamped.
    The returned pair is inclusive, and ``start > stop`` means "empty".
    """
    if start < 0:
        start = max(length + start, 0)
    if stop < 0:
        stop = length + stop
    stop = min(stop, length - 1)
    return start, stop
