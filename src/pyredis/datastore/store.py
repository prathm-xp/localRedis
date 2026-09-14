"""The keyspace: key -> value plus key -> expiry deadline.

Structure mirrors Redis: two dictionaries rather than one dictionary of wrapper
objects. Most keys never get a TTL, so keeping deadlines in a separate,
usually-small dict avoids paying for an extra attribute on every key.

Expiry is enforced two ways, again as Redis does:

* **Lazily**, on every access. This is what makes expiry *correct* - a key is
  never observable after its deadline, whatever the background task is doing.
* **Actively**, by a background task calling :meth:`active_expire_cycle`. This
  is what makes expiry *tidy* - memory is reclaimed for keys nobody looks at
  again. A min-heap keyed on the deadline means a cycle costs O(log n) per key
  actually removed instead of sampling blindly.

The store also broadcasts a *touch* for every key it mutates. Two subsystems
subscribe: blocking commands (``BLPOP``, ``XREAD BLOCK``) wake up, and
``WATCH``ed transactions are marked dirty. Routing both through one signal
keeps the mutation sites from having to know about either feature.
"""

from __future__ import annotations

import heapq
import time
from collections import deque
from collections.abc import Callable, Iterator
from typing import Final, TypeVar

from pyredis.datastore.objects import SortedSet, Stream
from pyredis.errors import wrong_type

__all__ = [
    "NO_EXPIRY",
    "NO_SUCH_KEY",
    "Store",
    "TouchListener",
    "current_time_ms",
    "require_type",
    "type_name_of",
]

#: Return values of :meth:`Store.pttl_ms`, matching the TTL/PTTL replies.
NO_SUCH_KEY: Final = -2
NO_EXPIRY: Final = -1

#: Called with every mutated key. Must not raise and must not block.
TouchListener = Callable[[bytes], None]

_T = TypeVar("_T")

_TYPE_NAMES: Final[dict[type, str]] = {
    bytes: "string",
    deque: "list",
    Stream: "stream",
    SortedSet: "zset",
}


def current_time_ms() -> int:
    """Wall-clock milliseconds since the epoch, the unit Redis TTLs use."""
    return time.time_ns() // 1_000_000


def type_name_of(value: object) -> str:
    """The Redis type name for a stored value (``string``, ``list``, ...)."""
    try:
        return _TYPE_NAMES[type(value)]
    except KeyError:  # pragma: no cover - guards against a new type being added
        raise AssertionError(f"unregistered value type {type(value).__name__}") from None


class Store:
    """A single logical database.

    Not thread-safe by design: the server runs one asyncio event loop, so every
    method here executes on the same thread and no locking is required. See
    ``docs/ARCHITECTURE.md`` for the reasoning behind that choice.
    """

    __slots__ = ("_clock", "_data", "_expire_heap", "_expires", "_listeners")

    def __init__(self, clock: Callable[[], int] = current_time_ms) -> None:
        self._data: dict[bytes, object] = {}
        self._expires: dict[bytes, int] = {}
        # (deadline_ms, key) min-heap. May hold stale entries for keys whose
        # deadline changed or was removed; they are filtered out on pop.
        self._expire_heap: list[tuple[int, bytes]] = []
        self._listeners: list[TouchListener] = []
        self._clock = clock

    # -- observation --------------------------------------------------------

    def __len__(self) -> int:
        """Number of live keys. Expired-but-not-yet-collected keys are excluded."""
        self._collect_expired()
        return len(self._data)

    def __contains__(self, key: bytes) -> bool:
        return self.get(key) is not None

    @property
    def clock(self) -> Callable[[], int]:
        return self._clock

    def get(self, key: bytes) -> object | None:
        """Return the value for ``key``, or ``None`` if absent or expired.

        Expired keys are removed here, which is what guarantees they are never
        visible regardless of when the active expire cycle last ran.
        """
        deadline = self._expires.get(key)
        if deadline is not None and deadline <= self._clock():
            self._remove(key)
            return None
        return self._data.get(key)

    def all_keys(self) -> list[bytes]:
        """Every live key. O(n); only ``KEYS`` and persistence use it."""
        self._collect_expired()
        return list(self._data)

    def items(self) -> Iterator[tuple[bytes, object, int | None]]:
        """Yield ``(key, value, deadline_ms)`` for every live key."""
        for key in self.all_keys():
            yield key, self._data[key], self._expires.get(key)

    def type_name(self, key: bytes) -> str:
        """Redis type name for ``key``, or ``"none"`` when it does not exist."""
        value = self.get(key)
        return "none" if value is None else type_name_of(value)

    def pttl_ms(self, key: bytes) -> int:
        """Remaining TTL in milliseconds, or :data:`NO_EXPIRY` / :data:`NO_SUCH_KEY`."""
        if self.get(key) is None:
            return NO_SUCH_KEY
        deadline = self._expires.get(key)
        if deadline is None:
            return NO_EXPIRY
        return max(deadline - self._clock(), 0)

    def expire_at_ms(self, key: bytes) -> int | None:
        """Absolute deadline for ``key``, or ``None`` if it has no TTL."""
        return self._expires.get(key)

    # -- mutation -----------------------------------------------------------

    def set(
        self,
        key: bytes,
        value: object,
        *,
        expires_at_ms: int | None = None,
        keep_ttl: bool = False,
    ) -> None:
        """Store ``value`` at ``key``.

        By default any existing TTL is discarded, which is what plain ``SET``
        does. ``keep_ttl`` preserves it (``SET ... KEEPTTL``) and
        ``expires_at_ms`` replaces it.
        """
        self._data[key] = value
        if expires_at_ms is not None:
            self._set_deadline(key, expires_at_ms)
        elif not keep_ttl:
            self._expires.pop(key, None)
        self.touch(key)

    def setdefault(self, key: bytes, factory: Callable[[], _T]) -> _T:
        """Return the value at ``key``, creating it with ``factory`` if absent.

        Does not touch the key: the caller is expected to mutate the returned
        container and touch once, so waiters see the finished state.
        """
        value = self.get(key)
        if value is None:
            value = factory()
            self._data[key] = value
        return value  # type: ignore[return-value]

    def delete(self, key: bytes) -> bool:
        """Delete ``key``. Returns ``True`` if it existed."""
        if self.get(key) is None:
            return False
        self._remove(key)
        return True

    def set_expiry_at(self, key: bytes, deadline_ms: int) -> bool:
        """Attach an absolute deadline to an existing key."""
        if self.get(key) is None:
            return False
        self._set_deadline(key, deadline_ms)
        self.touch(key)
        return True

    def persist(self, key: bytes) -> bool:
        """Remove the TTL from ``key``. Returns ``True`` if there was one."""
        if self.get(key) is None or self._expires.pop(key, None) is None:
            return False
        self.touch(key)
        return True

    def flush(self) -> None:
        """Drop every key. Touches each one so watchers and waiters react."""
        keys = list(self._data)
        self._data.clear()
        self._expires.clear()
        self._expire_heap.clear()
        for key in keys:
            self.touch(key)

    def load(self, key: bytes, value: object, deadline_ms: int | None) -> None:
        """Insert a key during RDB load, skipping already-expired entries.

        Bypasses :meth:`touch` because nothing can be watching or blocking
        before the server starts accepting connections.
        """
        if deadline_ms is not None and deadline_ms <= self._clock():
            return
        self._data[key] = value
        if deadline_ms is not None:
            self._set_deadline(key, deadline_ms)

    # -- touch notification -------------------------------------------------

    def add_touch_listener(self, listener: TouchListener) -> None:
        self._listeners.append(listener)

    def touch(self, key: bytes) -> None:
        """Announce that ``key`` changed.

        Commands that mutate a container in place (``RPUSH`` on an existing
        list, ``XADD`` on an existing stream) must call this explicitly; the
        store cannot see those mutations.
        """
        for listener in self._listeners:
            listener(key)

    # -- expiry -------------------------------------------------------------

    def active_expire_cycle(self, limit: int = 200) -> int:
        """Remove up to ``limit`` keys whose deadline has passed.

        Called on a timer by the server. Returns the number of keys removed so
        the caller can decide whether another cycle is worthwhile.
        """
        now = self._clock()
        removed = 0
        heap = self._expire_heap
        while heap and removed < limit:
            deadline, key = heap[0]
            if deadline > now:
                break
            heapq.heappop(heap)
            # Skip stale heap entries: the key's deadline moved or vanished.
            if self._expires.get(key) == deadline:
                self._remove(key)
                removed += 1
        self._compact_expire_heap()
        return removed

    def _collect_expired(self) -> None:
        """Remove every key that is already due, so ``all_keys``/``len`` never lie.

        Draining the deadline heap costs O(k log n) for the k keys actually
        expired; scanning the deadline dict would be O(m) over every key that
        has a TTL, expired or not. The limit can never be reached - there are at
        most ``len(_expires)`` real removals - so this always drains fully.
        """
        self.active_expire_cycle(limit=len(self._expires) + 1)

    def _set_deadline(self, key: bytes, deadline_ms: int) -> None:
        self._expires[key] = deadline_ms
        heapq.heappush(self._expire_heap, (deadline_ms, key))
        self._compact_expire_heap()

    def _remove(self, key: bytes) -> None:
        """Delete a key and its deadline.

        Any matching heap entry is left behind and filtered out when it
        surfaces; searching the heap here would make deletion O(n).
        """
        self._data.pop(key, None)
        self._expires.pop(key, None)
        self.touch(key)

    def _compact_expire_heap(self) -> None:
        """Rebuild the heap from the deadline dict when stale entries pile up.

        Rebuilding from ``_expires`` rather than filtering the heap in place is
        what makes this correct: repeatedly re-setting the same TTL pushes
        identical ``(deadline, key)`` tuples that a filter cannot tell apart.
        The threshold leaves room for at least ``len(_expires) + 64`` further
        pushes, so the rebuild cost is amortized O(1) per push.
        """
        if len(self._expire_heap) > 2 * len(self._expires) + 64:
            self._expire_heap = [(deadline, key) for key, deadline in self._expires.items()]
            heapq.heapify(self._expire_heap)


def require_type(value: object, expected: type[_T]) -> _T:
    """Narrow ``value`` to ``expected`` or raise the Redis ``WRONGTYPE`` error.

    ``None`` is passed through unchanged by the callers that allow a missing
    key, so this is only reached for values that really exist.
    """
    if not isinstance(value, expected):
        raise wrong_type()
    return value
