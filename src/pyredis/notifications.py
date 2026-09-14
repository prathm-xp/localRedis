"""Consumers of the store's *touch* signal.

:class:`~pyredis.datastore.Store` announces every key it mutates. Two features
need to hear about that, and both live here because they are the same idea
applied twice: turn "this key changed" into "this client should react".

:class:`BlockingManager`
    Wakes coroutines parked in ``BLPOP`` or ``XREAD BLOCK``.

:class:`WatchRegistry`
    Marks ``WATCH``ing connections dirty so their ``EXEC`` aborts.

Keeping both out of the store means mutation sites stay ignorant of blocking
and transactions; they just call ``store.touch(key)``.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pyredis.connection import Connection

__all__ = ["BlockingManager", "WatchRegistry"]


class BlockingManager:
    """Parks coroutines until one of a set of keys is touched.

    A waiter is woken, not handed a value: after waking it must re-check the
    keyspace, because another client may have taken the element first. Callers
    therefore loop - wait, retry, wait again with the remaining timeout - which
    is also how they honour the original timeout across several wake-ups.
    """

    __slots__ = ("_waiters",)

    def __init__(self) -> None:
        self._waiters: dict[bytes, list[asyncio.Future[None]]] = {}

    def on_touch(self, key: bytes) -> None:
        """Touch listener. Wakes everything parked on ``key``."""
        waiters = self._waiters.pop(key, None)
        if not waiters:
            return
        for future in waiters:
            if not future.done():
                future.set_result(None)

    async def wait_for_any(self, keys: list[bytes], timeout_seconds: float | None) -> bool:
        """Block until one of ``keys`` is touched, or the timeout elapses.

        ``timeout_seconds`` of ``None`` waits indefinitely. Returns ``True`` if
        woken by a touch, ``False`` on timeout.
        """
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        for key in keys:
            self._waiters.setdefault(key, []).append(future)
        try:
            if timeout_seconds is None:
                await future
            else:
                await asyncio.wait_for(future, timeout_seconds)
        except TimeoutError:
            return False
        finally:
            self._discard(keys, future)
        return True

    def _discard(self, keys: list[bytes], future: asyncio.Future[None]) -> None:
        """Remove ``future`` from every key it was registered under.

        A future woken via one key is still listed under the others, and a
        cancelled future is listed under all of them.
        """
        for key in keys:
            waiters = self._waiters.get(key)
            if waiters is None:
                continue
            try:
                waiters.remove(future)
            except ValueError:
                continue
            if not waiters:
                del self._waiters[key]

    @property
    def waiting_keys(self) -> int:
        """Number of keys with at least one waiter. Used by ``INFO``."""
        return len(self._waiters)


class WatchRegistry:
    """Tracks which connections are watching which keys.

    ``WATCH`` gives ``MULTI``/``EXEC`` optimistic concurrency control: if any
    watched key is touched between ``WATCH`` and ``EXEC``, the transaction is
    abandoned. Rather than versioning keys, the registry flips a flag on the
    watching connection the moment the key is touched, which is both cheaper
    and exactly how Redis implements it.
    """

    __slots__ = ("_watchers",)

    def __init__(self) -> None:
        self._watchers: dict[bytes, set[Connection]] = {}

    def on_touch(self, key: bytes) -> None:
        """Touch listener. Marks every connection watching ``key`` as dirty."""
        for connection in self._watchers.get(key, ()):
            connection.watch_dirty = True

    def watch(self, connection: Connection, key: bytes) -> None:
        self._watchers.setdefault(key, set()).add(connection)
        connection.watched_keys.add(key)

    def unwatch_all(self, connection: Connection) -> None:
        """Drop every watch held by ``connection`` and clear its dirty flag."""
        for key in connection.watched_keys:
            watchers = self._watchers.get(key)
            if watchers is None:
                continue
            watchers.discard(connection)
            if not watchers:
                del self._watchers[key]
        connection.watched_keys.clear()
        connection.watch_dirty = False

    @property
    def watched_key_count(self) -> int:
        return len(self._watchers)
