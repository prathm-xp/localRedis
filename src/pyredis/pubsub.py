"""Publish/subscribe routing.

Pub/Sub is independent of the keyspace: messages are not stored, and a message
published with no subscribers is simply dropped. The registry here maps
channels and patterns to connections; per-connection bookkeeping (which
channels *this* client holds, needed for the subscription counts in replies)
lives on :class:`~pyredis.connection.Connection`.

Delivery order matters for compatibility: Redis delivers to exact-channel
subscribers first, then to pattern subscribers, and a client subscribed both
ways receives the message twice.

Subscribers are held in insertion-ordered dicts rather than sets, so delivery
order is deterministic (oldest subscriber first) without sorting on every
message - which would have cost O(n log n) per publish.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyredis.globmatch import glob_match
from pyredis.protocol import encode

if TYPE_CHECKING:  # pragma: no cover
    from pyredis.connection import Connection

__all__ = ["PubSub"]


class PubSub:
    """Channel and pattern subscription registry."""

    __slots__ = ("_channels", "_patterns")

    def __init__(self) -> None:
        # The inner dicts are ordered sets: the values are always None.
        self._channels: dict[bytes, dict[Connection, None]] = {}
        self._patterns: dict[bytes, dict[Connection, None]] = {}

    # -- subscription management -------------------------------------------

    def subscribe(self, connection: Connection, channel: bytes) -> None:
        self._channels.setdefault(channel, {})[connection] = None
        connection.subscribed_channels.add(channel)

    def unsubscribe(self, connection: Connection, channel: bytes) -> None:
        subscribers = self._channels.get(channel)
        if subscribers is not None:
            subscribers.pop(connection, None)
            if not subscribers:
                del self._channels[channel]
        connection.subscribed_channels.discard(channel)

    def psubscribe(self, connection: Connection, pattern: bytes) -> None:
        self._patterns.setdefault(pattern, {})[connection] = None
        connection.subscribed_patterns.add(pattern)

    def punsubscribe(self, connection: Connection, pattern: bytes) -> None:
        subscribers = self._patterns.get(pattern)
        if subscribers is not None:
            subscribers.pop(connection, None)
            if not subscribers:
                del self._patterns[pattern]
        connection.subscribed_patterns.discard(pattern)

    def remove_connection(self, connection: Connection) -> None:
        """Drop every subscription held by a disconnecting client."""
        for channel in list(connection.subscribed_channels):
            self.unsubscribe(connection, channel)
        for pattern in list(connection.subscribed_patterns):
            self.punsubscribe(connection, pattern)

    # -- delivery -----------------------------------------------------------

    def publish(self, channel: bytes, message: bytes) -> int:
        """Deliver ``message`` and return the number of clients it reached.

        A client subscribed to the channel directly *and* via a matching
        pattern is counted - and delivered to - twice, as in Redis.
        """
        delivered = 0

        payload = encode([b"message", channel, message])
        for connection in list(self._channels.get(channel, ())):
            connection.send(payload)
            delivered += 1

        for pattern, subscribers in self._patterns.items():
            if not glob_match(pattern, channel):
                continue
            pattern_payload = encode([b"pmessage", pattern, channel, message])
            for connection in list(subscribers):
                connection.send(pattern_payload)
                delivered += 1

        return delivered

    # -- introspection ------------------------------------------------------

    def active_channels(self) -> list[bytes]:
        """Channels with at least one subscriber (``PUBSUB CHANNELS``)."""
        return list(self._channels)

    def subscriber_count(self, channel: bytes) -> int:
        return len(self._channels.get(channel, ()))

    @property
    def pattern_count(self) -> int:
        """Total number of pattern subscriptions (``PUBSUB NUMPAT``)."""
        return len(self._patterns)
