"""Publish/Subscribe commands.

The subscribe family is unusual in two ways, both of which follow from RESP2
having no dedicated push type. First, ``SUBSCRIBE a b c`` sends *three* replies,
one per channel, so these handlers write their own output and return
``NO_REPLY``. Second, a subscribed connection enters a restricted mode where
only a handful of commands are legal - otherwise a client could not tell a
reply to ``GET`` apart from a message pushed by another client.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyredis.commands.base import CommandSet
from pyredis.errors import CommandError
from pyredis.protocol import NO_REPLY, Reply, encode

if TYPE_CHECKING:  # pragma: no cover
    from pyredis.connection import Connection

commands = CommandSet()

__all__ = ["commands"]


@commands.add("SUBSCRIBE", arity=-2, allowed_in_subscribe_mode=True, allowed_in_multi=False)
def subscribe(connection: Connection, args: list[bytes]) -> Reply:
    """``SUBSCRIBE channel [channel ...]``."""
    for channel in args[1:]:
        connection.server.pubsub.subscribe(connection, channel)
        connection.send(encode([b"subscribe", channel, connection.subscription_count]))
    return NO_REPLY


@commands.add("UNSUBSCRIBE", arity=-1, allowed_in_subscribe_mode=True, allowed_in_multi=False)
def unsubscribe(connection: Connection, args: list[bytes]) -> Reply:
    """``UNSUBSCRIBE [channel ...]``. With no channel, unsubscribes from all."""
    channels = args[1:] or sorted(connection.subscribed_channels)
    if not channels:
        # Redis still acknowledges, with a nil channel and the current count.
        connection.send(encode([b"unsubscribe", None, connection.subscription_count]))
        return NO_REPLY

    for channel in channels:
        connection.server.pubsub.unsubscribe(connection, channel)
        connection.send(encode([b"unsubscribe", channel, connection.subscription_count]))
    return NO_REPLY


@commands.add("PSUBSCRIBE", arity=-2, allowed_in_subscribe_mode=True, allowed_in_multi=False)
def psubscribe(connection: Connection, args: list[bytes]) -> Reply:
    """``PSUBSCRIBE pattern [pattern ...]``. Patterns use the ``KEYS`` glob syntax."""
    for pattern in args[1:]:
        connection.server.pubsub.psubscribe(connection, pattern)
        connection.send(encode([b"psubscribe", pattern, connection.subscription_count]))
    return NO_REPLY


@commands.add("PUNSUBSCRIBE", arity=-1, allowed_in_subscribe_mode=True, allowed_in_multi=False)
def punsubscribe(connection: Connection, args: list[bytes]) -> Reply:
    """``PUNSUBSCRIBE [pattern ...]``. With no pattern, unsubscribes from all."""
    patterns = args[1:] or sorted(connection.subscribed_patterns)
    if not patterns:
        connection.send(encode([b"punsubscribe", None, connection.subscription_count]))
        return NO_REPLY

    for pattern in patterns:
        connection.server.pubsub.punsubscribe(connection, pattern)
        connection.send(encode([b"punsubscribe", pattern, connection.subscription_count]))
    return NO_REPLY


@commands.add("PUBLISH", arity=3)
def publish(connection: Connection, args: list[bytes]) -> Reply:
    """``PUBLISH channel message``. Returns the number of clients reached.

    Not a write command: messages are never stored, so there is nothing for a
    replica to apply. (Real Redis does forward publishes across a replication
    link; this server does not - see ``docs/LIMITATIONS.md``.)
    """
    return connection.server.pubsub.publish(args[1], args[2])


@commands.add("PUBSUB", arity=-2)
def pubsub(connection: Connection, args: list[bytes]) -> Reply:
    """``PUBSUB CHANNELS [pattern] | NUMSUB [channel ...] | NUMPAT``."""
    from pyredis.globmatch import glob_match

    registry = connection.server.pubsub
    subcommand = args[1].upper()

    if subcommand == b"CHANNELS" and len(args) <= 3:
        channels = registry.active_channels()
        if len(args) == 3:
            channels = [c for c in channels if glob_match(args[2], c)]
        return channels

    if subcommand == b"NUMSUB":
        out: list[Reply] = []
        for channel in args[2:]:
            out.append(channel)
            out.append(registry.subscriber_count(channel))
        return out

    if subcommand == b"NUMPAT" and len(args) == 2:
        return registry.pattern_count

    raise CommandError(
        f"ERR Unknown PUBSUB subcommand or wrong number of arguments for "
        f"'{args[1].decode(errors='replace')}'"
    )
