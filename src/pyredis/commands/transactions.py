"""Transactions: MULTI, EXEC, DISCARD, WATCH, UNWATCH.

A Redis transaction is not a rollback-capable transaction. ``MULTI`` starts
queueing, ``EXEC`` runs the queue, and because the server is single-threaded
and nothing awaits in the middle, the queue runs with nothing interleaved. What
you do *not* get is atomic failure: if the third command fails, the first two
have already happened and stay happened.

There are two distinct failure modes, and Redis treats them differently:

* A command that cannot even be queued - unknown name, wrong arity - poisons
  the transaction. ``EXEC`` then refuses with ``EXECABORT`` and runs nothing.
* A command that queues fine but fails at run time - ``INCR`` on a string, say -
  produces an error *inside* the ``EXEC`` reply array. The rest still runs.

``WATCH`` adds optimistic concurrency control: if any watched key is touched
between ``WATCH`` and ``EXEC``, ``EXEC`` returns nil and runs nothing. The
watching is done by :class:`~pyredis.notifications.WatchRegistry`, which flips
a flag on this connection the moment the key changes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyredis.commands.base import CommandSet
from pyredis.errors import CommandError
from pyredis.protocol import NULL_ARRAY, OK, Reply

if TYPE_CHECKING:  # pragma: no cover
    from pyredis.connection import Connection

commands = CommandSet()

__all__ = ["commands"]


@commands.add("MULTI", arity=1, queued_in_multi=False)
def multi(connection: Connection, args: list[bytes]) -> Reply:
    """``MULTI``. Start queueing commands. Transactions do not nest."""
    if connection.in_multi:
        raise CommandError("ERR MULTI calls can not be nested")
    connection.in_multi = True
    connection.queued_commands.clear()
    connection.multi_aborted = False
    return OK


@commands.add("DISCARD", arity=1, queued_in_multi=False)
def discard(connection: Connection, args: list[bytes]) -> Reply:
    """``DISCARD``. Abandon the queue and leave transaction mode."""
    if not connection.in_multi:
        raise CommandError("ERR DISCARD without MULTI")
    _reset(connection)
    return OK


@commands.add("EXEC", arity=1, queued_in_multi=False)
async def exec_(connection: Connection, args: list[bytes]) -> Reply:
    """``EXEC``. Run the queued commands and return their replies as an array."""
    if not connection.in_multi:
        raise CommandError("ERR EXEC without MULTI")

    if connection.multi_aborted:
        _reset(connection)
        raise CommandError("EXECABORT Transaction discarded because of previous errors.")

    if connection.watch_dirty:
        _reset(connection)
        return NULL_ARRAY

    queued = list(connection.queued_commands)
    _reset(connection)

    # Replies are collected rather than written one by one so that the whole
    # transaction appears as a single array, as clients expect.
    return [await connection.invoke(command) for command in queued]


@commands.add("WATCH", arity=-2, queued_in_multi=False)
def watch(connection: Connection, args: list[bytes]) -> Reply:
    """``WATCH key [key ...]``. Must be called before ``MULTI``."""
    if connection.in_multi:
        raise CommandError("ERR WATCH inside MULTI is not allowed")
    for key in args[1:]:
        connection.server.watch.watch(connection, key)
    return OK


@commands.add("UNWATCH", arity=1, queued_in_multi=False)
def unwatch(connection: Connection, args: list[bytes]) -> Reply:
    """``UNWATCH``. Forget every watched key."""
    connection.server.watch.unwatch_all(connection)
    return OK


def _reset(connection: Connection) -> None:
    """Leave transaction mode and release the watches, as Redis does after EXEC."""
    connection.in_multi = False
    connection.queued_commands.clear()
    connection.multi_aborted = False
    connection.server.watch.unwatch_all(connection)
