"""Replication commands seen by a *master*: REPLCONF, PSYNC and WAIT.

The handshake a replica performs is, in order:

.. code-block:: text

    PING                                  -> +PONG
    REPLCONF listening-port <port>        -> +OK
    REPLCONF capa psync2                  -> +OK
    PSYNC ? -1                            -> +FULLRESYNC <replid> <offset>
                                             $<len>\\r\\n<rdb bytes>

After the RDB transfer the same socket becomes a one-way command stream, and
this connection stops being a client and starts being a replica. It is not
completely one-way: the replica sends ``REPLCONF ACK <offset>`` back, either
unprompted or in answer to ``REPLCONF GETACK *``, and that is what ``WAIT``
counts.

The client half of the handshake lives in :mod:`pyredis.replication.replica`.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyredis.commands.base import CommandSet
from pyredis.commands.parsing import parse_int
from pyredis.errors import CommandError
from pyredis.persistence import dump_rdb_bytes
from pyredis.protocol import NO_REPLY, OK, Reply, SimpleString

if TYPE_CHECKING:  # pragma: no cover
    from pyredis.connection import Connection

logger = logging.getLogger(__name__)

commands = CommandSet()

__all__ = ["commands"]


@commands.add("REPLCONF", arity=-2, allowed_in_multi=False)
def replconf(connection: Connection, args: list[bytes]) -> Reply:
    """``REPLCONF listening-port <port> | capa <c> | GETACK * | ACK <offset>``."""
    subcommand = args[1].upper()

    if subcommand == b"LISTENING-PORT" and len(args) == 3:
        connection.replica_listening_port = parse_int(args[2])
        return OK

    if subcommand == b"CAPA":
        # Capabilities are recorded by acknowledging them; none change behaviour here.
        return OK

    if subcommand == b"GETACK":
        # Sent by our master. The offset reported is the one from *before* this
        # command, because the connection counts a command's bytes only after
        # running it - which is exactly what the master expects.
        offset = connection.server.replication.processed_offset
        connection.reply(
            [b"REPLCONF", b"ACK", b"%d" % offset],
            force=True,  # the one reply a replica sends up the master link
        )
        return NO_REPLY

    if subcommand == b"ACK" and len(args) == 3:
        connection.server.replication.record_ack(connection, parse_int(args[2]))
        return NO_REPLY  # acknowledgements are never answered

    raise CommandError(f"ERR Unrecognized REPLCONF option: {args[1].decode(errors='replace')}")


@commands.add("PSYNC", arity=3, allowed_in_multi=False)
def psync(connection: Connection, args: list[bytes]) -> Reply:
    """``PSYNC <replid> <offset>``. Always answers with a full resync.

    Partial resynchronisation needs a replication backlog to replay from, which
    this server does not keep; answering ``+FULLRESYNC`` unconditionally is the
    correct fallback and is what Redis itself does when the backlog cannot
    satisfy the request.

    The RDB payload is sent as a bulk string *without* a trailing CRLF - the
    one place the protocol deviates from normal bulk-string framing.
    """
    server = connection.server
    if server.config.is_replica:
        raise CommandError("ERR Can't PSYNC from a replica")

    payload, result = dump_rdb_bytes(server.store)
    connection.reply(SimpleString(f"FULLRESYNC {server.replication.replication_id} 0"))
    connection.send(b"$%d\r\n" % len(payload) + payload)

    server.replication.register_replica(connection, connection.replica_listening_port)
    connection.become_replica_link()

    if result.skipped_keys:
        logger.warning(
            "full resync omitted %d key(s) of unsupported type; streams do not "
            "transfer in a resync and will only replicate from now on",
            result.skipped_keys,
        )
    return NO_REPLY


@commands.add("WAIT", arity=3)
async def wait(connection: Connection, args: list[bytes]) -> Reply:
    """``WAIT numreplicas timeout``.

    Returns how many replicas have acknowledged everything written so far. A
    timeout of 0 waits indefinitely. This is an acknowledgement count, not a
    durability guarantee: replication here is asynchronous, so a write can be
    lost if the master dies before any replica acknowledges it.
    """
    required = parse_int(args[1])
    timeout_ms = parse_int(args[2])
    if timeout_ms < 0:
        raise CommandError("ERR timeout is negative")
    return await connection.server.replication.wait_for_acks(required, timeout_ms)
