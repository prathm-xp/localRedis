"""The replica side of replication: connect to a master and follow its stream.

Runs as one background task for the life of the server. It performs the
handshake, loads the RDB snapshot the master sends, and then hands the socket
to an ordinary :class:`~pyredis.connection.Connection` in ``MASTER_LINK`` mode.

That last step is the design decision worth noticing: the master's write stream
*is* a stream of commands, so applying it through the same parser, dispatcher
and handlers that serve clients means there is exactly one command path in the
server. A separate "apply replicated command" path would be a second
implementation of the same logic, free to drift from the first.

If the link drops, the task reconnects with capped exponential backoff rather
than exiting - a replica that gives up after one network blip is not much of a
replica.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Final

from pyredis.connection import Connection, ConnectionKind
from pyredis.errors import ReplicationError
from pyredis.persistence import load_rdb_bytes
from pyredis.protocol import encode_command
from pyredis.replication.state import LinkStatus

if TYPE_CHECKING:  # pragma: no cover
    from pyredis.server import Server

__all__ = ["replicate_from_master"]

logger = logging.getLogger(__name__)

INITIAL_BACKOFF: Final = 0.5
MAX_BACKOFF: Final = 16.0
#: Guards each handshake step so a silent master cannot hang the task forever.
HANDSHAKE_TIMEOUT: Final = 10.0


async def replicate_from_master(server: Server) -> None:
    """Keep this server synchronised with its master, reconnecting as needed."""
    assert server.config.replicaof is not None
    host, port = server.config.replicaof
    backoff = INITIAL_BACKOFF

    while True:
        try:
            await _connect_and_follow(server, host, port)
            backoff = INITIAL_BACKOFF  # a clean session resets the penalty
            logger.warning("connection to master %s:%d closed; reconnecting", host, port)
        except asyncio.CancelledError:
            raise
        except (OSError, ReplicationError) as exc:
            logger.error("replication link to %s:%d failed: %s", host, port, exc)
        finally:
            server.replication.link_status = LinkStatus.DOWN

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF)


async def _connect_and_follow(server: Server, host: str, port: int) -> None:
    logger.info("connecting to master %s:%d", host, port)
    reader, writer = await asyncio.open_connection(host, port)

    try:
        await _handshake(server, reader, writer)
        payload = await asyncio.wait_for(_read_rdb_payload(reader), HANDSHAKE_TIMEOUT)

        server.store.flush()
        loaded = load_rdb_bytes(payload, server.store)
        server.replication.processed_offset = 0
        server.replication.link_status = LinkStatus.UP
        logger.info("full resync complete: %d keys loaded from master", loaded)

        # From here the socket carries commands, so let the normal read loop
        # apply them. Replies are suppressed and offsets counted by the kind.
        link = Connection(
            server,
            reader,
            writer,
            server.next_client_id(),
            kind=ConnectionKind.MASTER_LINK,
        )
        server.connections[link.id] = link
        await link.run()
    finally:
        writer.close()
        with contextlib.suppress(OSError, asyncio.CancelledError):
            await writer.wait_closed()


async def _handshake(
    server: Server, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    """Perform PING / REPLCONF / REPLCONF / PSYNC and check each reply."""
    await _exchange(reader, writer, encode_command("PING"), expect=b"+PONG")
    await _exchange(
        reader,
        writer,
        encode_command("REPLCONF", "listening-port", str(server.config.port)),
        expect=b"+OK",
    )
    await _exchange(reader, writer, encode_command("REPLCONF", "capa", "psync2"), expect=b"+OK")

    writer.write(encode_command("PSYNC", "?", "-1"))
    await writer.drain()
    response = await _read_line(reader)
    if not response.startswith(b"+FULLRESYNC"):
        raise ReplicationError(f"expected +FULLRESYNC from master, got {response!r}")
    logger.debug("master answered %s", response.decode(errors="replace"))


async def _exchange(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, request: bytes, *, expect: bytes
) -> None:
    writer.write(request)
    await writer.drain()
    response = await asyncio.wait_for(_read_line(reader), HANDSHAKE_TIMEOUT)
    if response != expect:
        raise ReplicationError(f"handshake failed: expected {expect!r}, got {response!r}")


async def _read_line(reader: asyncio.StreamReader) -> bytes:
    """Read one CRLF-terminated protocol line, without the terminator."""
    line = await reader.readuntil(b"\r\n")
    return line[:-2]


async def _read_rdb_payload(reader: asyncio.StreamReader) -> bytes:
    """Read the ``$<length>\\r\\n<bytes>`` snapshot that follows ``+FULLRESYNC``.

    Note the missing trailing CRLF: unlike every other bulk string in the
    protocol, the RDB transfer is not terminated. Reading two bytes too many
    here would silently eat the first command of the replication stream.
    """
    header = await _read_line(reader)
    if not header.startswith(b"$"):
        raise ReplicationError(f"expected an RDB bulk payload, got {header!r}")
    try:
        length = int(header[1:])
    except ValueError:
        raise ReplicationError(f"malformed RDB payload length: {header!r}") from None
    if length < 0:
        raise ReplicationError(f"negative RDB payload length: {length}")
    return await reader.readexactly(length)
