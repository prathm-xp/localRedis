"""The server object: lifecycle, accept loop and background tasks.

Concurrency model: one thread, one asyncio event loop. Every client gets a
coroutine, not a thread, so ten thousand idle connections cost ten thousand
small objects instead of ten thousand stacks. Because command execution never
yields in the middle, each command is atomic with respect to every other one
and the keyspace needs no locks - the same guarantee Redis gets from being
single-threaded, reached the same way.

The blocking commands are the interesting consequence: ``BLPOP`` cannot block
the loop, so it awaits a future that the keyspace's touch signal resolves. The
client waits; the server does not.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
import os
import socket
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyredis import __version__
from pyredis.commands import COMMAND_TABLE
from pyredis.config import ServerConfig
from pyredis.connection import Connection
from pyredis.datastore import Store
from pyredis.errors import RdbError
from pyredis.notifications import BlockingManager, WatchRegistry
from pyredis.persistence import load_rdb
from pyredis.protocol import RespErrorReply, encode
from pyredis.pubsub import PubSub
from pyredis.replication.state import ReplicationState

if TYPE_CHECKING:  # pragma: no cover
    from pyredis.commands.base import CommandTable

__all__ = ["Server", "ServerStats"]

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ServerStats:
    """Counters surfaced by ``INFO``."""

    connections_received: int = 0
    commands_processed: int = 0
    expired_keys: int = 0
    keyspace_hits: int = 0
    keyspace_misses: int = 0


class Server:
    """Owns the keyspace, the listening socket and every connected client."""

    def __init__(self, config: ServerConfig, commands: CommandTable = COMMAND_TABLE) -> None:
        self.config = config
        self.commands = commands
        self.store = Store()
        self.pubsub = PubSub()
        self.blocking = BlockingManager()
        self.watch = WatchRegistry()
        self.replication = ReplicationState(is_replica=config.is_replica)
        self.stats = ServerStats()
        self.connections: dict[int, Connection] = {}
        #: Unix time of the last successful snapshot, reported by LASTSAVE.
        self.last_save_time = int(time.time())

        # Both features react to keyspace mutation; see pyredis.notifications.
        self.store.add_touch_listener(self.blocking.on_touch)
        self.store.add_touch_listener(self.watch.on_touch)

        self._client_ids = itertools.count(1)
        self._started_at = time.monotonic()
        self._shutdown = asyncio.Event()
        self._asyncio_server: asyncio.Server | None = None
        self._background: list[asyncio.Task[None]] = []

    # -- introspection ------------------------------------------------------

    @property
    def uptime_seconds(self) -> int:
        return int(time.monotonic() - self._started_at)

    @property
    def port(self) -> int:
        """The port actually bound, which differs from the config when port 0 is used."""
        if self._asyncio_server is not None and self._asyncio_server.sockets:
            return int(self._asyncio_server.sockets[0].getsockname()[1])
        return self.config.port

    def next_client_id(self) -> int:
        return next(self._client_ids)

    def forget_connection(self, connection: Connection) -> None:
        self.connections.pop(connection.id, None)

    # -- lifecycle ----------------------------------------------------------

    async def serve(self) -> None:
        """Run until :meth:`shutdown` is called. Returns once everything is closed."""
        self._load_snapshot()

        self._asyncio_server = await asyncio.start_server(
            self._accept,
            host=self.config.host,
            port=self.config.port,
            reuse_address=True,
            start_serving=True,
        )
        logger.info(
            "pyredis %s ready to accept connections on %s:%d (role=%s, pid=%d)",
            __version__,
            self.config.host,
            self.port,
            self.replication.role,
            os.getpid(),
        )

        self._background.append(asyncio.create_task(self._expire_loop(), name="active-expiry"))
        if self.config.is_replica:
            # Imported here rather than at module scope: only a replica needs it,
            # and it imports the connection layer that imports this module.
            from pyredis.replication.replica import replicate_from_master

            self._background.append(
                asyncio.create_task(replicate_from_master(self), name="replication-link")
            )

        try:
            await self._shutdown.wait()
        finally:
            await self._close_everything()

    def shutdown(self) -> None:
        """Ask the server to stop. Safe to call from a signal handler."""
        self._shutdown.set()

    async def _close_everything(self) -> None:
        logger.info("shutting down")
        for task in self._background:
            task.cancel()
        # Shutdown is best effort: report nothing a cancelled task raises on its way out.
        await asyncio.gather(*self._background, return_exceptions=True)
        self._background.clear()

        for connection in list(self.connections.values()):
            connection.abort()
        self.connections.clear()

        if self._asyncio_server is not None:
            self._asyncio_server.close()
            await self._asyncio_server.wait_closed()

    # -- connections --------------------------------------------------------

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if len(self.connections) >= self.config.max_clients:
            logger.warning("refusing connection: maxclients (%d) reached", self.config.max_clients)
            writer.write(encode(RespErrorReply("ERR max number of clients reached")))
            with contextlib.suppress(OSError):
                await writer.drain()
            writer.close()
            return

        _disable_nagle(writer)
        connection = Connection(self, reader, writer, self.next_client_id())
        self.connections[connection.id] = connection
        self.stats.connections_received += 1
        logger.debug("client %d connected from %s", connection.id, connection.peername)

        try:
            await connection.run()
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - last-resort guard
            logger.exception("unhandled error serving client %d", connection.id)
            connection.abort()

    # -- background tasks ---------------------------------------------------

    async def _expire_loop(self) -> None:
        """Reclaim memory from keys that expired and were never touched again."""
        interval = self.config.expire_cycle_interval
        while True:
            await asyncio.sleep(interval)
            removed = self.store.active_expire_cycle()
            if removed:
                self.stats.expired_keys += removed
                logger.debug("active expiry removed %d keys", removed)

    # -- persistence --------------------------------------------------------

    def _load_snapshot(self) -> None:
        """Load the RDB file if there is one.

        A missing file is normal - that is just an empty database. A file that
        exists but cannot be parsed is fatal: starting anyway would silently
        present an empty dataset in place of the user's data, and the next
        ``SAVE`` would overwrite the evidence. Redis refuses to start for the
        same reason.
        """
        path = self.config.rdb_path
        if not path.exists():
            logger.debug("no RDB file at %s; starting with an empty keyspace", path)
            return
        try:
            loaded = load_rdb(path, self.store)
        except (OSError, RdbError) as exc:
            logger.error("refusing to start: cannot load %s: %s", path, exc)
            raise
        logger.info("loaded %d keys from %s", loaded, path)


def _disable_nagle(writer: asyncio.StreamWriter) -> None:
    """Turn off Nagle's algorithm.

    Request/response traffic is small and latency-sensitive; without this,
    replies can sit in the kernel for tens of milliseconds waiting for a full
    segment that never arrives.
    """
    sock = writer.get_extra_info("socket")
    if sock is None:
        return
    # Not fatal, and the option is not available on every platform.
    with contextlib.suppress(OSError):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
