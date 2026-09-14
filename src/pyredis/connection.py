"""One client connection: its state, its read loop, and command dispatch.

A :class:`Connection` wraps an ``asyncio`` stream pair and owns everything that
is per-client: the parse buffer, subscription set, transaction queue and watch
list. The same class serves three roles, distinguished by :class:`ConnectionKind`:

``CLIENT``
    An ordinary client.
``REPLICA_LINK``
    A client that issued ``PSYNC`` and is now receiving the write stream. It
    still reads, because replicas send ``REPLCONF ACK`` back.
``MASTER_LINK``
    On a replica, the link *to* its master. The master's write stream is just
    a sequence of commands, so reusing the normal read loop to apply them
    avoids a second, subtly different command path. Replies are suppressed and
    the applied byte count is tracked.

Replies are buffered per read batch and written once. A pipeline of a thousand
commands therefore costs one ``write`` rather than a thousand, which is where
most of the throughput in the benchmark comes from.
"""

from __future__ import annotations

import asyncio
import logging
from enum import Enum, auto
from typing import TYPE_CHECKING, Final

from pyredis.errors import CommandError, ProtocolError
from pyredis.protocol import (
    NO_REPLY,
    QUEUED,
    ParserLimits,
    Reply,
    RespErrorReply,
    RespReader,
    encode,
)

if TYPE_CHECKING:  # pragma: no cover
    from pyredis.commands.base import CommandSpec
    from pyredis.server import Server

__all__ = ["Connection", "ConnectionKind"]

logger = logging.getLogger(__name__)

#: Bytes requested per socket read.
READ_CHUNK: Final = 64 * 1024

#: Drop a connection whose unsent output grows past this. Redis calls the same
#: idea ``client-output-buffer-limit``; without it one slow replica or a client
#: that never reads can exhaust the server's memory.
MAX_OUTPUT_BUFFER: Final = 256 * 1024 * 1024

_SUBSCRIBE_MODE_ERROR: Final = (
    "ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT "
    "/ RESET are allowed in this context"
)


class ConnectionKind(Enum):
    """What a connection is being used for. See the module docstring."""

    CLIENT = auto()
    REPLICA_LINK = auto()
    MASTER_LINK = auto()


class Connection:
    """A single client connection and its command loop."""

    def __init__(
        self,
        server: Server,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        client_id: int,
        kind: ConnectionKind = ConnectionKind.CLIENT,
    ) -> None:
        self.server = server
        self.id = client_id
        self.kind = kind
        self.name: bytes = b""

        self._reader = reader
        self._writer = writer
        self._parser = RespReader(ParserLimits(max_inline_length=64 * 1024))
        self._out: list[bytes] = []
        self._closing = False

        # Pub/Sub state. The registry in `server.pubsub` holds the reverse map.
        self.subscribed_channels: set[bytes] = set()
        self.subscribed_patterns: set[bytes] = set()

        # Transaction state.
        self.in_multi = False
        self.queued_commands: list[list[bytes]] = []
        #: Set when a queued command failed to parse, so EXEC must abort.
        self.multi_aborted = False
        self.watched_keys: set[bytes] = set()
        self.watch_dirty = False

        #: Port this peer listens on, learned from ``REPLCONF listening-port``
        #: during the replication handshake. Reported by ``INFO replication``.
        self.replica_listening_port: int | None = None

        #: Set by a handler to propagate something other than the command as
        #: received. ``None`` means "propagate verbatim" and an empty list means
        #: "propagate nothing". Consumed and cleared once per command.
        self.propagation_override: list[bytes] | None = None

        peer = writer.get_extra_info("peername")
        self.peername: str = _format_peer(peer)
        self.peer_host: str = peer[0] if isinstance(peer, tuple) else ""

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<Connection id={self.id} kind={self.kind.name} peer={self.peername}>"

    # -- state predicates ---------------------------------------------------

    @property
    def in_subscribe_mode(self) -> bool:
        """RESP2 restricts a subscribed connection to a handful of commands."""
        return bool(self.subscribed_channels or self.subscribed_patterns)

    @property
    def subscription_count(self) -> int:
        """Total subscriptions, reported in every (P)SUBSCRIBE reply."""
        return len(self.subscribed_channels) + len(self.subscribed_patterns)

    @property
    def is_closing(self) -> bool:
        return self._closing

    def become_replica_link(self) -> None:
        """Mark this connection as a replica receiving the write stream.

        Exposed as a method so command handlers never need to import the
        connection module, which keeps the import graph acyclic.
        """
        self.kind = ConnectionKind.REPLICA_LINK

    # -- output -------------------------------------------------------------

    def reply(self, value: Reply, *, force: bool = False) -> None:
        """Queue a reply for this client.

        Replies to a master are suppressed - it is streaming commands at us and
        is not waiting for answers. ``force`` is for the one exception,
        ``REPLCONF ACK``, which a replica must send back up the same link.
        """
        if value is NO_REPLY:
            return
        if self.kind is ConnectionKind.MASTER_LINK and not force:
            return
        self._out.append(encode(value))

    def send(self, data: bytes) -> None:
        """Write pre-encoded bytes, from this connection or another one.

        Used for propagation to replicas and for Pub/Sub delivery. Any replies
        already queued are written first so ordering cannot be inverted.
        """
        if self._closing or self._writer.is_closing():
            return
        self._write_buffered()
        self._write(data)

    def _write(self, data: bytes) -> None:
        self._writer.write(data)
        if self._output_buffer_size() > MAX_OUTPUT_BUFFER:
            logger.warning(
                "client %d (%s) exceeded the output buffer limit; closing",
                self.id,
                self.peername,
            )
            self.abort()

    def _write_buffered(self) -> None:
        if self._out:
            self._write(b"".join(self._out))
            self._out.clear()

    async def flush(self) -> None:
        """Write queued replies and apply backpressure."""
        self._write_buffered()
        try:
            await self._writer.drain()
        except (ConnectionResetError, BrokenPipeError, OSError):
            self.abort()

    def _output_buffer_size(self) -> int:
        transport = self._writer.transport
        try:
            return transport.get_write_buffer_size()
        except (AttributeError, NotImplementedError):  # pragma: no cover
            return 0

    # -- lifecycle ----------------------------------------------------------

    def request_close(self) -> None:
        """Close once the current batch of replies has been written."""
        self._closing = True

    def abort(self) -> None:
        """Drop the connection immediately, discarding unwritten output."""
        self._closing = True
        self._out.clear()
        transport = self._writer.transport
        if not transport.is_closing():
            transport.abort()

    async def run(self) -> None:
        """Read, parse and execute until the peer goes away or we close."""
        try:
            while not self._closing:
                try:
                    data = await self._reader.read(READ_CHUNK)
                except (ConnectionResetError, BrokenPipeError, OSError) as exc:
                    logger.debug("client %d read failed: %s", self.id, exc)
                    break
                if not data:
                    break

                self._parser.feed(data)
                if self._parser.pending > self.server.config.max_query_buffer:
                    self.reply(RespErrorReply("ERR Protocol error: query buffer limit exceeded"))
                    await self.flush()
                    break

                if not await self._drain_commands():
                    await self.flush()
                    break
                await self.flush()
        except asyncio.CancelledError:
            raise
        finally:
            await self._teardown()

    async def _drain_commands(self) -> bool:
        """Execute every complete command in the buffer.

        Returns ``False`` when the connection must be closed - a protocol error
        or an explicit ``QUIT``.
        """
        while True:
            try:
                parsed = self._parser.read_command()
            except ProtocolError as exc:
                # Framing is lost; there is no way to resynchronise the stream.
                self.reply(RespErrorReply(f"ERR {exc}"), force=True)
                return False
            if parsed is None:
                return True

            if parsed.args:
                await self._execute(parsed.args)
            if self.kind is ConnectionKind.MASTER_LINK:
                # Counted after execution so REPLCONF GETACK reports the offset
                # from before itself, which is what the master expects.
                self.server.replication.advance_processed(parsed.size)
            if self._closing:
                return False

    async def _execute(self, args: list[bytes]) -> None:
        """Apply dispatch policy, then run the command and queue its reply."""
        spec = self.server.commands.lookup(args[0])
        if spec is None:
            self._fail_command(unknown_command_error(args))
            return

        if not spec.accepts_arity(len(args)):
            self._fail_command(arity_error(spec.name))
            return

        if self.in_subscribe_mode and not spec.allowed_in_subscribe_mode:
            self.reply(RespErrorReply(_SUBSCRIBE_MODE_ERROR % spec.name))
            return

        if self.in_multi and not spec.allowed_in_multi:
            # Queueing it would put its self-written output inside the EXEC array.
            self._fail_command(
                RespErrorReply(f"ERR {spec.name.upper()} is not allowed in transactions")
            )
            return

        if self.in_multi and spec.queued_in_multi:
            self.queued_commands.append(args)
            self.reply(QUEUED)
            return

        self.reply(await self.run_command(spec, args))

    def _fail_command(self, error: RespErrorReply) -> None:
        """Report an error that also poisons an open transaction.

        A command rejected at queue time leaves the transaction unrunnable, so
        ``EXEC`` must refuse with ``EXECABORT`` rather than silently skip it.
        """
        self.reply(error)
        if self.in_multi:
            self.multi_aborted = True

    async def invoke(self, args: list[bytes]) -> Reply:
        """Look up and run one command, returning its reply without writing it.

        This is the seam ``EXEC`` uses to run queued commands: it bypasses the
        queueing and subscribe-mode policy that has already been applied.
        """
        spec = self.server.commands.lookup(args[0])
        if spec is None:
            return unknown_command_error(args)
        if not spec.accepts_arity(len(args)):
            return arity_error(spec.name)
        return await self.run_command(spec, args)

    async def run_command(self, spec: CommandSpec, args: list[bytes]) -> Reply:
        """Execute a command and propagate it if it was a write."""
        if spec.is_write and self.server.config.is_replica and self.kind is ConnectionKind.CLIENT:
            return RespErrorReply("READONLY You can't write against a read only replica.")

        self.server.stats.commands_processed += 1
        self.propagation_override = None
        try:
            result = spec.handler(self, args)
            if spec.is_async:
                result = await result  # type: ignore[misc]
        except CommandError as exc:
            return RespErrorReply(exc.message)

        if spec.is_write and not self.server.config.is_replica:
            # Annotated because the handler may have set this via
            # rewrite_propagation(); without it the checker narrows to None.
            override: list[bytes] | None = self.propagation_override
            if override is None:
                self.server.replication.propagate(args)
            elif override:
                self.server.replication.propagate(override)
            # An empty override means the command changed nothing worth sending.
        self.propagation_override = None
        return result  # type: ignore[return-value]

    def rewrite_propagation(self, *args: bytes) -> None:
        """Propagate ``args`` to replicas instead of the command as received.

        Required whenever a command is non-deterministic: ``XADD key *`` would
        generate a different ID on every replica, and ``SET k v PX 100`` would
        start its clock at a different instant. Both are rewritten to an
        explicit, replay-safe form.
        """
        if not args:
            raise ValueError("use suppress_propagation() to propagate nothing")
        self.propagation_override = list(args)

    def suppress_propagation(self) -> None:
        """Send nothing to replicas for this command.

        Used when a write command turns out not to have written: a ``BLPOP``
        that timed out, for instance. Replaying it on a replica would block the
        replication stream on a list that will never be filled.
        """
        self.propagation_override = []

    async def _teardown(self) -> None:
        """Release every registration this connection holds."""
        self._closing = True
        self.server.pubsub.remove_connection(self)
        self.server.watch.unwatch_all(self)
        self.server.replication.remove_replica(self)
        self.server.forget_connection(self)
        try:
            if not self._writer.is_closing():
                self._writer.close()
            await self._writer.wait_closed()
        except (ConnectionResetError, BrokenPipeError, OSError):
            pass
        logger.debug("client %d (%s) disconnected", self.id, self.peername)


def unknown_command_error(args: list[bytes]) -> RespErrorReply:
    """Build Redis's unknown-command error, arguments and all."""
    rendered = "".join(f"'{_printable(arg)}', " for arg in args[1:])
    return RespErrorReply(
        f"ERR unknown command '{_printable(args[0])}', with args beginning with: {rendered}"
    )


def arity_error(name: str) -> RespErrorReply:
    return RespErrorReply(f"ERR wrong number of arguments for '{name}' command")


def _printable(raw: bytes) -> str:
    return raw.decode("utf-8", errors="replace")


def _format_peer(peer: object) -> str:
    if isinstance(peer, tuple) and len(peer) >= 2:
        return f"{peer[0]}:{peer[1]}"
    return str(peer)
