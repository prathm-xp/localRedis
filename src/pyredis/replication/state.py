"""Replication bookkeeping shared by the master and replica roles.

One master, many replicas, asynchronous propagation - the same model Redis
uses. A master appends every write it executes to a byte stream and sends that
stream to each attached replica; a replica applies the stream and counts the
bytes it has processed. That single counter, the *replication offset*, is what
makes ``WAIT`` and ``REPLCONF ACK`` work.

Two details are easy to get wrong and are handled explicitly here:

* The offset only starts advancing once a replica has attached. Redis allocates
  its replication backlog on the first ``PSYNC``, so a master that never had a
  replica reports ``master_repl_offset:0`` however many writes it served.
* ``REPLCONF GETACK`` itself occupies bytes in the stream. A replica must
  answer with the offset *before* that command, then count it - otherwise the
  two sides disagree by exactly the length of the GETACK forever.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

from pyredis.protocol import encode_command

if TYPE_CHECKING:  # pragma: no cover
    from pyredis.connection import Connection

__all__ = ["LinkStatus", "ReplicaHandle", "ReplicationState"]

logger = logging.getLogger(__name__)

#: Sent to every replica when ``WAIT`` needs fresh acknowledgements.
GETACK: Final = encode_command("REPLCONF", "GETACK", "*")


class LinkStatus:
    """Values reported as ``master_link_status`` in ``INFO replication``."""

    UP: Final = "up"
    DOWN: Final = "down"


@dataclass(slots=True)
class ReplicaHandle:
    """A replica attached to this master."""

    connection: Connection
    #: Port the replica listens on, learned from ``REPLCONF listening-port``.
    listening_port: int | None = None
    #: Highest offset the replica has acknowledged.
    ack_offset: int = 0


@dataclass(slots=True)
class ReplicationState:
    """Replication state for whichever role this server is playing."""

    #: True when this server replicates from another.
    is_replica: bool
    #: 40-character replication ID, regenerated on every start as Redis does.
    replication_id: str = field(default_factory=lambda: secrets.token_hex(20))
    #: Bytes of command stream produced by this master.
    master_offset: int = 0
    #: Bytes of command stream applied from our master (replica role only).
    processed_offset: int = 0
    #: State of the link to our master (replica role only).
    link_status: str = LinkStatus.DOWN
    replicas: list[ReplicaHandle] = field(default_factory=list)
    #: Redis allocates its backlog on the first PSYNC; before that the offset
    #: stays at zero no matter how many writes are served.
    _backlog_active: bool = False
    _ack_waiters: list[asyncio.Future[None]] = field(default_factory=list)

    @property
    def role(self) -> str:
        return "slave" if self.is_replica else "master"

    @property
    def replica_count(self) -> int:
        return len(self.replicas)

    # -- master side --------------------------------------------------------

    def register_replica(self, connection: Connection, listening_port: int | None) -> ReplicaHandle:
        """Attach a replica after a successful ``PSYNC`` and start the backlog."""
        handle = ReplicaHandle(connection=connection, listening_port=listening_port)
        self.replicas.append(handle)
        self._backlog_active = True
        logger.info("replica attached from %s (total %d)", connection.peername, len(self.replicas))
        return handle

    def remove_replica(self, connection: Connection) -> None:
        before = len(self.replicas)
        self.replicas = [r for r in self.replicas if r.connection is not connection]
        if len(self.replicas) != before:
            logger.info("replica detached (total %d)", len(self.replicas))

    def handle_for(self, connection: Connection) -> ReplicaHandle | None:
        return next((r for r in self.replicas if r.connection is connection), None)

    def propagate(self, args: list[bytes]) -> None:
        """Send one write command to every attached replica.

        Encoded once and reused, so fan-out costs one buffer copy per replica
        rather than a re-serialization.
        """
        if not self._backlog_active:
            return
        payload = encode_command(*args)
        self.master_offset += len(payload)
        for handle in self.replicas:
            handle.connection.send(payload)

    def request_acks(self) -> None:
        """Ask every replica for its current offset."""
        if not self.replicas:
            return
        self.master_offset += len(GETACK)
        for handle in self.replicas:
            handle.connection.send(GETACK)

    def record_ack(self, connection: Connection, offset: int) -> None:
        """Record ``REPLCONF ACK <offset>`` from a replica and wake ``WAIT``."""
        handle = self.handle_for(connection)
        if handle is None:
            return
        handle.ack_offset = max(handle.ack_offset, offset)
        for waiter in self._ack_waiters:
            if not waiter.done():
                waiter.set_result(None)
        self._ack_waiters.clear()

    def count_acked(self, offset: int) -> int:
        """Replicas that have acknowledged at least ``offset``."""
        return sum(1 for handle in self.replicas if handle.ack_offset >= offset)

    async def wait_for_acks(self, required: int, timeout_ms: int) -> int:
        """Implement ``WAIT``: how many replicas caught up, within the timeout.

        ``timeout_ms`` of 0 means wait indefinitely. The return value is the
        replica count at the moment the wait ends, which may exceed or fall
        short of ``required``.
        """
        target = self.master_offset
        acked = self.count_acked(target)
        if acked >= required or not self.replicas:
            return acked

        # Nothing has been written since the last acknowledgement, so every
        # replica is already current and just has not said so.
        self.request_acks()

        loop = asyncio.get_running_loop()
        deadline = None if timeout_ms == 0 else loop.time() + timeout_ms / 1000

        while True:
            acked = self.count_acked(target)
            if acked >= required:
                return acked

            remaining = None
            if deadline is not None:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return acked

            waiter: asyncio.Future[None] = loop.create_future()
            self._ack_waiters.append(waiter)
            try:
                await asyncio.wait_for(waiter, remaining)
            except TimeoutError:
                return self.count_acked(target)
            finally:
                if waiter in self._ack_waiters:
                    self._ack_waiters.remove(waiter)

    # -- replica side -------------------------------------------------------

    def advance_processed(self, nbytes: int) -> None:
        """Count ``nbytes`` of the master's stream as applied."""
        self.processed_offset += nbytes

    def info_fields(self) -> list[tuple[str, str]]:
        """Key/value pairs for the ``replication`` section of ``INFO``."""
        fields = [
            ("role", self.role),
            ("connected_slaves", str(len(self.replicas))),
        ]
        for index, handle in enumerate(self.replicas):
            host = handle.connection.peer_host or "?"
            port = handle.listening_port if handle.listening_port is not None else "?"
            fields.append(
                (f"slave{index}", f"ip={host},port={port},state=online,offset={handle.ack_offset}")
            )
        if self.is_replica:
            fields.append(("master_link_status", self.link_status))
            fields.append(("slave_repl_offset", str(self.processed_offset)))
            fields.append(("slave_read_only", "1"))
        fields.append(("master_replid", self.replication_id))
        fields.append(("master_repl_offset", str(self.master_offset)))
        return fields
