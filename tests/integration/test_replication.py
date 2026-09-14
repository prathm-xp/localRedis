"""End-to-end tests for master/replica replication.

Each test starts two real server processes and lets them replicate over a real
socket. Replication is asynchronous, so assertions about replica state go
through :func:`eventually` rather than a fixed sleep.
"""

from __future__ import annotations

import time
from collections.abc import Callable

import pytest

from tests.integration.client import Error, RespClient, Status, encode_command

pytestmark = [pytest.mark.integration, pytest.mark.slow]

PROPAGATION_TIMEOUT = 10.0


def eventually(check: Callable[[], object], timeout: float = PROPAGATION_TIMEOUT) -> object:
    """Poll ``check`` until it returns something truthy, or fail with the last value."""
    deadline = time.monotonic() + timeout
    last: object = None
    while time.monotonic() < deadline:
        last = check()
        if last:
            return last
        time.sleep(0.05)
    raise AssertionError(f"condition never became true within {timeout}s (last value: {last!r})")


def _complete_handshake(connection: RespClient) -> None:
    """Run the replica handshake and swallow the RDB payload."""
    assert connection.command("PING") == Status("PONG")
    assert connection.command("REPLCONF", "listening-port", "1234") == Status("OK")
    assert connection.command("REPLCONF", "capa", "psync2") == Status("OK")
    connection.send_command("PSYNC", "?", "-1")
    assert connection.read_reply().startswith("FULLRESYNC ")

    header = b""
    while not header.endswith(b"\r\n"):
        header += connection.read_raw(1)
    assert header.startswith(b"$")
    connection.read_raw(int(header[1:]))


def info_field(connection: RespClient, section: str, field: str) -> str:
    """Read one ``field:value`` line out of an ``INFO`` section."""
    raw: bytes = connection.command("INFO", section)
    for line in raw.decode().splitlines():
        if line.startswith(f"{field}:"):
            return line.split(":", 1)[1]
    raise AssertionError(f"{field} missing from INFO {section}")


@pytest.fixture
def master_and_replica(server_factory):
    """A master with one replica attached and the initial sync complete."""
    master = server_factory()
    replica = server_factory("--replicaof", f"localhost {master.port}")

    master_client = master.client()
    replica_client = replica.client()

    eventually(lambda: info_field(replica_client, "replication", "master_link_status") == "up")
    eventually(lambda: int(info_field(master_client, "replication", "connected_slaves")) == 1)

    try:
        yield master, replica, master_client, replica_client
    finally:
        master_client.close()
        replica_client.close()


class TestRoles:
    def test_a_standalone_server_is_a_master(self, server_factory):
        server = server_factory()
        with server.client() as connection:
            assert info_field(connection, "replication", "role") == "master"

    def test_a_replica_reports_the_slave_role(self, master_and_replica):
        _, _, _, replica_client = master_and_replica
        assert info_field(replica_client, "replication", "role") == "slave"

    def test_master_reports_its_replica(self, master_and_replica):
        _, replica, master_client, _ = master_and_replica
        assert int(info_field(master_client, "replication", "connected_slaves")) == 1
        assert f"port={replica.port}" in info_field(master_client, "replication", "slave0")

    def test_replication_id_is_40_hex_characters(self, server_factory):
        server = server_factory()
        with server.client() as connection:
            replication_id = info_field(connection, "replication", "master_replid")
            assert len(replication_id) == 40
            assert all(character in "0123456789abcdef" for character in replication_id)

    def test_offset_starts_at_zero(self, server_factory):
        server = server_factory()
        with server.client() as connection:
            connection.command("SET", "k", "v")  # no replica, so no backlog
            assert info_field(connection, "replication", "master_repl_offset") == "0"


class TestPropagation:
    def test_a_write_reaches_the_replica(self, master_and_replica):
        _, _, master_client, replica_client = master_and_replica
        master_client.command("SET", "name", "aryan")
        eventually(lambda: replica_client.command("GET", "name") == b"aryan")

    def test_several_writes_arrive_in_order(self, master_and_replica):
        _, _, master_client, replica_client = master_and_replica
        for index in range(20):
            master_client.command("SET", f"k{index}", index)
        eventually(lambda: replica_client.command("GET", "k19") == b"19")
        assert replica_client.command("DBSIZE") == 20

    def test_deletes_propagate(self, master_and_replica):
        _, _, master_client, replica_client = master_and_replica
        master_client.command("SET", "k", "v")
        eventually(lambda: replica_client.command("EXISTS", "k") == 1)
        master_client.command("DEL", "k")
        eventually(lambda: replica_client.command("EXISTS", "k") == 0)

    def test_list_operations_propagate(self, master_and_replica):
        _, _, master_client, replica_client = master_and_replica
        master_client.command("RPUSH", "l", "a", "b", "c")
        eventually(lambda: replica_client.command("LRANGE", "l", 0, -1) == [b"a", b"b", b"c"])
        master_client.command("LPOP", "l")
        eventually(lambda: replica_client.command("LRANGE", "l", 0, -1) == [b"b", b"c"])

    def test_sorted_set_operations_propagate(self, master_and_replica):
        _, _, master_client, replica_client = master_and_replica
        master_client.command("ZADD", "z", 1, "one", 2, "two")
        eventually(lambda: replica_client.command("ZRANGE", "z", 0, -1) == [b"one", b"two"])

    def test_reads_are_not_propagated(self, master_and_replica):
        _, _, master_client, replica_client = master_and_replica
        master_client.command("SET", "k", "v")
        eventually(lambda: replica_client.command("GET", "k") == b"v")
        before = int(info_field(master_client, "replication", "master_repl_offset"))
        for _ in range(20):
            master_client.command("GET", "k")
        assert int(info_field(master_client, "replication", "master_repl_offset")) == before

    def test_transactions_propagate(self, master_and_replica):
        _, _, master_client, replica_client = master_and_replica
        master_client.command("MULTI")
        master_client.command("SET", "a", "1")
        master_client.command("SET", "b", "2")
        master_client.command("EXEC")
        eventually(lambda: replica_client.command("GET", "b") == b"2")
        assert replica_client.command("GET", "a") == b"1"


class TestDeterministicPropagation:
    """Non-deterministic commands must be rewritten before they are propagated."""

    def test_xadd_with_a_generated_id_uses_the_same_id_on_the_replica(self, master_and_replica):
        _, _, master_client, replica_client = master_and_replica
        generated = master_client.command("XADD", "events", "*", "f", "v")
        eventually(lambda: replica_client.command("XLEN", "events") == 1)
        assert replica_client.command("XRANGE", "events", "-", "+")[0][0] == generated

    def test_relative_expiry_becomes_an_absolute_deadline(self, master_and_replica):
        _, _, master_client, replica_client = master_and_replica
        master_client.command("SET", "k", "v", "PX", 60_000)
        eventually(lambda: replica_client.command("EXISTS", "k") == 1)

        master_ttl = master_client.command("PTTL", "k")
        replica_ttl = replica_client.command("PTTL", "k")
        # An absolute PXAT keeps the two deadlines within a network round trip
        # of each other; a replayed relative PX would restart the clock.
        assert abs(master_ttl - replica_ttl) < 1000

    def test_expire_propagates_as_an_absolute_deadline(self, master_and_replica):
        _, _, master_client, replica_client = master_and_replica
        master_client.command("SET", "k", "v")
        master_client.command("EXPIRE", "k", 60)
        eventually(lambda: replica_client.command("TTL", "k") > 0)
        assert abs(master_client.command("PTTL", "k") - replica_client.command("PTTL", "k")) < 1000

    def test_a_blocking_pop_that_times_out_propagates_nothing(self, master_and_replica):
        _, _, master_client, replica_client = master_and_replica
        master_client.command("SET", "sync", "1")
        eventually(lambda: replica_client.command("GET", "sync") == b"1")

        before = int(info_field(master_client, "replication", "master_repl_offset"))
        master_client.settimeout(5.0)
        master_client.send_command("BLPOP", "nothing", "0.2")
        assert master_client.read_raw(5) == b"*-1\r\n"
        assert int(info_field(master_client, "replication", "master_repl_offset")) == before


class TestInitialSync:
    def test_existing_data_transfers_on_connect(self, server_factory):
        master = server_factory()
        with master.client() as master_client:
            master_client.command("SET", "before", "sync")
            master_client.command("RPUSH", "l", "a", "b")

        replica = server_factory("--replicaof", f"localhost {master.port}")
        with replica.client() as replica_client:
            eventually(lambda: replica_client.command("GET", "before") == b"sync")
            assert replica_client.command("LRANGE", "l", 0, -1) == [b"a", b"b"]

    def test_the_replica_discards_its_own_previous_data(self, server_factory, tmp_path):
        master = server_factory()
        replica_dir = tmp_path / "replica-data"
        replica_dir.mkdir()

        standalone = server_factory(directory=replica_dir)
        with standalone.client() as connection:
            connection.command("SET", "stale", "value")
            connection.command("SAVE")
        standalone.stop()

        replica = server_factory("--replicaof", f"localhost {master.port}", directory=replica_dir)
        with master.client() as master_client, replica.client() as replica_client:
            master_client.command("SET", "fresh", "value")
            eventually(lambda: replica_client.command("GET", "fresh") == b"value")
            assert replica_client.command("EXISTS", "stale") == 0


class TestReplicaReadOnly:
    def test_writes_are_refused(self, master_and_replica):
        _, _, _, replica_client = master_and_replica
        assert replica_client.command("SET", "k", "v") == Error(
            "READONLY You can't write against a read only replica."
        )

    def test_reads_are_allowed(self, master_and_replica):
        _, _, master_client, replica_client = master_and_replica
        master_client.command("SET", "k", "v")
        eventually(lambda: replica_client.command("GET", "k") == b"v")
        assert replica_client.command("PING") == Status("PONG")
        assert replica_client.command("DBSIZE") >= 1

    def test_psync_against_a_replica_is_refused(self, master_and_replica):
        _, _, _, replica_client = master_and_replica
        assert replica_client.command("PSYNC", "?", "-1") == Error("ERR Can't PSYNC from a replica")


class TestWait:
    def test_wait_with_no_replicas_returns_zero(self, server_factory):
        server = server_factory()
        with server.client() as connection:
            assert connection.command("WAIT", 0, 100) == 0

    def test_wait_counts_the_attached_replica(self, master_and_replica):
        _, _, master_client, _ = master_and_replica
        assert master_client.command("WAIT", 0, 100) == 1

    def test_wait_returns_once_the_replica_acknowledges(self, master_and_replica):
        _, _, master_client, _ = master_and_replica
        master_client.settimeout(15.0)
        master_client.command("SET", "k", "v")
        assert master_client.command("WAIT", 1, 5000) == 1

    def test_wait_for_more_replicas_than_exist_times_out(self, master_and_replica):
        _, _, master_client, _ = master_and_replica
        master_client.settimeout(15.0)
        master_client.command("SET", "k", "v")
        started = time.monotonic()
        assert master_client.command("WAIT", 5, 500) == 1
        assert 0.4 < time.monotonic() - started < 5.0

    def test_negative_timeout_is_rejected(self, master_and_replica):
        _, _, master_client, _ = master_and_replica
        assert master_client.command("WAIT", 1, -1) == Error("ERR timeout is negative")


class TestHandshakeProtocol:
    """Drive the handshake by hand, asserting the exact wire exchange."""

    def test_a_manual_replica_handshake(self, server_factory):
        master = server_factory()
        with master.client() as setup:
            setup.command("SET", "existing", "value")

        fake_replica = RespClient("127.0.0.1", master.port, timeout=10.0)
        try:
            assert fake_replica.command("PING") == Status("PONG")
            assert fake_replica.command("REPLCONF", "listening-port", "1234") == Status("OK")
            assert fake_replica.command("REPLCONF", "capa", "psync2") == Status("OK")

            fake_replica.send_command("PSYNC", "?", "-1")
            response = fake_replica.read_reply()
            assert response.startswith("FULLRESYNC ")
            replication_id, offset = response.split()[1:]
            assert len(replication_id) == 40
            assert offset == "0"

            # The RDB payload is a bulk string with no trailing CRLF.
            assert fake_replica.read_raw(1) == b"$"
            length = b""
            while not length.endswith(b"\r\n"):
                length += fake_replica.read_raw(1)
            payload = fake_replica.read_raw(int(length))
            assert payload.startswith(b"REDIS")

            # From here the socket carries the write stream.
            with master.client() as master_client:
                master_client.command("SET", "after", "sync")
            assert fake_replica.read_reply() == [b"SET", b"after", b"sync"]
        finally:
            fake_replica.close()

    def test_getack_is_answered_with_the_current_offset(self, server_factory):
        """WAIT asks for acknowledgements only when a write is outstanding.

        With nothing written since the last ack every replica is already
        current, so WAIT answers immediately - the SET below is what creates
        something to wait for.
        """
        master = server_factory()
        fake_replica = RespClient("127.0.0.1", master.port, timeout=10.0)
        try:
            _complete_handshake(fake_replica)

            with master.client() as master_client:
                master_client.settimeout(10.0)
                master_client.command("SET", "k", "v")

                propagated = fake_replica.read_reply()
                assert propagated == [b"SET", b"k", b"v"]
                # The master expects an offset counted in wire bytes.
                offset = len(encode_command("SET", "k", "v"))

                master_client.send_command("WAIT", 1, 3000)
                assert fake_replica.read_reply() == [b"REPLCONF", b"GETACK", b"*"]

                fake_replica.send_raw(encode_command("REPLCONF", "ACK", str(offset)))
                assert master_client.read_reply() == 1
        finally:
            fake_replica.close()

    def test_an_ack_short_of_the_target_does_not_satisfy_wait(self, server_factory):
        master = server_factory()
        fake_replica = RespClient("127.0.0.1", master.port, timeout=10.0)
        try:
            _complete_handshake(fake_replica)

            with master.client() as master_client:
                master_client.settimeout(10.0)
                master_client.command("SET", "k", "v")
                fake_replica.read_reply()

                master_client.send_command("WAIT", 1, 700)
                assert fake_replica.read_reply() == [b"REPLCONF", b"GETACK", b"*"]

                # Acknowledge less than was sent: the replica is behind.
                fake_replica.send_raw(encode_command("REPLCONF", "ACK", "1"))
                assert master_client.read_reply() == 0
        finally:
            fake_replica.close()

    def test_replconf_with_an_unknown_option(self, server_factory):
        server = server_factory()
        with server.client() as connection:
            assert connection.command("REPLCONF", "bogus", "1").startswith(
                "ERR Unrecognized REPLCONF option"
            )


class TestReplicaOffsets:
    def test_the_replica_tracks_the_byte_offset(self, master_and_replica):
        _, _, master_client, replica_client = master_and_replica
        assert int(info_field(replica_client, "replication", "slave_repl_offset")) >= 0

        master_client.command("SET", "k", "v")
        eventually(lambda: replica_client.command("GET", "k") == b"v")
        eventually(lambda: int(info_field(replica_client, "replication", "slave_repl_offset")) > 0)

    def test_master_and_replica_offsets_converge(self, master_and_replica):
        _, _, master_client, replica_client = master_and_replica
        for index in range(10):
            master_client.command("SET", f"k{index}", index)
        master_client.settimeout(15.0)
        master_client.command("WAIT", 1, 5000)

        master_offset = int(info_field(master_client, "replication", "master_repl_offset"))
        eventually(
            lambda: (
                int(info_field(replica_client, "replication", "slave_repl_offset")) == master_offset
            )
        )


class TestLinkResilience:
    def test_the_replica_reconnects_after_the_master_restarts(self, server_factory, tmp_path):
        master_dir = tmp_path / "master"
        master_dir.mkdir()
        master = server_factory(directory=master_dir)
        master_port = master.port

        replica = server_factory("--replicaof", f"localhost {master_port}")
        replica_client = replica.client(timeout=30.0)
        try:
            with master.client() as master_client:
                master_client.command("SET", "before", "1")
            eventually(lambda: replica_client.command("GET", "before") == b"1")

            master.stop()
            eventually(
                lambda: info_field(replica_client, "replication", "master_link_status") == "down"
            )

            restarted = server_factory(directory=master_dir, port=master_port)
            with restarted.client() as master_client:
                master_client.command("SET", "after", "2")

            eventually(
                lambda: info_field(replica_client, "replication", "master_link_status") == "up",
                timeout=30.0,
            )
            eventually(lambda: replica_client.command("GET", "after") == b"2", timeout=30.0)
        finally:
            replica_client.close()

    def test_a_detached_replica_is_removed_from_info(self, server_factory):
        master = server_factory()
        replica = server_factory("--replicaof", f"localhost {master.port}")
        with master.client() as master_client:
            eventually(
                lambda: int(info_field(master_client, "replication", "connected_slaves")) == 1
            )
            replica.stop()
            eventually(lambda: info_field(master_client, "replication", "connected_slaves") == "0")
