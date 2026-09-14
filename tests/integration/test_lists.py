"""End-to-end tests for list commands, including the blocking pops."""

from __future__ import annotations

import time

import pytest

from tests.integration.client import Error, Status

pytestmark = pytest.mark.integration


class TestPushAndRead:
    def test_rpush_appends(self, client):
        assert client.command("RPUSH", "l", "a") == 1
        assert client.command("RPUSH", "l", "b", "c") == 3
        assert client.command("LRANGE", "l", 0, -1) == [b"a", b"b", b"c"]

    def test_lpush_prepends_one_element_at_a_time(self, client):
        assert client.command("LPUSH", "l", "a", "b", "c") == 3
        assert client.command("LRANGE", "l", 0, -1) == [b"c", b"b", b"a"]

    def test_llen(self, client):
        assert client.command("LLEN", "missing") == 0
        client.command("RPUSH", "l", "a", "b")
        assert client.command("LLEN", "l") == 2

    def test_lrange_on_a_missing_key(self, client):
        assert client.command("LRANGE", "missing", 0, -1) == []

    def test_lrange_subranges(self, client):
        client.command("RPUSH", "l", "a", "b", "c", "d", "e")
        assert client.command("LRANGE", "l", 1, 3) == [b"b", b"c", b"d"]
        assert client.command("LRANGE", "l", 0, 0) == [b"a"]
        assert client.command("LRANGE", "l", -2, -1) == [b"d", b"e"]
        assert client.command("LRANGE", "l", -100, 100) == [b"a", b"b", b"c", b"d", b"e"]
        assert client.command("LRANGE", "l", 3, 1) == []
        assert client.command("LRANGE", "l", 10, 20) == []

    def test_lindex(self, client):
        client.command("RPUSH", "l", "a", "b", "c")
        assert client.command("LINDEX", "l", 0) == b"a"
        assert client.command("LINDEX", "l", -1) == b"c"
        assert client.command("LINDEX", "l", 99) is None
        assert client.command("LINDEX", "missing", 0) is None

    def test_lset(self, client):
        client.command("RPUSH", "l", "a", "b")
        assert client.command("LSET", "l", 0, "z") == Status("OK")
        assert client.command("LRANGE", "l", 0, -1) == [b"z", b"b"]

    def test_lset_out_of_range(self, client):
        client.command("RPUSH", "l", "a")
        assert client.command("LSET", "l", 5, "z") == Error("ERR index out of range")

    def test_lset_on_missing_key(self, client):
        assert client.command("LSET", "missing", 0, "z") == Error("ERR no such key")


class TestPop:
    def test_lpop_and_rpop(self, client):
        client.command("RPUSH", "l", "a", "b", "c")
        assert client.command("LPOP", "l") == b"a"
        assert client.command("RPOP", "l") == b"c"
        assert client.command("LRANGE", "l", 0, -1) == [b"b"]

    def test_pop_from_a_missing_key(self, client):
        assert client.command("LPOP", "missing") is None
        assert client.command("RPOP", "missing") is None

    def test_pop_with_count_returns_an_array(self, client):
        client.command("RPUSH", "l", "a", "b", "c")
        assert client.command("LPOP", "l", 2) == [b"a", b"b"]

    def test_pop_with_count_larger_than_the_list(self, client):
        client.command("RPUSH", "l", "a")
        assert client.command("LPOP", "l", 5) == [b"a"]

    def test_pop_with_count_zero(self, client):
        client.command("RPUSH", "l", "a")
        assert client.command("LPOP", "l", 0) == []

    def test_pop_with_count_on_a_missing_key_is_a_null_array(self, client):
        # A null array, not a null bulk string: the reply shape follows the
        # presence of the count argument.
        client.send_command("LPOP", "missing", 2)
        assert client.read_raw(5) == b"*-1\r\n"

    def test_negative_count_is_rejected(self, client):
        client.command("RPUSH", "l", "a")
        assert client.command("LPOP", "l", -1) == Error(
            "ERR value is out of range, must be positive"
        )

    def test_emptied_list_is_removed(self, client):
        client.command("RPUSH", "l", "a")
        client.command("LPOP", "l")
        assert client.command("EXISTS", "l") == 0
        assert client.command("TYPE", "l") == Status("none")


class TestBlockingPop:
    def test_blpop_returns_immediately_when_data_exists(self, client):
        client.command("RPUSH", "l", "a")
        assert client.command("BLPOP", "l", 0) == [b"l", b"a"]

    def test_brpop_takes_from_the_tail(self, client):
        client.command("RPUSH", "l", "a", "b")
        assert client.command("BRPOP", "l", 0) == [b"l", b"b"]

    def test_blpop_checks_keys_in_order(self, client):
        client.command("RPUSH", "second", "v")
        assert client.command("BLPOP", "first", "second", 0) == [b"second", b"v"]

    @pytest.mark.slow
    def test_blpop_times_out_with_a_null_array(self, client):
        client.settimeout(5.0)
        started = time.monotonic()
        client.send_command("BLPOP", "nothing", "0.2")
        assert client.read_raw(5) == b"*-1\r\n"
        assert 0.15 < time.monotonic() - started < 3.0

    @pytest.mark.slow
    def test_blpop_wakes_when_another_client_pushes(self, client, second_client):
        client.settimeout(5.0)
        client.send_command("BLPOP", "queue", 0)
        time.sleep(0.2)  # make sure the pop is actually blocked
        second_client.command("RPUSH", "queue", "job")
        assert client.read_reply() == [b"queue", b"job"]

    @pytest.mark.slow
    def test_blocking_client_does_not_stall_the_server(self, client, second_client):
        client.settimeout(5.0)
        client.send_command("BLPOP", "queue", 0)
        time.sleep(0.1)
        # The event loop must still be serving everyone else.
        for _ in range(10):
            assert second_client.command("PING") == Status("PONG")
        second_client.command("RPUSH", "queue", "job")
        assert client.read_reply() == [b"queue", b"job"]

    @pytest.mark.slow
    def test_two_blocked_clients_are_served_in_order(self, shared_server):
        first = shared_server.client(timeout=5.0)
        second = shared_server.client(timeout=5.0)
        pusher = shared_server.client()
        try:
            pusher.command("DEL", "queue")
            first.send_command("BLPOP", "queue", 0)
            time.sleep(0.2)
            second.send_command("BLPOP", "queue", 0)
            time.sleep(0.2)

            pusher.command("RPUSH", "queue", "one")
            assert first.read_reply() == [b"queue", b"one"]
            pusher.command("RPUSH", "queue", "two")
            assert second.read_reply() == [b"queue", b"two"]
        finally:
            for connection in (first, second, pusher):
                connection.close()

    @pytest.mark.slow
    def test_timeout_is_not_restarted_by_a_spurious_wake_up(self, shared_server):
        """Repeated wake-ups that yield nothing must not extend the deadline.

        The churn below touches the key - waking the blocked client each time -
        without ever leaving a list for it to pop, which is exactly the spurious
        wake-up the retry loop has to survive.
        """
        waiter = shared_server.client(timeout=10.0)
        churner = shared_server.client()
        try:
            churner.command("DEL", "queue")
            waiter.send_command("BLPOP", "queue", "1")
            time.sleep(0.1)

            started = time.monotonic()
            for _ in range(8):
                churner.command("SET", "queue", "not-a-list")
                churner.command("DEL", "queue")
                time.sleep(0.05)

            assert waiter.read_raw(5) == b"*-1\r\n"
            elapsed = time.monotonic() - started
            assert elapsed < 2.5, f"timeout appears to have been restarted ({elapsed:.2f}s)"
        finally:
            for connection in (waiter, churner):
                connection.close()

    def test_blpop_on_an_existing_wrong_type_errors_immediately(self, client):
        client.command("SET", "k", "string")
        assert client.command("BLPOP", "k", 0).startswith("WRONGTYPE")

    @pytest.mark.slow
    def test_a_blocked_client_is_not_served_by_a_non_list_value(self, shared_server):
        """Redis only ever serves a blocked BLPOP from a list."""
        waiter = shared_server.client(timeout=10.0)
        other = shared_server.client()
        try:
            other.command("DEL", "queue")
            waiter.send_command("BLPOP", "queue", 0)
            time.sleep(0.2)

            other.command("SET", "queue", "string")
            time.sleep(0.2)
            other.command("DEL", "queue")
            other.command("RPUSH", "queue", "job")

            assert waiter.read_reply() == [b"queue", b"job"]
        finally:
            for connection in (waiter, other):
                connection.close()

    def test_negative_timeout_is_rejected(self, client):
        assert client.command("BLPOP", "l", -1) == Error("ERR timeout is negative")

    def test_non_numeric_timeout_is_rejected(self, client):
        assert client.command("BLPOP", "l", "abc") == Error(
            "ERR timeout is not a float or out of range"
        )
