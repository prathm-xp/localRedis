"""End-to-end tests for MULTI/EXEC/DISCARD and WATCH."""

from __future__ import annotations

import pytest

from tests.integration.client import Error, Status

pytestmark = pytest.mark.integration


class TestMultiExec:
    def test_commands_are_queued(self, client):
        assert client.command("MULTI") == Status("OK")
        assert client.command("SET", "k", "v") == Status("QUEUED")
        assert client.command("GET", "k") == Status("QUEUED")

    def test_queued_commands_do_not_run_until_exec(self, client, second_client):
        client.command("MULTI")
        client.command("SET", "k", "v")
        assert second_client.command("GET", "k") is None
        client.command("EXEC")
        assert second_client.command("GET", "k") == b"v"

    def test_exec_returns_every_reply(self, client):
        client.command("MULTI")
        client.command("SET", "k", "v")
        client.command("INCR", "counter")
        client.command("GET", "k")
        assert client.command("EXEC") == [Status("OK"), 1, b"v"]

    def test_empty_transaction(self, client):
        client.command("MULTI")
        assert client.command("EXEC") == []

    def test_connection_leaves_transaction_mode_after_exec(self, client):
        client.command("MULTI")
        client.command("EXEC")
        assert client.command("SET", "k", "v") == Status("OK")

    def test_discard(self, client):
        client.command("MULTI")
        client.command("SET", "k", "v")
        assert client.command("DISCARD") == Status("OK")
        assert client.command("EXISTS", "k") == 0
        assert client.command("SET", "other", "v") == Status("OK")

    def test_exec_without_multi(self, client):
        assert client.command("EXEC") == Error("ERR EXEC without MULTI")

    def test_discard_without_multi(self, client):
        assert client.command("DISCARD") == Error("ERR DISCARD without MULTI")

    def test_multi_cannot_nest(self, client):
        client.command("MULTI")
        assert client.command("MULTI") == Error("ERR MULTI calls can not be nested")
        client.command("DISCARD")

    def test_transactions_are_per_connection(self, client, second_client):
        client.command("MULTI")
        # The other connection is unaffected and executes normally.
        assert second_client.command("SET", "k", "v") == Status("OK")
        client.command("DISCARD")


class TestTransactionErrors:
    def test_unknown_command_aborts_the_transaction(self, client):
        client.command("MULTI")
        assert client.command("NOSUCHCOMMAND").startswith("ERR unknown command")
        assert client.command("SET", "k", "v") == Status("QUEUED")
        assert client.command("EXEC") == Error(
            "EXECABORT Transaction discarded because of previous errors."
        )
        assert client.command("EXISTS", "k") == 0

    def test_bad_arity_aborts_the_transaction(self, client):
        client.command("MULTI")
        assert client.command("GET").startswith("ERR wrong number of arguments")
        assert client.command("EXEC").startswith("EXECABORT")

    def test_abort_leaves_transaction_mode(self, client):
        client.command("MULTI")
        client.command("NOSUCHCOMMAND")
        client.command("EXEC")
        assert client.command("PING") == Status("PONG")

    def test_a_runtime_error_does_not_abort_the_rest(self, client):
        # Redis has no rollback: the commands around the failure still apply.
        client.command("SET", "text", "not-a-number")
        client.command("MULTI")
        client.command("SET", "before", "1")
        client.command("INCR", "text")
        client.command("SET", "after", "1")
        replies = client.command("EXEC")

        assert replies[0] == Status("OK")
        assert replies[1] == Error("ERR value is not an integer or out of range")
        assert replies[2] == Status("OK")
        assert client.command("GET", "before") == b"1"
        assert client.command("GET", "after") == b"1"


class TestWatch:
    def test_exec_succeeds_when_nothing_changed(self, client):
        client.command("SET", "k", "1")
        assert client.command("WATCH", "k") == Status("OK")
        client.command("MULTI")
        client.command("INCR", "k")
        assert client.command("EXEC") == [2]

    def test_exec_aborts_when_a_watched_key_changes(self, client, second_client):
        client.command("SET", "k", "1")
        client.command("WATCH", "k")
        second_client.command("SET", "k", "99")

        client.command("MULTI")
        client.command("INCR", "k")
        client.send_command("EXEC")
        # A nil reply, which for EXEC is a null array.
        assert client.read_raw(5) == b"*-1\r\n"
        assert client.command("GET", "k") == b"99"

    def test_watch_notices_a_deletion(self, client, second_client):
        client.command("SET", "k", "1")
        client.command("WATCH", "k")
        second_client.command("DEL", "k")
        client.command("MULTI")
        client.command("SET", "k", "2")
        client.send_command("EXEC")
        assert client.read_raw(5) == b"*-1\r\n"

    def test_watching_a_key_this_connection_changes_also_aborts(self, client):
        # Redis does not special-case the watcher's own writes.
        client.command("SET", "k", "1")
        client.command("WATCH", "k")
        client.command("SET", "k", "2")
        client.command("MULTI")
        client.command("GET", "k")
        client.send_command("EXEC")
        assert client.read_raw(5) == b"*-1\r\n"

    def test_unwatch_clears_the_watch(self, client, second_client):
        client.command("SET", "k", "1")
        client.command("WATCH", "k")
        second_client.command("SET", "k", "99")
        assert client.command("UNWATCH") == Status("OK")
        client.command("MULTI")
        client.command("GET", "k")
        assert client.command("EXEC") == [b"99"]

    def test_exec_releases_the_watches(self, client, second_client):
        client.command("SET", "k", "1")
        client.command("WATCH", "k")
        client.command("MULTI")
        client.command("PING")
        client.command("EXEC")

        # The watch is gone, so a later change cannot abort the next transaction.
        second_client.command("SET", "k", "2")
        client.command("MULTI")
        client.command("GET", "k")
        assert client.command("EXEC") == [b"2"]

    def test_watching_multiple_keys(self, client, second_client):
        client.command("MSET", "a", "1", "b", "2")
        client.command("WATCH", "a", "b")
        second_client.command("SET", "b", "99")
        client.command("MULTI")
        client.command("GET", "a")
        client.send_command("EXEC")
        assert client.read_raw(5) == b"*-1\r\n"

    def test_watch_inside_multi_is_rejected(self, client):
        client.command("MULTI")
        assert client.command("WATCH", "k") == Error("ERR WATCH inside MULTI is not allowed")
        client.command("DISCARD")

    def test_an_unrelated_key_does_not_abort(self, client, second_client):
        client.command("MSET", "watched", "1", "other", "1")
        client.command("WATCH", "watched")
        second_client.command("SET", "other", "99")
        client.command("MULTI")
        client.command("GET", "watched")
        assert client.command("EXEC") == [b"1"]


class TestCommandsForbiddenInTransactions:
    """Regression tests for commands that write their own output.

    SUBSCRIBE sends one confirmation per channel rather than returning a reply.
    Queued inside MULTI it would emit those bytes in the middle of the EXEC
    array, producing a reply no client could parse. Redis rejects the command at
    queue time instead, and so does this server.
    """

    def test_subscribe_inside_multi_is_rejected(self, client):
        client.command("MULTI")
        assert client.command("SUBSCRIBE", "news") == Error(
            "ERR SUBSCRIBE is not allowed in transactions"
        )
        client.command("DISCARD")

    def test_rejection_poisons_the_transaction(self, client):
        client.command("MULTI")
        client.command("SUBSCRIBE", "news")
        assert client.command("SET", "k", "v") == Status("QUEUED")
        assert client.command("EXEC").startswith("EXECABORT")
        assert client.command("EXISTS", "k") == 0

    def test_the_connection_is_still_usable_afterwards(self, client):
        client.command("MULTI")
        client.command("SUBSCRIBE", "news")
        client.command("EXEC")
        assert client.command("PING") == Status("PONG")
        assert client.command("SET", "k", "v") == Status("OK")

    def test_the_connection_did_not_actually_subscribe(self, client, second_client):
        client.command("MULTI")
        client.command("SUBSCRIBE", "news")
        client.command("DISCARD")
        assert second_client.command("PUBLISH", "news", "hello") == 0

    @pytest.mark.parametrize(
        ("command", "args"),
        [
            ("SUBSCRIBE", ("news",)),
            ("UNSUBSCRIBE", ()),
            ("PSUBSCRIBE", ("news.*",)),
            ("PUNSUBSCRIBE", ()),
            ("PSYNC", ("?", "-1")),
            ("REPLCONF", ("listening-port", "1234")),
        ],
    )
    def test_every_self_writing_command_is_rejected(self, client, command, args):
        client.command("MULTI")
        reply = client.command(command, *args)
        assert reply == Error(f"ERR {command} is not allowed in transactions")
        client.command("DISCARD")

    def test_exec_replies_stay_parseable(self, client):
        """The whole point: the EXEC array must remain well-formed RESP."""
        client.command("MULTI")
        client.command("SET", "a", "1")
        client.command("SUBSCRIBE", "news")
        client.command("SET", "b", "2")
        assert client.command("EXEC").startswith("EXECABORT")
        assert client.command("PING") == Status("PONG")
