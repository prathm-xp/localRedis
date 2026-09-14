"""End-to-end tests for stream commands."""

from __future__ import annotations

import time

import pytest

from tests.integration.client import Error, Status

pytestmark = pytest.mark.integration


class TestXadd:
    def test_explicit_id(self, client):
        assert client.command("XADD", "s", "1-1", "f", "v") == b"1-1"
        assert client.command("TYPE", "s") == Status("stream")

    def test_ids_must_increase(self, client):
        client.command("XADD", "s", "5-5", "f", "v")
        assert client.command("XADD", "s", "5-5", "f", "v") == Error(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        )
        assert client.command("XADD", "s", "4-0", "f", "v") == Error(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        )
        assert client.command("XADD", "s", "5-6", "f", "v") == b"5-6"

    def test_zero_id_is_rejected(self, client):
        assert client.command("XADD", "s", "0-0", "f", "v") == Error(
            "ERR The ID specified in XADD must be greater than 0-0"
        )

    def test_auto_sequence(self, client):
        assert client.command("XADD", "s", "5-*", "f", "v") == b"5-0"
        assert client.command("XADD", "s", "5-*", "f", "v") == b"5-1"
        assert client.command("XADD", "s", "6-*", "f", "v") == b"6-0"

    def test_auto_sequence_for_millisecond_zero_starts_at_one(self, client):
        # 0-0 is reserved, so the 0-* case has to begin at 0-1.
        assert client.command("XADD", "s", "0-*", "f", "v") == b"0-1"

    def test_fully_automatic_id_uses_the_clock(self, client):
        before = int(time.time() * 1000)
        generated = client.command("XADD", "s", "*", "f", "v").decode()
        after = int(time.time() * 1000)
        milliseconds, sequence = (int(part) for part in generated.split("-"))
        assert before <= milliseconds <= after
        assert sequence == 0

    def test_automatic_ids_are_strictly_increasing(self, client):
        # Several entries inside one millisecond must still get distinct IDs.
        generated = [client.command("XADD", "s", "*", "f", str(i)) for i in range(50)]
        assert len(set(generated)) == 50
        parsed = [tuple(int(p) for p in g.decode().split("-")) for g in generated]
        assert parsed == sorted(parsed)

    def test_partial_id_defaults_the_sequence_to_zero(self, client):
        assert client.command("XADD", "s", "7", "f", "v") == b"7-0"

    def test_multiple_fields(self, client):
        client.command("XADD", "s", "1-1", "a", "1", "b", "2")
        entries = client.command("XRANGE", "s", "-", "+")
        assert entries == [[b"1-1", [b"a", b"1", b"b", b"2"]]]

    def test_odd_number_of_fields_is_rejected(self, client):
        assert client.command("XADD", "s", "1-1", "a", "1", "b") == Error(
            "ERR wrong number of arguments for 'xadd' command"
        )

    def test_malformed_id(self, client):
        assert client.command("XADD", "s", "abc", "f", "v") == Error(
            "ERR Invalid stream ID specified as stream command argument"
        )


class TestXlenAndXrange:
    @pytest.fixture
    def populated(self, client):
        for index in range(1, 6):
            client.command("XADD", "s", f"{index}-0", "n", str(index))
        return client

    def test_xlen(self, client):
        assert client.command("XLEN", "missing") == 0
        client.command("XADD", "s", "1-1", "f", "v")
        assert client.command("XLEN", "s") == 1

    def test_xrange_full(self, populated):
        entries = populated.command("XRANGE", "s", "-", "+")
        assert [entry[0] for entry in entries] == [b"1-0", b"2-0", b"3-0", b"4-0", b"5-0"]

    def test_xrange_is_inclusive(self, populated):
        entries = populated.command("XRANGE", "s", "2-0", "4-0")
        assert [entry[0] for entry in entries] == [b"2-0", b"3-0", b"4-0"]

    def test_xrange_with_partial_ids(self, populated):
        entries = populated.command("XRANGE", "s", "2", "3")
        assert [entry[0] for entry in entries] == [b"2-0", b"3-0"]

    def test_xrange_with_count(self, populated):
        entries = populated.command("XRANGE", "s", "-", "+", "COUNT", 2)
        assert [entry[0] for entry in entries] == [b"1-0", b"2-0"]

    def test_xrange_exclusive_bounds(self, populated):
        entries = populated.command("XRANGE", "s", "(2-0", "(4-0")
        assert [entry[0] for entry in entries] == [b"3-0"]

    def test_xrange_on_a_missing_key(self, client):
        assert client.command("XRANGE", "missing", "-", "+") == []

    def test_xrange_outside_the_data(self, populated):
        assert populated.command("XRANGE", "s", "10", "20") == []

    def test_xrevrange(self, populated):
        entries = populated.command("XREVRANGE", "s", "+", "-")
        assert [entry[0] for entry in entries] == [b"5-0", b"4-0", b"3-0", b"2-0", b"1-0"]

    def test_xrevrange_with_count(self, populated):
        entries = populated.command("XREVRANGE", "s", "+", "-", "COUNT", 2)
        assert [entry[0] for entry in entries] == [b"5-0", b"4-0"]

    def test_bad_option_is_a_syntax_error(self, populated):
        assert populated.command("XRANGE", "s", "-", "+", "LIMIT", 2) == Error("ERR syntax error")


class TestXread:
    def test_reads_entries_after_an_id(self, client):
        client.command("XADD", "s", "1-0", "n", "1")
        client.command("XADD", "s", "2-0", "n", "2")
        reply = client.command("XREAD", "STREAMS", "s", "1-0")
        assert reply == [[b"s", [[b"2-0", [b"n", b"2"]]]]]

    def test_is_exclusive_of_the_given_id(self, client):
        client.command("XADD", "s", "1-0", "n", "1")
        assert client.command("XREAD", "STREAMS", "s", "1-0") is None

    def test_reads_from_zero(self, client):
        client.command("XADD", "s", "1-0", "n", "1")
        reply = client.command("XREAD", "STREAMS", "s", "0")
        assert reply == [[b"s", [[b"1-0", [b"n", b"1"]]]]]

    def test_multiple_streams(self, client):
        client.command("XADD", "a", "1-0", "n", "1")
        client.command("XADD", "b", "1-0", "n", "2")
        reply = client.command("XREAD", "STREAMS", "a", "b", "0", "0")
        assert [stream[0] for stream in reply] == [b"a", b"b"]

    def test_streams_with_no_new_entries_are_omitted(self, client):
        client.command("XADD", "a", "1-0", "n", "1")
        client.command("XADD", "b", "1-0", "n", "2")
        reply = client.command("XREAD", "STREAMS", "a", "b", "0", "1-0")
        assert [stream[0] for stream in reply] == [b"a"]

    def test_with_count(self, client):
        for index in range(1, 6):
            client.command("XADD", "s", f"{index}-0", "n", str(index))
        reply = client.command("XREAD", "COUNT", 2, "STREAMS", "s", "0")
        assert len(reply[0][1]) == 2

    def test_nothing_to_read_is_a_null_array(self, client):
        client.command("XADD", "s", "1-0", "n", "1")
        client.send_command("XREAD", "STREAMS", "s", "5-0")
        assert client.read_raw(5) == b"*-1\r\n"

    def test_unbalanced_keys_and_ids(self, client):
        reply = client.command("XREAD", "STREAMS", "a", "b", "0")
        assert reply.startswith("ERR Unbalanced XREAD")

    def test_missing_streams_keyword(self, client):
        assert client.command("XREAD", "COUNT", 2, "s", "0").startswith("ERR syntax error")


@pytest.mark.slow
class TestXreadBlocking:
    def test_returns_immediately_when_entries_exist(self, client):
        client.command("XADD", "s", "1-0", "n", "1")
        reply = client.command("XREAD", "BLOCK", 0, "STREAMS", "s", "0")
        assert reply[0][0] == b"s"

    def test_blocks_until_an_entry_arrives(self, client, second_client):
        client.settimeout(5.0)
        client.command("XADD", "s", "1-0", "n", "1")
        client.send_command("XREAD", "BLOCK", 0, "STREAMS", "s", "1-0")
        time.sleep(0.2)
        second_client.command("XADD", "s", "2-0", "n", "2")
        assert client.read_reply() == [[b"s", [[b"2-0", [b"n", b"2"]]]]]

    def test_block_timeout_returns_a_null_array(self, client):
        client.settimeout(5.0)
        client.command("XADD", "s", "1-0", "n", "1")
        started = time.monotonic()
        client.send_command("XREAD", "BLOCK", 200, "STREAMS", "s", "1-0")
        assert client.read_raw(5) == b"*-1\r\n"
        assert 0.15 < time.monotonic() - started < 3.0

    def test_dollar_reads_only_entries_added_after_the_call(self, client, second_client):
        client.settimeout(5.0)
        client.command("XADD", "s", "1-0", "n", "old")
        client.send_command("XREAD", "BLOCK", 0, "STREAMS", "s", "$")
        time.sleep(0.2)
        second_client.command("XADD", "s", "2-0", "n", "new")
        reply = client.read_reply()
        assert reply == [[b"s", [[b"2-0", [b"n", b"new"]]]]]

    def test_dollar_on_a_missing_stream(self, client, second_client):
        client.settimeout(5.0)
        client.send_command("XREAD", "BLOCK", 0, "STREAMS", "fresh", "$")
        time.sleep(0.2)
        second_client.command("XADD", "fresh", "1-0", "n", "1")
        assert client.read_reply() == [[b"fresh", [[b"1-0", [b"n", b"1"]]]]]

    def test_blocked_reader_does_not_stall_the_server(self, client, second_client):
        client.settimeout(5.0)
        client.send_command("XREAD", "BLOCK", 0, "STREAMS", "s", "$")
        time.sleep(0.1)
        for _ in range(10):
            assert second_client.command("PING") == Status("PONG")
        second_client.command("XADD", "s", "1-0", "n", "1")
        assert client.read_reply()[0][0] == b"s"

    def test_negative_block_timeout_is_rejected(self, client):
        assert client.command("XREAD", "BLOCK", -1, "STREAMS", "s", "0") == Error(
            "ERR timeout is negative"
        )
