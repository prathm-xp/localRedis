"""Negative and edge-case tests at the wire level.

These bypass the convenience of ``RespClient.command`` and write raw bytes, so
they exercise the framing directly: partial deliveries, malformed requests and
abrupt disconnects. A server is only as robust as its behaviour here.
"""

from __future__ import annotations

import socket
import time

import pytest

from tests.integration.client import Error, RespClient, Status

pytestmark = pytest.mark.integration


class TestPartialDelivery:
    def test_command_split_across_packets(self, client):
        client.send_raw(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n")
        time.sleep(0.1)
        client.send_raw(b"$5\r\nhello\r\n")
        assert client.read_reply() == Status("OK")
        assert client.command("GET", "k") == b"hello"

    def test_command_delivered_one_byte_at_a_time(self, client):
        payload = b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n"
        for index in range(len(payload)):
            client.send_raw(payload[index : index + 1])
        assert client.read_reply() == b"hello"

    def test_header_split_from_body(self, client):
        client.send_raw(b"*2\r\n$4\r\nECHO\r\n$11\r\n")
        time.sleep(0.1)
        client.send_raw(b"hello world\r\n")
        assert client.read_reply() == b"hello world"

    def test_two_commands_arriving_in_one_packet(self, client):
        client.send_raw(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n")
        assert client.read_reply() == Status("PONG")
        assert client.read_reply() == Status("PONG")

    def test_a_packet_ending_mid_command(self, client):
        client.send_raw(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPI")
        assert client.read_reply() == Status("PONG")
        client.send_raw(b"NG\r\n")
        assert client.read_reply() == Status("PONG")


class TestEmptyAndNoOpRequests:
    def test_empty_multibulk_is_ignored(self, client):
        client.send_raw(b"*0\r\n")
        assert client.command("PING") == Status("PONG")

    def test_negative_multibulk_is_ignored(self, client):
        client.send_raw(b"*-1\r\n")
        assert client.command("PING") == Status("PONG")

    def test_blank_inline_line_is_ignored(self, client):
        client.send_raw(b"\r\n")
        assert client.command("PING") == Status("PONG")

    def test_many_blank_lines(self, client):
        client.send_raw(b"\r\n" * 20)
        assert client.command("PING") == Status("PONG")


class TestMalformedRequests:
    """Redis reports a protocol error and closes: framing cannot be recovered."""

    def _expect_error_then_close(self, connection: RespClient, fragment: str) -> None:
        reply = connection.read_reply()
        assert isinstance(reply, Error), f"expected an error, got {reply!r}"
        assert fragment in reply, f"{fragment!r} not in {reply!r}"
        assert connection.read_until_eof() == b"", "server should close after a protocol error"

    def test_invalid_multibulk_length(self, shared_server):
        with shared_server.client() as connection:
            connection.send_raw(b"*abc\r\n")
            self._expect_error_then_close(connection, "invalid multibulk length")

    def test_element_not_prefixed_with_dollar(self, shared_server):
        with shared_server.client() as connection:
            connection.send_raw(b"*1\r\n+PING\r\n")
            self._expect_error_then_close(connection, "expected '$'")

    def test_invalid_bulk_length(self, shared_server):
        with shared_server.client() as connection:
            connection.send_raw(b"*1\r\n$abc\r\n")
            self._expect_error_then_close(connection, "invalid bulk length")

    def test_negative_bulk_length(self, shared_server):
        with shared_server.client() as connection:
            connection.send_raw(b"*1\r\n$-1\r\n")
            self._expect_error_then_close(connection, "invalid bulk length")

    def test_bulk_length_beyond_the_limit(self, shared_server):
        with shared_server.client() as connection:
            connection.send_raw(b"*1\r\n$999999999999\r\n")
            self._expect_error_then_close(connection, "invalid bulk length")

    def test_multibulk_count_beyond_the_limit(self, shared_server):
        with shared_server.client() as connection:
            connection.send_raw(b"*99999999\r\n")
            self._expect_error_then_close(connection, "invalid multibulk length")

    def test_missing_crlf_after_a_bulk_body(self, shared_server):
        with shared_server.client() as connection:
            connection.send_raw(b"*1\r\n$4\r\nPINGXX")
            self._expect_error_then_close(connection, "expected CRLF")

    def test_unbalanced_quotes_inline(self, shared_server):
        with shared_server.client() as connection:
            connection.send_raw(b'ECHO "unterminated\r\n')
            self._expect_error_then_close(connection, "unbalanced quotes")

    def test_oversized_inline_request(self, shared_server):
        with shared_server.client() as connection:
            connection.send_raw(b"A" * 70_000)
            self._expect_error_then_close(connection, "too big inline request")

    def test_binary_garbage(self, shared_server):
        with shared_server.client() as connection:
            connection.send_raw(bytes([0xFF, 0xFE, 0x00, 0x01]) + b"\r\n")
            # Parsed as an inline command; the name is simply unknown.
            reply = connection.read_reply()
            assert isinstance(reply, Error)
            assert "unknown command" in reply

    def test_the_server_survives_every_malformed_request(self, shared_server, client):
        # The connections above were all closed; the server itself must be fine.
        assert client.command("PING") == Status("PONG")


class TestDisconnects:
    def test_disconnect_mid_command(self, shared_server, client):
        connection = shared_server.client()
        connection.send_raw(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhel")
        connection.close()
        time.sleep(0.2)
        assert client.command("PING") == Status("PONG")
        assert client.command("EXISTS", "k") == 0

    def test_abrupt_reset(self, shared_server, client):
        raw = socket.create_connection(("127.0.0.1", shared_server.port))
        raw.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\x01\x00\x00\x00\x00\x00\x00\x00")
        raw.sendall(b"*1\r\n$4\r\nPING\r\n")
        raw.close()  # sends RST rather than FIN
        time.sleep(0.2)
        assert client.command("PING") == Status("PONG")

    def test_connect_and_disconnect_without_sending_anything(self, shared_server, client):
        for _ in range(20):
            RespClient("127.0.0.1", shared_server.port).close()
        assert client.command("PING") == Status("PONG")

    def test_client_that_never_reads_replies(self, shared_server, client):
        connection = shared_server.client()
        try:
            for index in range(500):
                connection.send_command("SET", f"k{index}", "v")
            assert client.command("PING") == Status("PONG")
        finally:
            connection.close()


class TestLargeRequests:
    def test_large_argument(self, client):
        payload = b"y" * (4 * 1024 * 1024)
        assert client.command("SET", "big", payload) == Status("OK")
        assert client.command("GET", "big") == payload

    def test_many_arguments(self, client):
        pairs = []
        for index in range(1000):
            pairs += [f"field{index}", str(index)]
        assert client.command("MSET", *pairs) == Status("OK")
        assert client.command("GET", "field999") == b"999"

    def test_deep_pipeline(self, client):
        for index in range(2000):
            client.send_command("SET", f"p{index}", index)
        for _ in range(2000):
            assert client.read_reply() == Status("OK")
        assert client.command("DBSIZE") == 2000


class TestInlineCommands:
    def test_inline_set_and_get(self, client):
        client.send_raw(b"SET inline value\r\n")
        assert client.read_reply() == Status("OK")
        client.send_raw(b"GET inline\r\n")
        assert client.read_reply() == b"value"

    def test_inline_accepts_a_bare_newline(self, client):
        client.send_raw(b"PING\n")
        assert client.read_reply() == Status("PONG")

    def test_inline_hex_escape(self, client):
        client.send_raw(b'SET k "\\x41\\x42"\r\n')
        assert client.read_reply() == Status("OK")
        assert client.command("GET", "k") == b"AB"

    def test_inline_and_multibulk_can_be_mixed(self, client):
        client.send_raw(b"PING\r\n*1\r\n$4\r\nPING\r\n")
        assert client.read_reply() == Status("PONG")
        assert client.read_reply() == Status("PONG")
