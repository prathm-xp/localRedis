"""Unit tests for the incremental RESP request parser.

The parser is the component most exposed to hostile input, so the negative
cases here are as important as the happy path.
"""

from __future__ import annotations

import pytest

from pyredis.errors import ProtocolError
from pyredis.protocol import ParsedCommand, ParserLimits, RespReader, split_inline_args


def read_one(reader: RespReader) -> ParsedCommand:
    """Read a command that the test expects to be complete."""
    parsed = reader.read_command()
    assert parsed is not None, "expected a complete command"
    return parsed


def read_all(data: bytes) -> list[list[bytes]]:
    """Feed ``data`` in one go and drain every complete command."""
    reader = RespReader()
    reader.feed(data)
    out = []
    while (parsed := reader.read_command()) is not None:
        out.append(parsed.args)
    return out


class TestMultibulk:
    def test_single_command(self):
        assert read_all(b"*1\r\n$4\r\nPING\r\n") == [[b"PING"]]

    def test_command_with_arguments(self):
        data = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        assert read_all(data) == [[b"SET", b"foo", b"bar"]]

    def test_empty_bulk_argument(self):
        assert read_all(b"*2\r\n$4\r\nECHO\r\n$0\r\n\r\n") == [[b"ECHO", b""]]

    def test_binary_safe_argument(self):
        payload = b"\x00\r\n\xff"
        data = b"*2\r\n$3\r\nSET\r\n$4\r\n" + payload + b"\r\n"
        reader = RespReader()
        reader.feed(data)
        parsed = reader.read_command()
        assert parsed is not None
        assert parsed.args[1] == payload

    def test_pipelined_commands(self):
        data = b"*1\r\n$4\r\nPING\r\n" * 3
        assert read_all(data) == [[b"PING"]] * 3

    def test_zero_length_multibulk_is_a_no_op(self):
        assert read_all(b"*0\r\n") == [[]]

    def test_negative_multibulk_is_a_no_op(self):
        assert read_all(b"*-1\r\n") == [[]]

    def test_reported_size_matches_wire_bytes(self):
        data = b"*1\r\n$4\r\nPING\r\n"
        reader = RespReader()
        reader.feed(data)
        parsed = reader.read_command()
        assert parsed is not None
        assert parsed.size == len(data)

    def test_sizes_are_per_command_when_pipelined(self):
        one = b"*1\r\n$4\r\nPING\r\n"
        two = b"*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n"
        reader = RespReader()
        reader.feed(one + two)
        assert read_one(reader).size == len(one)
        assert read_one(reader).size == len(two)


class TestPartialDelivery:
    def test_empty_buffer_yields_nothing(self):
        assert RespReader().read_command() is None

    def test_byte_at_a_time_delivery(self):
        data = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
        reader = RespReader()
        for index in range(len(data) - 1):
            reader.feed(data[index : index + 1])
            assert reader.read_command() is None, f"completed early at byte {index}"
        reader.feed(data[-1:])
        parsed = reader.read_command()
        assert parsed is not None
        assert parsed.args == [b"SET", b"foo", b"bar"]
        assert parsed.size == len(data)

    def test_split_across_two_reads(self):
        reader = RespReader()
        reader.feed(b"*2\r\n$4\r\nECHO\r\n$5\r\nhel")
        assert reader.read_command() is None
        reader.feed(b"lo\r\n")
        assert read_one(reader).args == [b"ECHO", b"hello"]

    def test_trailing_partial_command_is_kept_for_later(self):
        reader = RespReader()
        reader.feed(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPI")
        assert read_one(reader).args == [b"PING"]
        assert reader.read_command() is None
        reader.feed(b"NG\r\n")
        assert read_one(reader).args == [b"PING"]

    def test_buffer_is_compacted_after_consumption(self):
        reader = RespReader()
        for _ in range(100):
            reader.feed(b"*1\r\n$4\r\nPING\r\n")
            reader.read_command()
        assert reader.pending == 0


class TestMalformedInput:
    def test_invalid_multibulk_length(self):
        reader = RespReader()
        reader.feed(b"*abc\r\n")
        with pytest.raises(ProtocolError, match="invalid multibulk length"):
            reader.read_command()

    def test_multibulk_length_with_whitespace_is_rejected(self):
        # int(b" 1 ") would succeed in Python; Redis rejects it.
        reader = RespReader()
        reader.feed(b"* 1 \r\n")
        with pytest.raises(ProtocolError, match="invalid multibulk length"):
            reader.read_command()

    def test_multibulk_length_over_limit(self):
        reader = RespReader()
        reader.feed(b"*1048577\r\n")
        with pytest.raises(ProtocolError, match="invalid multibulk length"):
            reader.read_command()

    def test_element_not_prefixed_with_dollar(self):
        reader = RespReader()
        reader.feed(b"*1\r\n+OK\r\n")
        with pytest.raises(ProtocolError, match=r"expected '\$', got '\+'"):
            reader.read_command()

    def test_invalid_bulk_length(self):
        reader = RespReader()
        reader.feed(b"*1\r\n$xx\r\n")
        with pytest.raises(ProtocolError, match="invalid bulk length"):
            reader.read_command()

    def test_negative_bulk_length_in_request(self):
        reader = RespReader()
        reader.feed(b"*1\r\n$-1\r\n")
        with pytest.raises(ProtocolError, match="invalid bulk length"):
            reader.read_command()

    def test_bulk_length_over_limit(self):
        reader = RespReader(ParserLimits(max_bulk_length=16))
        reader.feed(b"*1\r\n$17\r\n")
        with pytest.raises(ProtocolError, match="invalid bulk length"):
            reader.read_command()

    def test_missing_crlf_after_bulk_body(self):
        reader = RespReader()
        reader.feed(b"*1\r\n$3\r\nfooXX")
        with pytest.raises(ProtocolError, match="expected CRLF"):
            reader.read_command()

    def test_oversized_header_line_without_terminator(self):
        reader = RespReader(ParserLimits(max_inline_length=32))
        reader.feed(b"*" + b"9" * 64)
        with pytest.raises(ProtocolError, match="too big mbulk count string"):
            reader.read_command()

    def test_oversized_inline_request(self):
        reader = RespReader(ParserLimits(max_inline_length=32))
        reader.feed(b"A" * 64)
        with pytest.raises(ProtocolError, match="too big inline request"):
            reader.read_command()

    def test_large_but_legal_argument_is_accepted(self):
        payload = b"x" * 100_000
        data = b"*2\r\n$3\r\nSET\r\n$100000\r\n" + payload + b"\r\n"
        reader = RespReader()
        reader.feed(data)
        parsed = reader.read_command()
        assert parsed is not None
        assert parsed.args[1] == payload


class TestInlineCommands:
    def test_simple_inline(self):
        assert read_all(b"PING\r\n") == [[b"PING"]]

    def test_inline_with_bare_lf(self):
        assert read_all(b"PING\n") == [[b"PING"]]

    def test_inline_with_arguments(self):
        assert read_all(b"SET foo bar\r\n") == [[b"SET", b"foo", b"bar"]]

    def test_inline_collapses_repeated_blanks(self):
        assert read_all(b"SET   foo\tbar\r\n") == [[b"SET", b"foo", b"bar"]]

    def test_blank_line_produces_an_empty_command(self):
        assert read_all(b"\r\n") == [[]]

    def test_incomplete_inline_waits_for_newline(self):
        reader = RespReader()
        reader.feed(b"PIN")
        assert reader.read_command() is None
        reader.feed(b"G\r\n")
        assert read_one(reader).args == [b"PING"]

    def test_inline_followed_by_multibulk(self):
        assert read_all(b"PING\r\n*1\r\n$4\r\nPING\r\n") == [[b"PING"], [b"PING"]]


class TestInlineSplitting:
    def test_double_quoted_argument_keeps_spaces(self):
        assert split_inline_args(b'SET k "a b"') == [b"SET", b"k", b"a b"]

    def test_single_quoted_argument(self):
        assert split_inline_args(b"SET k 'a b'") == [b"SET", b"k", b"a b"]

    def test_hex_escape(self):
        assert split_inline_args(b'SET k "\\x41\\x42"') == [b"SET", b"k", b"AB"]

    def test_control_character_escapes(self):
        assert split_inline_args(b'ECHO "a\\nb\\tc"') == [b"ECHO", b"a\nb\tc"]

    def test_escaped_double_quote(self):
        assert split_inline_args(b'ECHO "say \\"hi\\""') == [b"ECHO", b'say "hi"']

    def test_escaped_single_quote(self):
        assert split_inline_args(b"ECHO 'it\\'s'") == [b"ECHO", b"it's"]

    def test_empty_quoted_argument_is_preserved(self):
        assert split_inline_args(b'ECHO ""') == [b"ECHO", b""]

    def test_unbalanced_double_quote(self):
        with pytest.raises(ProtocolError, match="unbalanced quotes"):
            split_inline_args(b'ECHO "abc')

    def test_unbalanced_single_quote(self):
        with pytest.raises(ProtocolError, match="unbalanced quotes"):
            split_inline_args(b"ECHO 'abc")

    def test_closing_quote_must_be_followed_by_whitespace(self):
        with pytest.raises(ProtocolError, match="unbalanced quotes"):
            split_inline_args(b'ECHO "abc"def')

    def test_quote_inside_bare_word_is_rejected(self):
        with pytest.raises(ProtocolError, match="unbalanced quotes"):
            split_inline_args(b'ECHO ab"cd')

    def test_only_whitespace_yields_no_arguments(self):
        assert split_inline_args(b"   ") == []
