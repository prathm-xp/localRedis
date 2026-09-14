"""Unit tests for RESP2 serialization."""

from __future__ import annotations

import pytest

from pyredis.protocol import (
    NULL_ARRAY,
    OK,
    RespErrorReply,
    SimpleString,
    encode,
    encode_command,
)


class TestScalars:
    def test_simple_string(self):
        assert encode(SimpleString("OK")) == b"+OK\r\n"
        assert encode(OK) == b"+OK\r\n"

    def test_error(self):
        assert encode(RespErrorReply("ERR unknown")) == b"-ERR unknown\r\n"

    def test_integer(self):
        assert encode(0) == b":0\r\n"
        assert encode(42) == b":42\r\n"
        assert encode(-1) == b":-1\r\n"

    def test_large_integer(self):
        assert encode(2**63 - 1) == b":9223372036854775807\r\n"

    def test_bulk_string_from_bytes(self):
        assert encode(b"foo") == b"$3\r\nfoo\r\n"

    def test_bulk_string_from_str_is_utf8(self):
        assert encode("hej") == b"$3\r\nhej\r\n"
        assert encode("é") == b"$2\r\n\xc3\xa9\r\n"

    def test_empty_bulk_string(self):
        assert encode(b"") == b"$0\r\n\r\n"

    def test_binary_safe_bulk_string(self):
        payload = bytes(range(256))
        assert encode(payload) == b"$256\r\n" + payload + b"\r\n"

    def test_null_bulk_string(self):
        assert encode(None) == b"$-1\r\n"

    def test_null_array_is_distinct_from_null_bulk(self):
        assert encode(NULL_ARRAY) == b"*-1\r\n"


class TestArrays:
    def test_empty_array(self):
        assert encode([]) == b"*0\r\n"

    def test_flat_array(self):
        assert encode([b"a", b"bb"]) == b"*2\r\n$1\r\na\r\n$2\r\nbb\r\n"

    def test_tuple_is_an_array_too(self):
        assert encode((1, 2)) == b"*2\r\n:1\r\n:2\r\n"

    def test_nested_array(self):
        assert encode([[b"x", 1], None]) == b"*2\r\n*2\r\n$1\r\nx\r\n:1\r\n$-1\r\n"

    def test_mixed_types(self):
        assert encode([OK, 7, None, b"z"]) == b"*4\r\n+OK\r\n:7\r\n$-1\r\n$1\r\nz\r\n"


class TestRejectedTypes:
    def test_bool_is_rejected_rather_than_silently_encoded_as_int(self):
        # bool is a subclass of int; encoding True as :1 would hide handler bugs.
        with pytest.raises(TypeError, match="bool is not a RESP type"):
            encode(True)

    def test_unknown_type_is_rejected(self):
        with pytest.raises(TypeError, match="cannot encode"):
            encode(3.5)  # type: ignore[arg-type]  # deliberately invalid

    def test_bool_nested_in_array_is_rejected(self):
        with pytest.raises(TypeError):
            encode([1, False])


class TestEncodeCommand:
    def test_encodes_bulk_array(self):
        assert encode_command("PING") == b"*1\r\n$4\r\nPING\r\n"

    def test_mixed_argument_types(self):
        assert encode_command(b"SET", "k", 1) == (b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n1\r\n")

    def test_roundtrips_through_the_reader(self):
        from pyredis.protocol import RespReader

        reader = RespReader()
        reader.feed(encode_command("SET", "key", "value"))
        parsed = reader.read_command()
        assert parsed is not None
        assert parsed.args == [b"SET", b"key", b"value"]
