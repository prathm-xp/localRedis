"""Unit tests for RDB serialization: the codecs, the round trip, and corruption.

The byte-level tests build files by hand rather than round-tripping through the
writer. Round-tripping alone would only prove the two halves agree with each
other, which is worthless for a format whose whole purpose is interoperability.
"""

from __future__ import annotations

from collections import deque

import pytest

from pyredis.datastore import SortedSet, Store, Stream, StreamId
from pyredis.errors import RdbError
from pyredis.persistence import dump_rdb, dump_rdb_bytes, load_rdb, load_rdb_bytes
from pyredis.persistence.crc64 import CHECK_VALUE, crc64
from pyredis.persistence.format import encode_length, encode_string
from pyredis.persistence.lzf import lzf_decompress


class TestCrc64:
    def test_published_check_value(self):
        # The check value documented in Redis's own crc64.c.
        assert crc64(b"123456789") == CHECK_VALUE

    def test_empty_input(self):
        assert crc64(b"") == 0

    def test_is_continuable(self):
        assert crc64(b"9", crc64(b"12345678")) == crc64(b"123456789")

    def test_detects_a_single_bit_change(self):
        assert crc64(b"hello") != crc64(b"hellp")


class TestLzf:
    def test_literal_run(self):
        # Control byte 0x02 => a literal run of three bytes.
        assert lzf_decompress(b"\x02abc", 3) == b"abc"

    def test_back_reference(self):
        # 0x00 'a'  -> literal run of one byte
        # 0xc0 0x00 -> match: length (0xc0 >> 5) + 2 = 8, offset 0 => copy from
        #              the start, overlapping, giving eight more 'a'.
        assert lzf_decompress(b"\x00a\xc0\x00", 9) == b"a" * 9

    def test_extended_match_length(self):
        # A length field of 7 means the real length follows in the next byte.
        compressed = b"\x00a" + b"\xe0\x0a\x00"
        assert lzf_decompress(compressed, 1 + 7 + 10 + 2) == b"a" * 20

    def test_truncated_literal_run(self):
        with pytest.raises(RdbError, match="truncated LZF literal run"):
            lzf_decompress(b"\x05ab", 6)

    def test_back_reference_before_the_start(self):
        with pytest.raises(RdbError, match="points before the start"):
            lzf_decompress(b"\xc0\x05", 8)

    def test_wrong_output_length_is_detected(self):
        with pytest.raises(RdbError, match="does not match expected"):
            lzf_decompress(b"\x02abc", 99)


class TestLengthCodec:
    @pytest.mark.parametrize("value", [0, 1, 63, 64, 16383, 16384, 2**31, 2**32, 2**33])
    def test_round_trip(self, value):
        from pyredis.persistence.format import ByteCursor

        assert ByteCursor(encode_length(value)).read_plain_length() == value

    @pytest.mark.parametrize(
        ("value", "expected_size"),
        [(0, 1), (63, 1), (64, 2), (16383, 2), (16384, 5), (2**32, 9)],
    )
    def test_narrowest_encoding_is_used(self, value, expected_size):
        assert len(encode_length(value)) == expected_size

    def test_negative_length_is_rejected(self):
        with pytest.raises(ValueError, match="cannot be negative"):
            encode_length(-1)


def build_rdb(*entries: bytes, version: bytes = b"0011", checksum: bool = True) -> bytes:
    """Assemble a minimal RDB image by hand."""
    body = bytearray(b"REDIS" + version)
    body.append(0xFE)  # SELECTDB
    body += encode_length(0)
    for entry in entries:
        body += entry
    body.append(0xFF)  # EOF
    trailer = crc64(bytes(body)) if checksum else 0
    return bytes(body) + trailer.to_bytes(8, "little")


def string_entry(key: bytes, value: bytes) -> bytes:
    return bytes([0]) + encode_string(key) + encode_string(value)


class TestReaderAgainstHandBuiltFiles:
    def test_single_string(self):
        store = Store()
        assert load_rdb_bytes(build_rdb(string_entry(b"k", b"v")), store) == 1
        assert store.get(b"k") == b"v"

    def test_several_keys(self):
        store = Store()
        image = build_rdb(string_entry(b"a", b"1"), string_entry(b"b", b"2"))
        assert load_rdb_bytes(image, store) == 2
        assert store.get(b"a") == b"1"
        assert store.get(b"b") == b"2"

    def test_empty_database(self):
        store = Store()
        assert load_rdb_bytes(build_rdb(), store) == 0

    def test_millisecond_expiry(self):
        store = Store()
        deadline = store.clock() + 100_000
        entry = bytes([0xFC]) + deadline.to_bytes(8, "little") + string_entry(b"k", b"v")
        load_rdb_bytes(build_rdb(entry), store)
        assert 0 < store.pttl_ms(b"k") <= 100_000

    def test_second_expiry(self):
        store = Store()
        deadline = store.clock() // 1000 + 100
        entry = bytes([0xFD]) + deadline.to_bytes(4, "little") + string_entry(b"k", b"v")
        load_rdb_bytes(build_rdb(entry), store)
        assert 0 < store.pttl_ms(b"k") <= 100_000

    def test_already_expired_keys_are_skipped(self):
        store = Store()
        entry = bytes([0xFC]) + (1000).to_bytes(8, "little") + string_entry(b"k", b"v")
        load_rdb_bytes(build_rdb(entry), store)
        assert store.get(b"k") is None

    def test_auxiliary_metadata_is_skipped(self):
        store = Store()
        aux = bytes([0xFA]) + encode_string(b"redis-ver") + encode_string(b"7.0.0")
        # The aux field precedes SELECTDB in a real file; placing it after is
        # equally legal and keeps the helper simple.
        assert load_rdb_bytes(build_rdb(aux, string_entry(b"k", b"v")), store) == 1

    def test_resize_hint_is_skipped(self):
        store = Store()
        resize = bytes([0xFB]) + encode_length(1) + encode_length(0)
        assert load_rdb_bytes(build_rdb(resize, string_entry(b"k", b"v")), store) == 1

    def test_idle_and_freq_opcodes_are_skipped(self):
        store = Store()
        extras = bytes([0xF8]) + encode_length(100) + bytes([0xF9, 5])
        assert load_rdb_bytes(build_rdb(extras + string_entry(b"k", b"v")), store) == 1

    def test_integer_encoded_strings(self):
        store = Store()
        # 0xC0/0xC1/0xC2 select 8-, 16- and 32-bit little-endian integers.
        int8 = bytes([0]) + encode_string(b"i8") + bytes([0xC0, 0xFB])  # -5
        int16 = bytes([0]) + encode_string(b"i16") + bytes([0xC1]) + (1000).to_bytes(2, "little")
        int32 = bytes([0]) + encode_string(b"i32") + bytes([0xC2]) + (70000).to_bytes(4, "little")
        load_rdb_bytes(build_rdb(int8, int16, int32), store)
        assert store.get(b"i8") == b"-5"
        assert store.get(b"i16") == b"1000"
        assert store.get(b"i32") == b"70000"

    def test_lzf_compressed_string(self):
        store = Store()
        compressed = b"\x00a\xc0\x00"  # expands to nine 'a' bytes
        value = bytes([0xC3]) + encode_length(len(compressed)) + encode_length(9) + compressed
        entry = bytes([0]) + encode_string(b"k") + value
        load_rdb_bytes(build_rdb(entry), store)
        assert store.get(b"k") == b"a" * 9

    def test_keys_from_other_databases_are_skipped(self):
        store = Store()
        image = bytearray(b"REDIS0011")
        image.append(0xFE)
        image += encode_length(0)
        image += string_entry(b"in-db-0", b"v")
        image.append(0xFE)
        image += encode_length(1)
        image += string_entry(b"in-db-1", b"v")
        image.append(0xFF)
        image += crc64(bytes(image)).to_bytes(8, "little")

        assert load_rdb_bytes(bytes(image), store) == 1
        assert store.get(b"in-db-0") == b"v"
        assert store.get(b"in-db-1") is None


class TestReaderRejectsCorruption:
    def test_wrong_magic(self):
        with pytest.raises(RdbError, match="not an RDB file"):
            load_rdb_bytes(b"NOTREDIS0011\xff" + bytes(8), Store())

    def test_unreadable_version(self):
        with pytest.raises(RdbError, match="malformed RDB version"):
            load_rdb_bytes(b"REDISxxxx\xff" + bytes(8), Store())

    def test_version_from_the_future(self):
        with pytest.raises(RdbError, match="newer than the supported version"):
            load_rdb_bytes(build_rdb(version=b"0099"), Store())

    def test_truncated_file(self):
        image = build_rdb(string_entry(b"k", b"value"))
        with pytest.raises(RdbError, match="unexpected end of file"):
            load_rdb_bytes(image[:-12], Store())

    def test_checksum_mismatch(self):
        image = bytearray(build_rdb(string_entry(b"k", b"v")))
        image[-1] ^= 0xFF
        with pytest.raises(RdbError, match="checksum mismatch"):
            load_rdb_bytes(bytes(image), Store())

    def test_zero_checksum_disables_verification(self):
        store = Store()
        assert load_rdb_bytes(build_rdb(string_entry(b"k", b"v"), checksum=False), store) == 1

    def test_unsupported_value_type_is_named(self):
        entry = bytes([4]) + encode_string(b"k")  # 4 is a hash
        with pytest.raises(RdbError, match=r"type 4 \(hash\) is not supported"):
            load_rdb_bytes(build_rdb(entry), Store())

    def test_listpack_encodings_are_reported_as_unsupported(self):
        entry = bytes([18]) + encode_string(b"k")  # quicklist v2
        with pytest.raises(RdbError, match="quicklist v2"):
            load_rdb_bytes(build_rdb(entry), Store())

    def test_unknown_value_type(self):
        entry = bytes([99]) + encode_string(b"k")
        with pytest.raises(RdbError, match="unknown RDB value type 99"):
            load_rdb_bytes(build_rdb(entry), Store())


class TestRoundTrip:
    def test_strings(self):
        source = Store()
        source.set(b"a", b"1")
        source.set(b"empty", b"")
        source.set(b"binary", bytes(range(256)))

        payload, result = dump_rdb_bytes(source)
        assert result.written_keys == 3
        assert result.skipped_keys == 0

        target = Store()
        assert load_rdb_bytes(payload, target) == 3
        assert target.get(b"a") == b"1"
        assert target.get(b"empty") == b""
        assert target.get(b"binary") == bytes(range(256))

    def test_lists(self):
        source = Store()
        source.set(b"l", deque([b"a", b"b", b"c"]))
        payload, _ = dump_rdb_bytes(source)
        target = Store()
        load_rdb_bytes(payload, target)
        restored = target.get(b"l")
        assert isinstance(restored, deque)
        assert list(restored) == [b"a", b"b", b"c"]

    def test_sorted_sets(self):
        source = Store()
        zset = SortedSet()
        zset.add(b"low", 1.5)
        zset.add(b"high", 99.0)
        source.set(b"z", zset)

        payload, _ = dump_rdb_bytes(source)
        target = Store()
        load_rdb_bytes(payload, target)

        restored = target.get(b"z")
        assert isinstance(restored, SortedSet)
        assert restored.score(b"low") == 1.5
        assert restored.score(b"high") == 99.0
        assert [m for _, m in restored.items()] == [b"low", b"high"]

    def test_expiries_survive(self):
        source = Store()
        deadline = source.clock() + 100_000
        source.set(b"k", b"v", expires_at_ms=deadline)
        payload, _ = dump_rdb_bytes(source)
        target = Store()
        load_rdb_bytes(payload, target)
        assert target.expire_at_ms(b"k") == deadline

    def test_keys_without_expiry_stay_without_one(self):
        source = Store()
        source.set(b"k", b"v")
        target = Store()
        load_rdb_bytes(dump_rdb_bytes(source)[0], target)
        assert target.expire_at_ms(b"k") is None

    def test_empty_store(self):
        payload, result = dump_rdb_bytes(Store())
        assert result.written_keys == 0
        assert load_rdb_bytes(payload, Store()) == 0

    def test_many_keys(self):
        source = Store()
        for index in range(1000):
            source.set(b"key%d" % index, b"value%d" % index)
        target = Store()
        assert load_rdb_bytes(dump_rdb_bytes(source)[0], target) == 1000
        assert target.get(b"key999") == b"value999"

    def test_written_checksum_validates(self):
        source = Store()
        source.set(b"k", b"v")
        payload, _ = dump_rdb_bytes(source)
        body, trailer = payload[:-8], payload[-8:]
        assert int.from_bytes(trailer, "little") == crc64(body)


class TestStreamsAreSkipped:
    def test_stream_keys_are_reported_not_written(self):
        source = Store()
        stream = Stream()
        stream.append(StreamId(1, 1), [b"f", b"v"])
        source.set(b"s", stream)
        source.set(b"k", b"v")

        payload, result = dump_rdb_bytes(source)
        assert result.written_keys == 1
        assert result.skipped_keys == 1

        target = Store()
        load_rdb_bytes(payload, target)
        assert target.get(b"k") == b"v"
        assert target.get(b"s") is None


class TestFileOperations:
    def test_dump_then_load(self, tmp_path):
        source = Store()
        source.set(b"k", b"v")
        path = tmp_path / "dump.rdb"
        dump_rdb(source, path)
        assert path.exists()

        target = Store()
        assert load_rdb(path, target) == 1
        assert target.get(b"k") == b"v"

    def test_no_temporary_file_is_left_behind(self, tmp_path):
        store = Store()
        store.set(b"k", b"v")
        dump_rdb(store, tmp_path / "dump.rdb")
        assert [p.name for p in tmp_path.iterdir()] == ["dump.rdb"]

    def test_overwriting_an_existing_file(self, tmp_path):
        path = tmp_path / "dump.rdb"
        first = Store()
        first.set(b"first", b"1")
        dump_rdb(first, path)

        second = Store()
        second.set(b"second", b"2")
        dump_rdb(second, path)

        target = Store()
        load_rdb(path, target)
        assert target.get(b"second") == b"2"
        assert target.get(b"first") is None
