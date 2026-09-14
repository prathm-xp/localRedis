"""Unit tests for the stream and sorted-set value types."""

from __future__ import annotations

import pytest

from pyredis.datastore import (
    MAX_SEQUENCE,
    SortedSet,
    Stream,
    StreamId,
    normalize_range,
    parse_stream_id,
)
from pyredis.errors import CommandError


class TestStreamId:
    def test_ordering_is_by_ms_then_sequence(self):
        assert StreamId(1, 1) < StreamId(1, 2) < StreamId(2, 0)

    def test_rendering(self):
        assert str(StreamId(5, 3)) == "5-3"
        assert StreamId(5, 3).encode() == b"5-3"

    def test_next_increments_sequence(self):
        assert StreamId(5, 3).next() == StreamId(5, 4)

    def test_next_rolls_over_to_the_next_millisecond(self):
        assert StreamId(5, MAX_SEQUENCE).next() == StreamId(6, 0)


class TestParseStreamId:
    def test_full_id(self):
        assert parse_stream_id(b"12-7", default_seq=0) == StreamId(12, 7)

    def test_partial_id_uses_the_default_sequence(self):
        assert parse_stream_id(b"12", default_seq=0) == StreamId(12, 0)
        assert parse_stream_id(b"12", default_seq=MAX_SEQUENCE) == StreamId(12, MAX_SEQUENCE)

    @pytest.mark.parametrize("raw", [b"", b"abc", b"1-x", b"-", b"1-2-3", b"1.5"])
    def test_invalid_ids_are_rejected(self, raw):
        with pytest.raises(CommandError, match="Invalid stream ID"):
            parse_stream_id(raw, default_seq=0)

    def test_negative_components_are_rejected(self):
        with pytest.raises(CommandError, match="Invalid stream ID"):
            parse_stream_id(b"-1-0", default_seq=0)


class TestStreamAppend:
    def test_append_and_length(self):
        stream = Stream()
        stream.append(StreamId(1, 1), [b"f", b"v"])
        assert len(stream) == 1
        assert stream.last_id == StreamId(1, 1)

    def test_empty_stream_last_id_is_zero(self):
        assert Stream().last_id == StreamId(0, 0)

    def test_zero_id_is_rejected(self):
        with pytest.raises(CommandError, match="must be greater than 0-0"):
            Stream().append(StreamId(0, 0), [])

    def test_ids_must_increase(self):
        stream = Stream()
        stream.append(StreamId(5, 5), [])
        with pytest.raises(CommandError, match="equal or smaller"):
            stream.append(StreamId(5, 5), [])
        with pytest.raises(CommandError, match="equal or smaller"):
            stream.append(StreamId(5, 4), [])
        stream.append(StreamId(5, 6), [])
        assert len(stream) == 2


class TestStreamIdGeneration:
    def test_auto_id_uses_the_clock(self):
        assert Stream().next_auto_id(1700) == StreamId(1700, 0)

    def test_auto_id_bumps_sequence_when_clock_has_not_moved(self):
        stream = Stream()
        stream.append(stream.next_auto_id(1700), [])
        assert stream.next_auto_id(1700) == StreamId(1700, 1)

    def test_auto_id_bumps_sequence_when_clock_went_backwards(self):
        stream = Stream()
        stream.append(StreamId(1700, 0), [])
        assert stream.next_auto_id(1600) == StreamId(1700, 1)

    def test_explicit_ms_with_auto_sequence_starts_at_zero(self):
        assert Stream().next_sequence_for(5) == StreamId(5, 0)

    def test_explicit_ms_zero_starts_at_sequence_one(self):
        # 0-0 is reserved, so the 0-* case has to start at 0-1.
        assert Stream().next_sequence_for(0) == StreamId(0, 1)

    def test_explicit_ms_with_auto_sequence_continues(self):
        stream = Stream()
        stream.append(StreamId(5, 0), [])
        assert stream.next_sequence_for(5) == StreamId(5, 1)

    def test_explicit_ms_with_auto_sequence_resets_for_a_new_ms(self):
        stream = Stream()
        stream.append(StreamId(5, 9), [])
        assert stream.next_sequence_for(6) == StreamId(6, 0)

    def test_sequence_overflow_is_reported(self):
        stream = Stream()
        stream.append(StreamId(5, MAX_SEQUENCE), [])
        with pytest.raises(CommandError, match="equal or smaller"):
            stream.next_sequence_for(5)


class TestStreamQueries:
    @pytest.fixture
    def stream(self) -> Stream:
        stream = Stream()
        for ms in (1, 2, 3, 4, 5):
            stream.append(StreamId(ms, 0), [b"n", b"%d" % ms])
        return stream

    def test_range_is_inclusive_at_both_ends(self, stream):
        found = stream.range(StreamId(2, 0), StreamId(4, 0))
        assert [entry.id for entry in found] == [StreamId(2, 0), StreamId(3, 0), StreamId(4, 0)]

    def test_range_covering_everything(self, stream):
        assert len(stream.range(StreamId(0, 0), StreamId(99, 0))) == 5

    def test_range_outside_the_data(self, stream):
        assert stream.range(StreamId(10, 0), StreamId(20, 0)) == []

    def test_inverted_range_is_empty(self, stream):
        assert stream.range(StreamId(4, 0), StreamId(2, 0)) == []

    def test_after_is_exclusive(self, stream):
        found = stream.after(StreamId(3, 0))
        assert [entry.id for entry in found] == [StreamId(4, 0), StreamId(5, 0)]

    def test_after_respects_count(self, stream):
        assert len(stream.after(StreamId(0, 0), count=2)) == 2

    def test_after_the_end_is_empty(self, stream):
        assert stream.after(StreamId(5, 0)) == []

    def test_fields_are_preserved(self, stream):
        assert stream.range(StreamId(1, 0), StreamId(1, 0))[0].fields == [b"n", b"1"]


class TestSortedSet:
    def test_add_and_score(self):
        zset = SortedSet()
        assert zset.add(b"a", 1.5) is True
        assert zset.score(b"a") == 1.5
        assert len(zset) == 1

    def test_re_adding_with_a_new_score_is_an_update(self):
        zset = SortedSet()
        zset.add(b"a", 1.0)
        assert zset.add(b"a", 2.0) is False
        assert zset.score(b"a") == 2.0
        assert len(zset) == 1

    def test_re_adding_the_same_score_is_a_no_op(self):
        zset = SortedSet()
        zset.add(b"a", 1.0)
        assert zset.add(b"a", 1.0) is False

    def test_missing_member_has_no_score_or_rank(self):
        zset = SortedSet()
        assert zset.score(b"nope") is None
        assert zset.rank(b"nope") is None

    def test_ordering_is_by_score_then_lexicographic_member(self):
        zset = SortedSet()
        for member, score in [(b"b", 1.0), (b"a", 1.0), (b"c", 0.5)]:
            zset.add(member, score)
        assert [m for _, m in zset.items()] == [b"c", b"a", b"b"]

    def test_rank_follows_the_ordering(self):
        zset = SortedSet()
        zset.add(b"x", 10.0)
        zset.add(b"y", 5.0)
        assert zset.rank(b"y") == 0
        assert zset.rank(b"x") == 1

    def test_rank_updates_after_a_score_change(self):
        zset = SortedSet()
        zset.add(b"x", 10.0)
        zset.add(b"y", 5.0)
        zset.add(b"y", 20.0)
        assert zset.rank(b"x") == 0
        assert zset.rank(b"y") == 1

    def test_remove(self):
        zset = SortedSet()
        zset.add(b"a", 1.0)
        assert zset.remove(b"a") is True
        assert zset.remove(b"a") is False
        assert len(zset) == 0
        assert zset.items() == []

    def test_contains(self):
        zset = SortedSet()
        zset.add(b"a", 1.0)
        assert b"a" in zset
        assert b"b" not in zset

    def test_range_with_positive_indexes(self):
        zset = SortedSet()
        for index in range(5):
            zset.add(b"m%d" % index, float(index))
        assert [m for _, m in zset.range(1, 3)] == [b"m1", b"m2", b"m3"]

    def test_range_with_negative_indexes(self):
        zset = SortedSet()
        for index in range(5):
            zset.add(b"m%d" % index, float(index))
        assert [m for _, m in zset.range(-2, -1)] == [b"m3", b"m4"]

    def test_range_clamps_out_of_bounds(self):
        zset = SortedSet()
        zset.add(b"a", 1.0)
        assert [m for _, m in zset.range(0, 99)] == [b"a"]
        assert zset.range(5, 10) == []


class TestNormalizeRange:
    @pytest.mark.parametrize(
        ("start", "stop", "length", "expected"),
        [
            (0, -1, 5, (0, 4)),
            (1, 3, 5, (1, 3)),
            (-2, -1, 5, (3, 4)),
            (-99, 99, 5, (0, 4)),
            (3, 1, 5, (3, 1)),  # inverted: caller treats start > stop as empty
            (0, -1, 0, (0, -1)),
        ],
    )
    def test_cases(self, start, stop, length, expected):
        assert normalize_range(start, stop, length) == expected
