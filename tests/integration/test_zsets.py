"""End-to-end tests for sorted-set commands."""

from __future__ import annotations

import pytest

from tests.integration.client import Error, Status

pytestmark = pytest.mark.integration


class TestZadd:
    def test_add_returns_the_number_of_new_members(self, client):
        assert client.command("ZADD", "z", 1, "a") == 1
        assert client.command("ZADD", "z", 2, "b", 3, "c") == 2
        assert client.command("ZCARD", "z") == 3

    def test_updating_a_score_is_not_an_addition(self, client):
        client.command("ZADD", "z", 1, "a")
        assert client.command("ZADD", "z", 5, "a") == 0
        assert client.command("ZSCORE", "z", "a") == b"5"

    def test_ch_counts_changes_as_well(self, client):
        client.command("ZADD", "z", 1, "a")
        assert client.command("ZADD", "z", "CH", 2, "a", 1, "b") == 2

    def test_nx_only_adds(self, client):
        client.command("ZADD", "z", 1, "a")
        assert client.command("ZADD", "z", "NX", 9, "a") == 0
        assert client.command("ZSCORE", "z", "a") == b"1"

    def test_xx_only_updates(self, client):
        assert client.command("ZADD", "z", "XX", 1, "a") == 0
        assert client.command("EXISTS", "z") == 0
        client.command("ZADD", "z", 1, "a")
        assert client.command("ZADD", "z", "XX", "CH", 5, "a") == 1

    def test_nx_and_xx_together_are_rejected(self, client):
        assert client.command("ZADD", "z", "NX", "XX", 1, "a").startswith(
            "ERR XX and NX options at the same time are not compatible"
        )

    def test_invalid_score(self, client):
        assert client.command("ZADD", "z", "abc", "a") == Error("ERR value is not a valid float")

    def test_a_bad_score_applies_nothing(self, client):
        # ZADD is all-or-nothing, so the valid pair before the bad one must not land.
        assert client.command("ZADD", "z", 1, "a", "bad", "b") == Error(
            "ERR value is not a valid float"
        )
        assert client.command("EXISTS", "z") == 0

    def test_odd_argument_count(self, client):
        assert client.command("ZADD", "z", 1, "a", 2) == Error("ERR syntax error")

    def test_infinity_scores(self, client):
        client.command("ZADD", "z", "inf", "high", "-inf", "low")
        assert client.command("ZSCORE", "z", "high") == b"inf"
        assert client.command("ZSCORE", "z", "low") == b"-inf"


class TestScoresAndRanks:
    @pytest.fixture
    def populated(self, client):
        client.command("ZADD", "z", 1, "one", 2, "two", 3, "three")
        return client

    def test_zscore(self, populated):
        assert populated.command("ZSCORE", "z", "two") == b"2"
        assert populated.command("ZSCORE", "z", "missing") is None
        assert populated.command("ZSCORE", "nokey", "two") is None

    def test_integral_scores_render_without_a_decimal_point(self, client):
        client.command("ZADD", "z", "1.0", "m")
        assert client.command("ZSCORE", "z", "m") == b"1"

    def test_fractional_scores_are_preserved(self, client):
        client.command("ZADD", "z", "1.5", "m")
        assert client.command("ZSCORE", "z", "m") == b"1.5"

    def test_zrank(self, populated):
        assert populated.command("ZRANK", "z", "one") == 0
        assert populated.command("ZRANK", "z", "three") == 2
        assert populated.command("ZRANK", "z", "missing") is None

    def test_zrevrank(self, populated):
        assert populated.command("ZREVRANK", "z", "three") == 0
        assert populated.command("ZREVRANK", "z", "one") == 2

    def test_ties_break_lexicographically(self, client):
        client.command("ZADD", "z", 1, "b", 1, "a", 1, "c")
        assert client.command("ZRANGE", "z", 0, -1) == [b"a", b"b", b"c"]

    def test_zincrby(self, client):
        assert client.command("ZINCRBY", "z", 5, "m") == b"5"
        assert client.command("ZINCRBY", "z", "2.5", "m") == b"7.5"
        assert client.command("ZINCRBY", "z", -7, "m") == b"0.5"

    def test_zincrby_from_zero_score(self, client):
        client.command("ZADD", "z", 0, "m")
        assert client.command("ZINCRBY", "z", 1, "m") == b"1"

    def test_zincrby_to_nan_is_rejected(self, client):
        client.command("ZADD", "z", "inf", "m")
        assert client.command("ZINCRBY", "z", "-inf", "m").startswith(
            "ERR resulting score is not a number"
        )


class TestZrange:
    @pytest.fixture
    def populated(self, client):
        client.command("ZADD", "z", 1, "one", 2, "two", 3, "three")
        return client

    def test_full_range(self, populated):
        assert populated.command("ZRANGE", "z", 0, -1) == [b"one", b"two", b"three"]

    def test_subrange(self, populated):
        assert populated.command("ZRANGE", "z", 1, 2) == [b"two", b"three"]

    def test_negative_indexes(self, populated):
        assert populated.command("ZRANGE", "z", -2, -1) == [b"two", b"three"]

    def test_out_of_range(self, populated):
        assert populated.command("ZRANGE", "z", 5, 10) == []
        assert populated.command("ZRANGE", "z", -100, 100) == [b"one", b"two", b"three"]

    def test_missing_key(self, client):
        assert client.command("ZRANGE", "missing", 0, -1) == []

    def test_withscores(self, populated):
        assert populated.command("ZRANGE", "z", 0, -1, "WITHSCORES") == [
            b"one",
            b"1",
            b"two",
            b"2",
            b"three",
            b"3",
        ]

    def test_zrevrange(self, populated):
        assert populated.command("ZREVRANGE", "z", 0, -1) == [b"three", b"two", b"one"]
        assert populated.command("ZREVRANGE", "z", 0, 0) == [b"three"]

    def test_zrevrange_withscores(self, populated):
        assert populated.command("ZREVRANGE", "z", 0, 0, "WITHSCORES") == [b"three", b"3"]

    def test_byscore_is_not_supported(self, populated):
        assert populated.command("ZRANGE", "z", 1, 2, "BYSCORE") == Error("ERR syntax error")


class TestZrem:
    def test_removes_members(self, client):
        client.command("ZADD", "z", 1, "a", 2, "b", 3, "c")
        assert client.command("ZREM", "z", "a", "c") == 2
        assert client.command("ZRANGE", "z", 0, -1) == [b"b"]

    def test_missing_members_are_not_counted(self, client):
        client.command("ZADD", "z", 1, "a")
        assert client.command("ZREM", "z", "a", "missing") == 1

    def test_missing_key(self, client):
        assert client.command("ZREM", "missing", "a") == 0

    def test_emptied_set_is_removed(self, client):
        client.command("ZADD", "z", 1, "a")
        client.command("ZREM", "z", "a")
        assert client.command("EXISTS", "z") == 0
        assert client.command("TYPE", "z") == Status("none")

    def test_ranks_are_correct_after_removal(self, client):
        client.command("ZADD", "z", 1, "a", 2, "b", 3, "c")
        client.command("ZREM", "z", "a")
        assert client.command("ZRANK", "z", "b") == 0
        assert client.command("ZRANK", "z", "c") == 1


class TestZcard:
    def test_counts_members(self, client):
        assert client.command("ZCARD", "missing") == 0
        client.command("ZADD", "z", 1, "a", 2, "b")
        assert client.command("ZCARD", "z") == 2
