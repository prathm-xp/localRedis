"""Unit tests for Redis-style glob matching."""

from __future__ import annotations

import pytest

from pyredis.globmatch import glob_match


class TestLiterals:
    def test_exact_match(self):
        assert glob_match(b"key", b"key")

    def test_non_match(self):
        assert not glob_match(b"key", b"keys")
        assert not glob_match(b"keys", b"key")

    def test_empty_pattern_matches_only_empty_string(self):
        assert glob_match(b"", b"")
        assert not glob_match(b"", b"a")

    def test_matching_is_case_sensitive(self):
        assert not glob_match(b"KEY", b"key")

    def test_binary_safe(self):
        assert glob_match(b"\x00\xff", b"\x00\xff")


class TestStar:
    def test_star_matches_everything(self):
        assert glob_match(b"*", b"")
        assert glob_match(b"*", b"anything")

    def test_prefix(self):
        assert glob_match(b"user:*", b"user:1")
        assert glob_match(b"user:*", b"user:")
        assert not glob_match(b"user:*", b"users:1")

    def test_suffix(self):
        assert glob_match(b"*:cache", b"a:b:cache")
        assert not glob_match(b"*:cache", b"a:cached")

    def test_infix(self):
        assert glob_match(b"a*z", b"abcz")
        assert glob_match(b"a*z", b"az")
        assert not glob_match(b"a*z", b"abc")

    def test_multiple_stars(self):
        assert glob_match(b"*a*b*", b"xxaxxbxx")
        assert not glob_match(b"*a*b*", b"xxbxxaxx")

    def test_consecutive_stars_collapse(self):
        assert glob_match(b"a**b", b"ab")
        assert glob_match(b"a**b", b"axxb")

    def test_backtracking(self):
        assert glob_match(b"*abc", b"ababc")
        assert glob_match(b"*a*a*a", b"aaaa")

    def test_pathological_pattern_terminates(self):
        # Iterative backtracking: no recursion limit, no exponential blow-up.
        assert not glob_match(b"*" * 20 + b"b", b"a" * 200)


class TestQuestionMark:
    def test_matches_exactly_one_byte(self):
        assert glob_match(b"h?llo", b"hello")
        assert glob_match(b"h?llo", b"hallo")
        assert not glob_match(b"h?llo", b"hllo")
        assert not glob_match(b"h?llo", b"heello")

    def test_combined_with_star(self):
        assert glob_match(b"?*", b"abc")
        assert not glob_match(b"?*", b"")


class TestCharacterClass:
    def test_set(self):
        assert glob_match(b"h[ae]llo", b"hello")
        assert glob_match(b"h[ae]llo", b"hallo")
        assert not glob_match(b"h[ae]llo", b"hillo")

    def test_range(self):
        assert glob_match(b"item[0-9]", b"item5")
        assert not glob_match(b"item[0-9]", b"itemx")

    def test_reversed_range_is_normalised(self):
        assert glob_match(b"item[9-0]", b"item5")

    def test_negation(self):
        assert glob_match(b"h[^e]llo", b"hallo")
        assert not glob_match(b"h[^e]llo", b"hello")

    def test_mixed_set_and_range(self):
        assert glob_match(b"[a-cx]y", b"by")
        assert glob_match(b"[a-cx]y", b"xy")
        assert not glob_match(b"[a-cx]y", b"dy")

    def test_unterminated_class_runs_to_end_of_pattern(self):
        assert glob_match(b"a[bc", b"ab")
        assert not glob_match(b"a[bc", b"ad")

    def test_class_requires_a_byte_to_consume(self):
        assert not glob_match(b"a[b]", b"a")


class TestEscaping:
    def test_escaped_star_is_literal(self):
        assert glob_match(rb"a\*b", b"a*b")
        assert not glob_match(rb"a\*b", b"axxb")

    def test_escaped_question_mark_is_literal(self):
        assert glob_match(rb"a\?b", b"a?b")
        assert not glob_match(rb"a\?b", b"axb")

    def test_escaped_bracket_is_literal(self):
        assert glob_match(rb"a\[b", b"a[b")

    def test_escaped_backslash(self):
        assert glob_match(b"a\\\\b", b"a\\b")

    def test_trailing_backslash_is_literal(self):
        assert glob_match(b"a\\", b"a\\")

    def test_escape_inside_class(self):
        assert glob_match(rb"a[\]]b", b"a]b")


@pytest.mark.parametrize(
    ("pattern", "key", "expected"),
    [
        (b"*", b"foo", True),
        (b"foo*", b"foobar", True),
        (b"*bar", b"foobar", True),
        (b"foo*bar", b"foobazbar", True),
        (b"foo?bar", b"fooXbar", True),
        (b"foo?bar", b"fooXXbar", False),
        (b"[fg]oo", b"foo", True),
        (b"[^f]oo", b"foo", False),
    ],
)
def test_keys_patterns(pattern, key, expected):
    assert glob_match(pattern, key) is expected
