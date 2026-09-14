"""Redis-style glob matching for ``KEYS`` and ``PSUBSCRIBE``.

Python's :mod:`fnmatch` is deliberately not used: it works on ``str`` rather
than ``bytes``, its character classes differ from Redis's, and on Windows
``fnmatch.fnmatch`` is case-insensitive. Keys are arbitrary binary data and
pattern semantics are part of the compatibility surface, so the matcher is
implemented directly.

Supported syntax, matching Redis's ``stringmatchlen``:

``*``
    Any sequence of bytes, including none.
``?``
    Exactly one byte.
``[abc]`` / ``[a-z]`` / ``[^abc]``
    One byte from a set, a range, or (with ``^``) outside the set.
``\\x``
    A literal ``x``, so ``\\*`` matches an actual asterisk.

Matching is iterative with backtracking on ``*`` rather than recursive, so a
pathological pattern cannot blow the Python stack.
"""

from __future__ import annotations

from typing import Final

__all__ = ["glob_match"]

_STAR: Final = 0x2A
_QUESTION: Final = 0x3F
_LBRACKET: Final = 0x5B
_RBRACKET: Final = 0x5D
_BACKSLASH: Final = 0x5C
_CARET: Final = 0x5E
_DASH: Final = 0x2D


def glob_match(pattern: bytes, string: bytes) -> bool:
    """Return whether ``string`` matches the glob ``pattern``."""
    pattern_len = len(pattern)
    string_len = len(string)

    p = s = 0
    # Position of the most recent '*' and the string offset it was matched at,
    # so a failed match can resume by letting that '*' absorb one more byte.
    star_p = -1
    star_s = 0

    while s < string_len:
        if p < pattern_len and pattern[p] == _STAR:
            star_p = p
            star_s = s
            p += 1
            continue

        matched, next_p = _match_one(pattern, p, string[s]) if p < pattern_len else (False, p)
        if matched:
            p = next_p
            s += 1
            continue

        if star_p == -1:
            return False
        star_s += 1
        s = star_s
        p = star_p + 1

    while p < pattern_len and pattern[p] == _STAR:
        p += 1
    return p == pattern_len


def _match_one(pattern: bytes, p: int, char: int) -> tuple[bool, int]:
    """Try to match a single byte at pattern offset ``p``.

    Returns whether it matched and the pattern offset just past the token.
    """
    token = pattern[p]

    if token == _QUESTION:
        return True, p + 1

    if token == _BACKSLASH:
        if p + 1 < len(pattern):
            return pattern[p + 1] == char, p + 2
        # A trailing backslash is a literal backslash.
        return char == _BACKSLASH, p + 1

    if token == _LBRACKET:
        return _match_class(pattern, p, char)

    return token == char, p + 1


def _match_class(pattern: bytes, p: int, char: int) -> tuple[bool, int]:
    """Match a ``[...]`` character class starting at ``p``.

    An unterminated class runs to the end of the pattern, which is what Redis
    does rather than treating it as an error.
    """
    i = p + 1
    negate = i < len(pattern) and pattern[i] == _CARET
    if negate:
        i += 1

    matched = False
    while i < len(pattern) and pattern[i] != _RBRACKET:
        if pattern[i] == _BACKSLASH and i + 1 < len(pattern):
            i += 1
            if pattern[i] == char:
                matched = True
            i += 1
        elif i + 2 < len(pattern) and pattern[i + 1] == _DASH and pattern[i + 2] != _RBRACKET:
            low, high = pattern[i], pattern[i + 2]
            if low > high:
                low, high = high, low
            if low <= char <= high:
                matched = True
            i += 3
        else:
            if pattern[i] == char:
                matched = True
            i += 1

    if i < len(pattern):  # step over the closing ']'
        i += 1
    return (not matched if negate else matched), i
