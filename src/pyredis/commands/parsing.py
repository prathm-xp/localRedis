"""Argument conversion helpers shared by command handlers.

Every function raises :class:`~pyredis.errors.CommandError` carrying the exact
message Redis produces, because clients and test suites match on those strings.
Centralising the conversions is what keeps that wording consistent across forty
commands instead of forty slightly different spellings.
"""

from __future__ import annotations

from typing import Final

from pyredis.errors import CommandError, not_an_integer

__all__ = [
    "INT64_MAX",
    "INT64_MIN",
    "check_int64",
    "parse_float",
    "parse_int",
    "parse_timeout_seconds",
]

INT64_MAX: Final = 2**63 - 1
INT64_MIN: Final = -(2**63)


def parse_int(raw: bytes) -> int:
    """Parse an integer argument.

    Redis parses with ``strtoll``, so surrounding whitespace, a leading ``+``
    and empty input are all errors - unlike Python's ``int``.
    """
    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError:
        raise not_an_integer() from None
    if not text or text.strip() != text or text.startswith("+"):
        raise not_an_integer()
    try:
        value = int(text)
    except ValueError:
        raise not_an_integer() from None
    check_int64(value)
    return value


def check_int64(value: int) -> None:
    """Reject values outside the signed 64-bit range Redis integers occupy."""
    if not INT64_MIN <= value <= INT64_MAX:
        raise not_an_integer()


def parse_float(raw: bytes) -> float:
    """Parse a double argument, accepting Redis's ``inf`` spellings."""
    try:
        text = raw.decode("ascii").strip()
    except UnicodeDecodeError:
        raise CommandError("ERR value is not a valid float") from None
    if not text:
        raise CommandError("ERR value is not a valid float")

    lowered = text.lower()
    if lowered in ("inf", "+inf", "infinity", "+infinity"):
        return float("inf")
    if lowered in ("-inf", "-infinity"):
        return float("-inf")
    try:
        value = float(text)
    except ValueError:
        raise CommandError("ERR value is not a valid float") from None
    if value != value:  # NaN
        raise CommandError("ERR value is not a valid float")
    return value


def parse_timeout_seconds(raw: bytes) -> float | None:
    """Parse a blocking command's timeout.

    Returns the timeout in seconds, or ``None`` for "block forever", which is
    what a timeout of 0 means.
    """
    try:
        timeout = float(raw.decode("ascii"))
    except (ValueError, UnicodeDecodeError):
        raise CommandError("ERR timeout is not a float or out of range") from None
    if timeout != timeout or timeout == float("inf"):
        raise CommandError("ERR timeout is not a float or out of range")
    if timeout < 0:
        raise CommandError("ERR timeout is negative")
    return None if timeout == 0 else timeout
