"""Incremental RESP request parser.

TCP gives us a byte stream, not messages. ``RespReader`` owns the framing: it
accumulates bytes with :meth:`feed` and hands back one fully received command
at a time from :meth:`read_command`, returning ``None`` when more bytes are
needed. That makes partially-delivered commands and pipelined commands the
same code path.

Two request encodings are accepted, matching Redis:

* **Multibulk** (``*2\\r\\n$4\\r\\nECHO\\r\\n$2\\r\\nhi\\r\\n``) - what every
  real client library sends.
* **Inline** (``ECHO hi\\r\\n``) - what you get from ``telnet``/``nc``. Redis
  supports it and it makes the server pleasant to poke at by hand.

Every complete command also reports how many wire bytes it occupied. The
replication code needs that number to track offsets, and threading it through
the parser is the only place it can be known exactly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from pyredis.errors import ProtocolError

__all__ = ["DEFAULT_LIMITS", "ParsedCommand", "ParserLimits", "RespReader", "split_inline_args"]

_CR: Final = 0x0D
_LF: Final = 0x0A
_STAR: Final = 0x2A
_DOLLAR: Final = 0x24
_BACKSLASH: Final = 0x5C
_DQUOTE: Final = 0x22
_SQUOTE: Final = 0x27
_MINUS: Final = 0x2D
_BLANKS: Final = (0x20, 0x09)  # space, tab
_WHITESPACE: Final = (0x20, 0x09, 0x0A, 0x0B, 0x0C, 0x0D)

# Escape sequences understood inside a double-quoted inline argument.
_ESCAPES: Final[dict[int, int]] = {
    ord("n"): 0x0A,
    ord("r"): 0x0D,
    ord("t"): 0x09,
    ord("b"): 0x08,
    ord("a"): 0x07,
}

#: Buffer compaction threshold: reclaim consumed bytes once this many pile up.
_COMPACT_THRESHOLD: Final = 64 * 1024


@dataclass(frozen=True, slots=True)
class ParsedCommand:
    """One complete request.

    ``args`` is the command name followed by its arguments, all raw bytes.
    ``size`` is the number of bytes the request occupied on the wire, which is
    what a replica reports back in ``REPLCONF ACK <offset>``.
    """

    args: list[bytes]
    size: int


@dataclass(frozen=True, slots=True)
class ParserLimits:
    """Bounds that stop a hostile or broken client from exhausting memory.

    The values mirror Redis's own compile-time limits.
    """

    #: Maximum number of elements in a multibulk request.
    max_multibulk_length: int = 1024 * 1024
    #: Maximum size of a single bulk string argument (512 MB, Redis's string cap).
    max_bulk_length: int = 512 * 1024 * 1024
    #: Maximum length of an inline request or of a protocol header line.
    max_inline_length: int = 64 * 1024


DEFAULT_LIMITS: Final = ParserLimits()


class RespReader:
    """Stateful, resumable parser for one connection's inbound byte stream."""

    __slots__ = ("_buf", "_limits", "_pos")

    def __init__(self, limits: ParserLimits = DEFAULT_LIMITS) -> None:
        self._buf = bytearray()
        self._pos = 0  # offset of the first byte not yet consumed by a command
        self._limits = limits

    # -- buffer management --------------------------------------------------

    def feed(self, data: bytes) -> None:
        """Append freshly received bytes to the parse buffer."""
        self._buf += data

    @property
    def pending(self) -> int:
        """Number of buffered bytes that are not yet part of a parsed command."""
        return len(self._buf) - self._pos

    def _compact(self) -> None:
        if self._pos >= _COMPACT_THRESHOLD or self._pos >= self.pending:
            del self._buf[: self._pos]
            self._pos = 0

    # -- parsing ------------------------------------------------------------

    def read_command(self) -> ParsedCommand | None:
        """Return the next complete command, or ``None`` if more bytes are needed.

        Raises:
            ProtocolError: the stream is not valid RESP. The caller must report
                the error to the client and close the connection.
        """
        start = self._pos
        if start >= len(self._buf):
            return None

        if self._buf[start] == _STAR:
            parsed = self._parse_multibulk(start)
        else:
            parsed = self._parse_inline(start)

        if parsed is None:
            return None

        args, end = parsed
        self._pos = end
        self._compact()
        return ParsedCommand(args, end - start)

    def _parse_multibulk(self, start: int) -> tuple[list[bytes], int] | None:
        line = self._read_crlf_line(start + 1, "too big mbulk count string")
        if line is None:
            return None
        raw_count, pos = line

        count = _strict_int(raw_count)
        if count is None or count > self._limits.max_multibulk_length:
            raise ProtocolError("Protocol error: invalid multibulk length")
        if count <= 0:
            # `*0\r\n` and `*-1\r\n` are well-formed no-ops that Redis ignores.
            return [], pos

        args: list[bytes] = []
        buf = self._buf
        for _ in range(count):
            if pos >= len(buf):
                return None
            if buf[pos] != _DOLLAR:
                raise ProtocolError(f"Protocol error: expected '$', got '{chr(buf[pos])}'")
            header = self._read_crlf_line(pos + 1, "too big bulk count string")
            if header is None:
                return None
            raw_length, pos = header

            length = _strict_int(raw_length)
            if length is None or length < 0 or length > self._limits.max_bulk_length:
                raise ProtocolError("Protocol error: invalid bulk length")

            end = pos + length
            if len(buf) < end + 2:
                return None
            if buf[end] != _CR or buf[end + 1] != _LF:
                raise ProtocolError("Protocol error: expected CRLF after bulk string")
            args.append(bytes(buf[pos:end]))
            pos = end + 2

        return args, pos

    def _parse_inline(self, start: int) -> tuple[list[bytes], int] | None:
        buf = self._buf
        newline = buf.find(b"\n", start)
        if newline == -1:
            if len(buf) - start > self._limits.max_inline_length:
                raise ProtocolError("Protocol error: too big inline request")
            return None

        end = newline
        if end > start and buf[end - 1] == _CR:
            end -= 1
        line = bytes(buf[start:end])
        if len(line) > self._limits.max_inline_length:
            raise ProtocolError("Protocol error: too big inline request")
        return split_inline_args(line), newline + 1

    def _read_crlf_line(self, offset: int, overflow_message: str) -> tuple[bytes, int] | None:
        """Read a CRLF-terminated protocol line starting at ``offset``.

        Returns the line contents and the offset just past the CRLF.
        """
        idx = self._buf.find(b"\r\n", offset)
        if idx == -1:
            if len(self._buf) - offset > self._limits.max_inline_length:
                raise ProtocolError("Protocol error: " + overflow_message)
            return None
        return bytes(self._buf[offset:idx]), idx + 2


def _strict_int(raw: bytes) -> int | None:
    """Parse an optionally-negative decimal integer, rejecting what Redis rejects.

    ``int(b" +1 ")`` succeeds in Python but is a protocol error in Redis, so the
    permitted character set is checked explicitly.
    """
    if not raw:
        return None
    body = raw[1:] if raw[0] == _MINUS else raw
    if not body or not body.isdigit():
        return None
    return int(raw)


def split_inline_args(line: bytes) -> list[bytes]:
    """Split an inline request into arguments, honouring quotes and escapes.

    Mirrors Redis's ``sdssplitargs``: bare words split on whitespace, double
    quotes support ``\\xHH`` and the usual C escapes, single quotes support
    only ``\\'``. A closing quote must be followed by whitespace or end of line.

    Raises:
        ProtocolError: on unbalanced quotes.
    """
    args: list[bytes] = []
    i = 0
    length = len(line)

    while True:
        while i < length and line[i] in _BLANKS:
            i += 1
        if i >= length:
            return args

        quote = line[i] if line[i] in (_DQUOTE, _SQUOTE) else 0
        if quote:
            i += 1
            token, i = _read_quoted(line, i, quote)
        else:
            token, i = _read_bare(line, i)
        args.append(token)


def _read_quoted(line: bytes, i: int, quote: int) -> tuple[bytes, int]:
    """Read a quoted token. ``i`` points just past the opening quote."""
    out = bytearray()
    length = len(line)
    while True:
        if i >= length:
            raise ProtocolError("Protocol error: unbalanced quotes in request")
        char = line[i]

        if char == _BACKSLASH and quote == _DQUOTE and i + 3 < length and line[i + 1] == ord("x"):
            try:
                out.append(int(line[i + 2 : i + 4], 16))
            except ValueError:
                out.append(char)
                i += 1
                continue
            i += 4
            continue
        if char == _BACKSLASH and quote == _DQUOTE and i + 1 < length:
            out.append(_ESCAPES.get(line[i + 1], line[i + 1]))
            i += 2
            continue
        if char == _BACKSLASH and quote == _SQUOTE and i + 1 < length and line[i + 1] == _SQUOTE:
            out.append(_SQUOTE)
            i += 2
            continue
        if char == quote:
            i += 1
            if i < length and line[i] not in _BLANKS:
                raise ProtocolError("Protocol error: unbalanced quotes in request")
            return bytes(out), i

        out.append(char)
        i += 1


def _read_bare(line: bytes, i: int) -> tuple[bytes, int]:
    """Read an unquoted token starting at ``i``."""
    out = bytearray()
    length = len(line)
    while i < length:
        char = line[i]
        if char in _WHITESPACE:
            break
        if char in (_DQUOTE, _SQUOTE):
            raise ProtocolError("Protocol error: unbalanced quotes in request")
        out.append(char)
        i += 1
    return bytes(out), i
