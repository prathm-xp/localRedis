"""A minimal blocking RESP client, used by the integration tests.

Deliberately not ``redis-py``. The point of these tests is to prove the wire
protocol is correct, and a third-party client would hide exactly the framing
details under test - besides adding a dependency the project does not need.

Reply mapping:

===================  ==========================================
RESP                 Python
===================  ==========================================
``+OK``              :class:`Status` (a ``str``)
``-ERR x``           :class:`Error` (a ``str``, not raised)
``:1``               ``int``
``$3\\r\\nfoo``      ``bytes``
``$-1`` / ``*-1``    ``None``
``*n``               ``list``
===================  ==========================================

Errors are returned rather than raised so a test can assert on the exact
message, which is a large part of what "Redis-compatible" means in practice.
"""

from __future__ import annotations

import contextlib
import socket
from typing import Any

__all__ = ["Error", "ProtocolViolationError", "RespClient", "Status", "encode_command"]

DEFAULT_TIMEOUT = 5.0


class Status(str):
    """A RESP simple string, distinguishable from a bulk string."""

    __slots__ = ()


class Error(str):
    """A RESP error reply."""

    __slots__ = ()


def encode_command(*args: str | bytes | int) -> bytes:
    """Encode a command as a RESP array of bulk strings."""
    out = [b"*%d\r\n" % len(args)]
    for arg in args:
        raw = arg if isinstance(arg, bytes) else str(arg).encode()
        out.append(b"$%d\r\n" % len(raw) + raw + b"\r\n")
    return b"".join(out)


class ProtocolViolationError(AssertionError):
    """The server sent something that is not valid RESP."""


class RespClient:
    """One connection to a server under test."""

    def __init__(self, host: str, port: int, timeout: float = DEFAULT_TIMEOUT) -> None:
        self._socket = socket.create_connection((host, port), timeout=timeout)
        self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._buffer = bytearray()

    # -- lifecycle ----------------------------------------------------------

    def close(self) -> None:
        with contextlib.suppress(OSError):
            self._socket.close()

    def __enter__(self) -> RespClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def settimeout(self, timeout: float | None) -> None:
        self._socket.settimeout(timeout)

    # -- sending ------------------------------------------------------------

    def send_raw(self, data: bytes) -> None:
        """Write arbitrary bytes, for tests that exercise malformed input."""
        self._socket.sendall(data)

    def send_command(self, *args: str | bytes | int) -> None:
        """Send a command without waiting for its reply (used for pipelining)."""
        self._socket.sendall(encode_command(*args))

    def command(self, *args: str | bytes | int) -> Any:
        """Send a command and return its reply."""
        self.send_command(*args)
        return self.read_reply()

    # -- receiving ----------------------------------------------------------

    def read_reply(self) -> Any:
        """Read exactly one reply, blocking until it is complete."""
        marker = self._read(1)
        if marker == b"+":
            return Status(self._read_line().decode())
        if marker == b"-":
            return Error(self._read_line().decode())
        if marker == b":":
            return int(self._read_line())
        if marker == b"$":
            length = int(self._read_line())
            if length == -1:
                return None
            value = self._read(length)
            self._read(2)  # trailing CRLF
            return value
        if marker == b"*":
            count = int(self._read_line())
            if count == -1:
                return None
            return [self.read_reply() for _ in range(count)]
        raise ProtocolViolationError(f"unexpected reply marker {marker!r}")

    def read_raw(self, count: int) -> bytes:
        """Read exactly ``count`` raw bytes, for wire-format assertions."""
        return self._read(count)

    def read_until_eof(self) -> bytes:
        """Read until the server closes the connection."""
        chunks = [bytes(self._buffer)]
        self._buffer.clear()
        while True:
            try:
                chunk = self._socket.recv(65536)
            except (TimeoutError, OSError):
                break
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)

    def _read(self, count: int) -> bytes:
        while len(self._buffer) < count:
            chunk = self._socket.recv(65536)
            if not chunk:
                raise ProtocolViolationError(
                    f"connection closed with {len(self._buffer)} of {count} bytes read"
                )
            self._buffer += chunk
        value = bytes(self._buffer[:count])
        del self._buffer[:count]
        return value

    def _read_line(self) -> bytes:
        while True:
            index = self._buffer.find(b"\r\n")
            if index != -1:
                line = bytes(self._buffer[:index])
                del self._buffer[: index + 2]
                return line
            chunk = self._socket.recv(65536)
            if not chunk:
                raise ProtocolViolationError("connection closed mid-line")
            self._buffer += chunk
