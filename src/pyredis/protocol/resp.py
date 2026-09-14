"""RESP2 reply model and serializer.

Command handlers return ordinary Python objects and this module turns them
into wire bytes. The mapping is deliberately small:

===========================  ==============================  =================
Python value                 RESP2 wire form                 Example
===========================  ==============================  =================
``SimpleString("OK")``       simple string                   ``+OK\r\n``
``RespErrorReply("ERR x")``  error                           ``-ERR x\r\n``
``int``                      integer                         ``:42\r\n``
``bytes`` / ``str``          bulk string                     ``$3\r\nfoo\r\n``
``None``                     null bulk string                ``$-1\r\n``
``list`` / ``tuple``         array (encoded recursively)     ``*1\r\n...``
``NULL_ARRAY``               null array                      ``*-1\r\n``
===========================  ==============================  =================

``str`` values are encoded as UTF-8. Everything the store holds is already
``bytes``; ``str`` is accepted so handlers can return literals readably.

Only RESP2 is implemented. See ``docs/PROTOCOL.md`` for why, and for the
behaviour of ``HELLO``.
"""

from __future__ import annotations

from typing import Any, Final, Union

__all__ = [
    "CRLF",
    "EMPTY_ARRAY",
    "NO_REPLY",
    "NULL_ARRAY",
    "OK",
    "PONG",
    "QUEUED",
    "NoReply",
    "NullArray",
    "Reply",
    "RespErrorReply",
    "SimpleString",
    "encode",
    "encode_command",
]

CRLF: Final = b"\r\n"


class SimpleString(str):
    """A ``str`` that serializes as a RESP simple string rather than a bulk string.

    Subclassing ``str`` keeps handlers readable (``return OK``) while making the
    wire representation explicit at the type level.
    """

    __slots__ = ()


class RespErrorReply(str):
    """An error reply. The value must already start with a Redis error code."""

    __slots__ = ()


class NullArray:
    """Singleton marker for the RESP2 null array (``*-1\r\n``).

    Distinct from ``None`` (null *bulk string*): some commands, notably
    ``BLPOP`` and ``XREAD`` on timeout, must return a null array specifically.
    """

    __slots__ = ()
    _instance: NullArray | None = None

    def __new__(cls) -> NullArray:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "NULL_ARRAY"


class NoReply:
    """Singleton marker meaning "this handler already wrote, or must stay silent".

    Returned by handlers that manage their own output (``SUBSCRIBE`` sends one
    reply per channel) or that must produce nothing at all (``REPLCONF ACK``
    from a replica, ``PSYNC`` before the RDB transfer). It lives beside the
    reply model so that the connection layer can import it without depending on
    the command layer.
    """

    __slots__ = ()
    _instance: NoReply | None = None

    def __new__(cls) -> NoReply:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "NO_REPLY"


NULL_ARRAY: Final = NullArray()
NO_REPLY: Final = NoReply()
EMPTY_ARRAY: Final[list[Reply]] = []

OK: Final = SimpleString("OK")
PONG: Final = SimpleString("PONG")
QUEUED: Final = SimpleString("QUEUED")

#: Anything ``encode`` accepts. Recursive, so the alias is intentionally loose.
Reply = Union[
    SimpleString,
    "NoReply",
    RespErrorReply,
    NullArray,
    None,
    int,
    bytes,
    str,
    # Recursive aliases are not expressible here, so the containers are
    # loose: `encode` validates each element as it walks the structure.
    list[Any],
    tuple[Any, ...],
]


def encode(value: Reply) -> bytes:
    """Serialize ``value`` to RESP2 bytes."""
    out: list[bytes] = []
    _encode_into(value, out)
    return b"".join(out)


def _encode_into(value: Reply, out: list[bytes]) -> None:
    # Ordered by expected frequency, with the two str subclasses checked before
    # plain str because they are more specific.
    if value is None:
        out.append(b"$-1\r\n")
    elif isinstance(value, bytes):
        out.append(b"$%d\r\n" % len(value))
        out.append(value)
        out.append(CRLF)
    elif isinstance(value, SimpleString):
        out.append(b"+%s\r\n" % value.encode())
    elif isinstance(value, RespErrorReply):
        out.append(b"-%s\r\n" % value.encode())
    elif isinstance(value, bool):
        # bool is a subclass of int; silently encoding it as 0/1 hides bugs.
        raise TypeError("bool is not a RESP type; return an int or SimpleString explicitly")
    elif isinstance(value, int):
        out.append(b":%d\r\n" % value)
    elif isinstance(value, str):
        encoded = value.encode()
        out.append(b"$%d\r\n" % len(encoded))
        out.append(encoded)
        out.append(CRLF)
    elif isinstance(value, (list, tuple)):
        out.append(b"*%d\r\n" % len(value))
        for item in value:
            _encode_into(item, out)
    elif isinstance(value, NullArray):
        out.append(b"*-1\r\n")
    else:
        raise TypeError(f"cannot encode {type(value).__name__} as a RESP reply")


def encode_command(*args: bytes | str | int) -> bytes:
    """Encode a command as a RESP array of bulk strings.

    Used for everything this server *sends* as a client: the replica handshake
    and the command stream propagated to replicas.
    """
    out: list[bytes] = [b"*%d\r\n" % len(args)]
    for arg in args:
        if isinstance(arg, bytes):
            raw = arg
        elif isinstance(arg, int):
            raw = b"%d" % arg
        else:
            raw = arg.encode()
        out.append(b"$%d\r\n" % len(raw))
        out.append(raw)
        out.append(CRLF)
    return b"".join(out)
