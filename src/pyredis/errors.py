"""Exception hierarchy shared by every layer of the server.

Two families matter:

``ProtocolError``
    The byte stream from a client is not valid RESP. Redis answers with an
    error and then *closes the connection*, because once framing is lost there
    is no way to resynchronise. We do the same.

``CommandError``
    A well-formed command that cannot be executed (wrong arity, wrong type,
    bad integer, ...). The connection stays open and the error is returned as
    a RESP error reply.

``CommandError`` messages follow the Redis convention of a leading uppercase
error *code* (``ERR``, ``WRONGTYPE``, ``EXECABORT``, ...) followed by a human
readable message.
"""

from __future__ import annotations

__all__ = [
    "CommandError",
    "ProtocolError",
    "PyRedisError",
    "RdbError",
    "ReplicationError",
    "not_an_integer",
    "syntax_error",
    "value_out_of_range",
    "wrong_number_of_arguments",
    "wrong_type",
]


class PyRedisError(Exception):
    """Base class for every error raised by this package."""


class ProtocolError(PyRedisError):
    """Malformed RESP input. Fatal for the connection it occurred on."""


class CommandError(PyRedisError):
    """An error that should be reported to the client as a RESP error reply.

    The string carried by the exception is sent verbatim (prefixed with ``-``),
    so it must already include the Redis error code.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class RdbError(PyRedisError):
    """The RDB file on disk is malformed or uses an unsupported encoding."""


class ReplicationError(PyRedisError):
    """The replication handshake or stream failed."""


# ---------------------------------------------------------------------------
# Factories for the error messages Redis emits verbatim.
# Keeping them in one place means the exact wording is asserted in one place
# too, which is what makes client-library compatibility testable.
# ---------------------------------------------------------------------------


def wrong_number_of_arguments(command: str) -> CommandError:
    return CommandError(f"ERR wrong number of arguments for '{command}' command")


def wrong_type() -> CommandError:
    return CommandError("WRONGTYPE Operation against a key holding the wrong kind of value")


def not_an_integer() -> CommandError:
    return CommandError("ERR value is not an integer or out of range")


def value_out_of_range() -> CommandError:
    return CommandError("ERR value is not an integer or out of range")


def syntax_error() -> CommandError:
    return CommandError("ERR syntax error")
