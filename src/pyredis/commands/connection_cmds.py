"""Connection-level commands: PING, ECHO, SELECT, CLIENT, HELLO, QUIT, RESET."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyredis import __version__
from pyredis.commands.base import CommandSet
from pyredis.commands.parsing import parse_int
from pyredis.errors import CommandError
from pyredis.protocol import OK, PONG, Reply, SimpleString

if TYPE_CHECKING:  # pragma: no cover
    from pyredis.connection import Connection

commands = CommandSet()

#: Only one database is implemented, so SELECT accepts index 0 and nothing else.
SUPPORTED_DB_INDEX = 0


@commands.add("PING", arity=-1, allowed_in_subscribe_mode=True)
def ping(connection: Connection, args: list[bytes]) -> Reply:
    """``PING [message]``.

    In subscribe mode RESP2 requires a two-element array reply rather than the
    usual ``+PONG``, because everything arriving on a subscribed connection has
    to look like a push message.
    """
    if len(args) > 2:
        raise CommandError("ERR wrong number of arguments for 'ping' command")

    if connection.in_subscribe_mode:
        return [b"pong", args[1] if len(args) == 2 else b""]
    return args[1] if len(args) == 2 else PONG


@commands.add("ECHO", arity=2)
def echo(connection: Connection, args: list[bytes]) -> Reply:
    """``ECHO message``."""
    return args[1]


@commands.add("SELECT", arity=2)
def select(connection: Connection, args: list[bytes]) -> Reply:
    """``SELECT index``. Only database 0 exists in this implementation."""
    index = parse_int(args[1])
    if index != SUPPORTED_DB_INDEX:
        raise CommandError("ERR DB index is out of range")
    return OK


@commands.add("QUIT", arity=-1, allowed_in_subscribe_mode=True)
def quit_(connection: Connection, args: list[bytes]) -> Reply:
    """``QUIT``. Replies, then closes once the reply has been written."""
    connection.request_close()
    return OK


@commands.add("RESET", arity=1, allowed_in_subscribe_mode=True)
def reset(connection: Connection, args: list[bytes]) -> Reply:
    """``RESET``. Returns the connection to a clean state."""
    connection.server.pubsub.remove_connection(connection)
    connection.server.watch.unwatch_all(connection)
    connection.in_multi = False
    connection.queued_commands.clear()
    connection.multi_aborted = False
    connection.name = b""
    return SimpleString("RESET")


@commands.add("HELLO", arity=-1, allowed_in_subscribe_mode=True)
def hello(connection: Connection, args: list[bytes]) -> Reply:
    """``HELLO [protover]``.

    Only RESP2 is implemented, so ``HELLO 3`` is refused with ``NOPROTO`` -
    which is exactly what a RESP2-only Redis does, and is what makes clients
    fall back cleanly instead of hanging on a reply shape they cannot parse.
    """
    if len(args) > 1:
        try:
            version = parse_int(args[1])
        except CommandError:
            raise CommandError("NOPROTO unsupported protocol version") from None
        if version != 2:
            raise CommandError("NOPROTO unsupported protocol version")

    # RESP2 has no map type, so the handshake map is sent as a flat array.
    return [
        b"server",
        b"pyredis",
        b"version",
        __version__.encode(),
        b"proto",
        2,
        b"id",
        connection.id,
        b"mode",
        b"standalone",
        b"role",
        connection.server.replication.role.encode(),
        b"modules",
        [],
    ]


@commands.add("CLIENT", arity=-2)
def client(connection: Connection, args: list[bytes]) -> Reply:
    """``CLIENT GETNAME|SETNAME|ID|INFO``."""
    subcommand = args[1].upper()

    if subcommand == b"ID" and len(args) == 2:
        return connection.id

    if subcommand == b"SETNAME" and len(args) == 3:
        if b" " in args[2] or b"\n" in args[2]:
            raise CommandError(
                "ERR Client names cannot contain spaces, newlines or special characters."
            )
        connection.name = args[2]
        return OK

    if subcommand == b"GETNAME" and len(args) == 2:
        return connection.name if connection.name else None

    if subcommand == b"INFO" and len(args) == 2:
        return _client_info(connection).encode()

    if subcommand == b"LIST" and len(args) == 2:
        lines = [_client_info(other) for other in connection.server.connections.values()]
        return ("\n".join(lines) + "\n").encode() if lines else b""

    raise CommandError(
        f"ERR Unknown subcommand or wrong number of arguments for "
        f"'{args[1].decode(errors='replace')}'. Try CLIENT HELP."
    )


def _client_info(connection: Connection) -> str:
    """One line of the ``field=value`` format ``CLIENT INFO``/``CLIENT LIST`` use."""
    name = connection.name.decode(errors="replace")
    # -1 in the `multi` field means "not in a transaction", as in Redis.
    queued = len(connection.queued_commands) if connection.in_multi else -1
    fields = {
        "id": connection.id,
        "addr": connection.peername,
        "name": name,
        "db": 0,
        "sub": len(connection.subscribed_channels),
        "psub": len(connection.subscribed_patterns),
        "multi": queued,
        "cmd": "client|info",
    }
    return " ".join(f"{key}={value}" for key, value in fields.items())
