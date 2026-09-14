"""Server administration and introspection: CONFIG, INFO, COMMAND, SAVE, ...."""

from __future__ import annotations

import os
import platform
import time
from typing import TYPE_CHECKING

from pyredis import COMPATIBLE_REDIS_VERSION, __version__
from pyredis.commands.base import CommandSet
from pyredis.errors import CommandError, syntax_error
from pyredis.globmatch import glob_match
from pyredis.persistence import dump_rdb
from pyredis.protocol import OK, Reply

if TYPE_CHECKING:  # pragma: no cover
    from pyredis.connection import Connection

commands = CommandSet()


@commands.add("CONFIG", arity=-2)
def config(connection: Connection, args: list[bytes]) -> Reply:
    """``CONFIG GET parameter [parameter ...]``.

    Read-only. This server has no runtime-reconfigurable parameters, so
    ``CONFIG SET`` is reported as an unknown subcommand rather than silently
    accepted and ignored.
    """
    if args[1].upper() != b"GET" or len(args) < 3:
        raise CommandError(
            f"ERR Unknown CONFIG subcommand or wrong number of arguments for "
            f"'{args[1].decode(errors='replace')}'"
        )

    parameters = connection.server.config.parameters()
    out: list[Reply] = []
    seen: set[str] = set()
    for pattern in args[2:]:
        for name, value in parameters.items():
            if name not in seen and glob_match(pattern.lower(), name.encode()):
                seen.add(name)
                out.append(name.encode())
                out.append(value.encode())
    return out


@commands.add("COMMAND", arity=-1)
def command(connection: Connection, args: list[bytes]) -> Reply:
    """``COMMAND [COUNT | DOCS | INFO name ...]``.

    Key-position metadata is not tracked, so ``first_key``/``last_key``/``step``
    are reported as zero. Clients use those for cluster routing, which this
    server does not implement.
    """
    table = connection.server.commands
    if len(args) == 1:
        return [
            _command_info(spec.name, spec.arity) for spec in sorted(table, key=lambda s: s.name)
        ]

    subcommand = args[1].upper()
    if subcommand == b"COUNT":
        return len(table)
    if subcommand == b"DOCS":
        # redis-cli issues this on connect; an empty map is a valid answer.
        return []
    if subcommand == b"INFO":
        out: list[Reply] = []
        for name in args[2:]:
            spec = table.lookup(name)
            out.append(_command_info(spec.name, spec.arity) if spec else None)
        return out
    raise syntax_error()


def _command_info(name: str, arity: int) -> list[Reply]:
    return [name.encode(), arity, [], 0, 0, 0]


@commands.add("DBSIZE", arity=1)
def dbsize(connection: Connection, args: list[bytes]) -> Reply:
    """``DBSIZE``. Counts live keys; expired ones are collected first."""
    return len(connection.server.store)


@commands.add("FLUSHDB", arity=-1, is_write=True)
@commands.add("FLUSHALL", arity=-1, is_write=True)
def flush(connection: Connection, args: list[bytes]) -> Reply:
    """``FLUSHALL``/``FLUSHDB`` ``[ASYNC|SYNC]``.

    There is only one database and one thread, so both commands do the same
    thing and both modifiers are accepted and behave synchronously.
    """
    if len(args) > 2 or (len(args) == 2 and args[1].upper() not in (b"ASYNC", b"SYNC")):
        raise syntax_error()
    connection.server.store.flush()
    return OK


@commands.add("SAVE", arity=1)
def save(connection: Connection, args: list[bytes]) -> Reply:
    """``SAVE``. Writes an RDB snapshot synchronously.

    Synchronous is the honest implementation here: a background save would need
    a forked process or a snapshot copy to be meaningful, and pretending to be
    asynchronous while blocking the loop would be worse than not offering it.
    """
    server = connection.server
    try:
        result = dump_rdb(server.store, server.config.rdb_path)
    except OSError as exc:
        raise CommandError(f"ERR error writing the RDB file: {exc.strerror or exc}") from None
    server.last_save_time = int(time.time())
    if result.skipped_keys:
        raise CommandError(
            f"ERR snapshot written, but {result.skipped_keys} key(s) of unsupported "
            f"type were skipped (streams are not persisted)"
        )
    return OK


@commands.add("BGSAVE", arity=-1)
def bgsave(connection: Connection, args: list[bytes]) -> Reply:
    """``BGSAVE``. Performs the save inline; see :func:`save` for why."""
    save(connection, [b"SAVE"])
    return OK


@commands.add("LASTSAVE", arity=1)
def lastsave(connection: Connection, args: list[bytes]) -> Reply:
    """``LASTSAVE``. Unix time of the last successful save."""
    return connection.server.last_save_time


@commands.add("TIME", arity=1)
def time_(connection: Connection, args: list[bytes]) -> Reply:
    """``TIME``. Server time as ``[seconds, microseconds]``."""
    now = time.time_ns()
    return [b"%d" % (now // 1_000_000_000), b"%d" % (now % 1_000_000_000 // 1000)]


@commands.add("INFO", arity=-1)
def info(connection: Connection, args: list[bytes]) -> Reply:
    """``INFO [section ...]``. Returns the standard text format as a bulk string."""
    requested = {arg.lower() for arg in args[1:]}
    wants_all = not requested or b"all" in requested or b"default" in requested

    sections: list[tuple[str, list[tuple[str, str]]]] = [
        ("Server", _server_section(connection)),
        ("Clients", _clients_section(connection)),
        ("Stats", _stats_section(connection)),
        ("Replication", connection.server.replication.info_fields()),
        ("Keyspace", _keyspace_section(connection)),
    ]

    chunks: list[str] = []
    for title, fields in sections:
        if not wants_all and title.lower().encode() not in requested:
            continue
        chunks.append(f"# {title}\r\n" + "".join(f"{key}:{value}\r\n" for key, value in fields))
    return "\r\n".join(chunks).encode()


def _server_section(connection: Connection) -> list[tuple[str, str]]:
    server = connection.server
    return [
        # Reported for client-library compatibility: libraries gate behaviour on
        # this field. The authoritative version of *this* server is pyredis_version.
        ("redis_version", COMPATIBLE_REDIS_VERSION),
        ("pyredis_version", __version__),
        ("redis_mode", "standalone"),
        ("os", _platform()),
        ("arch_bits", "64"),
        ("process_id", str(os.getpid())),
        ("tcp_port", str(server.port)),
        ("uptime_in_seconds", str(server.uptime_seconds)),
        ("executable", "pyredis-server"),
        ("config_file", ""),
    ]


def _clients_section(connection: Connection) -> list[tuple[str, str]]:
    server = connection.server
    return [
        ("connected_clients", str(len(server.connections))),
        ("blocked_clients", str(server.blocking.waiting_keys)),
        ("maxclients", str(server.config.max_clients)),
    ]


def _stats_section(connection: Connection) -> list[tuple[str, str]]:
    stats = connection.server.stats
    return [
        ("total_connections_received", str(stats.connections_received)),
        ("total_commands_processed", str(stats.commands_processed)),
        ("expired_keys", str(stats.expired_keys)),
        ("keyspace_hits", str(stats.keyspace_hits)),
        ("keyspace_misses", str(stats.keyspace_misses)),
        ("pubsub_channels", str(len(connection.server.pubsub.active_channels()))),
        ("pubsub_patterns", str(connection.server.pubsub.pattern_count)),
    ]


def _keyspace_section(connection: Connection) -> list[tuple[str, str]]:
    store = connection.server.store
    size = len(store)
    if size == 0:
        return []
    with_expiry = sum(1 for key in store.all_keys() if store.expire_at_ms(key) is not None)
    return [("db0", f"keys={size},expires={with_expiry},avg_ttl=0")]


def _platform() -> str:
    return f"{platform.system()} {platform.release()} {platform.machine()}"
