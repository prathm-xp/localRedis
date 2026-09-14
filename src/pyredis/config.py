"""Server configuration and command-line parsing.

Configuration is an immutable dataclass built once at startup. Nothing mutates
it afterwards, which removes a whole class of "who changed this?" questions and
lets every subsystem hold a reference without defensive copying.

Option names and semantics follow ``redis-server`` where they overlap, so the
familiar invocations work:

.. code-block:: text

    pyredis-server --port 6380 --dir /var/lib/pyredis --dbfilename dump.rdb
    pyredis-server --port 6380 --replicaof "localhost 6379"
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from pyredis import __version__

__all__ = ["DEFAULT_PORT", "ServerConfig", "parse_args"]

DEFAULT_PORT: Final = 6379

#: Loopback by default. Binding a database with no authentication to every
#: interface is the kind of default that ends up in an incident report, so
#: exposing it is opt-in via ``--host``.
DEFAULT_HOST: Final = "127.0.0.1"

_LOG_LEVELS: Final = ("debug", "info", "warning", "error")


@dataclass(frozen=True, slots=True)
class ServerConfig:
    """Immutable runtime configuration."""

    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    #: Directory holding the RDB file, exposed as the ``dir`` parameter.
    dir: Path = Path()
    #: RDB file name, exposed as the ``dbfilename`` parameter.
    dbfilename: str = "dump.rdb"
    #: ``(host, port)`` of the master when this server is a replica.
    replicaof: tuple[str, int] | None = None
    log_level: str = "info"
    #: Refuse a connection beyond this many concurrent clients.
    max_clients: int = 1000
    #: Close a connection whose unparsed input grows past this (1 GB, as Redis).
    max_query_buffer: int = 1024 * 1024 * 1024
    #: How often the active expiry task runs, in seconds.
    expire_cycle_interval: float = 0.1

    @property
    def is_replica(self) -> bool:
        return self.replicaof is not None

    @property
    def rdb_path(self) -> Path:
        """Full path to the RDB file."""
        return self.dir / self.dbfilename

    def parameters(self) -> dict[str, str]:
        """The subset of configuration visible through ``CONFIG GET``.

        Values are strings because ``CONFIG GET`` replies with bulk strings.
        """
        return {
            "dir": str(self.dir),
            "dbfilename": self.dbfilename,
            "port": str(self.port),
            "bind": self.host,
            "maxclients": str(self.max_clients),
            "loglevel": self.log_level,
            "appendonly": "no",
        }


def _parse_replicaof(values: list[str]) -> tuple[str, int]:
    """Accept both ``--replicaof host port`` and ``--replicaof "host port"``.

    redis-server takes two arguments; the single quoted-string form is what
    most tutorials and harnesses pass, so both are supported.
    """
    if len(values) == 1:
        values = values[0].split()
    if len(values) != 2:
        raise argparse.ArgumentTypeError(
            "--replicaof expects a host and a port, e.g. --replicaof 'localhost 6379'"
        )
    host, raw_port = values
    try:
        port = int(raw_port)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"--replicaof port must be an integer, got {raw_port!r}"
        ) from None
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError(f"--replicaof port out of range: {port}")
    return host, port


def _port(value: str) -> int:
    """Parse a listening port. 0 means "let the operating system choose one"."""
    try:
        port = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"port must be an integer, got {value!r}") from None
    if not 0 <= port <= 65535:
        raise argparse.ArgumentTypeError(f"port out of range: {port}")
    return port


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pyredis-server",
        description="A Redis-compatible in-memory key-value server.",
    )
    parser.add_argument("--version", action="version", version=f"pyredis-server {__version__}")
    parser.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help="interface to bind (default: %(default)s; use 0.0.0.0 to expose the server)",
    )
    parser.add_argument(
        "--port",
        type=_port,
        default=DEFAULT_PORT,
        help="TCP port to listen on, or 0 to let the OS choose (default: %(default)s)",
    )
    parser.add_argument(
        "--dir",
        type=Path,
        default=Path(),
        help="directory containing the RDB file (default: current directory)",
    )
    parser.add_argument(
        "--dbfilename", default="dump.rdb", help="RDB file name (default: %(default)s)"
    )
    parser.add_argument(
        "--replicaof",
        nargs="+",
        metavar=("HOST", "PORT"),
        help="replicate from this master, e.g. --replicaof 'localhost 6379'",
    )
    parser.add_argument(
        "--loglevel",
        choices=_LOG_LEVELS,
        default="info",
        help="logging verbosity (default: %(default)s)",
    )
    parser.add_argument(
        "--maxclients",
        type=int,
        default=1000,
        help="maximum concurrent client connections (default: %(default)s)",
    )
    return parser


def parse_args(argv: list[str] | None = None) -> ServerConfig:
    """Parse command-line arguments into a :class:`ServerConfig`.

    Raises:
        SystemExit: on malformed arguments, via argparse.
    """
    parser = build_parser()
    namespace = parser.parse_args(argv)

    replicaof = None
    if namespace.replicaof is not None:
        try:
            replicaof = _parse_replicaof(namespace.replicaof)
        except argparse.ArgumentTypeError as exc:
            parser.error(str(exc))

    if namespace.maxclients < 1:
        parser.error("--maxclients must be at least 1")

    return ServerConfig(
        host=namespace.host,
        port=namespace.port,
        dir=namespace.dir,
        dbfilename=namespace.dbfilename,
        replicaof=replicaof,
        log_level=namespace.loglevel,
        max_clients=namespace.maxclients,
    )
