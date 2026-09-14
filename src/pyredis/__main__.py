"""Command-line entry point: ``python -m pyredis`` or ``pyredis-server``."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import sys

from pyredis.config import ServerConfig, parse_args
from pyredis.errors import RdbError
from pyredis.server import Server

__all__ = ["main"]

LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=LOG_FORMAT,
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )


async def run(config: ServerConfig) -> None:
    """Start the server and stop it cleanly on SIGINT/SIGTERM."""
    server = Server(config)
    loop = asyncio.get_running_loop()

    for signal_name in ("SIGINT", "SIGTERM"):
        signal_number = getattr(signal, signal_name, None)
        if signal_number is None:
            continue
        # Not supported on the Windows proactor loop; KeyboardInterrupt in
        # main() covers that case.
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(signal_number, server.shutdown)

    await server.serve()


def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv)
    configure_logging(config.log_level)
    try:
        asyncio.run(run(config))
    except KeyboardInterrupt:
        # Ctrl+C on Windows, where add_signal_handler is unavailable.
        logging.getLogger("pyredis").info("interrupted; shutting down")
    except RdbError as exc:
        logging.getLogger("pyredis").error("corrupt snapshot: %s", exc)
        return 1
    except OSError as exc:
        logging.getLogger("pyredis").error("could not start server: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
