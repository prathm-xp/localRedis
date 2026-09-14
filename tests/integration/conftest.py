"""Fixtures that start real server processes and connect to them over TCP.

Every integration test talks to a subprocess over a socket - no in-process
shortcuts - so what is under test is the whole stack: socket handling, framing,
dispatch and storage.

One shared server serves most tests (starting a process per test would dominate
the runtime); the ``client`` fixture flushes the keyspace before each test so
they stay independent. Tests that need their own configuration - a replica, a
specific RDB directory, a low client limit - use ``server_factory``.
"""

from __future__ import annotations

import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from tests.integration.client import RespClient

STARTUP_TIMEOUT = 15.0
SHUTDOWN_TIMEOUT = 10.0


def find_free_port() -> int:
    """Ask the OS for an unused port."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@dataclass
class ServerProcess:
    """A running server subprocess under test."""

    process: subprocess.Popen[bytes]
    port: int
    directory: Path
    log_path: Path

    def client(self, timeout: float = 5.0) -> RespClient:
        return RespClient("127.0.0.1", self.port, timeout=timeout)

    def logs(self) -> str:
        try:
            return self.log_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return "<no log captured>"

    def stop(self) -> None:
        if self.process.poll() is not None:
            return
        self.process.terminate()
        try:
            self.process.wait(timeout=SHUTDOWN_TIMEOUT)
        except subprocess.TimeoutExpired:  # pragma: no cover - defensive
            self.process.kill()
            self.process.wait(timeout=SHUTDOWN_TIMEOUT)


def _wait_until_accepting(server: ServerProcess) -> None:
    """Block until the server accepts connections, or fail with its logs."""
    deadline = time.monotonic() + STARTUP_TIMEOUT
    while time.monotonic() < deadline:
        if server.process.poll() is not None:
            raise RuntimeError(
                f"server exited with code {server.process.returncode}\n{server.logs()}"
            )
        try:
            with socket.create_connection(("127.0.0.1", server.port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)
    server.stop()
    raise RuntimeError(f"server did not start within {STARTUP_TIMEOUT}s\n{server.logs()}")


def start_server(directory: Path, *extra_args: str, port: int | None = None) -> ServerProcess:
    """Launch a server subprocess and wait for it to accept connections."""
    port = port or find_free_port()
    log_path = directory / f"server-{port}.log"
    command = [
        sys.executable,
        "-m",
        "pyredis",
        "--port",
        str(port),
        "--dir",
        str(directory),
        "--loglevel",
        "debug",
        *extra_args,
    ]
    with log_path.open("wb") as log_file:
        process = subprocess.Popen(command, stdout=log_file, stderr=subprocess.STDOUT)

    server = ServerProcess(process=process, port=port, directory=directory, log_path=log_path)
    try:
        _wait_until_accepting(server)
    except Exception:
        server.stop()
        raise
    return server


@pytest.fixture(scope="session")
def shared_server(tmp_path_factory: pytest.TempPathFactory) -> Iterator[ServerProcess]:
    """One server shared by the tests that do not need special configuration."""
    directory = tmp_path_factory.mktemp("shared-server")
    server = start_server(directory)
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture
def client(shared_server: ServerProcess) -> Iterator[RespClient]:
    """A connection to the shared server, with a clean keyspace."""
    connection = shared_server.client()
    connection.command("FLUSHALL")
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def second_client(shared_server: ServerProcess) -> Iterator[RespClient]:
    """A second, independent connection to the shared server."""
    connection = shared_server.client()
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture
def server_factory(tmp_path: Path) -> Iterator[object]:
    """Start servers with custom arguments; all are stopped at teardown."""
    started: list[ServerProcess] = []
    counter = 0

    def factory(
        *extra_args: str, directory: Path | None = None, port: int | None = None
    ) -> ServerProcess:
        """Start a server. ``port`` pins the port, which restart tests need."""
        nonlocal counter
        if directory is None:
            counter += 1
            directory = tmp_path / f"server-{counter}"
            directory.mkdir(parents=True, exist_ok=True)
        server = start_server(directory, *extra_args, port=port)
        started.append(server)
        return server

    try:
        yield factory
    finally:
        for server in reversed(started):
            server.stop()
