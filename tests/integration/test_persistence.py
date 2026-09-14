"""End-to-end tests for RDB persistence: SAVE, restart, reload."""

from __future__ import annotations

import subprocess
import sys

import pytest

from tests.integration.client import Error, Status
from tests.integration.conftest import start_server

pytestmark = pytest.mark.integration


class TestSaveAndReload:
    def test_keys_survive_a_restart(self, tmp_path):
        first = start_server(tmp_path)
        try:
            with first.client() as connection:
                connection.command("SET", "name", "aryan")
                connection.command("SET", "count", "42")
                assert connection.command("SAVE") == Status("OK")
        finally:
            first.stop()

        assert (tmp_path / "dump.rdb").exists()

        second = start_server(tmp_path)
        try:
            with second.client() as connection:
                assert connection.command("GET", "name") == b"aryan"
                assert connection.command("GET", "count") == b"42"
                assert connection.command("DBSIZE") == 2
        finally:
            second.stop()

    def test_lists_and_sorted_sets_survive(self, tmp_path):
        first = start_server(tmp_path)
        try:
            with first.client() as connection:
                connection.command("RPUSH", "l", "a", "b", "c")
                connection.command("ZADD", "z", 1, "one", "2.5", "two")
                connection.command("SAVE")
        finally:
            first.stop()

        second = start_server(tmp_path)
        try:
            with second.client() as connection:
                assert connection.command("LRANGE", "l", 0, -1) == [b"a", b"b", b"c"]
                assert connection.command("ZRANGE", "z", 0, -1, "WITHSCORES") == [
                    b"one",
                    b"1",
                    b"two",
                    b"2.5",
                ]
        finally:
            second.stop()

    def test_ttls_survive(self, tmp_path):
        first = start_server(tmp_path)
        try:
            with first.client() as connection:
                connection.command("SET", "timed", "v", "EX", 10_000)
                connection.command("SET", "plain", "v")
                connection.command("SAVE")
        finally:
            first.stop()

        second = start_server(tmp_path)
        try:
            with second.client() as connection:
                assert 0 < connection.command("TTL", "timed") <= 10_000
                assert connection.command("TTL", "plain") == -1
        finally:
            second.stop()

    def test_already_expired_keys_are_not_restored(self, tmp_path):
        first = start_server(tmp_path)
        try:
            with first.client() as connection:
                connection.command("SET", "doomed", "v", "PXAT", 1000)  # 1970
                connection.command("SET", "alive", "v")
                connection.command("SAVE")
        finally:
            first.stop()

        second = start_server(tmp_path)
        try:
            with second.client() as connection:
                assert connection.command("EXISTS", "doomed") == 0
                assert connection.command("EXISTS", "alive") == 1
        finally:
            second.stop()

    def test_binary_values_survive(self, tmp_path):
        payload = bytes(range(256))
        first = start_server(tmp_path)
        try:
            with first.client() as connection:
                connection.command("SET", b"binary", payload)
                connection.command("SAVE")
        finally:
            first.stop()

        second = start_server(tmp_path)
        try:
            with second.client() as connection:
                assert connection.command("GET", b"binary") == payload
        finally:
            second.stop()

    def test_custom_dbfilename(self, tmp_path):
        first = start_server(tmp_path, "--dbfilename", "custom.rdb")
        try:
            with first.client() as connection:
                assert connection.command("CONFIG", "GET", "dbfilename")[1] == b"custom.rdb"
                connection.command("SET", "k", "v")
                connection.command("SAVE")
        finally:
            first.stop()

        assert (tmp_path / "custom.rdb").exists()

        second = start_server(tmp_path, "--dbfilename", "custom.rdb")
        try:
            with second.client() as connection:
                assert connection.command("GET", "k") == b"v"
        finally:
            second.stop()

    def test_bgsave_also_writes(self, tmp_path):
        server = start_server(tmp_path)
        try:
            with server.client() as connection:
                connection.command("SET", "k", "v")
                assert connection.command("BGSAVE") == Status("OK")
                assert (tmp_path / "dump.rdb").exists()
        finally:
            server.stop()

    def test_lastsave_advances(self, tmp_path):
        server = start_server(tmp_path)
        try:
            with server.client() as connection:
                before = connection.command("LASTSAVE")
                connection.command("SET", "k", "v")
                connection.command("SAVE")
                assert connection.command("LASTSAVE") >= before
        finally:
            server.stop()


class TestStartupBehaviour:
    def test_missing_rdb_file_is_not_an_error(self, tmp_path):
        server = start_server(tmp_path)
        try:
            with server.client() as connection:
                assert connection.command("DBSIZE") == 0
        finally:
            server.stop()
        assert "Traceback" not in server.logs()

    def test_a_corrupt_rdb_stops_the_server_starting(self, tmp_path):
        # Refusing to start is the safe behaviour: starting empty would hide
        # the problem and the next SAVE would destroy the original file.
        (tmp_path / "dump.rdb").write_bytes(b"NOT AN RDB FILE AT ALL")

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pyredis",
                "--port",
                "0",
                "--dir",
                str(tmp_path),
            ],
            capture_output=True,
            timeout=30,
            check=False,
        )
        assert result.returncode == 1
        assert b"not an RDB file" in result.stderr

    def test_a_truncated_rdb_stops_the_server_starting(self, tmp_path):
        server = start_server(tmp_path)
        try:
            with server.client() as connection:
                connection.command("SET", "k", "v")
                connection.command("SAVE")
        finally:
            server.stop()

        path = tmp_path / "dump.rdb"
        path.write_bytes(path.read_bytes()[:-10])

        result = subprocess.run(
            [sys.executable, "-m", "pyredis", "--port", "0", "--dir", str(tmp_path)],
            capture_output=True,
            timeout=30,
            check=False,
        )
        assert result.returncode == 1


class TestUnsupportedTypes:
    def test_save_reports_skipped_streams(self, tmp_path):
        server = start_server(tmp_path)
        try:
            with server.client() as connection:
                connection.command("XADD", "events", "1-1", "f", "v")
                connection.command("SET", "k", "v")
                reply = connection.command("SAVE")
                assert isinstance(reply, Error)
                assert "streams are not persisted" in reply
        finally:
            server.stop()

        # The rest of the dataset is still written and still loads.
        second = start_server(tmp_path)
        try:
            with second.client() as connection:
                assert connection.command("GET", "k") == b"v"
                assert connection.command("EXISTS", "events") == 0
        finally:
            second.stop()
