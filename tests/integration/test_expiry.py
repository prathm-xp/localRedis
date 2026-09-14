"""End-to-end tests for TTL and key expiration.

These use real sleeps because the point is to prove the wall-clock behaviour of
a running server; the deterministic, clock-injected tests live in
``tests/unit/test_store.py``. Waits are kept short and the tests are marked
``slow`` so they can be deselected.
"""

from __future__ import annotations

import time

import pytest

from tests.integration.client import Error, RespClient, Status

pytestmark = pytest.mark.integration


class TestSetWithExpiry:
    def test_set_px(self, client):
        assert client.command("SET", "k", "v", "PX", 10_000) == Status("OK")
        assert 0 < client.command("PTTL", "k") <= 10_000

    def test_set_ex(self, client):
        client.command("SET", "k", "v", "EX", 100)
        assert 0 < client.command("TTL", "k") <= 100

    def test_set_pxat(self, client):
        deadline = int(time.time() * 1000) + 50_000
        client.command("SET", "k", "v", "PXAT", deadline)
        assert 0 < client.command("TTL", "k") <= 50

    def test_set_exat(self, client):
        client.command("SET", "k", "v", "EXAT", int(time.time()) + 100)
        assert 0 < client.command("TTL", "k") <= 100

    def test_plain_set_clears_an_existing_ttl(self, client):
        client.command("SET", "k", "v", "EX", 100)
        client.command("SET", "k", "v2")
        assert client.command("TTL", "k") == -1

    def test_keepttl_preserves_the_ttl(self, client):
        client.command("SET", "k", "v", "EX", 100)
        client.command("SET", "k", "v2", "KEEPTTL")
        assert 0 < client.command("TTL", "k") <= 100

    def test_zero_expiry_is_rejected(self, client):
        assert client.command("SET", "k", "v", "EX", 0) == Error(
            "ERR invalid expire time in 'set' command"
        )

    def test_negative_expiry_is_rejected(self, client):
        assert client.command("SET", "k", "v", "PX", -1) == Error(
            "ERR invalid expire time in 'set' command"
        )

    def test_two_expiry_options_conflict(self, client):
        assert client.command("SET", "k", "v", "EX", 10, "PX", 100) == Error("ERR syntax error")


class TestTtlReporting:
    def test_ttl_of_a_missing_key(self, client):
        assert client.command("TTL", "missing") == -2
        assert client.command("PTTL", "missing") == -2

    def test_ttl_of_a_key_without_expiry(self, client):
        client.command("SET", "k", "v")
        assert client.command("TTL", "k") == -1
        assert client.command("PTTL", "k") == -1

    def test_ttl_rounds_up(self, client):
        # 1500ms must report 2s, not 1s: Redis rounds up.
        client.command("SET", "k", "v", "PX", 1500)
        assert client.command("TTL", "k") == 2


class TestExpireCommands:
    def test_expire(self, client):
        client.command("SET", "k", "v")
        assert client.command("EXPIRE", "k", 100) == 1
        assert 0 < client.command("TTL", "k") <= 100

    def test_pexpire(self, client):
        client.command("SET", "k", "v")
        assert client.command("PEXPIRE", "k", 100_000) == 1
        assert 0 < client.command("PTTL", "k") <= 100_000

    def test_expireat(self, client):
        client.command("SET", "k", "v")
        assert client.command("EXPIREAT", "k", int(time.time()) + 100) == 1
        assert 0 < client.command("TTL", "k") <= 100

    def test_pexpireat(self, client):
        client.command("SET", "k", "v")
        deadline = int(time.time() * 1000) + 100_000
        assert client.command("PEXPIREAT", "k", deadline) == 1
        assert 0 < client.command("TTL", "k") <= 100

    def test_expire_on_a_missing_key(self, client):
        assert client.command("EXPIRE", "missing", 100) == 0

    def test_expire_in_the_past_deletes_the_key(self, client):
        client.command("SET", "k", "v")
        assert client.command("EXPIRE", "k", -1) == 1
        assert client.command("EXISTS", "k") == 0

    def test_expireat_in_the_past_deletes_the_key(self, client):
        client.command("SET", "k", "v")
        assert client.command("EXPIREAT", "k", 1) == 1
        assert client.command("EXISTS", "k") == 0

    def test_persist(self, client):
        client.command("SET", "k", "v", "EX", 100)
        assert client.command("PERSIST", "k") == 1
        assert client.command("TTL", "k") == -1
        assert client.command("PERSIST", "k") == 0

    def test_expire_options_are_not_supported(self, client):
        client.command("SET", "k", "v")
        assert client.command("EXPIRE", "k", 100, "NX") == Error("ERR syntax error")


@pytest.mark.slow
class TestActualExpiration:
    def test_key_disappears_after_its_deadline(self, client):
        client.command("SET", "k", "v", "PX", 100)
        assert client.command("GET", "k") == b"v"
        time.sleep(0.2)
        assert client.command("GET", "k") is None
        assert client.command("EXISTS", "k") == 0

    def test_expired_key_leaves_the_keyspace(self, client):
        client.command("SET", "gone", "v", "PX", 100)
        client.command("SET", "stays", "v")
        time.sleep(0.2)
        assert client.command("KEYS", "*") == [b"stays"]
        assert client.command("DBSIZE") == 1

    def test_ttl_reports_minus_two_after_expiry(self, client):
        client.command("SET", "k", "v", "PX", 100)
        time.sleep(0.2)
        assert client.command("TTL", "k") == -2

    def test_active_expiry_reclaims_untouched_keys(self, client):
        # Nothing reads these keys, so only the background cycle can remove them.
        # The counter is cumulative for the server's lifetime, so compare deltas.
        before = _expired_keys(client)
        for index in range(20):
            client.command("SET", f"tmp:{index}", "v", "PX", 100)
        time.sleep(0.5)
        assert _expired_keys(client) - before == 20

    def test_expiry_applies_to_every_type(self, client):
        client.command("RPUSH", "l", "a")
        client.command("PEXPIRE", "l", 100)
        client.command("ZADD", "z", 1, "m")
        client.command("PEXPIRE", "z", 100)
        time.sleep(0.2)
        assert client.command("LLEN", "l") == 0
        assert client.command("ZCARD", "z") == 0


def _expired_keys(client: RespClient) -> int:
    """Read the cumulative expired-key counter out of ``INFO stats``."""
    raw: bytes = client.command("INFO", "stats")
    for line in raw.decode().splitlines():
        if line.startswith("expired_keys:"):
            return int(line.split(":", 1)[1])
    raise AssertionError("expired_keys missing from INFO stats")
