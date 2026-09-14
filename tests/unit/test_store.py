"""Unit tests for the keyspace: storage, expiry and touch notification.

Expiry is tested against an injected clock rather than ``time.sleep`` so the
suite stays fast and deterministic.
"""

from __future__ import annotations

from collections import deque

import pytest

from pyredis.datastore import NO_EXPIRY, NO_SUCH_KEY, SortedSet, Store, Stream, require_type
from pyredis.errors import CommandError


class FakeClock:
    """A manually advanced millisecond clock."""

    def __init__(self, now: int = 1_000_000) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now

    def advance(self, ms: int) -> None:
        self.now += ms


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def store(clock: FakeClock) -> Store:
    return Store(clock=clock)


class TestBasicStorage:
    def test_get_missing_key(self, store):
        assert store.get(b"nope") is None

    def test_set_then_get(self, store):
        store.set(b"k", b"v")
        assert store.get(b"k") == b"v"

    def test_overwrite(self, store):
        store.set(b"k", b"a")
        store.set(b"k", b"b")
        assert store.get(b"k") == b"b"

    def test_delete(self, store):
        store.set(b"k", b"v")
        assert store.delete(b"k") is True
        assert store.get(b"k") is None
        assert store.delete(b"k") is False

    def test_contains(self, store):
        store.set(b"k", b"v")
        assert b"k" in store
        assert b"other" not in store

    def test_len_and_keys(self, store):
        store.set(b"a", b"1")
        store.set(b"b", b"2")
        assert len(store) == 2
        assert sorted(store.all_keys()) == [b"a", b"b"]

    def test_flush(self, store):
        store.set(b"a", b"1")
        store.flush()
        assert len(store) == 0

    def test_setdefault_creates_once(self, store):
        first = store.setdefault(b"l", deque)
        first.append(b"x")
        second = store.setdefault(b"l", deque)
        assert second is first
        assert list(second) == [b"x"]

    def test_binary_keys_and_values(self, store):
        key, value = b"\x00\xff", b"\r\n\x00"
        store.set(key, value)
        assert store.get(key) == value


class TestTypeNames:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (b"s", "string"),
            (deque([b"a"]), "list"),
            (Stream(), "stream"),
            (SortedSet(), "zset"),
        ],
    )
    def test_type_name(self, store, value, expected):
        store.set(b"k", value)
        assert store.type_name(b"k") == expected

    def test_type_name_for_missing_key(self, store):
        assert store.type_name(b"k") == "none"

    def test_require_type_passes_matching_value(self):
        value = deque([b"a"])
        assert require_type(value, deque) is value

    def test_require_type_raises_wrongtype(self):
        with pytest.raises(CommandError, match="WRONGTYPE"):
            require_type(b"string", deque)


class TestExpiry:
    def test_key_without_ttl_never_expires(self, store, clock):
        store.set(b"k", b"v")
        clock.advance(10**9)
        assert store.get(b"k") == b"v"

    def test_key_is_readable_before_deadline(self, store, clock):
        store.set(b"k", b"v", expires_at_ms=clock.now + 100)
        clock.advance(99)
        assert store.get(b"k") == b"v"

    def test_key_is_gone_at_deadline(self, store, clock):
        store.set(b"k", b"v", expires_at_ms=clock.now + 100)
        clock.advance(100)
        assert store.get(b"k") is None

    def test_expired_key_is_excluded_from_keys(self, store, clock):
        store.set(b"live", b"v")
        store.set(b"dead", b"v", expires_at_ms=clock.now + 10)
        clock.advance(10)
        assert store.all_keys() == [b"live"]
        assert len(store) == 1

    def test_pttl_states(self, store, clock):
        assert store.pttl_ms(b"missing") == NO_SUCH_KEY
        store.set(b"plain", b"v")
        assert store.pttl_ms(b"plain") == NO_EXPIRY
        store.set(b"timed", b"v", expires_at_ms=clock.now + 250)
        assert store.pttl_ms(b"timed") == 250
        clock.advance(100)
        assert store.pttl_ms(b"timed") == 150

    def test_set_clears_existing_ttl(self, store, clock):
        store.set(b"k", b"v", expires_at_ms=clock.now + 100)
        store.set(b"k", b"v2")
        assert store.pttl_ms(b"k") == NO_EXPIRY

    def test_keep_ttl_preserves_deadline(self, store, clock):
        store.set(b"k", b"v", expires_at_ms=clock.now + 100)
        store.set(b"k", b"v2", keep_ttl=True)
        assert store.pttl_ms(b"k") == 100

    def test_set_expiry_at_on_existing_key(self, store, clock):
        store.set(b"k", b"v")
        assert store.set_expiry_at(b"k", clock.now + 50) is True
        assert store.pttl_ms(b"k") == 50

    def test_set_expiry_at_on_missing_key(self, store, clock):
        assert store.set_expiry_at(b"k", clock.now + 50) is False

    def test_persist(self, store, clock):
        store.set(b"k", b"v", expires_at_ms=clock.now + 50)
        assert store.persist(b"k") is True
        assert store.pttl_ms(b"k") == NO_EXPIRY
        assert store.persist(b"k") is False

    def test_load_skips_already_expired_entries(self, store, clock):
        store.load(b"fresh", b"v", clock.now + 10)
        store.load(b"stale", b"v", clock.now - 10)
        assert store.get(b"fresh") == b"v"
        assert store.get(b"stale") is None


class TestActiveExpireCycle:
    def test_removes_only_expired_keys(self, store, clock):
        store.set(b"soon", b"v", expires_at_ms=clock.now + 10)
        store.set(b"later", b"v", expires_at_ms=clock.now + 10_000)
        store.set(b"never", b"v")
        clock.advance(10)
        assert store.active_expire_cycle() == 1
        assert sorted(store.all_keys()) == [b"later", b"never"]

    def test_is_a_no_op_when_nothing_expired(self, store, clock):
        store.set(b"k", b"v", expires_at_ms=clock.now + 1000)
        assert store.active_expire_cycle() == 0

    def test_respects_the_limit(self, store, clock):
        for index in range(10):
            store.set(b"k%d" % index, b"v", expires_at_ms=clock.now + 1)
        clock.advance(1)
        assert store.active_expire_cycle(limit=4) == 4

    def test_stale_heap_entries_are_ignored(self, store, clock):
        # Re-setting the TTL leaves the old deadline behind in the heap.
        store.set(b"k", b"v", expires_at_ms=clock.now + 10)
        store.set_expiry_at(b"k", clock.now + 10_000)
        clock.advance(20)
        assert store.active_expire_cycle() == 0
        assert store.get(b"k") == b"v"

    def test_heap_does_not_grow_without_bound(self, store, clock):
        for _ in range(500):
            store.set(b"churn", b"v", expires_at_ms=clock.now + 10_000)
        # 500 pushes for a single live deadline must have been compacted away.
        assert len(store._expire_heap) <= 2 * len(store._expires) + 64


class TestTouchListeners:
    def test_set_touches(self, store):
        seen: list[bytes] = []
        store.add_touch_listener(seen.append)
        store.set(b"k", b"v")
        assert seen == [b"k"]

    def test_delete_touches(self, store):
        store.set(b"k", b"v")
        seen: list[bytes] = []
        store.add_touch_listener(seen.append)
        store.delete(b"k")
        assert seen == [b"k"]

    def test_lazy_expiry_touches(self, store, clock):
        store.set(b"k", b"v", expires_at_ms=clock.now + 10)
        seen: list[bytes] = []
        store.add_touch_listener(seen.append)
        clock.advance(10)
        store.get(b"k")
        assert seen == [b"k"]

    def test_flush_touches_every_key(self, store):
        store.set(b"a", b"1")
        store.set(b"b", b"2")
        seen: list[bytes] = []
        store.add_touch_listener(seen.append)
        store.flush()
        assert sorted(seen) == [b"a", b"b"]

    def test_explicit_touch(self, store):
        seen: list[bytes] = []
        store.add_touch_listener(seen.append)
        store.touch(b"k")
        assert seen == [b"k"]

    def test_load_does_not_touch(self, store, clock):
        seen: list[bytes] = []
        store.add_touch_listener(seen.append)
        store.load(b"k", b"v", None)
        assert seen == []

    def test_multiple_listeners_all_fire(self, store):
        first: list[bytes] = []
        second: list[bytes] = []
        store.add_touch_listener(first.append)
        store.add_touch_listener(second.append)
        store.set(b"k", b"v")
        assert first == second == [b"k"]
