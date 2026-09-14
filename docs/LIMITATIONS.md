# Limitations

What this server does not do, and why. A compatibility claim is only worth as
much as its caveats, so this list is deliberately complete.

---

## Verification status

**This implementation has not been compared side by side against a real
`redis-server`.** No Redis installation was available in the environment where
it was developed.

What that means in practice:

- Behaviour was implemented from the public Redis documentation and from the
  protocol specification, not by observing a running Redis.
- Correctness is established by this repository's own test suite: 585 tests,
  including the exact error strings, reply shapes and edge cases that
  compatibility consists of.
- **Do not read any claim here as "verified against Redis".** It is verified
  against the documented behaviour and against these tests.

Anyone with Redis installed can check it directly — the benchmark script takes a
`--port`, and every integration test talks plain RESP over TCP, so pointing them
at a real server is a one-line change.

---

## Not for production

No authentication (`AUTH`, ACLs), no TLS, no `maxmemory`, no eviction policy, no
`SHUTDOWN`, no cluster mode, no Sentinel, no Lua scripting, no keyspace
notifications, no `MONITOR`, no `SLOWLOG`.

The server binds to `127.0.0.1` by default for exactly that reason. Exposing an
unauthenticated database on `0.0.0.0` is opt-in via `--host`.

It is a learning and portfolio project. Use Redis in production.

---

## Data types

### Implemented

Strings, lists, streams and sorted sets — thoroughly, including the blocking and
range operations that make them interesting.

### Not implemented

| Type | Commands that would be needed |
| ---- | ----------------------------- |
| Hash | `HSET`, `HGET`, `HDEL`, `HGETALL`, `HINCRBY`, ... |
| Set | `SADD`, `SREM`, `SMEMBERS`, `SINTER`, `SUNION`, ... |
| Bitmap | `SETBIT`, `GETBIT`, `BITCOUNT`, `BITOP` |
| HyperLogLog | `PFADD`, `PFCOUNT`, `PFMERGE` |
| Geospatial | `GEOADD`, `GEOSEARCH`, `GEODIST` |

Hashes and sets would both be thin wrappers over Python's `dict` and `set` and
would add breadth without teaching anything the existing four types do not. The
project was scoped for depth instead.

---

## Commands with partial support

| Command | What is missing |
| ------- | --------------- |
| `SELECT` | Only index 0. There is one database. |
| `HELLO` | RESP2 only; `HELLO 3` returns `NOPROTO`. |
| `CONFIG` | `GET` only. No parameter here is safe to change at runtime, so `CONFIG SET` is reported as an unknown subcommand rather than accepted and ignored. |
| `COMMAND` | `COUNT`, `DOCS` and `INFO`. Key-position metadata (`first_key`/`last_key`/`step`) is reported as zero, because no key specs are tracked. Clients use those only for cluster routing. |
| `CLIENT` | `ID`, `SETNAME`, `GETNAME`, `INFO`, `LIST`. Not `KILL`, `PAUSE`, `NO-EVICT`, `UNPAUSE`. |
| `EXPIRE` family | The Redis 7 `NX`/`XX`/`GT`/`LT` modifiers return a syntax error. |
| `ZRANGE` | Index ranges and `WITHSCORES` only. Not `BYSCORE`, `BYLEX`, `REV` or `LIMIT`, and no `ZRANGEBYSCORE`/`ZRANGEBYLEX`. |
| `LPOP`/`RPOP` | Supported with and without a count. No `LMPOP`, `LPOS`, `LREM`, `LTRIM`, `LINSERT`, `RPOPLPUSH` or `LMOVE`. |
| `XREAD` | No consumer groups: no `XGROUP`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM` or `XAUTOCLAIM`. No `XDEL`, `XTRIM` or `XSETID`. |
| `BGSAVE` | Performs the save inline. See below. |
| `PSYNC` | Always a full resync. See below. |

---

## Persistence

### Streams are not persisted

`SAVE` writes strings, lists and sorted sets. Stream keys are skipped, and the
reply says how many:

```text
127.0.0.1:6379> SAVE
(error) ERR snapshot written, but 1 key(s) of unsupported type were skipped (streams are not persisted)
```

Redis encodes streams as listpacks — a packed, offset-threaded binary layout
substantially more involved than the rest of the format. Implementing half of it
would produce files that look valid and lose data silently, so it reports the
gap loudly instead.

The same applies to a full resync: streams do not transfer, and replicate only
from the moment the replica attaches.

### RDB interoperability is partial

| Direction | Status |
| --------- | ------ |
| Files this server writes → real Redis | Valid RDB v11; Redis will load them |
| Files real Redis writes → this server | Works for string, plain-list and `zset2` values, including integer-encoded and LZF-compressed strings |

The gap is that **Redis 7 usually encodes small collections as listpacks or
quicklists** (type bytes 16–21), which are not implemented. Such a file is
rejected with a clear message naming the type rather than loaded partially:

```text
RDB value type 18 (list (quicklist v2)) is not supported by this implementation
```

In practice, a `dump.rdb` from real Redis containing only string keys will load;
one containing lists, hashes or sets almost certainly will not.

### Only database 0 is loaded

An RDB file containing other databases loads database 0 and logs how many keys it
skipped.

### No AOF

There is no append-only file, so the durability story is snapshot-only: anything
written since the last `SAVE` is lost on a crash. Redis's AOF exists precisely
because that is often unacceptable.

### `BGSAVE` is not actually background

It performs the same synchronous save as `SAVE`. A real background save needs
`fork()` and copy-on-write — unavailable on Windows, and awkward in Python
regardless. Pretending to be asynchronous while blocking the event loop would be
worse than not offering it, so the behaviour is documented rather than faked.

### A corrupt snapshot stops the server starting

Deliberate. Starting with an empty keyspace would hide the problem, and the next
`SAVE` would overwrite the evidence. Redis refuses to start for the same reason.

---

## Replication

### Full resync only

`PSYNC` always answers `+FULLRESYNC`. Partial resynchronisation needs a
replication backlog — a ring buffer of recent commands a reconnecting replica can
replay from — which is not implemented. A replica that drops and reconnects
re-transfers the whole dataset.

Answering `+FULLRESYNC` unconditionally is the correct fallback, and is what
Redis itself does when its backlog cannot satisfy a request.

### Asynchronous, so writes can be lost

A master acknowledges a write before any replica has seen it. If the master dies
in between, that write is gone. `WAIT numreplicas timeout` returns how many
replicas have acknowledged an offset, which lets a client observe replication
progress — but it is **not** a durability guarantee, and no amount of `WAIT`
makes it one. Redis has the same property.

### No chained replication

A replica does not re-propagate to sub-replicas. Every replica connects directly
to the master.

### No failover

No Sentinel, no automatic promotion, no `REPLICAOF NO ONE` at runtime. A replica
is a replica for the life of the process.

### Transactions propagate individually

Redis wraps a replicated transaction in `MULTI`/`EXEC`. Here each command is
propagated on its own. Because a replica applies its master's stream
sequentially and nothing interleaves, the result is equivalent — but a chained or
concurrently-modified replica would not have that guarantee.

### Pub/Sub does not cross the replication link

`PUBLISH` on a master does not reach clients subscribed on a replica. Real Redis
forwards publishes to replicas; this does not.

---

## Protocol

### RESP2 only

`HELLO 3` returns `NOPROTO`. See [PROTOCOL.md](PROTOCOL.md#resp3) for what RESP3
would add and why the push type is the part that would genuinely help.

### Inline commands are supported but limited

Quoting follows Redis's `sdssplitargs`, including `\xHH` escapes. The 64 KB
inline limit applies.

---

## Concurrency

### Single-threaded, so one core

Throughput is bounded by one CPU. Redis has the same limit for command execution;
the difference is that Python's constant factor is far larger. See the
[benchmark numbers](../README.md#performance).

### O(n) commands block everyone

`KEYS *`, `FLUSHALL` and `DBSIZE` on a large keyspace stop the world for their
duration — exactly as in Redis, and exactly as unsuitable for production use
there. `KEYS` carries the warning in its docstring.

### Blocked-client ordering is not a documented guarantee

When several clients are blocked on the same key, futures resolve in registration
order and the tests confirm first-come-first-served in practice. Redis documents
this as a guarantee; here it is an emergent property of the event loop, not one
the implementation enforces. Do not depend on it.

---

## Version reporting

`INFO` reports:

```text
redis_version:7.0.0
pyredis_version:1.0.0
```

Client libraries gate behaviour on `redis_version`, and reporting nothing — or
something unparseable — breaks them. This server implements a **subset** of that
Redis version's commands; `pyredis_version` is the authoritative version of this
implementation. Other Redis-compatible servers take the same approach for the
same reason.

---

## Data structure complexity

`SortedSet` insertion and removal are O(n) rather than Redis's O(log n), because
it uses a sorted list rather than a skip list. Lookups, ranks and range queries
are O(log n) via binary search. The reasoning is in
[ARCHITECTURE.md](ARCHITECTURE.md#sortedset).

---

## What a "next version" would add, in order

1. Hashes and sets — the obvious breadth gap.
2. Listpack and quicklist decoding, which would make real Redis `dump.rdb` files
   load properly.
3. A replication backlog, enabling partial resync.
4. RESP3, with a real push type for Pub/Sub.
5. A skip list for sorted sets.
6. AOF persistence with `fsync` policies.
7. `maxmemory` with LRU/LFU eviction.
