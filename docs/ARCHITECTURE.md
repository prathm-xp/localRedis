# Architecture

How the server is put together, and why each piece is shaped the way it is.

---

## The shape of the system

```text
                    ┌─────────────────────────────────────────┐
                    │              Server                     │
                    │  config · store · pubsub · replication  │
                    │  blocking · watch · stats · connections │
                    └───────────────┬─────────────────────────┘
                                    │ owns
        ┌───────────────────────────┼───────────────────────────┐
        ▼                           ▼                           ▼
┌───────────────┐         ┌──────────────────┐        ┌──────────────────┐
│  Connection   │  ...    │   Connection     │        │ background tasks │
│  (a client)   │         │ (a replica link) │        │ expiry · replica │
└───────┬───────┘         └──────────────────┘        └──────────────────┘
        │
        │ per command
        ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│  RespReader   │──►│ CommandTable  │──►│    handler    │──►│     Store     │
│   framing     │   │   dispatch    │   │   execution   │   │    storage    │
└───────────────┘   └───────────────┘   └───────┬───────┘   └───────┬───────┘
                                                │                   │
                                                ▼                   ▼ touch
                                        ┌───────────────┐   ┌───────────────┐
                                        │ RESP encoder  │   │ blocking +    │
                                        │   replies     │   │ watch signals │
                                        └───────────────┘   └───────────────┘
```

Every arrow points one way. The store knows nothing about connections; command
handlers know nothing about sockets; the parser knows nothing about commands.
The only component that knows about everything is `Server`, and its job is to
hold the others.

---

## Components

| Module | Responsibility | Depends on |
| ------ | -------------- | ---------- |
| `config` | Immutable `ServerConfig`, CLI parsing | nothing |
| `errors` | Exception hierarchy, Redis error strings | nothing |
| `globmatch` | Redis-compatible glob matching | nothing |
| `protocol/reader` | Bytes → commands, resumably | `errors` |
| `protocol/resp` | Replies → bytes | nothing |
| `datastore/objects` | `Stream`, `SortedSet` | `errors` |
| `datastore/store` | Keyspace, expiry, touch signal | `objects`, `errors` |
| `notifications` | Blocking waiters, `WATCH` registry | nothing at runtime |
| `pubsub` | Channel and pattern routing | `globmatch`, `protocol` |
| `persistence/*` | RDB read and write | `datastore`, `errors` |
| `replication/state` | Offsets, replica registry, `WAIT` | `protocol` |
| `replication/replica` | Replica-side handshake and link | `connection`, `persistence` |
| `commands/*` | One module per command family | `datastore`, `protocol`, `errors` |
| `connection` | Per-client state, read loop, dispatch | `protocol`, `commands/base` |
| `server` | Lifecycle, accept loop, background tasks | everything |

The import graph is acyclic. Command modules reference `Connection` only under
`TYPE_CHECKING`, and where a handler genuinely needs to change connection state —
`PSYNC` turning a client into a replica — it calls a method
(`become_replica_link`) rather than importing the enum.

---

## Concurrency model

**One thread. One asyncio event loop. One coroutine per connection.**

The consequence that matters is this: a command handler runs from start to
finish without yielding, so **every command is atomic with respect to every other
command**. Nothing can observe a half-applied `MSET`. The keyspace needs no
locks, no atomics and no copy-on-write, because there is never a second thread
inside it.

That is the same guarantee Redis provides, reached the same way. It is also what
makes `MULTI`/`EXEC` nearly free: running the queued commands in a loop already
gives the required isolation.

### What it costs

- **One core.** Throughput is bounded by a single CPU. Real Redis has the same
  limit for command execution and is fine with it, because the bottleneck is
  usually the network. In Python the constant factor is far worse.
- **A slow command blocks everyone.** `KEYS *` on a large keyspace is O(n) and
  stops the world for its duration — exactly as in Redis, and exactly as
  unsuitable for production use there.

### The blocking commands

`BLPOP`, `BRPOP`, `XREAD BLOCK` and `WAIT` have to wait, and waiting on the only
thread would be fatal. They are written as coroutines that `await` a future:

```text
BLPOP queue 0
   ├─ is there an element?  ──yes──► pop it, reply
   └─ no
        └─ await a future registered against "queue"     ← the loop is free
             ▲
             │ RPUSH queue job  ──►  store.touch(b"queue")
             │                            │
             └────────────────────────────┘ BlockingManager.on_touch resolves it
```

The client waits. The server does not. A thousand blocked clients cost a
thousand small future objects and no threads.

Two subtleties the implementation has to get right:

1. **Waking is not the same as being served.** Another client may take the
   element first, so a woken waiter re-checks and, if it finds nothing, waits
   again — with the *remaining* timeout, computed from the original deadline. A
   client that asked to wait one second waits one second in total, however many
   spurious wake-ups it survives.
2. **A blocked client is only served by the right type.** If a key a client is
   blocked on gains a *string*, the client stays blocked rather than receiving a
   `WRONGTYPE` error, matching Redis. A key that already holds the wrong type
   when `BLPOP` is first called is an immediate error. The two cases are
   distinguished by a `first_pass` flag.

### Alternative considered: a thread per connection

It would use every core. But every keyspace operation would need a lock,
`MULTI`/`EXEC` would need a global one (removing most of the benefit), 10,000
idle connections would cost 10,000 stacks, and the whole class of race-condition
bugs would be back. For a request/response workload where each command is
microseconds of work, the event loop is the better fit.

---

## Storage model

### Two dictionaries

```python
self._data: dict[bytes, object]  # key -> value
self._expires: dict[bytes, int]  # key -> deadline in ms, only if it has one
```

Most keys never get a TTL. Keeping deadlines in a separate, usually-small
dictionary avoids paying for an expiry slot on every key, and is exactly how
Redis structures `redisDb`.

### Value types

| Redis type | Python representation | Why |
| ---------- | --------------------- | --- |
| string | `bytes` | Binary-safe by nature, and the protocol hands us bytes already |
| list | `collections.deque[bytes]` | O(1) push and pop at both ends, which is what lists are for |
| stream | `Stream` | Needs the monotonic-ID invariant and binary search |
| zset | `SortedSet` | Needs score lookup and ordered access at once |

`bytes` and `deque` are used directly rather than wrapped. A wrapper would add
an allocation and an attribute lookup to every operation and would buy nothing:
the type is already known, and `type_name_of` maps it to a Redis type name with
a dict lookup.

### Stream

An append-only `list[StreamEntry]`, ordered by construction because IDs must
increase. Range queries therefore use `bisect` rather than a scan: `XRANGE` is
O(log n + k) for k results.

`StreamId` is a `NamedTuple` of `(ms, seq)`, which gives the required ordering
for free — tuple comparison *is* stream-ID comparison.

The interesting logic is ID generation, where the clock does not cooperate:

- Two entries in the same millisecond → keep the timestamp, increment the
  sequence.
- The clock went backwards → keep the *last entry's* timestamp, increment the
  sequence. IDs must be strictly increasing whatever the clock does.
- `0-*` starts at `0-1`, because `0-0` is reserved.

### SortedSet

A `dict[member, score]` for O(1) score lookup, plus a `list[(score, member)]`
kept sorted with `bisect`.

| Operation | This implementation | Redis (skip list) |
| --------- | ------------------- | ----------------- |
| `ZSCORE` | O(1) | O(1) |
| `ZRANK` | O(log n) | O(log n) |
| `ZRANGE` by index | O(log n + k) | O(log n + k) |
| `ZADD` | O(n) | O(log n) |
| `ZREM` | O(n) | O(log n) |

The O(n) insert is a list shift — fast in absolute terms because it is `memmove`
on a contiguous array, but asymptotically worse. A skip list would fix it at
roughly five times the code. For a project whose point is clarity, and at the
set sizes it is built for, the trade is worth making. Recording it here rather
than hiding it is the other half of the decision.

---

## Expiry

Expiry has two jobs that pull in different directions: it must be **correct**
(an expired key is never visible) and it should be **tidy** (memory comes back
even for keys nobody reads again). Redis solves them separately, and so does
this.

### Lazy expiry makes it correct

`Store.get` checks the deadline on every access and removes the key if it has
passed. Correctness therefore does not depend on the background task running at
all — or on when it last ran.

### A deadline heap makes it tidy

Redis samples twenty random keys with TTLs, removes the expired ones, and repeats
if more than a quarter were expired. It is a probabilistic sweep.

This implementation keeps a min-heap of `(deadline, key)` instead:

```text
active_expire_cycle():
    while heap and heap[0].deadline <= now:
        pop
        if the key's current deadline still matches:   ← stale entries filtered
            remove the key
```

Checking whether there is anything to do is O(1) — look at the root. Each key
actually removed costs O(log n). Nothing is sampled and nothing is missed.

The subtlety is **stale entries**: re-setting a TTL leaves the old
`(deadline, key)` pair behind. They are ignored on pop, and when they outnumber
the live ones the heap is rebuilt *from the deadline dictionary* — not by
filtering the heap, because repeatedly setting the same TTL pushes identical
tuples that a filter cannot tell apart. (That was a real bug, caught by the test
that hammers one key's TTL five hundred times.)

The rebuild threshold leaves room for at least `len(_expires) + 64` further
pushes, so the amortised cost is O(1) per push.

---

## The keyspace touch signal

Two features need to know when a key changes:

- `BLPOP` and `XREAD BLOCK` — "did this key gain a value?"
- `WATCH` — "did this key change at all?"

Rather than building each into the store, the store broadcasts one signal and
both subscribe:

```python
store.add_touch_listener(blocking_manager.on_touch)
store.add_touch_listener(watch_registry.on_touch)
```

`Store.set`, `delete`, `flush`, `set_expiry_at`, `persist` and lazy expiry all
call `touch`. Commands that mutate a container **in place** — `RPUSH` onto an
existing list, `XADD` onto an existing stream — must call `store.touch(key)`
explicitly, because the store cannot see a mutation that never passed through it.

`WATCH` is implemented the way Redis implements it: rather than versioning keys,
the registry flips a `watch_dirty` flag on each watching connection the moment
the key is touched. `EXEC` then checks one boolean.

---

## The command layer

### Registration without a global registry

Each command module builds its own `CommandSet`:

```python
commands = CommandSet()


@commands.add("GET", arity=2)
def get(connection, args): ...
```

and `commands/__init__.py` composes them into one immutable `CommandTable`.
Importing a command module has no side effect beyond defining functions, and the
table is built once, explicitly, in one readable place. Duplicate registrations
raise at import time.

`arity` follows the Redis convention — positive means exact, negative means
minimum — so the numbers can be read straight off the Redis command table.

### Sync and async handlers

Most handlers are plain functions. The four that must wait are `async`. The spec
records which is which at registration time (`inspect.iscoroutinefunction`), so
the dispatcher branches on a boolean rather than inspecting anything per call,
and the common path allocates no coroutine.

### Dispatch policy

Before a handler runs, the connection applies, in order:

1. **Unknown command** → Redis's exact error, including the arguments.
2. **Wrong arity** → error. In `MULTI`, this and the previous case poison the
   transaction.
3. **Subscribe mode** → in RESP2 a subscribed connection may only issue a
   handful of commands, because otherwise a client could not tell a reply apart
   from a pushed message.
4. **Transaction queueing** → queue and reply `QUEUED`, unless the command is one
   of the transaction control commands.
5. **Replica read-only** → a write from a normal client on a replica is refused
   with `READONLY`. Writes arriving on the master link are not, which is the
   whole point of the connection-kind distinction.

`EXEC` re-enters through `Connection.invoke`, which performs lookup and the arity
check but skips queueing and subscribe-mode policy — those have already been
applied.

---

## Connections

One class serves three roles:

| Kind | What it is | Replies | Offsets |
| ---- | ---------- | ------- | ------- |
| `CLIENT` | An ordinary client | sent | — |
| `REPLICA_LINK` | A client that issued `PSYNC` | propagation only | receives `ACK` |
| `MASTER_LINK` | On a replica, the link to its master | suppressed | counted |

Reusing one class for the master link is the design decision that keeps
replication honest: the master's write stream *is* a stream of commands, so it
goes through the same parser, the same dispatcher and the same handlers. A
separate "apply a replicated command" path would be a second implementation of
the same logic, free to drift from the first.

### Reply batching

Replies are buffered per read batch and written once:

```text
read()  ──►  [cmd][cmd][cmd]...[cmd]  ──►  handle each, buffer each reply
                                       ──►  one write(), one drain()
```

A pipeline of a thousand commands costs one `write` rather than a thousand. That
is where most of the throughput in the benchmark comes from: without pipelining
the workload is bound by round trips and syscalls, not by anything computed.

Cross-connection sends — propagation to replicas, Pub/Sub delivery — flush the
target's buffered replies first, so ordering can never be inverted.

### Backpressure and buffer limits

- **Input:** a connection whose unparsed buffer exceeds `max_query_buffer`
  (1 GB, as Redis) is closed with a protocol error.
- **Output:** a connection whose unsent output exceeds 256 MB is dropped. Redis
  calls this `client-output-buffer-limit`; without it, one slow replica or one
  client that never reads can exhaust the server's memory.
- **Per-client:** `await writer.drain()` after each batch applies normal TCP
  backpressure to that client.

---

## Replication

### The handshake

```text
replica                                    master
   │                                          │
   │── PING ─────────────────────────────────►│
   │◄──────────────────────────────── +PONG ──│
   │── REPLCONF listening-port <port> ───────►│
   │◄────────────────────────────────── +OK ──│
   │── REPLCONF capa psync2 ─────────────────►│
   │◄────────────────────────────────── +OK ──│
   │── PSYNC ? -1 ───────────────────────────►│
   │◄───────────── +FULLRESYNC <replid> 0 ────│
   │◄───────────── $<len>\r\n<rdb bytes> ─────│   note: no trailing CRLF
   │                                          │
   │◄═══════════ write command stream ════════│
   │── REPLCONF ACK <offset> ────────────────►│
```

The RDB payload is the one place the protocol deviates from normal bulk-string
framing: there is no trailing CRLF. Reading two bytes too many would silently
consume the first command of the replication stream.

### Offsets

The replication offset is a byte count of the command stream, and it is what
makes `WAIT` and `REPLCONF ACK` work. Three details are easy to get wrong:

1. **The offset only starts advancing once a replica has attached.** Redis
   allocates its replication backlog on the first `PSYNC`, so a master that never
   had a replica reports `master_repl_offset:0` however many writes it served.
   Modelled here with a `_backlog_active` flag.

2. **`REPLCONF GETACK` occupies bytes in the stream itself.** A replica must
   answer with the offset from *before* that command and then count it. This
   falls out naturally from the read loop counting a command's bytes *after*
   executing it — get it the other way round and the two sides disagree by
   exactly the length of the GETACK, forever.

3. **`WAIT` with nothing outstanding returns immediately.** If no write has
   happened since the last acknowledgement, every replica is already current and
   simply has not said so. Sending `GETACK` in that case would be a pointless
   round trip.

### Deterministic propagation

Replication replays commands, so a non-deterministic command produces a
different result on the replica. Handlers rewrite themselves before propagation:

| Command | Propagated as | Why |
| ------- | ------------- | --- |
| `XADD key *` | `XADD key <resolved-id>` | The replica's clock and last-ID would generate a different ID |
| `SET k v PX 100` | `SET k v PXAT <deadline>` | A relative expiry restarts its clock on the replica |
| `SET k v NX` | `SET k v` | The condition was already evaluated; re-evaluating could reject a write the master accepted |
| `EXPIRE` family | `PEXPIREAT key <deadline>` | Same reason as `PX` |
| `BLPOP key 0` | `LPOP key` | Replaying a blocking command would stall the replication stream |
| `BLPOP` (timed out) | *nothing* | It did not write |
| `GETDEL key` | `DEL key` | The read half is irrelevant to the replica |

The mechanism is a per-command override on the connection with three states:
`None` means propagate verbatim, a list means propagate that instead, and an
empty list means propagate nothing.

### Resilience

The replica link runs as one background task for the life of the server. If it
drops, the task reconnects with exponential backoff capped at 16 seconds rather
than exiting — a replica that gives up after one network blip is not much of a
replica. Each handshake step has a 10-second timeout so a silent master cannot
hang the task.

On reconnect the replica **flushes its own keyspace** before loading the master's
snapshot. Anything it had is stale by definition.

---

## Persistence

RDB is a point-in-time binary snapshot:

```text
"REDIS0011"
0xFA <key> <value>        auxiliary metadata, repeated
0xFE <db>                 select database
0xFB <size> <expires>     size hint
  [0xFC <8-byte ms>]      optional expiry
  <type byte>
  <key>
  <value>
  ... repeated
0xFF
<8-byte CRC-64>
```

The length encoding is the part worth reading carefully: the top two bits of the
first byte select a 6-bit length, a 14-bit length, a 32/64-bit length, or an
*escape* into special string encodings — small integers stored as integers, and
LZF-compressed strings.

The reader implements all of those, including LZF decompression and the CRC-64
variant Redis uses (Jones polynomial, reflected, check value
`0xe9c6d914c4b8d9ca` — asserted in the test suite). The writer deliberately emits
only the plain encodings: they are valid, they are what the format makes
optional, and the output is trivially inspectable.

Snapshots are written to a temporary file and renamed into place, so a crash
mid-write cannot destroy a good file.

**Streams are not written**, and the writer reports how many keys it skipped.
The listpack encoding Redis uses for streams is substantially more involved than
the rest of the format, and implementing half of it would produce files that
look valid and lose data silently.

A corrupt RDB file **stops the server starting**. Starting with an empty
keyspace would hide the problem, and the next `SAVE` would overwrite the
evidence.

---

## Error handling

Two families, handled differently because they mean different things:

**`ProtocolError`** — the byte stream is not valid RESP. Framing is lost and
there is no way to resynchronise, so the server replies with an error and closes
the connection. That is what Redis does, and the alternative — trying to recover
— would mean interpreting arbitrary bytes as commands.

**`CommandError`** — a well-formed command that cannot be executed: wrong arity,
wrong type, a bad integer. The connection stays open and the error is returned as
a RESP error reply. Inside `MULTI`, *when* the error is detected matters: at
queue time it aborts the transaction with `EXECABORT`; at run time it appears
inside the `EXEC` reply array and the remaining commands still run.

Error messages are produced by factory functions in one module, so the exact
wording — which clients and test suites match on — is defined once.

Nothing is caught and ignored. The only broad handler is a last-resort guard
around each client task that logs a traceback and drops that one connection, so
a bug in one command cannot take the server down.

---

## Where the bottlenecks are

Measured, not guessed — see the benchmark numbers in the README.

1. **Syscalls, without pipelining.** One `read` and one `write` per command
   dominates. Pipelining depth 16 raises throughput about sixfold, which is the
   clearest evidence of where the time goes.
2. **Python's interpreter overhead.** Parsing and dispatch are bytecode. A C
   implementation would be an order of magnitude faster for identical
   algorithms.
3. **`bytes` allocation in the parser.** Each argument is copied out of the
   buffer. `memoryview` slices would avoid the copy but complicate lifetime
   management for values that get stored.
4. **`SortedSet` insertion**, O(n), at large set sizes.

Things deliberately *not* optimised, because correctness came first and the
profile does not justify them: no object pooling, no custom event loop, no
`__slots__` micro-tuning beyond the hot classes that already have it, no C
extension.

Obvious next steps if throughput mattered: `uvloop` on Linux (a drop-in event
loop replacement), `memoryview`-based zero-copy parsing, and a skip list for
sorted sets.
