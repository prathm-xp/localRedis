# pyredis

A Redis-compatible in-memory key-value server written from scratch in Python, with
no runtime dependencies outside the standard library.

It speaks the real RESP protocol, so `redis-cli`, `redis-py` and any other Redis
client can talk to it unmodified. It implements strings, lists, streams and sorted
sets; key expiration; transactions; publish/subscribe; RDB snapshot persistence;
and asynchronous master/replica replication.

This is a learning and portfolio project, not a production datastore. See
[Limitations](#limitations) for an honest account of what it does not do.

```text
$ pyredis-server --port 6379
$ redis-cli -p 6379
127.0.0.1:6379> SET greeting "hello"
OK
127.0.0.1:6379> GET greeting
"hello"
127.0.0.1:6379> SET session abc123 PX 100
OK
127.0.0.1:6379> GET session          # 100 ms later
(nil)
```

---

## Contents

- [Why this project](#why-this-project)
- [Quick start](#quick-start)
- [Features](#features)
- [Architecture](#architecture)
- [The RESP protocol](#the-resp-protocol)
- [Supported commands](#supported-commands)
- [Design decisions](#design-decisions)
- [Performance](#performance)
- [Testing](#testing)
- [Limitations](#limitations)
- [Development](#development)
- [Publishing to GitHub](#publishing-to-github)
- [Further documentation](#further-documentation)
- [License](#license)

---

## Why this project

Redis is a good system to rebuild because every layer of it is something you have
to get exactly right, and every layer is observable from the outside:

- **Networking.** A TCP server that must handle partial reads, pipelining, slow
  clients and abrupt disconnects.
- **Protocol.** A binary-safe wire format where framing errors are unrecoverable.
- **Data structures.** Each type has published complexity guarantees.
- **Time.** Expiry has to be correct on read *and* tidy in the background.
- **Concurrency.** Thousands of connections, blocking commands, and one thread.
- **Distribution.** Replication, offsets, and the difference between "sent" and
  "acknowledged".

The scope follows the CodeCrafters "Build Your Own Redis" track. The
implementation is entirely original: no challenge code, tests or artifacts were
used, and behaviour was written against the public Redis documentation and
verified with the test suite in this repository.

---

## Quick start

Requires **Python 3.11 or newer**. Nothing else.

```bash
git clone <your-repository-url>
cd localRedis
python -m venv .venv
```

Activate the environment — on Linux/macOS:

```bash
source .venv/bin/activate
```

On Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Then install and run:

```bash
python -m pip install -e ".[dev]"
```

```bash
pyredis-server --port 6379
```

Connect with `redis-cli` if you have it, or with anything that speaks RESP —
including `nc`, because inline commands are supported:

```bash
printf 'PING\r\nSET name aryan\r\nGET name\r\n' | nc 127.0.0.1 6379
```

A full, start-from-a-clean-machine setup guide — including how to check what you
already have installed and how to install what you don't — is in
[docs/SETUP.md](docs/SETUP.md).

---

## Features

Everything listed here is implemented and covered by the test suite.

**Networking and protocol**
- Asynchronous TCP server on a single event loop, handling many concurrent clients
- RESP2 request parsing, resumable across arbitrary packet boundaries
- Command pipelining, with replies batched into one write per read
- Inline commands, so `telnet`/`nc` work
- Protocol-error detection with Redis's exact error messages and connection close
- Bounded input and output buffers, and a configurable client limit

**Data types**
- Strings, with the full modern `SET` option set
- Lists (`deque`-backed, O(1) at both ends), including blocking pops
- Streams, with the complete entry-ID model and blocking reads
- Sorted sets, ordered by `(score, member)`

**Key lifecycle**
- Expiry in seconds or milliseconds, relative or absolute
- Lazy expiry on access plus an active background cycle driven by a deadline heap
- `TTL`/`PTTL`/`PERSIST` with Redis's exact return conventions

**Server features**
- Transactions: `MULTI`/`EXEC`/`DISCARD` with `WATCH`-based optimistic locking
- Publish/subscribe, with pattern subscriptions and RESP2 subscribe mode
- RDB snapshot persistence: real RDB v11 files, written atomically
- Master/replica replication: handshake, full resync, command propagation,
  `REPLCONF ACK` offsets and `WAIT`
- `INFO`, `CONFIG GET`, `CLIENT`, `COMMAND` introspection

---

## Architecture

```text
                         ┌──────────────────────────────┐
   client sockets ──────►│  asyncio event loop (1 thread)│
                         └──────────────┬───────────────┘
                                        │ one coroutine per connection
                                        ▼
                              ┌───────────────────┐
                              │    Connection     │  parse buffer, subscriptions,
                              │  (read loop)      │  transaction queue, watch list
                              └─────────┬─────────┘
                                        │
              ┌─────────────────────────┼─────────────────────────┐
              ▼                         ▼                         ▼
     ┌─────────────────┐      ┌──────────────────┐      ┌──────────────────┐
     │   RespReader    │      │  CommandTable    │      │  RESP serializer │
     │  (framing)      │      │  (dispatch)      │      │  (replies)       │
     └─────────────────┘      └────────┬─────────┘      └──────────────────┘
                                       │
                                       ▼
                              ┌──────────────────┐
                              │  command handler │
                              └────────┬─────────┘
                                       │
                                       ▼
                              ┌──────────────────┐
                              │      Store       │  keyspace + expiry deadlines
                              └────────┬─────────┘
                                       │ touch(key)
                       ┌───────────────┴───────────────┐
                       ▼                               ▼
              ┌─────────────────┐            ┌──────────────────┐
              │ BlockingManager │            │  WatchRegistry   │
              │ (wakes BLPOP)   │            │ (aborts EXEC)    │
              └─────────────────┘            └──────────────────┘

     writes ──► ReplicationState ──► replica connections
```

### Request lifecycle

1. The event loop wakes a connection's coroutine with a chunk of bytes.
2. `RespReader.feed` appends them; `read_command` returns complete commands one at
   a time, or `None` when more bytes are needed. Partial and pipelined commands
   are the same code path.
3. The dispatcher looks the command up, checks its arity, and applies connection
   policy — subscribe-mode restrictions, transaction queueing, replica read-only.
4. The handler runs. It never yields, so it is atomic with respect to every other
   command. The blocking commands are the deliberate exception and `await` a
   future rather than the loop.
5. Write commands are propagated to replicas, rewritten first if they are not
   deterministic.
6. The reply is encoded and buffered. Once the whole input batch is drained, all
   the replies are written in one `write` and the connection drains.

### Repository layout

```text
src/pyredis/
├── __main__.py           entry point, signal handling
├── config.py             immutable ServerConfig, CLI parsing
├── server.py             lifecycle, accept loop, background tasks
├── connection.py         per-client state, read loop, dispatch
├── errors.py             exception hierarchy, Redis error messages
├── globmatch.py          Redis-compatible glob matcher (KEYS, PSUBSCRIBE)
├── notifications.py      blocking waiters and WATCH registry
├── pubsub.py             channel and pattern routing
├── protocol/
│   ├── reader.py         incremental RESP parser
│   └── resp.py           reply model and serializer
├── commands/
│   ├── base.py           CommandSpec, CommandSet, CommandTable
│   ├── parsing.py        shared argument conversion
│   └── *.py              one module per command family
├── datastore/
│   ├── store.py          keyspace, expiry, touch signal
│   └── objects.py        Stream and SortedSet
├── persistence/
│   ├── format.py         RDB constants and length/string codecs
│   ├── rdb_reader.py     snapshot loading
│   ├── rdb_writer.py     snapshot writing
│   ├── crc64.py          CRC-64/REDIS
│   └── lzf.py            LZF decompression
└── replication/
    ├── state.py          offsets, replica registry, WAIT
    └── replica.py        replica-side handshake and link
```

There is more detail, including the concurrency model and the data-structure
trade-offs, in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## The RESP protocol

RESP frames every message with a one-byte type marker and a CRLF terminator.

| Marker | Type          | Example                        |
| ------ | ------------- | ------------------------------ |
| `+`    | Simple string | `+OK\r\n`                      |
| `-`    | Error         | `-ERR unknown command\r\n`     |
| `:`    | Integer       | `:42\r\n`                      |
| `$`    | Bulk string   | `$5\r\nhello\r\n`              |
| `*`    | Array         | `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` |

A null bulk string is `$-1\r\n` and a null array is `*-1\r\n`. They are *not*
interchangeable: `GET missing` returns the first and `BLPOP` on timeout returns
the second, and clients distinguish them.

Requests are arrays of bulk strings. `SET name aryan` arrives as:

```text
*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\naryan\r\n
```

Bulk strings carry an explicit length, which is what makes the protocol
binary-safe: keys and values may contain NUL bytes, CRLF, or anything else.

Only RESP2 is implemented. `HELLO 3` is refused with `NOPROTO`, which is what a
RESP2-only server returns and what makes clients fall back cleanly rather than
hang. See [docs/PROTOCOL.md](docs/PROTOCOL.md) for the parser's design, the
resumability strategy, and the exact protocol-error behaviour.

---

## Supported commands

82 commands are implemented. Every one listed here has integration-test coverage.
The full table, with per-command notes and argument forms, is in
[docs/COMMANDS.md](docs/COMMANDS.md).

### Connection and server

| Command | Supported | Notes |
| ------- | --------- | ----- |
| `PING` | Yes | Returns a two-element array in subscribe mode, as Redis does |
| `ECHO` | Yes | |
| `SELECT` | Partial | Only index 0; this server has a single database |
| `QUIT` | Yes | |
| `RESET` | Yes | |
| `HELLO` | Partial | RESP2 only; `HELLO 3` returns `NOPROTO` |
| `CLIENT` | Partial | `ID`, `SETNAME`, `GETNAME`, `INFO`, `LIST` |
| `CONFIG` | Partial | `GET` only; there are no runtime-writable parameters |
| `COMMAND` | Partial | `COUNT`, `DOCS`, `INFO`; key positions are not reported |
| `INFO` | Yes | Server, Clients, Stats, Replication, Keyspace sections |
| `DBSIZE`, `FLUSHALL`, `FLUSHDB` | Yes | |
| `SAVE`, `BGSAVE`, `LASTSAVE` | Partial | `BGSAVE` saves inline; see [Limitations](#limitations) |
| `TIME` | Yes | |

### Keys

| Command | Supported | Notes |
| ------- | --------- | ----- |
| `DEL`, `UNLINK`, `EXISTS`, `TYPE`, `KEYS` | Yes | `UNLINK` is an alias for `DEL` here |
| `RENAME`, `RANDOMKEY` | Yes | `RENAME` carries the TTL across |
| `TTL`, `PTTL`, `PERSIST` | Yes | `TTL` rounds up, as Redis does |
| `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT` | Partial | The `NX`/`XX`/`GT`/`LT` modifiers are not implemented |

### Strings

| Command | Supported | Notes |
| ------- | --------- | ----- |
| `SET` | Yes | `EX`, `PX`, `EXAT`, `PXAT`, `NX`, `XX`, `KEEPTTL`, `GET` |
| `GET`, `GETDEL`, `SETNX`, `MGET`, `MSET` | Yes | |
| `APPEND`, `STRLEN`, `GETRANGE` | Yes | |
| `INCR`, `DECR`, `INCRBY`, `DECRBY` | Yes | 64-bit, with overflow detection |

### Lists

| Command | Supported | Notes |
| ------- | --------- | ----- |
| `RPUSH`, `LPUSH`, `LLEN`, `LRANGE`, `LINDEX`, `LSET` | Yes | |
| `LPOP`, `RPOP` | Yes | Optional count; reply shape follows its presence |
| `BLPOP`, `BRPOP` | Yes | Non-blocking for the server; honours the original deadline |

### Streams

| Command | Supported | Notes |
| ------- | --------- | ----- |
| `XADD` | Yes | `*`, `<ms>-*` and explicit IDs; propagated with the ID resolved |
| `XLEN`, `XRANGE`, `XREVRANGE` | Yes | `COUNT`, partial IDs, and `(` exclusive bounds |
| `XREAD` | Yes | `COUNT`, `BLOCK`, and the `$` "from now on" ID |

### Sorted sets

| Command | Supported | Notes |
| ------- | --------- | ----- |
| `ZADD` | Yes | `NX`, `XX`, `CH`; all-or-nothing on a bad score |
| `ZCARD`, `ZSCORE`, `ZRANK`, `ZREVRANK`, `ZREM`, `ZINCRBY` | Yes | |
| `ZRANGE`, `ZREVRANGE` | Partial | Index ranges and `WITHSCORES`; not `BYSCORE`/`BYLEX`/`LIMIT` |

### Transactions and Pub/Sub

| Command | Supported | Notes |
| ------- | --------- | ----- |
| `MULTI`, `EXEC`, `DISCARD` | Yes | `EXECABORT` on queue-time errors |
| `WATCH`, `UNWATCH` | Yes | Optimistic locking via the keyspace touch signal |
| `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE` | Yes | |
| `PUBLISH`, `PUBSUB` | Yes | `CHANNELS`, `NUMSUB`, `NUMPAT` |

### Replication

| Command | Supported | Notes |
| ------- | --------- | ----- |
| `REPLCONF` | Yes | `listening-port`, `capa`, `GETACK`, `ACK` |
| `PSYNC` | Partial | Always a full resync; no replication backlog |
| `WAIT` | Yes | Counts acknowledgements; not a durability guarantee |

---

## Design decisions

### One thread, one event loop

Command handlers never yield in the middle, so each command is atomic with
respect to every other one and the keyspace needs no locks. That is the same
guarantee Redis gets from being single-threaded, reached the same way — and it
makes `MULTI`/`EXEC` almost free to implement.

The cost is that one CPU core is the ceiling, and a genuinely slow command blocks
everyone. The blocking commands are therefore written as coroutines that `await`
a future: the *client* waits, the server does not.

*Alternative considered:* a thread per connection. It would use multiple cores,
but every keyspace operation would need locking, `MULTI`/`EXEC` would need a
global lock, and 10,000 idle connections would cost 10,000 stacks.

### Two dictionaries, not one dictionary of wrappers

The keyspace is `dict[key, value]` plus a separate `dict[key, deadline]`. Most
keys never get a TTL, so this avoids paying for an expiry slot on every key. It
is also exactly how Redis structures `redisDb`.

### A deadline heap for active expiry

Lazy expiry on read makes expiry *correct*; it does not reclaim memory for keys
nobody reads again. Redis samples random keys with TTLs; this implementation
keeps a min-heap of `(deadline, key)`, so a background cycle costs O(log n) per
key actually removed and O(1) to discover there is nothing to do. Stale heap
entries — from a TTL that moved — are filtered out on pop, and the heap is
rebuilt from the authoritative dict when they outnumber the live ones.

### One keyspace "touch" signal, two consumers

`BLPOP` needs to know when a key gains a value. `WATCH` needs to know when a key
changes at all. Both are the same question, so the store broadcasts one signal
and two subscribers interpret it. Mutation sites stay ignorant of both features.

### Commands are rewritten before propagation

Replication replays commands, so a command that is not deterministic will produce
a different result on the replica:

- `XADD key *` would generate a different ID from the replica's own clock, so it
  is propagated with the ID made explicit.
- `SET k v PX 100` would start its clock when the replica applied it, so it is
  propagated as `SET k v PXAT <deadline>`. The same applies to all four `EXPIRE`
  variants, which propagate as `PEXPIREAT`.
- `BLPOP` would block the replication stream, so it propagates as the `LPOP` that
  actually happened — or as nothing at all, if it timed out.

### The replica applies its master's stream through the normal command path

A master's write stream *is* a stream of commands. Rather than writing a second
"apply a replicated command" path, the replica wraps the master socket in an
ordinary `Connection` in `MASTER_LINK` mode: same parser, same dispatcher, same
handlers, with replies suppressed and bytes counted. One command path cannot
drift from itself.

### A sorted list instead of a skip list

`SortedSet` is a dict for O(1) score lookup plus a sorted list for ordered
access. Rank and range queries are O(log n) via binary search; insertion and
removal are O(n) because of the list shift, where Redis's skip list is O(log n).
The simpler structure is a deliberate trade: it is ~60 lines instead of ~300, and
the asymptotics only matter at set sizes this server is not built for. It is
recorded here rather than hidden.

### No runtime dependencies

Everything is `asyncio`, `socket`, `heapq`, `bisect` and `struct`. That is not
minimalism for its own sake — it is the point of the exercise. The development
dependencies (`pytest`, `ruff`, `mypy`) are tools, not libraries the server uses.

---

## Performance

Measured with the benchmark in this repository against this server. Reproduce
with:

```bash
python benchmarks/bench.py --port 6379 --clients 10 --requests 5000 --pipeline 1
```

**Environment:** Python 3.13.5, Windows 11, AMD64, 12 logical CPUs, loopback
networking. Absolute numbers depend entirely on hardware and are only meaningful
compared against another server measured the same way on the same machine.

10 concurrent clients, no pipelining:

| Command | ops/sec | p50 (ms) | p95 (ms) | p99 (ms) |
| ------- | ------: | -------: | -------: | -------: |
| PING    |  15,106 |    0.434 |    1.181 |    1.316 |
| SET     |   9,799 |    1.057 |    1.442 |    1.625 |
| GET     |  10,004 |    1.017 |    1.397 |    1.603 |
| INCR    |  10,131 |    1.031 |    1.459 |    1.652 |

10 concurrent clients, pipeline depth 16:

| Command | ops/sec | p50 (ms) | p95 (ms) | p99 (ms) |
| ------- | ------: | -------: | -------: | -------: |
| PING    |  91,870 |    0.125 |    0.162 |    0.186 |
| SET     |  51,339 |    0.200 |    0.253 |    0.276 |
| GET     |  71,892 |    0.143 |    0.194 |    0.257 |
| INCR    |  62,338 |    0.175 |    0.230 |    0.255 |

The six-fold jump from pipelining is the headline: without it, throughput is
bounded by round trips and syscalls rather than by anything the server computes.
Replies are batched into one `write` per read precisely to make that case fast.

These figures have **not** been compared against a real `redis-server`, which was
not available in the environment where this was developed. C Redis would be
substantially faster; the benchmark script accepts a `--port`, so the comparison
is one command away for anyone who has it installed.

---

## Testing

585 tests: 250 unit and 335 integration.

```bash
pytest                       # everything
pytest tests/unit            # fast, no sockets
pytest tests/integration     # starts real server processes
pytest -m "not slow"         # skip the timing-sensitive tests
pytest -v                    # per-test names
```

**Unit tests** cover the pure logic: RESP parsing and serialization, the glob
matcher, the store and its expiry (against an injected clock, so they are
deterministic), the stream and sorted-set structures, and the RDB codecs.

**Integration tests** start real server subprocesses and talk to them over TCP
with a small RESP client written for the purpose — deliberately not `redis-py`,
because a third-party client would hide the framing details under test.

**Negative tests** cover empty requests, partial and byte-at-a-time delivery,
every class of malformed RESP, oversized requests, unbalanced quotes, abrupt
resets, mid-command disconnects, and clients that never read their replies.

**Replication tests** start two real servers, let them replicate over a real
socket, and assert on propagation, offset accounting, `WAIT` semantics, read-only
enforcement, and recovery after the master restarts. One test performs the
handshake by hand and asserts the exact wire exchange.

---

## Limitations

Stated plainly, because a compatibility claim is only worth as much as its
caveats. The full list with reasoning is in
[docs/LIMITATIONS.md](docs/LIMITATIONS.md).

- **Not for production.** No authentication, no TLS, no memory limits, no
  eviction policy. It binds to loopback by default for that reason.
- **Not verified against a real `redis-server`.** Behaviour was written against
  the Redis documentation and verified by this repository's test suite. No
  side-by-side comparison with C Redis has been run.
- **Streams are not persisted.** `SAVE` writes strings, lists and sorted sets and
  reports how many stream keys it skipped. Streams also do not transfer in a full
  resync; they replicate from the moment the replica attaches.
- **RDB interoperability is partial.** Files this server writes load in real
  Redis. Reading files *written* by Redis 7 works for string, plain-list and
  `zset2` values, including integer and LZF-compressed strings — but Redis
  usually encodes small collections as listpacks or quicklists, which are not
  implemented and are rejected with a clear error naming the type.
- **One database.** `SELECT 0` only.
- **No AOF**, no `BGSAVE` in a real background process, no partial resync, no
  replication backlog, no cluster mode, no Lua, no keyspace notifications.
- **Hashes and sets are not implemented.** Four types are, thoroughly.
- **`INFO` reports `redis_version:7.0.0`** for client-library compatibility, since
  libraries gate behaviour on that field. `pyredis_version` is the authoritative
  version of this implementation. This server implements a *subset* of that
  Redis version's commands.
- **`WAIT` counts acknowledgements, not durability.** Replication is
  asynchronous; a write can be lost if the master dies before any replica
  acknowledges it.

---

## Development

```bash
pytest                       # tests
ruff format .                # format
ruff check .                 # lint
ruff check --fix .           # lint and autofix
mypy                         # type check (strict, src and tests)
```

Or all four at once:

```bash
bash scripts/check.sh
```

```powershell
powershell -File scripts/check.ps1
```

Running the server in development, with verbose logging and an ephemeral port:

```bash
python -m pyredis --port 0 --loglevel debug
```

Server options:

```text
--host HOST            interface to bind (default: 127.0.0.1)
--port PORT            TCP port, or 0 to let the OS choose (default: 6379)
--dir DIR              directory holding the RDB file (default: .)
--dbfilename NAME      RDB file name (default: dump.rdb)
--replicaof "H P"      replicate from this master
--loglevel LEVEL       debug | info | warning | error (default: info)
--maxclients N         concurrent connection limit (default: 1000)
--version
```

### Trying replication locally

Start a master:

```bash
python -m pyredis --port 6379 --dir ./data-master
```

Start a replica in a second terminal:

```bash
python -m pyredis --port 6380 --dir ./data-replica --replicaof "localhost 6379"
```

Write to the master and read from the replica:

```bash
redis-cli -p 6379 SET city pune
```

```bash
redis-cli -p 6380 GET city
```

`redis-cli -p 6380 SET city other` will be refused with `READONLY`, and
`redis-cli -p 6379 INFO replication` shows the attached replica and its offset.

---

## Publishing to GitHub

Create an empty repository on GitHub first — no README, licence or `.gitignore`,
since this repository already has them. Then, from the project directory:

```bash
git init
```

```bash
git add .
```

```bash
git commit -m "Redis-compatible server in Python: protocol, storage, persistence, replication"
```

```bash
git branch -M main
```

Replace the placeholder below with the URL GitHub showed you:

```bash
git remote add origin https://github.com/<your-username>/<your-repository>.git
```

```bash
git push -u origin main
```

Before the first push, check that nothing unwanted is staged:

```bash
git status --short
```

`.venv/`, `__pycache__/`, `*.rdb` and `*.log` are already covered by
`.gitignore`. There are no credentials, tokens or machine-specific paths in the
repository. Update the `Homepage` and `Issues` URLs in `pyproject.toml` to point
at your repository.

---

## Further documentation

| Document | Contents |
| -------- | -------- |
| [docs/SETUP.md](docs/SETUP.md) | Clean-machine setup, verification, troubleshooting |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Components, concurrency model, trade-offs |
| [docs/PROTOCOL.md](docs/PROTOCOL.md) | RESP framing, the parser, error handling |
| [docs/COMMANDS.md](docs/COMMANDS.md) | Every command, its arguments and its replies |
| [docs/LIMITATIONS.md](docs/LIMITATIONS.md) | What is not implemented, and why |
| [docs/INTERVIEW.md](docs/INTERVIEW.md) | Explaining this project in a technical interview |

---

## License

MIT. See [LICENSE](LICENSE).

Redis is a trademark of Redis Ltd. This project is an independent
reimplementation for educational purposes and is not affiliated with, endorsed
by, or derived from the Redis source code.
