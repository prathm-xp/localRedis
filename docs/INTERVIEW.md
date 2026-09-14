# Talking about this project

Notes for explaining the project in a technical interview. Everything here is
accurate to what the code actually does — the fastest way to lose an interview
is to claim something the repository does not support.

**Frame it honestly:** this is a from-scratch implementation built to learn
systems engineering, with a thorough test suite. It is not production software
and has not been run in production. Say so; it is a stronger position than
overclaiming, and it invites the interesting questions.

---

## The 30-second version

> I built a Redis-compatible server from scratch in Python — no runtime
> dependencies, just the standard library. It speaks the real RESP wire
> protocol, so `redis-cli` and `redis-py` connect to it unmodified. It supports
> strings, lists, streams and sorted sets, key expiration, transactions,
> pub/sub, RDB snapshot persistence, and asynchronous master/replica
> replication. It's about 5,700 lines of source with 585 tests, and it's
> single-threaded on an asyncio event loop — the same concurrency model as real
> Redis, which is what makes every command atomic without any locking.

---

## The 2-minute version

> **What it is.** A Redis-compatible in-memory key-value server, written from
> scratch in Python with no runtime dependencies. Any Redis client can talk to
> it because it implements RESP, the actual wire protocol.
>
> **Architecture.** Four layers with strict boundaries. A protocol layer that
> turns a TCP byte stream into commands and replies back into bytes. A dispatch
> layer — an immutable command table built at import time, with per-command
> arity and flags. A command layer, one module per data type. And a storage
> layer: the keyspace, expiry, and the four value types.
>
> **Concurrency.** One thread, one asyncio event loop, one coroutine per
> connection. That's the key design decision. Because a handler never yields
> mid-execution, every command is atomic with respect to every other one and the
> keyspace needs no locks at all — the same guarantee Redis gets from being
> single-threaded. It also makes MULTI/EXEC almost free. The cost is one core,
> and blocking commands become interesting: BLPOP can't block the loop, so it
> awaits a future that the keyspace wakes. The client waits; the server doesn't.
>
> **The parts I'd point at.** The RESP parser is fully resumable — a command can
> arrive one byte at a time or share a packet with nine others, and it's the
> same code path. Expiry is enforced twice: lazily on read, which makes it
> correct, and by a background task driven by a min-heap of deadlines, which
> makes it tidy. And replication taught me the most: I had to rewrite
> non-deterministic commands before propagating them, because `XADD key *` would
> generate a different ID on the replica, and `SET k v PX 100` would restart its
> clock there.
>
> **Testing.** 585 tests. Unit tests for the pure logic — the parser, the store
> against an injected clock, the RDB codecs. Integration tests that start real
> server subprocesses and talk to them over TCP with a small RESP client I wrote,
> deliberately not redis-py, because a third-party client would hide the framing
> details under test. The replication tests start two real servers and let them
> replicate over a real socket.
>
> **What it doesn't do.** No auth, no TLS, no clustering, no hashes or sets, no
> AOF. Streams aren't persisted to RDB — and the save command tells you how many
> keys it skipped rather than losing them silently. And I haven't compared it
> side by side against a real redis-server, so I say "implements the documented
> behaviour, verified by my tests", not "verified against Redis".

---

## The engineering problems worth describing

Pick one or two. Each has a real bug or a real decision behind it.

### 1. The replication offset off-by-37

**The problem.** A master asks a replica `REPLCONF GETACK *`; the replica replies
with its byte offset in the command stream. Get the accounting wrong and the two
sides disagree permanently — and `WAIT` never returns.

**The subtlety.** `REPLCONF GETACK *` is itself 37 bytes of the stream. The
replica must answer with the offset from *before* that command, then count it.

**The fix.** The read loop counts a command's bytes *after* executing it. That
one ordering makes the whole thing fall out correctly, and it only works because
`ParsedCommand` carries `size` — the parser is the only place the exact wire
length is known. Reconstructing it by re-encoding the arguments would be wrong
for inline commands.

**Why it's a good story.** It's a distributed-systems bug that looks like an
accounting bug, and the fix is a single line placed carefully rather than extra
machinery.

### 2. Non-deterministic commands can't be replayed

**The problem.** Replication ships commands, not data. So `XADD events *` on the
master generates an ID from the master's clock, and the replica — replaying `*`
— generates a different one. The two diverge on the very first entry.

**The fix.** A per-command propagation override with three states: propagate
verbatim, propagate something else, propagate nothing.

| Command | Propagated as |
| ------- | ------------- |
| `XADD key *` | `XADD key <resolved-id>` |
| `SET k v PX 100` | `SET k v PXAT <absolute-deadline>` |
| `EXPIRE k 60` | `PEXPIREAT k <absolute-deadline>` |
| `BLPOP k 0` | `LPOP k` — or nothing, if it timed out |

**The generalisation.** Any replicated system has to decide between shipping
operations and shipping state. Operations are compact but must be deterministic;
state is bulky but trivially correct. This is the cost of choosing operations,
paid one command at a time.

### 3. The heap that never shrank

**The problem.** Active expiry uses a min-heap of `(deadline, key)`. Re-setting a
key's TTL leaves the old pair behind, so the heap grows. I added compaction that
filtered out entries whose deadline no longer matched the dictionary.

**The bug.** A test that set the same TTL on one key five hundred times pushed
five hundred *identical* tuples. Every one of them matched the dictionary, so the
filter removed nothing and the heap stayed at 500 entries for one live key.

**The fix.** Rebuild the heap from the deadline dictionary — the authoritative
source — rather than filtering the heap. Simpler *and* correct.

**Why it's worth telling.** A test I wrote to check a property I assumed held
found a real bug in the thing it was testing. That's the argument for property
assertions over example assertions, in one anecdote.

### 4. Blocking without blocking

**The problem.** `BLPOP queue 0` waits until something is pushed. On a
single-threaded server, waiting the obvious way stops everything.

**The design.** The handler is a coroutine that awaits a future registered
against the key. The store already broadcasts a "touch" for every key it mutates
— originally for `WATCH` — so the blocking manager subscribes to the same signal
and resolves the future. A thousand blocked clients cost a thousand small
objects and no threads.

**The two subtleties:**

- Waking isn't being served. Another client may take the element first, so a
  woken waiter re-checks and waits again — with the *remaining* timeout, computed
  from the original deadline, not a fresh one.
- A blocked client is only served by the right type. If the key gains a *string*,
  Redis leaves the client blocked rather than failing it. My first version raised
  `WRONGTYPE`. An integration test caught the difference, and the fix was to
  distinguish the first attempt (error) from later ones (skip).

### 5. Why protocol errors close the connection

**The observation.** A wrong-arity error leaves the connection open. A malformed
RESP frame closes it. That asymmetry looks arbitrary until you think about
framing.

**The reason.** If `$abc` appears where a length should be, the parser has no
idea where the next command starts. Every subsequent byte is arbitrary. There is
no resynchronisation point in RESP — no framing delimiter to scan forward to — so
the only safe action is to report and close. A command error, by contrast, leaves
framing intact.

**The generalisation.** Recoverable and unrecoverable errors want different
handling, and the distinction belongs in the exception hierarchy rather than in
`if` statements at the call sites. That's why `errors.py` has two families.

### 6. Reusing the connection class for the replication link

**The decision.** On a replica, the link to the master is an ordinary
`Connection` in a different mode, not a bespoke reader.

**Why.** The master's write stream *is* a stream of commands. Running it through
the same parser, dispatcher and handlers means there is exactly one command path
in the server. A separate "apply a replicated command" function would be a second
implementation of the same logic, free to drift.

**What the mode changes.** Replies are suppressed — except `REPLCONF ACK`, the
one thing a replica sends back — and processed bytes are counted.

---

## Trade-offs, and what I'd say about each

| Decision | Chosen | Alternative | Why |
| -------- | ------ | ----------- | --- |
| Concurrency | Single-threaded event loop | Thread per connection | Lock-free keyspace, atomic commands, cheap connections. Costs one core. |
| Sorted set | Sorted list + dict | Skip list | O(log n) reads, O(n) writes, one fifth the code. Documented, not hidden. |
| Dependencies | Standard library only | `uvloop`, `hiredis` | The point of the exercise is implementing it. `uvloop` is the obvious first optimisation. |
| RESP version | RESP2 | RESP3 | RESP2 is universally supported. RESP3's push type would let me lift the subscribe-mode restriction. |
| Persistence | RDB snapshots | AOF | Snapshots are self-contained and the format is interesting. AOF's value is durability, which isn't the goal here. |
| Streams in RDB | Skipped, and reported | Half-implement listpacks | A file that looks valid and silently loses data is worse than a loud gap. |
| Expiry | Deadline min-heap | Random sampling, as Redis | O(1) to find nothing to do; no key is ever missed. More memory per TTL'd key. |
| Test client | Wrote my own | `redis-py` | A third-party client would hide the framing under test, and add a dependency. |
| `BGSAVE` | Saves inline | Fake asynchrony | `fork()` isn't available on Windows. Documenting the gap beats lying about it. |
| Corrupt RDB | Refuse to start | Start empty | Starting empty hides the problem and the next `SAVE` destroys the evidence. |

---

## Performance: what to say

Have the numbers ready, and the caveat with them.

- ~10,000 ops/sec with 10 clients and no pipelining; ~15,000 for `PING`.
- ~51,000–92,000 ops/sec at pipeline depth 16.
- p99 latency under 1.7 ms unpipelined, under 0.3 ms pipelined.
- Measured on Python 3.13, Windows 11, 12 logical cores, over loopback.

**The insight to lead with:** the sixfold jump from pipelining says the
bottleneck is syscalls and round trips, not computation. That's why replies are
batched into one `write` per read batch — a thousand-command pipeline costs one
write, not a thousand.

**Where the remaining time goes**, in order: interpreter overhead in parsing and
dispatch; `bytes` allocation for every argument; `SortedSet` insertion at large
sizes.

**What I'd do next:** `uvloop` on Linux, a drop-in event-loop replacement;
`memoryview`-based zero-copy parsing; a skip list for sorted sets.

**Be explicit:** I have *not* benchmarked against real `redis-server`. C Redis
would be substantially faster. The benchmark script takes a `--port`, so the
comparison is one command away for anyone who has it.

---

## Questions you should expect, and good answers

**"Why single-threaded? Isn't that slow?"**
For this workload the bottleneck is network round trips, not CPU — the benchmark
shows that directly, since pipelining multiplies throughput sixfold without any
change to the compute. Single-threading buys atomic commands and a lock-free
keyspace, which is what makes MULTI/EXEC and the data structures simple and
correct. Redis made the same call for the same reason. The honest cost is one
core, and a genuinely CPU-bound command blocks everyone.

**"How do you handle a partially received command?"**
The parser is resumable. `feed` appends bytes, `read_command` returns a complete
command or `None`. An incomplete command leaves the buffer offset untouched, so
the next read retries from the same place. That makes partial delivery and
pipelining the same code path. There's a test that feeds a command one byte at a
time and asserts it completes on exactly the last byte.

**"What happens if a client sends garbage?"**
Depends which kind. Invalid RESP framing is unrecoverable — there's no
resynchronisation point — so the server replies with the protocol error and
closes, as Redis does. A valid frame with a bad command is just an error reply;
the connection stays open. There are also hard limits on multibulk count, bulk
length, inline length and total query buffer, so a client can't exhaust memory by
claiming a 500 GB argument.

**"How does expiry work? Do you scan every key?"**
Two mechanisms. Lazily on every read, which is what makes it *correct* — a key is
never visible past its deadline regardless of background timing. Then a
background task for memory, driven by a min-heap of deadlines: O(1) to check
whether anything is due, O(log n) per key actually removed. Redis samples random
keys instead; the heap is more precise at the cost of some memory per TTL'd key.

**"How would you add hashes?"**
A new value type backed by `dict[bytes, bytes]`, a `commands/hashes.py` module
with a `CommandSet`, one line in the command table, and an entry in the RDB type
map. The architecture is designed for it — that's what the per-family command
modules and the type-name mapping are for. It's maybe two hours including tests.

**"What's the hardest bug you hit?"**
Two candidates. The replication offset one — the GETACK command's own bytes
mustn't be counted when replying to it, which is a one-line ordering fix that's
invisible until the offsets drift. Or the expiry heap that wouldn't compact,
because my filter couldn't distinguish identical `(deadline, key)` tuples; the
fix was to rebuild from the authoritative dictionary instead of filtering.

**"How do you know it's actually Redis-compatible?"**
Carefully: I know it implements the *documented* behaviour and passes 585 tests
that assert exact error strings, reply shapes and edge cases. I have not run it
side by side against a real redis-server — that wasn't available where I built
it. So I'd say "compatible with the documented protocol for the implemented
command set", and I'd point at the limitations document rather than let someone
assume more.

**"What would you do differently?"**
I'd track key positions per command from the start. I skipped it, which is why
`COMMAND` reports zeros — and it would also have let me implement WATCH's
invalidation more generically. I'd also have written the RDB writer for listpacks
early, since not having them is the thing that stops real Redis dump files
loading.

**"Is this production-ready?"**
No, and I wouldn't present it as such. No auth, no TLS, no memory limits, no
eviction, no clustering. It binds to loopback by default precisely because it's
an unauthenticated database. It's a project for learning systems engineering, and
the test suite and documentation are the parts I'd want reviewed.

---

## What to demo live, if asked

1. Start it: `pyredis-server --port 6379`.
2. Connect with `redis-cli` — showing that a real client works is the whole
   compatibility claim in one move.
3. `SET session abc PX 100`, then `GET session` after a moment: nil.
4. `redis-cli -p 6379 SUBSCRIBE news` in one terminal, `PUBLISH news hello` in
   another.
5. Start a replica with `--replicaof "localhost 6379"`, write to the master, read
   from the replica, then try to write to the replica and get `READONLY`.
6. `nc 127.0.0.1 6379` and type `PING` — inline commands mean the raw protocol is
   inspectable by hand.
