# Command reference

Every command implemented by this server, with its arguments, its reply, and any
deviation from Redis. All 82 are covered by integration tests.

Notation follows the Redis documentation: `[optional]`, `<required>`,
`a|b` alternatives, `element ...` repetition.

---

## Connection

### `PING [message]`
Reply: `+PONG`, or the message as a bulk string if one is given.

In subscribe mode the reply is a two-element array — `["pong", ""]` or
`["pong", message]` — because everything on a subscribed connection must look
like a push message.

### `ECHO <message>`
Reply: the message, as a bulk string.

### `SELECT <index>`
Reply: `+OK` for index 0, otherwise `-ERR DB index is out of range`.

*Deviation:* this server has a single database.

### `QUIT`
Reply: `+OK`, then the connection closes once the reply is written.

### `RESET`
Reply: `+RESET`. Clears subscriptions, transaction state, watches and the client
name.

### `HELLO [protover]`
Reply: the handshake map as a flat array (`server`, `version`, `proto`, `id`,
`mode`, `role`, `modules`).

*Deviation:* RESP2 only. `HELLO 3` returns `-NOPROTO unsupported protocol version`.

### `CLIENT <subcommand>`

| Form | Reply |
| ---- | ----- |
| `CLIENT ID` | Integer connection id |
| `CLIENT SETNAME <name>` | `+OK`; names may not contain spaces or newlines |
| `CLIENT GETNAME` | The name, or nil if unset |
| `CLIENT INFO` | One `field=value` line for this connection |
| `CLIENT LIST` | One line per connected client |

*Deviation:* `KILL`, `PAUSE` and `UNPAUSE` are not implemented.

---

## Server

### `CONFIG GET <parameter> [parameter ...]`
Reply: a flat array of name/value pairs. Parameters are matched as globs, so
`CONFIG GET d*` returns both `dir` and `dbfilename`.

Exposed parameters: `dir`, `dbfilename`, `port`, `bind`, `maxclients`,
`loglevel`, `appendonly`.

*Deviation:* `CONFIG SET` is reported as an unknown subcommand. Nothing here is
safe to change at runtime, and accepting-then-ignoring would be worse.

### `COMMAND [COUNT | DOCS | INFO name ...]`

| Form | Reply |
| ---- | ----- |
| `COMMAND` | One array per command: `[name, arity, [flags], 0, 0, 0]` |
| `COMMAND COUNT` | Number of commands |
| `COMMAND DOCS` | Empty array |
| `COMMAND INFO <name>...` | Per-name info array, or nil for an unknown name |

*Deviation:* key positions are reported as zero; no key specs are tracked.

### `INFO [section ...]`
Reply: the standard text format as a bulk string. Sections: `Server`, `Clients`,
`Stats`, `Replication`, `Keyspace`. With no argument, all of them.

*Deviation:* `redis_version` reports 7.0.0 for client compatibility;
`pyredis_version` is authoritative. See [LIMITATIONS.md](LIMITATIONS.md#version-reporting).

### `DBSIZE`
Reply: integer count of live keys. Expired keys are collected first, so the count
is never stale.

### `FLUSHALL [ASYNC|SYNC]` / `FLUSHDB [ASYNC|SYNC]`
Reply: `+OK`. Both modifiers are accepted and behave synchronously; with one
database and one thread the two commands are identical.

### `SAVE`
Reply: `+OK`, or an error naming how many unsupported-type keys were skipped.
Writes an RDB snapshot atomically (temporary file plus rename).

### `BGSAVE`
Reply: `+OK`.

*Deviation:* performs the save inline. A real background save needs `fork()`.

### `LASTSAVE`
Reply: Unix time of the last successful save.

### `TIME`
Reply: two bulk strings, `[seconds, microseconds]`.

---

## Keys

### `DEL <key> [key ...]` / `UNLINK <key> [key ...]`
Reply: integer count of keys that existed.

*Deviation:* `UNLINK` is an alias. Redis unlinks on a background thread; here the
work is the same dictionary removal.

### `EXISTS <key> [key ...]`
Reply: integer. Each occurrence counts, so `EXISTS k k` returns 2 if `k` exists.

### `TYPE <key>`
Reply: `+string`, `+list`, `+stream`, `+zset` or `+none`.

### `KEYS <pattern>`
Reply: array of matching keys. Glob syntax: `*`, `?`, `[abc]`, `[a-z]`, `[^abc]`,
`\` to escape.

O(n) over the keyspace and blocks the server, as in Redis.

### `RANDOMKEY`
Reply: a random live key, or nil when the keyspace is empty.

### `RENAME <source> <destination>`
Reply: `+OK`, or `-ERR no such key`. Carries any TTL across.

### `TTL <key>` / `PTTL <key>`
Reply: remaining lifetime in seconds / milliseconds, `-1` if the key has no
expiry, `-2` if it does not exist. `TTL` rounds up, so 1500 ms reports 2.

### `PERSIST <key>`
Reply: 1 if a TTL was removed, 0 otherwise.

### `EXPIRE <key> <seconds>` / `PEXPIRE <key> <milliseconds>`
### `EXPIREAT <key> <unix-seconds>` / `PEXPIREAT <key> <unix-milliseconds>`
Reply: 1 if the expiry was set, 0 if the key does not exist.

A deadline in the past deletes the key immediately and returns 1.

All four propagate to replicas as `PEXPIREAT` with an absolute deadline.

*Deviation:* the `NX`/`XX`/`GT`/`LT` modifiers return a syntax error.

---

## Strings

### `SET <key> <value> [EX s | PX ms | EXAT ts | PXAT ts] [NX | XX] [KEEPTTL] [GET]`
Reply: `+OK`; nil if `NX`/`XX` prevented the write; with `GET`, the previous
value (or nil).

| Option | Meaning |
| ------ | ------- |
| `EX` / `PX` | Relative expiry in seconds / milliseconds; must be positive |
| `EXAT` / `PXAT` | Absolute Unix deadline in seconds / milliseconds |
| `NX` / `XX` | Only if the key is absent / present |
| `KEEPTTL` | Preserve the existing TTL instead of clearing it |
| `GET` | Also return the previous value; `WRONGTYPE` if it is not a string |

Without `KEEPTTL` or an expiry option, `SET` clears any existing TTL.

Propagated as `SET key value PXAT <deadline>`, with `NX`/`XX` dropped — the
condition has already been evaluated on the master.

### `SETNX <key> <value>`
Reply: 1 if created, 0 if the key already existed.

### `GET <key>`
Reply: the value, or nil. `WRONGTYPE` if the key holds a non-string.

### `GETDEL <key>`
Reply: the value, or nil. Removes the key. Propagated as `DEL`.

### `MGET <key> [key ...]`
Reply: array of values. Non-string values report nil rather than erroring.

### `MSET <key> <value> [key value ...]`
Reply: `+OK`. An odd argument count is an arity error.

### `APPEND <key> <value>`
Reply: the new length. Creates the key if absent; preserves any TTL.

### `STRLEN <key>`
Reply: length, or 0 for a missing key.

### `GETRANGE <key> <start> <end>`
Reply: the substring. Inclusive; negative indexes count from the end; out-of-range
bounds are clamped.

### `INCR <key>` / `DECR <key>` / `INCRBY <key> <n>` / `DECRBY <key> <n>`
Reply: the new value.

A missing key starts at zero. The stored value is reparsed on every call, so
anything that is not a valid 64-bit integer — *including* one with whitespace,
like `" 1"` — is `-ERR value is not an integer or out of range`. Overflow past
the 64-bit range is `-ERR increment or decrement would overflow`. Any TTL is
preserved.

---

## Lists

### `RPUSH <key> <element> [element ...]` / `LPUSH <key> <element> [element ...]`
Reply: the new length.

`LPUSH` inserts one element at a time, so `LPUSH k a b c` leaves `c` at the head.

### `LLEN <key>`
Reply: length, or 0 for a missing key.

### `LRANGE <key> <start> <stop>`
Reply: array of elements. Inclusive; negative indexes count from the end;
out-of-range bounds are clamped; `start > stop` gives an empty array.

### `LINDEX <key> <index>`
Reply: the element, or nil when the index is out of range.

### `LSET <key> <index> <element>`
Reply: `+OK`, `-ERR no such key`, or `-ERR index out of range`.

### `LPOP <key> [count]` / `RPOP <key> [count]`
Reply **without** a count: the element, or nil.
Reply **with** a count: an array (possibly empty), or a null array if the key is
missing.

The reply shape follows the presence of the count, and clients distinguish the
two. A negative count is `-ERR value is out of range, must be positive`. A list
emptied by a pop is deleted.

### `BLPOP <key> [key ...] <timeout>` / `BRPOP <key> [key ...] <timeout>`
Reply: `[key, element]`, or a null array on timeout.

Keys are checked left to right. `timeout` is in seconds and may be fractional; 0
blocks indefinitely. A negative timeout is `-ERR timeout is negative`.

The timeout is a deadline, not a budget that restarts: a client woken by a push
that another client wins resumes waiting for the *remaining* time.

A key that already holds a non-list is an immediate `WRONGTYPE`. A key that
gains a non-list while the client is blocked leaves it blocked, as in Redis.

Propagated as the `LPOP`/`RPOP` that actually happened, or as nothing on timeout.

---

## Streams

### `XADD <key> <* | ms-* | ms-seq> <field> <value> [field value ...]`
Reply: the entry ID as a bulk string.

| ID form | Behaviour |
| ------- | --------- |
| `*` | Current time, sequence 0; if the clock has not advanced past the last entry, keep its timestamp and increment the sequence |
| `<ms>-*` | The given timestamp; sequence continues from the last entry if it shares the timestamp, otherwise 0 — except `0-*`, which starts at `0-1` |
| `<ms>-<seq>` | Exactly that ID |

IDs must be strictly increasing:

```text
-ERR The ID specified in XADD is equal or smaller than the target stream top item
-ERR The ID specified in XADD must be greater than 0-0
```

An odd number of field/value arguments is an arity error.

Propagated with the ID resolved — sending `*` would generate a different ID on
the replica.

### `XLEN <key>`
Reply: number of entries, or 0 for a missing key.

### `XRANGE <key> <start> <end> [COUNT n]`
Reply: array of `[id, [field, value, ...]]`.

| Bound | Meaning |
| ----- | ------- |
| `-` | The lowest possible ID |
| `+` | The highest possible ID |
| `<ms>` | Widens to `<ms>-0` at the start and `<ms>-<max>` at the end |
| `(<id>` | Exclusive |

### `XREVRANGE <key> <end> <start> [COUNT n]`
Reply: as `XRANGE`, in descending order. Note the reversed argument order.

### `XREAD [COUNT n] [BLOCK ms] STREAMS <key> [key ...] <id> [id ...]`
Reply: array of `[key, [[id, [field, value, ...]], ...]]`, or a null array when
there is nothing to return. Streams with no new entries are omitted.

IDs are **exclusive** — `XREAD STREAMS s 5-0` returns entries strictly after
`5-0`. The special ID `$` means "entries added after this call started", and is
resolved to each stream's current last ID before any waiting begins.

`BLOCK 0` waits indefinitely. A negative block timeout is
`-ERR timeout is negative`. The key and ID counts must match.

*Deviation:* no consumer groups.

---

## Sorted sets

### `ZADD <key> [NX | XX] [CH] <score> <member> [score member ...]`
Reply: the number of members added, or — with `CH` — the number added or changed.

Scores are doubles; `inf`, `+inf` and `-inf` are accepted. Every score is parsed
before anything is written, so a bad score in the middle leaves the whole command
with no effect. `NX` and `XX` together is an error.

### `ZCARD <key>`
Reply: number of members, or 0 for a missing key.

### `ZSCORE <key> <member>`
Reply: the score as a bulk string, or nil.

Scores render as Redis renders them: an integral score comes back as `"1"`, not
`"1.0"`.

### `ZRANK <key> <member>` / `ZREVRANK <key> <member>`
Reply: zero-based index in ascending / descending order, or nil.

### `ZRANGE <key> <start> <stop> [WITHSCORES]` / `ZREVRANGE <key> <start> <stop> [WITHSCORES]`
Reply: array of members, or members and scores interleaved with `WITHSCORES`.

Ordering is by score, then lexicographically by member.

*Deviation:* index ranges only. `BYSCORE`, `BYLEX`, `REV` and `LIMIT` return a
syntax error.

### `ZREM <key> <member> [member ...]`
Reply: the number of members removed. An emptied sorted set is deleted.

### `ZINCRBY <key> <increment> <member>`
Reply: the new score as a bulk string. A result of NaN — from adding `+inf` to
`-inf` — is `-ERR resulting score is not a number (NaN)`.

---

## Transactions

### `MULTI`
Reply: `+OK`. Subsequent commands reply `+QUEUED` instead of executing.
Transactions do not nest.

### `EXEC`
Reply: an array of the queued replies, in order.

| Situation | Reply |
| --------- | ----- |
| Normal | Array of replies |
| A command failed to *queue* (unknown name, bad arity) | `-EXECABORT Transaction discarded because of previous errors.` |
| A watched key changed | Null array |
| No `MULTI` | `-ERR EXEC without MULTI` |

A command that queues fine but fails at run time produces an error *inside* the
array, and the remaining commands still run. There is no rollback — that is
Redis's behaviour, not an omission here.

`EXEC` releases all watches.

### `DISCARD`
Reply: `+OK`. Abandons the queue and releases watches.

Commands that write their own output — `SUBSCRIBE`, `UNSUBSCRIBE`,
`PSUBSCRIBE`, `PUNSUBSCRIBE`, `PSYNC`, `REPLCONF` — are rejected at queue time
with `ERR <CMD> is not allowed in transactions`, and poison the transaction.

### `WATCH <key> [key ...]`
Reply: `+OK`. Must be called before `MULTI`.

If any watched key is touched — by any client, *including this one* — before
`EXEC`, the transaction is abandoned.

### `UNWATCH`
Reply: `+OK`. Forgets every watched key.

---

## Publish / Subscribe

### `SUBSCRIBE <channel> [channel ...]`
Reply: one array per channel — `["subscribe", channel, count]` — where `count` is
this connection's total subscription count.

Entering subscribe mode restricts the connection; see
[PROTOCOL.md](PROTOCOL.md#subscribe-mode).

**Not allowed inside `MULTI`**, and neither are `UNSUBSCRIBE`, `PSUBSCRIBE`,
`PUNSUBSCRIBE`, `PSYNC` or `REPLCONF`. These commands write their own output
rather than returning a reply, so queueing one would put its bytes inside the
`EXEC` array and produce a reply no client could parse. The command is rejected
at queue time with `ERR SUBSCRIBE is not allowed in transactions`, which also
poisons the transaction so `EXEC` refuses with `EXECABORT`.

### `UNSUBSCRIBE [channel ...]`
Reply: one confirmation per channel. With no argument, unsubscribes from all; with
no argument and no subscriptions, a single confirmation with a nil channel.

### `PSUBSCRIBE <pattern> [pattern ...]` / `PUNSUBSCRIBE [pattern ...]`
As above, for glob patterns. Patterns use the same syntax as `KEYS`.

### `PUBLISH <channel> <message>`
Reply: the number of clients the message reached.

Exact-channel subscribers are delivered to first, then pattern subscribers. A
client subscribed both ways receives it twice and is counted twice.

*Deviation:* publishes do not cross the replication link.

### `PUBSUB <subcommand>`

| Form | Reply |
| ---- | ----- |
| `PUBSUB CHANNELS [pattern]` | Channels with at least one subscriber |
| `PUBSUB NUMSUB [channel ...]` | Flat array of channel/count pairs |
| `PUBSUB NUMPAT` | Number of pattern subscriptions |

---

## Replication

### `REPLCONF <option> <value>`

| Form | Reply | Sent by |
| ---- | ----- | ------- |
| `REPLCONF listening-port <port>` | `+OK` | replica, during handshake |
| `REPLCONF capa <capability>` | `+OK` | replica, during handshake |
| `REPLCONF GETACK *` | `REPLCONF ACK <offset>` | master, to request an offset |
| `REPLCONF ACK <offset>` | *nothing* | replica, reporting progress |

The offset a replica reports for `GETACK` is the one from *before* that command,
because a command's bytes are counted after it runs.

### `PSYNC <replid> <offset>`
Reply: `+FULLRESYNC <replid> 0`, followed by `$<length>\r\n<rdb bytes>` — a bulk
string with **no trailing CRLF**. The connection then becomes a replica link.

`-ERR Can't PSYNC from a replica` if this server is itself a replica.

*Deviation:* always a full resync; there is no replication backlog.

### `WAIT <numreplicas> <timeout-ms>`
Reply: the number of replicas that have acknowledged everything written so far.

A timeout of 0 waits indefinitely. If nothing has been written since the last
acknowledgement, it returns immediately — every replica is already current. The
return value may be less than `numreplicas` if the timeout expires.

*This is an acknowledgement count, not a durability guarantee.* Replication is
asynchronous.
