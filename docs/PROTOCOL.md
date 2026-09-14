# The RESP protocol

RESP (REdis Serialization Protocol) is what every Redis client speaks. It is
simple enough to implement in an afternoon and strict enough that getting it
slightly wrong breaks every client at once.

This document covers the wire format, how this implementation parses it, and the
behaviour at the edges — which is where compatibility actually lives.

---

## The wire format

Every message starts with a one-byte type marker and ends with `\r\n`.

| Marker | Type | Encoding | Example |
| ------ | ---- | -------- | ------- |
| `+` | Simple string | `+<text>\r\n` | `+OK\r\n` |
| `-` | Error | `-<code> <message>\r\n` | `-ERR unknown command 'X'\r\n` |
| `:` | Integer | `:<number>\r\n` | `:42\r\n` |
| `$` | Bulk string | `$<len>\r\n<bytes>\r\n` | `$5\r\nhello\r\n` |
| `*` | Array | `*<count>\r\n<element>...` | `*2\r\n:1\r\n:2\r\n` |

Two null forms exist and are **not interchangeable**:

```text
$-1\r\n     null bulk string   — GET on a missing key
*-1\r\n     null array         — BLPOP on timeout, EXEC after a failed WATCH
```

Clients distinguish them, so this server does too. The test suite asserts the
raw bytes in the cases where it matters.

### Why bulk strings carry a length

```text
$11\r\nhello\r\nworld\r\n
```

That value *contains* a CRLF. Because the length is explicit, the parser reads
exactly eleven bytes and never has to interpret the content. The protocol is
therefore binary-safe: keys and values may hold NUL bytes, newlines, arbitrary
binary — anything. This implementation keeps keys and values as `bytes` from the
socket all the way to the store precisely to preserve that.

---

## Requests

A request is an array of bulk strings — the command name followed by its
arguments:

```text
SET name aryan

*3\r\n
$3\r\nSET\r\n
$4\r\nname\r\n
$5\r\naryan\r\n
```

Command names are case-insensitive; arguments never are.

### Inline commands

Redis also accepts a plain, space-separated line, which is what makes it
possible to poke a server with `telnet` or `nc`:

```text
PING\r\n
SET greeting "hello world"\r\n
```

This is implemented, including the quoting rules Redis's `sdssplitargs` applies:

- Double quotes group an argument and support `\xHH` plus the usual `\n`, `\r`,
  `\t`, `\b`, `\a`, `\\` and `\"` escapes.
- Single quotes group an argument and support only `\'`.
- A closing quote must be followed by whitespace or end of line.
- Unbalanced quotes are a protocol error.

A bare `\n` terminator is accepted as well as `\r\n`, because that is what a
terminal sends.

---

## Parsing

TCP delivers a byte stream, not messages. A command may arrive in one packet, in
forty packets, or sharing a packet with nine other commands. `RespReader` owns
that problem so nothing above it has to think about it.

```python
reader = RespReader()
reader.feed(data)  # append whatever arrived
while (parsed := reader.read_command()) is not None:
    handle(parsed.args)  # None means "need more bytes"
```

The `None` return makes partial delivery and pipelining the same code path:
either there is a complete command or there is not.

### Resumability

The buffer is a `bytearray` with an integer offset marking the start of the next
unparsed command. A command that cannot be completed leaves the offset alone, so
the next `feed` retries from the same place.

Re-parsing the incomplete tail costs a rescan, but only of that one command:
everything complete has already been consumed and the offset advanced. There is
no quadratic behaviour when a large pipeline arrives in small chunks.

Consumed bytes are reclaimed once they pass a threshold or once the buffer is
fully drained, so a long-lived connection does not grow a `bytearray` forever.

### Wire-byte accounting

`read_command` returns the number of bytes each command occupied:

```python
@dataclass(frozen=True, slots=True)
class ParsedCommand:
    args: list[bytes]
    size: int
```

Replication needs that number — a replica reports byte offsets in
`REPLCONF ACK` — and the parser is the only place it can be known exactly.
Reconstructing it by re-encoding the parsed arguments would be both wasteful and
wrong for inline commands.

### Limits

A parser that trusts its input is a denial-of-service vector. The bounds mirror
Redis's own compile-time limits:

| Limit | Value | Guards against |
| ----- | ----- | -------------- |
| `max_multibulk_length` | 1,048,576 elements | `*999999999\r\n` |
| `max_bulk_length` | 512 MB | `$999999999999\r\n` |
| `max_inline_length` | 64 KB | endless bytes with no newline |
| `max_query_buffer` (server) | 1 GB | a client that never completes a command |

Integers in the protocol are parsed strictly. Python's `int()` accepts `" +1 "`;
Redis's `strtoll` does not, so the parser validates the character set before
converting. Without that, `* 1 \r\n` would be accepted here and rejected by real
Redis.

---

## Protocol errors

When the byte stream is not valid RESP, framing is lost. There is no way to find
where the next command starts, so the server reports the error and **closes the
connection** — as Redis does.

| Input | Error |
| ----- | ----- |
| `*abc\r\n` | `ERR Protocol error: invalid multibulk length` |
| `*99999999\r\n` | `ERR Protocol error: invalid multibulk length` |
| `*1\r\n+PING\r\n` | `ERR Protocol error: expected '$', got '+'` |
| `*1\r\n$abc\r\n` | `ERR Protocol error: invalid bulk length` |
| `*1\r\n$-1\r\n` | `ERR Protocol error: invalid bulk length` |
| `*1\r\n$4\r\nPINGXX` | `ERR Protocol error: expected CRLF after bulk string` |
| `ECHO "unterminated` | `ERR Protocol error: unbalanced quotes in request` |
| 70 KB with no newline | `ERR Protocol error: too big inline request` |

Contrast with **command** errors — wrong arity, wrong type, a bad integer. Those
leave framing intact, so the connection stays open and only that command fails.
The distinction is the reason `errors.py` has two exception families.

### No-op requests

Three inputs are well-formed and mean nothing. Redis ignores them silently and
so does this server; each is covered by a test, because a parser that crashes on
an empty line is a parser that crashes on a health check.

```text
*0\r\n        empty multibulk
*-1\r\n       null multibulk
\r\n          blank inline line
```

---

## Replies

Command handlers return ordinary Python objects and the serializer maps them:

| Python | RESP |
| ------ | ---- |
| `SimpleString("OK")` | `+OK\r\n` |
| `RespErrorReply("ERR x")` | `-ERR x\r\n` |
| `int` | `:42\r\n` |
| `bytes` / `str` | `$n\r\n...\r\n` |
| `None` | `$-1\r\n` |
| `list` / `tuple` | `*n\r\n...` |
| `NULL_ARRAY` | `*-1\r\n` |
| `NO_REPLY` | nothing at all |

`SimpleString` and `RespErrorReply` are `str` subclasses, so handlers stay
readable (`return OK`) while the wire form is explicit in the type.

`bool` is **rejected** with a `TypeError` rather than encoded. It is a subclass
of `int`, so `True` would silently become `:1` — and the handler that returned it
almost certainly meant something else. RESP2 has no boolean type.

`NO_REPLY` exists for handlers that manage their own output. `SUBSCRIBE a b c`
sends three separate confirmations, one per channel, and `REPLCONF ACK` sends
nothing at all.

---

## RESP3

Only RESP2 is implemented. `HELLO 3` returns:

```text
-NOPROTO unsupported protocol version
```

which is exactly what a RESP2-only server returns and what makes clients fall
back cleanly instead of hanging on a reply shape they cannot parse. `HELLO` and
`HELLO 2` return the handshake map as a flat array, which is how RESP2 
represents a map.

RESP3 adds real map, set, double, big-number and push types, and it is the push
type that would actually improve things here: in RESP2, Pub/Sub messages are
ordinary arrays, which is why a subscribed connection has to be restricted to a
handful of commands. Without the restriction a client could not tell a reply to
`GET` from a message pushed by another client.

Adding RESP3 would mean a per-connection protocol version, a second serializer
path, and the push type for Pub/Sub. It is a clean extension rather than a
rewrite, and it is not implemented here.

---

## Subscribe mode

Once a RESP2 connection subscribes to anything, only these are accepted:

```text
SUBSCRIBE  UNSUBSCRIBE  PSUBSCRIBE  PUNSUBSCRIBE  PING  QUIT  RESET
```

Anything else returns:

```text
-ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context
```

`PING` behaves differently in this mode — it returns a two-element array
`["pong", ""]` rather than `+PONG` — because everything arriving on a subscribed
connection has to look like a push message for the client to parse it
consistently.

---

## The replication stream

After `PSYNC`, the same socket carries a one-way stream of commands, with one
deviation from normal framing:

```text
+FULLRESYNC <replid> <offset>\r\n
$<length>\r\n<rdb bytes>              ← no trailing CRLF
*3\r\n$3\r\nSET\r\n...                ← ordinary commands from here on
```

The RDB payload is a bulk string **without** the trailing CRLF. A replica that
reads two bytes too many silently consumes the first two bytes of the first
propagated command, and the stream never recovers. Both the reader in
`replication/replica.py` and the integration test that performs the handshake by
hand assert this explicitly.

The stream is not quite one-way: the replica sends `REPLCONF ACK <offset>` back
up the same socket, which is the one case where a `MASTER_LINK` connection
produces output.
