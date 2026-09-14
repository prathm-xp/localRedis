#!/usr/bin/env bash
# Start a master and a replica, exercise them, and shut both down.
# A quick end-to-end smoke test that needs nothing but Python.
set -euo pipefail

cd "$(dirname "$0")/.."

MASTER_PORT=${MASTER_PORT:-7379}
REPLICA_PORT=${REPLICA_PORT:-7380}
WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT

echo "==> starting master on $MASTER_PORT"
mkdir -p "$WORKDIR/master" "$WORKDIR/replica"
python -m pyredis --port "$MASTER_PORT" --dir "$WORKDIR/master" &
MASTER_PID=$!
trap 'kill $MASTER_PID 2>/dev/null || true; rm -rf "$WORKDIR"' EXIT
sleep 1

echo "==> starting replica on $REPLICA_PORT"
python -m pyredis --port "$REPLICA_PORT" --dir "$WORKDIR/replica" \
    --replicaof "localhost $MASTER_PORT" &
REPLICA_PID=$!
trap 'kill $MASTER_PID $REPLICA_PID 2>/dev/null || true; rm -rf "$WORKDIR"' EXIT
sleep 2

echo "==> exercising the pair"
python - "$MASTER_PORT" "$REPLICA_PORT" <<'PY'
import socket
import sys
import time


def encode(*args):
    out = [b"*%d\r\n" % len(args)]
    for arg in args:
        raw = arg.encode() if isinstance(arg, str) else arg
        out.append(b"$%d\r\n" % len(raw) + raw + b"\r\n")
    return b"".join(out)


def talk(port, *args):
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        sock.sendall(encode(*args))
        return sock.recv(65536)


master, replica = int(sys.argv[1]), int(sys.argv[2])

print("  master  SET city pune      ->", talk(master, "SET", "city", "pune"))
print("  master  RPUSH l a b c      ->", talk(master, "RPUSH", "l", "a", "b", "c"))
print("  master  XADD s * f v       ->", talk(master, "XADD", "s", "*", "f", "v"))
time.sleep(1)
print("  replica GET city           ->", talk(replica, "GET", "city"))
print("  replica LRANGE l 0 -1      ->", talk(replica, "LRANGE", "l", "0", "-1"))
print("  replica SET city other     ->", talk(replica, "SET", "city", "other"))
print("  master  WAIT 1 1000        ->", talk(master, "WAIT", "1", "1000"))
PY

echo "==> done"
