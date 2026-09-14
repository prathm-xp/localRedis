"""A small, reproducible throughput and latency benchmark.

Runs against any RESP server - this one or a real ``redis-server`` - so the two
can be compared on the same machine with the same client. That comparison is
the only one worth making: absolute numbers depend entirely on the hardware.

Usage::

    python benchmarks/bench.py --port 6379
    python benchmarks/bench.py --port 6379 --clients 50 --requests 20000
    python benchmarks/bench.py --port 6379 --pipeline 16

Each client runs in its own thread with a blocking socket, which is what a real
client library does. Latency is measured per request batch and reported as
percentiles, because a mean hides exactly the tail the numbers are for.
"""

from __future__ import annotations

import argparse
import socket
import statistics
import threading
import time
from dataclasses import dataclass, field

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 6379
READ_CHUNK = 65536


def encode_command(*args: str | bytes) -> bytes:
    out = [b"*%d\r\n" % len(args)]
    for arg in args:
        raw = arg if isinstance(arg, bytes) else arg.encode()
        out.append(b"$%d\r\n" % len(raw) + raw + b"\r\n")
    return b"".join(out)


@dataclass
class Results:
    """Latency samples collected by every client thread."""

    latencies_ms: list[float] = field(default_factory=list)
    errors: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def record(self, samples: list[float]) -> None:
        with self.lock:
            self.latencies_ms.extend(samples)

    def record_error(self) -> None:
        with self.lock:
            self.errors += 1


class BenchClient:
    """One connection, with just enough RESP parsing to count replies."""

    def __init__(self, host: str, port: int) -> None:
        self.socket = socket.create_connection((host, port), timeout=30)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buffer = bytearray()

    def close(self) -> None:
        self.socket.close()

    def send(self, payload: bytes) -> None:
        self.socket.sendall(payload)

    def read_replies(self, count: int) -> None:
        """Consume exactly ``count`` replies.

        Only the reply *count* matters here, so this counts top-level type
        markers rather than building values - the benchmark should measure the
        server, not this parser.
        """
        remaining = count
        while remaining > 0:
            if not self.buffer:
                chunk = self.socket.recv(READ_CHUNK)
                if not chunk:
                    raise ConnectionError("server closed the connection")
                self.buffer += chunk
            remaining -= self._consume_complete_replies(remaining)

    def _consume_complete_replies(self, wanted: int) -> int:
        consumed = 0
        while consumed < wanted:
            marker = self.buffer[:1]
            if not marker:
                return consumed
            if marker in (b"+", b"-", b":"):
                end = self.buffer.find(b"\r\n")
                if end == -1:
                    return consumed
                del self.buffer[: end + 2]
            elif marker == b"$":
                end = self.buffer.find(b"\r\n")
                if end == -1:
                    return consumed
                length = int(self.buffer[1:end])
                if length == -1:
                    del self.buffer[: end + 2]
                else:
                    total = end + 2 + length + 2
                    if len(self.buffer) < total:
                        return consumed
                    del self.buffer[:total]
            else:
                raise ValueError(f"unexpected reply marker {marker!r}")
            consumed += 1
        return consumed


def run_client(
    host: str,
    port: int,
    command: bytes,
    requests: int,
    pipeline: int,
    results: Results,
    barrier: threading.Barrier,
) -> None:
    try:
        client = BenchClient(host, port)
    except OSError:
        results.record_error()
        barrier.wait()
        return

    payload = command * pipeline
    batches = max(requests // pipeline, 1)
    samples: list[float] = []

    barrier.wait()  # start every thread together
    try:
        for _ in range(batches):
            started = time.perf_counter()
            client.send(payload)
            client.read_replies(pipeline)
            samples.append((time.perf_counter() - started) * 1000)
    except (OSError, ValueError):
        results.record_error()
    finally:
        client.close()
    results.record(samples)


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    index = min(int(len(ordered) * fraction), len(ordered) - 1)
    return ordered[index]


def benchmark(
    name: str, command: bytes, args: argparse.Namespace
) -> tuple[str, float, float, float, float]:
    results = Results()
    barrier = threading.Barrier(args.clients)
    threads = [
        threading.Thread(
            target=run_client,
            args=(args.host, args.port, command, args.requests, args.pipeline, results, barrier),
            daemon=True,
        )
        for _ in range(args.clients)
    ]

    started = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = time.perf_counter() - started

    if results.errors:
        raise SystemExit(f"{results.errors} client(s) failed during the {name} run")

    completed = len(results.latencies_ms) * args.pipeline
    throughput = completed / elapsed if elapsed else 0.0
    # Divide by the pipeline depth to report per-request latency.
    per_request = [sample / args.pipeline for sample in results.latencies_ms]
    return (
        name,
        throughput,
        statistics.median(per_request),
        percentile(per_request, 0.95),
        percentile(per_request, 0.99),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--clients", type=int, default=10, help="concurrent connections")
    parser.add_argument("--requests", type=int, default=10_000, help="requests per client")
    parser.add_argument("--pipeline", type=int, default=1, help="requests per round trip")
    parser.add_argument("--value-size", type=int, default=16, help="SET value size in bytes")
    args = parser.parse_args()

    if args.pipeline < 1 or args.clients < 1 or args.requests < 1:
        parser.error("--clients, --requests and --pipeline must all be at least 1")

    value = b"x" * args.value_size
    workloads = [
        ("PING", encode_command("PING")),
        ("SET", encode_command(b"SET", b"bench:key", value)),
        ("GET", encode_command(b"GET", b"bench:key")),
        ("INCR", encode_command(b"INCR", b"bench:counter")),
    ]

    print(f"target      {args.host}:{args.port}")
    print(f"clients     {args.clients}")
    print(f"requests    {args.requests} per client ({args.clients * args.requests} total)")
    print(f"pipeline    {args.pipeline}")
    print()
    print(f"{'command':<10}{'ops/sec':>12}{'p50 ms':>10}{'p95 ms':>10}{'p99 ms':>10}")
    print("-" * 52)

    for name, command in workloads:
        label, throughput, p50, p95, p99 = benchmark(name, command, args)
        print(f"{label:<10}{throughput:>12,.0f}{p50:>10.3f}{p95:>10.3f}{p99:>10.3f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
