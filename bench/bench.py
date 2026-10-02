#!/usr/bin/env python3
"""Benchmarks rusty-redis against a real redis-server.

Throughput and latency come from redis-benchmark, run with identical settings
against both servers. Each repeat starts a fresh process of each server, so
both begin from an empty keyspace, and the order alternates between repeats so
neither always runs on the warmer machine. The median of the repeats is
reported.

Memory is measured separately: a fresh process per server and per data type,
its RSS read before and after a known load, divided by the number of items.
Load only, never load then delete, because freed pages are not reliably
returned to the OS.

Every run writes a result file recording the command line, commit, versions
and machine, as PLAN.md's measurement protocol asks.

    bench/bench.py                          # defaults
    bench/bench.py --pipeline 1,16 --repeat 5
    bench/bench.py --memory 200000          # also bytes per key, element, entry
    bench/bench.py --tests set,get --no-build
"""

import argparse
import csv
import datetime
import io
import os
import platform
import shutil
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BINARY = ROOT / "target" / "release" / "rusty-redis"

# redis-benchmark's built in tests that rusty-redis implements. ping_inline is
# left out on purpose: it uses Redis's inline protocol, which is a documented
# divergence here, not a speed difference.
DEFAULT_TESTS = "ping_mbulk,set,get,incr,lpush,rpush,lpop,rpop,lrange_100"

# Custom commands, run through redis-benchmark's command mode. __rand_int__ is
# replaced per request, within the --keyspace range.
CUSTOM = {
    "XADD": ["XADD", "bench:stream", "*", "f", "__rand_int__"],
}

# Rows redis-benchmark prints that are setup for another test, not results.
SETUP_ROWS = ("LPUSH (needed to benchmark LRANGE)",)

SERVERS = ("rusty-redis", "redis")


def die(msg):
    print(f"bench: {msg}", file=sys.stderr)
    sys.exit(1)


def run(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def port_free(port):
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", port)) != 0


def wait_ready(port, proc, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            die(f"server on port {port} exited during startup")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5) as s:
                s.sendall(b"*1\r\n$4\r\nPING\r\n")
                if s.recv(64).startswith(b"+PONG"):
                    return
        except OSError:
            pass
        time.sleep(0.05)
    die(f"server on port {port} did not answer PING within {timeout}s")


class Server:
    """One fresh server process, stopped on exit from the with block."""

    def __init__(self, which, port):
        self.which, self.port = which, port

    def __enter__(self):
        if not port_free(self.port):
            die(f"port {self.port} is already in use")
        if self.which == "rusty-redis":
            cmd = [str(BINARY), "--port", str(self.port)]
        else:
            cmd = ["redis-server", "--port", str(self.port), "--save", "",
                   "--appendonly", "no", "--daemonize", "no"]
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL)
        wait_ready(self.port, self.proc)
        return self

    def rss_bytes(self):
        out = run(["ps", "-o", "rss=", "-p", str(self.proc.pid)]).stdout.strip()
        return int(out) * 1024

    def __exit__(self, *exc):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()


def benchmark(port, args, pipeline, tests, custom):
    """One redis-benchmark invocation. Returns {test: (rps, p50, p99)}.

    redis-benchmark does not check replies, so a server answering every request
    with an error would look fast. Any error it reports makes the run invalid.
    """
    cmd = ["redis-benchmark", "-p", str(port), "-n", str(args.requests),
           "-c", str(args.clients), "-P", str(pipeline), "-r", str(args.keyspace),
           "-d", str(args.data_size), "--csv"]
    cmd += ["-t", tests] if tests else custom
    res = run(cmd)
    if res.returncode != 0 or "Error from server" in res.stdout + res.stderr:
        detail = (res.stderr.strip() or res.stdout.strip()).splitlines()
        die(f"redis-benchmark failed on port {port}: {' | '.join(detail[-3:])}")
    rows = {}
    for row in csv.DictReader(io.StringIO(res.stdout)):
        name = row["test"]
        if name in SETUP_ROWS:
            continue
        name = name.split(" (")[0]          # "LRANGE_100 (first 100 elements)"
        if custom:
            name = custom[0]
        rows[name] = (float(row["rps"]), float(row["p50_latency_ms"]),
                      float(row["p99_latency_ms"]))
    return rows


def throughput(args, ports):
    """{(server, pipeline, test): [(rps, p50, p99), ...]} over all repeats."""
    results = {}
    customs = [CUSTOM[name] for name in args.custom]
    for rep in range(args.repeat):
        order = SERVERS if rep % 2 == 0 else tuple(reversed(SERVERS))
        for which in order:
            with Server(which, ports[which]) as server:
                for pipeline in args.pipeline:
                    found = {}
                    if args.tests:
                        found.update(benchmark(server.port, args, pipeline, args.tests, None))
                    for custom in customs:
                        found.update(benchmark(server.port, args, pipeline, None, custom))
                    for test, value in found.items():
                        results.setdefault((which, pipeline, test), []).append(value)
            print(f"  repeat {rep + 1}/{args.repeat}: {which} done", file=sys.stderr)
    return results


def resp(*parts):
    out = b"*%d\r\n" % len(parts)
    for p in parts:
        p = p if isinstance(p, bytes) else str(p).encode()
        out += b"$%d\r\n%s\r\n" % (len(p), p)
    return out


def load(port, commands, batch=1000):
    """Sends commands pipelined, in batches. A PING closes each batch, so
    reading up to its +PONG means every reply in the batch has arrived."""
    with socket.create_connection(("127.0.0.1", port)) as s:
        for start in range(0, len(commands), batch):
            s.sendall(b"".join(commands[start:start + batch]) + resp("PING"))
            buf = b""
            while not buf.endswith(b"+PONG\r\n"):
                chunk = s.recv(1 << 16)
                if not chunk:
                    die(f"server on port {port} closed the connection during load")
                buf += chunk
            if b"\r\n-" in b"\r\n" + buf:
                die(f"server on port {port} replied with an error during load")


def memory(args, ports):
    """{(server, kind): bytes per item}, each from a fresh process."""
    n, value = args.memory, b"x" * args.data_size
    loads = {
        "string key": [resp("SET", f"key:{i}", value) for i in range(n)],
        "list element": [resp("RPUSH", "bench:list", *([value] * 100))
                         for _ in range(n // 100)],
        "stream entry": [resp("XADD", "bench:stream", f"{i + 1}-0", "f", value)
                         for i in range(n)],
    }
    results = {}
    for kind, commands in loads.items():
        items = n if kind != "list element" else (n // 100) * 100
        for which in SERVERS:
            with Server(which, ports[which]) as server:
                before = server.rss_bytes()
                load(server.port, commands)
                time.sleep(0.2)
                results[(which, kind)] = (server.rss_bytes() - before) / items
        print(f"  memory: {kind} done", file=sys.stderr)
    return results


def git(*a):
    return run(["git", "-C", str(ROOT), *a]).stdout.strip()


def machine():
    cpu = platform.processor() or platform.machine()
    if sys.platform == "darwin":
        cpu = run(["sysctl", "-n", "machdep.cpu.brand_string"]).stdout.strip() or cpu
    elif Path("/proc/cpuinfo").exists():
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    return f"{platform.system()} {platform.release()}, {cpu}, {os.cpu_count()} cores"


def median3(values):
    return tuple(statistics.median(v[i] for v in values) for i in range(3))


def report(args, tp, mem, started):
    commit = git("rev-parse", "--short", "HEAD")
    dirty = " (uncommitted changes)" if git("status", "--porcelain", "--untracked-files=no") else ""
    lines = [
        f"# Benchmark {started:%Y-%m-%d %H:%M}",
        "",
        f"- commit: `{commit}`{dirty}",
        f"- machine: {machine()}",
        f"- redis: {run(['redis-server', '--version']).stdout.strip()}",
        f"- rustc: {run(['rustc', '--version']).stdout.strip()}",
        f"- command: `{' '.join(['bench/bench.py', *sys.argv[1:]])}`",
        f"- settings: {args.requests} requests, {args.clients} clients, "
        f"keyspace {args.keyspace}, {args.data_size} byte values, "
        f"median of {args.repeat} repeats, fresh processes per repeat",
        "",
        "Both servers run on the same machine, one at a time. Redis runs commands",
        "on one thread with default settings; rusty-redis parses on Tokio's",
        "multi-threaded runtime and runs commands on one keyspace task.",
        "",
    ]
    tests = list(dict.fromkeys(t for (_, _, t) in tp))   # in the order they ran
    for pipeline in args.pipeline:
        lines += [
            f"## Throughput, pipeline {pipeline}",
            "",
            "| command | rusty-redis ops/s | redis ops/s | ratio | rusty-redis p50 / p99 ms | redis p50 / p99 ms |",
            "|---|---:|---:|---:|---:|---:|",
        ]
        for test in tests:
            ours = tp.get(("rusty-redis", pipeline, test))
            theirs = tp.get(("redis", pipeline, test))
            if not ours or not theirs:
                continue
            o, r = median3(ours), median3(theirs)
            lines.append(
                f"| {test} | {o[0]:,.0f} | {r[0]:,.0f} | {o[0] / r[0]:.2f}x "
                f"| {o[1]:.3f} / {o[2]:.3f} | {r[1]:.3f} / {r[2]:.3f} |")
        lines.append("")
    if mem:
        lines += [
            f"## Memory, {args.memory} items, {args.data_size} byte values",
            "",
            "RSS growth divided by item count, a fresh process per row and server.",
            "",
            "| item | rusty-redis bytes | redis bytes | ratio |",
            "|---|---:|---:|---:|",
        ]
        for kind in ("string key", "list element", "stream entry"):
            o, r = mem[("rusty-redis", kind)], mem[("redis", kind)]
            ratio = f"{o / r:.2f}x" if r > 0 else "n/a"
            lines.append(f"| {kind} | {o:,.0f} | {r:,.0f} | {ratio} |")
        lines.append("")
    return "\n".join(lines), commit


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--requests", type=int, default=100_000, help="requests per test")
    p.add_argument("--clients", type=int, default=50, help="parallel connections")
    p.add_argument("--pipeline", default="1,16",
                   help="comma separated pipeline depths, each run separately")
    p.add_argument("--repeat", type=int, default=3, help="repeats; the median is reported")
    p.add_argument("--keyspace", type=int, default=100_000, help="range of __rand_int__")
    p.add_argument("--data-size", type=int, default=3, help="value size in bytes")
    p.add_argument("--tests", default=DEFAULT_TESTS,
                   help="redis-benchmark -t tests, or '' for none")
    p.add_argument("--custom", default="XADD",
                   help=f"comma separated custom commands from {sorted(CUSTOM)}, or ''")
    p.add_argument("--memory", type=int, default=0,
                   help="items to load for the memory measurement, 0 to skip")
    p.add_argument("--ports", default="6410,6411", help="rusty-redis port,redis port")
    p.add_argument("--no-build", action="store_true", help="skip cargo build --release")
    p.add_argument("--out-dir", default=str(ROOT / "bench" / "results"))
    args = p.parse_args()

    args.pipeline = [int(x) for x in args.pipeline.split(",") if x]
    args.custom = [x for x in args.custom.split(",") if x]
    for name in args.custom:
        if name not in CUSTOM:
            die(f"unknown custom command {name}; known: {sorted(CUSTOM)}")
    ours_port, redis_port = (int(x) for x in args.ports.split(","))
    ports = {"rusty-redis": ours_port, "redis": redis_port}

    for tool in ("redis-server", "redis-benchmark", "cargo"):
        if not shutil.which(tool):
            die(f"{tool} not found on PATH")
    if not args.no_build:
        build = run(["cargo", "build", "--release", "--quiet"], cwd=ROOT)
        if build.returncode != 0:
            die("cargo build --release failed:\n" + build.stderr)
    if not BINARY.exists():
        die(f"{BINARY} not found; build it or drop --no-build")

    started = datetime.datetime.now()
    print("throughput:", file=sys.stderr)
    tp = throughput(args, ports)
    mem = None
    if args.memory:
        print("memory:", file=sys.stderr)
        mem = memory(args, ports)

    text, commit = report(args, tp, mem, started)
    print(text)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{started:%Y-%m-%d-%H%M}-{commit}.md"
    out.write_text(text + "\n")
    print(f"\nwritten to {out.relative_to(ROOT) if out.is_relative_to(ROOT) else out}",
          file=sys.stderr)


if __name__ == "__main__":
    main()
