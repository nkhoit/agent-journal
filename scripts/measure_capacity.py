#!/usr/bin/env python3
"""Bounded local synthetic measurements for issues 44/45; standard library only."""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import http.client
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import socket
import subprocess
import time
from urllib.parse import urlsplit


def summary(values):
    ordered = sorted(values)
    if not ordered:
        return {"n": 0}
    return {"n": len(ordered), "min_ms": ordered[0],
            "median_ms": ordered[len(ordered) // 2],
            "p95_ms": ordered[math.ceil(len(ordered) * 0.95) - 1],
            "max_ms": ordered[-1]}


def hardware():
    def read(path):
        try:
            return Path(path).read_text().strip()
        except OSError:
            return "unavailable"
    cpu = read("/proc/cpuinfo")
    model = next((line.split(":", 1)[1].strip() for line in cpu.splitlines()
                  if line.startswith("model name")), "unavailable")
    return {"architecture": platform.machine(), "os": platform.system(),
            "cpu_model": model, "visible_cpus": os.cpu_count(),
            "cpu_max": read("/sys/fs/cgroup/cpu.max"),
            "memory_max_bytes": read("/sys/fs/cgroup/memory.max"),
            "filesystem_note": "disposable temp filesystem; no cold-cache isolation"}


def http_phase(endpoint, noisy):
    address = urlsplit(endpoint)
    if address.scheme != "http" or address.hostname != "127.0.0.1":
        raise ValueError("fixture must bind IPv4 loopback")

    def request(method, path, principal, payload=None, key=None):
        connection = http.client.HTTPConnection("127.0.0.1", address.port, timeout=35)
        # Synthetic fixture credentials stay in process memory, never argv or output.
        headers = {"Authorization": "Bearer " + format(principal, "064x")}
        body = None
        if payload is not None:
            body = json.dumps(payload).encode()
            headers["Content-Type"] = "application/json"
        if key is not None:
            headers["Idempotency-Key"] = key
        started = time.monotonic()
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            raw = response.read()
            data = json.loads(raw) if raw else None
            return response.status, data, (time.monotonic() - started) * 1000
        finally:
            connection.close()

    phase_started = time.monotonic()

    def search(worker):
        results = Counter()
        times = []
        for _ in range(24):
            status, page, elapsed = request("GET", "/v1/spaces/bench/search?q=common&order=rank&limit=50", 1)
            results[str(status)] += 1
            if status == 200:
                if len(page["items"]) != 50:
                    raise ValueError("unexpected search page size")
                times.append(elapsed)
            elif status != 503:
                raise ValueError("unexpected search status")
            time.sleep(0.001)
        return {"statuses": dict(results), "success_ms": times,
                "finished_at": time.monotonic() - phase_started}

    def disconnect():
        # Close real TCP read requests immediately after sending, without waiting for responses.
        # Client-side send completion does not prove server-side SQL admission.
        for _ in range(24):
            with socket.create_connection(("127.0.0.1", address.port), timeout=35) as stream:
                stream.sendall(("GET /v1/spaces/bench/search?q=common&order=rank&limit=50 HTTP/1.1\r\n"
                                "Host: localhost\r\nAuthorization: Bearer " + format(1, "064x") +
                                "\r\nConnection: close\r\n\r\n").encode())
            time.sleep(0.002)
        return 24

    def progress():
        outcomes = {name: Counter() for name in ["append", "inbox", "ack"]}
        times = {name: [] for name in outcomes}
        rounds = []
        events = []
        for index in range(30):
            started = time.monotonic()
            payload = {"kind": "note", "content": "synthetic common mixed progress",
                       "attention": ["recipient"]}
            status, _, elapsed = request("POST", "/v1/spaces/bench/records", 3, payload,
                                         f"http-{'loaded' if noisy else 'control'}-{index}")
            outcomes["append"][str(status)] += 1
            events.append(("append", str(status), elapsed, time.monotonic() - phase_started))
            if status == 201:
                times["append"].append(elapsed)
            elif status != 503:
                raise ValueError("unexpected append status")
            status, page, elapsed = request("GET", "/v1/inbox?limit=50", 2)
            outcomes["inbox"][str(status)] += 1
            events.append(("inbox", str(status), elapsed, time.monotonic() - phase_started))
            if status == 200:
                times["inbox"].append(elapsed)
                if not page["items"]:
                    time.sleep(0.002)
                    continue
                item = page["items"][0]["inbox_item_id"]
                status, _, elapsed = request("POST", f"/v1/inbox/{item}/ack", 2)
                outcomes["ack"][str(status)] += 1
                events.append(("ack", str(status), elapsed, time.monotonic() - phase_started))
                if status == 204:
                    times["ack"].append(elapsed)
                    rounds.append((time.monotonic() - started) * 1000)
                elif status != 503:
                    raise ValueError("unexpected ack status")
            elif status != 503:
                raise ValueError("unexpected inbox status")
            time.sleep(0.002)
        return {"attempted_rounds": 30, "statuses": {k: dict(v) for k, v in outcomes.items()},
                "success_latency": {k: summary(v) for k, v in times.items()},
                "rounds_reaching_ack": summary(rounds), "_events": events}

    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=14) as pool:
        searches = [pool.submit(search, worker) for worker in range(12)] if noisy else []
        disconnects = pool.submit(disconnect) if noisy else None
        useful = pool.submit(progress)
        results = [future.result() for future in searches]
        progress_result = useful.result()
        sent_disconnects = disconnects.result() if disconnects else 0
    events = progress_result.pop("_events")
    if results:
        last_search = max(result["finished_at"] for result in results)
        overlap = {}
        for operation in ["append", "inbox", "ack"]:
            selected = [event for event in events if event[0] == operation and event[3] <= last_search]
            overlap[operation] = {"statuses": dict(Counter(event[1] for event in selected)),
                                  "success_latency": summary([event[2] for event in selected
                                                              if event[1] in ["200", "201", "204"]])}
        progress_result["before_last_search_worker_finished"] = overlap
        progress_result["last_search_worker_finished_ms"] = last_search * 1000
    statuses = Counter()
    times = []
    for result in results:
        statuses.update(result["statuses"])
        times.extend(result["success_ms"])
    return {"wall_ms": (time.monotonic() - started) * 1000,
            "search_workers": len(searches), "search_attempts_per_worker": 24 if noisy else 0,
            "search_statuses": dict(statuses), "successful_search_latency": summary(times),
            "tcp_read_disconnects_sent": sent_disconnects, "other_principal": progress_result}


def run(binary, records):
    process = subprocess.Popen([str(binary), str(records)], stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, text=True)
    try:
        line = process.stdout.readline()
        if not line:
            raise RuntimeError("fixture exited before measurements")
        fixture = json.loads(line)
        control = http_phase(fixture["endpoint"], False)
        loaded = http_phase(fixture["endpoint"], True)
        process.stdin.write("done\n")
        process.stdin.flush()
        final = json.loads(process.stdout.readline())
        if process.wait(timeout=120) != 0:
            raise RuntimeError("fixture failed during verification/cleanup")
        confirmed = sum(phase["other_principal"]["statuses"]["append"].get("201", 0)
                        for phase in [control, loaded])
        if final["final_records"] != records + 3 + confirmed:
            raise RuntimeError("HTTP confirmed appends do not match final verified count")
        return {"baseline": fixture["baseline"], "http_control": control,
                "http_loaded": loaded, "final": final}
    finally:
        if process.poll() is None:
            # Graceful drain only: never kill a fixture during an audited mutation.
            try:
                process.stdin.write("done\n")
                process.stdin.flush()
            except BrokenPipeError:
                pass
            process.wait(timeout=120)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/examples/capacity_fixture"))
    parser.add_argument("--sizes", type=int, nargs="+", default=[1000, 5000, 10000])
    args = parser.parse_args()
    if len(args.sizes) > 3 or any(not 100 <= size <= 20000 for size in args.sizes):
        parser.error("choose at most three sizes, each 100..20000")
    binary = args.binary.resolve(strict=True)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    print(json.dumps({"format": 1, "source_revision": revision,
                      "harness_sha256": {name: hashlib.sha256(Path(name).read_bytes()).hexdigest()
                                         for name in ["scripts/measure_capacity.py",
                                                      "crates/journald/examples/capacity_fixture.rs"]},
                      "hardware": hardware(),
                      "config": {"profile": "release (caller must build accordingly)",
                                 "blocking_limit": 8, "tokio_workers": 2,
                                 "search_limit": 50, "search_samples": 7, "backup_rounds": 3,
                                 "query": "synthetic common term matching every record",
                                 "bounded_work_not_wall_deadline": True},
                      "runs": [run(binary, size) for size in args.sizes]}, indent=2))


if __name__ == "__main__":
    main()
