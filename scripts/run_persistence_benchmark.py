#!/usr/bin/env python3
import json
import sqlite3
import sys
import time
import urllib.request


def fmt_us(value):
    if value is None:
        return "--"
    if value < 1000:
        return f"{value}us"
    return f"{value / 1000:.2f}ms"


def post_json(url, payload):
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def fetch_snapshot(base_url):
    with urllib.request.urlopen(f"{base_url}/api/snapshot", timeout=5) as response:
        return json.load(response)


def cleanup_benchmark_rows(db_path):
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM simulated_orders WHERE symbol = '__BENCH__/USDC'")
        cursor.execute("DELETE FROM signal_events WHERE symbol = '__BENCH__/USDC'")
        conn.commit()
    finally:
        conn.close()


def main():
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:3000"
    count = int(sys.argv[2]) if len(sys.argv) > 2 else 2000
    duration = float(sys.argv[3]) if len(sys.argv) > 3 else 10.0
    interval = float(sys.argv[4]) if len(sys.argv) > 4 else 0.25
    db_path = (
        sys.argv[5]
        if len(sys.argv) > 5
        else "/Users/wuge/Desktop/个人开发脚本/lighter-rust-bbo/data/lighter.db"
    )

    print(f"enqueueing {count} synthetic persistence tasks to {base_url}")
    response = post_json(f"{base_url}/api/dev/stress-persistence", {"count": count})
    print(f"enqueued: {response['enqueued']}")

    start = time.time()
    samples = []
    while time.time() - start < duration:
        snapshot = fetch_snapshot(base_url)
        metrics = snapshot["status"]["latency_metrics"]
        samples.append(metrics)
        time.sleep(interval)

    def max_metric(path):
        current = None
        for sample in samples:
            value = sample
            for key in path:
                value = value[key]
            if value is None:
                continue
            current = value if current is None else max(current, value)
        return current

    summary = {
        "peak_queue_depth": max_metric(["persistence_queue_depth"]),
        "persist_p99_max": max_metric(["persist_us", "p99_us"]),
        "batch_p99_max": max_metric(["persistence_batch_size", "p99_us"]),
        "batch_max": max_metric(["persistence_batch_size", "max_us"]),
        "queue_p99_max": max_metric(["queue_delay_us", "p99_us"]),
        "e2e_p99_max": max_metric(["end_to_end_us", "p99_us"]),
        "persist_samples_final": samples[-1]["persist_us"]["samples"] if samples else 0,
    }

    print("benchmark summary")
    print(f"peak queue depth: {summary['peak_queue_depth']}")
    print(f"persist p99 max: {fmt_us(summary['persist_p99_max'])}")
    print(f"batch p99 max: {summary['batch_p99_max'] or '--'} events")
    print(f"batch max: {summary['batch_max'] or '--'} events")
    print(f"queue p99 max: {fmt_us(summary['queue_p99_max'])}")
    print(f"e2e p99 max: {fmt_us(summary['e2e_p99_max'])}")
    print(f"persist samples final: {summary['persist_samples_final']}")

    cleanup_benchmark_rows(db_path)
    print("cleaned benchmark rows from sqlite")


if __name__ == "__main__":
    main()
