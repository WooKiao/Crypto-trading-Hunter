#!/usr/bin/env python3
import json
import sys
import time
import urllib.request


def fmt_us(value):
    if value is None:
        return "--"
    if value < 1000:
        return f"{value}us"
    return f"{value / 1000:.2f}ms"


def fetch_snapshot(base_url):
    with urllib.request.urlopen(f"{base_url}/api/snapshot", timeout=5) as response:
        return json.load(response)


def main():
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:3000"
    interval = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0

    print(f"sampling latency metrics from {base_url} every {interval:.2f}s")
    while True:
        data = fetch_snapshot(base_url)
        metrics = data["status"]["latency_metrics"]
        end_to_end = metrics["end_to_end_us"]
        queue_delay = metrics["queue_delay_us"]
        processing = metrics["processing_us"]
        persist = metrics["persist_us"]

        print(
            " | ".join(
                [
                    time.strftime("%H:%M:%S"),
                    f"e2e latest {fmt_us(end_to_end['latest_us'])}",
                    f"e2e p99 {fmt_us(end_to_end['p99_us'])}",
                    f"queue p99 {fmt_us(queue_delay['p99_us'])}",
                    f"process p99 {fmt_us(processing['p99_us'])}",
                    f"persist p99 {fmt_us(persist['p99_us'])}",
                    f"persist samples {persist['samples']}",
                    f"batch p99 {metrics['persistence_batch_size']['p99_us'] or '--'} events",
                    f"queue depth {metrics['persistence_queue_depth']}",
                    f"e2e samples {end_to_end['samples']}",
                ]
            )
        )
        time.sleep(interval)


if __name__ == "__main__":
    main()
