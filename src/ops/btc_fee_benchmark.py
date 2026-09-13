"""Run and privately archive the persistent E31 Bitcoin observation panel."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import signal
import threading
import time

from src.ephemeral.e31_btc_fee_benchmark import Benchmark, PublicClient, DATASETS

ROOT = Path(__file__).resolve().parents[2]


def archive(root):
    from huggingface_hub import HfApi
    api = HfApi(token=os.environ.get("HF_TOKEN"))
    repo = "dataforge-labs/dataforge-ephemeral"
    info = api.repo_info(repo, repo_type="dataset")
    if not info.private:
        raise RuntimeError("Refusing to archive the benchmark into a public repository")
    # No create/delete/mirror operation; existing samples and unrelated archives are untouched.
    api.upload_folder(repo_id=repo, repo_type="dataset", folder_path=str(root),
        allow_patterns=[f"{dataset}/**/*.parquet" for dataset in DATASETS],
        commit_message="Collect Bitcoin fee quotes and observed transaction outcomes")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=16200)
    parser.add_argument("--interval", type=float, default=120)
    parser.add_argument("--data-dir", type=Path, default=ROOT/"data")
    parser.add_argument("--archive", action="store_true")
    args = parser.parse_args()
    if args.archive:
        archive(args.data_dir)
        return 0
    if not 0 < args.duration <= 19800 or args.interval < 30:
        parser.error("duration must be 0..19800 seconds and interval at least 30 seconds")
    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    client = PublicClient(rpc_url=os.environ.get("DF_BTC_BENCHMARK_RPC", "https://bitcoin-rpc.publicnode.com"))
    collector = Benchmark(args.data_dir, run_id, client=client)
    deadline = time.monotonic()+args.duration
    print(f"E31 {run_id}: resumed {len(collector.state['active'])} transactions; "
          f"sample 1/{collector.denominator}; interval {args.interval}s", flush=True)
    try:
        while not stop.is_set() and time.monotonic() < deadline:
            started = time.monotonic()
            collector.tick()
            stop.wait(max(0, min(deadline-time.monotonic(), args.interval-(time.monotonic()-started))))
    finally:
        collector.finish()
    print(f"E31 complete: {collector.successful_rounds} successful rounds", flush=True)
    return 0 if collector.successful_rounds else 1


if __name__ == "__main__":
    raise SystemExit(main())
