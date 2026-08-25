"""Shared snapshot machinery: pinned blocks, partitioned output, and a per-run manifest.

THE CENTRAL RULE -- ONE PINNED BLOCK PER CHAIN PER RUN. Every read in a snapshot resolves against
the same integer block. Family 149 paid for this lesson: a panel whose entities were each sampled
on their own schedule could not be grouped, and the "median step 476 blocks" statistic it produced
was meaningless because it was computed across the union of unaligned series. `rpc.RPC` enforces
the discipline from the other side -- it REFUSES an implicit "latest", so the tip must be resolved
once, explicitly, and then passed as an int everywhere.

WHY THIS DATA CANNOT BE BACKFILLED, which is the whole product: these are `eth_call` view results,
not events. Free endpoints do not retain archive depth (ours was verified only to -540d), so a
missed day is gone permanently. Hence: two runs per day, dedupe by date, and a gap check that
fails loudly.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"


def utc_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def resolve_tip(rpc, chain_name: str, confirmations: int = 5) -> tuple[int, int]:
    """Resolve ONE block for this chain's snapshot, and its timestamp.

    Backs off `confirmations` blocks from the tip. A reorg at the very tip would otherwise make
    the snapshot describe a state that never became canonical -- cheap insurance, and it costs
    at most a few seconds of staleness.
    """
    tip = rpc.latest_block()
    block = tip - confirmations
    ts = rpc.block_timestamp(block)
    return block, ts


def partition_path(dataset: str, chain: str, date: str) -> Path:
    d = DATA / dataset / chain / date[:4] / date[5:7]
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{date}.parquet"


def write_partition(dataset: str, chain: str, date: str, df: pd.DataFrame,
                    block: int, ts: int) -> Path:
    """Write one day's rows. Idempotent: a second run on the same date OVERWRITES rather than
    appending, so the twice-daily schedule cannot double-count."""
    if df is None or not len(df):
        raise ValueError(f"{dataset}/{chain}/{date}: refusing to write an empty partition -- "
                         "an empty result is a failure signal, not data (families 70/81/86/93)")
    out = df.copy()
    out.insert(0, "snapshot_date", date)
    out.insert(1, "chain", chain)
    out.insert(2, "block", block)
    out.insert(3, "block_ts", ts)
    p = partition_path(dataset, chain, date)
    out.to_parquet(p, index=False)
    return p


def append_manifest(rows: list[dict]) -> Path:
    """One row per (dataset, chain, date) with counts and failure telemetry.

    The manifest is what makes a gap DETECTABLE. Without it a logger that silently returned
    nothing looks identical to a day on which nothing happened -- the exact confusion that cost
    families 70, 81, 86 and 93 a false zero each.
    """
    DATA.mkdir(parents=True, exist_ok=True)
    p = DATA / "manifest.parquet"
    new = pd.DataFrame(rows)
    if p.exists():
        old = pd.read_parquet(p)
        key = ["dataset", "chain", "snapshot_date"]
        old = old[~old.set_index(key).index.isin(new.set_index(key).index)]
        new = pd.concat([old, new], ignore_index=True)
    new.sort_values(["snapshot_date", "dataset", "chain"]).to_parquet(p, index=False)
    return p


class RunLog:
    """Collects manifest rows and prints a human-readable run summary."""

    def __init__(self) -> None:
        self.rows: list[dict] = []
        self.t0 = time.time()

    def record(self, dataset: str, chain: str, date: str, block: int, ts: int,
               n_rows: int, n_requests: int, n_failed: int, note: str = "") -> None:
        self.rows.append({
            "dataset": dataset, "chain": chain, "snapshot_date": date,
            "block": block, "block_ts": ts, "n_rows": n_rows,
            "n_requests": n_requests, "n_failed": n_failed,
            "loss_pct": round(100.0 * n_failed / max(n_requests, 1), 3),
            "note": note,
            "written_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        })
        print(f"  {dataset:<14} {chain:<9} block {block:>10,}  rows {n_rows:>6,}  "
              f"calls {n_requests:>6,}  failed {n_failed:>4}  {note}", flush=True)

    def flush(self) -> None:
        if self.rows:
            append_manifest(self.rows)
        print(f"\nrun complete in {(time.time() - self.t0) / 60:.1f} min, "
              f"{len(self.rows)} dataset-chain partitions written", flush=True)
