"""The resident ephemeral collector. Runs ~5.5h per invocation, four times a day.

WHY RESIDENT RATHER THAN CRON. GitHub cron fires at most every 5 minutes and scheduled runs are
best-effort. A sparse snapshot cannot see LIFECYCLE -- how long a transaction sat pending, or that
it was dropped and never mined -- and lifecycle is precisely the part no archive can reconstruct.
Actions caps a job at 6 hours, so 5.5h leaves margin, and four staggered starts give ~22h/day of
coverage.

Everything here is Ethereum-only by measurement, not by omission: Base's txpool_content returns
ZERO transactions because it runs a centralised sequencer with no public mempool to observe.

OUTPUT IS PARTITIONED BY RUN, not just by date, because there are four runs a day and each is an
independent observation window. A gap between windows is a real gap and is visible as one.
"""
from __future__ import annotations

import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.ephemeral import e1_mempool, e3_divergence

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"

DURATION_S = int(os.environ.get("DF_DURATION_S", 5 * 3600 + 1800))   # 5.5h
POLL_S = float(os.environ.get("DF_POLL_S", 5))
DIVERGENCE_EVERY_S = float(os.environ.get("DF_DIVERGENCE_EVERY_S", 300))
# Checkpoint cadence. A 5.5h job holds its tracker state in memory, so a runner kill would
# otherwise lose the entire window -- and an ephemeral window cannot be re-collected. Writing
# every 30 min caps the worst-case loss at 30 minutes instead of 5.5 hours.
CHECKPOINT_EVERY_S = float(os.environ.get("DF_CHECKPOINT_EVERY_S", 1800))


def write(dataset: str, df: pd.DataFrame, run_id: str, quiet: bool = False) -> Path | None:
    """One partition per dataset per run. An empty frame is a FAILURE SIGNAL, not data --
    it is refused rather than written (families 70/81/86/93)."""
    if df is None or not len(df):
        print(f"  !! {dataset}: empty frame, refusing to write", flush=True)
        return None
    date = run_id[:10]
    d = DATA / dataset / date[:4] / date[5:7]
    d.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    out.insert(0, "run_id", run_id)
    p = d / f"{run_id}.parquet"
    out.to_parquet(p, index=False)          # overwrite: checkpoints supersede each other
    if not quiet:
        print(f"  wrote {dataset}: {len(out):,} rows -> {p.relative_to(DATA)}", flush=True)
    return p


def main() -> int:
    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%MZ")
    print(f"DataForge ephemeral run {run_id}", flush=True)
    print(f"  duration {DURATION_S/3600:.2f}h | mempool poll {POLL_S}s | "
          f"divergence every {DIVERGENCE_EVERY_S/60:.0f}min\n", flush=True)

    tracker = e1_mempool.MempoolTracker(e1_mempool.ENDPOINTS[0])
    div_rows: list[pd.DataFrame] = []
    t0 = time.time()
    last_block = 0.0
    last_div = 0.0
    last_ckpt = 0.0
    failures: list[str] = []

    try:
        while time.time() - t0 < DURATION_S:
            tracker.poll_pending()
            now = time.time()
            if now - last_block >= 12:            # blocks arrive ~12s apart
                tracker.poll_blocks()
                last_block = now
            if now - last_div >= DIVERGENCE_EVERY_S:
                try:
                    div_rows.append(e3_divergence.sample())
                except Exception as exc:
                    failures.append(f"e3: {exc}")
                last_div = now
            if now - last_ckpt >= CHECKPOINT_EVERY_S and tracker.seen:
                # Partial write. fate is "unresolved" for anything not yet mined, because
                # reconciliation has not run -- never claim "dropped" without the evidence.
                write(e1_mempool.DATASET, tracker.frame(set()), run_id, quiet=True)
                if div_rows:
                    write(e3_divergence.DATASET, pd.concat(div_rows, ignore_index=True),
                          run_id, quiet=True)
                last_ckpt = now
            if tracker.n_poll % 240 == 0 and tracker.n_poll:
                print(f"    {(now-t0)/60:6.1f} min  seen {len(tracker.seen):>8,}  "
                      f"mined {len(tracker.mined):>8,}  failed {tracker.n_failed:>4}  "
                      f"resets {tracker.n_filter_resets}", flush=True)
            time.sleep(POLL_S)
    except KeyboardInterrupt:
        print("\n  interrupted -- reconciling what was collected", flush=True)

    # Final catch-up and reconciliation. Anything seen, never mined, and absent from the pool is
    # genuinely DROPPED -- rows that exist nowhere else.
    try:
        tracker.poll_blocks()
        still = tracker.reconcile()
    except Exception as exc:
        failures.append(f"reconcile: {exc}")
        still = set()

    df = tracker.frame(still)
    print(f"\n  E1: {len(df):,} txs | " +
          " ".join(f"{k} {v:,}" for k, v in df["fate"].value_counts().items()) +
          f" | polls {tracker.n_poll:,} failed {tracker.n_failed} "
          f"resets {tracker.n_filter_resets}", flush=True)
    write(e1_mempool.DATASET, df, run_id)
    if div_rows:
        write(e3_divergence.DATASET, pd.concat(div_rows, ignore_index=True), run_id)

    # Run-level telemetry, so a degraded run is visible without opening the data.
    write("e0_run_manifest", pd.DataFrame([{
        "started_utc": datetime.fromtimestamp(t0, timezone.utc).isoformat(timespec="seconds"),
        "duration_s": round(time.time() - t0, 1),
        "n_polls": tracker.n_poll, "n_failed_calls": tracker.n_failed,
        "n_filter_resets": tracker.n_filter_resets,
        "n_tx_seen": len(df), "n_divergence_samples": len(div_rows),
        "reconciled": bool(still), "failures": "; ".join(failures)[:500],
    }]), run_id)

    if failures:
        print("\nFAILURES:", flush=True)
        for f in failures:
            print("  " + f, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
