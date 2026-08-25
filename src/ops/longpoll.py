"""The resident ephemeral collector. Runs ~5.5h per invocation, four times a day.

WHY RESIDENT RATHER THAN CRON. GitHub cron fires at most every 5 minutes and scheduled runs are
best-effort. A sparse snapshot cannot see LIFECYCLE -- how long a transaction sat pending, or that
it was dropped and never mined -- and lifecycle is precisely the part no archive can reconstruct.
Actions caps a job at 6 hours, so 5.5h leaves margin, and four staggered starts give ~22h/day of
coverage.

TWO CHAINS, DIFFERENT CLOCKS. Ethereum blocks arrive every ~12s and its mempool turns over in
seconds, so E1/E3 poll hard (5s). Bitcoin blocks arrive every ~10 min, a full mempool read is
3.1 MB, and the measured churn is ~327 new transactions/min, so E8/E9 poll every 60s. Running both
in one process costs nothing extra -- the loop is almost entirely idle waiting on the network.

HONEST NOTE ON WHAT IS WORTH SELLING. E1/E3 (Ethereum) are ALSO published free and CC-0 by the
Flashbots Mempool Dumpster, with a longer history and a wider node network than ours. They are kept
because they cost nothing and are a genuine independent measurement, but they are NOT the product.
E8/E9 (Bitcoin) are the part with no free continuously-updated equivalent found.

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
from src.ephemeral import e1_mempool, e3_divergence, e8_btc_mempool, e10_quotes

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"

DURATION_S = int(os.environ.get("DF_DURATION_S", 5 * 3600 + 1800))   # 5.5h
POLL_S = float(os.environ.get("DF_POLL_S", 5))
DIVERGENCE_EVERY_S = float(os.environ.get("DF_DIVERGENCE_EVERY_S", 300))
# Bitcoin cadences. A full txid read is 3.1 MB gzipped, so 60s keeps the job's download to about
# 1 GB per 5.5h window while still resolving the ~327 new transactions/min that actually arrive.
BTC_POLL_S = float(os.environ.get("DF_BTC_POLL_S", 60))
BTC_BLOCK_EVERY_S = float(os.environ.get("DF_BTC_BLOCK_EVERY_S", 120))
BTC_DIVERGENCE_EVERY_S = float(os.environ.get("DF_BTC_DIVERGENCE_EVERY_S", 900))
# E10 quote benchmark. One round is 24 requests against free public endpoints, so 15 minutes keeps
# us at ordinary-client volume (~2,300/day) rather than hammering somebody's free service.
QUOTE_EVERY_S = float(os.environ.get("DF_QUOTE_EVERY_S", 900))
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


def _checkpoint(run_id: str, tracker, div_rows, btc, btc_div_rows, quote_rows=()) -> None:
    """Partial write of everything held in memory. For E1 the fate of anything not yet mined is
    'unresolved', because reconciliation has not run -- never claim 'dropped' without evidence."""
    if tracker.seen:
        write(e1_mempool.DATASET, tracker.frame(set()), run_id, quiet=True)
    if div_rows:
        write(e3_divergence.DATASET, pd.concat(div_rows, ignore_index=True), run_id, quiet=True)
    if btc.seen or btc.pre_existing:
        write(e8_btc_mempool.DATASET, btc.frame(), run_id, quiet=True)
    if btc_div_rows:
        write(e8_btc_mempool.DIVERGENCE_DATASET,
              pd.concat(btc_div_rows, ignore_index=True), run_id, quiet=True)
    if len(quote_rows):
        write(e10_quotes.DATASET, pd.concat(quote_rows, ignore_index=True), run_id, quiet=True)


def main() -> int:
    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%MZ")
    print(f"DataForge ephemeral run {run_id}", flush=True)
    print(f"  duration {DURATION_S/3600:.2f}h | ETH poll {POLL_S}s | BTC poll {BTC_POLL_S}s\n",
          flush=True)

    tracker = e1_mempool.MempoolTracker(e1_mempool.ENDPOINTS[0])
    btc = e8_btc_mempool.BtcMempoolTracker()
    div_rows: list[pd.DataFrame] = []
    btc_div_rows: list[pd.DataFrame] = []
    t0 = time.time()
    last_block = last_div = last_ckpt = 0.0
    last_btc = last_btc_block = last_btc_div = 0.0
    last_quote = 0.0
    quote_rows: list[pd.DataFrame] = []
    failures: list[str] = []

    try:
        while time.time() - t0 < DURATION_S:
            tracker.poll_pending()
            now = time.time()

            if now - last_block >= 12:            # ETH blocks arrive ~12s apart
                tracker.poll_blocks()
                last_block = now
            if now - last_div >= DIVERGENCE_EVERY_S:
                try:
                    div_rows.append(e3_divergence.sample())
                except Exception as exc:
                    failures.append(f"e3: {exc}")
                last_div = now

            # ---- Bitcoin, on its own much slower clock ----
            if now - last_btc >= BTC_POLL_S:
                try:
                    btc.poll_pool()
                except Exception as exc:
                    failures.append(f"e8 pool: {exc}")
                last_btc = now
            if now - last_btc_block >= BTC_BLOCK_EVERY_S:
                try:
                    btc.poll_blocks()
                except Exception as exc:
                    failures.append(f"e8 blocks: {exc}")
                last_btc_block = now
                # Refresh the cross-check pool in bulk, then verify. The bulk read settles far
                # more candidates per request than individual status checks ever could.
                try:
                    btc.verify_dropped(cap=100)
                except Exception as exc:
                    failures.append(f"e8 verify: {exc}")
            if now - last_btc_div >= BTC_DIVERGENCE_EVERY_S:
                try:
                    btc.refresh_other()
                    btc_div_rows.append(e8_btc_mempool.divergence())
                except Exception as exc:
                    failures.append(f"e9: {exc}")
                last_btc_div = now

            if now - last_quote >= QUOTE_EVERY_S:
                try:
                    quote_rows.append(e10_quotes.sample())
                except Exception as exc:
                    failures.append(f"e10: {exc}")
                last_quote = now

            if now - last_ckpt >= CHECKPOINT_EVERY_S:
                _checkpoint(run_id, tracker, div_rows, btc, btc_div_rows, quote_rows)
                last_ckpt = now
            if tracker.n_poll % 240 == 0 and tracker.n_poll:
                print(f"    {(now-t0)/60:6.1f} min  ETH seen {len(tracker.seen):>8,} "
                      f"mined {len(tracker.mined):>8,} failed {tracker.n_failed:>4}  |  "
                      f"BTC seen {len(btc.seen):>7,} mined {len(btc.mined):>7,} "
                      f"polls {btc.n_poll:>4} failed {btc.n_failed}", flush=True)
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
    try:
        btc.poll_pool()
        btc.poll_blocks()
        # Authoritative check on every drop candidate. A set-difference alone manufactured 642
        # fake drops in a 200s test; all 12 sampled were still pending.
        btc.refresh_other()
        btc.verify_dropped()
    except Exception as exc:
        failures.append(f"e8 final: {exc}")

    df = tracker.frame(still)
    # An empty frame has no columns at all, so summarising it must not assume `fate` exists.
    # A collector that gathered nothing is a FAILURE TO REPORT -- never an exception that
    # discards the three other datasets successfully collected in the same run. Hit for real:
    # a transiently rate-limited Ethereum endpoint took down an otherwise complete run.
    fates = (" ".join(f"{k} {v:,}" for k, v in df["fate"].value_counts().items())
             if len(df) else "NOTHING COLLECTED")
    print(f"\n  E1: {len(df):,} txs | {fates} | polls {tracker.n_poll:,} "
          f"failed {tracker.n_failed} resets {tracker.n_filter_resets}", flush=True)
    if not len(df):
        failures.append("e1: collected zero transactions -- endpoint or filter failure")
    write(e1_mempool.DATASET, df, run_id)
    if div_rows:
        write(e3_divergence.DATASET, pd.concat(div_rows, ignore_index=True), run_id)

    bdf = btc.frame()
    if len(bdf):
        print(f"  E8: {len(bdf):,} txs | " +
              " ".join(f"{k} {v:,}" for k, v in bdf["fate"].value_counts().items()) +
              f" | pre_existing {int(bdf['pre_existing'].sum()):,} "
              f"polls {btc.n_poll:,} failed {btc.n_failed}", flush=True)
    write(e8_btc_mempool.DATASET, bdf, run_id)
    if btc_div_rows:
        write(e8_btc_mempool.DIVERGENCE_DATASET,
              pd.concat(btc_div_rows, ignore_index=True), run_id)

    if quote_rows:
        qdf = pd.concat(quote_rows, ignore_index=True)
        okq = qdf[qdf["price"].notna()]
        print(f"  E10: {len(qdf):,} quotes, {len(qdf)-len(okq)} failed | "
              f"median spread {okq['spread_bps'].median():.1f} bps", flush=True)
        write(e10_quotes.DATASET, qdf, run_id)

    # Run-level telemetry, so a degraded run is visible without opening the data.
    write("e0_run_manifest", pd.DataFrame([{
        "started_utc": datetime.fromtimestamp(t0, timezone.utc).isoformat(timespec="seconds"),
        "duration_s": round(time.time() - t0, 1),
        "n_polls": tracker.n_poll, "n_failed_calls": tracker.n_failed,
        "n_filter_resets": tracker.n_filter_resets,
        "n_tx_seen": len(df), "n_divergence_samples": len(div_rows),
        "btc_n_polls": btc.n_poll, "btc_n_failed": btc.n_failed,
        "btc_n_tx_seen": len(bdf), "btc_n_pre_existing": int(bdf["pre_existing"].sum())
                                    if len(bdf) else 0,
        "btc_n_divergence_samples": len(btc_div_rows),
        "btc_n_drop_verified": btc.n_verified, "btc_n_flicker": btc.n_flicker,
        "btc_n_crosscheck_saved": btc.n_other_saved,
        "n_quote_rounds": len(quote_rows),
        "reconciled": bool(still), "failures": "; ".join(failures)[:500],
    }]), run_id)

    if failures:
        print("\nFAILURES:", flush=True)
        for f in failures[:20]:
            print("  " + f, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
