"""Were the fee estimators right? The study our two panels uniquely support.

THE QUESTION NOBODY CAN CURRENTLY ANSWER. Five providers publish a number meaning "pay this to
confirm within N blocks". Whether that number was correct requires knowing, for transactions that
actually paid around it, whether they confirmed in time, which needs the mempool lifecycle AND
the estimates on the same clock. Providers publish neither their own history nor anyone's
outcomes. e15 holds the promises, e8 holds what happened, and this joins them.

METHOD, deliberately simple because the data is young:
    for each estimate (provider, target N blocks, quoted rate X) at time T
        take transactions FIRST SEEN in [T, T + MATCH_WINDOW_S] whose fee rate is within BAND of X
        ask what share of them confirmed within N blocks of the chain tip at their arrival

TWO FAILURE MODES, both real, both reported rather than collapsed into one score:
    OVER-ESTIMATION   the promise is kept and the user overpaid, versus a cheaper provider whose
                      promise was ALSO kept. Shows as a high hit rate at a high quoted rate.
    UNDER-ESTIMATION  the promise is broken. Shows as a low hit rate.
Only reading hit rate and quoted rate TOGETHER separates them, so both always appear.

WHAT THIS IS NOT: a claim about any provider's long-run accuracy. Confirmation depends on the
whole mempool, and a band of transactions near a rate is a proxy for "a user who followed that
advice", not a controlled experiment. Cell sizes are printed everywhere precisely so a thin one
cannot be quoted as a result.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"

MATCH_WINDOW_S = 600.0     # a transaction counts as "following" an estimate within 10 minutes
BAND = 0.25                # fee rate within +/-25% of the quoted rate
MIN_CELL = 20              # below this a cell is shown but never summarised


def _load(ds: str) -> pd.DataFrame:
    root = DATA / ds
    fs = sorted(root.rglob("*.parquet")) if root.exists() else []
    if not fs:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in fs], ignore_index=True)


def build() -> pd.DataFrame:
    est = _load("e15_fee_estimators")
    tx = _load("e8_btc_mempool_lifecycle")
    if not len(est) or not len(tx):
        print("  need both e15 and e8 partitions on disk", flush=True)
        return pd.DataFrame()

    est = est[est.sat_per_vb.notna() & est.target_blocks.notna()].copy()
    tx = tx[tx.first_seen_ts.notna()].copy()
    if "fee_rate_sat_vb" not in tx.columns:
        print("  e8 partitions predate fee capture; nothing to join", flush=True)
        return pd.DataFrame()
    tx = tx[tx.fee_rate_sat_vb.notna()].copy()
    if "blocks_waited" not in tx.columns:
        tx["blocks_waited"] = pd.NA
    print(f"  estimates {len(est):,} | transactions with arrival + fee {len(tx):,}", flush=True)
    if not len(tx):
        return pd.DataFrame()

    rows = []
    for e in est.itertuples():
        lo, hi = e.sat_per_vb * (1 - BAND), e.sat_per_vb * (1 + BAND)
        w = tx[(tx.first_seen_ts >= e.sampled_ts)
               & (tx.first_seen_ts <= e.sampled_ts + MATCH_WINDOW_S)
               & (tx.fee_rate_sat_vb >= lo) & (tx.fee_rate_sat_vb <= hi)]
        if not len(w):
            continue
        # Exact where the tip was recorded at arrival; otherwise fall back to elapsed time at ten
        # minutes a block, flagged so the two bases are never silently mixed.
        exact = w[w.blocks_waited.notna()]
        if len(exact):
            hit = (pd.to_numeric(exact.blocks_waited, errors="coerce")
                   <= e.target_blocks).mean()
            n, basis = len(exact), "blocks"
        else:
            dw = w[w.dwell_seconds_local.notna()]
            if not len(dw):
                continue
            hit = (dw.dwell_seconds_local <= e.target_blocks * 600).mean()
            n, basis = len(dw), "time_approx"
        rows.append({"sampled_ts": e.sampled_ts, "provider": e.provider,
                     "target_blocks": e.target_blocks, "quoted_sat_vb": e.sat_per_vb,
                     "n_matched": int(n), "hit_rate": round(float(hit), 4), "basis": basis})
    return pd.DataFrame(rows)


def report(df: pd.DataFrame) -> None:
    if not len(df):
        print("  no matched cells yet: e15 and e8 need overlapping history", flush=True)
        return
    print(f"\n  matched cells {len(df):,}  (basis {df.basis.value_counts().to_dict()})")
    g = df.groupby(["provider", "target_blocks"]).agg(
        cells=("hit_rate", "size"), n=("n_matched", "sum"),
        quoted=("quoted_sat_vb", "mean"), hit=("hit_rate", "mean")).reset_index()
    print(f"\n  {'provider':<16}{'target':>7}{'cells':>7}{'n':>8}{'quoted':>9}{'hit':>7}   note")
    for r in g.sort_values(["target_blocks", "provider"]).itertuples():
        note = "" if r.n >= MIN_CELL else "THIN, not a result"
        print(f"  {r.provider:<16}{int(r.target_blocks):>7}{r.cells:>7}{r.n:>8}"
              f"{r.quoted:>9.2f}{r.hit:>6.0%}   {note}")
    solid = g[g.n >= MIN_CELL]
    print()
    if len(solid):
        print(f"  {len(solid)} of {len(g)} cells clear the {MIN_CELL}-transaction floor.")
        print("  Read hit rate WITH quoted rate: a high hit rate at a high quote is")
        print("  overpayment, not accuracy.")
    else:
        print(f"  Nothing clears the {MIN_CELL}-transaction floor yet. The instrument runs and")
        print("  is verified end to end; the finding needs more overlapping history.")


if __name__ == "__main__":
    d = build()
    report(d)
    if len(d):
        out = DATA / "derived_fee_accuracy"
        out.mkdir(parents=True, exist_ok=True)
        d.to_parquet(out / "fee_accuracy.parquet", index=False)
        print(f"\n  wrote {len(d):,} cells -> derived_fee_accuracy/fee_accuracy.parquet")
