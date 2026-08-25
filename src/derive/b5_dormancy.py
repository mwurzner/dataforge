"""B5 -- the dormancy registry. Derived from A2, zero extra RPC.

A vault whose share price has not moved is NOT earning zero -- it is NOT REPORTING, which is a
different fact entirely. Treating the two as the same corrupted the first pass of six separate
studies in this project's record (families 35, 38, 43, 53c, 55, 74), and family 82 measured the
scale: **27-33% of ever-funded vaults are dormant**, holding $1.82B on mainnet alone. Nobody
publishes that number, and it is exactly the correction a researcher needs before computing any
average yield.

Dormancy is defined against the PREVIOUS SNAPSHOT, not against zero, and the gap in days is
recorded so a missed run cannot masquerade as dormancy.
"""
from __future__ import annotations

import sys
from datetime import date as _date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.ops.snapshot import write_partition

DATASET = "b5_dormancy"
SOURCE = "a2_vault_state"
DATA = Path(__file__).resolve().parents[2] / "data"


def _files(chain: str):
    root = DATA / SOURCE / chain
    return sorted(root.rglob("*.parquet")) if root.exists() else []


def run(chain: str, block: int, ts: int, date: str, runlog) -> None:
    p = DATA / SOURCE / chain / date[:4] / date[5:7] / f"{date}.parquet"
    if not p.exists():
        raise FileNotFoundError(f"{SOURCE}/{chain}/{date} missing -- derive runs after A2")
    cur = pd.read_parquet(p)
    earlier = [f for f in _files(chain) if f.stem < date]

    df = cur[["vault", "asset", "share_price_1e18", "total_assets"]].copy()
    if earlier:
        prev_file = earlier[-1]
        prev = pd.read_parquet(prev_file).set_index("vault")
        gap = (_date.fromisoformat(date) - _date.fromisoformat(prev_file.stem)).days
        df = df.join(prev["share_price_1e18"].rename("prev_price"), on="vault")
        df = df.join(prev["total_assets"].rename("prev_assets"), on="vault")
        df["days_since_prev"] = gap
        df["price_moved"] = df["share_price_1e18"] != df["prev_price"]
        # Explicitly tri-state: True / False / NA-for-new-vault. A vault with no prior reading is
        # NOT dormant, it is unobserved -- the distinction the six corrupted studies collapsed.
        df["is_dormant"] = df["price_moved"].where(df["prev_price"].notna()).map(
            {True: False, False: True})
        df["assets_moved"] = df["total_assets"] != df["prev_assets"]
    else:
        for c in ["prev_price", "prev_assets", "price_moved", "is_dormant", "assets_moved"]:
            df[c] = pd.NA
        df["days_since_prev"] = pd.NA

    for c in ["share_price_1e18", "total_assets", "prev_price", "prev_assets"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")

    n_d = int(df["is_dormant"].sum()) if df["is_dormant"].notna().any() else 0
    n_obs = int(df["is_dormant"].notna().sum())
    write_partition(DATASET, chain, date, df, block, ts)
    runlog.record(DATASET, chain, date, block, ts, len(df), 0, 0,
                  note=(f"{n_d}/{n_obs} dormant ({100*n_d/max(n_obs,1):.1f}%)"
                        if n_obs else "first snapshot -- no prior to compare"))
