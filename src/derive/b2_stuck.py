"""B2 -- the stuck-market registry. Derived from A1, zero extra RPC.

THE SIGNATURE, and why nobody else publishes it: a lending market pinned at ~100% utilisation
keeps accruing interest on debt nobody repays. Its reported `totalSupplyAssets` therefore grows
without bound while nothing is actually withdrawable -- and because the SHARE PRICE RISES, the
standard bad-debt detector (which looks for a share-price DECLINE) structurally cannot see it.
Family 56 established the blind spot; family 57 showed 85% of such markets have no liquid venue
for their collateral; family 152 confirmed the mechanism against a clean control (two suspect
vaults 100% exposed, two normal vaults 0.0%).

The scale is not marginal. Market 0f956344 (USDC/sdeUSD) reports supply and borrow EQUAL TO THE
DOLLAR at 100.0% utilisation, and its reported supply is roughly four times Morpho's entire
Ethereum TVL. Between two consecutive daily snapshots it grew by $143M of phantom assets.

Equal supply and borrow is the purest form of the signature: at 100% utilisation both sides accrue
identically and stay identical forever.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.ops.snapshot import write_partition

DATASET = "b2_stuck_markets"
SOURCE = "a1_lending_market_state"
DATA = Path(__file__).resolve().parents[2] / "data"
STUCK_UTIL = 0.99          # family 56's threshold, unchanged


def _load(chain: str, date: str) -> pd.DataFrame | None:
    p = DATA / SOURCE / chain / date[:4] / date[5:7] / f"{date}.parquet"
    return pd.read_parquet(p) if p.exists() else None


def _prev_date(chain: str, date: str) -> pd.DataFrame | None:
    """Most recent earlier partition, whatever its date -- robust to a missed day."""
    root = DATA / SOURCE / chain
    if not root.exists():
        return None
    files = sorted(root.rglob("*.parquet"))
    earlier = [f for f in files if f.stem < date]
    return pd.read_parquet(earlier[-1]) if earlier else None


def run(chain: str, block: int, ts: int, date: str, runlog) -> None:
    cur = _load(chain, date)
    if cur is None or not len(cur):
        raise FileNotFoundError(f"{SOURCE}/{chain}/{date} missing -- derive runs after A1")
    prev = _prev_date(chain, date)

    df = cur[["market_id", "total_supply_assets", "total_supply_shares",
              "total_borrow_assets", "utilisation"]].copy()
    df["supply_share_price"] = df["total_supply_assets"] / df["total_supply_shares"].replace(0, pd.NA)

    # EMPTINESS MUST BE SEPARATED FROM STUCKNESS, or the signature is swamped by dead markets.
    # Permissionless venues generate mostly empty markets: family 35 found 615 of 1,054 mainnet
    # markets were dust seeded once and never touched, and family 37 found only 984 of Base's
    # 4,229 markets were ever supplied at all. In an empty market supply == borrow == 0, which
    # satisfies naive equality, and utilisation is 0/0. On the first Base snapshot that alone
    # produced 3,760 apparent "supply == borrow" markets out of 4,231.
    df["is_empty"] = (df["total_supply_assets"].fillna(0) <= 0)
    df["supply_eq_borrow"] = ((df["total_supply_assets"] == df["total_borrow_assets"])
                              & ~df["is_empty"])
    df["is_stuck"] = (df["utilisation"] >= STUCK_UTIL) & ~df["is_empty"]

    if prev is not None and len(prev):
        pj = prev.set_index("market_id")
        df = df.join(pj["total_supply_assets"].rename("prev_supply"), on="market_id")
        df = df.join((pj["total_supply_assets"] / pj["total_supply_shares"].replace(0, pd.NA))
                     .rename("prev_share_price"), on="market_id")
        # ZERO DENOMINATORS ARE NOT GROWTH. A market that held nothing yesterday and holds
        # something today has an UNDEFINED growth rate, not an infinite one -- and the columns are
        # object-dtype Python ints at this point (they are cast to float64 further down), so the
        # division RAISES rather than yielding inf. That is what took the daily job down on both
        # chains at once after passing for days: it is data-dependent, needing only one market to
        # be funded for the first time. Same class as the dormancy rule used throughout this
        # project -- an unobservable quantity must be NULL, never a number.
        df["supply_growth"] = (df["total_supply_assets"]
                               / df["prev_supply"].replace(0, pd.NA)) - 1.0
        df["share_price_growth"] = (df["supply_share_price"]
                                    / df["prev_share_price"].replace(0, pd.NA)) - 1.0
        # The full family 56 signature: pinned utilisation AND a rising price. The rise is what
        # makes it invisible to a decline-based detector.
        df["phantom_signature"] = df["is_stuck"] & (df["share_price_growth"] > 0)
    else:
        for c in ["prev_supply", "prev_share_price", "supply_growth",
                  "share_price_growth", "phantom_signature"]:
            df[c] = pd.NA

    for c in ["total_supply_assets", "total_supply_shares", "total_borrow_assets",
              "prev_supply"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")

    # TEMPLATE COHORTS. Base reported 779 stuck markets (63.6% of non-empty) against Ethereum's
    # 9.9% and family 56's published 10.6%. The 6x gap is not a chain difference: those 779 hold
    # only 43 DISTINCT supply values, with 598 sharing 361,000,444,898 to the digit and 100 more
    # sharing another. They are mass-deployed markets from a couple of templates, all seeded with
    # an identical amount and fully borrowed by the seeder -- one situation replicated, not 779
    # independent ones. Counting them singly overstates the signal ~18x.
    #
    # The cohort SIZE is recorded rather than a threshold flag, because where to cut is the
    # user's judgement, not ours. Family 30 hit the same shape: 13,109 byte-identical contracts
    # behind what looked like a population.
    cohort = df.groupby("total_supply_assets")["market_id"].transform("size")
    df["supply_cohort_size"] = cohort.where(~df["is_empty"], pd.NA)
    df["is_stuck_distinct"] = df["is_stuck"] & (cohort == 1)

    n_stuck = int(df["is_stuck"].sum())
    n_distinct = int(df["total_supply_assets"][df["is_stuck"]].nunique())
    n_live = int((~df["is_empty"]).sum())
    write_partition(DATASET, chain, date, df, block, ts)
    runlog.record(DATASET, chain, date, block, ts, len(df), 0, 0,
                  note=f"{n_stuck} stuck of {n_live} non-empty "
                       f"({100*n_stuck/max(n_live,1):.1f}%), {n_distinct} distinct supply values, "
                       f"{int(df['is_empty'].sum())} empty")
