"""Build the two products: a FREE rolling sample and a PAID dated snapshot.

WHY DATED SNAPSHOTS AND NOT A SUBSCRIPTION. A subscription is a promise of continuity, and a
logger that breaks while nobody is watching turns that promise into a refund obligation. A dated
snapshot -- "history through 2027-03-01" -- turns the same gap into a disclosed limitation. Given
this runs unattended on best-effort cron, that difference matters more than the pricing model.

WHY THE FREE SAMPLE IS NOT CHARITY. An unknown dataset sells zero copies at any price. The rolling
window is the only distribution channel available: it proves the series exists, is current, and is
shaped as described, while withholding the one thing that cannot be reproduced -- the accumulated
history.

Every bundle ships with a DATA DICTIONARY and, more unusually, a KNOWN-ARTIFACTS note. The
artifacts are the part a competitor cannot copy by pointing a script at an RPC endpoint.
"""
from __future__ import annotations

import json
import shutil
import sys
import zipfile
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
DIST = ROOT / "dist"

DATASETS = {
    "a1_lending_market_state": "Daily lending-market state (Morpho Blue): supply/borrow assets and "
                               "shares, utilisation, fee, last update. Read from `market(bytes32)` "
                               "at one pinned block per chain per day.",
    "a2_vault_state": "Daily ERC-4626 vault state: convertToAssets(1e18), totalAssets, totalSupply, "
                      "fee. Read from contract state, not reconstructed from events.",
    "b2_stuck_markets": "Derived: markets pinned at >=99% utilisation, with day-over-day supply and "
                        "share-price growth and the phantom signature flag.",
    "b5_dormancy": "Derived: vaults whose share price did not move since the previous snapshot, "
                   "distinguishing dormant from unobserved.",
}

ARTIFACTS = """KNOWN ARTIFACTS AND HOW TO HANDLE THEM
=======================================
These are measured properties of the underlying data, not defects in the collection. They are
documented because each one has silently corrupted a real analysis.

1. DORMANCY IS NOT A ZERO RETURN.
   A vault whose share price has not moved is not earning zero -- it is not reporting. Roughly
   27-33% of ever-funded vaults are dormant at any time. Averaging over them drags every yield
   statistic toward zero. Use b5_dormancy to exclude them, and note the tri-state: dormant, moving,
   or unobserved (no prior reading).

2. STUCK MARKETS INFLATE REPORTED ASSETS WITHOUT BOUND.
   A market at ~100% utilisation accrues interest on debt nobody repays, so totalSupplyAssets grows
   forever while nothing is withdrawable. Because the share price RISES, a bad-debt detector that
   looks for a price DECLINE cannot see it. One such market reports supply roughly 4x its
   protocol's entire chain TVL. Use b2_stuck_markets to flag them.

3. SHARE PRICE FROM EVENTS DIVERGES FROM CONTRACT STATE.
   Deposit/Withdraw events carry assets and shares, so share price looks reconstructable, and the
   reconstruction agrees with contract state to ~0.0005% median error -- FOR VAULTS THAT TRANSACT.
   On a vault with no flows for 270 days the two diverged by 170x. This dataset reads state, which
   is the correct quantity.

4. UINT128/UINT256 EXCEEDS INT64.
   Raw amounts are stored as float64 at the storage boundary. For exact integer arithmetic, read
   the raw values as Python ints before any pandas operation.

5. AMOUNTS ARE IN RAW TOKEN UNITS.
   Decimals vary per loan token, spanning ~25 orders of magnitude across a single chain. Ratios
   WITHIN a market or vault are decimal-free and safe; sums or comparisons ACROSS them are not.
   Join to the universe file for the loan token, then apply its decimals.

6. UTILISATION IS EXACT; APY IS NOT SUPPLIED.
   Utilisation is totalBorrowAssets/totalSupplyAssets from state. No APY field is provided, because
   an annualised rate requires a modelling choice (spot vs realised) that has repeatedly been the
   source of error. Compute it from consecutive share prices, which are exact.
"""


def _partitions(dataset: str) -> list[Path]:
    root = DATA / dataset
    return sorted(root.rglob("*.parquet")) if root.exists() else []


def _bundle(name: str, since: date | None, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = {}
    for ds in DATASETS:
        files = _partitions(ds)
        if since is not None:
            files = [f for f in files if f.stem >= since.isoformat()]
        if not files:
            continue
        frames = [pd.read_parquet(f) for f in files]
        df = pd.concat(frames, ignore_index=True)
        df.to_parquet(out_dir / f"{ds}.parquet", index=False)
        summary[ds] = {"rows": len(df), "days": df["snapshot_date"].nunique(),
                       "first": df["snapshot_date"].min(), "last": df["snapshot_date"].max(),
                       "chains": sorted(df["chain"].unique())}
    for u in (DATA / "universe").glob("*.parquet"):
        shutil.copy(u, out_dir / f"universe_{u.name}")
    (out_dir / "ARTIFACTS.txt").write_text(ARTIFACTS, encoding="utf-8")
    (out_dir / "DATA_DICTIONARY.json").write_text(
        json.dumps({"datasets": DATASETS, "coverage": summary}, indent=2), encoding="utf-8")
    return summary


def build_free_sample(days: int = 30) -> Path:
    """Rolling window: proves the series is live, withholds the accumulated history."""
    out = DIST / "free_sample"
    if out.exists():
        shutil.rmtree(out)
    s = _bundle("free_sample", date.today() - timedelta(days=days), out)
    print(f"free sample ({days}d): " + ", ".join(f"{k} {v['rows']:,} rows" for k, v in s.items()))
    return out


def build_full_snapshot() -> Path:
    """Dated full-history bundle -- the paid product."""
    tag = date.today().isoformat()
    out = DIST / f"snapshot_{tag}"
    if out.exists():
        shutil.rmtree(out)
    s = _bundle("full", None, out)
    zpath = DIST / f"dataforge_snapshot_{tag}.zip"
    with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as z:
        for f in out.rglob("*"):
            if f.is_file():
                z.write(f, f.relative_to(out))
    print(f"full snapshot -> {zpath.name} ({zpath.stat().st_size/1e6:.1f} MB)")
    for k, v in s.items():
        print(f"  {k:<26} {v['rows']:>8,} rows  {v['days']:>4} days  {v['first']} .. {v['last']}")
    return zpath


if __name__ == "__main__":
    build_free_sample()
    build_full_snapshot()
