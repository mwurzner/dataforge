"""A1 -- lending market state, daily, pinned.

WHAT IT CAPTURES that nothing free provides: `market(bytes32)` is a VIEW FUNCTION, not an event.
Dune and friends index Supply/Borrow/Repay/Withdraw events; reconstructing totalSupplyAssets from
those requires replaying the adaptive IRM's interest accrual, and family 56 demonstrated that path
diverges badly -- a market pinned at 100% utilisation accrues interest on debt nobody repays, so
its reported supply grows without bound while nothing is withdrawable. Family 152 found one such
market reporting $3.34B, roughly four times Morpho's entire Ethereum TVL. An event-based rebuild
does not see that; a state read does.

    Market struct = 6 x uint128, each ABI-padded to its own 32-byte word:
      totalSupplyAssets, totalSupplyShares, totalBorrowAssets, totalBorrowShares, lastUpdate, fee

Static market parameters (loan, collateral, oracle, irm, lltv) live in the universe file and are
deliberately NOT repeated here -- they never change, so repeating them daily would multiply the
dataset size for nothing.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.chain.multi import selector
from src.chain.rpc import Call, dec_uint
from src.ops.snapshot import write_partition

DATASET = "a1_lending_market_state"
MORPHO = "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb"
MKT = selector("market(bytes32)")
UNI = Path(__file__).resolve().parents[2] / "data" / "universe"


def market_ids(chain: str) -> list[str]:
    p = UNI / f"{chain}_markets.parquet"
    if not p.exists():
        raise FileNotFoundError(
            f"{p} missing -- run src.universe.discover first. Refusing to log against an empty "
            "universe, which would write a valid-looking partition describing nothing.")
    ids = pd.read_parquet(p)["id"].astype(str).str.lower().tolist()
    return [i if i.startswith("0x") else "0x" + i for i in ids]


def run(chain: str, rpc, block: int, ts: int, date: str, runlog) -> None:
    ids = market_ids(chain)
    r0, f0 = rpc.n_requests, rpc.n_failed
    rows = []
    for i in range(0, len(ids), 400):
        chunk = ids[i:i + 400]
        calls = [Call(MORPHO, MKT + m[2:].rjust(64, "0")) for m in chunk]
        out = rpc.batch_call(calls, block, strict=False)
        for mid, res in zip(chunk, out):
            if not res or res == "0x":
                continue          # market genuinely absent at this block -- not an error
            sa = dec_uint(res, 0)
            ss = dec_uint(res, 1)
            ba = dec_uint(res, 2)
            bs = dec_uint(res, 3)
            last = dec_uint(res, 4)
            fee = dec_uint(res, 5)
            if sa is None:
                continue
            rows.append({
                "market_id": mid,
                "total_supply_assets": sa, "total_supply_shares": ss,
                "total_borrow_assets": ba, "total_borrow_shares": bs,
                "last_update": last, "fee": fee,
                # Derived here because it is the single most-used field and is exact from state.
                "utilisation": (ba / sa) if sa else None,
            })
    df = pd.DataFrame(rows)
    # uint128 values exceed int64 -- cast at the storage boundary. SIXTH occurrence of this bug in
    # the research record; it is now a standing rule, not a discovery.
    for c in ["total_supply_assets", "total_supply_shares",
              "total_borrow_assets", "total_borrow_shares"]:
        df[c] = df[c].astype("float64")
    write_partition(DATASET, chain, date, df, block, ts)
    runlog.record(DATASET, chain, date, block, ts, len(df),
                  rpc.n_requests - r0, rpc.n_failed - f0,
                  note=f"{len(ids):,} in universe")
