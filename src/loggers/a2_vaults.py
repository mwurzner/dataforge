"""A2 -- ERC-4626 vault state, daily, pinned.

THIS IS THE DATASET WITH THE STRONGEST PROOF THAT IT CANNOT BE SUBSTITUTED.
A buyer could try to reconstruct share prices from Deposit/Withdraw events, which carry both
assets and shares. Family 81 validated that flow-derived price against `convertToAssets` at a
median error of 0.00046% -- so the substitution looks safe. Family 151 then found the condition it
depends on: it holds only for vaults that ACTUALLY TRANSACT. On adpUSDC the flow-implied price ran
1.000 -> 1.030 while `convertToAssets` reported 173.94 -- a 170x divergence -- because the vault
recorded ZERO deposits or withdrawals across 270 days while its reported price rose 165x.
An event-based rebuild is therefore not merely approximate; on exactly the vaults that matter most
it is wrong by two orders of magnitude. Only the state read is correct.

Four reads per vault: convertToAssets(1e18), totalAssets(), totalSupply(), fee().
`fee` is absent on non-MetaMorpho 4626s and simply comes back None -- a revert is information, and
`RPC.call` surfaces it as None rather than raising (see rpc.py's _is_revert).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.chain.multi import selector
from src.chain.rpc import Call, dec_uint
from src.ops.snapshot import write_partition

DATASET = "a2_vault_state"
CTA = selector("convertToAssets(uint256)") + f"{10**18:064x}"
TA = selector("totalAssets()")
TS = selector("totalSupply()")
FEE = selector("fee()")
UNI = Path(__file__).resolve().parents[2] / "data" / "universe"


def vault_rows(chain: str) -> pd.DataFrame:
    p = UNI / f"{chain}_vaults.parquet"
    if not p.exists():
        raise FileNotFoundError(
            f"{p} missing -- run src.universe.discover first. Refusing to log against an empty "
            "universe (families 70/81/86/93: an empty result must never look like data).")
    return pd.read_parquet(p)


def run(chain: str, rpc, block: int, ts: int, date: str, runlog) -> None:
    uni = vault_rows(chain)
    vaults = uni["vault"].astype(str).str.lower().tolist()
    asset_of = dict(zip(uni["vault"].astype(str).str.lower(), uni.get("asset", "")))
    r0, f0 = rpc.n_requests, rpc.n_failed

    rows, dead = [], 0
    for i in range(0, len(vaults), 250):
        chunk = vaults[i:i + 250]
        px = rpc.batch_call([Call(v, CTA) for v in chunk], block, strict=False)
        ta = rpc.batch_call([Call(v, TA) for v in chunk], block, strict=False)
        tsup = rpc.batch_call([Call(v, TS) for v in chunk], block, strict=False)
        fee = rpc.batch_call([Call(v, FEE) for v in chunk], block, strict=False)
        for v, p_, a_, s_, f_ in zip(chunk, px, ta, tsup, fee):
            pv = dec_uint(p_)
            if pv is None:
                dead += 1        # not yet deployed at this block, or not a live 4626
                continue
            rows.append({
                "vault": v,
                "asset": asset_of.get(v),
                "share_price_1e18": pv,      # convertToAssets(1e18), in ASSET units
                "total_assets": dec_uint(a_),
                "total_supply": dec_uint(s_),
                "fee": dec_uint(f_),         # None on non-MetaMorpho vaults; a revert is data
            })
    df = pd.DataFrame(rows)
    # uint256 exceeds int64 -- cast at the storage boundary (standing rule, 6 prior occurrences).
    for c in ["share_price_1e18", "total_assets", "total_supply", "fee"]:
        df[c] = df[c].astype("float64")
    write_partition(DATASET, chain, date, df, block, ts)
    runlog.record(DATASET, chain, date, block, ts, len(df),
                  rpc.n_requests - r0, rpc.n_failed - f0,
                  note=f"{dead} unreadable of {len(vaults):,}")
