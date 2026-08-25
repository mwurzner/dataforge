"""Universe discovery -- what exists to be measured. Runs WEEKLY, not daily.

THE ROT THIS PREVENTS IS THE MOST LIKELY FAILURE MODE OF THE WHOLE PROJECT. A logger pointed at a
stale universe keeps succeeding while silently measuring a shrinking sample, and nothing in the
output looks wrong. This project's own record has the cautionary case: family 140 recorded Morpho
as deployed on 2 chains when family 148's live census found ELEVEN. A universe that is never
refreshed is a dataset that quietly stops being what it claims.

TWO DISCOVERY ROUTES, each chosen for a measured reason:
  markets -- `CreateMarket` logs from Morpho Blue. One shared topic, so a single unfiltered sweep
             finds every market on the chain (family 44's 170x trick).
  vaults  -- the shared ERC-4626 `Deposit` topic, then VERIFY each contract answers `asset()` and
             `convertToAssets()`. Family 34 established the verification step is required: the
             `Deposit` signature collides with non-4626 contracts. This route finds ALL 4626
             vaults, not only MetaMorpho ones -- family 147 showed a symbol-matched shortlist
             missed 13 of 17 vaults because it was built from a 3-day window.

BOOTSTRAP: first run seeds from this project's existing research files rather than re-sweeping
years of history it already paid for; thereafter it is incremental from a per-chain watermark.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.chain.chains import CHAINS
from src.chain.multi import get_logs_chunked, rpc_for, selector, topic
from src.chain.rpc import Call, dec_address

ROOT = Path(__file__).resolve().parents[2]
UNI = ROOT / "data" / "universe"
SEED = Path(r"c:\Users\mwurz\Desktop\Personal\OracleRisk\data")

MORPHO = "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb"   # same address on Ethereum and Base
CREATE_MARKET = topic("CreateMarket(bytes32,(address,address,address,address,uint256))")
DEPOSIT = topic("Deposit(address,address,uint256,uint256)")
ASSET = selector("asset()")
CTA = selector("convertToAssets(uint256)") + f"{10**18:064x}"

SEED_MARKETS = {"ethereum": "markets.parquet", "base": "base_markets.parquet"}


def _wm_path(chain: str) -> Path:
    UNI.mkdir(parents=True, exist_ok=True)
    return UNI / f"{chain}_watermark.json"


def _load_wm(chain: str) -> dict:
    p = _wm_path(chain)
    return json.loads(p.read_text()) if p.exists() else {}


def _save_wm(chain: str, wm: dict) -> None:
    _wm_path(chain).write_text(json.dumps(wm, indent=1))


def bootstrap_markets(chain: str) -> pd.DataFrame | None:
    """Seed from prior research output so we do not re-sweep history already paid for."""
    src = SEED / SEED_MARKETS.get(chain, "")
    if not src.exists():
        return None
    df = pd.read_parquet(src)
    keep = [c for c in ["id", "loan", "collateral", "oracle", "irm", "lltv", "created"]
            if c in df.columns]
    out = df[keep].copy()
    out["id"] = out["id"].astype(str).str.lower()
    return out


def discover_markets(chain: str, rpc, tip: int) -> pd.DataFrame:
    path = UNI / f"{chain}_markets.parquet"
    wm = _load_wm(chain)
    have = pd.read_parquet(path) if path.exists() else None
    if have is None:
        have = bootstrap_markets(chain)
        if have is not None:
            start = int(have["created"].max()) + 1 if "created" in have.columns else 0
            print(f"  [{chain}] bootstrapped {len(have):,} markets from research seed, "
                  f"resuming sweep at block {start:,}", flush=True)
        else:
            have, start = pd.DataFrame(columns=["id"]), 0
    else:
        start = int(wm.get("markets_block", 0)) + 1

    cap = CHAINS[chain].getlogs_cap or 10000
    # get_logs_chunked RAISES on an unrecoverable provider error rather than returning short,
    # so there is no silent-loss path to count -- a failure aborts the refresh and the watermark
    # is left untouched, which is the correct behaviour for a universe file.
    new = []
    if start and start < tip:
        logs = get_logs_chunked(rpc, start, tip, cap, address=MORPHO, topics=[CREATE_MARKET])
        for l in logs:
            t = l.get("topics", [])
            if len(t) > 1:
                new.append({"id": t[1].lower(), "created": int(l["blockNumber"], 16)})
    if new:
        add = pd.DataFrame(new).drop_duplicates("id")
        have = pd.concat([have, add[~add["id"].isin(set(have["id"]))]], ignore_index=True)
    have = have.drop_duplicates("id")
    have.to_parquet(path, index=False)
    wm["markets_block"] = tip
    _save_wm(chain, wm)
    print(f"  [{chain}] markets: {len(have):,} total (+{len(new)} new)", flush=True)
    return have


def discover_vaults(chain: str, logs_rpc, call_rpc, tip: int, lookback_days: int = 8) -> pd.DataFrame:
    """Sweep the shared ERC-4626 Deposit topic, then VERIFY each new contract on-chain."""
    path = UNI / f"{chain}_vaults.parquet"
    wm = _load_wm(chain)
    have = pd.read_parquet(path) if path.exists() else pd.DataFrame(columns=["vault", "asset"])
    known = set(have["vault"]) if len(have) else set()

    bpd = int(86400 / CHAINS[chain].block_time_s)
    start = int(wm.get("vaults_block", tip - lookback_days * bpd)) + 1
    # The bare ERC-4626 Deposit topic is one of the busiest on any chain (~4k logs per 10k
    # mainnet blocks, family 149). The market cap of 10k blocks is far too wide for it, so this
    # sweep gets its own much smaller window; the chunker still halves further if a provider
    # complains. Weekly cadence means throughput here is irrelevant.
    cap = 1000
    logs = get_logs_chunked(logs_rpc, max(start, tip - lookback_days * bpd), tip, cap,
                            topics=[DEPOSIT])
    seen = {l["address"].lower() for l in logs}
    cand = sorted(seen - known)

    # RETRY POOL. Verification is two eth_calls, and under strict=False an INFRASTRUCTURE failure
    # (rate limit, node error) returns None exactly like a revert does. Without this, such a
    # candidate is skipped while the watermark advances past its Deposit event -- so a real vault
    # would be lost permanently unless it happened to receive another deposit. The bootstrap run
    # logged several such failures, so this is an observed problem, not a hypothetical one.
    pend_path = UNI / f"{chain}_pending.parquet"
    pending = set(pd.read_parquet(pend_path)["vault"]) if pend_path.exists() else set()
    cand = sorted(set(cand) | pending)

    verified, still_pending = [], []
    for i in range(0, len(cand), 200):
        chunk = cand[i:i + 200]
        assets = call_rpc.batch_call([Call(a, ASSET) for a in chunk], tip, strict=False)
        ctas = call_rpc.batch_call([Call(a, CTA) for a in chunk], tip, strict=False)
        for a, asr, ct in zip(chunk, assets, ctas):
            tok = dec_address(asr)
            if tok and ct and ct != "0x":
                verified.append({"vault": a, "asset": tok, "first_seen_block": tip})
            elif asr is None and ct is None:
                # BOTH calls came back empty: cannot distinguish "not a 4626" from "the node
                # refused us", so retry rather than discard. A contract that answers one and not
                # the other is genuinely not a vault and is dropped.
                still_pending.append(a)

    if verified:
        have = pd.concat([have, pd.DataFrame(verified)], ignore_index=True)
    have = have.drop_duplicates("vault")
    have.to_parquet(path, index=False)
    pd.DataFrame({"vault": sorted(set(still_pending) - set(have["vault"]))}).to_parquet(
        pend_path, index=False)
    wm["vaults_block"] = tip
    _save_wm(chain, wm)
    print(f"  [{chain}] vaults: {len(have):,} verified 4626 (+{len(verified)} new from "
          f"{len(cand):,} candidates, {len(still_pending)} pending retry)", flush=True)
    return have


def main() -> None:
    for name in CHAINS:
        print(f"[{name}] discovering universe ...", flush=True)
        call_rpc = rpc_for(CHAINS[name], "call")
        logs_rpc = rpc_for(CHAINS[name], "logs")
        tip = call_rpc.latest_block() - 5
        # logs pool for discovery sweeps, call pool for the 4626 verification reads
        discover_markets(name, logs_rpc, tip)
        discover_vaults(name, logs_rpc, call_rpc, tip)


if __name__ == "__main__":
    main()
