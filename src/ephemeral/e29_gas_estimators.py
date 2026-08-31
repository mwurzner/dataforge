"""E29 -- Ethereum gas-suggestion divergence across independent RPC providers.

WHY THIS PASSES THE CRITERION, verified 2026-08-31 before building. Every provider answers
eth_gasPrice and eth_maxPriorityFeePerGas for RIGHT NOW and none publishes a history of its own
answers. The suggestion is computed by each node from ITS OWN mempool view plus recent blocks, and
no archive holds a historical mempool, so a suggestion made at a past moment exists only if
somebody recorded it at the time. The OUTCOME half is different and we say so plainly: what a
block actually required is on chain forever and anyone can recompute it. Only the advice is scarce.
This is the same asymmetry that makes E15/E28 work, moved to a far larger fee market.

THE DIVERGENCE IS REAL, AND THE CONTROL MATTERED. A first pass showed a 6.3% spread while two of
four providers sat one block behind, which would have made this a latency measurement wearing a
disagreement costume. Comparing providers strictly WITHIN the same block over six consecutive
blocks:
    eth_gasPrice              median within-block spread   7.3%  (max 9.4%)
    eth_maxPriorityFeePerGas  median within-block spread 100.0%  (every block)
It is systematic rather than noisy. merkle returned a zero priority fee on every block sampled and
mevblocker on most, while drpc, flashbots, publicnode and 1rpc agreed on a real value. Those are
persistent house differences, and two of the disagreeing endpoints are MEV-protection RPCs that
see genuinely different order flow.

SCOPE IS ETHEREUM MAINNET ONLY, measured rather than assumed. The L2s were probed and are dead:
Base 0.0%, Optimism 0.0%, Polygon 0.0%, Arbitrum 0.3%. One sequencer posts one number and every
node echoes it, so there is no cross-provider view to record.

RARITY, checked rather than asserted. Owlracle serves gas history, but it is an aggregated view of
REALISED prices, which are on chain and backfillable by anyone. No source was found archiving
per-provider suggestions, and HuggingFace returned zero datasets across three search terms. Read
that as "none found" rather than "none exists": the search covered one dataset host and a handful
of known gas services, not the whole internet.
"""
from __future__ import annotations

import json
import time
import urllib.request

import pandas as pd

DATASET = "e29_gas_estimators"
BLOCK_DATASET = "e29_eth_block_fees"

HDRS = {"User-Agent": "dataforge/1.0", "Content-Type": "application/json"}

# Independent operators, not one company's aliases -- the whole point is that these nodes hold
# different mempool views. Endpoints that failed EVERY probe are left out (llamarpc 521 nine times
# out of nine, builder0x69, payload.de, gashawk, securerpc, rivet, blockpi; titan answers but
# serves no gasPrice). Ones that fail intermittently stay in, and their failures are recorded as
# errors rather than dropped, because an endpoint's own availability is a fact worth keeping.
#
# MERKLE IS KEPT DESPITE CURRENTLY RETURNING 429 AT EVERY CADENCE TESTED, up to 45s. That is a
# cooldown tripped by this module's own probing rather than a per-request limit, and it may clear
# from a different address. It is also the provider that disagreed most, so dropping it on a
# transient block would be the wrong call. If it stays dark the error column says so honestly.
# The panel does not depend on it: nodies independently returns a zero priority fee where
# blxrbdn and meowrpc return a real one, so the divergence survives merkle's absence.
PROVIDERS = [
    ("publicnode", "https://ethereum-rpc.publicnode.com"),
    ("merkle", "https://eth.merkle.io"),
    ("drpc", "https://eth.drpc.org"),
    ("1rpc", "https://1rpc.io/eth"),
    ("flashbots", "https://rpc.flashbots.net"),
    ("mevblocker", "https://rpc.mevblocker.io"),
    ("nodies", "https://eth-pokt.nodies.app"),
    ("blxrbdn", "https://virginia.rpc.blxrbdn.com"),
    ("meowrpc", "https://eth.meowrpc.com"),
]

# Percentiles of the priority fee actually paid by transactions in a block. p5/p10 are the
# marginal-inclusion end, which is what a suggestion is really trying to clear; the median travels
# alongside so a reader can pick a different definition rather than take ours on faith.
REWARD_PCTS = [5, 10, 25, 50]
WEI = 1e9  # wei per gwei


def _rpc(url: str, payload, timeout: int = 12):
    body = json.dumps(payload).encode()
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, data=body, headers=HDRS),
                                   timeout=timeout)
        return json.loads(r.read()), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {str(exc)[:70]}"


def _hex(v):
    """int from a hex quantity, or None. Never silently returns 0 -- a failed read and a genuine
    zero priority fee are different facts and the panel must be able to tell them apart."""
    if not isinstance(v, str) or not v.startswith("0x"):
        return None
    try:
        return int(v, 16)
    except ValueError:
        return None


def sample() -> pd.DataFrame:
    """One batched call per provider, so the three values share a single instant per endpoint."""
    req = [{"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
           {"jsonrpc": "2.0", "id": 2, "method": "eth_gasPrice", "params": []},
           {"jsonrpc": "2.0", "id": 3, "method": "eth_maxPriorityFeePerGas", "params": []}]
    rows = []
    for name, url in PROVIDERS:
        t0 = time.time()
        d, err = _rpc(url, req)
        lat = round(time.time() - t0, 3)
        vals = {}
        if d is not None and isinstance(d, list):
            for item in d:
                if isinstance(item, dict) and "result" in item:
                    vals[item.get("id")] = _hex(item["result"])
            if not vals:
                err = err or "no results in batch"
        elif err is None:
            err = "unexpected payload"
        rows.append({
            "sampled_ts": t0,
            "provider": name,
            "head_block": vals.get(1),
            "gas_price_wei": vals.get(2),
            "max_priority_fee_wei": vals.get(3),
            "latency_s": lat,
            "error": err,
        })

    df = pd.DataFrame(rows)
    df["gas_price_gwei"] = df.gas_price_wei / WEI
    df["priority_fee_gwei"] = df.max_priority_fee_wei / WEI

    # WITHIN-BLOCK spread, computed here because it is the quantity the control gate turned on and
    # a reader should not have to rebuild it. Comparing providers that sit on DIFFERENT heads
    # measures block lag, not disagreement, so the grouping key is the head block.
    for col, out in (("gas_price_wei", "gas_price_spread_pct"),
                     ("max_priority_fee_wei", "priority_spread_pct")):
        df[out] = None
        ok = df[df[col].notna() & df.head_block.notna()]
        for _, g in ok.groupby("head_block"):
            if len(g) < 2:
                continue
            lo, hi = g[col].min(), g[col].max()
            med = g[col].median()
            df.loc[g.index, out] = round(float((hi - lo) / med * 100), 3) if med else None
            df.loc[g.index, "n_answered"] = len(g)
    if "n_answered" not in df.columns:
        df["n_answered"] = pd.NA
    return df


def block_fees(url: str = "https://ethereum-rpc.publicnode.com", n: int = 20) -> pd.DataFrame:
    """What blocks ACTUALLY required: base fee plus priority-fee percentiles of included txs.

    This half is freely available and we do not pretend otherwise -- eth_feeHistory is standard
    JSON-RPC over on-chain data, so anyone can rebuild it whenever they like. It is carried here
    so the accuracy join is self-contained rather than requiring the reader to fetch it, exactly
    as E28 carries block outcomes next to E15's estimates.
    """
    req = {"jsonrpc": "2.0", "id": 1, "method": "eth_feeHistory",
           "params": [hex(n), "latest", REWARD_PCTS]}
    d, err = _rpc(url, req)
    if err or not isinstance(d, dict) or "result" not in d:
        return pd.DataFrame()
    res = d["result"]
    oldest = _hex(res.get("oldestBlock"))
    base = res.get("baseFeePerGas") or []
    rewards = res.get("reward") or []
    ratios = res.get("gasUsedRatio") or []
    if oldest is None or not rewards:
        return pd.DataFrame()

    rows = []
    for i, rw in enumerate(rewards):
        # baseFeePerGas is one longer than reward: it carries the NEXT block's base fee too.
        if i >= len(base):
            break
        vals = [_hex(x) for x in (rw or [])]
        if len(vals) != len(REWARD_PCTS):
            continue
        rows.append({
            "block_number": oldest + i,
            "base_fee_wei": _hex(base[i]),
            "gas_used_ratio": float(ratios[i]) if i < len(ratios) else None,
            **{f"reward_p{p}_wei": v for p, v in zip(REWARD_PCTS, vals)},
            "fetched_ts": time.time(),
        })
    df = pd.DataFrame(rows)
    if len(df):
        df["base_fee_gwei"] = df.base_fee_wei / WEI
        for p in REWARD_PCTS:
            df[f"reward_p{p}_gwei"] = df[f"reward_p{p}_wei"] / WEI
    return df


if __name__ == "__main__":
    d = sample()
    ok = d[d.gas_price_wei.notna()]
    print(f"{len(ok)}/{len(d)} providers answered, {d.error.notna().sum()} errors")
    for _, r in d.sort_values("provider").iterrows():
        # isinstance, not truthiness: a pandas NaN is TRUTHY, so `if r.error` reports every
        # SUCCESSFUL row as an error. Same defect as the usd_class bug in the mortality scan.
        if isinstance(r.error, str):
            print(f"   {r.provider:<12} ERROR {r.error}")
        else:
            print(f"   {r.provider:<12} block={r.head_block} gas={r.gas_price_gwei:.5f} "
                  f"prio={r.priority_fee_gwei:.5f} gwei  {r.latency_s*1000:.0f}ms")
    sp = pd.to_numeric(d.gas_price_spread_pct, errors="coerce").dropna()
    pp = pd.to_numeric(d.priority_spread_pct, errors="coerce").dropna()
    if len(sp):
        print(f"   within-block spread: gasPrice {sp.max():.1f}%, priority {pp.max():.1f}%")

    b = block_fees()
    print(f"\n{len(b)} blocks of fee history")
    if len(b):
        print(b[["block_number", "base_fee_gwei", "reward_p5_gwei", "reward_p10_gwei",
                 "reward_p50_gwei", "gas_used_ratio"]].tail(5).to_string(index=False))
