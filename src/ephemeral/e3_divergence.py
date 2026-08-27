"""E3 -- mempool divergence across nodes. Cheap, and genuinely unrecoverable.

Every node has its own view of the pending set, shaped by peering and propagation. Nobody
reconciles those views and nobody stores them, so the disagreement at a given instant exists only
if someone writes it down at that instant.

The probe that motivated this: at the same moment, four endpoints reported
    publicnode 85 | merkle 163 | drpc 131 | flashbots 170
pending transactions in their proposed next block. Those are not errors -- they are four honest
answers to the same question.

Uses `eth_getBlockByNumber("pending")`, which returns each node's own next-block candidate rather
than the full pool. That is deliberately the cheap read (0.1 MB, 0.1 s against 86 MB for
txpool_content) and it is the view that actually differs most between nodes.
"""
from __future__ import annotations

import json
import time
import urllib.request

import pandas as pd

DATASET = "e3_mempool_divergence"
HDRS = {"Content-Type": "application/json", "User-Agent": "dataforge/1.0"}

ENDPOINTS = {
    "publicnode": "https://ethereum-rpc.publicnode.com",
    "merkle": "https://eth.merkle.io",
    "drpc": "https://eth.drpc.org",
    "flashbots": "https://rpc.flashbots.net",
}


def _pending_hashes(url: str) -> tuple[set[str], str | None, float]:
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber",
                       "params": ["pending", False]}).encode()
    t0 = time.time()
    try:
        d = json.load(urllib.request.urlopen(
            urllib.request.Request(url, data=body, headers=HDRS), timeout=25))
        if "error" in d:
            return set(), str(d["error"])[:80], time.time() - t0
        txs = (d.get("result") or {}).get("transactions") or []
        return set(txs), None, time.time() - t0
    except Exception as exc:
        return set(), f"{type(exc).__name__}", time.time() - t0


def _gas(url: str) -> tuple[float | None, float | None]:
    """eth_gasPrice and eth_maxPriorityFeePerGas, in gwei, from one endpoint."""
    out = []
    for method in ("eth_gasPrice", "eth_maxPriorityFeePerGas"):
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": []}).encode()
        try:
            d = json.load(urllib.request.urlopen(
                urllib.request.Request(url, data=body, headers=HDRS), timeout=15))
            r = d.get("result")
            out.append(int(r, 16) / 1e9 if r else None)
        except Exception:
            out.append(None)
    return out[0], out[1]


def _owlracle() -> float | None:
    """One genuinely independent estimator, keyless. The public RPCs mostly share geth's own
    calculation and agree to the wei, so without an outside opinion this panel would be
    measuring one implementation against itself."""
    try:
        d = json.load(urllib.request.urlopen(urllib.request.Request(
            "https://api.owlracle.info/v4/eth/gas",
            headers={"User-Agent": HDRS["User-Agent"]}), timeout=15))
        speeds = d.get("speeds") or []
        if speeds:
            v = speeds[len(speeds) // 2]
            return v.get("maxFeePerGas") or v.get("gasPrice")
    except Exception:
        return None
    return None


def sample() -> pd.DataFrame:
    """One simultaneous read across all endpoints. Rows are per-endpoint, with the pairwise
    overlap recorded against the union so divergence is reconstructable later."""
    ts = time.time()
    views: dict[str, set[str]] = {}
    rows = []
    for name, url in ENDPOINTS.items():
        h, err, dt = _pending_hashes(url)
        views[name] = h
        rows.append({"sampled_ts": ts, "endpoint": name, "n_pending": len(h),
                     "latency_s": round(dt, 3), "error": err})
    union = set().union(*views.values()) if views else set()
    # Intersection over endpoints that actually answered -- a failed endpoint contributing an
    # empty set would otherwise drive the intersection to zero and look like total divergence.
    answered = [v for v in views.values() if v]
    inter = set.intersection(*answered) if answered else set()
    for r in rows:
        name = r["endpoint"]
        own = views[name]
        others = set().union(*[v for k, v in views.items() if k != name]) if len(views) > 1 else set()
        r["n_union"] = len(union)
        r["n_intersection"] = len(inter)
        # Transactions THIS node saw that no other node did -- the actual divergence signal.
        r["n_unique_to_this_node"] = len(own - others)
        r["share_of_union"] = (len(own) / len(union)) if union else None
        # GAS ADVICE, alongside the pending-set view. Endpoints disagree here too, and unlike
        # the pending set nobody records it: one probe found 8.26x between the cheapest and
        # dearest eth_gasPrice at the same instant. Read it knowing that most public RPCs return
        # geth's own number to the wei, so the spread is usually one dissenting endpoint rather
        # than a survey of independent opinions. Owlracle is added as an outside estimator for
        # exactly that reason and appears as its own row.
        g, pri = _gas(ENDPOINTS[name])
        r["gas_price_gwei"] = g
        r["priority_fee_gwei"] = pri
    owl = _owlracle()
    rows.append({"sampled_ts": ts, "endpoint": "owlracle", "n_pending": None,
                 "latency_s": None, "error": None if owl else "no estimate",
                 "n_union": len(union), "n_intersection": len(inter),
                 "n_unique_to_this_node": None, "share_of_union": None,
                 "gas_price_gwei": owl, "priority_fee_gwei": None})
    df = pd.DataFrame(rows)
    gp = pd.to_numeric(df["gas_price_gwei"], errors="coerce").dropna()
    df["gas_spread_ratio"] = round(float(gp.max() / gp.min()), 3) if len(gp) > 1 and gp.min() else None
    return df
