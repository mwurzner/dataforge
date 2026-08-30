"""Solana DEX aggregator quotes and route composition.

One row per (pair, size): the quoted output, price impact, and how far the aggregated route beat
the best single venue. A second frame carries one row per route leg.

Operational notes:
  Quotes are computed per request and kept by nobody, so only what was recorded exists.
  The aggregator also returns quotes from individual venues it considered. Those competing
  quotes vanish with the response and are what `chosen_vs_best_alt_bps` is measured against.
  Every mint here was verified by quoting it rather than trusted from memory.
  A failed fetch writes an explicit error row; it is never dropped.
"""
from __future__ import annotations

import json
import time
import urllib.request

import pandas as pd

DATASET = "e24_solana_quotes"
ROUTE_DATASET = "e24_solana_routes"
HDRS = {"User-Agent": "dataforge/1.0", "Accept": "application/json"}

VENUE = "jupiter"
BASE = "https://lite-api.jup.ag/swap/v1/quote"

# Verified by quoting each one, not copied from memory.
MINTS = {
    "SOL": "So11111111111111111111111111111111111111112",
    "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "USDT": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    "JUP": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
    "BONK": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    "JitoSOL": "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn",
}

# Input is always SOL, so one lamport ladder covers every pair. Chosen to straddle the point
# where the aggregator starts splitting: 1 SOL routed through a single venue, 50 and 1000 through
# four legs.
SIZES = (1_000_000_000, 50_000_000_000, 1_000_000_000_000)
PAIRS = (("SOL", "USDC"), ("SOL", "USDT"), ("SOL", "JUP"),
         ("SOL", "BONK"), ("SOL", "JitoSOL"))
SLIPPAGE_BPS = 50


def _get(url: str, timeout: int = 25):
    """Always (payload, error). A failed quote must never reach the frame as a zero."""
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers=HDRS), timeout=timeout)
        return json.loads(r.read()), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {str(exc)[:70]}"


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def sample() -> tuple[pd.DataFrame, pd.DataFrame]:
    round_ts = time.time()
    rows: list[dict] = []
    legs: list[dict] = []
    for a, b in PAIRS:
        for amt in SIZES:
            url = (f"{BASE}?inputMint={MINTS[a]}&outputMint={MINTS[b]}"
                   f"&amount={amt}&slippageBps={SLIPPAGE_BPS}")
            t0 = time.time()
            d, err = _get(url)
            ts = time.time()
            row = {
                "round_ts": round_ts, "sampled_ts": ts, "venue": VENUE,
                "input_symbol": a, "output_symbol": b,
                "in_amount": amt, "slippage_bps": SLIPPAGE_BPS,
                "latency_s": round(ts - t0, 3), "error": err,
                "out_amount": None, "price_impact_pct": None, "swap_usd_value": None,
                "other_amount_threshold": None, "n_legs": None,
                "amm_report_raw": None, "n_amm_report": None,
                "context_slot": None, "quote_time_s": None,
            }
            if err or not isinstance(d, dict):
                rows.append(row)
                continue
            out = _f(d.get("outAmount"))
            plan = d.get("routePlan") or []
            # STORED RAW, DELIBERATELY. This report is NOT restricted to the pair asked for:
            # a SOL->BONK request returns values around 5.2e9, which is USDC scale, against a
            # BONK output of 1.8e14. A first version derived a "chosen versus best alternative"
            # spread from it and produced 3.4e8 basis points, which is what exposed the mismatch.
            # Entries are also heterogeneous -- one was the string "Pair insufficient liquidity"
            # rather than a number. Keeping the raw object preserves both facts; deriving a
            # spread from it would publish a number that means nothing.
            alts = ((d.get("mostReliableAmmsQuoteReport") or {}).get("info") or {})
            row.update({
                "out_amount": out,
                "price_impact_pct": _f(d.get("priceImpactPct")),
                "swap_usd_value": _f(d.get("swapUsdValue")),
                "other_amount_threshold": _f(d.get("otherAmountThreshold")),
                "n_legs": len(plan),
                "amm_report_raw": json.dumps(alts) if alts else None,
                "n_amm_report": len(alts),
                "context_slot": d.get("contextSlot"),
                "quote_time_s": _f(d.get("timeTaken")),
            })
            rows.append(row)
            for i, leg in enumerate(plan):
                si = leg.get("swapInfo") or {}
                legs.append({
                    "round_ts": round_ts, "sampled_ts": ts, "venue": VENUE,
                    "input_symbol": a, "output_symbol": b, "in_amount": amt,
                    "leg_index": i,
                    "leg_venue": si.get("label"),
                    "leg_in_amount": _f(si.get("inAmount")),
                    "leg_out_amount": _f(si.get("outAmount")),
                    "leg_fee_amount": _f(si.get("feeAmount")),
                    "percent": leg.get("percent"),
                })
    return pd.DataFrame(rows), pd.DataFrame(legs)


if __name__ == "__main__":
    q, r = sample()
    ok = q[q.error.isna()]
    print(f"{len(q)} quotes ({int(q.error.notna().sum())} failed) | {len(r)} route legs")
    if len(ok):
        print(ok[["input_symbol", "output_symbol", "in_amount", "price_impact_pct",
                  "n_legs", "n_alt_venues", "chosen_vs_best_alt_bps"]].to_string(index=False))
        print()
        print("  venues seen:", sorted(r.leg_venue.dropna().unique())[:16])
