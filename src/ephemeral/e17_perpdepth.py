"""Perpetual futures order book depth.

One row per venue and market: spread, level counts, and resting notional within fixed distances
of mid.

Operational notes:
  Book shape is stored rather than raw ladders. Cumulative notional within a band is the
  quantity of interest and compresses far better.
  Where the spread exceeds a band, that band is correctly zero.
"""
from __future__ import annotations

import json
import time
import urllib.request

import pandas as pd

DATASET = "e17_perp_depth"
HDRS = {"User-Agent": "dataforge/1.0", "Accept": "application/json"}

# Distances from mid at which resting size is accumulated, in basis points.
BANDS = (5, 10, 25, 50, 100)

VENUES = [
    ("aevo", "ETH-PERP", "https://api.aevo.xyz/orderbook?instrument_name=ETH-PERP"),
    ("aevo", "BTC-PERP", "https://api.aevo.xyz/orderbook?instrument_name=BTC-PERP"),
    ("aevo", "SOL-PERP", "https://api.aevo.xyz/orderbook?instrument_name=SOL-PERP"),
    ("aevo", "AAPL-PERP", "https://api.aevo.xyz/orderbook?instrument_name=AAPL-PERP"),
    # PARADEX REMOVED 2026-08-29 ON ITS TERMS OF SERVICE, which read:
    #   "You further agree not to engage in data mining, robots, scraping, or similar data
    #    gathering or extraction methods of content or information from the Services."
    # That forbids the COLLECTION, not merely redistribution, so there is no archive-only
    # compromise available the way there is for a source that only restricts resale. Four
    # Paradex books were collected here every 15 minutes and published, which should not have
    # happened; hf_push redacts the historical rows from the public window on the next push.
]


def _get(url: str, timeout: int = 20):
    """Always (payload, error). A failed fetch must never reach the frame as an empty book."""
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers=HDRS), timeout=timeout)
        return json.loads(r.read()), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {str(exc)[:70]}"


def _levels(raw) -> list[tuple[float, float]]:
    """Both venues quote [[price, size], ...] as STRINGS. Unparsable levels are skipped."""
    out = []
    for lv in raw or []:
        try:
            out.append((float(lv[0]), float(lv[1])))
        except (TypeError, ValueError, IndexError):
            continue
    return out


def _depth_row(venue: str, market: str, bids, asks, lat: float, err: str | None) -> dict:
    row = {"sampled_ts": time.time(), "venue": venue, "market": market,
           "latency_s": lat, "error": err,
           "best_bid": None, "best_ask": None, "mid": None, "spread_bps": None,
           "bid_levels": len(bids), "ask_levels": len(asks),
           "bid_notional_total": None, "ask_notional_total": None}
    for b in BANDS:
        row[f"bid_notional_{b}bps"] = None
        row[f"ask_notional_{b}bps"] = None
    if err or not bids or not asks:
        # An unreachable venue and a genuinely one-sided book are different facts. Both leave the
        # measurements null; `error` is what separates them, and it is never invented.
        if not err and (not bids or not asks):
            row["error"] = "one-sided or empty book"
        return row

    bids = sorted(bids, key=lambda x: -x[0])
    asks = sorted(asks, key=lambda x: x[0])
    bb, ba = bids[0][0], asks[0][0]
    mid = (bb + ba) / 2
    row["best_bid"], row["best_ask"], row["mid"] = bb, ba, mid
    row["spread_bps"] = round((ba - bb) / mid * 1e4, 4) if mid else None
    row["bid_notional_total"] = round(sum(p * s for p, s in bids), 2)
    row["ask_notional_total"] = round(sum(p * s for p, s in asks), 2)
    for b in BANDS:
        lo, hi = mid * (1 - b / 1e4), mid * (1 + b / 1e4)
        row[f"bid_notional_{b}bps"] = round(sum(p * s for p, s in bids if p >= lo), 2)
        row[f"ask_notional_{b}bps"] = round(sum(p * s for p, s in asks if p <= hi), 2)
    return row


def sample() -> pd.DataFrame:
    rows = []
    for venue, market, url in VENUES:
        t0 = time.time()
        d, err = _get(url)
        lat = round(time.time() - t0, 3)
        bids = _levels((d or {}).get("bids")) if d else []
        asks = _levels((d or {}).get("asks")) if d else []
        rows.append(_depth_row(venue, market, bids, asks, lat, err))
    return pd.DataFrame(rows)


if __name__ == "__main__":
    df = sample()
    ok = df[df.error.isna()]
    print(f"{len(ok)}/{len(df)} books read")
    for r in df.itertuples():
        if r.error:
            print(f"  {r.venue:<9} {r.market:<14} ERROR {r.error}")
        else:
            print(f"  {r.venue:<9} {r.market:<14} spread {r.spread_bps:>7.2f} bps  "
                  f"levels {r.bid_levels:>3}/{r.ask_levels:<3} "
                  f"depth@10bps ${r.bid_notional_10bps:>12,.0f} / ${r.ask_notional_10bps:>12,.0f}")
