"""E17 -- second-tier perpetual-DEX order-book depth. Aevo and Paradex.

WHY THIS PASSES THE CRITERION, verified 2026-08-27 before building. An order book is the purest
ephemeral object in markets: it is overwritten on every update and no venue stores its own history.
The question is only whether someone else already captures it, and for the LEADING perp DEXs
someone does -- Tardis covers dydx, dydx-v4 and hyperliquid, and vendors resell Hyperliquid's own
S3 dumps. So the test was whether coverage stops at the leaders.

It does. Tardis publishes its covered-exchange list as an API: of 64 exchanges, AEVO, PARADEX,
VERTEX and DRIFT are all ABSENT. Neither venue serves its own history either -- Aevo's
`orderbook-history` returns 404, and Paradex's `/interactive` endpoint IGNORES time parameters,
returning a book stamped with the current millisecond however far back you ask. Current state
only, retained by nobody.

WHAT IS STORED, AND WHY NOT THE FULL LADDER. A raw ladder is mostly redundant between polls and
enormous over a year. What an execution-cost reader actually needs is the shape: how much size
rests within a given distance of mid. So each row carries top-of-book, level counts, and CUMULATIVE
NOTIONAL within 5/10/25/50/100 bps of mid on each side. That is the quantity behind "what does it
cost to trade $X here", it compresses to a few hundred bytes, and it cannot be reconstructed later
from trades because unfilled resting orders leave no trace.

AEVO ALSO LISTS TOKENIZED EQUITY PERPS (AAPL, META, COHR). Depth for those is covered by nobody at
all -- not by crypto vendors, who do not follow equity names, and not by equity vendors, who do not
follow crypto venues. One is sampled deliberately.

HONEST LIMIT: this is a SNAPSHOT series at the poll interval, not a tick-level reconstruction. A
book that moves and returns between polls is invisible to us. Tick data would need a websocket
holding state across a run, which the long-poll design could support later; the snapshot is what
survives a scheduler that can drop a run.

UNRESOLVED: Vertex. Both gateway.prod and archive.prod fail the TLS handshake from the development
machine under two independent HTTP stacks. It is absent from Tardis too, so it may well qualify --
but its access was never established, so it is not included rather than assumed.
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

# (venue, market, url). Chosen for liquidity, measured 2026-08-27, plus one equity perp.
VENUES = [
    ("aevo", "ETH-PERP", "https://api.aevo.xyz/orderbook?instrument_name=ETH-PERP"),
    ("aevo", "BTC-PERP", "https://api.aevo.xyz/orderbook?instrument_name=BTC-PERP"),
    ("aevo", "SOL-PERP", "https://api.aevo.xyz/orderbook?instrument_name=SOL-PERP"),
    ("aevo", "AAPL-PERP", "https://api.aevo.xyz/orderbook?instrument_name=AAPL-PERP"),
    ("paradex", "BTC-USD-PERP",
     "https://api.prod.paradex.trade/v1/orderbook/BTC-USD-PERP?depth=50"),
    ("paradex", "ETH-USD-PERP",
     "https://api.prod.paradex.trade/v1/orderbook/ETH-USD-PERP?depth=50"),
    ("paradex", "SOL-USD-PERP",
     "https://api.prod.paradex.trade/v1/orderbook/SOL-USD-PERP?depth=50"),
    ("paradex", "HYPE-USD-PERP",
     "https://api.prod.paradex.trade/v1/orderbook/HYPE-USD-PERP?depth=50"),
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
