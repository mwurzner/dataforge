"""On-chain options implied-volatility surface.

One row per instrument per sample: strike, expiry, mark and forward price, and the full greek
set including implied volatility.

Operational notes:
  One request returns the whole chain for an asset, so a complete surface costs one call.
  A failed fetch writes a single explicit error row for that asset and never an empty chain --
  an unreachable venue and a venue with no listings are different facts.
  Strike and expiry are parsed from the instrument name and left null when it does not match
  the expected shape, rather than guessed.
"""
from __future__ import annotations

import json
import time
import urllib.request

import pandas as pd

DATASET = "e22_options_surface"
BOOK_DATASET = "e22_options_book"
HDRS = {"User-Agent": "dataforge/1.0", "Accept": "application/json"}

VENUE = "aevo"
BASE = "https://api.aevo.xyz/markets?instrument_type=OPTION&asset="
BOOK = "https://api.aevo.xyz/orderbook?instrument_name="
ASSETS = ("ETH", "BTC", "SOL")

GREEKS = ("iv", "delta", "gamma", "vega", "theta", "rho")

# The book costs one request per instrument, so it is sampled on a LADDER rather than the whole
# chain. Measured when choosing the shape: the nearest expiry has no book at all -- market makers
# have pulled quotes by then -- while every later one quotes, and the bid-ask in vol points runs
# from about 0.62 at one day to 0.045 at nineteen. Tenor therefore buys more than strike depth
# does, so the ladder is wide in expiry and shallow in strike.
BOOK_ASSETS = ("ETH", "BTC")
BOOK_SKIP_FRONT = 1       # the expiring contract, measured empty
BOOK_EXPIRIES = 3
BOOK_STRIKES = 2          # nearest the forward, each side quoted as both put and call


def _get(url: str, timeout: int = 30):
    """Always (payload, error). A failed fetch must never reach the frame as an empty chain."""
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


def _parse_name(name: str) -> tuple[str | None, float | None, str | None]:
    """ASSET-DDMMMYY-STRIKE-C|P. Anything else yields nulls rather than a guess."""
    parts = (name or "").split("-")
    if len(parts) != 4:
        return None, None, None
    _, expiry, strike, kind = parts
    return expiry, _f(strike), (kind if kind in ("C", "P") else None)


def sample() -> pd.DataFrame:
    rows: list[dict] = []
    # ONE timestamp per asset, not per row. Stamping each row separately gave every row a
    # distinct microsecond value, so grouping by it returned singletons instead of a surface.
    # round_ts additionally ties the assets fetched together into one pass.
    round_ts = time.time()
    for asset in ASSETS:
        t0 = time.time()
        payload, err = _get(BASE + asset)
        sampled_ts = time.time()
        lat = round(sampled_ts - t0, 3)
        if err or not isinstance(payload, list):
            rows.append({"round_ts": round_ts, "sampled_ts": sampled_ts, "venue": VENUE,
                         "asset": asset, "instrument_name": None,
                         "error": err or "unexpected payload", "latency_s": lat})
            continue
        for m in payload:
            name = m.get("instrument_name")
            expiry, strike, kind = _parse_name(name)
            g = m.get("greeks") or {}
            row = {
                "round_ts": round_ts, "sampled_ts": sampled_ts, "venue": VENUE, "asset": asset,
                "instrument_name": name,
                "expiry": expiry,
                "expiry_ts": _f(m.get("expiry")),
                # Venue field first, parsed name as fallback, so a naming change cannot empty it.
                "strike": (_f(m.get("strike")) if m.get("strike") is not None else strike),
                # Prefer the venue's own field and fall back to the parsed name, so a change in
                # naming does not silently empty the column.
                "option_type": (m.get("option_type") or kind),
                "is_active": m.get("is_active"),
                "mark_price": _f(m.get("mark_price")),
                "index_price": _f(m.get("index_price")),
                "forward_price": _f(m.get("forward_price")),
                "price_step": _f(m.get("price_step")),
                "latency_s": lat, "error": None,
            }
            for k in GREEKS:
                row[k] = _f(g.get(k))
            rows.append(row)
    return pd.DataFrame(rows)


def _ladder(surface: pd.DataFrame) -> list[str]:
    """Instruments to pull books for: near the forward, across expiries, skipping the front."""
    out: list[str] = []
    live = surface[surface.error.isna() & surface.expiry_ts.notna() & surface.strike.notna()]
    for asset in BOOK_ASSETS:
        a = live[live.asset == asset]
        if a.empty:
            continue
        exps = sorted(a.expiry_ts.unique())[BOOK_SKIP_FRONT:BOOK_SKIP_FRONT + BOOK_EXPIRIES]
        for e in exps:
            grp = a[a.expiry_ts == e]
            fwd = grp.forward_price.dropna()
            if fwd.empty:
                continue
            f = float(fwd.iloc[0])
            near = sorted(grp.strike.unique(), key=lambda k: abs(k - f))[:BOOK_STRIKES]
            out.extend(grp[grp.strike.isin(near)].instrument_name.tolist())
    return out


def books(surface: pd.DataFrame) -> pd.DataFrame:
    """Top of book for the ladder. One row per instrument, with the quote in vol terms."""
    rows: list[dict] = []
    for name in _ladder(surface):
        t0 = time.time()
        ob, err = _get(BOOK + name)
        ts = time.time()
        row = {"sampled_ts": ts, "venue": VENUE, "instrument_name": name,
               "latency_s": round(ts - t0, 3), "error": err,
               "best_bid": None, "best_bid_size": None, "best_bid_iv": None,
               "best_ask": None, "best_ask_size": None, "best_ask_iv": None,
               "iv_spread": None, "bid_levels": 0, "ask_levels": 0}
        if not err and isinstance(ob, dict):
            bids, asks = ob.get("bids") or [], ob.get("asks") or []
            row["bid_levels"], row["ask_levels"] = len(bids), len(asks)
            # Levels are [price, size, iv] as strings. A one-sided book is a real state, not a
            # failure, so it leaves the missing side null and `error` stays None.
            if bids:
                row["best_bid"], row["best_bid_size"] = _f(bids[0][0]), _f(bids[0][1])
                row["best_bid_iv"] = _f(bids[0][2]) if len(bids[0]) > 2 else None
            if asks:
                row["best_ask"], row["best_ask_size"] = _f(asks[0][0]), _f(asks[0][1])
                row["best_ask_iv"] = _f(asks[0][2]) if len(asks[0]) > 2 else None
            if row["best_bid_iv"] is not None and row["best_ask_iv"] is not None:
                row["iv_spread"] = round(row["best_ask_iv"] - row["best_bid_iv"], 6)
        rows.append(row)
    return pd.DataFrame(rows)


if __name__ == "__main__":
    df = sample()
    ok = df[df.error.isna()]
    print(f"{len(df):,} rows | {len(ok):,} instruments | errors {int(df.error.notna().sum())}")
    if len(ok):
        print(ok.groupby("asset").agg(n=("instrument_name", "size"),
                                      iv_med=("iv", "median"),
                                      expiries=("expiry", "nunique")).to_string())
        print()
        print(ok[["instrument_name", "strike", "option_type", "mark_price",
                  "forward_price", "iv", "delta"]].head(6).to_string(index=False))
