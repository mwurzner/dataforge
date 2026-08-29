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
HDRS = {"User-Agent": "dataforge/1.0", "Accept": "application/json"}

VENUE = "aevo"
BASE = "https://api.aevo.xyz/markets?instrument_type=OPTION&asset="
ASSETS = ("ETH", "BTC", "SOL")

GREEKS = ("iv", "delta", "gamma", "vega", "theta", "rho")


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
                "strike": strike,
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
