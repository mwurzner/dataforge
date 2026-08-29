"""Perpetual mark and index prices, including equities and pre-IPO names.

One row per instrument per sample: the venue's mark, the reference index, and the basis between
them, with the market type so equities, commodities and crypto can be separated.

Operational notes:
  One request returns every perpetual, so a full snapshot costs one call.
  The venue serves index history but no mark history, so the mark and therefore the basis are
  only available if recorded as they happen.
  A failed fetch writes a single explicit error row and never an empty snapshot.
"""
from __future__ import annotations

import json
import time
import urllib.request

import pandas as pd

DATASET = "e23_perp_mark_index"
HDRS = {"User-Agent": "dataforge/1.0", "Accept": "application/json"}

VENUE = "aevo"
URL = "https://api.aevo.xyz/markets?instrument_type=PERPETUAL"


def _get(url: str, timeout: int = 30):
    """Always (payload, error). A failed fetch must never reach the frame as an empty snapshot."""
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


def sample() -> pd.DataFrame:
    ts = time.time()
    t0 = time.time()
    payload, err = _get(URL)
    lat = round(time.time() - t0, 3)
    if err or not isinstance(payload, list):
        return pd.DataFrame([{"sampled_ts": ts, "venue": VENUE, "instrument_name": None,
                              "error": err or "unexpected payload", "latency_s": lat}])
    rows = []
    for m in payload:
        mark, index = _f(m.get("mark_price")), _f(m.get("index_price"))
        rows.append({
            "sampled_ts": ts, "venue": VENUE,
            "instrument_name": m.get("instrument_name"),
            "underlying_asset": m.get("underlying_asset"),
            # equity / crypto / commodity / etf / fx / pre_ipo, as the venue classifies it.
            "market_type": m.get("market_type"),
            "is_rwa": m.get("is_rwa"),
            "is_active": m.get("is_active"),
            "mark_price": mark,
            "index_price": index,
            # Signed, and in basis points so instruments priced in the hundreds and the tens of
            # thousands are comparable. Null rather than zero where either leg is missing.
            "basis_bps": (round((mark - index) / index * 1e4, 4)
                          if (mark is not None and index) else None),
            "max_leverage": _f(m.get("max_leverage")),
            "latency_s": lat, "error": None,
        })
    return pd.DataFrame(rows)


if __name__ == "__main__":
    df = sample()
    ok = df[df.error.isna()]
    print(f"{len(df)} rows | errors {int(df.error.notna().sum())}")
    if len(ok):
        print(ok.groupby("market_type").agg(n=("instrument_name", "size"),
                                            med_basis=("basis_bps", "median")).to_string())
        rwa = ok[ok.is_rwa.astype(bool)].nlargest(6, "basis_bps")
        print()
        print(rwa[["instrument_name", "market_type", "mark_price",
                   "index_price", "basis_bps"]].to_string(index=False))
