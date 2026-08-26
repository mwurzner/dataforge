"""E15 -- Bitcoin fee-estimator divergence. Which estimator was right, and by how much.

WHY THIS PASSES THE CRITERION, verified 2026-08-26 before building. Fee estimates are recomputed
continuously from live mempool state and no provider publishes a history of its own outputs. They
are also not reconstructable after the fact for the same reason E8 exists: an estimate is a
function of the mempool at that instant, and no archive holds a historical mempool. Search found
accuracy CLAIMS ("81% vs 59.5%", "within 5-10%") but no dataset behind them, and no comparison
archive across providers.

WHY IT BELONGS NEXT TO E8 RATHER THAN ALONE: this project already records the mempool state that
drives these numbers, so E8 plus E15 make a self-contained accuracy study. Nobody else holds both
halves. A user of the pair can ask what no public source answers today: given the mempool at time
T, which estimator's number actually got a transaction confirmed in its promised window, and what
did following the wrong one cost.

FIRST MEASUREMENT, five estimators at one instant: 1.00, 1.93, 2.00, 2.00, 3.00 sat/vB. A 3x
spread on the same question at the same moment. Whoever followed the top estimator paid triple.

ALL FIVE ARE KEYLESS AND INDEPENDENT, which is what makes the divergence meaningful -- these are
different methodologies, not one feed resold. Horizons are normalised to a target-block bucket
where the provider exposes one; the raw payload field used is recorded per row so a later reader
can check our normalisation rather than trust it.
"""
from __future__ import annotations

import json
import time
import urllib.request

import pandas as pd

DATASET = "e15_fee_estimators"
HDRS = {"User-Agent": "dataforge/1.0", "Accept": "application/json"}


def _get(url: str, timeout: int = 20):
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers=HDRS), timeout=timeout)
        return json.loads(r.read()), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {str(exc)[:70]}"


def _rows(provider, url, extract):
    """extract(payload) -> list of (target_blocks, sat_per_vb, source_field)."""
    t0 = time.time()
    d, err = _get(url)
    lat = round(time.time() - t0, 3)
    if err or d is None:
        return [{"sampled_ts": t0, "provider": provider, "target_blocks": None,
                 "sat_per_vb": None, "source_field": None, "latency_s": lat, "error": err}]
    out = []
    try:
        for tb, v, field in extract(d):
            if v is None:
                continue
            out.append({"sampled_ts": t0, "provider": provider, "target_blocks": tb,
                        "sat_per_vb": float(v), "source_field": field,
                        "latency_s": lat, "error": None})
    except Exception:
        return [{"sampled_ts": t0, "provider": provider, "target_blocks": None,
                 "sat_per_vb": None, "source_field": None, "latency_s": lat,
                 "error": "unparsable"}]
    return out or [{"sampled_ts": t0, "provider": provider, "target_blocks": None,
                    "sat_per_vb": None, "source_field": None, "latency_s": lat,
                    "error": "no estimates in payload"}]


def sample() -> pd.DataFrame:
    rows = []

    rows += _rows("mempool.space", "https://mempool.space/api/v1/fees/recommended",
                  lambda d: [(1, d.get("fastestFee"), "fastestFee"),
                             (3, d.get("halfHourFee"), "halfHourFee"),
                             (6, d.get("hourFee"), "hourFee"),
                             (None, d.get("economyFee"), "economyFee")])

    # Blockstream returns a map of target-blocks -> sat/vB.
    rows += _rows("blockstream", "https://blockstream.info/api/fee-estimates",
                  lambda d: [(int(k), v, f"fee-estimates[{k}]")
                             for k, v in d.items() if k in ("1", "2", "3", "6", "144")])

    # BitGo quotes sat per KILOBYTE; /1000 for sat/vB.
    rows += _rows("bitgo", "https://www.bitgo.com/api/v2/btc/tx/fee",
                  lambda d: [(2, (d.get("feePerKb") or 0) / 1000 or None, "feePerKb/1000")]
                  + [(int(k), v / 1000, f"feeByBlockTarget[{k}]/1000")
                     for k, v in (d.get("feeByBlockTarget") or {}).items()
                     if k in ("1", "2", "3", "6")])

    # BlockCypher also quotes per kilobyte.
    rows += _rows("blockcypher", "https://api.blockcypher.com/v1/btc/main",
                  lambda d: [(1, (d.get("high_fee_per_kb") or 0) / 1000 or None,
                              "high_fee_per_kb/1000"),
                             (3, (d.get("medium_fee_per_kb") or 0) / 1000 or None,
                              "medium_fee_per_kb/1000"),
                             (6, (d.get("low_fee_per_kb") or 0) / 1000 or None,
                              "low_fee_per_kb/1000")])

    # bitcoiner.live keys by MINUTES; 30/60/120 map to ~3/6/12 blocks at 10 min/block.
    def _bl(d):
        est = d.get("estimates") or {}
        out = []
        for mins, tb in (("30", 3), ("60", 6), ("120", 12)):
            e = est.get(mins) or {}
            out.append((tb, e.get("sat_per_vbyte"), f"estimates[{mins}].sat_per_vbyte"))
        return out
    rows += _rows("bitcoiner.live", "https://bitcoiner.live/api/fees/estimates/latest", _bl)

    df = pd.DataFrame(rows)
    # Divergence per horizon, computed here so the panel is usable without a join. Only over
    # providers that ANSWERED -- a failed provider must not read as an infinitely cheap estimate.
    df["spread_ratio"] = None
    for tb, g in df[df.sat_per_vb.notna() & df.target_blocks.notna()].groupby("target_blocks"):
        if len(g) < 2:
            continue
        lo, hi = g.sat_per_vb.min(), g.sat_per_vb.max()
        df.loc[g.index, "spread_ratio"] = round(float(hi / lo), 3) if lo else None
        df.loc[g.index, "n_answered"] = len(g)
    if "n_answered" not in df.columns:
        df["n_answered"] = pd.NA
    return df


if __name__ == "__main__":
    d = sample()
    ok = d[d.sat_per_vb.notna()]
    print(f"{len(ok)} estimates from {ok.provider.nunique()} providers, "
          f"{d.error.notna().sum()} errors")
    for tb, g in ok[ok.target_blocks.notna()].groupby("target_blocks"):
        lo, hi = g.sat_per_vb.min(), g.sat_per_vb.max()
        print(f"   target {int(tb):>3} blocks: {len(g)} providers, "
              f"{lo:.2f} to {hi:.2f} sat/vB ({hi/lo if lo else float('nan'):.1f}x)")
