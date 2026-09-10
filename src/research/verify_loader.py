"""Instrument before belief: the loader must reproduce numbers measured independently first.

Two targets, both measured during reconnaissance by a separate throwaway script that shares no
code with load.py:
    e30  top-3 (drpc, blxrbdn, flashbots) minus bottom-2 (merkle, nodies) mean daily
         sufficient_priority at horizon 1  ==  +0.337, positive on 8 of 8 days
    e28  mempool.space at target_blocks == 1  ==  93.0% sufficient, median overpay 2.00x

If these do not come back, the loader is wrong and nothing computed downstream may be believed.
Run against the PUBLIC source deliberately, since that is the window those numbers came from.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import pandas as pd  # noqa: E402

from src.research.load import clean, load  # noqa: E402

TOP = ["drpc", "blxrbdn", "flashbots"]
BOT = ["merkle", "nodies"]
fails = []


def check(name, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL':<5}{name:<50}{detail}")
    if not cond:
        fails.append(name)


print("=" * 78)
print("TARGET 1 -- e30 gas, top-3 minus bottom-2, public window")
e30 = load("e30_gas_estimator_accuracy", source="public")
e30c, _ = clean(e30, "e30_gas_estimator_accuracy")
if len(e30c):
    e30c = e30c.assign(day=pd.to_datetime(e30c.sampled_ts, unit="s").dt.date)
    h1 = e30c[e30c.horizon_blocks == 1]
    piv = h1.pivot_table(index="provider", columns="day",
                         values="sufficient_priority", aggfunc="mean")
    gap = piv.loc[[p for p in TOP if p in piv.index]].mean() - \
        piv.loc[[p for p in BOT if p in piv.index]].mean()
    print(f"    per-day gap: {gap.round(3).to_dict()}")
    check("mean gap ~ +0.337", abs(gap.mean() - 0.337) < 0.005, f"{gap.mean():.3f}")
    check("positive on all 8 days", bool((gap > 0).all()) and len(gap) == 8,
          f"{int((gap > 0).sum())}/{len(gap)}")
    check("min gap ~ +0.273", abs(gap.min() - 0.273) < 0.005, f"{gap.min():.3f}")
else:
    check("e30 loaded", False, "empty")

print()
print("=" * 78)
print("TARGET 2 -- e28 bitcoin, mempool.space at target 1")
e28 = load("e28_fee_estimator_accuracy", source="public")
e28c, _ = clean(e28, "e28_fee_estimator_accuracy")
if len(e28c):
    m = e28c[(e28c.provider == "mempool.space") & (e28c.target_blocks == 1.0)]
    suff = m.sufficient.mean()
    over = m.overpay_ratio.median()
    check("sufficiency ~ 93.0%", abs(suff - 0.930318) < 0.001, f"{suff:.4f}  n={len(m)}")
    check("median overpay ~ 2.00x", abs(over - 2.00) < 0.01, f"{over:.4f}")
else:
    check("e28 loaded", False, "empty")

print()
print("=" * 78)
print("SILENT-ZERO GUARD -- clean() must actually neutralise the known traps")
for ds in ("e17_perp_depth", "e22_options_book", "e10_quote_benchmark"):
    raw = load(ds, source="public", verbose=False)
    if not len(raw):
        print(f"  {ds}: not loaded, skipped")
        continue
    c, funnel = clean(raw, ds, verbose=False)
    nulled = {k: v for k, v in funnel.items() if k.endswith("_zeros_nulled")}
    print(f"  {ds:<24}raw={funnel['raw']:<7} err_dropped={funnel['error_rows_dropped']:<6} "
          f"clean={funnel['clean']:<7} {nulled}")
    if "error" in raw.columns:
        check(f"{ds}: no error rows survive", not c["error"].notna().any() if "error" in c else True)

print()
print("=" * 78)
print("RESULT:", "LOADER VERIFIED" if not fails else f"FAILURES: {fails}")
if fails:
    sys.exit(1)
