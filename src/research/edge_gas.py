"""H1 -- is there an execution-cost edge in which RPC provider you ask for a gas suggestion?

PRIMARY CONTRAST IS ZERO-BAR-EXCLUDED, and that specification choice is forced by the data rather
than chosen for taste. `sufficient_priority` is `predicted >= bar`, so on a block that cleared at a
zero tip the bar is 0 and EVERY provider is trivially sufficient. Measured on the discovery window,
22.0% of blocks clear at a zero tip, and merkle/nodies -- which suggest exactly 0 in 99.9% of
samples -- score 1.000 on precisely those blocks and 0.000 on all others. Their pooled 0.22 is
therefore the zero-bar frequency restated: a property of Ethereum, not of merkle. Excluding those
blocks does not shrink the contrast, it widens it, because zero-bar blocks pull every provider
toward 1.

HORIZONS ARE NESTED, NOT INDEPENDENT. build() takes the cheapest bar over blocks n+1..n+h, so
h=3 strictly contains h=1; three rows per suggestion is one observation. h=1 is primary and the
horizon sweep is a mechanism gate, never extra n.

THE UNIT IS THE DAY. A provider's suggestion is near-constant between blocks (drpc returns the
client-default tip, merkle returns 0), so 123k rows are nowhere near 123k observations. The honest
test is whether the sign is consistent across independent days.

AND THE BAR IS ARITHMETIC, NOT OPINION. A unanimous-sign day test cannot beat p = 2^-n. Eight days
all-positive gives one-sided p = 0.0039, which does NOT clear this estate's pre-registered
p <= 0.00135 (t>3, Harvey-Liu-Zhu). Ten days is the first n that can. This is reported explicitly
rather than quietly compared against a laxer threshold.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.research.load import clean, load  # noqa: E402
from src.research.stats import block_bootstrap, monotone, permutation_test, sign_test  # noqa: E402

TOP = ["drpc", "blxrbdn", "flashbots"]
BOT = ["merkle", "nodies"]
HLZ_P = 0.00135


def daily_gap(h1: pd.DataFrame) -> pd.Series:
    piv = h1.pivot_table(index="provider", columns="day",
                         values="sufficient_priority", aggfunc="mean")
    top = [p for p in TOP if p in piv.index]
    bot = [p for p in BOT if p in piv.index]
    return piv.loc[top].mean() - piv.loc[bot].mean()


def run(source: str = "auto") -> dict:
    raw = load("e30_gas_estimator_accuracy", source=source)
    df, _ = clean(raw, "e30_gas_estimator_accuracy")
    if not len(df):
        print("  no data", flush=True)
        return {}
    df["day"] = pd.to_datetime(df.sampled_ts, unit="s").dt.date
    h1 = df[df.horizon_blocks == 1].copy()

    zero_bar = h1.cleared_p10_gwei.fillna(-1) == 0
    print(f"\n  zero-bar blocks excluded: {zero_bar.mean():.1%} of rows "
          f"({int(zero_bar.sum()):,})", flush=True)
    nz = h1[~zero_bar].copy()

    gap = daily_gap(nz)
    n_days = len(gap)
    obs = float(gap.mean())
    st = sign_test(gap.values)
    bb = block_bootstrap(gap.values, np.mean, block=2, n_boot=4000)

    # PLACEBO: shuffle the provider label WITHIN each block. Preserves the block's clearing bar,
    # the set of suggestions made, and the day structure; destroys only which provider said what.
    #
    # Vectorised as a (block x provider) matrix so the whole shuffle is one argsort. The naive
    # per-group transform is ~300 x 22k groups and does not finish; it would be far worse on the
    # 11-day archive this is headed for.
    rng = np.random.default_rng(11)
    five = TOP + BOT
    mat = nz[nz.provider.isin(five)].pivot_table(
        index="target_block", columns="provider", values="sufficient_priority", aggfunc="mean")
    keep = mat.dropna()
    print(f"  placebo matrix: {len(keep):,} blocks where all {len(five)} answered "
          f"(of {len(mat):,}; joint-availability drop {1 - len(keep) / max(len(mat), 1):.1%})",
          flush=True)
    day_of = nz.groupby("target_block").day.first().reindex(keep.index).values
    V = keep[five].to_numpy(dtype=float)
    ti = list(range(len(TOP)))
    bi = list(range(len(TOP), len(five)))
    days = pd.factorize(day_of)[0]
    nd = days.max() + 1

    def gap_of(M):
        per_block = M[:, ti].mean(1) - M[:, bi].mean(1)
        return float(np.mean([per_block[days == d].mean() for d in range(nd)]))

    obs_mat = gap_of(V)

    def draw():
        order = np.argsort(rng.random(V.shape), axis=1)
        return gap_of(np.take_along_axis(V, order, axis=1))

    pl = permutation_test(obs_mat, draw, n=2000, rng=rng)
    print(f"  (matrix contrast {obs_mat:+.4f} vs panel contrast {obs:+.4f})", flush=True)

    # MECHANISM GATE: sufficiency must rise monotonically with the size of the suggestion.
    q = pd.qcut(nz.predicted_priority_gwei.rank(method="first"), 5, labels=False)
    dose = nz.groupby(q).sufficient_priority.mean().tolist()
    gate = monotone(dose)

    print(f"\n  per-day gap (top-3 minus bottom-2, zero-bar excluded), n={n_days} days:")
    print("   " + gap.round(4).to_string().replace("\n", "\n   "))
    print(f"\n  mean gap          {obs:+.4f}")
    print(f"  days positive     {st['n_pos']}/{st['n']}")
    print(f"  sign test p       {st['p']:.5f}  (two-sided; floor for n={st['n']} is "
          f"{2 * 0.5 ** st['n']:.5f})")
    print(f"  block bootstrap   p={bb['p']:.4f}  95% CI [{bb['lo']:+.4f}, {bb['hi']:+.4f}]  "
          f"se={bb['se']:.4f}")
    print(f"  placebo           p={pl['p']:.4f}  null band [{pl['lo']:+.4f}, {pl['hi']:+.4f}]  "
          f"MDE={pl['mde']:.4f}")
    print(f"  mechanism gate    {'PASS' if gate else 'FAIL'}  dose-response "
          f"{[round(d, 3) for d in dose]}")

    one_sided = st["p"] / 2
    clears = one_sided <= HLZ_P
    print(f"\n  pre-registered bar p<={HLZ_P} (t>3, HLZ). one-sided sign p = {one_sided:.5f}")
    print(f"  VERDICT: {'CLEARS' if clears else 'DOES NOT CLEAR'} the bar. "
          f"{'' if clears else f'A unanimous {n_days}-day test tops out at {0.5 ** n_days:.5f}; '}"
          f"{'' if clears else 'the first n that can clear it is 10.'}")
    return {"n_days": n_days, "gap": obs, "sign_p": st["p"], "one_sided": one_sided,
            "placebo_p": pl["p"], "mde": pl["mde"], "gate": gate, "clears": clears,
            "boot_p": bb["p"], "boot_lo": bb["lo"], "boot_hi": bb["hi"]}


if __name__ == "__main__":
    run()
