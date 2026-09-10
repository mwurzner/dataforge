"""Shared statistical instruments. The thing this estate has never had.

Across ~154 research families every instrument was copy-pasted per study, which is how a subtly
different variant ends up in each one. These are the canonical versions, lifted from the
instances that have actually been used to kill and confirm findings:

    nw_ols            Bot/src/analysis/insider_sells_factor.py:84
    bh_fdr            Bot/src/analysis/joint_equity_test.py:115
    permutation_test  OracleRisk/src/research/base_capital.py:152-158  (the only prior variant
                      that emits p, a 95% band and an MDE together, which is what makes a null
                      interpretable rather than merely underpowered)
    block_bootstrap   NEW. No moving-block or stationary bootstrap exists anywhere in the estate;
                      serial dependence was always handled by HAC or by clustering. High-frequency
                      panels sampled every 20-30s need it.

THE P-VALUE CONVENTION IS SHARED ON PURPOSE, so numbers from different studies are comparable:
    p = max(2 * min((b <= 0).mean(), (b >= 0).mean()), 1 / n_boot)
A bootstrap can never report a p below 1/n_boot, and pretending otherwise is how a "p < 0.001"
gets quoted off 400 draws.
"""
from __future__ import annotations

from math import comb

import numpy as np


# --------------------------------------------------------------------------- regression
def nw_ols(y, X, lags: int = 5):
    """OLS with Newey-West (Bartlett) standard errors. Returns (beta, tstat).

    Adds its own intercept, so beta[0]/t[0] are the intercept and its t. Pure numpy: the estate's
    HAC helpers are split between statsmodels one-liners and this, and this one travels.
    """
    y = np.asarray(y, dtype=float).ravel()
    X = np.asarray(X, dtype=float)
    if X.ndim == 1:
        X = X[:, None]
    X = np.column_stack([np.ones(len(y)), X])
    n, k = X.shape
    if n <= k:
        return np.full(k, np.nan), np.full(k, np.nan)
    XtX_inv = np.linalg.pinv(X.T @ X)
    beta = XtX_inv @ X.T @ y
    resid = y - X @ beta
    S = (X * resid[:, None]).T @ (X * resid[:, None])
    for L in range(1, min(lags, n - 1) + 1):
        w = 1.0 - L / (lags + 1.0)
        A = (X[L:] * resid[L:, None]).T @ (X[:-L] * resid[:-L, None])
        S += w * (A + A.T)
    cov = XtX_inv @ S @ XtX_inv
    se = np.sqrt(np.maximum(np.diag(cov), 0.0))
    with np.errstate(divide="ignore", invalid="ignore"):
        t = np.where(se > 0, beta / se, np.nan)
    return beta, t


# --------------------------------------------------------------------------- multiple testing
def bh_fdr(pvals, q: float = 0.10):
    """Benjamini-Hochberg step-up. Returns a boolean mask of rejections.

    Uses the largest-passing-rank fix, so a small p is not rejected merely because a larger one
    failed. NaNs never pass. NOTE this controls the false-discovery rate across INDEPENDENT tests
    and does nothing about correlated ones -- always follow it with a statement about how
    correlated the tests actually are.
    """
    p = np.asarray(pvals, dtype=float)
    ok = np.isfinite(p)
    out = np.zeros(len(p), dtype=bool)
    if not ok.any():
        return out
    idx = np.where(ok)[0]
    order = idx[np.argsort(p[idx])]
    m = len(order)
    thresh = q * (np.arange(1, m + 1)) / m
    passed = p[order] <= thresh
    if not passed.any():
        return out
    cut = np.max(np.where(passed)[0])
    out[order[:cut + 1]] = True
    return out


# --------------------------------------------------------------------------- placebo
def permutation_test(observed: float, draw_null, n: int = 4000, rng=None) -> dict:
    """Permutation placebo. Returns p, the 95% null band, and the MDE.

    `draw_null` is a zero-argument callable returning ONE null statistic; the caller decides what
    gets shuffled, because that choice is the entire content of the test. Shuffle the thing whose
    identity the claim depends on and nothing else.

    The MDE is the 95th percentile of |null - median(null)|: an effect smaller than this could not
    have been detected, so a null below it rules out nothing. Report it beside every non-result.
    """
    rng = rng or np.random.default_rng(0)
    null = np.array([float(draw_null()) for _ in range(n)], dtype=float)
    null = null[np.isfinite(null)]
    if len(null) < 50:
        return {"p": float("nan"), "lo": float("nan"), "hi": float("nan"),
                "mde": float("nan"), "n_draws": len(null), "null_median": float("nan")}
    med = float(np.median(null))
    dev = np.abs(null - med)
    p = float((dev >= abs(observed - med)).mean())
    lo, hi = (float(x) for x in np.percentile(null, [2.5, 97.5]))
    return {"p": max(p, 1.0 / len(null)), "lo": lo, "hi": hi,
            "mde": float(np.percentile(dev, 95)), "n_draws": len(null), "null_median": med}


# --------------------------------------------------------------------------- bootstrap
def block_bootstrap(values, stat=np.mean, block: int | None = None,
                    n_boot: int = 2000, rng=None) -> dict:
    """Moving-block bootstrap for a serially dependent, time-ordered series.

    Written because the estate has none, and a high-frequency panel needs it: consecutive samples
    taken 20-30s apart are nowhere near independent, so an iid bootstrap reports a standard error
    several times too small. Default block length n**(1/3) is the usual rule of thumb; pass an
    explicit block covering the autocorrelation horizon (e.g. one day of samples) when known.
    """
    v = np.asarray(values, dtype=float)
    v = v[np.isfinite(v)]
    n = len(v)
    if n < 8:
        return {"obs": float("nan"), "p": float("nan"), "lo": float("nan"),
                "hi": float("nan"), "se": float("nan"), "n": n, "block": 0}
    rng = rng or np.random.default_rng(0)
    b = int(block or max(2, round(n ** (1 / 3))))
    b = min(b, n)
    n_blocks = int(np.ceil(n / b))
    starts_hi = n - b + 1
    draws = np.empty(n_boot, dtype=float)
    for i in range(n_boot):
        s = rng.integers(0, starts_hi, size=n_blocks)
        samp = np.concatenate([v[j:j + b] for j in s])[:n]
        draws[i] = stat(samp)
    obs = float(stat(v))
    p = 2.0 * min((draws <= 0).mean(), (draws >= 0).mean())
    lo, hi = (float(x) for x in np.percentile(draws, [2.5, 97.5]))
    return {"obs": obs, "p": max(float(p), 1.0 / n_boot), "lo": lo, "hi": hi,
            "se": float(draws.std(ddof=1)), "n": n, "block": b}


# --------------------------------------------------------------------------- day-level
def sign_test(x) -> dict:
    """Exact two-sided binomial sign test. The honest test when the unit is the DAY.

    With ~16 days a t-test on 100k rows is theatre; this asks only whether the sign is consistent
    across independent days, which is what a per-day panel can actually support.
    """
    a = np.asarray(x, dtype=float)
    a = a[np.isfinite(a) & (a != 0)]
    n = len(a)
    if n == 0:
        return {"n": 0, "n_pos": 0, "p": float("nan")}
    k = int((a > 0).sum())
    tail = sum(comb(n, i) for i in range(min(k, n - k) + 1)) / (2.0 ** n)
    return {"n": n, "n_pos": k, "p": float(min(1.0, 2.0 * tail))}


def monotone(values) -> bool:
    """Mechanism gate: is the dose-response monotone non-decreasing across ordered buckets?

    A hypothesis that passes its statistical bar without its mechanism gate is luck, not a finding.
    """
    v = [x for x in values if x == x]
    return len(v) >= 3 and all(v[i] <= v[i + 1] for i in range(len(v) - 1))
