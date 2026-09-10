"""Data access for the edge hunt, with the known silent-zero traps filtered at the door.

SOURCES, in descending completeness:
    archive  private dataforge-ephemeral, full history, needs HF_TOKEN
    public   the frozen per-dataset 7-day samples, no auth, never grows
"auto" prefers the archive and falls back to public, printing WHICH it used, because a study that
silently ran on 7 days when it thought it had 16 is a study with the wrong n.

THE THREE ZERO-ON-ERROR COLUMNS, from a read of the collectors. Every panel encodes failure as
null-plus-error-string except these, where a failed row still carries a literal 0 and would drag
any mean toward it:
    e10_quote_benchmark   latency_s = 0.0        on rate-limit cooldown skip rows
    e17_perp_depth        bid_levels/ask_levels  = 0 on error rows
    e22_options_book      bid_levels/ask_levels  = 0 on error rows
And two route frames carry NO error column at all (e16_dex_routes, e24_solana_routes), so failure
there is visible only as an absent row; clean() refuses to guess and says so.
"""
from __future__ import annotations

import io
import json
import os
import sys
import urllib.request
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
CACHE = ROOT / ".research_cache"
UA = {"User-Agent": "dataforge-research/1.0"}
ARCHIVE = "dataforge-labs/dataforge-ephemeral"

ZERO_ON_ERROR = {
    "e10_quote_benchmark": ["latency_s"],
    "e17_perp_depth": ["bid_levels", "ask_levels"],
    "e22_options_book": ["bid_levels", "ask_levels"],
}
NO_ERROR_COLUMN = {"e16_dex_routes", "e24_solana_routes"}


def _repo_of(dataset: str) -> str | None:
    """Which public repo publishes this dataset, from hf_push itself (single source of truth)."""
    from src.ops import hf_push as H
    for name, prod in H.PRODUCTS.items():
        if dataset in prod["datasets"]:
            return f"{H.OWNER}/{name}"
    return None


def _tree(repo: str, path: str = "", token: str | None = None):
    req = urllib.request.Request(
        f"https://huggingface.co/api/datasets/{repo}/tree/main/{path}",
        headers={**UA, **({"Authorization": f"Bearer {token}"} if token else {})})
    try:
        return json.loads(urllib.request.urlopen(req, timeout=60).read())
    except Exception:
        return []


def _parts(repo: str, dataset: str, token: str | None) -> list[str]:
    out = []
    for y in _tree(repo, dataset, token):
        if y.get("type") != "directory":
            continue
        for m in _tree(repo, y["path"], token):
            if m.get("type") != "directory":
                continue
            for f in _tree(repo, m["path"], token):
                if f["path"].endswith(".parquet"):
                    out.append(f["path"])
    return sorted(out)


def load(dataset: str, source: str = "auto", verbose: bool = True) -> pd.DataFrame:
    """Concatenate every partition of one dataset. Prints which source and how much."""
    token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGINGFACE_HUB_TOKEN")
    tries = []
    if source in ("auto", "archive") and token:
        tries.append((ARCHIVE, token))
    if source in ("auto", "public"):
        r = _repo_of(dataset)
        if r:
            tries.append((r, None))
    for repo, tok in tries:
        parts = _parts(repo, dataset, tok)
        if not parts:
            continue
        CACHE.mkdir(parents=True, exist_ok=True)
        frames = []
        for p in parts:
            c = CACHE / (repo.split("/")[-1] + "__" + p.replace("/", "_"))
            if not c.exists():
                req = urllib.request.Request(
                    f"https://huggingface.co/datasets/{repo}/resolve/main/{p}",
                    headers={**UA, **({"Authorization": f"Bearer {tok}"} if tok else {})})
                c.write_bytes(urllib.request.urlopen(req, timeout=300).read())
            frames.append(pd.read_parquet(io.BytesIO(c.read_bytes())))
        df = pd.concat(frames, ignore_index=True)
        if verbose:
            kind = "ARCHIVE (full history)" if tok else "PUBLIC SAMPLE (frozen, 7d)"
            days = sorted({p.split("/")[-1][:10] for p in parts})
            print(f"  {dataset}: {len(df):,} rows from {kind}, {len(parts)} parts, "
                  f"{len(days)} days {days[0]}..{days[-1]}", flush=True)
        return df
    if verbose:
        print(f"  {dataset}: NOT FOUND in any source "
              f"(token present: {bool(token)})", flush=True)
    return pd.DataFrame()


def clean(df: pd.DataFrame, dataset: str, verbose: bool = True) -> tuple[pd.DataFrame, dict]:
    """Drop failed rows and neutralise the zero-on-error columns. Returns (df, funnel).

    Never silently drops: the funnel is returned and printed so WHERE rows go is visible, which is
    the estate's standing rule after several studies reported a clean zero that was a failed fetch.
    """
    funnel = {"raw": len(df)}
    if not len(df):
        return df, funnel
    out = df
    if "error" in out.columns:
        bad = out["error"].notna()
        funnel["error_rows_dropped"] = int(bad.sum())
        out = out[~bad].copy()
    else:
        funnel["error_rows_dropped"] = 0
        if dataset in NO_ERROR_COLUMN:
            funnel["warning"] = ("no error column exists in this dataset; failures are visible "
                                 "only as absent rows and cannot be filtered here")
    for col in ZERO_ON_ERROR.get(dataset, []):
        if col in out.columns:
            n0 = int((out[col] == 0).sum())
            if n0:
                funnel[f"{col}_zeros_nulled"] = n0
                out.loc[out[col] == 0, col] = pd.NA
    funnel["clean"] = len(out)
    if verbose:
        print(f"    funnel {dataset}: " + ", ".join(f"{k}={v}" for k, v in funnel.items()),
              flush=True)
    return out, funnel
