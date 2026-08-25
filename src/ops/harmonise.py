"""Make every partition of a dataset share one schema, so the published data loads as one table.

WHY THIS EXISTS. Collectors gain columns over time -- E1 gained `dwell_seconds_local`,
`block_observed_ts` and `lag_blocks` when the clock-mixing bug was fixed; E10 gained `fee_usd`
when a fixed fee turned out to be masquerading as routing quality; E0 gains a column every time a
new collector is added. Partitions written before a change keep the old schema, and the dataset
silently becomes un-loadable as a whole: HuggingFace's viewer breaks, `load_dataset` errors, and
`pd.concat` only appears to work because it quietly fills the gaps.

Found the honest way -- by downloading the published data back and trying to load it, rather than
trusting that the upload returned 200. Three datasets were already drifting.

NULL IS THE ONLY CORRECT FILL. A missing column means the value was NOT CAPTURED for that
partition, which is different from zero and very different from false. Filling `fee_usd` with 0
would assert "no fee was charged", which is exactly the false claim the fee fix removed.

THIS ONLY EVER ADDS COLUMNS. It never drops, renames or rewrites a value that exists, so it
cannot lose data -- the worst case is a column of nulls that was always null anyway.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"


def _groups(files: list[Path]) -> dict[str, list[Path]]:
    """Split a directory into GROUPS THAT ARE ACTUALLY THE SAME TABLE.

    Run- and date-partitioned datasets use timestamp stems (2026-08-25T1657Z), so every file is
    another slice of one table and they form a single group. `universe/` is different: it holds
    ethereum_markets, ethereum_vaults, base_markets and base_vaults side by side, which are four
    DIFFERENT ENTITY TYPES. Unioning those gave a markets table spurious `vault` columns -- a
    real mistake this function exists to prevent, caught by inspecting the output rather than by
    the run succeeding.
    """
    out: dict[str, list[Path]] = {}
    for f in files:
        stem = f.stem
        temporal = len(stem) >= 10 and stem[:4].isdigit() and stem[4] == "-" and stem[7] == "-"
        out.setdefault("__partitions__" if temporal else stem, []).append(f)
    return out


def harmonise_dataset(root: Path, verbose: bool = True) -> tuple[int, int]:
    """Union the columns across a dataset's partitions and backfill the missing ones as NULL."""
    all_files = sorted(root.rglob("*.parquet"))
    fixed_total = 0
    for _name, files in _groups(all_files).items():
        fixed_total += _harmonise_group(files, verbose)
    return fixed_total, len(all_files)


def _harmonise_group(files: list[Path], verbose: bool = True) -> int:
    if len(files) < 2:
        return 0

    schemas = {}
    for f in files:
        try:
            schemas[f] = list(pd.read_parquet(f).columns)
        except Exception as exc:
            print(f"    !! unreadable, skipped: {f.name} ({exc})", flush=True)

    if not schemas:
        return 0
    # Union in first-seen order, so the newest collector's column order is preserved rather than
    # alphabetised into something unrecognisable.
    union: list[str] = []
    for cols in schemas.values():
        for c in cols:
            if c not in union:
                union.append(c)

    fixed = 0
    for f, cols in schemas.items():
        missing = [c for c in union if c not in cols]
        # ORDER COUNTS TOO. A partition holding every column in a DIFFERENT ORDER is still a
        # different schema to Arrow and to HuggingFace's viewer, and the first version of this
        # function skipped exactly those -- so it reported success while the dataset stayed
        # un-loadable. Rewrite whenever the column sequence differs, not only when one is absent.
        if not missing and list(cols) == union:
            continue
        d = pd.read_parquet(f)
        for c in missing:
            d[c] = pd.NA
        d[union].to_parquet(f, index=False)
        fixed += 1
        if verbose:
            why = (f"+{len(missing)} null column(s) {missing[:4]}"
                   f"{'...' if len(missing) > 4 else ''}") if missing else "reordered columns"
            print(f"    {f.name}: {why}", flush=True)
    return fixed


def main() -> int:
    if not DATA.exists():
        print("  no data directory", flush=True)
        return 0
    total_fixed = 0
    for ds in sorted(p for p in DATA.iterdir() if p.is_dir() and not p.name.startswith(".")):
        fixed, n = harmonise_dataset(ds)
        if n:
            status = f"{fixed} harmonised" if fixed else "already uniform"
            print(f"  {ds.name:<30} {n:>3} partitions  {status}", flush=True)
        total_fixed += fixed
    print(f"  -> {total_fixed} partitions rewritten", flush=True)

    # Verify rather than assume: every dataset must now load as a single frame.
    bad = []
    for ds in sorted(p for p in DATA.iterdir() if p.is_dir() and not p.name.startswith(".")):
        files = sorted(ds.rglob("*.parquet"))
        if len(files) < 2:
            continue
        for _n, grp in _groups(files).items():
            if len(grp) < 2:
                continue
            if len({tuple(pd.read_parquet(f).columns) for f in grp}) != 1:
                bad.append(f"{ds.name}/{_n}")
    if bad:
        print(f"  !! STILL NOT UNIFORM: {bad}", flush=True)
        return 1
    print("  all datasets load as one table", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
