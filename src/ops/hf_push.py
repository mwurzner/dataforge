"""Push collected partitions to HuggingFace: full history private, rolling sample public.

WHY HUGGINGFACE RATHER THAN THE GIT REPO. Measured volume for the ephemeral collector is ~135
MB/day (67.4 bytes/row at ~1,516 rows/min), which is ~49 GB/year. A GitHub repo becomes unusable
well before that; HF datasets are git-LFS backed and built for exactly this. It is also where data
buyers already look, so the same push serves storage and distribution.

TWO TARGETS, and the split is the product:
    dataforge-ephemeral  PRIVATE  full history -- the thing with value, because it cannot be
                                  reconstructed after the fact at any price
    dataforge-sample     PUBLIC   rolling 30 days -- the only distribution channel available; an
                                  unknown dataset sells zero copies at any price, and the window
                                  proves the series is live while withholding the accumulated part

THE GIT PUSH IS DELIBERATELY KEPT AS WELL. An HF outage during a 5.5h collection window would
otherwise cost an ephemeral window, and an ephemeral window cannot be re-collected. Two
destinations is cheap insurance against the one failure mode that is unrecoverable.
"""
from __future__ import annotations

import os
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
OWNER = os.environ.get("HF_OWNER", "SleeveZipper")
FULL_REPO = f"{OWNER}/dataforge-ephemeral"
SAMPLE_REPO = f"{OWNER}/dataforge-sample"
SAMPLE_DAYS = int(os.environ.get("DF_SAMPLE_DAYS", 30))

# Datasets whose partitions are named by RUN (ephemeral) or by DATE (state panels).
DATASETS = ["e0_run_manifest", "e1_mempool_lifecycle", "e3_mempool_divergence",
            "a1_lending_market_state", "a2_vault_state",
            "b2_stuck_markets", "b5_dormancy", "universe"]

CARD = """---
license: odc-by
pretty_name: DataForge ephemeral on-chain panels
tags:
  - ethereum
  - mempool
  - defi
  - blockchain
---

# DataForge

Daily and sub-minute snapshots of on-chain data that **cannot be reconstructed after the fact**.

## Why this exists

Most on-chain data is retroactively obtainable. Free archive endpoints serve historical contract
state going back years, and every event ever emitted is permanently queryable. We verified this
directly rather than assuming it: two independent free endpoints returned vault and market state
at -90d, -360d and -720d.

Pending transactions are different. They are gossiped across the peer-to-peer network and then
either get mined -- at which point the transaction body becomes permanent but its **timing does
not** -- or they vanish entirely, leaving no trace in any archive. No node retains a historical
mempool.

So this dataset captures the two facts the chain will never hold:

1. **When** a transaction was first seen, and how long it sat pending
2. **Which** transactions were never mined at all

## Datasets

| name | contents |
|---|---|
| `e1_mempool_lifecycle` | per-transaction first-seen, mined block, dwell seconds, and fate (`mined` / `dropped` / `still_pending`) |
| `e3_mempool_divergence` | pending-set size and overlap across four independent nodes at the same instant |
| `e0_run_manifest` | per-run telemetry: polls, failed calls, filter resets, coverage |
| `a1_lending_market_state` | Morpho Blue market state (supply, borrow, utilisation, fee) |
| `a2_vault_state` | ERC-4626 vault state (`convertToAssets`, `totalAssets`, `totalSupply`, fee) |
| `b2_stuck_markets` | markets pinned at >=99% utilisation with the phantom-accrual signature |
| `b5_dormancy` | vaults whose share price did not move -- dormant vs unobserved |

The `a*` and `b*` panels are convenience, not scarcity: the same state is obtainable from archive
nodes. Only the `e*` datasets are genuinely un-backfillable.

## Known artifacts

Documented because each has silently corrupted a real analysis:

- **Dormancy is not a zero return.** A vault whose share price has not moved is not reporting, not
  earning nothing. Roughly 27-33% of ever-funded vaults are dormant at any time.
- **Stuck markets inflate reported assets without bound.** At ~100% utilisation, interest accrues
  on debt nobody repays, so reported supply grows forever while nothing is withdrawable. Because
  the share price *rises*, detectors that look for a decline cannot see it.
- **Mempool observations are node-local.** `dropped` means absent from the observing node's pool
  and never mined in the observation window. Another node may have retained it. This is why
  `e3_mempool_divergence` exists.
- **Amounts are raw token units**, spanning ~25 orders of magnitude across a single chain. Ratios
  within a market or vault are decimal-free; sums across them are not.
- **Base has no public mempool.** It runs a centralised sequencer, so `e*` datasets are
  Ethereum-only. That is a property of the chain, not a coverage gap.

## Coverage

The ephemeral collector runs four times daily for ~5.5h each (~22h/day). Gaps are real, are
recorded in `e0_run_manifest`, and cannot be repaired -- an unobserved minute is unobservable
forever.
"""


def _partitions(dataset: str) -> list[Path]:
    root = DATA / dataset
    return sorted(root.rglob("*.parquet")) if root.exists() else []


def _stage(target: Path, since: date | None) -> dict:
    """Copy partitions into a staging tree, optionally windowed. Returns per-dataset counts."""
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)
    summary = {}
    for ds in DATASETS:
        files = _partitions(ds)
        if since is not None:
            # Partition stems are either YYYY-MM-DD or YYYY-MM-DDTHHMMZ; both sort by date.
            files = [f for f in files if f.stem[:10] >= since.isoformat()]
        if not files:
            continue
        n = 0
        for f in files:
            rel = f.relative_to(DATA)
            dst = target / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(f, dst)
            n += 1
        summary[ds] = n
    (target / "README.md").write_text(CARD, encoding="utf-8")
    return summary


def push(repo: str, since: date | None, label: str) -> bool:
    from huggingface_hub import HfApi

    token = os.environ.get("HF_TOKEN")
    if not token:
        print(f"  !! HF_TOKEN not set -- skipping {label}. The git push still ran, so no "
              f"ephemeral window was lost.", flush=True)
        return False
    stage = ROOT / "dist" / f"hf_{label}"
    summary = _stage(stage, since)
    if not summary:
        print(f"  !! nothing to push for {label}", flush=True)
        return False
    api = HfApi(token=token)
    api.upload_folder(repo_id=repo, repo_type="dataset", folder_path=str(stage),
                      commit_message=f"{label}: {date.today().isoformat()}")
    total = sum(summary.values())
    print(f"  pushed {total} partitions to {repo} ({label})", flush=True)
    for k, v in sorted(summary.items()):
        print(f"      {k:<26} {v:>5} files", flush=True)
    return True


def main() -> int:
    ok_full = push(FULL_REPO, None, "full")
    ok_sample = push(SAMPLE_REPO, date.today() - timedelta(days=SAMPLE_DAYS), "sample")
    # Never fail the workflow on a push problem: the collection already succeeded and was
    # committed to git. Losing a push is recoverable; losing a window is not.
    return 0 if (ok_full or ok_sample) else 0


if __name__ == "__main__":
    raise SystemExit(main())
