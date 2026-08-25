"""Push collected partitions to HuggingFace: full history private, rolling sample public.

WHY HUGGINGFACE RATHER THAN THE GIT REPO. Measured volume is ~135 MB/day for E1 alone, which is
~49 GB/year. A GitHub repo becomes unusable well before that; HF datasets are git-LFS backed and
built for exactly this. It is also where data users already look, so the same push serves storage
and distribution. NOTE the free-account private ceiling is 100 GB, which E1 alone reaches in about
two years.

TWO TARGETS:
    dataforge-ephemeral  PRIVATE  full history
    dataforge-sample     PUBLIC   rolling window of the ephemeral sets, full history of the rest

WHAT IS ACTUALLY SCARCE, stated honestly because it changed:
    E8/E9 (BITCOIN)  -- no continuously-updated free per-transaction mempool archive was found.
                        Checked 2026-08-25: MempoolScape (Kaggle) is a fixed Dec'20-Feb'21 window,
                        Blocknative is EVM-only and sunset its archive in March 2025, and the free
                        Bitcoin tooling (Johoe, Bitcoin Visuals, blockchain.com) is AGGREGATE
                        mempool size, not per-transaction lifecycle. This is the scarce part.
    E1/E3 (ETHEREUM) -- NOT scarce. The Flashbots Mempool Dumpster publishes the same measurement
                        daily, free, CC-0, since Sept 2023, including never-mined transactions and
                        a per-source arrival log. Kept because it costs nothing and is a genuine
                        independent measurement, but it is not a product and must not be sold as
                        one.
    a*/b* PANELS     -- NOT scarce. Free archive endpoints serve the same contract state to -720d
                        (verified on two independent endpoints).

THE GIT PUSH IS DELIBERATELY KEPT AS WELL. An HF outage during a 5.5h collection window would
otherwise cost an ephemeral window, and an ephemeral window cannot be re-collected.
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
SAMPLE_DAYS = int(os.environ.get("DF_SAMPLE_DAYS", 7))

# Datasets whose partitions are named by RUN (ephemeral) or by DATE (state panels).
DATASETS = ["e0_run_manifest", "e1_mempool_lifecycle", "e3_mempool_divergence",
            "e8_btc_mempool_lifecycle", "e9_btc_mempool_divergence",
            "a1_lending_market_state", "a2_vault_state",
            "b2_stuck_markets", "b5_dormancy", "universe"]

# The rolling window applies ONLY to the sets whose accumulated history is the thing being held
# back. Windowing the state panels would withhold nothing (they are freely backfillable) while
# making the public repo less useful, so they go out at full history.
EPHEMERAL = {"e0_run_manifest", "e1_mempool_lifecycle", "e3_mempool_divergence",
             "e8_btc_mempool_lifecycle", "e9_btc_mempool_divergence"}

CARD = """---
license: odc-by
pretty_name: DataForge mempool lifecycle panels
tags:
  - bitcoin
  - ethereum
  - mempool
  - blockchain
---

# DataForge

Continuous observation of the **Bitcoin and Ethereum mempools**: when each transaction was first
seen, how long it waited, and which ones vanished without ever being mined.

## What is actually scarce here, and what is not

We would rather say this plainly than let a user discover it.

| dataset | scarce? |
|---|---|
| `e8_btc_mempool_lifecycle`, `e9_btc_mempool_divergence` | **Yes.** No continuously-updated free per-transaction Bitcoin mempool archive was found. The public Bitcoin tools publish mempool *size*, not per-transaction lifecycle; the one per-transaction dataset we located covers a fixed Dec 2020 - Feb 2021 window. |
| `e1_mempool_lifecycle`, `e3_mempool_divergence` | **No.** The [Flashbots Mempool Dumpster](https://github.com/flashbots/mempool-dumpster) publishes the same measurement daily, free and CC-0, since September 2023, with a wider node network than ours. Use theirs. Ours is published as an independent second observation, nothing more. |
| `a1_*`, `a2_*`, `b2_*`, `b5_*` | **No.** Free archive endpoints serve the same contract state back years; verified at -90d, -360d and -720d on two independent endpoints. Convenience only. |

## Why mempool timing cannot be reconstructed later

A confirmed transaction keeps its **body** forever and loses its **timing**. We verified this
directly on Bitcoin rather than assuming it: a first-seen timestamp is served while a transaction
is unconfirmed and returns `0` once it is mined, at one day, thirty days and a year back alike.
Transactions that are never mined leave no trace in any archive at all.

## Datasets

| name | contents |
|---|---|
| `e8_btc_mempool_lifecycle` | per-transaction first-seen, mined height, dwell seconds, and fate |
| `e9_btc_mempool_divergence` | pending-set size and overlap across independent Bitcoin providers |
| `e1_mempool_lifecycle` | the same for Ethereum |
| `e3_mempool_divergence` | pending-set size and overlap across four Ethereum nodes |
| `e0_run_manifest` | per-run telemetry: polls, failed calls, coverage |
| `a1_lending_market_state` | Morpho Blue market state (supply, borrow, utilisation, fee) |
| `a2_vault_state` | ERC-4626 vault state (`convertToAssets`, `totalAssets`, `totalSupply`, fee) |
| `b2_stuck_markets` | markets pinned at >=99% utilisation with the phantom-accrual signature |
| `b5_dormancy` | vaults whose share price did not move -- dormant vs unobserved |

## Known artifacts

Documented because each has silently corrupted a real analysis:

- **First-seen is OUR observation, not the network's.** We derive it from our own polling and never
  read a provider's own first-seen field. A transaction is first seen by the network slightly
  before we see it.
- **Transactions already pending when a run starts are marked `pre_existing` and carry a NULL
  first-seen.** Their true arrival is earlier than anything we can observe, and stamping our start
  time on them would fabricate a dwell time.
- **Two dwell measurements on Ethereum.** `dwell_seconds` uses the block's `timestamp` field, which
  is the *proposer's* clock, so it mixes two clocks and can read 0. `dwell_seconds_local` uses the
  time we observed the block, so both ends share one clock; it is fresh when `lag_blocks == 0` and
  late by roughly 12s per block above that. Filter on `lag_blocks == 0` for timing work.
- **Mempool observations are node-local.** `dropped` means absent from the observing node's pool
  and never mined in the observation window. Retention is a node policy: Bitcoin Core evicts after
  336h by default, yet our Bitcoin provider served 115-day-old entries. `e9` measures the
  disagreement between providers instead of hiding it.
- **Bitcoin dwell times are enormous.** A random 40 of the live mempool had a median age of ~107
  days, against Ethereum dwell times of seconds to minutes. Bitcoin carries a large standing
  backlog of fee-starved transactions; this is a real property, not a collection error.
- **Base has no public mempool.** It runs a centralised sequencer, so there is nothing to observe.

## Coverage

The collector runs four times daily for ~5.5h each (~22h/day). Gaps are real, are recorded in
`e0_run_manifest`, and cannot be repaired -- an unobserved minute is unobservable forever.
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
        if since is not None and ds in EPHEMERAL:
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


_last_full_summary: dict = {}


def push(repo: str, since: date | None, label: str) -> bool:
    from huggingface_hub import HfApi

    global _last_full_summary

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
    if label == "full":
        _last_full_summary = dict(summary)
    total = sum(summary.values())
    print(f"  pushed {total} partitions to {repo} ({label})", flush=True)
    for k, v in sorted(summary.items()):
        print(f"      {k:<30} {v:>5} files", flush=True)
    return True


def _receipt(ok_full: bool, summary: dict) -> None:
    """Record what was ACTUALLY uploaded. prune_git refuses to delete anything not named here,
    so a failed push can never cost the only copy of a window."""
    import json
    (DATA / ".hf_receipt.json").write_text(json.dumps({
        "pushed_at": date.today().isoformat(),
        "full_ok": bool(ok_full),
        "repo": FULL_REPO,
        "datasets": sorted(summary),
    }, indent=2), encoding="utf-8")


def main() -> int:
    ok_full = push(FULL_REPO, None, "full")
    ok_sample = push(SAMPLE_REPO, date.today() - timedelta(days=SAMPLE_DAYS), "sample")
    _receipt(ok_full, _last_full_summary)
    # Never fail the workflow on a push problem: the collection already succeeded and was
    # committed to git. Losing a push is recoverable; losing a window is not.
    return 0 if (ok_full or ok_sample) else 0


if __name__ == "__main__":
    raise SystemExit(main())
