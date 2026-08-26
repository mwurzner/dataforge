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
DATASETS = ["e0_run_manifest", "e1_mempool_lifecycle",
            "e1_mempool_minutely", "e1_mempool_dropped", "e3_mempool_divergence",
            "e8_btc_mempool_lifecycle", "e9_btc_mempool_divergence",
            "e10_quote_benchmark", "e11_ltc_mempool_lifecycle", "e12_onramp_quotes",
            "e13_remittance_quotes", "e14_l2_preconf", "e15_fee_estimators",
            "a1_lending_market_state", "a2_vault_state",
            "b2_stuck_markets", "b5_dormancy", "universe"]

# The rolling window applies ONLY to the sets whose accumulated history is the thing being held
# back. Windowing the state panels would withhold nothing (they are freely backfillable) while
# making the public repo less useful, so they go out at full history.
EPHEMERAL = {"e0_run_manifest", "e1_mempool_lifecycle",
             "e1_mempool_minutely", "e1_mempool_dropped", "e3_mempool_divergence",
             "e8_btc_mempool_lifecycle", "e9_btc_mempool_divergence",
             "e10_quote_benchmark", "e11_ltc_mempool_lifecycle", "e12_onramp_quotes",
             "e13_remittance_quotes", "e14_l2_preconf",
             "e15_fee_estimators"}

CARD = """---
license: odc-by
pretty_name: Bitcoin and Ethereum mempool observation panels
tags:
  - bitcoin
  - ethereum
  - mempool
  - blockchain
---

# DataForge mempool panels

Continuous observation of the Bitcoin and Ethereum mempools: when each transaction was first
seen, how long it waited, and which ones disappeared without being mined.

A confirmed transaction keeps its body forever but loses its timing, and one that is never mined
leaves no trace at all. We checked this on Bitcoin before collecting anything: the public
first-seen lookup answers while a transaction is unconfirmed and returns 0 once it is mined,
whether that was a day ago or a year ago. So it gets recorded as it happens or not at all.

The collector runs four windows of about 5.5 hours per day (roughly 22h coverage). This public
repo carries a rolling {SAMPLE_DAYS}-day window of the mempool datasets and the full history of
the contract-state panels. The full mempool history accumulates in a private repo; open a
discussion here if you want access.

## Which of these are hard to get elsewhere

Some of this is freely available from better sources. Where that is true it says so, and points
you there.

| dataset | free elsewhere? |
|---|---|
| `e8_btc_mempool_lifecycle`, `e9_btc_mempool_divergence` | Not that we could find. Public Bitcoin mempool tools (Johoe, Bitcoin Visuals, blockchain.com) publish aggregate queue size, not per-transaction lifecycle. The one per-transaction dataset we located covers Dec 2020 to Feb 2021 only. |
| `e1_*`, `e3_mempool_divergence` (Ethereum) | Yes. The [Flashbots Mempool Dumpster](https://github.com/flashbots/mempool-dumpster) publishes the same measurement daily, CC-0, since September 2023, from a wider node network. Use theirs; ours is an independent second observation. |
| `e10_quote_benchmark`, `e12_onramp_quotes`, `e13_remittance_quotes` | The live quotes are free to anyone at the moment they ask; a recorded time series was not found anywhere. On-ramp comparisons that exist are one-off blog snapshots of advertised fee schedules, not effective rates over time. |
| `e15_fee_estimators` | Not that we could find. No provider publishes a history of its own estimates, and an estimate is a function of the mempool at that instant, which no archive holds. Accuracy claims circulate; the data behind them does not. |
| `e14_l2_preconf` | Partly. The `violation` rows are scarce (a replaced unsafe block is served by no archive). The `heartbeat` lag is **not**: L2 block timestamps and L1 batch times are both permanent, so the lag series can be rebuilt afterwards. Treat the heartbeats as coverage attestation, not as a scarce series. |
| `a1_*`, `a2_*`, `b2_*`, `b5_*` (contract state) | Yes. Free archive nodes serve the same state years back; we checked at -90d, -360d and -720d on two endpoints. Kept for convenience. |

## Datasets

| name | one row is |
|---|---|
| `e8_btc_mempool_lifecycle` | a Bitcoin transaction we observed: first seen, mined height, dwell, fate |
| `e9_btc_mempool_divergence` | one provider's pending-set size and overlap at a sample instant |
| `e1_mempool_minutely` | one minute of Ethereum mempool activity: arrivals, fates, dwell quantiles |
| `e1_mempool_dropped` | an Ethereum transaction that was never mined, full detail |
| `e3_mempool_divergence` | one Ethereum node's pending view at a sample instant, versus three others |
| `e10_quote_benchmark` | one swap quote from one aggregator (LI.FI, KyberSwap, CoW), with fee split out |
| `e11_ltc_mempool_lifecycle` | a Litecoin transaction we observed, same fields as e8 (single provider, no cross-check) |
| `e12_onramp_quotes` | one retail fiat on-ramp quote (Mercuryo full quotes; Ramp reference price and fee bounds) |
| `e13_remittance_quotes` | one provider's quote on one remittance corridor: rate, fee, amount received, shortfall vs the round's best |
| `e15_fee_estimators` | one Bitcoin fee estimate from one provider at one confirmation target, with the divergence across providers at that target |
| `e14_l2_preconf` | L2 sequencer promise-keeping: heartbeats with the unsafe-to-safe lag per chain, plus a row for any sampled promise the canonical chain replaced |
| `e0_run_manifest` | one collection window: polls, failures, coverage counters |
| `a1_lending_market_state` | one Morpho market's supply/borrow/utilisation at the daily block |
| `a2_vault_state` | one ERC-4626 vault's share price and totals at the daily block |
| `b2_stuck_markets` | one market tested against the stuck signature (>=99% utilisation, rising price) |
| `b5_dormancy` | one vault tested for an unmoved share price |

Partitions are parquet, one file per collection window, under `dataset/YYYY/MM/`.

```python
from huggingface_hub import snapshot_download
import pandas as pd, glob

path = snapshot_download("SleeveZipper/dataforge-sample", repo_type="dataset",
                         allow_patterns="e8_btc_mempool_lifecycle/**")
df = pd.concat(map(pd.read_parquet, glob.glob(f"{path}/e8_btc_mempool_lifecycle/**/*.parquet",
                                              recursive=True)))
```

## Before you build on this

Most of these came out of getting something wrong first.

- `first_seen_ts` is when WE saw it, on our clock, from our own polling. The network saw it a
  little earlier. We never copy a provider's own first-seen field.
- Rows with `pre_existing = true` were already in the pool when a window started. Their true
  arrival time is unknowable, so `first_seen_ts` is null there rather than a fabricated value.
- Bitcoin `fate = "dropped"` starts 2026-08-26. Before that a classifier defect made drops
  impossible to record (a status endpoint reports `confirmed: false` for replaced and waiting
  transactions alike, and we mistook that for pendency). Partitions from 2026-08-25 are kept
  as collected rather than rewritten.
- We only call something dropped once it is missing from the chain, missing from two providers'
  pools, and missing for ten polls running. Weaker tests do not work: a plain poll-to-poll diff
  invented 642 "drops" in 200 seconds and every one we checked was still in the mempool.
- Mempools are node-local. Retention is a node policy, not a protocol rule; our provider served
  115-day-old entries while Bitcoin Core defaults to eviction after 336 hours. `e9` records how
  much two providers disagree (typically several thousand transactions at any instant).
- Bitcoin dwell times are long. Around 10 minutes for transactions that confirm quickly, but the
  pool also holds a standing backlog whose median age is over 100 days. That is Bitcoin's fee
  market, not a collection error.
- Ethereum has two dwell columns. `dwell_seconds` uses the block's own timestamp (the proposer's
  clock); `dwell_seconds_local` uses ours at observation. For timing work filter
  `lag_blocks == 0`, where the local reading is fresh.
- Ethereum is stored as per-minute aggregates plus full rows for never-mined transactions, from
  2026-08-25 onward. For per-transaction Ethereum data use the Flashbots Dumpster.
- Quote rows are what each aggregator served, which is not what you could have filled. One round
  has LI.FI 5% above the other two; read outliers as provider behaviour rather than free money.
  LI.FI quotes include their 25 bps fee, split out into `fee_usd`.
- Remittance quotes have a sparse partial precedent: the Wayback Machine holds occasional
  captures of Wise's comparison pages and at least one of the comparison API itself (2022). Those
  are scattered single points, not a panel, but this dataset is "denser than anything that
  exists" rather than "the only record".
- Remittance quotes come from Wise's own comparison feed. Coverage per corridor is whatever
  Wise compares against, competitor quotes can lag (see `date_collected`), and the publisher has
  an interest in looking cheapest. The bias is constant and visible rather than hidden, and the
  feed does publish Wise losing where it loses (3% behind Xoom on USD-MXN in our first round).
- Fee estimators disagree far more than their marketing suggests. A single sample showed a
  6.7x spread across five providers on the same six-block target. Horizons are normalised to a
  target-block bucket and the raw payload field is recorded per row, so you can check the
  normalisation rather than trust it. Paired with `e8_btc_mempool_lifecycle` this supports an
  accuracy study no public source can currently answer: given the mempool at time T, whose
  estimate actually confirmed in its promised window.
- Base (the L2) has no public mempool to observe; it runs a centralised sequencer.

## Coverage

`e0_run_manifest` records every window with poll counts and failure counts. Gaps between windows
are real and cannot be repaired afterwards; nothing is interpolated.

## License and contact

ODC-BY: use freely, attribute "DataForge (SleeveZipper)". Questions and access requests via the
discussions tab.
"""

CARD = CARD.replace("{SAMPLE_DAYS}", str(SAMPLE_DAYS))


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
    try:
        api.upload_folder(repo_id=repo, repo_type="dataset", folder_path=str(stage),
                          commit_message=f"{label}: {date.today().isoformat()}")
    except Exception as exc:
        # The workflow docstring promises this module never fails the run -- the git push
        # already succeeded, so the window is safe and HF is re-pushed in full next time.
        # Before this guard, an invalidated token would have crashed the step instead.
        print(f"  !! HF upload failed for {label}: {type(exc).__name__}: {str(exc)[:120]}",
              flush=True)
        return False
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
