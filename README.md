# DataForge

The private **Bounded market research** pilot records hourly options, spot, prediction-market and yield observations. New research storage is capped at 5 MB/day. See the [storage budget, collection limits and research plan](docs/bounded_market_research.md).

## Loading the public datasets

The [DataForge Hugging Face datasets](https://huggingface.co/dataforge-labs) use descriptive table names such as `options_surface`, `bitcoin_fee_recommendations` and `remittance_quotes`.

```python
from datasets import load_dataset

data = load_dataset("dataforge-labs/crypto-options-surface", "options_surface", split="train")
```

See the [table naming guide](docs/dataset_names.md) for the full name mapping and compatibility details. Public folders use the same descriptive names as the tables. File contents and collection are unchanged; the guide maps earlier paths and explains how to access the original layout.

Bitcoin fee recommendations and sampled real transaction outcomes are collected by the
**Bitcoin fee benchmark** workflow. See [measurement design and operating instructions](docs/bitcoin_fee_benchmark.md).

Daily snapshots of on-chain **contract state** for DeFi lending markets and ERC-4626 vaults.

## Why this exists

`eth_getLogs` is backfillable by anyone at any time, which is why event data is given away free.
`eth_call` at a historical block needs archive depth that free endpoints do not retain. **A daily
snapshot of state is therefore permanently unbuyable once the window closes.**

The substitute does not work. Deposit/Withdraw events carry both assets and shares, so share
prices look reconstructable — and the reconstruction matches contract state to a median error of
0.00046% *for vaults that actually transact*. On one real vault that recorded zero flows for 270
days, the flow-implied price read 1.000–1.030 while `convertToAssets` reported **173.94** — a 170x
divergence. An event-based rebuild is wrong by two orders of magnitude on exactly the vaults that
matter most.

## What runs

| | |
|---|---|
| `src/universe/discover.py` | weekly — what exists to be measured |
| `src/ops/daily.py` | twice daily — one pinned block per chain, all loggers |
| `src/ops/gapcheck.py` | after every run — fails loudly on a hole |

Coverage: Ethereum + Base. No API keys required.

## Design rules

1. **One pinned block per chain per run.** Every read resolves against the same integer block.
2. **Twice daily, idempotent by date.** Cron is best-effort; state cannot be backfilled.
3. **Weekly universe refresh.** A stale universe silently measures a shrinking sample.
4. **Never render a failure as a zero.** Empty partitions are refused, not written.
5. **Raw immutable, derived recomputable.**
