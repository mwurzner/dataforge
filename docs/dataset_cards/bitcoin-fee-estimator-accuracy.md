# Bitcoin fee estimator comparisons

Timestamped Bitcoin fee recommendations from public providers, paired with fee-rate percentiles from sampled transactions that were mined. The panel supports comparisons of provider recommendations across confirmation targets and network conditions.

## Contents

| Table | Record |
|---|---|
| `bitcoin_fee_recommendations` | A provider's fee recommendation at one observation time and confirmation target |
| `bitcoin_fee_percentile_comparisons` | A historical recommendation compared with sampled fee percentiles for a block |

## Interpreting the scores

`sufficient` indicates whether the recommendation met the sampled `cleared_p10` fee-rate threshold. `overpay_ratio` divides the recommendation by that threshold. These are percentile comparisons: they do not prove that a transaction would have confirmed, or measure fees a customer could safely have avoided.

`cleared_p10` is calculated from fee-observed transactions classified as standalone using their recorded ancestor count. `cleared_p10_all_txs` includes the wider fee-observed sample. Package structure can change after observation, and neither percentile is a miner's admission rule.

The historical matching rule selects the latest recommendation at or before the block's observation time minus `target_blocks * 600` seconds. It approximates the target with elapsed time, rather than tracking the next N actual blocks. Use `lead_seconds` to inspect the resulting timing.

## Limitations

- Provider targets and confidence levels may differ. Comparisons require care even when targets have similar labels.
- Fee coverage is partial. Check `n_standalone` and `n_priced`; the percentiles do not describe every transaction in a block.
- Polling, network delays, changing package structure and private submissions limit conclusions about confirmation.
- Ratios are undefined where the comparison fee is zero. Missing values should not be replaced with large overpayment estimates.
- These public sample tables do not contain the separate transaction-outcome observations being collected for further benchmarking.
