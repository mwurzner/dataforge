# Ethereum gas estimator comparisons

Ethereum mainnet gas suggestions from public RPC providers, recorded alongside provider-reported head blocks and subsequent block-fee statistics. The panel supports analysis of differences in gas recommendations and their relationship to observed block conditions.

## Contents

| Table | Record |
|---|---|
| `e29_gas_estimators` | A provider's gas-price and priority-fee suggestions, with its reported head block |
| `e29_eth_block_fees` | A block's base fee, priority-fee percentiles and gas-used ratio |
| `e30_gas_estimator_accuracy` | A suggestion compared with block-fee thresholds over a specified horizon |

## Interpreting the scores

Compare providers at the same `head_block` to reduce differences caused by stale chain views. Suggestions at head N are evaluated against subsequent blocks, with `horizon_blocks` recording the comparison window.

`sufficient_priority` compares the suggested tip with the lowest 10th-percentile priority-fee threshold across that window. `sufficient_total` compares the gas-price suggestion with the lowest corresponding base-fee-plus-tip threshold. Both are proxies; neither proves that a particular transaction would have been included.

`overpay_priority` compares the suggestion with the priority-fee percentile in the final block of the horizon. Its denominator therefore differs from the window minimum used for sufficiency. A zero denominator produces a null ratio.

## Limitations

- Block reward percentiles come from `eth_feeHistory` and are weighted by gas used. They are not unweighted transaction-count percentiles.
- Block fees do not reveal a universal admission threshold. Private order flow and builder policies affect inclusion.
- The panel covers selected public endpoints on Ethereum mainnet, not all providers or Layer 2 networks.
- Suggestions are sampled periodically, so some blocks have no observation. Inspect timestamps, `head_block` and error fields before comparing providers.
- Fee units differ across raw and derived fields: raw suggestions use wei, while the comparison table uses gwei.
