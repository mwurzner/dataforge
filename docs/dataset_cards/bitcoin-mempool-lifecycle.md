# Bitcoin and Litecoin mempool lifecycle

Transaction observations covering first sight, time spent pending, confirmation and disappearance from monitored mempools. The repository also contains comparisons between Bitcoin provider views and a measure of how much of each block was visible before it arrived.

## Contents

| Table | Record |
|---|---|
| `e8_btc_mempool_lifecycle` | A Bitcoin transaction's observed lifecycle, fee information and outcome |
| `e9_btc_mempool_divergence` | A provider's pending-set size and overlap with other views |
| `e11_ltc_mempool_lifecycle` | A Litecoin transaction lifecycle observation |
| `e8_btc_block_composition` | A block's previously observed and newly seen transaction counts |

## Observation coverage

`pre_existing` identifies transactions already pending when a collection window opened. Their first-seen times do not establish when they originally entered the network.

`fee_rate_sat_vb` is populated only where a fee was obtained. Coverage changed substantially during the initial collection period, especially on 2026-08-28. Measure the non-null share within each partition and outcome group before conducting fee-based analysis. A missing fee does not by itself invalidate the other lifecycle fields.

Bitcoin dropped-transaction records begin on 2026-08-26. An earlier collection defect prevented this classification, so the absence of drops before that date is not evidence that none occurred.

## Outcomes and package fields

`dropped` requires the collector's absence checks and debounce period. `unresolved` records a disappearance whose outcome could not be established. These labels describe observations within a collection window; a transaction may later reappear or confirm. Litecoin uses a single-provider view, so it has less cross-checking than Bitcoin.

`effective_fee_rate_sat_vb` is `ancestor_fees_sat / ancestor_vsize`. Ancestor and descendant fields describe package structure when recorded, which can change as related transactions confirm. This ratio is useful context for fee analysis, but it is not a guarantee of miner selection.

## Block composition and limitations

The block-composition table excludes the coinbase and counts transactions the collector had already observed pending. Transactions first seen in a block may reflect private submission, missed polls or gaps in provider visibility. The unseen share is an upper-bound proxy for private submission, not a direct measurement.

Use `polls_so_far`, `poll_failures` and `pool_size_at_block` to assess that comparison. Mempools differ between nodes, and timestamps include polling and network delays. Zero fee rates can be valid observations; distinguish them from nulls.
