# Bitcoin mining pool templates

Timestamped Stratum job messages collected directly from Bitcoin mining pool endpoints. The data records changes in the work each endpoint sends to miners, including the previous block hash, coinbase data and clean-jobs flag.

## Contents

| Table | Record |
|---|---|
| `bitcoin_mining_pool_jobs` | A job received from a pool endpoint, with its observation time, nTime, coinbase, merkle branch count and clean-jobs flag |

## Using the data

Use `pool` to identify the endpoint and `operator` to group endpoints belonging to the same operator. Compare `observed_ts` across endpoints to study when new jobs arrived. The pool's `nTime` field uses its own clock and update policy, so it should not be used as an arrival timestamp.

The initial configuration covers eleven endpoints across ten operators. Endpoint availability varies between collection windows. Regional endpoints operated by the same pool may distribute different jobs.

## Limitations

- Observations come from one collection location and include connection latency.
- Merkle branches are stored as a count and first entry. The dataset does not contain the full transaction list for each proposed block.
- The clean-jobs flag instructs miners to discard earlier jobs. Check the previous block hash when identifying a change of chain tip.
- Endpoint coverage is a sample of mining pool activity. It does not measure each operator's full mining capacity or global response time.
