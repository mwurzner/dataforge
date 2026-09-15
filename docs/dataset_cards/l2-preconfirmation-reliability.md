# L2 sequencer preconfirmation observations

Periodic observations of unsafe and safe heads on selected OP Stack networks, with subsequent checks of sampled unsafe block hashes against the chain reported by the endpoint. The panel records coverage and detected changes to previously observed blocks.

## Contents

| Table | Record |
|---|---|
| `l2_preconfirmation_checks` | A heartbeat with head and check statistics, or a detected violation |

## Using the data

`row_type` separates heartbeats from violations. Use `n_checked` and `n_failed` to assess how many checks completed before interpreting a period with no detected violations.

`lag_blocks` measures the reported gap between unsafe and safe heads. Use it only where `safe_tag_plausible` is true. Some endpoints have returned stale or inconsistent safe tags, which can produce implausibly large lags.

Compare lag with each chain's own history. Different sequencer and batch-publication policies can produce different normal levels across networks.

## Limitations

- The configured coverage includes nine OP Stack chains. Results do not establish behaviour across other rollup architectures.
- Sampling can miss a block that is proposed and replaced between observations.
- Observations depend on public endpoint responses. A mismatch warrants investigation and is not, by itself, proof of a sequencer failure or an L1 finality failure.
- An unsafe head is an early chain view, not a universal transaction-level confirmation guarantee.
- No detected violations means none were found in completed checks; it does not establish uninterrupted reliability outside those checks.
