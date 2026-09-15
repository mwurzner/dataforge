# Ethereum attestation pool

Slot-level observations comparing attestations visible in a consensus client's pending pool with attestations subsequently included in blocks. The panel supports analysis of local pool coverage and inclusion counts.

## Contents

| Table | Record |
|---|---|
| `e18_attestation_pool` | Attester-slot counts observed pending and included, with their difference and collection coverage |

## Using the data

`attesters_seen_in_pool` counts attester slots observed in the pending pool. `attesters_included` counts those included in blocks. `attesters_net_never_included` is the signed difference between these counts.

Negative differences can occur because blocks contain attestations the observed client did not hold during polling. Despite its name, `attesters_net_never_included` does not identify individual attestations that permanently failed to reach the chain.

Filter on `window_closed` when an analysis requires a fully observed inclusion window. Use `n_polls_seen` to assess observation coverage, and examine the distribution across slots before combining counts.

## Limitations

- The panel observes one consensus client's pool. Absence from this client does not imply absence from the network.
- Counts represent attester slots, not identified validators. They cannot establish the performance of a particular validator.
- Attestations that arrive and are included between polls may be missed.
- Slots near the end of a collection window may have incomplete inclusion counts and `window_closed = false`.
