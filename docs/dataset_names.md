# Dataset table names

The public Hugging Face repositories expose descriptive table names through dataset configurations. For example, the options repository offers `options_surface`, `options_order_book` and `collection_runs`.

```python
from datasets import load_dataset

data = load_dataset(
    "dataforge-labs/crypto-options-surface",
    "options_surface",
    split="train",
)
```

Install `datasets` to load the tables. The `train` split is the complete selected table, not a training/test allocation. Use `streaming=True` when appropriate for a larger table. The repository's primary measurement table is its default configuration; select a name explicitly to choose another table. Tables with different schemas remain separate configurations.

## Compatibility and storage

These are public table names, not filesystem migrations. Each name selects all existing Parquet partitions in its original directory. The collector IDs, raw columns, sample windows, archive paths and private checkpoints are unchanged. Existing direct file URLs and scripts using directory paths remain valid. No data is moved, copied or deleted to introduce a name.

The cards include a collapsed file-path reference. This guide lists every public table. Private-only collection tables retain their internal IDs and are not made public by this change.

| Public table name | Existing storage directory |
|---|---|
| `bitcoin_block_announcements` | `e21_btc_block_propagation/` |
| `bitcoin_block_composition` | `e8_btc_block_composition/` |
| `bitcoin_cash_block_announcements` | `e27_bch_block_propagation/` |
| `bitcoin_cash_peer_connections` | `e27_bch_p2p_peers/` |
| `bitcoin_cash_peer_relay_fees` | `e27_bch_relay_floor/` |
| `bitcoin_cash_transaction_announcements` | `e27_bch_tx_propagation/` |
| `bitcoin_fee_percentile_comparisons` | `e28_fee_estimator_accuracy/` |
| `bitcoin_fee_recommendations` | `e15_fee_estimators/` |
| `bitcoin_mempool_comparison` | `e9_btc_mempool_divergence/` |
| `bitcoin_mining_pool_jobs` | `e20_stratum_jobs_direct/` |
| `bitcoin_peer_connections` | `e21_btc_p2p_peers/` |
| `bitcoin_peer_relay_fees` | `e21_btc_relay_floor/` |
| `bitcoin_transaction_announcements` | `e21_btc_tx_propagation/` |
| `bitcoin_transaction_lifecycle` | `e8_btc_mempool_lifecycle/` |
| `collection_runs` | `e0_run_manifest/` |
| `dogecoin_block_announcements` | `e26_doge_block_propagation/` |
| `dogecoin_peer_connections` | `e26_doge_p2p_peers/` |
| `dogecoin_peer_relay_fees` | `e26_doge_relay_floor/` |
| `dogecoin_transaction_announcements` | `e26_doge_tx_propagation/` |
| `ethereum_attestation_observations` | `e18_attestation_pool/` |
| `ethereum_block_fees` | `e29_eth_block_fees/` |
| `ethereum_dropped_transactions` | `e1_mempool_dropped/` |
| `ethereum_gas_percentile_comparisons` | `e30_gas_estimator_accuracy/` |
| `ethereum_gas_recommendations` | `e29_gas_estimators/` |
| `ethereum_mempool_comparison` | `e3_mempool_divergence/` |
| `ethereum_mempool_minute_summary` | `e1_mempool_minutely/` |
| `ethereum_swap_quotes` | `e10_quote_benchmark/` |
| `ethereum_swap_routes` | `e16_dex_routes/` |
| `ethereum_transaction_lifecycle` | `e1_mempool_lifecycle/` |
| `fiat_onramp_quotes` | `e12_onramp_quotes/` |
| `l2_preconfirmation_checks` | `e14_l2_preconf/` |
| `litecoin_block_announcements` | `e25_ltc_block_propagation/` |
| `litecoin_peer_connections` | `e25_ltc_p2p_peers/` |
| `litecoin_peer_relay_fees` | `e25_ltc_relay_floor/` |
| `litecoin_transaction_announcements` | `e25_ltc_tx_propagation/` |
| `litecoin_transaction_lifecycle` | `e11_ltc_mempool_lifecycle/` |
| `options_order_book` | `e22_options_book/` |
| `options_surface` | `e22_options_surface/` |
| `perpetual_mark_and_index_prices` | `e23_perp_mark_index/` |
| `perpetual_order_book_depth` | `e17_perp_depth/` |
| `remittance_quotes` | `e13_remittance_quotes/` |
| `solana_swap_quotes` | `e24_solana_quotes/` |
| `solana_swap_routes` | `e24_solana_routes/` |

## Maintaining the names

`src/ops/dataset_names.py` is the mapping used by the card generator and README-only publisher. Adding a public table requires an explicit name. The publisher verifies that the configured paths cover every existing Parquet file exactly once before committing a card, and compares all non-README file identities after publication.

Hugging Face documents this mechanism in its [manual dataset configuration guide](https://huggingface.co/docs/hub/datasets-manual-configuration).
