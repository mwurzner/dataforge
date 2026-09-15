# Ethereum mempool observations

Ethereum pending-transaction activity, lifecycle observations and comparisons between provider views. The continuing panels contain minute-level summaries, transactions classified as dropped and differences between observed pending sets.

## Contents

| Table | Record |
|---|---|
| `ethereum_mempool_minute_summary` | A minute of pending-transaction activity |
| `ethereum_dropped_transactions` | A transaction classified as dropped during observation |
| `ethereum_transaction_lifecycle` | A transaction lifecycle record; this series ends on 2026-08-26 |
| `ethereum_mempool_comparison` | A comparison of pending sets from several providers |

## Using the data

Use the minute-level table for aggregate activity and the divergence table to assess differences between observed mempools. General per-transaction lifecycle collection ended on 2026-08-26; the minute-level and dropped-transaction panels continued.

For a separate source of Ethereum mempool observations, see the [Flashbots Mempool Dumpster](https://github.com/flashbots/mempool-dumpster). Measurements from different observers may differ because of connectivity, transaction visibility and collection methods.

## Limitations

- A mempool is local to a node. These observations do not cover all public or private transaction flow.
- A dropped classification requires absence from the observed pending views and a chain check after a debounce period. It describes the collector's observation window, not a guarantee that the transaction can never be mined or rebroadcast.
- Timestamps reflect the collector's observations and include polling and network delays.
- Minute-level summaries cannot recover individual transaction paths. Check the available dates before selecting a table for transaction-level work.
