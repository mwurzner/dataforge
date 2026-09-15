# Bitcoin network propagation

Timestamped block and transaction announcements received from connected Bitcoin peers, with peer metadata and advertised relay-fee floors. These observations support analysis of announcement timing and differences between connected peers.

## Contents

| Table | Record |
|---|---|
| `bitcoin_block_announcements` | A peer's announcement of a block, timestamped on receipt |
| `bitcoin_transaction_announcements` | A retained transaction announcement from a peer, timestamped on receipt |
| `bitcoin_peer_relay_fees` | A peer's advertised minimum relay fee at the time it was received |
| `bitcoin_peer_connections` | Peer address, user agent, services and connection or handshake state |

## Timing and sampling

Compare receipt times for the same block or transaction across peers. The earliest recorded announcement is a reference within this observer's sample; it is not the original broadcast time. Multiple peer announcements of the same block should not be counted as separate blocks.

Transaction announcements are sampled by transaction hash at roughly one in sixty-four. The same hash rule is applied across peers, allowing comparisons for the retained transactions. This does not remove gaps caused by late connections or disconnected peers.

The Bitcoin collector targets roughly 120 concurrent peer connections.

## Limitations

- Measurements come from one collection location and include network latency, peer announcement policies and connection state. Transaction-announcement timing is not a pure measure of network transit time.
- Peer availability changes during collection. Use the peer table to distinguish missing observations from delayed announcements.
- User agents are self-reported and should not be treated as verified software or chain identities.
- Advertised relay floors describe connected peers' policies. They do not establish a network-wide minimum fee or guarantee miner acceptance.
- Short collection windows can contain few distinct blocks. Check the collection manifest and unique block counts before comparing periods or chains.
