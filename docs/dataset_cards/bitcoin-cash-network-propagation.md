# Bitcoin Cash network propagation

Timestamped block and transaction announcements received from connected Bitcoin Cash peers, with peer metadata and advertised relay-fee floors. These observations support analysis of announcement timing and differences between connected peers.

## Contents

| Table | Record |
|---|---|
| `e27_bch_block_propagation` | A peer's announcement of a block, timestamped on receipt |
| `e27_bch_tx_propagation` | A retained transaction announcement from a peer, timestamped on receipt |
| `e27_bch_relay_floor` | A peer's advertised minimum relay fee at the time it was received |
| `e27_bch_p2p_peers` | Peer address, user agent, services and connection or handshake state |

## Timing and sampling

Compare receipt times for the same block or transaction across peers. The earliest recorded announcement is a reference within this observer's sample; it is not the original broadcast time. Multiple peer announcements of the same block should not be counted as separate blocks.

The configured collector does not subsample transaction announcements. It retains announcements received during active connections; this does not imply complete network coverage.

Bitcoin Cash peers are obtained from DNS seeds. Comparisons with Bitcoin should account for differences in network activity, peer discovery and node policies.

## Limitations

- Measurements come from one collection location and include network latency, peer announcement policies and connection state. Transaction-announcement timing is not a pure measure of network transit time.
- Peer availability changes during collection. Use the peer table to distinguish missing observations from delayed announcements.
- User agents are self-reported and should not be treated as verified software or chain identities.
- Advertised relay floors describe connected peers' policies. They do not establish a network-wide minimum fee or guarantee miner acceptance.
- Short collection windows can contain few distinct blocks. Check the collection manifest and unique block counts before comparing periods or chains.
