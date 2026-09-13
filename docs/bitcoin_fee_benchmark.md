# Bitcoin fee benchmarking collection

E31 collects observations needed to study transaction fees and confirmation delays. It never
constructs, signs or broadcasts transactions. It makes no promise that paying an observed fee
would have guaranteed another transaction's inclusion.

The independent **Bitcoin fee benchmark** Actions workflow runs five 4.5-hour windows per day,
polling every 120 seconds. Scheduled starts are best-effort: this is not continuous coverage.
The existing ephemeral workflow continues collecting its other panels.

| Dataset | Contents |
|---|---|
| `e31_btc_fee_quotes` | Five providers' raw recommendation payloads, request/response times, original units and targets, and observer chain tips bracketing the round. |
| `e31_btc_tx_observations` | A fixed hash sample of pending transactions, fees/vsize, full RPC mempool entry, arrival bounds, node admission time/height, and package changes. |
| `e31_btc_tx_events` | Confirmations checked against canonical block hashes, reorg/disagreement observations, unresolved absences, follow-up limits and run boundaries. |
| `e31_btc_collection_rounds` | Timing, node/status sources, tip hashes/heights, mempool occupancy and relay floors, successes, failures, sampling limits and observation gaps. |

These four tables go to the existing private Git data repository and private Hugging Face
`dataforge-labs/dataforge-ephemeral` archive. They are not added to the frozen public samples.
The compressed checkpoint at `data/fee_benchmark_state/state.json.gz` is retained in private
Git, including pending transactions and recent quote references. A corrupt or unsupported
checkpoint fails visibly rather than silently restarting the study. Parquet is written each
round with fixed Arrow schemas and replaced atomically before the checkpoint advances.

**Sampling and outcomes**

SHA-256 of a fixed domain prefix and txid selects approximately 1/256 transactions, independently
of fee or outcome. This is a sample of one RPC endpoint's observed pending set, not a sample of
all Bitcoin broadcasts. Transactions arriving and confirming between polls can be missed.
Changing backend nodes behind a public URL can also change the observed set.

Entries seen at startup, after a source change, or after a gap over five minutes are marked
`left_censored`: their arrival cannot be established. Otherwise arrival is bounded by the
previous request start and the current response receipt. The node's own admission time/height
is recorded separately and is not substituted for the observer clock.

`prior_quote_ids_json` references the latest available recommendation per provider/source field
before the entire arrival interval, with a maximum age of five minutes. This deliberately
discards quotes that arrived during an uncertain arrival interval. Consumers must also check
`head_stable_during_quotes`, raw units and native targets before using a quote for a specific
block-horizon analysis. Missing quote associations remain missing.

Node package information includes ancestor/descendant fees and sizes, dependencies, spent-by
links, and replaceability signalling. A changed package produces another observation. This
captures evidence relevant to CPFP. RBF signalling is not proof of a replacement, nor does
absence of the signal prove replacement is impossible. Exact replacement chains are not
inferred from disappearance; those outcomes remain unresolved.

Pending transactions survive job boundaries and are followed for up to seven days, with a cap
of 10,000 active transactions and 16 status checks per round. Capacity exclusions and gaps are
recorded. Status checks use oldest-checked-first scheduling to avoid starving old observations.
A 404, rate limit or failed request never means a transaction dropped. If a transaction remains
unresolved after seven days, an explicit right-censored event closes follow-up. A window-end
event does not close follow-up.

Confirmations require the Esplora block hash to agree with the RPC canonical hash. Delay bounds
use local observation times; a miner's block timestamp is retained separately. Integer
`blocks_since_node_admission` uses the node's recorded admission height, never minutes divided
by ten. It is node-local evidence and needs reorg/source-quality checks. The collector checks
through six confirmations, then stops tracking that transaction; deeper later reorgs will not
be detected. Sampled IDs are remembered for 30 days; cohort identity is
`(txid, origin_run_id, origin_round_id)` to distinguish any later re-enrolment.

**Use for a paid audit**

Start with standalone transactions (`ancestorcount == 1`) without observed package changes,
valid arrival bounds, comparable native fee units, earlier quotes, and adequate follow-up.
Report delay intervals and censored cases, not only confirmed survivors. Stratify by fee,
vsize, congestion and arrival block regime. One transaction contributes one outcome even when
compared with several providers or targets; do not treat all those comparisons as independent.

This observational panel cannot identify which estimator a sender used or prove savings from
switching estimators. A customer-level validation also needs the customer's recorded decision
time, recommendation received, chosen fee, txid, size, service deadline, bump/CPFP decisions,
and actual paid fees. Do not collect wallet credentials. A controlled broadcast experiment
would need a separate design and authorization to spend fees; this collector has no such path.

E28 remains a historical percentile proxy for compatibility. Its `sufficient` field does not
prove inclusion; new records explicitly carry that qualification. E15 retains its legacy
columns while adding request/response provenance and native target labels. New E31 data must
not be spliced into old time-based E28 scoring as though the old observations were equivalent.

**Running and checking**

```bash
python -m unittest discover -s tests -v
python -m src.ops.btc_fee_benchmark --duration 180 --data-dir data/benchmark-smoke
python -m src.ops.btc_fee_benchmark --duration 16200
```

The archive command `python -m src.ops.btc_fee_benchmark --archive` requires the existing write
token in `HF_TOKEN` and refuses a public destination. Collection needs no credentials.
Stop with Ctrl+C or SIGTERM to flush run-boundary observations. A round with fewer than two
successful quote providers is marked unhealthy; a job with no healthy rounds fails after
preserving its data. Inspect the round table and quote errors to detect partial outages.

Checkpoint files are saved locally every round and pushed by the workflow at the end of its
window. Runner loss before the push can still lose that window: local checkpoints protect
against process interruptions, not destruction of the runner. Git is the authoritative resume
state; HF holds observation tables, not the mutable checkpoint. Do not run multiple collectors
against the same checkpoint outside the workflow's concurrency guard.

API semantics: [Bitcoin Core mempool entries](https://bitcoincore.org/en/doc/23.0.0/rpc/blockchain/getrawmempool/),
[mempool limits and fee floors](https://bitcoincore.org/en/doc/30.0.0/rpc/blockchain/getmempoolinfo/),
[Esplora transaction status](https://github.com/blockstream/esplora/blob/master/API.md).
