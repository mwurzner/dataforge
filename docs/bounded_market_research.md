# Bounded market research

This is an observational research pilot. It does not sign, broadcast, place orders, or claim executable profits. Data stays in the existing private Hugging Face archive. No paid sources or new compute subscriptions are used.

## Storage decision, 24 September 2026

HF reports 6.66 GB for the private archive. The September 17–23 file inventory grows by 233.51 MB/day on average (range 186.68–328.00 MB), equivalent to 85.23 GB/year before changes. File growth and billed/history storage are different measures; these are projections, not a quota guarantee. HF currently includes 100 GB private storage for a free account. [HF limits](https://huggingface.co/docs/hub/storage-limits).

A live research trial produced 103 rows and 31,804 compressed bytes: 0.763 MB/day or 0.279 GB/year at 24 hourly runs. Actual results depend on provider payloads and scheduling. The publisher enforces:

- At most 256 KB for one new research snapshot.
- At most 5 MB per UTC date across new research snapshots, including manual runs.
- At most 2 GB total for this research table.
- No new research upload when the observed archive size would exceed 90 GB.
- Private visibility, no overwrite of an existing path, expected-parent commit, and verification that old file identities remain unchanged.

The daily cap is 1.825 GB/year, about 2.14% of the measured baseline growth. Ignoring history overhead and any compression savings, 93.34 GB of remaining space lasts roughly 400 days at the old rate or 391 days at baseline plus the full cap. Provider growth can change that; use actual HF billing storage as the authority. This guard stops only the optional research uploader, not established collectors. A 100 GB account-wide guarantee cannot be made while those continue uncapped.

Future E31 Bitcoin observations use Zstandard-compressed run-owned hourly files in daily subdirectories. Observations are still checkpointed every round, before the cross-run state checkpoint. Each local checkpoint appends to its current unpublished shard with atomic replacement. Completed runs publish finished shards. All original rows, round IDs and existing files are retained. No historical compaction or history squash runs.

This reduces future file growth from roughly 2,500 tiny files/day to about 100–120/day at five normal runs. The exact count depends on UTC-hour boundaries and empty tables. A bounded replay of 12 existing files reduced 309,146 bytes to 143,279 bytes without removing any rows; do not assume that 54% reduction applies to the entire archive. Storage projections conservatively count no savings.

## What the new table records

`e32_market_research/YYYY/MM/DD/<timestamp>-<run_id>.parquet` contains fixed-schema envelopes with source, kind, key, request/response timestamps, monotonic latency, HTTP status, error, source URL, schema version and a JSON payload retaining source units. Payloads over 64 KB become explicit oversized-payload error rows; a response over 8 MB aborts the bounded discovery request. No truncated payload is presented as complete.

The `Bounded market research` workflow runs hourly at minute 37 and can be dispatched manually. GitHub scheduling is best effort. This cadence targets changes lasting hours/days, not transient arbitrage. A workflow artifact preserves observations and receipts for 14 days if publication fails.

| Source | Bounded selection | Contents and interpretation |
|---|---|---|
| Deribit | BTC/ETH; three paired strikes at each of two expiries 2–90 days out; both perpetuals | Instrument definitions, five-level books, index, source timestamps, contract currency/size, funding fields returned on perpetual books. Twenty-four option books at maximum; not a full option chain. |
| Coinbase Exchange | BTC-USD and ETH-USD | Spot best bid/ask and sizes; different quote/collateral conventions from inverse Deribit contracts must be normalized before a hedge test. |
| Polymarket | Six fixed markets: one September BTC threshold plus five October Fed-rate outcomes | Exact market definitions, both binary books (ten best levels), source level counts/timestamps and fee endpoint responses; fixed IDs remain followed through closure. No silent replacement of expired markets. |
| Pendle | Top eight markets by liquidity among maturities 7–180 days away, selected from a bounded paginated universe | PT/SY/underlying identifiers, expiry, protocol, indicative yields/liquidity/fees and market-specific redemption information. These analytics are not executable size-specific quotes. Selection and pagination completeness are recorded. |

Polymarket pair checks consume the retained ten-level ask ladders at 10, 100 and 1,000 shares per outcome. Insufficient retained size yields null even if deeper liquidity exists. Output is **gross surplus before all costs**, with `fees_applied=false` and `execution_verified=false`. Each check can be reproduced from its stored books. Matching a binary payoff does not prove fills, conversion availability, collateral safety or fee-adjusted profit. In the initial live trial all 18 pair-size checks were negative before fees.

The five Fed markets are a research cohort, not automatically an approved exhaustive basket. Verify the exact rules, negative-risk conversion semantics and collateral denomination before constructing an event-level payoff proof. Markets can resolve, be disputed or stop accepting orders. The BTC cohort expires first; review and explicitly version a replacement cohort rather than dropping it from history.

Pendle universe timestamps use the actual page request/receive envelope for each selected market. Discovery completeness is recorded, but this is not a longitudinal history of every delisted or expired market. A future locked PT trade requires a size-specific quote, gas estimate, exact redemption asset and rights, adverse underlying conversion scenarios and full settlement follow-up. No indicative APY is treated as a locked USD yield.

## Improvements to existing collection

- Quote sources execute in bounded independent workers. One source exception no longer discards unrelated completed source results.
- Options error-only surfaces are handled without crashing the book selector.
- Existing market/quote panel buffers are checkpointed along with the previously covered Bitcoin/Ethereum buffers. Writes replace only the current run's local checkpoint atomically; final archive upload remains on the existing schedule. This reduces in-memory loss exposure but does not make GitHub runners persistent or eliminate collection gaps.
- Provider-level error counts and absent panels appear in workflow logs, the workflow summary and the existing manifest failure text. Detailed source errors remain in the measurement rows. No old dataset schema is expanded just to add telemetry.
- E31's observation semantics and transaction sampling remain unchanged; only future physical partitioning/compression changes.

## Research thesis and stopping rules

Latency is one source of advantage, not the only one. Small capacity, collateral availability, ability to wait, reliable protocol accounting, financing costs and willingness to bear particular risks can sustain slower opportunities. They do not guarantee positive expected returns for this account.

The useful distinction is between a **payoff identity** and an **achievable profitable trade**. A complete token set or matched option box can have a mechanically specified payoff while entry costs, execution, funding, collateral and operational risks prevent profit. Atomic smart-contract execution can condition state changes on a minimum output, but a reverted transaction can still consume gas, and competing searchers can remove the opportunity before inclusion.

Priority experiments:

1. **Maturity/redemption carry:** monitor PT discounts to the exact redeemable asset and compare with financing, gas and plausible loss scenarios. A fixed yield is often compensation for lockup and underlying/protocol risk, not an inefficiency. Do not treat a PT as a guaranteed dollar redemption.
2. **Outcome identities:** test binary complete sets and carefully validated event baskets. Use actual sizes, fee schedules, clock skew and partial-fill stress. Only a simulated profitable all-cost transaction is a candidate; the first sample contains none.
3. **Options/funding relative value:** use contractual payoff identities and realized carrying costs rather than a forecast that a mark/index spread will reverse. Inverse settlement, margin liquidation and future floating funding matter.
4. **Later extension, not deployed:** liquid-staking redemption discounts. Potential compensation comes from accepting a withdrawal wait or liquidity imbalance; queue delay, gas and slashing/finalization haircuts must be measured. Lido's documentation explicitly permits a reduced redemption amount following protocol losses. [Lido withdrawal mechanics](https://docs.lido.fi/guides/lido-tokens-integration-guide/).

Freeze hypothesis, entry rules, trade size, fee/financing assumptions, information-availability timestamps and rejection criteria before scoring future observations. Run a paper evaluation at 30 days; expand only a promising, independently validated experiment. Preserve failures and expired cohorts. Do not evaluate the Bot project's locked holdout.

Selling raw data is not automatically the better business: this pilot mainly records accessible feeds and has no established buyer demand or exclusive rights. A narrow, paid execution/fee audit with a concrete customer cost question is a more controllable revenue experiment than risking capital on unproven trades, but delivery costs and sales still determine profit. Public API access does not establish redistribution rights; keep this research private until those are resolved.

References: [Deribit collection guide](https://docs.deribit.com/articles/options-data-collection-best-practices), [Polymarket books](https://docs.polymarket.com/market-data/prices-order-books), [Polymarket fees](https://docs.polymarket.com/trading/fees), [Pendle API](https://docs.pendle.finance/pendle-v2-dev/Backend/ApiOverview), [Pendle PT/SY pricing](https://docs.pendle.finance/pendle-v2-dev/FAQ#pt-pricing-always-price-to-sy-not-asset).

## Operations

Local read-only trial: `python -m src.ops.market_research --output research-review`.

Authorized private publication: `HF_TOKEN=... python -m src.ops.market_research --publish` (the workflow supplies the existing secret; never store or print it).

Budget exhaustion fails closed and retains existing files. Source failures are archived as error rows and generate a workflow warning. An all-source outage fails the run after preserving evidence. A concurrent archive writer can reject the parent-locked upload; the artifact retains that snapshot. Publication never deletes or modifies other dataset files. No automatic account upgrade is enabled.
