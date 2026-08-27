# The kill list

Every candidate dataset checked against the scarcity criterion, and why it failed.

> **The criterion.** Is it written to an immutable public record, or retained by any queryable
> archive? If yes, it is retroactively available and worthless as a product. Only data that is
> **discarded, overwritten, or never stored** qualifies.

Kept because it is the most reusable thing this project has produced. The recurring shape:
**if a dataset is valuable and accessible, a vendor has already built it.** What survives sits
where the buyer base is real but unformed. Hit rate to date: **4 built of 18 checked.**

The discipline, in order, and it is not negotiable — three of the four surviving datasets were
found this way, and the one that was not had to be withdrawn:

1. search for an existing vendor **first**
2. probe for keyless access **second**
3. measure that the data actually **varies** third
4. write a collector only then

## Killed

| # | candidate | why it failed |
|---|---|---|
| 1 | Ethereum mempool | Flashbots publishes a mempool dump under **CC-0** |
| 2 | Hyperliquid order books | venue's own S3, plus SonarX CC0, plus four vendors |
| 3 | Polymarket depth | five vendors sell it |
| 4 | CoW Swap quotes | reconstructable from Dune |
| 5 | missed-slot proposer | beaconcha.in serves it, and it is recomputable from beacon state |
| 6 | bridge quotes | two free providers, and the quotes are not comparable across them |
| 7 | RBF replacements | retained past confirmation, so not discarded |
| 8 | GPU spot pricing | gputracker, gpualpha |
| 9 | GPU availability | GPU Finder 30-day reliability, Foundry Signals 5-min polling, Cast AI history to Jan 2024 |
| 10 | EV charging status | Paren, EVSE Insights |
| 11 | energy / grid | archived by regulatory mandate, or licensed by the exchange |
| 12 | bank deposit rates | no API exists at all — a collection problem, not a scarcity one |
| 13 | exchange status pages | statuspages retain their own history |
| 14 | dYdX depth | Tardis.dev |
| 15 | weather forecast vintages | Open-Meteo **Previous Model Runs API** serves prior vintages |
| 16 | independent uptime | IsDown holds ~520k incidents |
| 17 | **npm / PyPI yanked releases** | see below |
| 18 | **Kalshi order book depth** | see below |

## 17 — npm / PyPI yanked releases (killed 2026-08-27)

Both halves fail, and the npm half fails decisively.

**PyPI retains yanked releases.** They stay in the index carrying `yanked: true` and
`yanked_reason` — a persistent field, not a discarded one. Measured: `requests` shows 2 yanked of
163 releases, `urllib3` exposes `1.25`, `1.25.1`, `2.0.0`, `2.0.1` as yanked. Nothing is lost.

**npm's unpublish history is fully replayable.** The registry's CouchDB change feed answers
`_changes?since=0` with **HTTP 200**, and unpublishes are *in* that record: of the first 400
changes replayed from sequence zero, **399 carry `deleted: true`**. Anyone can reconstruct the
entire publish/unpublish history from the beginning at any time. This is the same shape as the
Ethereum mempool kill — the thing that looked ephemeral is the thing the source deliberately
retains so that mirrors can bootstrap.

Cost to kill: about two minutes, no code written.

## 18 — Kalshi order book depth (killed 2026-08-27)

**Access passes; rarity fails.** Full multi-level depth is served keyless: 409 price levels
across 15 markets (mean 27, up to 66 levels on a single weather market), spanning BTC, ETH, NYC
temperature, Nasdaq and Fed-decision series.

**But six vendors already sell the historical archive** — Lychee, Oddpool, DepthFeed, Kalshi
BackTest, Predexon, and kalshibacktesting.com. DepthFeed's own description is the proposed design
verbatim: *"polls Kalshi's public REST orderbook continuously at full depth"* and sells
*"whole-day zstd Parquet for bulk backtests."* Predexon has recorded it tick by tick since January
2026. Same shape as Polymarket (5 vendors) and Hyperliquid.

**Two measurement errors worth recording, because both rendered as clean zeros.** The probe first
reported every book as empty across 4,000 markets. Both causes were mine: the volume field is
`volume_fp`, not `volume`, and the book is served under `orderbook_fp.no_dollars`, not
`orderbook.no`. A field name that does not exist returns `None`, and `None` formats as zero. What
caught it was dumping the raw response body instead of accepting an implausible result — a market
in Bitcoin at midday does not have an empty book.
