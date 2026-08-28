# The kill list

Every candidate dataset checked against the scarcity criterion, and why it failed.

> **The criterion.** Is it written to an immutable public record, or retained by any queryable
> archive? If yes, it is retroactively available and worthless as a product. Only data that is
> **discarded, overwritten, or never stored** qualifies.

Kept because it is the most reusable thing this project has produced. The recurring shape:
**if a dataset is valuable and accessible, a vendor has already built it.** What survives sits
where the buyer base is real but unformed. Hit rate to date: **8 built of 26 checked.**

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
| 19 | **stablecoin issuer mint/redeem latency** | see below |
| 20 | **stock borrow fees / short availability** | see below |
| 21 | **Bitcoin block templates** (what a node WOULD mine) | see below |
| 22 | node peer topology (`getpeerinfo`) | gateway returns **501 Not Implemented**; no keyless source |
| 23 | **MEV-Boost relay bids** (incl. losing bids) | Flashbots' own **relayscan.io Bid Archive** publishes all of it, monthly, back to 2024 |
| 24 | **Stratum mining-pool job data** | **UNBLOCKED and BUILT** via a third-party public feed; see below |
| 25 | **Rescue archiving** (buy/keep data before it disappears) | structurally unsellable -- see below |
| 26 | **EIP-4844 blob sidecars** | blobscan indexes blobs from the **2024-03-13 Dencun activation** with the payloads stored; fully backfillable |

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

## 19 — stablecoin issuer mint/redeem latency (killed 2026-08-27)

**Fails on observability, not on rarity — the dataset cannot be built at all.**

The premise was sound: issuer-side processing time is neither on-chain nor published, so nobody
holds it. That is true, and it is exactly why it is uncollectable. Latency needs two timestamps.
The **request** is submitted through a private issuer portal and is never visible to us; only the
**fulfilment** is, as an on-chain `Transfer` from or to the zero address — and that is an event,
so it is backfillable by anyone. One observable timestamp of the two means no latency.

Probed to confirm no keyless surface exists: Circle's account endpoints return **401**, and the
only public responses are a ping and reserve totals (Paxos markets, Tether transparency) — balance
sheets, not queue state.

Same class as candidate 12, bank deposit rates: the obstacle is collection, not competition.

## 20 — stock borrow fees / short availability (killed 2026-08-27)

Picked as the B6 non-crypto wildcard, on the reasoning that borrow rates are *quote-shaped* — the
same shape that makes the remittance panel work — and that borrow cost is among the highest-value
non-crypto signals that is plausibly free.

**Dead on both axes simultaneously, which no other candidate managed.**

*The source retains it:* IBKR's own Short Stock Availability Tool exposes historical indicative
borrow rates and exports them as CSV.

*And vendors cover it:* QuantRocket archives IBKR borrow fees back to **2018**, updated daily;
IBorrowDesk keeps historical charts of the same feed; OptionMetrics IvyDB Borrow Rate runs from
**January 2016** across the OPRA universe.

The file that prompted the idea — IBKR's `ftp3.interactivebrokers.com` shortable-securities dump —
*is* genuinely ephemeral, and IBKR keeps no history of it. That turned out not to matter: a file
being overwritten is irrelevant when the same numbers are published, and archived, in three other
places. **Ephemerality of one delivery channel is not scarcity of the data.** Worth keeping as a
distinct failure mode from the others on this list.

Cost to kill: one search.

## 21 — Bitcoin block templates (killed 2026-08-28)

A node's `getblocktemplate` returns the exact block it WOULD mine right now -- 6,337
transactions with fees when probed. That is a genuine counterfactual: comparing it against the
block miners actually produced exposes selection deviation, out-of-band payment and acceleration.
Nobody stores a template, so it looked strong.

**mempool.space already computes and archives the comparison.**
`/api/v1/block/{hash}/audit-summary` returns `expectedFees`, `expectedWeight`, `addedTxs`,
`missingTxs`, `freshTxs`, `matchRate`, plus `acceleratedTxs`, `prioritizedTxs`, `fullrbfTxs` and
`sigopTxs`. And it is RETAINED: probed at heights 964000, 950000, 900000 and **800000** (mid-2023),
all returning a full audit.

The template itself is ephemeral; the ANSWER it would give is archived. Same shape as candidate 20
-- the ephemerality of an input does not matter when the output is already published.

Cost to kill: four minutes.

## 23 — MEV-Boost relay bids (killed 2026-08-28)

Relays receive many builder bids per slot and only one wins, so the LOSING bids never touch the
chain and relays retain them only briefly. A textbook fit for the criterion.

**Flashbots already publishes the whole thing.** [relayscan.io's Bid Archive](https://bidarchive.relayscan.io/)
is "a full, public archive of bids across relays", organised by month and running from 2024
through 2025, and the collector is open source.

## THE SCREENING SHORTCUT THIS REVEALS

This is the SECOND idea killed by Flashbots specifically -- candidate 1, the Ethereum mempool,
died to their CC-0 Mempool Dumpster. That is not coincidence: **Flashbots publishes MEV-adjacent
data openly as a matter of policy**, because transparency of the MEV supply chain is their stated
mission.

So any Ethereum MEV-adjacent candidate -- mempool, bids, relays, builders, bundles, order flow --
should be checked against Flashbots FIRST, before anything else. It is the single highest-yield
check in this domain and it costs one search.

Cost to kill: one search.

## 24 — Stratum mining-pool job data (BLOCKED, then UNBLOCKED, 2026-08-28)

**This one passed both technical tests and was stopped on conduct.** Recorded separately from the
kills because permission would unblock it; the data is there and unheld.

RARITY: clean. `stratum.work` streams pool `mining.notify` messages live and stores nothing --
no archive, no export, no API, no retention policy. No historical stratum dataset found anywhere.

ACCESS: works. Two of four pools delivered full jobs within seconds, and the first sample already
showed the signal -- braiins and viabtc on the SAME previous block with `ntime` 25 seconds apart
and coinbase structures of 114 vs 218 characters. That is pool-to-pool template timing, and no
chain records it.

TERMS: **silent, which is not the same as permissive.**
  * solo.ckpool.org -- "No registration required", but asks users to minimise bandwidth.
  * Braiins -- terms cover mining operations; silent on automated access or data collection.
  * ViaBTC -- terms page does not render; a broad clause reserves the right to terminate access
    for conduct creating "problems or possible legal liabilities".

THE DECIDING CONCERN, which is about conduct rather than law: `stratum.work` does this openly as
a FREE PUBLIC TRANSPARENCY TOOL. We would hold persistent connections to commercial pools,
consume their infrastructure, contribute no hashrate, and SELL the result. A pool operator could
reasonably object to that even where the terms do not anticipate it.

TO UNBLOCK: written permission from specific pools (Braiins is developer-friendly and might well
agree), or restrict to explicitly permissive pools -- though ckpool and public-pool.io are small
solo pools, so that version answers a much weaker question, since the value is in comparing pools
with real hashrate.

A PROCESS NOTE ON MY OWN CONDUCT: the access probe authenticated with a Bitcoin address I
invented. If a fabricated address were real and a block were ever found, rewards would route to a
stranger. Any future work here uses the operator's own address or an explicitly non-address
worker name. Inventing one, even for a test, was careless.

### HOW IT WAS UNBLOCKED

The pool-terms problem existed only because WE would be the ones connecting. stratum.work already
does that collecting as a public transparency tool, and documents `GET /api/stream` "allowing for
custom integrations". Consuming that is the intended use, touches no pool, and is strictly better
data: 30 pools in ONE stream with pool names already resolved, against 2 of 4 pools reachable when
connecting directly.

Measured on the first minute: 30 pools on the same previous block with **39 seconds of ntime
spread**, and merkle-branch counts of 9, 10 and 13 -- different templates for the same block.
~26 MB/day, 6 invariants passing.

**PUBLICATION REMAINS GATED, and this is a legal gate, not caution.** Collecting for our own use
raises nothing. REDISTRIBUTING or SELLING a systematic extraction of a third party's compiled
stream engages the **EU sui generis database right**, which protects the compiler's investment
independently of copyright -- and the source repo carries **no LICENCE** granting redistribution.
The operator is in the Netherlands, so EU law applies directly.

So e19 is in `ARCHIVE_ONLY`: it accrues privately, because a day not collected is lost forever,
and it appears in NO published product until bboerst agrees in writing. Credit to him and to
0xB10C (whose idea it was) is mandatory in any published form.


| # | candidate | status |
|---|---|---|
| B4 | **DEX aggregator route composition** | **BUILT** as `e16_dex_routes` |
| B1 | **second-tier perp DEX depth** (Aevo, Paradex) | **CLEARED, not yet built** |

### B1 — second-tier perp DEX depth (cleared 2026-08-27)

**Access:** both venues serve full depth keyless. Aevo `ETH-PERP` returned **50 bids / 38 asks**;
Paradex serves live books plus an `/interactive` variant.

**The venues do not serve history.** Aevo's `orderbook-history` returns **404**. Paradex's
`/interactive` endpoint ignores time parameters entirely — passing a timestamp an hour old returns
a book stamped with the current millisecond, incrementing on each call. Current-state only.

**No vendor covers them, established from the authoritative source rather than a search summary.**
Tardis publishes its covered-exchange list as an API: of **64 exchanges**, it carries `dydx`,
`dydx-v4` and `hyperliquid` — and **not Aevo, Paradex, Vertex or Drift**. This is the pattern the
plan predicted: vendors build for the venues that lead, and the second tier goes uncovered.

**UNRESOLVED: Vertex.** Both `gateway.prod.vertexprotocol.com` and
`archive.prod.vertexprotocol.com` fail TLS handshake from this machine under both `urllib` and
`requests`. Not covered by Tardis either, but its access and history are untested — a GitHub
runner may reach what this machine cannot. Treat Vertex as unknown, not as cleared.

**Caveat on the rarity finding:** Tardis is one vendor, albeit the specialist for tick-level book
history and the one the plan's own kill condition names. Amberdata, Kaiko and CoinAPI were not
individually confirmed, and searching did not resolve them either way.
