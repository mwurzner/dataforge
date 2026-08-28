"""Publish the panels to HuggingFace: three public products, one private archive.

WHY THREE REPOS AND NOT ONE. Everything used to land in a single repo called `dataforge-sample`,
which by the end held 18 unrelated datasets. No name could describe it, because a Bitcoin mempool
panel and a bank remittance panel have no reader in common: somebody hunting remittance pricing
would never open a repo with "mempool" in the title, and somebody after mempool data had to wade
past lending-market parquet to reach it. The naming problem was a packaging problem.

So the split is by AUDIENCE, not by dataset count:

    bitcoin-mempool-lifecycle   what happens to transactions before they confirm
    crypto-execution-costs      what it costs to move money, quoted and compared
    remittance-pricing-panel    what banks and money transmitters charge, corridor by corridor

The private archive keeps everything, including panels we do NOT publish because they are freely
available elsewhere (Morpho lending state, which any archive node serves). Publishing those would
add clutter to whichever product they were bolted onto without giving a reader anything they
could not already get.

WINDOWING. Public repos carry a FIXED sample of the ephemeral panels; the accumulated history
stays private. The sample does not advance, so it cannot be accumulated week by week. The run manifest is exempt and always published in full: it holds no measurements,
only what was collected and when, and a coverage record you cannot inspect is worth nothing.
"""
from __future__ import annotations

import os
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
OWNER = os.environ.get("HF_OWNER", "dataforge-labs")
ARCHIVE_REPO = f"{OWNER}/dataforge-ephemeral"          # private, everything, never browsed
# THE PUBLIC SAMPLE IS FROZEN, NOT ROLLING, and that distinction is the business model.
#
# A rolling window leaks everything. Anyone downloading once a week accumulates the whole history
# for free, because next week's window holds days last week's did not. A rolling sample withholds
# a backlog only from someone who is not paying attention, which is exactly the wrong assumption
# about the one visitor who cares enough to come back.
#
# A frozen week shows schema, coverage and quality, and returning weekly yields nothing new.
#
# RE-PINNED QUARTERLY, and that cadence is the compromise. A window frozen forever eventually
# looks abandoned: a browser arriving in December sees August files and assumes the project died.
# A window that moves weekly leaks the whole backlog. Re-pinning four times a year gives up about
# 28 days of 365 (~8%) and keeps a recent week on display.
#
# The dates are EXPLICIT rather than computed. The entire point is that the sample does not move,
# and date arithmetic against "today" is exactly how it would start moving again by accident.
# Changing two strings four times a year cannot go subtly wrong.
#
#   NEXT RE-PIN: 2026-10-01, to a representative week of September.
#   Pick a week showing both a busy and a quiet fee period, not seven flat days.
#
# The repo does not look dead between re-pins: the run manifest publishes in full on every run,
# so HF's "updated" timestamp stays current and the coverage table shows collection through
# yesterday even while the sample data itself is fixed.
SAMPLE_DAYS = int(os.environ.get("DF_SAMPLE_DAYS", 7))
SAMPLE_START = os.environ.get("DF_SAMPLE_START", "2026-08-25")
SAMPLE_END = os.environ.get("DF_SAMPLE_END", "")
# Warn once the pinned week is this old. 34 days => the 2026-08-25 pin starts
# nagging on 2026-09-28, a few days before the planned 2026-10-01 re-pin.
SAMPLE_REPIN_DAYS = int(os.environ.get("DF_SAMPLE_REPIN_DAYS", 34))


def window_age_warning() -> str | None:
    """Shout when the published sample has gone stale.

    The window is FIXED on purpose, which means nothing breaks when it goes out of date -- it
    just quietly keeps showing an old week. A frozen window that everyone has forgotten is the
    failure mode this design invites, so the run says so rather than relying on a diary entry.
    """
    from datetime import date as _d
    y, m, d = (int(x) for x in SAMPLE_START.split("-"))
    age = (_d.today() - _d(y, m, d)).days
    if age >= SAMPLE_REPIN_DAYS:
        return (f"SAMPLE WINDOW IS {age} DAYS OLD (pinned {SAMPLE_START}). Re-pin it to a "
                f"representative recent week: set DF_SAMPLE_START. The public repos MIRROR the "
                f"window, so the old week is removed automatically.")
    return None


def sample_window() -> tuple[str, str]:
    """The fixed inclusive [start, end] the public repos publish. Never derived from today."""
    from datetime import date as _d, timedelta as _t
    if SAMPLE_END:
        return SAMPLE_START, SAMPLE_END
    y, m, d = (int(x) for x in SAMPLE_START.split("-"))
    return SAMPLE_START, (_d(y, m, d) + _t(days=SAMPLE_DAYS - 1)).isoformat()

MANIFEST = "e0_run_manifest"
# Held back from the public window. The manifest is deliberately absent: it is the coverage
# attestation and is useless withheld.
WINDOWED = {
    "e1_mempool_lifecycle", "e1_mempool_minutely", "e1_mempool_dropped",
    "e3_mempool_divergence", "e8_btc_mempool_lifecycle", "e9_btc_mempool_divergence",
    "e10_quote_benchmark", "e11_ltc_mempool_lifecycle", "e12_onramp_quotes",
    "e13_remittance_quotes", "e14_l2_preconf", "e15_fee_estimators",
    "e16_dex_routes", "e17_perp_depth",
    "e18_attestation_pool", "e20_stratum_jobs_direct",
}
# Collected and archived, never published: freely available from an archive node.
ARCHIVE_ONLY = {"a1_lending_market_state", "a2_vault_state", "b2_stuck_markets",
                "b5_dormancy", "universe",
                # e19 is DELIBERATELY archive-only, and this is a legal gate rather than an
                # oversight. The stratum jobs are collected from stratum.work's public SSE feed,
                # which is their compiled database. Collecting it for our own use raises nothing;
                # REDISTRIBUTING or SELLING a systematic extraction engages the EU sui generis
                # database right, and the source repo carries no licence granting redistribution.
                # Do not move this into PRODUCTS until the maintainer (bboerst) has agreed in
                # writing, and even then credit him and 0xB10C prominently.
                "e19_stratum_jobs"}

_SHARED_TAIL = """
## Coverage

`e0_run_manifest` lists every collection window with its poll counts and failure counts, and is
published in full rather than windowed. Gaps between windows are real, cannot be filled in
afterwards, and nothing here is interpolated.

## License and contact

ODC-BY: use it freely, credit "DataForge (dataforge-labs)". Questions and requests for the full
history via the discussions tab.
"""

MINING_CARD = '''# Bitcoin mining pool templates

What each mining pool is working on, second by second. Pools push `mining.notify` jobs to their
miners: the block they are building on, the coinbase they will claim, the merkle branches, and a
clean-jobs flag meaning "drop everything, there is a new block". Jobs are replaced every few
seconds. When a block is found, every losing pool's template is gone, and no chain records it.

## The measurement

Pools working on the **same** previous block do not agree. One observation across six pools:

| pool | nTime | coinbase length |
|---|---|---|
| slushpool | 6a918a58 | 318 |
| braiins | 6a918a62 | 318 |
| f2pool | 6a918a67 | 980 |
| viabtc | 6a918a70 | 652 |
| antpool | 6a918a71 | 848 |
| poolin | 6a918a71 | 516 |

**25 seconds of nTime spread**, and coinbase structures from 318 to 980 characters. That is who
saw the new block first, and how differently each pool built on it. Note rows 1 and 2: the same
operator on two endpoints, 10 seconds apart.

## What is in here

| name | one row is |
|---|---|
| `e20_stratum_jobs_direct` | one `mining.notify` from one pool: the block it builds on, its nTime, coinbase, merkle branch count and clean-jobs flag |

## How it was collected

Directly from the pools' own stratum endpoints, one read-only connection each, subscribing as an
observer and never submitting a share. `pool` is the host we connected to rather than an identity
decoded from data, so provenance is exact. `operator` collapses hostnames belonging to one pool --
`braiins` and `slushpool` are the same operator and should usually be treated as one.

**No Bitcoin address is used anywhere in this collection.** Two pools serve jobs on subscribe
alone; four accept a plain worker name. Two further pools (solo.ckpool.org, public-pool.io)
require a valid payout address and are therefore EXCLUDED rather than handed an invented one.

## Before you build on this

- **nTime is the pool's own clock**, not our receive time, and pools do not all update it on the
  same cadence. It is a good indicator of when a pool rebuilt its template and a poor stopwatch.
  For arrival ordering use `observed_ts`, which is ours and consistent across pools.
- **`observed_ts` carries our network latency to each pool**, which differs by pool and by
  region. Differences of tens of milliseconds between pools are not meaningful; differences of
  seconds are.
- **Six pools, five operators.** This is not the whole network, and pools running regional
  endpoints may serve different templates elsewhere than the one we connect to.
- **A dropped connection loses jobs silently.** `pool_reconnects` and `pool_connect_failures` are
  carried on every row so a gap can be told from a quiet pool.
- Merkle branches are stored as a count plus the first entry. The count plus the coinbase
  identifies a distinct template; the full branch list is large and mostly redundant.
- `clean_jobs = true` is the interesting flag: it means the pool has switched blocks. Sorting
  those by `observed_ts` gives the propagation order across pools for each new block.
'''

ATTESTATION_CARD = '''# Ethereum attestation pool

Attestations this node saw, against the attestations that reached a block. On chain you can see
that a validator's attestation is missing. You cannot see whether it was never produced, produced
but never propagated, or propagated and then never packed by a proposer. That distinction lives
only in the pool, and the pool is not kept by anyone.

The consensus API is explicit about what it serves: attestations "known by the node but not
necessarily incorporated into any block". Every provider documents that endpoint as real-time,
and none archives it. Measured churn on a single 15-second gap: 118 attestations left the pool
and 272 appeared.

## What this is NOT

Attester effectiveness is already well served and you should use the existing tools for it.
[Rated](https://rated.network) and [beaconcha.in](https://beaconcha.in) both publish participation
rate, correctness and inclusion delay, all derived from on-chain data and all reconstructable from
an archive node. None of that is here, because none of it is scarce.

What is here is the counterfactual half: the attester slots that entered this node's pool and
never reached a block.

## What is in here

| name | one row is |
|---|---|
| `e18_attestation_pool` | one slot: attester slots seen in the pool, attester slots included on chain, and the difference |

## Reading it

`attesters_seen_in_pool` is the population count of the union of aggregation bitfields observed
for that slot, summed over committee groups. `attesters_included` is the same count taken from
the blocks that carried attestations for that slot. `attesters_net_never_included` is the first
minus the second.

**That figure is signed on purpose, and negative values are real.** A proposer can include
attestations this node never held, so a negative number means the network carried more than we
saw. Clamping it at zero would hide exactly the propagation asymmetry the column exists to show.
In a well-observed slot the two counts agree to within about 0.5% (roughly 28,160 against
28,163); the interesting rows are the ones that do not.

## Before you build on this

- **One node's view.** This is what a single consensus client held, not what the network held. An
  attestation absent here may have been present elsewhere. Treat it as a lower bound on what
  existed, never as a claim about the network. This is the same limitation as our mempool
  datasets, stated the same way.
- **It counts attester slots, not validators.** An SSZ bitfield's population count tells you how
  many attester slots it represents, not which validators they were. Resolving identity needs
  committee assignments (~8 MB per epoch, ~1.8 GB/day) fetched purely to map bit positions, which
  is not collected. So this answers "how many were left out", not "was it me".
- **The pool holds roughly the last 50 slots**, about ten minutes. An attestation that arrived and
  was included between two polls is invisible to us. `n_polls_seen` is on every row so a
  well-observed slot can be told from a glimpsed one; low values mean low confidence.
- **A failed block fetch overstates the gap**, because uncounted inclusions look like exclusions.
  `block_fetch_failures` and `missed_slots_seen` are carried on every row rather than smoothed
  away. A missed slot genuinely has no block and is counted separately from a fetch that failed.
- Slots near the end of a collection window are finalised early, so their inclusion counts are
  incomplete by construction. Prefer slots with a high `n_polls_seen`.
'''

PRODUCTS = {
    "bitcoin-mining-pool-templates": {
        "datasets": ["e20_stratum_jobs_direct", MANIFEST],
        "example": "e20_stratum_jobs_direct",
        "pretty": "What each Bitcoin mining pool is building on, second by second",
        "tags": ["bitcoin", "mining", "mining-pools", "stratum", "blockchain",
                 "propagation", "time-series"],
        "size": "100K<n<1M",
        "body": MINING_CARD,
    },
    "ethereum-attestation-pool": {
        "datasets": ["e18_attestation_pool", MANIFEST],
        "example": "e18_attestation_pool",
        "pretty": "Ethereum attestations seen in the pool but never included on chain",
        "tags": ["ethereum", "beacon-chain", "consensus", "attestations", "staking",
                 "validator", "blockchain", "time-series"],
        "size": "10K<n<100K",
        "body": ATTESTATION_CARD,
    },
    "bitcoin-mempool-lifecycle": {
        "datasets": ["e8_btc_mempool_lifecycle", "e9_btc_mempool_divergence",
                     "e11_ltc_mempool_lifecycle", "e15_fee_estimators",
                     "e1_mempool_minutely", "e1_mempool_dropped", "e1_mempool_lifecycle",
                     "e3_mempool_divergence", MANIFEST],
        "example": "e8_btc_mempool_lifecycle",
        "pretty": "Bitcoin mempool lifecycle, dwell times and fee estimates",
        "tags": ["mempool", "transaction-fees", "fee-estimation", "bitcoin", "litecoin",
                 "ethereum", "blockchain", "cryptocurrency", "time-series"],
        "size": "1M<n<10M",
        "body": """# Bitcoin mempool lifecycle

What happens to a transaction between being broadcast and being mined: when we first saw it, how
long it waited, what fee it paid, and whether it was ever mined at all.

A confirmed transaction keeps its body forever but loses its timing, and one that is never mined
leaves no trace at all. We checked this on Bitcoin before collecting anything: the public
first-seen lookup answers while a transaction is unconfirmed and returns 0 once it is mined,
whether that was a day ago or a year ago. So it gets recorded as it happens or not at all.

Bitcoin is the point of this repo. Litecoin runs the same collector. Ethereum is included as a
second observation, but if you want Ethereum specifically, the
[Flashbots Mempool Dumpster](https://github.com/flashbots/mempool-dumpster) publishes the same
measurement daily under CC-0, from a wider node network and going back to September 2023. Theirs
is better than ours; use it.

## What is in here

| name | one row is |
|---|---|
| `e8_btc_mempool_lifecycle` | a Bitcoin transaction: first seen, fee rate, mined height, blocks waited, fate |
| `e9_btc_mempool_divergence` | one provider's pending-set size and overlap with another, at an instant |
| `e11_ltc_mempool_lifecycle` | the same as e8, for Litecoin (single provider, no cross-check) |
| `e15_fee_estimators` | one fee estimate from one of five providers, at one confirmation target |
| `e1_mempool_minutely` | one minute of Ethereum mempool activity: arrivals, fates, dwell quantiles |
| `e1_mempool_dropped` | an Ethereum transaction that was never mined, in full |
| `e3_mempool_divergence` | one Ethereum node's pending view against three others |
| `e0_run_manifest` | one collection window: polls, failures, coverage counters |

## Before you build on this

Most of these came out of getting something wrong first.

- `first_seen_ts` is when WE saw it, on our clock, from our own polling. The network saw it a
  little earlier. We never copy a provider's own first-seen field.
- Rows with `pre_existing = true` were already pending when a window opened. Their real arrival
  time is unknowable, so `first_seen_ts` is null there rather than a made-up value.
- Bitcoin `fate = "dropped"` starts 2026-08-26. Before that, a bug made drops impossible to
  record: the status endpoint reports `confirmed: false` for a replaced transaction exactly as it
  does for a waiting one, and we read that as proof it was still pending. Earlier partitions are
  kept as collected rather than rewritten.
- We only call something dropped once it is missing from the chain, missing from two providers'
  pools, and missing for ten polls running. Weaker tests do not work: a plain poll-to-poll diff
  invented 642 "drops" in 200 seconds and every one we checked was still in the mempool.
- `fee_rate_sat_vb` is PARTIAL, and its coverage IMPROVES over the life of the dataset. It is
  sampled from the recent-arrivals feed, which returns only the ten newest transactions per call,
  so coverage is bounded by how often we call it. It is never estimated for the rest, because a
  guessed fee rate would ruin the analyses the column exists for.
  Measured coverage, as a share of arrivals we actually observed (`pre_existing == False`):
  **2026-08-25 none, 2026-08-26 ~4%, 2026-08-27 ~14%, 2026-08-28 ~16% early, ~75% late.**
  Coverage of PRE-EXISTING transactions -- which is what dropped rows inherit -- moved from
  **~1% to ~93%** on 2026-08-28. Partitions from 2026-08-29 onward are far denser than anything
  earlier, and the gap is now large enough that pooling them unexamined would be a mistake.
  Four things held it down, all fixed on 2026-08-28: fee capture did not exist on the first day;
  the quote collectors shared a thread with the sampler and blocked it for ~40 minutes of every
  4.5-hour run; the sampler's intended 2-second cadence never actually ran, because it was
  checked inside a loop that sleeps 5 seconds; and fees were fetched one transaction at a time
  when a full node returns its ENTIRE mempool, with an exact fee and vsize per entry, in a single
  request. That last one is what mattered, and the lesson is which node you ask: public Bitcoin
  RPC nodes hold wildly different mempools, and one with a 4 GB `maxmempool` covers 93% of our
  tracked set in a single call where a 256 MB node covers 34%.
  **Even repaired, 100% is not attainable, and the reason is worth understanding before you rely
  on this column.** The feed returns only the TEN NEWEST transactions per call, so capture is
  capped at 10 per poll however fast we ask. Measured Bitcoin arrival rates ran between 4.3/s and
  11.0/s within a single hour. At a 2-second cadence the ceiling is 5/s, which is above the quiet
  rate and well below the busy one -- so coverage is itself a function of network congestion, and
  is LOWEST exactly when the mempool is most interesting. Treat the sampled set as a sample, and
  do not assume it is representative across congestion regimes without checking.
  DO NOT take these figures on trust -- they are computable from the data itself, and any
  analysis sensitive to coverage should compute them per partition rather than assume a constant:
  ```python
  obs = df[~df.pre_existing.astype(bool)]
  coverage = obs.fee_rate_sat_vb.notna().mean()
  ```
  Rows without a fee rate are complete in every other respect (first seen, dwell, fate, blocks
  waited), so they remain usable for lifecycle work; only fee-conditioned analysis is affected.
- **`fee_rate_sat_vb` is effectively ABSENT on dropped rows (~1%), and this is structural.**
  Every dropped transaction observed so far was already in the mempool when its run began
  (`pre_existing == True`), which follows from the mechanism: a transaction we watch from arrival
  is mined or still pending within a 4.5-hour window, whereas eviction takes far longer, so only
  transactions that predate the run live long enough to be dropped. Fees come from the
  recent-ARRIVALS feed, which by definition never saw them.
  Recovering the fee afterwards does not work, and this was tested rather than assumed: a
  transaction that leaves the mempool unmined returns **HTTP 404 immediately** from `/tx/{id}`.
  A run's confirmed drops were probed and **0 of 147** were retrievable. The provider forgets
  them at once, so there is no window in which to ask.
  Related trap, worth knowing if you query the API yourself: `/tx/{id}/status` answers
  `{"confirmed": false}` for txids that CANNOT EXIST (all-zeros, all-f's were both tested), so
  that field distinguishes "not in a block" and nothing more. Our drop classification does not
  rest on it -- pool membership is the discriminator -- but a naive reading of it would be wrong.
- Four columns come from a full Bitcoin node's mempool rather than an explorer API, and are
  **null for transactions that node did not hold** (~32% coverage, the node's share of the
  tracked set). None of them survives confirmation or eviction, which is why they are collected:
  `node_first_seen_ts` (that node's own first-seen clock, an independent check on ours),
  `rbf_signalled` (BIP-125 replaceability as live mempool state), and `ancestor_count` /
  `descendant_count` (unconfirmed chains, which is how CPFP fee-bumping appears).
  A caveat on `node_first_seen_ts` that we have not explained: on transactions where both clocks
  exist, ours is a median ~7,000s LATER than the node's. Some of that is our polling interval and
  some is rebroadcast, but not all of it, so treat the two clocks as different measurements
  rather than one corrected version of the other.
- A fee rate of exactly 0 is real and rare, about 1 in 6,000 of the sampled arrivals, and most of
  those we have seen went on to confirm. Filter on `fee_rate_sat_vb > 0` if a zero would break
  your arithmetic, rather than treating it as a decode fault.
- Mempools are node-local, and how long a node keeps things is its own choice rather than a
  protocol rule. Ours served 115-day-old entries where Bitcoin Core would have evicted after 336
  hours. `e9` tracks how far apart two providers are, usually several thousand transactions.
- Bitcoin dwell times are long. Around 10 minutes for transactions that confirm quickly, but the
  pool also holds a standing backlog whose median age is over 100 days. That is Bitcoin's fee
  market, not a collection error.
- Fee estimators disagree much more than their marketing suggests: one sample showed a 6.7x
  spread across five providers on the same six-block target. Targets are normalised to a block
  count and the raw payload field is kept per row, so you can check that rather than trust it.
- Ethereum is stored as per-minute aggregates plus full rows for never-mined transactions.
""",
    },
    "crypto-execution-costs": {
        "datasets": ["e10_quote_benchmark", "e16_dex_routes", "e17_perp_depth",
                     "e12_onramp_quotes",
                     "e14_l2_preconf", MANIFEST],
        "example": "e10_quote_benchmark",
        "pretty": "Crypto execution costs: DEX quotes, fiat on-ramps, L2 confirmation",
        "tags": ["defi", "dex", "execution-cost", "slippage", "onramp", "ethereum",
                 "blockchain", "cryptocurrency", "time-series"],
        "size": "10K<n<100K",
        "body": """# Crypto execution costs

What it actually costs to move money on-chain, recorded as it was quoted. Three angles: swap
quotes from competing DEX aggregators, retail fiat on-ramp quotes, and how long an L2 takes to
turn a sequencer promise into something L1 confirms.

Quotes are computed per request against live state and stored by nobody. The live comparison is
free to anyone at the moment they ask; a record of what was quoted over time is not, as far as we
could find.

## What is in here

| name | one row is |
|---|---|
| `e10_quote_benchmark` | one swap quote from LI.FI, KyberSwap or CoW, at a fixed size, fee split out |
| `e17_perp_depth` | one order-book snapshot from a perp DEX: spread, level counts, and resting notional within 5/10/25/50/100 bps of mid |
| `e16_dex_routes` | one leg of the route a router chose: venue, pool, amount, and how many venues the trade was split across |
| `e12_onramp_quotes` | one retail fiat on-ramp quote, buy or sell |
| `e14_l2_preconf` | an L2 heartbeat (unsafe vs safe head) or a sequencer promise the chain replaced |
| `e0_run_manifest` | one collection window: polls, failures, coverage counters |

## Before you build on this

- `e16_dex_routes` records the route a router picked for a trade nobody placed. Executed swaps
  are on-chain forever; a quoted route for a hypothetical size is computed on demand and kept by
  nobody, so this maps where routable liquidity sits rather than where volume went. It surfaces
  venues too small for volume rankings, and the split widens sharply with size: one round put
  WBTC/USDC through a single venue at 0.01 BTC and ten venues at 10 BTC. Only KyberSwap exposes
  a leg breakdown; the others contribute the venue name alone.
- `e17_perp_depth` covers Aevo and Paradex, second-tier perpetual DEXs that no depth archive
  carries -- Tardis lists 64 exchanges and includes dydx, dydx-v4 and hyperliquid but neither of
  these. Neither venue serves its own history either. Rows are SNAPSHOTS at the poll interval,
  not a tick-level book, so a move that reverses between polls is invisible. Depth is cumulative
  resting notional within a band of mid; where the spread is wider than the band the value is
  correctly zero, which happens often on the thinner markets.
- Quote rows are what each aggregator served, which is not what you could have filled. One round
  has LI.FI 5% above the other two; read outliers as provider behaviour rather than free money.
- LI.FI applies a fixed fee of about 25 bps at every size. It is split into `fee_usd` precisely
  so its pricing policy is not mistaken for worse routing. Read price and fee together.
- The on-ramp panel is thin: Mercuryo answers full quotes without a key, Ramp gives a reference
  price and fee bounds, and MoonPay, Transak, Guardarian and Mt Pelerin all require keys. It is
  1.5 providers, not a market survey.
- In `e14`, only the `violation` rows are hard to get elsewhere. The heartbeat lag can be rebuilt
  after the fact from L2 block timestamps and L1 batch times, both of which are permanent, so
  treat heartbeats as coverage attestation rather than as a scarce series.
- Optimism's public endpoint serves a stale `safe` tag (tens of millions of blocks behind, where
  Base is a dozen). Rows carry `safe_tag_plausible` so that shows up instead of being published
  as a property of the rollup.
""",
    },
    "remittance-pricing-panel": {
        "datasets": ["e13_remittance_quotes", MANIFEST],
        "example": "e13_remittance_quotes",
        "pretty": "Cross-border remittance pricing, by corridor and provider",
        "tags": ["remittance", "exchange-rates", "fintech", "payments", "banking",
                 "foreign-exchange", "time-series", "finance"],
        "size": "10K<n<100K",
        "body": """# Cross-border remittance pricing

What it costs to send money across nine major corridors, quoted per provider, several times a
day. Each round captures 8 to 22 providers per corridor, including Wise, Western Union, PayPal,
Xoom, WorldRemit, Instarem and a range of retail banks, with the rate, the fee, and the amount
that would actually arrive.

The World Bank surveys remittance prices quarterly by mystery shopping. We could not find a
high-frequency record of provider pricing anywhere, and quotes change far faster than quarterly.

Worst-versus-best spread on the same transfer runs from 2.5% to 8.8% depending on the corridor.

## What is in here

| name | one row is |
|---|---|
| `e13_remittance_quotes` | one provider's quote on one corridor: rate, fee, amount received, shortfall against the best in that round |
| `e0_run_manifest` | one collection window: polls, failures, coverage counters |

## Before you build on this

- The source is Wise's own public comparison service. Which competitors appear in a corridor is
  Wise's choice, competitor quotes can lag (see `date_collected` where present), and the
  publisher has an obvious interest in looking cheap. The bias is at least constant and visible,
  and `is_wise_own_quote` marks their own rows. For what it is worth, the feed does publish Wise
  losing: it was 3% behind Xoom on USD to MXN in our first round.
- The Wayback Machine holds occasional captures of Wise's comparison pages and at least one of
  the comparison API, from 2022. Those are scattered single points rather than a panel, so this
  is denser than anything that exists rather than the only record of it.
- `shortfall_vs_best_pct` is computed within a round, so it compares providers quoted at the same
  moment rather than against a daily benchmark.
""",
    },
}


def _card(name: str, p: dict) -> str:
    tags = "\n".join(f"  - {t}" for t in p["tags"])
    repo = f"{OWNER}/{name}"
    ex = p["example"]
    _w = sample_window()
    load = (
        "\n```python\n"
        "from huggingface_hub import snapshot_download\n"
        "import pandas as pd, glob\n\n"
        f'path = snapshot_download("{repo}", repo_type="dataset",\n'
        f'                         allow_patterns="{ex}/**")\n'
        "df = pd.concat(map(pd.read_parquet,\n"
        f'                   glob.glob(f"{{path}}/{ex}/**/*.parquet", recursive=True)))\n'
        "```\n"
    )
    # QUOTED. A colon inside pretty_name ("Crypto execution costs: DEX quotes...")
    # makes the entire front matter fail to parse, and HF then applies none of the
    # metadata, silently undoing the discoverability work this block exists for.
    # Caught by validating the YAML instead of eyeballing it.
    return (f"---\nlicense: odc-by\npretty_name: \"{p['pretty']}\"\ntags:\n{tags}\n"
            f"task_categories:\n  - time-series-forecasting\n"
            f"size_categories:\n  - {p['size']}\n---\n\n"
            + p["body"]
            + f"\nPartitions are parquet, one file per collection window, under "
              f"`dataset/YYYY/MM/`. This repo carries a FIXED {SAMPLE_DAYS}-day sample "
              f"({_w[0]} to {_w[1]}) so you can check schema, coverage and quality before "
              f"asking for more. It does not advance, so there is nothing to gain by "
              f"re-downloading it. The full history is held privately, available on request.\n"
            + load + _SHARED_TAIL)


def _partitions(dataset: str) -> list[Path]:
    root = DATA / dataset
    return sorted(root.rglob("*.parquet")) if root.exists() else []


def _stage(target: Path, datasets, window, card: str | None) -> dict:
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)
    summary = {}
    for ds in datasets:
        files = _partitions(ds)
        if window is not None and ds in WINDOWED:
            lo, hi = window
            # Stems are YYYY-MM-DD or YYYY-MM-DDTHHMMZ; both compare correctly as strings.
            files = [f for f in files if lo <= f.stem[:10] <= hi]
        if not files:
            continue
        for f in files:
            dst = target / f.relative_to(DATA)
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(f, dst)
        summary[ds] = len(files)
    if card:
        (target / "README.md").write_text(card, encoding="utf-8")
    return summary


_last_archive_summary: dict = {}


def _push(repo: str, datasets, window, card, label, private: bool) -> bool:
    global _last_archive_summary
    from huggingface_hub import HfApi

    token = os.environ.get("HF_TOKEN")
    if not token:
        print(f"  !! HF_TOKEN not set, skipping {label}. The git push still ran, so no window "
              f"was lost.", flush=True)
        return False
    stage = ROOT / "dist" / f"hf_{label}"
    summary = _stage(stage, datasets, window, card)
    if not summary:
        print(f"  !! nothing to push for {label}", flush=True)
        return False
    api = HfApi(token=token)
    try:
        api.create_repo(repo_id=repo, repo_type="dataset", private=private, exist_ok=True)
        # A PUBLIC repo MIRRORS its fixed window; the archive ACCUMULATES. Without this the
        # upload is purely additive, so moving SAMPLE_START would leave the old week in place
        # and the free sample would grow with every re-pin -- which is exactly what freezing the
        # window exists to prevent. Scoped to parquet so the card is never touched, and only for
        # windowed repos (window is None for the archive, which must never be pruned).
        extra = {}
        if window is not None:
            n = sum(summary.values())
            # A PARTIAL stage is the dangerous case, not an empty one. If the data checkout were
            # incomplete, or SAMPLE_START were mistyped to a date matching almost nothing, an
            # additive upload would merely be a no-op -- but a MIRRORING upload would delete
            # published partitions. So compare against what the repo already holds and refuse to
            # shrink it drastically. Deleting collected data is the one irreversible mistake here.
            try:
                published = sum(1 for f in api.list_repo_files(repo_id=repo, repo_type="dataset")
                                if f.endswith(".parquet"))
            except Exception:
                published = 0
            if n < 1:
                raise RuntimeError("refusing to prune a public repo from an empty stage")
            if published and n < published * 0.5:
                print(f"  !! {label}: stage has {n} partitions against {published} published. "
                      f"REFUSING to mirror -- uploading additively instead. If this is a "
                      f"deliberate re-pin to a smaller window, set DF_ALLOW_SHRINK=1.",
                      flush=True)
                if os.environ.get("DF_ALLOW_SHRINK") != "1":
                    extra = {}
                else:
                    extra["delete_patterns"] = ["**/*.parquet"]
            else:
                extra["delete_patterns"] = ["**/*.parquet"]
            if extra:
                print(f"  {label}: mirroring {n} partitions for {window[0]}..{window[1]} "
                      f"(stale parquet pruned; repo held {published})", flush=True)
        api.upload_folder(repo_id=repo, repo_type="dataset", folder_path=str(stage),
                          commit_message=f"{label}: {date.today().isoformat()}", **extra)
    except Exception as exc:
        # Never fail the run on a publish problem: git already holds the window.
        print(f"  !! HF upload failed for {label}: {type(exc).__name__}: {str(exc)[:120]}",
              flush=True)
        return False
    if label == "archive":
        _last_archive_summary = dict(summary)
    print(f"  pushed {sum(summary.values())} partitions to {repo}", flush=True)
    for k, v in sorted(summary.items()):
        print(f"      {k:<30} {v:>5} files", flush=True)
    return True


def _receipt(ok: bool, summary: dict) -> None:
    import json
    (DATA / ".hf_receipt.json").write_text(json.dumps({
        "pushed_at": date.today().isoformat(), "full_ok": bool(ok),
        "repo": ARCHIVE_REPO, "datasets": sorted(summary)}, indent=2), encoding="utf-8")


def main() -> int:
    everything = sorted({d for p in PRODUCTS.values() for d in p["datasets"]} | ARCHIVE_ONLY)
    _w = window_age_warning()
    if _w:
        print("  !! " + _w, flush=True)

    ok = _push(ARCHIVE_REPO, everything, None, None, "archive", private=True)
    _receipt(ok, _last_archive_summary)

    window = sample_window()
    print(f"  public sample is FROZEN at {window[0]} to {window[1]}", flush=True)
    for name, p in PRODUCTS.items():
        _push(f"{OWNER}/{name}", p["datasets"], window, _card(name, p), name, private=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
