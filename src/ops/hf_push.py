"""Publish the panels to HuggingFace: public products by audience, one private archive.

WHY THREE REPOS AND NOT ONE. Everything used to land in a single repo called `dataforge-sample`,
which by the end held 18 unrelated datasets. No name could describe it, because a Bitcoin mempool
panel and a bank remittance panel have no reader in common: somebody hunting remittance pricing
would never open a repo with "mempool" in the title, and somebody after mempool data had to wade
past lending-market parquet to reach it. The naming problem was a packaging problem.

So the split is by AUDIENCE, not by dataset count:

    bitcoin-mempool-lifecycle       what happens to transactions before they confirm
    bitcoin-fee-estimator-accuracy  what estimators advised against what blocks required
    crypto-execution-costs          what it costs to move money, quoted and compared
    remittance-pricing-panel        what banks and money transmitters charge, corridor by corridor
    bitcoin-mining-pool-templates   what pools are building on, and how blocks propagate
    ethereum-attestation-pool       attestations seen waiting against attestations included
    crypto-options-surface          every listed strike, priced, with its greeks
    equity-perp-price-discovery     what an equity price does when its cash market is shut
    solana-dex-execution            Solana swap quotes and the routes behind them
    litecoin-network-propagation    the same P2P measurement on a faster chain
    dogecoin-network-propagation    and on the fastest, at a block a minute
    bitcoin-cash-network-propagation  same cadence as Bitcoin, different node mix

A product is added when a collector has an audience the existing repos do not reach, not when
it produces a new table. PRODUCTS below is the authority; this list is a summary of it.

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


def _nag(msg: str) -> None:
    """Put the stale-window warning somewhere a person will actually meet it.

    Printing into the log of a GREEN run is the diary entry this design set out to avoid --
    nobody opens the output of a job that succeeded. The annotation surfaces on the run page,
    the step summary on the run itself, and the sentinel lets the workflow open an issue,
    which is the only one of the three that reaches an inbox unprompted.
    """
    nl = chr(10)
    print(f"::warning title=Sample window stale::{msg}", flush=True)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        try:
            with open(summary, "a", encoding="utf-8") as fh:
                fh.write(nl + "> [!WARNING]" + nl + "> " + msg + nl)
        except OSError:
            pass
    if os.environ.get("GITHUB_ACTIONS"):
        try:
            # Written to the CODE checkout, never into data/ -- that directory is committed
            # to the private data repo and a sentinel does not belong in published history.
            (ROOT / ".repin_needed").write_text(msg, encoding="utf-8")
        except OSError:
            pass


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
    "e16_dex_routes", "e17_perp_depth", "e28_fee_estimator_accuracy",
    "e18_attestation_pool", "e20_stratum_jobs_direct",
    "e21_btc_block_propagation", "e21_btc_p2p_peers",
    "e22_options_surface", "e22_options_book", "e21_btc_relay_floor",
    "e21_btc_tx_propagation", "e23_perp_mark_index",
    "e24_solana_quotes", "e24_solana_routes", "e8_btc_block_composition",
    "e25_ltc_block_propagation", "e25_ltc_tx_propagation",
    "e25_ltc_relay_floor", "e25_ltc_p2p_peers",
    "e26_doge_block_propagation", "e26_doge_tx_propagation",
    "e26_doge_relay_floor", "e26_doge_p2p_peers",
    "e27_bch_block_propagation", "e27_bch_tx_propagation",
    "e27_bch_relay_floor", "e27_bch_p2p_peers",
}
# Collected and archived, never published: freely available from an archive node.
ARCHIVE_ONLY = {"a1_lending_market_state", "a2_vault_state", "b2_stuck_markets",
                "b5_dormancy", "universe",
                # e19 is DELIBERATELY archive-only, and this is a legal gate rather than an
                # Behaviour here is deliberate; see the private design notes.
                "e19_stratum_jobs"}

# TERMS REGISTER, checked 2026-08-29 for every source whose OWN data we publish. Read this
# before adding a product: the gate is the source's terms, not whether the endpoint answers.
#
#   FORBIDS COLLECTION      paradex   removed from e17 and redacted below
#   REQUIRES A DATA LICENCE derive    never built; its terms cover quotes, marks and books
#   THIRD-PARTY COMPILATION stratum.work  archive-only, see ARCHIVE_ONLY
#   NO TERMS PUBLISHED      aevo      no ToS on site, app or docs; robots allows all; no key
#   NO RESTRICTION FOUND    wise      public unauthenticated comparison endpoint, ToS silent
#   NO BLANKET DISALLOW     the fee estimators, quote aggregators and on-ramps in e10/e12/e15
#
# RPC endpoints are deliberately NOT in this register. Reading Bitcoin through a provider yields
# the CHAIN's facts, which are nobody's compilation; the provider's terms govern our use of their
# service, not our right to the ledger. That distinction is why e8 is publishable and e17's
# Paradex half was not.
#
# NOT EXHAUSTIVE, and worth saying so: this prioritised by exposure rather than reading all 31
# hosts end to end. A source added later without its own check is exactly how the Paradex rows
# reached a public repo.

# Rows withheld because the source's terms forbid automated COLLECTION, not merely resale, so
# there is no archive-only compromise of the kind e19 gets. Applied at stage time and to
# HISTORICAL partitions, which is what makes it retroactive: a public repo mirrors its window,
# so rows already published disappear on the next push with no manual repo surgery.
#
# Deliberately applied to the private archive as well. Keeping a private copy of data we should
# not have gathered buys nothing and states the wrong intent. The git data repo still holds the
# raw record, so the decision stays reversible if the source ever licenses it.
#
# paradex: "You further agree not to engage in data mining, robots, scraping, or similar data
# gathering or extraction methods of content or information from the Services."
REDACTIONS = {"e17_perp_depth": ("venue", {"paradex"})}

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

What each mining pool is building on, at the moment it was building it.

Pools issue work to their miners describing the block they are extending, the coinbase they will
claim, and whether previous work should be abandoned. That work is replaced every few seconds.
Once a block is found, the templates every pool was building are gone.

## Contents

| name | one row is |
|---|---|
| `e20_stratum_jobs_direct` | one job from one pool: the block it extends, its nTime, coinbase, merkle branch count and clean-jobs flag |

## Templates

Pools building on the same block do not agree, and the disagreement is structural rather than
cosmetic. In one observation across eleven endpoints on a single block, nTime spread across 30
seconds, coinbase length ranged from 318 to 1,488 characters, and merkle branch counts split
between 10 and 13, meaning some pools were assembling materially smaller blocks than others at
the same moment. Two endpoints run by the same operator differed by 10 seconds.

`pool` is the endpoint connected to rather than an identity inferred from the data. `operator`
collapses endpoints belonging to the same operator, and rows should usually be grouped on it.

## Before you build on this

- One vantage point, roughly 120 peers out of tens of thousands reachable. This is a sample of
  the network, not the network.
- nTime is the pool's own clock and pools do not all update it on the same cadence. It indicates
  when a pool rebuilt its template. For arrival ordering use `observed_ts`, which is consistent
  across pools.
- Eleven endpoints across ten operators. Pools running regional endpoints may serve different
  work elsewhere, so this is a sample of what each operator was building, not a census of it.
- Merkle branches are stored as a count and first entry. The count with the coinbase identifies a
  distinct template; the full list is large and mostly redundant.
- `clean_jobs = true` marks a pool switching blocks. Sorting those by `observed_ts` gives the
  order in which pools reacted.
'''

ATTESTATION_CARD = '''# Ethereum attestation pool

Attestations seen waiting, against attestations that reached a block.

On chain you can see that a validator's attestation is missing. You cannot see whether it was
never produced, produced but never propagated, or propagated and never included. That difference
is only visible while the attestation is still pending.

## Contents

| name | one row is |
|---|---|
| `e18_attestation_pool` | one slot: attester slots seen waiting, attester slots included, and the difference |

## What this is not

Attester effectiveness is well served elsewhere. Participation rate, correctness and inclusion
delay are all derivable from chain data and several public tools publish them. None of that is
here, because none of it is scarce.

## Reading it

`attesters_seen_in_pool` counts attester slots observed waiting for that slot.
`attesters_included` counts those that reached a block. `attesters_net_never_included` is the
first minus the second.

The difference is signed, and negative values are ordinary: a proposer can include attestations
this vantage point never held, so a negative number means the network carried more than was seen
here. Clamping it at zero would hide the asymmetry the column exists to show.

Read the median rather than the total. Across 1,319 slots with a fully observed inclusion window
the aggregate gap is -2.0%, but that is pulled by a tail. The median slot is -0.07%, p10 is
-2.77% and p90 is -0.01%. A typical slot agrees with the chain almost exactly and a minority
accounts for nearly all of the difference.

No slot has yet shown more attesters here than on chain. If you see positive values in quantity,
suspect the collection before the network.

## Before you build on this

- One vantage point. This is what a single consensus client held, not what the network held. An
  attestation absent here may have been present elsewhere. Treat it as a lower bound.
- It counts attester slots, not validators. A bitfield population tells you how many attester
  slots it represents, not which validators they were. This answers how many were left out, not
  whether a particular validator was.
- Attestations remain visible for roughly ten minutes. One that arrived and was included between
  two observations is invisible here. `n_polls_seen` is on every row; low values mean low
  confidence.
- `window_closed` marks slots whose inclusion window was fully observed. Rows without it have
  incomplete inclusion counts by construction. Filter on it before drawing conclusions.
- Slots near the end of a collection window are finalised early and will show `window_closed`
  as false.
'''

PRODUCTS = {
    "bitcoin-mining-pool-templates": {
        "datasets": ["e20_stratum_jobs_direct", MANIFEST],
        "example": "e20_stratum_jobs_direct",
        "pretty": "What each Bitcoin mining pool is building on, second by second",
        "tags": ["bitcoin", "mining", "mining-pools", "stratum", "p2p", "network",
                 "block-propagation", "blockchain", "time-series"],
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
    "equity-perp-price-discovery": {
        "datasets": ["e23_perp_mark_index", MANIFEST],
        "example": "e23_perp_mark_index",
        "pretty": "Round-the-clock perpetual prices for equities and pre-IPO names",
        "tags": ["equities", "perpetual-futures", "price-discovery", "after-hours",
                 "pre-ipo", "basis", "finance", "time-series", "cryptocurrency"],
        "size": "1M<n<10M",
        "body": """# Equity and pre-IPO perpetual price discovery

What a price for Apple, Nvidia, OpenAI or Anthropic does when no cash market is open.

Perpetual futures on real-world assets trade around the clock, including the roughly 60% of the
week when US equities are shut. The venue publishes an index history but no mark history, so the
price it actually margins against, and the gap between that and the index, exist only if they
were recorded as they happened.

Two of these names have no public market at any hour.

## Contents

| name | one row is |
|---|---|
| `e23_perp_mark_index` | one instrument at one moment: mark, index, and the basis between them |

## Reading it

`market_type` separates the population: equity, etf, commodity, fx, pre_ipo, and crypto.
`is_rwa` marks the real-world-asset subset in one flag.

`basis_bps` is (mark - index) / index in basis points, signed, and null rather than zero where
either leg is missing. Basis points make an instrument priced at 38 comparable to one priced at
78,000.

**The crypto rows are the control, and they are included for that reason.** Crypto perpetuals
have no closed hours, so they show what this basis looks like when the underlying never stops.
Compare an equity against them rather than against zero.

The interesting window is when the cash market is shut. One Saturday sample had the whole
equity book live, with marks moving while their indices barely did.

## Before you build on this

- The mark is the venue's own, used for margining. It is not necessarily a traded price, and
  a few of these instruments' books are in the separate `crypto-execution-costs` repo.
- How the index is derived for an equity while its cash market is closed is the venue's
  business and is not modelled here. It was observed to move slightly even on a Saturday, so
  treat it as the venue's reference rather than a last cash close.
- `pre_ipo` names have no public reference market at all, at any hour. Their basis is measured
  against a construction of the venue's own, so read it as internal consistency, not as a
  premium to a market price.
- One venue. This is where one book put the price, not a consensus.
- Instruments are listed and delisted over time, so count distinct `instrument_name` per day
  rather than assuming a fixed universe.
""",
    },
    "crypto-options-surface": {
        "datasets": ["e22_options_surface", "e22_options_book", MANIFEST],
        "example": "e22_options_surface",
        "pretty": "Implied volatility surface for on-chain crypto options",
        "tags": ["options", "implied-volatility", "derivatives", "greeks", "cryptocurrency",
                 "ethereum", "bitcoin", "solana", "time-series", "finance"],
        "size": "1M<n<10M",
        "body": """# Crypto options implied volatility surface

Every listed option on one venue, priced, with its greeks, sampled through the day.

An option quote is computed on demand and kept by nobody. The chain that existed at any past
moment is not recoverable from the venue or from the chain it settles on: only trades leave a
record, and most of these strikes never trade.

## Contents

| name | one row is |
|---|---|
| `e22_options_surface` | one instrument at one moment: strike, expiry, mark, forward, and the full greek set |
| `e22_options_book` | top of book for a near-the-money ladder: bid, ask, their sizes, and both quoted in vol terms |

## Reading it

`iv`, `delta`, `gamma`, `vega`, `theta` and `rho` are the venue's own values, published
alongside its mark price rather than recomputed here. That is deliberate: re-deriving implied
volatility needs a rate and dividend assumption, and a number the venue itself margins against
is the more useful one.

Each asset's chain is fetched in one request and every row from it shares a `sampled_ts`,
so grouping on `(asset, sampled_ts)` recovers one surface and adding `expiry` recovers one
smile. `round_ts` is shared by the assets collected in the same pass, which is what to
group on when comparing across assets at a moment.

`strike` and `expiry` are parsed from the instrument name, and both are null where the name
does not match the expected shape rather than being guessed. `expiry_ts` comes from the venue.

## What the mark costs you

The surface carries the venue's mark. `e22_options_book` carries what was actually quoted:
`best_bid_iv` and `best_ask_iv` give the two sides in vol terms and `iv_spread` the distance
between them. Across one ladder that distance ran from 4.2 to 18.6 vol points, and the mark sat
inside the band every time, near the middle rather than at it.

The ladder is deliberate rather than exhaustive, because a book costs one request per
instrument. It takes the strikes nearest the forward across several expiries and skips the
expiring contract, which was measured to have no book at all. So `e22_options_book` covers a
slice of `e22_options_surface` and never all of it; the two join on `instrument_name` within a
round.

A one-sided book is a real state and is recorded as one, with the missing side null while
`error` stays empty. `error` is set only where the request itself failed.

## Before you build on this

- One venue, and not the largest one. This is what a single order book quoted, not a
  market-wide consensus. Anything inferred about crypto volatility generally needs a second
  source.
- Every listed instrument appears, including deep out-of-the-money strikes that never trade.
  A mark price is published for those too, so filter before treating the surface as tradeable.
- Put and call at the same strike and expiry carry the same implied volatility by construction.
  Two rows agreeing is parity, not confirmation.
- Sampled at an interval, so a move that reverses between samples is invisible. `sampled_ts`
  is ours, taken at observation, and carries our network distance to the venue.
- A fetch that fails writes one explicit error row for that asset with the measurements null.
  Check `error` before reading an absent chain as a delisting.
- Coverage per asset differs and changes as the venue lists and expires instruments; count
  distinct `expiry` per day rather than assuming a fixed ladder.
""",
    },
    "ethereum-mempool": {
        "datasets": ["e1_mempool_minutely", "e1_mempool_dropped", "e1_mempool_lifecycle",
                     "e3_mempool_divergence", MANIFEST],
        "example": "e1_mempool_minutely",
        "pretty": "Ethereum pending transactions, dwell times and drops",
        "tags": ["mempool", "ethereum", "transactions", "blockchain", "time-series"],
        "size": "1M<n<10M",
        "body": """# Ethereum mempool

Transactions seen pending on Ethereum: how long they waited, and which of them were never mined
at all.

## Contents

| name | one row is |
|---|---|
| `e1_mempool_minutely` | one minute of pending-set activity |
| `e1_mempool_dropped` | a transaction seen pending that never reached a block |
| `e1_mempool_lifecycle` | a transaction seen pending, per transaction (ends 2026-08-26, see below) |
| `e3_mempool_divergence` | one comparison of pending sets across several independent views |

## Read this before you use it

The Flashbots Mempool Dumpster publishes the same measurement daily under CC-0, from a wider
node set and with a longer history. For most Ethereum questions theirs is the better source and
you should use it.

What this adds is a SECOND, independent observer. A dwell time is only meaningful against
somebody else's dwell time, and `e3_mempool_divergence` exists to size how much two views of the
pending set actually disagree. If you are checking a result against another source rather than
looking for the deepest single feed, that is what this is for.

`e1_mempool_lifecycle` stops at 2026-08-26. Per-transaction rows were discontinued there for
exactly the reason above: duplicating a freely available measurement was not worth the storage it
took. `e1_mempool_minutely` and `e1_mempool_dropped` continue, and the dropped rows are the part
with no free equivalent, because a transaction that is never mined leaves no on-chain record.

## Before you build on this

- Pending sets are node-local. What one view holds is not what the network holds, and retention
  is a configuration choice rather than a protocol rule.
- `dropped` means confirmed absent from the chain, absent from every pending view held, and still
  absent after a debounce period. It is never inferred from a single observation.
- Timestamps are ours, taken at observation. They are not consensus timestamps and carry our
  network distance to whatever served the data.
- The minutely table is an aggregate by construction. If you need per-transaction Ethereum rows
  for a date after 2026-08-26, the Dumpster has them and this does not.
""",
    },
    "dogecoin-network-propagation": {
        "datasets": ["e26_doge_block_propagation", "e26_doge_tx_propagation",
                     "e26_doge_relay_floor", "e26_doge_p2p_peers", MANIFEST],
        "example": "e26_doge_block_propagation",
        "pretty": "Dogecoin block and transaction propagation, at one block a minute",
        "tags": ["dogecoin", "peer-to-peer", "propagation", "networking", "latency",
                 "blockchain", "time-series"],
        "size": "1M<n<10M",
        "body": """# Dogecoin network propagation

How fast the Dogecoin peer network learns things, measured from connections held to its reachable
peers at once.

Dogecoin targets a block every minute. That is ten times Bitcoin's rate, and it makes this the
densest propagation record of the set: one 140-second window produced 33 block announcements here
against a single block on Bitcoin.

Nothing here can be reconstructed later. A block carries only the timestamp its miner claimed, an
announcement carries none at all, and a peer's own relay policy is broadcast and forgotten.

## Contents

| name | one row is |
|---|---|
| `e26_doge_block_propagation` | one peer announcing one block, timestamped |
| `e26_doge_tx_propagation` | one peer announcing one transaction, timestamped |
| `e26_doge_relay_floor` | one peer's own minimum relay fee, at the moment it announced it |
| `e26_doge_p2p_peers` | one peer connected to: user agent, services, handshake state |

## What a one-minute block buys you

Propagation is measured against the first peer to tell us, so the useful quantity is the spread
across peers for the same block. On a ten-minute chain a collection window catches a handful of
blocks; here it catches dozens, which is the difference between an anecdote and a distribution.

The node population is unusually uniform: peers run Shibetoshi 1.14.x almost exclusively, against
the several independent implementations seen on Bitcoin Cash. A network where nearly every node
runs the same build is a useful control for anything that might be implementation-specific.

## Transactions are NOT sampled here

Bitcoin's transaction announcements are sampled at roughly one in sixty-four because they arrive
in the thousands per second. Dogecoin does not need that: one window carried 1,080 announcements
of only about 22 distinct transactions, so everything is kept. A near-empty mempool is itself the
reason the propagation curves are complete rather than sampled.

## Before you build on this

- Peers come from DNS seeds rather than a crawler. Two of the four published seeds no longer
  resolve, so the reachable set is smaller and more concentrated than Bitcoin's.
- Timings are ours and include network distance to each peer. Differences of milliseconds are
  partly geography; differences of seconds are not. `peer_addr` is retained so this can be
  controlled for.
- A peer that disconnects stops announcing, which resembles slowness. `e26_doge_p2p_peers`
  carries handshake state so a gap can be told from a silence.
- Merge-mined with Litecoin, so block timing here is not independent of that chain. If you are
  comparing the two, that is a shared cause and not a coincidence.
""",
    },
    "bitcoin-cash-network-propagation": {
        "datasets": ["e27_bch_block_propagation", "e27_bch_tx_propagation",
                     "e27_bch_relay_floor", "e27_bch_p2p_peers", MANIFEST],
        "example": "e27_bch_block_propagation",
        "pretty": "Bitcoin Cash propagation across several independent node implementations",
        "tags": ["bitcoin-cash", "peer-to-peer", "propagation", "networking", "latency",
                 "blockchain", "time-series"],
        "size": "1M<n<10M",
        "body": """# Bitcoin Cash network propagation

How fast the Bitcoin Cash peer network learns things, measured from connections held to its
reachable peers at once.

Nothing here can be reconstructed later. A block carries only the timestamp its miner claimed, an
announcement carries none at all, and a peer's own relay policy is broadcast and forgotten.

## Contents

| name | one row is |
|---|---|
| `e27_bch_block_propagation` | one peer announcing one block, timestamped |
| `e27_bch_tx_propagation` | one peer announcing one transaction, timestamped |
| `e27_bch_relay_floor` | one peer's own minimum relay fee, at the moment it announced it |
| `e27_bch_p2p_peers` | one peer connected to: user agent, services, handshake state |

## The reason to have this one

Bitcoin Cash keeps Bitcoin's ten-minute cadence and its transaction format, and carries a
fraction of the load. That makes it the closest thing available to a control for the Bitcoin
panels: same protocol, same block target, very different congestion.

Its node population is the opposite of Dogecoin's. One window held Bitcoin Cash Node, Bitcoin ABC
and Bitcoin SV peers simultaneously, which are independent implementations speaking the same
protocol on the same network. Anything that differs between them is visible here and invisible on
a chain where every node runs the same build.

## Before you build on this

- Blocks are sparse by construction at ten minutes apart, so a short window may catch one or
  none. Judge coverage from `e0_run_manifest` and from the peer table rather than from the block
  count alone.
- Peers come from DNS seeds rather than a crawler, so the reachable set is smaller and more
  concentrated than Bitcoin's.
- Transactions are not sampled here, unlike the Bitcoin panels: the load does not require it, so
  the propagation curves are complete rather than one-in-sixty-four.
- Timings are ours and include network distance to each peer. `peer_addr` is retained so this can
  be controlled for.
- A peer that disconnects stops announcing, which resembles slowness. `e27_bch_p2p_peers` carries
  handshake state so a gap can be told from a silence.
""",
    },
    "litecoin-network-propagation": {
        "datasets": ["e25_ltc_block_propagation", "e25_ltc_tx_propagation",
                     "e25_ltc_relay_floor", "e25_ltc_p2p_peers", MANIFEST],
        "example": "e25_ltc_block_propagation",
        "pretty": "Litecoin block and transaction propagation across the peer network",
        "tags": ["litecoin", "peer-to-peer", "propagation", "networking", "latency",
                 "blockchain", "time-series"],
        "size": "1M<n<10M",
        "body": """# Litecoin network propagation

How fast the Litecoin peer network learns things, measured from connections held to its reachable
peers at once.

Nothing here can be reconstructed later. A block carries only the timestamp its miner claimed, an
announcement carries none at all, and a peer's own relay policy is broadcast and forgotten.

## Contents

| name | one row is |
|---|---|
| `e25_ltc_block_propagation` | one peer announcing one block, timestamped |
| `e25_ltc_tx_propagation` | one peer announcing one transaction, timestamped |
| `e25_ltc_relay_floor` | one peer's own minimum relay fee, at the moment it announced it |
| `e25_ltc_p2p_peers` | one peer connected to: user agent, services, handshake state |

## Why a second chain at all

Litecoin runs the same wire protocol as Bitcoin and targets a block every 2.5 minutes rather than
10. That makes it the denser of the two: one short window produced 69 block announcements here
against 2 on Bitcoin over the same period, because there is simply more to see.

It exists so the Bitcoin figures have something to be compared against. A propagation delay or a
relay-floor distribution means little on its own; it means considerably more set beside the same
measurement on a chain with four times the block rate and a different node population. The
companion panels are in `bitcoin-network-propagation`, collected by the same code in the same
process, which is what makes the comparison fair.

## The node population is different, and that is the point

Peers here run LitecoinCore, and the versions observed spread much wider than Bitcoin's, from
0.21.x down to a node still on 0.15.1. Relay floors spread wider too: one sample held 0.1, 1.0
and 100.0 sat/vB, the last being a node that will not forward anything remotely ordinary.

## Before you build on this

- Peers come from DNS seeds rather than a crawler, because Litecoin has no Bitnodes equivalent.
  Two of the four published seeds no longer resolve, so the reachable set is smaller and more
  concentrated than Bitcoin's. It is a sample of the network and a narrower one.
- Timings are ours and include network distance to each peer. Differences of milliseconds are
  partly geography; differences of seconds are not. `peer_addr` is retained so this can be
  controlled for.
- Transactions are announced with deliberately randomised timing on this network too, so the
  transaction table measures that privacy behaviour rather than raw relay speed. Compare it
  against the block table, not against zero.
- Transactions are SAMPLED at roughly one in ten, by txid. This chain carries enough of them that
  keeping everything would dominate the whole project's storage. The test is a property of the
  hash, so it is identical on every peer and a curve is never truncated by when we started
  watching. The Dogecoin and Bitcoin Cash panels are quiet enough to keep everything, and say so.
- A peer that disconnects stops announcing, which resembles slowness. `e25_ltc_p2p_peers` carries
  handshake state so a gap can be told from a silence.
""",
    },
    "bitcoin-network-propagation": {
        "datasets": ["e21_btc_block_propagation", "e21_btc_tx_propagation",
                     "e21_btc_relay_floor", "e21_btc_p2p_peers", MANIFEST],
        "example": "e21_btc_block_propagation",
        "pretty": "Bitcoin block and transaction propagation across the peer network",
        "tags": ["bitcoin", "peer-to-peer", "propagation", "networking", "latency",
                 "blockchain", "time-series"],
        "size": "1M<n<10M",
        "body": """# Bitcoin network propagation

How fast the Bitcoin peer network learns things, measured from connections held to roughly 120
peers at once.

Nothing here can be reconstructed later. A block carries only the timestamp its miner claimed, an
announcement carries none at all, and a peer's own relay policy is broadcast and forgotten.

## Contents

| name | one row is |
|---|---|
| `e21_btc_block_propagation` | one peer announcing one block, timestamped |
| `e21_btc_tx_propagation` | one peer announcing one sampled transaction, timestamped |
| `e21_btc_relay_floor` | one peer's own minimum relay fee, at the moment it announced it |
| `e21_btc_p2p_peers` | one peer connected to: user agent, services, handshake state |

## Blocks against transactions

These propagate on completely different timescales, and the contrast is the point.

Across four blocks in one window, half the peers had a new block within about 0.4 seconds, with a
tail out to 52 seconds on one peer. Transactions took a median of about 23 seconds between the
first and last peer, p90 71 seconds, and a maximum of 103.

That gap is not congestion. Nodes deliberately randomise transaction announcement timing so that
a watching node cannot work out where a transaction entered the network. Blocks get no such
treatment, because there is nothing to hide. So the transaction table measures a privacy
mechanism and the block table measures raw relay speed.

## Sampling and floors

Transactions are sampled by txid, roughly one in sixty-four, because announcements otherwise
arrive in the thousands per second. The test is a property of the hash, so it is the same on
every peer and a curve is never truncated by when we started watching. Only the announcement is
recorded; the body is never requested and is on chain anyway once mined.

`e21_btc_relay_floor` is the other half of whether a transaction can move at all. Below a peer's
minimum relay fee it is not forwarded, so it never reaches a miner. One sample of 41 peers held 8
distinct floors, with only 34% at the common 1.0 sat/vB default, 56% below it and 10% above. The
non-round values are nodes whose own mempool is evicting.

## Before you build on this

- Timings are ours and include network distance to each peer, which varies with geography.
  Differences of milliseconds are partly distance; differences of seconds are not. `peer_addr` is
  retained so this can be controlled for.
- Roughly 120 peers out of tens of thousands reachable. A sample of the network, not the network.
- A peer that disconnects stops announcing, which resembles slowness. `e21_btc_p2p_peers` carries
  handshake state so a gap can be told from a silence.
- Relay floors flatten while the mempool is quiet, because nodes then sit at whatever they were
  configured with. They move when a mempool fills, which is when they matter.
""",
    },
    "bitcoin-fee-estimator-accuracy": {
        "datasets": ["e15_fee_estimators", "e28_fee_estimator_accuracy", MANIFEST],
        "example": "e28_fee_estimator_accuracy",
        "pretty": "What Bitcoin fee estimators advised, against what blocks required",
        "tags": ["bitcoin", "fee-estimation", "transaction-fees", "benchmark",
                 "forecasting", "blockchain", "time-series"],
        "size": "100K<n<1M",
        "body": """# Bitcoin fee estimator accuracy

What five fee estimators told you to pay, and what the block actually required.

Only one half of this is scarce, and it is worth being precise about which.

Outcomes are freely available. One of these providers will serve you a year of realised per-block
fee rates for the asking, so what a block required is not a secret and never was.

The advice is the perishable half. Every provider answers for right now and none serves a history
of what it said: the documented history endpoints return 404, and the one that looks like an
archive turns out to hold realised rates rather than past forecasts. A recommendation made last
month therefore exists only if somebody wrote it down at the time.

That is what this repo is. Not a clever method -- the join is arithmetic anyone could do -- but
the half of the inputs that cannot be obtained after the fact.

## Contents

| name | one row is |
|---|---|
| `e15_fee_estimators` | one provider's recommendation at one moment, for one confirmation target |
| `e28_fee_estimator_accuracy` | one block against one provider's forecast for it: predicted, cleared, and whether it would have worked |

## Reading it

`sufficient` answers the only question a wallet actually asks: would paying the recommendation
have got the transaction into that block. `overpay_ratio` is the recommendation divided by what
cleared, so 1.0 is exact and 3.0 means paying triple.

The two together are the whole point, because either alone is misleading. A provider can be
sufficient every single time by quoting an absurd number, and cheap every time by quoting one
that rarely works. One early window showed both failure modes at once: two providers were
sufficient on 100% of blocks while quoting about 3x the going rate, and another quoted 1.1x and
was sufficient on 83%. At a six-block target that same cheap provider fell to 41%.

`target_blocks` is the provider's own horizon, and a forecast is matched to the block it was
about -- roughly `target_blocks` block-times after it was made -- never to a block that had
already been found when the forecast was issued.

## What counts as the rate that cleared

`cleared_p10` is the 10th percentile fee rate among **standalone** transactions in the block:
those with no unconfirmed parent.

That restriction is doing real work and the number is wrong without it. About 93% of the
cheapest-looking transactions in a block are not standalone -- they were admitted on a relative's
fee, through a parent or a fee-bumping child. Counting them measures the lowest rate *visible*
in a block rather than the lowest rate that would have *worked*, and it inflates apparent
overpayment by roughly three and a half times. `cleared_p10_all_txs` carries the unrestricted
figure so the gap between the two definitions is visible in the data.

The minimum is not used at all: blocks legitimately contain zero-fee transactions handed straight
to a pool, and those measure a private arrangement rather than a market.

## Before you build on this

- Five providers, and they are the ones that answer without an API key. That is a selection.
- `n_standalone` is on every row. A block with few standalone transactions gives a noisier
  percentile, and low-activity periods produce fewer of them.
- Fee coverage in the underlying panel is partial and varies with load, so the percentile is
  computed over transactions whose fee we sampled, not over the whole block. It is never imputed.
- A quiet mempool flatters every provider: when almost anything confirms, being sufficient is
  easy and overpayment is large. Read the two columns together, and read them by regime rather
  than pooled across months.
- `lead_seconds` records how far ahead of the block each forecast was actually made. Matching is
  to the newest forecast at or before the target moment, so this varies with sampling cadence.
""",
    },
    "bitcoin-mempool-lifecycle": {
        "datasets": ["e8_btc_mempool_lifecycle", "e9_btc_mempool_divergence",
                     "e11_ltc_mempool_lifecycle", "e8_btc_block_composition", MANIFEST],
        "example": "e8_btc_mempool_lifecycle",
        "pretty": "Bitcoin mempool lifecycle, dwell times and fee estimates",
        "tags": ["mempool", "transaction-fees", "fee-estimation", "bitcoin", "litecoin",
                 "ethereum", "blockchain", "cryptocurrency", "time-series"],
        "size": "1M<n<10M",
        "body": """# Bitcoin mempool lifecycle

Transactions from the moment they appear in the mempool to whatever happens to them: mined,
still waiting, or dropped without ever reaching a block.

A confirmed transaction keeps its contents forever but loses its timing. One that is never mined
leaves no record at all. Both are recorded here as they happen.

## Contents

| name | one row is |
|---|---|
| `e8_btc_mempool_lifecycle` | a Bitcoin transaction: first seen, fee rate, mined height, blocks waited, fate |
| `e9_btc_mempool_divergence` | one view of the pending set at one instant, sized and compared against the others |
| `e11_ltc_mempool_lifecycle` | the same, for Litecoin |
| `e8_btc_block_composition` | one block: how much of it we had already seen pending |

Ethereum moved to its own repo, `ethereum-mempool`, rather than sitting inside a Bitcoin panel.
Peer-to-peer propagation and relay floors likewise moved to `bitcoin-network-propagation`.

## Fee rate coverage

`fee_rate_sat_vb` is present on a portion of rows and that portion changes over time. It is
sampled, never estimated: a guessed fee rate would ruin the analyses the column exists for.

Coverage by day, as a share of transactions observed arriving (`pre_existing == False`):

| date | coverage |
|---|---|
| 2026-08-25 | none |
| 2026-08-26 | ~4% |
| 2026-08-27 | ~14% |
| 2026-08-28 | ~16% early, ~67% late |
| 2026-08-29 onward | ~67% |

Coverage of transactions already pending when a collection window opened moved from about 1% to
about 99% on 2026-08-28. Dropped rows inherit that figure and reached 94% on 2026-08-29.

Partitions from 2026-08-29 are substantially denser than earlier ones. Compute coverage per
partition rather than assuming a constant:

```python
obs = df[~df.pre_existing.astype(bool)]
coverage = obs.fee_rate_sat_vb.notna().mean()
```

Rows without a fee rate are complete in every other respect, so they remain usable for lifecycle
work. Only fee-conditioned analysis is affected.

## Reading the fate column

`dropped` means the transaction was confirmed absent from the chain, absent from every pending
view held, and stayed absent through a debounce period. It is not inferred from a single
observation. `unresolved` means it went missing but could not be confirmed, and is never counted
as a drop.

Bitcoin `fate = "dropped"` begins 2026-08-26. Earlier partitions contain no drops because a
defect made them impossible to record, not because none occurred.

## How much of a block did we already know about

`e8_btc_block_composition` counts, for every block, how many of its transactions were already in
our mempool view and how many appeared for the first time in the block itself.

The second number is the interesting one. A transaction that reaches a miner without crossing the
public relay network looks exactly like this, which is what direct-to-pool submission is. So does
a transaction broadcast in the seconds between two of our polls, and so does one our providers
simply never held. **The column is therefore an upper bound on out-of-band submission, not a
measurement of it**, and `polls_so_far`, `poll_failures` and `pool_size_at_block` travel on every
row so the bound can be tightened or discarded.

It doubles as a coverage statement for the rest of this repo. If you want to know how complete
the mempool view behind `e8_btc_mempool_lifecycle` is, this is the answer, block by block, in the
data rather than in a claim. One observed block held 4,823 transactions excluding the coinbase,
of which 4,804 were already in view: 99.6% captured, and 19 seen for the first time in the block
itself.

The coinbase is excluded, since it is created by the miner and never broadcast; counting it would
add one guaranteed unseen transaction to every block. Blocks mined before a run's baseline was
established are not recorded at all, because none of their transactions could have been seen and
the row would read 100% unseen for a reason that has nothing to do with the network.

## Package fee rates

A transaction's own fee rate is not what decides whether it is mined. A miner sorts by the
ANCESTOR fee rate, so a transaction with an unconfirmed low-fee parent is worth less than it
appears, and a child paying a large fee lifts its parent with it.

`effective_fee_rate_sat_vb` is `ancestor_fees_sat / ancestor_vsize`. For a transaction with no
unconfirmed parents it equals `fee_rate_sat_vb` exactly; where they diverge, the effective rate
is the one that governs inclusion. One snapshot held a transaction paying 28.4 sat/vB whose
effective rate was 2.1.

`ancestor_count` above 1 marks membership in an unconfirmed chain, which is how fee bumping by
a child shows up. The descendant columns are the mirror: what is waiting on this transaction.

These come from a full node's own view, so they are null for transactions it never held, and
they are recorded as first observed. Package structure changes as parents confirm, so treat
them as the state at first sight rather than a running value.

## Before you build on this

- Pending sets are node-local. What one view holds is not what the network holds, and retention
  is a configuration choice rather than a protocol rule. `e9` exists to size that disagreement,
  and it is large: at one instant the views ranged from about 6,000 to about 87,000 pending
  transactions, with only 7% common to all of them.
- Dropped rows carry a fee rate far less often than mined rows. Every drop observed so far was
  already pending when its window opened, and fee sampling favours arrivals.
- A fee rate of exactly 0 is real and rare. Filter on `fee_rate_sat_vb > 0` if a zero would
  break your arithmetic rather than treating it as a decode fault.
- Timestamps are ours, taken at observation. They are not consensus timestamps and carry our
  network distance to whatever served the data.
- Ethereum rows are included for comparison. If Ethereum is your subject, the Flashbots Mempool
  Dumpster publishes the same measurement daily under CC-0 from a wider node set and a longer
  history. Theirs is better; use it.
""",
    },
    "fiat-onramp-pricing": {
        "datasets": ["e12_onramp_quotes", MANIFEST],
        "example": "e12_onramp_quotes",
        "pretty": "What retail buyers are quoted to convert cash into crypto",
        "tags": ["on-ramp", "payments", "retail", "pricing", "cryptocurrency",
                 "fees", "time-series"],
        "size": "100K<n<1M",
        "body": """# Fiat on-ramp pricing

What it costs a retail buyer to turn a card payment into crypto, quoted at the moment they would
have bought.

An on-ramp quote is computed per request against a rate, a spread and a fee schedule that all
move. Nobody keeps the quotes, so the only record of what a buyer was actually offered is one
made as it was offered.

## Contents

| name | one row is |
|---|---|
| `e12_onramp_quotes` | one provider quoting one fiat amount into one asset: rate, fees, and what the buyer receives |

## Reading it

The amount received is the figure that matters. Providers split their margin differently between
the exchange rate and the explicit fee, so comparing on the advertised fee alone will mislead
you, and comparing on rate alone will mislead you in the other direction.

Rows carry the quoted rate and the fee separately as well, so the split itself can be studied.

## Before you build on this

- A small number of providers, and they are the ones that quote without an API key. That is a
  selection, not a market: the providers requiring a key are absent and are not a random subset.
- Quotes are indicative. A real purchase adds identity checks, card-issuer behaviour and limits
  that a quote does not reflect, and any of those can change the outcome or block it entirely.
- Refusals are written as explicit error rows rather than omitted, so a provider that was
  unavailable is distinguishable from one that had nothing to offer. Check `error` before
  reading an absence as a decline.
- Coverage per asset changes as providers list and delist pairs.
""",
    },
    "l2-preconfirmation-reliability": {
        "datasets": ["e14_l2_preconf", MANIFEST],
        "example": "e14_l2_preconf",
        "pretty": "Whether L2 sequencers keep the inclusion promises they make",
        "tags": ["layer-2", "rollup", "sequencer", "ethereum", "reliability",
                 "blockchain", "time-series"],
        "size": "100K<n<1M",
        "body": """# L2 sequencer preconfirmation reliability

A rollup sequencer tells you your transaction is included before anything is settled on L1. This
records whether that promise held.

The promise is the ephemeral part. If a sequencer replaces an unsafe block, the version it
originally gossiped is discarded and no archive serves it, so a broken promise is only
observable to somebody who wrote down the original.

## Contents

| name | one row is |
|---|---|
| `e14_l2_preconf` | either a heartbeat, recording how far the unsafe head ran ahead of the safe head, or a violation |

## Mostly an attested absence

Violations are rare, and the value here is in the zeros being credible rather than in the
events. That requires the coverage rows: `n_checked` counts promises actually verified against
the canonical chain and `n_failed` counts the checks that could not complete. A run with no
violations and no checks is not the same as a run with no violations and thousands.

`row_type` separates heartbeats from violations.

## Before you build on this

- `lag_blocks` is only meaningful where `safe_tag_plausible` is true. One chain's endpoint has
  served a stale safe tag intermittently, giving a lag of tens of millions of blocks against
  another chain's few dozen, then recovering to a normal figure hours later. It is kept rather
  than dropped, because an endpoint's own inconsistency is a fact about running on public
  infrastructure, and because it comes and goes you cannot exclude a chain once and be done.
  The flag is false on exactly the affected rows; filter on it, not on the chain name.
- The unsafe head is sampled every few seconds, so a block that was proposed and replaced between
  two samples is invisible. This undercounts violations and cannot overcount them.
- Nine chains, ALL OP-stack. That is nine independent sequencer operators, which is enough to
  compare them against each other, but it is still one rollup architecture: nothing here
  generalises to a rollup built differently.
- Safe-head lag varies enormously between them even at rest, from tens of blocks to several
  thousand, and one endpoint reports a lag of tens of millions. Compare a chain against its own
  history rather than against another chain's absolute level.
""",
    },
    "solana-dex-execution": {
        "datasets": ["e24_solana_quotes", "e24_solana_routes", MANIFEST],
        "example": "e24_solana_quotes",
        "pretty": "Solana swap quotes and the venues an aggregator routes them through",
        "tags": ["solana", "dex", "routing", "liquidity", "execution", "cryptocurrency",
                 "trading", "time-series"],
        "size": "1M<n<10M",
        "body": """# Solana DEX execution costs and routing

What a swap was quoted at on Solana, and which venues the aggregator split it across to get
there.

A quote is computed per request against pool state that has already changed by the time anyone
asks again. The fill that follows may land on chain; the quote never does, and neither does the
route the aggregator considered and rejected.

## Contents

| name | one row is |
|---|---|
| `e24_solana_quotes` | one pair at one size at one moment: output, price impact, dollar value, leg count |
| `e24_solana_routes` | one leg of the chosen route: venue, amounts, and the percentage of the order it carried |

## Reading it

Join the two on `(round_ts, input_symbol, output_symbol, in_amount)`. A quote with `n_legs` above
one was split, and `percent` on each leg says how the order was divided.

The size ladder is fixed at 1, 50 and 1,000 SOL so that impact is comparable over time rather
than against a moving notional. Impact rises with size on every pair, which is the sanity check
this table has to pass: one observation ran 0.0000%, 0.0013% and 0.065% on a thin pair against
0.0000%, 0.000018% and 0.000068% on the deepest one.

Routing is the part that is hard to get elsewhere. The same pair goes through one venue at small
size and four or five at large size, and the venue mix changes between rounds, which maps where
routable liquidity actually sits rather than where volume was reported.

## A column that is raw on purpose

`amm_report_raw` is the aggregator's own report on the venues it considered, stored verbatim as
JSON rather than parsed into a comparison.

It is kept raw because it is NOT restricted to the pair being quoted. A request for SOL into a
memecoin returns values on the scale of a stablecoin quote, and at least one entry has been the
string "Pair insufficient liquidity" rather than a number. An earlier version derived a
"chosen route versus best single venue" spread from this field and produced 340 million basis
points, which is what exposed the mismatch. The raw object keeps the information without
publishing a derived number that means nothing.

## Before you build on this

- One aggregator. It is the dominant one on Solana, but a quote is what it would route, not the
  best price obtainable anywhere.
- Quotes are indicative. A real swap adds transaction landing risk, priority fees and slippage
  against a moving pool, none of which a quote reflects.
- `price_impact_pct` is the aggregator's own estimate, not something recomputed here.
- Five pairs, all with SOL as the input. That is a slice of Solana, not a census.
- A failed request is written as an explicit error row with the measurements null, so an
  unavailable aggregator is distinguishable from a pair with no route.
""",
    },
    "crypto-execution-costs": {
        "datasets": ["e10_quote_benchmark", "e16_dex_routes", "e17_perp_depth",
                     MANIFEST],
        "example": "e10_quote_benchmark",
        "pretty": "What it costs to trade on a DEX: quotes, routes and book depth",
        "tags": ["defi", "dex", "execution-cost", "slippage", "onramp", "ethereum",
                 "blockchain", "cryptocurrency", "time-series"],
        "size": "10K<n<100K",
        "body": """# Crypto execution costs

What it costs to trade, quoted at the moment someone would have traded.

Executed trades are on chain forever. Quotes for trades nobody placed are computed on demand and
kept by nobody, so they only exist if they were recorded when they were served.

## Contents

| name | one row is |
|---|---|
| `e10_quote_benchmark` | one swap quote at a fixed size, with the provider fee separated out |
| `e16_dex_routes` | one leg of a route a router chose: venue, pool, amount, and how many venues the trade was split across |
| `e17_perp_depth` | one order book snapshot: spread, level counts, resting notional within fixed distances of mid |

## Reading it

Quotes are what a provider served, which is not what you would necessarily have filled. Provider
fees are separated from price so that a fee policy is not mistaken for routing quality.

`e16_dex_routes` maps where routable liquidity sits rather than where volume went, and it
surfaces venues too small to appear in volume rankings. The split widens sharply with size: one
observation put the same pair through a single venue at small size and ten venues at large size.

`e17_perp_depth` stores book shape rather than raw ladders, as cumulative resting notional within
a band of mid. Where a spread is wider than a band, that band is correctly zero, which happens
often on thinner markets.

## Before you build on this

- Not every provider answers every request. Refusals and rate-limit skips are written as explicit
  error rows rather than omitted, so a provider that was unavailable is distinguishable from one
  that had nothing to offer. Check the `error` column before treating absence as meaning.
- One provider is queried on a subset of pairs. Its coverage is deliberately narrower than the
  others and should not be read as a market-wide view.
- Only some routers expose a leg breakdown. Rows from the others carry the venue name alone,
  which is why `pool` and `swap_amount_raw` are frequently null.
- Depth rows are snapshots at the observation interval, not a tick-level book. A move that
  reverses between observations is invisible.
- Depth covers a single venue, so it sizes that one book rather than the market. Treat it as
  one participant's view; a second venue would be a control and there is not one here.
""",
    },
    "remittance-pricing-panel": {
        "datasets": ["e13_remittance_quotes", MANIFEST],
        "example": "e13_remittance_quotes",
        "pretty": "Cross-border remittance pricing, by corridor and provider",
        "tags": ["remittance", "exchange-rates", "fintech", "payments", "banking",
                 "foreign-exchange", "time-series", "finance"],
        "size": "10K<n<100K",
        "body": """# Remittance pricing panel

What sending money across borders actually costs, quoted corridor by corridor.

Published averages are periodic and aggregated. These are the individual quotes as offered, at
the moment they were offered.

## Contents

| name | one row is |
|---|---|
| `e13_remittance_quotes` | one provider quoting one corridor: rate, fees, and amount received |

## Reading it

Twenty corridors covering the largest remittance lanes plus intra-European pairs where bank
pricing is widest. Corridors added later begin later; the manifest records when each dataset
started.

The amount received is the figure that matters. Rate and fee split differently between providers
and comparing on rate alone will mislead you.

## Before you build on this

- The source is a comparison service run by one of the providers being compared. Which
  competitors appear in a corridor is that provider's choice, competitor quotes can lag, and the
  publisher has an obvious interest in appearing cheap. The bias is at least constant and
  visible.
- Quotes are indicative. A real transfer may differ, and some providers apply limits or
  verification steps that a quote does not reflect.
- Coverage per corridor varies with how many competitors the source lists, which changes over
  time.
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
        red = REDACTIONS.get(ds)
        for f in files:
            dst = target / f.relative_to(DATA)
            dst.parent.mkdir(parents=True, exist_ok=True)
            if red is None:
                shutil.copy(f, dst)
                continue
            # A source whose terms forbid collection must not be published from HISTORICAL
            # partitions either, and rewriting the archive is the wrong lever: it destroys a
            # record we may need. Filtering at stage time is declarative, reversible, and --
            # because a public repo mirrors its window -- removes the rows already up there on
            # the very next push, with no manual repo surgery.
            import pandas as pd
            col, drop = red
            df = pd.read_parquet(f)
            if col in df.columns:
                df = df[~df[col].isin(drop)]
            if not len(df):
                dst.unlink(missing_ok=True)
                continue
            df.to_parquet(dst, index=False)
        files = [f for f in files if (target / f.relative_to(DATA)).exists()]
        if not files:
            continue
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
    # A product carrying ONLY the manifest is not a product. Every product includes the manifest,
    # which is never windowed, so a newly defined product would otherwise publish immediately --
    # a public repo whose card describes data it does not yet have, sitting empty until the
    # collector that fills it next runs. Creating the repo is the irreversible half; waiting is
    # free. The archive (window is None) is exempt: it accumulates everything by design.
    if window is not None and not any(k != MANIFEST for k in summary):
        print(f"  !! {label}: manifest only, no dataset partitions yet -- not publishing", flush=True)
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
        _nag(_w)

    ok = _push(ARCHIVE_REPO, everything, None, None, "archive", private=True)
    _receipt(ok, _last_archive_summary)

    window = sample_window()
    print(f"  public sample is FROZEN at {window[0]} to {window[1]}", flush=True)
    for name, p in PRODUCTS.items():
        _push(f"{OWNER}/{name}", p["datasets"], window, _card(name, p), name, private=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
