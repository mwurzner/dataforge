"""Publish the panels to HuggingFace: public products by audience, one private archive.

WHY THREE REPOS AND NOT ONE. Everything used to land in a single repo called `dataforge-sample`,
which by the end held 18 unrelated datasets. No name could describe it, because a Bitcoin mempool
panel and a bank remittance panel have no reader in common: somebody hunting remittance pricing
would never open a repo with "mempool" in the title, and somebody after mempool data had to wade
past lending-market parquet to reach it. The naming problem was a packaging problem.

So the split is by AUDIENCE, not by dataset count:

    bitcoin-mempool-lifecycle       what happens to transactions before they confirm
    crypto-execution-costs          what it costs to move money, quoted and compared
    remittance-pricing-panel        what banks and money transmitters charge, corridor by corridor
    bitcoin-mining-pool-templates   what pools are building on, and how blocks propagate
    ethereum-attestation-pool       attestations seen waiting against attestations included
    crypto-options-surface          every listed strike, priced, with its greeks

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
    "e16_dex_routes", "e17_perp_depth",
    "e18_attestation_pool", "e20_stratum_jobs_direct",
    "e21_btc_block_propagation", "e21_btc_p2p_peers",
    "e22_options_surface",
}
# Collected and archived, never published: freely available from an archive node.
ARCHIVE_ONLY = {"a1_lending_market_state", "a2_vault_state", "b2_stuck_markets",
                "b5_dormancy", "universe",
                # e19 is DELIBERATELY archive-only, and this is a legal gate rather than an
                # Behaviour here is deliberate; see the private design notes.
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

MINING_CARD = '''# Bitcoin mining pool templates and block propagation

What each mining pool is building on, and how quickly the network learns a new block exists.

Pools issue work to their miners describing the block they are extending, the coinbase they will
claim, and whether previous work should be abandoned. That work is replaced every few seconds.
Once a block is found, the templates every pool was building are gone.

## Contents

| name | one row is |
|---|---|
| `e20_stratum_jobs_direct` | one job from one pool: the block it extends, its nTime, coinbase, merkle branch count and clean-jobs flag |
| `e21_btc_block_propagation` | one peer announcing one block, timestamped |
| `e21_btc_p2p_peers` | one peer connected to: user agent, services, whether it completed a handshake |

## Templates

Pools building on the same block do not agree. In one observation across six pools, nTime spread
across 25 seconds and coinbase structures ranged from 318 to 980 characters. Two endpoints run by
the same operator differed by 10 seconds.

`pool` is the endpoint connected to rather than an identity inferred from the data. `operator`
collapses endpoints belonging to the same operator, and rows should usually be grouped on it.

## Propagation

Connections are held to roughly 120 peers and each announcement of a new block is timestamped.
Sorting one block's rows by `received_ts` gives its propagation curve.

Across four blocks in one window:

| peers | first to median | full spread |
|---|---|---|
| 32 | 0.39s | 3.74s |
| 31 | 0.27s | 2.00s |
| 33 | 0.45s | 52.37s |

Half the peers are reached within about half a second. The tail is the interesting part: in the
third block one peer was 52 seconds behind. Nothing on chain records that, because a block
carries only the timestamp its miner claimed.

## Before you build on this

- Propagation times are ours and include network distance to each peer, which varies with
  geography. Differences of milliseconds are partly distance; differences of seconds are not.
  `peer_addr` is retained so this can be controlled for.
- One vantage point, roughly 120 peers out of tens of thousands reachable. This is a sample of
  the network, not the network.
- A peer that disconnects stops announcing, which resembles slowness. `e21_btc_p2p_peers` carries
  handshake state so a gap can be told from a silence.
- nTime is the pool's own clock and pools do not all update it on the same cadence. It indicates
  when a pool rebuilt its template. For arrival ordering use `observed_ts`, which is consistent
  across pools.
- Six pools across five operators. Pools running regional endpoints may serve different work
  elsewhere.
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
        "datasets": ["e20_stratum_jobs_direct", "e21_btc_block_propagation",
                     "e21_btc_p2p_peers", MANIFEST],
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
    "crypto-options-surface": {
        "datasets": ["e22_options_surface", MANIFEST],
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
| `e15_fee_estimators` | one fee estimate from one provider at one moment, with its target |
| `e1_mempool_lifecycle` | an Ethereum transaction seen pending (ends 2026-08-26, see below) |
| `e1_mempool_minutely` | one minute of Ethereum mempool activity |
| `e1_mempool_dropped` | an Ethereum transaction that was never mined |
| `e3_mempool_divergence` | Ethereum pending-set comparison across views |

`e1_mempool_lifecycle` stops at 2026-08-26. Per-transaction Ethereum rows were discontinued
there: the Flashbots Mempool Dumpster publishes the same measurement under CC-0 from a wider
node set, so a duplicate was not worth the storage it took. `e1_mempool_minutely` and
`e1_mempool_dropped` continue. The earlier partitions are kept and keep their own name, so
nothing joins them to the newer tables and reads a change of population as a trend.

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

What it costs to trade, quoted at the moment someone would have traded.

Executed trades are on chain forever. Quotes for trades nobody placed are computed on demand and
kept by nobody, so they only exist if they were recorded when they were served.

## Contents

| name | one row is |
|---|---|
| `e10_quote_benchmark` | one swap quote at a fixed size, with the provider fee separated out |
| `e16_dex_routes` | one leg of a route a router chose: venue, pool, amount, and how many venues the trade was split across |
| `e17_perp_depth` | one order book snapshot: spread, level counts, resting notional within fixed distances of mid |
| `e12_onramp_quotes` | one fiat on-ramp quote |
| `e14_l2_preconf` | an L2 sequencer promise, and whether it held |

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
- `e14_l2_preconf` is mostly an attested absence. Violations are rare and the value is in the
  zeros being credible, which requires the coverage rows alongside them.
- `lag_blocks` is only meaningful where `safe_tag_plausible` is true. One chain's endpoint
  served a stale `safe` tag through much of the period, which yields a lag of tens of millions
  of blocks. The raw numbers are published uncorrected so the endpoint's own inconsistency
  stays visible; filter on the flag before using the column.
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
