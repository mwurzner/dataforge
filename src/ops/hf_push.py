"""Publish the panels to HuggingFace: public products by audience, one private archive.

WHY THREE REPOS AND NOT ONE. Everything used to land in a single repo called `dataforge-sample`,
which by the end held 18 unrelated datasets. No name could describe it, because a Bitcoin mempool
panel and a bank remittance panel have no reader in common: somebody hunting remittance pricing
would never open a repo with "mempool" in the title, and somebody after mempool data had to wade
past lending-market parquet to reach it. The naming problem was a packaging problem.

So the split is by AUDIENCE, not by dataset count:

    bitcoin-mempool-lifecycle       what happens to transactions before they confirm
    bitcoin-fee-estimator-accuracy  what estimators advised against what blocks required
    ethereum-gas-estimator-accuracy the same question on Ethereum, across RPC providers
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

from src.ops.dataset_names import PUBLIC_NAMES, public_configs, storage_reference

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
# FROZEN FOREVER, PER DATASET, AND THERE IS NO RE-PIN CHORE. The quarterly cadence this file used
# to prescribe defended against exactly one thing -- "a browser arriving in December sees August
# files and assumes the project died" -- and that worry is already answered below by publishing
# e0_run_manifest IN FULL, never windowed. Liveness is proven by the manifest and by the repo's
# own last-modified stamp, so rotating the sample bought nothing while costing ~28 days of free
# data a year plus a recurring manual chore that would eventually be missed.
#
# EACH DATASET GETS ITS OWN SAMPLE_DAYS FROM ITS OWN FIRST DAY. A single global range gave the
# datasets built last a one-to-three day sample while the oldest had seven, so the newest and most
# distinctive repos looked abandoned at birth.
#
# THE PINS ARE EXPLICIT RATHER THAN DERIVED FROM THE FILES ON DISK, which is the subtle part.
# Deriving "the first seven dates present" is the natural implementation and it breaks silently:
# prune_git trims the two HEAVY datasets to a 14-day rolling buffer, so a derived start would
# creep forward as old partitions were pruned and the frozen sample would quietly become a rolling
# one -- the exact leak this design exists to prevent. Explicit dates in git cannot slide, and
# prune_git now refuses to delete anything inside a sample window, which keeps the two consistent.
#
# ADDING A DATASET: add one line here when you add it to WINDOWED. A windowed dataset with no pin
# is EXCLUDED from the public sample and warned about loudly; it is never published with a guessed
# window, and it still reaches the private archive as normal.
SAMPLE_DAYS = int(os.environ.get("DF_SAMPLE_DAYS", 7))
SAMPLE_STARTS = {
    "e1_mempool_lifecycle": "2026-08-25",
    "e1_mempool_minutely": "2026-08-26",
    "e1_mempool_dropped": "2026-08-26",
    "e3_mempool_divergence": "2026-08-25",
    "e8_btc_mempool_lifecycle": "2026-08-25",
    "e8_btc_block_composition": "2026-08-30",
    "e9_btc_mempool_divergence": "2026-08-25",
    "e10_quote_benchmark": "2026-08-25",
    "e11_ltc_mempool_lifecycle": "2026-08-26",
    "e12_onramp_quotes": "2026-08-26",
    "e13_remittance_quotes": "2026-08-26",
    "e14_l2_preconf": "2026-08-26",
    "e15_fee_estimators": "2026-08-26",
    "e16_dex_routes": "2026-08-28",
    "e17_perp_depth": "2026-08-28",
    "e18_attestation_pool": "2026-08-28",
    "e20_stratum_jobs_direct": "2026-08-28",
    "e21_btc_block_propagation": "2026-08-29",
    "e21_btc_tx_propagation": "2026-08-29",
    "e21_btc_relay_floor": "2026-08-29",
    "e21_btc_p2p_peers": "2026-08-29",
    "e22_options_surface": "2026-08-29",
    "e22_options_book": "2026-08-29",
    "e23_perp_mark_index": "2026-08-29",
    "e24_solana_quotes": "2026-08-30",
    "e24_solana_routes": "2026-08-30",
    "e25_ltc_block_propagation": "2026-08-30",
    "e25_ltc_tx_propagation": "2026-08-30",
    "e25_ltc_relay_floor": "2026-08-30",
    "e25_ltc_p2p_peers": "2026-08-30",
    "e26_doge_block_propagation": "2026-08-30",
    "e26_doge_tx_propagation": "2026-08-30",
    "e26_doge_relay_floor": "2026-08-30",
    "e26_doge_p2p_peers": "2026-08-30",
    "e27_bch_block_propagation": "2026-08-30",
    "e27_bch_tx_propagation": "2026-08-30",
    "e27_bch_relay_floor": "2026-08-30",
    "e27_bch_p2p_peers": "2026-08-30",
    "e28_fee_estimator_accuracy": "2026-08-30",
    "e29_gas_estimators": "2026-08-31",
    "e29_eth_block_fees": "2026-08-31",
    "e30_gas_estimator_accuracy": "2026-08-31",
}


def dataset_window(ds: str) -> tuple[str, str] | None:
    """The fixed sample window for one dataset, or None if it has no pin.

    None is a refusal, not a default. An unpinned dataset is left out of the public sample rather
    than published with a guessed window, because guessing is how a frozen sample becomes rolling.
    """
    start = SAMPLE_STARTS.get(ds)
    if not start:
        return None
    y, m, d = (int(x) for x in start.split("-"))
    return start, (date(y, m, d) + timedelta(days=SAMPLE_DAYS - 1)).isoformat()


def repo_span(datasets) -> tuple[str, str] | None:
    """Earliest start and latest end across a product's windowed datasets, for the card."""
    ws = [w for w in (dataset_window(d) for d in datasets if d in WINDOWED) if w]
    return (min(w[0] for w in ws), max(w[1] for w in ws)) if ws else None


def unpinned_warning() -> str | None:
    """Warn when a windowed dataset has no pin -- the only thing left that needs a human.

    Replaces the old calendar nag, which fired on a schedule and could always be safely deferred.
    A warning that is always safe to ignore trains you to ignore warnings. This one fires only
    when a new collector has been added and its repo would otherwise publish nothing.
    """
    missing = sorted(d for d in WINDOWED if d not in SAMPLE_STARTS)
    if not missing:
        return None
    return (f"{len(missing)} windowed dataset(s) have no SAMPLE_STARTS pin and are EXCLUDED from "
            f"the public sample: {', '.join(missing)}. Add each one's first collection date to "
            f"SAMPLE_STARTS in hf_push.py. The private archive is unaffected.")


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



MANIFEST = "e0_run_manifest"
# Held back from the public window. The manifest is deliberately absent: it is the coverage
# attestation and is useless withheld.
WINDOWED = {
    "e1_mempool_lifecycle", "e1_mempool_minutely", "e1_mempool_dropped",
    "e3_mempool_divergence", "e8_btc_mempool_lifecycle", "e9_btc_mempool_divergence",
    "e10_quote_benchmark", "e11_ltc_mempool_lifecycle", "e12_onramp_quotes",
    "e13_remittance_quotes", "e14_l2_preconf", "e15_fee_estimators",
    "e16_dex_routes", "e17_perp_depth", "e28_fee_estimator_accuracy",
    "e29_gas_estimators", "e29_eth_block_fees",
    "e30_gas_estimator_accuracy",
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
                "e19_stratum_jobs",
                # New observation panel stays private until its outcome semantics are validated.
                "e31_btc_fee_quotes", "e31_btc_tx_observations", "e31_btc_tx_events",
                "e31_btc_collection_rounds"}

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
# HISTORICAL partitions, which makes row filtering retroactive for uploaded partitions,
# so matching rows are replaced on the next upload. Fully excluded partitions need
# a separately reviewed removal; routine publication never deletes remote files.
#
# Deliberately applied to the private archive as well. Keeping a private copy of data we should
# not have gathered buys nothing and states the wrong intent. The git data repo still holds the
# raw record, so the decision stays reversible if the source ever licenses it.
#
# paradex: "You further agree not to engage in data mining, robots, scraping, or similar data
# gathering or extraction methods of content or information from the Services."
REDACTIONS = {"e17_perp_depth": ("venue", {"paradex"})}

_SHARED_TAIL = '''
## Coverage

`collection_runs` records collection windows, poll counts and failures. It is published in full and may cover dates beyond the fixed data sample. Collection gaps are not interpolated. Use this table together with measurement timestamps and error fields to assess coverage.

## License and contact

The public sample is published under ODC-BY. Attribute it to "DataForge (dataforge-labs)". For questions about the data or access to additional history, open a discussion in this repository.
'''
PRODUCTS = {
    "bitcoin-mining-pool-templates": {
        "datasets": ["e20_stratum_jobs_direct", MANIFEST],
        "example": "e20_stratum_jobs_direct",
        "pretty": 'Bitcoin mining pool Stratum job observations',
        "tags": ["bitcoin", "mining", "mining-pools", "stratum", "p2p", "network",
                 "block-propagation", "blockchain", "time-series"],
        "size": "100K<n<1M",
        "body": (ROOT / "docs" / "dataset_cards" / "bitcoin-mining-pool-templates.md").read_text(encoding="utf-8"),
    },
    "ethereum-attestation-pool": {
        "datasets": ["e18_attestation_pool", MANIFEST],
        "example": "e18_attestation_pool",
        "pretty": 'Ethereum pending and included attestation counts',
        "tags": ["ethereum", "beacon-chain", "consensus", "attestations", "staking",
                 "validator", "blockchain", "time-series"],
        "size": "10K<n<100K",
        "body": (ROOT / "docs" / "dataset_cards" / "ethereum-attestation-pool.md").read_text(encoding="utf-8"),
    },
    "equity-perp-price-discovery": {
        "datasets": ["e23_perp_mark_index", MANIFEST],
        "example": "e23_perp_mark_index",
        "pretty": 'Equity and pre-IPO perpetual mark and index prices',
        "tags": ["equities", "perpetual-futures", "price-discovery", "after-hours",
                 "pre-ipo", "basis", "finance", "time-series", "cryptocurrency"],
        "size": "1M<n<10M",
        "body": (ROOT / "docs" / "dataset_cards" / "equity-perp-price-discovery.md").read_text(encoding="utf-8"),
    },
    "crypto-options-surface": {
        "datasets": ["e22_options_surface", "e22_options_book", MANIFEST],
        "example": "e22_options_surface",
        "pretty": 'Crypto options marks, implied volatility and order books',
        "tags": ["options", "implied-volatility", "derivatives", "greeks", "order-book",
                 "bitcoin", "ethereum", "solana", "cryptocurrency", "time-series"],
        "size": "1M<n<10M",
        "body": (ROOT / "docs" / "dataset_cards" / "crypto-options-surface.md").read_text(encoding="utf-8"),
    },
    "ethereum-mempool": {
        "datasets": ["e1_mempool_minutely", "e1_mempool_dropped", "e1_mempool_lifecycle",
                     "e3_mempool_divergence", MANIFEST],
        "example": "e1_mempool_minutely",
        "pretty": 'Ethereum pending-transaction activity and lifecycle observations',
        "tags": ["mempool", "ethereum", "transactions", "blockchain", "time-series"],
        "size": "1M<n<10M",
        "body": (ROOT / "docs" / "dataset_cards" / "ethereum-mempool.md").read_text(encoding="utf-8"),
    },
    "dogecoin-network-propagation": {
        "datasets": ["e26_doge_block_propagation", "e26_doge_tx_propagation",
                     "e26_doge_relay_floor", "e26_doge_p2p_peers", MANIFEST],
        "example": "e26_doge_block_propagation",
        "pretty": 'Dogecoin peer announcements and relay-fee policies',
        "tags": ["dogecoin", "peer-to-peer", "propagation", "networking", "latency",
                 "blockchain", "time-series"],
        "size": "1M<n<10M",
        "body": (ROOT / "docs" / "dataset_cards" / "dogecoin-network-propagation.md").read_text(encoding="utf-8"),
    },
    "bitcoin-cash-network-propagation": {
        "datasets": ["e27_bch_block_propagation", "e27_bch_tx_propagation",
                     "e27_bch_relay_floor", "e27_bch_p2p_peers", MANIFEST],
        "example": "e27_bch_block_propagation",
        "pretty": 'Bitcoin Cash peer announcements and relay-fee policies',
        "tags": ["bitcoin-cash", "peer-to-peer", "propagation", "networking", "latency",
                 "blockchain", "time-series"],
        "size": "1M<n<10M",
        "body": (ROOT / "docs" / "dataset_cards" / "bitcoin-cash-network-propagation.md").read_text(encoding="utf-8"),
    },
    "litecoin-network-propagation": {
        "datasets": ["e25_ltc_block_propagation", "e25_ltc_tx_propagation",
                     "e25_ltc_relay_floor", "e25_ltc_p2p_peers", MANIFEST],
        "example": "e25_ltc_block_propagation",
        "pretty": 'Litecoin peer announcements and relay-fee policies',
        "tags": ["litecoin", "peer-to-peer", "propagation", "networking", "latency",
                 "blockchain", "time-series"],
        "size": "1M<n<10M",
        "body": (ROOT / "docs" / "dataset_cards" / "litecoin-network-propagation.md").read_text(encoding="utf-8"),
    },
    "bitcoin-network-propagation": {
        "datasets": ["e21_btc_block_propagation", "e21_btc_tx_propagation",
                     "e21_btc_relay_floor", "e21_btc_p2p_peers", MANIFEST],
        "example": "e21_btc_block_propagation",
        "pretty": 'Bitcoin peer announcements and relay-fee policies',
        "tags": ["bitcoin", "peer-to-peer", "propagation", "networking", "latency",
                 "blockchain", "time-series"],
        "size": "1M<n<10M",
        "body": (ROOT / "docs" / "dataset_cards" / "bitcoin-network-propagation.md").read_text(encoding="utf-8"),
    },
    "bitcoin-fee-estimator-accuracy": {
        "datasets": ["e15_fee_estimators", "e28_fee_estimator_accuracy", MANIFEST],
        "example": "e28_fee_estimator_accuracy",
        "pretty": 'Bitcoin fee recommendations and block-fee comparisons',
        "tags": ["bitcoin", "fee-estimation", "transaction-fees", "benchmark",
                 "forecasting", "blockchain", "time-series"],
        "size": "100K<n<1M",
        "body": (ROOT / "docs" / "dataset_cards" / "bitcoin-fee-estimator-accuracy.md").read_text(encoding="utf-8"),
    },
    "ethereum-gas-estimator-accuracy": {
        "datasets": ["e29_gas_estimators", "e29_eth_block_fees",
                     "e30_gas_estimator_accuracy", MANIFEST],
        "example": "e30_gas_estimator_accuracy",
        "pretty": 'Ethereum gas recommendations and block-fee comparisons',
        "tags": ["ethereum", "gas", "gas-price", "fee-estimation", "rpc",
                 "benchmark", "blockchain", "time-series"],
        "size": "100K<n<1M",
        "body": (ROOT / "docs" / "dataset_cards" / "ethereum-gas-estimator-accuracy.md").read_text(encoding="utf-8"),
    },
    "bitcoin-mempool-lifecycle": {
        "datasets": ["e8_btc_mempool_lifecycle", "e9_btc_mempool_divergence",
                     "e11_ltc_mempool_lifecycle", "e8_btc_block_composition", MANIFEST],
        "example": "e8_btc_mempool_lifecycle",
        "pretty": 'Bitcoin and Litecoin transaction lifecycle observations',
        "tags": ["mempool", "bitcoin", "litecoin", "unconfirmed-transactions",
                 "transaction-fees", "blockchain", "cryptocurrency", "time-series"],
        "size": "1M<n<10M",
        "body": (ROOT / "docs" / "dataset_cards" / "bitcoin-mempool-lifecycle.md").read_text(encoding="utf-8"),
    },
    "fiat-onramp-pricing": {
        "datasets": ["e12_onramp_quotes", MANIFEST],
        "example": "e12_onramp_quotes",
        "pretty": 'Fiat on-ramp quotes, reference prices and fees',
        "tags": ["on-ramp", "payments", "retail", "pricing", "cryptocurrency",
                 "fees", "time-series"],
        "size": "100K<n<1M",
        "body": (ROOT / "docs" / "dataset_cards" / "fiat-onramp-pricing.md").read_text(encoding="utf-8"),
    },
    "l2-preconfirmation-reliability": {
        "datasets": ["e14_l2_preconf", MANIFEST],
        "example": "e14_l2_preconf",
        "pretty": 'OP Stack unsafe-head observations and canonical-chain checks',
        "tags": ["layer-2", "rollup", "sequencer", "ethereum", "reliability",
                 "blockchain", "time-series"],
        "size": "100K<n<1M",
        "body": (ROOT / "docs" / "dataset_cards" / "l2-preconfirmation-reliability.md").read_text(encoding="utf-8"),
    },
    "solana-dex-execution": {
        "datasets": ["e24_solana_quotes", "e24_solana_routes", MANIFEST],
        "example": "e24_solana_quotes",
        "pretty": 'Solana swap quotes and route details',
        "tags": ["solana", "dex", "routing", "liquidity", "execution", "cryptocurrency",
                 "trading", "time-series"],
        "size": "1M<n<10M",
        "body": (ROOT / "docs" / "dataset_cards" / "solana-dex-execution.md").read_text(encoding="utf-8"),
    },
    "crypto-execution-costs": {
        "datasets": ["e10_quote_benchmark", "e16_dex_routes", "e17_perp_depth",
                     MANIFEST],
        "example": "e10_quote_benchmark",
        "pretty": 'Ethereum swap quotes, routing and perpetual order-book depth',
        "tags": ["dex", "ethereum", "defi", "execution-cost", "slippage", "routing",
                 "perpetual-futures", "order-book", "trading", "time-series"],
        "size": "10K<n<100K",
        "body": (ROOT / "docs" / "dataset_cards" / "crypto-execution-costs.md").read_text(encoding="utf-8"),
    },
    "remittance-pricing-panel": {
        "datasets": ["e13_remittance_quotes", MANIFEST],
        "example": "e13_remittance_quotes",
        "pretty": 'Remittance comparison rates, fees and recipient amounts',
        "tags": ["remittance", "exchange-rates", "fintech", "payments", "banking",
                 "foreign-exchange", "time-series", "finance"],
        "size": "10K<n<100K",
        "body": (ROOT / "docs" / "dataset_cards" / "remittance-pricing-panel.md").read_text(encoding="utf-8"),
    },
}


def _card(name: str, p: dict) -> str:
    import yaml

    tags = "\n".join(f"  - {t}" for t in p["tags"])
    repo = f"{OWNER}/{name}"
    ex = PUBLIC_NAMES[p["example"]]
    configs = yaml.safe_dump({'configs': public_configs(p)}, sort_keys=False)
    _w = repo_span(p["datasets"])
    load = (
        "\n```python\n"
        "from datasets import load_dataset\n\n"
        f'data = load_dataset("{repo}",\n'
        f'                    "{ex}", split="train")\n'
        "df = data.to_pandas()\n"
        "```\n"
    )
    # QUOTED. A colon inside pretty_name ("Crypto execution costs: DEX quotes...")
    # makes the entire front matter fail to parse, and HF then applies none of the
    # metadata, silently undoing the discoverability work this block exists for.
    # Caught by validating the YAML instead of eyeballing it.
    return (f"---\nlicense: odc-by\npretty_name: \"{p['pretty']}\"\ntags:\n{tags}\n"
            f"task_categories:\n  - time-series-forecasting\n"
            f"size_categories:\n  - {p['size']}\n" + configs + "---\n\n"
            + p["body"]
            + f"\n## Files and access\n\n"
              f"Data is stored as Parquet files under `table_name/YYYY/MM/`, with partitions "
              f"for collection windows. Each measurement table has a fixed {SAMPLE_DAYS}-day "
              f"sample beginning at its configured collection start date"
            + (f". The sample windows in this repository span {_w[0]} to {_w[1]}" if _w else "")
            + ". Availability within each window depends on successful collection. "
              "The public sample dates remain fixed as additional history accumulates privately. "
              "Contact DataForge through the discussions tab to enquire about additional history.\n"
              "\n### Load a table\n"
            + "\nInstall `datasets` and `pandas` to run this example. The `train` split contains "
              "all observations in the selected table; it is not a predefined modelling split.\n"
            + load + _SHARED_TAIL + storage_reference(p))


def _partitions(dataset: str) -> list[Path]:
    root = DATA / dataset
    return sorted(root.rglob("*.parquet")) if root.exists() else []


def _stage(target: Path, datasets, window, card: str | None) -> dict:
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)
    summary = {}
    for ds in datasets:
        output_dir = target / (PUBLIC_NAMES[ds] if window is not None else ds)
        files = _partitions(ds)
        if window is not None and ds in WINDOWED:
            w = dataset_window(ds)
            if w is None:
                print(f"  !! {ds}: no SAMPLE_STARTS pin, excluded from the public sample",
                      flush=True)
                continue
            lo, hi = w
            # Stems are YYYY-MM-DD or YYYY-MM-DDTHHMMZ; both compare correctly as strings.
            files = [f for f in files if lo <= f.stem[:10] <= hi]
        if not files:
            continue
        red = REDACTIONS.get(ds)
        for f in files:
            dst = output_dir / f.relative_to(DATA / ds)
            dst.parent.mkdir(parents=True, exist_ok=True)
            if red is None:
                shutil.copy(f, dst)
                continue
            # A source whose terms forbid collection must not be published from HISTORICAL
            # partitions either, and rewriting the archive is the wrong lever: it destroys a
            # record we may need. Filtering at stage time is declarative, reversible, and --
            # the upload replaces each corresponding partition. Entirely excluded
            # partitions require a separately reviewed removal from published history.
            import pandas as pd
            col, drop = red
            df = pd.read_parquet(f)
            if col in df.columns:
                df = df[~df[col].isin(drop)]
            if not len(df):
                dst.unlink(missing_ok=True)
                continue
            df.to_parquet(dst, index=False)
        files = [f for f in files if (output_dir / f.relative_to(DATA / ds)).exists()]
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
        # Publish additively: a partial checkout must never remove published data.
        # Renames use hf_rename's verified copies and exact paths. Future sample re-pins
        # need a separate reviewed migration; routine uploads do not prune old samples.
        api.upload_folder(repo_id=repo, repo_type="dataset", folder_path=str(stage),
                          commit_message=f"{label}: {date.today().isoformat()}")
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
    _w = unpinned_warning()
    if _w:
        print("  !! " + _w, flush=True)
        _nag(_w)

    ok = _push(ARCHIVE_REPO, everything, None, None, "archive", private=True)
    _receipt(ok, _last_archive_summary)

    # This tuple is now only a flag meaning "public repo"; the real window is resolved per
    # dataset inside _stage. The archive still passes None and stays purely additive.
    window = ("per-dataset", "per-dataset")
    print(f"  public sample: each dataset FROZEN at its own {SAMPLE_DAYS} days from its first "
          f"collection", flush=True)
    for name, p in PRODUCTS.items():
        _push(f"{OWNER}/{name}", p["datasets"], window, _card(name, p), name, private=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
