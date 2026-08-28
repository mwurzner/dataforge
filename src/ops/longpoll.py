"""The resident ephemeral collector. Runs ~5.5h per invocation, four times a day.

WHY RESIDENT RATHER THAN CRON. GitHub cron fires at most every 5 minutes and scheduled runs are
best-effort. A sparse snapshot cannot see LIFECYCLE -- how long a transaction sat pending, or that
it was dropped and never mined -- and lifecycle is precisely the part no archive can reconstruct.
Actions caps a job at 6 hours, so 5.5h leaves margin, and four staggered starts give ~22h/day of
coverage.

TWO CHAINS, DIFFERENT CLOCKS. Ethereum blocks arrive every ~12s and its mempool turns over in
seconds, so E1/E3 poll hard (5s). Bitcoin blocks arrive every ~10 min, a full mempool read is
3.1 MB, and the measured churn is ~327 new transactions/min, so E8/E9 poll every 60s. Running both
in one process costs nothing extra -- the loop is almost entirely idle waiting on the network.

HONEST NOTE ON WHAT IS WORTH SELLING. E1/E3 (Ethereum) are ALSO published free and CC-0 by the
Flashbots Mempool Dumpster, with a longer history and a wider node network than ours, so they are
NOT the product. E8/E9 (Bitcoin) are the part with no free continuously-updated equivalent.

AND "THEY COST NOTHING" WAS WRONG -- an earlier version of this file said exactly that. Measured:
E1 is 135 MB of the 158 MB/day this project writes, i.e. 85% of all storage, which exhausts the
100 GB free tier in 1.7 years. Aggregating E1 to per-minute rows (keeping full rows only for the
never-mined transactions, which exist nowhere else) extends that to 11.7 years and leaves the
headroom for Bitcoin. The independent-observation value survives, because what a second observer
actually contributes is a dwell DISTRIBUTION to disagree with, not duplicate transaction bodies
that are on-chain forever anyway.

OUTPUT IS PARTITIONED BY RUN, not just by date, because there are four runs a day and each is an
independent observation window. A gap between windows is a real gap and is visible as one.
"""
from __future__ import annotations

import os
import sys
import time
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.ephemeral import (e1_mempool, e3_divergence, e8_btc_mempool, e10_quotes,
                           e12_onramp, e13_remit, e14_preconf, e15_feeest,
                           e17_perpdepth)

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"

DURATION_S = int(os.environ.get("DF_DURATION_S", 5 * 3600 + 1800))   # 5.5h
POLL_S = float(os.environ.get("DF_POLL_S", 5))
DIVERGENCE_EVERY_S = float(os.environ.get("DF_DIVERGENCE_EVERY_S", 300))
# Bitcoin cadences. A full txid read is 3.1 MB gzipped, so 60s keeps the job's download to about
# 1 GB per 5.5h window while still resolving the ~327 new transactions/min that actually arrive.
BTC_POLL_S = float(os.environ.get("DF_BTC_POLL_S", 60))
BTC_BLOCK_EVERY_S = float(os.environ.get("DF_BTC_BLOCK_EVERY_S", 120))
BTC_DIVERGENCE_EVERY_S = float(os.environ.get("DF_BTC_DIVERGENCE_EVERY_S", 900))
# E10 quote benchmark. One round is 24 requests against free public endpoints, so 15 minutes keeps
# us at ordinary-client volume (~2,300/day) rather than hammering somebody's free service.
QUOTE_EVERY_S = float(os.environ.get("DF_QUOTE_EVERY_S", 900))
# Checkpoint cadence. A 5.5h job holds its tracker state in memory, so a runner kill would
# otherwise lose the entire window -- and an ephemeral window cannot be re-collected. Writing
# every 30 min caps the worst-case loss at 30 minutes instead of 5.5 hours.
CHECKPOINT_EVERY_S = float(os.environ.get("DF_CHECKPOINT_EVERY_S", 1800))
# Seconds between pre-existing fee lookups. 2.0 gives ~0.5 req/s, about 8,000 transactions over a
# 4.5h run, roughly 10% of a typical 82,000-transaction opening snapshot. Raise it to back off.
PRE_FEE_EVERY_S = float(os.environ.get("DF_PRE_FEE_EVERY_S", 2.0))
# Full-mempool RPC snapshot. One call supplies ~34% of the tracked set's fees against ~9% from a
# whole run of per-transaction sampling, so this is the dominant source. 15 minutes keeps the wire
# cost near 83 MB per run (4.6 MB gzipped per call) against a free public node.
RPC_MEMPOOL_EVERY_S = float(os.environ.get("DF_RPC_MEMPOOL_EVERY_S", 900))
# E1 STORAGE MODE. "aggregate" (default) stores a per-minute summary plus full rows for the
# never-mined transactions, instead of a row per transaction. E1 is 85% of everything this
# project stores AND is the one dataset established as NOT scarce -- Flashbots publishes the same
# measurement free and CC-0 from a wider network. Per-transaction rows exhaust the 100 GB free
# tier in 1.7 years; aggregating extends it to 11.7 and leaves the room for Bitcoin, which has no
# free equivalent. Set DF_E1_MODE=full to restore per-transaction storage.
E1_MODE = os.environ.get("DF_E1_MODE", "aggregate").lower()


def write(dataset: str, df: pd.DataFrame, run_id: str, quiet: bool = False) -> Path | None:
    """One partition per dataset per run. An empty frame is a FAILURE SIGNAL, not data --
    it is refused rather than written (families 70/81/86/93)."""
    if df is None or not len(df):
        print(f"  !! {dataset}: empty frame, refusing to write", flush=True)
        return None
    date = run_id[:10]
    d = DATA / dataset / date[:4] / date[5:7]
    d.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    out.insert(0, "run_id", run_id)
    p = d / f"{run_id}.parquet"
    out.to_parquet(p, index=False)          # overwrite: checkpoints supersede each other
    if not quiet:
        print(f"  wrote {dataset}: {len(out):,} rows -> {p.relative_to(DATA)}", flush=True)
    return p


def _fee_sampler(btc, stop: "threading.Event", failures: list) -> None:
    """Sample /mempool/recent on its OWN clock, because the main loop cannot deliver one.

    THE BUG THIS FIXES. The sampler used to sit inline behind `if now - last_fee_sample >= 2`,
    but the loop ends in `time.sleep(POLL_S)` and production sets DF_POLL_S=5. A 2s condition
    checked once every 5s samples every 5s. The intended 2s cadence never ran.

    It matters because coverage is bounded by call frequency: /mempool/recent returns only the
    ten newest transactions, so one call per 5s captures at most 2.0 tx/s against a measured
    arrival rate of ~4.3 tx/s -- a ceiling near 47%, and the realised figure was 16%. At a true
    2s cadence the ceiling is ~5.0 tx/s, i.e. above the arrival rate.

    Thread-safe by inspection, not by assumption: poll_recent writes ONLY to `self.fees` (which
    nothing else writes) and two diagnostic counters. It never touches `seen`, `mined` or `pool`,
    which are the structures the main loop mutates.
    """
    while not stop.is_set():
        try:
            btc.poll_recent()
        except Exception as exc:
            if len(failures) < 200:
                failures.append(f"e8 recent: {exc}")
        stop.wait(2.0)


def _node_fee_snapshotter(btc, stop: "threading.Event", failures: list) -> None:
    """Take a full-mempool fee snapshot from a public Bitcoin node, periodically.

    Own thread because the call is ~2s and multi-megabyte, and the main loop must not wait on it.
    Same thread-safety argument as the other samplers: writes only `self.fees` and its own
    counters, never `seen`, `mined` or `pool`.
    """
    while not stop.is_set():
        try:
            btc.snapshot_node_fees()
        except Exception as exc:
            if len(failures) < 200:
                failures.append(f"e8 node snapshot: {exc}")
        stop.wait(RPC_MEMPOOL_EVERY_S)


def _pre_fee_sampler(btc, stop: "threading.Event", failures: list) -> None:
    """Walk the opening mempool snapshot, capturing fees before those transactions can be evicted.

    Separate from _fee_sampler because it solves the opposite problem. That one covers ARRIVALS,
    which is where mined rows get their fees. This covers the transactions that were ALREADY
    pending when the run began -- the only ones that live long enough to be dropped, and
    therefore the only source of fee data for drop rows.

    Same thread-safety argument as _fee_sampler: this writes to `self.fees`, which nothing else
    writes, and to two counters where a lost increment is cosmetic. It never touches `seen`,
    `mined` or `pool`.
    """
    # Let poll_pool establish the baseline before draining a worklist built from it.
    stop.wait(20.0)
    while not stop.is_set():
        try:
            btc.sample_pre_existing_fee()
        except Exception as exc:
            if len(failures) < 200:
                failures.append(f"e8 pre-fee: {exc}")
        stop.wait(PRE_FEE_EVERY_S)


def _quote_round():
    """Every quote-shaped collector, run OFF the main loop.

    These take ~140s of wall clock, nearly all of it deliberate pacing (see e10_quotes._MIN_GAP,
    added after LI.FI answered 429 to 19.5% of requests). Running them inline starved the 2s
    Bitcoin fee sampling for that entire window -- roughly 40 minutes of a 4.5h run -- and that
    sampling feeds the fee-accuracy study directly.

    Safe on a worker thread because these collectors share NO state with the mempool trackers:
    each opens its own HTTP connections and returns a fresh DataFrame, which the main thread
    appends. Nothing here touches `tracker`, `btc` or `ltc`.
    """
    q, r = e10_quotes.sample()
    return q, r, e12_onramp.sample(), e13_remit.sample(), e17_perpdepth.sample()


def _collect_quotes(fut, quote_rows, route_rows, onramp_rows, remit_rows, depth_rows, failures):
    """Drain a finished quote round. Returns True if the future was consumed."""
    if fut is None or not fut.done():
        return False
    try:
        _q, _r, _o, _rm, _d = fut.result()
        quote_rows.append(_q)
        if len(_r):
            route_rows.append(_r)
        onramp_rows.append(_o)
        remit_rows.append(_rm)
        depth_rows.append(_d)
    except Exception as exc:
        failures.append(f"quote round: {exc}")
    return True


def _checkpoint(run_id: str, tracker, div_rows, btc, btc_div_rows, quote_rows=()) -> None:
    """Partial write of everything held in memory. For E1 the fate of anything not yet mined is
    'unresolved', because reconciliation has not run -- never claim 'dropped' without evidence."""
    if tracker.seen:
        f = tracker.frame(set())
        if E1_MODE == "full":
            write(e1_mempool.DATASET, f, run_id, quiet=True)
        else:
            write("e1_mempool_minutely", e1_mempool.aggregate(f), run_id, quiet=True)
            write("e1_mempool_dropped", e1_mempool.keep_dropped(f), run_id, quiet=True)
    if div_rows:
        write(e3_divergence.DATASET, pd.concat(div_rows, ignore_index=True), run_id, quiet=True)
    if btc.seen or btc.pre_existing:
        write(e8_btc_mempool.DATASET, btc.frame(), run_id, quiet=True)
    if btc_div_rows:
        write(e8_btc_mempool.DIVERGENCE_DATASET,
              pd.concat(btc_div_rows, ignore_index=True), run_id, quiet=True)
    if len(quote_rows):
        write(e10_quotes.DATASET, pd.concat(quote_rows, ignore_index=True), run_id, quiet=True)


def main() -> int:
    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%MZ")
    print(f"DataForge ephemeral run {run_id}", flush=True)
    print(f"  duration {DURATION_S/3600:.2f}h | ETH poll {POLL_S}s | BTC poll {BTC_POLL_S}s\n",
          flush=True)

    tracker = e1_mempool.MempoolTracker(e1_mempool.ENDPOINTS[0])
    btc = e8_btc_mempool.BtcMempoolTracker()
    # Litecoin rides the identical Esplora API (probed: litecoinspace.org answers the same
    # endpoints). Tiny pool (~10 KB a poll), no second provider, so no cross-check and no
    # divergence sampling; the frame records that honestly rather than faking a weaker claim.
    ltc = e8_btc_mempool.BtcMempoolTracker(e8_btc_mempool.CHAINS["ltc"]["primary"],
                                           e8_btc_mempool.CHAINS["ltc"]["secondary"])
    preconf = e14_preconf.make_watchers()
    div_rows: list[pd.DataFrame] = []
    btc_div_rows: list[pd.DataFrame] = []
    t0 = time.time()
    last_block = last_div = last_ckpt = 0.0
    last_btc = last_btc_block = last_btc_div = 0.0
    last_quote = 0.0
    last_ltc = last_ltc_block = 0.0
    last_preconf = last_preconf_safe = last_preconf_hb = 0.0
    last_fee = last_fee_sample = 0.0
    fee_rows: list[pd.DataFrame] = []
    quote_rows: list[pd.DataFrame] = []
    route_rows: list[pd.DataFrame] = []
    depth_rows: list[pd.DataFrame] = []
    quote_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='quotes')
    quote_future = None
    fee_stop = threading.Event()
    fee_thread = None
    pre_fee_thread = None
    node_fee_thread = None
    onramp_rows: list[pd.DataFrame] = []
    remit_rows: list[pd.DataFrame] = []
    failures: list[str] = []

    # The fee sampler runs on its own clock from here; see _fee_sampler for why it cannot
    # live in the main loop. Daemon so a crash in the loop can never leave it running.
    fee_thread = threading.Thread(target=_fee_sampler, args=(btc, fee_stop, failures),
                                  name="fee-sampler", daemon=True)
    fee_thread.start()
    pre_fee_thread = threading.Thread(target=_pre_fee_sampler, args=(btc, fee_stop, failures),
                                      name="pre-fee-sampler", daemon=True)
    pre_fee_thread.start()
    node_fee_thread = threading.Thread(target=_node_fee_snapshotter,
                                       args=(btc, fee_stop, failures),
                                       name="node-fee-snapshot", daemon=True)
    node_fee_thread.start()

    try:
        while time.time() - t0 < DURATION_S:
            tracker.poll_pending()
            now = time.time()

            if now - last_block >= 12:            # ETH blocks arrive ~12s apart
                tracker.poll_blocks()
                last_block = now
            if now - last_div >= DIVERGENCE_EVERY_S:
                try:
                    div_rows.append(e3_divergence.sample())
                except Exception as exc:
                    failures.append(f"e3: {exc}")
                last_div = now

            # ---- Bitcoin, on its own much slower clock ----
            if now - last_btc >= BTC_POLL_S:
                try:
                    btc.poll_pool()
                except Exception as exc:
                    failures.append(f"e8 pool: {exc}")
                last_btc = now
            if now - last_btc_block >= BTC_BLOCK_EVERY_S:
                try:
                    btc.poll_blocks()
                except Exception as exc:
                    failures.append(f"e8 blocks: {exc}")
                last_btc_block = now
                # Refresh the cross-check pool in bulk, then verify. The bulk read settles far
                # more candidates per request than individual status checks ever could.
                try:
                    btc.verify_dropped(cap=100)
                except Exception as exc:
                    failures.append(f"e8 verify: {exc}")
            if now - last_btc_div >= BTC_DIVERGENCE_EVERY_S:
                try:
                    btc.refresh_other()
                    btc_div_rows.append(e8_btc_mempool.divergence())
                except Exception as exc:
                    failures.append(f"e9: {exc}")
                last_btc_div = now

            if now - last_preconf >= 6:
                for w in preconf.values():
                    try:
                        w.poll_unsafe()
                    except Exception as exc:
                        failures.append(f"e14 {w.chain}: {exc}")
                last_preconf = now
            if now - last_preconf_safe >= 60:
                for w in preconf.values():
                    try:
                        w.poll_safe_and_verify()
                    except Exception as exc:
                        failures.append(f"e14 safe {w.chain}: {exc}")
                last_preconf_safe = now
            if now - last_preconf_hb >= 120:
                for w in preconf.values():
                    w.heartbeat()
                last_preconf_hb = now

            if now - last_fee >= 300:      # 5 min: estimators refresh on that order
                try:
                    fee_rows.append(e15_feeest.sample())
                except Exception as exc:
                    failures.append(f"e15: {exc}")
                last_fee = now

            if now - last_ltc >= BTC_POLL_S:
                try:
                    ltc.poll_pool()
                except Exception as exc:
                    failures.append(f"e11 pool: {exc}")
                last_ltc = now
            if now - last_ltc_block >= 150:       # Litecoin blocks arrive ~2.5 min apart
                try:
                    ltc.poll_blocks()
                    ltc.verify_dropped(cap=30)
                except Exception as exc:
                    failures.append(f"e11 blocks: {exc}")
                last_ltc_block = now

            # Non-blocking: collect a finished round, then start the next one. The main loop
            # must never wait on these -- see _quote_round.
            if _collect_quotes(quote_future, quote_rows, route_rows, onramp_rows,
                               remit_rows, depth_rows, failures):
                quote_future = None
            if quote_future is None and now - last_quote >= QUOTE_EVERY_S:
                quote_future = quote_pool.submit(_quote_round)
                last_quote = now

            if now - last_ckpt >= CHECKPOINT_EVERY_S:
                _checkpoint(run_id, tracker, div_rows, btc, btc_div_rows, quote_rows)
                last_ckpt = now
            if tracker.n_poll % 240 == 0 and tracker.n_poll:
                print(f"    {(now-t0)/60:6.1f} min  ETH seen {len(tracker.seen):>8,} "
                      f"mined {len(tracker.mined):>8,} failed {tracker.n_failed:>4}  |  "
                      f"BTC seen {len(btc.seen):>7,} mined {len(btc.mined):>7,} "
                      f"polls {btc.n_poll:>4} failed {btc.n_failed}", flush=True)
            time.sleep(POLL_S)
    except KeyboardInterrupt:
        print("\n  interrupted -- reconciling what was collected", flush=True)

    # A round still in flight holds real observations. Give it a bounded chance to land rather
    # than discarding it, but never let it delay reconciliation indefinitely.
    fee_stop.set()
    if fee_thread is not None:
        fee_thread.join(timeout=10)
    if pre_fee_thread is not None:
        pre_fee_thread.join(timeout=10)
    if node_fee_thread is not None:
        node_fee_thread.join(timeout=15)

    if quote_future is not None:
        try:
            quote_future.result(timeout=180)
        except Exception as exc:
            failures.append(f"quote round (final): {exc}")
        _collect_quotes(quote_future, quote_rows, route_rows, onramp_rows,
                        remit_rows, depth_rows, failures)
    quote_pool.shutdown(wait=False)

    # Final catch-up and reconciliation. Anything seen, never mined, and absent from the pool is
    # genuinely DROPPED -- rows that exist nowhere else.
    try:
        tracker.poll_blocks()
        still = tracker.reconcile()
    except Exception as exc:
        failures.append(f"reconcile: {exc}")
        still = set()
    try:
        ltc.poll_pool()
        ltc.poll_blocks()
        ltc.verify_dropped()
    except Exception as exc:
        failures.append(f"e11 final: {exc}")
    try:
        btc.poll_pool()
        btc.poll_blocks()
        # Authoritative check on every drop candidate. A set-difference alone manufactured 642
        # fake drops in a 200s test; all 12 sampled were still pending.
        btc.refresh_other()
        btc.verify_dropped()
    except Exception as exc:
        failures.append(f"e8 final: {exc}")

    df = tracker.frame(still)
    # An empty frame has no columns at all, so summarising it must not assume `fate` exists.
    # A collector that gathered nothing is a FAILURE TO REPORT -- never an exception that
    # discards the three other datasets successfully collected in the same run. Hit for real:
    # a transiently rate-limited Ethereum endpoint took down an otherwise complete run.
    fates = (" ".join(f"{k} {v:,}" for k, v in df["fate"].value_counts().items())
             if len(df) else "NOTHING COLLECTED")
    print(f"\n  E1: {len(df):,} txs | {fates} | polls {tracker.n_poll:,} "
          f"failed {tracker.n_failed} resets {tracker.n_filter_resets}", flush=True)
    if not len(df):
        failures.append("e1: collected zero transactions -- endpoint or filter failure")
    if E1_MODE == "full":
        write(e1_mempool.DATASET, df, run_id)
    else:
        # NEW DATASET NAMES on purpose. Reusing e1_mempool_lifecycle for a dropped-only subset
        # would silently change what an existing name means, and anyone joining old partitions to
        # new ones would get a population change disguised as a trend.
        agg = e1_mempool.aggregate(df)
        drp = e1_mempool.keep_dropped(df)
        write("e1_mempool_minutely", agg, run_id)
        write("e1_mempool_dropped", drp, run_id)
        if len(df):
            print(f"       stored as {len(agg):,} minute rows + {len(drp):,} dropped rows "
                  f"instead of {len(df):,} per-tx rows", flush=True)
    if div_rows:
        write(e3_divergence.DATASET, pd.concat(div_rows, ignore_index=True), run_id)

    bdf = btc.frame()
    # A SHORT RUN OBSERVES NOTHING. The first polls establish a baseline of pre-existing
    # transactions whose first_seen is unobservable; only what arrives AFTER that is lifecycle.
    # Four early test dispatches published ~80k-row partitions that were 100% baseline -- the
    # standing pool re-listed, with zero first-seen rows and zero drops. Pure noise in the
    # product dataset. A frame with no observed arrivals and no resolved drops is refused.
    observed = int(bdf["first_seen_ts"].notna().sum()) if len(bdf) else 0
    n_drops = int((bdf["fate"] == "dropped").sum()) if len(bdf) else 0
    if observed == 0 and n_drops == 0:
        print(f"  E8: {len(bdf):,} rows but ZERO observed lifecycle (all baseline) -- "
              f"partition refused, window too short", flush=True)
        bdf = bdf.iloc[0:0]
    if len(bdf):
        print(f"  E8: {len(bdf):,} txs | " +
              " ".join(f"{k} {v:,}" for k, v in bdf["fate"].value_counts().items()) +
              f" | pre_existing {int(bdf['pre_existing'].sum()):,} "
              f"polls {btc.n_poll:,} failed {btc.n_failed}", flush=True)
        # Fee coverage is the number a buyer conditions on, so it is reported every run rather
        # than left to be rediscovered from the parquet.
        _obs = bdf[~bdf["pre_existing"].astype(bool)]
        _cov = _obs["fee_rate_sat_vb"].notna().mean() if len(_obs) else float("nan")
        _pre = bdf[bdf["pre_existing"].astype(bool)]
        _pcov = _pre["fee_rate_sat_vb"].notna().mean() if len(_pre) else float("nan")
        _drp = bdf[bdf["fate"] == "dropped"]
        _dcov = _drp["fee_rate_sat_vb"].notna().mean() if len(_drp) else float("nan")
        # pre-existing coverage is the one that matters for DROP rows: every dropped
        # transaction observed so far was already pending when its run began.
        print(f"      fee coverage: arrivals {_cov:.1%} | pre-existing {_pcov:.1%} "
              f"| dropped {_dcov:.1%} "
              f"| sampler ok {btc.n_pre_fee_ok:,} miss {btc.n_pre_fee_miss:,} | node snapshots {btc.n_node_snapshots} gave {btc.n_node_fees:,}",
              flush=True)
    write(e8_btc_mempool.DATASET, bdf, run_id)
    if btc_div_rows:
        write(e8_btc_mempool.DIVERGENCE_DATASET,
              pd.concat(btc_div_rows, ignore_index=True), run_id)

    ldf = ltc.frame()
    l_observed = int(ldf["first_seen_ts"].notna().sum()) if len(ldf) else 0
    l_drops = int((ldf["fate"] == "dropped").sum()) if len(ldf) else 0
    if l_observed == 0 and l_drops == 0:
        print(f"  E11: {len(ldf):,} rows but ZERO observed lifecycle -- partition refused",
              flush=True)
    else:
        print(f"  E11 ltc: {len(ldf):,} txs | " +
              " ".join(f"{k} {v:,}" for k, v in ldf["fate"].value_counts().items()), flush=True)
        write("e11_ltc_mempool_lifecycle", ldf, run_id)

    if onramp_rows:
        odf = pd.concat(onramp_rows, ignore_index=True)
        ok_o = odf[odf["effective_rate"].notna()]
        print(f"  E12: {len(odf):,} on-ramp rows, {len(odf)-len(ok_o)} failed", flush=True)
        write(e12_onramp.DATASET, odf, run_id)

    if fee_rows:
        fdf = pd.concat(fee_rows, ignore_index=True)
        okf = fdf[fdf.sat_per_vb.notna()]
        sr = pd.to_numeric(fdf.spread_ratio, errors="coerce").dropna()
        print(f"  E15: {len(okf):,} fee estimates from {okf.provider.nunique()} providers, "
              f"max divergence {sr.max():.1f}x" if len(sr) else
              f"  E15: {len(okf):,} fee estimates", flush=True)
        write(e15_feeest.DATASET, fdf, run_id)

    pfs = []
    for w in preconf.values():
        try:
            w.poll_safe_and_verify()
            w.heartbeat()
        except Exception as exc:
            failures.append(f"e14 final {w.chain}: {exc}")
        pfs.append(w.frame())
    pdf = pd.concat([f for f in pfs if len(f)], ignore_index=True) if any(len(f) for f in pfs)         else pd.DataFrame()
    if len(pdf):
        nv = int((pdf["row_type"] == "violation").sum())
        for w in preconf.values():
            hb = pdf[(pdf.chain == w.chain) & (pdf.row_type == "heartbeat")]
            if len(hb):
                print(f"  E14 {w.chain}: lag {hb.lag_blocks.iloc[-1]} blocks, "
                      f"checked {w.n_checked}, violations "
                      f"{int(((pdf.chain == w.chain) & (pdf.row_type == 'violation')).sum())}",
                      flush=True)
        write(e14_preconf.DATASET, pdf, run_id)

    if remit_rows:
        rdf = pd.concat(remit_rows, ignore_index=True)
        okr = rdf[rdf["provider"].notna()]
        print(f"  E13: {len(okr):,} remittance quotes across "
              f"{okr['corridor'].nunique()} corridors, {rdf['error'].notna().sum()} errors",
              flush=True)
        write(e13_remit.DATASET, rdf, run_id)

    if depth_rows:
        ddf = pd.concat(depth_rows, ignore_index=True)
        okd = ddf[ddf.error.isna()]
        print(f"  E17: {len(ddf):,} depth snapshots, {len(okd)} readable "
              f"across {ddf.market.nunique()} markets", flush=True)
        write(e17_perpdepth.DATASET, ddf, run_id)

    if route_rows:
        rdf = pd.concat(route_rows, ignore_index=True)
        print(f"  E16: {len(rdf):,} route legs across {rdf.venue.nunique()} venues", flush=True)
        write(e10_quotes.ROUTE_DATASET, rdf, run_id)

    if quote_rows:
        qdf = pd.concat(quote_rows, ignore_index=True)
        okq = qdf[qdf["price"].notna()]
        print(f"  E10: {len(qdf):,} quotes, {len(qdf)-len(okq)} failed | "
              f"median spread {okq['spread_bps'].median():.1f} bps", flush=True)
        write(e10_quotes.DATASET, qdf, run_id)

    # Run-level telemetry, so a degraded run is visible without opening the data.
    write("e0_run_manifest", pd.DataFrame([{
        "started_utc": datetime.fromtimestamp(t0, timezone.utc).isoformat(timespec="seconds"),
        "duration_s": round(time.time() - t0, 1),
        "n_polls": tracker.n_poll, "n_failed_calls": tracker.n_failed,
        "n_filter_resets": tracker.n_filter_resets,
        "n_tx_seen": len(df), "n_divergence_samples": len(div_rows),
        "btc_n_polls": btc.n_poll, "btc_n_failed": btc.n_failed,
        "btc_n_tx_seen": len(bdf), "btc_n_pre_existing": int(bdf["pre_existing"].sum())
                                    if len(bdf) else 0,
        "btc_n_divergence_samples": len(btc_div_rows),
        "btc_n_drop_verified": btc.n_verified, "btc_n_flicker": btc.n_flicker,
        "btc_n_crosscheck_saved": btc.n_other_saved,
        "btc_n_fee_polls": btc.n_fee_polls, "btc_n_fees_captured": len(btc.fees),
        "n_quote_rounds": len(quote_rows),
        "reconciled": bool(still), "failures": "; ".join(failures)[:500],
    }]), run_id)

    if failures:
        print("\nFAILURES:", flush=True)
        for f in failures[:20]:
            print("  " + f, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
