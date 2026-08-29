"""Bitcoin and Litecoin mempool lifecycle.

Records each transaction from first sighting to its outcome: mined, still pending, or dropped.
Fee and vsize are attached where available.

Operational notes:
  A drop is only recorded once the transaction is absent from the chain, absent from every
  provider pool we hold, and has stayed absent for the debounce period. Weaker tests produce
  false positives in bulk.
  Fee coverage is partial and varies with load. It is never imputed.
"""
from __future__ import annotations

import gzip
import gzip
import json
import os
import random
import time
import urllib.request

import pandas as pd

DATASET = "e8_btc_mempool_lifecycle"
DIVERGENCE_DATASET = "e9_btc_mempool_divergence"
HDRS = {"User-Agent": "dataforge/1.0", "Accept-Encoding": "gzip"}

PROVIDERS = {
    "mempool.space": "https://mempool.space/api",
    "blockstream.info": "https://blockstream.info/api",
}

CHAINS = {
    "btc": {"dataset": "e8_btc_mempool_lifecycle",
            "divergence": "e9_btc_mempool_divergence",
            "primary": "https://mempool.space/api",
            "secondary": "https://blockstream.info/api"},
    "ltc": {"dataset": "e11_ltc_mempool_lifecycle",
            "divergence": None,          # no second free Litecoin Esplora found; stated, not faked
            "primary": "https://litecoinspace.org/api",
            "secondary": None},
}

# A public Bitcoin Core RPC. `getrawmempool true` returns the node's WHOLE mempool with an exact
# Behaviour here is deliberate; see the private design notes.
NODE_RPCS = [
    ("tatum", "https://bitcoin-mainnet.gateway.tatum.io"),
    ("publicnode", "https://bitcoin-rpc.publicnode.com"),
    ("drpc", "https://bitcoin.drpc.org"),
]
NODE_RPC = os.environ.get("DF_BTC_NODE_RPC", NODE_RPCS[0][1])

# TIERED, because mempool SIZE turns out to be a proxy for transaction AGE, and the two tiers
# Behaviour here is deliberate; see the private design notes.
LTC_NODES = [("tatum", "https://litecoin-mainnet.gateway.tatum.io")]

NODE_FAST = [("drpc", "https://bitcoin.drpc.org"),
             ("publicnode", "https://bitcoin-rpc.publicnode.com")]
NODE_DEEP = [("tatum", "https://bitcoin-mainnet.gateway.tatum.io"),
             ("publicnode", "https://bitcoin-rpc.publicnode.com")]

BASELINE_POLLS = 3      # union of the first N polls defines "was already here"
# ABSENT_POLLS IS TUNED FROM MEASUREMENT, and it is the lever that matters. At 3 the primary
# Behaviour here is deliberate; see the private design notes.
ABSENT_POLLS = 10
VERIFY_CAP = 1500       # authoritative status checks per run; the rest stay `unresolved`


def _get(url: str, timeout: int = 45):
    """Returns (payload, error). A transport failure must NEVER render as an empty mempool --
    families 70/81/86/93 each had a fully-failed harvest print as a clean zero."""
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers=HDRS), timeout=timeout)
        raw = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
        s = raw.decode().strip()
        try:
            return json.loads(s), None
        except json.JSONDecodeError:
            return s, None            # /block-height returns a bare hash, not JSON
    except Exception as exc:
        return None, f"{type(exc).__name__}: {str(exc)[:100]}"


class BtcMempoolTracker:
    """Tracks first-seen and fate for every transaction observed entering the mempool."""

    def __init__(self, base: str = PROVIDERS["mempool.space"],
                 secondary: str | None = None) -> None:
        self.base = base
        # Explicit rather than inferred: a chain with no second free provider has NO cross-check,
        # and that must be visible in the data instead of silently degrading to a weaker claim.
        self.secondary = secondary
        self.seen: dict[str, float] = {}                 # txid -> first_seen on OUR clock
        self.mined: dict[str, tuple[int, float]] = {}    # txid -> (height, observed_ts)
        self.left: dict[str, float] = {}                 # txid -> when it went absent
        self.absent: dict[str, int] = {}                 # txid -> consecutive absences
        self.pool: set[str] = set()
        # PRE-EXISTING transactions were already in the pool when we started, so their true
        # first_seen is EARLIER than anything we can observe. Stamping our start time on them
        # would fabricate a dwell time, so it stays null -- family 144's left-truncation lesson.
        self.pre_existing: set[str] = set()
        self.tip = 0
        self.n_poll = 0
        self.n_failed = 0
        self.n_verified = 0
        self.n_flicker = 0          # candidates the status check proved were never gone
        self.n_pre_fee_ok = 0       # pre-existing txs whose fee we captured while still pending
        self.n_pre_fee_miss = 0     # ...and those already gone by the time we asked
        self.n_node_snapshots = 0   # full-mempool RPC snapshots taken
        self.n_node_fees = 0        # fees learned from them that we did not already have
        # Per-transaction mempool STRUCTURE, available only from a full node and discarded by
        # every other source we poll. Keyed by txid; see snapshot_node_fees.
        self.node_meta: dict[str, dict] = {}
        # WHICH node answered, counted per source. Coverage depends entirely on this: the
        # Behaviour here is deliberate; see the private design notes.
        self.node_source = None
        self.node_source_counts: dict[str, int] = {}
        self.node_pool_size: list[tuple[float, int, int]] = []   # (ts, node_count, our_count)
        self._pre_worklist: list[str] | None = None
        # FEE RATE PER TRANSACTION. Without it this dataset can describe WHEN things happened
        # but not WHY, and no fee-market question can be asked of it -- which only became
        # obvious when a study needed the column and it was not there. The txid list carries no
        # fee, and a per-transaction lookup across ~85k entries is out of the question, but
        # /mempool/recent returns the 10 newest arrivals with fee and vsize for ~1 KB. Polled on
        # the fast loop it samples a steady fraction of arrivals at negligible cost.
        self.fees: dict[str, tuple[int, int]] = {}      # txid -> (fee_sat, vsize)
        self.n_fee_polls = 0
        # Chain tip when we first saw each transaction. Without it, "confirmed within N blocks"
        # can only be approximated from elapsed time at ten minutes a block, which is wrong by a
        # whole block whenever an interval runs long or short -- and block intervals are the
        # noisiest thing in Bitcoin. With it, blocks_waited is exact arithmetic.
        self.tip_at_seen: dict[str, int] = {}
        # Behaviour here is deliberate; see the private design notes.
        self.other: set[str] = set()
        self.other_ts = 0.0
        self.n_other_saved = 0      # candidates killed by the cross-check, costing no status call

    def poll_pool(self) -> tuple[int, int]:
        """One full mempool read, diffed against the last. Returns (new, newly-absent)."""
        ids, err = _get(f"{self.base}/mempool/txids")
        self.n_poll += 1
        if not isinstance(ids, list):
            self.n_failed += 1
            return 0, 0
        now = time.time()
        cur = set(ids)

        if self.n_poll <= BASELINE_POLLS:
            # BASELINE WINDOW: union across several polls, so provider flicker cannot hand an
            # already-present transaction a fabricated arrival time.
            self.pre_existing |= cur
            self.pool = cur
            return 0, 0

        new = cur - self.pool - self.pre_existing
        for t in new:
            if t not in self.seen:
                self.seen[t] = now
                if self.tip:
                    self.tip_at_seen[t] = self.tip
        # Debounce absences. Presence resets the counter, so a transaction has to be consistently
        # gone rather than merely missing from one load-balanced node's view.
        for t in cur:
            if t in self.absent:
                del self.absent[t]
                self.left.pop(t, None)
                getattr(self, "_verdict", {}).pop(t, None)   # evidence no longer applies
        newly_absent = 0
        for t in (self.pool | self.pre_existing) - cur:
            self.absent[t] = self.absent.get(t, 0) + 1
            if self.absent[t] == ABSENT_POLLS:
                self.left[t] = now
                newly_absent += 1
        self.pool = cur
        return len(new), newly_absent

    def refresh_other(self, base: str | None = None) -> int:
        """One bulk read of a different provider. Cheap per candidate settled."""
        if base is None:
            base = self.secondary
        if base is None:
            return 0                      # no cross-check available on this chain

        ids, err = _get(f"{base}/mempool/txids")
        if not isinstance(ids, list):
            self.n_failed += 1
            return 0
        self.other = set(ids)
        self.other_ts = time.time()
        return len(self.other)

    def poll_recent(self) -> int:
        """Capture fee and vsize for the newest arrivals. Cheap, and the only route to a fee
        rate that does not cost one request per transaction."""
        d, err = _get(f"{self.base}/mempool/recent", timeout=20)
        self.n_fee_polls += 1
        if not isinstance(d, list):
            self.n_failed += 1
            return 0
        got = 0
        for tx in d:
            t = tx.get("txid")
            v = tx.get("vsize")
            f = tx.get("fee")
            if t and isinstance(v, int) and v > 0 and isinstance(f, int) and t not in self.fees:
                self.fees[t] = (f, v)
                got += 1
        return got

    def poll_blocks(self) -> int:
        """Mark txids mined. Bitcoin blocks arrive ~every 10 min, so this is cheap."""
        h, err = _get(f"{self.base}/blocks/tip/height")
        if not isinstance(h, int):
            self.n_failed += 1
            return 0
        if self.tip == 0:
            self.tip = h - 1
        marked = 0
        # Cap the catch-up so a long stall cannot blow the time budget.
        for n in range(self.tip + 1, min(h, self.tip + 6) + 1):
            bh, e1 = _get(f"{self.base}/block-height/{n}")
            if not isinstance(bh, str) or len(bh) != 64:
                self.n_failed += 1
                continue
            txs, e2 = _get(f"{self.base}/block/{bh}/txids")
            if not isinstance(txs, list):
                self.n_failed += 1
                continue
            obs = time.time()
            for t in txs:
                if (t in self.seen or t in self.pre_existing) and t not in self.mined:
                    self.mined[t] = (n, obs)
                    marked += 1
            self.tip = n
        return marked

    def verify_dropped(self, cap: int = VERIFY_CAP) -> dict:
        """Authoritative check on drop candidates. `dropped` is the column this dataset exists
        for, so it is never inferred from a set difference alone.

        INCREMENTAL BY DESIGN, and that is not an optimisation. A 420s test produced 685
        candidates of which the provider confirmed 685 were still pending -- a 100% flicker rate
        -- so a 5.5h run generates thousands. Verifying them all in one burst at the end would
        blow past any sane cap and leave most rows `unresolved`. Instead the loop calls this
        every cycle with a small budget, spending otherwise-idle time, and results accumulate in
        self._verdict so no candidate is ever checked twice."""
        if not hasattr(self, "_verdict"):
            self._verdict = {}
        checked = self._verdict
        cand = [t for t in self.left if t not in self.mined and t not in checked]
        if self.other:
            still_elsewhere = [t for t in cand if t in self.other]
            for t in still_elsewhere:
                checked[t] = "flicker"
                self.n_flicker += 1
                self.n_other_saved += 1
            cand = [t for t in cand if t not in self.other]
        for t in cand[:cap]:
            st, err = _get(f"{self.base}/tx/{t}/status", timeout=20)
            if not isinstance(st, dict):
                self.n_failed += 1
                continue                      # unverifiable -> stays `unresolved`
            self.n_verified += 1
            if st.get("confirmed"):
                h = st.get("block_height")
                if isinstance(h, int) and t not in self.mined:
                    self.mined[t] = (h, time.time())   # we simply missed the block
                checked[t] = "mined"
            elif st.get("confirmed") is False:
                # `confirmed: False` MEANS "NOT ON CHAIN". IT DOES NOT MEAN "STILL PENDING".
                # A replaced or evicted transaction reports exactly the same thing as a waiting
                # one, so treating this field as proof of pendency made `dropped` structurally
                # impossible to record -- 1,031,595 published rows contained ZERO drops, and the
                # "685 of 685 flicker" result was 685 misclassifications. Verified against a
                # transaction known to have been RBF-replaced: /status says confirmed:False while
                # the mempool does not contain it.
                #
                # Pendency is decided by the POOL, which we already hold, so this costs nothing.
                if t in self.pool or t in self.other:
                    checked[t] = "flicker"    # genuinely still there; the absence was provider noise
                    self.n_flicker += 1
                else:
                    checked[t] = "gone"       # not on chain AND not in any pool -> really dropped
            else:
                checked[t] = "gone"           # 404: the provider has no record of it at all
        return checked

    def _rpc(self, url: str, method: str, params: list, timeout: int = 180):
        """One JSON-RPC call, gzipped. Returns (result, error); never raises into the caller."""
        body = json.dumps({"jsonrpc": "1.0", "id": "dataforge",
                           "method": method, "params": params}).encode()
        try:
            req = urllib.request.Request(url, data=body, headers={
                **HDRS, "Content-Type": "application/json", "Accept-Encoding": "gzip"})
            r = urllib.request.urlopen(req, timeout=timeout)
            raw = r.read()
            if r.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
            return json.loads(raw).get("result"), None
        except Exception as exc:
            return None, f"{type(exc).__name__}: {str(exc)[:70]}"

    def snapshot_node_fees(self, nodes: list | None = None) -> int:
        """Learn the fee of EVERY transaction in a full node's mempool, in one request.

        This is the cheapest large gain available to this dataset. The per-transaction routes are
        bounded by call frequency -- the arrivals feed returns ten transactions per call, and the
        pre-existing sampler one -- so both crawl. `getrawmempool true` returns the node's entire
        mempool with an exact fee and vsize for every entry: measured at 30,683 transactions,
        100% of them carrying both, for 4.6 MB gzipped in 2.2 seconds.

        MEASURED OVERLAP with our tracked set: 34%, against ~9% from an entire run of the
        per-transaction sampler. The two compose rather than compete -- the sampler skips
        anything already in `self.fees`, so it automatically spends its budget on the remainder.

        ON THE 66% THIS NODE DOES NOT HOLD. The obvious worry is that a smaller mempool means the
        lowest-fee transactions were screened out, which would be exactly the population that
        gets evicted and becomes our drop rows. Checked against the node's own config rather than
        assumed: maxmempool is 256 MB against 8.6 MB in use, so it is not evicting, and
        mempoolminfee sits at the 0.1 sat/vB floor, so it is not fee-filtering. The gap is
        propagation, not policy.

        Gzip is requested explicitly: it cuts the wire cost 16.8 MB -> 4.6 MB, and this is a free
        public service.
        """
        # Try the largest mempool first and fall back. Coverage is decided by which node
        entries = None
        for name, u in (nodes if nodes is not None else NODE_RPCS):
            got, err = self._rpc(u, "getrawmempool", [True])
            if isinstance(got, dict) and got:
                entries = got
                self.node_source = name
                self.node_source_counts[name] = self.node_source_counts.get(name, 0) + 1
                break
        if not isinstance(entries, dict):
            self.n_failed += 1
            return 0
        self.n_node_snapshots += 1
        got = 0
        for txid, e in entries.items():
            if txid in self.fees:
                continue
            vsize = e.get("vsize")
            base = (e.get("fees") or {}).get("base")
            if not isinstance(vsize, int) or vsize <= 0 or base is None:
                continue
            # `fees.base` is BTC; the dataset stores satoshis, as the arrivals feed does.
            sat = int(round(float(base) * 1e8))
            if sat >= 0:
                self.fees[txid] = (sat, vsize)
                got += 1
            # Structure a full node exposes and the HTTP APIs do not. All of it is mempool
            # state, so all of it is gone the moment the transaction confirms or is evicted.
            if txid not in self.node_meta:
                anc, desc = e.get("ancestorcount"), e.get("descendantcount")
                self.node_meta[txid] = {
                    # When THIS node first saw it -- an independent first-seen clock, and the
                    # only external check available on our own timestamps.
                    "node_first_seen_ts": e.get("time"),
                    # Signals it may be replaced. Live mempool state: once the transaction
                    # confirms or is replaced, nothing serves this flag any more.
                    "rbf_signalled": e.get("bip125-replaceable"),
                    # >1 means the transaction is part of an unconfirmed chain, which is how
                    # CPFP fee-bumping shows up.
                    "ancestor_count": anc if isinstance(anc, int) else None,
                    "descendant_count": desc if isinstance(desc, int) else None,
                    "ancestor_vsize": e.get("ancestorsize"),
                }
        self.n_node_fees += got
        # The node's mempool size against ours, at the same instant. These disagree enormously
        self.node_pool_size.append((time.time(), len(entries),
                                    len(self.pool) or len(self.pre_existing)))
        return got

    def sample_pre_existing_fee(self) -> bool:
        """Capture fee and vsize for ONE pre-existing transaction, while it is still pending.

        WHY THIS EXISTS. Every dropped transaction in the dataset so far is `pre_existing`: a
        transaction watched from arrival is mined or still pending inside a 4.5h run, so only
        ones that predate the run live long enough to be evicted. But fees come from the
        recent-ARRIVALS feed, which by definition never saw them -- leaving drop rows, the most
        valuable in the dataset, at ~1% fee coverage.

        Recovering it AFTER the drop is impossible, and that was measured rather than assumed:
        `/tx/{id}` returns 404 immediately once a transaction leaves the mempool unmined, and 0
        of 147 confirmed drops were retrievable. So the only moment to capture the fee is while
        the transaction is still pending, which is what this does.

        UNIFORM AT RANDOM, deliberately. Eviction favours the lowest fee rates, so sampling by
        any fee-related heuristic would bias exactly the distribution these rows exist to study.
        A uniform sample of the pending set is representative by construction; a targeted one
        would quietly not be. The worklist is shuffled once and drained, so no transaction is
        ever fetched twice and the sample stays uniform across the run.

        `vsize` is null on this endpoint; `weight` is returned instead, and vsize = weight/4 by
        definition, so the resulting fee rate is exact rather than approximated.
        """
        if self._pre_worklist is None:
            wl = [t for t in self.pre_existing if t not in self.fees]
            random.shuffle(wl)
            self._pre_worklist = wl
        while self._pre_worklist:
            t = self._pre_worklist.pop()
            if t in self.fees:
                continue
            d, err = _get(f"{self.base}/tx/{t}", timeout=20)
            if not isinstance(d, dict):
                # Already evicted, or the provider refused. Counted, never silent.
                self.n_pre_fee_miss += 1
                return False
            fee, weight, vsize = d.get("fee"), d.get("weight"), d.get("vsize")
            if not isinstance(vsize, int) or vsize <= 0:
                vsize = (weight // 4) if isinstance(weight, int) and weight >= 4 else None
            if isinstance(fee, int) and isinstance(vsize, int) and vsize > 0:
                self.fees[t] = (fee, vsize)
                self.n_pre_fee_ok += 1
                return True
            self.n_pre_fee_miss += 1
            return False
        return False          # worklist exhausted: every pre-existing tx has been tried

    def frame(self) -> pd.DataFrame:
        """One row per tracked transaction. Fate is only ever claimed with evidence."""
        verdict = getattr(self, "_verdict", {})
        rows = []
        for t in set(self.seen) | self.pre_existing:
            first = self.seen.get(t)                       # None for pre-existing
            height, obs = self.mined.get(t, (None, None))
            leftat = self.left.get(t)
            v = verdict.get(t)
            if height is not None:
                fate = "mined"
            elif v == "flicker" or t in self.pool:
                fate = "still_pending"
            elif v == "gone":
                # Debounced AND confirmed by the provider as neither pending nor on chain.
                fate = "dropped"
            elif leftat is not None:
                # Went absent but we could not verify it. NEVER claim `dropped` without evidence.
                fate = "unresolved"
            else:
                fate = "still_pending"
            fee, vsize = self.fees.get(t, (None, None))
            nm = self.node_meta.get(t) or {}
            rows.append({
                "txid": t,
                "first_seen_ts": first,
                "fee_sat": fee,
                "vsize": vsize,
                # Null unless we sampled this transaction on the recent feed. Never imputed:
                # a guessed fee rate would corrupt exactly the analyses the column exists for.
                "fee_rate_sat_vb": (round(fee / vsize, 4)
                                    if (fee is not None and vsize) else None),
                "tip_at_first_seen": self.tip_at_seen.get(t),
                "blocks_waited": ((height - self.tip_at_seen[t])
                                  if (height is not None and t in self.tip_at_seen) else None),
                "pre_existing": t in self.pre_existing,
                # From a full node's mempool; null for transactions that node never held.
                "node_first_seen_ts": nm.get("node_first_seen_ts"),
                "rbf_signalled": nm.get("rbf_signalled"),
                "ancestor_count": nm.get("ancestor_count"),
                "descendant_count": nm.get("descendant_count"),
                "mined_height": height,
                "block_observed_ts": obs,
                "left_pool_ts": leftat,
                "drop_verified": v is not None,
                # Both ends on OUR clock, and null wherever first_seen is unobservable.
                "dwell_seconds_local": (max(obs - first, 0.0)
                                        if (first is not None and obs is not None) else None),
                "fate": fate,
            })
        return pd.DataFrame(rows)


def divergence() -> pd.DataFrame:
    """Same instant, every provider. Their mempools differ by node policy and peering, and
    nobody records the difference -- measured 82,949 vs 77,006 on the first probe."""
    ts = time.time()
    views: dict[str, set[str]] = {}
    rows = []

    for nname, nurl in NODE_RPCS:
        t0 = time.time()
        ids, nerr = None, None
        try:
            body = json.dumps({"jsonrpc": "1.0", "id": "dataforge",
                               "method": "getrawmempool", "params": [False]}).encode()
            req = urllib.request.Request(nurl, data=body, headers={
                **HDRS, "Content-Type": "application/json", "Accept-Encoding": "gzip"})
            r = urllib.request.urlopen(req, timeout=90)
            raw = r.read()
            if r.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
            ids = json.loads(raw).get("result")
        except Exception as exc:
            nerr = f"{type(exc).__name__}: {str(exc)[:70]}"
        ok = isinstance(ids, list)
        key = f"node:{nname}"
        views[key] = set(ids) if ok else set()
        rows.append({"sampled_ts": ts, "provider": key,
                     "n_pending": len(views[key]),
                     "latency_s": round(time.time() - t0, 3),
                     "error": None if ok else (nerr or "bad payload")})

    for name, base in PROVIDERS.items():
        t0 = time.time()
        ids, err = _get(f"{base}/mempool/txids")
        ok = isinstance(ids, list)
        views[name] = set(ids) if ok else set()
        rows.append({"sampled_ts": ts, "provider": name,
                     "n_pending": len(views[name]),
                     "latency_s": round(time.time() - t0, 3),
                     "error": None if ok else (err or "bad payload")})
    # A failed provider contributing an empty set would drive the intersection to zero and look
    # like total divergence, so only providers that ANSWERED are counted.
    answered = [v for v in views.values() if v]
    union = set().union(*answered) if answered else set()
    inter = set.intersection(*answered) if answered else set()
    for r in rows:
        own = views[r["provider"]]
        others = set().union(*[v for k, v in views.items() if k != r["provider"]]) or set()
        r["n_union"] = len(union)
        r["n_intersection"] = len(inter)
        r["n_unique_to_this_provider"] = len(own - others)
        r["share_of_union"] = (len(own) / len(union)) if union else None
    return pd.DataFrame(rows)
