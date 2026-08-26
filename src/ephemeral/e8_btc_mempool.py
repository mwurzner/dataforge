"""E8 -- Bitcoin mempool lifecycle. The second chain, and it PASSES the same test as E1.

MEASURED 2026-08-25, not assumed. The decisive probe was not "can we see the mempool" (of course
we can) but "can someone retrieve a first-seen time for a transaction mined weeks ago":

    live (unconfirmed)   -> [1778435949]   AVAILABLE
    ~1 day old           -> [0]            NOT AVAILABLE
    ~30 days old         -> [0]            NOT AVAILABLE
    ~365 days old        -> [0]            NOT AVAILABLE

Confirmation ERASES the timing. So Bitcoin mempool lifecycle is un-backfillable exactly as
Ethereum's is, and it is free and keyless from two independent providers.

WE DERIVE first_seen FROM OUR OWN POLLING and never read the provider's own first-seen field.
Legally that makes the timestamps OUR measurements of a public broadcast network rather than a
redistribution of somebody else's dataset. Methodologically it makes E8 the same instrument as E1.

THE ARTIFACT THIS MODULE EXISTS TO AVOID, found by testing rather than reasoning. A naive
poll-to-poll diff reported 642 transactions "dropped" in 200 seconds. Sampling 12 of them against
the provider's own status endpoint: ALL TWELVE WERE STILL PENDING. The txid endpoint is served by
load-balanced nodes whose views differ slightly, so set-differencing consecutive polls
manufactures disappearances that never happened -- and `dropped` is the single most valuable
column here, so a fabricated one would poison exactly what the dataset is for.

Two defences, both measured rather than assumed sufficient:
  1. DEBOUNCE. A transaction must be absent for ABSENT_POLLS consecutive polls before it is even a
     candidate, which removes single-poll flicker.
  2. AUTHORITATIVE VERIFICATION. Every surviving candidate is checked against /tx/{id}/status at
     the end of the run. Only a transaction the provider itself reports as neither confirmed nor
     present is called `dropped`. Anything we could not check is `unresolved` -- never `dropped`.
The same flicker corrupts first-seen, so the BASELINE is the UNION of the first BASELINE_POLLS
polls rather than a single one; a transaction that flickers out of the first poll would otherwise
be handed a fabricated arrival time.

WHY BITCOIN IS A DIFFERENT DATASET, not a copy of E1. Measured on a random 40 of the live
mempool: median age ~107 DAYS, against Ethereum dwell times of seconds to minutes. Bitcoin runs a
large standing backlog of fee-starved transactions that Ethereum has no equivalent of.

CAVEAT THAT MUST TRAVEL WITH THE DATA: a mempool is NODE-LOCAL and retention is a node policy.
Bitcoin Core evicts after 336h by default, yet mempool.space served 115-day-old entries. Polling
two providers measures that difference instead of hiding it -- they disagreed by ~5,900
transactions at the same instant.

MEASURED COST: 3.1 MB gzipped per full poll, ~327 new transactions/min, ~471k rows/day.
"""
from __future__ import annotations

import gzip
import json
import time
import urllib.request

import pandas as pd

DATASET = "e8_btc_mempool_lifecycle"
DIVERGENCE_DATASET = "e9_btc_mempool_divergence"
HDRS = {"User-Agent": "dataforge/1.0", "Accept-Encoding": "gzip"}

# Both free, keyless, independent. mempool.space leads on latency (0.55s vs 3.79s measured);
# blockstream.info is the cross-check and the divergence counterpart.
PROVIDERS = {
    "mempool.space": "https://mempool.space/api",
    "blockstream.info": "https://blockstream.info/api",
}

# OTHER UTXO CHAINS RUNNING THE SAME ESPLORA API. Probed 2026-08-25: litecoinspace.org answers
# /mempool/txids identically (159 txids, 0.15s), so the collector generalises with a URL swap and
# no code change. Blockchair covers BCH/Doge/Zcash but caps a mempool listing at 10 rows, so those
# are NOT collectable free and are excluded rather than half-collected. Liquid answered but its
# mempool was empty, which is a property of the chain, not a failure.
#
# HONEST ON VALUE: Litecoin costs almost nothing to add (10 KB a poll against Bitcoin's 5.8 MB)
# and is just as un-backfillable, but its audience is far thinner. The reason to collect it is
# that a CROSS-CHAIN UTXO fee-market panel during a congestion event is something nobody has,
# and not collecting is the only irreversible choice available.
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

BASELINE_POLLS = 3      # union of the first N polls defines "was already here"
# ABSENT_POLLS IS TUNED FROM MEASUREMENT, and it is the lever that matters. At 3 the primary
# provider generated ~600 candidates per 10 minutes of which the status endpoint confirmed ~100%
# were still pending -- roughly 60 phantoms a minute, which no per-transaction verification budget
# can absorb over a 5.5h run. Flicker returns within a poll or two; a genuine drop (RBF
# replacement, or eviction after Bitcoin Core's 336h) never comes back. So requiring TEN
# consecutive absences -- ten minutes at the production cadence -- separates them almost for free,
# where verification was paying per candidate to learn the same thing.
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
        # A SECOND PROVIDER'S POOL, refreshed in bulk. Measured flicker on the primary was 685 of
        # 685 -- roughly 98 spurious candidates a minute -- while per-transaction status checks
        # clear only ~20/min, so verification could never keep up and most rows would end
        # `unresolved`. One 3 MB bulk read of an INDEPENDENT provider settles thousands at once:
        # a transaction still sitting in another node's pool is definitively not dropped.
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
        # SECOND-PROVIDER GATE. Kept, but MEASURED AS WEAK and worth saying so: it cleared only 6
        # of 600 candidates in a 10-minute test, because our own e9 data shows mempool.space is
        # very nearly a superset of blockstream.info (5,150 unique versus 205 at the same instant).
        # A transaction absent from the larger pool is almost always absent from the smaller one,
        # so this settles the rare case where the primary is the one at fault, and no more.
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
