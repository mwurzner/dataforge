"""E1 -- mempool transaction lifecycle. The flagship ephemeral dataset.

WHY THIS ONE IS DIFFERENT FROM EVERYTHING IN PHASE 1. Phase 1 collected contract state and the
premise turned out to be false: free endpoints serve historical state to -720d, so all of it was
retroactively obtainable. Pending transactions are not. They are gossiped on the p2p network and
either get mined -- at which point the BODY becomes permanent but the TIMING does not -- or they
vanish leaving no trace anywhere. No archive node holds a mempool.

So the two facts worth capturing are exactly the two the chain will never have:
    1. WHEN a transaction was first seen, and how long it sat pending
    2. WHICH transactions were never mined at all

MEASURED DESIGN, not assumed (probed 2026-08-25):
    txpool_content            86.0 MB, 7.2 s, 93,654 txs   -> far too heavy to poll
    eth_getFilterChanges      ~10 KB, 0.06 s, ~140 new hashes per 10 s
So the loop is INCREMENTAL: a pending-transaction filter yields only new hashes, and blocks are
polled separately to mark what got mined. One single txpool_content at the END reconciles the
remainder into genuinely-dropped versus still-pending. That is ~1 MB/hour instead of ~500.

BASE IS DELIBERATELY EXCLUDED. Probed: Base's txpool_content returns ZERO transactions, because it
runs a centralised sequencer -- transactions go straight to it and there is no public mempool to
observe. This dataset is Ethereum-only, and that is a property of the chain, not a gap in coverage.
"""
from __future__ import annotations

import json
import sys
import time
import urllib.request
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

DATASET = "e1_mempool_lifecycle"
HDRS = {"Content-Type": "application/json", "User-Agent": "dataforge/1.0"}

# Endpoints that answered txpool/filter methods in the probe. publicnode is the only one that
# served BOTH txpool_status and the pending filter, so it leads; the others are fallbacks for the
# filter only.
ENDPOINTS = [
    "https://ethereum-rpc.publicnode.com",
    "https://eth.merkle.io",
    "https://rpc.flashbots.net",
]


def _rpc(url: str, method: str, params: list, timeout: int = 60):
    """Returns (result, error_string). NEVER conflates a transport failure with an empty result --
    families 70/81/86/93 each had a fully-failed harvest render as a clean zero."""
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    try:
        d = json.load(urllib.request.urlopen(
            urllib.request.Request(url, data=body, headers=HDRS), timeout=timeout))
        if "error" in d:
            return None, str(d["error"])[:120]
        return d.get("result"), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {str(exc)[:100]}"


class MempoolTracker:
    """Tracks first-seen and fate for every pending transaction observed during a run."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.filter_id: str | None = None
        self.seen: dict[str, float] = {}          # hash -> first_seen unix
        self.mined: dict[str, tuple[int, int]] = {}   # hash -> (block, block_ts)
        self.last_block = 0
        self.n_poll = 0
        self.n_failed = 0
        self.n_filter_resets = 0

    # ------------------------------------------------------------------ filter
    def ensure_filter(self) -> bool:
        if self.filter_id:
            return True
        fid, err = _rpc(self.url, "eth_newPendingTransactionFilter", [])
        if fid:
            self.filter_id = fid
            return True
        self.n_failed += 1
        return False

    def poll_pending(self) -> int:
        """One incremental read of new pending hashes."""
        if not self.ensure_filter():
            return 0
        res, err = _rpc(self.url, "eth_getFilterChanges", [self.filter_id])
        self.n_poll += 1
        if res is None:
            # Filters expire server-side and are silently dropped by some providers. Recreating is
            # correct, but it must be COUNTED: a reset loses the hashes seen since the last poll,
            # which is a small measurable gap rather than an invisible one.
            self.filter_id = None
            self.n_filter_resets += 1
            self.n_failed += 1
            return 0
        now = time.time()
        new = 0
        for h in res:
            if h not in self.seen:
                self.seen[h] = now
                new += 1
        return new

    # ------------------------------------------------------------------ blocks
    def poll_blocks(self) -> int:
        """Mark hashes mined. Fetches hash-only blocks, which are small."""
        head, err = _rpc(self.url, "eth_blockNumber", [])
        if not head:
            self.n_failed += 1
            return 0
        head = int(head, 16)
        if self.last_block == 0:
            self.last_block = head - 1
        marked = 0
        # Cap the catch-up so a long stall cannot blow the time budget.
        for n in range(self.last_block + 1, min(head, self.last_block + 12) + 1):
            blk, err = _rpc(self.url, "eth_getBlockByNumber", [hex(n), False])
            if not blk:
                self.n_failed += 1
                continue
            ts = int(blk["timestamp"], 16)
            for h in blk.get("transactions", []):
                if h in self.seen and h not in self.mined:
                    self.mined[h] = (n, ts)
                    marked += 1
            self.last_block = n
        return marked

    # ------------------------------------------------------------------ finish
    def reconcile(self) -> set[str]:
        """One full txpool read at the end: what is STILL pending. Everything we saw that is
        neither mined nor still pending was genuinely DROPPED -- and those rows exist nowhere
        else in the world, which is the point of the whole dataset."""
        res, err = _rpc(self.url, "txpool_content", [], timeout=180)
        if not isinstance(res, dict):
            return set()
        still = set()
        for bucket in ("pending", "queued"):
            for _sender, txs in (res.get(bucket) or {}).items():
                for _nonce, tx in txs.items():
                    h = tx.get("hash")
                    if h:
                        still.add(h)
        return still

    def frame(self, still_pending: set[str]) -> pd.DataFrame:
        rows = []
        for h, first in self.seen.items():
            mb, mts = self.mined.get(h, (None, None))
            if mb is not None:
                fate = "mined"
                dwell = max(mts - first, 0.0) if mts else None
            elif h in still_pending:
                fate = "still_pending"
                dwell = None
            elif still_pending:
                fate = "dropped"          # confirmed absent from the pool AND never mined
                dwell = None
            else:
                fate = "unresolved"       # reconciliation itself failed; do not claim "dropped"
                dwell = None
            rows.append({"tx_hash": h, "first_seen_ts": first,
                         "mined_block": mb, "mined_ts": mts,
                         "dwell_seconds": dwell, "fate": fate})
        return pd.DataFrame(rows)


def run(duration_s: int, poll_s: float, runlog=None) -> pd.DataFrame:
    """Poll for `duration_s`, then reconcile. Returns the lifecycle frame."""
    t = MempoolTracker(ENDPOINTS[0])
    t0 = time.time()
    last_block_poll = 0.0
    print(f"E1 mempool: polling {ENDPOINTS[0]} for {duration_s/3600:.2f}h "
          f"every {poll_s}s", flush=True)
    while time.time() - t0 < duration_s:
        t.poll_pending()
        # Blocks arrive every ~12s; polling them faster only wastes calls.
        if time.time() - last_block_poll >= 12:
            t.poll_blocks()
            last_block_poll = time.time()
        el = time.time() - t0
        if t.n_poll % 120 == 0 and t.n_poll:
            print(f"    {el/60:5.1f} min  seen {len(t.seen):>7,}  mined {len(t.mined):>7,}  "
                  f"failed {t.n_failed:>3}  resets {t.n_filter_resets}", flush=True)
        time.sleep(poll_s)
    t.poll_blocks()
    still = t.reconcile()
    df = t.frame(still)
    print(f"  E1 done: {len(df):,} txs  mined {(df['fate']=='mined').sum():,}  "
          f"dropped {(df['fate']=='dropped').sum():,}  "
          f"still_pending {(df['fate']=='still_pending').sum():,}  "
          f"polls {t.n_poll:,}  failed {t.n_failed}  filter_resets {t.n_filter_resets}", flush=True)
    return df
