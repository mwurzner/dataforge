"""E14 -- L2 sequencer preconfirmation reliability. Do "instant" L2 blocks ever get replaced?

WHY THIS PASSES THE CRITERION, verified 2026-08-26 before building. An OP-Stack chain shows two
heads: the UNSAFE head (what the sequencer just promised) and the SAFE head (what has been
derived from batches posted to L1). If the batch data conflicts with what the sequencer
gossiped, every node reorgs to the L1-derived chain and the promised unsafe blocks are
DISCARDED -- no archive serves a replaced unsafe block, so the violation is only observable by
someone who recorded the promise before it broke. No monitoring dataset of this exists that we
could find; the OP Stack specs describe the mechanism and nobody measures it.

CORRECTION, 2026-08-26, after this module was written: THE HEARTBEAT LAG IS LARGELY DERIVABLE
AFTER THE FACT and should not be sold as scarce. Every L2 block header carries its production
timestamp permanently (verified: blocks 100k, 1M and 10M back all still serve theirs), and the
batches that make a block safe are L1 transactions, equally permanent. So both ends of the lag
are archived, and a determined analyst can rebuild the series without having watched. Only the
VIOLATION rows survive the criterion: a promised hash that the canonical chain replaced is served
by no archive.

That leaves E14 as a cheap watcher for a rare event, not a continuous product. It stays because
it costs ~2 requests a minute inside a loop that already runs, and because if a violation ever
happens nobody else will have recorded it. The heartbeat is kept as ATTESTED COVERAGE for the
zeros, not as a saleable series.

WHO CARES: anyone crediting funds on an L2 preconfirmation -- exchanges, payment apps, bridges --
and researchers scoring sequencer trust. The absence of violations is itself the product, but an
absence is only worth anything with attested coverage, so the dataset has two row types:

    heartbeat   every ~2 min per chain: unsafe height, safe height, and the LAG between them.
                The lag series is continuously informative on its own -- it is the real,
                measured finality gap (batch-posting cadence) that documentation states as a
                design target rather than an observation.
    violation   a sampled unsafe block whose hash no longer matches the canonical chain at that
                height once the safe head passed it. Expected RARE; that is the point.

HONEST LIMIT: we sample the unsafe head every few seconds, so with 2s blocks we see roughly
every other block. A violation confined entirely to unsampled blocks is invisible to us. The
heartbeat records our sampling density so coverage is computable, and `checked` counts on each
heartbeat say how many sampled promises were actually verified against the canonical chain.
"""
from __future__ import annotations

import json
import time
import urllib.request

import pandas as pd

DATASET = "e14_l2_preconf"
HDRS = {"Content-Type": "application/json", "User-Agent": "dataforge/1.0"}

CHAINS = {
    "base": "https://mainnet.base.org",
    "optimism": "https://mainnet.optimism.io",
}


def _rpc(url: str, method: str, params: list, timeout: int = 20):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    try:
        d = json.load(urllib.request.urlopen(
            urllib.request.Request(url, data=body, headers=HDRS), timeout=timeout))
        if "error" in d:
            return None, str(d["error"])[:80]
        return d.get("result"), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {str(exc)[:60]}"


class PreconfWatcher:
    """Remembers sampled unsafe-head hashes and verifies them once the safe head passes."""

    def __init__(self, chain: str, url: str) -> None:
        self.chain = chain
        self.url = url
        self.promised: dict[int, tuple[str, float]] = {}   # height -> (hash, seen_ts)
        self.safe_height = 0
        self.last_unsafe = 0          # highest unsafe head OBSERVED, independent of the buffer
        self.n_checked = 0
        self.n_failed = 0
        self.rows: list[dict] = []

    def poll_unsafe(self) -> None:
        blk, err = _rpc(self.url, "eth_getBlockByNumber", ["latest", False])
        if not isinstance(blk, dict):
            self.n_failed += 1
            return
        h = int(blk["number"], 16)
        self.last_unsafe = max(self.last_unsafe, h)
        # First write wins: the promise we verify is the FIRST hash we saw at this height.
        self.promised.setdefault(h, (blk["hash"], time.time()))

    def poll_safe_and_verify(self, cap: int = 30, min_age_s: float = 180) -> None:
        """Verify promises by AGE, not by the safe head.

        The original design waited for the safe head to pass a promised height. That does not
        work: Optimism's public endpoint serves safe = 117,387,667 against latest = 156,074,280,
        a 38.7M-block gap that is a stale tag rather than a real lag (Base's is 13 blocks). With
        that tag, no promise on Optimism ever became due and the watcher silently verified
        NOTHING -- a collector that looks alive and checks nothing.

        The violation test never needed the safe tag anyway: it asks whether the canonical chain
        still holds the hash we saw at that height, which only requires enough time to pass. The
        safe head is still recorded on the heartbeat, flagged for plausibility, because the
        endpoint disagreeing with itself is worth knowing.
        """
        blk, err = _rpc(self.url, "eth_getBlockByNumber", ["safe", False])
        if isinstance(blk, dict):
            self.safe_height = int(blk["number"], 16)
        else:
            self.n_failed += 1
        now = time.time()
        due = sorted(h for h, (_hash, seen) in self.promised.items()
                     if now - seen >= min_age_s)[:cap]
        for h in due:
            uhash, seen = self.promised.pop(h)
            canon, err = _rpc(self.url, "eth_getBlockByNumber", [hex(h), False])
            if not isinstance(canon, dict):
                self.n_failed += 1
                continue
            self.n_checked += 1
            if canon["hash"] != uhash:
                # THE EVENT. A sequencer promise that the canonical chain replaced.
                self.rows.append({
                    "ts": time.time(), "chain": self.chain, "row_type": "violation",
                    "height": h, "unsafe_hash": uhash, "unsafe_seen_ts": seen,
                    "canonical_hash": canon["hash"],
                    "unsafe_height": None, "safe_height": self.safe_height,
                    "lag_blocks": None, "safe_tag_plausible": None,
                    "n_checked": None, "n_failed": None,
                })

    def heartbeat(self) -> None:
        # Refresh the unsafe head FIRST so both numbers describe the same moment. Without this
        # the lag compares a stale unsafe reading against a fresh safe one, and the safe head can
        # overtake it: a short test run reported a lag of MINUS 38 blocks on Base. The
        # plausibility flag already caught it, but a flag on a wrong number is worse than a right
        # number, and one extra call every two minutes is nothing.
        self.poll_unsafe()
        # The lag must use the last OBSERVED unsafe head. Falling back to the safe height when
        # the promise buffer is empty (right after verification drains it) printed "lag 0",
        # which is an artifact of our bookkeeping, not a measurement of the chain.
        unsafe = self.last_unsafe or self.safe_height
        lag = (unsafe - self.safe_height) if (self.safe_height and unsafe) else None
        # A lag beyond a day of blocks is a broken tag, not a chain property. Recorded with the
        # raw numbers intact and a flag, so the endpoint's own inconsistency stays visible
        # instead of being published as a measurement of the rollup.
        plausible = (lag is not None and 0 <= lag <= 43200)
        self.rows.append({
            "ts": time.time(), "chain": self.chain, "row_type": "heartbeat",
            "height": None, "unsafe_hash": None, "unsafe_seen_ts": None,
            "canonical_hash": None,
            "unsafe_height": unsafe, "safe_height": self.safe_height,
            "lag_blocks": lag, "safe_tag_plausible": plausible,
            "n_checked": self.n_checked, "n_failed": self.n_failed,
        })
        # Promises far behind an advancing safe head that we never got to verify (cap budget)
        # must not accumulate without bound.
        if len(self.promised) > 4000:
            for h in sorted(self.promised)[:1000]:
                del self.promised[h]

    def frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows)


def make_watchers() -> dict[str, "PreconfWatcher"]:
    return {name: PreconfWatcher(name, url) for name, url in CHAINS.items()}


if __name__ == "__main__":
    ws = make_watchers()
    print("sampling for ~100s...")
    t0 = time.time()
    while time.time() - t0 < 100:
        for w in ws.values():
            w.poll_unsafe()
        if int(time.time() - t0) % 30 < 3:
            for w in ws.values():
                w.poll_safe_and_verify()
        time.sleep(3)
    for w in ws.values():
        w.poll_safe_and_verify()
        w.heartbeat()
        d = w.frame()
        hb = d[d.row_type == "heartbeat"].iloc[-1]
        print(f"  {w.chain:<9} unsafe {hb.unsafe_height}  safe {hb.safe_height}  "
              f"lag {hb.lag_blocks} blocks  checked {w.n_checked}  failed {w.n_failed}  "
              f"violations {len(d[d.row_type == 'violation'])}")
