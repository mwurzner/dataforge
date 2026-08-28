"""E19 -- Bitcoin mining-pool stratum jobs: what each pool is working on, right now.

WHAT THIS IS. Pools push `mining.notify` jobs to their miners every few seconds: the previous
block hash they are building on, the coinbase they will claim, the merkle branches, and a
clean-jobs flag telling miners to abandon previous work. Jobs are replaced constantly and no
chain records them. Once a block is found, the templates every pool was working on -- including
every pool that lost -- are gone.

WHY IT PASSES THE CRITERION, verified 2026-08-28 before building. stratum.work streams these
messages live and stores nothing: no archive, no export, no API for history, no retention policy.
No historical stratum dataset was found anywhere. The measurement is visible in the first sample:
two pools on the SAME previous block with `ntime` 25 seconds apart, and coinbase structures of
114 against 218 characters.

WHY WE READ A THIRD-PARTY STREAM RATHER THAN THE POOLS DIRECTLY. Connecting to pools ourselves
works -- it was tested -- but it means holding persistent connections to commercial mining pools,
consuming their infrastructure, contributing no hashrate, and selling the result. Braiins and
ViaBTC terms are SILENT on that, and silence is not permission. stratum.work already does the
collecting as a public transparency tool and documents `GET /api/stream` "allowing for custom
integrations", so consuming it is the intended use and puts no load on any pool.

    CREDIT, which is not optional. The collection is stratum.work's work (bboerst,
    github.com/bboerst/stratum-work), and the idea is 0xB10C's. Any published form of this
    dataset must say so prominently.

    !! PUBLICATION IS GATED. Collecting for our own use raises nothing. REDISTRIBUTING or SELLING
    a systematic extraction of a third party's compiled stream engages the EU sui generis
    database right, which protects the compiler's investment independently of copyright -- and
    the repo carries NO LICENCE granting redistribution. So this dataset is collected now
    (history cannot be recovered later) and must NOT be added to any published product until
    the maintainer has agreed in writing. It is deliberately absent from hf_push's PRODUCTS.

WHAT IS STORED. One row per `mining.notify` observed: the pool, the job, what it builds on, and
the timing. Merkle branches are kept as a COUNT plus their first entry rather than the full list
-- the list is large, and the branch count plus coinbase is what identifies a distinct template.
The full coinbase is kept because pool identity and fee claim are read from it.

HONEST LIMITS:
  * This is stratum.work's view of the pools, not ours. Their collector's coverage, latency and
    pool selection are inherited wholesale, including any gaps.
  * `lat_ms` is THEIR measured latency from their collector to the pool, not ours, and not the
    pool's own send time. It is a property of their vantage point.
  * A dropped SSE connection loses jobs silently, so reconnects and gaps are counted and carried.
"""
from __future__ import annotations

import json
import time
import urllib.request

import pandas as pd

DATASET = "e19_stratum_jobs"
STREAM = "https://stratum.work/api/stream"
HDRS = {"User-Agent": "dataforge/1.0 (research; contact via github.com/mwurzner)",
        "Accept": "text/event-stream"}


class StratumJobCollector:
    """Reads the public SSE stream and keeps one row per distinct job observation."""

    def __init__(self) -> None:
        self.rows: list[dict] = []
        self.seen: set[tuple] = set()
        self.n_events = 0
        self.n_reconnects = 0
        self.n_parse_fail = 0
        self.pools: set[str] = set()

    def _row(self, d: dict) -> None:
        pool = d.get("pool_name")
        key = (pool, d.get("job_id"), d.get("prev_hash"), d.get("ntime"))
        if key in self.seen:
            return
        self.seen.add(key)
        if pool:
            self.pools.add(pool)
        br = d.get("merkle_branches") or []
        cb1 = d.get("coinbase1") or ""
        cb2 = d.get("coinbase2") or ""
        self.rows.append({
            "observed_ts": time.time(),
            "pool_name": pool,
            "height": d.get("height"),
            "job_id": d.get("job_id"),
            "prev_hash": d.get("prev_hash"),
            # nTime is the pool's own clock in the template. Pools on the SAME prev_hash with
            # different nTime is the propagation/timing signal this dataset exists for.
            "ntime": d.get("ntime"),
            "version": d.get("version"),
            "nbits": d.get("nbits"),
            # True means "abandon previous work" -- i.e. the pool has switched to a new block.
            "clean_jobs": d.get("clean_jobs"),
            "n_merkle_branches": len(br),
            "first_merkle_branch": br[0] if br else None,
            "coinbase1": cb1,
            "coinbase2": cb2,
            "coinbase_len": len(cb1) + len(cb2),
            "extranonce1": d.get("extranonce1"),
            "extranonce2_length": d.get("extranonce2_length"),
            # stratum.work's OWN collector-to-pool latency, not ours. Their vantage point.
            "source_lat_ms": d.get("lat_ms"),
            "source_timestamp": d.get("timestamp"),
        })

    def run(self, duration_s: float, chunk_timeout: int = 45) -> int:
        """Consume the stream for duration_s, reconnecting on drop."""
        end = time.time() + duration_s
        while time.time() < end:
            try:
                req = urllib.request.Request(STREAM, headers=HDRS)
                with urllib.request.urlopen(req, timeout=chunk_timeout) as r:
                    buf = b""
                    while time.time() < end:
                        chunk = r.read(4096)
                        if not chunk:
                            break
                        buf += chunk
                        while b"\n" in buf:
                            line, buf = buf.split(b"\n", 1)
                            line = line.strip()
                            if not line.startswith(b"data:"):
                                continue
                            self.n_events += 1
                            try:
                                d = json.loads(line[5:].strip())
                            except Exception:
                                self.n_parse_fail += 1
                                continue
                            if isinstance(d, dict) and d.get("pool_name"):
                                self._row(d)
            except Exception:
                # A dropped stream loses jobs silently, so reconnects are COUNTED and carried
                # on the frame rather than hidden.
                self.n_reconnects += 1
                time.sleep(2.0)
        return len(self.rows)

    def frame(self) -> pd.DataFrame:
        df = pd.DataFrame(self.rows)
        if len(df):
            df["source_reconnects"] = self.n_reconnects
            df["source_parse_failures"] = self.n_parse_fail
        return df


if __name__ == "__main__":
    c = StratumJobCollector()
    print("reading stratum.work SSE for ~60s...")
    c.run(60)
    df = c.frame()
    print(f"\n{len(df):,} distinct jobs from {len(c.pools)} pools "
          f"({c.n_events:,} events, {c.n_reconnects} reconnects, {c.n_parse_fail} parse fails)")
    if len(df):
        print(f"  pools: {sorted(c.pools)[:12]}")
        h = df.groupby("prev_hash").agg(pools=("pool_name", "nunique"),
                                        jobs=("job_id", "size")).sort_values("jobs", ascending=False)
        print(f"\n  distinct prev_hash values seen: {len(h)}")
        print(h.head(3).to_string())
        top = df.prev_hash.value_counts().index[0]
        sub = df[df.prev_hash == top].drop_duplicates("pool_name")
        print(f"\n  same block, ntime by pool (the timing signal):")
        print(sub[["pool_name", "ntime", "n_merkle_branches", "coinbase_len",
                   "clean_jobs"]].head(8).to_string(index=False))
