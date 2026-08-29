"""Mining pool stratum jobs, via a third-party public stream.

One row per job observed: pool, block being built on, coinbase, merkle branch count and timing.

ARCHIVE ONLY. This is a systematic extraction of a third party's compiled stream. Collecting it
for internal use is fine; redistributing or selling it is not, absent written permission from
the source. It is excluded from every published product in hf_push and must stay that way.
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
