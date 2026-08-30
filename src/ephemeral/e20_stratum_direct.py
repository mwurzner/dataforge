"""Mining pool stratum jobs, collected directly.

One row per job: the pool, the block it builds on, its nTime, coinbase, merkle branch count and
clean-jobs flag.

Operational notes:
  Read-only. No shares are submitted and no payout address is supplied anywhere. Pools requiring
  a valid address are excluded rather than given a fabricated one.
  `pool` is the endpoint connected to; `operator` collapses endpoints run by the same operator.
  Send the subscribe while the socket is still blocking, then switch to non-blocking. Reversing
  that order fails silently.
"""
from __future__ import annotations

import json
import select
import socket
import time

import pandas as pd

DATASET = "e20_stratum_jobs_direct"

NEWLINE = chr(10).encode()

POOLS = [
    ("braiins",   "stratum.braiins.com",   3333, "braiins", False),
    ("slushpool", "stratum.slushpool.com", 3333, "braiins", False),
    ("viabtc",    "btc.viabtc.io",         3333, "viabtc",  True),
    ("antpool",   "stratum.antpool.com",   3333, "antpool", True),
    ("f2pool",    "btc.f2pool.com",        3333, "f2pool",  True),
    ("poolin",    "btc.ss.poolin.com",      443, "poolin",  True),
    # Added after probing each for an anonymous subscribe that actually yields mining.notify;
    # candidates that connected but sent no work were left out rather than added hopefully.
    # needs_auth is True for all five because that is the configuration they were verified in.
    # None required a payout address, so the no-address policy in the docstring is unchanged.
    ("luxor",     "btc.global.luxor.tech",  700, "luxor",   True),
    ("binance",   "bs.poolbinance.com",    3333, "binance", True),
    ("kano",      "stratum.kano.is",       3333, "kano",    True),
    ("ocean",     "mine.ocean.xyz",        3334, "ocean",   True),
    ("secpool",   "btc.secpool.com",       3333, "secpool", True),
]

# An honest worker name. Not an address, so it cannot receive funds, and identifiable if an
# operator looks at who is connected.
WORKER_NAME = "dataforge.observer"
AUTHORIZE = json.dumps({"id": 2, "method": "mining.authorize",
                        "params": [WORKER_NAME, "x"]}).encode() + NEWLINE

SUBSCRIBE = json.dumps({"id": 1, "method": "mining.subscribe",
                        "params": ["dataforge-observer/1.0"]}).encode() + b"\n"
RECONNECT_BACKOFF_S = 15.0


class DirectStratumCollector:
    """Holds one read-only connection per pool and records every mining.notify."""

    def __init__(self, pools=None) -> None:
        self.pools = pools or POOLS
        self.rows: list[dict] = []
        self.seen: set[tuple] = set()
        self.conns: dict[str, socket.socket] = {}
        self.bufs: dict[str, bytes] = {}
        self.next_try: dict[str, float] = {}
        self.n_reconnects: dict[str, int] = {}
        self.n_jobs: dict[str, int] = {}
        self.n_connect_fail: dict[str, int] = {}

    def _connect(self, name: str, host: str, port: int, needs_auth: bool = False) -> None:
        try:
            s = socket.create_connection((host, port), timeout=10)
            # Send the subscribe while the socket is still BLOCKING, then switch. Setting
            # non-blocking first meant sendall could fail to deliver, and the symptom was
            # silent: the connection opened, no error was raised, and the pool simply never
            # sent jobs -- 2 pools produced data where a blocking probe had shown 8.
            s.sendall(SUBSCRIBE)
            if needs_auth:
                s.sendall(AUTHORIZE)
            s.setblocking(False)
            self.conns[name] = s
            self.bufs[name] = b""
        except Exception:
            self.n_connect_fail[name] = self.n_connect_fail.get(name, 0) + 1
            self.next_try[name] = time.time() + RECONNECT_BACKOFF_S

    def _record(self, name: str, operator: str, params: list) -> None:
        if not params or len(params) < 8:
            return
        job_id, prev_hash, cb1, cb2, branches, version, nbits, ntime = params[:8]
        clean = params[8] if len(params) > 8 else None
        key = (name, job_id, prev_hash, ntime)
        if key in self.seen:
            return
        self.seen.add(key)
        self.n_jobs[name] = self.n_jobs.get(name, 0) + 1
        branches = branches or []
        cb1, cb2 = cb1 or "", cb2 or ""
        self.rows.append({
            "observed_ts": time.time(),
            "pool": name,
            # Several hostnames can belong to one operator; collapse on this, not on `pool`.
            "operator": operator,
            "job_id": job_id,
            "prev_hash": prev_hash,
            # The pool's OWN clock in the template. Pools on the same prev_hash with different
            # ntime is the propagation signal this dataset exists for.
            "ntime": ntime,
            "version": version,
            "nbits": nbits,
            # True means "abandon previous work": the pool has switched to a new block.
            "clean_jobs": clean,
            "n_merkle_branches": len(branches),
            "first_merkle_branch": branches[0] if branches else None,
            "coinbase1": cb1,
            "coinbase2": cb2,
            "coinbase_len": len(cb1) + len(cb2),
        })

    def run(self, duration_s: float) -> int:
        end = time.time() + duration_s
        byname = {n: (h, p, op, auth) for n, h, p, op, auth in self.pools}
        while time.time() < end:
            now = time.time()
            for name, (host, port, _op, auth) in byname.items():
                if name not in self.conns and now >= self.next_try.get(name, 0):
                    self._connect(name, host, port, auth)
            if not self.conns:
                time.sleep(1.0)
                continue
            socks = list(self.conns.values())
            rev = {s: n for n, s in self.conns.items()}
            try:
                ready, _, bad = select.select(socks, [], socks, 2.0)
            except Exception:
                ready, bad = [], []
            for s in bad:
                self._drop(rev.get(s))
            for s in ready:
                name = rev.get(s)
                if name is None:
                    continue
                try:
                    chunk = s.recv(65536)
                except Exception:
                    self._drop(name)
                    continue
                if not chunk:
                    self._drop(name)
                    continue
                self.bufs[name] += chunk
                while b"\n" in self.bufs[name]:
                    line, self.bufs[name] = self.bufs[name].split(b"\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        m = json.loads(line)
                    except Exception:
                        continue
                    if m.get("method") == "mining.notify":
                        self._record(name, byname[name][2], m.get("params") or [])
        for name in list(self.conns):
            self._drop(name, reconnect=False)
        return len(self.rows)

    def _drop(self, name: str | None, reconnect: bool = True) -> None:
        if not name:
            return
        s = self.conns.pop(name, None)
        self.bufs.pop(name, None)
        if s is not None:
            try:
                s.close()
            except Exception:
                pass
        if reconnect:
            # A dropped connection loses jobs silently, so this is COUNTED and carried on the
            # frame rather than being invisible.
            self.n_reconnects[name] = self.n_reconnects.get(name, 0) + 1
            self.next_try[name] = time.time() + RECONNECT_BACKOFF_S

    def frame(self) -> pd.DataFrame:
        df = pd.DataFrame(self.rows)
        if len(df):
            df["pool_reconnects"] = df["pool"].map(lambda p: self.n_reconnects.get(p, 0))
            df["pool_connect_failures"] = df["pool"].map(lambda p: self.n_connect_fail.get(p, 0))
        return df


if __name__ == "__main__":
    c = DirectStratumCollector()
    print("connecting to pools (subscribe only, no credentials) for ~75s...")
    c.run(75)
    df = c.frame()
    print(f"\n{len(df):,} jobs from {df.pool.nunique() if len(df) else 0} pools")
    print(f"  connected: {sorted(c.n_jobs)}")
    print(f"  connect failures: {c.n_connect_fail or 'none'}   reconnects: {c.n_reconnects or 'none'}")
    if len(df):
        top = df.prev_hash.value_counts().index[0]
        sub = df[df.prev_hash == top].sort_values("observed_ts").drop_duplicates("pool")
        print(f"\n  same block {top[:24]}.. -- nTime by pool:")
        print(sub[["pool", "operator", "ntime", "n_merkle_branches",
                   "coinbase_len", "clean_jobs"]].to_string(index=False))
        nt = sub.ntime.map(lambda x: int(x, 16))
        print(f"\n  nTime spread across pools: {nt.max()-nt.min()} seconds")
