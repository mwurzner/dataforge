"""E20 -- Bitcoin mining-pool stratum jobs, collected DIRECTLY from the pools.

WHY THIS EXISTS ALONGSIDE E19, WHICH LOOKS THE SAME. The difference is provenance, and it is the
whole point. E19 reads stratum.work's public SSE feed: excellent data, but it is THEIR compiled
database, so redistributing or selling a systematic extraction engages the EU sui generis database
right and their repo carries no licence. E20 connects to the pools ourselves, which makes US the
maker of the database. Same measurement, different rights. E19 stays archive-only; E20 is ours.

WHAT IS COLLECTED. Pools push `mining.notify` to every connected miner: the previous block hash
they are building on, the coinbase they will claim, the merkle branches, nTime, and a clean-jobs
flag meaning "abandon previous work, there is a new block". Jobs are replaced every few seconds
and no chain records them. Once a block is found, every losing pool's template is gone.

The signal, visible in the first minute of testing: pools on the SAME previous block with nTime
spread across tens of seconds, and different merkle-branch counts -- i.e. who saw the new block
first, and how differently they each built on it.

HOW WE CONNECT, and why this shape was chosen deliberately:
  * NO BITCOIN ADDRESS IS EVER SUPPLIED. Two pools send jobs on `mining.subscribe` alone; four
    more need a `mining.authorize` first, and accept a plain worker name. `dataforge.observer`
    is honest, identifiable, and cannot receive funds. Two pools (ckpool solo, public-pool)
    require a VALID BTC ADDRESS and are therefore EXCLUDED rather than handed a fabricated one --
    an invented address that happened to be real would route any reward to a stranger, which is
    exactly the mistake an early probe of this idea made.

    A MEASUREMENT ERROR WORTH RECORDING, because it nearly set the pool list wrong. The first
    probe reported "8 of 10 pools send jobs on subscribe alone". It was false: it searched for
    the SUBSTRING "mining.notify", which appears in every pool's subscribe RESPONSE as the name
    of a subscription, not as an actual job. Six of those eight were sending a 116-byte
    acknowledgement and nothing else. The tell was the byte count -- a real job with merkle
    branches cannot fit in 116 bytes. Detection now matches on the JSON method field.
  * An honest identifier in the subscribe params, not a spoofed miner string. If an operator
    looks at who is connected, they should be able to tell what we are.
  * ONE connection per pool, held open. A miner would hold one too; this is the smallest possible
    footprint for the data.
  * We never submit shares, so we consume no share-validation work.

STATED PLAINLY: pool terms are SILENT on read-only connections rather than permissive, and this
is collected on the operator's explicit decision to proceed. Nothing here circumvents access
control -- the endpoints are public and unauthenticated, the protocol is used as designed, and
the jobs are broadcast simultaneously to thousands of miners. But "not prohibited" is not the
same as "invited", and that distinction is recorded rather than glossed.

HONEST LIMITS:
  * `pool_name` is the HOST WE CHOSE to connect to, not a claim decoded from the data. That is
    better provenance than inferring identity from a coinbase, but it means a pool operating
    several stratum hostnames appears once per hostname. slushpool.com and braiins.com are the
    same operator and are both listed; rows are tagged so they can be collapsed.
  * A dropped connection loses jobs silently, so reconnects are counted per pool and carried.
  * We see what a miner on that endpoint sees. Pools running regional endpoints may serve
    different templates elsewhere.
"""
from __future__ import annotations

import json
import select
import socket
import time

import pandas as pd

DATASET = "e20_stratum_jobs_direct"

NEWLINE = chr(10).encode()

# Probed 2026-08-28: each of these delivered mining.notify on subscribe alone, no authorize.
# poolin resolves only on the btc.ss host; bs.poolin.com does not resolve and binance timed out.
# `operator` collapses hostnames run by the same pool (slushpool is Braiins' legacy endpoint).
# (name, host, port, operator, needs_authorize). Measured 2026-08-28 with method-based
# detection. EXCLUDED: solo.ckpool.org and public-pool.io both require a valid BTC address
# ("Authorization validation error"), and we will not invent one. bs.poolin.com does not
# resolve; binance timed out.
POOLS = [
    ("braiins",   "stratum.braiins.com",   3333, "braiins", False),
    ("slushpool", "stratum.slushpool.com", 3333, "braiins", False),
    ("viabtc",    "btc.viabtc.io",         3333, "viabtc",  True),
    ("antpool",   "stratum.antpool.com",   3333, "antpool", True),
    ("f2pool",    "btc.f2pool.com",        3333, "f2pool",  True),
    ("poolin",    "btc.ss.poolin.com",      443, "poolin",  True),
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
