"""Batched, BLOCK-PINNED JSON-RPC client for Base.

The single most dangerous bug in historical replay is reading CURRENT chain state while scoring
a PAST block: it fabricates profit out of nothing and every downstream number becomes fiction.
(The equities project lost real time to exactly this class of silent bug -- CIK padding, state
persistence -- so here it is prevented by the type system rather than by discipline.)

Enforcement: every state-reading method REQUIRES an integer block number. Reading the chain tip
is possible only through `latest_block()`, which is separately named and never implicit. There
is no code path that silently falls back to "latest".

Free public endpoint (verified 2026-07-14): archive state available to at least -40,000,000
blocks (~926 days), and batch JSON-RPC is supported.
"""
from __future__ import annotations

import itertools
import os
import random
import time
from dataclasses import dataclass
from pathlib import Path

import requests


def _load_env() -> None:
    """Load ChainArb/.env so BASE_RPC_URL is picked up without any shell setup."""
    env = Path(__file__).resolve().parents[2] / ".env"
    if not env.exists():
        return
    # utf-8-sig strips a BOM if present. Windows editors and PowerShell's `Set-Content -Encoding
    # utf8` write one, which silently turns the first key into "﻿BASE_RPC_URL" -- the
    # variable then never matches and the code quietly falls back to the slow public endpoints.
    try:
        for line in env.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip().lstrip("﻿")
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                v = v.strip().strip('"').strip("'")
                if v:
                    os.environ[k.strip()] = v
    except Exception:
        pass


_load_env()

# Free Base endpoints WITH ARCHIVE state (surveyed 2026-07-14). Most public endpoints keep only
# recent state ("missing trie node"/"no-archive") and are useless for historical replay.
# Both survivors rate-limit aggressively, so we rotate across them and pace requests.
# Setting BASE_RPC_URL (e.g. a free-tier Alchemy/dRPC key) overrides and is far faster.
ARCHIVE_ENDPOINTS = [
    "https://mainnet.base.org",
    "https://base.drpc.org",
]
DEFAULT_URL = os.environ.get("BASE_RPC_URL", "").strip()

# Well-known function selectors (first 4 bytes of keccak(signature)); hardcoded to avoid a
# keccak dependency for the read-only path.
SEL = {
    "getReserves()": "0x0902f1ac",
    "slot0()": "0x3850c7bd",
    "liquidity()": "0x1a686502",
    "token0()": "0x0dfe1681",
    "token1()": "0xd21220a7",
    "fee()": "0xddca3f43",
    "decimals()": "0x313ce567",
    "totalSupply()": "0x18160ddd",
    "balanceOf(address)": "0x70a08231",
}


class RPCError(RuntimeError):
    pass


@dataclass
class Call:
    """One eth_call: `to` contract, hex `data` payload."""
    to: str
    data: str


def _hexblock(block: int) -> str:
    if not isinstance(block, int) or isinstance(block, bool):
        raise TypeError(
            f"block must be an int (got {block!r}). Historical reads must be PINNED; "
            "use latest_block() explicitly if you truly want the chain tip.")
    return hex(block)


def _is_batch_refusal(err: dict) -> bool:
    """dRPC's free plan refuses batches of >3 ('code 31'). Other providers phrase it differently.
    Detecting this lets us fall back to CONCURRENT single calls, which are not restricted."""
    msg = str(err.get("message", "")).lower()
    return err.get("code") == 31 or ("batch" in msg and ("not allowed" in msg or "limit" in msg))


def _is_revert(err: dict) -> bool:
    """True for a contract-level revert (a legitimate 'no' from the chain) as opposed to a
    transport/protocol failure (rate limit, batch cap, node error), which must NOT be mistaken
    for data. Conflating the two silently turns infrastructure failure into false findings."""
    msg = str(err.get("message", "")).lower()
    return err.get("code") == 3 or "revert" in msg or "out of gas" in msg


class RPC:
    # The free Base endpoint enforces "maximum 10 calls in 1 batch". Exceeding it returns a
    # single error object with id=null, which is trivially mistaken for "every call returned
    # nothing" -- it silently produced 43 phantom "unsupported" pools before this was caught.
    def __init__(self, url: str | None = None, batch_size: int | None = None,
                 timeout: int = 45, max_retries: int = 10, min_interval: float | None = None,
                 urls: list[str] | None = None, private: bool | None = None):
        # Multi-chain path: an explicit `urls` list (public archive endpoints for ANY chain,
        # optionally led by a private one) fully replaces the Base-specific defaults below.
        if urls is not None:
            self.urls = list(urls)
            self.private = bool(private)
        else:
            primary = url or DEFAULT_URL
            # Rate limits are per-provider, so a dedicated endpoint plus the public archive ones
            # give ADDITIVE throughput. The private endpoint is listed twice so rotation favours it.
            self.urls = ([primary, primary] + list(ARCHIVE_ENDPOINTS)) if primary \
                else list(ARCHIVE_ENDPOINTS)
            self.private = bool(url or DEFAULT_URL)
        self._next = 0
        if batch_size is None:
            batch_size = 100 if self.private else 5
        if min_interval is None:
            min_interval = 0.0 if self.private else 0.35
        self.batch_size = batch_size
        self.timeout = timeout
        self.max_retries = max_retries
        self.min_interval = min_interval      # pacing floor; public endpoints throttle hard
        # Always use concurrent single calls. Rate limiting -- not batching -- is the binding
        # constraint on every free tier tested, and providers disagree about batch caps (dRPC
        # free: 3; mainnet.base.org: 10), so capability detection across a POOL of endpoints is
        # both fragile and pointless. Concurrency works identically everywhere.
        self.batching: bool | None = False
        self.concurrency = 8 if self.private else 4
        self._last_call = 0.0
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self.n_requests = 0          # crude budget telemetry
        self.n_failed = 0            # non-revert failures tolerated under strict=False

    # ---------------------------------------------------------------- transport
    def _pace(self) -> None:
        """Keep a floor between requests. Free endpoints answer 'over rate limit' per CALL, and
        those look like empty results downstream, so pacing is correctness, not just courtesy."""
        gap = time.monotonic() - self._last_call
        if gap < self.min_interval:
            time.sleep(self.min_interval - gap)
        self._last_call = time.monotonic()

    def _post(self, payload):
        last = None
        for attempt in range(self.max_retries):
            url = self.urls[self._next % len(self.urls)]
            self._next += 1                          # rotate: spreads load across endpoints
            self._pace()
            try:
                r = self.session.post(url, json=payload, timeout=self.timeout)
                self.n_requests += 1
                if r.status_code in (429, 503) or r.status_code >= 500:
                    time.sleep(min(0.75 * (attempt + 1), 5))
                    continue
                if 400 <= r.status_code < 500:
                    # Client errors are NOT transient (malformed request, batch cap exceeded,
                    # bad key). Retrying them just burns minutes before the real failure surfaces.
                    try:
                        return r.json()          # JSON-RPC errors carry the useful diagnosis
                    except ValueError:
                        raise RPCError(f"HTTP {r.status_code}: {r.text[:200]}")
                r.raise_for_status()
                return r.json()
            except requests.RequestException as exc:
                last = exc
                time.sleep(min(0.75 * (attempt + 1), 5))
        raise RPCError(f"RPC failed after {self.max_retries} attempts ({self.urls}): {last}")

    def _rpc(self, method: str, params: list):
        out = self._post({"jsonrpc": "2.0", "method": method, "params": params, "id": 1})
        if "error" in out:
            raise RPCError(f"{method}: {out['error']}")
        return out["result"]

    # ---------------------------------------------------------------- tip (explicit only)
    def latest_block(self) -> int:
        """The chain tip. The ONLY place an unpinned read is allowed -- never call this from
        inside a historical replay."""
        return int(self._rpc("eth_blockNumber", []), 16)

    def block_timestamp(self, block: int) -> int:
        b = self._rpc("eth_getBlockByNumber", [_hexblock(block), False])
        return int(b["timestamp"], 16)

    # ---------------------------------------------------------------- pinned reads
    def call(self, to: str, data: str, block: int,
             from_addr: str | None = None) -> str | None:
        """Single pinned eth_call. Returns hex result, or None if the call reverted.

        A revert is normal and informative here (e.g. getReserves() on a concentrated-liquidity
        pool), so it is surfaced as None rather than raised.

        `from_addr` sets msg.sender. eth_call verifies no signature, so this IMPERSONATES that
        address -- essential for simulating a token transfer from a real holder. Omitting it
        makes msg.sender the zero address, which holds nothing, so every transfer reverts for
        insufficient balance and every token looks like a honeypot.
        """
        tx = {"to": to, "data": data}
        if from_addr:
            tx["from"] = from_addr
        out = self._post({"jsonrpc": "2.0", "method": "eth_call", "id": 1,
                          "params": [tx, _hexblock(block)]})
        if "error" in out:
            return None
        return out.get("result")

    # ---------------------------------------------------------------- capability probe
    def _probe_batching(self, block: int) -> None:
        """Determine ONCE whether the provider honours JSON-RPC batches.

        Discovering this by failure is expensive: an oversized batch can return HTTP 400, which
        looks transient, and the retry ladder then burns ~5 minutes before falling back. One
        cheap probe of `batch_size` no-op calls settles it immediately.
        """
        probe = [{"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": i}
                 for i in range(min(self.batch_size, 8))]
        try:
            out = self._post(probe)
        except RPCError:
            self.batching = False
        else:
            if isinstance(out, list) and out and all("result" in x for x in out):
                self.batching = True
            else:
                self.batching = False
        if not self.batching:
            print(f"  note: provider does not honour JSON-RPC batching; using "
                  f"{self.concurrency} concurrent single calls", flush=True)

    # ---------------------------------------------------------------- concurrent fallback
    def _call_one(self, c: Call, blk: str) -> tuple[str | None, bool]:
        """One eth_call. Returns (result, is_infrastructure_failure).

        Retries generously on 429: the provider's limiter is a token bucket, so a rejected call
        succeeds moments later. Giving up early here would silently shrink the pool universe.
        """
        for attempt in range(8):
            self._pace()
            url = self.urls[self._next % len(self.urls)]
            self._next += 1
            try:
                r = self.session.post(
                    url, timeout=self.timeout,
                    json={"jsonrpc": "2.0", "method": "eth_call", "id": 1,
                          "params": [{"to": c.to, "data": c.data}, blk]})
                self.n_requests += 1
                if r.status_code in (429, 503) or r.status_code >= 500:
                    time.sleep(min(0.5 * (2 ** attempt), 8) * (0.5 + random.random()))
                    continue
                out = r.json()
                if "error" in out:
                    if _is_revert(out["error"]):
                        return None, False           # legitimate contract 'no'
                    time.sleep(min(0.5 * (2 ** attempt), 8) * (0.5 + random.random()))
                    continue
                return out.get("result"), False
            except (requests.RequestException, ValueError):
                time.sleep(min(0.5 * (2 ** attempt), 8) * (0.5 + random.random()))
        return None, True

    def _concurrent_calls(self, calls: list[Call], block: int,
                          strict: bool) -> list[str | None]:
        """Fan out single eth_calls across threads.

        Used when the provider caps JSON-RPC batching (dRPC free allows only 3/batch). Per-call
        concurrency is not restricted, so this recovers the throughput that batching would have
        given -- roughly `concurrency / latency` calls per second.
        """
        from concurrent.futures import ThreadPoolExecutor

        blk = _hexblock(block)
        results: list[str | None] = [None] * len(calls)
        failures = 0
        with ThreadPoolExecutor(max_workers=self.concurrency) as ex:
            for i, (res, failed) in enumerate(ex.map(lambda c: self._call_one(c, blk), calls)):
                results[i] = res
                failures += int(failed)
        if failures:
            msg = (f"{failures}/{len(calls)} eth_calls failed at block {block} "
                   f"(NOT reverts -- infrastructure)")
            if strict:
                raise RPCError(msg)
            self.n_failed += failures
            print(f"  WARNING: {msg}")
        return results

    def batch_call(self, calls: list[Call], block: int,
                   strict: bool = True) -> list[str | None]:
        """Many pinned eth_calls in as few HTTP round trips as possible.

        All calls resolve against the SAME block, which is what makes a graph snapshot
        internally consistent -- mixing blocks across pools invents arbitrage that never existed.

        Failure handling is deliberately loud. A None result means the CONTRACT reverted (real
        information: e.g. getReserves() on a concentrated-liquidity pool). Transport or protocol
        failures are retried and, if still failing under `strict`, raised -- never quietly
        returned as None, because "the node refused us" and "the pool has no reserves" are
        opposite facts that happen to look identical.
        """
        if not calls:
            return []
        if self.batching is None:
            self._probe_batching(block)
        if self.batching is False:
            return self._concurrent_calls(calls, block, strict)

        blk = _hexblock(block)
        results: list[str | None] = [None] * len(calls)
        reverted: set[int] = set()
        pending = list(range(len(calls)))

        for attempt in range(self.max_retries):
            failed: list[int] = []
            for chunk in chunked(pending, self.batch_size):
                payload = [{"jsonrpc": "2.0", "method": "eth_call", "id": i,
                            "params": [{"to": calls[i].to, "data": calls[i].data}, blk]}
                           for i in chunk]
                try:
                    out = self._post(payload)
                except RPCError:
                    failed.extend(chunk)
                    continue
                if isinstance(out, dict):
                    # a whole-batch rejection (e.g. batch cap) arrives as one object, id=null
                    if "error" in out:
                        failed.extend(chunk)
                        continue
                    out = [out]
                seen: set[int] = set()
                for item in out:
                    # provider refuses batching at all -> switch to concurrent single calls
                    if "error" in item and _is_batch_refusal(item["error"]):
                        self.batching = False
                        print("  note: provider caps JSON-RPC batching; using concurrent "
                              f"single calls (x{self.concurrency})")
                        return self._concurrent_calls(calls, block, strict)
                    idx = item.get("id")
                    if idx is None or not isinstance(idx, int) or not (0 <= idx < len(calls)):
                        continue
                    seen.add(idx)
                    if "error" in item:
                        if _is_revert(item["error"]):
                            reverted.add(idx)          # legitimate "no"
                        else:
                            failed.append(idx)         # infrastructure problem -> retry
                    else:
                        results[idx] = item.get("result")
                failed.extend(i for i in chunk if i not in seen)
            if not failed:
                pending = []
                break
            pending = failed
            time.sleep(min(1.5 * (attempt + 1), 6))

        if pending:
            msg = (f"{len(pending)}/{len(calls)} eth_calls failed at block {block} after "
                   f"{self.max_retries} attempts (NOT reverts -- infrastructure)")
            if strict:
                raise RPCError(msg)
            self.n_failed += len(pending)
            print(f"  WARNING: {msg}")
        return results


# ---------------------------------------------------------------- decode helpers
def dec_uint(hexstr: str | None, word: int = 0) -> int | None:
    """Read the `word`-th 32-byte word of an ABI return as an unsigned int."""
    if not hexstr or hexstr == "0x":
        return None
    body = hexstr[2:]
    lo = word * 64
    if len(body) < lo + 64:
        return None
    return int(body[lo:lo + 64], 16)


def dec_address(hexstr: str | None, word: int = 0) -> str | None:
    v = dec_uint(hexstr, word)
    return None if v is None else "0x" + f"{v:040x}"


def enc_address(addr: str) -> str:
    return addr.lower().replace("0x", "").rjust(64, "0")


def enc_uint(v: int) -> str:
    return f"{v:064x}"


def chunked(it, n):
    it = iter(it)
    while True:
        c = list(itertools.islice(it, n))
        if not c:
            return
        yield c
