"""Per-chain RPC construction + the extra read primitives the keeper census needs.

The base `RPC` class (copied verbatim from LSTBasis) already gives pinned `call`/`batch_call`,
loud failure handling, and the no-lookahead guard. This adds only what a bounty census requires
on top: log fetching, transaction receipts (for EXACT gas paid, never modelled), and block
timestamps — each pinned, each failing loudly.
"""
from __future__ import annotations

import os

from .chains import Chain
from .keccak import keccak256
from .rpc import RPC, RPCError


def keccak(text: str) -> bytes:
    """Pure-Python keccak-256, replacing eth_utils.keccak.

    eth-utils needs a compiled hashing backend (eth-hash), which is one more thing that can fail
    to install or be blocked on a runner. Selectors and event topics are deterministic constants
    of the ABI, so computing them in pure Python removes the dependency permanently rather than
    hardcoding a lookup table that breaks the next time a signature is needed.
    """
    return keccak256(text.encode())

# dRPC key (if present) serves several of these chains far faster than the public endpoints.
_DRPC = os.environ.get("BASE_RPC_URL", "").strip()


def rpc_for(chain: Chain, purpose: str = "call") -> RPC:
    """Build an RPC pointed at one chain, for a SPECIFIC method class.

    Endpoint capability varies by method -- publicnode serves eth_call happily and returns 403 on
    eth_getLogs -- so a single pool is wrong. `purpose="call"` gets the high-volume read pool;
    `purpose="logs"` gets the getLogs-capable pool used by the weekly universe refresh.
    """
    pool = chain.rpcs_logs if purpose == "logs" else chain.rpcs_call
    urls: list[str] = list(pool)
    private = False
    if chain.drpc_slug and _DRPC and "drpc.org" in _DRPC:
        drpc_url = _DRPC.replace("network=base", f"network={chain.drpc_slug}")
        urls = [drpc_url, drpc_url] + urls
        private = True
    rpc = RPC(urls=urls, private=private)
    if purpose != "logs" and not private:
        # Apply the per-chain MEASURED throughput settings. Only for the high-volume call pool:
        # getLogs is a weekly job whose throughput is irrelevant, and pushing it would only risk
        # tripping the provider limits that the chunker then has to recover from.
        rpc.min_interval = chain.min_interval
        rpc.concurrency = chain.concurrency
    return rpc


def topic(sig: str) -> str:
    """Event topic0 = keccak(event signature)."""
    return "0x" + keccak(sig).hex()


def selector(sig: str) -> str:
    return "0x" + keccak(sig).hex()[:8]


def get_logs(rpc: RPC, from_block: int, to_block: int, address: str | list[str] | None = None,
             topics: list | None = None) -> list[dict]:
    """Pinned eth_getLogs over an EXPLICIT block range. Raises loudly on provider errors so a
    range-cap or rate-limit failure can never be mistaken for 'no events happened'."""
    params: dict = {"fromBlock": hex(from_block), "toBlock": hex(to_block)}
    if address is not None:
        params["address"] = address
    if topics is not None:
        params["topics"] = topics
    return rpc._rpc("eth_getLogs", [params])


def get_logs_chunked(rpc: RPC, from_block: int, to_block: int, cap: int,
                     address=None, topics=None, on_progress=None) -> list[dict]:
    """Walk a large range in <=cap-block windows (the measured getLogs limit for this chain).

    If a window still trips a 'too many results' error, it is halved and retried, so a busy
    contract cannot silently drop events -- the same loud-degradation discipline as batch_call.
    """
    out: list[dict] = []
    start = from_block
    while start <= to_block:
        end = min(start + cap - 1, to_block)
        lo, hi = start, end
        while True:
            try:
                out.extend(get_logs(rpc, lo, hi, address, topics))
                break
            except RPCError as exc:
                msg = str(exc).lower()
                # "timeout" and "too large" belong here too: a provider that times out on a wide
                # window is telling us the window is too wide, exactly like a results cap.
                if any(k in msg for k in ("results", "range", "limit", "timeout",
                                          "too large", "query returned more")) and hi > lo:
                    hi = lo + (hi - lo) // 2          # too many results -> shrink and retry
                    continue
                raise
        if on_progress:
            on_progress(hi, to_block, len(out))
        start = hi + 1
    return out


def get_receipt(rpc: RPC, tx_hash: str) -> dict | None:
    return rpc._rpc("eth_getTransactionReceipt", [tx_hash])


def gas_paid_wei(rpc: RPC, tx_hash: str) -> int | None:
    """EXACT gas cost of a transaction = gasUsed x effectiveGasPrice, from the receipt.
    This is measured, never estimated -- a modelled gas number is exactly how a census of
    'profitable' claims becomes fiction."""
    r = get_receipt(rpc, tx_hash)
    if not r or r.get("gasUsed") is None:
        return None
    gas_used = int(r["gasUsed"], 16)
    price = r.get("effectiveGasPrice")
    if price is None:
        return None
    l2_cost = gas_used * int(price, 16)
    # OP-stack / L2 chains add an L1 data fee, also in the receipt. Include it or understate cost.
    l1_fee = int(r.get("l1Fee", "0x0"), 16) if r.get("l1Fee") else 0
    return l2_cost + l1_fee


def block_ts(rpc: RPC, block: int) -> int:
    return rpc.block_timestamp(block)


def find_block_at_time(rpc: RPC, target_ts: int, tip: int, block_time_s: float,
                       tol_s: int = 30) -> int:
    """Block whose timestamp is ~target_ts, by guided bisection.

    Needed to convert a 'last 30 days' window into a block range per chain without assuming a
    perfectly constant block time (chains drift, halt, and reorg early history).
    """
    lo, hi = 1, tip
    # seed near the linear estimate to cut iterations
    guess = max(1, tip - int((block_ts(rpc, tip) - target_ts) / block_time_s))
    guess = min(max(guess, lo), hi)
    for _ in range(40):
        t = block_ts(rpc, guess)
        if abs(t - target_ts) <= tol_s:
            return guess
        if t < target_ts:
            lo = guess + 1
        else:
            hi = guess - 1
        if lo > hi:
            break
        guess = (lo + hi) // 2
    return guess
