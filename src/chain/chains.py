"""Chain registry for DataForge -- the only chain-specific configuration in the project.

Launch scope is Ethereum + Base, which family 148's census showed hold 69% of Morpho vaults and
93% of its TVL. Adding a chain is adding a row here, nothing else.

*** ENDPOINT CAPABILITY VARIES BY METHOD, WHICH IS WHY THERE ARE TWO POOLS PER CHAIN. ***
Probed 2026-08-25, every candidate tested on eth_blockNumber, eth_call AND eth_getLogs:
    ethereum-rpc.publicnode.com   tip ok  call ok  getLogs 403   <- fast for calls, useless for logs
    eth.merkle.io                 tip ok  call ok  getLogs 400
    eth.drpc.org                  tip ok  call ok  getLogs OK    <- only full-capability free one
    rpc.flashbots.net             tip ok  call 403 getLogs OK
    rpc.ankr.com/eth              requires an API key now
    cloudflare-eth.com            refuses
    1rpc.io/eth                   rate-limited immediately
    base-rpc.publicnode.com       tip ok  call ok  getLogs 403
    mainnet.base.org              tip ok  call ok  getLogs OK
    base.drpc.org                 tip ok  call ok  getLogs OK
    base.llamarpc.com             HTTP 521
    1rpc.io/base                  getLogs capped at 50 blocks
Assuming one pool serves every method is exactly the class of unverified assumption that has cost
this project repeatedly -- a 403 on getLogs from a "known good" endpoint would have surfaced as a
universe that silently stopped growing.

WHY NO ARCHIVE AND NO SECRETS ARE NEEDED: forward logging reads the CURRENT tip, so archive depth
is irrelevant -- it is only required for backfill, which by design we never do. That is what keeps
this project free and keyless.

VOLUME NOTE: `rpcs_call` carries ~30k reads/day and is the throughput-critical pool.
`rpcs_logs` is used only by the WEEKLY universe refresh, so its lower throughput is irrelevant.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Chain:
    name: str
    rpcs_call: list[str]            # high-volume eth_call pool, rotated
    rpcs_logs: list[str]            # eth_getLogs-capable pool (weekly discovery only)
    block_time_s: float
    native_symbol: str
    explorer: str
    getlogs_cap: int | None = None  # measured, never assumed (families 29/86/93/144b)
    drpc_slug: str | None = None
    # Throughput, MEASURED not guessed. RPC defaults to 0.35s/4-way for public endpoints, tuned
    # for flaky Base free endpoints; on these pools that yields only ~217 calls/min and made a
    # full daily run take 93 minutes. Measured on 300 real market reads per chain -- see the
    # per-chain comments below for the curve. Deliberately NOT set to the fastest observed value:
    # a 300-call burst does not prove a 15,000-call sustained run, and a shared Actions IP is a
    # worse place to discover a rate limit than this laptop.
    min_interval: float = 0.05
    concurrency: int = 12

    @property
    def rpcs(self) -> list[str]:    # back-compat for helpers expecting a single list
        return self.rpcs_call


CHAINS: dict[str, Chain] = {
    "ethereum": Chain(
        name="ethereum",
        rpcs_call=[
            "https://ethereum-rpc.publicnode.com",
            "https://eth.merkle.io",
            "https://eth.drpc.org",
        ],
        rpcs_logs=[
            "https://eth.drpc.org",
            "https://rpc.flashbots.net",
        ],
        block_time_s=12.0,
        native_symbol="ETH",
        explorer="https://etherscan.io",
        getlogs_cap=10000,          # verified in family 149's Deposit sweep
        drpc_slug="ethereum",
        # measured 2026-08-25: 0.35/4 -> 217/min | 0.10/8 -> 693 | 0.05/12 -> 946 | 0.0/24 -> 1095
        # zero failures at every setting above 0.35/4; 0.05/12 keeps headroom.
        min_interval=0.05,
        concurrency=12,
    ),
    "base": Chain(
        name="base",
        rpcs_call=[
            "https://base-rpc.publicnode.com",
            "https://mainnet.base.org",
            "https://base.drpc.org",
        ],
        rpcs_logs=[
            "https://base.drpc.org",
            "https://mainnet.base.org",
        ],
        block_time_s=2.0,           # verified 2.000 s/block in family 54b
        native_symbol="ETH",
        explorer="https://basescan.org",
        getlogs_cap=10000,          # measured family 143b: 10k OK, 50k -> HTTP 400
        drpc_slug="base",
        # DELIBERATELY MORE CONSERVATIVE THAN ETHEREUM, and the reason is measured: Base's pool is
        # demonstrably flakier -- mainnet.base.org answered only 5/20 under burst in the endpoint
        # probe, and the first full A1 Base run logged 18 infrastructure failures across 9,403
        # calls (0.19%) even at the old 0.35/4 default. Ethereum logged none. 0.10/8 is the
        # middle setting measured safe there (~693 calls/min, ~3x the old default) and leaves
        # more headroom on the weaker pool. Revisit only with a Base-specific measurement.
        min_interval=0.10,
        concurrency=8,
    ),
}
