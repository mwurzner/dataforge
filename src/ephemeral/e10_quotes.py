"""E10 -- cross-aggregator swap quote benchmark. Verified BEFORE building, unlike E1.

WHY THIS PASSES THE TEST. A swap quote is computed on demand from live pool state and stored by
nobody. There is no historical-quote endpoint on any aggregator, because there is no history to
serve -- the number existed for the moment it was asked for. Re-deriving it later would mean
replaying every routable pool across dozens of venues at a past block, which is exactly the
reconstruction the aggregators exist to avoid doing twice.

WHAT ALREADY EXISTS, checked first this time (2026-08-25):
  * LlamaSwap / DefiLlama Swap compares aggregator quotes LIVE, in the browser, and archives
    nothing. The comparison is available to anyone at the moment they ask, and gone after.
  * DefiLlama's aggregator rankings track VOLUME, not quote quality.
  * The only published quote benchmarks are produced BY the aggregators being benchmarked -- a
    LI.FI study of ETH->USDC found LI.FI winning ~84% of the time -- on one pair and one window.
So the live comparison is a commodity and the TIME SERIES is not, and no neutral party keeps one.
That independence is the actual product here, more than the data.

MEASURED DISPERSION, 2026-08-25, WETH->USDC on mainnet:
    size      LI.FI     KyberSwap        CoW    spread    best
    0.1     2,465.90    2,474.19    2,468.97   33.5 bps   KyberSwap
    1       2,468.00    2,474.18    2,472.62   25.0 bps   KyberSwap
    10      2,466.91    2,472.99    2,472.89   24.6 bps   KyberSwap
    100     2,467.14    2,473.32    2,472.81   25.0 bps   KyberSwap
    500     2,464.56    2,468.02    2,472.46   32.0 bps   CoW
THE WINNER CHANGES WITH SIZE, which is the interesting part.

BUT MOST OF THAT GAP IS A FEE, NOT ROUTING -- caught before it became the headline. LI.FI applies
a "LIFI Fixed Fee" of exactly 25.0 bps ($6.18 on 1 WETH, $617.63 on 100 WETH), so its apparent
underperformance is its own take rate. Ex-fee it quotes 2,470.00 against KyberSwap's ~2,474, and
true ROUTING dispersion between the three is far smaller than the 24-37 bps headline implies.
Both readings are legitimate and they answer different questions -- what a user actually receives
through the public endpoint (fee included, dispersion large) versus which router finds the better
path (fee excluded, dispersion small) -- so the panel records the fee separately and never blends
them into one number.

HONEST LIMITS, stated up front rather than discovered by a buyer:
  * QUOTES ARE NOT FILLS. Realised execution differs through slippage, MEV and reverts. This
    measures what each router PROMISED, which is the decision input, not the outcome.
  * Fee conventions differ between providers; CoW quotes net of its fee model. Cross-provider
    comparison is therefore indicative at the basis-point level, not exact.
  * DefiLlama could begin archiving its own live comparison at any time and give it away, as it
    does with everything else. This is a thinner moat than Bitcoin mempool data.
  * Redistribution terms for these APIs are not established. We record OUR OWN requests and
    responses, which is a measurement rather than a copy of somebody's dataset, but that is a
    reasoned position and not a cleared one.

RESPECTFUL BY DESIGN. These are free public endpoints and hammering them is both rude and a good
way to get blocked. One round of 2 pairs x 4 sizes x 3 providers is 24 requests; at the default
15-minute cadence that is ~2,300 requests/day spread evenly, which is ordinary traffic.
"""
from __future__ import annotations

import json
import time
import urllib.request

import pandas as pd

DATASET = "e10_quote_benchmark"
HDRS = {"User-Agent": "dataforge/1.0", "Content-Type": "application/json"}
# A burn address: quotes need a taker, and we never sign or send anything.
TAKER = "0x0000000000000000000000000000000000000001"

TOKENS = {
    "WETH": ("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", 18),
    "USDC": ("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", 6),
    "WBTC": ("0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599", 8),
}
# (sell, buy, [sizes in whole units of the sell token])
PAIRS = [
    ("WETH", "USDC", [0.1, 1, 10, 100]),
    ("WBTC", "USDC", [0.01, 0.1, 1, 10]),
]


def _req(url: str, body: dict | None = None, timeout: int = 25):
    """Returns (payload, error). A failed quote must never be recorded as a bad price."""
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            url, headers=HDRS, data=(json.dumps(body).encode() if body else None)), timeout=timeout)
        return json.loads(r.read()), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {str(exc)[:80]}"


def _lifi(sell, buy, amt):
    d, e = _req(f"https://li.quest/v1/quote?fromChain=1&toChain=1&fromToken={sell}"
                f"&toToken={buy}&fromAmount={amt}&fromAddress={TAKER}")
    if e:
        return None, e
    try:
        est = d["estimate"]
        # THE FEE MUST BE SEPARATED FROM THE ROUTE. LI.FI applies a "LIFI Fixed Fee" of 25 bps
        # (measured: $6.18 on 1 WETH, $617.63 on 100 WETH -- 25.0 bps both times). Reporting only
        # the net output makes a FEE POLICY look like bad routing, which is what the first version
        # of this module did.
        fee_usd = sum(float(c.get("amountUSD") or 0) for c in est.get("feeCosts", []))
        return int(est["toAmount"]), None, {"fee_usd": fee_usd, "tool": d.get("tool")}
    except Exception:
        return None, "unparsable"


def _kyber(sell, buy, amt):
    d, e = _req("https://aggregator-api.kyberswap.com/ethereum/api/v1/routes"
                f"?tokenIn={sell}&tokenOut={buy}&amountIn={amt}")
    if e:
        return None, e
    try:
        rs = d["data"]["routeSummary"]
        return int(rs["amountOut"]), None, {"fee_usd": 0.0, "tool": "kyberswap"}
    except Exception:
        return None, "unparsable"


def _cow(sell, buy, amt):
    d, e = _req("https://api.cow.fi/mainnet/api/v1/quote",
                {"sellToken": sell, "buyToken": buy, "from": TAKER,
                 "kind": "sell", "sellAmountBeforeFee": str(amt)})
    if e:
        return None, e
    try:
        q = d["quote"]
        # CoW deducts its fee from the SELL side, so buyAmount is already net of it. Recorded in
        # sell-token units, which is not directly comparable to LI.FI's USD figure -- hence the
        # separate column rather than a single "fee" number that would silently mix units.
        return int(q["buyAmount"]), None, {"fee_sell_raw": q.get("feeAmount"), "tool": "cow"}
    except Exception:
        return None, "unparsable"


PROVIDERS = {"lifi": _lifi, "kyberswap": _kyber, "cow": _cow}


def sample() -> pd.DataFrame:
    """One full round: every provider, every size, every pair, as close to simultaneous as
    sequential HTTP allows. The timestamp is recorded PER QUOTE, not per round, because prices
    move during the round and pretending otherwise would fabricate simultaneity."""
    rows = []
    for sell_sym, buy_sym, sizes in PAIRS:
        sell, sdec = TOKENS[sell_sym]
        buy, bdec = TOKENS[buy_sym]
        for size in sizes:
            amt = int(size * 10 ** sdec)
            for pname, fn in PROVIDERS.items():
                t0 = time.time()
                res = fn(sell, buy, amt)
                out, err = res[0], res[1]
                meta = res[2] if len(res) > 2 and isinstance(res[2], dict) else {}
                rows.append({
                    "quoted_ts": t0,
                    "pair": f"{sell_sym}/{buy_sym}",
                    "sell_symbol": sell_sym, "buy_symbol": buy_sym,
                    "sell_size": size,
                    "sell_amount_raw": str(amt),      # str: uint256 overflows parquet ints
                    "provider": pname,
                    "buy_amount_raw": str(out) if out is not None else None,
                    # Decimal-adjusted unit price, which is what is actually comparable.
                    "price": (out / 10 ** bdec) / size if out else None,
                    "latency_s": round(time.time() - t0, 3),
                    "error": err,
                    # Provider fee, so a fee POLICY is never mistaken for routing quality.
                    "fee_usd": meta.get("fee_usd"),
                    "fee_sell_raw": (str(meta["fee_sell_raw"])
                                     if meta.get("fee_sell_raw") is not None else None),
                    "route_tool": meta.get("tool"),
                })
                time.sleep(0.25)          # be a polite client of a free service
    df = pd.DataFrame(rows)

    # Per (pair, size): who won, and by how much. Computed here so the panel is usable without
    # re-deriving it, and only over providers that ACTUALLY ANSWERED -- a failed provider must
    # not count as an infinitely bad price.
    df["best_provider"] = None
    df["spread_bps"] = None
    for key, g in df.groupby(["pair", "sell_size"], sort=False):
        ok = g[g["price"].notna()]
        if len(ok) < 2:
            continue
        best = ok.loc[ok["price"].idxmax()]
        bps = (ok["price"].max() - ok["price"].min()) / ok["price"].max() * 1e4
        df.loc[g.index, "best_provider"] = best["provider"]
        df.loc[g.index, "spread_bps"] = round(float(bps), 2)
        df.loc[g.index, "n_answered"] = len(ok)
    return df


if __name__ == "__main__":
    d = sample()
    print(d[["pair", "sell_size", "provider", "price", "spread_bps",
             "best_provider", "latency_s", "error"]].to_string(index=False))
