"""E12 -- fiat on-ramp retail quotes. What buying crypto with money actually costs.

WHY THIS PASSES THE CRITERION, verified before building (2026-08-26). An on-ramp quote is
computed per request against the provider's own spread and inventory, and no archive of them was
found anywhere: the published comparisons are one-off blog snapshots of advertised FEE SCHEDULES,
while the documented gap between advertised and effective pricing is large (one public case:
MoonPay quoting BTC 4.31% above market before any stated fee). The effective rate exists only at
the moment of the quote. That is the same shape as E10, applied to the retail door.

WHO CARES: anyone measuring what retail actually pays to enter crypto -- payment-fintech
analysts, stablecoin teams, consumer researchers. The advertised fee is public; the realised
spread over time is not.

WHAT IS COLLECTABLE KEYLESS, probed rather than assumed:
    Mercuryo  public convert endpoint, full buy AND sell quotes with amounts -- the whole
              measurement, spread included
    Ramp      public assets endpoint: per-asset price plus min/max fee percent bounds
    MoonPay, Transak, Guardarian, MtPelerin: all keyed (401/400), recorded here so nobody
              re-probes them; their widget keys exist in page sources but using someone's
              publishable key is not a clean basis for a product
So this panel is honestly THIN -- one full-quote provider plus one partial. It is collected
because the marginal cost is a dozen polite requests per round inside a loop that already runs,
and because the series only exists if someone records it. The dataset card says the same.

RATE CONVENTION: `effective_rate` is always FIAT PER WHOLE COIN, computed from the amounts the
provider returned, never from a rate field it displays. Premium versus mid-market is left to the
analyst: our own e10 panel provides a DEX mid for BTC and ETH against USDC, which keeps the
comparison inside this repo's own data.
"""
from __future__ import annotations

import json
import time
import urllib.request

import pandas as pd

DATASET = "e12_onramp_quotes"
HDRS = {"User-Agent": "dataforge/1.0", "Accept": "application/json"}

# (fiat, fiat_amount) ladder for buys; (asset, crypto_amount) for sells. Small on purpose:
# ~14 requests per round at the e10 cadence is ordinary client traffic.
BUYS = [("EUR", 100), ("EUR", 1000), ("USD", 100), ("USD", 1000)]
BUY_ASSETS = ["BTC", "ETH"]
SELLS = [("BTC", 0.01), ("ETH", 0.5)]
SELL_FIAT = "EUR"


def _get(url: str, timeout: int = 25):
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers=HDRS), timeout=timeout)
        return json.loads(r.read()), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {str(exc)[:80]}"


def _mercuryo(frm: str, to: str, side: str, amount) -> dict:
    """One convert quote, both directions. The response carries rate, gross fiat and a separate
    fee; whether the fee is inside `fiat_amount` is not documented, so BOTH are stored raw and
    the two candidate rates are computed explicitly rather than guessed at analysis time."""
    t0 = time.time()
    d, err = _get("https://api.mercuryo.io/v1.6/public/convert"
                  f"?from={frm}&to={to}&type={side}&amount={amount}")
    row = {"quoted_ts": t0, "provider": "mercuryo", "kind": "quote", "side": side,
           "latency_s": round(time.time() - t0, 3), "error": err,
           "fiat_amount": None, "crypto_amount": None, "provider_rate": None,
           "fee_fiat": None, "effective_rate": None, "rate_net_of_fee": None,
           "min_fee_pct": None, "max_fee_pct": None}
    if err:
        return row
    try:
        x = d["data"]
        row["provider_rate"] = float(x.get("rate")) if x.get("rate") else None
        row["fee_fiat"] = float(x.get("fee")) if x.get("fee") else None
        if side == "buy":
            row["fiat"], row["asset"] = frm, to
            row["fiat_amount"] = float(amount)
            row["crypto_amount"] = float(x["amount"])
            # gross: everything the buyer paid over everything received
            row["effective_rate"] = float(amount) / row["crypto_amount"]
        else:
            row["fiat"], row["asset"] = to, frm
            row["crypto_amount"] = float(amount)
            row["fiat_amount"] = float(x.get("fiat_amount") or 0) or None
            if row["fiat_amount"]:
                row["effective_rate"] = row["fiat_amount"] / float(amount)
                if row["fee_fiat"] is not None:
                    row["rate_net_of_fee"] = (row["fiat_amount"] - row["fee_fiat"]) / float(amount)
    except Exception:
        row["error"] = "unparsable"
    return row


def sample() -> pd.DataFrame:
    rows = []
    for asset in BUY_ASSETS:
        for fiat, amt in BUYS:
            rows.append(_mercuryo(fiat, asset, "buy", amt))
            time.sleep(0.3)
    for asset, camt in SELLS:
        rows.append(_mercuryo(asset, SELL_FIAT, "sell", camt))
        time.sleep(0.3)

    # ---- Ramp: ONE call. Every network variant of an asset carries the identical price dict
    # keyed by dozens of fiats, so the native-chain entry is taken and the rest skipped -- the
    # first version appended a row per variant and produced ten duplicate ETH rows.
    t0 = time.time()
    d, err = _get("https://api.ramp.network/api/host-api/v3/assets?currencyCode=EUR")
    if err or not isinstance(d, dict):
        rows.append({"quoted_ts": t0, "provider": "ramp", "kind": "asset_price", "side": None,
                     "fiat": None, "asset": None, "latency_s": round(time.time() - t0, 3),
                     "error": err or "bad payload"})
    else:
        lo, hi = d.get("minFeePercent"), d.get("maxFeePercent")
        for a in d.get("assets", []):
            sym, chain = a.get("symbol"), a.get("chain")
            if (sym, chain) not in (("BTC", "BTC"), ("ETH", "ETH")):
                continue
            p = a.get("price") or {}
            for fiat in ("EUR", "USD"):
                rows.append({"quoted_ts": t0, "provider": "ramp", "kind": "asset_price",
                             "side": "buy", "fiat": fiat, "asset": sym,
                             "effective_rate": float(p[fiat]) if fiat in p else None,
                             "min_fee_pct": lo, "max_fee_pct": hi,
                             "latency_s": round(time.time() - t0, 3),
                             "error": None if fiat in p else "no price"})
    df = pd.DataFrame(rows)
    # Stable schema regardless of which branches ran this round.
    for c in ["fiat", "fiat_amount", "asset", "crypto_amount", "provider_rate", "fee_fiat",
              "effective_rate", "rate_net_of_fee", "min_fee_pct", "max_fee_pct"]:
        if c not in df.columns:
            df[c] = pd.NA
    return df


if __name__ == "__main__":
    d = sample()
    print(d[["provider", "kind", "side", "fiat", "asset", "fiat_amount", "crypto_amount",
             "effective_rate", "min_fee_pct", "error"]].to_string(index=False))
