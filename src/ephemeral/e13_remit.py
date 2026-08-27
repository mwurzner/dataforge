"""E13 -- cross-border remittance pricing panel, from Wise's public comparison service.

WHY THIS PASSES THE CRITERION, verified before building (2026-08-26). A remittance quote is
priced per request and stored by nobody public: the only systematic record found is the World
Bank's Remittance Prices Worldwide survey, which is QUARTERLY and mystery-shopped. No
high-frequency archive of provider margins exists that we could find. The quotes themselves are
gone the moment they change.

WHO CARES, and this one is unusually direct: Wise, Western Union, PayPal and Remitly are LISTED
companies whose take-rates drive earnings. A daily panel of effective pricing per corridor per
provider is competitive telemetry on public companies, of the kind equity analysts pay for in
other industries. First probe showed a 4.5% gap between best and worst provider on the same
EUR to USD transfer.

THE SOURCE, stated plainly because it shapes the data: this is WISE'S OWN comparison feed. Wise
collects competitor quotes to power its transparency marketing, so (a) coverage per corridor is
whatever Wise chooses to compare against, (b) a competitor's quote carries a collection lag that
the `date_collected` field exposes when present, and (c) the publisher has an interest in looking
cheapest. We record the feed as observed and keep Wise's own quote clearly labelled, so the bias
is at least constant and visible rather than hidden. Redistribution terms for this feed are a
reasoned position, not a cleared one, same as e10 and e12.

CORRIDORS: the largest remittance lanes plus the intra-European pair where bank pricing is worst.
One amount per corridor; ~9 requests per round at the e10 cadence is ordinary client traffic.
"""
from __future__ import annotations

import json
import time
import urllib.request

import pandas as pd

DATASET = "e13_remittance_quotes"
HDRS = {"User-Agent": "dataforge/1.0", "Accept": "application/json"}

# (source, target, send_amount). High-volume lanes per World Bank flows, plus EUR/USD.
# Twenty of the largest World Bank remittance lanes. Chosen as the moderate expansion: ~20
# requests per round against a free endpoint is ordinary client traffic, where the aggressive
# option (35+ corridors with tiered amounts) risked a block that would cost the whole panel.
# Codes here are CURRENCIES, not countries: an early version used GTM/DOM/VNM/NGA/
# MAR/POL/UKR/PAK and ten of eleven new corridors silently returned nothing.
# Corridors added later simply start later; the manifest already makes that visible, and no
# existing series is disturbed.
CORRIDORS = [
    ("USD", "MXN", 500),
    ("USD", "INR", 500),
    ("USD", "PHP", 500),
    ("USD", "GTQ", 500),
    ("USD", "DOP", 500),
    ("USD", "VND", 500),
    ("USD", "NGN", 500),
    ("USD", "COP", 500),
    ("USD", "BRL", 500),
    ("EUR", "USD", 1000),
    ("EUR", "INR", 1000),
    ("EUR", "TRY", 500),
    ("EUR", "MAD", 500),
    ("EUR", "PLN", 500),
    ("EUR", "UAH", 500),
    ("EUR", "NGN", 500),
    ("GBP", "EUR", 1000),
    ("GBP", "INR", 1000),
    ("GBP", "PKR", 500),
    ("GBP", "NGN", 500),
]


def _get(url: str, timeout: int = 25):
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers=HDRS), timeout=timeout)
        return json.loads(r.read()), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {str(exc)[:80]}"


def sample() -> pd.DataFrame:
    rows = []
    for src, dst, amt in CORRIDORS:
        t0 = time.time()
        d, err = _get("https://api.wise.com/v3/comparisons"
                      f"?sourceCurrency={src}&targetCurrency={dst}&sendAmount={amt}")
        if err or not isinstance(d, dict):
            rows.append({"quoted_ts": t0, "corridor": f"{src}-{dst}", "send_amount": amt,
                         "provider": None, "latency_s": round(time.time() - t0, 3),
                         "error": err or "bad payload"})
            time.sleep(0.4)
            continue
        lat = round(time.time() - t0, 3)
        for p in d.get("providers", []):
            for q in (p.get("quotes") or [])[:1]:
                rows.append({
                    "quoted_ts": t0,
                    "corridor": f"{src}-{dst}",
                    "send_amount": float(amt),
                    "provider": p.get("alias"),
                    "rate": q.get("rate"),
                    "fee": q.get("fee"),
                    "received_amount": q.get("receivedAmount"),
                    # When Wise mystery-shopped this competitor, if it says so. Staleness is
                    # part of the measurement, not noise to discard.
                    "date_collected": q.get("dateCollected"),
                    "is_wise_own_quote": p.get("alias") == "wise",
                    "latency_s": lat,
                    "error": None,
                })
        time.sleep(0.4)
    df = pd.DataFrame(rows)
    for c in ["rate", "fee", "received_amount", "date_collected", "is_wise_own_quote"]:
        if c not in df.columns:
            df[c] = pd.NA
    # Best-in-corridor context, computed per round so the panel is usable without a join:
    # how much of the send amount each provider's total cost eats, versus the round's best.
    if "received_amount" in df.columns:
        df["received_amount"] = pd.to_numeric(df["received_amount"], errors="coerce")
        best = df.groupby("corridor")["received_amount"].transform("max")
        df["shortfall_vs_best_pct"] = ((best - df["received_amount"]) / best * 100).round(4)
    return df


if __name__ == "__main__":
    d = sample()
    ok = d[d["provider"].notna()]
    print(f"{len(ok)} quotes across {ok['corridor'].nunique()} corridors, "
          f"{d['error'].notna().sum()} errors")
    for c, g in ok.groupby("corridor"):
        w = g[g.is_wise_own_quote == True]
        print(f"  {c:<8} providers {len(g):>2}  worst shortfall "
              f"{g.shortfall_vs_best_pct.max():5.2f}%  "
              f"wise shortfall {w.shortfall_vs_best_pct.iloc[0] if len(w) else float('nan'):5.2f}%")
