"""Fee-estimator accuracy: what was predicted against what actually cleared.

One row per (block, provider, target): the fee rate a provider recommended, the fee rate the
block actually required, and whether paying the recommendation would have got you in.

WHY THIS IS NOT RECONSTRUCTABLE, checked rather than assumed (2026-08-30).

Only ONE half of this is scarce, and saying which matters. Outcomes are freely available: one of
the providers serves a year of realised per-block fee rates for the asking. Predictions are not.
Every provider we poll answers only for right now -- two documented history endpoints return 404
and the one that looks like an archive serves realised rates, not past advice -- so a forecast
made at a past moment exists only if somebody recorded it then.

So the asset is the ESTIMATE history, and this table is the join somebody would otherwise have to
build. A competitor starting to log estimates today reaches parity in a few months; the head
start is the moat, not the method. Searched for an existing published version and found none, but
that search covered one dataset host and a handful of known services rather than the whole
internet, so read it as "none found" rather than "none exists".

Operational notes:
  Derived, never fetched. It is a pure function of e15 and e8 and costs no requests, so a bug
  found later can be repaired by recomputing rather than by re-collecting.
  The clearing rate is a LOW PERCENTILE of STANDALONE transactions, and both halves matter.
  The minimum would measure zero-fee transactions handed straight to a pool. Including chained
  transactions would measure ones admitted on a relative's fee: 93% of the cheapest-looking
  mined transactions are in an unconfirmed chain, and counting them turned a 5.4x overpayment
  into an apparent 18.8x.
"""
from __future__ import annotations

import pandas as pd

DATASET = "e28_fee_estimator_accuracy"

# Where a block's marginal price is read off. p10 rather than the minimum, for the reason in the
# docstring; p05 and the median travel alongside so a reader can pick a different definition.
CLEARING_Q = 0.10
BLOCK_SECONDS = 600.0


def build(fee_df: pd.DataFrame, mempool_df: pd.DataFrame) -> pd.DataFrame:
    """Join estimator forecasts to the block each forecast was about."""
    if fee_df is None or mempool_df is None or not len(fee_df) or not len(mempool_df):
        return pd.DataFrame()

    mined = mempool_df[(mempool_df.get("fate") == "mined")
                       & mempool_df.get("fee_rate_sat_vb").notna()
                       & mempool_df.get("mined_height").notna()
                       & mempool_df.get("block_observed_ts").notna()]
    if not len(mined):
        return pd.DataFrame()

    # THE CLEARING RATE IS MEASURED ON STANDALONE TRANSACTIONS ONLY, and getting this wrong
    # changes the headline by a factor of three and a half.
    #
    # 93% of the cheapest-looking mined transactions turn out to sit in an unconfirmed chain:
    # they were not admitted on their own fee, they were dragged in by a relative. Including
    # them answers "what is the lowest rate visible in a block", which nobody needs. Excluding
    # them answers "what would MY transaction have had to pay", which is the question an
    # estimator is trying to answer. Measured on one window: p10 of 0.106 across all mined
    # against 0.371 standalone, turning an apparent 18.8x overpayment into 5.4x.
    solo = mined[mined.get("ancestor_count") == 1] if "ancestor_count" in mined.columns else mined
    if not len(solo):
        return pd.DataFrame()

    blocks = solo.groupby("mined_height").agg(
        block_observed_ts=("block_observed_ts", "min"),
        n_standalone=("fee_rate_sat_vb", "size"),
        cleared_p05=("fee_rate_sat_vb", lambda s: float(s.quantile(0.05))),
        cleared_p10=("fee_rate_sat_vb", lambda s: float(s.quantile(CLEARING_Q))),
        cleared_median=("fee_rate_sat_vb", "median"),
        cleared_min=("fee_rate_sat_vb", "min"),
    ).reset_index()
    # The all-transaction figure is carried too, so the difference between the two definitions
    # is visible in the data rather than only in this comment.
    allb = mined.groupby("mined_height").agg(
        n_priced=("fee_rate_sat_vb", "size"),
        cleared_p10_all=("fee_rate_sat_vb", lambda s: float(s.quantile(CLEARING_Q))),
    ).reset_index()
    blocks = blocks.merge(allb, on="mined_height", how="left")

    est = fee_df[fee_df.get("error").isna() & fee_df.get("sat_per_vb").notna()
                 & fee_df.get("target_blocks").notna()]
    if not len(est):
        return pd.DataFrame()

    rows = []
    for _, b in blocks.iterrows():
        t_block = float(b.block_observed_ts)
        for target in sorted(est.target_blocks.dropna().unique()):
            # A forecast with target k made at time T is a claim about the next k blocks, so the
            # one that was ABOUT this block was made roughly k block-times earlier. Take the
            # newest forecast at or before that moment: using the closest in absolute time would
            # let a forecast made AFTER the block answer for it.
            want = t_block - float(target) * BLOCK_SECONDS
            sub = est[(est.target_blocks == target) & (est.sampled_ts <= want)]
            if not len(sub):
                continue
            for provider, g in sub.groupby("provider"):
                r = g.loc[g.sampled_ts.idxmax()]
                pred = float(r.sat_per_vb)
                cleared = float(b.cleared_p10)
                rows.append({
                    "height": int(b.mined_height),
                    "block_observed_ts": t_block,
                    "provider": provider,
                    "target_blocks": float(target),
                    "predicted_sat_vb": pred,
                    "predicted_ts": float(r.sampled_ts),
                    "lead_seconds": round(t_block - float(r.sampled_ts), 1),
                    "cleared_p05": float(b.cleared_p05),
                    "cleared_p10": cleared,
                    "cleared_median": float(b.cleared_median),
                    "cleared_min": float(b.cleared_min),
                    "cleared_p10_all_txs": float(b.cleared_p10_all),
                    "n_standalone": int(b.n_standalone),
                    "n_priced": int(b.n_priced),
                    # Would paying the recommendation have got you in? Null rather than a guess
                    # where the block priced nothing to compare against.
                    "sufficient": (pred >= cleared) if cleared == cleared else None,
                    # How many times the going rate the recommendation was. Above 1 is overpaying.
                    "overpay_ratio": (round(pred / cleared, 4) if cleared and cleared > 0 else None),
                })
    return pd.DataFrame(rows)


if __name__ == "__main__":
    import io, json, urllib.request
    UA = {"User-Agent": "dataforge/1.0"}
    R = "dataforge-labs/bitcoin-mempool-lifecycle"

    def load(ds, k=3):
        t = json.loads(urllib.request.urlopen(urllib.request.Request(
            f"https://huggingface.co/api/datasets/{R}/tree/main/{ds}/2026/08", headers=UA),
            timeout=60).read())
        fs = sorted(f["path"] for f in t if f["path"].endswith(".parquet"))[-k:]
        return pd.concat([pd.read_parquet(io.BytesIO(urllib.request.urlopen(
            urllib.request.Request(f"https://huggingface.co/datasets/{R}/resolve/main/{f}",
                                   headers=UA), timeout=90).read())) for f in fs],
            ignore_index=True)

    df = build(load("e15_fee_estimators"), load("e8_btc_mempool_lifecycle", 1))
    print(f"{len(df)} rows over {df.height.nunique() if len(df) else 0} blocks")
    if len(df):
        print(df.groupby(["provider", "target_blocks"]).agg(
            n=("height", "size"), sufficient=("sufficient", "mean"),
            overpay=("overpay_ratio", "median")).to_string())
