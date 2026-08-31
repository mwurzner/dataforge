"""E30 -- did following each provider's gas suggestion actually get you included, and at what cost.

One row per (provider, head block, horizon): what the provider suggested while its head was block
N, what block N+h actually required, and whether paying the suggestion would have worked.

DERIVED, NEVER FETCHED. A pure function of E29's two frames, so it costs no requests and a bug
found in six months is repaired by recomputing rather than by re-collecting. Same contract as E28.

THE MATCHING NEEDS NO TIMESTAMPS, which removes the main way this kind of join goes wrong. Each
E29 row records the head block the provider reported at the moment it answered, so a suggestion
made at head N is by construction advice about getting into block N+1. Matching on block height is
exact; matching on wall-clock time would have to guess at propagation and sampling lag.

WHAT COUNTS AS THE RATE THAT CLEARED. reward_p10 -- the 10th percentile of priority fees actually
paid by transactions in the block. Not the minimum: blocks routinely include zero-priority
transactions placed by builders or bundled privately, and those measure a private arrangement
rather than the price a stranger would have had to pay. p5 and the median travel alongside so a
reader can choose a different definition rather than accept ours.

TWO SUFFICIENCY QUESTIONS, because wallets ask two different ones. A legacy transaction sets a
total gasPrice; an EIP-1559 transaction sets a priority tip on top of a base fee it does not
control. Both are answered, and they can disagree.

READ THE ZEROS CAREFULLY. A cleared rate of exactly zero is common and genuine -- it means the
block accepted at least a tenth of its transactions at no tip at all. Sufficiency is then trivially
true and the overpay ratio is undefined rather than infinite, so it is stored as null. Rows where
the provider itself returned a null are dropped, never coerced to zero: a failed read and a
genuine zero suggestion are different facts.
"""
from __future__ import annotations

import pandas as pd

DATASET = "e30_gas_estimator_accuracy"

HORIZONS = (1, 2, 3)
WEI = 1e9


def build(est_df: pd.DataFrame, blocks_df: pd.DataFrame) -> pd.DataFrame:
    """Join E29 suggestions to the blocks they were predicting."""
    if est_df is None or blocks_df is None or not len(est_df) or not len(blocks_df):
        return pd.DataFrame()
    need_e = {"provider", "head_block", "gas_price_wei", "max_priority_fee_wei", "sampled_ts"}
    need_b = {"block_number", "base_fee_wei", "reward_p10_wei"}
    if not need_e.issubset(est_df.columns) or not need_b.issubset(blocks_df.columns):
        return pd.DataFrame()

    est = est_df[est_df.head_block.notna() & est_df.gas_price_wei.notna()
                 & est_df.max_priority_fee_wei.notna()].copy()
    if not len(est):
        return pd.DataFrame()
    est["head_block"] = est.head_block.astype("int64")

    # One suggestion per provider per head block: the freshest, since a later sample at the same
    # head reflects a more complete mempool view.
    est = (est.sort_values("sampled_ts").groupby(["provider", "head_block"], as_index=False).last())

    blocks = blocks_df[blocks_df.block_number.notna()].drop_duplicates(
        subset="block_number", keep="last").set_index("block_number")

    rows = []
    for _, e in est.iterrows():
        n = int(e.head_block)
        pred_prio = float(e.max_priority_fee_wei)
        pred_total = float(e.gas_price_wei)
        for h in HORIZONS:
            window = [n + k for k in range(1, h + 1)]
            if not all(b in blocks.index for b in window):
                continue
            tgt = blocks.loc[n + h]

            # Would it have got in within h blocks? Cheapest bar across the window.
            prio_bars, total_bars = [], []
            for b in window:
                r = blocks.loc[b]
                p10 = r.get("reward_p10_wei")
                bf = r.get("base_fee_wei")
                if pd.notna(p10):
                    prio_bars.append(float(p10))
                    if pd.notna(bf):
                        total_bars.append(float(bf) + float(p10))
            if not prio_bars:
                continue
            prio_bar = min(prio_bars)
            total_bar = min(total_bars) if total_bars else None

            cleared = float(tgt.reward_p10_wei) if pd.notna(tgt.reward_p10_wei) else None
            base = float(tgt.base_fee_wei) if pd.notna(tgt.base_fee_wei) else None

            rows.append({
                "provider": e.provider,
                "head_block": n,
                "target_block": n + h,
                "horizon_blocks": h,
                "sampled_ts": float(e.sampled_ts),
                "predicted_priority_gwei": pred_prio / WEI,
                "predicted_gas_price_gwei": pred_total / WEI,
                "cleared_p5_gwei": (float(tgt.reward_p5_wei) / WEI
                                    if "reward_p5_wei" in tgt and pd.notna(tgt.reward_p5_wei)
                                    else None),
                "cleared_p10_gwei": cleared / WEI if cleared is not None else None,
                "cleared_p50_gwei": (float(tgt.reward_p50_wei) / WEI
                                     if "reward_p50_wei" in tgt and pd.notna(tgt.reward_p50_wei)
                                     else None),
                "base_fee_gwei": base / WEI if base is not None else None,
                "gas_used_ratio": (float(tgt.gas_used_ratio)
                                   if "gas_used_ratio" in tgt and pd.notna(tgt.gas_used_ratio)
                                   else None),
                # Would paying the suggestion have got you in within the horizon?
                "sufficient_priority": bool(pred_prio >= prio_bar),
                "sufficient_total": (bool(pred_total >= total_bar)
                                     if total_bar is not None else None),
                # How many times the going rate the suggestion was. Null, not infinity, where the
                # block cleared at zero -- that is an undefined ratio, not an enormous one.
                "overpay_priority": (round(pred_prio / cleared, 4)
                                     if cleared else None),
                "overpay_total": (round(pred_total / (base + cleared), 4)
                                  if (base is not None and cleared is not None
                                      and (base + cleared) > 0) else None),
            })
    return pd.DataFrame(rows)


if __name__ == "__main__":
    import time

    from src.ephemeral import e29_gas_estimators as e29

    print("sampling suggestions for ~3 min so a few blocks accumulate...")
    frames = []
    for i in range(9):
        frames.append(e29.sample())
        if i < 8:
            time.sleep(20)
    est = pd.concat(frames, ignore_index=True)
    blocks = e29.block_fees(n=40)

    acc = build(est, blocks)
    print(f"\n{len(acc)} accuracy rows over {acc.target_block.nunique() if len(acc) else 0} blocks")
    if len(acc):
        g = acc.groupby(["provider", "horizon_blocks"]).agg(
            n=("target_block", "size"),
            sufficient=("sufficient_priority", "mean"),
            overpay=("overpay_priority", "median"))
        print(g.to_string())
