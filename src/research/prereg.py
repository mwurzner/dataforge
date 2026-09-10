"""Lock the hypotheses for the ephemeral edge hunt, BEFORE the private archive is touched.

HONESTY NOTE THAT MATTERS MORE THAN THE REGISTRY ITSELF. Two of these three hypotheses were formed
AFTER looking at the public 7-8 day sample, during reconnaissance. Calling them "pre-registered"
without saying so would be exactly the self-deception this whole apparatus exists to prevent, so
each row records its prior exposure:

    H1  gas frontier      SEEN. The top-vs-bottom split and its 8/8-day consistency were measured
                          on the public window before this file was written. Running it on the
                          archive is a REPLICATION on partly-overlapping data, not a blind test.
    H2  bitcoin frontier  PARTLY SEEN. The aggregate sufficiency/overpay table was inspected; the
                          per-day structure and the mechanism gate were not.
    H3  mempool hazard    BLIND. Never looked at. This is the only genuine pre-registration here.

The genuine out-of-sample for all three is the FORWARD re-run in ~3 months on data that does not
exist yet, which is why the bars are fixed now rather than after.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.research.registry import preregister, show, trial_count  # noqa: E402

STUDY = "ephemeral-edge-v1"

HYPOTHESES = [
    {
        "hid": "H1-gas-frontier",
        "hypothesis": (
            "Among the RPC providers in e29/e30, at least one DOMINATES another for Ethereum "
            "inclusion: equal-or-higher sufficient_priority at horizon 1 while requiring a "
            "strictly lower overpay_priority. Stated as a cost claim, not alpha."),
        "bar": (
            "dominance holds on >=80% of days AND day-level sign test p<0.05 AND the mechanism "
            "gate passes (sufficiency monotone non-decreasing across suggestion quantiles)"),
        "unit": "day (a provider's suggestion is near-constant between blocks, so blocks are not "
                "independent; effective n ~ number of days, not ~123k rows)",
        "notes": (
            "PRIOR EXPOSURE: SEEN. Measured on the public 8-day window during recon: top group "
            "(drpc/blxrbdn/flashbots ~0.56) vs bottom (merkle/nodies ~0.22), gap positive 8/8 "
            "days, mean +0.337, min +0.273. This archive run is a REPLICATION, not a blind test."),
    },
    {
        "hid": "H2-btcfee-frontier",
        "hypothesis": (
            "Among the five Bitcoin estimators in e15/e28, at each target_blocks, at least one "
            "provider dominates another: equal-or-higher `sufficient` at strictly lower "
            "`overpay_ratio`."),
        "bar": (
            "dominance holds on >=80% of days at the same target_blocks AND day-level sign test "
            "p<0.05 AND sufficiency monotone non-decreasing across quoted-rate quantiles"),
        "unit": "day; secondary unit block, clustered by day",
        "notes": (
            "PRIOR EXPOSURE: PARTLY SEEN. The pooled table over the public 7-day window was "
            "inspected (target 1: providers span 79.5%-98.4% sufficiency at 1.61x-3.22x overpay, "
            "no dominance visible pooled). Per-day structure and the gate were NOT examined."),
    },
    {
        "hid": "H3-mempool-hazard",
        "hypothesis": (
            "Mempool state observable at first sight in e8_btc_mempool_lifecycle "
            "(fee_rate_sat_vb, effective_fee_rate_sat_vb, ancestor_count) predicts blocks_waited "
            "BEYOND what the contemporaneous e15 estimator advice implies: i.e. a user reading "
            "the mempool can beat the published estimators."),
        "bar": (
            "out-of-sample improvement in a proper scoring rule on the second half after fitting "
            "on the first, AND permutation placebo p<0.05 (fee rate shuffled within arrival "
            "window), AND monotone gate (higher fee rate -> shorter wait across quantiles)"),
        "unit": "transaction, clustered by block and by day; report both n_tx and n_days",
        "notes": (
            "PRIOR EXPOSURE: NONE. Never inspected. This is the only genuine blind "
            "pre-registration in this study. Note e8 is also the only panel with enough rows "
            "(~100k/run) to support a powered hazard model."),
    },
]

if __name__ == "__main__":
    preregister(STUDY, HYPOTHESES)
    df = show()
    print()
    print(df[["run_at", "hid", "stage", "verdict"]].to_string(index=False))
    print()
    print(f"  trials recorded so far (result rows): {trial_count()}")
    print("  NOTE: multiple testing across 3 hypotheses, and H1/H2 are the SAME question on two")
    print("  chains, so they are correlated. BH-FDR will be applied but does not fix that.")
