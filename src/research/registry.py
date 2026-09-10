"""Pre-registration and trial counting.

WHY THIS EXISTS AT ALL. The estate's equities work logs every trial to data/experiments.csv and
reads the row count back as the denominator for deflated-Sharpe math; the on-chain work never did,
and its own joint_panel.py says so. This study logs, because the whole point of pre-registering is
that the hypotheses were fixed before the data was seen, and a file written afterwards proves
nothing.

THE ORDER MATTERS AND IS ENFORCED: preregister() refuses to run once results exist for the same
hypothesis id, so a hypothesis cannot be quietly reworded after seeing its answer.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "data" / "experiments.csv"

COLUMNS = ["run_at", "study", "hid", "hypothesis", "bar", "unit", "stage",
           "statistic", "value", "p", "mde", "n_eff", "gate_pass", "fdr_pass",
           "verdict", "notes"]


def _read() -> pd.DataFrame:
    if REGISTRY.exists():
        return pd.read_csv(REGISTRY)
    return pd.DataFrame(columns=COLUMNS)


def _append(rows: list[dict]) -> None:
    REGISTRY.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[COLUMNS]
    df.to_csv(REGISTRY, mode="a", header=not REGISTRY.exists(), index=False)


def preregister(study: str, hypotheses: list[dict]) -> pd.DataFrame:
    """Lock hypotheses BEFORE looking. Each dict needs hid, hypothesis, bar, unit.

    Refuses if any hid already carries a RESULT row, which is the only way this file can be
    prevented from being rewritten to match an answer.
    """
    have = _read()
    if len(have):
        done = set(have.loc[have.stage.eq("result"), "hid"].astype(str))
        clash = sorted({h["hid"] for h in hypotheses} & done)
        if clash:
            raise RuntimeError(
                f"refusing to pre-register {clash}: results already exist for these ids. "
                f"A hypothesis cannot be restated after its answer is known.")
        already = set(have.loc[have.stage.eq("prereg"), "hid"].astype(str))
        hypotheses = [h for h in hypotheses if h["hid"] not in already]
        if not hypotheses:
            print("  all hypotheses already pre-registered; nothing added", flush=True)
            return _read()
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    _append([{"run_at": now, "study": study, "hid": h["hid"],
              "hypothesis": h["hypothesis"], "bar": h["bar"], "unit": h["unit"],
              "stage": "prereg", "verdict": "PENDING",
              "notes": h.get("notes", "")} for h in hypotheses])
    print(f"  pre-registered {len(hypotheses)} hypotheses -> {REGISTRY}", flush=True)
    return _read()


def record(study: str, hid: str, statistic: str, value, p, mde, n_eff,
           gate_pass, fdr_pass, verdict: str, notes: str = "") -> None:
    """Append a RESULT. Refuses if the hypothesis was never pre-registered."""
    have = _read()
    pre = set(have.loc[have.stage.eq("prereg"), "hid"].astype(str)) if len(have) else set()
    if hid not in pre:
        raise RuntimeError(f"{hid} was never pre-registered; refusing to record a result for it")
    _append([{"run_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
              "study": study, "hid": hid, "stage": "result", "statistic": statistic,
              "value": value, "p": p, "mde": mde, "n_eff": n_eff,
              "gate_pass": gate_pass, "fdr_pass": fdr_pass, "verdict": verdict,
              "notes": notes}])


def trial_count() -> int:
    """Number of RESULT rows ever logged. The denominator for deflated-Sharpe math."""
    have = _read()
    return int(have.stage.eq("result").sum()) if len(have) else 0


def show() -> pd.DataFrame:
    have = _read()
    if not len(have):
        print("  registry empty", flush=True)
    return have
