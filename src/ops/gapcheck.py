"""Gap check -- the thing that makes a hole DETECTABLE instead of silent.

An undetected gap is what destroys the resale value of a time series: a buyer discounts a gapped
panel heavily, and the gap is invisible in the data itself because a missing day looks exactly
like a day on which nothing was written. Families 70, 81, 86 and 93 each had a 100%-failed harvest
render as a clean zero; this is the standing defence against that.

Run after every daily job. Exits non-zero on a gap so the workflow goes red.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

DATA = Path(__file__).resolve().parents[2] / "data"
GRACE_DAYS = 1          # today may legitimately be mid-run


def main() -> int:
    p = DATA / "manifest.parquet"
    if not p.exists():
        print("no manifest yet -- first run has not completed")
        return 0
    m = pd.read_parquet(p)
    m["snapshot_date"] = pd.to_datetime(m["snapshot_date"]).dt.date
    today = date.today()
    problems: list[str] = []

    print("coverage by dataset x chain:")
    for (ds, ch), g in m.groupby(["dataset", "chain"]):
        days = sorted(set(g["snapshot_date"]))
        first, last = days[0], days[-1]
        expected = {first + timedelta(days=i) for i in range((last - first).days + 1)}
        missing = sorted(expected - set(days))
        stale = (today - last).days
        flag = ""
        if missing:
            problems.append(f"{ds}/{ch}: {len(missing)} missing day(s), e.g. {missing[:3]}")
            flag += f"  MISSING {len(missing)}"
        if stale > GRACE_DAYS:
            problems.append(f"{ds}/{ch}: last snapshot {last} ({stale} days stale)")
            flag += f"  STALE {stale}d"
        print(f"  {ds:<26} {ch:<9} {first} .. {last}  {len(days):>4} days  "
              f"median rows {int(g['n_rows'].median()):>6,}{flag}")

    # A silent collapse in row count is a gap by another name: the partition exists, so the date
    # check passes, while the universe behind it has quietly emptied.
    for (ds, ch), g in m.groupby(["dataset", "chain"]):
        g = g.sort_values("snapshot_date")
        if len(g) >= 4:
            recent, base = g["n_rows"].iloc[-1], g["n_rows"].iloc[:-1].median()
            if base and recent < 0.5 * base:
                problems.append(f"{ds}/{ch}: row count collapsed to {recent:,} "
                                f"from a median of {int(base):,}")

    if problems:
        print("\nGAP CHECK FAILED:")
        for x in problems:
            print("  " + x)
        return 1
    print("\ngap check clean")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
