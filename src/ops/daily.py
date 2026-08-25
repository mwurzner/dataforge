"""The daily run. One pinned block per chain, every Tier-A logger, then the Tier-B derivations.

SCHEDULED TWICE A DAY AND IDEMPOTENT BY DATE. GitHub's cron is explicitly best-effort and drops
runs; for EVENT data a dropped run self-heals on the next backfill, but these are STATE reads and
free endpoints do not retain archive depth -- so a missed day is gone permanently. Two attempts
per day, with `write_partition` overwriting rather than appending, converts a dropped run from a
permanent hole into a non-event.

A logger that raises does NOT abort the run. Each is isolated so one failing chain or one bad
provider cannot cost the other datasets their only chance at today's state.
"""
from __future__ import annotations

import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.chain.chains import CHAINS
from src.chain.multi import rpc_for
from src.derive import b2_stuck, b5_dormancy
from src.loggers import a1_markets, a2_vaults
from src.ops.snapshot import RunLog, resolve_tip, utc_date

# Tier A reads the chain. Tier B is a PURE FUNCTION of Tier A's output and costs no RPC at all,
# which is why the derived layer can be recomputed from scratch if a bug is found months later --
# raw stays immutable, derived stays disposable.
LOGGERS = [a1_markets, a2_vaults]
DERIVERS = [b2_stuck, b5_dormancy]


def main() -> int:
    date = utc_date()
    log = RunLog()
    failures: list[str] = []
    print(f"DataForge daily run  {date}\n", flush=True)

    for chain in CHAINS:
        try:
            rpc = rpc_for(CHAINS[chain], "call")
            block, ts = resolve_tip(rpc, chain)
        except Exception as exc:
            failures.append(f"{chain}: could not resolve tip: {exc}")
            print(f"  [{chain}] TIP FAILED: {exc}", flush=True)
            continue
        print(f"  [{chain}] pinned block {block:,} (ts {ts})", flush=True)
        for mod in LOGGERS:
            try:
                mod.run(chain, rpc, block, ts, date, log)
            except Exception as exc:
                failures.append(f"{chain}/{mod.DATASET}: {exc}")
                print(f"  [{chain}] {mod.DATASET} FAILED: {exc}", flush=True)
                traceback.print_exc()
        for mod in DERIVERS:
            try:
                mod.run(chain, block, ts, date, log)
            except Exception as exc:
                failures.append(f"{chain}/{mod.DATASET}: {exc}")
                print(f"  [{chain}] {mod.DATASET} FAILED: {exc}", flush=True)

    log.flush()
    if failures:
        print("\nFAILURES (%d):" % len(failures), flush=True)
        for f in failures:
            print("  " + f, flush=True)
        # Non-zero exit so the workflow surfaces it. Partitions that DID succeed are already
        # written and committed -- a partial day is far better than none, and the manifest
        # records exactly which datasets are missing.
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
