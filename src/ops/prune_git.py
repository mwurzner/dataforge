"""Keep the git data repo a ROLLING BUFFER, with HuggingFace as the permanent archive.

THE PROBLEM THIS SOLVES, measured rather than anticipated. E1 writes ~135 MB/day and E8 adds
~30 MB more. GitHub starts warning around 1 GB and refusing around 5 GB, so the private data repo
becomes unusable in roughly five weeks and the whole pipeline stops. That is not a distant
scaling concern; it is a scheduled outage.

THE DESIGN. HuggingFace is git-LFS backed and built for this, so it holds the full history. Git
keeps only the last RETAIN_DAYS of the heavy partitions, which preserves the reason git is in the
loop at all: if HuggingFace is down during a collection window, the data still lands somewhere
durable, and an ephemeral window cannot be re-collected.

TWO RULES THAT MAKE IT SAFE:
  1. NOTHING IS PRUNED UNLESS THE FULL PUSH SUCCEEDED. hf_push writes a receipt naming the
     datasets it actually uploaded; a dataset missing from the receipt is never touched. A prune
     that ran on a failed push would destroy the only copy.
  2. LIGHT DATASETS ARE NEVER PRUNED. The manifests and divergence samples are kilobytes, and
     they are what makes a gap DETECTABLE later. Losing the heavy rows is recoverable from HF;
     losing the record of what was collected is not.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
RECEIPT = DATA / ".hf_receipt.json"
RETAIN_DAYS = int(os.environ.get("DF_GIT_RETAIN_DAYS", 14))

# Only the high-volume lifecycle sets are ever pruned. Everything else is small and stays, because
# the manifests are the evidence of what was collected and when.
HEAVY = {"e1_mempool_lifecycle", "e8_btc_mempool_lifecycle"}


def main() -> int:
    if not RECEIPT.exists():
        print("  no HF receipt -- refusing to prune. Git is the only copy until HF confirms.",
              flush=True)
        return 0
    try:
        receipt = json.loads(RECEIPT.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"  unreadable receipt ({exc}) -- refusing to prune", flush=True)
        return 0

    if not receipt.get("full_ok"):
        print("  last full-history push did NOT succeed -- refusing to prune", flush=True)
        return 0
    pushed = set(receipt.get("datasets", []))
    # THE CUTOFF MUST RESPECT THE RECEIPT'S DATE, not just today's. The receipt proves what the
    # last SUCCESSFUL push covered; a partition created after it was never uploaded anywhere.
    # With a cutoff of (today - RETAIN) alone, an HF outage lasting longer than RETAIN_DAYS
    # would let this delete the only copy of the windows collected during the outage -- the one
    # loss this whole design exists to prevent.
    cutoff = min((date.today() - timedelta(days=RETAIN_DAYS)).isoformat(),
                 str(receipt.get("pushed_at", "0000")))
    print(f"  receipt {receipt.get('pushed_at')} | retaining {RETAIN_DAYS}d "
          f"(cutoff {cutoff})", flush=True)

    freed = 0
    removed = 0
    for ds in sorted(HEAVY):
        root = DATA / ds
        if not root.exists():
            continue
        if ds not in pushed:
            print(f"    {ds}: NOT in the receipt -- skipped", flush=True)
            continue
        for f in sorted(root.rglob("*.parquet")):
            # Partition stems are YYYY-MM-DD or YYYY-MM-DDTHHMMZ; both sort by date.
            if f.stem[:10] < cutoff:
                freed += f.stat().st_size
                f.unlink()
                removed += 1
    print(f"    pruned {removed} partitions, {freed/1e6:.1f} MB freed "
          f"(full history remains on HuggingFace)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
