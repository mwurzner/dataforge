"""Ethereum beacon attestation pool.

One row per slot: attester slots observed in the pool against attester slots that reached a
block, and the difference between them.

Operational notes:
  Counts come from SSZ bitfield populations. The highest set bit is a length delimiter and is
  stripped before counting.
  window_closed marks slots whose inclusion window was fully observed. Rows without it have
  incomplete inclusion counts by construction.
  The difference is signed. Negative values are expected and meaningful.
"""
from __future__ import annotations

import gzip
import json
import time
import urllib.request

import pandas as pd

DATASET = "e18_attestation_pool"
# Accept-Encoding is not a detail here. The pool response is 3.4 MB plain and 0.63 MB gzipped
# -- a 5.4x difference against a free public endpoint. At a 12s cadence the plain version would
HDRS = {"User-Agent": "dataforge/1.0", "Accept": "application/json",
        "Accept-Encoding": "gzip"}

BEACON_APIS = [
    ("publicnode", "https://ethereum-beacon-api.publicnode.com"),
]

# An attestation must be included within 32 slots of its own slot, so a group is only final once
# the chain is that far past it. 40 leaves margin for a block fetch we missed.
FINALISE_AFTER_SLOTS = 40

# Seconds between pool polls. NOT the 12s slot time, deliberately. An attestation lingers in the
# pool for roughly 50 slots (~10 minutes), so a 30s cadence still observes each one ~20 times --
POLL_EVERY_S = 30.0


def _get(url: str, timeout: int = 40):
    """Always (payload, error). A failed fetch must never look like an empty pool."""
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers=HDRS), timeout=timeout)
        raw = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
        return json.loads(raw), None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {str(exc)[:70]}"


def bitfield(hex_bits: str) -> tuple[int, int]:
    """Decode an SSZ bitlist to (mask, population), stripping the length delimiter."""
    if not hex_bits:
        return 0, 0
    try:
        raw = bytes.fromhex(hex_bits[2:] if hex_bits.startswith("0x") else hex_bits)
    except ValueError:
        return 0, 0
    n = int.from_bytes(raw, "little")
    if n == 0:
        return 0, 0
    mask = n & ((1 << (n.bit_length() - 1)) - 1)
    return mask, bin(mask).count("1")


class AttestationPoolTracker:
    def __init__(self) -> None:
        self.base: str | None = None
        self.groups: dict[tuple[int, str], dict] = {}
        self.included: dict[tuple[int, str], int] = {}
        self.blocks_read: set[int] = set()
        self.head_slot = 0
        self.n_polls = 0
        self.n_failed = 0
        self.n_block_fail = 0
        self.n_missed_slots = 0
        # Slots already emitted. A slot can re-enter the pool after being finalised (a late
        # aggregate arrives), which produced DUPLICATE rows for the same slot until this
        # existed -- caught by a uniqueness invariant, not by anything failing.
        self.finalised: set[int] = set()
        self.rows: list[dict] = []

    def _pick(self) -> str | None:
        if self.base:
            return self.base
        for _name, b in BEACON_APIS:
            d, _err = _get(f"{b}/eth/v1/beacon/headers/head", timeout=25)
            if isinstance(d, dict):
                self.base = b
                return b
        return None

    def poll_pool(self) -> int:
        b = self._pick()
        if not b:
            self.n_failed += 1
            return 0
        d, _err = _get(f"{b}/eth/v1/beacon/pool/attestations")
        if not isinstance(d, dict) or not isinstance(d.get("data"), list):
            self.n_failed += 1
            return 0
        self.n_polls += 1
        now = time.time()
        for a in d["data"]:
            try:
                slot = int(a["data"]["slot"])
            except (KeyError, TypeError, ValueError):
                continue
            key = (slot, a.get("committee_bits") or "")
            mask, pop = bitfield(a.get("aggregation_bits") or "")
            g = self.groups.get(key)
            if g is None:
                g = self.groups[key] = {
                    "first_seen_ts": now, "last_seen_ts": now, "n_polls_seen": 0,
                    "union_mask": 0, "max_single_pop": 0, "aggregates": set(),
                    "beacon_block_root": a["data"].get("beacon_block_root"),
                }
            g["last_seen_ts"] = now
            g["n_polls_seen"] += 1
            g["union_mask"] |= mask
            g["max_single_pop"] = max(g["max_single_pop"], pop)
            g["aggregates"].add(a.get("aggregation_bits"))
        return len(d["data"])

    def poll_blocks(self, cap: int = 8) -> int:
        """Record which attester bits actually reached a block."""
        b = self._pick()
        if not b:
            return 0
        d, _err = _get(f"{b}/eth/v1/beacon/headers/head", timeout=25)
        try:
            head = int(d["data"]["header"]["message"]["slot"])
        except Exception:
            self.n_failed += 1
            return 0
        self.head_slot = max(self.head_slot, head)
        got = 0
        for slot in range(max(head - cap, 0), head + 1):
            if slot in self.blocks_read:
                continue
            bd, berr = _get(f"{b}/eth/v1/beacon/blocks/{slot}/attestations", timeout=30)
            if not isinstance(bd, dict) or not isinstance(bd.get("data"), list):
                # A MISSED SLOT genuinely has no block, which is a fact about the chain. A fetch
                # FAILURE is a fact about us, and would overstate "never included". Different
                # things, counted differently; both mark the slot read so we do not spin.
                self.blocks_read.add(slot)
                if berr and "404" in berr:
                    self.n_missed_slots += 1
                elif berr:
                    self.n_block_fail += 1
                continue
            self.blocks_read.add(slot)
            for a in bd["data"]:
                try:
                    aslot = int(a["data"]["slot"])
                except (KeyError, TypeError, ValueError):
                    continue
                key = (aslot, a.get("committee_bits") or "")
                mask, _pop = bitfield(a.get("aggregation_bits") or "")
                self.included[key] = self.included.get(key, 0) | mask
                got += 1
        return got

    def finalise(self, force: bool = False) -> int:
        """Emit ONE ROW PER SLOT once its 32-slot inclusion window has closed.

        WHY PER SLOT, AND NOT PER COMMITTEE GROUP. The first version keyed on
        (slot, committee_bits) and matched the pool against the chain on that key. It cannot
        work: in the pool an attestation covers ONE committee, while a proposer AGGREGATES
        ACROSS committees, so the on-chain attestation carries different committee_bits and the
        keys never meet. The symptom was unmistakable -- 434 attesters seen per group and 0
        matched, with blocks holding 7 attestations against thousands of pool groups.

        Resolving it at validator level would need committee assignments (~8 MB per epoch, ~1.8
        GB/day) purely to map bit positions to validator indices. Not worth it, because the
        quantity of interest survives without them: an SSZ bitfield's POPULATION COUNT is the
        number of attester slots it represents, whatever committee those bits belong to. Summing
        per-committee unions on the pool side and per-attestation populations on the chain side
        gives two counts of the same thing, comparable directly.

        WHAT THIS COSTS, stated plainly: the result is a per-slot COUNT, not a per-validator
        verdict. It answers "how many attester slots did this node see that never reached a
        block", which is the aggregate question. It cannot say WHICH validator was left out --
        that needs the committee mapping above and is a deliberate v2.
        """
        cutoff = self.head_slot - FINALISE_AFTER_SLOTS
        slots = sorted({k[0] for k in self.groups
                        if (force or k[0] <= cutoff) and k[0] not in self.finalised})
        for slot in slots:
            keys = [k for k in self.groups if k[0] == slot]
            seen_total = 0
            polls = 0
            aggregates = 0
            first = min(self.groups[k]["first_seen_ts"] for k in keys)
            last = max(self.groups[k]["last_seen_ts"] for k in keys)
            largest = 0
            for k in keys:
                g = self.groups.pop(k)
                seen_total += bin(g["union_mask"]).count("1")
                polls += g["n_polls_seen"]
                aggregates += len(g["aggregates"])
                largest = max(largest, g["max_single_pop"])
            inc_keys = [k for k in self.included if k[0] == slot]
            inc_total = 0
            for k in inc_keys:
                inc_total += bin(self.included.pop(k)).count("1")
            self.finalised.add(slot)
            self.rows.append({
                "slot": slot,
                # FALSE means the 32-slot inclusion window had not closed when this row was
                # written, so `attesters_included` is incomplete BY CONSTRUCTION and the net
                # figure is meaningless. Filter on this before drawing any conclusion.
                # TRUE only if we actually READ every block that could have carried this
                # slot's attestations, i.e. slot+1 .. slot+32 all fall inside the range of
                # blocks this run observed. `slot <= head - 40` is necessary but NOT sufficient:
                # a run that starts mid-stream never read the blocks that included its earliest
                # pool slots, so those rows would report ~zero inclusions and a huge false gap.
                "window_closed": bool(
                    slot <= cutoff
                    and self.blocks_read
                    and slot + 1 >= min(self.blocks_read)
                    and slot + 32 <= max(self.blocks_read)),
                "n_committee_groups": len(keys),
                "first_seen_ts": first,
                "last_seen_ts": last,
                "pool_dwell_s": round(last - first, 3),
                "n_polls_seen": polls,
                "n_distinct_aggregates": aggregates,
                "attesters_seen_in_pool": seen_total,
                "attesters_included": inc_total,
                # THE MEASUREMENT. Positive means this node watched attester slots enter the pool
                # that no block ever carried. NEGATIVE is possible and is not an error: a
                # proposer can include attestations our node never held, which is itself a fact
                # about propagation worth keeping rather than clamping to zero.
                "attesters_net_never_included": seen_total - inc_total,
                "largest_single_aggregate": largest,
                "block_fetch_failures": self.n_block_fail,
                "missed_slots_seen": self.n_missed_slots,
            })
        return len(slots)

    def frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows)


if __name__ == "__main__":
    t = AttestationPoolTracker()
    print("sampling the attestation pool for ~100s...")
    t0 = time.time()
    while time.time() - t0 < 100:
        n = t.poll_pool()
        t.poll_blocks()
        print(f"  {time.time()-t0:5.0f}s  pooled {n:,}  live groups {len(t.groups):,}  "
              f"blocks read {len(t.blocks_read)}  missed slots {t.n_missed_slots}", flush=True)
        time.sleep(12)
    t.finalise(force=True)
    df = t.frame()
    print(f"\n{len(df):,} groups finalised (forced, so inclusion is UNDER-counted in this demo)")
    if len(df):
        cols = ["slot", "n_committee_groups", "attesters_seen_in_pool", "attesters_included",
                "attesters_net_never_included", "n_polls_seen"]
        print(df.sort_values("slot").tail(8)[cols].to_string(index=False))
        print(f"\n  seen {df.attesters_seen_in_pool.sum():,} | "
              f"included {df.attesters_included.sum():,} | "
              f"net never included {df.attesters_net_never_included.sum():,} | "
              f"block fetch failures {t.n_block_fail}")
