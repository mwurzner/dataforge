"""Never-mined transaction capture, an optional add-on to the peer collectors in e21_btc_p2p.

A transaction that is mined keeps its body on chain forever. One that is replaced, double-spent
or simply dropped leaves nothing behind: once peers evict it, no archive can produce it again.
This module asks peers for the body of every transaction they announce, watches the blocks that
follow, and at the end of the window keeps only the transactions that were NOT mined, plus the
record of which mined or announced transaction conflicted with them.

Three tables per chain, all archive-only until the outcome rules are validated:

  <prefix>_unmined_tx     one row per transaction seen in the window and not mined by its end:
                          first-seen time, raw body, and any conflict observed
  <prefix>_chain_blocks   one row per block fetched: hash, parent, header time, and two compact
                          blobs (8-byte txid prefixes, 8-byte spent-outpoint digests) so a later
                          job can decide what happened to a transaction after its window closed
  <prefix>_capture_stats  one row per window: what was asked for, received, missed, and why

Operational notes:
  No peer address is stored in any of the three tables. Which peer announced a transaction
  first is exactly what would link it to an IP address, so it never leaves this object.
  Everything here is defensive. The collector calls in through `safe()`, which counts an
  exception instead of raising it and switches the capture off for the rest of the window after
  MAX_ERRORS, so a parsing surprise can cost this add-on but never the propagation timing.
  Bodies are requested from ONE announcing peer, once, with a single retry elsewhere. That is
  the load any ordinary node puts on its peers and no more.
  A block is requested only after two distinct peers announce it. Lone announcements are mostly
  stale tips and old blocks, and fetching each of those would be megabytes of waste.
  And it is requested BLOCK_DELAY_S later, not at once. Processing a full Bitcoin block takes
  tens of milliseconds in the collector's only thread, and in the first seconds after a block
  appears that thread is timestamping the other peers' announcements of it, which is the
  measurement the propagation tables exist for. Nothing here needs the block that quickly.
  Internally every hash stays in wire byte order; hex in the output is the usual display order.
"""
from __future__ import annotations

import hashlib
import struct
import time

import pandas as pd
import pyarrow as pa

MSG_TX, MSG_BLOCK = 1, 2
MSG_WITNESS_TX = 0x40000001

RETRY_AFTER_S = 20            # a body not delivered by then is asked for once more, elsewhere
BLOCK_RETRY_AFTER_S = 90
BLOCK_DELAY_S = 20            # wait this long after the second announcement before fetching
TICK_EVERY_S = 5
GETDATA_BATCH = 500
MAX_ERRORS = 200
MAX_PENDING_BYTES = 400_000_000      # bodies held in memory; requests pause above this
MAX_RAW_OUT_BYTES = 60_000_000       # bodies written per window; rows beyond it keep no body
MAX_UNFETCHED_ROWS = 200_000
WALKBACK_S = 3 * 3600                # fetch ancestors this far before the window opened:
                                     # enough to bridge the usual gap between two windows
                                     # without re-fetching the whole previous one
WALKBACK_CAP = 600                   # and never more blocks than this
NULL_OUTPOINT = b"\x00" * 32 + b"\xff\xff\xff\xff"

# Which peers may be asked for bodies on a network that several chains share. Bitcoin Cash,
# eCash and Bitcoin SV still use one network magic, and a Bitcoin SV block can be gigabytes.
BCH_FAMILIES = ("Bitcoin Cash Node", "BCHD", "bchd", "Bitcoin Verde", "Flowee", "Knuth",
                "BCH Unlimited", "BUCash")


# Explicit column types. A window with no conflicts would otherwise write `conflict_txid` as an
# untyped all-null column, and partitions of one table would stop loading as one table.
UNMINED_SCHEMA = {
    "txid": pa.string(), "first_seen_ts": pa.float64(), "fetched_ts": pa.float64(),
    "size_bytes": pa.int64(), "n_inputs": pa.int64(), "status": pa.string(),
    "conflict_txid": pa.string(), "conflict_block": pa.string(), "raw": pa.binary(),
    "raw_dropped": pa.bool_(),
}
BLOCK_SCHEMA = {
    "block_hash": pa.string(), "prev_hash": pa.string(), "header_time": pa.int64(),
    "version": pa.int64(), "size_bytes": pa.int64(), "fetched_ts": pa.float64(),
    "via": pa.string(), "n_tx": pa.int64(), "parse_ok": pa.bool_(),
    "txid_prefixes": pa.binary(), "spend_digests": pa.binary(),
}


def _typed(rows: list[dict], schema: dict) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=list(schema))
    return df.astype({name: pd.ArrowDtype(kind) for name, kind in schema.items()})


def sha256d(b: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(b).digest()).digest()


def _varint(b: bytes, i: int) -> tuple[int, int]:
    n = b[i]
    if n < 0xfd:
        return n, i + 1
    if n == 0xfd:
        return struct.unpack_from("<H", b, i + 1)[0], i + 3
    if n == 0xfe:
        return struct.unpack_from("<I", b, i + 1)[0], i + 5
    return struct.unpack_from("<Q", b, i + 1)[0], i + 9


def parse_tx(b: bytes, i: int, whole: bool = False) -> tuple[bytes, list[bytes], int]:
    """(txid in wire order, spent outpoints, index after the transaction).

    Handles the legacy layout and the segwit one (marker 0x00, flag 0x01). The txid of a segwit
    transaction is the hash of its serialization WITHOUT the witness, so that is rebuilt here.
    Raises on anything truncated; the caller counts that.

    `whole` says the buffer holds exactly one transaction (a `tx` message). That is what makes
    Litecoin's MWEB flag (0x08) parseable without understanding the extension block: whatever
    follows the outputs is skipped and the lock time is the last four bytes.
    """
    start = i
    flag = b[i + 5] if b[i + 4] == 0 else 0
    if whole and flag & 8:
        i += 6
        body_start = i
        n_in, i = _varint(b, i)
        ins = []
        for _ in range(n_in):
            ins.append(b[i:i + 36])
            i += 36
            n, i = _varint(b, i)
            i += n + 4
        n_out, i = _varint(b, i)
        for _ in range(n_out):
            i += 8
            n, i = _varint(b, i)
            i += n
        if i + 4 > len(b):
            raise ValueError("truncated transaction")
        return sha256d(b[start:start + 4] + b[body_start:i] + b[-4:]), ins, len(b)
    segwit = flag == 1
    i += 6 if segwit else 4
    body_start = i
    n_in, i = _varint(b, i)
    ins = []
    for _ in range(n_in):
        ins.append(b[i:i + 36])
        i += 36
        n, i = _varint(b, i)
        i += n + 4
    n_out, i = _varint(b, i)
    for _ in range(n_out):
        i += 8
        n, i = _varint(b, i)
        i += n
    body_end = i
    if segwit:
        for _ in range(n_in):
            items, i = _varint(b, i)
            for _ in range(items):
                n, i = _varint(b, i)
                i += n
    i += 4
    if i > len(b):
        raise ValueError("truncated transaction")
    if segwit:
        txid = sha256d(b[start:start + 4] + b[body_start:body_end] + b[i - 4:i])
    else:
        txid = sha256d(b[start:i])
    return txid, ins, i


def _skip_auxpow(b: bytes, i: int) -> int:
    """Merged-mined blocks (Dogecoin) carry the parent chain's proof between header and body."""
    _, _, i = parse_tx(b, i)          # parent coinbase
    i += 32                           # parent block hash
    n, i = _varint(b, i)
    i += 32 * n + 4                   # coinbase merkle branch + index
    n, i = _varint(b, i)
    i += 32 * n + 4                   # chain merkle branch + index
    return i + 80                     # parent header


class TxCapture:
    def __init__(self, chain: str, segwit: bool = False, auxpow: bool = False,
                 ua_allow: tuple | None = None) -> None:
        self.chain = chain
        self.segwit = segwit
        self.auxpow = auxpow
        self.ua_allow = ua_allow
        self.enabled = True
        self.disabled_reason = None
        self.t0 = time.time()
        self.want: dict[bytes, dict] = {}        # announced, body not here yet
        self.pending: dict[bytes, dict] = {}     # body here, not mined
        self.mined: set[bytes] = set()           # seen in a block this window
        self.gaveup: dict[bytes, float] = {}     # announced, never delivered -> first-seen ts
        self.spends: dict[bytes, bytes] = {}     # outpoint -> first pending spender
        self.pending_bytes = 0
        self.blocks: dict[bytes, dict] = {}
        self.block_req: dict[bytes, dict] = {}
        self.block_announcers: dict[bytes, list] = {}
        self.block_due: dict[bytes, float] = {}  # announced twice, to be fetched at this time
        self.out: dict[tuple, list[bytes]] = {}  # peer -> getdata entries waiting to be sent
        self._last_tick = 0.0
        self.n = dict(tx_announced=0, tx_requested=0, tx_received=0, tx_unsolicited=0,
                      tx_notfound=0, tx_gaveup=0, tx_mined_seen=0, tx_mined_before_fetch=0,
                      tx_skipped_saturated=0, conflicts_seen=0, conflicts_mined=0,
                      blocks_requested=0, blocks_received=0, blocks_walkback=0,
                      blocks_parse_incomplete=0, errors=0)

    # -- the only entry point the collector uses ------------------------------------------
    def safe(self, fn, *args) -> None:
        if not self.enabled:
            return
        try:
            fn(*args)
        except Exception as exc:
            self.n["errors"] += 1
            if self.n["errors"] >= MAX_ERRORS:
                self.enabled = False
                self.disabled_reason = f"{MAX_ERRORS} errors, last: {type(exc).__name__}: {str(exc)[:80]}"

    def _allowed(self, st: dict) -> bool:
        if self.ua_allow is None:
            return True
        ua = st.get("user_agent") or ""
        return any(f in ua for f in self.ua_allow)

    def drain(self):
        """(peer, getdata payload) pairs ready to send; clears the queue."""
        if not self.out:
            return []
        msgs = []
        for key, entries in self.out.items():
            for j in range(0, len(entries), GETDATA_BATCH):
                part = entries[j:j + GETDATA_BATCH]
                n = len(part)
                head = bytes([n]) if n < 0xfd else b"\xfd" + struct.pack("<H", n)
                msgs.append((key, head + b"".join(part)))
        self.out = {}
        return msgs

    # -- transactions ---------------------------------------------------------------------
    def on_tx_inv(self, key, txid: bytes, now: float, st: dict) -> None:
        if txid in self.pending or txid in self.mined or txid in self.gaveup:
            return
        w = self.want.get(txid)
        if w is not None:
            if len(w["peers"]) < 3 and key not in w["peers"] and self._allowed(st):
                w["peers"].append(key)
            return
        self.n["tx_announced"] += 1
        if not self._allowed(st):
            return
        if self.pending_bytes > MAX_PENDING_BYTES:
            self.n["tx_skipped_saturated"] += 1
            return
        self.want[txid] = {"ts": now, "peers": [key], "asked_ts": now, "tries": 1}
        self._ask_tx(key, txid)

    def _ask_tx(self, key, txid: bytes) -> None:
        typ = MSG_WITNESS_TX if self.segwit else MSG_TX
        self.out.setdefault(key, []).append(struct.pack("<I", typ) + txid)
        self.n["tx_requested"] += 1

    def on_message(self, key, cmd: str, payload: bytes, now: float) -> None:
        if cmd == "tx":
            self._on_tx(payload, now)
        elif cmd == "block":
            self._on_block(key, payload, now)
        elif cmd == "notfound":
            self._on_notfound(key, payload, now)

    def _on_tx(self, payload: bytes, now: float) -> None:
        txid, ins, _ = parse_tx(payload, 0, whole=True)
        w = self.want.pop(txid, None)
        if w is None:
            self.n["tx_unsolicited"] += 1
            return
        self.n["tx_received"] += 1
        if txid in self.mined:
            return
        entry = {"first_seen_ts": w["ts"], "fetched_ts": now, "raw": payload, "ins": ins,
                 "status": "pending_at_window_end", "conflict_txid": None, "conflict_block": None}
        self.pending[txid] = entry
        self.pending_bytes += len(payload)
        for op in ins:
            other = self.spends.get(op)
            if other is None:
                self.spends[op] = txid
            elif other != txid:
                # Two announced transactions spending the same coin: a replacement or a
                # double-spend attempt. Both rows are kept and each names the other.
                self.n["conflicts_seen"] += 1
                if entry["conflict_txid"] is None:
                    entry["status"], entry["conflict_txid"] = "conflict_seen", other
                first = self.pending.get(other)
                if first is not None and first["conflict_txid"] is None:
                    first["status"], first["conflict_txid"] = "conflict_seen", txid

    def _on_notfound(self, key, payload: bytes, now: float) -> None:
        n, i = _varint(payload, 0)
        for _ in range(min(n, 5000)):
            if i + 36 > len(payload):
                break
            h = payload[i + 4:i + 36]
            i += 36
            w = self.want.get(h)
            if w is not None:
                self.n["tx_notfound"] += 1
                w["asked_ts"] = 0.0          # retry on the next tick
            elif h in self.block_req:
                self.block_req[h]["ts"] = 0.0

    # -- blocks ---------------------------------------------------------------------------
    def on_block_announce(self, key, block_hash_hex: str, st: dict, now: float) -> None:
        if not self._allowed(st):
            return
        h = bytes.fromhex(block_hash_hex)[::-1]
        if h in self.blocks or h in self.block_req:
            return
        who = self.block_announcers.setdefault(h, [])
        if key not in who and len(who) < 8:
            who.append(key)
        if len(who) >= 2 and h not in self.block_due:
            self.block_due[h] = now + BLOCK_DELAY_S

    def _ask_block(self, key, h: bytes, walk: bool, now: float) -> None:
        self.block_req[h] = {"key": key, "ts": now, "tries": 1, "walk": walk}
        # Without the witness flag: txids and spent coins are all that is needed from a block,
        # and the stripped form is smaller and parses as the legacy layout on every chain here.
        self.out.setdefault(key, []).append(struct.pack("<I", MSG_BLOCK) + h)
        self.n["blocks_requested"] += 1

    def _on_block(self, key, payload: bytes, now: float) -> None:
        header = payload[:80]
        h = sha256d(header)
        req = self.block_req.pop(h, None)
        if req is None or h in self.blocks:
            return
        self.block_announcers.pop(h, None)
        version = struct.unpack_from("<i", header, 0)[0]
        prev = header[4:36]
        header_time = struct.unpack_from("<I", header, 68)[0]
        row = {"block_hash": h[::-1].hex(), "prev_hash": prev[::-1].hex(),
               "header_time": header_time, "version": version, "size_bytes": len(payload),
               "fetched_ts": now, "via": "walkback" if req["walk"] else "announce",
               "n_tx": 0, "parse_ok": False, "txid_prefixes": b"", "spend_digests": b""}
        self.blocks[h] = row
        self.n["blocks_received"] += 1
        if req["walk"]:
            self.n["blocks_walkback"] += 1
        # Ancestors first, so a parsing failure below cannot break the chain of blocks.
        if (prev not in self.blocks and prev not in self.block_req and prev != b"\x00" * 32
                and header_time > self.t0 - WALKBACK_S and len(self.blocks) < WALKBACK_CAP):
            self._ask_block(key, prev, walk=True, now=now)
        try:
            i = 80
            if self.auxpow and version & 0x100:
                i = _skip_auxpow(payload, i)
            n_tx, i = _varint(payload, i)
            prefixes, digests = [], []
            for _ in range(n_tx):
                txid, ins, i = parse_tx(payload, i)
                prefixes.append(txid[::-1][:8])
                self._mined(txid, ins, h, digests)
            row["n_tx"] = n_tx
            row["parse_ok"] = i == len(payload)
            row["txid_prefixes"] = b"".join(prefixes)
            row["spend_digests"] = b"".join(digests)
        finally:
            if not row["parse_ok"]:
                self.n["blocks_parse_incomplete"] += 1

    def _mined(self, txid: bytes, ins: list, block: bytes, digests: list) -> None:
        self.mined.add(txid)
        if self.want.pop(txid, None) is not None:
            self.n["tx_mined_before_fetch"] += 1
        self.gaveup.pop(txid, None)
        mine = self.pending.pop(txid, None)
        if mine is not None:
            self.n["tx_mined_seen"] += 1
            self.pending_bytes -= len(mine["raw"])
        for op in ins:
            if op == NULL_OUTPOINT:
                continue
            digests.append(hashlib.sha256(op).digest()[:8])
            other = self.spends.get(op)
            if other is None:
                continue
            if other == txid:
                del self.spends[op]
                continue
            # A DIFFERENT transaction spending this coin has now been mined, so the one we hold
            # can never confirm. This is the row the whole module exists for.
            loser = self.pending.get(other)
            if loser is not None and loser["status"] != "conflict_mined":
                self.n["conflicts_mined"] += 1
                loser["status"] = "conflict_mined"
                loser["conflict_txid"], loser["conflict_block"] = txid, block

    # -- housekeeping ---------------------------------------------------------------------
    def tick(self, now: float, connected) -> None:
        """Retry what was not delivered. `connected` is the collector's live peer mapping."""
        if now - self._last_tick < TICK_EVERY_S:
            return
        self._last_tick = now
        for h in [b for b, due in self.block_due.items() if now >= due]:
            del self.block_due[h]
            if h in self.blocks or h in self.block_req:
                continue
            peer = next((p for p in self.block_announcers.get(h, []) if p in connected), None)
            if peer is not None:
                self._ask_block(peer, h, walk=False, now=now)
        for txid in [t for t, w in self.want.items() if now - w["asked_ts"] > RETRY_AFTER_S]:
            w = self.want[txid]
            nxt = next((p for p in w["peers"][w["tries"]:] if p in connected), None)
            if nxt is None or w["tries"] >= 2:
                del self.want[txid]
                self.n["tx_gaveup"] += 1
                if len(self.gaveup) < MAX_UNFETCHED_ROWS:
                    self.gaveup[txid] = w["ts"]
                continue
            w["tries"] += 1
            w["asked_ts"] = now
            self._ask_tx(nxt, txid)
        for h in [b for b, r in self.block_req.items() if now - r["ts"] > BLOCK_RETRY_AFTER_S]:
            r = self.block_req.pop(h)
            if r["tries"] >= 3:
                continue
            others = [p for p in self.block_announcers.get(h, []) if p in connected and p != r["key"]]
            nxt = others[0] if others else (r["key"] if r["key"] in connected else None)
            if nxt is None:
                nxt = next(iter(connected), None) if r["walk"] else None
            if nxt is None:
                continue
            self._ask_block(nxt, h, walk=r["walk"], now=now)
            self.block_req[h]["tries"] = r["tries"] + 1

    # -- output ---------------------------------------------------------------------------
    def unmined_frame(self) -> pd.DataFrame:
        order = {"conflict_mined": 0, "conflict_seen": 1, "pending_at_window_end": 2}
        rows, budget = [], MAX_RAW_OUT_BYTES
        for txid, e in sorted(self.pending.items(), key=lambda kv: (order[kv[1]["status"]], kv[1]["first_seen_ts"])):
            keep = len(e["raw"]) <= budget
            if keep:
                budget -= len(e["raw"])
            rows.append({
                "txid": txid[::-1].hex(), "first_seen_ts": e["first_seen_ts"],
                "fetched_ts": e["fetched_ts"], "size_bytes": len(e["raw"]), "n_inputs": len(e["ins"]),
                "status": e["status"],
                "conflict_txid": e["conflict_txid"][::-1].hex() if e["conflict_txid"] else None,
                "conflict_block": e["conflict_block"][::-1].hex() if e["conflict_block"] else None,
                "raw": e["raw"] if keep else None, "raw_dropped": not keep,
            })
        for txid, ts in self.gaveup.items():
            rows.append({"txid": txid[::-1].hex(), "first_seen_ts": ts, "fetched_ts": None,
                         "size_bytes": None, "n_inputs": None, "status": "body_not_delivered",
                         "conflict_txid": None, "conflict_block": None, "raw": None,
                         "raw_dropped": False})
        return _typed(rows, UNMINED_SCHEMA)

    def blocks_frame(self) -> pd.DataFrame:
        return _typed(sorted(self.blocks.values(), key=lambda r: r["header_time"]), BLOCK_SCHEMA)

    def stats_frame(self) -> pd.DataFrame:
        row = dict(self.n)
        row.update(capture_started_ts=self.t0, enabled_at_end=self.enabled,
                   disabled_reason=self.disabled_reason, pending_at_end=len(self.pending),
                   still_wanted_at_end=len(self.want), pending_bytes_at_end=self.pending_bytes,
                   earliest_block_time=min((b["header_time"] for b in self.blocks.values()), default=None),
                   latest_block_time=max((b["header_time"] for b in self.blocks.values()), default=None))
        schema = {name: pa.int64() for name in row}
        schema.update(capture_started_ts=pa.float64(), enabled_at_end=pa.bool_(),
                      disabled_reason=pa.string())
        return _typed([row], schema)
