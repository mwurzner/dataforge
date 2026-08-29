"""Bitcoin block propagation across the peer network.

One row per (block, peer): when that peer announced that block to us. A second table records
per-peer connection state.

Operational notes:
  fRelay is 1: peers announce transactions, which TX_DATASET samples by txid. getdata is never
  sent, so no transaction body is ever fetched, only announcement timing.
  Dialling is bounded per pass. An unbounded loop starves the poll that collects data.
  run() closes its connections on return, so call it once for the whole window with a stop
  event rather than repeatedly with a short duration.
  Timestamps are ours and include network distance to each peer.
"""
from __future__ import annotations

import hashlib
import json
import random
import select
import socket
import struct
import time
import urllib.request

import pandas as pd

DATASET = "e21_btc_block_propagation"
PEER_DATASET = "e21_btc_p2p_peers"
FLOOR_DATASET = "e21_btc_relay_floor"
TX_DATASET = "e21_btc_tx_propagation"

# Transaction announcements arrive in the thousands per second across ~120 peers, so they are
# SAMPLED by txid rather than captured whole: a txid is tracked when its first byte-pair is
# below TX_SAMPLE_CUT, i.e. one in 64. The filter is a property of the hash, so it is uniform,
# stateless, and identical on every peer -- which is what makes a propagation curve comparable
# across peers rather than an artifact of when we started watching.
TX_SAMPLE_CUT = 4          # of 256
TX_ROW_CAP = 400_000       # hard stop; a run that hits it is reported, never silently trimmed

MAGIC = bytes.fromhex("f9beb4d9")          # mainnet
PROTOCOL_VERSION = 70016
USER_AGENT = b"/dataforge-observer:1.0/"   # honest, and identifiable to any operator who looks
MSG_TX, MSG_BLOCK = 1, 2
MSG_WITNESS_BLOCK = 0x40000002
MSG_WITNESS_TX = 0x40000001
BITNODES = "https://bitnodes.io/api/v1/snapshots/latest/"

# Dialling is sequential and blocking, so it is bounded per pass: unreachable peers are common
# (roughly 2 in 3), and an unbounded loop starves the select() that actually collects data.
CONNECTS_PER_PASS = 8
CONNECT_TIMEOUT_S = 3
REFILL_COOLDOWN_S = 300


def _checksum(b: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(b).digest()).digest()[:4]


def _msg(cmd: str, payload: bytes = b"") -> bytes:
    return (MAGIC + cmd.encode().ljust(12, b"\x00")
            + struct.pack("<I", len(payload)) + _checksum(payload) + payload)


def _varstr(b: bytes) -> bytes:
    return bytes([len(b)]) + b if len(b) < 0xfd else b"\xfd" + struct.pack("<H", len(b)) + b


def _version_payload() -> bytes:
    # fRelay = 1 at the end: ask peers TO announce transactions. It was 0 while this collector
    # measured only blocks. Announcements are the point of TX_DATASET and are not sent to a peer
    # that declined relay.
    #
    # getdata is still never sent, so no transaction BODY is requested -- only the inv, which is
    # the timing. Bandwidth is bounded by that: an inv entry is 36 bytes.
    return (struct.pack("<iQq", PROTOCOL_VERSION, 0, int(time.time()))
            + b"\x00" * 26 + b"\x00" * 26
            + struct.pack("<Q", random.getrandbits(64))
            + _varstr(USER_AGENT) + struct.pack("<i", 0) + b"\x01")


def _read_varint(b: bytes, i: int):
    if i >= len(b):
        return None, i
    n = b[i]
    if n < 0xfd:
        return n, i + 1
    if n == 0xfd:
        return struct.unpack_from("<H", b, i + 1)[0], i + 3
    if n == 0xfe:
        return struct.unpack_from("<I", b, i + 1)[0], i + 5
    return struct.unpack_from("<Q", b, i + 1)[0], i + 9


def peer_list(limit: int = 150) -> list[tuple[str, int]]:
    """Live reachable peers from the Bitnodes crawler, IPv4 only, shuffled for diversity."""
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            BITNODES, headers={"User-Agent": "dataforge/1.0"}), timeout=40)
        nodes = json.loads(r.read()).get("nodes", {})
    except Exception:
        return []
    out = []
    for addr in nodes:
        if addr.startswith("[") or addr.count(":") != 1:
            continue                       # skip IPv6 and onion
        host, _, port = addr.rpartition(":")
        try:
            out.append((host, int(port)))
        except ValueError:
            continue
    random.shuffle(out)
    return out[:limit]


class BlockPropagationCollector:
    def __init__(self, peers=None, target: int = 120) -> None:
        self.target = target
        self.peers = peers if peers is not None else peer_list(target * 2)
        self.conns: dict[tuple, socket.socket] = {}
        self.bufs: dict[tuple, bytes] = {}
        self.state: dict[tuple, dict] = {}
        self.rows: list[dict] = []
        self.floor_rows: list[dict] = []
        self.tx_rows: list[dict] = []
        self.tx_seen: set = set()
        self.n_tx_inv = 0
        self.n_tx_capped = 0
        self.seen: set[tuple] = set()
        self.n_handshakes = 0
        self.n_connect_fail = 0
        self.n_disconnect = 0
        self._last_refill = 0.0

    def _connect(self, key) -> None:
        host, port = key
        try:
            s = socket.create_connection((host, port), timeout=CONNECT_TIMEOUT_S)
            s.sendall(_msg("version", _version_payload()))
            s.setblocking(False)
            self.conns[key] = s
            self.bufs[key] = b""
            self.state[key] = {"handshook": False, "user_agent": None,
                               "services": None, "n_block_inv": 0, "connected_ts": time.time()}
        except Exception:
            self.n_connect_fail += 1

    def _drop(self, key, deliberate: bool = False) -> None:
        s = self.conns.pop(key, None)
        self.bufs.pop(key, None)
        if s is not None:
            # Closing our own connections at the end of a run is not a disconnect. Counting it
            # as one made every peer look like it had dropped us.
            if not deliberate:
                self.n_disconnect += 1
            try:
                s.close()
            except Exception:
                pass

    def _handle(self, key, cmd: str, payload: bytes) -> None:
        st = self.state.get(key)
        if st is None:
            return
        if cmd == "version":
            try:
                st["services"] = struct.unpack_from("<Q", payload, 4)[0]
                i = 4 + 8 + 8 + 26 + 26 + 8
                n, i = _read_varint(payload, i)
                st["user_agent"] = payload[i:i + n].decode("utf-8", "ignore")
            except Exception:
                pass
            try:
                self.conns[key].sendall(_msg("verack"))
            except Exception:
                self._drop(key)
        elif cmd == "verack":
            if not st["handshook"]:
                st["handshook"] = True
                self.n_handshakes += 1
                try:
                    # sendcmpct(high_bandwidth=1, version=2): ask this peer to PUSH new blocks
                    # to us rather than announce-then-wait. That is what makes the timing a
                    # propagation measurement rather than a round-trip measurement.
                    self.conns[key].sendall(
                        _msg("sendcmpct", struct.pack("<BQ", 1, 2)))
                except Exception:
                    self._drop(key)
        elif cmd == "feefilter" and len(payload) >= 8:
            # BIP133. The peer's own minimum relay fee: below this it will not even forward a
            # transaction, let alone mine it. Broadcast on connect and again whenever the floor
            # moves, stored by nobody, and it is what decides whether a cheap transaction can
            # cross the network at all. Peers send it whether or not we asked for transaction
            # relay -- measured on both settings before this was added.
            self.floor_rows.append({
                "observed_ts": time.time(),
                "peer_addr": f"{key[0]}:{key[1]}",
                "min_relay_fee_sat_kvb": struct.unpack_from("<q", payload, 0)[0],
            })
        elif cmd == "ping":
            try:
                self.conns[key].sendall(_msg("pong", payload))
            except Exception:
                self._drop(key)
        elif cmd in ("cmpctblock", "headers"):
            # Peers in BIP152 high-bandwidth mode push `cmpctblock` directly; others may
            # announce via `headers`. Both paths are handled so no announcement is missed.
            #
            # A CORRECTION TO MY OWN DIAGNOSIS, kept because the reasoning was wrong in an
            # instructive way. A 180s test produced 52 handshakes and ZERO blocks, and I
            # concluded that modern nodes had stopped announcing by inv. They have not: a
            # subsequent 11-minute run saw a block announced by 15 peers, ALL of them via inv.
            # The zero was sampling luck -- blocks arrive every ~10 minutes and a 180s window
            now = time.time()
            try:
                if cmd == "cmpctblock":
                    hdrs = [payload[:80]] if len(payload) >= 80 else []
                else:
                    n, i = _read_varint(payload, 0)
                    hdrs = []
                    for _ in range(min(n or 0, 200)):
                        if i + 80 > len(payload):
                            break
                        hdrs.append(payload[i:i + 80])
                        i += 81                      # 80-byte header + varint tx count (0)
            except Exception:
                hdrs = []
            for raw in hdrs:
                h = hashlib.sha256(hashlib.sha256(raw).digest()).digest()[::-1].hex()
                st["n_block_inv"] += 1
                k = (h, key)
                if k in self.seen:
                    continue
                self.seen.add(k)
                self.rows.append({
                    "received_ts": now,
                    "block_hash": h,
                    "peer_addr": f"{key[0]}:{key[1]}",
                    "peer_user_agent": st.get("user_agent"),
                    "peer_services": st.get("services"),
                    "peer_connected_ts": st.get("connected_ts"),
                    "announce_via": cmd,
                })
        elif cmd == "inv":
            now = time.time()
            n, i = _read_varint(payload, 0)
            if n is None:
                return
            for _ in range(min(n, 5000)):
                if i + 36 > len(payload):
                    break
                typ = struct.unpack_from("<I", payload, i)[0]
                h = payload[i + 4:i + 36][::-1].hex()      # little-endian on the wire
                i += 36
                if typ in (MSG_TX, MSG_WITNESS_TX):
                    self.n_tx_inv += 1
                    # Sampled on the hash itself, so every peer's view of the same transaction is
                    # either all in or all out. We never send getdata for these: the body is on
                    # chain once mined and is not what this measures -- the TIMING is.
                    if int(h[:2], 16) >= TX_SAMPLE_CUT:
                        continue
                    k = (h, key)
                    if k in self.tx_seen:
                        continue
                    if len(self.tx_rows) >= TX_ROW_CAP:
                        self.n_tx_capped += 1
                        continue
                    self.tx_seen.add(k)
                    self.tx_rows.append({
                        "received_ts": now,
                        "txid": h,
                        "peer_addr": f"{key[0]}:{key[1]}",
                        "peer_user_agent": st.get("user_agent"),
                    })
                    continue
                if typ in (MSG_BLOCK, MSG_WITNESS_BLOCK):
                    st["n_block_inv"] += 1
                    k = (h, key)
                    if k in self.seen:
                        continue
                    self.seen.add(k)
                    self.rows.append({
                        "received_ts": now,
                        "block_hash": h,
                        "peer_addr": f"{key[0]}:{key[1]}",
                        "peer_user_agent": st.get("user_agent"),
                        "peer_services": st.get("services"),
                        "peer_connected_ts": st.get("connected_ts"),
                        "announce_via": "inv",
                    })

    def run(self, duration_s: float, stop=None) -> int:
        """Hold peers for the WHOLE duration; `stop` ends it early without tearing down early.

        This takes a stop event rather than being called repeatedly in a short loop, because
        run() closes every connection when it returns. Calling it on a 120s cycle meant the peer
        set was demolished and rebuilt every two minutes -- 167 handshakes and 312 connect
        failures across a 200s window, with only ONE peer still attached when a block arrived.
        Propagation cannot be measured through a peer set that is constantly being rebuilt.
        """
        end = time.time() + duration_s
        pool = list(self.peers)
        while time.time() < end and not (stop is not None and stop.is_set()):
            # BOUNDED, and it must be. This loop used to dial until the target was met,
            # with a 6s socket timeout per attempt and no stop check -- so on a run where most
            # peers are unreachable it blocked for up to 120 x 6s = 12 MINUTES in a single pass
            # before returning to select(). A 90s run took over 6 minutes to exit.
            dialled = 0
            while (len(self.conns) < self.target and pool and dialled < CONNECTS_PER_PASS
                   and not (stop is not None and stop.is_set())):
                self._connect(pool.pop())
                dialled += 1
            if not self.conns:
                time.sleep(1.0)
                if not pool:
                    # Refill from the crawler, but NOT more than once every REFILL_COOLDOWN_S:
                    # peer_list() is a 40s HTTP fetch, and calling it on every exhausted pass
                    # would hammer the crawler and stall the loop.
                    if time.time() - self._last_refill >= REFILL_COOLDOWN_S:
                        self._last_refill = time.time()
                        pool = [p for p in peer_list(self.target * 3) if p not in self.state]
                    if not pool:
                        break
                continue
            rev = {s: k for k, s in self.conns.items()}
            try:
                ready, _, bad = select.select(list(self.conns.values()), [],
                                              list(self.conns.values()), 1.0)
            except Exception:
                ready, bad = [], []
            for s in bad:
                self._drop(rev.get(s))
            for s in ready:
                key = rev.get(s)
                if key is None:
                    continue
                try:
                    chunk = s.recv(65536)
                except Exception:
                    self._drop(key)
                    continue
                if not chunk:
                    self._drop(key)
                    continue
                buf = self.bufs[key] + chunk
                while len(buf) >= 24:
                    if buf[:4] != MAGIC:
                        cut = buf.find(MAGIC, 1)
                        if cut == -1:
                            buf = b""
                            break
                        buf = buf[cut:]
                        continue
                    ln = struct.unpack_from("<I", buf, 16)[0]
                    if len(buf) < 24 + ln:
                        break
                    cmd = buf[4:16].rstrip(b"\x00").decode("ascii", "ignore")
                    self._handle(key, cmd, buf[24:24 + ln])
                    buf = buf[24 + ln:]
                self.bufs[key] = buf
        for key in list(self.conns):
            self._drop(key, deliberate=True)
        return len(self.rows)

    def frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows)

    def tx_frame(self) -> pd.DataFrame:
        """One row per (sampled transaction, peer): when that peer announced it to us."""
        df = pd.DataFrame(self.tx_rows)
        if len(df):
            # Delay from the FIRST peer to announce it, which is the only reference available
            # without a trusted clock: we cannot know when it was broadcast, only when the
            # earliest peer we hold told us.
            first = df.groupby("txid").received_ts.transform("min")
            df["delay_from_first_s"] = (df.received_ts - first).round(4)
        return df

    def floor_frame(self) -> pd.DataFrame:
        """One row per feefilter received. A peer appears repeatedly as its floor moves."""
        df = pd.DataFrame(self.floor_rows)
        if len(df):
            # sat/kvB is the wire unit; sat/vB is what every fee tool quotes.
            df["min_relay_fee_sat_vb"] = (df.min_relay_fee_sat_kvb / 1000.0).round(4)
        return df

    def peer_frame(self) -> pd.DataFrame:
        return pd.DataFrame([{
            "peer_addr": f"{k[0]}:{k[1]}",
            "handshook": v["handshook"],
            "user_agent": v["user_agent"],
            "services": v["services"],
            "n_block_inv": v["n_block_inv"],
            "connected_ts": v["connected_ts"],
        } for k, v in self.state.items()])


if __name__ == "__main__":
    c = BlockPropagationCollector(target=80)
    print(f"peers from bitnodes: {len(c.peers)}; connecting (target {c.target})...")
    c.run(180)
    df, pf = c.frame(), c.peer_frame()
    print(f"\n  handshakes {c.n_handshakes} | connect fails {c.n_connect_fail} | "
          f"disconnects {c.n_disconnect}")
    print(f"  {len(df):,} block announcements across {df.block_hash.nunique() if len(df) else 0} blocks")
    if len(df):
        for h, g in df.groupby("block_hash"):
            t = g.received_ts.sort_values()
            print(f"    {h[:20]}..  {len(g)} peers | "
                  f"first->median {t.median()-t.min():.2f}s | first->last {t.max()-t.min():.2f}s")
    ua = pf[pf.handshook].user_agent.value_counts().head(5)
    print(f"\n  top peer user agents:\n{ua.to_string() if len(ua) else '   -'}")
