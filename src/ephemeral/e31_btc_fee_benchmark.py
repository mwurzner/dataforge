"""Persistent observational Bitcoin fee benchmark. Never signs or broadcasts transactions.

Raw quotes, node-local admission metadata, interval-censored arrivals, package changes,
canonical confirmation checks and failures are retained separately. A public transaction
matching a recommendation is not evidence that its sender followed that recommendation.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from src.ephemeral import e15_feeest

DATASETS = ("e31_btc_fee_quotes", "e31_btc_tx_observations", "e31_btc_tx_events",
            "e31_btc_collection_rounds")
SCHEMA_VERSION = 1

# Fixed Arrow schemas, including error-only rounds and all-null outcome columns.
FIELDS = {
    DATASETS[0]: {
        "str": "quote_id round_id provider source_field source_url raw_payload_json source_fee_unit native_target_kind error head_before_hash head_after_hash observer_rpc",
        "float": "sampled_ts request_started_ts response_received_ts target_blocks sat_per_vb latency_s native_target_value spread_ratio n_answered",
        "int": "head_before_height head_after_height",
        "bool": "legacy_target_is_approximation head_stable_during_quotes"},
    DATASETS[1]: {
        "str": "txid source round_id record_kind raw_entry_json prior_quote_ids_json origin_run_id origin_round_id",
        "float": "observed_ts request_started_ts first_observed_ts arrival_lower_ts arrival_upper_ts node_admission_ts fee_rate_sat_vb",
        "int": "fee_sat vsize sampling_denominator node_admission_height",
        "bool": "left_censored initial_head_stable"},
    DATASETS[2]: {
        "str": "txid origin_run_id origin_round_id event reason previous_confirmation_json status_json rpc_block_hash block_hash evidence error_type confirmation_json",
        "float": "observed_ts block_timestamp confirmation_lower_ts confirmation_upper_ts delay_lower_s delay_upper_s last_pending_observed_ts",
        "int": "schema_version block_height blocks_since_node_admission",
        "bool": "observation_complete"},
    DATASETS[3]: {
        "str": "round_id rpc_source status_source error_type error_stage head_hash mempool_info_json mempool_info_error",
        "float": "started_ts finished_ts snapshot_started_ts snapshot_received_ts previous_snapshot_gap_s",
        "int": "sampling_denominator schema_version pool_count hash_selected_count admitted_count capacity_skipped head_height active_count quote_count successful_quote_providers",
        "bool": "continuous_with_previous snapshot_head_stable"},
}


def table_for(dataset, rows, run_id):
    types = {"str": pa.string(), "float": pa.float64(), "int": pa.int64(), "bool": pa.bool_()}
    schema = pa.schema([pa.field("run_id", pa.string())] + [pa.field(name, types[kind])
        for kind, names in FIELDS[dataset].items() for name in names.split()])
    return pa.Table.from_pylist([{"run_id": run_id, **json_safe(row)} for row in rows], schema=schema)


def selected(txid: str, denominator: int = 256) -> bool:
    # Fixed Bernoulli sample, independent of fee and eventual outcome. No survivor resampling.
    return int(hashlib.sha256(("dataforge-fee-v1:" + txid).encode()).hexdigest(), 16) % denominator == 0


def json_safe(value):
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [json_safe(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def atomic_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump(json_safe(value), f, separators=(",", ":"), allow_nan=False)
    os.replace(tmp, path)


class PublicClient:
    """Only the read RPC methods used by this collector are permitted."""
    METHODS = {"getbestblockhash", "getblockheader", "getblockhash", "getrawmempool", "getmempoolinfo"}

    def __init__(self, rpc_url="https://bitcoin-rpc.publicnode.com",
                 esplora_url="https://mempool.space/api"):
        self.rpc_url, self.esplora_url = rpc_url, esplora_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "dataforge-fee-benchmark/1.0"})

    def rpc(self, method, params=None):
        if method not in self.METHODS:
            raise ValueError("Only read methods are permitted")
        r = self.session.post(self.rpc_url, json={"jsonrpc": "2.0", "id": "dataforge",
                              "method": method, "params": params or []}, timeout=(10, 30))
        r.raise_for_status()
        body = r.json()
        if body.get("error"):
            raise RuntimeError("RPC error: " + str(body["error"].get("code")))
        return body["result"]

    def head(self):
        block_hash = self.rpc("getbestblockhash")
        header = self.rpc("getblockheader", [block_hash])
        return {"height": int(header["height"]), "hash": block_hash,
                "observed_ts": time.time(), "block_timestamp": header.get("time")}

    def status(self, txid):
        r = self.session.get(f"{self.esplora_url}/tx/{txid}/status", timeout=(10, 15))
        r.raise_for_status()
        return r.json()


class Benchmark:
    def __init__(self, root: Path, run_id: str, client=None, denominator=256,
                 max_active=10000, max_age_s=7*86400, status_budget=16):
        if denominator < 1 or max_active < 1 or status_budget < 1:
            raise ValueError("Sampling limits must be positive")
        self.root, self.run_id = Path(root), run_id
        self.client = client or PublicClient()
        self.denominator, self.max_active = denominator, max_active
        self.max_age_s, self.status_budget = max_age_s, status_budget
        self.path = self.root / "fee_benchmark_state" / "state.json.gz"
        if self.path.exists():
            with gzip.open(self.path, "rt", encoding="utf-8") as f:
                self.state = json.load(f)
            if self.state["schema_version"] != SCHEMA_VERSION:
                raise ValueError("Unsupported checkpoint schema; refusing to reset observations")
        else:
            self.state = {"schema_version": SCHEMA_VERSION, "active": {}, "seen": {},
                          "last_snapshot": None, "previous_pool": [], "quotes": [], "tick": 0}
        self.rows = {name: [] for name in DATASETS}
        self.successful_rounds = 0

    def event(self, txid, kind, ts, **fields):
        tx = self.state["active"].get(txid, {})
        self.rows[DATASETS[2]].append({"txid": txid, "event": kind,
            "origin_run_id": tx.get("origin_run_id"), "origin_round_id": tx.get("origin_round_id"),
            "observed_ts": ts, "schema_version": SCHEMA_VERSION, **fields})

    def observe(self, entries, started, received, before, after, round_id):
        if not isinstance(entries, dict):
            raise ValueError("Invalid mempool response; must not be interpreted as an empty pool")
        previous = self.state["last_snapshot"]
        source = self.client.rpc_url
        continuous = bool(previous and previous["source"] == source
                          and 0 <= started - previous["received"] <= 300)
        old = set(self.state["previous_pool"]) if continuous else set()
        stable_head = before["hash"] == after["hash"]
        selected_count = skipped = admitted = 0
        for txid, entry in entries.items():
            if not selected(txid, self.denominator):
                continue
            selected_count += 1
            if txid not in self.state["active"]:
                if txid in self.state["seen"]:
                    continue
                if len(self.state["active"]) >= self.max_active:
                    skipped += 1
                    continue
                # A transaction present at start/restart is left-censored, never a new arrival.
                left_censored = not continuous or txid in old
                lower = previous["started"] if not left_censored else None
                earlier = [q for q in self.state["quotes"]
                           if lower is not None and not q.get("error")
                           and q.get("response_received_ts") is not None
                           and 0 <= lower-q["response_received_ts"] <= 300]
                by_source = {}
                for q in sorted(earlier, key=lambda q: q["response_received_ts"]):
                    by_source[(q["provider"], q.get("source_field"))] = q["quote_id"]
                tx = {"first_observed_ts": received, "arrival_lower_ts": lower,
                      "arrival_upper_ts": received, "left_censored": left_censored,
                      "node_admission_ts": entry.get("time"),
                      "node_admission_height": entry.get("height"),
                      "origin_run_id": self.run_id, "origin_round_id": round_id,
                      "source": source, "last_status_check_ts": 0,
                      "last_unconfirmed_request_ts": started,
                      "last_pending_observed_ts": received, "confirmation": None,
                      "package": None, "initial_head_stable": stable_head,
                      "prior_quote_ids": list(by_source.values())}
                self.state["active"][txid] = tx
                self.state["seen"][txid] = received
                admitted += 1
            tx = self.state["active"][txid]
            if tx["confirmation"]:
                self.event(txid, "reappeared_pending", received,
                           previous_confirmation_json=json.dumps(tx["confirmation"]))
                tx["confirmation"] = None
            tx["last_unconfirmed_request_ts"] = started
            tx["last_pending_observed_ts"] = received
            package = {k: entry.get(k) for k in ("vsize", "fees", "ancestorcount",
                       "descendantcount", "ancestorsize", "descendantsize", "depends",
                       "spentby", "bip125-replaceable")}
            if package != tx["package"]:
                fee = (entry.get("fees") or {}).get("base")
                vsize = entry.get("vsize")
                self.rows[DATASETS[1]].append({"txid": txid, "observed_ts": received,
                    "request_started_ts": started, "source": source,
                    "round_id": round_id, "record_kind": "initial" if tx["package"] is None else "package_change",
                    "fee_sat": round(float(fee)*1e8) if fee is not None else None,
                    "vsize": vsize, "fee_rate_sat_vb": float(fee)*1e8/vsize if fee is not None and vsize else None,
                    "raw_entry_json": json.dumps(entry, separators=(",", ":")),
                    "sampling_denominator": self.denominator,
                    "prior_quote_ids_json": json.dumps(tx["prior_quote_ids"]),
                    **{k: tx[k] for k in ("origin_run_id", "origin_round_id", "first_observed_ts",
                       "arrival_lower_ts", "arrival_upper_ts", "left_censored", "node_admission_ts",
                       "node_admission_height", "initial_head_stable")}})
                tx["package"] = package
        self.state["last_snapshot"] = {"started": started, "received": received, "source": source,
                                        "height": after["height"], "hash": after["hash"]}
        self.state["previous_pool"] = list(entries)
        return {"pool_count": len(entries), "hash_selected_count": selected_count,
                "admitted_count": admitted, "capacity_skipped": skipped,
                "continuous_with_previous": continuous, "snapshot_head_stable": stable_head,
                "previous_snapshot_gap_s": started-previous["received"] if previous else None}

    def reconcile(self, entries, head, now):
        active = self.state["active"]
        candidates = sorted((t for t in active if t not in entries or active[t]["confirmation"]),
                            key=lambda t: active[t]["last_status_check_ts"])
        hash_cache = {}
        for txid in candidates[:self.status_budget]:
            tx = active[txid]
            checked_at = time.time()
            tx["last_status_check_ts"] = checked_at
            try:
                status = self.client.status(txid)
                observed = time.time()
                if status.get("confirmed") is True:
                    height, block_hash = int(status["block_height"]), status["block_hash"]
                    if height not in hash_cache:
                        hash_cache[height] = self.client.rpc("getblockhash", [height])
                    canonical = hash_cache[height] == block_hash
                    if not canonical or height > head["height"]:
                        self.event(txid, "chain_disagreement", observed,
                                   status_json=json.dumps(status), rpc_block_hash=hash_cache[height])
                        continue
                    previous = tx["confirmation"]
                    if not previous or previous["block_hash"] != block_hash:
                        if previous:
                            self.event(txid, "confirmation_changed", observed,
                                       previous_confirmation_json=json.dumps(previous))
                        lower = tx["arrival_lower_ts"]
                        self.event(txid, "confirmed", observed, block_hash=block_hash,
                            block_height=height, block_timestamp=status.get("block_time"),
                            confirmation_lower_ts=tx["last_unconfirmed_request_ts"],
                            confirmation_upper_ts=observed,
                            delay_lower_s=max(0, tx["last_unconfirmed_request_ts"]-tx["arrival_upper_ts"])
                                if lower is not None else None,
                            delay_upper_s=observed-lower if lower is not None else None,
                            blocks_since_node_admission=height-tx["node_admission_height"]
                                if tx["node_admission_height"] is not None else None,
                            evidence="esplora_status_and_rpc_canonical_hash")
                        tx["confirmation"] = {"height": height, "block_hash": block_hash}
                    if head["height"]-height+1 >= 6:
                        self.event(txid, "six_confirmations", observed, block_height=height,
                                   block_hash=block_hash, observation_complete=True)
                        del active[txid]
                elif status.get("confirmed") is False:
                    if tx["confirmation"]:
                        self.event(txid, "confirmation_revoked", observed,
                                   previous_confirmation_json=json.dumps(tx["confirmation"]))
                        tx["confirmation"] = None
                    self.event(txid, "absent_unconfirmed", observed,
                               reason="Absent from sampled node; replacement, eviction and other-node presence unresolved")
                else:
                    raise ValueError("Invalid transaction status")
            except Exception as exc:
                # Never convert a 404, 429, timeout or node disagreement into a dropped tx.
                self.event(txid, "status_error", time.time(), error_type=type(exc).__name__)
        for txid, tx in list(active.items()):
            if now-tx["first_observed_ts"] > self.max_age_s:
                self.event(txid, "right_censored", now, reason="followup_limit",
                           last_pending_observed_ts=tx["last_pending_observed_ts"],
                           confirmation_json=json.dumps(tx["confirmation"]))
                del active[txid]
        # Retain sampled identifiers for 30 days; re-enrolment beyond that is documented.
        self.state["seen"] = {t: ts for t, ts in self.state["seen"].items()
                              if t in active or now-ts <= 30*86400}

    def tick(self):
        self.state["tick"] += 1
        round_id = f"{self.run_id}-{self.state['tick']:06d}"
        start = time.time()
        record = {"round_id": round_id, "started_ts": start,
                  "rpc_source": self.client.rpc_url, "status_source": self.client.esplora_url,
                  "sampling_denominator": self.denominator, "schema_version": SCHEMA_VERSION,
                  "error_type": None}
        stage = "head_before_quotes"
        try:
            before = self.client.head()
            stage = "quotes"
            quotes = json_safe(e15_feeest.sample().to_dict("records"))
            for i, q in enumerate(quotes):
                q.update({"quote_id": f"{round_id}-{i}", "round_id": round_id,
                          "head_before_height": before["height"], "head_before_hash": before["hash"],
                          "head_after_height": None, "head_after_hash": None,
                          "head_stable_during_quotes": None,
                          "observer_rpc": self.client.rpc_url})
            self.rows[DATASETS[0]].extend(quotes)
            self.state["quotes"].extend(quotes)
            record["quote_count"] = len(quotes)
            record["successful_quote_providers"] = len({q["provider"] for q in quotes
                if not q.get("error") and q.get("sat_per_vb") is not None})
            stage = "head_after_quotes"
            after_quotes = self.client.head()
            for q in quotes:
                q.update({"head_after_height": after_quotes["height"], "head_after_hash": after_quotes["hash"],
                          "head_stable_during_quotes": before["hash"] == after_quotes["hash"]})
            self.state["quotes"] = [q for q in self.state["quotes"]
                if q.get("response_received_ts", 0) >= start-1800]
            stage = "mempool_snapshot"
            request_start = time.time()
            entries = self.client.rpc("getrawmempool", [True])
            received = time.time()
            after = self.client.head()
            record.update(self.observe(entries, request_start, received, after_quotes, after, round_id))
            record.update({"snapshot_started_ts": request_start, "snapshot_received_ts": received,
                           "head_height": after["height"], "head_hash": after["hash"]})
            try:
                record["mempool_info_json"] = json.dumps(self.client.rpc("getmempoolinfo"), separators=(",", ":"))
            except Exception as exc:
                record["mempool_info_error"] = type(exc).__name__
            stage = "reconcile"
            self.reconcile(entries, after, received)
            if record["successful_quote_providers"] >= 2:
                self.successful_rounds += 1
            else:
                record.update(error_type="InsufficientQuoteProviders", error_stage="quotes")
        except Exception as exc:
            record["error_type"] = type(exc).__name__
            record["error_stage"] = stage
        record.update({"finished_ts": time.time(), "active_count": len(self.state["active"])})
        self.rows[DATASETS[3]].append(record)
        self.checkpoint(round_id)
        print(f"E31 round {round_id}: active={record['active_count']} "
              f"admitted={record.get('admitted_count', 0)} error={record['error_type']}", flush=True)
        return record

    def checkpoint(self, partition_id):
        # Unique per-round partitions bound memory and survive abrupt runner termination.
        # Write observations before state: a crash may replay events, never skip unwritten ones.
        for dataset, rows in self.rows.items():
            if not rows:
                continue
            directory = self.root / dataset / self.run_id[:4] / self.run_id[5:7]
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / f"{partition_id}.parquet"
            temp = path.with_suffix(".parquet.tmp")
            pq.write_table(table_for(dataset, rows, self.run_id), temp)
            os.replace(temp, path)
        atomic_json(self.path, self.state)
        self.rows = {name: [] for name in DATASETS}

    def finish(self):
        now = time.time()
        for txid, tx in self.state["active"].items():
            self.event(txid, "window_end", now, reason="followup_continues_next_run",
                       last_pending_observed_ts=tx["last_pending_observed_ts"],
                       confirmation_json=json.dumps(tx["confirmation"]))
        self.checkpoint(f"{self.run_id}-end")
