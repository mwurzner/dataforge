import gzip
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from src.ephemeral import e15_feeest
from src.ephemeral.e31_btc_fee_benchmark import Benchmark, PublicClient, DATASETS, table_for


class FakeClient:
    rpc_url = "https://node.example"
    esplora_url = "https://status.example"
    status_value = {"confirmed": False}
    block_hash = "block101"

    def status(self, txid):
        if isinstance(self.status_value, Exception):
            raise self.status_value
        return self.status_value

    def rpc(self, method, params=None):
        return self.block_hash


def entry(fee=.00001, descendants=1):
    return {"vsize": 200, "fees": {"base": fee, "ancestor": fee, "descendant": fee},
            "height": 100, "time": 110, "ancestorcount": 1, "descendantcount": descendants,
            "ancestorsize": 200, "descendantsize": 200*descendants,
            "depends": [], "spentby": [], "bip125-replaceable": True}


class MeasurementTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.client = FakeClient()
        self.b = Benchmark(self.root, "2026-09-14T000000Z", self.client, denominator=1)
        self.head = {"height": 100, "hash": "block100"}

    def tearDown(self):
        self.tmp.cleanup()

    def observe(self, entries, start=100, end=102):
        return self.b.observe(entries, start, end, self.head, self.head, "round")

    def test_startup_is_left_censored_and_restart_retains_origin(self):
        self.observe({"tx": entry()})
        self.assertTrue(self.b.state["active"]["tx"]["left_censored"])
        self.assertIsNone(self.b.state["active"]["tx"]["arrival_lower_ts"])
        self.b.finish()
        resumed = Benchmark(self.root, "2026-09-14T010000Z", self.client, denominator=1)
        self.assertEqual(resumed.state["active"]["tx"]["origin_run_id"], self.b.run_id)
        self.assertIn("tx", resumed.state["active"])

    def test_new_arrival_quotes_must_precede_entire_arrival_interval(self):
        self.observe({})
        self.b.state["quotes"] = [
            {"provider": "a", "source_field": "1", "quote_id": "old", "response_received_ts": 95},
            {"provider": "a", "source_field": "1", "quote_id": "future", "response_received_ts": 101},
            {"provider": "b", "source_field": "1", "quote_id": "stale", "response_received_ts": -400}]
        self.observe({"tx": entry()}, 200, 202)
        tx = self.b.state["active"]["tx"]
        self.assertFalse(tx["left_censored"])
        self.assertEqual(tx["arrival_lower_ts"], 100)
        self.assertEqual(tx["prior_quote_ids"], ["old"])

    def test_gap_makes_arrival_left_censored(self):
        self.observe({})
        result = self.observe({"tx": entry()}, 1000, 1002)
        self.assertFalse(result["continuous_with_previous"])
        self.assertTrue(self.b.state["active"]["tx"]["left_censored"])

    def test_capacity_is_recorded_and_package_updates_are_preserved(self):
        self.b.max_active = 1
        result = self.observe({"a": entry(), "b": entry()})
        self.assertEqual(result["capacity_skipped"], 1)
        self.observe({"a": entry(descendants=2)}, 200, 202)
        rows = self.b.rows[DATASETS[1]]
        self.assertEqual([r["record_kind"] for r in rows], ["initial", "package_change"])
        self.assertEqual(json.loads(rows[-1]["raw_entry_json"])["descendantcount"], 2)

    def test_absence_and_http_failure_never_mean_dropped(self):
        self.observe({"tx": entry()})
        self.client.status_value = RuntimeError("404")
        self.b.reconcile({}, self.head, 200)
        self.assertEqual(self.b.rows[DATASETS[2]][-1]["event"], "status_error")
        self.assertIn("tx", self.b.state["active"])
        self.client.status_value = {"confirmed": False}
        self.b.reconcile({}, self.head, 210)
        self.assertEqual(self.b.rows[DATASETS[2]][-1]["event"], "absent_unconfirmed")

    def test_confirmation_bounds_and_reorg(self):
        self.observe({})
        self.observe({"tx": entry()}, 200, 202)
        self.client.status_value = {"confirmed": True, "block_height": 101, "block_hash": "block101"}
        with patch("src.ephemeral.e31_btc_fee_benchmark.time.time", return_value=300):
            self.b.reconcile({}, {"height": 102}, 300)
        event = self.b.rows[DATASETS[2]][-1]
        self.assertEqual(event["event"], "confirmed")
        self.assertEqual(event["blocks_since_node_admission"], 1)
        self.assertEqual(event["delay_upper_s"], 200)
        self.assertEqual(event["delay_lower_s"], 0)
        self.client.status_value = {"confirmed": False}
        self.b.reconcile({}, {"height": 102}, 310)
        self.assertIsNone(self.b.state["active"]["tx"]["confirmation"])
        self.assertIn("confirmation_revoked", [x["event"] for x in self.b.rows[DATASETS[2]]])

    def test_canonical_mismatch_is_not_a_success(self):
        self.observe({"tx": entry()})
        self.client.status_value = {"confirmed": True, "block_height": 101, "block_hash": "orphan"}
        self.b.reconcile({}, {"height": 108}, 300)
        self.assertIsNone(self.b.state["active"]["tx"]["confirmation"])
        self.assertEqual(self.b.rows[DATASETS[2]][-1]["event"], "chain_disagreement")

    def test_six_confirmations_finish_followup_but_do_not_resample(self):
        self.observe({"tx": entry()})
        self.client.status_value = {"confirmed": True, "block_height": 101, "block_hash": "block101"}
        self.b.reconcile({}, {"height": 106}, 300)
        self.assertNotIn("tx", self.b.state["active"])
        self.observe({"tx": entry()}, 310, 312)
        self.assertNotIn("tx", self.b.state["active"])

    def test_followup_expiry_is_censored_not_failed(self):
        self.observe({"tx": entry()})
        self.b.max_age_s = 10
        self.b.reconcile({"tx": entry()}, self.head, 200)
        self.assertEqual(self.b.rows[DATASETS[2]][-1]["event"], "right_censored")
        self.assertNotIn("tx", self.b.state["active"])

    def test_schemas_are_identical_for_success_and_error_partitions(self):
        for dataset in DATASETS:
            a = table_for(dataset, [{}], "run")
            b = table_for(dataset, [{"error_type": "HTTPError", "provider": "test"}], "run")
            self.assertEqual(a.schema, b.schema)

    def test_corrupt_checkpoint_fails_instead_of_resetting(self):
        self.b.path.parent.mkdir(parents=True)
        self.b.path.write_bytes(b"invalid checkpoint")
        with self.assertRaises(Exception):
            Benchmark(self.root, "run", self.client)

    def test_rpc_disallows_spending_methods(self):
        client = PublicClient()
        with self.assertRaises(ValueError):
            client.rpc("sendrawtransaction", ["anything"])

    def test_quotes_survive_failure_of_the_second_head_read(self):
        self.client.head = unittest.mock.Mock(side_effect=[self.head, RuntimeError("node failed")])
        quotes = pd.DataFrame([{"provider": "a", "sat_per_vb": 2, "error": None,
                                "response_received_ts": 200, "source_field": "1"}])
        with patch.object(e15_feeest, "sample", return_value=quotes):
            record = self.b.tick()
        self.assertEqual(record["error_stage"], "head_after_quotes")
        files = list((self.root/DATASETS[0]).rglob("*.parquet"))
        frame = pd.read_parquet(files[0])
        self.assertEqual(len(frame), 1)
        self.assertTrue(frame.head_after_hash.isna().all())
        self.assertIsNone(self.b.state["last_snapshot"])

    def test_invalid_pool_response_preserves_previous_snapshot(self):
        self.observe({"tx": entry()})
        previous = self.b.state["last_snapshot"].copy()
        with self.assertRaises(ValueError):
            self.observe(None, 200, 202)
        self.assertEqual(previous, self.b.state["last_snapshot"])
        self.assertIn("tx", self.b.state["active"])


class QuoteTests(unittest.TestCase):
    def test_response_time_and_native_minutes(self):
        with patch.object(e15_feeest, "_get", return_value=({"halfHourFee": 2}, None)), \
             patch.object(e15_feeest.time, "time", side_effect=[100, 103]):
            row = e15_feeest._rows("mempool.space", "https://example", lambda d: [(3, 2, "halfHourFee")])[0]
        self.assertEqual(row["response_received_ts"], 103)
        self.assertEqual(row["native_target_kind"], "minutes")
        self.assertEqual(row["native_target_value"], 30)
        self.assertTrue(row["legacy_target_is_approximation"])
        self.assertEqual(json.loads(row["raw_payload_json"]), {"halfHourFee": 2})

    def test_block_target_and_errors(self):
        with patch.object(e15_feeest, "_get", return_value=({"2": 1.5}, None)):
            row = e15_feeest._rows("blockstream", "url", lambda d: [(2, 1.5, "fee-estimates[2]")])[0]
        self.assertFalse(row["legacy_target_is_approximation"])
        with patch.object(e15_feeest, "_get", return_value=(None, "HTTPError")):
            bad = e15_feeest._rows("blockstream", "url", lambda d: [])[0]
        self.assertIsNone(bad["sat_per_vb"])
        self.assertEqual(bad["error"], "HTTPError")
        self.assertIn("response_received_ts", bad)


if __name__ == "__main__":
    unittest.main()
