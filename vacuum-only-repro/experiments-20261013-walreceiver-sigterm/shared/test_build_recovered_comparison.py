import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("build-recovered-comparison.py")
SPEC = importlib.util.spec_from_file_location("build_recovered_comparison", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def sample(at, query_suffix="00", committed=10):
    backends = []
    for pid in range(10, 20):
        backends.append({
            "pid": pid, "backendStartUtc": "2026-10-13T05:00:00Z",
            "queryStartUtc": f"2026-10-13T05:01:{query_suffix}",
            "xactStartUtc": f"2026-10-13T05:01:{query_suffix}",
            "state": "active", "waitEventType": "IPC", "waitEvent": "SyncRep",
        })
    return {"sampleStatus": "ok", "observedAtUtc": at, "backends": backends,
            "committedTransactions": committed}


class RecoveredComparisonTests(unittest.TestCase):
    def write_rows(self, rows):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        path = Path(temp.name) / "samples.jsonl"
        path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        return path

    def test_stable_same_identity_streak_is_measured(self):
        rows = [sample(f"2026-10-13T05:01:{second:02d}Z") for second in (0, 1, 2, 3)]
        result = MODULE.measure_writer_streak(
            self.write_rows(rows), MODULE.parse_time("2026-10-13T05:01:00Z"),
            MODULE.parse_time("2026-10-13T05:01:03Z"))
        self.assertEqual(result["qualifyingSamples"], 4)
        self.assertEqual(result["spanSeconds"], 3.0)
        self.assertTrue(result["sameTenBackendQueryAndTransactionStarts"])

    def test_changed_identity_breaks_longer_claim(self):
        rows = [sample("2026-10-13T05:01:00Z"), sample("2026-10-13T05:01:01Z", query_suffix="01Z")]
        with self.assertRaisesRegex(ValueError, "no continuously sampled"):
            MODULE.measure_writer_streak(
                self.write_rows(rows), MODULE.parse_time("2026-10-13T05:01:00Z"),
                MODULE.parse_time("2026-10-13T05:01:01Z"))

    def test_gap_breaks_streak_and_reset_is_required(self):
        rows = [sample("2026-10-13T05:01:00Z"), sample("2026-10-13T05:01:04Z")]
        with self.assertRaisesRegex(ValueError, "no continuously sampled"):
            MODULE.measure_writer_streak(
                self.write_rows(rows), MODULE.parse_time("2026-10-13T05:01:00Z"),
                MODULE.parse_time("2026-10-13T05:01:04Z"))

    def test_real_artifacts_build_separate_incomplete_result(self):
        root = MODULE_PATH.parents[1]
        original = root / "sigterm/results/measurement/timing.json"
        before = MODULE.sha256(original)
        result = MODULE.build(root)
        after = MODULE.sha256(original)
        self.assertEqual(before, after)
        self.assertGreater(result["incidentClaim"]["durationSeconds"], 7200)
        self.assertFalse(result["originalHarness"]["treatmentTestComplete"])
        self.assertEqual(result["archiveRecovery"]["treatmentRestoreEventsAfterSignal"], 2054)
        self.assertEqual(result["control"]["clientServerCommitDifference"], 0)
        serialized = json.dumps(result)
        self.assertNotIn("subscriptionId", serialized)
        self.assertNotIn("resourceId", serialized)
        self.assertNotIn('"pid"', serialized)


if __name__ == "__main__":
    unittest.main()
