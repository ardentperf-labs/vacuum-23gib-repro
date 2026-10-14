import datetime as dt
import importlib.util
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
spec = importlib.util.spec_from_file_location("stable_writer_evidence", HERE / "stable-writer-evidence.py")
evidence = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = evidence
spec.loader.exec_module(evidence)
runner_spec = importlib.util.spec_from_file_location("test_runner_for_activity_sql", HERE / "03-test.py")
runner = importlib.util.module_from_spec(runner_spec)
sys.modules[runner_spec.name] = runner
runner_spec.loader.exec_module(runner)

BASE = 1_800_000_000.0


def utc(epoch):
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def sample(epoch, *, query_start=None, xact_start=None, direct=True, backend_count=10):
    backends = []
    for pid in range(1001, 1001 + backend_count):
        backend = {"pid": pid, "state": "active", "waitEventType": "IPC", "waitEvent": "SyncRep"}
        if direct:
            backend.update({"backendStartUtc": utc(BASE - 500),
                            "queryStartUtc": utc(query_start if query_start is not None else BASE - 100),
                            "xactStartUtc": utc(xact_start if xact_start is not None else BASE - 100)})
        else:
            backend.update({"queryAgeSeconds": epoch - (query_start if query_start is not None else BASE - 100),
                            "xactAgeSeconds": epoch - (xact_start if xact_start is not None else BASE - 100)})
        backends.append(backend)
    return {"observedAtUtc": utc(epoch), "sampleEpoch": epoch, "sampleStatus": "ok",
            "benchSessions": 10, "backends": backends}


def analyze(samples, transactions=(), **kwargs):
    return evidence.analyze_stable_writers(
        samples, transactions, BASE, BASE + 9000,
        native_log_clean=True, native_counts_reconciled=True, **kwargs)


class StableWriterEvidenceTests(unittest.TestCase):
    def test_future_activity_sampler_exports_direct_backend_query_and_transaction_starts(self):
        query = runner.client_activity_query("vacuum-pgbench-test")
        self.assertIn("'backendStartUtc',to_char(backend_start AT TIME ZONE 'UTC'", query)
        self.assertIn("'queryStartUtc',to_char(query_start AT TIME ZONE 'UTC'", query)
        self.assertIn("'xactStartUtc',to_char(xact_start AT TIME ZONE 'UTC'", query)

    def test_stable_ten_writers_bridge_reported_sampler_error_without_inventing_wait_continuity(self):
        rows = [sample(BASE),
                {"sampleEpoch": BASE + 3600, "sampleStatus": "error", "error": "temporary transport reset"},
                sample(BASE + 7210)]
        result = analyze(rows)
        detail = result["maxStableAllWritersInFlightWithoutCompletion"]
        self.assertTrue(result["claimValid"], result)
        self.assertEqual(result["maxStableAllWritersInFlightWithoutCompletionSeconds"], 7210)
        self.assertEqual(detail["writerCount"], 10)
        self.assertEqual(len(detail["writers"]), 10)
        self.assertEqual(detail["nativeCompletionCountWithin"], 0)
        self.assertEqual(len(detail["sampleErrorsInSpan"]), 1)
        self.assertGreater(detail["maximumSuccessfulSampleGapSeconds"], 7200)
        self.assertIn("no wait-event state is inferred", detail["waitStateInterpretation"])
        self.assertEqual(detail["observedWaitEventCounts"], {"IPC/SyncRep": 20})

    def test_pid_query_or_transaction_identity_change_breaks_candidate(self):
        changed_query = sample(BASE + 7210, query_start=BASE - 99)
        result = analyze([sample(BASE), changed_query])
        self.assertFalse(result["claimValid"])
        self.assertLess(result["maxStableAllWritersInFlightWithoutCompletionSeconds"], 7200)

        changed_pid = sample(BASE + 7210)
        changed_pid["backends"][0]["pid"] = 9999
        result = analyze([sample(BASE), changed_pid])
        self.assertFalse(result["claimValid"])
        self.assertLess(result["maxStableAllWritersInFlightWithoutCompletionSeconds"], 7200)

        changed_xact = sample(BASE + 7210, xact_start=BASE - 98)
        result = analyze([sample(BASE), changed_xact])
        self.assertFalse(result["claimValid"])
        self.assertLess(result["maxStableAllWritersInFlightWithoutCompletionSeconds"], 7200)

    def test_native_completion_inside_gap_splits_the_stable_inflight_interval(self):
        result = analyze([sample(BASE), sample(BASE + 8000)],
                         transactions=[{"completedAtEpoch": BASE + 4000}])
        self.assertFalse(result["claimValid"])
        self.assertLess(result["maxStableAllWritersInFlightWithoutCompletionSeconds"], 7200)

    def test_invalid_successful_observation_breaks_claim_instead_of_being_bridged(self):
        invalid = sample(BASE + 3600, backend_count=9)
        result = analyze([sample(BASE), invalid, sample(BASE + 7210)])
        self.assertFalse(result["claimValid"])
        self.assertEqual(len(result["invalidSuccessfulSamplesInWindow"]), 1)
        self.assertLess(result["maxStableAllWritersInFlightWithoutCompletionSeconds"], 7200)

    def test_native_log_validation_is_required_even_when_identity_span_exceeds_two_hours(self):
        result = evidence.analyze_stable_writers([sample(BASE), sample(BASE + 7210)], [], BASE, BASE + 9000,
            native_log_clean=False, native_counts_reconciled=True)
        self.assertEqual(result["maxStableAllWritersInFlightWithoutCompletionSeconds"], 7210)
        self.assertFalse(result["claimValid"])
        self.assertFalse(result["nativeLogValidation"]["nativeLogClean"])

        result = evidence.analyze_stable_writers([sample(BASE), sample(BASE + 7210)], [], BASE, BASE + 9000,
            native_log_clean=True, native_counts_reconciled=False)
        self.assertFalse(result["claimValid"])
        self.assertFalse(result["nativeLogValidation"]["nativeCountsReconciled"])

    def test_reconstructed_age_timestamps_are_explicitly_tolerance_bounded(self):
        rows = [sample(BASE, direct=False), sample(BASE + 7210, direct=False)]
        result = analyze(rows)
        detail = result["maxStableAllWritersInFlightWithoutCompletion"]
        self.assertTrue(result["claimValid"])
        self.assertEqual(detail["timestampMethod"], "reconstructed_from_age")
        self.assertEqual(detail["reconstructedStartToleranceSeconds"], 0.01)

        drifted = sample(BASE + 7210, direct=False, query_start=BASE - 99.7)
        result = analyze([sample(BASE, direct=False), drifted])
        self.assertFalse(result["claimValid"])
        self.assertLess(result["maxStableAllWritersInFlightWithoutCompletionSeconds"], 7200)

    def test_reconstruction_tolerance_cannot_be_silently_widened(self):
        with self.assertRaisesRegex(ValueError, "between 0 and 0.5"):
            analyze([sample(BASE, direct=False)], reconstructed_start_tolerance_seconds=0.75)


if __name__ == "__main__":
    unittest.main()
