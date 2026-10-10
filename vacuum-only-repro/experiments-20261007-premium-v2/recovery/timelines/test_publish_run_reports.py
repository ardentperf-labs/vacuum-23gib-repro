import importlib.util
import json
import sys
import tempfile
from unittest import mock
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("publish_run_reports.py")
SPEC = importlib.util.spec_from_file_location("publish_run_reports_tested", MODULE_PATH)
publisher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publisher)


class CompletionGateTests(unittest.TestCase):
    def test_all_five_conditions_are_required(self):
        process = {"exitCode": 0, "status": "complete"}
        timing = {"testComplete": True}
        summary = {"status": {"testComplete": True}, "coverage": {"valid": True}}
        complete, checks = publisher.completion_state(process, timing, summary)
        self.assertTrue(complete)
        self.assertTrue(all(checks.values()))

        variants = [
            ({"exitCode": 1}, timing, summary),
            ({"exitCode": 0, "status": "running"}, timing, summary),
            (process, {"testComplete": False}, summary),
            (process, timing, {"status": {"testComplete": False}, "coverage": {"valid": True}}),
            (process, timing, {"status": {"testComplete": True}, "coverage": {"valid": False}}),
        ]
        for changed_process, changed_timing, changed_summary in variants:
            with self.subTest(process=changed_process, timing=changed_timing, summary=changed_summary):
                complete, _ = publisher.completion_state(changed_process, changed_timing, changed_summary)
                self.assertFalse(complete)


class PhaseWindowRatioTests(unittest.TestCase):
    def test_phase_table_rate_ratios_use_rates_with_unequal_coverage(self):
        summary = {"windows": {"vacuum": {
            "primary_data": {"coveredSeconds": 120.0, "readIops": 2.0,
                              "readMBps": 4.0, "readAwaitMs": 1.0},
            "replica_data": {"coveredSeconds": 60.0, "readIops": 10.0,
                              "readMBps": 12.0, "readAwaitMs": 2.0},
        }}}

        def fake_read(path, default=None):
            return summary if path.name == "summary.json" else default

        with mock.patch.object(publisher, "read", side_effect=fake_read), \
                mock.patch.object(publisher, "observed_timing", return_value={}), \
                mock.patch.object(publisher, "first_sample_time", return_value=None), \
                mock.patch.object(publisher, "observed_vm_size", return_value=(None, None)), \
                mock.patch.object(publisher, "retention_and_cleanup_status", return_value=""), \
                mock.patch.object(publisher, "render_timeline", return_value=[]):
            _, body, _, _, _ = publisher.run_report("baseline-23gib")

        self.assertIn(
            "| VACUUM only | 120.0 / 60.0 | 2.00 / 10.00 | 5.00× | 4.00 / 12.00 | 3.00× | 1.000 ms / 2.000 ms |",
            body,
        )
        # Multiplying the rates by each role's different coverage would show
        # 2.50× IOPS and 1.50× bytes, which contradicts the rate headings.
        self.assertNotIn("2.50× | 4.00 / 12.00 | 1.50×", body)


class TimingProvenanceTests(unittest.TestCase):
    def test_report_labels_command_lower_bound_and_first_progress_separately(self):
        first_progress = {
            "event": "vacuum_progress_phase",
            "phase": "scanning heap",
            "time": "2026-10-07T08:36:01.632152124Z",
        }
        later_progress = {
            "event": "vacuum_progress_phase",
            "phase": "vacuuming indexes",
            "time": "2026-10-07T08:40:00Z",
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            run = Path(temp_dir)
            (run / "results").mkdir()
            (run / "results/events.jsonl").write_text(
                json.dumps(first_progress) + "\n" + json.dumps(later_progress) + "\n")
            observed = publisher.observed_timing(run)

        self.assertEqual(observed["heapWorkFirstProgress"], first_progress["time"])
        self.assertEqual(observed["heapWorkFirstProgressPhase"], "scanning heap")
        timing = {
            "vacuumStart": "2026-10-07T08:36:00.425657Z",
            "heapWorkStart": "2026-10-07T08:36:00.425657Z",
            "testComplete": True,
        }
        summary = {"status": {"testComplete": True}, "coverage": {"valid": True}}
        process = {"exitCode": 0, "status": "complete"}

        def fake_read(path, default=None):
            if path.name == "timing.json":
                return timing
            if path.name == "summary.json":
                return summary
            if path.name == "test-process.json":
                return process
            return default

        with mock.patch.object(publisher, "read", side_effect=fake_read), \
                mock.patch.object(publisher, "observed_timing", return_value=observed), \
                mock.patch.object(publisher, "totals_for_run", return_value=None), \
                mock.patch.object(publisher, "first_sample_time", return_value=None), \
                mock.patch.object(publisher, "observed_vm_size", return_value=(None, None)), \
                mock.patch.object(publisher, "retention_and_cleanup_status", return_value=""), \
                mock.patch.object(publisher, "render_timeline", return_value=[]):
            _, body, _, complete, _ = publisher.run_report("baseline-23gib")

        self.assertTrue(complete)
        self.assertIn("| VACUUM command start (heap-work lower bound) | 2026-10-07T08:36:00.425657Z |", body)
        self.assertIn("| First VACUUM progress observed (scanning heap) | 2026-10-07T08:36:01.632152124Z (persisted test event) |", body)


class MarkedBlockTests(unittest.TestCase):
    def test_replace_existing_block_and_preserve_surrounding_text(self):
        before = "intro\n\n<!-- S -->\nold\n<!-- E -->\n\noutro\n"
        updated = publisher.replace_block(before, "<!-- S -->", "<!-- E -->", "new")
        self.assertEqual(updated, "intro\n\n<!-- S -->\nnew\n<!-- E -->\n\noutro\n")

    def test_append_missing_block_without_replacing_authored_text(self):
        before = "authored notes\n"
        updated = publisher.replace_block(before, "<!-- S -->", "<!-- E -->", "generated")
        self.assertIn("authored notes", updated)
        self.assertIn("<!-- S -->\ngenerated\n<!-- E -->", updated)

    def test_duplicate_or_unbalanced_markers_fail_closed(self):
        for text in ("<!-- S --><!-- S --><!-- E -->", "<!-- S --> no end"):
            with self.subTest(text=text):
                with self.assertRaises(RuntimeError):
                    publisher.replace_block(text, "<!-- S -->", "<!-- E -->", "new")


class PublicationGateTests(unittest.TestCase):
    def test_require_complete_refuses_when_raw_totals_are_unreconciled(self):
        report = Path("/tmp/should-not-be-written/REPORT.md")
        rows = [(report, "summary", "timeline", True,
                 {"run": name, "totalsAvailable": False, "vacuumSeconds": 1, "throughCatchupSeconds": 2})
                for name in publisher.RUNS]
        with mock.patch.object(publisher, "run_report", side_effect=rows), \
                mock.patch.object(sys, "argv", ["publish_run_reports.py", "--write", "--require-complete"]), \
                mock.patch.object(Path, "write_text") as write_text:
            self.assertEqual(publisher.main(), 2)
            write_text.assert_not_called()


if __name__ == "__main__":
    unittest.main()
