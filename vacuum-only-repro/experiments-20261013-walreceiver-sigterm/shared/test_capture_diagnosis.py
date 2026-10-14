from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("capture_diagnosis", HERE / "capture-diagnosis.py")
assert SPEC and SPEC.loader
capture = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = capture
SPEC.loader.exec_module(capture)


class CaptureDiagnosisTests(unittest.TestCase):
    def test_nested_postgres_archive_restore_is_whitelisted(self):
        line = (
            '2026-10-13T01:02:03.456789Z '
            + json.dumps({
                "level": "info", "ts": "2026-10-13T01:02:03.456Z",
                "logger": "postgres", "msg": "record",
                "record": {
                    "log_time": "2026-10-13 01:02:03.456 UTC",
                    "process_id": 731, "error_severity": "LOG", "sql_state_code": "00000",
                    "message": 'restored log file "00000001000000000000002A" from archive',
                    "query": "select 'do not expose query'", "detail": "password=do-not-expose",
                    "conninfo": "host=x password=do-not-expose",
                },
            })
        )
        row = capture.safe_log_record(line, "hdd-pg-2")
        self.assertIsNotNone(row)
        self.assertEqual(row["archiveRestoreOutcome"], "success")
        self.assertEqual(row["walName"], "00000001000000000000002A")
        self.assertEqual(row["postgresLogTime"], "2026-10-13 01:02:03.456 UTC")
        self.assertEqual(row["postgresRecord"], {
            "message": 'restored log file "00000001000000000000002A" from archive',
            "log_time": "2026-10-13 01:02:03.456 UTC",
            "process_id": 731, "error_severity": "LOG", "sql_state_code": "00000",
        })
        serialized = json.dumps(row)
        for forbidden in ("do not expose query", "do-not-expose", "conninfo", '"detail"', '"query"'):
            self.assertNotIn(forbidden, serialized)

    def test_restore_report_counts_after_signal_and_stream_return(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            logs = root / "postgres.log"
            intervention = root / "walreceiver-intervention.json"
            primary = root / "primary-pgstat.jsonl"
            replica = root / "replica-pgstat.jsonl"
            output = root / "report.json"
            events = [
                {"timestamp": "2026-10-13T00:59:59Z", "logger": "postgres", "msg": "record",
                 "record": {"log_time": "2026-10-13 00:59:59 UTC", "process_id": 7,
                            "error_severity": "LOG", "sql_state_code": "00000",
                            "message": 'restored log file "000000010000000000000029" from archive'}},
                {"timestamp": "2026-10-13T01:01:00Z", "logger": "postgres", "msg": "record",
                 "record": {"log_time": "2026-10-13 01:01:00 UTC", "process_id": 7,
                            "error_severity": "LOG", "sql_state_code": "00000",
                            "message": 'restored log file "00000001000000000000002A" from archive',
                            "query": "secret query"}},
                {"timestamp": "2026-10-13T01:02:00Z", "logger": "postgres", "msg": "record",
                 "record": {"log_time": "2026-10-13 01:02:00 UTC", "process_id": 7,
                            "error_severity": "LOG", "sql_state_code": "00000",
                            "message": 'restored log file "00000001000000000000002B" from archive'}},
            ]
            logs.write_text("".join(f"{row['timestamp']} {json.dumps(row)}\n" for row in events))
            intervention.write_text(json.dumps({"mode": "sigterm", "status": "sent",
                "executedAtUtc": "2026-10-13T01:00:00Z",
                "signalResult": {"signalSent": True, "pid": 42,
                                 "signalAcceptedAtUtc": "2026-10-13T01:00:00.500Z"}}))
            primary.write_text(json.dumps({"sampledAt": "2026-10-13T01:00:30Z",
                    "replicationSenders": [{"pid": 20, "application_name": "hdd-pg-2", "state": "catchup",
                                            "sync_state": "quorum"}]}) + "\n" +
                json.dumps({"sampledAt": "2026-10-13T01:03:00Z",
                    "replicationSenders": [{"pid": 20, "application_name": "hdd-pg-2", "state": "streaming",
                                            "sync_state": "quorum"}]}) + "\n")
            replica.write_text(json.dumps({"sampledAt": "2026-10-13T01:02:30Z", "walReceiver": None}) + "\n" +
                               json.dumps({"sampledAt": "2026-10-13T01:03:01Z",
                                           "walReceiver": {"pid": 501, "status": "streaming",
                                                           "written_lsn": "0/100"}}) + "\n")
            result = subprocess.run([
                sys.executable, str(HERE / "analysis-restore-path-report.py"),
                "--postgres-log", str(logs), "--intervention", str(intervention),
                "--primary-pgstat", str(primary), "--replica-pgstat", str(replica),
                "--out", str(output),
            ], text=True, capture_output=True, check=False)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(output.read_text())
            self.assertEqual(report["signal"]["atUtc"], "2026-10-13T01:00:00.5Z")
            restore = report["archiveRestore"]
            self.assertEqual(restore["successfulRestoreCountAll"], 3)
            self.assertEqual(restore["successfulRestoreCountAfterSignal"], 2)
            self.assertEqual(restore["firstPostSignalWal"], "00000001000000000000002A")
            self.assertEqual(restore["lastPostSignalWal"], "00000001000000000000002B")
            self.assertEqual(report["streamReturn"]["primarySender"]["firstStreaming"]["syncState"], "quorum")
            self.assertEqual(report["streamReturn"]["primarySender"]["firstCatchup"]["state"], "catchup")
            self.assertIn("absence not observed", report["streamReturn"]["primarySender"]["returnClaimStatus"])
            self.assertEqual(report["streamReturn"]["replicaReceiver"]["firstObservedAbsence"]["timestamp"],
                             "2026-10-13T01:02:30Z")
            self.assertEqual(report["streamReturn"]["replicaReceiver"]["firstReceiverPidChange"]["oldReceiverPid"], 42)
            self.assertEqual(report["streamReturn"]["replicaReceiver"]["firstStreaming"]["status"], "streaming")
            self.assertEqual(report["streamReturn"]["replicaReceiver"]["returnClaimStatus"],
                             "receiver streaming observed after sampled receiver absence")
            self.assertEqual(report["streamReturn"]["secondsFromLastRestoreToFirstPrimaryStreaming"], 60.0)
            self.assertNotIn("secret query", output.read_text())

    def test_restore_report_also_accepts_safe_captured_event_rows(self):
        raw = ('2026-10-13T01:01:00Z ' + json.dumps({
            "logger": "postgres", "msg": "record",
            "record": {"log_time": "2026-10-13 01:01:00 UTC", "process_id": 7,
                       "error_severity": "LOG", "sql_state_code": "00000",
                       "message": 'restored log file "00000001000000000000002A" from archive',
                       "query": "must not be retained"},
        }))
        safe_row = capture.safe_log_record(raw)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw_path = root / "postgres-raw.log"
            event_path = root / "postgres-log-events-redacted.jsonl"
            raw_path.write_text(raw + "\n")
            event_path.write_text(json.dumps(safe_row) + "\n")
            output = root / "timeline.json"
            result = subprocess.run([
                sys.executable, str(HERE / "analysis-restore-path-report.py"),
                "--postgres-log", str(raw_path), "--postgres-log", str(event_path),
                "--signal-at", "2026-10-13T01:00:00Z",
                "--out", str(output),
            ], text=True, capture_output=True, check=False)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(output.read_text())
            self.assertEqual(report["archiveRestore"]["successfulRestoreCountAfterSignal"], 1)
            self.assertEqual(report["archiveRestore"]["firstPostSignalWal"], "00000001000000000000002A")
            self.assertEqual(report["inputCoverage"]["duplicateRestoreRowsSkipped"], 1)
            self.assertNotIn("must not be retained", output.read_text())


if __name__ == "__main__":
    unittest.main()
