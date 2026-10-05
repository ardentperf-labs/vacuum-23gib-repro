import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("vacuum_test", Path(__file__).with_name("03-test.py"))
vacuum_test = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vacuum_test)


class ReplicationLogTests(unittest.TestCase):
    def test_parses_cnpg_nested_record_and_kubernetes_timestamp_prefix(self):
        messages = [
            ("sender_timeout", "terminating walsender process due to replication timeout", "walsender"),
            ("receiver_timeout", "terminating walreceiver due to timeout", "walreceiver"),
            ("receiver_disconnect", "could not receive data from WAL stream: server closed connection", "walreceiver"),
            ("receiver_connected", "started streaming WAL from primary at 0/4000000", "walreceiver"),
        ]
        lines = []
        for i, (_, message, backend) in enumerate(messages):
            ts = f"2026-10-04T10:00:0{i}.123456789Z"
            row = {"level": "info", "ts": ts, "logger": "postgres", "msg": "record",
                   "record": {"message": message, "session_id": f"session-{i}",
                              "process_id": str(100 + i), "backend_type": backend}}
            lines.append(ts + " " + json.dumps(row))
        lines.append(json.dumps({"ts": "2026-10-04T10:00:05Z", "record": {"message": "checkpoint complete"}}))

        events = vacuum_test.parse_replication_log("\n".join(lines), "replica")
        self.assertEqual([e["event"] for e in events], [x[0] for x in messages])
        self.assertEqual(events[0]["time"], "2026-10-04T10:00:00.123456789Z")
        self.assertEqual(events[0]["sessionId"], "session-0")
        self.assertEqual(events[0]["processId"], "100")
        self.assertEqual(events[0]["backendType"], "walsender")
        self.assertTrue(all(e["role"] == "replica" for e in events))
        fallback = json.dumps({"record": {"message": messages[3][1], "log_time": "2026-10-04 10:00:04.123 UTC"}})
        self.assertEqual(vacuum_test.parse_replication_log(fallback, "replica")[0]["time"],
                         "2026-10-04T10:00:04.123Z")

    def test_unavailable_pod_log_is_recorded_without_raising(self):
        pods = {r: {"metadata": {"name": r + "-pod"}} for r in ("primary", "replica")}
        marks = []
        def fake_cmd(argv, timeout):
            if "primary-pod" in argv:
                raise RuntimeError("pod log unavailable")
            return '{"ts":"2026-10-04T10:00:00Z","record":{"message":"checkpoint complete"}}\n'
        with tempfile.TemporaryDirectory() as d, patch.object(vacuum_test, "cmd", fake_cmd):
            events = vacuum_test.capture_replication_logs([], "repro", pods, "2026-10-04T10:00:00Z",
                Path(d), lambda name, **values: marks.append((name, values)))
            self.assertEqual([e["event"] for e in events], ["replication_log_capture_failed"])
            self.assertEqual(events[0]["role"], "primary")
            self.assertEqual(marks[0][0], "replication_log_capture_failed")
            self.assertEqual(marks[0][1]["role"], "primary")
            self.assertTrue((Path(d) / "raw" / "replication-log-errors.json").exists())


if __name__ == "__main__":
    unittest.main()
