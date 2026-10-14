import importlib.util
import datetime as dt
import json
import sys
import tempfile
import threading
import time
import hashlib
from types import SimpleNamespace
import unittest
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
spec = importlib.util.spec_from_file_location("test_runner", HERE / "03-test.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class InterventionTests(unittest.TestCase):
    def test_event_payload_may_include_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            events = Path(tmp) / "events.jsonl"
            runner.event(events, "pgbench_runtime_verified", path="/usr/bin/pgbench", name="pgbench", version="16.15")
            row = json.loads(events.read_text())
        self.assertEqual(row["event"], "pgbench_runtime_verified")
        self.assertEqual(row["path"], "/usr/bin/pgbench")
        self.assertEqual(row["name"], "pgbench")
        self.assertEqual(row["version"], "16.15")

    def test_client_activity_captures_direct_backend_start_times(self):
        query = runner.client_activity_query("future-run-client")
        for key in ("backendStartUtc", "xactStartUtc", "queryStartUtc"):
            self.assertIn("'" + key + "'", query)

    def test_vacuum_marker_program_targets_absolute_remote_workdir(self):
        query = runner.vacuum_query("VACUUM (VERBOSE) public.vacuum_target;")
        self.assertIn("cat > __VACUUM_WORK_DIRECTORY__/vacuum-start.marker", query)
        self.assertIn("cat > __VACUUM_WORK_DIRECTORY__/vacuum-complete.marker", query)
        with mock.patch.object(runner.uuid, "uuid4", return_value=SimpleNamespace(hex="testid")), \
             mock.patch.object(runner, "cmd", return_value="/controller/tmp/vacuum-testid\n123\n") as command:
            work, pid = runner.launch_vacuum([], "repro", "hdd-pg-1", "repro", "vacuum-test")
        self.assertEqual(work, "/controller/tmp/vacuum-testid")
        self.assertEqual(pid, 123)
        remote = command.call_args.args[0][-1]
        self.assertIn('sed "s@__VACUUM_WORK_DIRECTORY__@$marker_dir@g"', remote)
        self.assertIn('grep -F "$d/vacuum-start.marker"', remote)
        self.assertIn('grep -F "$d/vacuum-complete.marker"', remote)

    def test_fetch_log_parses_boundary_markers_from_separate_files(self):
        start = "VACUUM_START|2026-10-13T05:18:12.699771Z|4E/5D024630|320769|3324|24"
        complete = "VACUUM_COMPLETE|2026-10-13T06:14:43.543859Z|57/457628|36933111460|15124919|6071392"
        # COPY TO PROGRAM writes these rows to the marker files, not stdout
        # captured in vacuum.log. This is the production failure mode that
        # previously made a successful VACUUM look incomplete.
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / "vacuum.log"
            with mock.patch.object(runner, "cmd", side_effect=[
                    "VACUUM\n", start + "\n", complete + "\n"]):
                markers = runner.fetch_log([], "repro", "hdd-pg-1", "/tmp/vacuum-test", destination)
            saved_log = destination.read_text()
        self.assertEqual(markers["start"], {
            "time": "2026-10-13T05:18:12.699771Z", "flushLsn": "4E/5D024630",
            "walBytes": 320769, "walRecords": 3324, "walFpi": 24})
        self.assertEqual(markers["complete"], {
            "time": "2026-10-13T06:14:43.543859Z", "flushLsn": "57/457628",
            "walBytes": 36933111460, "walRecords": 15124919, "walFpi": 6071392})
        self.assertIn(start, saved_log)
        self.assertIn(complete, saved_log)

    def test_fetch_log_keeps_log_marker_fallback_when_marker_files_are_missing(self):
        start = "VACUUM_START|2026-10-13T05:18:12.699771Z|4E/5D024630|320769|3324|24"
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / "vacuum.log"
            with mock.patch.object(runner, "cmd", side_effect=[
                    start + "\n", RuntimeError("not found"), RuntimeError("not found")]):
                markers = runner.fetch_log([], "repro", "hdd-pg-1", "/tmp/vacuum-test", destination)
        self.assertEqual(markers["start"]["flushLsn"], "4E/5D024630")

    def test_sigterm_is_exactly_one_proc_verified_os_walreceiver(self):
        import base64
        command_before = "postgres: walreceiver streaming 0/100"
        command_after = "postgres: walreceiver streaming 0/123"
        sha_before = __import__("hashlib").sha256(command_before.encode()).hexdigest()
        sha_after = __import__("hashlib").sha256(command_after.encode()).hexdigest()
        b64_before = base64.b64encode(command_before.encode()).decode()
        b64_after = base64.b64encode(command_after.encode()).decode()
        response = {"inRecovery": True, "receiverCount": 1, "receivers": [
            {"pid": 1234, "backendType": "walreceiver", "applicationName": "",
             "backendStart": "2026-10-12T00:00:00+00:00", "status": "streaming", "receiveStartLsn": "0/1"}]}
        remote_output = (
            "RECEIVER|t|1|1234|walreceiver|streaming|2026-10-12 00:00:00+00|0/1\n"
            f"PROCESS_BEFORE|1234|26|98765|/usr/lib/postgresql/16/bin/postgres|{sha_before}|{b64_before}\n"
            f"PROCESS_AFTER|1234|26|98765|/usr/lib/postgresql/16/bin/postgres|{sha_after}|{b64_after}\n"
            "SIGNAL|SIGTERM|2026-10-12T00:10:00.000Z|2026-10-12T00:10:00.001Z\n")
        with mock.patch.object(runner, "sql", return_value=response) as sql, \
             mock.patch.object(runner, "cmd", return_value=remote_output) as command:
            result = runner.sigterm_replica_walreceiver(["kubectl"], "repro", "hdd-pg-2", "repro")
        self.assertTrue(result["signalSent"])
        self.assertEqual(result["pid"], 1234)
        self.assertEqual(result["receiverRecheck"]["pid"], 1234)
        self.assertEqual(result["processIdentity"]["startTicks"], 98765)
        self.assertEqual(result["processIdentity"]["uid"], 26)
        self.assertEqual(result["processIdentity"]["commandLineBefore"], command_before)
        self.assertEqual(result["processIdentity"]["commandLineAfter"], command_after)
        self.assertNotEqual(result["processIdentity"]["commandLineSha256Before"],
                            result["processIdentity"]["commandLineSha256After"])
        self.assertEqual(result["processIdentity"]["commandLine"], command_after)
        self.assertEqual(result["signalAcceptedAtUtc"], "2026-10-12T00:10:00.001Z")
        query = sql.call_args.args[4]
        self.assertIn("JOIN pg_stat_activity", query)
        self.assertIn("a.backend_type='walreceiver'", query)
        self.assertNotIn("pg_terminate_backend", query)
        remote = command.call_args.args[0][-4]
        self.assertIn('first=$(read_receiver)', remote)
        self.assertIn('second=$(read_receiver)', remote)
        self.assertIn('/proc/$p/stat', remote)
        self.assertIn('/proc/$p/status', remote)
        self.assertIn('/proc/$p/cmdline', remote)
        self.assertIn('before_key=$(printf', remote)
        self.assertIn('[ "$after_key" = "$before_key" ]', remote)
        self.assertNotIn('[ "$after" = "$before" ]', remote)
        self.assertIn('PROCESS_BEFORE|%s', remote)
        self.assertIn('PROCESS_AFTER|%s', remote)
        self.assertIn('kill -TERM "$pid"', remote)
        syntax = runner.subprocess.run(["sh", "-n"], input=remote, text=True, capture_output=True)
        self.assertEqual(syntax.returncode, 0, syntax.stderr)

    def test_proc_identity_pair_accepts_changing_wal_lsn_title(self):
        import base64
        import hashlib
        def fields(cmd, ticks="98765", pid="1234"):
            return [pid, "26", ticks, "/usr/lib/postgresql/16/bin/postgres",
                    hashlib.sha256(cmd.encode()).hexdigest(), base64.b64encode(cmd.encode()).decode()]
        before, after = runner.verified_proc_identity_pair(
            fields("postgres: walreceiver streaming 0/100"),
            fields("postgres: walreceiver streaming 0/200"), 1234)
        self.assertNotEqual(before["commandLineSha256"], after["commandLineSha256"])

    def test_proc_identity_pair_rejects_pid_reuse_and_changed_start_ticks(self):
        import base64
        import hashlib
        def fields(cmd, ticks="98765", pid="1234"):
            return [pid, "26", ticks, "/usr/lib/postgresql/16/bin/postgres",
                    hashlib.sha256(cmd.encode()).hexdigest(), base64.b64encode(cmd.encode()).decode()]
        command = "postgres: walreceiver streaming 0/100"
        with self.assertRaisesRegex(RuntimeError, "process identity"):
            runner.verified_proc_identity_pair(fields(command), fields(command, pid="1235"), 1234)
        with self.assertRaisesRegex(RuntimeError, "stable PID/UID/starttime/executable"):
            runner.verified_proc_identity_pair(fields(command), fields(command, ticks="98766"), 1234)

    def test_sigterm_refuses_ambiguous_receiver_count(self):
        response = {"inRecovery": True, "receiverCount": 2, "receivers": []}
        with mock.patch.object(runner, "sql", return_value=response) as sql, \
             mock.patch.object(runner, "cmd") as command:
            with self.assertRaisesRegex(RuntimeError, "exactly one verified replica walreceiver"):
                runner.sigterm_replica_walreceiver(["kubectl"], "repro", "hdd-pg-2", "repro")
        self.assertEqual(sql.call_count, 1)
        command.assert_not_called()

    def test_sigterm_refuses_non_recovery_pod(self):
        response = {"inRecovery": False, "receiverCount": 1, "receivers": [
            {"pid": 1234, "backendType": "walreceiver", "status": "streaming"}]}
        with mock.patch.object(runner, "sql", return_value=response), \
             mock.patch.object(runner, "cmd") as command:
            with self.assertRaisesRegex(RuntimeError, "exactly one verified replica walreceiver"):
                runner.sigterm_replica_walreceiver(["kubectl"], "repro", "hdd-pg-1", "repro")
        command.assert_not_called()

    def test_sigterm_refuses_receiver_not_streaming(self):
        response = {"inRecovery": True, "receiverCount": 1, "receivers": [
            {"pid": 1234, "backendType": "walreceiver", "status": "starting"}]}
        with mock.patch.object(runner, "sql", return_value=response), \
             mock.patch.object(runner, "cmd") as command:
            with self.assertRaisesRegex(RuntimeError, "exactly one verified replica walreceiver"):
                runner.sigterm_replica_walreceiver(["kubectl"], "repro", "hdd-pg-2", "repro")
        command.assert_not_called()

    def test_sigterm_schedule_uses_remote_vacuum_marker_time(self):
        marker = {"time": "2026-10-12T00:00:00.000000Z", "flushLsn": "0/1"}
        with mock.patch.object(runner, "sigterm_replica_walreceiver", return_value={
                "pid": 1234, "signalSent": True, "signalRequestedAtUtc": "2026-10-12T00:10:00.001Z"}) as signal:
            epoch = dt.datetime.fromisoformat(marker["time"].replace("Z", "+00:00")).timestamp()
            result, error = runner.execute_intervention([], "repro", "hdd-pg-2", "repro", "sigterm", marker, epoch + 600, 600)
        self.assertIsNone(error)
        self.assertEqual(result["scheduledAtUtc"], "2026-10-12T00:10:00Z")
        self.assertEqual(result["executedAtUtc"], "2026-10-12T00:10:00.001Z")
        self.assertEqual(result["vacuumStartMarker"], marker)
        self.assertEqual(signal.call_count, 1)

    def test_recovery_requires_transaction_progress_over_rolling_minute(self):
        monitor = runner.ClientMonitor([], "repro", "primary", "repro", "bench", HERE/"unused.jsonl")
        now = runner.time.time()
        with monitor.lock:
            for i in range(61):
                monitor.history.append({"sampleStatus": "ok", "sampleEpoch": now-60+i,
                    "committedTransactions": i*5, "benchSessions": 10, "unrelatedAppSessions": 0, "backends": []})
        result = monitor.progress_health()
        self.assertTrue(result["healthy"], result)
        self.assertAlmostEqual(result["transactionsPerSecond"], 5.0)

    def test_stalled_or_prolonged_pending_writer_is_not_recovered(self):
        monitor = runner.ClientMonitor([], "repro", "primary", "repro", "bench", HERE/"unused.jsonl")
        now = runner.time.time()
        with monitor.lock:
            for i in range(61):
                monitor.history.append({"sampleStatus": "ok", "sampleEpoch": now-60+i,
                    "committedTransactions": 0, "benchSessions": 10, "unrelatedAppSessions": 0,
                    "backends": [{"pid": 10, "state": "active", "queryAgeSeconds": 75, "xactAgeSeconds": 75,
                                  "waitEventType": "Lock", "waitEvent": "transactionid"}]})
        result = monitor.progress_health()
        self.assertFalse(result["healthy"])
        self.assertEqual(result["transactionsPerSecond"], 0.0)
        self.assertGreater(result["prolongedPendingCount"], 0)

    def test_control_records_marker_without_signaling(self):
        marker = {"time": "2026-10-12T00:00:00.000000Z", "flushLsn": "0/1"}
        observed = "2026-10-12T00:10:00.013Z"
        with mock.patch.object(runner, "sql", return_value={
                "observedAtUtc": observed, "inRecovery": True,
                "receiver": {"pid": 4, "status": "streaming"}}) as sql, \
             mock.patch.object(runner, "sigterm_replica_walreceiver") as signal:
            epoch = dt.datetime.fromisoformat(marker["time"].replace("Z", "+00:00")).timestamp()
            result, error = runner.execute_intervention([], "repro", "hdd-pg-2", "repro", "control", marker, epoch + 600, 600)
        self.assertIsNone(error)
        self.assertEqual(result["status"], "marked")
        self.assertEqual(result["scheduledAtUtc"], "2026-10-12T00:10:00Z")
        self.assertEqual(result["executedAtUtc"], observed)
        self.assertIn("clock_timestamp()", sql.call_args.args[4])
        self.assertEqual(sql.call_count, 1)
        signal.assert_not_called()

    def test_control_query_failure_marks_intervention_failed(self):
        with mock.patch.object(runner, "sql", side_effect=RuntimeError("replica SQL unavailable")):
            result, error = runner.execute_intervention([], "repro", "hdd-pg-2", "repro",
                "control", {"time": "2026-10-12T00:00:00Z"}, 1_800_000_600, 600)
        self.assertEqual(result["status"], "failed")
        self.assertIsNone(result["executedAtUtc"])
        self.assertIn("SQL unavailable", error)

    def test_monotonic_scheduler_runs_once_without_blocking_sampler(self):
        started = threading.Event()
        release = threading.Event()
        dispatches = []
        dispatch_wall_times = []
        scheduled = time.time() + 0.05

        def action():
            dispatches.append(time.monotonic())
            dispatch_wall_times.append(time.time())
            started.set()
            self.assertTrue(release.wait(2))
            return {"mode": "control", "status": "marked", "executedAtUtc": "remote-clock"}, None

        scheduler = runner.InterventionScheduler("control", {"time": "marker"}, scheduled, 600, action).start()
        self.assertTrue(started.wait(1), "scheduled action did not dispatch")
        # Simulate the main thread continuing to sample while the remote API action is pending.
        samples = 0
        for _ in range(20):
            samples += 1
            time.sleep(0.005)
        self.assertEqual(samples, 20)
        self.assertIsNone(scheduler.ready_result())
        release.set()
        self.assertTrue(scheduler._done.wait(1))
        self.assertTrue(scheduler.join_once(timeout=1))
        result, error = scheduler.ready_result()
        self.assertIsNone(error)
        self.assertEqual(result["executedAtUtc"], "remote-clock")
        self.assertEqual(len(dispatches), 1)
        self.assertGreaterEqual(dispatch_wall_times[0], scheduled - 0.01)
        self.assertLess(dispatch_wall_times[0] - scheduled, 0.5)
        self.assertGreaterEqual(result["schedulerDispatchLatenessSeconds"], 0)
        self.assertTrue(scheduler._joined)
        self.assertEqual(scheduler.ready_result(), (result, None))
        self.assertEqual(len(dispatches), 1)

    def test_monotonic_scheduler_cancels_before_deadline_without_dispatch(self):
        dispatches = []
        scheduler = runner.InterventionScheduler("sigterm", {"time": "marker"}, time.time() + 30, 600,
            lambda: (dispatches.append("unexpected"), None)).start()
        scheduler.cancel()
        self.assertTrue(scheduler.join_once(timeout=1))
        result, error = scheduler.ready_result()
        self.assertEqual(dispatches, [])
        self.assertEqual(result["status"], "cancelled_before_dispatch")
        self.assertIn("cancelled", error)
        self.assertTrue(scheduler._joined)

    def test_client_artifacts_use_bounded_hash_verified_ranges(self):
        client = {"workDirectory": "/tmp/pgbench-test", "pgbenchPid": 812,
                  "clients": 10}
        files = {"pgbench.out": b"native progress output " * 4,
                 "status": b"0\n", "pgbench.pid": b"812\n",
                 "transaction.sql": (runner.CLIENT_DIR / "transaction.sql").read_bytes(),
                 "run.sh": b"#!/bin/sh\n"}
        files.update({f"pgbench-log.812{'.'+str(i) if i else ''}": (f"log-{i}-".encode() * 3)
                      for i in range(10)})
        inventory = {name: len(content) for name, content in files.items()}
        ranges = []

        def fingerprint(_base, _ns, _pod, _client, name):
            payload = files[name]
            return len(payload), hashlib.sha256(payload).hexdigest()

        def read_chunk(_base, _ns, _pod, _client, name, offset, expected, chunk_bytes):
            ranges.append((name, offset, expected, chunk_bytes))
            return files[name][offset:offset+expected]

        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch.object(runner, "CLIENT_LOG_CHUNK_BYTES", 8), \
             mock.patch.object(runner, "remote_client_status", return_value="0"), \
             mock.patch.object(runner, "client_file_inventory", side_effect=[inventory, inventory]), \
             mock.patch.object(runner, "client_file_fingerprint", side_effect=lambda *args:fingerprint(*args)), \
             mock.patch.object(runner, "client_file_chunk", side_effect=lambda *args:read_chunk(*args)):
            copied = runner.fetch_client_logs([], "repro", "primary", client, Path(tmp))
            manifest = json.loads((Path(tmp)/"transfer-manifest.json").read_text())
            for path in copied:
                self.assertEqual(path.read_bytes(), files[path.name])
        self.assertTrue(ranges)
        self.assertTrue(all(expected <= 8 and chunk == 8 for _, _, expected, chunk in ranges))
        self.assertTrue(manifest["transferComplete"])
        self.assertFalse(manifest["captureComplete"])
        self.assertEqual(manifest["files"]["pgbench.out"]["sha256"], hashlib.sha256(files["pgbench.out"]).hexdigest())
        self.assertGreater(manifest["files"]["pgbench.out"]["chunks"], 1)

    def test_remote_client_directory_is_removed_only_after_stop_and_capture(self):
        client = {"workDirectory": "/tmp/pgbench-test", "wrapperPid": 812, "pgbenchPid": 813}
        inventory = {"pgbench.out": 80, "status": 2}
        for stopped in (False, True):
            for captured in (False, True):
                with self.subTest(stopped=stopped, captured=captured), tempfile.TemporaryDirectory() as tmp, \
                     mock.patch.object(runner, "cmd", return_value="") as command, \
                     mock.patch.object(runner, "remote_client_status", return_value="0"), \
                     mock.patch.object(runner, "client_file_inventory", return_value=inventory):
                    receipt = runner.finalize_client_directory([], "repro", "primary", client, tmp,
                        stopped, captured, "test receipt")
                    rm_calls = [call for call in command.call_args_list if "rm -rf --" in call.args[0][-1]]
                    should_remove = stopped and captured
                    self.assertEqual(bool(rm_calls), should_remove)
                    self.assertEqual(receipt["remoteDirectoryPreserved"], not should_remove)
                    receipt_path = Path(tmp)/("client-preservation.json" if not should_remove else "client-directory-cleanup.json")
                    self.assertTrue(receipt_path.is_file())
                    if not should_remove:
                        saved = json.loads(receipt_path.read_text())
                        self.assertEqual(saved["reason"], "test receipt")
                        self.assertEqual(saved["inventory"], inventory)


if __name__ == "__main__":
    unittest.main()
