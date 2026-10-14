import importlib.util
import datetime as dt
import json
import sys
import tempfile
import threading
import time
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

    def test_sigterm_is_exactly_one_proc_verified_os_walreceiver(self):
        import base64
        command_line = "postgres: walreceiver streaming 0/123"
        command_sha = __import__("hashlib").sha256(command_line.encode()).hexdigest()
        command_b64 = base64.b64encode(command_line.encode()).decode()
        response = {"inRecovery": True, "receiverCount": 1, "receivers": [
            {"pid": 1234, "backendType": "walreceiver", "applicationName": "",
             "backendStart": "2026-10-12T00:00:00+00:00", "status": "streaming", "receiveStartLsn": "0/1"}]}
        remote_output = (
            "RECEIVER|t|1|1234|walreceiver|streaming|2026-10-12 00:00:00+00|0/1\n"
            f"PROCESS|1234|26|98765|/usr/lib/postgresql/16/bin/postgres|{command_sha}|{command_b64}\n"
            "SIGNAL|SIGTERM|2026-10-12T00:10:00.000Z|2026-10-12T00:10:00.001Z\n")
        with mock.patch.object(runner, "sql", return_value=response) as sql, \
             mock.patch.object(runner, "cmd", return_value=remote_output) as command:
            result = runner.sigterm_replica_walreceiver(["kubectl"], "repro", "hdd-pg-2", "repro")
        self.assertTrue(result["signalSent"])
        self.assertEqual(result["pid"], 1234)
        self.assertEqual(result["receiverRecheck"]["pid"], 1234)
        self.assertEqual(result["processIdentity"]["startTicks"], 98765)
        self.assertEqual(result["processIdentity"]["uid"], 26)
        self.assertEqual(result["processIdentity"]["commandLine"], command_line)
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
        self.assertIn('[ "$after" = "$before" ]', remote)
        self.assertIn('kill -TERM "$pid"', remote)
        syntax = runner.subprocess.run(["sh", "-n"], input=remote, text=True, capture_output=True)
        self.assertEqual(syntax.returncode, 0, syntax.stderr)

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
        # Repeated harvesting cannot run or persist another action.
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


if __name__ == "__main__":
    unittest.main()
