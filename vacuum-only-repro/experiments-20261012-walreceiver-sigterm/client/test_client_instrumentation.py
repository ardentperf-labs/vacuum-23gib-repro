import importlib.util
import io
import base64
import json
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[1] / "run" / "03-test.py"
sys.path.insert(0, str(SCRIPT.parent))
SPEC = importlib.util.spec_from_file_location("pgbench_measurement", SCRIPT)
measurement = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(measurement)


class FakeMonitor:
    def __init__(self):
        self.stopped = False

    def latest(self):
        return {
            "benchSessions": 10,
            "unrelatedAppSessions": 0,
            "committedTransactions": 1,
            "backends": [{"state": "active", "xactAgeSeconds": 0.2, "queryAgeSeconds": 0.1}],
        }

    def stop(self):
        self.stopped = True
        return True


class ClientInstrumentationTests(unittest.TestCase):
    def test_native_seven_field_log_keeps_latency_lag_and_completion_time(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "pgbench-log.123"
            log.write_text("0 81 4621 0 1412881037 912698 3005\n")

            result = measurement.summarize_pgbench_logs([log])

        self.assertEqual(result["transactionLogRows"], 1)
        self.assertEqual(result["latencyMs"]["mean"], 4.621)
        self.assertEqual(result["scheduleLagMs"]["mean"], 3.005)
        self.assertAlmostEqual(result["executionMs"]["mean"], 1.616)
        self.assertEqual(result["malformedLogRows"], [])

    def test_graceful_stop_uses_pgbench_duration_signal(self):
        with patch.object(measurement, "cmd", return_value="graceful_stop_requested") as remote:
            result = measurement.signal_pgbench_stop(
                ["kubectl"], "repro", "primary", {"workDirectory": "/tmp/pgbench"}
            )

        self.assertEqual(result, "graceful_stop_requested")
        command = remote.call_args.args[0][-1]
        self.assertIn("kill -ALRM", command)
        self.assertNotIn("kill -INT", command)

    def test_pgbench_runtime_accepts_pinned_postgresql_16_builds_and_rejects_malformed_versions(self):
        valid_versions = [
            "pgbench (PostgreSQL) 16.15",
            "pgbench (PostgreSQL) 16.15 (Debian 16.15-1.pgdg13+1)",
        ]
        for version in valid_versions:
            with self.subTest(version=version), patch.object(
                measurement, "cmd", return_value=f"/usr/lib/postgresql/16/bin/pgbench\n{version}\n"
            ):
                result = measurement.check_pgbench_runtime(["kubectl"], "repro", "primary")
                self.assertEqual(result["path"], "/usr/lib/postgresql/16/bin/pgbench")
                self.assertEqual(result["version"], version)

        invalid_versions = [
            "pgbench (PostgreSQL 16.15)",  # Old, incorrect version-gate expectation.
            "pgbench (PostgreSQL) 17.15",
            "pgbench (PostgreSQL) 16.14",
            "pgbench (PostgreSQL) 16.15-custom",
            "pgbench (PostgreSQL) 16.15 (Other 16.15-1.pgdg13+1)",
            "pgbench (PostgreSQL) 16.15 (Debian 16.15-1.pgdg13+1) extra",
        ]
        for version in invalid_versions:
            with self.subTest(version=version), patch.object(
                measurement, "cmd", return_value=f"/usr/bin/pgbench\n{version}\n"
            ):
                with self.assertRaisesRegex(RuntimeError, "no verified PostgreSQL 16 pgbench client"):
                    measurement.check_pgbench_runtime(["kubectl"], "repro", "primary")

    def test_cnpg_app_secret_is_validated_and_pgpass_is_stdin_only(self):
        password = "secret:with\\slash"
        secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "type": "kubernetes.io/basic-auth",
            "metadata": {
                "name": "hdd-pg-app",
                "labels": {"cnpg.io/cluster": "hdd-pg"},
                "ownerReferences": [{"kind": "Cluster", "name": "hdd-pg", "uid": "cluster-uid"}],
            },
            "data": {
                key: base64.b64encode(value.encode()).decode()
                for key, value in {"username": "app", "dbname": "repro", "port": "5432", "password": password}.items()
            },
        }
        saved_credential = measurement.ACTIVE_CREDENTIAL

        def fake_run(argv, **kwargs):
            if "get" in argv and "secret" in argv:
                return SimpleNamespace(returncode=0, stdout=json.dumps(secret), stderr="")
            script = argv[-1]
            path = next(line.split("=", 1)[1].strip("'") for line in script.splitlines() if line.startswith("p="))
            self.assertIn("-i", argv)
            self.assertIn("mkdir -m 700", script)
            self.assertIn("chmod 600", script)
            self.assertEqual(kwargs["input"], "127.0.0.1:5432:repro:app:secret\\:with\\\\slash\n")
            self.assertNotIn(password, " ".join(argv))
            self.assertNotIn(secret["data"]["password"], " ".join(argv))
            return SimpleNamespace(returncode=0, stdout=path + "\n", stderr="")

        try:
            with patch.object(measurement.subprocess, "run", side_effect=fake_run):
                credential = measurement.install_cnpg_app_pgpass(
                    ["kubectl"], "repro", "hdd-pg-1", "hdd-pg", "cluster-uid", "repro"
                )
            self.assertEqual(credential["username"], "app")
            self.assertEqual(credential["database"], "repro")
            self.assertEqual(credential["host"], "127.0.0.1")
            self.assertNotIn("password", measurement.ACTIVE_CREDENTIAL)
            self.assertEqual(measurement.ACTIVE_CREDENTIAL["path"], credential["pgpassFilePath"])

            wrong_owner = json.loads(json.dumps(secret))
            wrong_owner["metadata"]["ownerReferences"][0]["uid"] = "different-cluster"
            with patch.object(
                measurement.subprocess,
                "run",
                return_value=SimpleNamespace(returncode=0, stdout=json.dumps(wrong_owner), stderr=""),
            ):
                with self.assertRaises(RuntimeError) as error:
                    measurement.read_cnpg_app_secret(["kubectl"], "repro", "hdd-pg", "cluster-uid", "repro")
            self.assertNotIn(password, str(error.exception))
        finally:
            measurement.ACTIVE_CREDENTIAL = saved_credential

    def test_app_auth_preflight_uses_tcp_and_pgpass_file_before_setup(self):
        credential = {
            "pgpassFilePath": "/tmp/pgbench-auth-test/pgpass",
            "port": 5432,
        }
        with patch.object(measurement, "cmd", return_value="app|repro|on|0") as remote:
            measurement.verify_client_app_auth(["kubectl"], "repro", "hdd-pg-1", "repro", credential)

        auth_argv = remote.call_args_list[0].args[0]
        self.assertIn("PGPASSFILE=/tmp/pgbench-auth-test/pgpass", auth_argv)
        self.assertIn("127.0.0.1", auth_argv)
        self.assertIn("-U", auth_argv)
        self.assertEqual(auth_argv[auth_argv.index("-U") + 1], "app")

    def test_capture_keeps_native_logged_and_server_completion_counts_separate(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            raw = out / "client" / "raw"
            raw.mkdir(parents=True)
            pgbench_out = raw / "pgbench.out"
            pgbench_log = raw / "pgbench-log.123"
            pgbench_out.write_text(
                "number of transactions actually processed: 1\n"
                "number of failed transactions: 0 (0.000%)\n"
            )
            pgbench_log.write_text("0 0 4621 0 1412881037 912698 3005\n")
            active = {
                "base": ["kubectl"],
                "namespace": "repro",
                "pod": "primary",
                "client": {"workDirectory": "/tmp/pgbench", "status": "0"},
                "outputDirectory": str(out),
                "drainedSample": {"benchSessions": 0, "committedTransactions": 1},
                "stopResult": {"serverActivityBeforeStopRequest": {"backends": []}},
            }

            def fetch_logs(_base, _namespace, _pod, _client, _dest):
                return [pgbench_out, pgbench_log]

            with patch.object(measurement, "fetch_client_logs", side_effect=fetch_logs), patch.object(
                measurement, "remote_client_status", return_value="0"
            ):
                result = measurement.capture_pgbench_results(active)

        self.assertEqual(result["completedTransactions"], 1)
        self.assertEqual(result["loggedCompletedTransactions"], 1)
        self.assertEqual(result["committedTransactions"], 1)
        self.assertEqual(result["validationErrors"], [])

    def test_commit_count_reconciliation_allows_only_observed_inflight_surplus(self):
        explained = {
            "completedTransactions": 10,
            "loggedCompletedTransactions": 10,
            "committedTransactions": 11,
            "failedTransactions": 0,
            "malformedLogRows": [],
            "pendingTransactionsAtStopRequest": 0,
            "inFlightActiveSessionsAtStopRequest": 1,
        }
        self.assertEqual(measurement.pgbench_summary_errors(explained), [])
        self.assertTrue(explained["commitLogReconciliation"]["explainedByInFlightGracefulDrain"])

        unexplained = {**explained, "committedTransactions": 12, "inFlightActiveSessionsAtStopRequest": 0}
        errors = measurement.pgbench_summary_errors(unexplained)
        self.assertTrue(any("cannot be reconciled" in error for error in errors))

        malformed = {**explained, "malformedLogRows": [{"line": 1}], "failedTransactions": 1}
        errors = measurement.pgbench_summary_errors(malformed)
        self.assertTrue(any("malformed" in error for error in errors))
        self.assertTrue(any("failed transactions" in error for error in errors))

    def test_vacuum_target_catchup_accepts_any_quorum_sender(self):
        primary = {
            "role": "primary",
            "appSessions": 0,
            "benchSessions": 10,
            "flushLsn": "0/20",
            "senders": [{"state": "streaming", "syncState": "quorum"}],
        }
        replica = {
            "role": "replica",
            "appSessions": 0,
            "benchSessions": 0,
            "replayLsn": "0/20",
            "receiver": {"status": "streaming"},
        }
        self.assertTrue(measurement.replica_reached_vacuum_target(primary, replica, "0/10"))
        primary["senders"][0]["syncState"] = "async"
        self.assertFalse(measurement.replica_reached_vacuum_target(primary, replica, "0/10"))

    def test_post_warmup_checkpoint_replay_precedes_vacuum_boundary_without_waiting_for_live_flush(self):
        calls = []
        sql_results = [
            {"flushLsn": "0/10", "walBytes": "100", "walRecords": 1, "walFpi": 0, "statsReset": "reset"},
            {"flushLsn": "0/20", "walBytes": "200", "walRecords": 2, "walFpi": 1, "statsReset": "reset"},
            {"recovery": True, "replayLsn": "0/20"},
            {"flushLsn": "0/30", "walBytes": "300", "walRecords": 3, "walFpi": 2, "statsReset": "reset"},
        ]

        def fake_sql(_base, _namespace, pod, _database, query, *args, **kwargs):
            if query == measurement.WAL_SQL:
                calls.append("primary_wal")
            else:
                self.assertEqual(pod, "replica")
                calls.append("replica_replay")
            return sql_results.pop(0)

        def fake_cmd(_argv, *args, **kwargs):
            calls.append("checkpoint_sql")
            self.assertEqual(args[1] if len(args) > 1 else kwargs.get("input"), "CHECKPOINT;\n")
            return ""

        class Monitor:
            def latest(self):
                return {
                    "sampleStatus": "ok",
                    "sampleEpoch": time.time(),
                    "benchSessions": 10,
                    "unrelatedAppSessions": 0,
                }

        def mark(name, **_fields):
            calls.append(name)

        with patch.object(measurement, "sql", side_effect=fake_sql), patch.object(
            measurement, "cmd", side_effect=fake_cmd
        ), patch.object(measurement, "remote_client_status", return_value="RUNNING"):
            result = measurement.pre_vacuum_checkpoint(
                ["kubectl"], "repro", "primary", "replica", "repro",
                {"clients": 10}, Monitor(), 1, 1, mark,
            )

        self.assertEqual(result["targetLsn"], "0/20")
        self.assertEqual(result["replicaReplayLsn"], "0/20")
        self.assertEqual(result["primaryFlushLsnAtReplay"], "0/30")
        self.assertTrue(result["clientStillRunning"])
        self.assertLess(calls.index("pre_vacuum_checkpoint_started"), calls.index("checkpoint_sql"))
        self.assertLess(calls.index("checkpoint_sql"), calls.index("pre_vacuum_checkpoint_completed"))
        self.assertLess(calls.index("pre_vacuum_checkpoint_completed"), calls.index("replica_replay"))
        self.assertLess(calls.index("replica_replay"), calls.index("pre_vacuum_checkpoint_replay_complete"))

    def test_main_finally_gracefully_stops_and_captures_after_warmup_failure(self):
        original_state = measurement.STATE
        monitor = FakeMonitor()
        drained = {
            "benchSessions": 0,
            "unrelatedAppSessions": 0,
            "committedTransactions": 1,
            "observedAtUtc": "2026-10-09T00:00:00Z",
        }

        def fail_after_client_start(_args):
            measurement.ACTIVE_MONITOR = monitor
            measurement.ACTIVE_CLIENT_LAUNCH_ATTEMPTED = True
            measurement.ACTIVE_CLIENT = {
                "base": ["kubectl"],
                "namespace": "repro",
                "pod": "primary",
                "client": {"workDirectory": "/tmp/pgbench", "status": "RUNNING"},
                "outputDirectory": str(Path(directory) / "run"),
            }
            measurement.ACTIVE_CREDENTIAL = {
                "base": ["kubectl"], "namespace": "repro", "pod": "primary",
                "directory": "/tmp/pgbench-auth-test", "path": "/tmp/pgbench-auth-test/pgpass",
                "outputDirectory": str(Path(directory) / "run"),
            }
            raise RuntimeError("synthetic warm-up failure")

        cleanup_order = []
        def capture(active):
            self.assertEqual(active["drainedSample"], drained)
            active.update(captured=True, summary={"completedTransactions": 1, "committedTransactions": 1})
            cleanup_order.append("capture")
            return active["summary"]

        def fake_cmd(argv, *args, **kwargs):
            cleanup_order.append("cleanup")
            return ""

        try:
            with tempfile.TemporaryDirectory() as directory:
                measurement.STATE = Path(directory) / ".state"
                measurement.ACTIVE_CLIENT = None
                measurement.ACTIVE_MONITOR = None
                measurement.ACTIVE_CREDENTIAL = None
                measurement.ACTIVE_CLIENT_LAUNCH_ATTEMPTED = False
                with patch.object(sys, "argv", [str(SCRIPT)]), patch.object(
                    measurement, "run", side_effect=fail_after_client_start
                ), patch.object(measurement, "remote_client_status", return_value="RUNNING"), patch.object(
                    measurement,
                    "stop_pgbench",
                    return_value={"status": "0", "exitCode": 0, "signal": "SIGALRM"},
                ) as stop, patch.object(measurement, "wait_for_bench_drain", return_value=drained) as drain, patch.object(
                    measurement, "capture_pgbench_results", side_effect=capture
                ) as capture_logs, patch.object(measurement, "cmd", side_effect=fake_cmd), redirect_stderr(io.StringIO()):
                    result = measurement.main()

                self.assertEqual(result, 2)
                stop.assert_called_once()
                drain.assert_called_once_with(monitor, 120)
                capture_logs.assert_called_once()
                self.assertTrue(monitor.stopped)
                self.assertEqual(measurement.ACTIVE_CLIENT["stopResult"]["serverActivityAfterDrain"], drained)
                self.assertLess(cleanup_order.index("capture"), cleanup_order.index("cleanup"))
                self.assertIsNone(measurement.ACTIVE_CREDENTIAL)
                self.assertFalse(measurement.ACTIVE_CLIENT_LAUNCH_ATTEMPTED)
        finally:
            measurement.ACTIVE_CLIENT = None
            measurement.ACTIVE_MONITOR = None
            measurement.ACTIVE_CREDENTIAL = None
            measurement.ACTIVE_CLIENT_LAUNCH_ATTEMPTED = False
            measurement.STATE = original_state


if __name__ == "__main__":
    unittest.main()
