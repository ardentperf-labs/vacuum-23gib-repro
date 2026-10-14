import importlib.util
import json
import math
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("plot_signal_comparison", HERE / "plot_signal_comparison.py")
plotter = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = plotter
spec.loader.exec_module(plotter)

BASE = 1_800_000_000


def utc(epoch):
    return plotter.utc_text(epoch)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value) + "\n")


def write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def client_row(epoch, *, waiting=10, sample_epoch=None):
    backends = []
    for index in range(10):
        active_sync = index < waiting
        backends.append({"pid": 4000+index, "state": "active" if active_sync else "idle",
                         "waitEvent": "SyncRep" if active_sync else "ClientRead",
                         "waitEventType": "IPC" if active_sync else "Client",
                         "xactAgeSeconds": max(0, epoch-(BASE-30-index)) if active_sync else None,
                         "queryAgeSeconds": max(0, epoch-(BASE-30-index)) if active_sync else 0.1})
    return {"observedAtUtc": utc(epoch), "sampleEpoch": epoch if sample_epoch is None else sample_epoch,
            "sampleStatus": "ok", "benchSessions": 10, "unrelatedAppSessions": 0,
            "committedTransactions": 5, "backends": backends}


def make_lane(root, lane, *, include_logs=True, signal_sent=None):
    measurement = root / lane / "results" / "measurement"
    start, vacuum_end, observation_end = BASE, BASE+40, BASE+60
    mode = "sigterm" if lane == "sigterm" else "control"
    timing = {"vacuumStart": utc(start), "vacuumComplete": utc(vacuum_end),
              "observationEnd": utc(observation_end), "testComplete": True,
              "remoteVacuumExitCode": 0, "interventionMode": mode,
              "clientStop": {"requestedAtUtc": utc(start+65)}}
    write_json(measurement / "timing.json", timing)
    write_json(measurement / "client-config.json", {"tool": "pgbench", "version": "16.15",
        "clients": 10, "targetTps": 5, "synchronousCommit": "on", "statementTimeoutSeconds": 0,
        "protocol": "simple", "maxTries": 1, "noVacuum": True, "warmupSeconds": 300,
        "stopSignal": "SIGALRM", "stopSignalSemantics": "drains active transactions"})
    write_jsonl(measurement / "events.jsonl", [
        {"event": "client_started", "time": utc(start-60)},
        {"event": "sample_attempt_failed", "time": utc(start+32), "phase": "vacuum", "error": "synthetic stream gap"},
    ])
    intervention = {"mode": mode, "status": "sent" if lane == "sigterm" else "marked",
                    "executedAtUtc": utc(start+20),
                    "signalResult": {"signalSent": signal_sent if signal_sent is not None else lane == "sigterm"}}
    write_json(measurement / "walreceiver-intervention.json", intervention)

    sample_rows = []
    for offset, sender in ((0, "streaming"), (5, "catchup"), (10, "streaming"),
                           (15, None), (20, "streaming"), (30, "streaming"), (35, "streaming"),
                           (40, "streaming"), (50, "streaming"), (60, "streaming")):
        senders = [] if sender is None else [{"state": sender, "syncState": "quorum"}]
        primary = {"epoch": start+offset, "ts": utc(start+offset), "flushLsn": "1/40000000",
                   "senders": senders, "vacuumProgress": {"phase": "scanning heap" if offset < 30 else "vacuuming heap"}}
        replica = {"epoch": start+offset, "replayLsn": "1/00000000"}
        sample_rows.append({"seq": offset, "primary": primary, "replica": replica})
    write_jsonl(measurement / "samples.jsonl", sample_rows)
    session_rows = [client_row(start+offset, waiting=10 if offset < 20 else 4,
                               sample_epoch=start+offset+5)
                    for offset in (0, 1, 2, 3, 4, 5, 10, 20, 40, 60)]
    write_jsonl(measurement / "client" / "sessions.jsonl", session_rows)

    if include_logs:
        raw = measurement / "client" / "raw"
        raw.mkdir(parents=True, exist_ok=True)
        logs = []
        offsets = (1, 8, 12, 22, 31, 45, 50, 53, 56, 58)
        for index, offset in enumerate(offsets, 1):
            timestamp = start+offset
            completion_sec = int(timestamp)
            completion_us = int((timestamp-completion_sec)*1_000_000)
            logs.append(f"{index % 10} {index} 1000 0 {completion_sec} {completion_us} 100")
        (raw / "pgbench-log.0").write_text("\n".join(logs) + "\n")
        (raw / "pgbench.out").write_text("number of transactions actually processed: 10\nnumber of failed transactions: 0\n")
        write_json(measurement / "client-summary.json", {
            "completedTransactions": 10, "loggedCompletedTransactions": 10, "failedTransactions": 0,
            "pgbenchExitCode": 0, "validationErrors": [],
            "commitLogReconciliation": {"difference": 0, "possibleInflightAtStopRequest": 1}})
    return measurement


def make_recorded_failure_pilot(root):
    treatment = make_lane(root, "sigterm")
    control = make_lane(root, "control")
    long_end = BASE + 8400
    for measurement, lane in ((treatment, "sigterm"), (control, "control")):
        timing = json.loads((measurement / "timing.json").read_text())
        timing.update({"vacuumComplete": utc(BASE+800), "observationEnd": utc(long_end),
                       "clientStop": {"requestedAtUtc": utc(long_end+5)},
                       "remoteVacuumExitCode": 0})
        if lane == "sigterm":
            timing.update({"testComplete": False, "errors": ["initial pg_terminate_backend returned false"]})
        write_json(measurement / "timing.json", timing)

        samples_path = measurement / "samples.jsonl"
        samples, _ = plotter.read_jsonl(samples_path)
        samples.append({"seq": 8400, "primary": {"epoch": long_end, "ts": utc(long_end),
            "flushLsn": "1/50000000", "senders": [{"state": "streaming"}],
            "vacuumProgress": {"phase": "vacuuming heap"}},
            "replica": {"epoch": long_end, "replayLsn": "1/10000000"}})
        write_jsonl(samples_path, samples)

        sessions_path = measurement / "client" / "sessions.jsonl"
        sessions, _ = plotter.read_jsonl(sessions_path)
        sessions.append(client_row(long_end, waiting=0, sample_epoch=long_end+5))
        write_jsonl(sessions_path, sessions)

    intervention = json.loads((treatment / "walreceiver-intervention.json").read_text())
    scheduled = BASE + 600
    executed = BASE + 675
    pid, ticks, uid = 74, 433516, 26
    timestamp = utc(executed)
    mechanism = "OS kill -TERM after verifying process start ticks, ownership and walreceiver command line"
    intervention.update({"mode": "sigterm", "status": "sent", "afterVacuumStartSeconds": 600,
        "scheduledAtUtc": utc(scheduled), "executedAtUtc": timestamp,
        "timingDeviation": "SIGTERM was delivered after the scheduled time following a failed SQL signal attempt.",
        "signalResult": {"signal": "SIGTERM", "signalSent": True, "pid": pid,
                         "mechanism": mechanism}})
    write_json(treatment / "walreceiver-intervention.json", intervention)
    write_json(treatment / "walreceiver-os-sigterm-receipt.json", {
        "scheduledAtUtc": utc(scheduled), "attemptedAtUtc": timestamp, "returnCode": 0,
        "stdout": f"SIGNAL_UTC={timestamp}\nSIGNAL_SENT=SIGTERM PID={pid} START_TICKS={ticks}\n",
        "stderr": "", "pid": pid, "processStartTicks": ticks, "uid": uid,
        "mechanism": mechanism, "priorFunctionSentSignal": False})
    return treatment, control


class SignalComparisonPlotTests(unittest.TestCase):
    def test_full_pair_validation_series_markers_units_and_linear_png(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            make_lane(root, "sigterm")
            make_lane(root, "control")
            data = plotter.overview(root)
            treatment, control = data["runs"]
            self.assertFalse(data["previewOnly"])
            self.assertTrue(treatment["nativeLogValidation"]["valid"])
            self.assertEqual(len(treatment["series"]["nativeCompletedTps"]), 6)
            self.assertEqual(treatment["series"]["nativeCompletedTps"][0]["completedTransactions"], 2)
            self.assertEqual(treatment["timeline"]["signal"]["signalTimeUsed"], utc(BASE+20))
            self.assertIsNone(control["timeline"]["signal"].get("signalEpoch"))
            self.assertEqual(treatment["timeline"]["vacuumCompleteEpoch"], BASE+40)
            self.assertEqual(treatment["series"]["replicationTelemetry"][0]["senderState"], "streaming")
            self.assertAlmostEqual(treatment["series"]["replicationTelemetry"][0]["replicaReplayBacklogGiB"], 1.0)
            self.assertIn("no wait state is interpolated", data["comparison"]["waitObservationRule"])
            output = root / "results" / "comparison-overview.png"
            plotter.render(data, output)
            plotter.write_overview(data, output)
            self.assertGreater(output.stat().st_size, 10_000)
            persisted = json.loads(output.with_suffix(".json").read_text())
            self.assertEqual(persisted["comparison"]["units"]["replicaReplayBacklogGiB"], "GiB (2^30 bytes)")

    def test_gaps_and_failed_samples_break_sampled_wait_lines(self):
        points = [{"epoch": BASE, "syncRepWriters": 10},
                  {"epoch": BASE+1, "syncRepWriters": 10},
                  {"epoch": BASE+2, "syncRepWriters": 10},
                  {"epoch": BASE+8, "syncRepWriters": 0}]
        errors = [{"mappedDbEpoch": BASE+1.5}]
        x, y = plotter.series_with_breaks(points, "syncRepWriters", BASE,
                                          plotter.CLIENT_SAMPLE_GAP_SECONDS, errors)
        self.assertTrue(any(math.isnan(value) for value in y))
        self.assertEqual(plotter.gap_metadata(points, 2.5, errors)["maskedIntervalCount"], 2)

    def test_native_count_mismatch_rejects_final_graph(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            measurement = make_lane(root, "sigterm")
            make_lane(root, "control")
            (measurement / "client" / "raw" / "pgbench.out").write_text(
                "number of transactions actually processed: 7\nnumber of failed transactions: 0\n")
            with self.assertRaisesRegex(RuntimeError, "Native pgbench log validation failed"):
                plotter.overview(root)

    def test_incomplete_native_logs_are_preview_only_and_emit_no_zero_tps(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            make_lane(root, "sigterm", include_logs=False)
            make_lane(root, "control", include_logs=False)
            data = plotter.overview(root, preview=True)
            self.assertTrue(data["previewOnly"])
            for run in data["runs"]:
                self.assertFalse(run["nativeLogValidation"]["valid"])
                self.assertEqual(run["series"]["nativeCompletedTps"], [])

    def test_preview_cli_refuses_to_write_inside_final_results_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(SystemExit):
                plotter.main(["--experiment-root", str(root), "--preview-incomplete",
                              "--output-directory", str(root / "results")])

    def test_recorded_late_signal_failure_override_requires_complete_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            treatment, _ = make_recorded_failure_pilot(root)
            with self.assertRaisesRegex(RuntimeError, "testComplete=true"):
                plotter.overview(root)

            data = plotter.overview(root, allow_recorded_test_failure=True)
            self.assertTrue(data["allowRecordedTestFailure"])
            run = data["runs"][0]
            override = run["timeline"]["recordedTestFailureOverride"]
            self.assertEqual(override["rawTimingErrors"], ["initial pg_terminate_backend returned false"])
            self.assertEqual(override["nativeLogValidation"]["validLogRows"], 10)
            self.assertTrue(override["nativeLogValidation"]["reconciliationValidated"])
            self.assertTrue(override["osReceipt"]["stdoutSignalTimestampMatchesCanonical"])
            self.assertGreaterEqual(run["timeline"]["observationDurationSeconds"], 8400)
            self.assertIn("at least 8400 seconds of observation",
                          data["comparison"]["pilotFailureOverrideRule"])

            output = root / "pilot.png"
            plotter.render(data, output)
            self.assertGreater(output.stat().st_size, 10_000)

    def test_recorded_failure_override_rejects_bad_native_counts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            treatment, _ = make_recorded_failure_pilot(root)
            (treatment / "client" / "raw" / "pgbench.out").write_text(
                "number of transactions actually processed: 7\nnumber of failed transactions: 0\n")
            with self.assertRaisesRegex(RuntimeError, "Native pgbench log validation failed"):
                plotter.overview(root, allow_recorded_test_failure=True)

    def test_recorded_failure_override_rejects_short_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            treatment, _ = make_recorded_failure_pilot(root)
            timing_path = treatment / "timing.json"
            timing = json.loads(timing_path.read_text())
            timing["observationEnd"] = utc(BASE+8399)
            timing["clientStop"] = {"requestedAtUtc": utc(BASE+8404)}
            write_json(timing_path, timing)
            with self.assertRaisesRegex(RuntimeError, "at least 8400 seconds"):
                plotter.overview(root, allow_recorded_test_failure=True)

    def test_recorded_failure_override_rejects_receipt_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            treatment, _ = make_recorded_failure_pilot(root)
            receipt_path = treatment / "walreceiver-os-sigterm-receipt.json"
            receipt = json.loads(receipt_path.read_text())
            receipt["processStartTicks"] += 1
            write_json(receipt_path, receipt)
            with self.assertRaisesRegex(RuntimeError, "PID/start ticks do not match"):
                plotter.overview(root, allow_recorded_test_failure=True)

    def test_recorded_failure_override_cannot_accept_incomplete_vacuum_or_control_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            treatment, control = make_recorded_failure_pilot(root)
            timing_path = treatment / "timing.json"
            timing = json.loads(timing_path.read_text())
            timing["vacuumComplete"] = None
            write_json(timing_path, timing)
            with self.assertRaisesRegex(RuntimeError, "completed VACUUM marker"):
                plotter.overview(root, allow_recorded_test_failure=True)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, control = make_recorded_failure_pilot(root)
            timing_path = control / "timing.json"
            timing = json.loads(timing_path.read_text())
            timing["testComplete"] = False
            timing["errors"] = ["control runner failed"]
            write_json(timing_path, timing)
            with self.assertRaisesRegex(RuntimeError, "control: timing.json does not mark"):
                plotter.overview(root, allow_recorded_test_failure=True)

    def test_recorded_failure_override_rejects_client_window_not_covering_observation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            treatment, _ = make_recorded_failure_pilot(root)
            timing_path = treatment / "timing.json"
            timing = json.loads(timing_path.read_text())
            timing["clientStop"] = {"requestedAtUtc": utc(BASE+100)}
            write_json(timing_path, timing)
            with self.assertRaisesRegex(RuntimeError, "client stop request precedes observation end"):
                plotter.overview(root, allow_recorded_test_failure=True)


if __name__ == "__main__":
    unittest.main()
