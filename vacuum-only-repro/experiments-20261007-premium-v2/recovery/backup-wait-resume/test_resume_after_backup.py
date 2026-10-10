from __future__ import annotations

import importlib.util
import copy
from pathlib import Path
import tempfile
import unittest
from unittest import mock

MODULE_PATH = Path(__file__).with_name("resume_after_backup.py")
spec = importlib.util.spec_from_file_location("resume_after_backup", MODULE_PATH)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)
CAPTURE_PATH = Path(__file__).with_name("capture_prebackup_tmp.py")
capture_spec = importlib.util.spec_from_file_location("capture_prebackup_tmp", CAPTURE_PATH)
module_capture = importlib.util.module_from_spec(capture_spec)
assert capture_spec.loader is not None
capture_spec.loader.exec_module(module_capture)
CHAIN_BIND_PATH = Path(__file__).with_name("bind_terminal_process.py")
bind_spec = importlib.util.spec_from_file_location("bind_terminal_process", CHAIN_BIND_PATH)
module_bind = importlib.util.module_from_spec(bind_spec)
assert bind_spec.loader is not None
bind_spec.loader.exec_module(module_bind)


class ResumeEvidenceTests(unittest.TestCase):
    def test_numeric_copy_progress_normalizes_100_and_keeps_null_pending(self):
        self.assertEqual(module.completion_percent(100), 100)
        self.assertEqual(module.completion_percent(100.0), 100)
        self.assertEqual(module.completion_percent(None), None)
        with self.assertRaises(module.ResumeError):
            module.completion_percent("unknown")
        with self.assertRaises(module.ResumeError):
            module.completion_percent(True)

    def test_backup_snapshot_pending_access_state_is_waitable_but_failed_is_not(self):
        module.validate_backup_snapshot_access_state("Pending")
        module.validate_backup_snapshot_access_state("Available")
        with self.assertRaises(module.ResumeError):
            module.validate_backup_snapshot_access_state("Failed")

    def test_backup_pair_identity_remains_pinned_if_cached_evidence_is_mutated(self):
        original = {"id": "/snapshots/data", "uniqueId": "snapshot-uid",
            "volumeSnapshotUID": "vss-uid", "volumeSnapshotContent": "content-name",
            "volumeSnapshotContentUID": "content-uid", "sourcePVC": "hdd-pg-1",
            "sourcePV": "pv-data", "sourcePVCUID": "pvc-uid", "sourceDiskId": "/disks/data",
            "sourceDiskUniqueId": "disk-uid", "sourceUniqueId": "disk-uid", "createOption": "Copy"}
        pins = {}
        module.extend_snapshot_identity_pins(pins, "data-snapshot", original)
        module.extend_snapshot_identity_pins(pins, "wal-snapshot", {**original,
            "id": "/snapshots/wal", "uniqueId": "wal-uid", "sourcePVC": "hdd-pg-1-wal",
            "sourcePV": "pv-wal", "sourcePVCUID": "wal-pvc-uid", "sourceDiskId": "/disks/wal",
            "sourceDiskUniqueId": "wal-disk-uid", "sourceUniqueId": "wal-disk-uid"})
        before_refresh = copy.deepcopy(pins)
        mutable_cache = copy.deepcopy(pins)
        mutable_cache["data-snapshot"]["uniqueId"] = "replacement-uid"
        with self.assertRaises(module.ResumeError):
            module.validate_snapshot_identity_pins(before_refresh, mutable_cache)
        with self.assertRaises(module.ResumeError):
            module.extend_snapshot_identity_pins(pins, "data-snapshot",
                {**original, "uniqueId": "replacement-uid"})
        module.validate_snapshot_identity_pins(before_refresh, pins)

    def test_snapshot_vss_and_content_uids_pin_as_soon_as_they_become_observable(self):
        pins = {}
        vss_only = {"volumeSnapshotUID": "vss-uid", "sourcePVC": "hdd-pg-1",
                    "sourcePV": "pv-data", "sourcePVCUID": "pvc-uid",
                    "sourceDiskId": "/disks/data", "sourceDiskUniqueId": "disk-uid"}
        module.extend_snapshot_identity_pins(pins, "data-snapshot", vss_only)
        content_visible = {**vss_only, "id": "/snapshots/data", "volumeSnapshotContent": "content-a",
                           "volumeSnapshotContentUID": "content-uid-a"}
        module.extend_snapshot_identity_pins(pins, "data-snapshot", content_visible)
        with self.assertRaises(module.ResumeError):
            module.extend_snapshot_identity_pins(pins, "data-snapshot",
                {**content_visible, "volumeSnapshotContent": "content-b"})
        with self.assertRaises(module.ResumeError):
            module.extend_snapshot_identity_pins(pins, "data-snapshot",
                {**content_visible, "volumeSnapshotContentUID": "content-uid-b"})
        with self.assertRaises(module.ResumeError):
            module.extend_snapshot_identity_pins(pins, "data-snapshot",
                {**content_visible, "id": "/snapshots/recreated"})

    def test_archive_cache_port_forward_uses_kubectl_argv_and_reaps_process(self):
        calls = {}

        class PortForward:
            def __init__(self):
                self.signals = []
                self.wait_count = 0
                self.killed = False

            def send_signal(self, value):
                self.signals.append(value)

            def wait(self, timeout):
                self.wait_count += 1
                if self.wait_count == 1:
                    raise module.subprocess.TimeoutExpired("kubectl port-forward", timeout)
                return 0

            def kill(self):
                self.killed = True

        child = PortForward()

        def fake_popen(argv, **kwargs):
            calls["argv"] = argv
            calls["kwargs"] = kwargs
            return child

        with tempfile.TemporaryDirectory() as td:
            runner = module.Resume.__new__(module.Resume)
            runner.output = Path(td)
            runner.env = {"KUBECONFIG": "/pinned/kubeconfig"}
            with mock.patch.object(module.subprocess, "Popen", side_effect=fake_popen), \
                 mock.patch.object(module, "invoke", return_value="404") as curl:
                self.assertEqual(runner.archive_cache_status("hdd-pg-1"), "404")
        argv = calls["argv"]
        self.assertEqual(argv[:6], ["kubectl", "port-forward", "-n", "repro",
                                    "pod/hdd-pg-1", "--address=127.0.0.1"])
        self.assertRegex(argv[6], r"^[1-9][0-9]*:8010$")
        self.assertEqual(calls["kwargs"]["env"]["KUBECONFIG"], "/pinned/kubeconfig")
        self.assertEqual(child.signals, [module.signal.SIGTERM])
        self.assertTrue(child.killed)
        self.assertEqual(child.wait_count, 2)
        curl.assert_called_once()

    def test_backup_uid_owner_and_original_online_request_are_pinned(self):
        backup = {
            "metadata": {"name": "vacuum-backup-deadbeef", "namespace": "repro",
                         "uid": "12345678-1234-1234-1234-123456789abc",
                         "labels": {"repro.stack.token": "deadbeef" * 4}},
            "spec": {"cluster": {"name": "hdd-pg"}, "method": "volumeSnapshot",
                     "online": True, "onlineConfiguration": {
                         "waitForArchive": True, "immediateCheckpoint": False}},
            "status": {"phase": "running"},
        }
        self.assertEqual(module.validate_backup_object(
            backup, "vacuum-backup-deadbeef", "deadbeef" * 4,
            "12345678-1234-1234-1234-123456789abc"), "running")
        changed = {**backup, "metadata": {**backup["metadata"], "uid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}}
        with self.assertRaises(module.ResumeError):
            module.validate_backup_object(changed, "vacuum-backup-deadbeef", "deadbeef" * 4,
                                          "12345678-1234-1234-1234-123456789abc")
        changed = {**backup, "spec": {**backup["spec"], "online": False}}
        with self.assertRaises(module.ResumeError):
            module.validate_backup_object(changed, "vacuum-backup-deadbeef", "deadbeef" * 4,
                                          "12345678-1234-1234-1234-123456789abc")

    def test_backup_outputs_must_be_exact_distinct_data_wal_pair(self):
        good = {"status": {"snapshotBackupStatus": {"elements": [
            {"type": "PG_DATA", "name": "data-snapshot"},
            {"type": "PG_WAL", "name": "wal-snapshot"}]}}}
        self.assertEqual(module.extract_backup_pair(good), {"data": "data-snapshot", "wal": "wal-snapshot"})
        bad = {"status": {"snapshotBackupStatus": {"elements": [
            {"type": "PG_DATA", "name": "same"},
            {"type": "PG_WAL", "name": "same"}]}}}
        with self.assertRaises(module.ResumeError):
            module.extract_backup_pair(bad)
        duplicate = {"status": {"snapshotBackupStatus": {"elements": [
            {"type": "PG_DATA", "name": "data"},
            {"type": "PG_DATA", "name": "data2"}]}}}
        with self.assertRaises(module.ResumeError):
            module.extract_backup_pair(duplicate)

    def test_keeper_and_stack_owner_tokens_are_distinct_and_stage_bound(self):
        stack_token = "f1b90aec9e24f6a2008d6991738742cf"
        source_token = "16244c2c1353a0fc1b108ec455fd70c8"
        manifest = {"ownerToken": source_token}
        stage = {"ownerToken": stack_token,
                 "sourceManifest": {"ownerToken": source_token},
                 "sourceSnapshotMetadata": {"tags": {"reprotoken": source_token}},
                 "stagingDiskMetadata": {"tags": {"reprotoken": stack_token}},
                 "restoreSnapshotMetadata": {"tags": {"reprotoken": stack_token}}}
        self.assertEqual(module.validate_source_owner_binding(stack_token, manifest, stage), source_token)
        with self.assertRaises(module.ResumeError):
            module.validate_source_owner_binding(source_token, manifest, stage)
        wrong_stage = {**stage, "stagingDiskMetadata": {"tags": {"reprotoken": source_token}}}
        with self.assertRaises(module.ResumeError):
            module.validate_source_owner_binding(stack_token, manifest, wrong_stage)

    def test_control_capture_matches_original_generated_backup_and_source_annotations(self):
        token = "deadbeef" * 4
        source = "/subscriptions/sub/resourceGroups/keeper/providers/Microsoft.Compute/snapshots/keeper"
        copy = "/subscriptions/sub/resourceGroups/stack/providers/Microsoft.Compute/snapshots/copy"
        account, container = "ownedaccount", "cnpgbackups"
        backup_name = f"vacuum-backup-{token[:8]}"
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as td:
            control = Path(td) / "control"
            control.mkdir()
            (control / "backup.yaml").write_text(
                "apiVersion: postgresql.cnpg.io/v1\n"
                "kind: Backup\n"
                f'metadata: {{name: {backup_name}, namespace: repro, labels: {{repro.stack.token: "{token}"}}}}\n'
                "spec: {cluster: {name: hdd-pg}, method: volumeSnapshot, online: true, "
                "onlineConfiguration: {waitForArchive: true, immediateCheckpoint: false}}\n")
            (control / "cluster.yaml").write_text("\n".join((
                f'name: hdd-pg, namespace: repro, labels: {{repro.stack.token: "{token}"}}',
                f"instances: 1", f"imageName: {module.SYSIMAGE}",
                f"name: vacuum-data-{token[:8]}, kind: VolumeSnapshot", "storageClass: azure-premiumv2",
                f'vacuum-only-repro/source-snapshot-id: "{source}"',
                f'vacuum-only-repro/restore-copy-id: "{copy}"',
                f"destinationPath: https://{account}.blob.core.windows.net/{container}/cnpg-vacuum-only/{token}/",
                "serverName: hdd-pg", "waitForArchive: true", "immediateCheckpoint: false",
                "azureCredentials: {connectionString: {name: azure-barman-credentials, key: connection-string}}")))
            (control / "seed-path").write_text(
                f"/controller/tmp/seed-{token}-20261007T123456Z-1234\n")
            hashes = module.validate_control_capture(control, token, token[:8], account, container,
                                                      backup_name, f"vacuum-data-{token[:8]}", source, copy)
            self.assertEqual(set(hashes), {"backup.yaml", "cluster.yaml", "seed-path"})

    def test_capture_receipt_pins_process_and_exact_copied_files(self):
        file_hashes = {"backup.yaml": "a" * 64, "cluster.yaml": "b" * 64, "seed-path": "c" * 64}
        with tempfile.TemporaryDirectory() as td:
            state = Path(td) / ".state"
            source_temp = state / ".prepare.abCD123"
            receipt = {"version": 1, "stack": "100gib", "processStatusAtCapture": "running",
                       "originalScriptSha256": module.ORIGINAL_02_SHA,
                       "sourceStageHelperSha256": module.STAGE_HELPER_SHA,
                       "runningProcessReceiptSha256": "d" * 64,
                       "childPidAtCapture": 12345, "supervisorPidAtCapture": 12346,
                       "sourceTempPath": str(source_temp),
                       "runStartedAtUtc": "2026-10-07T02:00:00Z",
                       "capturedAtUtc": "2026-10-07T03:00:00Z", "filesSha256": file_hashes}
            process = {"stack": "100gib", "phase": "prepare", "status": "running",
                       "childPid": 12345, "supervisorPid": 12346,
                       "scriptSha256": {"02-prepare.sh": module.ORIGINAL_02_SHA},
                       "sourceStageHelperSha256": module.STAGE_HELPER_SHA,
                       "startedAtUtc": "2026-10-07T02:00:00Z"}
            self.assertEqual(module.validate_capture_receipt(receipt, process, file_hashes, state, "d" * 64),
                             str(source_temp))
            changed = {**receipt, "runningProcessReceiptSha256": "e" * 64}
            with self.assertRaises(module.ResumeError):
                module.validate_capture_receipt(changed, process, file_hashes, state, "d" * 64)
            changed = {**receipt, "filesSha256": {**file_hashes, "backup.yaml": "0" * 64}}
            with self.assertRaises(module.ResumeError):
                module.validate_capture_receipt(changed, process, file_hashes, state, "d" * 64)

    def test_terminal_binding_preserves_the_same_run_across_status_transition(self):
        running = {"stack": "100gib", "phase": "prepare", "status": "running",
                   "startedAtUtc": "2026-10-07T02:00:00Z", "supervisorPid": 12346,
                   "childPid": 12345, "command": ["bash", "02-prepare.sh"],
                   "scriptSha256": {"02-prepare.sh": module.ORIGINAL_02_SHA},
                   "sourceStageHelperSha256": module.STAGE_HELPER_SHA}
        terminal = {**running, "status": "failed", "exitCode": 1,
                    "finishedAtUtc": "2026-10-07T04:01:00Z"}
        capture = {"capturedAtUtc": "2026-10-07T03:00:00Z"}
        binding = {"runIdentity": {k: running[k] for k in (
                       "stack", "phase", "startedAtUtc", "supervisorPid", "childPid",
                       "command", "scriptSha256", "sourceStageHelperSha256")},
                   "runningProcessReceiptSha256": "a" * 64,
                   "terminalProcessReceiptSha256": "b" * 64,
                   "captureReceiptSha256": "c" * 64,
                   "terminalStatus": "failed", "terminalObservedAtUtc": "2026-10-07T04:02:00Z"}
        module.validate_process_identity_chain(running, terminal, binding,
                                              "a" * 64, "b" * 64, "c" * 64, capture)
        changed = {**binding, "runIdentity": {**binding["runIdentity"], "childPid": 777}}
        with self.assertRaises(module.ResumeError):
            module.validate_process_identity_chain(running, terminal, changed,
                                                  "a" * 64, "b" * 64, "c" * 64, capture)

    def test_capture_helper_accepts_exact_absolute_supervisor_argv(self):
        token = "f1b90aec9e24f6a2008d6991738742cf"
        state = {"version": 1, "status": "ready", "ownerToken": token}
        process = {"stack": "100gib", "phase": "prepare", "status": "running",
                   "scriptSha256": {"02-prepare.sh": module_capture.ORIGINAL_02_SHA},
                   "sourceStageHelperSha256": module_capture.STAGE_HELPER_SHA,
                   "command": ["bash", str(module_capture.STACK / "02-prepare.sh")],
                   "childPid": 12345, "supervisorPid": 12346}
        child = ["bash", str(module_capture.STACK / "02-prepare.sh")]
        supervisor = ["python3", "-u", str(module_capture.ROOT / "run-phase.py"),
                      str(module_capture.STACK), "prepare"]
        self.assertEqual(module_capture.validate_running_receipt(
            process, state, (module_capture.STACK / "02-prepare.sh").resolve(),
            module_capture.ORIGINAL_02_SHA, module_capture.STAGE_HELPER_SHA,
            child, supervisor), (12345, 12346))
        wrong = ["python3", "-u", str(module_capture.ROOT / "run-phase.py"),
                 "/tmp/not-the-stack", "prepare"]
        with self.assertRaises(module_capture.CaptureError):
            module_capture.validate_running_receipt(
                process, state, (module_capture.STACK / "02-prepare.sh").resolve(),
                module_capture.ORIGINAL_02_SHA, module_capture.STAGE_HELPER_SHA,
                child, wrong)

    def test_seed_event_is_bound_to_job_uid_and_replica_chronology(self):
        event = {"involvedObject": {"kind": "Job", "name": "hdd-pg-2-snapshot-recovery",
                                    "uid": "job-uid"},
                 "reason": "Completed", "message": "Job completed",
                 "eventTime": "2026-10-07T03:01:00Z"}
        selected = module.select_completed_seed_event(
            [event], "job-uid", "2026-10-07T03:00:00Z", "2026-10-07T03:02:00Z")
        self.assertEqual(selected, event)
        with self.assertRaises(module.ResumeError):
            module.select_completed_seed_event(
                [event], "different-job", "2026-10-07T03:00:00Z", "2026-10-07T03:02:00Z")
        with self.assertRaises(module.ResumeError):
            module.select_completed_seed_event(
                [event], "job-uid", "2026-10-07T03:02:00Z", "2026-10-07T03:03:00Z")


if __name__ == "__main__":
    unittest.main()
