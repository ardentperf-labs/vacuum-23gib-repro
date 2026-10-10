import unittest
from pathlib import Path
import sys
import hashlib
import json
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parent))

from export_cnpg_wal import (
    ExportError,
    backup_history_name,
    compression_for_object,
    parse_history,
    verify_preparation_capture_inputs,
    wal_sequence,
)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + "\n")
    return path


class WalRangeTests(unittest.TestCase):
    def test_baseline_backup_requires_one_exact_segment(self):
        self.assertEqual(
            wal_sequence(
                "000000010000004E00000050",
                "000000010000004E00000050",
                16 * 1024 * 1024,
            ),
            ["000000010000004E00000050"],
        )

    def test_range_crossing_log_file_is_contiguous(self):
        self.assertEqual(
            wal_sequence(
                "0000000100000000000000FF",
                "000000010000000100000001",
                16 * 1024 * 1024,
            ),
            [
                "0000000100000000000000FF",
                "000000010000000100000000",
                "000000010000000100000001",
            ],
        )

    def test_cross_timeline_range_fails_closed(self):
        with self.assertRaises(ExportError):
            wal_sequence(
                "000000010000004E00000050",
                "000000020000004E00000050",
                16 * 1024 * 1024,
            )

    def test_invalid_segment_index_fails_closed(self):
        with self.assertRaises(ExportError):
            wal_sequence(
                "000000010000000000000100",
                "000000010000000000000100",
                16 * 1024 * 1024,
            )

    def test_history_name_uses_backup_start_offset_in_segment(self):
        self.assertEqual(
            backup_history_name(
                "4E/50000028",
                "000000010000004E00000050",
                16 * 1024 * 1024,
            ),
            "000000010000004E00000050.00000028.backup",
        )

    def test_history_name_rejects_lsn_wal_mismatch(self):
        with self.assertRaises(ExportError):
            backup_history_name(
                "4E/51000028",
                "000000010000004E00000050",
                16 * 1024 * 1024,
            )


class ArchiveMetadataTests(unittest.TestCase):
    def test_compression_must_match_saved_configuration(self):
        self.assertEqual(compression_for_object({"compression": "gzip"}, "segment.gz"), ".gz")
        self.assertEqual(compression_for_object({}, "segment"), "")
        with self.assertRaises(ExportError):
            compression_for_object({}, "segment.gz")

    def test_backup_history_must_match_exact_lsn_timeline_and_label(self):
        record = {
            "begin_lsn": "4E/50000028",
            "end_lsn": "4E/50000138",
            "begin_wal": "000000010000004E00000050",
            "end_wal": "000000010000004E00000050",
            "backup_name": "vacuum-backup-example",
            "timeline": 1,
        }
        history = """START WAL LOCATION: 4E/50000028 (file 000000010000004E00000050)\nCHECKPOINT LOCATION: 4E/50000060\nSTOP WAL LOCATION: 4E/50000138 (file 000000010000004E00000050)\nBACKUP METHOD: streamed\nSTART TIME: 2026-10-06 18:08:44 UTC\nSTOP TIME: 2026-10-06 19:03:21 UTC\nLABEL: vacuum-backup-example\nSTART TIMELINE: 1\nSTOP TIMELINE: 1\n"""
        parse_history(history.encode(), record)
        with self.assertRaises(ExportError):
            parse_history(history.replace("STOP TIMELINE: 1", "STOP TIMELINE: 2").encode(), record)


class OriginalPreparationSchemaTests(unittest.TestCase):
    def make_completed_fixture(self, root):
        owner = "a" * 32
        source_id = "/subscriptions/sub/resourceGroups/source/providers/Microsoft.Compute/snapshots/keeper"
        restore_id = "/subscriptions/sub/resourceGroups/snapshots/providers/Microsoft.Compute/snapshots/restore-copy"
        stage_disk_id = "/subscriptions/sub/resourceGroups/nodes/providers/Microsoft.Compute/disks/stage"
        stack_root = root / "50gib"
        state_path = stack_root / ".state/state.json"
        prepare_path = stack_root / ".state/prepare.json"
        process_path = stack_root / ".state/prepare-process.json"
        script_path = stack_root / "02-prepare.sh"
        manifest_path = stack_root / "source-manifest.json"
        config_path = stack_root / "run-config.json"
        helper_path = root / "recovery/source-restore/stage-allowlisted-source.sh"
        stage_path = stack_root / ".state/source-stage/receipt-example.json"
        helper_path.parent.mkdir(parents=True, exist_ok=True)
        helper_path.write_text("#!/bin/sh\nexit 0\n")
        script_path.parent.mkdir(parents=True, exist_ok=True)
        script_path.write_text("#!/bin/bash\n# pinned fixture preparation\n")
        manifest = {"subscriptionId": "sub", "sourceSnapshotId": source_id, "uniqueId": "source-uid"}
        prepared = {
            "version": 1,
            "status": "complete",
            "sourceSnapshotId": source_id,
            "restoreCopySnapshotId": restore_id,
            "fixtureTargetHeapGiB": 50,
            "retainedSnapshotResourceGroup": "snapshots",
            "cluster": "repro/hdd-pg",
            "instances": 2,
            "replicaCaughtUpToLsn": "1/2",
            "backup": "vacuum-backup-example",
            "replicaSnapshots": {"data": "vacuum-data-example", "wal": "vacuum-wal-example"},
            "clientCreated": False,
        }
        write_json(manifest_path, manifest)
        write_json(config_path, {"targetHeapGiB": 50})
        write_json(prepare_path, prepared)
        stage_receipt = {
            "version": 1,
            "status": "ready",
            "ownerToken": owner,
            "sourceSnapshotId": source_id,
            "sourceSnapshotUniqueId": "source-uid",
            "restoreSnapshotId": restore_id,
            "restoreSnapshotUniqueId": "restore-uid",
            "stagingDiskId": stage_disk_id,
            "stagingDiskUniqueId": "disk-uid",
        }
        write_json(stage_path, stage_receipt)
        state = {
            "status": "ready",
            "ownerToken": owner,
            "subscriptionId": "sub",
            "resourceGroup": "test-rg",
            "nodeResourceGroup": "nodes",
            "snapshotResourceGroup": "snapshots",
            "snapshotSourceCopyId": restore_id,
            "snapshotSourceStagingDiskId": stage_disk_id,
            "snapshotSourceStageReceiptPath": str(stage_path),
        }
        write_json(state_path, state)
        process = {
            "stack": "50gib",
            "phase": "prepare",
            "status": "complete",
            "exitCode": 0,
            "finishedAtUtc": "2026-10-06T00:00:00Z",
            "scriptSha256": {"02-prepare.sh": digest(script_path)},
            "sourceStageHelperSha256": digest(helper_path),
        }
        write_json(process_path, process)
        stage_rel = "50gib/.state/source-stage/receipt-example.json"
        hashes = {
            "stateSha256": digest(state_path),
            "sourceManifestSha256": digest(manifest_path),
            "prepareProcessSha256": digest(process_path),
            "prepareMetadataSha256": digest(prepare_path),
            "prepareScriptSha256": digest(script_path),
            "sourceStageReceiptPath": stage_rel,
            "sourceStageReceiptSha256": digest(stage_path),
            "sourceStageHelperSha256": digest(helper_path),
        }
        retirement = {
            "version": 1,
            "phase": "complete",
            "preparation": {"50gib": hashes},
            "retiredStagingDisks": [{
                "stack": "50gib",
                "id": stage_disk_id,
                "uniqueId": "disk-uid",
                "sourceSnapshotId": source_id,
                "sourceSnapshotUniqueId": "source-uid",
                "restoreSnapshotId": restore_id,
                "restoreSnapshotUniqueId": "restore-uid",
                "sourceStageReceiptPath": stage_rel,
                "sourceStageReceiptSha256": digest(stage_path),
                "absentVerified": True,
            }],
        }
        write_json(root / "preflight/staging-retirement.json", retirement)
        return prepare_path, retirement

    def test_original_prepare_json_without_final_audit_is_accepted(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            prepare_path, _ = self.make_completed_fixture(root)
            result = verify_preparation_capture_inputs("50gib", root)
            self.assertEqual(result["prepared"]["backup"], "vacuum-backup-example")
            self.assertEqual(result["prepared"]["replicaSnapshots"], {
                "data": "vacuum-data-example", "wal": "vacuum-wal-example"})
            self.assertNotIn("finalAudit", result["prepared"])
            self.assertTrue(prepare_path.is_file())

    def test_changed_prepare_metadata_hash_blocks_capture(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            prepare_path, _ = self.make_completed_fixture(root)
            prepared = json.loads(prepare_path.read_text())
            prepared["replicaSnapshots"]["wal"] = "changed-wal"
            write_json(prepare_path, prepared)
            with self.assertRaises(ExportError):
                verify_preparation_capture_inputs("50gib", root)


if __name__ == "__main__":
    unittest.main()
