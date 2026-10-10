import copy
import datetime as dt
import gzip
import hashlib
import importlib.util
import json
import shutil
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().with_name("verify_wal_retention.py")
SPEC = importlib.util.spec_from_file_location("wal_cleanup_guard", MODULE_PATH)
GUARD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GUARD)
SOURCE_ROOT = MODULE_PATH.parents[2]
BASE_EXPORT = SOURCE_ROOT / "recovery/backup-retention-review/exports/baseline-23gib/vacuum-backup-4b116309"
WAL_NAME = "000000010000004E00000050"


def write_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_fixture(root: Path):
    source_prepare = json.loads((SOURCE_ROOT / "baseline-23gib/.state/prepare.json").read_text())
    source_receipt = json.loads((BASE_EXPORT / "receipt.json").read_text())
    source_inventory = json.loads((SOURCE_ROOT / "baseline-23gib/.state/recovery/backup.json").read_text())
    backup_name = source_prepare["backup"]
    backup_documents = [item for item in source_inventory["items"]
                        if item.get("kind") == "Backup" and
                        item.get("metadata", {}).get("name") == backup_name]
    assert len(backup_documents) == 1
    source_backup_document = backup_documents[0]
    prepare_by_stack = {}
    process_by_stack = {}
    audit_rows = []
    for stack in GUARD.STACKS:
        state = json.loads((SOURCE_ROOT / stack / ".state/state.json").read_text())
        storage = json.loads((SOURCE_ROOT / stack / ".state/storage.json").read_text())
        prepare = copy.deepcopy(source_prepare)
        process = {"phase": "prepare", "stack": stack, "status": "complete", "exitCode": 0}
        if stack == "baseline-23gib":
            prepare["finalAudit"]["receipt"] = str(
                root / "baseline-23gib/.state/recovery/baseline-finalize-audit.json")
        prepare_path = root / stack / ".state/prepare.json"
        process_path = root / stack / ".state/prepare-process.json"
        write_json(root / stack / ".state/state.json", state)
        write_json(root / stack / ".state/storage.json", storage)
        write_json(prepare_path, prepare)
        write_json(process_path, process)
        prepare_by_stack[stack] = prepare
        process_by_stack[stack] = process

        backup_name = prepare["backup"]
        export_rel = Path("recovery/backup-retention-review/exports") / stack / backup_name
        export_dir = root / export_rel
        export_dir.mkdir(parents=True)
        for item in BASE_EXPORT.iterdir():
            if item.is_file():
                shutil.copyfile(item, export_dir / item.name)
        receipt = copy.deepcopy(source_receipt)
        receipt["stack"] = stack
        owner = state["ownerToken"]
        snapshot_group = state["snapshotResourceGroup"]
        archive_prefix = f"cnpg-vacuum-only/{owner}/hdd-pg/wals/"
        receipt["source"].update({
            "subscriptionId": storage["subscriptionId"],
            "resourceGroup": storage["resourceGroup"],
            "storageAccountId": storage["accountId"],
            "storageAccountName": storage["accountName"],
            "blobHost": storage["accountName"] + ".blob.core.windows.net",
            "container": storage["containerName"],
            "destinationPrefix": f"cnpg-vacuum-only/{owner}/",
            "serverName": "hdd-pg",
            "archivePrefix": archive_prefix,
        })
        for index, (role, snapshot) in enumerate(zip(("data", "wal"), receipt["snapshots"])):
            snapshot["snapshot_name"] = prepare["replicaSnapshots"][role]
            snapshot["snapshot_uid"] = f"test-vss-{stack}-{role}"
            snapshot["snapshot_content_name"] = f"test-content-{stack}-{role}"
            snapshot["snapshot_content_uid"] = f"test-vsc-{stack}-{role}"
            snapshot["azure_snapshot_id"] = (
                f"/subscriptions/{state['subscriptionId']}/resourceGroups/{snapshot_group}/"
                f"providers/Microsoft.Compute/snapshots/{stack}-{role}-snapshot")
            snapshot["azure_snapshot_unique_id"] = f"test-azure-snapshot-{stack}-{role}"
            audit_rows.append({
                "id": snapshot["azure_snapshot_id"],
                "uniqueId": snapshot["azure_snapshot_unique_id"],
                "tags": {"reprotoken": owner, "reproPurpose": "premiumv2-vacuum-backup"},
                "state": "Succeeded", "accessState": "Available",
                "directCanNotDelete": True, "groupCanNotDelete": False,
            })
        if stack == "baseline-23gib":
            recovery_dir = root / stack / ".state/recovery"
            recovery_dir.mkdir(parents=True, exist_ok=True)
            for record in ("baseline-finalize-audit.json", "backup.json"):
                shutil.copyfile(SOURCE_ROOT / stack / ".state/recovery" / record,
                                recovery_dir / record)
            finalizer_path = recovery_dir / "baseline-finalize-audit.json"
            finalizer = json.loads(finalizer_path.read_text())
            for snapshot in receipt["snapshots"]:
                finalizer["retainedSnapshots"].append({
                    "id": snapshot["azure_snapshot_id"],
                    "uniqueId": snapshot["azure_snapshot_unique_id"],
                })
            write_json(finalizer_path, finalizer)
            # The baseline's saved Backup inventory is the frozen source object.
        else:
            source_backup_document = copy.deepcopy(source_backup_document)
            source_backup_document["metadata"]["labels"]["repro.stack.token"] = owner
        for item in receipt["exportedObjects"]:
            old_prefix = source_receipt["source"]["archivePrefix"]
            assert item["objectName"].startswith(old_prefix)
            item["objectName"] = archive_prefix + item["objectName"][len(old_prefix):]
        write_json(export_dir / "receipt.json", receipt)

        if stack in ("50gib", "100gib"):
            evidence_dir = (root / "recovery/backup-retention-review/evidence" /
                            stack / backup_name)
            backup_document_path = evidence_dir / "backup.json"
            write_json(backup_document_path, source_backup_document)
            capture = {
                "status": "complete", "stack": stack,
                "backup": {"name": backup_name, "uid": receipt["backup"]["uid"]},
                "replicaSnapshots": copy.deepcopy(prepare["replicaSnapshots"]),
                "sourceReceipts": {
                    "prepareMetadataSha256": digest(prepare_path),
                    "prepareProcessSha256": digest(process_path),
                },
                "snapshotLineage": copy.deepcopy(receipt["snapshots"]),
                "files": {"backup.json": {
                    "bytes": backup_document_path.stat().st_size,
                    "sha256": digest(backup_document_path),
                }},
            }
            capture_path = evidence_dir / "capture.json"
            write_json(capture_path, capture)
        if stack == "baseline-23gib":
            # Do not mutate the original source object used by later fixture stacks.
            source_backup_document = backup_documents[0]

    audit = {
        "checkedAtUtc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "preservedPriorSnapshots": True,
        "allSnapshotsProtected": True,
        "priorSnapshotCount": len(audit_rows),
        "currentSnapshotCount": len(audit_rows),
        "missing": [], "changedIdentities": [], "unprotected": [],
        "snapshots": audit_rows,
    }
    write_json(root / GUARD.RETENTION_AUDIT, audit)
    return root


class WalRetentionGuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = build_fixture(Path(self.temp.name))

    def tearDown(self):
        self.temp.cleanup()

    def export_file(self, stack: str, filename: str = WAL_NAME) -> Path:
        backup = json.loads((self.root / stack / ".state/prepare.json").read_text())["backup"]
        return (self.root / "recovery/backup-retention-review/exports" /
                stack / backup / filename)

    def receipt_file(self, stack: str) -> Path:
        backup = json.loads((self.root / stack / ".state/prepare.json").read_text())["backup"]
        return (self.root / "recovery/backup-retention-review/exports" /
                stack / backup / "receipt.json")

    def test_complete_three_stack_archive_matches_prepare_and_retention_inventory(self):
        result = GUARD.verify(self.root)
        self.assertEqual(result["status"], "complete")
        self.assertEqual([x["stack"] for x in result["stacks"]], list(GUARD.STACKS))
        self.assertTrue(all(x["pairedSnapshotsVerified"] == 2 for x in result["stacks"]))
        self.assertEqual(result["cloudCalls"], 0)

    def test_missing_50gib_export_receipt_fails_closed(self):
        self.receipt_file("50gib").unlink()
        with self.assertRaisesRegex(GUARD.WalRetentionError, "Required local evidence is missing"):
            GUARD.verify(self.root)

    def test_mismatched_100gib_replica_snapshot_pair_fails_closed(self):
        receipt = json.loads(self.receipt_file("100gib").read_text())
        receipt["snapshots"][1]["snapshot_name"] = "different-wal-snapshot"
        write_json(self.receipt_file("100gib"), receipt)
        with self.assertRaisesRegex(GUARD.WalRetentionError, "exported snapshot pair differs"):
            GUARD.verify(self.root)

    def test_lost_required_wal_segment_fails_closed(self):
        self.export_file("50gib").unlink()
        with self.assertRaisesRegex(GUARD.WalRetentionError, "Required local evidence is missing"):
            GUARD.verify(self.root)

    def test_changed_wal_bytes_fail_sha256_check(self):
        path = self.export_file("100gib")
        with path.open("r+b") as stream:
            stream.seek(0)
            stream.write(b"X")
        with self.assertRaisesRegex(GUARD.WalRetentionError, "size or SHA-256 changed"):
            GUARD.verify(self.root)

    def test_snapshot_uid_change_in_current_retention_audit_fails_closed(self):
        audit_path = self.root / GUARD.RETENTION_AUDIT
        audit = json.loads(audit_path.read_text())
        audit["snapshots"][0]["uniqueId"] = "changed-azure-unique-id"
        write_json(audit_path, audit)
        with self.assertRaisesRegex(GUARD.WalRetentionError, "snapshot ID/UID is missing, changed"):
            GUARD.verify(self.root)

    def test_untrusted_symlinked_wal_file_fails_closed(self):
        target = self.export_file("50gib")
        target.unlink()
        external = Path(self.temp.name) / "outside-wal"
        external.write_bytes(b"not an export")
        target.symlink_to(external)
        with self.assertRaisesRegex(GUARD.WalRetentionError, "Symlink is not allowed"):
            GUARD.verify(self.root)

    def test_export_range_must_match_frozen_backup_status(self):
        receipt_path = self.receipt_file("100gib")
        original = json.loads(receipt_path.read_text())
        mutations = {
            "beginLSN": "4E/50000029",
            "endLSN": "4E/50000139",
            "beginWal": "000000010000004E00000051",
            "endWal": "000000010000004E00000051",
            "timeline": 2,
        }
        for field, value in mutations.items():
            with self.subTest(field=field):
                receipt = copy.deepcopy(original)
                receipt["backup"][field] = value
                write_json(receipt_path, receipt)
                with self.assertRaisesRegex(
                        GUARD.WalRetentionError,
                        "range/timeline differs from the frozen Backup status"):
                    GUARD.verify(self.root)
        write_json(receipt_path, original)

    def test_compression_metadata_uses_exporter_suffix_values(self):
        base = "000000010000004E00000050"
        for suffix, compression in ((".gz", "gz"), (".bz2", "bz2"), (".xz", "xz")):
            with self.subTest(suffix=suffix):
                record = {"objectName": "owned/prefix/" + base + suffix,
                          "compression": compression}
                self.assertEqual(GUARD._expected_suffix(record, base, "wal"), suffix)

    def test_exported_compressed_object_uses_exporter_suffix_metadata(self):
        stack = "50gib"
        receipt_path = self.receipt_file(stack)
        receipt = json.loads(receipt_path.read_text())
        wal_record = next(item for item in receipt["exportedObjects"] if item["kind"] == "wal")
        original = self.export_file(stack)
        compressed_path = original.with_name(original.name + ".gz")
        compressed_path.write_bytes(gzip.compress(original.read_bytes()))
        original.unlink()
        wal_record["objectName"] += ".gz"
        wal_record["bytes"] = compressed_path.stat().st_size
        wal_record["sha256"] = digest(compressed_path)
        wal_record["compression"] = "gz"
        write_json(receipt_path, receipt)
        result = GUARD.verify(self.root)
        self.assertEqual(result["status"], "complete")

    def test_timeline_history_object_uses_archive_prefix_without_segment_directory(self):
        root = Path(self.temp.name) / "single-object"
        export_dir = root / "export"
        export_dir.mkdir(parents=True)
        prefix = "owned/archive/"
        base = "00000002.history"
        content = b"1 4E/50000028 timeline switch\n"
        (export_dir / base).write_bytes(content)
        record = {
            "objectName": prefix + base,
            "bytes": len(content),
            "sha256": digest(export_dir / base),
            "uncompressedBytes": len(content),
            "kind": "timeline-history",
        }
        path, decoded = GUARD._read_export_object(
            root, export_dir, prefix, record, base, "timeline-history", None)
        self.assertEqual(path, export_dir / base)
        self.assertEqual(decoded, content)

    def test_wal_sequence_advances_across_log_boundary(self):
        actual = GUARD.EXPORTER.wal_sequence(
            "0000000100000000000000FF", "000000010000000100000000", 16 * 1024 * 1024)
        self.assertEqual(actual, [
            "0000000100000000000000FF",
            "000000010000000100000000",
        ])


if __name__ == "__main__":
    unittest.main(verbosity=2)
