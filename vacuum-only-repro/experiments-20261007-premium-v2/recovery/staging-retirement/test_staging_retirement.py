"""Focused fail-closed tests for the staging retirement safety predicates."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).with_name("retire_staging_disks.py")
SPEC = importlib.util.spec_from_file_location("retire_staging_disks", SCRIPT)
retirement = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(retirement)


SUB = "00000000-0000-0000-0000-000000000000"
OWNER = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
NODE_RG = f"/subscriptions/{SUB}/resourceGroups/nodes-{OWNER}"
STAGE_ID = NODE_RG + "/providers/Microsoft.Compute/disks/restore-stage-test"
SOURCE_ID = f"/subscriptions/{SUB}/resourceGroups/source/providers/Microsoft.Compute/snapshots/source"
CHILD_ID = f"/subscriptions/{SUB}/resourceGroups/snapshots/providers/Microsoft.Compute/snapshots/child"


def stage_record():
    return {
        "stack": "50gib", "id": STAGE_ID, "uniqueId": "disk-uid",
        "sourceSnapshotId": SOURCE_ID, "sourceSnapshotUniqueId": "source-uid",
        "restoreSnapshotId": CHILD_ID, "restoreSnapshotUniqueId": "child-uid",
        "sourceStageReceiptPath": "50gib/.state/source-stage/receipt.json",
        "sourceStageReceiptSha256": "receipt-hash", "ownerToken": OWNER,
        "subscriptionId": SUB, "nodeResourceGroup": f"nodes-{OWNER}",
        "snapshotResourceGroupId": f"/subscriptions/{SUB}/resourceGroups/snapshots",
        "location": "eastus", "zone": "1",
    }


def stage_disk(**overrides):
    resource = {
        "id": STAGE_ID,
        "name": "restore-stage-test",
        "location": "eastus",
        "sku": {"name": "Standard_LRS"},
        "properties": {
            "uniqueId": "disk-uid", "provisioningState": "Succeeded",
            "diskState": "Unattached", "diskSizeGB": 512,
            "creationData": {"createOption": "Copy", "sourceResourceId": SOURCE_ID,
                             "sourceUniqueId": "source-uid"},
        },
        "tags": {"reprotoken": OWNER, "reproPurpose": "premiumv2-source-stage",
                 "sourceSnapshotId": SOURCE_ID},
    }
    for key, value in overrides.items():
        if key == "properties":
            resource["properties"].update(value)
        elif key == "tags":
            resource["tags"].update(value)
        else:
            resource[key] = value
    return resource


def p2v_disk(percent=100, option="Copy"):
    return {
        "id": NODE_RG + "/providers/Microsoft.Compute/disks/pvc-data",
        "sku": {"name": "PremiumV2_LRS"}, "zones": ["1"],
        "managedBy": "/subscriptions/x/resourceGroups/nodes/providers/Microsoft.Compute/virtualMachines/vm",
        "properties": {"uniqueId": "p2v-uid", "provisioningState": "Succeeded",
            "diskSizeGB": 512, "diskIOPSReadWrite": 3000, "diskMBpsReadWrite": 125,
            "logicalSectorSize": 512, "completionPercent": percent,
            "creationData": {"createOption": option}},
    }


def backup_snapshot(percent):
    return {
        "id": CHILD_ID,
        "sku": {"name": "Standard_ZRS"},
        "tags": {"reproPurpose": "premiumv2-vacuum-backup",
                 "source_volume_id": p2v_disk()["id"]},
        "properties": {"provisioningState": "Succeeded", "snapshotAccessState": "Available",
            "completionPercent": percent, "creationData": {"createOption": "Copy",
                "sourceResourceId": p2v_disk()["id"]}},
    }


def audit(unique_id="child-uid", direct=True, group=False):
    return {"preservedPriorSnapshots": True, "allSnapshotsProtected": True,
        "snapshots": [{"id": CHILD_ID, "uniqueId": unique_id,
                       "directCanNotDelete": direct, "groupCanNotDelete": group}]}


class StageIdentityTests(unittest.TestCase):
    def test_scope_must_be_the_owned_node_group(self):
        outside = stage_disk(id=STAGE_ID.replace(f"nodes-{OWNER}", "other-rg"))
        with self.assertRaises(retirement.SafetyError):
            retirement.validate_disk_scope(outside, NODE_RG)

    def test_stage_requires_exact_source_and_unique_identity(self):
        retirement.validate_stage_disk(stage_disk(), stage_record())
        changed = stage_disk(properties={"uniqueId": "replaced-disk-uid"})
        with self.assertRaises(retirement.SafetyError):
            retirement.validate_stage_disk(changed, stage_record())

    def test_attached_stage_is_never_deleted(self):
        attached = stage_disk(managedBy="/subscriptions/x/virtualMachines/other-vm")
        with self.assertRaises(retirement.SafetyError):
            retirement.validate_stage_disk(attached, stage_record())

    def test_owner_or_source_tag_change_fails_closed(self):
        changed = stage_disk(tags={"reprotoken": "wrong-owner"})
        with self.assertRaises(retirement.SafetyError):
            retirement.validate_stage_disk(changed, stage_record())

    def test_absent_stage_requires_matching_accepted_delete_journal(self):
        record = stage_record()
        self.assertFalse(retirement.stage_absent_is_authorized(record, None))
        journal = {"phase": "delete-accepted", "deleteAccepted": True,
            "id": record["id"], "uniqueId": record["uniqueId"],
            "sourceStageReceiptSha256": record["sourceStageReceiptSha256"],
            "retirementScriptSha256": retirement.sha256_file(SCRIPT)}
        self.assertTrue(retirement.stage_absent_is_authorized(record, journal))
        journal["uniqueId"] = "different-disk"
        self.assertFalse(retirement.stage_absent_is_authorized(record, journal))

    def test_nonstage_standard_copy_disk_is_not_certified_by_100_percent(self):
        disk = {"id": STAGE_ID + "-other", "sku": {"name": "Standard_LRS"},
                "properties": {"creationData": {"createOption": "Copy"},
                               "completionPercent": 100}}
        with self.assertRaises(retirement.SafetyError):
            retirement.validate_nonstage_node_disk_sku(disk)


class PreparationGateTests(unittest.TestCase):
    def make_fixture(self, root: Path, process_status="complete"):
        stack = root / "50gib"
        state_dir = stack / ".state"
        receipt_path = state_dir / "source-stage/receipt.json"
        receipt_path.parent.mkdir(parents=True)
        (stack / "02-prepare.sh").write_text("#!/bin/bash\ntrue\n")
        source_id = SOURCE_ID
        (stack / "run-config.json").write_text(json.dumps({"targetHeapGiB": 50}))
        (stack / "source-manifest.json").write_text(json.dumps({"sourceSnapshotId": source_id,
                                                                 "uniqueId": "source-uid"}))
        state = {"status": "ready", "subscriptionId": SUB, "ownerToken": OWNER,
            "nodeResourceGroup": f"nodes-{OWNER}",
            "snapshotResourceGroup": "snapshots",
            "snapshotResourceGroupId": f"/subscriptions/{SUB}/resourceGroups/snapshots",
            "snapshotSourceStagingDiskId": STAGE_ID,
            "snapshotSourceCopyId": CHILD_ID,
            "snapshotSourceStageReceiptPath": str(receipt_path),
            "location": "eastus", "availabilityZone": "1"}
        (state_dir / "state.json").write_text(json.dumps(state))
        script_hash = retirement.sha256_file(stack / "02-prepare.sh")
        process = {"status": process_status, "exitCode": 0,
                   "scriptSha256": {"02-prepare.sh": script_hash}}
        (state_dir / "prepare-process.json").write_text(json.dumps(process))
        metadata = {"status": "complete", "fixtureTargetHeapGiB": 50,
            "instances": 2, "clientCreated": False, "replicaCaughtUpToLsn": "1/2",
            "backup": "backup", "replicaSnapshots": {"data": "data-vss", "wal": "wal-vss"},
            "sourceSnapshotId": source_id}
        (state_dir / "prepare.json").write_text(json.dumps(metadata))
        receipt = {"version": 1, "status": "ready", "ownerToken": OWNER,
            "sourceSnapshotId": source_id, "sourceSnapshotUniqueId": "source-uid",
            "stagingDiskId": STAGE_ID, "stagingDiskUniqueId": "disk-uid",
            "restoreSnapshotId": CHILD_ID, "restoreSnapshotUniqueId": "child-uid"}
        receipt_path.write_text(json.dumps(receipt))
        return stack

    def test_incomplete_prepare_process_blocks_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_fixture(root, process_status="running")
            with patch.object(retirement, "ROOT", root):
                with self.assertRaises(retirement.SafetyError):
                    retirement.preparation_evidence(root, "50gib")

    def test_complete_preparation_binds_exact_receipt_and_hashes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_fixture(root)
            with patch.object(retirement, "ROOT", root):
                context, stage = retirement.preparation_evidence(root, "50gib")
            self.assertEqual(stage["uniqueId"], "disk-uid")
            self.assertEqual(stage["restoreSnapshotUniqueId"], "child-uid")
            self.assertEqual(context["evidence"]["sourceStageReceiptSha256"],
                             retirement.sha256_file(root / stage["sourceStageReceiptPath"]))
            self.assertTrue(context["evidence"]["stateSha256"])
            self.assertTrue(context["evidence"]["sourceManifestSha256"])


class HydrationAndRetentionTests(unittest.TestCase):
    def test_partial_premiumv2_copy_blocks_gate(self):
        with self.assertRaises(retirement.SafetyError):
            retirement.validate_target_disk(p2v_disk(percent=0), NODE_RG, "1")

    def test_missing_premiumv2_copy_percent_blocks_gate(self):
        with self.assertRaises(retirement.SafetyError):
            retirement.validate_target_disk(p2v_disk(percent=None), NODE_RG, "1")

    def test_fully_hydrated_premiumv2_copy_passes(self):
        result = retirement.validate_target_disk(p2v_disk(), NODE_RG, "1")
        self.assertEqual(result["completionPercent"], 100)

    def test_empty_disk_allows_null_completion_percent(self):
        disk = p2v_disk(percent=None, option="Empty")
        self.assertIsNone(retirement.validate_target_disk(disk, NODE_RG, "1")["completionPercent"])

    def test_successful_snapshot_audit_preserves_child_identity_and_lock(self):
        compared = retirement.compare_snapshot_audits(audit(), audit(), [(CHILD_ID, "child-uid")])
        self.assertTrue(compared["unchangedIdentities"])
        self.assertTrue(compared["allSnapshotsProtected"])

    def test_changed_snapshot_unique_id_blocks_final_receipt(self):
        with self.assertRaises(retirement.SafetyError):
            retirement.compare_snapshot_audits(audit(), audit(unique_id="replacement"),
                                               [(CHILD_ID, "child-uid")])

    def test_changed_snapshot_protection_blocks_final_receipt(self):
        with self.assertRaises(retirement.SafetyError):
            retirement.compare_snapshot_audits(audit(), audit(direct=False, group=False),
                                               [(CHILD_ID, "child-uid")])

    def test_premium_source_backup_snapshot_needs_explicit_100(self):
        measured = {retirement.canonical_id(p2v_disk()["id"])}
        retirement.validate_backup_snapshot_materialization(backup_snapshot(100), measured)
        for percent in (None, 0):
            with self.subTest(percent=percent), self.assertRaises(retirement.SafetyError):
                retirement.validate_backup_snapshot_materialization(backup_snapshot(percent), measured)

    def test_backup_snapshot_requires_exact_measured_source_tag_and_lineage(self):
        measured = {retirement.canonical_id(p2v_disk()["id"])}
        missing_tag = backup_snapshot(100)
        del missing_tag["tags"]["source_volume_id"]
        with self.assertRaises(retirement.SafetyError):
            retirement.validate_backup_snapshot_materialization(missing_tag, measured)
        wrong_tag = backup_snapshot(100)
        wrong_tag["tags"]["source_volume_id"] = SOURCE_ID
        with self.assertRaises(retirement.SafetyError):
            retirement.validate_backup_snapshot_materialization(wrong_tag, measured)
        wrong_lineage = backup_snapshot(100)
        wrong_lineage["properties"]["creationData"]["sourceResourceId"] = SOURCE_ID
        with self.assertRaises(retirement.SafetyError):
            retirement.validate_backup_snapshot_materialization(wrong_lineage, measured)

    def test_standard_origin_child_copy_can_be_available_with_unobservable_percent(self):
        record = stage_record()
        snapshot = {"id": CHILD_ID, "sku": {"name": "Standard_LRS"},
            "tags": {"reprotoken": OWNER, "reproPurpose": "premiumv2-keeper-copy",
                     "sourceSnapshotId": SOURCE_ID},
            "properties": {"uniqueId": "child-uid", "provisioningState": "Succeeded",
                "snapshotAccessState": "Available", "incremental": True,
                "diskSizeGB": 512, "completionPercent": None,
                "creationData": {"createOption": "Copy", "sourceResourceId": STAGE_ID,
                                  "sourceUniqueId": "disk-uid"}}}
        retirement.validate_child_snapshot(snapshot, record)

    def test_snapshot_copy_start_requires_100_percent(self):
        snapshot = {"id": CHILD_ID, "properties": {"provisioningState": "Succeeded",
            "completionPercent": 100, "creationData": {"createOption": "CopyStart"}}}
        self.assertTrue(retirement.validate_copy_status(snapshot, "snapshot")[0])
        snapshot["properties"]["completionPercent"] = None
        self.assertFalse(retirement.validate_copy_status(snapshot, "snapshot")[0])

    def test_arm_get_rejects_foreign_authority_before_sending_bearer_token(self):
        azure = object.__new__(retirement.Azure)
        azure.token = "test-token"
        with self.assertRaises(retirement.SafetyError):
            azure.get("https://attacker.example/resource", "2025-01-02")


if __name__ == "__main__":
    unittest.main(verbosity=2)
