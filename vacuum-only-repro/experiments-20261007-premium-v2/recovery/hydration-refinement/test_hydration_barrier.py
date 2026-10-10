#!/usr/bin/env python3
"""Focused regression tests for the staged hydration barrier."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import urllib.parse


MODULE_PATH = Path(os.environ.get(
    "TEST_HYDRATION_BARRIER_PATH", Path(__file__).with_name("hydration-barrier.py")))
SPEC = importlib.util.spec_from_file_location("staged_hydration_barrier", MODULE_PATH)
barrier = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(barrier)


SUBSCRIPTION = "00000000-0000-0000-0000-000000000000"
RUNS = ("baseline-23gib", "50gib", "100gib")
STAGED = ("50gib", "100gib")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    return path


class Fixture:
    """Small repository-shaped fixture with fake ARM responses."""

    def __init__(self, root):
        self.root = root
        self.states = {}
        self.disk_lists = {}
        self.snapshot_lists = {}
        self.audit_rows = []
        self.measured_disk_ids = {}
        self.stage = {}
        self.owner_tokens = {}
        self.source_manifests = {}
        self.disk_unique_ids = {}
        (root / "recovery/source-restore").mkdir(parents=True)
        (root / "recovery/staging-retirement").mkdir(parents=True)
        (root / "preflight").mkdir(parents=True)
        (root / "recovery/source-restore/stage-allowlisted-source.sh").write_text(
            "#!/bin/sh\n# fixture helper\n", encoding="utf-8")
        (root / "recovery/staging-retirement/retire_staging_disks.py").write_text(
            "# fixture retirement program\n", encoding="utf-8")

        for name in RUNS:
            self._make_stack(name)
        self._write_retirement()

    def _id(self, name, provider, resource):
        rg = (self.states[name]["nodeResourceGroup"] if provider == "disks"
              else self.states[name]["snapshotResourceGroup"])
        return (f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{rg}/providers/"
                f"Microsoft.Compute/{provider}/{resource}")

    def _make_stack(self, name):
        owner = {"baseline-23gib": "a" * 32, "50gib": "b" * 32, "100gib": "c" * 32}[name]
        source_owner = {"baseline-23gib": "d" * 32, "50gib": "e" * 32, "100gib": "f" * 32}[name]
        self.owner_tokens[name] = owner
        node_rg = "nodes-" + name
        snapshot_rg = "snapshots-" + name
        snapshot_rg_id = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{snapshot_rg}"
        state = {
            "subscriptionId": SUBSCRIPTION,
            "ownerToken": owner,
            "nodeResourceGroup": node_rg,
            "snapshotResourceGroup": snapshot_rg,
            "snapshotResourceGroupId": snapshot_rg_id,
            "availabilityZone": "1",
            "status": "ready",
            "snapshotSourceCopyId": "",
        }
        self.states[name] = state
        stack = self.root / name
        write_json(stack / ".state/state.json", state)
        write_json(stack / ".state/known-failed-source-copies.json", {
            "version": 1,
            "snapshots": [],
        })

        source_id = (f"/subscriptions/{SUBSCRIPTION}/resourceGroups/external-source-"
                     f"{name}/providers/Microsoft.Compute/snapshots/source-{name}")
        manifest = {
            "subscriptionId": SUBSCRIPTION,
            "ownerToken": source_owner,
            "sourceSnapshotId": source_id,
            "uniqueId": "source-uid-" + name,
            "sku": {"name": "Standard_ZRS"},
        }
        self.source_manifests[name] = manifest
        write_json(stack / "source-manifest.json", manifest)
        (stack / "02-prepare.sh").write_text("#!/bin/sh\n# fixture prepare\n", encoding="utf-8")
        script_hash = digest(stack / "02-prepare.sh")
        process = {
            "stack": name,
            "phase": "prepare",
            "status": "complete",
            "exitCode": 0,
            "finishedAtUtc": "2026-10-06T20:00:00Z",
            "scriptSha256": {"02-prepare.sh": script_hash},
        }
        prepare = {
            "status": "complete",
            "fixtureTargetHeapGiB": barrier.TARGET_HEAP_GIB[name],
            "instances": 2,
            "clientCreated": False,
            "retainedSnapshotResourceGroup": snapshot_rg,
        }

        if name in STAGED:
            helper_hash = digest(self.root / barrier.SOURCE_STAGE_HELPER)
            process["sourceStageHelperSha256"] = helper_hash
            suffix = "4b554415" if name == "50gib" else "ef6f2376"
            stage_disk_id = self._id(name, "disks", "restore-stage-" + suffix)
            stage_disk_uid = "stage-uid-" + name
            child_id = self._id(name, "snapshots", "keeper-copy-" + suffix)
            child_uid = "child-uid-" + name
            source_meta = {
                "id": source_id,
                "uniqueId": manifest["uniqueId"],
                "sku": {"name": "Standard_ZRS"},
                "provisioningState": "Succeeded",
                "snapshotAccessState": "Available",
                "tags": {"reprotoken": source_owner},
            }
            stage_disk_meta = {
                "id": stage_disk_id,
                "uniqueId": stage_disk_uid,
                "sku": {"name": "Standard_LRS"},
                "provisioningState": "Succeeded",
                "diskState": "Unattached",
                "managedBy": None,
                "creationData": {
                    "createOption": "Copy",
                    "sourceResourceId": source_id,
                    "sourceUniqueId": manifest["uniqueId"],
                },
                "tags": {
                    "reprotoken": owner,
                    "reproPurpose": barrier.STAGE_PURPOSE,
                    "sourceSnapshotId": source_id,
                },
            }
            child_meta = {
                "id": child_id,
                "uniqueId": child_uid,
                "sku": {"name": "Standard_LRS"},
                "provisioningState": "Succeeded",
                "snapshotAccessState": "Available",
                "creationData": {
                    "createOption": "Copy",
                    "sourceResourceId": stage_disk_id,
                    "sourceUniqueId": stage_disk_uid,
                },
                "tags": {
                    "reprotoken": owner,
                    "reproPurpose": "premiumv2-keeper-copy",
                    "sourceSnapshotId": source_id,
                    "sourceDiskId": stage_disk_id,
                },
            }
            stage = {
                "version": 1,
                "status": "ready",
                "ownerToken": owner,
                "subscriptionId": SUBSCRIPTION,
                "resourceSuffix": suffix,
                "sourceSnapshotId": source_id,
                "sourceSnapshotUniqueId": manifest["uniqueId"],
                "sourceManifest": manifest,
                "sourceSnapshotMetadata": source_meta,
                "stagingDiskId": stage_disk_id,
                "stagingDiskUniqueId": stage_disk_uid,
                "stagingDiskMetadata": stage_disk_meta,
                "restoreSnapshotId": child_id,
                "restoreSnapshotUniqueId": child_uid,
                "restoreSnapshotMetadata": child_meta,
            }
            receipt_path = stack / ".state/source-stage" / ("receipt-" + suffix + ".json")
            write_json(receipt_path, stage)
            write_json(stack / ".state/source-stage" / ("plan-" + suffix + ".json"), {
                "sourceSnapshotId": source_id,
                "sourceSnapshotUniqueId": manifest["uniqueId"],
                "stagingDiskId": stage_disk_id,
                "ownedIncrementalSnapshotId": child_id,
            })
            prepare.update({"restoreCopySnapshotId": child_id, "sourceKind": "incremental_copy"})
            self.stage[name] = {
                "source": source_id,
                "sourceOwner": source_owner,
                "receiptPath": receipt_path.relative_to(self.root).as_posix(),
                "receiptSha256": digest(receipt_path),
                "stageDiskId": stage_disk_id,
                "stageDiskUid": stage_disk_uid,
                "childId": child_id,
                "childUid": child_uid,
                "stageDiskMetadata": stage_disk_meta,
                "sourceMetadata": source_meta,
                "childMetadata": child_meta,
            }
            state.update({
                "snapshotSourceStagingDiskId": stage_disk_id,
                "snapshotSourceStageReceiptPath": str(receipt_path.resolve()),
                "snapshotSourceCopyId": child_id,
            })
        else:
            self.stage[name] = None
            baseline_copy_id = self._id(name, "snapshots", "keeper-copy-baseline")
            state["snapshotSourceCopyId"] = baseline_copy_id
            prepare.update({"restoreCopySnapshotId": baseline_copy_id, "sourceKind": "incremental_copy"})

        write_json(stack / ".state/state.json", state)
        prepare["sourceSnapshotId"] = source_id

        write_json(stack / ".state/prepare-process.json", process)
        write_json(stack / ".state/prepare.json", prepare)

        disks = []
        measured_rows = []
        self.measured_disk_ids[name] = set()
        for index, role in enumerate(("primary-data", "primary-wal", "replica-data", "replica-wal")):
            disk_id = self._id(name, "disks", role)
            unique_id = f"premium-uid-{name}-{index}"
            disk = {
                "id": disk_id,
                "name": role,
                "sku": {"name": barrier.PREMIUM_SKU},
                "zones": ["1"],
                "properties": {
                    "uniqueId": unique_id,
                    "diskSizeGB": 512,
                    "diskIOPSReadWrite": 3000,
                    "diskMBpsReadWrite": 125,
                    "logicalSectorSize": 512,
                    "provisioningState": "Succeeded",
                    "completionPercent": None,
                    "creationData": {"createOption": "Empty"},
                },
            }
            disks.append(disk)
            self.measured_disk_ids[name].add(disk_id.lower())
            self.disk_unique_ids[disk_id.lower()] = unique_id
            measured_rows.append({
                "stack": name,
                "id": disk_id,
                "uniqueId": unique_id,
                "createOption": "Empty",
                "completionPercent": None,
            })
        self.disk_lists[name] = disks

        snapshot_id = (self.stage[name]["childId"] if self.stage[name]
                       else self._id(name, "snapshots", "keeper-copy-baseline"))
        snapshot_uid = (self.stage[name]["childUid"] if self.stage[name]
                        else "snapshot-uid-" + name)
        source_resource = (self.stage[name]["stageDiskId"] if self.stage[name] else source_id)
        self.snapshot_lists[name] = [self._snapshot(
            name, snapshot_id, snapshot_uid, source_resource,
            (self.stage[name]["stageDiskUid"] if self.stage[name] else manifest["uniqueId"]),
            "Copy", None,
            owner, "Succeeded", "Available")]
        self._add_audit_snapshot(snapshot_id, snapshot_uid)

    def _snapshot(self, stack, snapshot_id, unique_id, source_id, source_uid, option, percent,
                  owner=None, state="Succeeded", access="Available"):
        return {
            "id": snapshot_id,
            "name": snapshot_id.rsplit("/", 1)[-1],
            "sku": {"name": "Standard_ZRS"},
            "tags": {
                "reprotoken": owner or self.owner_tokens[stack],
                "reproPurpose": "cnpg-backup-test",
                "sourceSnapshotId": source_id,
            },
            "properties": {
                "uniqueId": unique_id,
                "provisioningState": state,
                "snapshotAccessState": access,
                "completionPercent": percent,
                "creationData": {
                    "createOption": option,
                    "sourceResourceId": source_id,
                    "sourceUniqueId": source_uid,
                },
            },
        }

    def _add_audit_snapshot(self, resource_id, unique_id):
        self.audit_rows.append({
            "id": resource_id,
            "uniqueId": unique_id,
            "directCanNotDelete": True,
            "groupCanNotDelete": False,
        })

    def add_backup_snapshot(self, stack, option, percent):
        resource_id = self._id(stack, "snapshots", f"backup-{option}-{percent}")
        source_disk_id = next(iter(self.measured_disk_ids[stack]))
        source_uid = self.disk_unique_ids[source_disk_id]
        snap = self._snapshot(stack, resource_id, f"backup-uid-{option}-{percent}",
                              source_disk_id, source_uid, option, percent)
        snap["tags"]["reproPurpose"] = "premiumv2-vacuum-backup"
        snap["tags"]["source_volume_id"] = source_disk_id
        self.snapshot_lists[stack].append(snap)
        self._add_audit_snapshot(resource_id, snap["properties"]["uniqueId"])
        self._rewrite_audit()

    def add_live_stage_copy(self, stack):
        stage = self.stage[stack]
        self.disk_lists[stack].append({
            "id": stage["stageDiskId"],
            "sku": {"name": "Standard_LRS"},
            "tags": stage["stageDiskMetadata"]["tags"],
            "properties": {
                "uniqueId": stage["stageDiskUid"],
                "provisioningState": "Succeeded",
                "completionPercent": 100,
                "creationData": {"createOption": "Copy"},
            },
        })

    def add_standard_aux_copy(self, stack):
        disk_id = self._id(stack, "disks", "aux-standard-copy")
        self.disk_lists[stack].append({
            "id": disk_id,
            "sku": {"name": "Standard_LRS"},
            "tags": {"reprotoken": self.owner_tokens[stack], "reproPurpose": "auxiliary"},
            "properties": {
                "uniqueId": "aux-standard-uid",
                "provisioningState": "Succeeded",
                "completionPercent": 100,
                "creationData": {"createOption": "Copy"},
            },
        })

    def add_pinned_failed_copy(self):
        stack = "100gib"
        source = self.source_manifests[stack]
        snapshot_id = self._id(stack, "snapshots", "keeper-pv2-known-failed")
        unique_id = "known-failed-copy-uid"
        snapshot = self._snapshot(
            stack, snapshot_id, unique_id, source["sourceSnapshotId"], source["uniqueId"],
            "CopyStart", 0.0, self.owner_tokens[stack], "Failed", "Unknown")
        snapshot["tags"]["reproPurpose"] = "premiumv2-keeper-copy"
        snapshot["tags"]["sourceSnapshotId"] = source["sourceSnapshotId"]
        snapshot["properties"]["copyCompletionError"] = {
            "code": "CopyFailed", "message": "preserved terminal source copy failure"}
        self.snapshot_lists[stack].append(snapshot)
        self._add_audit_snapshot(snapshot_id, unique_id)
        pin_path = self.root / stack / ".state/known-failed-source-copies.json"
        write_json(pin_path, {"version": 1, "snapshots": [{
            "id": snapshot_id,
            "uniqueId": unique_id,
            "ownerToken": self.owner_tokens[stack],
            "reprotoken": self.owner_tokens[stack],
            "sourceSnapshotId": source["sourceSnapshotId"],
            "provisioningState": "Failed",
            "completionPercent": 0.0,
            "reproPurpose": "premiumv2-keeper-copy",
        }]})
        self._rewrite_audit()
        return snapshot, pin_path

    def refresh_after_audit(self, after):
        after_path = self.root / "preflight/snapshot-audit-after.json"
        write_json(after_path, after)
        rows = sorted([
            [item["id"].lower().rstrip("/"), item["uniqueId"],
             bool(item["directCanNotDelete"]), bool(item["groupCanNotDelete"])]
            for item in after["snapshots"]
        ])
        fingerprint = hashlib.sha256(json.dumps(rows, separators=(",", ":")).encode()).hexdigest()
        receipt_path = self.root / "preflight/staging-retirement.json"
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        receipt["snapshotAudit"]["afterSha256"] = digest(after_path)
        receipt["snapshotAudit"]["afterIdentityProtectionSha256"] = fingerprint
        write_json(receipt_path, receipt)

    def _rewrite_audit(self):
        rows = sorted([
            [item["id"].lower().rstrip("/"), item["uniqueId"],
             bool(item["directCanNotDelete"]), bool(item["groupCanNotDelete"])]
            for item in self.audit_rows
        ])
        fingerprint = hashlib.sha256(json.dumps(rows, separators=(",", ":")).encode()).hexdigest()
        before = write_json(self.root / "preflight/snapshot-audit-before.json", {"snapshots": self.audit_rows})
        after = write_json(self.root / "preflight/snapshot-audit-after.json", {"snapshots": self.audit_rows})
        retirement = json.loads((self.root / "preflight/staging-retirement.json").read_text())
        retirement["snapshotAudit"].update({
            "beforeSha256": digest(before),
            "afterSha256": digest(after),
            "beforeIdentityProtectionSha256": fingerprint,
            "afterIdentityProtectionSha256": fingerprint,
        })
        write_json(self.root / "preflight/staging-retirement.json", retirement)

    def _write_retirement(self):
        before = write_json(self.root / "preflight/snapshot-audit-before.json", {"snapshots": self.audit_rows})
        after = write_json(self.root / "preflight/snapshot-audit-after.json", {"snapshots": self.audit_rows})
        rows = sorted([
            [item["id"].lower().rstrip("/"), item["uniqueId"],
             bool(item["directCanNotDelete"]), bool(item["groupCanNotDelete"])]
            for item in self.audit_rows
        ])
        fingerprint = hashlib.sha256(json.dumps(rows, separators=(",", ":")).encode()).hexdigest()
        preparation = {}
        for name in RUNS:
            stack = self.root / name
            stage = self.stage[name]
            preparation[name] = {
                "prepareProcessSha256": digest(stack / ".state/prepare-process.json"),
                "prepareMetadataSha256": digest(stack / ".state/prepare.json"),
                "prepareScriptSha256": digest(stack / "02-prepare.sh"),
                "stateSha256": digest(stack / ".state/state.json"),
                "sourceManifestSha256": digest(stack / "source-manifest.json"),
                "sourceStageReceiptPath": stage["receiptPath"] if stage else None,
                "sourceStageReceiptSha256": stage["receiptSha256"] if stage else None,
                "sourceStageHelperSha256": digest(self.root / barrier.SOURCE_STAGE_HELPER) if stage else None,
            }
        retired = []
        measured = []
        for name in RUNS:
            for disk in self.disk_lists[name]:
                props = disk["properties"]
                if disk.get("sku", {}).get("name") == barrier.PREMIUM_SKU:
                    measured.append({
                        "stack": name,
                        "id": disk["id"],
                        "uniqueId": props["uniqueId"],
                        "createOption": props["creationData"]["createOption"],
                        "completionPercent": props["completionPercent"],
                    })
        for name in STAGED:
            stage = self.stage[name]
            retired.append({
                "stack": name,
                "id": stage["stageDiskId"],
                "uniqueId": stage["stageDiskUid"],
                "sourceSnapshotId": stage["source"],
                "sourceSnapshotUniqueId": "source-uid-" + name,
                "restoreSnapshotId": stage["childId"],
                "restoreSnapshotUniqueId": stage["childUid"],
                "sourceStageReceiptPath": stage["receiptPath"],
                "sourceStageReceiptSha256": stage["receiptSha256"],
                "absentVerified": True,
            })
        write_json(self.root / "preflight/staging-retirement.json", {
            "version": 1,
            "phase": "complete",
            "completedAtUtc": "2026-10-06T22:00:00Z",
            "retirementScriptSha256": digest(self.root / barrier.RETIREMENT_SCRIPT),
            "preparation": preparation,
            "retiredStagingDisks": retired,
            "snapshotAudit": {
                "beforePath": "preflight/snapshot-audit-before.json",
                "afterPath": "preflight/snapshot-audit-after.json",
                "beforeSha256": digest(before),
                "afterSha256": digest(after),
                "beforeIdentityProtectionSha256": fingerprint,
                "afterIdentityProtectionSha256": fingerprint,
                "preservedPriorSnapshots": True,
                "allSnapshotsProtected": True,
                "unchangedIdentities": True,
            },
            "measuredPremiumV2Disks": measured,
            "standardCopyLimitation": (
                "Standard snapshot copy progress is unobservable; these checks do not prove "
                "that unobservable Standard-origin backend copies have finished."
            ),
        })

    def arm_get(self, request, timeout=60):
        path = urllib.parse.urlsplit(request.full_url).path
        for name in RUNS:
            state = self.states[name]
            node_prefix = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{state['nodeResourceGroup']}"
            snapshot_prefix = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{state['snapshotResourceGroup']}"
            if path == node_prefix:
                body = {"tags": {"reprotoken": state["ownerToken"]}}
                return _JsonResponse(body)
            if path == snapshot_prefix:
                body = {"tags": {"reprotoken": state["ownerToken"]}}
                return _JsonResponse(body)
            if path.startswith(node_prefix + "/providers/Microsoft.Compute/disks"):
                return _JsonResponse({"value": self.disk_lists[name]})
            if path.startswith(snapshot_prefix + "/providers/Microsoft.Compute/snapshots"):
                return _JsonResponse({"value": self.snapshot_lists[name]})
        raise AssertionError("Unexpected ARM path: " + path)


class _JsonResponse:
    def __init__(self, body):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return json.dumps(self.body).encode("utf-8")


class HydrationBarrierRegressionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.fixture = Fixture(Path(self.temp.name))
        self.root_patch = mock.patch.object(barrier, "ROOT", self.fixture.root)
        self.root_patch.start()
        self.az_patches = [
            mock.patch.object(barrier.subprocess, "check_output", return_value='{"accessToken":"test"}'),
            mock.patch.object(barrier.urllib.request, "urlopen", side_effect=self.fixture.arm_get),
        ]
        for patcher in self.az_patches:
            patcher.start()

    def tearDown(self):
        for patcher in reversed(self.az_patches):
            patcher.stop()
        self.root_patch.stop()
        self.temp.cleanup()

    def test_live_source_stage_at_100_percent_still_blocks(self):
        self.fixture.add_live_stage_copy("50gib")
        with self.assertRaisesRegex(RuntimeError, "source-stage disk reappeared|source-stage disk remains"):
            barrier.verify()

    def test_missing_or_changed_retirement_receipt_blocks(self):
        receipt_path = self.fixture.root / "preflight/staging-retirement.json"
        original = receipt_path.read_text(encoding="utf-8")
        receipt_path.unlink()
        pending = barrier.verify()
        self.assertFalse(pending["complete"])
        self.assertIn("missing", pending["pending"][0]["reason"])
        receipt_path.write_text(original, encoding="utf-8")
        changed = json.loads(original)
        changed["retiredStagingDisks"][0]["id"] += "-substituted"
        write_json(receipt_path, changed)
        with self.assertRaisesRegex(RuntimeError, "differs from its source receipt"):
            barrier.verify()

    def test_noncomplete_retirement_receipt_returns_pending(self):
        receipt_path = self.fixture.root / "preflight/staging-retirement.json"
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        receipt["phase"] = "retiring"
        write_json(receipt_path, receipt)
        result = barrier.verify()
        self.assertFalse(result["complete"])
        self.assertIn("phase=retiring", result["pending"][0]["reason"])

    def test_exact_pinned_terminal_failed_copy_remains_preserved(self):
        self.fixture.add_pinned_failed_copy()
        result = barrier.verify()
        self.assertTrue(result["complete"])
        stack = next(item for item in result["stacks"] if item["stack"] == "100gib")
        failed = next(row for row in stack["resources"]
                      if row["kind"] == "snapshot" and row["terminalFailedCopyPreserved"])
        self.assertEqual(failed["completionPercent"], 0.0)
        self.assertEqual(failed["createOption"], "CopyStart")
        self.assertTrue(failed["complete"])

    def test_changed_failed_copy_unique_id_fails_against_exact_pin(self):
        snapshot, _ = self.fixture.add_pinned_failed_copy()
        snapshot["properties"]["uniqueId"] = "substituted-unique-id"
        for row in self.fixture.audit_rows:
            if row["id"] == snapshot["id"]:
                row["uniqueId"] = "substituted-unique-id"
        self.fixture._rewrite_audit()
        with self.assertRaisesRegex(RuntimeError, "unexpected failed retained snapshot"):
            barrier.verify()

    def test_missing_failed_copy_pin_fails_closed(self):
        _, pin_path = self.fixture.add_pinned_failed_copy()
        write_json(pin_path, {"version": 1, "snapshots": []})
        with self.assertRaisesRegex(RuntimeError, "unexpected failed retained snapshot"):
            barrier.verify()

    def test_changed_audited_snapshot_identity_fails_closed(self):
        after = json.loads((self.fixture.root / "preflight/snapshot-audit-after.json").read_text(encoding="utf-8"))
        after["snapshots"][0]["uniqueId"] = "changed-audit-unique-id"
        self.fixture.refresh_after_audit(after)
        with self.assertRaisesRegex(RuntimeError, "Snapshot IDs or CanNotDelete protection changed"):
            barrier.verify()

    def test_changed_lock_scope_fingerprint_fails_even_if_still_protected(self):
        after_path = self.fixture.root / "preflight/snapshot-audit-after.json"
        after = json.loads(after_path.read_text(encoding="utf-8"))
        after["snapshots"][0]["directCanNotDelete"] = False
        after["snapshots"][0]["groupCanNotDelete"] = True
        self.fixture.refresh_after_audit(after)
        with self.assertRaisesRegex(RuntimeError, "Snapshot IDs or CanNotDelete protection changed"):
            barrier.verify()

    def test_changed_measured_disk_unique_id_fails_closed(self):
        self.fixture.disk_lists["50gib"][0]["properties"]["uniqueId"] = "substituted-premium-uid"
        with self.assertRaisesRegex(RuntimeError, "measured PremiumV2 disk changed"):
            barrier.verify()

    def test_standard_copied_auxiliary_disk_at_100_percent_blocks(self):
        self.fixture.add_standard_aux_copy("baseline-23gib")
        result = barrier.verify()
        self.assertFalse(result["complete"])
        stack = next(item for item in result["stacks"] if item["stack"] == "baseline-23gib")
        auxiliary = next(row for row in stack["resources"] if row["id"].endswith("/aux-standard-copy"))
        self.assertFalse(auxiliary["complete"])
        self.assertFalse(auxiliary["hydrationProven"])
        self.assertEqual(auxiliary["completionPercent"], 100)

    def test_backup_snapshot_with_zero_or_missing_progress_blocks(self):
        for percent in (0, None):
            with self.subTest(percent=percent):
                self.fixture.add_backup_snapshot("baseline-23gib", "CopyStart", percent)
                result = barrier.verify()
                self.assertFalse(result["complete"])
                stack = next(item for item in result["stacks"] if item["stack"] == "baseline-23gib")
                backup = next(row for row in stack["resources"]
                              if row["kind"] == "snapshot" and "/backup-CopyStart-" in row["id"])
                self.assertFalse(backup["complete"])
                self.assertTrue(backup["sourceIsMeasuredPremiumV2"])

    def test_premium_source_backup_needs_100_even_with_standard_destination_sku(self):
        self.fixture.add_backup_snapshot("baseline-23gib", "Copy", 100)
        result = barrier.verify()
        self.assertTrue(result["complete"])
        stack = next(item for item in result["stacks"] if item["stack"] == "baseline-23gib")
        backup = next(row for row in stack["resources"]
                      if row["kind"] == "snapshot" and "/backup-Copy-100" in row["id"])
        self.assertEqual(backup["sku"], "Standard_ZRS")
        self.assertTrue(backup["sourceIsMeasuredPremiumV2"])
        self.assertTrue(backup["hydrationProven"])

    def test_standard_origin_copy_is_restore_ready_but_hydration_unproven(self):
        result = barrier.verify()
        self.assertTrue(result["complete"])
        standard_copy = next(
            row for stack in result["stacks"] for row in stack["resources"]
            if row["kind"] == "snapshot" and row["sourceIsKnownStandardOrigin"]
        )
        self.assertTrue(standard_copy["complete"])
        self.assertEqual(standard_copy["state"], "Succeeded")
        self.assertEqual(standard_copy["snapshotAccessState"], "Available")
        self.assertFalse(standard_copy["hydrationProven"])
        self.assertIn("restore-readiness-only", standard_copy["hydrationBasis"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
