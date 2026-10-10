#!/usr/bin/env python3
"""Local-only refusal-gate tests for cleanup-run.py; no Azure CLI is invoked."""
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("cleanup_run", ROOT / "cleanup-run.py")
cleanup_run = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cleanup_run)


def save(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def hash_files(stack: Path, relative_paths):
    result = {}
    for relative in relative_paths:
        path = stack / "run" / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text(f"frozen fixture: {relative}\n")
        result[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def finalizer_continuation_fixture(stack: Path):
    token = "d" * 32
    save(stack / "run/.state/state.json", {"ownerToken": token, "subscriptionId": "sub"})
    prepare_hashes = hash_files(stack, cleanup_run.PREPARE_FROZEN_INPUTS)
    finalize_hashes = hash_files(stack, cleanup_run.FINALIZE_FROZEN_INPUTS)
    prepare_command = ["bash", str((stack / "run/02-prepare.sh").resolve())]
    finalize_command = ["python3", str((stack / "run/recovery/finalize-prepared-seed-attempt-2.py").resolve()),
                        "--apply", "--owner-token", token]
    save(stack / "run/.state/prepare-process.json", {
        "phase": "prepare", "status": "failed", "exitCode": -15,
        "stack": "run", "command": prepare_command, "scriptSha256": prepare_hashes})
    finalize_base = {"phase": "finalize", "stack": "run", "command": finalize_command,
                     "scriptSha256": finalize_hashes}
    save(stack / "run/.state/finalize-process-attempt-1.json", {
        **finalize_base, "attempt": 1, "status": "failed", "exitCode": 2})
    save(stack / "run/.state/finalize-process-attempt-2.json", {
        **finalize_base, "attempt": 2, "status": "complete", "exitCode": 0})
    return {"ownerToken": token, "subscriptionId": "sub"}


def offline_seed_fixture(stack: Path, state):
    backup = {"metadata": {"name": "seed", "uid": "backup-uid"},
              "spec": {"online": False},
              "status": {"phase": "completed", "method": "volumeSnapshot",
                         "snapshotBackupStatus": {"elements": [{"type": "PG_DATA"}, {"type": "PG_WAL"}]}}}
    save(stack / "run/.state/recovery/evidence/seed-backup-completed.json", backup)
    return {"status": "complete", "ownerToken": state["ownerToken"],
            "subscriptionId": state["subscriptionId"], "sourceSnapshotsUntouched": True,
            "allSnapshotsRetained": True, "archiveSecretAbsent": True, "archiveWriterAbsent": True,
            "finalization": {"archiveReferencesRemoved": True,
                "archiveSecretRemovedAfterReferences": True, "bootstrapSourceRemoved": True,
                "recoveryTargetRemoved": True, "volumeSnapshotLineagePreserved": True},
            "offlineSeedBackup": {"name": "seed", "uid": "backup-uid", "online": False},
            "primaryAndReplicaSnapshots": {"records": [
                {"id": "data-id", "backupUid": "backup-uid", "retentionLock": "CanNotDelete"},
                {"id": "wal-id", "backupUid": "backup-uid", "retentionLock": "CanNotDelete"}]}}


def good_summary():
    return {"status": {"testComplete": True, "vacuumWindowCoverageValid": True},
            "coverage": {"valid": True}}


def good_native():
    return {"pgbenchExitCode": 0, "failedTransactions": 0,
            "completedTransactions": 100, "loggedCompletedTransactions": 100,
            "committedTransactions": 103, "pendingTransactionsAtStopRequest": 4,
            "inFlightActiveSessionsAtStopRequest": 2, "validationErrors": []}


class CleanupRunTests(unittest.TestCase):
    def test_actual_watcher_helper_contract_is_normalized_for_consumers(self):
        token = "a" * 32
        sub = "00000000-0000-0000-0000-000000000000"
        rg = f"rg-vacuum-watcher-{token}"
        name = f"vacuum-watcher-{token[:12]}"
        rid = f"/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Network/networkWatchers/{name}"
        state = {"ownerToken": token, "subscriptionId": sub, "location": "eastus",
                 "networkWatcherBaseline": {"location": "eastus", "ids": []},
                 "networkWatcher": {"owned": True, "status": "ready", "ownerToken": token,
                     "resourceGroup": rg, "name": name, "location": "eastus", "id": rid,
                     "etag": "saved-etag"}}
        _, contract = cleanup_run.watcher_ownership(ROOT, state)
        self.assertEqual(contract["networkWatcher"], {"id": rid, "location": "eastus"})
        self.assertEqual(contract["savedEtag"], "saved-etag")
        self.assertEqual(cleanup_run.watcher_cleanup_decision(contract, None), "preserve-gate-absent")

    def test_test_gate_requires_process_summary_coverage_and_reconciled_counts(self):
        with tempfile.TemporaryDirectory() as name:
            stack = Path(name)
            save(stack / ".state/test-process.json", {
                "phase": "test", "status": "complete", "exitCode": 0,
                "command": ["python", "03-test.py", "--out", str(stack / "results")]})
            save(stack / "results/summary.json", good_summary())
            save(stack / "results/timing.json", {"testComplete": True})
            save(stack / "results/client-summary.json", good_native())
            receipt = cleanup_run.validate_test_outputs(stack)
            self.assertEqual(receipt["completedTransactions"], 100)
            self.assertEqual(receipt["stopDrainDifference"], 3)
            cases = [
                (".state/test-process.json", {"status": "failed", "exitCode": 1},
                 "process must have"),
                ("results/summary.json", {"coverage": {"valid": False}}, "coverage is not valid"),
                ("results/client-summary.json", {"failedTransactions": 1}, "failed count"),
                ("results/client-summary.json", {"loggedCompletedTransactions": 99}, "do not reconcile"),
                ("results/client-summary.json", {"committedTransactions": 90}, "cannot be reconciled"),
            ]
            for relative, changes, message in cases:
                with self.subTest(relative=relative, changes=changes):
                    base = json.loads((stack / relative).read_text())
                    base.update(changes)
                    save(stack / relative, base)
                    with self.assertRaisesRegex(cleanup_run.Refusal, message):
                        cleanup_run.validate_test_outputs(stack)
                    if relative.endswith("client-summary.json"):
                        save(stack / relative, good_native())
                    elif relative.endswith("summary.json"):
                        save(stack / relative, good_summary())
                    else:
                        save(stack / relative, {"phase": "test", "status": "complete", "exitCode": 0,
                            "command": ["python", "03-test.py", "--out", str(stack / "results")]})

    def test_test_output_path_cannot_escape_stack(self):
        with tempfile.TemporaryDirectory() as name:
            stack = Path(name)
            with self.assertRaisesRegex(cleanup_run.Refusal, "escapes"):
                cleanup_run.results_dir_from_receipt(stack, {"command": ["03-test.py", "--out", "/tmp/other"]})

    def test_private_archive_receipt_and_each_object_are_hash_size_pinned(self):
        with tempfile.TemporaryDirectory() as name:
            stack = Path(name)
            archive_dir = stack / "recovery/archive-input"
            source_dir = stack / "recovery/source-evidence"
            archive_dir.mkdir(parents=True)
            source_dir.mkdir(parents=True)
            blobs = {"segment-one": b"abc", "segment-two.backup": b"history"}
            objects = [{"objectName": f"source/path/{filename}", "bytes": len(content),
                        "sha256": hashlib.sha256(content).hexdigest()} for filename, content in blobs.items()]
            receipt = {"status": "complete", "exportedObjects": objects,
                       "verification": {"allStartThroughEndSegmentsPresent": True, "rangeContiguous": True}}
            receipt_bytes = json.dumps(receipt, sort_keys=True).encode()
            (source_dir / "archive-receipt.json").write_bytes(receipt_bytes)
            (archive_dir / "receipt.json").write_bytes(receipt_bytes)
            file_records = []
            for filename, content in blobs.items():
                (archive_dir / filename).write_bytes(content)
                file_records.append({"path": f"recovery/archive-input/{filename}", "bytes": len(content),
                                     "sha256": hashlib.sha256(content).hexdigest()})
            label = b"BACKUP LABEL\n"
            (archive_dir / "backup_label.txt").write_bytes(label)
            file_records.append({"path": "recovery/archive-input/backup_label.txt", "bytes": len(label),
                                 "sha256": hashlib.sha256(label).hexdigest()})
            file_records.extend([
                {"path": "recovery/archive-input/receipt.json", "bytes": len(receipt_bytes),
                 "sha256": hashlib.sha256(receipt_bytes).hexdigest()},
                {"path": "recovery/source-evidence/archive-receipt.json", "bytes": len(receipt_bytes),
                 "sha256": hashlib.sha256(receipt_bytes).hexdigest()}])
            save(stack / "recovery/source-evidence-manifest.json", {"version": 1, "files": file_records})
            manifest = {"archive": {"sourceReceipt": "recovery/source-evidence/archive-receipt.json",
                                     "objects": objects}}
            verified = cleanup_run.validate_archive_manifest_binding(stack, manifest, receipt)
            self.assertEqual(len(verified), len(file_records))
            (archive_dir / "segment-one").write_bytes(b"changed")
            with self.assertRaisesRegex(cleanup_run.Refusal, "differs from its pinned size or SHA-256"):
                cleanup_run.validate_archive_manifest_binding(stack, manifest, receipt)

    def test_offline_seed_requires_completed_offline_paired_backup_and_bound_snapshots(self):
        with tempfile.TemporaryDirectory() as name:
            stack = Path(name)
            backup = {"metadata": {"name": "seed", "uid": "backup-uid"},
                      "spec": {"online": False},
                      "status": {"phase": "completed", "method": "volumeSnapshot",
                                 "snapshotBackupStatus": {"elements": [{"type": "PG_DATA"}, {"type": "PG_WAL"}]}}}
            evidence = stack / "run/.state/recovery/evidence/seed-backup-completed.json"
            save(evidence, backup)
            save(stack / "run/.state/recovery/evidence/other-array-evidence.json", ["unrelated", "json", "array"])
            duplicate_capture = stack / "run/.state/recovery/evidence/seed-backup-recheck.json"
            duplicate_capture.parent.mkdir(parents=True, exist_ok=True)
            duplicate_capture.write_text(json.dumps(backup, indent=2, sort_keys=True))
            state = {"ownerToken": "a" * 32, "subscriptionId": "sub"}
            save(stack / "run/.state/prepare-process.json", {"phase": "prepare", "status": "complete", "exitCode": 0})
            prepare = {"status": "complete", "ownerToken": "a" * 32, "subscriptionId": "sub",
                       "sourceSnapshotsUntouched": True, "allSnapshotsRetained": True,
                       "archiveSecretAbsent": True, "archiveWriterAbsent": True,
                       "offlineSeedBackup": {"name": "seed", "uid": "backup-uid", "online": False},
                       "primaryAndReplicaSnapshots": {"records": [
                           {"id": "data-id", "backupUid": "backup-uid", "retentionLock": "CanNotDelete"},
                           {"id": "wal-id", "backupUid": "backup-uid", "retentionLock": "CanNotDelete"}]}}
            receipt = cleanup_run.validate_offline_seed(stack, state, prepare)
            self.assertTrue(receipt["verified"] if "verified" in receipt else receipt["online"] is False)
            backup["spec"]["online"] = True
            save(evidence, backup)
            duplicate_capture.write_text(json.dumps(backup, indent=2, sort_keys=True))
            with self.assertRaisesRegex(cleanup_run.Refusal, "No captured completed offline paired"):
                cleanup_run.validate_offline_seed(stack, state, prepare)

    def test_verified_finalizer_continuation_preserves_failed_prepare_and_offline_seed_gates(self):
        with tempfile.TemporaryDirectory() as name:
            stack = Path(name)
            state = finalizer_continuation_fixture(stack)
            original_prepare = json.loads((stack / "run/.state/prepare-process.json").read_text())
            self.assertEqual(original_prepare["status"], "failed")
            self.assertEqual(original_prepare["exitCode"], -15)
            continuation = cleanup_run.validate_preparation_receipt(stack, state, original_prepare)
            self.assertEqual(continuation["mode"], "verified-finalizer-continuation")
            self.assertEqual(continuation["finalizerAttempt"], 2)
            self.assertTrue(any(row["kind"] == "finalizeProcess" for row in continuation["proofFiles"]))
            self.assertEqual(len(continuation["proofFiles"]), 11)

            prepare = offline_seed_fixture(stack, state)
            seed = cleanup_run.validate_offline_seed(stack, state, prepare, continuation)
            self.assertEqual(seed["backupUid"], "backup-uid")
            self.assertEqual(seed["preparationVerification"]["mode"], "verified-finalizer-continuation")
            with self.assertRaisesRegex(cleanup_run.Refusal, "differs from the current process"):
                cleanup_run.validate_offline_seed(stack, state, prepare,
                    {"mode": "verified-finalizer-continuation", "verified": True})
            prepare["finalization"]["volumeSnapshotLineagePreserved"] = False
            with self.assertRaisesRegex(cleanup_run.Refusal, "complete archive/lineage preparation audit"):
                cleanup_run.validate_offline_seed(stack, state, prepare, continuation)

    def test_verified_finalizer_continuation_rejects_failed_latest_wrong_owner_and_changed_hashes(self):
        cases = ("failed-latest", "wrong-owner", "changed-finalizer-input", "changed-original-manifest")
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as name:
                stack = Path(name)
                state = finalizer_continuation_fixture(stack)
                latest_path = stack / "run/.state/finalize-process-attempt-2.json"
                latest = json.loads(latest_path.read_text())
                if case == "failed-latest":
                    earlier_path = stack / "run/.state/finalize-process-attempt-1.json"
                    earlier = json.loads(earlier_path.read_text())
                    earlier.update({"status": "complete", "exitCode": 0})
                    save(earlier_path, earlier)
                    latest.update({"status": "failed", "exitCode": 1})
                    message = "Latest finalizer process receipt"
                elif case == "wrong-owner":
                    latest["command"][-1] = "e" * 32
                    message = "exact owner-token apply invocation"
                elif case == "changed-finalizer-input":
                    (stack / "run/recovery/seed-replica-and-finalize.py").write_text("changed\n")
                    message = "frozen finalizer helper/config/manifest input changed"
                else:
                    (stack / "run/source-manifest.json").write_text("changed original pinned manifest\n")
                    message = "frozen original preparation helper/config/manifest input changed"
                save(latest_path, latest)
                original_prepare = json.loads((stack / "run/.state/prepare-process.json").read_text())
                with self.assertRaisesRegex(cleanup_run.Refusal, message):
                    cleanup_run.validate_preparation_receipt(stack, state, original_prepare)

    def test_finalizer_continuation_proof_set_accepts_extra_authorization_kind(self):
        with tempfile.TemporaryDirectory() as name:
            stack = Path(name)
            state = finalizer_continuation_fixture(stack)
            continuation = cleanup_run.validate_preparation_receipt(
                stack, state, json.loads((stack / "run/.state/prepare-process.json").read_text()))
            helper = cleanup_run.import_helper(
                ROOT / "run/recovery/verify-teardown-authorization.py", "test_auth_extra_kind")
            required_paths = {
                "state": "run/.state/state.json",
                "experimentPlan": "experiment-plan.json",
                "prepareAudit": "run/.state/recovery/prepare-audit.json",
                "prepareProcess": "run/.state/prepare-process.json",
                "sourceManifest": "run/source-manifest.json",
                "offlineSeedBackup": "run/.state/recovery/evidence/offline.json",
                "testProcess": "run/.state/test-process.json",
                "testSummary": "results/summary.json",
                "testTiming": "results/timing.json",
                "nativeClientSummary": "results/client-summary.json",
                "newBlobStorage": "run/.state/storage.json",
                "newBlobContainer": "run/.state/storage-container.json",
                "priorSnapshotInventory": "preflight/all-snapshots-before.json",
                "priorLockInventory": "preflight/locks.json",
                "beforeCleanupSnapshotAudit": "verification/before-snapshots.json",
                "beforeCleanupLocks": "verification/before-locks.json",
                "04Script": "run/04-teardown.sh",
                "authorizationHelper": "run/recovery/verify-teardown-authorization.py",
                "cleanupRunScript": "cleanup-run.py",
            }
            blob_path = stack / "run/recovery/archive-input/blob"
            blob_path.parent.mkdir(parents=True, exist_ok=True)
            blob_path.write_bytes(b"verified archive fixture")
            required_paths["archiveInput"] = "run/recovery/archive-input/blob"
            evidence_manifest = {"files": [{"path": "recovery/archive-input/blob",
                "bytes": blob_path.stat().st_size,
                "sha256": hashlib.sha256(blob_path.read_bytes()).hexdigest()}]}
            save(stack / "run/recovery/source-evidence-manifest.json", evidence_manifest)
            required_paths["sourceEvidenceManifest"] = "run/recovery/source-evidence-manifest.json"
            save(stack / "run/.state/recovery/prepare-audit.json", {
                "primaryAndReplicaSnapshots": {"records": [
                    {"id": "/snapshots/data", "uniqueId": "data-uid"},
                    {"id": "/snapshots/wal", "uniqueId": "wal-uid"}]}})
            for relative in required_paths.values():
                path = stack / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                if not path.exists():
                    path.write_text("proof fixture\n")
            by_kind = {kind: cleanup_run.proof_file(stack / relative, kind, stack)
                       for kind, relative in required_paths.items()}
            before_audit = stack / "verification/before-snapshots.json"
            before_locks = stack / "verification/before-locks.json"
            gates = {
                "stateProof": by_kind["state"],
                "planProof": by_kind["experimentPlan"],
                "prepareAuditProof": by_kind["prepareAudit"],
                "prepareProcessProof": by_kind["prepareProcess"],
                "test": {"proofFiles": [by_kind[k] for k in
                    ("testProcess", "testSummary", "testTiming", "nativeClientSummary")]},
                "archive": {"verifiedFiles": [by_kind[k] for k in
                    ("sourceManifest", "sourceEvidenceManifest", "archiveInput")]},
                "offlineSeedBackupProof": by_kind["offlineSeedBackup"],
                "newBlob": {"proofFiles": [by_kind["newBlobStorage"], by_kind["newBlobContainer"]]},
                "baselineProofFiles": [by_kind["priorSnapshotInventory"], by_kind["priorLockInventory"]],
                "preparedSnapshotEvidenceProofs": [],
                "preparationVerification": continuation,
            }
            proof_files = cleanup_run.teardown_authorization_proofs(
                gates, before_audit, before_locks, stack)
            finalizer_records = [row for row in proof_files if row["kind"] == "finalizeProcess"]
            self.assertEqual(len(finalizer_records), 1)
            validated = helper.validate_proof_files(stack, proof_files)
            self.assertIn(finalizer_records[0]["path"].casefold(), validated)

    def test_before_cleanup_audit_binds_all_42_priors_and_prepared_pair(self):
        with tempfile.TemporaryDirectory() as name:
            stack = Path(name)
            prior = [{"id": f"old-{index}", "uniqueId": f"uid-{index}"} for index in range(42)]
            prepare = {"primaryAndReplicaSnapshots": {"records": [
                {"id": "new-data", "uniqueId": "new-uid-data"},
                {"id": "new-wal", "uniqueId": "new-uid-wal"}]}}
            inventory = [{"id": item["id"], "uniqueId": item["uniqueId"], "directCanNotDelete": True,
                          "groupCanNotDelete": False, "state": "Succeeded", "accessState": "Available",
                          "sizeGiB": 512, "tags": {}} for item in prior]
            for role in ("data", "wal"):
                inventory.append({"id": f"new-{role}", "uniqueId": f"new-uid-{role}",
                                  "directCanNotDelete": True, "groupCanNotDelete": False,
                                  "state": "Succeeded", "accessState": "Available", "sizeGiB": 512,
                                  "tags": {"reprotoken": "owner", "reproPurpose": "premiumv2-vacuum-backup"}})
            audit = {"checkedAtUtc": "2026-10-09T00:00:00Z", "preservedPriorSnapshots": True,
                     "allSnapshotsProtected": True, "priorSnapshotCount": 42,
                     "currentSnapshotCount": 44, "snapshots": inventory}
            result = cleanup_run.validate_snapshot_audit(audit, prior, prepare, "owner", stack)
            self.assertEqual(result["priorSnapshotCount"], 42)
            self.assertEqual({row["id"] for row in result["preparedSnapshots"]}, {"new-data", "new-wal"})
            teardown = {"retainedSnapshots": [
                {"id": "new-data", "uniqueId": "new-uid-data", "retained": True,
                 "resourceLock": "CanNotDelete"},
                {"id": "new-wal", "uniqueId": "new-uid-wal", "retained": True,
                 "resourceLock": "CanNotDelete"}]}
            cleanup_run.verify_teardown_preserved_new(stack, teardown, audit, prepare)
            teardown["retainedSnapshots"][0]["uniqueId"] = "changed-teardown-uid"
            with self.assertRaisesRegex(cleanup_run.Refusal, "uniqueId differs from the pinned"):
                cleanup_run.verify_teardown_preserved_new(stack, teardown, audit, prepare)
            audit["snapshots"][-1]["uniqueId"] = "changed-seed-uid"
            with self.assertRaisesRegex(cleanup_run.Refusal, "uniqueId differs from its preparation evidence"):
                cleanup_run.validate_snapshot_audit(audit, prior, prepare, "owner", stack)
            audit["snapshots"][-1]["uniqueId"] = "new-uid-wal"
            audit["snapshots"].pop(0)
            with self.assertRaisesRegex(cleanup_run.Refusal, "lost or changed"):
                cleanup_run.validate_snapshot_audit(audit, prior, prepare, "owner", stack)

    def test_prepared_seed_uids_fall_back_to_captured_arm_evidence(self):
        with tempfile.TemporaryDirectory() as name:
            stack = Path(name)
            evidence = stack / "run/.state/recovery/evidence"
            evidence.mkdir(parents=True)
            records = [{"id": "/snapshots/data"}, {"id": "/snapshots/wal"}]
            for role in ("data", "wal"):
                save(evidence / f"seed-{role}-arm-snapshot-observed-azureSnapshot.json",
                     {"id": f"/snapshots/{role}", "uniqueId": f"uid-{role}"})
            expected, proofs = cleanup_run.prepared_snapshot_uids(
                stack, {"primaryAndReplicaSnapshots": {"records": records}})
            self.assertEqual(expected, {"/snapshots/data": "uid-data", "/snapshots/wal": "uid-wal"})
            self.assertEqual(len(proofs), 2)
            records[0]["uniqueId"] = "different-uid"
            with self.assertRaisesRegex(cleanup_run.Refusal, "differs from captured ARM"):
                cleanup_run.prepared_snapshot_uids(
                    stack, {"primaryAndReplicaSnapshots": {"records": records}})

    def test_direct_04_invocation_refuses_before_any_azure_command(self):
        with tempfile.TemporaryDirectory() as name:
            experiment = Path(name)
            run = experiment / "run"
            state_dir = run / ".state"
            recovery = run / "recovery"
            fake_bin = experiment / "fake-bin"
            state_dir.mkdir(parents=True)
            recovery.mkdir(parents=True)
            fake_bin.mkdir()
            token = "a" * 32
            snapshot_group = f"rg-vacuum-pv2-snap-{token}"
            state = {"version": 1, "subscriptionId": "sub", "resourceGroup": f"rg-vacuum-only-{token}",
                     "clusterName": f"aks-vacuum-only-{token[:12]}", "ownerToken": token,
                     "nodeResourceGroup": "", "requestedNodeResourceGroup": f"rg-vacuum-nodes-{token}",
                     "snapshotResourceGroup": snapshot_group,
                     "snapshotResourceGroupId": f"/subscriptions/sub/resourceGroups/{snapshot_group}",
                     "availabilityZone": "1", "kubeconfigPath": ".state/kubeconfig", "location": "eastus"}
            save(state_dir / "state.json", state)
            save(run / "source-manifest.json", {"sourceSnapshotId": "/subscriptions/sub/resourceGroups/source/providers/Microsoft.Compute/snapshots/source"})
            script = run / "04-teardown.sh"
            helper = recovery / "verify-teardown-authorization.py"
            shutil.copy2(ROOT / "run" / "04-teardown.sh", script)
            shutil.copy2(ROOT / "run" / "recovery" / "verify-teardown-authorization.py", helper)
            marker = experiment / "azure-was-called"
            az = fake_bin / "az"
            az.write_text(f"#!/bin/sh\nprintf called > '{marker}'\nexit 91\n", encoding="utf-8")
            az.chmod(0o700)
            environment = dict(os.environ)
            environment["PATH"] = str(fake_bin) + os.pathsep + environment.get("PATH", "")
            completed = subprocess.run(["bash", str(script)], cwd=experiment, env=environment,
                                       stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, text=True, timeout=15, check=False)
            self.assertNotEqual(completed.returncode, 0)
            self.assertIn("Missing or invalid one-shot cleanup-run authorization", completed.stdout)
            self.assertFalse(marker.exists(), "direct 04 invocation reached an Azure command")

    def test_teardown_authorization_helper_local_self_test(self):
        helper = ROOT / "run" / "recovery" / "verify-teardown-authorization.py"
        completed = subprocess.run(["python3", str(helper), "--self-test"], cwd=ROOT,
                                   stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, text=True, timeout=15, check=False)
        self.assertEqual(completed.returncode, 0, completed.stdout)
        self.assertIn("PASS (local-only)", completed.stdout)

    def test_prior_lock_ids_and_scopes_must_survive(self):
        baseline_rows = [{"id": "/locks/old", "scope": "/snapshots/old", "level": "CanNotDelete"}]
        baseline = cleanup_run.normalize_locks(baseline_rows)
        same = cleanup_run.normalize_locks(baseline_rows + [{"id": "/locks/new", "scope": "/snapshots/new", "level": "CanNotDelete"}])
        cleanup_run.assert_locks_preserved(baseline, same, baseline)
        with self.assertRaisesRegex(cleanup_run.Refusal, "protection locks changed or disappeared"):
            cleanup_run.assert_locks_preserved(baseline, {}, baseline)

    def test_watcher_gate_must_be_root_reviewed_and_bound_to_exact_resource(self):
        with tempfile.TemporaryDirectory() as name:
            stack = Path(name)
            watcher_id = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/networkWatchers/name"
            watcher = {"id": watcher_id, "location": "eastus"}
            contract = {"networkWatcher": watcher, "savedEtag": "etag-1"}
            state = {"ownerToken": "a" * 32}
            gate_path = stack / ".state/regional-watcher-safety-gate.json"
            gate = {"version": 1, "authorized": True, "authorizedBy": "root",
                    "authorizedAtUtc": cleanup_run.now_utc(), "stackName": stack.name,
                    "ownerToken": "a" * 32, "networkWatcherId": watcher_id, "region": "eastus",
                    "sharedRegionalUseReviewedSafeToDelete": True,
                    "sharedRegionalUseAssessment": "Root reviewed exact regional usage and found this watcher safe to remove.",
                    "watcherResourceReview": {"observedAtUtc": cleanup_run.now_utc(),
                        "networkWatcherEtag": "etag-1", "resourceGroupResourceIds": [watcher_id],
                        "childFlowLogIds": []}}
            save(gate_path, gate)
            validated = cleanup_run.validate_watcher_gate(gate_path, contract, stack, state)
            self.assertEqual(validated["networkWatcherId"], watcher_id)
            self.assertTrue(validated["sharedRegionalUseReviewedSafeToDelete"])
            self.assertEqual(cleanup_run.watcher_cleanup_decision(contract, validated),
                             "delete-after-owned-stack-teardown")
            unsafe_gate = dict(gate)
            unsafe_gate["sharedRegionalUseReviewedSafeToDelete"] = False
            unsafe_gate["watcherResourceReview"] = {**gate["watcherResourceReview"],
                "resourceGroupResourceIds": [watcher_id, watcher_id + "/flowLogs/shared"],
                "childFlowLogIds": [watcher_id + "/flowLogs/shared"]}
            save(gate_path, unsafe_gate)
            unsafe = cleanup_run.validate_watcher_gate(gate_path, contract, stack, state)
            self.assertEqual(cleanup_run.watcher_cleanup_decision(contract, unsafe),
                             "preserve-shared-use-review")
            self.assertEqual(cleanup_run.watcher_cleanup_decision(contract, None),
                             "preserve-gate-absent")
            self.assertEqual(cleanup_run.watcher_cleanup_decision(contract, None, "invalid gate"),
                             "preserve-invalid-gate")
            unsafe_gate["sharedRegionalUseReviewedSafeToDelete"] = True
            save(gate_path, unsafe_gate)
            with self.assertRaisesRegex(cleanup_run.Refusal, "child or unexpected resource"):
                cleanup_run.validate_watcher_gate(gate_path, contract, stack, state)
            gate["authorizedBy"] = "someone-else"
            save(gate_path, gate)
            with self.assertRaisesRegex(cleanup_run.Refusal, "issued by the root"):
                cleanup_run.validate_watcher_gate(gate_path, contract, stack, state)

    def test_watcher_resume_requires_safe_new_gate_after_compute_cleanup(self):
        watcher_contract = {"networkWatcher": {"id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/networkWatchers/name"}}
        prior = {"status": "complete", "attempts": [{"ownedWatcherCleanup": {"status": "preserve-gate-absent"}}]}
        self.assertFalse(cleanup_run.can_resume_watcher_only(prior, watcher_contract, None))
        unsafe = {"sharedRegionalUseReviewedSafeToDelete": False}
        self.assertFalse(cleanup_run.can_resume_watcher_only(prior, watcher_contract, unsafe))
        safe = {"sharedRegionalUseReviewedSafeToDelete": True}
        self.assertTrue(cleanup_run.can_resume_watcher_only(prior, watcher_contract, safe))
        prior["attempts"][-1]["ownedWatcherCleanup"]["status"] = "deleted"
        self.assertFalse(cleanup_run.can_resume_watcher_only(prior, watcher_contract, safe))
        prior["attempts"][-1]["ownedWatcherCleanup"]["status"] = "watcher-review-required-after-helper-refusal"
        self.assertTrue(cleanup_run.can_resume_watcher_only(prior, watcher_contract, safe))
        prior["attempts"][-1]["ownedWatcherCleanup"]["status"] = "preserve-live-child-resources"
        self.assertTrue(cleanup_run.can_resume_watcher_only(prior, watcher_contract, safe))

    def test_live_watcher_child_inventory_requires_complete_empty_pages_before_delete(self):
        self.assertEqual(cleanup_run.validate_watcher_child_list({"value": []}, "flowLogs"), [])
        self.assertEqual(cleanup_run.validate_watcher_child_list(
            {"value": [{"id": "/watchers/w/flowLogs/shared"}]}, "flowLogs"),
            ["/watchers/w/flowLogs/shared"])
        with self.assertRaisesRegex(cleanup_run.Refusal, "paginated and incomplete"):
            cleanup_run.validate_watcher_child_list({"value": [], "nextLink": "https://next"}, "flowLogs")
        with self.assertRaisesRegex(cleanup_run.Refusal, "malformed child"):
            cleanup_run.validate_watcher_child_list({"value": [{}]}, "flowLogs")

    def test_existing_teardown_receipt_supports_phased_resume_and_rejects_wrong_owner(self):
        with tempfile.TemporaryDirectory() as name:
            stack = Path(name)
            state = {"ownerToken": "a" * 32, "subscriptionId": "sub",
                     "resourceGroup": "rg-test", "requestedNodeResourceGroup": "rg-nodes",
                     "snapshotResourceGroupId": "/subscriptions/sub/resourceGroups/rg-snapshots"}
            receipt = {"status": "complete", "ownerToken": "a" * 32, "subscriptionId": "sub",
                       "deletedTestResourceGroup": "rg-test", "deletedNodeResourceGroup": "rg-nodes",
                       "retainedSnapshotResourceGroupId": "/subscriptions/sub/resourceGroups/rg-snapshots",
                       "completedAtUtc": cleanup_run.now_utc()}
            save(stack / "run/.state/teardown.json", receipt)
            self.assertEqual(cleanup_run.existing_teardown_receipt(stack, state), receipt)
            state["ownerToken"] = "b" * 32
            with self.assertRaisesRegex(cleanup_run.Refusal, "matching complete ownership receipt"):
                cleanup_run.existing_teardown_receipt(stack, state)


if __name__ == "__main__":
    unittest.main()
