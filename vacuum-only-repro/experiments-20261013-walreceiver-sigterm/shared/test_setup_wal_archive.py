#!/usr/bin/env python3
"""Offline ownership, lineage, and smoke-gate tests for setup-wal-archive.py."""

from __future__ import annotations

import base64
import importlib.util
import hashlib
import json
import shutil
import tempfile
import unittest
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("setup-wal-archive.py")
SPEC = importlib.util.spec_from_file_location("setup_wal_archive", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class SetupWalArchiveTests(unittest.TestCase):
    def setUp(self) -> None:
        self.token = "0123456789abcdef0123456789abcdef"
        self.account = "cnpgwal" + self.token[:17]
        self.container = "wal-" + self.token[:12]
        self.ctx = {
            "ownerToken": self.token,
            "subscriptionId": "00000000-0000-0000-0000-000000000001",
            "resourceGroup": f"rg-vacuum-only-{self.token}",
            "location": "eastus",
            "accountName": self.account,
            "containerName": self.container,
        }
        self.pair = MODULE.read_json(SCRIPT.with_name("source-pair.json"))

    def _copy_actual_sigterm_receipts(self, temporary: Path) -> tuple[Path, Path]:
        """Copy real local receipt shapes into an isolated, offline test tree."""
        source_experiment = SCRIPT.parent.parent
        source_state = source_experiment / "sigterm" / ".state"
        state_path = source_state / "state.json"
        prepare_path = source_state / "prepare.json"
        if state_path.is_file() and prepare_path.is_file():
            state = json.loads(state_path.read_text())
            prepare = json.loads(prepare_path.read_text())
        else:
            # Keep the test repeatable after the live stack is torn down. These
            # fields mirror the real receipts above: AKS clusterName is distinct
            # from the `repro/hdd-pg` CNPG cluster path.
            token = self.token
            state = {
                "status": "ready", "ownerToken": token,
                "subscriptionId": self.pair["subscriptionId"], "location": "eastus",
                "resourceGroup": f"rg-vacuum-only-{token}",
                "nodeResourceGroup": f"rg-vacuum-nodes-{token}",
                "clusterName": f"aks-vacuum-only-{token[:12]}",
            }
            prepare = {
                "status": "complete", "ownerToken": token,
                "subscriptionId": state["subscriptionId"], "resourceGroup": state["resourceGroup"],
                "clusterName": state["clusterName"], "cluster": "repro/hdd-pg",
                "systemIdentifier": self.pair["fixture"]["systemIdentifier"],
                "heapBytes": self.pair["fixture"]["heapBytes"],
                "sourcePair": {"dataSnapshotId": self.pair["data"]["id"],
                               "walSnapshotId": self.pair["wal"]["id"]},
                "sourceSnapshotRoleAssignments": {"status": "not-created", "locksModified": False,
                                                   "sourceLocksVerifiedUnmodified": True},
            }

        experiment = temporary / "experiment"
        shared = experiment / "shared"
        state_dir = experiment / "sigterm" / ".state"
        shared.mkdir(parents=True, mode=0o700)
        state_dir.mkdir(parents=True, mode=0o700)
        state["kubeconfigPath"] = str(state_dir / "kubeconfig")
        (state_dir / "kubeconfig").write_text("offline-test-placeholder\n")
        (state_dir / "state.json").write_text(json.dumps(state))
        (state_dir / "prepare.json").write_text(json.dumps(prepare))
        for name in ("source-pair.json", "source-pair-proof.json"):
            shutil.copyfile(SCRIPT.parent / name, shared / name)
        return experiment, shared

    def test_storage_names_are_token_scoped_and_provider_valid(self) -> None:
        names = MODULE.expected_storage(self.token)
        self.assertEqual(names["accountName"], self.account)
        self.assertEqual(names["containerName"], self.container)
        self.assertEqual(len(names["accountName"]), 24)
        self.assertNotEqual(MODULE.expected_storage("f" * 32), names)
        with self.assertRaises(MODULE.Refusal):
            MODULE.expected_storage("not-an-owner-token")

    def test_destination_and_blob_names_match_barman_layout(self) -> None:
        destination = MODULE.destination_path(self.account, self.container, self.token)
        self.assertEqual(
            destination,
            f"https://{self.account}.blob.core.windows.net/{self.container}/cnpg-wal-archive/{self.token}/",
        )
        wal = "000000020000005700000009"
        self.assertEqual(
            MODULE.expected_blob_name(self.token, "hdd-pg", wal),
            f"cnpg-wal-archive/{self.token}/hdd-pg/wals/0000000200000057/{wal}",
        )
        with self.assertRaises(MODULE.Refusal):
            MODULE.expected_blob_name(self.token, "other-cluster", wal)

    def test_generated_cluster_patch_references_only_the_private_secret(self) -> None:
        patch = MODULE.make_barman_patch(self.account, self.container, self.token)
        backend = patch["spec"]["backup"]["barmanObjectStore"]
        self.assertEqual(backend["serverName"], "hdd-pg")
        self.assertEqual(backend["azureCredentials"]["connectionString"], {
            "name": "azure-barman-credentials", "key": "connection-string"
        })
        self.assertNotIn("imageName", patch["spec"])
        self.assertNotIn("externalClusters", patch["spec"])
        self.assertNotIn("plugins", patch["spec"])

    def test_secret_payload_is_base64_and_does_not_use_plaintext_manifest_field(self) -> None:
        secret_value = "DefaultEndpointsProtocol=https;AccountName=redacted;AccountKey=topsecret=="
        manifest = MODULE.make_secret_manifest(self.token, secret_value)
        self.assertNotIn("stringData", manifest)
        self.assertEqual(base64.b64decode(manifest["data"]["connection-string"]).decode(), secret_value)
        self.assertEqual(manifest["metadata"]["labels"]["repro.stack.token"], self.token)

    def test_account_verification_requires_exact_owner_and_private_security(self) -> None:
        live = {
            "id": MODULE.expected_account_id(self.ctx),
            "name": self.account,
            "resourceGroup": self.ctx["resourceGroup"],
            "location": "eastus",
            "provisioningState": "Succeeded",
            "kind": "StorageV2",
            "sku": {"name": "Standard_LRS"},
            "allowBlobPublicAccess": False,
            "enableHttpsTrafficOnly": True,
            "minimumTlsVersion": "TLS1_2",
            "tags": {"reprotoken": self.token, "reproPurpose": "private-cnpg-wal-archive", "reproStack": "walreceiver-sigterm"},
        }
        self.assertTrue(MODULE.account_matches(live, self.ctx))
        for mutation in (
            {"allowBlobPublicAccess": True},
            {"tags": {"reprotoken": "f" * 32, "reproPurpose": "private-cnpg-wal-archive", "reproStack": "walreceiver-sigterm"}},
            {"sku": {"name": "Standard_GRS"}},
        ):
            altered = {**live, **mutation}
            self.assertFalse(MODULE.account_matches(altered, self.ctx))

    def test_static_snapshot_lineage_is_exact_and_retained(self) -> None:
        cluster = {"metadata": {"annotations": {
            MODULE.SOURCE_IDS["data"]: self.pair["data"]["id"],
            MODULE.SOURCE_IDS["wal"]: self.pair["wal"]["id"],
        }}}
        snapshots = []
        contents = []
        for kind in ("data", "wal"):
            vss_name = f"source-{kind}-{self.token[:8]}"
            vsc_name = f"source-{kind}-content-{self.token[:8]}"
            snapshots.append({"metadata": {"name": vss_name, "uid": f"{kind}-vss", "labels": {"repro.stack.token": self.token}},
                              "spec": {"source": {"volumeSnapshotContentName": vsc_name}}})
            contents.append({"metadata": {"name": vsc_name, "uid": f"{kind}-vsc", "labels": {"repro.stack.token": self.token}},
                             "spec": {"deletionPolicy": "Retain", "driver": "disk.csi.azure.com",
                                      "source": {"snapshotHandle": self.pair[kind]["id"]},
                                      "volumeSnapshotRef": {"name": vss_name, "namespace": "repro"}}})
        lineage = MODULE.lineage_objects(self.ctx | {"pair": self.pair}, cluster, snapshots, contents)
        self.assertEqual(lineage["data"]["snapshotHandle"], self.pair["data"]["id"])
        changed = [dict(contents[0]), contents[1]]
        changed[0] = {**changed[0], "spec": {**changed[0]["spec"], "deletionPolicy": "Delete"}}
        with self.assertRaises(MODULE.Refusal):
            MODULE.lineage_objects(self.ctx | {"pair": self.pair}, cluster, snapshots, changed)

    def test_plan_is_read_only_and_contains_no_credential(self) -> None:
        ctx = self.ctx | {"stack": "sigterm"}
        plan = MODULE.build_plan(ctx)
        self.assertEqual(plan["mode"], "plan-only")
        self.assertEqual(plan["sourceSnapshotMutations"], 0)
        self.assertEqual(plan["sourceLockMutations"], 0)
        self.assertIsNone(plan["pgWalRestoreTarget"])
        serialized = str(plan)
        self.assertNotIn("AccountKey", serialized)
        self.assertNotIn("connectionString", serialized)

    def test_validate_local_accepts_actual_sigterm_receipts_offline(self) -> None:
        # The actual stack has an AKS resource name in clusterName, while its
        # CNPG Cluster identity is separately recorded as repro/hdd-pg.
        with tempfile.TemporaryDirectory(prefix="wal-archive-receipt-test-") as raw:
            temporary = Path(raw)
            temporary.chmod(0o700)
            experiment, shared = self._copy_actual_sigterm_receipts(temporary)
            with patch.object(MODULE, "EXPERIMENT", experiment), patch.object(MODULE, "HERE", shared):
                context = MODULE.validate_local("sigterm")
            self.assertEqual(context["prepare"]["clusterName"], context["state"]["clusterName"])
            self.assertEqual(context["prepare"]["cluster"], "repro/hdd-pg")
            self.assertEqual(context["accountName"], MODULE.expected_storage(context["ownerToken"])["accountName"])

    def test_validate_local_rejects_cluster_and_source_pair_mismatch(self) -> None:
        with tempfile.TemporaryDirectory(prefix="wal-archive-receipt-test-") as raw:
            temporary = Path(raw)
            temporary.chmod(0o700)
            experiment, shared = self._copy_actual_sigterm_receipts(temporary)
            state_dir = experiment / "sigterm" / ".state"
            prepare_path = state_dir / "prepare.json"
            prepare = json.loads(prepare_path.read_text())
            prepare["clusterName"] = "hdd-pg"
            prepare_path.write_text(json.dumps(prepare))
            with patch.object(MODULE, "EXPERIMENT", experiment), patch.object(MODULE, "HERE", shared):
                with self.assertRaises(MODULE.Refusal):
                    MODULE.validate_local("sigterm")

    def _resume_context_and_receipt(self):
        ctx = self.ctx | {"pair": self.pair}
        lineage = {}
        for kind in ("data", "wal"):
            lineage[kind] = {
                "volumeSnapshot": f"source-{kind}-{self.token[:8]}",
                "volumeSnapshotUid": f"{kind}-snapshot-uid",
                "volumeSnapshotContent": f"source-{kind}-content-{self.token[:8]}",
                "volumeSnapshotContentUid": f"{kind}-content-uid",
                "snapshotHandle": self.pair[kind]["id"],
                "deletionPolicy": "Retain",
            }
        source_locks = {"resourceGroup": {"lockId": "/locks/rg", "level": "CanNotDelete"},
                        "data": {"lockId": "/locks/data", "level": "CanNotDelete"},
                        "wal": {"lockId": "/locks/wal", "level": "CanNotDelete"}}
        receipt = {
            "version": 1, "status": "configuring", "configuredAtUtc": "2026-10-13T00:00:00+00:00",
            "ownerToken": self.token, "subscriptionId": self.ctx["subscriptionId"],
            "resourceGroup": self.ctx["resourceGroup"], "location": "eastus",
            "accountName": self.account, "accountId": MODULE.expected_account_id(ctx),
            "containerName": self.container, "containerPublicAccess": None,
            "destinationPath": MODULE.destination_path(self.account, self.container, self.token),
            "serverName": "hdd-pg", "secretReference": {"namespace": "repro", "name": MODULE.SECRET_NAME,
                                                                "key": MODULE.SECRET_KEY},
            "postgresImage": MODULE.SYSTEM_IMAGE,
            "backend": "CNPG 1.30.1 in-tree BarmanObjectStore", "plugin": None,
            "sourceSnapshotIds": {kind: self.pair[kind]["id"] for kind in ("data", "wal")},
            "sourceLockVerification": source_locks, "snapshotLineage": lineage,
            "connectionStringStoredIn": f"repro/{MODULE.SECRET_NAME}:{MODULE.SECRET_KEY}",
            "connectionStringPersistedLocally": False,
        }
        return ctx, receipt, source_locks, lineage

    def test_configuring_receipt_resumes_only_exact_owned_pair_and_lineage(self) -> None:
        ctx, receipt, source_locks, lineage = self._resume_context_and_receipt()
        MODULE.validate_configuring_receipt(receipt, ctx,
                                            account_id=MODULE.expected_account_id(ctx),
                                            source_locks=source_locks, lineage=lineage)
        for field, value in (("ownerToken", "f" * 32),
                             ("accountName", "someoneelsesaccount"),
                             ("destinationPath", "https://other.blob.core.windows.net/container/"),
                             ("status", "ready")):
            changed = {**receipt, field: value}
            with self.subTest(field=field), self.assertRaises(MODULE.Refusal):
                MODULE.validate_configuring_receipt(changed, ctx)
        changed_pair = json.loads(json.dumps(receipt))
        changed_pair["sourceSnapshotIds"]["data"] = changed_pair["sourceSnapshotIds"]["wal"]
        with self.assertRaises(MODULE.Refusal):
            MODULE.validate_configuring_receipt(changed_pair, ctx)
        changed_lineage = json.loads(json.dumps(receipt))
        changed_lineage["snapshotLineage"]["wal"]["volumeSnapshotContent"] = "unrelated-content"
        with self.assertRaises(MODULE.Refusal):
            MODULE.validate_configuring_receipt(changed_lineage, ctx, lineage=lineage)
        with self.assertRaises(MODULE.Refusal):
            MODULE.validate_configuring_receipt(receipt, ctx, source_locks={"changed": True})

    def test_configured_backend_must_be_exact_and_unknown_backend_is_rejected(self) -> None:
        expected = MODULE.make_barman_patch(self.account, self.container, self.token)["spec"]["backup"]["barmanObjectStore"]
        MODULE.validate_backup_store({}, None)
        MODULE.validate_backup_store({"barmanObjectStore": expected}, expected)
        wrong = json.loads(json.dumps(expected))
        wrong["destinationPath"] = "https://other.blob.core.windows.net/container/"
        with self.assertRaises(MODULE.Refusal):
            MODULE.validate_backup_store({"barmanObjectStore": wrong}, expected)
        with self.assertRaises(MODULE.Refusal):
            MODULE.validate_backup_store({"barmanObjectStore": expected}, None)

    def test_resume_secret_must_match_exact_secret_reference_and_credential(self) -> None:
        connection = "DefaultEndpointsProtocol=https;AccountName=redacted;AccountKey=topsecret=="
        secret = MODULE.make_secret_manifest(self.token, connection)
        MODULE.validate_archive_secret(secret, self.ctx, connection)
        changed = json.loads(json.dumps(secret))
        changed["data"][MODULE.SECRET_KEY] = base64.b64encode(b"different secret").decode()
        with self.assertRaises(MODULE.Refusal):
            MODULE.validate_archive_secret(changed, self.ctx, connection)

    def test_secret_access_readiness_queries_each_pod_service_account_twice(self) -> None:
        ctx = self.ctx | {"kubeconfig": Path("/private/kubeconfig")}
        command = MODULE.secret_access_argv(ctx, "cnpg-instance")
        self.assertIn("secrets/azure-barman-credentials", command)
        self.assertIn("system:serviceaccount:repro:cnpg-instance", command)
        self.assertNotIn("AccountKey", " ".join(command))
        pods = [{"spec": {"serviceAccountName": "cnpg-instance"}},
                {"spec": {"serviceAccountName": "cnpg-replica"}}]
        with patch.object(MODULE, "run", return_value=SimpleNamespace(returncode=0, stdout="yes\n")) as mocked_run, \
                patch.object(MODULE.time, "sleep"):
            result = MODULE.wait_for_secret_access(ctx, pods, timeout_seconds=30)
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["consecutiveReadOnlyAccessReviews"], 2)
        self.assertEqual(result["checkedServiceAccounts"], ["cnpg-instance", "cnpg-replica"])
        self.assertEqual(mocked_run.call_count, 4)

    def test_archiver_success_requires_new_archive_and_no_new_failure(self) -> None:
        self.assertTrue(MODULE.archiver_counts_advanced(
            {"archivedCount": 5, "failedCount": 1}, {"archivedCount": 6, "failedCount": 1}))
        self.assertFalse(MODULE.archiver_counts_advanced(
            {"archivedCount": 5, "failedCount": 1}, {"archivedCount": 5, "failedCount": 1}))
        with self.assertRaises(MODULE.Refusal):
            MODULE.archiver_counts_advanced(
                {"archivedCount": 5, "failedCount": 1}, {"archivedCount": 6, "failedCount": 2})

            # Restore the actual cluster name and corrupt one paired snapshot
            # ID; ownership and both members of the pinned pair must still gate.
            prepare["clusterName"] = json.loads((state_dir / "state.json").read_text())["clusterName"]
            prepare["sourcePair"]["dataSnapshotId"] = prepare["sourcePair"]["walSnapshotId"]
            prepare_path.write_text(json.dumps(prepare))
            with patch.object(MODULE, "EXPERIMENT", experiment), patch.object(MODULE, "HERE", shared):
                with self.assertRaises(MODULE.Refusal):
                    MODULE.validate_local("sigterm")

    def test_smoke_gate_requires_three_matching_real_hashes(self) -> None:
        good = "a" * 64
        MODULE.require_matching_smoke_hashes(good, good, good)
        with self.assertRaises(MODULE.Refusal):
            MODULE.require_matching_smoke_hashes(good, "b" * 64, good)
        with self.assertRaises(MODULE.Refusal):
            MODULE.require_matching_smoke_hashes("not-a-hash", "not-a-hash", "not-a-hash")

    def test_restore_path_cannot_target_pg_wal_or_pgdata(self) -> None:
        MODULE.validate_restore_destination("/run/wal-archive-smoke-0123456789ab-000000020000005700000009")
        for unsafe in ("/var/lib/postgresql/data/pgdata/pg_wal/000000020000005700000009",
                       "/tmp/pg_wal/000000020000005700000009",
                       "/tmp/PGDATA/smoke",
                       "/run/wal-archive-smoke-0123456789ab-../../etc/passwd",
                       "/run/wal-archive-smoke-0123456789ab-000000020000005700000009/../pg_wal/file"):
            with self.subTest(path=unsafe), self.assertRaises(MODULE.Refusal):
                MODULE.validate_restore_destination(unsafe)

    def test_smoke_wal_segment_is_retained_only_after_hash_verification(self) -> None:
        payload = b"closed wal segment proof"
        digest = hashlib.sha256(payload).hexdigest()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "azure-download"
            source.write_bytes(payload)
            relative = MODULE.retain_smoke_wal(root / "state", "000000020000005700000009", source, digest)
            target = root / "state" / relative
            self.assertEqual(target.read_bytes(), payload)
            self.assertEqual(target.stat().st_mode & 0o777, 0o600)
            self.assertEqual(MODULE.retain_smoke_wal(root / "state", "000000020000005700000009", source, digest), relative)
            with self.assertRaises(MODULE.Refusal):
                MODULE.retain_smoke_wal(root / "state", "000000020000005700000009", source, "f" * 64)

    def test_primary_wal_copy_retries_truncated_and_nonzero_streams(self) -> None:
        wal_name = "000000020000005700000009"
        segment = b"\0" * (16 * MODULE.MIB)
        digest = hashlib.sha256(segment).hexdigest()
        with tempfile.TemporaryDirectory(prefix="wal-copy-retry-test-") as raw:
            root = Path(raw)
            state_dir = root / "state"
            state_dir.mkdir(mode=0o700)
            destination = root / "wal.segment"
            ctx = {"stateDir": state_dir, "kubeconfig": root / "kubeconfig"}
            attempts_by_offset = {}
            receipts = []

            def fake_subprocess(argv, *, stdout, stderr, timeout, check):
                offset = next(int(arg.split("=", 1)[1]) for arg in argv if arg.startswith("skip="))
                attempt = attempts_by_offset.get(offset, 0) + 1
                attempts_by_offset[offset] = attempt
                receipts.append((offset, attempt))
                if offset == 0 and attempt == 1:
                    payload, code, error = b"truncated", 0, b""
                elif offset == 2 and attempt == 1:
                    payload, code, error = b"partial", 1, b"transient stream reset"
                else:
                    payload = segment[offset * MODULE.MIB:(offset + 2) * MODULE.MIB]
                    code, error = 0, b""
                return SimpleNamespace(returncode=code, stdout=payload, stderr=error)

            with patch.object(MODULE, "run", return_value=SimpleNamespace(stdout=f"{digest}  wal\n")) as mocked_hash, \
                    patch.object(MODULE.subprocess, "run", side_effect=fake_subprocess) as mocked_run, \
                    patch.object(MODULE.time, "sleep") as mocked_sleep:
                MODULE.copy_primary_wal(ctx, "hdd-pg-1", wal_name, destination)

            self.assertEqual(mocked_hash.call_count, 2)
            self.assertEqual(mocked_run.call_count, 10)
            self.assertEqual(mocked_sleep.call_count, 2)
            self.assertEqual(receipts[:4], [(0, 1), (0, 2), (2, 1), (2, 2)])
            self.assertEqual(destination.stat().st_size, 16 * MODULE.MIB)
            self.assertEqual(destination.read_bytes(), segment)
            self.assertEqual(destination.stat().st_mode & 0o777, 0o600)
            receipt = state_dir / "wal-archive-copy-attempts.jsonl"
            rows = [json.loads(line) for line in receipt.read_text().splitlines()]
            self.assertEqual([(row["offsetMiB"], row["attempt"]) for row in rows], receipts)
            self.assertEqual([row["bytes"] for row in rows[:4]], [len(b"truncated"), 2 * MODULE.MIB,
                                                                    len(b"partial"), 2 * MODULE.MIB])
            self.assertEqual([row["exitCode"] for row in rows[:4]], [0, 0, 1, 0])
            self.assertIn("transient stream reset", rows[2]["error"])
            self.assertEqual(receipt.stat().st_mode & 0o777, 0o600)

    def test_primary_wal_copy_exhaustion_preserves_private_attempt_receipts(self) -> None:
        wal_name = "000000020000005700000009"
        segment_hash = hashlib.sha256(b"\0" * (16 * MODULE.MIB)).hexdigest()
        with tempfile.TemporaryDirectory(prefix="wal-copy-exhaustion-test-") as raw:
            root = Path(raw)
            state_dir = root / "state"
            state_dir.mkdir(mode=0o700)
            destination = root / "wal.segment"
            ctx = {"stateDir": state_dir, "kubeconfig": root / "kubeconfig"}

            def failed_run(argv, *, stdout, stderr, timeout, check):
                return SimpleNamespace(returncode=1, stdout=b"short", stderr=b"copy failed")

            with patch.object(MODULE, "run", return_value=SimpleNamespace(stdout=f"{segment_hash}  wal\n")), \
                    patch.object(MODULE.subprocess, "run", side_effect=failed_run) as mocked_run, \
                    patch.object(MODULE.time, "sleep") as mocked_sleep:
                with self.assertRaisesRegex(MODULE.Refusal, "exact WAL chunk after three bounded attempts"):
                    MODULE.copy_primary_wal(ctx, "hdd-pg-1", wal_name, destination)

            self.assertEqual(mocked_run.call_count, 3)
            self.assertEqual(mocked_sleep.call_count, 2)
            self.assertEqual(destination.stat().st_size, 0)
            receipt = state_dir / "wal-archive-copy-attempts.jsonl"
            rows = [json.loads(line) for line in receipt.read_text().splitlines()]
            self.assertEqual([row["attempt"] for row in rows], [1, 2, 3])
            self.assertTrue(all(row["offsetMiB"] == 0 and row["exitCode"] == 1
                                and row["bytes"] == len(b"short") for row in rows))
            self.assertTrue(all(row["error"] == "copy failed" for row in rows))
            self.assertEqual(receipt.stat().st_mode & 0o777, 0o600)

    def test_restore_log_path_is_token_scoped_and_cannot_escape_tmp(self) -> None:
        MODULE.validate_restore_log_destination("/run/wal-archive-smoke-0123456789ab.json")
        for unsafe in ("/run/wal-archive-smoke-0123456789ab/../../var/log/postgres.json",
                       "/controller/log/postgres.json"):
            with self.subTest(path=unsafe):
                with self.assertRaises(MODULE.Refusal):
                    MODULE.validate_restore_log_destination(unsafe)


if __name__ == "__main__":
    unittest.main()
