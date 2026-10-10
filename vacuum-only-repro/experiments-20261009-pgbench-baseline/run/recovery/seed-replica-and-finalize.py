#!/usr/bin/env python3
"""Resume a CNPG snapshot-seeded replica from the already-retained offline Backup.

This is deliberately separate from the frozen run/02-prepare.sh.  It is local
dry-run by default; cloud/Kubernetes mutations require both --apply and the
run's exact owner token.  It never deletes snapshots or disks.  If apply fails,
the helper preserves live resources and receipts for root review; it does not
attempt a broad rollback or assume a rerun is safe.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent.parent
STATE_DIR = HERE / ".state"
RECOVERY = STATE_DIR / "recovery"
EVIDENCE = RECOVERY / "evidence"
NAMESPACE = "repro"
CLUSTER = "hdd-pg"
SECRET = "azure-barman-credentials"
STORAGE_CLASS = "azure-premiumv2"
SYSIMAGE = (
    "ghcr.io/cloudnative-pg/postgresql:16.15-202609140815-system-trixie"
    "@sha256:a30ea813ae1c82fc94743c4e71766688158b58d8ddf1f4e20ff5d3edb569a215"
)
IMAGE = (
    "ghcr.io/cloudnative-pg/postgresql:16.15-202609281634-standard-trixie"
    "@sha256:1dd42563007500b94b943f2a1620f724efedfc9eea2d07a50c6db4582c2431fd"
)
READER_ROLE = "acdd72a7-3385-48ef-bd42-f606fba81ae7"


class Refusal(RuntimeError):
    pass


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Refusal(f"Cannot read valid JSON input {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise Refusal(f"Expected an object in {path}")
    return value


def canon(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def clean_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)[:120] or "item"


class Runner:
    def __init__(self, subscription: str, kubeconfig: Path, apply: bool):
        self.subscription = subscription
        self.kubeconfig = kubeconfig
        self.apply = apply

    def call(self, argv: list[str], *, timeout_s: int = 180, stdin: str | None = None) -> str:
        try:
            result = subprocess.run(
                argv,
                input=stdin,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout_s,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise Refusal(f"Command failed to complete ({shlex.join(argv[:3])}…): {exc}") from exc
        if result.returncode:
            # Never echo stdin or secret-bearing command output.
            detail = result.stderr.strip().splitlines()[-1:] or [f"exit {result.returncode}"]
            raise Refusal(f"Command failed ({shlex.join(argv[:3])}…): {detail[0]}")
        return result.stdout

    def k(self, *args: str, timeout_s: int = 180, stdin: str | None = None) -> str:
        return self.call(
            ["kubectl", f"--kubeconfig={self.kubeconfig}", *args],
            timeout_s=timeout_s,
            stdin=stdin,
        )

    def kj(self, *args: str, timeout_s: int = 180) -> dict[str, Any]:
        return json.loads(self.k(*args, "-o", "json", timeout_s=timeout_s))

    def az(self, *args: str, timeout_s: int = 900) -> str:
        return self.call(["az", *args, "--subscription", self.subscription, "-o", "json"], timeout_s=timeout_s)

    def azj(self, *args: str, timeout_s: int = 900) -> Any:
        return json.loads(self.az(*args, timeout_s=timeout_s))


class Journal:
    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)

    def event(self, phase: str, resource: str, name: str, details: str) -> None:
        obj = {"atUtc": utc_now(), "phase": phase, "resource": resource, "name": name, "details": details}
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, canon(obj))
            os.fsync(fd)
        finally:
            os.close(fd)

    def evidence(self, phase: str, resource: str, name: str, value: Any) -> Path:
        raw = canon(value)
        digest = hashlib.sha256(raw).hexdigest()
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        path = EVIDENCE / f"{stamp}-{clean_name(phase)}-{clean_name(resource)}-{clean_name(name)}-{digest}.json"
        EVIDENCE.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.write(fd, raw)
            os.fsync(fd)
        finally:
            os.close(fd)
        self.event(phase, resource, name, f"sha256={digest} file={path}")
        return path


def jq_like_time(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def require(condition: bool, message: str) -> None:
    if not condition:
        raise Refusal(message)


def locate_evidence(pattern: str) -> dict[str, Any]:
    return read_json(locate_evidence_path(pattern))


def locate_evidence_path(pattern: str) -> Path:
    paths = sorted(EVIDENCE.glob(pattern))
    require(bool(paths), f"Missing required saved evidence: {pattern}")
    # Repeated progress captures are append-only. Use the latest exact phase.
    return paths[-1]


def locate_named_evidence(pattern: str, metadata_name: str) -> tuple[Path, dict[str, Any]]:
    matches: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(EVIDENCE.glob(pattern)):
        value = read_json(path)
        if value.get("metadata", {}).get("name") == metadata_name:
            matches.append((path, value))
    require(bool(matches), f"Missing saved {metadata_name} evidence matching {pattern}")
    return matches[-1]


def direct_lock(locks: list[dict[str, Any]], resource_id: str) -> bool:
    target = resource_id.rstrip("/").lower()
    for lock in locks:
        if lock.get("level") != "CanNotDelete":
            continue
        scope = lock.get("scope")
        if not scope and lock.get("id"):
            scope = lock["id"].lower().split("/providers/microsoft.authorization/locks/", 1)[0]
        if isinstance(scope, str) and scope.rstrip("/").lower() == target:
            return True
    return False


def verify_disk_response(disk: dict[str, Any], *, expected_id: str, node_rg: str, location: str,
                         zone: str, expected_source: str) -> dict[str, Any]:
    creation = disk.get("creationData") or {}
    percent = disk.get("completionPercent")
    try:
        percent = float(percent)
    except (TypeError, ValueError):
        percent = None
    zones = [str(item) for item in (disk.get("zones") or [])]
    require((disk.get("id") or "").lower() == expected_id.lower(), "Azure disk ID does not match its PV handle.")
    require(f"/resourcegroups/{node_rg.lower()}/providers/microsoft.compute/disks/" in expected_id.lower(),
            "PVC disk is outside the pinned AKS node resource group.")
    require((disk.get("location") or "").lower() == location.lower(), "PVC disk location mismatch.")
    require((disk.get("sku") or {}).get("name") == "PremiumV2_LRS", "PVC disk is not PremiumV2_LRS.")
    require((disk.get("diskSizeGB") or disk.get("diskSizeGb")) == 512, "PVC disk is not 512 GiB.")
    require(disk.get("diskIOPSReadWrite") == 3000 and disk.get("diskMBpsReadWrite") == 125,
            "PVC disk IOPS/throughput differ from the run configuration.")
    require(disk.get("provisioningState") == "Succeeded" and disk.get("managedBy"),
            "PVC disk is not provisioned and attached.")
    require(creation.get("createOption") in ("Copy", "CopyStart") and percent == 100,
            "PVC disk is not a fully hydrated snapshot copy (numeric 100 required).")
    require(zone in zones, "PVC disk is outside the pinned availability zone.")
    sector = disk.get("logicalSectorSize") or creation.get("logicalSectorSize")
    require(str(sector) == "512", "PVC disk logical sector size is not 512 bytes.")
    source = creation.get("sourceResourceId")
    require(isinstance(source, str) and source.lower() == expected_source.lower(),
            "PVC disk sourceResourceId does not match the pinned snapshot lineage.")
    return {
        "id": disk["id"], "sku": disk["sku"]["name"],
        "diskSizeGiB": disk.get("diskSizeGB") or disk.get("diskSizeGb"),
        "iops": disk.get("diskIOPSReadWrite"), "throughputMBps": disk.get("diskMBpsReadWrite"),
        "completionPercent": percent, "createOption": creation.get("createOption"),
        "sector": sector, "zones": zones, "provisioningState": disk.get("provisioningState"),
        "sourceResourceId": source,
    }


def psql(runner: Runner, kubeconfig: Path, pod: str, sql: str, timeout_s: int = 180) -> str:
    return runner.k(
        "exec", "-n", NAMESPACE, "-c", "postgres", pod, "--",
        "psql", "-XAtq", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "repro", "-c", sql,
        timeout_s=timeout_s,
    ).strip()


def run_apply(runner: Runner, journal: Journal, *, state: dict[str, Any], manifest: dict[str, Any],
              snapshot_meta: dict[str, Any], source_bindings: dict[str, Any], storage: dict[str, Any],
              saved_cluster: dict[str, Any], saved_backup: dict[str, Any]) -> None:
    token = state["ownerToken"]
    sub = state["subscriptionId"]
    rg = state["resourceGroup"]
    node_rg = state["nodeResourceGroup"]
    snap_rg = state["snapshotResourceGroup"]
    location = state["location"]
    zone = str(state["availabilityZone"])
    short = token[:8]
    backup_name = f"vacuum-seed-{short}"
    data_name = f"source-data-{short}"
    wal_name = f"source-wal-{short}"
    seed_data_vss = backup_name
    seed_wal_vss = f"{backup_name}-wal"
    server_name = f"hdd-pg-seed-{short}"
    source_data_id = manifest["sourceSnapshotId"]
    source_wal_id = manifest["pairedWAL"]["sourceSnapshotId"]
    output_records: list[dict[str, Any]] = []
    source_lsn = manifest["backup"]["endLSN"]

    account = storage.get("accountName")
    container = storage.get("containerName")
    require(bool(account and container), "Owned Blob storage account/container metadata is incomplete.")
    destination = f"https://{account}.blob.core.windows.net/{container}/cnpg-vacuum-only/{token}/"
    expected_writer = {
        "destinationPath": destination,
        "serverName": server_name,
        "azureCredentials": {"connectionString": {"name": SECRET, "key": "connection-string"}},
    }

    context = runner.k("config", "current-context").strip()
    require(state["clusterName"].lower() in context.lower(), "Kube context does not belong to this pinned AKS run.")
    ns = runner.kj("get", "namespace", NAMESPACE)
    require((ns.get("metadata", {}).get("labels", {}).get("repro.stack.token")) == token,
            "Namespace owner token mismatch.")
    secret = runner.kj("get", "secret", SECRET, "-n", NAMESPACE)
    require(secret.get("metadata", {}).get("labels", {}).get("repro.stack.token") == token
            and len((secret.get("data") or {}).get("connection-string", "")) > 0,
            "Run-owned WAL Secret is missing or invalid.")
    # Intentionally do not persist/print the Secret response.

    current_cluster = runner.kj("get", "cluster", CLUSTER, "-n", NAMESPACE)
    current_uid = current_cluster.get("metadata", {}).get("uid")
    require(current_uid == saved_cluster.get("metadata", {}).get("uid"), "Cluster UID changed since primary recovery.")
    require(current_cluster.get("metadata", {}).get("labels", {}).get("repro.stack.token") == token,
            "Cluster owner token mismatch.")
    spec = current_cluster.get("spec") or {}
    status = current_cluster.get("status") or {}
    require(spec.get("instances") == 1 and status.get("readyInstances") == 1
            and status.get("currentPrimary") == f"{CLUSTER}-1",
            "Safe continuation requires exactly the recovered one-instance primary.")
    require(status.get("systemID") == str(snapshot_meta["systemIdentifier"]), "Primary system identifier changed.")
    require(spec.get("imageName") == SYSIMAGE, "Primary is not on the pinned recovery image before seeding.")
    require((spec.get("bootstrap") or {}).get("recovery", {}).get("source") == "retained-source",
            "Original retained-source recovery reference changed.")
    require((spec.get("backup") or {}).get("volumeSnapshot") is not None,
            "Retained volumeSnapshot backup configuration disappeared.")
    require(spec.get("externalClusters"), "Retained WAL source is not available during snapshot seeding.")
    if spec.get("storage", {}).get("storageClass") != STORAGE_CLASS:
        raise Refusal("Primary DATA storage class changed.")
    if (spec.get("walStorage") or {}).get("storageClass") != STORAGE_CLASS:
        raise Refusal("Primary WAL storage class changed.")
    journal.evidence("continuation-primary-one-ready", "cluster", CLUSTER, current_cluster)

    backups = runner.kj("get", "backups", "-n", NAMESPACE)
    candidates = [
        item for item in backups.get("items", [])
        if (item.get("spec", {}).get("cluster") or {}).get("name") == CLUSTER
        and item.get("status", {}).get("method") == "volumeSnapshot"
        and item.get("status", {}).get("phase") == "completed"
        and item.get("metadata", {}).get("creationTimestamp", "") >= current_cluster["metadata"]["creationTimestamp"]
    ]
    require(len(candidates) == 1 and candidates[0].get("metadata", {}).get("name") == backup_name,
            "The seed Backup is not the sole completed post-Cluster snapshot candidate; refusing ambiguous selection.")
    backup = runner.kj("get", "backup", backup_name, "-n", NAMESPACE)
    require(backup.get("metadata", {}).get("uid") == saved_backup.get("metadata", {}).get("uid"),
            "Seed Backup UID changed since the persisted completion evidence.")
    require(backup.get("metadata", {}).get("labels", {}).get("repro.stack.token") == token,
            "Seed Backup owner token mismatch.")
    require(backup.get("spec", {}).get("cluster", {}).get("name") == CLUSTER
            and backup.get("spec", {}).get("method") == "volumeSnapshot"
            and backup.get("spec", {}).get("online") is False
            and backup.get("status", {}).get("phase") == "completed"
            and backup.get("status", {}).get("method") == "volumeSnapshot"
            and backup.get("status", {}).get("majorVersion") == 16,
            "Existing offline paired Backup is not a completed PG16 volumeSnapshot candidate.")
    require(backup.get("status", {}).get("online") in (False, None), "Seed Backup unexpectedly reports online mode.")
    require(jq_types(backup) == ["PG_DATA", "PG_WAL"], "Seed Backup does not contain exactly paired DATA/WAL elements.")
    journal.evidence("continuation-seed-backup-verified", "backup", backup_name, backup)

    # Verify the immutable original source snapshots and exact scoped retention locks.
    source_bindings = source_bindings["bindings"]
    source_locks = runner.azj("lock", "list", "--resource-group", manifest["resourceGroup"])
    for role, source_id, expected_uid, binding_key in (
        ("data", source_data_id, manifest["uniqueId"], "data"),
        ("wal", source_wal_id, manifest["pairedWAL"]["uniqueId"], "wal"),
    ):
        live = runner.azj("snapshot", "show", "--ids", source_id)
        binding = source_bindings[binding_key]
        require(live.get("uniqueId") == expected_uid and live.get("provisioningState") == "Succeeded"
                and live.get("snapshotAccessState") == "Available"
                and (live.get("completionPercent") is None or float(live["completionPercent"]) == 100)
                and (live.get("diskSizeGB") or live.get("diskSizeGb")) == 512
                and live.get("tags", {}).get("reprotoken") == manifest["ownerToken"],
                f"Pinned source {role} snapshot identity/readiness changed.")
        require(binding.get("sourceSnapshotId", "").lower() == source_id.lower()
                and binding.get("sourceSnapshotUniqueId") == expected_uid,
                f"Persisted source {role} binding differs from the pinned manifest.")
        require(direct_lock(source_locks, source_id), f"Pinned source {role} snapshot is missing its direct CanNotDelete lock.")
        journal.evidence("continuation-source-snapshot-verified", "azureSnapshot", role, live)

    # Require the existing paired backup VSSs to be fully ready before CNPG is asked
    # to create replica claims. CNPG 1.30.1 uses these exact Backup elements for the
    # replica source when WAL archiving is configured on the Cluster.
    output_by_type = {
        element["type"]: element["name"]
        for element in backup["status"]["snapshotBackupStatus"]["elements"]
    }
    output_ids: dict[str, str] = {}
    live_primary_pvcs = {
        item["metadata"]["name"]: item
        for item in runner.kj("get", "pvc", "-n", NAMESPACE, "-l", f"cnpg.io/cluster={CLUSTER}").get("items", [])
    }
    require(set(live_primary_pvcs) == {f"{CLUSTER}-1", f"{CLUSTER}-1-wal"},
            "Expected only the two primary PVCs before replica creation.")
    saved_primary_pvcs = {
        f"{CLUSTER}-1": locate_named_evidence(
            "*-seed-primary-pvc-source-persistentVolumeClaim-*.json", f"{CLUSTER}-1"),
        f"{CLUSTER}-1-wal": locate_named_evidence(
            "*-seed-primary-pvc-source-persistentVolumeClaim-*.json", f"{CLUSTER}-1-wal"),
    }
    for typ, vss_name, pvc_name in (
        ("PG_DATA", output_by_type["PG_DATA"], f"{CLUSTER}-1"),
        ("PG_WAL", output_by_type["PG_WAL"], f"{CLUSTER}-1-wal"),
    ):
        pvc = live_primary_pvcs[pvc_name]
        saved_pvc_path, saved_pvc = saved_primary_pvcs[pvc_name]
        expected_primary_vss = data_name if typ == "PG_DATA" else wal_name
        pvc_labels = pvc.get("metadata", {}).get("labels", {})
        pvc_owner_refs = pvc.get("metadata", {}).get("ownerReferences", [])
        datasource = pvc.get("spec", {}).get("dataSource") or {}
        datasource_ref = pvc.get("spec", {}).get("dataSourceRef") or {}
        require(pvc.get("status", {}).get("phase") == "Bound"
                and pvc.get("metadata", {}).get("uid") == saved_pvc.get("metadata", {}).get("uid")
                and any(ref.get("kind") == "Cluster" and ref.get("name") == CLUSTER
                        and ref.get("uid") == current_uid for ref in pvc_owner_refs)
                and pvc_labels.get("cnpg.io/cluster") == CLUSTER
                and pvc_labels.get("cnpg.io/instanceName") == f"{CLUSTER}-1"
                and pvc_labels.get("cnpg.io/pvcRole") == typ
                and datasource.get("kind") == "VolumeSnapshot"
                and datasource.get("name") == expected_primary_vss
                and datasource_ref.get("kind") == "VolumeSnapshot"
                and datasource_ref.get("name") == expected_primary_vss,
                f"Primary PVC {pvc_name} differs from its saved UID/Cluster/snapshot-source evidence.")
        pv_name = pvc.get("spec", {}).get("volumeName")
        require(bool(pv_name), f"Primary PVC {pvc_name} has no bound PV.")
        pv = runner.kj("get", "pv", pv_name)
        saved_pv_path, saved_pv = locate_named_evidence(
            "*-primary-pv-source-persistentVolume-*.json", pv_name)
        claim_ref = pv.get("spec", {}).get("claimRef") or {}
        saved_claim_ref = saved_pv.get("spec", {}).get("claimRef") or {}
        require(claim_ref.get("namespace") == NAMESPACE and claim_ref.get("name") == pvc_name
                and claim_ref.get("uid") == pvc.get("metadata", {}).get("uid")
                and pv.get("metadata", {}).get("uid") == saved_pv.get("metadata", {}).get("uid")
                and saved_claim_ref.get("namespace") == NAMESPACE
                and saved_claim_ref.get("name") == pvc_name
                and saved_claim_ref.get("uid") == saved_pvc.get("metadata", {}).get("uid")
                and saved_pv.get("spec", {}).get("persistentVolumeReclaimPolicy") == "Retain"
                and pv.get("spec", {}).get("persistentVolumeReclaimPolicy") == "Retain"
                and pv.get("spec", {}).get("csi", {}).get("driver") == "disk.csi.azure.com",
                f"Primary PV for {pvc_name} claimRef/reclaim policy/driver is not the saved Retain binding.")
        vss = runner.kj("get", "volumesnapshot", vss_name, "-n", NAMESPACE)
        expected_pvc_role = "PG_DATA" if typ == "PG_DATA" else "PG_WAL"
        require(vss.get("metadata", {}).get("labels", {}).get("cnpg.io/backupName") == backup_name
                and vss.get("metadata", {}).get("labels", {}).get("cnpg.io/cluster") == CLUSTER
                and vss.get("metadata", {}).get("labels", {}).get("repro.stack.token") == token
                and vss.get("status", {}).get("readyToUse") is True
                and vss.get("spec", {}).get("source", {}).get("persistentVolumeClaimName") == pvc_name,
                f"Backup {typ} VolumeSnapshot is not ready/owned/bound to its exact primary PVC.")
        require(vss.get("metadata", {}).get("annotations", {}).get("vacuum-only-repro/seed-backup-uid")
                == backup["metadata"]["uid"]
                and vss.get("metadata", {}).get("annotations", {}).get("vacuum-only-repro/source-pvc-uid")
                == pvc["metadata"]["uid"], f"Backup {typ} VolumeSnapshot owner UID annotations mismatch.")
        content_name = vss.get("status", {}).get("boundVolumeSnapshotContentName")
        require(bool(content_name), f"Backup {typ} VolumeSnapshot lacks bound VolumeSnapshotContent.")
        content = runner.kj("get", "volumesnapshotcontent", content_name)
        require(content.get("spec", {}).get("deletionPolicy") == "Retain"
                and content.get("spec", {}).get("driver") == "disk.csi.azure.com"
                and content.get("spec", {}).get("volumeSnapshotRef", {}).get("uid") == vss["metadata"]["uid"]
                and content.get("status", {}).get("readyToUse") is True,
                f"Backup {typ} VolumeSnapshotContent is not retained and bound to the exact VSS UID.")
        snap_id = content.get("status", {}).get("snapshotHandle")
        require(isinstance(snap_id, str) and f"/resourcegroups/{snap_rg.lower()}/providers/microsoft.compute/snapshots/" in snap_id.lower(),
                f"Backup {typ} Azure snapshot escaped the run-owned retained group.")
        arm = runner.azj("snapshot", "show", "--ids", snap_id)
        saved_seed_matches = []
        for evidence_path in EVIDENCE.glob("*-seed-arm-snapshot-observed-azureSnapshot-*.json"):
            evidence_value = read_json(evidence_path)
            if (evidence_value.get("id") or "").lower() == snap_id.lower():
                saved_seed_matches.append((evidence_path, evidence_value))
        require(len(saved_seed_matches) == 1,
                f"Expected one persisted seed-snapshot observation for Backup {typ} output.")
        saved_seed_path, saved_seed_arm = saved_seed_matches[0]
        primary_disk_id = (pv.get("spec", {}).get("csi") or {}).get("volumeHandle")
        arm_creation = arm.get("creationData") or {}
        saved_creation = saved_seed_arm.get("creationData") or {}
        current_seed_source = arm_creation.get("sourceResourceId")
        saved_seed_source = saved_creation.get("sourceResourceId")
        current_source_uid = arm_creation.get("sourceUniqueId")
        saved_source_uid = saved_creation.get("sourceUniqueId")
        completion = arm.get("completionPercent")
        require(arm.get("id", "").lower() == snap_id.lower()
                and arm.get("provisioningState") == "Succeeded"
                and arm.get("snapshotAccessState") == "Available"
                and (completion is None or float(completion) == 100)
                and (arm.get("diskSizeGB") or arm.get("diskSizeGb")) == 512
                and arm.get("incremental") is True
                and bool(arm.get("uniqueId"))
                and arm.get("uniqueId") == saved_seed_arm.get("uniqueId")
                and isinstance(primary_disk_id, str)
                and isinstance(current_seed_source, str) and current_seed_source.lower() == primary_disk_id.lower()
                and isinstance(saved_seed_source, str) and saved_seed_source.lower() == primary_disk_id.lower()
                and bool(current_source_uid) and current_source_uid == saved_source_uid
                and arm.get("tags", {}).get("reprotoken") == token
                and arm.get("tags", {}).get("reproPurpose") == "premiumv2-vacuum-backup",
                f"Backup {typ} Azure snapshot is not the exact ready run-owned retained output.")
        seed_locks = runner.azj("lock", "list", "--resource-group", snap_rg)
        require(direct_lock(seed_locks, snap_id), f"Backup {typ} snapshot lacks its direct CanNotDelete lock.")
        output_ids[typ] = snap_id
        record = {
            "id": snap_id, "volumeSnapshot": vss_name, "sourcePVC": pvc_name,
            "volumeSnapshotUid": vss["metadata"]["uid"],
            "volumeSnapshotContentUid": content["metadata"]["uid"],
            "backupUid": backup["metadata"]["uid"], "retentionLock": "CanNotDelete",
            "uniqueId": arm.get("uniqueId"), "seedSnapshotEvidence": str(saved_seed_path),
        }
        output_records.append(record)
        journal.evidence("continuation-seed-snapshot-verified", "azureSnapshot", typ, arm)
        journal.evidence("continuation-seed-vss-verified", "volumeSnapshot", vss_name, vss)
        journal.evidence("continuation-seed-vsc-verified", "volumeSnapshotContent", content_name, content)
        journal.evidence("continuation-primary-pvc-pv-source-verified", "persistentVolumeClaim", pvc_name,
                         {"pvc": pvc, "pvcEvidence": str(saved_pvc_path), "pv": pv,
                          "pvEvidence": str(saved_pv_path)})

    # The Cluster needs its own WAL archive writer to make a completed Backup a
    # valid replica source. The bootstrap externalCluster is read-only and cannot
    # satisfy CNPG's replica-candidate gate. Use a separate serverName so new WALs
    # never collide with the imported retained-source archive under hdd-pg/.
    current_writer = (spec.get("backup") or {}).get("barmanObjectStore")
    if current_writer is not None and current_writer != expected_writer:
        raise Refusal("Cluster already has a different Barman writer; refusing to adopt/change it.")
    if current_writer is None:
        journal.event("active-archive-writer-patch-intent", "cluster", CLUSTER,
                      f"serverName={server_name}; distinct from retained-source hdd-pg")
        patch = {"spec": {"backup": {"barmanObjectStore": expected_writer}}}
        runner.k("patch", "cluster", CLUSTER, "-n", NAMESPACE, "--type=merge", "-p", json.dumps(patch))
    current_cluster = runner.kj("get", "cluster", CLUSTER, "-n", NAMESPACE)
    require((current_cluster.get("spec", {}).get("backup") or {}).get("barmanObjectStore") == expected_writer,
            "Active run-owned Barman writer config did not persist exactly.")
    require(current_cluster.get("metadata", {}).get("uid") == saved_cluster["metadata"]["uid"],
            "Cluster UID changed while enabling the archive writer.")
    journal.evidence("active-archive-writer-enabled", "cluster", CLUSTER, current_cluster)

    # Prove a WAL segment archived through the new, distinct writer before scale-up.
    # This is WAL-only; no fixture DML or row count is run.
    archive_sql = "SELECT json_build_object('archived',archived_count,'failed',failed_count,'lastWal',last_archived_wal)::text FROM pg_stat_archiver;"
    before = json.loads(psql(runner, runner.kubeconfig, f"{CLUSTER}-1", archive_sql))
    psql(runner, runner.kubeconfig, f"{CLUSTER}-1", "SELECT pg_switch_wal();")
    journal.event("replica-seed-wal-switch-requested", "postgres", f"{CLUSTER}-1",
                  f"beforeArchived={before.get('archived')} beforeFailed={before.get('failed')}")
    end = time.monotonic() + 1800
    archive_after: dict[str, Any] = {}
    while time.monotonic() < end:
        try:
            archive_after = json.loads(psql(runner, runner.kubeconfig, f"{CLUSTER}-1", archive_sql))
        except (Refusal, json.JSONDecodeError):
            time.sleep(5)
            continue
        if (int(archive_after.get("archived", 0)) > int(before.get("archived", 0))
                and int(archive_after.get("failed", 0)) == int(before.get("failed", 0))
                and archive_after.get("lastWal")):
            break
        time.sleep(5)
    require(int(archive_after.get("archived", 0)) > int(before.get("archived", 0))
            and int(archive_after.get("failed", 0)) == int(before.get("failed", 0))
            and archive_after.get("lastWal"),
            "Active run-owned WAL archive did not successfully archive a segment; refusing replica scale-up.")
    journal.evidence("replica-seed-wal-archive-verified", "postgresArchive", f"{CLUSTER}-1",
                     {"before": before, "after": archive_after, "serverName": server_name})

    # Recheck the candidate immediately before scaling. Do not allow a second
    # Backup or missing snapshot to make CNPG choose a different source.
    current_cluster = runner.kj("get", "cluster", CLUSTER, "-n", NAMESPACE)
    require(current_cluster.get("metadata", {}).get("uid") == saved_cluster["metadata"]["uid"]
            and current_cluster.get("spec", {}).get("instances") == 1
            and (current_cluster.get("status") or {}).get("readyInstances") == 1,
            "Cluster changed before snapshot replica scaling.")
    live_backups = runner.kj("get", "backups", "-n", NAMESPACE).get("items", [])
    eligible = [b for b in live_backups if b.get("metadata", {}).get("name") == backup_name
                and b.get("metadata", {}).get("uid") == backup["metadata"]["uid"]
                and b.get("status", {}).get("phase") == "completed"
                and b.get("status", {}).get("method") == "volumeSnapshot"]
    require(len(eligible) == 1, "Exact offline Backup candidate changed before scaling.")
    pvc_list = runner.kj("get", "pvc", "-n", NAMESPACE, "-l", f"cnpg.io/cluster={CLUSTER}").get("items", [])
    require({p.get("metadata", {}).get("name") for p in pvc_list} == {f"{CLUSTER}-1", f"{CLUSTER}-1-wal"},
            "Replica PVCs exist before scale-up; refusing to adopt or overwrite them.")
    jobs = runner.kj("get", "jobs", "-n", NAMESPACE).get("items", [])
    require(not any(j.get("metadata", {}).get("labels", {}).get("cnpg.io/instanceName") == f"{CLUSTER}-2"
                    for j in jobs), "A pre-existing replica job exists; refusing adoption.")

    journal.event("snapshot-replica-scale-intent", "cluster", CLUSTER,
                  f"backupUid={backup['metadata']['uid']} data={seed_data_vss} wal={seed_wal_vss}")
    scale_patch = {
        "spec": {
            "instances": 2,
            "postgresql": {
                "synchronous": {"method": "any", "number": 1, "dataDurability": "preferred"},
                "parameters": {"synchronous_commit": "on"},
            },
        }
    }
    runner.k("patch", "cluster", CLUSTER, "-n", NAMESPACE, "--type=merge", "-p", json.dumps(scale_patch))
    journal.event("snapshot-replica-scale-requested", "cluster", CLUSTER, "instances=2 synchronous=ANY 1 preferred synchronous_commit=on")

    job_uid: str | None = None
    job_record: dict[str, Any] | None = None
    deadline = time.monotonic() + 10800
    while time.monotonic() < deadline:
        current_cluster = runner.kj("get", "cluster", CLUSTER, "-n", NAMESPACE)
        require(current_cluster.get("metadata", {}).get("uid") == saved_cluster["metadata"]["uid"],
                "Cluster UID changed during replica creation.")
        job_items = runner.kj("get", "jobs", "-n", NAMESPACE).get("items", [])
        matching = [j for j in job_items if j.get("metadata", {}).get("labels", {}).get("cnpg.io/cluster") == CLUSTER
                    and j.get("metadata", {}).get("labels", {}).get("cnpg.io/instanceName") == f"{CLUSTER}-2"]
        wrong = [j for j in matching if j.get("metadata", {}).get("labels", {}).get("cnpg.io/jobRole") != "snapshot-recovery"]
        if wrong:
            # Stop the observed pg_basebackup/join path immediately. Only reverse
            # this helper's exact 2->1 scale under the same Cluster UID.
            before_stop = runner.kj("get", "cluster", CLUSTER, "-n", NAMESPACE)
            if before_stop.get("metadata", {}).get("uid") == saved_cluster["metadata"]["uid"] \
                    and before_stop.get("spec", {}).get("instances") == 2:
                resource_version = before_stop.get("metadata", {}).get("resourceVersion")
                patch_ops = [
                    {"op": "test", "path": "/metadata/uid", "value": saved_cluster["metadata"]["uid"]},
                    {"op": "test", "path": "/metadata/resourceVersion", "value": resource_version},
                    {"op": "replace", "path": "/spec/instances", "value": 1},
                    {"op": "remove", "path": "/spec/postgresql/synchronous"},
                ]
                journal.event("wrong-replica-job-scale-down-intent", "cluster", CLUSTER,
                              f"observedJobRole={wrong[0].get('metadata', {}).get('labels', {}).get('cnpg.io/jobRole')}")
                runner.k("patch", "cluster", CLUSTER, "-n", NAMESPACE, "--type=json", "-p", json.dumps(patch_ops))
                journal.event("wrong-replica-job-scaled-down", "cluster", CLUSTER,
                              "stopped non-snapshot replica path; all snapshots remain untouched")
            raise Refusal("CNPG selected a non-snapshot replica job; helper scaled back to one instance and stopped.")
        snapshots = [j for j in matching if j.get("metadata", {}).get("labels", {}).get("cnpg.io/jobRole") == "snapshot-recovery"]
        if snapshots:
            job_record = snapshots[0]
            job_uid = job_record.get("metadata", {}).get("uid")
            container = next((c for c in job_record.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
                              if c.get("name") == "snapshot-recovery"), None)
            command = " ".join((container or {}).get("command", []))
            if not ((container or {}).get("image") == SYSIMAGE and "restoresnapshot" in command
                    and "--immediate" in command):
                before_stop = runner.kj("get", "cluster", CLUSTER, "-n", NAMESPACE)
                if before_stop.get("metadata", {}).get("uid") == saved_cluster["metadata"]["uid"] \
                        and before_stop.get("spec", {}).get("instances") == 2:
                    ops = [
                        {"op": "test", "path": "/metadata/uid", "value": saved_cluster["metadata"]["uid"]},
                        {"op": "test", "path": "/metadata/resourceVersion", "value": before_stop["metadata"]["resourceVersion"]},
                        {"op": "replace", "path": "/spec/instances", "value": 1},
                        {"op": "remove", "path": "/spec/postgresql/synchronous"},
                    ]
                    runner.k("patch", "cluster", CLUSTER, "-n", NAMESPACE, "--type=json", "-p", json.dumps(ops))
                raise Refusal("CNPG snapshot-recovery Job command/image was not the approved path; scaled back to one.")
            journal.evidence("snapshot-recovery-job-observed", "job", job_record["metadata"]["name"], job_record)
        if job_record:
            status = job_record.get("status") or {}
            if status.get("failed", 0) > 0:
                raise Refusal("CNPG snapshot-recovery Job failed; stop and inspect its preserved evidence.")
            if status.get("succeeded", 0) > 0 and (current_cluster.get("status") or {}).get("readyInstances") == 2:
                break
        time.sleep(5)
    require(job_uid and job_record and (job_record.get("status") or {}).get("succeeded", 0) > 0,
            "No completed snapshot-recovery Job was observed within the timeout.")
    current_cluster = runner.kj("get", "cluster", CLUSTER, "-n", NAMESPACE)
    require(current_cluster.get("status", {}).get("readyInstances") == 2
            and current_cluster.get("status", {}).get("currentPrimary") == f"{CLUSTER}-1"
            and current_cluster.get("status", {}).get("systemID") == str(snapshot_meta["systemIdentifier"]),
            "Two-instance Cluster readiness/primary/system ID mismatch.")
    journal.evidence("snapshot-recovery-primary-and-replica-ready", "cluster", CLUSTER, current_cluster)
    journal.evidence("snapshot-recovery-job-complete", "job", job_record["metadata"]["name"], job_record)

    events = runner.kj("get", "events", "-n", NAMESPACE).get("items", [])
    completed_events = [e for e in events if (e.get("involvedObject") or {}).get("uid") == job_uid
                        and e.get("reason") == "Completed" and e.get("message") == "Job completed"]
    require(bool(completed_events), "Completed Event for the exact snapshot-recovery Job UID is missing.")
    journal.evidence("snapshot-recovery-completion-event", "event", job_record["metadata"]["name"], completed_events[-1])

    pvc_list = runner.kj("get", "pvc", "-n", NAMESPACE, "-l", f"cnpg.io/cluster={CLUSTER}").get("items", [])
    pvc_by_name = {p.get("metadata", {}).get("name"): p for p in pvc_list}
    expected_sources = {
        f"{CLUSTER}-1": data_name,
        f"{CLUSTER}-1-wal": wal_name,
        f"{CLUSTER}-2": seed_data_vss,
        f"{CLUSTER}-2-wal": seed_wal_vss,
    }
    require(set(pvc_by_name) == set(expected_sources), "CNPG did not create exactly the four expected DATA/WAL PVCs.")
    journal.evidence("four-pvcs-snapshot-source-verified", "persistentVolumeClaims", CLUSTER,
                     {"items": pvc_list, "expectedSources": expected_sources})
    pvcs_for_disks: list[tuple[str, str, str]] = []
    for claim, expected_vss in expected_sources.items():
        pvc = pvc_by_name[claim]
        require(pvc.get("status", {}).get("phase") == "Bound"
                and pvc.get("metadata", {}).get("labels", {}).get("cnpg.io/cluster") == CLUSTER,
                f"PVC {claim} is not Bound/owned by the expected Cluster.")
        data_source = pvc.get("spec", {}).get("dataSource") or pvc.get("spec", {}).get("dataSourceRef") or {}
        require(data_source.get("kind") == "VolumeSnapshot" and data_source.get("name") == expected_vss,
                f"PVC {claim} is not cloned from exact expected VolumeSnapshot {expected_vss}.")
        pv_name = pvc.get("spec", {}).get("volumeName")
        require(bool(pv_name), f"PVC {claim} has no PV name.")
        pv = runner.kj("get", "pv", pv_name)
        disk_id = (pv.get("spec", {}).get("csi") or {}).get("volumeHandle")
        require(bool(disk_id), f"PV for {claim} is not backed by an Azure managed disk.")
        expected_snapshot_id = source_data_id if claim == f"{CLUSTER}-1" else \
            source_wal_id if claim == f"{CLUSTER}-1-wal" else \
            output_ids["PG_DATA"] if claim == f"{CLUSTER}-2" else output_ids["PG_WAL"]
        pvcs_for_disks.append((claim, disk_id, expected_snapshot_id))
        journal.evidence("snapshot-pv-lineage-verified", "persistentVolume", claim, pv)

    # Poll all four Azure data disks to numeric 100. Do not create another Backup
    # or snapshot while any Copy/CopyStart remains in progress.
    disk_records: list[dict[str, Any]] = []
    for claim, disk_id, expected_source_id in pvcs_for_disks:
        journal.event("disk-hydration-wait-started", "azureDisk", claim, f"id={disk_id}")
        deadline = time.monotonic() + 86400
        while time.monotonic() < deadline:
            disk = runner.azj("disk", "show", "--ids", disk_id, timeout_s=900)
            creation = disk.get("creationData") or {}
            percent = disk.get("completionPercent")
            try:
                numeric_percent = float(percent)
            except (TypeError, ValueError):
                numeric_percent = None
            require(disk.get("provisioningState") != "Failed", f"Disk {claim} entered Failed state.")
            if creation.get("createOption") in ("Copy", "CopyStart") and numeric_percent == 100 \
                    and disk.get("provisioningState") == "Succeeded":
                break
            time.sleep(20)
        require(time.monotonic() < deadline, f"Disk {claim} did not finish Copy/CopyStart within 24 hours.")
        disk = runner.azj("disk", "show", "--ids", disk_id, timeout_s=900)
        rec = verify_disk_response(disk, expected_id=disk_id, node_rg=node_rg, location=location,
                                   zone=zone, expected_source=expected_source_id)
        rec["claim"] = claim
        disk_records.append(rec)
        journal.evidence("disk-hydration-complete", "azureDisk", claim, disk)
    require(len({d["id"].lower() for d in disk_records}) == 4, "Expected four distinct hydrated DATA/WAL disks.")

    # Validate the physical fixture without row-counting or touching table rows.
    fixture_sql = (
        "SELECT json_build_object('systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),"
        "'inRecovery',pg_is_in_recovery(),'heapBytes',pg_relation_size('public.vacuum_target'),"
        "'tableBytes',pg_total_relation_size('public.vacuum_target'),"
        "'relationFileNode',pg_relation_filenode('public.vacuum_target'),"
        "'liveEstimate',(SELECT n_live_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),"
        "'deadEstimate',(SELECT n_dead_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),"
        "'tableAutovacuumDisabled',(SELECT 'autovacuum_enabled=false'=ANY(reloptions) FROM pg_class WHERE oid='public.vacuum_target'::regclass),"
        "'hasIndex',to_regclass('public.vacuum_target_id_idx') IS NOT NULL,"
        "'synchronousCommit',current_setting('synchronous_commit'),'walLevel',current_setting('wal_level'),"
        "'fullPageWrites',current_setting('full_page_writes'),'recoveryPrefetch',current_setting('recovery_prefetch'),"
        "'vacuumCostDelay',current_setting('vacuum_cost_delay'),'sharedBuffers',current_setting('shared_buffers'),"
        "'maintenanceIoConcurrency',current_setting('maintenance_io_concurrency'),"
        "'maintenanceWorkMem',current_setting('maintenance_work_mem'),'checkpointTimeout',current_setting('checkpoint_timeout'),"
        "'maxWalSize',current_setting('max_wal_size'),'trackIoTiming',current_setting('track_io_timing'),"
        "'trackWalIoTiming',current_setting('track_wal_io_timing'),'autovacuum',current_setting('autovacuum'),"
        "'appSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend'))::text;"
    )
    primary_fixture = json.loads(psql(runner, runner.kubeconfig, f"{CLUSTER}-1", fixture_sql))
    require(primary_fixture.get("systemIdentifier") == str(snapshot_meta["systemIdentifier"])
            and primary_fixture.get("inRecovery") is False
            and primary_fixture.get("heapBytes") == snapshot_meta["heapBytes"]
            and primary_fixture.get("tableBytes") == snapshot_meta["tableBytes"]
            and primary_fixture.get("relationFileNode") == locate_evidence("*-primary-fixture-verified-*").get("relationFileNode")
            and primary_fixture.get("tableAutovacuumDisabled") is True
            and primary_fixture.get("hasIndex") is True
            and primary_fixture.get("synchronousCommit") == "on"
            and primary_fixture.get("walLevel") == "logical"
            and primary_fixture.get("fullPageWrites") == "on"
            and primary_fixture.get("recoveryPrefetch") == "try"
            and primary_fixture.get("vacuumCostDelay") == "0"
            and primary_fixture.get("appSessions") == 0,
            "Primary fixture identity/size/settings/client-free check failed.")
    expected_settings = {
        "autovacuum": "on", "sharedBuffers": "256MB", "maintenanceIoConcurrency": "10",
        "maintenanceWorkMem": "64MB", "checkpointTimeout": "5min", "maxWalSize": "1GB",
        "trackIoTiming": "on", "trackWalIoTiming": "on",
    }
    require(all(primary_fixture.get(k) == v for k, v in expected_settings.items()),
            "Primary CNPG default/measurement settings changed.")
    replica_fixture = json.loads(psql(runner, runner.kubeconfig, f"{CLUSTER}-2", fixture_sql))
    require(replica_fixture.get("systemIdentifier") == str(snapshot_meta["systemIdentifier"])
            and replica_fixture.get("inRecovery") is True
            and replica_fixture.get("heapBytes") == primary_fixture["heapBytes"]
            and replica_fixture.get("tableBytes") == primary_fixture["tableBytes"]
            and replica_fixture.get("relationFileNode") == primary_fixture["relationFileNode"]
            and replica_fixture.get("tableAutovacuumDisabled") is True
            and replica_fixture.get("hasIndex") is True
            and replica_fixture.get("appSessions") == 0,
            "Replica fixture/system identifier/lineage/client-free check failed.")
    journal.evidence("primary-fixture-cached-read-only-verified", "postgresFixture", f"{CLUSTER}-1", primary_fixture)
    journal.evidence("replica-fixture-cached-read-only-verified", "postgresFixture", f"{CLUSTER}-2", replica_fixture)

    flush_lsn = psql(runner, runner.kubeconfig, f"{CLUSTER}-1", "SELECT pg_current_wal_flush_lsn();")
    end = time.monotonic() + 3600
    while time.monotonic() < end:
        streaming = psql(runner, runner.kubeconfig, f"{CLUSTER}-1",
                         "SELECT count(*) FROM pg_stat_replication WHERE state='streaming' AND replay_lsn>=sent_lsn;")
        replayed = psql(runner, runner.kubeconfig, f"{CLUSTER}-2",
                        f"SELECT pg_last_wal_replay_lsn()>='{flush_lsn}'::pg_lsn;")
        if streaming == "1" and replayed == "t":
            break
        time.sleep(5)
    require(streaming == "1" and replayed == "t", "Replica did not stream and replay through the pinned primary flush LSN.")
    journal.event("replica-caught-up", "postgres", f"{CLUSTER}-2", f"flushLsn={flush_lsn}")

    # Switch only the image, preserving the snapshot-derived PVs and exact DB.
    cluster = runner.kj("get", "cluster", CLUSTER, "-n", NAMESPACE)
    require(cluster.get("metadata", {}).get("uid") == saved_cluster["metadata"]["uid"]
            and cluster.get("spec", {}).get("instances") == 2,
            "Cluster identity/count changed before measurement image pin.")
    runner.k("patch", "cluster", CLUSTER, "-n", NAMESPACE, "--type=merge", "-p",
             json.dumps({"spec": {"imageName": IMAGE, "primaryUpdateMethod": "restart"}}))
    digest = IMAGE.rsplit("@", 1)[1]
    end = time.monotonic() + 10800
    final_pods: dict[str, Any] = {}
    image_ready = False
    while time.monotonic() < end:
        cluster = runner.kj("get", "cluster", CLUSTER, "-n", NAMESPACE)
        pods = runner.kj("get", "pods", "-n", NAMESPACE, "-l", f"cnpg.io/cluster={CLUSTER}").get("items", [])
        final_pods = {p.get("metadata", {}).get("name"): p for p in pods}
        if (cluster.get("status", {}).get("readyInstances") == 2
                and set(final_pods) == {f"{CLUSTER}-1", f"{CLUSTER}-2"}
                and all(p.get("status", {}).get("phase") == "Running"
                        and any(c.get("name") == "postgres" and c.get("image") == IMAGE
                                for c in p.get("spec", {}).get("containers", []))
                        and any(c.get("name") == "postgres" and c.get("ready") is True
                                and (c.get("imageID") or "").endswith(digest)
                                for c in p.get("status", {}).get("containerStatuses", []))
                        for p in final_pods.values())):
            image_ready = True
            break
        time.sleep(10)
    require(image_ready and cluster.get("status", {}).get("readyInstances") == 2
            and set(final_pods) == {f"{CLUSTER}-1", f"{CLUSTER}-2"},
            "Both postgres containers did not reach the pinned measurement image and digest before timeout.")
    journal.evidence("measurement-image-pods-ready", "pods", CLUSTER, {"cluster": cluster, "pods": list(final_pods.values())})

    # Image restart may generate a little WAL. Re-pin the final flush point after
    # both postgres containers run the measurement digest, then verify catch-up.
    flush_lsn = psql(runner, runner.kubeconfig, f"{CLUSTER}-1", "SELECT pg_current_wal_flush_lsn();")
    end = time.monotonic() + 3600
    streaming = replayed = ""
    while time.monotonic() < end:
        streaming = psql(runner, runner.kubeconfig, f"{CLUSTER}-1",
                         "SELECT count(*) FROM pg_stat_replication WHERE state='streaming' AND replay_lsn>=sent_lsn;")
        replayed = psql(runner, runner.kubeconfig, f"{CLUSTER}-2",
                        f"SELECT pg_last_wal_replay_lsn()>='{flush_lsn}'::pg_lsn;")
        if streaming == "1" and replayed == "t":
            break
        time.sleep(5)
    require(streaming == "1" and replayed == "t",
            "Replica did not remain caught up after the measurement-image restart.")
    journal.event("measurement-image-replica-caught-up", "postgres", f"{CLUSTER}-2", f"flushLsn={flush_lsn}")

    # Keep the temporary reader/writer credentials until the pinned image has
    # restarted and the replica has replayed its final WAL. Then remove both
    # archive references before deleting the run-owned Secret.
    remove_patch = '[{"op":"remove","path":"/spec/externalClusters"},{"op":"remove","path":"/spec/backup/barmanObjectStore"}]'
    journal.event("archive-reference-removal-dryrun-started", "cluster", CLUSTER,
                  "remove externalClusters and active Barman writer after pinned image/catch-up")
    runner.k("patch", "cluster", CLUSTER, "-n", NAMESPACE, "--type=json", "--dry-run=server", "-o", "json", "-p", remove_patch)
    runner.k("patch", "cluster", CLUSTER, "-n", NAMESPACE, "--type=json", "-p", remove_patch)
    journal.event("archive-references-removed", "cluster", CLUSTER,
                  "pinned image ready and replica caught up; removed source reader and seed writer")
    runner.k("delete", "secret", SECRET, "-n", NAMESPACE, "--wait=true", timeout_s=300)
    secret_name = runner.k("get", "secret", SECRET, "-n", NAMESPACE,
                           "--ignore-not-found", "-o", "name").strip()
    if secret_name:
        raise Refusal("Archive Secret still exists after removal; refusing to finalize.")
    journal.event("archive-secret-removed", "secret", SECRET,
                  "both archive references were removed after pinned image and replica catch-up")

    final_cluster = runner.kj("get", "cluster", CLUSTER, "-n", NAMESPACE)
    require(final_cluster.get("metadata", {}).get("uid") == saved_cluster["metadata"]["uid"]
            and final_cluster.get("metadata", {}).get("labels", {}).get("repro.stack.token") == token
            and final_cluster.get("spec", {}).get("instances") == 2
            and final_cluster.get("status", {}).get("readyInstances") == 2
            and final_cluster.get("status", {}).get("currentPrimary") == f"{CLUSTER}-1"
            and final_cluster.get("status", {}).get("systemID") == str(snapshot_meta["systemIdentifier"])
            and final_cluster.get("spec", {}).get("imageName") == IMAGE
            and (final_cluster.get("spec", {}).get("backup") or {}).get("barmanObjectStore") is None
            and final_cluster.get("spec", {}).get("externalClusters") is None
            and final_cluster.get("spec", {}).get("postgresql", {}).get("parameters", {}).get("synchronous_commit") == "on"
            and final_cluster.get("spec", {}).get("postgresql", {}).get("synchronous", {}).get("method") == "any"
            and final_cluster.get("spec", {}).get("postgresql", {}).get("synchronous", {}).get("number") == 1
            and final_cluster.get("spec", {}).get("postgresql", {}).get("synchronous", {}).get("dataDurability") == "preferred",
            "Final measurement Cluster configuration failed its immutable source/HA checks.")

    final_pvcs = runner.kj("get", "pvc", "-n", NAMESPACE, "-l", f"cnpg.io/cluster={CLUSTER}").get("items", [])
    require(len(final_pvcs) == 4, "Final Cluster does not have exactly four DATA/WAL PVCs.")
    all_vss_sources = {
        f"{CLUSTER}-1": data_name, f"{CLUSTER}-1-wal": wal_name,
        f"{CLUSTER}-2": seed_data_vss, f"{CLUSTER}-2-wal": seed_wal_vss,
    }
    require(all((p.get("spec", {}).get("dataSource") or {}).get("kind") == "VolumeSnapshot"
                and (p.get("spec", {}).get("dataSource") or {}).get("name") == all_vss_sources.get(p.get("metadata", {}).get("name"))
                and p.get("status", {}).get("phase") == "Bound" for p in final_pvcs),
            "Final DATA/WAL PVCs lost their exact paired snapshot lineage.")

    # Capture all retained run-owned snapshots and confirm direct lock protection.
    all_locks = runner.azj("lock", "list", "--resource-group", snap_rg)
    snapshot_inventory = runner.azj("snapshot", "list", "--resource-group", snap_rg)
    run_snapshots = [s for s in snapshot_inventory if s.get("tags", {}).get("reprotoken") == token]
    require(bool(run_snapshots), "No run-owned retained snapshot inventory found.")
    checked_snapshots: list[dict[str, Any]] = []
    for item in run_snapshots:
        sid = item.get("id")
        require(isinstance(sid, str) and direct_lock(all_locks, sid),
                f"Run-owned snapshot lacks a direct CanNotDelete lock: {sid}")
        live = runner.azj("snapshot", "show", "--ids", sid)
        require(live.get("id", "").lower() == sid.lower()
                and live.get("tags", {}).get("reprotoken") == token,
                f"Run-owned snapshot identity/owner changed during final verification: {sid}")
        percent = live.get("completionPercent")
        try:
            percent = None if percent is None else float(percent)
        except (TypeError, ValueError):
            raise Refusal(f"Run-owned snapshot has invalid completionPercent: {sid}")
        require(live.get("provisioningState") == "Succeeded"
                and live.get("snapshotAccessState") == "Available"
                and (percent is None or percent == 100)
                and (live.get("diskSizeGB") or live.get("diskSizeGb")) == 512
                and live.get("incremental") is True
                and bool(live.get("uniqueId")),
                f"Run-owned snapshot is not fully ready/identified: {sid}")
        checked_snapshots.append(live)
    journal.evidence("all-owned-snapshots-retained-and-locked", "azureSnapshotInventory", snap_rg,
                     {"items": checked_snapshots, "locks": all_locks})

    audit_path = RECOVERY / "prepare-audit.json"
    prep_path = STATE_DIR / "prepare.json"
    require(not audit_path.exists() and not prep_path.exists(), "Final preparation receipt already exists; refusing overwrite.")
    now = utc_now()
    assignments = (state.get("sourceSnapshotReaderAssignments") or {}).get("assignments", [])
    audit = {
        "version": 1, "status": "complete", "verifiedAtUtc": now,
        "ownerToken": token, "subscriptionId": sub, "resourceGroup": rg,
        "nodeResourceGroup": node_rg, "retainedSnapshotResourceGroup": snap_rg,
        "source": {
            "dataSnapshotId": source_data_id, "walSnapshotId": source_wal_id,
            "backupName": manifest["backup"]["name"], "backupUid": manifest["backup"]["uid"],
            "backupEndLSN": source_lsn, "bindings": read_json(RECOVERY / "source-bindings.json"),
            "readerAssignments": assignments,
            "preparedTupleEstimates": {
                "live": snapshot_meta["preparedLiveTuplesEstimate"],
                "dead": snapshot_meta["preparedDeadTuplesEstimate"],
            },
        },
        "offlineSeedBackup": {
            "name": backup_name, "uid": backup["metadata"]["uid"], "online": False,
            "majorVersion": 16, "metadataFieldsRequired": ["phase", "method", "paired DATA/WAL elements"],
            "replicaSourceEligible": True,
        },
        "primaryAndReplicaSnapshots": {
            "data": output_ids["PG_DATA"], "wal": output_ids["PG_WAL"], "records": output_records,
        },
        "snapshotRecoveryJobUid": job_uid,
        "replicaCaughtUpToLsn": flush_lsn,
        "walArchive": {"serverName": server_name, "lastArchivedWal": archive_after.get("lastWal"),
                       "before": before, "after": archive_after, "distinctFromRetainedSource": True},
        "fixture": primary_fixture,
        "replicaFixture": replica_fixture,
        "observedTupleEstimates": {"primaryLive": primary_fixture.get("liveEstimate"),
                                   "primaryDead": primary_fixture.get("deadEstimate")},
        "hydratedPremiumV2Disks": disk_records,
        "archiveSecretAbsent": True, "archiveWriterAbsent": True,
        "clientCreated": False, "noFixtureDML": True,
        "cluster": "repro/hdd-pg", "instances": 2, "measurementImage": IMAGE,
        "allSnapshotsRetained": True, "sourceSnapshotsUntouched": True,
        "replicaSnapshotRecoveryJob": {
            "uid": job_uid, "name": job_record["metadata"]["name"],
            "role": "snapshot-recovery", "containerImage": SYSIMAGE,
            "command": "restoresnapshot --immediate", "completed": True,
        },
    }
    estimated = round(23 * 260.583 / 110, 1)
    prep = {
        "version": 1, "status": "complete", "sourceSnapshotId": source_data_id,
        "sourceKind": "paired-retained-cnp-snapshot", "systemIdentifier": str(snapshot_meta["systemIdentifier"]),
        "sourceSnapshotHeapBytes": snapshot_meta["heapBytes"],
        "sourceSnapshotTableBytes": snapshot_meta["tableBytes"],
        "fixtureTargetHeapGiB": 23, "fixtureTargetHeapBytes": snapshot_meta["heapBytes"],
        "heapBytes": primary_fixture["heapBytes"], "tableBytes": primary_fixture["tableBytes"],
        "estimatedVacuumMinutes": estimated,
        "preparedLiveTuplesEstimate": snapshot_meta["preparedLiveTuplesEstimate"],
        "preparedDeadTuplesEstimate": snapshot_meta["preparedDeadTuplesEstimate"],
        "replicaCaughtUpToLsn": flush_lsn, "cluster": "repro/hdd-pg", "instances": 2,
        "availabilityZone": zone, "retainedSnapshotResourceGroup": snap_rg,
        "storage": "four separate 512-GiB PremiumV2_LRS DATA/WAL disks at 3000 IOPS / 125 MB/s",
        "backup": backup_name,
        "replicaSnapshots": {"data": output_ids["PG_DATA"], "wal": output_ids["PG_WAL"]},
        "measurementImage": IMAGE, "clientCreated": False,
        "finalAudit": {
            "verifiedAtUtc": now, "receipt": str(audit_path), "archiveSecretAbsent": True,
            "archiveWriterAbsent": True, "ownedRetentionSnapshotsReady": True,
            "allFourPremiumV2DisksHydrated": True, "sourceLineageVerified": True,
            "snapshotSeededReplica": True,
        },
    }
    # Append receipts atomically and never replace an earlier run result.
    audit_raw = canon(audit)
    prep_raw = canon(prep)
    audit_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    for path, raw in ((audit_path, audit_raw), (prep_path, prep_raw)):
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.write(fd, raw)
            os.fsync(fd)
        finally:
            os.close(fd)
    for parent in (audit_path.parent, prep_path.parent):
        dfd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    journal.event("preparation-finalized", "run", CLUSTER,
                  f"prepare={prep_path} audit={audit_path} snapshotRecoveryJobUid={job_uid}")
    print(f"Snapshot-seeded baseline finalized; no client was created. Receipts: {prep_path} and {audit_path}")


def jq_types(backup: dict[str, Any]) -> list[str]:
    elements = (((backup.get("status") or {}).get("snapshotBackupStatus") or {}).get("elements") or [])
    return sorted(str(item.get("type")) for item in elements)


def local_preflight(owner_token: str | None, apply: bool) -> tuple[dict[str, Any], ...]:
    state = read_json(STATE_DIR / "state.json")
    manifest = read_json(HERE / "source-manifest.json")
    snapshot_meta = read_json(HERE / "snapshot.json")
    run_config = read_json(HERE / "run-config.json")
    source_bindings = read_json(RECOVERY / "source-bindings.json")
    storage = read_json(STATE_DIR / "storage.json")
    saved_cluster = locate_evidence("*-primary-recovery-ready-cluster-*")
    saved_backup = locate_evidence("*-seed-backup-completed-backup-*")

    token = state.get("ownerToken")
    require(isinstance(token, str) and re.fullmatch(r"[a-f0-9]{32}", token) is not None,
            "Run owner token is missing/invalid.")
    if owner_token is not None:
        require(owner_token == token, "--owner-token does not match the pinned run owner token.")
    if apply:
        require(owner_token == token, "Apply requires the exact --owner-token for this run.")
    require(state.get("status") == "ready" and state.get("subscriptionId") == manifest["subscriptionId"],
            "Run state/source manifest subscription/status mismatch.")
    require(state.get("resourceGroup") and state.get("nodeResourceGroup") and state.get("snapshotResourceGroup"),
            "Pinned resource group identity is incomplete.")
    require(manifest.get("ownerToken") and manifest.get("sourceSnapshotId")
            and manifest.get("pairedWAL", {}).get("sourceSnapshotId"), "Pinned paired source manifest is incomplete.")
    require(str(manifest.get("systemIdentifier")) == str(snapshot_meta.get("systemIdentifier")) == "7692518806614212629",
            "Pinned source systemIdentifier mismatch.")
    require(snapshot_meta.get("heapBytes") == 24696061952 and snapshot_meta.get("tableBytes") == 25040748544,
            "Pinned source fixture is not the 23-GiB prepared baseline.")
    require(run_config.get("client", {}).get("targetTps") == 5
            and run_config.get("client", {}).get("synchronousCommit") == "on"
            and run_config.get("diskSku") == "PremiumV2_LRS"
            and run_config.get("diskIops") == 3000
            and run_config.get("diskMBps") == 125,
            "Run configuration no longer matches the approved baseline.")
    require(source_bindings.get("version") == 1
            and source_bindings.get("sourceBackupUid") == manifest.get("backup", {}).get("uid")
            and source_bindings.get("endLSN") == manifest.get("backup", {}).get("endLSN"),
            "Persisted source WAL binding receipt does not match the pinned source manifest.")
    require(storage.get("status") == "ready" and storage.get("ownerToken") == token
            and storage.get("subscriptionId") == state.get("subscriptionId")
            and storage.get("resourceGroup") == state.get("resourceGroup")
            and storage.get("publicAccess") is None,
            "Run-owned private WAL storage metadata is invalid.")
    require(saved_cluster.get("metadata", {}).get("labels", {}).get("repro.stack.token") == token
            and saved_cluster.get("status", {}).get("systemID") == str(snapshot_meta["systemIdentifier"]),
            "Persisted primary-ready evidence has the wrong stack token/system identifier.")
    require(saved_backup.get("metadata", {}).get("labels", {}).get("repro.stack.token") == token
            and saved_backup.get("metadata", {}).get("name") == f"vacuum-seed-{token[:8]}"
            and saved_backup.get("status", {}).get("phase") == "completed"
            and saved_backup.get("spec", {}).get("online") is False,
            "Persisted offline seed Backup evidence is missing or changed.")
    require(not (STATE_DIR / "prepare.json").exists() and not (RECOVERY / "prepare-audit.json").exists(),
            "Final prepare receipt already exists; refusing to overwrite.")
    return state, manifest, snapshot_meta, source_bindings, storage, saved_cluster, saved_backup


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="perform the reviewed Kubernetes/Azure read/write sequence")
    parser.add_argument("--owner-token", help="required with --apply; exact token from this run's state.json")
    args = parser.parse_args()
    journal: Journal | None = None
    try:
        values = local_preflight(args.owner_token, args.apply)
        state, manifest, snapshot_meta, source_bindings, storage, saved_cluster, saved_backup = values
        plan = {
            "mode": "apply" if args.apply else "dry-run",
            "run": HERE.name, "ownerToken": state["ownerToken"],
            "cluster": f"{NAMESPACE}/{CLUSTER}",
            "clusterUid": saved_cluster["metadata"]["uid"],
            "offlineBackup": saved_backup["metadata"]["name"],
            "backupUid": saved_backup["metadata"]["uid"],
            "sourceSystemIdentifier": snapshot_meta["systemIdentifier"],
            "dataSnapshotId": manifest["sourceSnapshotId"],
            "walSnapshotId": manifest["pairedWAL"]["sourceSnapshotId"],
            "steps": [
                "verify exact primary, offline Backup, paired ready VSS/VSC and retained Azure snapshots",
                "enable active run-owned Barman writer with a distinct serverName",
                "force and verify one archived WAL segment; no fixture DML",
                "scale to two with synchronous ANY 1 / synchronous_commit on",
                "require CNPG snapshot-recovery restoresnapshot --immediate; scale back on any other job",
                "verify four exact source PVCs and Azure Copy/CopyStart disks at numeric 100",
                "verify fixture identity/settings and replica catch-up; no rowcount or fixture rewrite",
                "pin measurement image and catch up; remove archive references/Secret; write prepare.json and audit",
            ],
            "failurePolicy": "preserve cloud resources/evidence; root review is required before recovery or rerun",
        }
        if not args.apply:
            script_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
            proof_inputs = [
                STATE_DIR / "state.json", STATE_DIR / "storage.json",
                HERE / "source-manifest.json", HERE / "snapshot.json", HERE / "run-config.json",
                RECOVERY / "source-bindings.json",
                locate_evidence_path("*-primary-recovery-ready-cluster-*.json"),
                locate_evidence_path("*-seed-backup-completed-backup-*.json"),
            ]
            proof = {
                "version": 1, "phase": "seed", "mode": "read-only-local-dry-run",
                "createdAtUtc": utc_now(), "ownerToken": state["ownerToken"],
                "scriptSha256": script_hash, "clusterUid": saved_cluster["metadata"]["uid"],
                "seedBackupUid": saved_backup["metadata"]["uid"],
                "sourceDataId": manifest["sourceSnapshotId"],
                "sourceDataUid": manifest["uniqueId"],
                "sourceWalId": manifest["pairedWAL"]["sourceSnapshotId"],
                "sourceWalUid": manifest["pairedWAL"]["uniqueId"],
                "localInputs": {
                    str(path.relative_to(HERE)): hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in proof_inputs
                },
                "remoteActionsPerformed": False,
            }
            journal = Journal(RECOVERY / "phase-events.jsonl")
            proof_path = journal.evidence("seed-dry-run-proof", "run", HERE.name, proof)
            plan["dryRunProof"] = str(proof_path)
            print(json.dumps(plan, indent=2))
            return 0
        kubeconfig_value = state.get("kubeconfigPath") or str(STATE_DIR / "kubeconfig")
        kubeconfig = Path(kubeconfig_value)
        if not kubeconfig.is_absolute():
            kubeconfig = HERE / kubeconfig
        require(kubeconfig.is_file(), f"Pinned kubeconfig is missing: {kubeconfig}")
        journal = Journal(RECOVERY / "phase-events.jsonl")
        journal.event("continuation-started", "run", HERE.name,
                      f"backup={saved_backup['metadata']['name']} clusterUid={saved_cluster['metadata']['uid']}")
        runner = Runner(state["subscriptionId"], kubeconfig, True)
        run_apply(runner, journal, state=state, manifest=manifest, snapshot_meta=snapshot_meta,
                  source_bindings=source_bindings, storage=storage,
                  saved_cluster=saved_cluster, saved_backup=saved_backup)
        return 0
    except (Refusal, OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        if journal is not None:
            try:
                journal.event("continuation-failed", "run", HERE.name,
                              f"{str(exc)[:700]}; resources/evidence preserved, no automatic rollback; root review required")
            except OSError:
                pass
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
