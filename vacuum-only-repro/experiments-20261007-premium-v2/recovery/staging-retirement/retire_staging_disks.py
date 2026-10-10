#!/usr/bin/env python3
"""Retire the two verified Standard restore stages before measurement.

This tool is read-only by default. `--apply` deletes only the two stage disk
IDs recorded in the completed 50-GiB and 100-GiB source-stage receipts. It
never deletes snapshots or changes locks. Standard snapshot copy progress is
not observable, so stage retirement does not prove that every Azure backend
snapshot copy has finished.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
RUNS = ("baseline-23gib", "50gib", "100gib")
STAGE_RUNS = ("50gib", "100gib")
SUBSCRIPTION = "00000000-0000-0000-0000-000000000000"
ARM = "https://management.azure.com"
DISK_API = "2025-01-02"
RG_API = "2024-03-01"
SNAP_API = "2025-01-02"
CHILD_PURPOSE = "premiumv2-keeper-copy"
TARGET_CLAIMS = ("hdd-pg-1", "hdd-pg-1-wal", "hdd-pg-2", "hdd-pg-2-wal")
STANDARD_COPY_LIMITATION = (
    "Standard_LRS snapshot copy progress is not exposed by Azure. These checks "
    "establish that measured Premium SSD v2 disks report complete hydration, "
    "the verified staging disks are absent, and no observable owned copy is "
    "active. They do not prove that unobservable Standard-origin backend "
    "snapshot copies have finished."
)


class SafetyError(RuntimeError):
    """A required identity or readiness condition was not proven."""


def read_json(path: Path) -> dict:
    return json.loads(path.read_text())


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def canonical_id(value: str) -> str:
    if not isinstance(value, str) or not value.startswith("/subscriptions/"):
        raise SafetyError(f"Invalid Azure resource ID: {value!r}")
    return value.rstrip("/").lower()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SafetyError(message)


def write_json_atomic(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def within_root(path: Path) -> Path:
    resolved = path.resolve(strict=True)
    try:
        resolved.relative_to(ROOT.resolve())
    except ValueError as exc:
        raise SafetyError(f"Evidence path escapes the experiment root: {path}") from exc
    return resolved


def repo_relative(path: Path) -> str:
    return str(within_root(path).relative_to(ROOT.resolve()))


def get_unique_id(resource: dict) -> str | None:
    properties = resource.get("properties", {})
    return properties.get("uniqueId") or resource.get("uniqueId")


def get_properties(resource: dict) -> dict:
    return resource.get("properties", resource)


def make_stage_record(stack: str, state: dict, receipt: dict, receipt_path: Path,
                      receipt_hash: str) -> dict:
    return {
        "stack": stack,
        "id": receipt["stagingDiskId"],
        "uniqueId": receipt["stagingDiskUniqueId"],
        "sourceSnapshotId": receipt["sourceSnapshotId"],
        "sourceSnapshotUniqueId": receipt["sourceSnapshotUniqueId"],
        "restoreSnapshotId": receipt["restoreSnapshotId"],
        "restoreSnapshotUniqueId": receipt["restoreSnapshotUniqueId"],
        "sourceStageReceiptPath": repo_relative(receipt_path),
        "sourceStageReceiptSha256": receipt_hash,
        "ownerToken": state["ownerToken"],
        "subscriptionId": state["subscriptionId"],
        "nodeResourceGroup": state["nodeResourceGroup"],
        "snapshotResourceGroupId": state["snapshotResourceGroupId"],
        "location": state["location"],
        "zone": str(state["availabilityZone"]),
    }


def preparation_evidence(root: Path, stack: str) -> tuple[dict, dict | None]:
    """Validate the same preparation exit, metadata, and script hashes as coordinator."""
    base = root / stack
    state = read_json(base / ".state/state.json")
    process_path = base / ".state/prepare-process.json"
    metadata_path = base / ".state/prepare.json"
    process = read_json(process_path)
    metadata = read_json(metadata_path)
    config = read_json(base / "run-config.json")
    require(process.get("status") == "complete" and process.get("exitCode") == 0,
            f"{stack}: preparation process is not complete with exit 0")
    require(metadata.get("status") == "complete" and state.get("status") == "ready",
            f"{stack}: preparation metadata or infrastructure is incomplete")
    require(metadata.get("fixtureTargetHeapGiB") == config.get("targetHeapGiB") and
            metadata.get("instances") == 2 and metadata.get("clientCreated") is False,
            f"{stack}: prepared metadata differs from the assigned fixture")
    require(metadata.get("replicaCaughtUpToLsn") and metadata.get("backup") and
            set(metadata.get("replicaSnapshots", {})) == {"data", "wal"},
            f"{stack}: completed backup or replica catch-up evidence is missing")
    script = base / "02-prepare.sh"
    script_hash = sha256_file(script)
    require(process.get("scriptSha256", {}).get("02-prepare.sh") == script_hash,
            f"{stack}: 02-prepare.sh changed after its preparation run")
    helper_hash = process.get("sourceStageHelperSha256")
    if helper_hash:
        helper = root / "recovery/source-restore/stage-allowlisted-source.sh"
        require(helper.exists() and sha256_file(helper) == helper_hash,
                f"{stack}: source-stage helper changed after preparation")
    manifest = read_json(base / "source-manifest.json")
    require(canonical_id(metadata.get("sourceSnapshotId")) ==
            canonical_id(manifest.get("sourceSnapshotId")),
            f"{stack}: prepared source snapshot differs from its pinned manifest")
    require(state.get("subscriptionId") == SUBSCRIPTION and
            state.get("ownerToken") and state.get("nodeResourceGroup") and
            state.get("snapshotResourceGroupId"), f"{stack}: incomplete ownership metadata")
    stage_record = None
    stage_receipt_path = None
    stage_receipt_hash = None
    if stack in STAGE_RUNS:
        raw_path = state.get("snapshotSourceStageReceiptPath")
        require(raw_path, f"{stack}: state lacks the source-stage receipt path")
        candidate = Path(raw_path)
        if not candidate.is_absolute():
            candidate = root / candidate
        stage_receipt_path = within_root(candidate)
        receipt = read_json(stage_receipt_path)
        stage_receipt_hash = sha256_file(stage_receipt_path)
        require(receipt.get("version") == 1 and receipt.get("status") == "ready" and
                receipt.get("ownerToken") == state["ownerToken"],
                f"{stack}: source-stage receipt is not ready or owner-bound")
        require(canonical_id(receipt.get("sourceSnapshotId")) ==
                canonical_id(manifest.get("sourceSnapshotId")),
                f"{stack}: stage receipt source snapshot differs from pinned input")
        require(receipt.get("sourceSnapshotUniqueId") == manifest.get("uniqueId"),
                f"{stack}: stage receipt source snapshot uniqueId differs from pinned manifest")
        require(canonical_id(receipt.get("stagingDiskId")) ==
                canonical_id(state.get("snapshotSourceStagingDiskId")) and
                canonical_id(receipt.get("restoreSnapshotId")) ==
                canonical_id(state.get("snapshotSourceCopyId")),
                f"{stack}: stage receipt identities differ from state.json")
        stage_record = make_stage_record(stack, state, receipt, stage_receipt_path,
                                         stage_receipt_hash)
    else:
        require(state.get("snapshotSourceStagingDiskId") is None and
                state.get("snapshotSourceStageReceiptPath") is None,
                "baseline-23gib: unexpected source-stage resource must be reviewed")
        audit_path = base / ".state/recovery/baseline-finalize-audit.json"
        audit = read_json(audit_path)
        hydrated = audit.get("hydratedPremiumV2Disks", [])
        require(audit.get("status") == "complete" and len(hydrated) == 4 and
                all(item.get("completionPercent") == 100.0 or
                    (item.get("createOption") == "Empty" and
                     item.get("completionPercent") is None) for item in hydrated),
                "baseline-23gib: final hydration audit is not complete")
    evidence = {
        "prepareProcessSha256": sha256_file(process_path),
        "prepareMetadataSha256": sha256_file(metadata_path),
        "prepareScriptSha256": script_hash,
        "stateSha256": sha256_file(base / ".state/state.json"),
        "sourceManifestSha256": sha256_file(base / "source-manifest.json"),
        "sourceStageReceiptPath": repo_relative(stage_receipt_path) if stage_receipt_path else None,
        "sourceStageReceiptSha256": stage_receipt_hash,
        "sourceStageHelperSha256": helper_hash,
        "targetHeapGiB": config["targetHeapGiB"],
        "ownerToken": state["ownerToken"],
    }
    return {"state": state, "metadata": metadata, "config": config,
            "evidence": evidence, "sourceManifest": manifest}, stage_record


def snapshot_fingerprint(audit: dict) -> tuple[str, list[tuple[str, str, bool, bool]]]:
    require(isinstance(audit, dict) and isinstance(audit.get("snapshots"), list),
            "Snapshot audit must be an object with a .snapshots array")
    identities = []
    for item in audit["snapshots"]:
        require(isinstance(item.get("id"), str) and item.get("uniqueId"),
                "Snapshot audit contains an entry without id/uniqueId")
        identities.append((canonical_id(item["id"]), str(item["uniqueId"]),
                           bool(item.get("directCanNotDelete")),
                           bool(item.get("groupCanNotDelete"))))
    identities.sort()
    return sha256_bytes(json.dumps(identities, separators=(",", ":")).encode()), identities


def compare_snapshot_audits(before: dict, after: dict,
                            expected_child_snapshots: list[tuple[str, str]]) -> dict:
    for audit, label in ((before, "before"), (after, "after")):
        require(audit.get("preservedPriorSnapshots") is True,
                f"{label} snapshot audit reports a missing or changed prior snapshot")
        require(audit.get("allSnapshotsProtected") is True,
                f"{label} snapshot audit found an unprotected snapshot")
    before_hash, before_rows = snapshot_fingerprint(before)
    after_hash, after_rows = snapshot_fingerprint(after)
    require(before_rows == after_rows,
            "Snapshot IDs, unique IDs, or direct/group CanNotDelete protection changed")
    row_by_id = {row[0]: row for row in after_rows}
    for snapshot_id, unique_id in expected_child_snapshots:
        row = row_by_id.get(canonical_id(snapshot_id))
        require(row is not None and row[1] == unique_id,
                f"Retained restore snapshot missing or changed: {snapshot_id}")
        require(row[2] is True or row[3] is True,
                f"Retained restore snapshot is not protected: {snapshot_id}")
    return {"beforeIdentityProtectionSha256": before_hash,
            "afterIdentityProtectionSha256": after_hash,
            "preservedPriorSnapshots": True, "allSnapshotsProtected": True,
            "unchangedIdentities": True, "snapshotCount": len(before_rows)}


def validate_copy_status(resource: dict, kind: str) -> tuple[bool, str | None]:
    props = get_properties(resource)
    option = (props.get("creationData") or {}).get("createOption")
    state = props.get("provisioningState")
    percent = props.get("completionPercent")
    error = props.get("copyCompletionError")
    if state == "Failed" or error is not None:
        return False, f"{kind} copy failed: {resource.get('id')}"
    if option in ("Copy", "CopyStart"):
        complete_percent = percent == 100 if kind == "disk" else percent in (None, 100)
        complete = (state == "Succeeded" and complete_percent and
                    (option != "CopyStart" or percent == 100))
        return complete, None if complete else f"{kind} copy is incomplete: {resource.get('id')}"
    if kind == "disk" and option == "Empty":
        complete = state == "Succeeded" and percent in (None, 100)
        return complete, None if complete else f"Empty disk is not ready: {resource.get('id')}"
    if kind == "disk" and option in ("FromImage", "Attach"):
        complete = state == "Succeeded" and percent in (None, 100)
        return complete, None if complete else f"Disk is not ready: {resource.get('id')}"
    return False, f"Unknown {kind} create option cannot be certified: {resource.get('id')}"


def validate_nonstage_node_disk_sku(resource: dict) -> None:
    props = get_properties(resource)
    option = (props.get("creationData") or {}).get("createOption")
    if option in ("Copy", "CopyStart"):
        sku = resource.get("sku", {}).get("name")
        require(sku in ("PremiumV2_LRS", "UltraSSD_LRS"),
                f"Copied non-stage node disk has unobservable/unsupported SKU {sku}: {resource.get('id')}")


def validate_backup_snapshot_materialization(resource: dict,
                                              measured_disk_ids: set[str]) -> None:
    tags = resource.get("tags", {})
    if tags.get("reproPurpose") != "premiumv2-vacuum-backup":
        return
    source_id = tags.get("source_volume_id") or tags.get("sourceVolumeId")
    require(source_id, f"PremiumV2 backup snapshot lacks its source disk tag: {resource.get('id')}")
    props = get_properties(resource)
    source_resource_id = (props.get("creationData") or {}).get("sourceResourceId")
    source_key = canonical_id(source_id)
    require(source_key in measured_disk_ids and source_resource_id and
            canonical_id(source_resource_id) == source_key and
            (props.get("creationData") or {}).get("createOption") in ("Copy", "CopyStart"),
            f"Backup snapshot source tag/lineage is not one of the current measured disks: {resource.get('id')}")
    require(props.get("provisioningState") == "Succeeded" and
            props.get("snapshotAccessState") == "Available" and
            props.get("completionPercent") == 100 and
            props.get("copyCompletionError") is None,
            f"Backup snapshot sourced from a measured Premium disk lacks explicit 100% completion: {resource.get('id')}")


def validate_target_disk(resource: dict, owner_rg_id: str, zone: str) -> dict:
    props = get_properties(resource)
    resource_id = resource.get("id", "")
    require(canonical_id(resource_id).startswith(canonical_id(owner_rg_id) +
            "/providers/microsoft.compute/disks/"),
            "Measured PremiumV2 disk escaped its owned node resource group")
    size = props.get("diskSizeGB")
    if size is None:
        size = props.get("diskSizeGb")
    sector = props.get("logicalSectorSize")
    if sector is None:
        sector = (props.get("creationData") or {}).get("logicalSectorSize")
    require(resource.get("sku", {}).get("name") == "PremiumV2_LRS" and
            size == 512 and props.get("diskIOPSReadWrite") == 3000 and
            props.get("diskMBpsReadWrite") == 125 and str(sector) == "512" and
            str(zone) in [str(x) for x in resource.get("zones", [])],
            f"Measured disk settings differ from the target: {resource_id}")
    option = (props.get("creationData") or {}).get("createOption")
    require(option in ("Copy", "CopyStart", "Empty"),
            f"Measured PremiumV2 disk has an unexpected create option: {resource_id}")
    complete, error = validate_copy_status(resource, "disk")
    require(complete, error or f"Measured disk is not fully provisioned: {resource_id}")
    require(resource.get("managedBy") or props.get("managedBy"),
            f"Measured PVC disk is unattached: {resource_id}")
    return {"id": resource_id, "uniqueId": get_unique_id(resource),
            "createOption": (props.get("creationData") or {}).get("createOption"),
            "completionPercent": props.get("completionPercent")}


def validate_disk_scope(resource: dict, owner_rg_id: str) -> None:
    resource_id = resource.get("id", "")
    require(canonical_id(resource_id).startswith(canonical_id(owner_rg_id) +
            "/providers/microsoft.compute/disks/"),
            f"Disk escaped its owner node resource group: {resource_id}")


def validate_stage_disk(resource: dict, record: dict) -> None:
    props = get_properties(resource)
    size = props.get("diskSizeGB")
    if size is None:
        size = props.get("diskSizeGb")
    require(canonical_id(resource.get("id")) == canonical_id(record["id"]) and
            get_unique_id(resource) == record["uniqueId"],
            f"Staging disk identity changed: {record['id']}")
    tags = resource.get("tags", {})
    source = props.get("creationData", {})
    require(resource.get("sku", {}).get("name") == "Standard_LRS" and size == 512 and
            str(resource.get("location", "")).lower() == str(record["location"]).lower() and
            props.get("provisioningState") == "Succeeded" and
            props.get("diskState") == "Unattached" and
            not (resource.get("managedBy") or props.get("managedBy")) and
            tags.get("reprotoken") == record["ownerToken"] and
            tags.get("reproPurpose") == "premiumv2-source-stage" and
            canonical_id(tags.get("sourceSnapshotId")) == canonical_id(record["sourceSnapshotId"]) and
            source.get("createOption") == "Copy" and
            canonical_id(source.get("sourceResourceId")) == canonical_id(record["sourceSnapshotId"]) and
            source.get("sourceUniqueId") == record["sourceSnapshotUniqueId"],
            f"Staging disk no longer matches its exact unmounted owner/source-bound receipt: {record['id']}")


def validate_child_snapshot(resource: dict, record: dict) -> None:
    props = get_properties(resource)
    source = props.get("creationData", {})
    tags = resource.get("tags", {})
    require(canonical_id(resource.get("id")) == canonical_id(record["restoreSnapshotId"]) and
            get_unique_id(resource) == record["restoreSnapshotUniqueId"] and
            props.get("provisioningState") == "Succeeded" and
            props.get("snapshotAccessState") == "Available" and
            resource.get("sku", {}).get("name") in ("Standard_LRS", "Standard_ZRS") and
            props.get("incremental") is True and
            (props.get("diskSizeGB") or props.get("diskSizeGb")) == 512 and
            source.get("createOption") == "Copy" and
            canonical_id(source.get("sourceResourceId")) == canonical_id(record["id"]) and
            source.get("sourceUniqueId") == record["uniqueId"] and
            tags.get("reprotoken") == record["ownerToken"] and
            tags.get("reproPurpose") == CHILD_PURPOSE and
            canonical_id(tags.get("sourceSnapshotId")) == canonical_id(record["sourceSnapshotId"]),
            f"Retained child snapshot no longer matches the stage receipt: {record['restoreSnapshotId']}")
    complete, error = validate_copy_status(resource, "snapshot")
    require(complete, error or f"Retained child snapshot is not available: {record['restoreSnapshotId']}")


class Azure:
    def __init__(self):
        result = subprocess.run(["az", "account", "get-access-token", "--resource",
            "https://management.azure.com/", "-o", "json"], text=True,
            capture_output=True, timeout=60, check=True)
        self.token = json.loads(result.stdout)["accessToken"]

    def get(self, path: str, api: str) -> dict | None:
        url = path if path.startswith("https://") else ARM + path
        require(url.startswith(ARM + "/"),
                "Refusing ARM GET outside management.azure.com")
        separator = "&" if "?" in url else "?"
        url += separator + "api-version=" + urllib.parse.quote(api)
        request = urllib.request.Request(url, headers={"Authorization": "Bearer " + self.token})
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None
            raise

    def collection(self, path: str, api: str) -> list[dict]:
        result = []
        url = ARM + path + "?api-version=" + urllib.parse.quote(api)
        while url:
            if not url.startswith(ARM + "/"):
                raise SafetyError("ARM pagination escaped management.azure.com")
            request = urllib.request.Request(url, headers={"Authorization": "Bearer " + self.token})
            with urllib.request.urlopen(request, timeout=45) as response:
                body = json.load(response)
            require(isinstance(body.get("value"), list), "ARM collection lacks a value array")
            result.extend(body["value"])
            url = body.get("nextLink")
        return result

    @staticmethod
    def delete_disk(resource_id: str) -> None:
        subscription = resource_id.split("/")[2]
        subprocess.run(["az", "disk", "delete", "--ids", resource_id,
            "--subscription", subscription, "--yes", "--no-wait"],
            text=True, capture_output=True, timeout=120, check=True)


def kubectl(kubeconfig: Path, *args: str) -> dict:
    command = ["kubectl", "--kubeconfig", str(kubeconfig), *args, "-o", "json"]
    result = subprocess.run(command, text=True, capture_output=True, timeout=60, check=True)
    return json.loads(result.stdout)


def validate_cluster_and_pvcs(root: Path, stack: str, context: dict,
                              disk_records: dict[str, dict]) -> None:
    state = context["state"]
    metadata = context["metadata"]
    kubeconfig = within_root(Path(state["kubeconfigPath"]))
    namespace = kubectl(kubeconfig, "get", "namespace", "repro")
    require(namespace.get("metadata", {}).get("labels", {}).get("repro.stack.token") ==
            state["ownerToken"], f"{stack}: namespace owner token changed")
    cluster = kubectl(kubeconfig, "-n", "repro", "get",
                      "cluster.postgresql.cnpg.io", "hdd-pg")
    cmeta, status = cluster.get("metadata", {}), cluster.get("status", {})
    require(cmeta.get("labels", {}).get("repro.stack.token") == state["ownerToken"] and
            status.get("readyInstances") == 2 and status.get("currentPrimary") == "hdd-pg-1",
            f"{stack}: CNPG cluster is not at the ready two-instance baseline")
    jobs = kubectl(kubeconfig, "-n", "repro", "get", "jobs",
                   "-l", "cnpg.io/cluster=hdd-pg")
    require(all((item.get("status", {}).get("active") or 0) == 0
                for item in jobs.get("items", [])),
            f"{stack}: a CNPG job is active and may contend with storage retirement")
    backup_name = metadata["backup"]
    backup = kubectl(kubeconfig, "-n", "repro", "get", "backup", backup_name)
    elements = backup.get("status", {}).get("snapshotBackupStatus", {}).get("elements", [])
    element_types = sorted(item.get("type") for item in elements)
    require(backup.get("metadata", {}).get("labels", {}).get("repro.stack.token") ==
            state["ownerToken"] and backup.get("status", {}).get("phase") == "completed" and
            element_types == ["PG_DATA", "PG_WAL"], f"{stack}: paired Backup is not complete")
    by_type = {item.get("type"): item.get("name") for item in elements}
    replicas = metadata["replicaSnapshots"]
    require(by_type.get("PG_DATA") == replicas.get("data") and
            by_type.get("PG_WAL") == replicas.get("wal"),
            f"{stack}: completed Backup outputs differ from preparation metadata")
    pvc_doc = kubectl(kubeconfig, "-n", "repro", "get", "pvc", *TARGET_CLAIMS)
    pvcs = {item.get("metadata", {}).get("name"): item for item in pvc_doc.get("items", [])}
    require(set(pvcs) == set(TARGET_CLAIMS), f"{stack}: expected four DATA/WAL PVCs")
    pv_names = []
    for name, pvc in pvcs.items():
        spec = pvc.get("spec", {})
        require(pvc.get("status", {}).get("phase") == "Bound" and
                spec.get("storageClassName") == "azure-premiumv2" and
                spec.get("resources", {}).get("requests", {}).get("storage") == "512Gi" and
                spec.get("volumeName"), f"{stack}: PVC is not bound to the expected class/size: {name}")
        pv_names.append(spec["volumeName"])
    require(len(set(pv_names)) == 4, f"{stack}: DATA/WAL PVCs share a PV")
    pv_doc = kubectl(kubeconfig, "get", "pv", *pv_names)
    pvs = {item.get("metadata", {}).get("name"): item for item in pv_doc.get("items", [])}
    require(set(pvs) == set(pv_names), f"{stack}: one or more PVC PVs disappeared")
    bound_ids = {}
    for pvc_name, pvc in pvcs.items():
        pv = pvs[pvc["spec"]["volumeName"]]
        require(pv.get("spec", {}).get("csi", {}).get("driver") == "disk.csi.azure.com",
                f"{stack}: PVC PV is not backed by Azure managed disk: {pvc_name}")
        disk_id = pv.get("spec", {}).get("csi", {}).get("volumeHandle")
        require(disk_id and canonical_id(disk_id) in disk_records,
                f"{stack}: PVC volume handle is not one of the four target disks: {pvc_name}")
        bound_ids[pvc_name] = canonical_id(disk_id)
    require(len(set(bound_ids.values())) == 4 and
            {canonical_id(item["id"]) for item in disk_records.values()} == set(bound_ids.values()),
            f"{stack}: live PVCs do not bind exactly the four inspected PremiumV2 disks")
    require(pvcs["hdd-pg-2"]["spec"].get("dataSource", {}).get("kind") == "VolumeSnapshot" and
            pvcs["hdd-pg-2"]["spec"].get("dataSource", {}).get("name") == replicas["data"] and
            pvcs["hdd-pg-2-wal"]["spec"].get("dataSource", {}).get("kind") == "VolumeSnapshot" and
            pvcs["hdd-pg-2-wal"]["spec"].get("dataSource", {}).get("name") == replicas["wal"],
            f"{stack}: replica PVCs are not seeded from the completed paired snapshots")
    vss_doc = kubectl(kubeconfig, "-n", "repro", "get", "volumesnapshot.snapshot.storage.k8s.io",
                      replicas["data"], replicas["wal"])
    vss = {item.get("metadata", {}).get("name"): item for item in vss_doc.get("items", [])}
    require(set(vss) == {replicas["data"], replicas["wal"]},
            f"{stack}: paired VolumeSnapshots disappeared")
    for name, source_pvc in ((replicas["data"], "hdd-pg-1"),
                             (replicas["wal"], "hdd-pg-1-wal")):
        item = vss[name]
        labels = item.get("metadata", {}).get("labels", {})
        source = item.get("spec", {}).get("source", {})
        require(labels.get("cnpg.io/cluster") == "hdd-pg" and
                labels.get("cnpg.io/backupName") == backup_name and
                item.get("status", {}).get("readyToUse") is True and
                source.get("persistentVolumeClaimName") == source_pvc,
                f"{stack}: paired replica-seeding snapshot is not ready/bound to the Backup source: {name}")


def verify_resource_groups(azure: Azure, context: dict, stack: str) -> tuple[str, str]:
    state = context["state"]
    prefix = f"/subscriptions/{state['subscriptionId']}/resourceGroups/"
    node_id = prefix + state["nodeResourceGroup"]
    snapshot_id = state["snapshotResourceGroupId"]
    expected_snapshot_id = prefix + state.get("snapshotResourceGroup", "")
    require(state.get("snapshotResourceGroup") and
            canonical_id(snapshot_id) == canonical_id(expected_snapshot_id),
            f"{stack}: snapshot group ID differs from the state-bound group name")
    for group_id in (node_id, snapshot_id):
        group = azure.get(group_id, RG_API)
        require(group is not None and group.get("tags", {}).get("reprotoken") == state["ownerToken"],
                f"{stack}: live resource-group owner tag changed: {group_id}")
    return node_id, snapshot_id


def validate_snapshot_inventory(azure: Azure, context: dict, stack: str,
                                expected_children: dict[str, dict],
                                measured_disk_ids: set[str]) -> None:
    state = context["state"]
    source_id = context["sourceManifest"]["sourceSnapshotId"]
    snapshot_rg = state["snapshotResourceGroupId"]
    group_path = canonical_id(snapshot_rg)
    parts = snapshot_rg.split("/")
    path = f"/subscriptions/{parts[2]}/resourceGroups/{parts[4]}/providers/Microsoft.Compute/snapshots"
    snapshots = azure.collection(path, SNAP_API)
    pins_path = ROOT / stack / ".state/known-failed-source-copies.json"
    pins = {}
    if pins_path.exists():
        pins_doc = read_json(pins_path)
        require(pins_doc.get("version") == 1 and isinstance(pins_doc.get("snapshots"), list),
                f"{stack}: malformed terminal failed-copy pin file")
        for pin in pins_doc["snapshots"]:
            pin_id = canonical_id(pin.get("id"))
            require(pin_id.startswith(group_path + "/providers/microsoft.compute/snapshots/") and
                    pin.get("ownerToken") == state["ownerToken"] and
                    pin.get("reprotoken") == state["ownerToken"] and
                    canonical_id(pin.get("sourceSnapshotId")) == canonical_id(source_id) and
                    pin.get("reproPurpose") == CHILD_PURPOSE and
                    pin.get("provisioningState") == "Failed" and
                    pin.get("completionPercent") == 0.0,
                    f"{stack}: failed-copy pin is not exact and owner/source-bound")
            pins[pin_id] = pin
    seen_pins = set()
    seen_children = set()
    for snapshot in snapshots:
        sid = canonical_id(snapshot.get("id"))
        require(sid.startswith(group_path + "/providers/microsoft.compute/snapshots/") and
                snapshot.get("tags", {}).get("reprotoken") == state["ownerToken"],
                f"{stack}: snapshot escaped owned group or owner tags: {snapshot.get('id')}")
        props = get_properties(snapshot)
        pin = pins.get(sid)
        if pin:
            valid_failed = (get_unique_id(snapshot) == pin.get("uniqueId") and
                props.get("provisioningState") == "Failed" and
                props.get("snapshotAccessState") == "Unknown" and
                (props.get("creationData") or {}).get("createOption") == "CopyStart" and
                props.get("completionPercent") == 0.0 and
                snapshot.get("tags", {}).get("reproPurpose") == CHILD_PURPOSE and
                canonical_id(snapshot.get("tags", {}).get("sourceSnapshotId")) == canonical_id(source_id))
            require(valid_failed, f"{stack}: known failed source copy no longer matches its pin")
            seen_pins.add(sid)
        else:
            complete, error = validate_copy_status(snapshot, "snapshot")
            require(complete, error or f"{stack}: snapshot copy is not complete: {sid}")
            validate_backup_snapshot_materialization(snapshot, measured_disk_ids)
            require(props.get("snapshotAccessState") == "Available",
                    f"{stack}: snapshot is not Available: {sid}")
        if sid in expected_children:
            validate_child_snapshot(snapshot, expected_children[sid])
            seen_children.add(sid)
    require(set(pins) == seen_pins,
            f"{stack}: one or more pinned terminal failed snapshots are missing/changed")
    require(set(expected_children) == seen_children,
            f"{stack}: one or more retained stage child snapshots are missing")


def measurement_not_started(root: Path) -> None:
    forbidden = [root / "measurement-gate.json"]
    for stack in RUNS:
        base = root / stack
        forbidden.extend(base / ".state" / name for name in
                         ("test-process.json", "test-state.json", "results.json", "timing.json"))
        forbidden.extend(base / name for name in ("results.json", "timing.json"))
    existing = [str(path.relative_to(root)) for path in forbidden if path.exists()]
    require(not existing, "Measurement/test evidence already exists; refusing retirement: " +
            ", ".join(existing))
    coord_path = root / "coordinator-state.json"
    if coord_path.exists():
        phase = read_json(coord_path).get("phase", "")
        require(phase not in {"launching-measurements", "measuring", "measurements-complete",
                              "cleaning-owned-compute", "complete"},
                f"Coordinator already advanced to {phase}; refusing staging retirement")


def safe_journal(root: Path, record: dict) -> dict:
    path = HERE / f"retired-{record['stack']}.json"
    if not path.exists():
        return {"path": path, "journal": None}
    journal = read_json(path)
    require(journal.get("id", "").lower() == record["id"].lower() and
            journal.get("uniqueId") == record["uniqueId"] and
            journal.get("sourceStageReceiptSha256") == record["sourceStageReceiptSha256"] and
            journal.get("restoreSnapshotId", "").lower() == record["restoreSnapshotId"].lower() and
            journal.get("restoreSnapshotUniqueId") == record["restoreSnapshotUniqueId"] and
            journal.get("retirementScriptSha256") == sha256_file(Path(__file__)),
            f"{record['stack']}: prior retirement journal is bound to different resource identity")
    return {"path": path, "journal": journal}


def save_journal(path: Path, record: dict, phase: str, **details) -> dict:
    journal = {"version": 1, "stack": record["stack"], "phase": phase,
        "retirementScriptSha256": sha256_file(Path(__file__)),
        "id": record["id"], "uniqueId": record["uniqueId"],
        "sourceSnapshotId": record["sourceSnapshotId"],
        "sourceSnapshotUniqueId": record["sourceSnapshotUniqueId"],
        "restoreSnapshotId": record["restoreSnapshotId"],
        "restoreSnapshotUniqueId": record["restoreSnapshotUniqueId"],
        "sourceStageReceiptPath": record["sourceStageReceiptPath"],
        "sourceStageReceiptSha256": record["sourceStageReceiptSha256"],
        "updatedAtUtc": dt.datetime.now(dt.timezone.utc).isoformat(), **details}
    write_json_atomic(path, journal)
    return journal


def stage_absent_is_authorized(record: dict, journal: dict | None) -> bool:
    return bool(journal and journal.get("phase") in ("delete-accepted", "absent-verified") and
                journal.get("deleteAccepted") is True and
                journal.get("retirementScriptSha256") == sha256_file(Path(__file__)) and
                journal.get("id", "").lower() == record["id"].lower() and
                journal.get("uniqueId") == record["uniqueId"] and
                journal.get("sourceStageReceiptSha256") == record["sourceStageReceiptSha256"])


def call_retention_audit(root: Path, label: str) -> Path:
    command = [sys.executable, str(root / "retention-audit.py"), "--label", label]
    subprocess.run(command, cwd=root, text=True, capture_output=True,
                   timeout=1800, check=True)
    path = root / "preflight" / f"snapshot-audit-{label}.json"
    require(path.exists(), "retention-audit.py did not write its expected inventory")
    return path


def poll_absent(azure: Azure, record: dict, timeout_seconds: int = 900) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        resource = azure.get(record["id"], DISK_API)
        if resource is None:
            return
        props = get_properties(resource)
        require(canonical_id(resource.get("id")) == canonical_id(record["id"]) and
                get_unique_id(resource) == record["uniqueId"],
                f"Resource at the stage ID changed while waiting for deletion: {record['id']}")
        require(props.get("provisioningState") in ("Succeeded", "Deleting") and
                props.get("diskState") == "Unattached" and
                not (resource.get("managedBy") or props.get("managedBy")),
                f"Stage disk changed unexpectedly while waiting for deletion: {record['id']}")
        time.sleep(10)
    raise SafetyError(f"Timed out waiting for stage disk absence: {record['id']}")


def build_plan(root: Path, azure: Azure) -> dict:
    contexts, stages = {}, []
    for stack in RUNS:
        context, stage = preparation_evidence(root, stack)
        contexts[stack] = context
        if stage:
            stages.append(stage)
    require(len(stages) == 2 and {item["stack"] for item in stages} == set(STAGE_RUNS),
            "Expected exactly the two owner-bound 50/100-GiB Standard source stages")
    children_by_stack: dict[str, dict[str, dict]] = {name: {} for name in STAGE_RUNS}
    for record in stages:
        child_key = canonical_id(record["restoreSnapshotId"])
        children_by_stack[record["stack"]][child_key] = record
    measured = []
    for stack in RUNS:
        context = contexts[stack]
        node_rg, snapshot_rg = verify_resource_groups(azure, context, stack)
        node_path = node_rg + "/providers/Microsoft.Compute/disks"
        disks = azure.collection(node_path, DISK_API)
        require(len(disks) >= 4, f"{stack}: fewer than four live node-resource-group disks")
        all_target = []
        stage_record = next((item for item in stages if item["stack"] == stack), None)
        expected_stage_id = canonical_id(stage_record["id"]) if stage_record else None
        stage_disk_seen = False
        for disk in disks:
            disk_id = canonical_id(disk.get("id"))
            validate_disk_scope(disk, node_rg)
            if disk_id == expected_stage_id:
                validate_stage_disk(disk, stage_record)
                stage_disk_seen = True
                continue
            purpose = disk.get("tags", {}).get("reproPurpose")
            require(purpose != "premiumv2-source-stage",
                    f"{stack}: unrecorded source-stage disk found; refusing broad cleanup")
            validate_nonstage_node_disk_sku(disk)
            complete, error = validate_copy_status(disk, "disk")
            require(complete, error or f"{stack}: disk is not in a terminal usable state")
            if disk.get("sku", {}).get("name") == "PremiumV2_LRS":
                all_target.append(disk)
        if stage_record and not stage_disk_seen:
            journal_path = HERE / f"retired-{stack}.json"
            journal = read_json(journal_path) if journal_path.exists() else None
            require(stage_absent_is_authorized(stage_record, journal),
                    f"{stack}: exact source-stage disk is absent without accepted-delete receipt")
        require(len(all_target) == 4, f"{stack}: expected exactly four PremiumV2 disks, found {len(all_target)}")
        disk_records = {}
        for disk in all_target:
            info = validate_target_disk(disk, node_rg, context["state"]["availabilityZone"])
            require(info["uniqueId"], f"{stack}: target disk lacks Azure uniqueId")
            disk_records[canonical_id(info["id"])] = info
            measured.append({"stack": stack, **info})
        validate_cluster_and_pvcs(root, stack, context, disk_records)
        validate_snapshot_inventory(azure, context, stack, children_by_stack.get(stack, {}),
                                    set(disk_records))
        # Require the child snapshot to exist independently and have its exact receipt lineage.
        for record in (item for item in stages if item["stack"] == stack):
            child = azure.get(record["restoreSnapshotId"], SNAP_API)
            require(child is not None, f"{stack}: retained child snapshot is absent")
            validate_child_snapshot(child, record)
    return {"contexts": contexts, "stages": stages, "measuredPremiumV2Disks": measured}


def load_stage_resources(azure: Azure, stages: list[dict], journals: dict[str, dict | None]) -> None:
    for record in stages:
        disk = azure.get(record["id"], DISK_API)
        journal = journals[record["stack"]]
        if disk is None:
            require(stage_absent_is_authorized(record, journal),
                    f"{record['stack']}: stage disk is absent without a matching accepted-delete journal")
        else:
            validate_stage_disk(disk, record)
        child = azure.get(record["restoreSnapshotId"], SNAP_API)
        require(child is not None, f"{record['stack']}: retained stage child snapshot is absent")
        validate_child_snapshot(child, record)


def apply_deletions(azure: Azure, stages: list[dict], journals: dict[str, dict | None]) -> None:
    for record in stages:
        journal_path = HERE / f"retired-{record['stack']}.json"
        disk = azure.get(record["id"], DISK_API)
        journal = read_json(journal_path) if journal_path.exists() else None
        if disk is None:
            require(stage_absent_is_authorized(record, journal),
                    f"{record['stack']}: stage absent without trusted delete acceptance")
            save_journal(journal_path, record, "absent-verified", deleteAccepted=True,
                         absenceObservedAtUtc=dt.datetime.now(dt.timezone.utc).isoformat())
            continue
        validate_stage_disk(disk, record)
        save_journal(journal_path, record, "delete-intent", deleteAccepted=False,
                     intentAtUtc=dt.datetime.now(dt.timezone.utc).isoformat())
        # Exact ARM ID, subscription, uniqueId, unattached state, and tags were rechecked above.
        azure.delete_disk(record["id"])
        save_journal(journal_path, record, "delete-accepted", deleteAccepted=True,
                     deleteAcceptedAtUtc=dt.datetime.now(dt.timezone.utc).isoformat())
        poll_absent(azure, record)
        save_journal(journal_path, record, "absent-verified", deleteAccepted=True,
                     absentVerifiedAtUtc=dt.datetime.now(dt.timezone.utc).isoformat())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="delete the two exact stage disks")
    args = parser.parse_args()
    measurement_not_started(ROOT)
    azure = Azure()
    plan = build_plan(ROOT, azure)
    journals = {}
    for record in plan["stages"]:
        journals[record["stack"]] = safe_journal(ROOT, record)["journal"]
    load_stage_resources(azure, plan["stages"], journals)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    before_path = call_retention_audit(ROOT, "staging-retirement-before-" + stamp)
    before = read_json(before_path)
    expected_children = [(item["restoreSnapshotId"], item["restoreSnapshotUniqueId"])
                         for item in plan["stages"]]
    # The before audit proves all snapshots are protected before any disk operation.
    before_fp, _ = snapshot_fingerprint(before)
    require(before.get("preservedPriorSnapshots") is True and
            before.get("allSnapshotsProtected") is True,
            "Pre-retirement global snapshot audit did not prove preserved, protected snapshots")
    before_ids = {row[0]: row[1] for row in snapshot_fingerprint(before)[1]}
    for snapshot_id, unique_id in expected_children:
        require(before_ids.get(canonical_id(snapshot_id)) == unique_id,
                f"Pre-retirement global audit omitted or changed retained child {snapshot_id}")
    preparation = {name: plan["contexts"][name]["evidence"] for name in RUNS}
    plan_doc = {
        "version": 1, "mode": "plan-only" if not args.apply else "apply",
        "createdAtUtc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "scriptSha256": sha256_file(Path(__file__)),
        "preparation": preparation,
        "deleteOnly": [{key: record[key] for key in
            ("stack", "id", "uniqueId", "sourceSnapshotId", "sourceSnapshotUniqueId",
             "restoreSnapshotId", "restoreSnapshotUniqueId", "sourceStageReceiptPath",
             "sourceStageReceiptSha256", "location", "zone")} for record in plan["stages"]],
        "measuredPremiumV2Disks": plan["measuredPremiumV2Disks"],
        "snapshotAuditBeforePath": repo_relative(before_path),
        "snapshotAuditBeforeSha256": sha256_file(before_path),
        "snapshotIdentityProtectionSha256": before_fp,
        "action": "Delete only the two listed unattached owner/source-bound Standard_LRS staging disks; "
                  "preserve every snapshot and every lock.",
        "standardCopyLimitation": STANDARD_COPY_LIMITATION,
        "officialReferences": [
            "https://learn.microsoft.com/en-us/azure/virtual-machines/scripts/create-managed-disk-from-snapshot",
            "https://learn.microsoft.com/en-us/azure/virtual-machines/disks-incremental-snapshots",
            "https://learn.microsoft.com/en-us/azure/virtual-machines/disks-instant-access-snapshots",
            "https://learn.microsoft.com/en-us/azure/virtual-machines/managed-disks-overview",
        ],
    }
    write_json_atomic(HERE / "plan.json", plan_doc)
    if not args.apply:
        print(json.dumps(plan_doc, indent=2))
        return 0
    require(not (ROOT / "preflight/staging-retirement.json").exists() or
            read_json(ROOT / "preflight/staging-retirement.json").get("phase") != "complete",
            "A completed global retirement receipt already exists; refusing duplicate apply")
    apply_deletions(azure, plan["stages"], journals)
    # Verify both resource IDs are now absent before asking the global audit to compare snapshots.
    for record in plan["stages"]:
        require(azure.get(record["id"], DISK_API) is None,
                f"Stage disk still exists after deletion: {record['id']}")
    after_path = call_retention_audit(ROOT, "staging-retirement-after-" + stamp)
    after = read_json(after_path)
    snapshot_comparison = compare_snapshot_audits(before, after, expected_children)
    # Child snapshot live checks are repeated after both source disks have disappeared.
    for record in plan["stages"]:
        child = azure.get(record["restoreSnapshotId"], SNAP_API)
        require(child is not None, f"Child snapshot disappeared after stage retirement: {record['restoreSnapshotId']}")
        validate_child_snapshot(child, record)
    final = {
        "version": 1, "phase": "complete",
        "completedAtUtc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "retirementScriptSha256": sha256_file(Path(__file__)),
        "preparation": preparation,
        "retiredStagingDisks": [
            {"stack": record["stack"], "id": record["id"], "uniqueId": record["uniqueId"],
             "sourceSnapshotId": record["sourceSnapshotId"],
             "sourceSnapshotUniqueId": record["sourceSnapshotUniqueId"],
             "restoreSnapshotId": record["restoreSnapshotId"],
             "restoreSnapshotUniqueId": record["restoreSnapshotUniqueId"],
             "sourceStageReceiptPath": record["sourceStageReceiptPath"],
             "sourceStageReceiptSha256": record["sourceStageReceiptSha256"],
             "absentVerified": True} for record in plan["stages"]],
        "snapshotAudit": {
            "beforePath": repo_relative(before_path), "afterPath": repo_relative(after_path),
            "beforeSha256": sha256_file(before_path), "afterSha256": sha256_file(after_path),
            **snapshot_comparison,
        },
        "measuredPremiumV2Disks": plan["measuredPremiumV2Disks"],
        "standardCopyLimitation": STANDARD_COPY_LIMITATION,
    }
    write_json_atomic(ROOT / "preflight/staging-retirement.json", final)
    print(json.dumps(final, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (SafetyError, subprocess.SubprocessError, OSError, KeyError, ValueError) as error:
        print(f"staging retirement blocked: {error}", file=sys.stderr)
        raise SystemExit(2)
