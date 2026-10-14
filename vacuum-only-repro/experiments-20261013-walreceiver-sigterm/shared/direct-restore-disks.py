#!/usr/bin/env python3
"""Create or bind the retained DATA/WAL disks without CSI snapshot import.

This is an apply-gated recovery helper for the two pinned experiment stacks.
It never creates, deletes, or changes a source snapshot and never changes its
retention locks. Azure disks are created only in the stack's owned AKS node
resource group, from the exact snapshot IDs in source-pair.json. Existing disks
are reusable only when their deterministic IDs, owner tags, source IDs, source
unique IDs, and root creation receipt all match.

Examples (run from the repository root):
  python3 experiments/20261012-walreceiver-sigterm/shared/direct-restore-disks.py \
    --stack sigterm --owner-token TOKEN
  python3 experiments/20261012-walreceiver-sigterm/shared/direct-restore-disks.py \
    --stack sigterm --owner-token TOKEN --apply --bind-only
  python3 experiments/20261012-walreceiver-sigterm/shared/direct-restore-disks.py \
    --stack sigterm --owner-token TOKEN --apply --bind-only --static-direct-restore

Without --apply this only validates and prints the plan. --create-only creates
and verifies both Azure managed disks but does not bind PVCs. --bind-only uses
already-created, receipt-verified disks and only creates PVs/patches the two
primary PVCs. With neither mode, --apply does both in that order.

For the static-direct-restore path, Azure disks are first copied from the exact
retained DATA/WAL snapshots using the caller's Azure CLI identity. This flag
allows a stack-owned static VolumeSnapshot's CSI readiness status to remain
false (for example, while the AKS CSI identity cannot read the protected
snapshot). It still verifies the complete Kubernetes source-reference chain,
then binds the CNPG primary PVCs directly to the pre-copied disks. The
VolumeSnapshot objects and PVC dataSource fields remain as lineage evidence;
no CSI snapshot import is needed for those already-populated disks.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


HERE = Path(__file__).resolve().parent
EXPERIMENT = HERE.parent
STACKS = {"sigterm", "control"}
NAMESPACE = "repro"
CLUSTER_NAME = "hdd-pg"
STORAGE_CLASS = "azure-standard-hdd"
SIZE = "512Gi"
ROLE_LABEL = "cnpg.io/pvcRole"
OWNER_LABEL = "repro.stack.token"
SNAPSHOT_API = "snapshot.storage.k8s.io"
DRIVER = "disk.csi.azure.com"
RECEIPT_NAME = "created-disks.json"
BINDING_NAME = "binding.json"


class Refusal(RuntimeError):
    pass


def fail(message: str) -> None:
    raise Refusal(message)


def run(argv: list[str], *, stdin: str | None = None, timeout: int = 240) -> str:
    try:
        proc = subprocess.run(
            argv,
            input=stdin,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        fail(f"Command timed out after {timeout}s: {argv[0]} {argv[1] if len(argv) > 1 else ''}")
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout).strip()
        fail(f"Command failed ({proc.returncode}): {' '.join(argv[:5])}: {detail[-2000:]}")
    return proc.stdout


def run_json(argv: list[str], *, timeout: int = 240) -> Any:
    raw = run(argv, timeout=timeout)
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        fail(f"Expected JSON from {' '.join(argv[:4])}: {exc}")


def az(sub: str, *args: str, timeout: int = 240) -> str:
    return run(["az", *args, "--subscription", sub, "-o", "json"], timeout=timeout)


def az_json(sub: str, *args: str, timeout: int = 240) -> Any:
    return json.loads(az(sub, *args, timeout=timeout))


def kubectl(kubeconfig: Path, *args: str, timeout: int = 180, stdin: str | None = None) -> str:
    return run(["kubectl", "--kubeconfig", str(kubeconfig), *args], stdin=stdin, timeout=timeout)


def kubectl_json(kubeconfig: Path, *args: str, timeout: int = 180) -> Any:
    return json.loads(kubectl(kubeconfig, *args, timeout=timeout))


def lower(value: str) -> str:
    return value.casefold()


def expect(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"Cannot read valid JSON at {path}: {exc}")


def atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(payload, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def canonical_id(value: Any) -> str:
    return str(value or "").rstrip("/").casefold()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stack", required=True, choices=sorted(STACKS))
    parser.add_argument("--owner-token", required=True, help="Full 32-character token from that stack's private state.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--create-only", action="store_true", help="Create/verify Azure disks; do not bind Kubernetes PVCs.")
    mode.add_argument("--bind-only", action="store_true", help="Bind receipt-verified Azure disks; do not create disks.")
    parser.add_argument("--apply", action="store_true", help="Perform the planned Azure/Kubernetes writes.")
    parser.add_argument(
        "--static-direct-restore",
        action="store_true",
        help="For --bind-only, permit unready static VolumeSnapshot status after validating its exact spec lineage.",
    )
    return parser.parse_args()


def check_snapshot(sub: str, source: dict[str, Any], pair: dict[str, Any]) -> dict[str, Any]:
    snapshot = az_json(sub, "snapshot", "show", "--ids", source["id"])
    expect(canonical_id(snapshot.get("id")) == canonical_id(source["id"]), f"Live {source['kind']} snapshot ID changed.")
    expect(snapshot.get("uniqueId") == source["uniqueId"], f"Live {source['kind']} snapshot uniqueId changed.")
    expect(lower(snapshot.get("location", "")) == lower(pair["region"]), f"Live {source['kind']} snapshot is in the wrong region.")
    expect(snapshot.get("provisioningState") == "Succeeded", f"Live {source['kind']} snapshot is not Succeeded.")
    expect(snapshot.get("snapshotAccessState") == "Available", f"Live {source['kind']} snapshot is not Available.")
    expect(int(snapshot.get("diskSizeGB", 0)) == 512, f"Live {source['kind']} snapshot size is not 512 GiB.")
    expect(snapshot.get("tags", {}).get("reprotoken") == pair["resourceGroupOwnerToken"], f"Live {source['kind']} snapshot owner tag changed.")
    expect(canonical_id(snapshot.get("resourceGroup")) == lower(pair["resourceGroup"]), f"Live {source['kind']} snapshot moved resource groups.")
    return snapshot


def validate_static_stack(stack: str, token: str) -> dict[str, Any]:
    expect(re.fullmatch(r"[0-9a-f]{32}", token) is not None, "Owner token must be the exact 32-character lowercase stack token.")
    stack_dir = EXPERIMENT / stack
    state_path = stack_dir / ".state" / "state.json"
    kubeconfig = stack_dir / ".state" / "kubeconfig"
    state = read_json(state_path)
    pair_path = HERE / "source-pair.json"
    proof_path = HERE / "source-pair-proof.json"
    pair = read_json(pair_path)
    proof = read_json(proof_path)
    expect(state.get("ownerToken") == token, "Provided owner token does not match this stack's provision state.")
    expect(state.get("status") == "ready", "Stack provision state is not ready.")
    expect(state.get("subscriptionId") == pair.get("subscriptionId"), "Stack subscription does not match the retained source pair.")
    expect(lower(state.get("location", "")) == lower(pair.get("region", "")) == "eastus", "Stack and retained source pair must both be EastUS.")
    expect(state.get("resourceGroup") == f"rg-vacuum-only-{token}", "Stack RG does not follow the provisioner's exact owner-token name.")
    expect(state.get("nodeResourceGroup") == f"rg-vacuum-nodes-{token}", "AKS node RG does not follow the provisioner's exact owner-token name.")
    expect(Path(state.get("kubeconfigPath", "")).resolve() == kubeconfig.resolve(), "State kubeconfig path is not this stack's isolated kubeconfig.")
    expect(kubeconfig.is_file(), "This stack's private kubeconfig is missing.")
    expect(pair.get("version") == 1 and pair.get("kind") == "offline-postgresql-pair" and pair.get("diskSizeGiB") == 512, "Pinned source-pair metadata is not the expected 512-GiB PostgreSQL pair.")
    pair_sha = hashlib.sha256(pair_path.read_bytes()).hexdigest()
    expect(proof.get("provenance", {}).get("copiedPairSha256") == pair_sha, "Pinned source-pair proof hash does not match the metadata.")
    expect(pair.get("fixture", {}).get("sourceSnapshotBeforeVacuum") is True, "Pinned source pair is not attested as pre-VACUUM.")
    expect(pair.get("restoreEvidence", {}).get("status") == "complete", "Pinned source pair lacks completed restore evidence.")
    expect(pair.get("restoreEvidence", {}).get("sourceSnapshotsUntouched") is True, "Pinned source restore evidence does not attest snapshots untouched.")

    active = run(["az", "account", "show", "--query", "id", "-o", "tsv"]).strip()
    expect(lower(active) == lower(pair["subscriptionId"]), "Active Azure CLI subscription does not match the pinned pair.")

    rg = az_json(pair["subscriptionId"], "group", "show", "--name", state["nodeResourceGroup"])
    expect(lower(rg.get("id")) == lower(f"/subscriptions/{pair['subscriptionId']}/resourceGroups/{state['nodeResourceGroup']}"), "Live node resource group ID changed.")
    expect(lower(rg.get("location", "")) == "eastus", "Live node resource group is not EastUS.")
    expect(rg.get("tags", {}).get("reprotoken") == token, "Live AKS node resource group is not tagged to this stack.")

    sources: dict[str, dict[str, Any]] = {}
    for kind in ("data", "wal"):
        src = {"kind": kind, "id": pair[kind]["id"], "uniqueId": pair[kind]["uniqueId"]}
        sources[kind] = check_snapshot(pair["subscriptionId"], src, pair)
    return {
        "stack": stack,
        "token": token,
        "stackDir": stack_dir,
        "stateDir": stack_dir / ".state",
        "state": state,
        "pair": pair,
        "pairSha": pair_sha,
        "kubeconfig": kubeconfig,
        "sources": sources,
    }


def check_created_receipt(ctx: dict[str, Any]) -> dict[str, Any]:
    path = ctx["stateDir"] / "direct-restore" / RECEIPT_NAME
    receipt = read_json(path)
    pair = ctx["pair"]
    token = ctx["token"]
    expect(receipt.get("version") == 1, "Root's direct-disk receipt has an unsupported version.")
    expect(receipt.get("ownerToken") == token, "Root's direct-disk receipt owner token differs from this stack.")
    expect(receipt.get("subscriptionId") == pair["subscriptionId"], "Root's direct-disk receipt subscription differs from the pinned pair.")
    expect(receipt.get("resourceGroup") == ctx["state"]["nodeResourceGroup"], "Root's direct-disk receipt node resource group differs from this stack.")
    expect(receipt.get("sourcePairSha256") == ctx["pairSha"], "Root's direct-disk receipt source-pair hash differs from the pinned pair.")
    disks = receipt.get("disks")
    expect(isinstance(disks, dict) and set(disks) == {"data", "wal"}, "Root's direct-disk receipt must identify exactly one DATA and one WAL disk.")
    return receipt


def disk_expected_id(ctx: dict[str, Any], kind: str) -> str:
    token_short = ctx["token"][:8]
    name = f"restore-{kind}-{token_short}"
    return f"/subscriptions/{ctx['pair']['subscriptionId']}/resourceGroups/{ctx['state']['nodeResourceGroup']}/providers/Microsoft.Compute/disks/{name}"


def verify_disk(ctx: dict[str, Any], kind: str, receipt: dict[str, Any] | None, *, require_receipt: bool) -> dict[str, Any]:
    expected_id = disk_expected_id(ctx, kind)
    disk_name = expected_id.rsplit("/", 1)[1]
    disk = az_json(ctx["pair"]["subscriptionId"], "disk", "show", "--resource-group", ctx["state"]["nodeResourceGroup"], "--name", disk_name)
    src = ctx["sources"][kind]
    expect(canonical_id(disk.get("id")) == canonical_id(expected_id), f"Existing {kind} disk has a different ARM ID; refusing adoption.")
    expect(lower(disk.get("location", "")) == "eastus", f"Existing {kind} disk is not in EastUS.")
    expect(disk.get("sku", {}).get("name") == "Standard_LRS", f"Existing {kind} disk is not Standard_LRS.")
    expect(int(disk.get("diskSizeGb", disk.get("diskSizeGB", 0))) == 512, f"Existing {kind} disk is not 512 GiB.")
    expect(disk.get("provisioningState") == "Succeeded", f"Existing {kind} disk is not provisioned successfully.")
    expect(canonical_id(disk.get("creationData", {}).get("sourceResourceId")) == canonical_id(src["id"]), f"Existing {kind} disk was not copied from the exact pinned snapshot.")
    source_unique = disk.get("creationData", {}).get("sourceUniqueId")
    expect(not source_unique or source_unique == src["uniqueId"], f"Existing {kind} disk sourceUniqueId differs from the pinned snapshot.")
    tags = disk.get("tags") or {}
    expect(tags.get("reprotoken") == ctx["token"], f"Existing {kind} disk owner token tag changed.")
    expect(tags.get("reproPurpose") == "walreceiver-direct-restore", f"Existing {kind} disk purpose tag is not the authorized direct-restore marker.")
    expect(tags.get("reproDiskKind") == kind, f"Existing {kind} disk kind tag is wrong.")
    unique = disk.get("uniqueId")
    expect(isinstance(unique, str) and unique, f"Existing {kind} disk has no Azure uniqueId.")
    expect(not disk.get("zones"), f"Existing {kind} disk is zonal; this node pool has no configured zones and cannot safely bind it.")
    if require_receipt:
        expect(receipt is not None, "A root-created-disk receipt is required before binding.")
        item = receipt["disks"][kind]
        expect(canonical_id(item.get("id")) == canonical_id(expected_id), f"Root receipt {kind} disk ID differs from the deterministic owned disk ID.")
        expect(item.get("uniqueId") == unique, f"Root receipt {kind} disk uniqueId differs from Azure live state.")
        expect(canonical_id(item.get("sourceSnapshotId")) == canonical_id(src["id"]), f"Root receipt {kind} source snapshot ID differs from the pinned pair.")
        expect(item.get("sourceSnapshotUniqueId") == src["uniqueId"], f"Root receipt {kind} source snapshot uniqueId differs from the pinned pair.")
    return disk


def pv_name(ctx: dict[str, Any], kind: str) -> str:
    return f"restore-{kind}-{ctx['token'][:8]}"


def snapshot_names(ctx: dict[str, Any], kind: str) -> tuple[str, str]:
    token_short = ctx["token"][:8]
    return f"source-{kind}-{token_short}", f"source-{kind}-content-{token_short}"


def verify_snapshot_lineage(
    *,
    token: str,
    kind: str,
    source_snapshot_id: str,
    snapshot_name: str,
    content_name: str,
    snapshot: dict[str, Any],
    content: dict[str, Any],
    static_direct_restore: bool,
) -> dict[str, Any]:
    """Check retained-source lineage; optionally ignore CSI readiness status.

    The direct-copy route has already populated each managed disk from the
    exact source snapshot through Azure CLI. In that route, CSI snapshot
    status is only a control-plane permission signal. Kubernetes still binds
    the CNPG PVC to our static PV, so validate object ownership and source
    specs while deliberately not requiring CSI readiness.
    """
    expect(snapshot.get("metadata", {}).get("labels", {}).get(OWNER_LABEL) == token, f"VolumeSnapshot {snapshot_name} is not owned by this stack.")
    snapshot_meta = snapshot.get("metadata", {})
    snapshot_spec = snapshot.get("spec", {})
    expect(snapshot_spec.get("source", {}).get("volumeSnapshotContentName") == content_name, f"VolumeSnapshot {snapshot_name} spec references an unexpected content object.")
    if not static_direct_restore:
        expect(snapshot.get("status", {}).get("readyToUse") is True, f"VolumeSnapshot {snapshot_name} is not ready to use.")
        expect(snapshot.get("status", {}).get("boundVolumeSnapshotContentName") == content_name, f"VolumeSnapshot {snapshot_name} is bound to an unexpected content object.")

    expect(content.get("metadata", {}).get("labels", {}).get(OWNER_LABEL) == token, f"VolumeSnapshotContent {content_name} is not owned by this stack.")
    content_meta = content.get("metadata", {})
    content_spec = content.get("spec", {})
    expect(content_spec.get("driver") == DRIVER, f"VolumeSnapshotContent {content_name} driver changed.")
    expect(content_spec.get("deletionPolicy") == "Retain", f"VolumeSnapshotContent {content_name} retention policy changed.")
    expect(canonical_id(content_spec.get("source", {}).get("snapshotHandle")) == canonical_id(source_snapshot_id), f"VolumeSnapshotContent {content_name} source ID differs from the pinned snapshot.")
    content_ref = content_spec.get("volumeSnapshotRef", {})
    expect(content_ref.get("name") == snapshot_name and content_ref.get("namespace") == NAMESPACE, f"VolumeSnapshotContent {content_name} is bound to the wrong namespaced snapshot.")
    if content_ref.get("uid"):
        expect(content_ref.get("uid") == snapshot_meta.get("uid"), f"VolumeSnapshotContent {content_name} reference UID changed.")
    else:
        expect(static_direct_restore, f"VolumeSnapshotContent {content_name} has not bound to the exact VolumeSnapshot UID.")
    return {
        "snapshotUid": snapshot_meta.get("uid"),
        "snapshotContentUid": content_meta.get("uid"),
        "snapshotHandle": content_spec["source"]["snapshotHandle"],
    }


def k8s_inputs(ctx: dict[str, Any], *, static_direct_restore: bool = False) -> dict[str, Any]:
    kc = ctx["kubeconfig"]
    sub = ctx["pair"]["subscriptionId"]
    token = ctx["token"]
    cluster = kubectl_json(kc, "-n", NAMESPACE, "get", "cluster", CLUSTER_NAME, "-o", "json")
    expect(cluster.get("metadata", {}).get("labels", {}).get(OWNER_LABEL) == token, "CNPG Cluster owner token label does not match this stack.")
    cluster_uid = cluster.get("metadata", {}).get("uid")
    expect(isinstance(cluster_uid, str) and cluster_uid, "CNPG Cluster has no UID.")
    expect(cluster.get("spec", {}).get("instances") == 1, "CNPG must still have exactly the seeded primary before direct disk binding.")
    anns = cluster.get("metadata", {}).get("annotations", {})
    expect(canonical_id(anns.get("vacuum-only-repro/source-data-snapshot-id")) == canonical_id(ctx["pair"]["data"]["id"]), "CNPG Cluster DATA source annotation differs from the pinned snapshot.")
    expect(canonical_id(anns.get("vacuum-only-repro/source-wal-snapshot-id")) == canonical_id(ctx["pair"]["wal"]["id"]), "CNPG Cluster WAL source annotation differs from the pinned snapshot.")
    bootstrap = cluster.get("spec", {}).get("bootstrap", {}).get("recovery", {}).get("volumeSnapshots", {})
    expect(bootstrap.get("storage", {}).get("name") == snapshot_names(ctx, "data")[0], "CNPG bootstrap DATA VolumeSnapshot reference changed.")
    expect(bootstrap.get("walStorage", {}).get("name") == snapshot_names(ctx, "wal")[0], "CNPG bootstrap WAL VolumeSnapshot reference changed.")

    sc = kubectl_json(kc, "get", "storageclass", STORAGE_CLASS, "-o", "json")
    expect(sc.get("provisioner") == DRIVER, "Pinned Standard HDD StorageClass CSI driver changed.")
    params = sc.get("parameters") or {}
    expect(params.get("skuName") == "Standard_LRS" and params.get("cachingMode") == "None", "Pinned Standard HDD StorageClass must remain Standard_LRS with caching None.")
    expect(sc.get("reclaimPolicy") == "Delete", "Pinned Standard HDD StorageClass reclaim policy must remain Delete.")

    pvs = kubectl_json(kc, "get", "pv", "-o", "json").get("items", [])
    pv_by_name = {pv.get("metadata", {}).get("name"): pv for pv in pvs}
    pvcs = kubectl_json(kc, "-n", NAMESPACE, "get", "pvc", "-o", "json").get("items", [])
    pvc_by_name = {pvc.get("metadata", {}).get("name"): pvc for pvc in pvcs}
    pods = kubectl_json(kc, "-n", NAMESPACE, "get", "pods", "-o", "json").get("items", [])
    nodes = kubectl_json(kc, "get", "nodes", "-o", "json").get("items", [])
    node_by_name = {node.get("metadata", {}).get("name"): node for node in nodes}
    result: dict[str, Any] = {"cluster": cluster, "pvs": pv_by_name, "pvcs": {}, "pods": pods, "nodeByName": node_by_name}

    for kind, pvc_name in (("data", "hdd-pg-1"), ("wal", "hdd-pg-1-wal")):
        pvc = pvc_by_name.get(pvc_name)
        expect(pvc is not None, f"Expected primary PVC {pvc_name} is missing.")
        meta = pvc.get("metadata", {})
        spec = pvc.get("spec", {})
        expect(meta.get("labels", {}).get(OWNER_LABEL) == token, f"PVC {pvc_name} is not labeled to this stack.")
        expect(meta.get("labels", {}).get("cnpg.io/cluster") == CLUSTER_NAME, f"PVC {pvc_name} does not belong to the pinned CNPG Cluster.")
        expect(meta.get("labels", {}).get("cnpg.io/instanceName") == "hdd-pg-1", f"PVC {pvc_name} is not the primary instance claim.")
        expect(meta.get("labels", {}).get(ROLE_LABEL) == ("PG_DATA" if kind == "data" else "PG_WAL"), f"PVC {pvc_name} CNPG role label changed.")
        owner = [ref for ref in meta.get("ownerReferences", []) if ref.get("kind") == "Cluster" and ref.get("name") == CLUSTER_NAME and ref.get("controller") is True]
        expect(len(owner) == 1 and owner[0].get("uid") == cluster_uid, f"PVC {pvc_name} controller owner does not match the exact CNPG Cluster UID.")
        expect(pvc.get("status", {}).get("phase") == "Pending", f"PVC {pvc_name} is not Pending; refusing to replace or adopt a bound claim.")
        expect(not spec.get("volumeName"), f"PVC {pvc_name} already names a PV.")
        expect(spec.get("storageClassName") == STORAGE_CLASS, f"PVC {pvc_name} has an unexpected StorageClass.")
        expect(spec.get("volumeMode", "Filesystem") == "Filesystem", f"PVC {pvc_name} is not a filesystem claim.")
        expect(spec.get("accessModes") == ["ReadWriteOnce"], f"PVC {pvc_name} access mode changed.")
        expect(spec.get("resources", {}).get("requests", {}).get("storage") == SIZE, f"PVC {pvc_name} request is not exactly 512Gi.")
        expect(meta.get("uid"), f"PVC {pvc_name} has no UID.")
        snap_name, content_name = snapshot_names(ctx, kind)
        ref = spec.get("dataSource") or {}
        ref2 = spec.get("dataSourceRef") or {}
        for source_ref, field in ((ref, "dataSource"), (ref2, "dataSourceRef")):
            expect(source_ref.get("apiGroup") == SNAPSHOT_API and source_ref.get("kind") == "VolumeSnapshot" and source_ref.get("name") == snap_name, f"PVC {pvc_name} {field} no longer references the pinned {kind} snapshot.")
        # Keep both PVC source references untouched and verify the immutable
        # Kubernetes object specs. Static direct restores do not depend on the
        # CSI controller being able to read the protected Azure source; their
        # source-volume copy was already made by the caller's Azure identity.
        vs = kubectl_json(kc, "-n", NAMESPACE, "get", "volumesnapshot", snap_name, "-o", "json")
        vsc = kubectl_json(kc, "get", "volumesnapshotcontent", content_name, "-o", "json")
        snapshot_record = verify_snapshot_lineage(
            token=token,
            kind=kind,
            source_snapshot_id=ctx["pair"][kind]["id"],
            snapshot_name=snap_name,
            content_name=content_name,
            snapshot=vs,
            content=vsc,
            static_direct_restore=static_direct_restore,
        )

        consumers = []
        for pod in pods:
            pod_meta = pod.get("metadata", {})
            pod_spec = pod.get("spec", {})
            claims = {v.get("persistentVolumeClaim", {}).get("claimName") for v in pod_spec.get("volumes", [])}
            if pvc_name in claims:
                consumers.append(pod)
        if not static_direct_restore:
            expect(consumers, f"No current CNPG pod consumes PVC {pvc_name}; cannot infer its intended node safely.")
        node_names: set[str] = set()
        for pod in consumers:
            pod_meta = pod.get("metadata", {})
            expect(pod_meta.get("labels", {}).get("cnpg.io/cluster") == CLUSTER_NAME, f"PVC {pvc_name} is also mounted by a non-CNPG pod.")
            node_name = pod_spec_node = pod.get("spec", {}).get("nodeName") or pod.get("status", {}).get("nominatedNodeName")
            if not node_name:
                node_name = meta.get("annotations", {}).get("volume.kubernetes.io/selected-node")
            if node_name:
                node_names.add(node_name)
        if static_direct_restore:
            expect(len(node_names) <= 1, f"PVC {pvc_name} resolves to multiple nodes.")
            node_name = next(iter(node_names)) if node_names else None
        else:
            expect(len(node_names) == 1, f"PVC {pvc_name} does not resolve to one nominated/selected node.")
            node_name = next(iter(node_names))
        selected = meta.get("annotations", {}).get("volume.kubernetes.io/selected-node")
        if static_direct_restore and not node_name and selected:
            node_name = selected
        if node_name:
            expect(node_name in node_by_name, f"PVC {pvc_name} selected node {node_name} is not present in this stack.")
        if selected and node_name:
            expect(selected == node_name, f"PVC {pvc_name} selected-node annotation conflicts with its pod.")
        result["pvcs"][kind] = {
            "object": pvc,
            "uid": meta["uid"],
            "name": pvc_name,
            "snapshotName": snap_name,
            "snapshotUid": snapshot_record["snapshotUid"],
            "snapshotContentName": content_name,
            "snapshotContentUid": snapshot_record["snapshotContentUid"],
            "snapshotHandle": snapshot_record["snapshotHandle"],
            "selectedNode": node_name,
        }
    selected_nodes = {result["pvcs"][k]["selectedNode"] for k in ("data", "wal") if result["pvcs"][k]["selectedNode"]}
    expect(len(selected_nodes) <= 1, "DATA and WAL primary PVCs resolve to different nodes.")
    return result


def expected_tags(ctx: dict[str, Any], kind: str) -> dict[str, str]:
    return {
        "reprotoken": ctx["token"],
        "reproPurpose": "walreceiver-direct-restore",
        "reproDiskKind": kind,
    }


def create_or_validate_disks(ctx: dict[str, Any], *, apply: bool) -> tuple[dict[str, Any], dict[str, Any]]:
    receipt_path = ctx["stateDir"] / "direct-restore" / RECEIPT_NAME
    receipt = read_json(receipt_path) if receipt_path.exists() else None
    receipt_disks = receipt.get("disks", {}) if receipt else {}
    live_disks: dict[str, Any] = {}
    for kind in ("data", "wal"):
        disk_name = disk_expected_id(ctx, kind).rsplit("/", 1)[1]
        source = ctx["pair"][kind]
        tags = expected_tags(ctx, kind)
        if not apply:
            # A dry run can safely describe a missing disk without attempting a
            # write. Existing disks are validated as strictly as on apply.
            try:
                disk = az_json(ctx["pair"]["subscriptionId"], "disk", "show", "--resource-group", ctx["state"]["nodeResourceGroup"], "--name", disk_name)
            except Refusal as exc:
                if "ResourceNotFound" not in str(exc) and "could not be found" not in str(exc).lower():
                    raise
                disk = None
        else:
            try:
                disk = az_json(ctx["pair"]["subscriptionId"], "disk", "show", "--resource-group", ctx["state"]["nodeResourceGroup"], "--name", disk_name)
            except Refusal as exc:
                if "ResourceNotFound" not in str(exc) and "could not be found" not in str(exc).lower():
                    raise
                disk = None
                argv = [
                    "az", "disk", "create",
                    "--resource-group", ctx["state"]["nodeResourceGroup"],
                    "--name", disk_name,
                    "--location", "eastus",
                    "--sku", "Standard_LRS",
                    "--size-gb", "512",
                    "--source", source["id"],
                    "--tags", *[f"{k}={v}" for k, v in tags.items()],
                    "--subscription", ctx["pair"]["subscriptionId"],
                    "-o", "json",
                ]
                disk = run_json(argv, timeout=2400)
        if disk is None:
            live_disks[kind] = None
            continue
        expect(canonical_id(disk.get("id")) == canonical_id(disk_expected_id(ctx, kind)), f"Existing {kind} disk ID is not the deterministic owned target.")
        # Exact tags are required for both root-created and helper-created
        # disks. This stops the helper from adopting an unrelated resource even
        # if someone happens to use the same name.
        for key, value in tags.items():
            expect((disk.get("tags") or {}).get(key) == value, f"Existing {kind} disk tag {key} does not prove the exact stack owner and restore role.")
        if receipt is not None and kind in receipt_disks:
            item = receipt_disks[kind]
            expect(canonical_id(item.get("id")) == canonical_id(disk.get("id")), f"Root receipt {kind} disk ID differs from Azure live state.")
            expect(item.get("uniqueId") == disk.get("uniqueId"), f"Root receipt {kind} disk uniqueId differs from Azure live state.")
            expect(canonical_id(item.get("sourceSnapshotId")) == canonical_id(source["id"]), f"Root receipt {kind} source snapshot ID differs from the pinned pair.")
            expect(item.get("sourceSnapshotUniqueId") == source["uniqueId"], f"Root receipt {kind} source snapshot UID differs from the pinned pair.")
        # Full identity and provenance checks happen in verify_disk after the
        # receipt has been written by this helper or supplied by the root.
        live_disks[kind] = disk
    if not apply:
        return receipt or {}, live_disks
    # Root-created disk receipt is the cross-check for externally created
    # volumes. If absent, create one only after both exact tagged disks exist.
    # Its schema is deliberately the same contract used by the controller.
    if receipt is None:
        expect(all(live_disks[k] is not None for k in ("data", "wal")), "Both DATA and WAL disks must exist before receipt creation.")
        receipt = {
            "version": 1,
            "ownerToken": ctx["token"],
            "subscriptionId": ctx["pair"]["subscriptionId"],
            "resourceGroup": ctx["state"]["nodeResourceGroup"],
            "sourcePairSha256": ctx["pairSha"],
            "createdAtUtc": datetime.now(timezone.utc).isoformat(),
            "disks": {},
        }
        for kind, disk in live_disks.items():
            receipt["disks"][kind] = {
                "id": disk["id"],
                "uniqueId": disk["uniqueId"],
                "sourceSnapshotId": ctx["pair"][kind]["id"],
                "sourceSnapshotUniqueId": ctx["pair"][kind]["uniqueId"],
            }
        atomic_json(receipt_path, receipt)
    else:
        # Verify receipt-level provenance before permitting binding.
        expect(receipt.get("version") == 1 and receipt.get("ownerToken") == ctx["token"], "Existing root-created disk receipt has a wrong owner or version.")
        expect(receipt.get("subscriptionId") == ctx["pair"]["subscriptionId"], "Existing root-created disk receipt subscription mismatch.")
        expect(receipt.get("resourceGroup") == ctx["state"]["nodeResourceGroup"], "Existing root-created disk receipt resource group mismatch.")
        expect(receipt.get("sourcePairSha256") == ctx["pairSha"], "Existing root-created disk receipt pair hash mismatch.")
    return receipt, live_disks


def pv_object(ctx: dict[str, Any], kind: str, disk: dict[str, Any], k8s: dict[str, Any]) -> dict[str, Any]:
    pv_name_value = pv_name(ctx, kind)
    claim = k8s["pvcs"][kind]
    disk_zones = disk.get("zones") or []
    node_affinity = None
    if disk_zones:
        selected_node = claim.get("selectedNode")
        expect(selected_node, f"Zonal {kind} disk requires a selected node before static binding.")
        node = k8s["nodeByName"][selected_node]
        zone_labels = node.get("metadata", {}).get("labels", {})
        zone_key = "topology.disk.csi.azure.com/zone"
        zone = str(disk_zones[0]) if len(disk_zones) == 1 else ""
        expect(zone and zone_labels.get(zone_key) == zone, f"{kind} disk zone does not match the nominated node's Azure Disk CSI zone label.")
        node_affinity = {
            "required": {
                "nodeSelectorTerms": [
                    {"matchExpressions": [{"key": zone_key, "operator": "In", "values": [zone]}]}
                ]
            }
        }
    spec: dict[str, Any] = {
        "capacity": {"storage": SIZE},
        "accessModes": ["ReadWriteOnce"],
        "persistentVolumeReclaimPolicy": "Delete",
        "storageClassName": STORAGE_CLASS,
        "volumeMode": "Filesystem",
        "claimRef": {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "namespace": NAMESPACE,
            "name": claim["name"],
            "uid": claim["uid"],
        },
        "csi": {
            "driver": DRIVER,
            "volumeHandle": disk["id"],
            "fsType": "ext4",
            "volumeAttributes": {
                "cachingMode": "None",
                "kind": "Managed",
                "skuName": "Standard_LRS",
            },
        },
    }
    if node_affinity is not None:
        spec["nodeAffinity"] = node_affinity
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {
            "name": pv_name_value,
            "labels": {OWNER_LABEL: ctx["token"], "app.kubernetes.io/part-of": "walreceiver-sigterm-repro"},
            "annotations": {
                "vacuum-only-repro/created-by": "direct-restore-disks.py",
                "vacuum-only-repro/source-snapshot-id": ctx["pair"][kind]["id"],
                "vacuum-only-repro/source-snapshot-unique-id": ctx["pair"][kind]["uniqueId"],
                "vacuum-only-repro/source-pair-sha256": ctx["pairSha"],
                "vacuum-only-repro/source-pvc-uid": claim["uid"],
                "vacuum-only-repro/owner-token": ctx["token"],
            },
        },
        "spec": spec,
    }


def verify_existing_pv(ctx: dict[str, Any], kind: str, pv: dict[str, Any], disk: dict[str, Any], k8s: dict[str, Any]) -> None:
    claim = k8s["pvcs"][kind]
    expect(pv.get("metadata", {}).get("labels", {}).get(OWNER_LABEL) == ctx["token"], f"PV {pv_name(ctx, kind)} already exists without this stack's owner token; refusing adoption.")
    ann = pv.get("metadata", {}).get("annotations", {})
    expect(ann.get("vacuum-only-repro/created-by") == "direct-restore-disks.py", f"PV {pv_name(ctx, kind)} was not created by this helper.")
    expect(ann.get("vacuum-only-repro/source-pvc-uid") == claim["uid"], f"PV {pv_name(ctx, kind)} claims another PVC UID.")
    expect(ann.get("vacuum-only-repro/source-snapshot-unique-id") == ctx["pair"][kind]["uniqueId"], f"PV {pv_name(ctx, kind)} has a different source snapshot UID.")
    spec = pv.get("spec", {})
    ref = spec.get("claimRef", {})
    expect(ref.get("namespace") == NAMESPACE and ref.get("name") == claim["name"] and ref.get("uid") == claim["uid"], f"PV {pv_name(ctx, kind)} claimRef does not bind the exact Pending PVC UID.")
    expect(canonical_id(spec.get("csi", {}).get("volumeHandle")) == canonical_id(disk.get("id")), f"PV {pv_name(ctx, kind)} points to a different managed disk.")
    expect(spec.get("csi", {}).get("driver") == DRIVER, f"PV {pv_name(ctx, kind)} CSI driver changed.")
    expect(spec.get("csi", {}).get("volumeAttributes") == {"cachingMode": "None", "kind": "Managed", "skuName": "Standard_LRS"}, f"PV {pv_name(ctx, kind)} disk attributes changed.")
    expect(spec.get("persistentVolumeReclaimPolicy") == "Delete" and spec.get("storageClassName") == STORAGE_CLASS, f"PV {pv_name(ctx, kind)} reclaim policy or StorageClass changed.")


def bind_one(ctx: dict[str, Any], kind: str, disk: dict[str, Any], k8s: dict[str, Any], *, apply: bool) -> dict[str, Any]:
    kc = ctx["kubeconfig"]
    pv_name_value = pv_name(ctx, kind)
    pv_obj = k8s["pvs"].get(pv_name_value)
    if pv_obj is None:
        obj = pv_object(ctx, kind, disk, k8s)
        if apply:
            kubectl(kc, "create", "-f", "-", stdin=json.dumps(obj))
            pv_obj = kubectl_json(kc, "get", "pv", pv_name_value, "-o", "json")
        else:
            pv_obj = obj
    else:
        verify_existing_pv(ctx, kind, pv_obj, disk, k8s)
    if apply:
        # This only assigns spec.volumeName. The existing dataSource and
        # dataSourceRef remain intact and are rechecked immediately afterward.
        kubectl(kc, "-n", NAMESPACE, "patch", "pvc", k8s["pvcs"][kind]["name"], "--type=merge", "-p", json.dumps({"spec": {"volumeName": pv_name_value}}))
        bound_pvc = kubectl_json(kc, "-n", NAMESPACE, "get", "pvc", k8s["pvcs"][kind]["name"], "-o", "json")
        pvc_spec = bound_pvc.get("spec", {})
        source_before = k8s["pvcs"][kind]["object"]["spec"]
        expect(pvc_spec.get("dataSource") == source_before.get("dataSource"), f"PVC {k8s['pvcs'][kind]['name']} dataSource changed while binding.")
        expect(pvc_spec.get("dataSourceRef") == source_before.get("dataSourceRef"), f"PVC {k8s['pvcs'][kind]['name']} dataSourceRef changed while binding.")
        expect(pvc_spec.get("volumeName") == pv_name_value, f"PVC {k8s['pvcs'][kind]['name']} did not retain the requested PV binding.")
        end = time.monotonic() + 180
        while time.monotonic() < end:
            bound_pvc = kubectl_json(kc, "-n", NAMESPACE, "get", "pvc", k8s["pvcs"][kind]["name"], "-o", "json")
            if bound_pvc.get("status", {}).get("phase") == "Bound":
                break
            if bound_pvc.get("status", {}).get("phase") not in (None, "Pending"):
                fail(f"PVC {k8s['pvcs'][kind]['name']} entered unexpected phase {bound_pvc.get('status', {}).get('phase')}.")
            time.sleep(2)
        expect(bound_pvc.get("status", {}).get("phase") == "Bound", f"PVC {k8s['pvcs'][kind]['name']} did not become Bound within 180 seconds.")
        pv_obj = kubectl_json(kc, "get", "pv", pv_name_value, "-o", "json")
        ref = pv_obj.get("spec", {}).get("claimRef", {})
        expect(ref.get("uid") == k8s["pvcs"][kind]["uid"], f"Bound PV {pv_name_value} claimRef changed to another PVC UID.")
        expect(canonical_id(pv_obj.get("spec", {}).get("csi", {}).get("volumeHandle")) == canonical_id(disk.get("id")), f"Bound PV {pv_name_value} disk ID changed.")
    return pv_obj


def main() -> int:
    args = parse_args()
    expect(not args.static_direct_restore or args.bind_only, "--static-direct-restore is only valid with --bind-only.")
    ctx = validate_static_stack(args.stack, args.owner_token)
    state_dir = ctx["stateDir"]
    binding_path = state_dir / "direct-restore" / BINDING_NAME
    if args.apply:
        (state_dir / "direct-restore").mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(state_dir / "direct-restore", 0o700)
        lock_path = state_dir / "direct-restore" / "binding.lock"
        with lock_path.open("a") as lock:
            os.chmod(lock_path, 0o600)
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                fail("A direct-restore helper is already running for this stack.")
            return run_under_lock(args, ctx, binding_path)
    return run_under_lock(args, ctx, binding_path)


def run_under_lock(args: argparse.Namespace, ctx: dict[str, Any], binding_path: Path) -> int:
    # Check whether the live root receipt is present before allowing a bind.
    receipt_path = ctx["stateDir"] / "direct-restore" / RECEIPT_NAME
    receipt = read_json(receipt_path) if receipt_path.exists() else None
    if args.bind_only:
        expect(receipt is not None, f"Cannot bind without the root-created paired disk receipt: {receipt_path}")
        check_created_receipt(ctx)
        receipt = check_created_receipt(ctx)

    create_only = args.create_only
    do_create = not args.bind_only
    do_bind = not create_only
    k8s = None
    if do_bind:
        k8s = k8s_inputs(ctx, static_direct_restore=args.static_direct_restore)

    if args.bind_only:
        disks = {kind: verify_disk(ctx, kind, receipt, require_receipt=True) for kind in ("data", "wal")}
    elif do_create:
        # In --apply mode the helper can create disks itself. If the parent
        # controller has already created them, strict owner/provenance checks
        # let it resume safely and preserve the root receipt.
        receipt, raw = create_or_validate_disks(ctx, apply=args.apply)
        if args.apply and not receipt:
            receipt = check_created_receipt(ctx)
        disks = {kind: verify_disk(ctx, kind, receipt or None, require_receipt=args.apply) for kind in ("data", "wal") if raw.get(kind) is not None}
    else:
        disks = {}

    if not args.apply:
        print(json.dumps({
            "mode": "dry-run",
            "stack": args.stack,
            "ownerToken": args.owner_token,
            "sourcePairSha256": ctx["pairSha"],
            "createDisks": do_create,
            "bindPVs": do_bind,
            "disks": {kind: (disk or {}).get("id") if disk else None for kind, disk in disks.items()},
            "pvcUIDs": ({kind: value["uid"] for kind, value in k8s["pvcs"].items()} if k8s else {}),
            "actions": "No resources changed; pass --apply only after reviewing the IDs and PVC UIDs.",
        }, indent=2))
        return 0

    if args.create_only:
        print(f"Verified direct restore DATA/WAL disks for {args.stack}; no Kubernetes PV/PVC changes made.")
        return 0

    expect(k8s is not None, "Internal error: Kubernetes inputs were not checked before bind.")
    # Recheck receipt and every Azure resource immediately before binding.
    receipt = check_created_receipt(ctx)
    disks = {kind: verify_disk(ctx, kind, receipt, require_receipt=True) for kind in ("data", "wal")}
    before = read_json(binding_path) if binding_path.exists() else None
    if before:
        expect(before.get("ownerToken") == ctx["token"] and before.get("sourcePairSha256") == ctx["pairSha"], "Existing helper binding receipt belongs to another stack or source pair.")
        expect(before.get("clusterUid") == k8s["cluster"].get("metadata", {}).get("uid"), "Existing helper binding receipt references a different CNPG Cluster UID.")
        for kind in ("data", "wal"):
            expect(before.get("pvcs", {}).get(kind, {}).get("uid") == k8s["pvcs"][kind]["uid"], f"Existing helper binding receipt references another {kind} PVC UID.")

    record: dict[str, Any] = before or {
        "version": 1,
        "status": "binding",
        "startedAtUtc": datetime.now(timezone.utc).isoformat(),
        "stack": args.stack,
        "ownerToken": ctx["token"],
        "subscriptionId": ctx["pair"]["subscriptionId"],
        "resourceGroup": ctx["state"]["nodeResourceGroup"],
        "sourcePairSha256": ctx["pairSha"],
        "clusterUid": k8s["cluster"]["metadata"]["uid"],
        "snapshots": {},
        "disks": {},
        "pvcs": {},
        "nodeAffinity": "omitted for zoneless Standard_LRS disks and the unzoned AKS node pool; exact nominated node recorded per PVC",
    }
    for kind in ("data", "wal"):
        src = ctx["pair"][kind]
        claim = k8s["pvcs"][kind]
        disk = disks[kind]
        record["snapshots"][kind] = {"id": src["id"], "uniqueId": src["uniqueId"], "snapshotName": claim["snapshotName"], "snapshotUid": claim["snapshotUid"], "contentName": claim["snapshotContentName"], "contentUid": claim["snapshotContentUid"]}
        record["disks"][kind] = {"id": disk["id"], "uniqueId": disk["uniqueId"], "sourceSnapshotId": src["id"], "sourceSnapshotUniqueId": src["uniqueId"], "sku": disk["sku"]["name"], "sizeGiB": 512}
        record["pvcs"][kind] = {"name": claim["name"], "uid": claim["uid"], "sourceDataSource": claim["object"]["spec"].get("dataSource"), "sourceDataSourceRef": claim["object"]["spec"].get("dataSourceRef"), "selectedNode": claim["selectedNode"], "pvName": pv_name(ctx, kind), "pvUid": None}
    atomic_json(binding_path, record)

    for kind in ("data", "wal"):
        # Refresh PVC/PV inventory between bind operations. CNPG may react to
        # the primary becoming available; this helper only binds the original
        # primary PVC UIDs captured above and will never touch replica claims.
        pv_obj = bind_one(ctx, kind, disks[kind], k8s, apply=True)
        record["pvcs"][kind]["pvUid"] = pv_obj.get("metadata", {}).get("uid")
        record["pvcs"][kind]["boundAtUtc"] = datetime.now(timezone.utc).isoformat()
        atomic_json(binding_path, record)

    record["status"] = "bound"
    record["completedAtUtc"] = datetime.now(timezone.utc).isoformat()
    atomic_json(binding_path, record)
    print(f"Bound the exact retained DATA/WAL snapshots to the existing primary PVC UIDs for {args.stack}.")
    print(f"Local binding receipt: {binding_path}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Refusal as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(2)
