#!/usr/bin/env python3
"""Resume preparation after the retained snapshot pair is already restored.

This path validates the existing primary and its DATA/WAL disks. In the
original resume mode it also checks the removed source-grant ledger and
restored snapshot locks. In --direct-source-copy mode, source disks were copied
from the retained snapshots by the caller's Azure CLI identity, so no AKS
snapshot grants or source lock changes are expected; it verifies that exact
AKS grants are absent and the original locks remain intact. It then enables
the original CNPG synchronous replica configuration, waits for streaming
seeding, and writes prepare.json. It never bootstraps, rewrites the fixture,
creates source grants, changes source locks, or edits source snapshots.

Without --apply the script validates the restored primary and prints its plan.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
EXPERIMENT = HERE.parent
SYSTEM_ID = "7692518806614212629"
TARGET_HEAP = 24696061952
TARGET_TABLE = 25040748544
MEASUREMENT_IMAGE = "ghcr.io/cloudnative-pg/postgresql:16.15-202609281634-standard-trixie@sha256:1dd42563007500b94b943f2a1620f724efedfc9eea2d07a50c6db4582c2431fd"
ROLE_ID = "7efff54f-a5b4-42b5-a1c5-5411624893ce"
NS = "repro"
C = "hdd-pg"


class Refusal(RuntimeError):
    pass


def require(ok: bool, message: str) -> None:
    if not ok:
        raise Refusal(message)


def call(argv: list[str], *, timeout: int = 240, stdin: str | None = None) -> str:
    try:
        result = subprocess.run(argv, input=stdin, text=True, capture_output=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired:
        raise Refusal(f"Command timed out: {' '.join(argv[:5])}")
    if result.returncode:
        detail = (result.stderr or result.stdout).strip()
        raise Refusal(f"Command failed ({result.returncode}): {' '.join(argv[:5])}: {detail[-1600:]}")
    return result.stdout


def json_file(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise Refusal(f"Cannot read valid JSON {path}: {exc}")


def az_json(sub: str, *args: str, timeout: int = 240) -> Any:
    return json.loads(call(["az", *args, "--subscription", sub, "-o", "json"], timeout=timeout))


def k_json(kubeconfig: Path, *args: str) -> Any:
    return json.loads(call(["kubectl", "--kubeconfig", str(kubeconfig), *args, "-o", "json"], timeout=180))


def k(kubeconfig: Path, *args: str, timeout: int = 180) -> str:
    return call(["kubectl", "--kubeconfig", str(kubeconfig), *args], timeout=timeout)


def lower(value: Any) -> str:
    return str(value or "").casefold().rstrip("/")


def atomic_json(path: Path, data: Any) -> None:
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    tmp = Path(name)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(data, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stack", required=True, choices=("sigterm", "control"))
    parser.add_argument("--owner-token", required=True)
    parser.add_argument("--apply", action="store_true", help="Scale the existing Cluster and finalize its preparation evidence.")
    parser.add_argument("--direct-source-copy", action="store_true", help="Require static direct restore and verify no AKS snapshot grants or source lock changes were made.")
    parser.add_argument("--replica-timeout-seconds", type=int, default=21600)
    return parser.parse_args()


def check_source_lock(pair: dict[str, Any], sub: str, kind: str, sid: str) -> dict[str, Any]:
    rows = az_json(sub, "lock", "list", "--resource", sid)
    expected = pair[kind]["lock"]
    found = [x for x in rows if lower(x.get("id")) == lower(expected["id"]) and x.get("name") == expected["name"]]
    require(len(found) == 1, f"Retained {kind} source lock is missing or ambiguous.")
    live = found[0]
    require(live.get("level") == expected["level"] == "CanNotDelete" and live.get("notes") == expected["notes"], f"Retained {kind} source lock differs from pinned metadata.")
    return live


def validate_stack(stack: str, token: str, *, direct_source_copy: bool = False) -> dict[str, Any]:
    stack_dir = EXPERIMENT / stack
    sd = stack_dir / ".state"
    state = json_file(sd / "state.json")
    pair_path = HERE / "source-pair.json"
    proof = json_file(HERE / "source-pair-proof.json")
    pair = json_file(pair_path)
    grants_path = sd / "source-snapshot-role-assignments.json"
    locks_path = sd / "source-snapshot-lock-cycle.json"
    grants = None if direct_source_copy else json_file(grants_path)
    locks = None if direct_source_copy else json_file(locks_path)
    lock_events_path = sd / "source-snapshot-lock-cycles.jsonl"
    binding_path = sd / "direct-restore" / "binding.json"
    binding = json_file(binding_path) if binding_path.exists() else None
    disk_receipt = json_file(sd / "direct-restore" / "created-disks.json")
    kubeconfig = sd / "kubeconfig"
    prepare_path = sd / "prepare.json"

    require(len(token) == 32 and all(c in "0123456789abcdef" for c in token), "Pass this stack's exact lowercase owner token.")
    require(state.get("ownerToken") == token and state.get("status") == "ready", "Owner token or provision status does not match the selected stack.")
    require(state.get("subscriptionId") == pair.get("subscriptionId") and state.get("location", "").casefold() == "eastus", "Stack subscription or region differs from the pinned pair.")
    require(state.get("resourceGroup") == f"rg-vacuum-only-{token}" and state.get("nodeResourceGroup") == f"rg-vacuum-nodes-{token}", "Stack resource-group ownership names do not match its token.")
    require(Path(state.get("kubeconfigPath", "")).resolve() == kubeconfig.resolve() and kubeconfig.is_file(), "Isolated stack kubeconfig is missing or changed.")
    pair_sha = hashlib.sha256(pair_path.read_bytes()).hexdigest()
    require(proof.get("provenance", {}).get("copiedPairSha256") == pair_sha, "Pinned source pair hash does not match its proof.")
    require(pair.get("version") == 1 and pair.get("kind") == "offline-postgresql-pair" and pair.get("diskSizeGiB") == 512, "Pinned source pair is not the audited 512-GiB PostgreSQL pair.")
    require(pair.get("fixture", {}).get("systemIdentifier") == SYSTEM_ID and pair.get("fixture", {}).get("heapBytes") == TARGET_HEAP and pair.get("fixture", {}).get("tableBytes") == TARGET_TABLE, "Pinned source pair fixture identity/size changed.")
    require(pair.get("restoreEvidence", {}).get("status") == "complete" and pair.get("restoreEvidence", {}).get("sourceSnapshotsUntouched") is True, "Pinned pair restore evidence is incomplete.")
    require(not prepare_path.exists(), "prepare.json already exists; refusing to overwrite completed preparation evidence.")
    require(disk_receipt.get("ownerToken") == token and disk_receipt.get("sourcePairSha256") == pair_sha, "Direct disk creation receipt is not owned by this stack/pair.")
    binding_mode = "static-direct-restore" if binding is not None else "dynamic-csi-snapshot-copy"
    if binding is not None:
        require(binding.get("status") == "bound" and binding.get("ownerToken") == token and binding.get("sourcePairSha256") == pair_sha, "Static DATA/WAL binding receipt is not complete for this stack/pair.")
    if direct_source_copy:
        require(binding is not None and binding_mode == "static-direct-restore", "--direct-source-copy requires a completed static DATA/WAL binding receipt.")
        require(not grants_path.exists() and not locks_path.exists() and not lock_events_path.exists(), "Direct-source mode refuses prior source grant or lock-cycle ledgers.")
    else:
        require(grants is not None and locks is not None, "Source permission evidence is missing.")

    principal = ""
    lock_events: list[dict[str, Any]] = []
    if direct_source_copy:
        aks = az_json(pair["subscriptionId"], "aks", "show", "--resource-group", state["resourceGroup"], "--name", state["clusterName"])
        principal = aks.get("identity", {}).get("principalId", "")
        require(principal, "AKS control-plane identity is missing from the owned stack.")
    else:
        # The original resume mode requires exact grants to have been removed
        # and the original CanNotDelete locks restored.
        assert grants is not None and locks is not None
        require(grants.get("ownerToken") == token and grants.get("status") == "removed" and grants.get("postRemovalConfirmed") is True, "Source role-grant ledger is not fully removed.")
        require(grants.get("sourcePairSha256") == pair_sha and grants.get("principalId"), "Removed role-grant ledger does not match this source pair/principal.")
        principal = grants["principalId"]
        assignments = grants.get("assignments", [])
        require(len(assignments) == 2, "Role-grant ledger does not contain exactly the DATA/WAL pair.")
        for kind in ("data", "wal"):
            rows = [x for x in assignments if x.get("kind") == kind]
            require(len(rows) == 1, f"Role-grant ledger is missing the {kind} entry.")
            item = rows[0]
            require(item.get("createdByThisStack") is True and item.get("removed") is True and item.get("postRemovalRoleListAbsent") is True, f"Temporary {kind} source grant was not removed and verified.")
            require(lower(item.get("scope")) == lower(pair[kind]["id"]) and item.get("uniqueId") == pair[kind]["uniqueId"], f"Temporary {kind} grant ledger scope/UID differs from the pinned source.")
        require(locks.get("status") == "restored" and locks.get("snapshotDeletionAttempted") is False and locks.get("removed") == {"resourceGroup": False, "data": False, "wal": False}, "Source lock-cycle ledger does not show all locks restored.")
        require(lower(locks.get("sourcePairSha256")) == lower(pair_sha), "Source lock-cycle ledger belongs to a different pair.")
        require(lock_events_path.is_file(), "Source lock-cycle event evidence is missing.")
        try:
            lock_events = [json.loads(line) for line in lock_events_path.read_text().splitlines() if line.strip()]
        except json.JSONDecodeError as exc:
            raise Refusal(f"Source lock-cycle event evidence is invalid JSON: {exc}")
        require(len(lock_events) >= 2 and all(e.get("snapshotDeletionAttempted") is False for e in lock_events), "Source lock-cycle events do not prove both grant cleanups without snapshot deletion.")

    sub = pair["subscriptionId"]
    active = call(["az", "account", "show", "--query", "id", "-o", "tsv"]).strip()
    require(lower(active) == lower(sub), "Active Azure subscription differs from the pinned source pair.")
    rg = az_json(sub, "group", "show", "--name", pair["resourceGroup"])
    require(rg.get("tags", {}).get("reprotoken") == pair["resourceGroupOwnerToken"], "Retained source resource group owner tag changed.")
    rg_lock_rows = az_json(sub, "lock", "list", "--resource-group", pair["resourceGroup"])
    rg_lock = pair["resourceGroupLock"]
    require(sum(1 for x in rg_lock_rows if lower(x.get("id")) == lower(rg_lock["id"]) and x.get("name") == rg_lock["name"] and x.get("level") == "CanNotDelete" and x.get("notes") == rg_lock["notes"]) == 1, "Retained source resource-group lock does not match the pinned definition.")
    snapshots = []
    for kind in ("data", "wal"):
        sid = pair[kind]["id"]
        snap = az_json(sub, "snapshot", "show", "--ids", sid)
        require(lower(snap.get("id")) == lower(sid) and snap.get("uniqueId") == pair[kind]["uniqueId"], f"Live {kind} source snapshot identity changed.")
        require(snap.get("provisioningState") == "Succeeded" and snap.get("snapshotAccessState") == "Available" and int(snap.get("diskSizeGB", 0)) == 512, f"Live {kind} source snapshot is not available at 512 GiB.")
        require(lower(snap.get("location")) == "eastus" and snap.get("tags", {}).get("reprotoken") == pair["resourceGroupOwnerToken"], f"Live {kind} source snapshot region/owner changed.")
        direct_lock = check_source_lock(pair, sub, kind, sid)
        snapshots.append({"kind": kind, "snapshotId": snap["id"], "uniqueId": snap["uniqueId"], "sourceDiskId": snap.get("creationData", {}).get("sourceResourceId"), "provisioningState": snap.get("provisioningState"), "snapshotAccessState": snap.get("snapshotAccessState"), "completionPercent": snap.get("completionPercent"), "diskSizeGB": snap.get("diskSizeGB"), "sku": (snap.get("sku") or {}).get("name"), "directLock": direct_lock})

    # The retained-pair global lock serializes this live permission check with
    # the other stack's short source-lock cleanup cycle. Disabling principal
    # name expansion also avoids an unnecessary Microsoft Graph lookup.
    for kind in ("data", "wal"):
        scope = pair[kind]["id"]
        rows = az_json(sub, "role", "assignment", "list", "--scope", scope, "--fill-principal-name", "false")
        exact = [x for x in rows if x.get("principalId") == principal and lower(x.get("scope")) == lower(scope) and lower(x.get("roleDefinitionId", "")).endswith(ROLE_ID)]
        if direct_source_copy:
            require(not exact, f"Direct-source mode found an AKS Disk Snapshot Contributor assignment at the {kind} snapshot scope.")
        else:
            require(not exact, f"An exact temporary Disk Snapshot Contributor assignment remains at the {kind} snapshot scope.")

    ns = k_json(kubeconfig, "get", "namespace", NS)
    require(ns.get("metadata", {}).get("labels", {}).get("repro.stack.token") == token, "Namespace ownership label changed.")
    sc = k_json(kubeconfig, "get", "storageclass", "azure-standard-hdd")
    require(sc.get("provisioner") == "disk.csi.azure.com" and sc.get("parameters", {}).get("skuName") == "Standard_LRS" and sc.get("parameters", {}).get("cachingMode") == "None" and sc.get("reclaimPolicy") == "Delete", "Standard HDD StorageClass settings changed.")
    cluster = k_json(kubeconfig, "-n", NS, "get", "cluster", C)
    meta = cluster.get("metadata", {})
    spec = cluster.get("spec", {})
    require(meta.get("labels", {}).get("repro.stack.token") == token, "CNPG Cluster owner token changed.")
    require(meta.get("annotations", {}).get("vacuum-only-repro/source-data-snapshot-id", "").casefold() == pair["data"]["id"].casefold() and meta.get("annotations", {}).get("vacuum-only-repro/source-wal-snapshot-id", "").casefold() == pair["wal"]["id"].casefold(), "CNPG Cluster source snapshot ID annotations changed.")
    bootstrap = spec.get("bootstrap") or {}
    require(bootstrap.get("recovery") is None and bootstrap.get("pg_basebackup") is None, "CNPG Cluster still has snapshot recovery or pg_basebackup bootstrap configured.")
    require(spec.get("backup") is None and spec.get("externalClusters") is None, "CNPG Cluster still has backup or external archive references configured.")
    require(spec.get("imageName") == MEASUREMENT_IMAGE and cluster.get("status", {}).get("image") == MEASUREMENT_IMAGE, "CNPG Cluster is not already on the exact measurement image.")
    require(spec.get("instances") in (1, 2), "CNPG Cluster instance count is unexpected for a resume.")
    cluster_uid = meta.get("uid")
    require(cluster_uid, "CNPG Cluster has no UID.")

    # Verify direct disk, PV, PVC and source-reference chain for the primary.
    pvs = {x["metadata"]["name"]: x for x in k_json(kubeconfig, "get", "pv").get("items", [])}
    pvcs = {x["metadata"]["name"]: x for x in k_json(kubeconfig, "-n", NS, "get", "pvc").get("items", [])}
    for kind, claim_name, role in (("data", "hdd-pg-1", "PG_DATA"), ("wal", "hdd-pg-1-wal", "PG_WAL")):
        receipt_claim = (binding or {}).get("pvcs", {}).get(kind, {})
        claim = pvcs.get(claim_name)
        require(claim is not None, f"Primary {kind} PVC is missing.")
        if binding is not None:
            require(claim.get("metadata", {}).get("uid") == receipt_claim.get("uid"), f"Primary {kind} PVC UID differs from the static binding receipt.")
        cm = claim["metadata"]
        cs = claim.get("spec", {})
        require(cm.get("labels", {}).get("repro.stack.token") == token and cm.get("labels", {}).get("cnpg.io/cluster") == C and cm.get("labels", {}).get("cnpg.io/pvcRole") == role, f"Primary {kind} PVC ownership/role labels changed.")
        owner = [x for x in cm.get("ownerReferences", []) if x.get("kind") == "Cluster" and x.get("name") == C and x.get("controller") is True]
        require(len(owner) == 1 and owner[0].get("uid") == cluster_uid, f"Primary {kind} PVC owner is not this exact CNPG Cluster UID.")
        require(claim.get("status", {}).get("phase") == "Bound" and cs.get("storageClassName") == "azure-standard-hdd" and cs.get("resources", {}).get("requests", {}).get("storage") == "512Gi", f"Primary {kind} PVC is not bound to the expected 512-GiB HDD class.")
        if binding is not None:
            require(cs.get("volumeName") == receipt_claim.get("pvName"), f"Primary {kind} PVC PV name differs from the static binding receipt.")
        source_name = f"source-{kind}-{token[:8]}"
        source = pair[kind]
        ds = cs.get("dataSource") or {}
        dsref = cs.get("dataSourceRef") or {}
        for ref in (ds, dsref):
            require(ref.get("apiGroup") == "snapshot.storage.k8s.io" and ref.get("kind") == "VolumeSnapshot" and ref.get("name") == source_name, f"Primary {kind} PVC lost its exact source VolumeSnapshot reference.")
        if binding is not None:
            require(receipt_claim.get("sourceDataSource") == ds and receipt_claim.get("sourceDataSourceRef") == dsref, f"Primary {kind} PVC source references differ from its static binding receipt.")
        pv_name = receipt_claim.get("pvName") if binding is not None else cs.get("volumeName")
        pv = pvs.get(pv_name)
        require(pv is not None, f"Static {kind} PV is missing.")
        ps = pv.get("spec", {})
        pr = ps.get("claimRef", {})
        require(pr.get("namespace") == NS and pr.get("name") == claim_name and pr.get("uid") == cm.get("uid"), f"{kind} PV claimRef changed.")
        csi = ps.get("csi", {})
        disk_id = csi.get("volumeHandle", "")
        require(csi.get("driver") == "disk.csi.azure.com", f"{kind} PV has a different CSI driver.")
        attributes = csi.get("volumeAttributes", {})
        require(attributes.get("cachingMode") == "None" and attributes.get("kind") == "Managed" and attributes.get("skuName") == "Standard_LRS", f"{kind} PV attributes are not uncached Standard_LRS.")
        require(ps.get("storageClassName") == "azure-standard-hdd" and ps.get("persistentVolumeReclaimPolicy") == "Delete", f"{kind} PV StorageClass or reclaim policy changed.")
        require(lower(disk_id).startswith(lower(f"/subscriptions/{sub}/resourceGroups/{state['nodeResourceGroup']}/providers/Microsoft.Compute/disks/")), f"{kind} disk is outside the exact owned AKS node resource group.")
        if binding is not None:
            disk = binding.get("disks", {}).get(kind, {})
            require(lower(disk_id) == lower(disk.get("id")), f"Static {kind} PV points to another disk.")
            require(disk.get("sourceSnapshotUniqueId") == source["uniqueId"] and lower(disk.get("sourceSnapshotId")) == lower(source["id"]), f"Static {kind} disk receipt source identity changed.")
        live_disk = az_json(sub, "disk", "show", "--ids", disk_id)
        require(lower(live_disk.get("id")) == lower(disk_id), f"Live {kind} disk identity differs from the bound PV.")
        require(live_disk.get("sku", {}).get("name") == "Standard_LRS" and int(live_disk.get("diskSizeGB", 0)) == 512 and lower(live_disk.get("location")) == "eastus" and lower(live_disk.get("creationData", {}).get("sourceResourceId")) == lower(source["id"]), f"Live {kind} disk size/SKU/source differs from the pinned pair.")
        source_uid = live_disk.get("creationData", {}).get("sourceUniqueId")
        require(not source_uid or source_uid == source["uniqueId"], f"Live {kind} disk source uniqueId differs from the pinned pair.")
        if binding is not None:
            require(live_disk.get("uniqueId") == disk.get("uniqueId"), f"Live static {kind} disk uniqueId differs from its receipt.")

    # A primary ready on the measurement image and with the fixture intact is
    # the explicit resume point. No DML or bootstrap call is made here.
    pods = k_json(kubeconfig, "-n", NS, "get", "pods", "-l", f"cnpg.io/cluster={C}").get("items", [])
    primary_name = f"{C}-1"
    primary = k_json(kubeconfig, "-n", NS, "get", "pod", primary_name)
    require(primary.get("metadata", {}).get("labels", {}).get("repro.stack.token") == token and primary.get("metadata", {}).get("labels", {}).get("cnpg.io/instanceRole") == "primary", "Restored primary pod ownership/role changed.")
    require(primary.get("status", {}).get("phase") == "Running" and any(c.get("type") == "Ready" and c.get("status") == "True" for c in primary.get("status", {}).get("conditions", [])), "Restored primary is not ready.")
    require(any(c.get("name") == "postgres" and c.get("image") == MEASUREMENT_IMAGE for c in primary.get("spec", {}).get("containers", [])), "Restored primary is not running the exact measurement image.")

    return {"stack": stack, "token": token, "stackDir": stack_dir, "sd": sd, "state": state, "pair": pair, "pairSha": pair_sha, "grants": grants, "locks": locks, "lockEvents": lock_events, "principalId": principal, "directSourceCopy": direct_source_copy, "snapshots": snapshots, "binding": binding, "bindingMode": binding_mode, "kubeconfig": kubeconfig, "cluster": cluster, "primary": primary, "pods": pods, "preparePath": prepare_path}


SQL = """SELECT json_build_object(
 'systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),
 'inRecovery',pg_is_in_recovery(),
 'heapBytes',pg_relation_size('public.vacuum_target'),
 'tableBytes',pg_total_relation_size('public.vacuum_target'),
 'payloadBytes',(SELECT octet_length(payload) FROM public.vacuum_target WHERE id=4),
 'firstLiveId',(SELECT id FROM public.vacuum_target ORDER BY id LIMIT 1),
 'adjacentOddIdAbsent',NOT EXISTS(SELECT 1 FROM public.vacuum_target WHERE id=5),
 'indexName',(SELECT indexrelid::regclass::text FROM pg_index WHERE indrelid='public.vacuum_target'::regclass AND indexrelid='public.vacuum_target_id_idx'::regclass),
 'tableAutovacuumDisabled',EXISTS(SELECT 1 FROM pg_class WHERE oid='public.vacuum_target'::regclass AND 'autovacuum_enabled=false'=ANY(reloptions)),
 'settings',(SELECT json_object_agg(name,current_setting(name)) FROM pg_settings WHERE name=ANY(ARRAY['wal_level','full_page_writes','recovery_prefetch','vacuum_cost_delay','maintenance_io_concurrency','maintenance_work_mem','shared_buffers','max_wal_size','checkpoint_timeout','track_io_timing','track_wal_io_timing','autovacuum','wal_sender_timeout','wal_receiver_timeout','synchronous_standby_names'])))::text;"""


def sql(ctx: dict[str, Any], pod: str, statement: str) -> str:
    return call(["kubectl", "--kubeconfig", str(ctx["kubeconfig"]), "exec", "-i", "-c", "postgres", "-n", NS, pod, "--", "psql", "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "repro", "-c", statement], timeout=180).strip()


def validate_fixture(meta: dict[str, Any], *, recovery: bool) -> None:
    require(meta.get("systemIdentifier") == SYSTEM_ID and meta.get("inRecovery") is recovery, "PostgreSQL primary/replica system ID or recovery state differs from the restored fixture.")
    require(meta.get("heapBytes") == TARGET_HEAP and meta.get("tableBytes") == TARGET_TABLE and meta.get("payloadBytes") == 1536, "Restored fixture sizes/payload differ from the pinned 23-GiB pair.")
    require(meta.get("firstLiveId") == 4 and meta.get("adjacentOddIdAbsent") is True and meta.get("indexName") == "vacuum_target_id_idx" and meta.get("tableAutovacuumDisabled") is True, "Restored fixture row/index/autovacuum markers changed.")
    settings = meta.get("settings", {})
    required = {
        "wal_level": "logical",
        "full_page_writes": "on",
        "recovery_prefetch": "try",
        "vacuum_cost_delay": "0",
        "maintenance_io_concurrency": "10",
        "maintenance_work_mem": "64MB",
        "shared_buffers": "256MB",
        "max_wal_size": "1GB",
        "checkpoint_timeout": "5min",
        "track_io_timing": "on",
        "track_wal_io_timing": "on",
        "wal_sender_timeout": "5s",
        "wal_receiver_timeout": "5s",
        "autovacuum": "on",
    }
    for key, value in required.items():
        require(settings.get(key) == value, f"Restored PostgreSQL setting {key} differs from the pinned fixture.")


def lsns_caught_up(target: str, replay: str) -> bool:
    try:
        def number(lsn: str) -> int:
            hi, lo = lsn.split("/", 1)
            return (int(hi, 16) << 32) + int(lo, 16)
        return number(replay) >= number(target)
    except Exception:
        return False


def write_evidence(ctx: dict[str, Any], primary_meta: dict[str, Any], replica_meta: dict[str, Any], pods: list[dict[str, Any]], replica: str, jobs: list[dict[str, Any]], target_lsn: str, replay_lsn: str, attach_output: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    sd = ctx["sd"]
    pair = ctx["pair"]
    token = ctx["token"]
    primary_pod = f"{C}-1"
    receiver = sql(ctx, replica, "SELECT coalesce((SELECT status FROM pg_stat_wal_receiver LIMIT 1),'');")
    require(receiver == "streaming", "Replica WAL receiver is not streaming at finalization.")
    require(sql(ctx, primary_pod, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend';") == "0", "Application client sessions exist before the test.")
    require(sql(ctx, replica, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend';") == "0", "Replica has application client sessions before the test.")

    # Existing verifier captures per-node VM SKU, attached disk identities,
    # Standard_LRS, and both Azure attachment caching and CSI caching.
    verifier = ctx["stackDir"] / "verify-attachments.py"
    require(verifier.is_file(), "Stack-local verify-attachments.py is missing.")
    attach_output = call([sys.executable, str(verifier)], timeout=900)
    attach_path = sd / "attachment-verification.stdout.json"
    attach_path.write_text(attach_output)
    os.chmod(attach_path, 0o600)
    measurement_cluster = k_json(ctx["kubeconfig"], "-n", NS, "get", "cluster", C)
    cluster_path = sd / "measurement-cluster.json"
    cluster_path.write_text(json.dumps(measurement_cluster, indent=2) + "\n")
    os.chmod(cluster_path, 0o600)
    (sd / "measurement-cluster.sha256").write_text(hashlib.sha256(cluster_path.read_bytes()).hexdigest() + "  measurement-cluster.json\n")

    roles = {p.get("metadata", {}).get("name"): p for p in pods}
    primary = roles[primary_pod]
    replica_obj = roles[replica]
    require(primary.get("spec", {}).get("nodeName") != replica_obj.get("spec", {}).get("nodeName"), "CNPG primary and replica landed on the same node.")
    if ctx["directSourceCopy"]:
        source_permission_evidence = {
            "status": "not-created",
            "mode": "caller-identity-direct-copy",
            "aksPrincipalId": ctx["principalId"],
            "roleAssignmentsCreated": False,
            "exactAssignmentsAbsent": True,
            "evidence": ".state/direct-restore/source-preflight.json",
            "locksModified": False,
            "sourceLocksVerifiedUnmodified": True,
            "retentionLockDefinitionsMatch": True,
            "verifiedBy": "finalize-restored-primary.py --direct-source-copy",
        }
    else:
        source_permission_evidence = {
            "status": "removed",
            "principalId": ctx["principalId"],
            "evidence": ".state/source-snapshot-role-assignments.json",
            "globalLock": ".state/walreceiver-source-restore.lock",
            "locksModified": True,
            "locksRestoredVerified": True,
            "originalLockDefinitionsRestored": True,
            "lockStateEvidence": ".state/source-snapshot-lock-cycle.json",
            "lockCycleEvidence": ".state/source-snapshot-lock-cycles.jsonl",
            "lockState": ctx["locks"],
            "lockCycles": ctx["lockEvents"],
        }
    final = {
        "version": 1,
        "status": "complete",
        "verifiedAtUtc": now,
        "ownerToken": token,
        "location": "eastus",
        "subscriptionId": pair["subscriptionId"],
        "resourceGroup": ctx["state"]["resourceGroup"],
        "clusterName": ctx["state"]["clusterName"],
        "cluster": f"{NS}/{C}",
        "instances": 2,
        "measurementImage": MEASUREMENT_IMAGE,
        "sourceKind": "reused-prepared-paired-snapshot",
        "fixtureTargetHeapGiB": 23,
        "fixtureTargetHeapBytes": TARGET_HEAP,
        "systemIdentifier": SYSTEM_ID,
        "heapBytes": TARGET_HEAP,
        "tableBytes": TARGET_TABLE,
        "preparedLiveTuplesEstimate": 0,
        "preparedDeadTuplesEstimate": 0,
        "fixture": {"sourceKind": "reused-prepared-paired-snapshot", "heapBytes": TARGET_HEAP, "tableBytes": TARGET_TABLE, "payloadBytes": 1536, "firstLiveId": 4, "adjacentOddIdAbsent": True, "indexName": "vacuum_target_id_idx", "tableAutovacuumDisabled": True, "snapshotBeforeVacuum": True},
        "sourcePair": {"manifest": "../shared/source-pair.json", "manifestSha256": ctx["pairSha"], "backupName": pair["backup"]["name"], "backupUid": pair["backup"]["uid"], "resourceGroup": pair["resourceGroup"], "dataSnapshotId": pair["data"]["id"], "walSnapshotId": pair["wal"]["id"], "dataUniqueId": pair["data"]["uniqueId"], "walUniqueId": pair["wal"]["uniqueId"], "liveAzureVerification": ctx["snapshots"], "bootstrapMode": "CNPG offline recovery from the retained paired DATA/WAL snapshots, statically bound to owned Standard_LRS disks", "sourceSnapshotsCreatedByTestStack": False, "requiredExternalWalArchive": False, "archiveReferencesRemoved": True},
        "sourceSnapshotRoleAssignments": source_permission_evidence,
        "replicaSeed": {"method": "CNPG streaming join/basebackup from restored primary", "completedJobs": jobs, "receiverStatus": receiver, "primaryFlushLsn": target_lsn, "replicaReplayLsn": replay_lsn, "caughtUp": True},
        "finalRoles": {"primary": primary_pod, "replica": replica},
        "clientCreated": False,
        "snapshotDeletionPolicy": "Retain",
        "noBackupCRCreated": True,
        "settings": {"primary": primary_meta["settings"], "replica": replica_meta["settings"]},
        "resume": {"mode": "restored-primary", "existingFixtureReused": True, "bootstrapOrDmlRepeated": False, "volumeRestoreMode": ctx["bindingMode"], "staticBindingEvidence": (".state/direct-restore/binding.json" if ctx["bindingMode"] == "static-direct-restore" else None)},
    }
    atomic_json(ctx["preparePath"], final)


def finalize(ctx: dict[str, Any], apply: bool, replica_timeout: int) -> None:
    cluster = ctx["cluster"]
    kubeconfig = ctx["kubeconfig"]
    primary_meta = json.loads(sql(ctx, f"{C}-1", SQL))
    validate_fixture(primary_meta, recovery=False)
    if not apply:
        print(json.dumps({"mode": "dry-run", "stack": ctx["stack"], "ownerToken": ctx["token"], "primary": f"{C}-1", "primarySystemIdentifier": primary_meta["systemIdentifier"], "heapBytes": primary_meta["heapBytes"], "tableBytes": primary_meta["tableBytes"], "sourceSnapshotsUntouched": True, "directSourceCopy": ctx["directSourceCopy"], "sourcePermissionsUnchanged": ctx["directSourceCopy"], "grantsRemoved": not ctx["directSourceCopy"], "locksRestored": True, "planned": "Enable CNPG ANY 1 preferred sync, wait for streaming replica, verify catch-up and attachments, write .state/prepare.json"}, indent=2))
        return

    if cluster["spec"].get("instances") == 1:
        patch = {"spec": {"instances": 2, "postgresql": {"synchronous": {"method": "any", "number": 1, "dataDurability": "preferred"}}}}
        k(kubeconfig, "-n", NS, "patch", "cluster", C, "--type=merge", "-p", json.dumps(patch))
    end = time.monotonic() + replica_timeout
    replica_name = ""
    jobs: list[dict[str, Any]] = []
    while time.monotonic() < end:
        cluster = k_json(kubeconfig, "-n", NS, "get", "cluster", C)
        raw_jobs = k_json(kubeconfig, "-n", NS, "get", "jobs", "-l", f"cnpg.io/cluster={C}").get("items", [])
        jobs = [{"name": j.get("metadata", {}).get("name"), "uid": j.get("metadata", {}).get("uid"), "role": j.get("metadata", {}).get("labels", {}).get("cnpg.io/jobRole"), "instance": j.get("metadata", {}).get("labels", {}).get("cnpg.io/instanceName"), "succeeded": j.get("status", {}).get("succeeded", 0), "active": j.get("status", {}).get("active", 0)} for j in raw_jobs if j.get("metadata", {}).get("labels", {}).get("repro.stack.token") == ctx["token"]]
        pods = k_json(kubeconfig, "-n", NS, "get", "pods", "-l", f"cnpg.io/cluster={C}").get("items", [])
        if cluster.get("status", {}).get("readyInstances") == 2:
            ready_replicas = [p for p in pods if p.get("metadata", {}).get("labels", {}).get("repro.stack.token") == ctx["token"] and p.get("metadata", {}).get("labels", {}).get("cnpg.io/instanceRole") == "replica" and p.get("status", {}).get("phase") == "Running" and any(x.get("type") == "Ready" and x.get("status") == "True" for x in p.get("status", {}).get("conditions", []))]
            if ready_replicas:
                replica_name = ready_replicas[0]["metadata"]["name"]
                break
        time.sleep(10)
    require(replica_name, "CNPG streaming replica did not become ready before the timeout.")

    pods = k_json(kubeconfig, "-n", NS, "get", "pods", "-l", f"cnpg.io/cluster={C}").get("items", [])
    owned_ready = [p for p in pods if p.get("metadata", {}).get("labels", {}).get("repro.stack.token") == ctx["token"] and p.get("status", {}).get("phase") == "Running" and any(x.get("type") == "Ready" and x.get("status") == "True" for x in p.get("status", {}).get("conditions", []))]
    require(len(owned_ready) == 2 and any(p["metadata"]["name"] == f"{C}-1" and p["metadata"].get("labels", {}).get("cnpg.io/instanceRole") == "primary" for p in owned_ready) and any(p["metadata"]["name"] == replica_name and p["metadata"].get("labels", {}).get("cnpg.io/instanceRole") == "replica" for p in owned_ready), "Expected exactly two owned ready instances with primary and replica roles.")
    primary_meta = json.loads(sql(ctx, f"{C}-1", SQL))
    replica_meta = json.loads(sql(ctx, replica_name, SQL))
    validate_fixture(primary_meta, recovery=False)
    validate_fixture(replica_meta, recovery=True)
    require(primary_meta.get("settings", {}).get("synchronous_standby_names") == f'ANY 1 ("{C}-2")', "Primary synchronous standby setting is not ANY 1 for hdd-pg-2.")

    caught = False
    target_lsn = ""
    replay_lsn = ""
    deadline = time.monotonic() + 3600
    while time.monotonic() < deadline:
        target_lsn = sql(ctx, f"{C}-1", "SELECT pg_current_wal_flush_lsn();")
        replay_lsn = sql(ctx, replica_name, "SELECT coalesce(pg_last_wal_replay_lsn()::text,'');")
        if lsns_caught_up(target_lsn, replay_lsn):
            caught = True
            break
        time.sleep(5)
    require(caught, "Replica did not replay through the primary flush LSN within one hour.")
    write_evidence(ctx, primary_meta, replica_meta, owned_ready, replica_name, jobs, target_lsn, replay_lsn, "")
    print(f"Prepared restored primary and streaming replica; evidence: {ctx['preparePath']}")


def main() -> int:
    args = parse_args()
    local_lock_path = EXPERIMENT / args.stack / ".state" / "02-prepare.lock"
    local_lock_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with local_lock_path.open("a") as local:
        os.chmod(local_lock_path, 0o600)
        try:
            fcntl.flock(local.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise Refusal("The stack's original preparation/cleanup process still holds 02-prepare.lock.")
        source_lock = EXPERIMENT.parent.parent / ".state" / "walreceiver-source-restore.lock"
        source_lock.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(source_lock.parent, 0o700)
        with source_lock.open("a") as shared:
            os.chmod(source_lock, 0o600)
            fcntl.flock(shared.fileno(), fcntl.LOCK_EX)
            ctx = validate_stack(args.stack, args.owner_token, direct_source_copy=args.direct_source_copy)
            # All live source snapshot, lock, and grant checks are complete.
            # Do not keep the workspace lock while CNPG streams for hours.
            fcntl.flock(shared.fileno(), fcntl.LOCK_UN)
        finalize(ctx, args.apply, args.replica_timeout_seconds)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Refusal as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(2)
