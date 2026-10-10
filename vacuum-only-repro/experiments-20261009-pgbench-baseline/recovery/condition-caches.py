#!/usr/bin/env python3
"""Drop Linux page caches on this run's two PostgreSQL worker nodes.

Default mode is local-only. Apply mode requires --apply and the exact run owner
token, and revalidates Azure and Kubernetes immediately before creating two
short-lived privileged pods pinned to the primary and replica nodes.
"""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any


HERE = Path(__file__).resolve().parents[1]
RUN = HERE / "run"
STATE = RUN / ".state"
RECOVERY = STATE / "recovery"
RECEIPT = RECOVERY / "cache-conditioning.json"
EVENTS = RECOVERY / "cache-conditioning-events.jsonl"
LOCK = RECOVERY / "cache-conditioning.lock"
NAMESPACE = "repro"
CLUSTER = "hdd-pg"
SECRET = "azure-barman-credentials"
DISK_CLAIMS = ("hdd-pg-1", "hdd-pg-1-wal", "hdd-pg-2", "hdd-pg-2-wal")
EXPECTED_SUBSCRIPTION = "00000000-0000-0000-0000-000000000000"
EXPECTED_IMAGE = (
    "ghcr.io/cloudnative-pg/postgresql:16.15-202609281634-standard-trixie"
    "@sha256:1dd42563007500b94b943f2a1620f724efedfc9eea2d07a50c6db4582c2431fd"
)


class Refusal(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise Refusal(message)


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def canon(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Refusal(f"Cannot read valid JSON input {path}: {exc}") from exc
    require(isinstance(value, dict), f"Expected a JSON object in {path}.")
    return value


def save_json(path: Path, value: Any, *, exclusive: bool = False) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    raw = canon(value)
    if exclusive:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.write(fd, raw)
            os.fsync(fd)
        finally:
            os.close(fd)
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, raw)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(temporary, path)
    os.chmod(path, 0o600)


def event(phase: str, resource: str, name: str, details: Any = None) -> None:
    RECOVERY.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(RECOVERY, 0o700)
    item = {"atUtc": utc_now(), "phase": phase, "resource": resource, "name": name,
            "details": details if details is not None else {}}
    fd = os.open(EVENTS, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(fd, canon(item))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.chmod(EVENTS, 0o600)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def local_plan(owner_token: str | None, apply: bool) -> dict[str, Any]:
    state = read_json(STATE / "state.json")
    cfg = read_json(RUN / "run-config.json")
    require(state.get("status") == "ready", "Run state is not ready.")
    token = state.get("ownerToken")
    require(isinstance(token, str) and re.fullmatch(r"[a-f0-9]{32}", token) is not None,
            "Run owner token is missing or invalid.")
    if owner_token is not None:
        require(owner_token == token, "--owner-token does not match the saved run owner token.")
    if apply:
        require(owner_token == token, "Apply requires --owner-token with this run's exact owner token.")
    require(state.get("subscriptionId") == EXPECTED_SUBSCRIPTION,
            "Run subscription differs from the pinned experiment subscription.")
    require(state.get("resourceGroup") == f"rg-vacuum-only-{token}"
            and state.get("nodeResourceGroup") == f"rg-vacuum-nodes-{token}"
            and state.get("requestedNodeResourceGroup") == state.get("nodeResourceGroup")
            and state.get("clusterName") == f"aks-vacuum-only-{token[:12]}",
            "Saved Azure ownership names do not match this run token.")
    require(state.get("location", "").casefold() == "eastus"
            and str(state.get("availabilityZone")) == "1",
            "Run location/zone differs from East US zone 1.")
    require(cfg.get("targetHeapGiB") == 23 and cfg.get("diskSku") == "PremiumV2_LRS"
            and cfg.get("diskIops") == 3000 and cfg.get("diskMBps") == 125
            and cfg.get("diskSizeGiB") == 512 and cfg.get("nodeCount") == 2
            and cfg.get("nodeVmSize") == "Standard_D2s_v4" and cfg.get("zone") == "1"
            and cfg.get("postgresImage") == EXPECTED_IMAGE,
            "Run configuration differs from the pinned baseline/image.")
    kubeconfig = Path(state.get("kubeconfigPath") or STATE / "kubeconfig").expanduser()
    if not kubeconfig.is_absolute():
        kubeconfig = RUN / kubeconfig
    kubeconfig = kubeconfig.resolve()
    require(kubeconfig.is_file() and kubeconfig.is_relative_to(HERE),
            "Kubeconfig is missing or outside this experiment workspace.")

    prep_path = STATE / "prepare.json"
    audit_path = RECOVERY / "prepare-audit.json"
    finalized_paths = sorted(STATE.glob("finalize-process*.json"),
                             key=lambda path: read_json(path).get("attempt", 1))
    seed_path = finalized_paths[-1] if finalized_paths else STATE / "finalize-process.json"
    require(prep_path.is_file() and audit_path.is_file() and seed_path.is_file(),
            "Successful final preparation and seed receipts are required before cache conditioning.")
    prep, audit, seed = read_json(prep_path), read_json(audit_path), read_json(seed_path)
    require(prep.get("status") == "complete" and prep.get("clientCreated") is False
            and prep.get("measurementImage") == EXPECTED_IMAGE,
            "Final preparation receipt is incomplete or does not prove no client was created.")
    require(prep.get("finalAudit", {}).get("allFourPremiumV2DisksHydrated") is True
            and prep.get("finalAudit", {}).get("archiveSecretAbsent") is True
            and prep.get("finalAudit", {}).get("archiveWriterAbsent") is True
            and prep.get("finalAudit", {}).get("snapshotSeededReplica") is True,
            "Final preparation receipt does not prove the four-disk, no-writer, seeded baseline.")
    require(audit.get("status") == "complete" and audit.get("ownerToken") == token
            and audit.get("subscriptionId") == EXPECTED_SUBSCRIPTION
            and audit.get("resourceGroup") == state.get("resourceGroup")
            and audit.get("nodeResourceGroup") == state.get("nodeResourceGroup")
            and audit.get("cluster") == "repro/hdd-pg" and audit.get("instances") == 2
            and audit.get("clientCreated") is False and audit.get("noFixtureDML") is True
            and audit.get("archiveSecretAbsent") is True and audit.get("archiveWriterAbsent") is True,
            "Preparation audit does not bind this owner and no-client/no-writer state.")
    require(seed.get("phase") == "finalize" and seed.get("stack") == RUN.name
            and seed.get("status") == "complete" and seed.get("exitCode") == 0,
            "Replica seed phase lacks a terminal successful run-phase receipt.")
    require(audit.get("measurementImage") == EXPECTED_IMAGE and prep.get("cluster") == "repro/hdd-pg",
            "Preparation receipts do not bind the pinned measurement image and Cluster.")
    disk_records = audit.get("hydratedPremiumV2Disks")
    require(isinstance(disk_records, list) and len(disk_records) == 4,
            "Preparation audit does not list exactly four hydrated PremiumV2 disks.")
    by_claim = {d.get("claim"): d for d in disk_records if isinstance(d, dict)}
    require(set(by_claim) == set(DISK_CLAIMS), "Preparation audit disk claims are not the exact DATA/WAL pair set.")
    disk_ids = []
    for claim, disk in by_claim.items():
        disk_id = disk.get("id")
        require(isinstance(disk_id, str) and disk_id.startswith("/subscriptions/"),
                f"Preparation audit has no Azure disk ID for {claim}.")
        require(disk.get("sku") == "PremiumV2_LRS" and disk.get("diskSizeGiB") == 512
                and disk.get("iops") == 3000 and disk.get("throughputMBps") == 125
                and float(disk.get("completionPercent", -1)) == 100
                and disk.get("createOption") in ("Copy", "CopyStart")
                and str(disk.get("sector")) == "512" and "1" in [str(z) for z in disk.get("zones", [])],
                f"Preparation audit does not prove the expected hydrated disk settings for {claim}.")
        disk_ids.append(disk_id.casefold())
    require(len(set(disk_ids)) == 4, "Preparation audit repeats an Azure managed disk ID.")

    require(not RECEIPT.exists(), "A cache-conditioning receipt already exists; refusing to repeat or overwrite it.")
    condition_process = STATE / "condition-process.json"
    if condition_process.exists():
        phase = read_json(condition_process)
        require(phase.get("phase") == "condition" and phase.get("stack") == RUN.name
                and phase.get("status") in ("starting", "running"),
                "Existing condition phase receipt is not an active run-phase invocation.")
    for path in (STATE / "test-state.json", STATE / "test-process.json"):
        require(not path.exists(), f"Measurement has already started ({path.name}); cache conditioning is too late.")
    results = RUN / "results"
    require(not results.exists() or not any(results.iterdir()),
            "Measurement result artifacts already exist; cache conditioning must precede client/test work.")
    return {"state": state, "config": cfg, "prepare": prep, "audit": audit,
            "seedProcess": seed, "token": token, "diskRecords": by_claim,
            "kubeconfig": kubeconfig, "sourceSha256": sha256(Path(__file__).resolve())}


def command(argv: list[str], *, kubeconfig: Path | None = None, timeout_s: int = 180,
            stdin: str | None = None, allow_not_found: bool = False) -> str:
    if argv and argv[0] == "kubectl" and kubeconfig is not None:
        argv = ["kubectl", f"--kubeconfig={kubeconfig}", *argv[1:]]
    try:
        result = subprocess.run(argv, input=stdin, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=timeout_s, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise Refusal(f"Command did not complete ({argv[0]} {argv[1:3]}): {exc}") from exc
    if result.returncode != 0:
        if allow_not_found and result.returncode == 1 and "NotFound" in result.stderr:
            return ""
        detail = result.stderr.strip().splitlines()[-1:] or [f"exit {result.returncode}"]
        raise Refusal(f"Command failed ({argv[0]} {argv[1:3]}): {detail[0]}")
    return result.stdout


def kjson(kubeconfig: Path, *args: str, timeout_s: int = 180,
          allow_not_found: bool = False) -> dict[str, Any] | None:
    raw = command(["kubectl", *args, "-o", "json"], kubeconfig=kubeconfig,
                  timeout_s=timeout_s, allow_not_found=allow_not_found)
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise Refusal(f"kubectl {args[0]} returned invalid JSON.") from exc
    require(isinstance(value, dict), f"kubectl {args[0]} did not return an object.")
    return value


def azjson(subscription: str, *args: str, timeout_s: int = 900) -> Any:
    raw = command(["az", *args, "--subscription", subscription, "-o", "json"], timeout_s=timeout_s)
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise Refusal(f"Azure CLI {args[0]} returned invalid JSON.") from exc


def parse_provider_id(value: str) -> dict[str, str]:
    pattern = (r"^azure:///subscriptions/([^/]+)/resourceGroups/([^/]+)/providers/"
               r"Microsoft\.Compute/virtualMachineScaleSets/([^/]+)/virtualMachines/([^/]+)$")
    match = re.fullmatch(pattern, value or "", re.IGNORECASE)
    require(match is not None, f"AKS node has an unexpected Azure providerID: {value!r}.")
    return {"subscriptionId": match.group(1), "resourceGroup": match.group(2),
            "vmss": match.group(3), "instanceId": match.group(4)}


def validate_live(plan: dict[str, Any]) -> dict[str, Any]:
    state, token = plan["state"], plan["token"]
    sub, rg, node_rg, aks = (state["subscriptionId"], state["resourceGroup"],
                             state["nodeResourceGroup"], state["clusterName"])
    kubeconfig = plan["kubeconfig"]
    account = azjson(sub, "account", "show")
    require(str(account.get("id", "")).casefold() == sub.casefold(), "Active Azure account subscription mismatch.")
    aks_live = azjson(sub, "aks", "show", "--resource-group", rg, "--name", aks)
    require(aks_live.get("name") == aks and aks_live.get("tags", {}).get("reprotoken") == token
            and aks_live.get("nodeResourceGroup", "").casefold() == node_rg.casefold()
            and aks_live.get("provisioningState") == "Succeeded",
            "Live AKS identity/ownership/readiness does not match this run.")
    pools = azjson(sub, "aks", "nodepool", "list", "--resource-group", rg, "--cluster-name", aks)
    require(isinstance(pools, list) and len(pools) == 1, "AKS must contain exactly its one isolated node pool.")
    pool = pools[0]
    pool_name = pool.get("name")
    require(pool_name == "nodepool1" and pool.get("count") == 2
            and pool.get("vmSize") == "Standard_D2s_v4" and pool.get("mode") == "System"
            and pool.get("provisioningState") == "Succeeded",
            "AKS node pool is not the exact two-node owned baseline pool.")
    vmsses = azjson(sub, "vmss", "list", "--resource-group", node_rg)
    require(isinstance(vmsses, list) and len(vmsses) == 1,
            "AKS node resource group does not contain exactly one VM scale set.")
    vmss = vmsses[0]
    vmss_name = vmss.get("name", "")
    require(vmss_name.casefold().startswith("aks-nodepool1-")
            and (vmss.get("sku") or {}).get("name") == "Standard_D2s_v4"
            and str((vmss.get("sku") or {}).get("capacity")) == "2"
            and vmss.get("provisioningState") == "Succeeded",
            "VM scale set is not the exact two-node D2s_v4 pool for this owned AKS.")
    instances = azjson(sub, "vmss", "list-instances", "--resource-group", node_rg,
                       "--name", vmss_name)
    require(isinstance(instances, list) and len(instances) == 2,
            "Azure VM scale set does not have exactly two live instances.")
    instance_ids = {str(item.get("instanceId")) for item in instances}
    require(len(instance_ids) == 2,
            "Azure VM scale set instance identities are not distinct.")

    context = command(["kubectl", "config", "current-context"], kubeconfig=kubeconfig).strip()
    require(aks.casefold() in context.casefold(), "Kubernetes context does not belong to this run's AKS.")
    namespace = kjson(kubeconfig, "get", "namespace", NAMESPACE)
    require(namespace and namespace.get("metadata", {}).get("labels", {}).get("repro.stack.token") == token,
            "Kubernetes namespace owner token mismatch.")
    cluster = kjson(kubeconfig, "get", "cluster", CLUSTER, "-n", NAMESPACE)
    require(cluster and cluster.get("metadata", {}).get("labels", {}).get("repro.stack.token") == token,
            "CNPG Cluster owner token mismatch.")
    spec, status = cluster.get("spec", {}), cluster.get("status", {})
    require(spec.get("instances") == 2 and status.get("readyInstances") == 2
            and status.get("currentPrimary") == "hdd-pg-1"
            and status.get("systemID") == str(plan["prepare"].get("systemIdentifier"))
            and spec.get("imageName") == EXPECTED_IMAGE
            and spec.get("postgresql", {}).get("parameters", {}).get("synchronous_commit") == "on"
            and spec.get("postgresql", {}).get("synchronous", {}).get("method") == "any"
            and spec.get("postgresql", {}).get("synchronous", {}).get("number") == 1
            and (spec.get("backup", {}).get("barmanObjectStore") is None)
            and spec.get("externalClusters") in (None, []),
            "CNPG Cluster readiness, image, system ID, or synchronous settings changed.")

    pods_obj = kjson(kubeconfig, "get", "pods", "-n", NAMESPACE)
    pods = pods_obj.get("items", [])
    active = [p for p in pods if p.get("status", {}).get("phase") not in ("Succeeded", "Failed")]
    require(len(active) == 2, "Unexpected active pod in repro namespace; writer/client removal is not proven.")
    by_instance = {p.get("metadata", {}).get("labels", {}).get("cnpg.io/instanceName"): p for p in active}
    require(set(by_instance) == {"hdd-pg-1", "hdd-pg-2"},
            "Only the owned primary and replica may be active in the repro namespace.")
    primary, replica = by_instance["hdd-pg-1"], by_instance["hdd-pg-2"]
    for name, pod in by_instance.items():
        require(pod.get("metadata", {}).get("labels", {}).get("cnpg.io/cluster") == CLUSTER
                and pod.get("metadata", {}).get("labels", {}).get("cnpg.io/instanceRole")
                == ("primary" if name == "hdd-pg-1" else "replica")
                and pod.get("status", {}).get("phase") == "Running"
                and any(c.get("type") == "Ready" and c.get("status") == "True"
                        for c in pod.get("status", {}).get("conditions", [])),
                f"CNPG instance {name} is not the expected ready owned pod.")
    primary_node = primary.get("spec", {}).get("nodeName")
    replica_node = replica.get("spec", {}).get("nodeName")
    require(primary_node and replica_node and primary_node != replica_node,
            "Primary and replica are not placed on distinct worker nodes.")
    nodes_obj = kjson(kubeconfig, "get", "nodes")
    nodes = nodes_obj.get("items", [])
    require(len(nodes) == 2, "AKS must expose exactly the two isolated measurement worker nodes.")
    node_map: dict[str, dict[str, Any]] = {}
    node_identity: dict[str, dict[str, str]] = {}
    for node in nodes:
        meta = node.get("metadata", {})
        name = meta.get("name")
        labels = meta.get("labels", {})
        require(name and labels.get("agentpool") == pool_name
                and labels.get("kubernetes.azure.com/agentpool") == pool_name
                and labels.get("topology.kubernetes.io/zone") == "eastus-1",
                f"AKS node {name!r} is outside the expected node pool/zone.")
        require(any(c.get("type") == "Ready" and c.get("status") == "True"
                    for c in node.get("status", {}).get("conditions", [])),
                f"AKS node {name} is not Ready.")
        identity = parse_provider_id(node.get("spec", {}).get("providerID", ""))
        require(identity["subscriptionId"].casefold() == sub.casefold()
                and identity["resourceGroup"].casefold() == node_rg.casefold()
                and identity["vmss"].casefold() == vmss_name.casefold()
                and identity["instanceId"] in instance_ids,
                f"AKS node {name} Azure identity is outside the owned VM scale set.")
        node_map[name] = node
        node_identity[name] = identity
    require({primary_node, replica_node} == set(node_map),
            "Primary and replica do not occupy both exact owned worker nodes.")

    jobs_obj = kjson(kubeconfig, "get", "jobs", "-n", NAMESPACE)
    jobs = jobs_obj.get("items", [])
    invalid_jobs = [j.get("metadata", {}).get("name") for j in jobs
                    if int((j.get("status") or {}).get("active") or 0) > 0
                    or int((j.get("status") or {}).get("succeeded") or 0) < 1
                    or j.get("metadata", {}).get("labels", {}).get("cnpg.io/cluster") != CLUSTER
                    or j.get("metadata", {}).get("labels", {}).get("cnpg.io/jobRole") != "snapshot-recovery"
                    or not any(ref.get("kind") == "Cluster" and ref.get("name") == CLUSTER
                               for ref in j.get("metadata", {}).get("ownerReferences", []))]
    require(not invalid_jobs, f"Unexpected or active Kubernetes Jobs remain in repro: {invalid_jobs}.")
    secret = kjson(kubeconfig, "get", "secret", SECRET, "-n", NAMESPACE, allow_not_found=True)
    require(secret is None, "WAL archive Secret remains in the measurement namespace.")

    # This reads only session/GUC status; it does not access or count fixture rows.
    sql = ("SELECT current_setting('synchronous_commit') || '|' || "
           "(SELECT count(*)::text FROM pg_stat_activity WHERE datname=current_database() "
           "AND usename='app' AND backend_type='client backend')")
    psql = ["kubectl", "exec", "-n", NAMESPACE, "-c", "postgres", primary["metadata"]["name"],
            "--", "psql", "-XAtq", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "repro", "-c", sql]
    activity = command(psql, kubeconfig=kubeconfig, timeout_s=45).strip()
    require(activity == "on|0", "Primary synchronous_commit/app client session check failed.")

    pvc_obj = kjson(kubeconfig, "get", "pvc", "-n", NAMESPACE, "-l", f"cnpg.io/cluster={CLUSTER}")
    pvc_by_name = {p.get("metadata", {}).get("name"): p for p in pvc_obj.get("items", [])}
    require(set(pvc_by_name) == set(DISK_CLAIMS), "PVC inventory is not the exact four DATA/WAL claims.")
    attachments_obj = kjson(kubeconfig, "get", "volumeattachments")
    attachments = attachments_obj.get("items", [])
    actual_disks: dict[str, dict[str, Any]] = {}
    for claim in DISK_CLAIMS:
        pvc = pvc_by_name[claim]
        require(pvc.get("status", {}).get("phase") == "Bound", f"PVC {claim} is not Bound.")
        pv_name = pvc.get("spec", {}).get("volumeName")
        require(bool(pv_name), f"PVC {claim} has no bound PV.")
        pv = kjson(kubeconfig, "get", "pv", pv_name)
        require(pv is not None and pv.get("status", {}).get("phase") == "Bound"
                and (pv.get("spec", {}).get("claimRef") or {}).get("namespace") == NAMESPACE
                and (pv.get("spec", {}).get("claimRef") or {}).get("name") == claim,
                f"PV for {claim} is not bound to the exact claim.")
        csi = pv.get("spec", {}).get("csi") or {}
        disk_id = csi.get("volumeHandle")
        require(csi.get("driver") == "disk.csi.azure.com"
                and disk_id and disk_id.casefold() == plan["diskRecords"][claim]["id"].casefold(),
                f"PV for {claim} does not bind the exact audited Azure disk ID.")
        disk = azjson(sub, "disk", "show", "--ids", disk_id)
        rec = plan["diskRecords"][claim]
        creation = disk.get("creationData") or {}
        try:
            percent = float(disk.get("completionPercent"))
        except (TypeError, ValueError):
            percent = None
        require(disk.get("id", "").casefold() == disk_id.casefold()
                and disk.get("resourceGroup", "").casefold() == node_rg.casefold()
                and disk.get("location", "").casefold() == "eastus"
                and disk.get("provisioningState") == "Succeeded"
                and (disk.get("sku") or {}).get("name") == "PremiumV2_LRS"
                and (disk.get("diskSizeGB") or disk.get("diskSizeGb")) == 512
                and disk.get("diskIOPSReadWrite") == 3000 and disk.get("diskMBpsReadWrite") == 125
                and percent == 100 and creation.get("createOption") in ("Copy", "CopyStart")
                and str(disk.get("logicalSectorSize") or creation.get("logicalSectorSize")) == "512"
                and "1" in [str(x) for x in (disk.get("zones") or [])]
                and creation.get("sourceResourceId", "").casefold() == str(rec.get("sourceResourceId", "")).casefold(),
                f"Live Azure disk for {claim} no longer matches the fully hydrated preparation receipt.")
        node = primary_node if claim.startswith("hdd-pg-1") else replica_node
        ident = node_identity[node]
        matching_attachments = [a for a in attachments
                                if (a.get("spec", {}).get("source") or {}).get("persistentVolumeName") == pv_name]
        require(len(matching_attachments) == 1
                and matching_attachments[0].get("spec", {}).get("attacher") == "disk.csi.azure.com"
                and matching_attachments[0].get("spec", {}).get("nodeName") == node
                and (matching_attachments[0].get("status") or {}).get("attached") is True,
                f"VolumeAttachment for {claim} is not attached to its exact primary/replica worker node.")
        managed_by = [disk.get("managedBy", ""), *(disk.get("managedByExtended") or [])]
        managed_by = [str(value).casefold() for value in managed_by if value]
        attachment = f"virtualmachinescalesets/{ident['vmss']}/virtualmachines/{ident['instanceId']}".casefold()
        require(not managed_by or any(attachment in owner or f"/virtualmachines/{node}".casefold() in owner
                                      for owner in managed_by),
                f"Azure disk for {claim} is not attached to its exact primary/replica worker node.")
        actual_disks[claim] = {"claim": claim, "diskId": disk_id, "pv": pv_name,
                               "completionPercent": percent, "provisioningState": disk.get("provisioningState"),
                               "createOption": creation.get("createOption"), "managedBy": managed_by,
                               "volumeAttachment": matching_attachments[0],
                               "sourceResourceId": creation.get("sourceResourceId")}

    existing = kjson(kubeconfig, "get", "pods", "-n", NAMESPACE,
                     "-l", f"vacuum-only-repro/cache-conditioning={token[:12]}")
    require(not existing.get("items"), "A cache-conditioning pod already exists; refusing adoption or deletion.")
    return {"azure": {"account": account, "aks": aks_live, "nodePool": pool,
                       "vmss": vmss, "vmssInstances": instances},
            "kubernetes": {"context": context, "namespaceUid": namespace["metadata"]["uid"],
                           "clusterUid": cluster["metadata"]["uid"], "clusterResourceVersion": cluster["metadata"].get("resourceVersion"),
                           "pods": {k: {"name": v["metadata"]["name"], "uid": v["metadata"]["uid"],
                                       "node": v["spec"]["nodeName"]} for k, v in
                                    (("primary", primary), ("replica", replica))},
                           "nodes": {k: {"uid": v["metadata"]["uid"], "providerID": v["spec"]["providerID"],
                                         "labels": v["metadata"].get("labels", {})} for k, v in node_map.items()},
                           "activityCheck": activity, "completedSnapshotJobs": [j.get("metadata", {}).get("name") for j in jobs],
                           "archiveSecretAbsent": True},
            "disks": actual_disks}


def pod_object(name: str, token: str, role: str, node: str) -> dict[str, Any]:
    command_text = (
        "printf 'cache_conditioning_begin_utc=%s\\n' \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"; "
        "sync; printf 'sync_complete_utc=%s\\n' \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"; "
        "printf '3\\n' > /host-vm/drop_caches; printf 'drop_caches=3\\n'; "
        "printf 'drop_caches_write_complete_utc=%s\\n' \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\""
    )
    return {
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "namespace": NAMESPACE,
                     "labels": {"repro.stack.token": token,
                                "vacuum-only-repro/cache-conditioning": token[:12],
                                "vacuum-only-repro/cache-conditioning-role": role},
                     "annotations": {"vacuum-only-repro/cache-conditioning-source-sha256": sha256(Path(__file__).resolve())}},
        "spec": {"nodeName": node, "restartPolicy": "Never", "activeDeadlineSeconds": 180,
                 "automountServiceAccountToken": False, "enableServiceLinks": False,
                 "hostPID": False, "hostIPC": False, "hostNetwork": False,
                 "containers": [{"name": "drop-caches", "image": EXPECTED_IMAGE,
                                 "imagePullPolicy": "IfNotPresent",
                                 "command": ["/bin/sh", "-ec", command_text],
                                 "securityContext": {"privileged": True, "runAsUser": 0},
                                 "resources": {"requests": {"cpu": "10m", "memory": "16Mi"},
                                               "limits": {"cpu": "100m", "memory": "64Mi"}},
                                 "volumeMounts": [{"name": "host-vm", "mountPath": "/host-vm", "readOnly": False}]}],
                 "volumes": [{"name": "host-vm", "hostPath": {"path": "/proc/sys/vm", "type": "Directory"}}]},
    }


def pod_json(name: str, role: str, node: str, token: str) -> dict[str, Any]:
    return pod_object(name, token, role, node)


def wait_and_capture(kubeconfig: Path, pod: dict[str, Any], token: str, timeout_s: int = 240) -> dict[str, Any]:
    meta = pod["metadata"]
    name, role, expected_node = meta["name"], meta["labels"]["vacuum-only-repro/cache-conditioning-role"], pod["spec"]["nodeName"]
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        live = kjson(kubeconfig, "get", "pod", name, "-n", NAMESPACE)
        require(live is not None and live.get("metadata", {}).get("labels", {}).get("repro.stack.token") == token
                and live.get("metadata", {}).get("labels", {}).get("vacuum-only-repro/cache-conditioning") == token[:12]
                and live.get("metadata", {}).get("labels", {}).get("vacuum-only-repro/cache-conditioning-role") == role
                and live.get("spec", {}).get("nodeName") == expected_node
                and live.get("spec", {}).get("containers", [{}])[0].get("image") == EXPECTED_IMAGE,
                f"Conditioning pod {name} identity or placement changed.")
        phase = live.get("status", {}).get("phase")
        if phase in ("Succeeded", "Failed"):
            container_status = (live.get("status", {}).get("containerStatuses") or [{}])[0]
            terminated = (container_status.get("state") or {}).get("terminated") or {}
            logs = command(["kubectl", "logs", "-n", NAMESPACE, name], kubeconfig=kubeconfig, timeout_s=60)
            require(phase == "Succeeded" and terminated.get("exitCode") == 0,
                    f"Conditioning pod {name} failed (phase={phase}, exit={terminated.get('exitCode')}).")
            require("sync_complete_utc=" in logs and "drop_caches=3" in logs
                    and "drop_caches_write_complete_utc=" in logs,
                    f"Conditioning pod {name} did not prove sync and cache-drop command completion.")
            return {"name": name, "uid": live.get("metadata", {}).get("uid"), "role": role,
                    "node": expected_node, "phase": phase, "exitCode": terminated.get("exitCode"),
                    "startedAt": terminated.get("startedAt"), "finishedAt": terminated.get("finishedAt"),
                    "logs": logs}
        time.sleep(2)
    raise Refusal(f"Conditioning pod {name} did not finish within {timeout_s} seconds.")


def delete_owned_pod(kubeconfig: Path, token: str, pod_record: dict[str, Any]) -> None:
    name, uid = pod_record.get("name"), pod_record.get("uid")
    live = kjson(kubeconfig, "get", "pod", name, "-n", NAMESPACE, allow_not_found=True)
    if live is None:
        return
    labels = live.get("metadata", {}).get("labels", {})
    require(live.get("metadata", {}).get("uid") == uid
            and labels.get("repro.stack.token") == token
            and labels.get("vacuum-only-repro/cache-conditioning") == token[:12]
            and labels.get("vacuum-only-repro/cache-conditioning-role") == pod_record.get("role"),
            f"Refusing to delete pod {name}: live UID/owner/role no longer matches our create receipt.")
    # kubectl delete does not expose UID preconditions. Recheck the exact UID
    # and owner labels immediately above, then delete this unique token-scoped
    # helper name and verify that no helper-label pod remains at the end.
    command(["kubectl", "delete", "pod", name, "-n", NAMESPACE,
             "--wait=true", "--timeout=120s"], kubeconfig=kubeconfig, timeout_s=150)
    remaining = kjson(kubeconfig, "get", "pods", "-n", NAMESPACE, "-l",
                      f"vacuum-only-repro/cache-conditioning={token[:12]}")
    require(not any(p.get("metadata", {}).get("uid") == uid for p in remaining.get("items", [])),
            f"Owned conditioning pod {name} remains after delete.")


def apply(plan: dict[str, Any]) -> None:
    token = plan["token"]
    kubeconfig = plan["kubeconfig"]
    RECOVERY.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(RECOVERY, 0o700)
    lock_handle = LOCK.open("a+")
    os.chmod(LOCK, 0o600)
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        raise Refusal("Another cache-conditioning helper is active.") from exc
    held_locks = []
    try:
        for lock_name in ("02-prepare.lock", "test.lock"):
            handle = (STATE / lock_name).open("a+")
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                handle.close()
                raise Refusal(f"Preparation or measurement is active ({lock_name}).") from exc
            held_locks.append(handle)

        before = validate_live(plan)
        before["verifiedAtUtc"] = utc_now()
        event("cache-conditioning-preflight-complete", "run", HERE.name,
              {"sourceSha256": plan["sourceSha256"], "diskClaims": list(DISK_CLAIMS),
               "primaryNode": before["kubernetes"]["pods"]["primary"]["node"],
               "replicaNode": before["kubernetes"]["pods"]["replica"]["node"]})
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S")
        pods = []
        for role, node in (("primary", before["kubernetes"]["pods"]["primary"]["node"]),
                           ("replica", before["kubernetes"]["pods"]["replica"]["node"])):
            name = f"cache-drop-{token[:8]}-{role}"
            pods.append(pod_json(name, role, node, token))
        intent = {"version": 1, "status": "running", "run": HERE.name,
                  "ownerToken": token, "subscriptionId": EXPECTED_SUBSCRIPTION,
                  "startedAtUtc": utc_now(), "sourceSha256": plan["sourceSha256"],
                  "image": EXPECTED_IMAGE,
                  "semantics": "On each exact PostgreSQL worker node, run sync then write 3 to host /proc/sys/vm/drop_caches; this clears Linux page cache, dentries, and inodes, not PostgreSQL shared_buffers.",
                  "hostPath": {"path": "/proc/sys/vm", "mountPath": "/host-vm", "type": "Directory"},
                  "before": before,
                  "pods": [{"name": p["metadata"]["name"], "role": p["metadata"]["labels"]["vacuum-only-repro/cache-conditioning-role"],
                            "node": p["spec"]["nodeName"], "uid": None} for p in pods]}
        save_json(RECEIPT, intent, exclusive=True)
        event("cache-conditioning-started", "run", HERE.name,
              {"pods": intent["pods"], "sourceSha256": plan["sourceSha256"]})

        captured: list[dict[str, Any]] = []
        failure: Exception | None = None
        try:
            listing = {"apiVersion": "v1", "kind": "List", "items": pods}
            command(["kubectl", "create", "-f", "-"], kubeconfig=kubeconfig,
                    timeout_s=120, stdin=json.dumps(listing))
            event("cache-conditioning-pods-created", "podList", "primary+replica", intent["pods"])
            for pod in pods:
                live = kjson(kubeconfig, "get", "pod", pod["metadata"]["name"], "-n", NAMESPACE)
                uid = live.get("metadata", {}).get("uid") if live else None
                require(bool(uid), f"Created conditioning pod {pod['metadata']['name']} has no UID.")
                for item in intent["pods"]:
                    if item["name"] == pod["metadata"]["name"]:
                        item["uid"] = uid
                save_json(RECEIPT, intent)
            for pod in pods:
                captured.append(wait_and_capture(kubeconfig, pod, token))
            event("cache-drop-commands-complete", "podList", "primary+replica",
                  {"finishedAtUtc": utc_now(), "podResults": captured})
        except Exception as exc:
            failure = exc
            event("cache-conditioning-failed", "run", HERE.name, {"error": str(exc), "podResults": captured})
        cleanup_errors = []
        by_name = {p["metadata"]["name"]: p for p in pods}
        for record in captured:
            try:
                delete_owned_pod(kubeconfig, token, record)
                event("cache-conditioning-pod-deleted", "pod", record["name"], {"uid": record["uid"]})
            except Exception as exc:
                cleanup_errors.append(f"{record.get('name')}: {exc}")
        # Clean up created pods which failed before successful log capture. Never
        # delete a pod unless its exact name, owner labels, and live UID match.
        for name, pod in by_name.items():
            if any(x.get("name") == name for x in captured):
                continue
            live = kjson(kubeconfig, "get", "pod", name, "-n", NAMESPACE, allow_not_found=True)
            if live is None:
                continue
            status = live.get("status", {})
            labels = live.get("metadata", {}).get("labels", {})
            uid = live.get("metadata", {}).get("uid")
            if (labels.get("repro.stack.token") == token
                    and labels.get("vacuum-only-repro/cache-conditioning") == token[:12]
                    and labels.get("vacuum-only-repro/cache-conditioning-role") == pod["metadata"]["labels"]["vacuum-only-repro/cache-conditioning-role"]
                    and live.get("spec", {}).get("nodeName") == pod["spec"]["nodeName"]
                    and uid):
                try:
                    try:
                        logs = command(["kubectl", "logs", "-n", NAMESPACE, name],
                                       kubeconfig=kubeconfig, timeout_s=30)
                        logs_error = None
                    except Exception as exc:
                        logs, logs_error = "", str(exc)
                    captured.append({"name": name, "uid": uid,
                                     "role": pod["metadata"]["labels"]["vacuum-only-repro/cache-conditioning-role"],
                                     "node": pod["spec"]["nodeName"], "phase": status.get("phase"),
                                     "logs": logs, "logsError": logs_error, "partial": True})
                    delete_owned_pod(kubeconfig, token, captured[-1])
                    event("cache-conditioning-pod-deleted", "pod", name,
                          {"uid": uid, "partial": True})
                except Exception as exc:
                    cleanup_errors.append(f"{name}: {exc}")
            else:
                cleanup_errors.append(f"{name}: live pod identity changed; left untouched")
        helper_pods = {"items": []}
        try:
            helper_pods = kjson(kubeconfig, "get", "pods", "-n", NAMESPACE,
                                "-l", f"vacuum-only-repro/cache-conditioning={token[:12]}")
            if helper_pods.get("items"):
                cleanup_errors.append("cache-conditioning pods remain after cleanup: " +
                                      ", ".join(p.get("metadata", {}).get("name", "?")
                                                for p in helper_pods["items"]))
        except Exception as exc:
            cleanup_errors.append(f"Could not verify helper pod removal: {exc}")
            helper_pods = {"items": None}
        intent["podResults"] = captured
        intent["cleanupErrors"] = cleanup_errors
        intent["finishedAtUtc"] = utc_now()
        remaining_pods = helper_pods.get("items") or []
        intent["after"] = {"verifiedAtUtc": utc_now(), "allHelperPodsDeleted": helper_pods.get("items") == [],
                            "remainingHelperPods": [p.get("metadata", {}).get("name") for p in remaining_pods]}
        successful_results = [p for p in captured if not p.get("partial")
                              and p.get("phase") == "Succeeded" and p.get("exitCode") == 0]
        intent["cacheDropSucceeded"] = failure is None and len(successful_results) == 2
        intent["status"] = "complete" if (intent["cacheDropSucceeded"] and not cleanup_errors) else "failed"
        save_json(RECEIPT, intent)
        if failure is not None:
            raise Refusal(f"Cache conditioning failed; preserved receipt at {RECEIPT}: {failure}") from failure
        if cleanup_errors:
            raise Refusal(f"Cache conditioning ran, but helper pod cleanup was incomplete: {cleanup_errors}; receipt: {RECEIPT}")
        require(intent["cacheDropSucceeded"], f"Cache-drop commands lack two complete successful pod results; receipt: {RECEIPT}.")
        print(json.dumps({"status": intent["status"], "receipt": str(RECEIPT),
                          "pods": [{"role": p["role"], "node": p["node"],
                                    "startedAt": p.get("startedAt"), "finishedAt": p.get("finishedAt")}
                                   for p in captured], "sourceSha256": plan["sourceSha256"]}, indent=2))
    finally:
        for handle in held_locks:
            handle.close()
        lock_handle.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="perform the guarded one-shot cache drop")
    parser.add_argument("--owner-token", help="exact owner token from this run's state.json")
    args = parser.parse_args()
    try:
        plan = local_plan(args.owner_token, args.apply)
        if not args.apply:
            print(json.dumps({"mode": "local-only dry-run", "azureCalls": 0,
                              "kubernetesCalls": 0, "run": HERE.name,
                              "ownerTokenMatched": args.owner_token == plan["token"] if args.owner_token else None,
                              "diskClaims": list(DISK_CLAIMS), "image": EXPECTED_IMAGE,
                              "hostPath": "/proc/sys/vm", "command": "sync; write 3 to drop_caches",
                              "applyCommand": f"python3 {Path(__file__).name} --apply --owner-token {plan['token']}"}, indent=2))
            return 0
        apply(plan)
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
