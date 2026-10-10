#!/usr/bin/env python3
"""Bypass a failed snapshot CreateVolume by prebinding Azure-created copies.

The default mode is local-only and does not contact Azure or Kubernetes. Apply
mode is deliberately separate: it revalidates the live Cluster, source
VolumeSnapshots/Contents, and the two exact Pending primary PVCs before each
Azure disk create and before each PVC binding change. It never deletes a disk,
snapshot, Kubernetes object, or source permission.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any


HERE = Path(__file__).resolve().parents[1]
RUN = HERE / "run"
STATE_DIR = RUN / ".state"
STATE_FILE = STATE_DIR / "state.json"
RUN_CONFIG = RUN / "run-config.json"
SOURCE_MANIFEST = RUN / "source-manifest.json"
SNAPSHOT_META = RUN / "snapshot.json"
RECOVERY_DIR = STATE_DIR / "recovery" / "pending-disk-restore"
RECEIPT = RECOVERY_DIR / "restore.json"
EVENTS = RECOVERY_DIR / "events.jsonl"
LOCK = RECOVERY_DIR / "restore.lock"
NAMESPACE = "repro"
CLUSTER = "hdd-pg"
STORAGE_CLASS = "azure-premiumv2"
TARGET_SNAPSHOT_END_LSN = "4E/50000138"


class GuardError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except Exception as exc:
        raise GuardError(f"Cannot read valid JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise GuardError(f"Expected a JSON object in {path}.")
    return value


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.chmod(temp, 0o600)
    os.replace(temp, path)


def record_event(phase: str, resource: str, name: str, details: dict[str, Any] | str = "") -> None:
    RECOVERY_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    entry = {"atUtc": utc_now(), "phase": phase, "resource": resource, "name": name, "details": details}
    with EVENTS.open("a") as f:
        f.write(json.dumps(entry, sort_keys=True, separators=(",", ":")) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.chmod(EVENTS, 0o600)


def save_evidence(phase: str, resource: str, name: str, raw: str) -> Path:
    RECOVERY_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    contents = raw if raw.endswith("\n") else raw + "\n"
    digest = hashlib.sha256(contents.encode()).hexdigest()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    safe_name = "".join(c if c.isalnum() or c in "-_." else "_" for c in name)[:120]
    path = RECOVERY_DIR / "evidence" / f"{stamp}-{phase}-{resource}-{safe_name}-{digest}.json"
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_text(contents)
    os.chmod(path, 0o600)
    record_event(phase, resource, name, {"sha256": digest, "file": str(path)})
    return path


def run_command(args: list[str], *, env: dict[str, str] | None = None, stdin: str | None = None,
                timeout: int = 180) -> str:
    try:
        p = subprocess.run(args, input=stdin, text=True, capture_output=True, env=env,
                           timeout=timeout, check=False)
    except subprocess.TimeoutExpired as exc:
        raise GuardError(f"Timed out running {args[0]} {args[1:3]}.") from exc
    if p.returncode != 0:
        stderr = p.stderr.strip().replace("\n", " ")[:1400]
        raise GuardError(f"Command failed ({p.returncode}): {args[0]} {args[1:3]}: {stderr}")
    return p.stdout


def parse_object(raw: str, what: str) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except Exception as exc:
        raise GuardError(f"{what} did not return valid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise GuardError(f"{what} did not return a JSON object.")
    return value


def lower(value: Any) -> str:
    return str(value or "").casefold()


def source_specs(manifest: dict[str, Any], stack_token: str) -> list[dict[str, str]]:
    data = manifest
    wal = manifest.get("pairedWAL")
    if not isinstance(wal, dict):
        raise GuardError("Pinned source manifest has no paired WAL snapshot.")
    specs = [
        {"role": "data", "claim": f"{CLUSTER}-1", "pvcRole": "PG_DATA",
         "vss": f"source-data-{stack_token[:8]}",
         "snapshotId": data.get("sourceSnapshotId", ""), "snapshotUid": data.get("uniqueId", "")},
        {"role": "wal", "claim": f"{CLUSTER}-1-wal", "pvcRole": "PG_WAL",
         "vss": f"source-wal-{stack_token[:8]}",
         "snapshotId": wal.get("sourceSnapshotId", ""), "snapshotUid": wal.get("uniqueId", "")},
    ]
    for item in specs:
        if not item["snapshotId"].startswith("/subscriptions/") or not item["snapshotUid"]:
            raise GuardError(f"Pinned {item['role']} snapshot identity is incomplete.")
    return specs


def load_local_plan(expected_token: str | None) -> dict[str, Any]:
    state = load_json(STATE_FILE)
    run_cfg = load_json(RUN_CONFIG)
    manifest = load_json(SOURCE_MANIFEST)
    meta = load_json(SNAPSHOT_META)
    token = state.get("ownerToken")
    if not isinstance(token, str) or len(token) != 32 or any(c not in "0123456789abcdef" for c in token):
        raise GuardError("Run state does not contain a lowercase 32-hex owner token.")
    if expected_token and expected_token != token:
        raise GuardError("--owner-token does not match the run state owner token.")
    if state.get("status") != "ready":
        raise GuardError("Run state is not ready.")
    if state.get("subscriptionId") != "00000000-0000-0000-0000-000000000000":
        raise GuardError("Run subscription differs from the pinned experiment subscription.")
    if (state.get("resourceGroup") != f"rg-vacuum-only-{token}" or
            state.get("nodeResourceGroup") != f"rg-vacuum-nodes-{token}" or
            state.get("requestedNodeResourceGroup") != state.get("nodeResourceGroup")):
        raise GuardError("Run resource groups do not match this stack's owner token.")
    if state.get("availabilityZone") != "1" or state.get("location", "eastus").casefold() != "eastus":
        raise GuardError("Run location/zone differs from pinned East US zone 1.")
    if not (run_cfg.get("targetHeapGiB") == 23 and run_cfg.get("diskSku") == "PremiumV2_LRS" and
            run_cfg.get("diskIops") == 3000 and run_cfg.get("diskMBps") == 125 and
            run_cfg.get("diskSizeGiB") == 512 and run_cfg.get("zone") == "1"):
        raise GuardError("Run configuration differs from the pinned 23-GiB PremiumV2 baseline.")
    if (manifest.get("ownerToken") == token or not manifest.get("ownerToken") or
            manifest.get("sourceSnapshotId") != meta.get("sourceSnapshotId") or
            manifest.get("pairedWAL", {}).get("sourceSnapshotId") != meta.get("pairedWALSnapshotId") or
            str(manifest.get("systemIdentifier")) != str(meta.get("systemIdentifier")) or
            manifest.get("backup", {}).get("endLSN") != TARGET_SNAPSHOT_END_LSN):
        raise GuardError("Run source manifest/snapshot metadata do not match the pinned paired fixture.")
    if manifest.get("backup", {}).get("uid") == "" or manifest.get("backup", {}).get("phase") != "completed":
        raise GuardError("Pinned CNPG source Backup identity is incomplete.")

    specs = source_specs(manifest, token)
    short = token[:8]
    sub = state["subscriptionId"]
    node_rg = state["nodeResourceGroup"]
    for item in specs:
        item["diskName"] = f"restore-{item['role']}-{short}"
        item["diskId"] = (f"/subscriptions/{sub}/resourceGroups/{node_rg}/providers/"
                          f"Microsoft.Compute/disks/{item['diskName']}")
        item["pvName"] = f"restore-{item['role']}-{short}"
        item["ownerToken"] = token
        item["sourceSnapshotUniqueId"] = item.pop("snapshotUid")
    return {"state": state, "manifest": manifest, "meta": meta, "specs": specs,
            "sourceOwnerToken": manifest.get("ownerToken"), "token": token,
            "short": short, "subscriptionId": sub, "nodeResourceGroup": node_rg,
            "location": "eastus", "zone": "1"}


def disk_command(plan: dict[str, Any], spec: dict[str, str]) -> list[str]:
    return ["az", "disk", "create", "--subscription", plan["subscriptionId"],
            "--resource-group", plan["nodeResourceGroup"], "--name", spec["diskName"],
            "--location", plan["location"], "--zone", plan["zone"],
            "--source", spec["snapshotId"], "--sku", "PremiumV2_LRS", "--size-gb", "512",
            "--disk-iops-read-write", "3000", "--disk-mbps-read-write", "125",
            "--logical-sector-size", "512", "--tags",
            f"reprotoken={plan['token']}", "reproPurpose=pgbench-baseline-source-restore",
            f"sourceSnapshotId={spec['snapshotId']}",
            f"sourceSnapshotUniqueId={spec['sourceSnapshotUniqueId']}",
            "--no-wait", "--only-show-errors", "--output", "json"]


def command_result_evidence(command: list[str], stdout: str) -> str:
    try:
        response: dict[str, Any] = {"response": json.loads(stdout)}
    except (json.JSONDecodeError, TypeError):
        response = {"stdout": stdout}
    return json.dumps({"command": command, "exitCode": 0, **response}, sort_keys=True)


def local_only_dry_run(plan: dict[str, Any]) -> None:
    print(json.dumps({
        "mode": "local-only dry-run",
        "azureCalls": 0,
        "kubernetesCalls": 0,
        "sourceManifestUnchanged": True,
        "sourceSnapshotIds": [x["snapshotId"] for x in plan["specs"]],
        "plannedDisks": [{"id": x["diskId"], "sourceSnapshotId": x["snapshotId"],
                          "sourceSnapshotUniqueId": x["sourceSnapshotUniqueId"],
                          "sku": "PremiumV2_LRS", "sizeGiB": 512, "iops": 3000,
                          "throughputMBps": 125, "sector": 512, "zone": "1",
                          "tags": {"reprotoken": plan["token"],
                                   "reproPurpose": "pgbench-baseline-source-restore"}}
                         for x in plan["specs"]],
        "plannedStaticPVs": [{"name": x["pvName"], "claim": f"{NAMESPACE}/{x['claim']}",
                              "sourceVolumeSnapshot": x["vss"], "reclaimPolicy": "Retain",
                              "zoneAffinity": "eastus-1"} for x in plan["specs"]],
        "applyCommand": f"{Path(__file__).name} --apply --owner-token {plan['token']}"
    }, indent=2))


class ApplyRun:
    def __init__(self, plan: dict[str, Any], wait_timeout: int):
        self.plan = plan
        self.wait_timeout = wait_timeout
        self.kubeconfig = plan["state"].get("kubeconfigPath") or str(STATE_DIR / "kubeconfig")
        if not os.path.isabs(self.kubeconfig):
            self.kubeconfig = str(RUN / self.kubeconfig)
        if not Path(self.kubeconfig).is_file():
            raise GuardError("Kubeconfig path from run state does not exist.")
        self.env = os.environ.copy()
        self.env["KUBECONFIG"] = self.kubeconfig
        RECOVERY_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(RECOVERY_DIR, 0o700)
        self.lockfile = LOCK.open("a+")
        os.chmod(LOCK, 0o600)
        try:
            fcntl.flock(self.lockfile.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise GuardError("Another pending-disk restore helper is active.") from exc
        self.receipt = self.load_or_create_receipt()

    def load_or_create_receipt(self) -> dict[str, Any]:
        token = self.plan["token"]
        if RECEIPT.exists():
            receipt = load_json(RECEIPT)
            if receipt.get("ownerToken") != token or receipt.get("status") not in ("intent", "partial", "ready"):
                raise GuardError("Existing fallback receipt belongs to a different run or has an unknown status.")
            saved_cluster = receipt.get("cluster", {})
            if saved_cluster.get("name") != CLUSTER or saved_cluster.get("namespace") != NAMESPACE:
                raise GuardError("Existing fallback receipt is not pinned to this Cluster.")
            return receipt
        receipt = {
            "version": 1, "status": "intent", "createdAtUtc": utc_now(),
            "ownerToken": token, "subscriptionId": self.plan["subscriptionId"],
            "nodeResourceGroup": self.plan["nodeResourceGroup"],
            "sourceBackup": {"name": self.plan["manifest"]["backup"]["name"],
                             "uid": self.plan["manifest"]["backup"]["uid"],
                             "endLSN": self.plan["manifest"]["backup"]["endLSN"]},
            "cluster": {"namespace": NAMESPACE, "name": CLUSTER, "uid": None},
            "sourceSnapshots": [], "restoreDisks": [], "staticPVs": [],
            "warning": "Operator-created disks directly copy the pinned snapshot pair; source manifest and original VolumeSnapshot bindings remain unchanged."
        }
        for spec in self.plan["specs"]:
            receipt["restoreDisks"].append({
                "role": spec["role"], "name": spec["diskName"], "id": spec["diskId"],
                "sourceSnapshotId": spec["snapshotId"],
                "sourceSnapshotUniqueId": spec["sourceSnapshotUniqueId"],
                "pvc": f"{NAMESPACE}/{spec['claim']}", "pvcUid": None,
                "pvName": spec["pvName"], "status": "intent"
            })
            receipt["staticPVs"].append({
                "name": spec["pvName"], "role": spec["role"],
                "diskId": spec["diskId"], "diskUniqueId": None,
                "pvc": f"{NAMESPACE}/{spec['claim']}", "pvcUid": None,
                "uid": None, "status": "intent", "preexistingAbsent": None
            })
        atomic_json(RECEIPT, receipt)
        record_event("restore-intents-written", "receipt", str(RECEIPT),
                     {"ownerToken": token, "plannedDisks": [x["id"] for x in receipt["restoreDisks"]]})
        return receipt

    def write_receipt(self) -> None:
        self.receipt["updatedAtUtc"] = utc_now()
        atomic_json(RECEIPT, self.receipt)

    def kubectl_json(self, args: list[str], label: str, timeout_s: int = 120) -> dict[str, Any]:
        raw = run_command(["kubectl", *args, "-o", "json"], env=self.env, timeout=timeout_s)
        save_evidence("live-check", "kubernetes", label, raw)
        return parse_object(raw, label)

    def get_kube_object(self, args: list[str], label: str) -> dict[str, Any] | None:
        cmd = ["kubectl", *args, "-o", "json"]
        p = subprocess.run(cmd, text=True, capture_output=True, env=self.env, timeout=120, check=False)
        if p.returncode != 0:
            if "NotFound" in p.stderr or "not found" in p.stderr.casefold():
                return None
            raise GuardError(f"Cannot read {label}: {p.stderr.strip().replace(chr(10), ' ')[:1000]}")
        save_evidence("live-check", "kubernetes", label, p.stdout)
        return parse_object(p.stdout, label)

    def cluster_live(self, *, allow_recovered_primary: bool = False) -> dict[str, Any]:
        cluster = self.kubectl_json(["get", "cluster", CLUSTER, "-n", NAMESPACE], "cluster")
        md = cluster.get("metadata", {})
        anns = md.get("annotations", {})
        manifest = self.plan["manifest"]
        if md.get("labels", {}).get("repro.stack.token") != self.plan["token"]:
            raise GuardError("Live Cluster is not labeled for this owner token.")
        if (anns.get("vacuum-only-repro/source-data-snapshot-id") != manifest["sourceSnapshotId"] or
                anns.get("vacuum-only-repro/source-wal-snapshot-id") != manifest["pairedWAL"]["sourceSnapshotId"] or
                anns.get("vacuum-only-repro/source-backup-uid") != manifest["backup"]["uid"] or
                anns.get("vacuum-only-repro/source-backup-end-lsn") != manifest["backup"]["endLSN"]):
            raise GuardError("Live Cluster source annotations differ from the pinned paired Backup/source manifest.")
        if cluster.get("spec", {}).get("instances") != 1:
            raise GuardError("Live Cluster is no longer the one-primary recovery target.")
        ready_instances = cluster.get("status", {}).get("readyInstances", 0)
        if ready_instances not in (0, None):
            if (not allow_recovered_primary or ready_instances != 1 or
                    cluster.get("status", {}).get("currentPrimary") != f"{CLUSTER}-1"):
                raise GuardError("The primary became ready outside the guarded static-PV binding stage.")
        system_id = cluster.get("status", {}).get("systemID")
        if system_id and str(system_id) != str(self.plan["meta"]["systemIdentifier"]):
            raise GuardError("Live Cluster system ID differs from the pinned fixture.")
        saved_uid = self.receipt.get("cluster", {}).get("uid")
        if saved_uid and saved_uid != md.get("uid"):
            raise GuardError("Live Cluster UID changed after fallback intent was recorded.")
        if not md.get("uid"):
            raise GuardError("Live Cluster has no UID.")
        self.receipt["cluster"] = {"namespace": NAMESPACE, "name": CLUSTER, "uid": md["uid"]}
        self.write_receipt()
        return cluster

    def live_snapshots_and_pvcs(self, cluster: dict[str, Any], *,
                                allow_expected_bound: bool = False,
                                require_all_bound: bool = False) -> list[dict[str, Any]]:
        node_json = self.kubectl_json(["get", "nodes"], "nodes")
        expected_zone = f"{self.plan['location']}-{self.plan['zone']}"
        ready_nodes = [n for n in node_json.get("items", [])
                       if n.get("metadata", {}).get("labels", {}).get("topology.disk.csi.azure.com/zone") == expected_zone]
        if not ready_nodes:
            raise GuardError(f"No AKS node advertises the expected Azure disk zone {expected_zone}.")

        specs = self.plan["specs"]
        pvc_objects = []
        snapshot_records = []
        for spec in specs:
            vss = self.kubectl_json(["get", "volumesnapshot", spec["vss"], "-n", NAMESPACE], f"{spec['role']}-source-vss")
            vss_md = vss.get("metadata", {})
            vss_anns = vss_md.get("annotations", {})
            if vss_md.get("labels", {}).get("repro.stack.token") != self.plan["token"]:
                raise GuardError(f"{spec['role']} source VolumeSnapshot has wrong owner token.")
            if (vss_anns.get("vacuum-only-repro/source-snapshot-id") != spec["snapshotId"] or
                    vss_anns.get("vacuum-only-repro/source-snapshot-unique-id") != spec["sourceSnapshotUniqueId"] or
                    vss_anns.get("vacuum-only-repro/source-backup-uid") != self.plan["manifest"]["backup"]["uid"]):
                raise GuardError(f"{spec['role']} source VolumeSnapshot identity/backup annotations changed.")
            content_name = vss.get("status", {}).get("boundVolumeSnapshotContentName")
            if vss.get("status", {}).get("readyToUse") is not True or not content_name:
                raise GuardError(f"{spec['role']} source VolumeSnapshot is not ready and bound.")
            vsc = self.kubectl_json(["get", "volumesnapshotcontent", content_name], f"{spec['role']}-source-vsc")
            vsc_spec = vsc.get("spec", {})
            vsc_status = vsc.get("status", {})
            content_id = vsc_spec.get("source", {}).get("snapshotHandle")
            status_id = vsc_status.get("snapshotHandle") or content_id
            if (vsc_spec.get("deletionPolicy") != "Retain" or
                    lower(content_id) != lower(spec["snapshotId"]) or
                    lower(status_id) != lower(spec["snapshotId"]) or
                    vsc.get("metadata", {}).get("labels", {}).get("repro.stack.token") != self.plan["token"] or
                    vsc.get("metadata", {}).get("annotations", {}).get("vacuum-only-repro/source-backup-uid") != self.plan["manifest"]["backup"]["uid"]):
                raise GuardError(f"{spec['role']} source VolumeSnapshotContent is not the exact retained source binding.")
            snapshot_records.append({"role": spec["role"], "snapshotId": spec["snapshotId"],
                                    "snapshotUniqueId": spec["sourceSnapshotUniqueId"],
                                    "volumeSnapshot": spec["vss"], "volumeSnapshotUid": vss_md.get("uid"),
                                    "volumeSnapshotContent": content_name,
                                    "volumeSnapshotContentUid": vsc.get("metadata", {}).get("uid")})

            pvc = self.kubectl_json(["get", "pvc", spec["claim"], "-n", NAMESPACE], f"{spec['role']}-pvc")
            md = pvc.get("metadata", {})
            labels = md.get("labels", {})
            ds = pvc.get("spec", {}).get("dataSource", {})
            owners = md.get("ownerReferences", [])
            owner_cluster_ref = any(o.get("kind") == "Cluster" and o.get("uid") == cluster["metadata"]["uid"]
                                    for o in owners)
            if (labels.get("cnpg.io/cluster") != CLUSTER or
                    labels.get("cnpg.io/instanceName") != f"{CLUSTER}-1" or
                    labels.get("cnpg.io/pvcRole") != spec["pvcRole"]):
                raise GuardError(f"PVC {spec['claim']} CNPG cluster/instance/role labels do not match.")
            # CNPG versions do not consistently set a Cluster ownerReference on
            # volume claims; when present, it must be the current Cluster UID.
            if owners and not owner_cluster_ref:
                raise GuardError(f"PVC {spec['claim']} has ownerReferences that do not include the current Cluster UID.")
            volume_name = pvc.get("spec", {}).get("volumeName", "")
            phase = pvc.get("status", {}).get("phase")
            if allow_expected_bound and volume_name == spec["pvName"]:
                if phase not in ("Pending", "Bound"):
                    raise GuardError(f"PVC {spec['claim']} has unexpected phase {phase!r} on the helper PV.")
                self.validate_receipted_pv(spec, pvc)
            elif phase != "Pending" or volume_name:
                raise GuardError(f"PVC {spec['claim']} is no longer Pending and unbound to this helper's PV.")
            if require_all_bound and (phase != "Bound" or volume_name != spec["pvName"]):
                raise GuardError(f"Ready primary is not using the exact receipted {spec['role']} static PV.")
            if (ds.get("kind") != "VolumeSnapshot" or ds.get("name") != spec["vss"] or
                    ds.get("apiGroup") != "snapshot.storage.k8s.io"):
                raise GuardError(f"PVC {spec['claim']} dataSource no longer names the pinned source VolumeSnapshot.")
            pvc_objects.append(pvc)

            existing_pvs = self.kubectl_json(["get", "pv"], f"pvs-before-{spec['role']}").get("items", [])
            for pv in existing_pvs:
                ref = pv.get("spec", {}).get("claimRef", {})
                if (ref.get("namespace"), ref.get("name"), ref.get("uid")) == (NAMESPACE, spec["claim"], md.get("uid")):
                    expected = self.receipt_disk(spec["role"]).get("id")
                    actual = pv.get("spec", {}).get("csi", {}).get("volumeHandle")
                    if pv.get("metadata", {}).get("name") != spec["pvName"] or (expected and lower(actual) != lower(expected)):
                        raise GuardError(f"PVC {spec['claim']} already has a PV other than this helper's exact planned PV.")
        self.receipt["sourceSnapshots"] = snapshot_records
        for spec, pvc in zip(specs, pvc_objects):
            record = self.receipt_disk(spec["role"])
            uid = pvc["metadata"]["uid"]
            if record.get("pvcUid") and record["pvcUid"] != uid:
                raise GuardError(f"PVC {spec['claim']} UID changed after disk intent was recorded.")
            record["pvcUid"] = uid
            record["dataSourceName"] = spec["vss"]
            record["pvcResourceVersion"] = pvc["metadata"].get("resourceVersion")
        self.write_receipt()
        return pvc_objects

    def receipt_disk(self, role: str) -> dict[str, Any]:
        found = [x for x in self.receipt["restoreDisks"] if x.get("role") == role]
        if len(found) != 1:
            raise GuardError(f"Fallback receipt must contain exactly one {role} disk intent.")
        return found[0]

    def receipt_pv(self, role: str) -> dict[str, Any]:
        found = [x for x in self.receipt["staticPVs"] if x.get("role") == role]
        if len(found) != 1:
            raise GuardError(f"Fallback receipt must contain exactly one {role} static-PV intent.")
        return found[0]

    def disk_record_from_id(self, disk_id: str) -> dict[str, str]:
        return {"id": disk_id}

    def validate_receipted_pv(self, spec: dict[str, str], pvc: dict[str, Any]) -> dict[str, Any]:
        rec = self.receipt_pv(spec["role"])
        disk_rec = self.receipt_disk(spec["role"])
        if (rec.get("name") != spec["pvName"] or rec.get("diskId") != spec["diskId"] or
                rec.get("pvc") != f"{NAMESPACE}/{spec['claim']}" or
                rec.get("pvcUid") != pvc.get("metadata", {}).get("uid") or
                not rec.get("preexistingAbsent")):
            raise GuardError(f"PVC {spec['claim']} is bound to a PV without this run's saved create intent.")
        pv = self.kubectl_json(["get", "pv", spec["pvName"]], f"{spec['role']}-receipted-pv")
        if rec.get("uid") and rec["uid"] != pv.get("metadata", {}).get("uid"):
            raise GuardError(f"PV {spec['pvName']} UID differs from this run's saved PV intent.")
        if rec.get("diskUniqueId") and rec["diskUniqueId"] != disk_rec.get("diskUniqueId"):
            raise GuardError(f"PV {spec['pvName']} disk uniqueId differs from this run's receipt.")
        self.validate_pv(pv, spec, pvc, self.disk_record_from_id(disk_rec["id"]))
        if not rec.get("uid"):
            rec["uid"] = pv.get("metadata", {}).get("uid")
            rec["resourceVersion"] = pv.get("metadata", {}).get("resourceVersion")
            rec["diskUniqueId"] = disk_rec.get("diskUniqueId")
            rec["status"] = pv.get("status", {}).get("phase", "Pending")
            self.write_receipt()
        return pv

    def check_source_snapshot(self, spec: dict[str, str]) -> dict[str, Any]:
        raw = run_command(["az", "snapshot", "show", "--subscription", self.plan["subscriptionId"],
                           "--ids", spec["snapshotId"], "--only-show-errors", "--output", "json"], timeout=180)
        snapshot = parse_object(raw, f"{spec['role']} source snapshot")
        save_evidence("source-snapshot-revalidated", "azureSnapshot", spec["role"], raw)
        if (lower(snapshot.get("id")) != lower(spec["snapshotId"]) or
                snapshot.get("uniqueId") != spec["sourceSnapshotUniqueId"] or
                snapshot.get("provisioningState") != "Succeeded" or
                snapshot.get("snapshotAccessState") != "Available" or
                snapshot.get("tags", {}).get("reprotoken") != self.plan["sourceOwnerToken"] or
                (snapshot.get("diskSizeGB") or snapshot.get("diskSizeGb")) != 512 or
                snapshot.get("incremental") is not True or snapshot.get("sku", {}).get("name") != "Standard_ZRS"):
            raise GuardError(f"{spec['role']} source snapshot does not match pinned ID/uniqueId/owner/readiness/size.")
        return snapshot

    def source_direct_locks(self, spec: dict[str, str]) -> list[dict[str, Any]]:
        source_rg = self.plan["manifest"].get("resourceGroup")
        raw = run_command(["az", "lock", "list", "--subscription", self.plan["subscriptionId"],
                           "--resource-group", source_rg, "--only-show-errors", "--output", "json"],
                          timeout=180)
        save_evidence("source-locks-revalidated", "azureLocks", spec["role"], raw)
        try:
            locks = json.loads(raw)
        except Exception as exc:
            raise GuardError(f"{spec['role']} source snapshot lock list was invalid JSON: {exc}") from exc
        if not isinstance(locks, list):
            raise GuardError(f"{spec['role']} source snapshot lock list was not an array.")
        direct = []
        for lock in locks:
            scope = lock.get("scope") or lock.get("id", "").split("/providers/Microsoft.Authorization/locks/", 1)[0]
            if lower(scope) == lower(spec["snapshotId"]):
                direct.append({k: lock.get(k) for k in ("id", "name", "level", "scope", "notes")})
        if not direct or any(x.get("level") != "CanNotDelete" for x in direct):
            raise GuardError(f"{spec['role']} source snapshot lost its direct CanNotDelete lock.")
        return sorted(direct, key=lambda x: lower(x.get("id")))

    def kubernetes_preflight(self) -> list[dict[str, Any]]:
        cluster = self.cluster_live()
        pvcs = self.live_snapshots_and_pvcs(cluster)
        return pvcs

    def kubernetes_bind_preflight(self) -> list[dict[str, Any]]:
        cluster = self.cluster_live(allow_recovered_primary=True)
        ready = cluster.get("status", {}).get("readyInstances", 0) == 1
        return self.live_snapshots_and_pvcs(cluster, allow_expected_bound=True,
                                            require_all_bound=ready)

    def check_node_resource_group(self) -> dict[str, Any]:
        expected_id = (f"/subscriptions/{self.plan['subscriptionId']}/resourceGroups/"
                       f"{self.plan['nodeResourceGroup']}")
        raw = run_command(["az", "group", "show", "--subscription", self.plan["subscriptionId"],
                           "--name", self.plan["nodeResourceGroup"], "--only-show-errors", "--output", "json"],
                          timeout=180)
        group = parse_object(raw, "owned AKS node resource group")
        save_evidence("node-resource-group-revalidated", "azureResourceGroup", self.plan["nodeResourceGroup"], raw)
        if (lower(group.get("id")) != lower(expected_id) or
                lower(group.get("name")) != lower(self.plan["nodeResourceGroup"]) or
                lower(group.get("location")) != self.plan["location"] or
                group.get("tags", {}).get("reprotoken") != self.plan["token"]):
            raise GuardError("AKS node resource group identity/location/owner token changed.")
        return group

    def check_disk(self, disk: dict[str, Any], spec: dict[str, str]) -> None:
        disk_id = disk.get("id", "")
        props = disk.get("properties", {})
        creation = disk.get("creationData", props.get("creationData", {}))
        sku = disk.get("sku", {})
        size = disk.get("diskSizeGB", props.get("diskSizeGB", disk.get("diskSizeGb")))
        percent = disk.get("completionPercent", props.get("completionPercent"))
        actual_rg = disk.get("resourceGroup")
        id_rg = disk_id.split("/resourceGroups/", 1)[1].split("/", 1)[0] if "/resourceGroups/" in disk_id else ""
        if (lower(disk_id) != lower(spec["diskId"]) or
                (actual_rg is not None and lower(actual_rg) != lower(self.plan["nodeResourceGroup"])) or
                lower(id_rg) != lower(self.plan["nodeResourceGroup"]) or
                lower(disk.get("location")) != self.plan["location"] or
                sku.get("name") != "PremiumV2_LRS" or size != 512 or
                disk.get("diskIOPSReadWrite", props.get("diskIOPSReadWrite")) != 3000 or
                disk.get("diskMBpsReadWrite", props.get("diskMBpsReadWrite")) != 125 or
                disk.get("provisioningState", props.get("provisioningState")) == "Failed" or
                disk.get("tags", {}).get("reprotoken") != self.plan["token"] or
                disk.get("tags", {}).get("reproPurpose") != "pgbench-baseline-source-restore" or
                disk.get("tags", {}).get("sourceSnapshotId") != spec["snapshotId"] or
                disk.get("tags", {}).get("sourceSnapshotUniqueId") != spec["sourceSnapshotUniqueId"] or
                lower(creation.get("sourceResourceId")) != lower(spec["snapshotId"])):
            raise GuardError(f"Azure disk {spec['diskId']} does not match the exact owned source-copy identity/parameters.")
        source_uid = creation.get("sourceUniqueId")
        if source_uid and source_uid != spec["sourceSnapshotUniqueId"]:
            raise GuardError(f"Azure disk {spec['diskId']} sourceUniqueId differs from the pinned snapshot UID.")
        disk_uid = disk.get("uniqueId") or props.get("uniqueId")
        if not disk_uid:
            raise GuardError(f"Azure disk {spec['diskId']} lacks its uniqueId.")
        zones = [str(x) for x in disk.get("zones", props.get("zones", []))]
        if self.plan["zone"] not in zones:
            raise GuardError(f"Azure disk {spec['diskId']} is not in availability zone {self.plan['zone']}.")
        option = creation.get("createOption")
        if option not in ("Copy", "CopyStart"):
            raise GuardError(f"Azure disk {spec['diskId']} has unsupported createOption={option!r}.")
        if percent is not None and (not isinstance(percent, (float, int)) or isinstance(percent, bool)):
            raise GuardError(f"Azure disk {spec['diskId']} completionPercent is not numeric.")
        sector = disk.get("logicalSectorSize", props.get("logicalSectorSize", creation.get("logicalSectorSize")))
        if sector is None or str(sector) != "512":
            raise GuardError(f"Azure disk {spec['diskId']} logical sector size differs from the pinned 512-byte contract.")

    def show_disk(self, spec: dict[str, str]) -> dict[str, Any] | None:
        p = subprocess.run(["az", "disk", "show", "--subscription", self.plan["subscriptionId"],
                            "--ids", spec["diskId"], "--only-show-errors", "--output", "json"],
                           text=True, capture_output=True, timeout=180, check=False)
        if p.returncode != 0:
            text = p.stderr.casefold()
            if "resourcenotfound" in text or "could not be found" in text or "not found" in text:
                return None
            raise GuardError(f"Cannot inspect planned Azure disk {spec['diskId']}: {p.stderr.strip().replace(chr(10), ' ')[:1000]}")
        save_evidence("disk-inspected", "azureDisk", spec["role"], p.stdout)
        disk = parse_object(p.stdout, f"{spec['role']} disk show")
        self.check_disk(disk, spec)
        return disk

    def create_or_reuse_disk(self, spec: dict[str, str]) -> dict[str, Any]:
        # A second live check at the last safe point avoids allocating a disk
        # after CSI has successfully bound its pending source PVC.
        self.kubernetes_preflight()
        self.check_node_resource_group()
        source_before = self.check_source_snapshot(spec)
        locks_before = self.source_direct_locks(spec)
        record = self.receipt_disk(spec["role"])
        previous_lock_record = record.get("sourceDirectLocks")
        if previous_lock_record and previous_lock_record != locks_before:
            raise GuardError(f"{spec['role']} source snapshot direct lock set changed since the recorded intent.")
        record["sourceSnapshotUniqueIdBeforeCopy"] = source_before.get("uniqueId")
        record["sourceDirectLocks"] = locks_before
        self.write_receipt()
        disk = self.show_disk(spec)
        if disk is None:
            record_event("disk-create-intent", "azureDisk", spec["diskId"],
                         {"sourceSnapshotId": spec["snapshotId"],
                          "sourceSnapshotUniqueId": spec["sourceSnapshotUniqueId"],
                          "sku": "PremiumV2_LRS", "sizeGiB": 512, "iops": 3000,
                          "throughputMBps": 125, "zone": self.plan["zone"]})
            # Persist the exact planned ARM ID/name and pinned source identity
            # before Azure accepts a possible long-running operation.
            self.check_node_resource_group()
            self.receipt_disk(spec["role"])["status"] = "create-intent"
            self.write_receipt()
            command = disk_command(self.plan, spec)
            create_raw = run_command(command, timeout=1200)
            save_evidence("disk-create-submitted", "azureDisk", spec["role"],
                          command_result_evidence(command, create_raw))
            # The --no-wait response may omit hydrated-resource fields; the
            # following disk show is the authoritative identity check.
            self.receipt_disk(spec["role"])["status"] = "copy-submitted"
            self.write_receipt()
        else:
            record_event("existing-planned-disk-validated", "azureDisk", spec["diskId"],
                         "accepted only after token, purpose, exact source ID/UID, SKU/performance, zone, and uniqueId checks")
            self.receipt_disk(spec["role"])["status"] = "existing-validated"
            self.write_receipt()

        deadline = time.monotonic() + self.wait_timeout
        last_sig = None
        while True:
            disk = self.show_disk(spec)
            if disk is None:
                raise GuardError(f"Planned disk {spec['diskId']} disappeared; no deletion will be attempted.")
            props = disk.get("properties", {})
            percent = disk.get("completionPercent", props.get("completionPercent"))
            state = disk.get("provisioningState", props.get("provisioningState"))
            option = disk.get("creationData", props.get("creationData", {})).get("createOption")
            sig = (state, percent, option)
            if sig != last_sig:
                self.receipt_disk(spec["role"])["lastHydrationPoll"] = {
                    "atUtc": utc_now(), "provisioningState": state,
                    "completionPercent": percent, "createOption": option}
                self.write_receipt()
                record_event("disk-hydration-progress", "azureDisk", spec["diskId"],
                             {"provisioningState": state, "completionPercent": percent,
                              "createOption": option})
                last_sig = sig
            if state == "Failed":
                raise GuardError(f"Planned disk {spec['diskId']} reached Failed; preserve it and inspect evidence.")
            if state == "Succeeded" and isinstance(percent, (float, int)) and not isinstance(percent, bool) and float(percent) == 100.0:
                break
            if time.monotonic() >= deadline:
                raise GuardError(f"Disk hydration did not reach numeric 100 percent before timeout: {spec['diskId']}.")
            time.sleep(20)
        self.receipt_disk(spec["role"])["status"] = "hydrated-100"
        self.receipt_disk(spec["role"])["diskUniqueId"] = disk.get("uniqueId") or disk.get("properties", {}).get("uniqueId")
        self.receipt_disk(spec["role"])["azureDiskMetadata"] = {
            "id": disk.get("id"), "uniqueId": self.receipt_disk(spec["role"])["diskUniqueId"],
            "location": disk.get("location"), "zones": disk.get("zones"),
            "sku": disk.get("sku", {}).get("name"),
            "diskSizeGiB": disk.get("diskSizeGB", disk.get("diskSizeGb")),
            "diskIOPSReadWrite": disk.get("diskIOPSReadWrite"),
            "diskMBpsReadWrite": disk.get("diskMBpsReadWrite"),
            "completionPercent": disk.get("completionPercent"),
            "creationData": disk.get("creationData")}
        self.write_receipt()
        source_after = self.check_source_snapshot(spec)
        locks_after = self.source_direct_locks(spec)
        if (source_after.get("uniqueId") != record.get("sourceSnapshotUniqueIdBeforeCopy") or
                locks_after != locks_before):
            raise GuardError(f"{spec['role']} source snapshot identity or direct retention lock changed during disk copy.")
        self.receipt_disk(spec["role"])["sourceSnapshotUniqueIdAfterCopy"] = source_after.get("uniqueId")
        self.receipt_disk(spec["role"])["sourceDirectLocksAfterCopy"] = locks_after
        self.receipt_disk(spec["role"])["creationDataSourceUniqueId"] = (
            disk.get("creationData", {}).get("sourceUniqueId") or
            disk.get("properties", {}).get("creationData", {}).get("sourceUniqueId"))
        self.write_receipt()
        record_event("disk-copy-hydrated", "azureDisk", spec["diskId"],
                     {"sourceSnapshotId": spec["snapshotId"], "sourceSnapshotUniqueId": spec["sourceSnapshotUniqueId"],
                      "diskUniqueId": self.receipt_disk(spec["role"])["diskUniqueId"],
                      "completionPercent": 100})
        return disk

    def pv_object(self, spec: dict[str, str], pvc: dict[str, Any], disk: dict[str, Any]) -> dict[str, Any]:
        return {
            "apiVersion": "v1", "kind": "PersistentVolume",
            "metadata": {"name": spec["pvName"],
                         "labels": {"repro.stack.token": self.plan["token"],
                                    "vacuum-only-repro/source-role": spec["role"]},
                         "annotations": {"pv.kubernetes.io/provisioned-by": "disk.csi.azure.com",
                                         "vacuum-only-repro/source-snapshot-id": spec["snapshotId"],
                                         "vacuum-only-repro/source-snapshot-unique-id": spec["sourceSnapshotUniqueId"],
                                         "vacuum-only-repro/restore-disk-unique-id": self.receipt_disk(spec["role"])["diskUniqueId"]}},
            "spec": {"capacity": {"storage": "512Gi"}, "accessModes": ["ReadWriteOnce"],
                     "persistentVolumeReclaimPolicy": "Retain", "storageClassName": STORAGE_CLASS,
                     "volumeMode": "Filesystem",
                     "claimRef": {"apiVersion": "v1", "kind": "PersistentVolumeClaim",
                                  "namespace": NAMESPACE, "name": spec["claim"],
                                  "uid": pvc["metadata"]["uid"]},
                     "csi": {"driver": "disk.csi.azure.com", "volumeHandle": disk["id"],
                             "volumeAttributes": {"fsType": "ext4", "cachingMode": "None",
                                                  "kind": "Managed", "skuName": "PremiumV2_LRS",
                                                  "DiskIOPSReadWrite": "3000", "DiskMBpsReadWrite": "125",
                                                  "LogicalSectorSize": "512"}},
                     "nodeAffinity": {"required": {"nodeSelectorTerms": [{"matchExpressions": [
                         {"key": "topology.disk.csi.azure.com/zone", "operator": "In",
                          "values": [f"{self.plan['location']}-{self.plan['zone']}"]}]}]}}}}

    def validate_pv(self, pv: dict[str, Any], spec: dict[str, str], pvc: dict[str, Any], disk: dict[str, Any]) -> None:
        md = pv.get("metadata", {})
        body = pv.get("spec", {})
        ref = body.get("claimRef", {})
        csi = body.get("csi", {})
        annotations = md.get("annotations", {})
        zone = f"{self.plan['location']}-{self.plan['zone']}"
        zone_matches = any(
            expression.get("key") == "topology.disk.csi.azure.com/zone" and
            expression.get("operator") == "In" and zone in expression.get("values", [])
            for term in body.get("nodeAffinity", {}).get("required", {}).get("nodeSelectorTerms", [])
            for expression in term.get("matchExpressions", []))
        if (md.get("name") != spec["pvName"] or
                md.get("labels", {}).get("repro.stack.token") != self.plan["token"] or
                md.get("labels", {}).get("vacuum-only-repro/source-role") != spec["role"] or
                annotations.get("vacuum-only-repro/source-snapshot-id") != spec["snapshotId"] or
                annotations.get("vacuum-only-repro/source-snapshot-unique-id") != spec["sourceSnapshotUniqueId"] or
                annotations.get("vacuum-only-repro/restore-disk-unique-id") != self.receipt_disk(spec["role"]).get("diskUniqueId") or
                ref.get("namespace") != NAMESPACE or ref.get("name") != spec["claim"] or
                ref.get("uid") != pvc["metadata"]["uid"] or
                lower(csi.get("volumeHandle")) != lower(disk["id"]) or
                csi.get("driver") != "disk.csi.azure.com" or
                body.get("persistentVolumeReclaimPolicy") != "Retain" or
                body.get("storageClassName") != STORAGE_CLASS or
                body.get("capacity", {}).get("storage") != "512Gi" or
                body.get("accessModes") != ["ReadWriteOnce"] or body.get("volumeMode") != "Filesystem" or
                not zone_matches):
            raise GuardError(f"PV {spec['pvName']} does not match the exact prebound owned disk/PVC/zone contract.")
        attrs = csi.get("volumeAttributes", {})
        if (attrs.get("fsType") != "ext4" or attrs.get("cachingMode") != "None" or
                attrs.get("kind") != "Managed" or attrs.get("skuName") != "PremiumV2_LRS" or
                attrs.get("DiskIOPSReadWrite") != "3000" or attrs.get("DiskMBpsReadWrite") != "125" or
                attrs.get("LogicalSectorSize") != "512"):
            raise GuardError(f"PV {spec['pvName']} caching/managed/SKU attributes are invalid.")

    def create_or_validate_pv(self, spec: dict[str, str], pvc: dict[str, Any], disk: dict[str, Any]) -> dict[str, Any]:
        obj = self.pv_object(spec, pvc, disk)
        rec = self.receipt_pv(spec["role"])
        if (rec.get("name") != spec["pvName"] or rec.get("diskId") != disk["id"] or
                rec.get("preexistingAbsent") is not True or
                rec.get("pvcUid") not in (None, pvc["metadata"]["uid"])):
            raise GuardError(f"PV {spec['pvName']} lacks this run's exact absent-before-create/PVC/disk intent.")
        if rec.get("uid") and rec.get("diskUniqueId") != self.receipt_disk(spec["role"]).get("diskUniqueId"):
            raise GuardError(f"PV {spec['pvName']} saved disk uniqueId differs from the hydrated restore disk.")
        rec.update({"pvcUid": pvc["metadata"]["uid"],
                    "diskUniqueId": self.receipt_disk(spec["role"])["diskUniqueId"],
                    "status": "create-intent"})
        self.write_receipt()
        existing = self.get_kube_object(["get", "pv", spec["pvName"]], f"planned-pv-{spec['role']}")
        if existing is None:
            # Server dry-run validates the manifest without creating a PV.
            run_command(["kubectl", "apply", "--dry-run=server", "-f", "-", "-o", "json"],
                        env=self.env, stdin=json.dumps(obj), timeout=180)
            raw = run_command(["kubectl", "create", "-f", "-", "-o", "json"],
                              env=self.env, stdin=json.dumps(obj), timeout=180)
            save_evidence("static-pv-created", "persistentVolume", spec["pvName"], raw)
            pv = parse_object(raw, f"{spec['role']} PV apply")
        else:
            pv = existing
            save_evidence("static-pv-existing-checked", "persistentVolume", spec["pvName"], json.dumps(pv))
            if rec.get("uid") and rec["uid"] != pv.get("metadata", {}).get("uid"):
                raise GuardError(f"PV {spec['pvName']} pre-existing UID differs from this run's saved create.")
        self.validate_pv(pv, spec, pvc, disk)
        rec = {"name": spec["pvName"], "role": spec["role"], "uid": pv.get("metadata", {}).get("uid"),
               "resourceVersion": pv.get("metadata", {}).get("resourceVersion"),
               "diskId": disk["id"], "diskUniqueId": self.receipt_disk(spec["role"])["diskUniqueId"],
               "pvc": f"{NAMESPACE}/{spec['claim']}", "pvcUid": pvc["metadata"]["uid"],
               "preexistingAbsent": True, "status": pv.get("status", {}).get("phase", "Pending"),
               "reclaimPolicy": "Retain"}
        old = self.receipt_pv(spec["role"])
        old.update(rec)
        self.write_receipt()
        return pv

    def bind_pvc(self, spec: dict[str, str], pvc_before: dict[str, Any], pv: dict[str, Any]) -> dict[str, Any]:
        # Revalidate all source and target identity immediately before binding.
        self.kubernetes_bind_preflight()
        current = self.kubectl_json(["get", "pvc", spec["claim"], "-n", NAMESPACE], f"{spec['role']}-pvc-before-bind")
        uid = pvc_before["metadata"]["uid"]
        pv_uid = pv.get("metadata", {}).get("uid")
        if current.get("metadata", {}).get("uid") != uid:
            raise GuardError(f"PVC {spec['claim']} UID changed before the guarded static bind.")
        vol_name = current.get("spec", {}).get("volumeName", "")
        if vol_name == spec["pvName"]:
            return self.wait_for_bound_pvc(spec, uid, pv)
        if vol_name:
            raise GuardError(f"PVC {spec['claim']} raced to a different PV ({vol_name}); refusing adoption.")
        if current.get("status", {}).get("phase") != "Pending":
            raise GuardError(f"PVC {spec['claim']} is no longer Pending before the guarded static bind.")
        if current.get("spec", {}).get("dataSource", {}).get("name") != spec["vss"]:
            raise GuardError(f"PVC {spec['claim']} dataSource changed before the guarded static bind.")
        rv = current.get("metadata", {}).get("resourceVersion")
        if not rv or not pv_uid:
            raise GuardError("Missing PVC resourceVersion or helper PV UID for the binding guard.")
        patch = [
            {"op": "test", "path": "/metadata/uid", "value": uid},
            {"op": "test", "path": "/metadata/resourceVersion", "value": rv},
            {"op": "test", "path": "/status/phase", "value": "Pending"},
            {"op": "test", "path": "/spec/dataSource/kind", "value": "VolumeSnapshot"},
            {"op": "test", "path": "/spec/dataSource/name", "value": spec["vss"]},
            {"op": "add", "path": "/spec/volumeName", "value": spec["pvName"]},
        ]
        if "volumeName" in current.get("spec", {}):
            patch.insert(-1, {"op": "test", "path": "/spec/volumeName", "value": ""})
        record_event("pvc-bind-intent", "persistentVolumeClaim", spec["claim"],
                     {"pvcUid": uid, "resourceVersion": rv, "pvName": spec["pvName"],
                      "pvUid": pv_uid, "dataSource": spec["vss"]})
        raw = run_command(["kubectl", "patch", "pvc", spec["claim"], "-n", NAMESPACE,
                           "--type=json", "-p", json.dumps(patch), "-o", "json"],
                          env=self.env, timeout=180)
        save_evidence("pvc-volumeName-patched", "persistentVolumeClaim", spec["claim"], raw)
        patched = parse_object(raw, f"{spec['role']} PVC guarded patch")
        if patched.get("metadata", {}).get("uid") != uid:
            raise GuardError(f"PVC {spec['claim']} UID changed in patch response.")
        if patched.get("spec", {}).get("volumeName") != spec["pvName"]:
            raise GuardError(f"PVC {spec['claim']} did not record the planned PV name.")
        return self.wait_for_bound_pvc(spec, uid, pv)

    def wait_for_bound_pvc(self, spec: dict[str, str], uid: str, pv: dict[str, Any]) -> dict[str, Any]:
        deadline = time.monotonic() + 600
        while True:
            pvc = self.kubectl_json(["get", "pvc", spec["claim"], "-n", NAMESPACE],
                                    f"{spec['role']}-pvc-wait-bound")
            if pvc.get("metadata", {}).get("uid") != uid:
                raise GuardError(f"PVC {spec['claim']} UID changed while waiting for the helper PV bind.")
            volume_name = pvc.get("spec", {}).get("volumeName", "")
            phase = pvc.get("status", {}).get("phase")
            if volume_name and volume_name != spec["pvName"]:
                raise GuardError(f"PVC {spec['claim']} bound elsewhere to {volume_name}; refusing adoption.")
            if volume_name == spec["pvName"] and phase == "Bound":
                self.verify_bound_pvc(spec, pvc, pv)
                return pvc
            if phase != "Pending" or time.monotonic() >= deadline:
                raise GuardError(f"PVC {spec['claim']} did not reach Bound on its receipted static PV.")
            time.sleep(5)

    def verify_bound_pvc(self, spec: dict[str, str], pvc: dict[str, Any], pv: dict[str, Any]) -> None:
        if pvc.get("metadata", {}).get("uid") != self.receipt_disk(spec["role"]).get("pvcUid"):
            raise GuardError(f"PVC {spec['claim']} does not match the saved Pending PVC UID.")
        if pvc.get("spec", {}).get("volumeName") != spec["pvName"]:
            raise GuardError(f"PVC {spec['claim']} is not bound to the planned helper PV.")
        pv_uid = pv.get("metadata", {}).get("uid")
        live_pv = self.kubectl_json(["get", "pv", spec["pvName"]], f"{spec['role']}-pv-after-bind")
        ref = live_pv.get("spec", {}).get("claimRef", {})
        if (live_pv.get("metadata", {}).get("uid") != pv_uid or
                ref.get("namespace") != NAMESPACE or ref.get("name") != spec["claim"] or
                ref.get("uid") != pvc.get("metadata", {}).get("uid") or
                lower(live_pv.get("spec", {}).get("csi", {}).get("volumeHandle")) !=
                lower(self.receipt_disk(spec["role"])["id"])):
            raise GuardError(f"PVC/PV binding for {spec['claim']} does not match exact saved UID and disk ID.")
        rec = self.receipt_disk(spec["role"])
        rec["status"] = "bound-to-static-pv"
        rec["boundAtUtc"] = utc_now()
        self.write_receipt()
        record_event("pvc-bound-to-static-pv", "persistentVolumeClaim", spec["claim"],
                     {"pvcUid": pvc["metadata"]["uid"], "pvName": spec["pvName"],
                      "pvUid": pv_uid, "diskId": rec["id"], "diskUniqueId": rec["diskUniqueId"]})

    def apply(self) -> None:
        # Confirm both claims first and persist their UIDs before any Azure write.
        cluster = self.cluster_live()
        pvc_objects = self.live_snapshots_and_pvcs(cluster)
        for spec, pvc in zip(self.plan["specs"], pvc_objects):
            record = self.receipt_disk(spec["role"])
            if record.get("pvcUid") and record["pvcUid"] != pvc["metadata"]["uid"]:
                raise GuardError(f"Pending PVC {spec['claim']} changed UID since intent.")
            record["pvcUid"] = pvc["metadata"]["uid"]
            record["clusterUid"] = cluster["metadata"]["uid"]
            pv_record = self.receipt_pv(spec["role"])
            if pv_record.get("pvcUid") and pv_record["pvcUid"] != pvc["metadata"]["uid"]:
                raise GuardError(f"PVC {spec['claim']} changed UID since the static-PV intent.")
            existing_pv = self.get_kube_object(["get", "pv", spec["pvName"]],
                                               f"prewrite-planned-pv-{spec['role']}")
            if pv_record.get("preexistingAbsent") is None:
                if existing_pv is not None:
                    raise GuardError(f"Planned PV name {spec['pvName']} existed before this helper recorded its create intent.")
                pv_record["preexistingAbsent"] = True
                pv_record["absentCheckedAtUtc"] = utc_now()
            elif existing_pv is not None:
                self.validate_receipted_pv(spec, pvc)
            pv_record["pvcUid"] = pvc["metadata"]["uid"]
        self.receipt["status"] = "intent"
        self.write_receipt()
        # Hydrate and verify both disks before creating either PV. This leaves
        # both snapshot-backed claims pending throughout Azure write operations.
        disks = [self.create_or_reuse_disk(spec) for spec in self.plan["specs"]]
        # Last strict gate before any PV can auto-bind and let CNPG start.
        self.kubernetes_preflight()
        pvs = []
        for spec, initial_pvc, disk in zip(self.plan["specs"], pvc_objects, disks):
            current_pvc = self.kubectl_json(["get", "pvc", spec["claim"], "-n", NAMESPACE],
                                            f"{spec['role']}-pvc-before-pv")
            if (current_pvc.get("metadata", {}).get("uid") != initial_pvc["metadata"]["uid"] or
                    current_pvc.get("status", {}).get("phase") != "Pending" or
                    current_pvc.get("spec", {}).get("volumeName", "") or
                    current_pvc.get("spec", {}).get("dataSource", {}).get("name") != spec["vss"]):
                raise GuardError(f"PVC {spec['claim']} changed or bound elsewhere before PV creation; refusing adoption.")
            pv = self.create_or_validate_pv(spec, current_pvc, disk)
            pvs.append(pv)
        # The PV controller may already have bound either exact claimRef. The
        # binding-stage preflight accepts only this run's saved PV UID/disk/PVC
        # identity; it also permits the primary to have become ready only once
        # both claims are Bound to those exact PVs. No Azure writes follow.
        for spec, initial_pvc, pv in zip(self.plan["specs"], pvc_objects, pvs):
            self.bind_pvc(spec, initial_pvc, pv)
        self.receipt["status"] = "ready"
        self.receipt["completedAtUtc"] = utc_now()
        self.receipt["sourceManifestUnchanged"] = True
        self.receipt["sourceSnapshotsUntouched"] = True
        self.receipt["path"] = "pinned retained DATA/WAL snapshot -> PremiumV2 managed disk Copy -> static Azure Disk CSI PV"
        self.write_receipt()
        record_event("restore-pending-disks-ready", "receipt", str(RECEIPT),
                     {"clusterUid": self.receipt["cluster"]["uid"],
                      "disks": [x["id"] for x in self.receipt["restoreDisks"]],
                      "pvs": [x["name"] for x in self.receipt["staticPVs"]]})
        print(f"Prepared and prebound both pinned-source PremiumV2 disks. Receipt: {RECEIPT}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Create disks/PVs and prebind the exact Pending PVCs.")
    parser.add_argument("--owner-token", help="Required in apply mode; must match local run ownership state.")
    parser.add_argument("--wait-timeout-seconds", type=int, default=86400,
                        help="Maximum wait for each disk's numeric completionPercent=100 (default: 24 hours).")
    args = parser.parse_args()
    try:
        plan = load_local_plan(args.owner_token)
        if not args.apply:
            local_only_dry_run(plan)
            return 0
        if args.owner_token is None:
            raise GuardError("Apply mode requires --owner-token with the exact run token.")
        if args.wait_timeout_seconds < 1:
            raise GuardError("--wait-timeout-seconds must be positive.")
        runner = ApplyRun(plan, args.wait_timeout_seconds)
        runner.apply()
        return 0
    except (GuardError, OSError, KeyError, TypeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        print("No deletion is attempted; inspect the append-only receipt/evidence before retrying.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
