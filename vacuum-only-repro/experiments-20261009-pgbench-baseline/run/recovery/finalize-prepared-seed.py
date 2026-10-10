#!/usr/bin/env python3
"""Finalize already hydrated snapshot-seeded primary/replica disks.

This continuation performs no backup, snapshot, disk, or fixture-write work.
It is local dry-run by default.  The root phase runner owns finalize-process.json.
"""
from __future__ import annotations

import argparse
import base64
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
NAMESPACE, CLUSTER = "repro", "hdd-pg"
ARCHIVE_SECRET, APP_SECRET = "azure-barman-credentials", "hdd-pg-app"
IMAGE = "ghcr.io/cloudnative-pg/postgresql:16.15-202609281634-standard-trixie@sha256:1dd42563007500b94b943f2a1620f724efedfc9eea2d07a50c6db4582c2431fd"
SYSIMAGE = "ghcr.io/cloudnative-pg/postgresql:16.15-202609140815-system-trixie@sha256:a30ea813ae1c82fc94743c4e71766688158b58d8ddf1f4e20ff5d3edb569a215"
EXPECTED_SYSID = "7692518806614212629"


class Refusal(RuntimeError):
    pass


def require(ok: bool, message: str) -> None:
    if not ok:
        raise Refusal(message)


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def canon(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Refusal(f"Invalid local JSON {path}: {exc}") from exc
    require(isinstance(value, dict), f"Expected a JSON object at {path}")
    return value


def saved(pattern: str, *, name: str | None = None) -> tuple[Path, dict[str, Any]]:
    matches = []
    for path in sorted(EVIDENCE.glob(pattern)):
        value = read_json(path)
        if name is None or value.get("metadata", {}).get("name") == name:
            matches.append((path, value))
    require(bool(matches), f"Missing persisted evidence: {pattern} name={name}")
    return matches[-1]


class Runner:
    def __init__(self, sub: str, kubeconfig: Path):
        self.sub, self.kubeconfig = sub, kubeconfig

    def call(self, argv: list[str], timeout: int = 180) -> str:
        try:
            result = subprocess.run(argv, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=timeout, check=False)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise Refusal(f"Command failed to complete ({shlex.join(argv[:3])}…): {exc}") from exc
        if result.returncode:
            tail = result.stderr.strip().splitlines()[-1:] or [f"exit {result.returncode}"]
            raise Refusal(f"Command failed ({shlex.join(argv[:3])}…): {tail[0]}")
        return result.stdout

    def k(self, *args: str, timeout: int = 180) -> str:
        return self.call(["kubectl", f"--kubeconfig={self.kubeconfig}", *args], timeout)

    def kj(self, *args: str, timeout: int = 180) -> dict[str, Any]:
        return json.loads(self.k(*args, "-o", "json", timeout=timeout))

    def azj(self, *args: str, timeout: int = 600) -> Any:
        return json.loads(self.call(["az", *args, "--subscription", self.sub, "-o", "json"], timeout))


class Journal:
    def __init__(self):
        self.path = RECOVERY / "phase-events.jsonl"
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)

    def event(self, phase: str, resource: str, name: str, details: str) -> None:
        record = {"atUtc": now(), "phase": phase, "resource": resource, "name": name, "details": details}
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, canon(record)); os.fsync(fd)
        finally:
            os.close(fd)

    def proof(self, phase: str, value: Any) -> Path:
        raw = canon(value); digest = hashlib.sha256(raw).hexdigest()
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        path = EVIDENCE / f"{stamp}-{phase}-{digest}.json"
        EVIDENCE.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.write(fd, raw); os.fsync(fd)
        finally:
            os.close(fd)
        self.event(phase, "evidence", path.name, f"sha256={digest}")
        return path


def direct_lock(locks: list[dict[str, Any]], target: str) -> bool:
    target = target.rstrip("/").lower()
    for lock in locks:
        if lock.get("level") != "CanNotDelete":
            continue
        scope = lock.get("scope")
        if not scope and isinstance(lock.get("id"), str):
            scope = lock["id"].lower().split("/providers/microsoft.authorization/locks/", 1)[0]
        if isinstance(scope, str) and scope.rstrip("/").lower() == target:
            return True
    return False


def local_preflight(owner_token: str | None) -> dict[str, Any]:
    state = read_json(STATE_DIR / "state.json")
    storage_path = STATE_DIR / "storage.json"
    storage = read_json(storage_path)
    manifest = read_json(HERE / "source-manifest.json")
    snapshot = read_json(HERE / "snapshot.json")
    config = read_json(HERE / "run-config.json")
    bindings = read_json(RECOVERY / "source-bindings.json")
    require(state.get("status") == "ready" and storage.get("status") == "ready"
            and storage.get("ownerToken") == state.get("ownerToken"),
            "Run and archive-storage ownership state is not ready or does not match.")
    require(re.fullmatch(r"[a-f0-9]{32}", str(manifest.get("ownerToken", ""))) is not None,
            "Pinned retained-source owner token is invalid.")
    token = state["ownerToken"]
    require(re.fullmatch(r"[a-f0-9]{32}", token) is not None, "Invalid stack owner token.")
    if owner_token is not None:
        require(owner_token == token, "--owner-token does not match run state.")
    require(manifest.get("systemIdentifier") == EXPECTED_SYSID
            and str(snapshot.get("systemIdentifier")) == EXPECTED_SYSID
            and snapshot.get("heapBytes") == 24696061952
            and snapshot.get("tableBytes") == 25040748544,
            "Pinned 23-GiB source fixture identity or size mismatch.")
    require(config.get("client", {}).get("targetTps") == 5
            and config.get("client", {}).get("synchronousCommit") == "on"
            and config.get("snapshotRetention") == "preserve-all",
            "Run configuration differs from approved 5-TPS preserve-all baseline.")
    require(bindings.get("endLSN") == manifest.get("backup", {}).get("endLSN")
            and bindings.get("sourceBackupUid") == manifest.get("backup", {}).get("uid"),
            "Saved original paired-source LSN/Backup lineage mismatch.")
    require(not (STATE_DIR / "prepare.json").exists()
            and not (RECOVERY / "prepare-audit.json").exists(),
            "Final prepare receipts already exist; refusing overwrite.")

    cluster_path, cluster_evidence = saved("*-snapshot-recovery-primary-and-replica-ready-cluster-*.json")
    backup_path, backup_evidence = saved("*-seed-backup-completed-backup-*.json")
    job_path, job = saved("*-snapshot-recovery-job-complete-job-*.json")
    event_path, event = saved("*-snapshot-recovery-completion-event-event-*.json")
    primary_fixture_path, primary_fixture = saved("*-primary-fixture-cached-read-only-verified-*.json")
    replica_fixture_path, replica_fixture = saved("*-replica-fixture-cached-read-only-verified-*.json")
    pods_path, pods_proof = saved("*-measurement-image-pods-ready-pods-*.json")
    dryrun_path = EVIDENCE / "root-archive-bootstrap-removal-dryrun.json"
    dryrun_cluster = read_json(dryrun_path)
    patched_recovery = dryrun_cluster.get("spec", {}).get("bootstrap", {}).get("recovery", {})
    require(dryrun_cluster.get("metadata", {}).get("uid") == cluster_evidence.get("metadata", {}).get("uid")
            and patched_recovery.get("database") == "repro" and patched_recovery.get("owner") == "app"
            and patched_recovery.get("volumeSnapshots") == cluster_evidence.get("spec", {}).get("bootstrap", {}).get("recovery", {}).get("volumeSnapshots")
            and "source" not in patched_recovery and "recoveryTarget" not in patched_recovery
            and dryrun_cluster.get("spec", {}).get("externalClusters") is None
            and (dryrun_cluster.get("spec", {}).get("backup") or {}).get("barmanObjectStore") is None,
            "Root's server-side archive/bootstrap removal dry-run is absent or mismatched.")
    require(job.get("metadata", {}).get("uid") and job.get("status", {}).get("succeeded", 0) > 0
            and job.get("metadata", {}).get("labels", {}).get("cnpg.io/jobRole") == "snapshot-recovery"
            and event.get("reason") == "Completed"
            and (event.get("involvedObject") or {}).get("uid") == job["metadata"]["uid"],
            "Saved completed snapshot-recovery Job/Event evidence does not pair by UID.")
    require(cluster_evidence.get("metadata", {}).get("uid") == pods_proof.get("cluster", {}).get("metadata", {}).get("uid")
            and cluster_evidence.get("status", {}).get("systemID") == EXPECTED_SYSID,
            "Saved Cluster/image proof UID or system identifier mismatch.")

    disks: dict[str, tuple[Path, dict[str, Any]]] = {}
    for claim in (f"{CLUSTER}-1", f"{CLUSTER}-1-wal", f"{CLUSTER}-2", f"{CLUSTER}-2-wal"):
        matches = sorted(EVIDENCE.glob(f"*-disk-hydration-complete-azureDisk-{claim}-*.json"))
        require(bool(matches), f"Missing persisted hydrated disk proof for {claim}.")
        disk_path = matches[-1]
        disk = read_json(disk_path)
        disks[claim] = (disk_path, disk)
        require(disk.get("provisioningState") == "Succeeded"
                and float(disk.get("completionPercent", -1)) == 100
                and (disk.get("creationData") or {}).get("createOption") in ("Copy", "CopyStart"),
                f"Saved hydrated disk proof is invalid for {claim}.")
    seed_snapshots = {}
    for phase_file in sorted(EVIDENCE.glob("*-continuation-seed-snapshot-verified-azureSnapshot-*.json")):
        record = read_json(phase_file)
        label = phase_file.name.split("azureSnapshot-", 1)[1].split("-", 1)[0]
        if label in ("PG_DATA", "PG_WAL"):
            seed_snapshots[label] = (phase_file, record)
    require(set(seed_snapshots) == {"PG_DATA", "PG_WAL"}, "Persisted paired seed snapshot proofs are incomplete.")
    return {
        "state": state, "storage": storage, "storagePath": storage_path,
        "manifest": manifest, "snapshot": snapshot, "config": config,
        "bindings": bindings, "clusterEvidence": cluster_evidence, "clusterEvidencePath": cluster_path,
        "backupEvidence": backup_evidence, "backupEvidencePath": backup_path,
        "job": job, "jobPath": job_path, "jobEvent": event, "jobEventPath": event_path,
        "primaryFixtureEvidence": primary_fixture, "primaryFixturePath": primary_fixture_path,
        "replicaFixtureEvidence": replica_fixture, "replicaFixturePath": replica_fixture_path,
        "podsEvidence": pods_proof, "podsEvidencePath": pods_path, "seedSnapshots": seed_snapshots,
        "disks": disks, "rootDryrun": dryrun_cluster, "rootDryrunPath": dryrun_path,
    }


SQL = """SELECT json_build_object(
 'version',current_setting('server_version'),
 'systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),
 'inRecovery',pg_is_in_recovery(),
 'heapBytes',pg_relation_size('public.vacuum_target'),
 'tableBytes',pg_total_relation_size('public.vacuum_target'),
 'relationFileNode',pg_relation_filenode('public.vacuum_target'),
 'liveEstimate',(SELECT n_live_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),
 'deadEstimate',(SELECT n_dead_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),
 'autovacuumDisabled','autovacuum_enabled=false'=ANY((SELECT reloptions FROM pg_class WHERE oid='public.vacuum_target'::regclass)),
 'hasIndex',to_regclass('public.vacuum_target_id_idx') IS NOT NULL,
 'walLevel',current_setting('wal_level'),'fullPageWrites',current_setting('full_page_writes'),
 'recoveryPrefetch',current_setting('recovery_prefetch'),'vacuumCostDelay',current_setting('vacuum_cost_delay'),
 'synchronousCommit',current_setting('synchronous_commit'),
 'synchronousStandbyNames',current_setting('synchronous_standby_names'),
 'sharedBuffers',current_setting('shared_buffers'),'maintenanceIoConcurrency',current_setting('maintenance_io_concurrency'),
 'maintenanceWorkMem',current_setting('maintenance_work_mem'),'checkpointTimeout',current_setting('checkpoint_timeout'),
 'maxWalSize',current_setting('max_wal_size'),'trackIoTiming',current_setting('track_io_timing'),
 'trackWalIoTiming',current_setting('track_wal_io_timing'),'autovacuum',current_setting('autovacuum'),
 'appSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend'),
 'flushLsn',CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_current_wal_flush_lsn() END,
 'replayLsn',pg_last_wal_replay_lsn())::text"""


def psql(r: Runner, kube: Path, pod: str) -> dict[str, Any]:
    raw = r.k("exec", "-n", NAMESPACE, "-c", "postgres", pod, "--", "psql", "-XAtq",
              "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "repro", "-c", SQL).strip()
    return json.loads(raw)


def check_fixture(fixture: dict[str, Any], *, replica: bool, snapshot: dict[str, Any]) -> None:
    require(str(fixture.get("systemIdentifier")) == EXPECTED_SYSID
            and fixture.get("inRecovery") is replica
            and fixture.get("version", "").startswith("16.15")
            and fixture.get("heapBytes") == snapshot["heapBytes"]
            and fixture.get("tableBytes") == snapshot["tableBytes"]
            and fixture.get("autovacuumDisabled") is True
            and fixture.get("hasIndex") is True
            and fixture.get("appSessions") == 0,
            f"{'Replica' if replica else 'Primary'} fixture identity/size/role/client-free check failed.")
    expected = {"walLevel": "logical", "fullPageWrites": "on", "recoveryPrefetch": "try",
                "vacuumCostDelay": "0", "synchronousCommit": "on", "sharedBuffers": "256MB",
                "maintenanceIoConcurrency": "10", "maintenanceWorkMem": "64MB",
                "checkpointTimeout": "5min", "maxWalSize": "1GB", "trackIoTiming": "on",
                "trackWalIoTiming": "on", "autovacuum": "on"}
    require(all(str(fixture.get(key)) == value for key, value in expected.items()),
            f"{'Replica' if replica else 'Primary'} CNPG/PostgreSQL settings differ from the pinned baseline.")
    if not replica:
        require(fixture.get("synchronousStandbyNames") == 'ANY 1 ("hdd-pg-2")',
                "Primary synchronous standby configuration is not ANY 1 hdd-pg-2.")


def wait_caught_up(r: Runner, kube: Path, journal: Journal, reason: str) -> tuple[dict[str, Any], dict[str, Any]]:
    deadline = time.monotonic() + 600
    primary = replica = {}
    while time.monotonic() < deadline:
        primary = psql(r, kube, f"{CLUSTER}-1")
        replica = psql(r, kube, f"{CLUSTER}-2")
        if (primary.get("systemIdentifier") == replica.get("systemIdentifier") == EXPECTED_SYSID
                and primary.get("flushLsn") and replica.get("replayLsn")
                and r.k("exec", "-n", NAMESPACE, "-c", "postgres", f"{CLUSTER}-1", "--", "psql",
                        "-XAtq", "-U", "postgres", "-d", "repro", "-c",
                        f"SELECT count(*) FROM pg_stat_replication WHERE state='streaming' AND replay_lsn>='{primary['flushLsn']}'::pg_lsn;").strip() == "1"):
            journal.event(reason, "postgres", f"{CLUSTER}-2", f"flushLsn={primary['flushLsn']} replayLsn={replica['replayLsn']}")
            return primary, replica
        time.sleep(3)
    raise Refusal("Replica did not remain streaming and replay through primary flush LSN within ten minutes.")


def validate_live(r: Runner, journal: Journal, data: dict[str, Any], *, before_finalize: bool) -> dict[str, Any]:
    state, manifest, snapshot = data["state"], data["manifest"], data["snapshot"]
    token, short = state["ownerToken"], state["ownerToken"][:8]
    cluster = r.kj("get", "cluster", CLUSTER, "-n", NAMESPACE)
    meta, spec, status = cluster.get("metadata", {}), cluster.get("spec", {}), cluster.get("status", {})
    require(meta.get("uid") == data["clusterEvidence"].get("metadata", {}).get("uid")
            and meta.get("labels", {}).get("repro.stack.token") == token,
            "Live Cluster UID/token differs from the retained snapshot-seeded proof.")
    require(spec.get("instances") == 2 and status.get("readyInstances") == 2
            and status.get("currentPrimary") == f"{CLUSTER}-1"
            and status.get("systemID") == EXPECTED_SYSID
            and spec.get("imageName") == IMAGE,
            "Live Cluster is not the expected ready two-instance measurement-image baseline.")
    recovery = (spec.get("bootstrap") or {}).get("recovery") or {}
    require(recovery.get("database") == "repro" and recovery.get("owner") == "app"
            and recovery.get("volumeSnapshots", {}).get("storage", {}).get("name") == f"source-data-{short}"
            and recovery.get("volumeSnapshots", {}).get("walStorage", {}).get("name") == f"source-wal-{short}",
            "Bootstrap application ownership or original paired DATA/WAL VolumeSnapshot lineage changed.")
    if before_finalize:
        require(recovery.get("source") == "retained-source"
                and recovery.get("recoveryTarget", {}).get("targetLSN") == manifest["backup"]["endLSN"]
                and spec.get("externalClusters")
                and (spec.get("backup") or {}).get("barmanObjectStore", {}).get("serverName") == f"hdd-pg-seed-{short}",
                "Temporary bootstrap source/archive references are not in the expected pre-finalize state.")
    else:
        require("source" not in recovery and "recoveryTarget" not in recovery
                and not spec.get("externalClusters")
                and (spec.get("backup") or {}).get("barmanObjectStore") is None,
                "Final Cluster still has a temporary bootstrap/archive reference.")
    journal.proof("finalize-cluster-live", cluster)

    namespace = r.kj("get", "namespace", NAMESPACE)
    require(namespace.get("metadata", {}).get("labels", {}).get("repro.stack.token") == token,
            "Namespace owner token mismatch.")
    backup_name = f"vacuum-seed-{short}"
    backup = r.kj("get", "backup", backup_name, "-n", NAMESPACE)
    require(backup.get("metadata", {}).get("uid") == data["backupEvidence"].get("metadata", {}).get("uid"),
            "Live offline seed Backup identity changed.")
    require(backup.get("metadata", {}).get("labels", {}).get("repro.stack.token") == token
            and backup.get("status", {}).get("phase") == "completed"
            and backup.get("status", {}).get("method") == "volumeSnapshot"
            and backup.get("spec", {}).get("online") is False
            and backup.get("status", {}).get("majorVersion") == 16,
            "Offline paired seed Backup is no longer a completed PG16 volumeSnapshot backup.")
    elements = (backup.get("status", {}).get("snapshotBackupStatus") or {}).get("elements", [])
    require(sorted(e.get("type") for e in elements) == ["PG_DATA", "PG_WAL"],
            "Offline seed Backup does not contain exactly paired DATA/WAL elements.")
    data["backup"] = backup
    journal.proof("finalize-offline-seed-backup", backup)

    vss_names = {e["type"]: e["name"] for e in elements}
    new_snapshot_ids: dict[str, str] = {}
    new_snapshot_records: list[dict[str, Any]] = []
    disks: list[dict[str, Any]] = []
    pvcs = r.kj("get", "pvc", "-n", NAMESPACE, "-l", f"cnpg.io/cluster={CLUSTER}").get("items", [])
    by_name = {p.get("metadata", {}).get("name"): p for p in pvcs}
    expected_claims = {f"{CLUSTER}-1", f"{CLUSTER}-1-wal", f"{CLUSTER}-2", f"{CLUSTER}-2-wal"}
    require(set(by_name) == expected_claims, "Expected only the four paired primary/replica DATA/WAL PVCs.")
    expect_vss = {f"{CLUSTER}-1": f"source-data-{short}", f"{CLUSTER}-1-wal": f"source-wal-{short}",
                  f"{CLUSTER}-2": vss_names["PG_DATA"], f"{CLUSTER}-2-wal": vss_names["PG_WAL"]}
    for typ, vss_name in vss_names.items():
        vss = r.kj("get", "volumesnapshot", vss_name, "-n", NAMESPACE)
        source_claim = f"{CLUSTER}-1" if typ == "PG_DATA" else f"{CLUSTER}-1-wal"
        source_pvc = by_name[source_claim]
        content_name = vss.get("status", {}).get("boundVolumeSnapshotContentName")
        require(vss.get("status", {}).get("readyToUse") is True and content_name,
                f"Seed {typ} VolumeSnapshot is not ready/bound.")
        require(vss.get("metadata", {}).get("labels", {}).get("cnpg.io/backupName") == backup_name
                and vss.get("metadata", {}).get("labels", {}).get("cnpg.io/cluster") == CLUSTER
                and vss.get("metadata", {}).get("labels", {}).get("repro.stack.token") == token
                and vss.get("metadata", {}).get("annotations", {}).get("vacuum-only-repro/seed-backup-uid") == backup["metadata"]["uid"]
                and vss.get("metadata", {}).get("annotations", {}).get("vacuum-only-repro/source-pvc-uid") == source_pvc.get("metadata", {}).get("uid")
                and vss.get("spec", {}).get("source", {}).get("persistentVolumeClaimName") == source_claim,
                f"Seed {typ} VolumeSnapshot owner/primary-PVC UID binding changed.")
        content = r.kj("get", "volumesnapshotcontent", content_name)
        sid = content.get("status", {}).get("snapshotHandle")
        require(content.get("spec", {}).get("deletionPolicy") == "Retain"
                and content.get("spec", {}).get("volumeSnapshotRef", {}).get("uid") == vss.get("metadata", {}).get("uid")
                and isinstance(sid, str) and f"/resourcegroups/{state['snapshotResourceGroup'].lower()}/providers/microsoft.compute/snapshots/" in sid.lower(),
                f"Seed {typ} retained VSS/VSC identity or snapshot scope mismatch.")
        seed_evidence = data["seedSnapshots"][typ]
        saved_arm = seed_evidence[1]
        arm = r.azj("snapshot", "show", "--ids", sid)
        primary_pv = r.kj("get", "pv", source_pvc.get("spec", {}).get("volumeName", ""))
        source_disk_id = (primary_pv.get("spec", {}).get("csi") or {}).get("volumeHandle")
        require(arm.get("id", "").lower() == sid.lower()
                and arm.get("uniqueId") == saved_arm.get("uniqueId")
                and (arm.get("creationData") or {}).get("sourceResourceId", "").lower() == str(source_disk_id).lower()
                and (saved_arm.get("creationData") or {}).get("sourceResourceId", "").lower() == str(source_disk_id).lower()
                and arm.get("provisioningState") == "Succeeded" and arm.get("snapshotAccessState") == "Available"
                and (arm.get("completionPercent") is None or float(arm.get("completionPercent")) == 100)
                and (arm.get("diskSizeGB") or arm.get("diskSizeGb")) == 512
                and arm.get("incremental") is True and arm.get("tags", {}).get("reprotoken") == token,
                f"Seed {typ} Azure snapshot UID/readiness/ownership changed.")
        new_snapshot_ids[typ] = sid
        new_snapshot_records.append({"id": sid, "uniqueId": arm.get("uniqueId"), "type": typ,
                                     "volumeSnapshot": vss_name, "volumeSnapshotUid": vss.get("metadata", {}).get("uid"),
                                     "volumeSnapshotContentUid": content.get("metadata", {}).get("uid"),
                                     "backupUid": backup.get("metadata", {}).get("uid"),
                                     "retentionLock": "CanNotDelete"})
        journal.proof("finalize-seed-paired-snapshot", {"volumeSnapshot": vss, "content": content, "azureSnapshot": arm})

    data["snapshotIds"] = new_snapshot_ids
    data["snapshotRecords"] = new_snapshot_records

    expected_source = {
        f"{CLUSTER}-1": manifest["sourceSnapshotId"],
        f"{CLUSTER}-1-wal": manifest["pairedWAL"]["sourceSnapshotId"],
        f"{CLUSTER}-2": new_snapshot_ids["PG_DATA"], f"{CLUSTER}-2-wal": new_snapshot_ids["PG_WAL"],
    }
    for claim in sorted(expected_claims):
        pvc = by_name[claim]
        owner_refs = pvc.get("metadata", {}).get("ownerReferences", [])
        require(pvc.get("status", {}).get("phase") == "Bound"
                and pvc.get("metadata", {}).get("labels", {}).get("cnpg.io/cluster") == CLUSTER
                and pvc.get("metadata", {}).get("labels", {}).get("cnpg.io/instanceName") == claim.removesuffix("-wal")
                and any(o.get("kind") == "Cluster" and o.get("uid") == meta.get("uid") for o in owner_refs),
                f"PVC {claim} is not the exact Bound cluster-owned claim.")
        datasource = pvc.get("spec", {}).get("dataSource") or {}
        datasource_ref = pvc.get("spec", {}).get("dataSourceRef") or {}
        require(datasource.get("kind") == "VolumeSnapshot" and datasource.get("name") == expect_vss[claim]
                and datasource_ref.get("kind") == "VolumeSnapshot" and datasource_ref.get("name") == expect_vss[claim],
                f"PVC {claim} changed its paired VolumeSnapshot reference.")
        pv = r.kj("get", "pv", pvc.get("spec", {}).get("volumeName", ""))
        cref = pv.get("spec", {}).get("claimRef") or {}
        attrs = ((pv.get("spec", {}).get("csi") or {}).get("volumeAttributes") or {})
        disk_id = (pv.get("spec", {}).get("csi") or {}).get("volumeHandle")
        expected_reclaim = "Retain" if claim in (f"{CLUSTER}-1", f"{CLUSTER}-1-wal") else "Delete"
        require(cref.get("namespace") == NAMESPACE and cref.get("name") == claim
                and cref.get("uid") == pvc.get("metadata", {}).get("uid")
                and pv.get("spec", {}).get("persistentVolumeReclaimPolicy") == expected_reclaim
                and (pv.get("spec", {}).get("csi") or {}).get("driver") == "disk.csi.azure.com"
                and attrs.get("cachingMode") == "None" and attrs.get("kind") == "Managed"
                and attrs.get("skuName") == "PremiumV2_LRS" and attrs.get("DiskIOPSReadWrite") == "3000"
                and attrs.get("DiskMBpsReadWrite") == "125" and attrs.get("LogicalSectorSize") == "512",
                f"PV for {claim} has incorrect claim UID/reclaim/performance/caching configuration.")
        disk = r.azj("disk", "show", "--ids", disk_id, timeout=900)
        pct = disk.get("completionPercent")
        try:
            numeric_pct = float(pct)
        except (TypeError, ValueError):
            numeric_pct = None
        creation = disk.get("creationData") or {}
        require((disk.get("id") or "").lower() == str(disk_id).lower()
                and f"/resourcegroups/{state['nodeResourceGroup'].lower()}/providers/microsoft.compute/disks/" in str(disk_id).lower()
                and disk.get("provisioningState") == "Succeeded" and disk.get("managedBy")
                and numeric_pct == 100 and creation.get("createOption") in ("Copy", "CopyStart")
                and (creation.get("sourceResourceId") or "").lower() == expected_source[claim].lower()
                and (disk.get("sku") or {}).get("name") == "PremiumV2_LRS"
                and (disk.get("diskSizeGB") or disk.get("diskSizeGb")) == 512
                and disk.get("diskIOPSReadWrite") == 3000 and disk.get("diskMBpsReadWrite") == 125
                and str(disk.get("logicalSectorSize") or creation.get("logicalSectorSize")) == "512"
                and str(state["availabilityZone"]) in [str(x) for x in (disk.get("zones") or [])],
                f"Disk for {claim} is not an exact fully hydrated 512-GiB PremiumV2 snapshot copy.")
        record = {"claim": claim, "id": disk_id, "sourceResourceId": creation.get("sourceResourceId"),
                  "sku": disk["sku"]["name"], "diskSizeGiB": disk.get("diskSizeGB") or disk.get("diskSizeGb"),
                  "iops": disk.get("diskIOPSReadWrite"), "throughputMBps": disk.get("diskMBpsReadWrite"),
                  "completionPercent": numeric_pct, "createOption": creation.get("createOption"),
                  "sector": disk.get("logicalSectorSize") or creation.get("logicalSectorSize"),
                  "zones": disk.get("zones"), "provisioningState": disk.get("provisioningState")}
        disks.append(record)
        journal.proof("finalize-disk-pvc-pv-verified", {"pvc": pvc, "pv": pv, "disk": disk})

    require(len({d["id"].lower() for d in disks}) == 4, "Final prepared state must use four distinct DATA/WAL disks.")
    data["diskRecords"] = disks

    # Read-only retention audit: every extant snapshot in both dedicated source/run
    # groups must remain directly protected by CanNotDelete.
    source_group = manifest["resourceGroup"]
    snap_group = state["snapshotResourceGroup"]
    group_audits = []
    for group in (source_group, snap_group):
        locks = r.azj("lock", "list", "--resource-group", group)
        inventory = r.azj("snapshot", "list", "--resource-group", group)
        require(bool(inventory), f"Snapshot retention inventory is empty for {group}.")
        unlocked = [s.get("id") for s in inventory if not direct_lock(locks, s.get("id", ""))]
        require(not unlocked, f"Snapshot retention audit found unprotected snapshots: {unlocked[:3]}")
        group_audits.append({"resourceGroup": group, "snapshotCount": len(inventory),
                             "directCanNotDeleteCount": len([l for l in locks if l.get("level") == "CanNotDelete"]),
                             "snapshotIds": [s.get("id") for s in inventory], "locks": locks})
    for sid, uid in ((manifest["sourceSnapshotId"], manifest["uniqueId"]),
                     (manifest["pairedWAL"]["sourceSnapshotId"], manifest["pairedWAL"]["uniqueId"])):
        live = r.azj("snapshot", "show", "--ids", sid)
        require(live.get("id", "").lower() == sid.lower() and live.get("uniqueId") == uid
                and live.get("tags", {}).get("reprotoken") == manifest.get("ownerToken")
                and live.get("provisioningState") == "Succeeded" and live.get("snapshotAccessState") == "Available",
                "Pinned retained-source snapshot UID/readiness changed.")
        journal.proof("finalize-retained-source-snapshot", live)
    data["retentionAudits"] = group_audits
    journal.proof("finalize-retention-lock-audit", group_audits)

    pods = r.kj("get", "pods", "-n", NAMESPACE, "-l", f"cnpg.io/cluster={CLUSTER}").get("items", [])
    podmap = {p.get("metadata", {}).get("name"): p for p in pods}
    digest = IMAGE.rsplit("@", 1)[1]
    require(set(podmap) == {f"{CLUSTER}-1", f"{CLUSTER}-2"}
            and all(p.get("status", {}).get("phase") == "Running"
                    and any(c.get("name") == "postgres" and c.get("image") == IMAGE
                            for c in p.get("spec", {}).get("containers", []))
                    and any(c.get("name") == "postgres" and c.get("ready") is True
                            and (c.get("imageID") or "").endswith(digest)
                            for c in p.get("status", {}).get("containerStatuses", [])) for p in pods),
            "Both current postgres containers must be ready on the pinned measurement image digest.")
    primary, replica = wait_caught_up(r, r.kubeconfig, journal, "finalize-replica-caught-up-prepatch")
    check_fixture(primary, replica=False, snapshot=snapshot)
    check_fixture(replica, replica=True, snapshot=snapshot)
    require(primary.get("relationFileNode") == replica.get("relationFileNode"),
            "Primary and replica relation file node differ.")
    journal.proof("finalize-fixture-and-guc-prepatch", {"primary": primary, "replica": replica})

    app_secret = r.kj("get", "secret", APP_SECRET, "-n", NAMESPACE)
    secret_meta = app_secret.get("metadata", {})
    secret_data = app_secret.get("data", {})
    owner_refs = secret_meta.get("ownerReferences", [])
    username = base64.b64decode(secret_data.get("username", "")).decode(errors="strict")
    dbname = base64.b64decode(secret_data.get("dbname", "")).decode(errors="strict")
    require(app_secret.get("type") == "kubernetes.io/basic-auth" and username == "app" and dbname == "repro"
            and bool(secret_data.get("password"))
            and any(o.get("kind") == "Cluster" and o.get("uid") == meta.get("uid") for o in owner_refs),
            "Generated hdd-pg-app Secret must be basic-auth, owned by the live Cluster, and target app/repro.")
    # Do not save or print any Secret data/password.

    archive_secret = r.kj("get", "secret", ARCHIVE_SECRET, "-n", NAMESPACE)
    require(archive_secret.get("metadata", {}).get("labels", {}).get("repro.stack.token") == token
            and bool((archive_secret.get("data") or {}).get("connection-string")),
            "Temporary archive Secret is absent or not owned by this run before reference removal.")
    sanitized_archive_secret = {"name": ARCHIVE_SECRET, "uid": archive_secret.get("metadata", {}).get("uid"),
                                "ownerToken": token, "connectionStringPresent": True}
    journal.proof("finalize-archive-secret-prepatch", sanitized_archive_secret)

    if before_finalize:
        rv = meta.get("resourceVersion")
        ops = [
            {"op": "test", "path": "/metadata/uid", "value": meta["uid"]},
            {"op": "test", "path": "/metadata/resourceVersion", "value": rv},
            {"op": "test", "path": "/spec/bootstrap/recovery/source", "value": "retained-source"},
            {"op": "test", "path": "/spec/bootstrap/recovery/recoveryTarget/targetLSN", "value": manifest["backup"]["endLSN"]},
            {"op": "test", "path": "/spec/externalClusters/0/name", "value": "retained-source"},
            {"op": "test", "path": "/spec/backup/barmanObjectStore/serverName", "value": f"hdd-pg-seed-{token[:8]}"},
            {"op": "remove", "path": "/spec/bootstrap/recovery/source"},
            {"op": "remove", "path": "/spec/bootstrap/recovery/recoveryTarget"},
            {"op": "remove", "path": "/spec/externalClusters"},
            {"op": "remove", "path": "/spec/backup/barmanObjectStore"},
        ]
        patch = json.dumps(ops, separators=(",", ":"))
        dryrun = json.loads(r.k("patch", "cluster", CLUSTER, "-n", NAMESPACE, "--type=json",
                                 "--dry-run=server", "-o", "json", "-p", patch))
        dryrun_rec = dryrun.get("spec", {}).get("bootstrap", {}).get("recovery", {})
        require(dryrun.get("metadata", {}).get("uid") == meta["uid"]
                and dryrun_rec.get("database") == "repro" and dryrun_rec.get("owner") == "app"
                and dryrun_rec.get("volumeSnapshots") == recovery.get("volumeSnapshots")
                and "source" not in dryrun_rec and "recoveryTarget" not in dryrun_rec
                and not dryrun.get("spec", {}).get("externalClusters")
                and (dryrun.get("spec", {}).get("backup") or {}).get("barmanObjectStore") is None,
                "CNPG server-side dry-run did not accept the exact safe finalization patch.")
        journal.proof("finalize-bootstrap-archive-removal-dryrun", dryrun)
        journal.event("finalize-bootstrap-archive-removal-intent", "cluster", CLUSTER,
                      f"uid={meta['uid']} rv={rv}; preserve paired VolumeSnapshots, db=repro owner=app")
        r.k("patch", "cluster", CLUSTER, "-n", NAMESPACE, "--type=json", "-p", patch)
        journal.event("finalize-bootstrap-archive-references-removed", "cluster", CLUSTER,
                      "removed recovery source/target and temporary archive references; kept paired VolumeSnapshots")
        post = r.kj("get", "cluster", CLUSTER, "-n", NAMESPACE)
        post_recovery = (post.get("spec", {}).get("bootstrap") or {}).get("recovery") or {}
        require(post.get("metadata", {}).get("uid") == meta["uid"]
                and post_recovery.get("volumeSnapshots") == recovery.get("volumeSnapshots")
                and post_recovery.get("database") == "repro" and post_recovery.get("owner") == "app"
                and "source" not in post_recovery and "recoveryTarget" not in post_recovery
                and not post.get("spec", {}).get("externalClusters")
                and (post.get("spec", {}).get("backup") or {}).get("barmanObjectStore") is None,
                "Live Cluster did not persist exact archive/bootstrap finalization patch.")
        r.k("delete", "secret", ARCHIVE_SECRET, "-n", NAMESPACE, "--wait=true", timeout=300)
        absent = r.k("get", "secret", ARCHIVE_SECRET, "-n", NAMESPACE, "--ignore-not-found", "-o", "name").strip()
        require(absent == "", "Archive Secret remains after its references were removed.")
        journal.event("finalize-archive-secret-removed", "secret", ARCHIVE_SECRET,
                      "temporary archive writer/source references removed first")
    else:
        require("source" not in recovery and "recoveryTarget" not in recovery,
                "Current Cluster has not completed the expected bootstrap/archive finalization.")

    # Wait for reconciliation after removing the temporary configuration. This
    # only reads state and SQL; it does not issue fixture DML or start a client.
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        cluster = r.kj("get", "cluster", CLUSTER, "-n", NAMESPACE)
        if (cluster.get("metadata", {}).get("uid") == meta.get("uid")
                and cluster.get("status", {}).get("readyInstances") == 2
                and cluster.get("status", {}).get("currentPrimary") == f"{CLUSTER}-1"
                and cluster.get("status", {}).get("systemID") == EXPECTED_SYSID):
            break
        time.sleep(5)
    require(cluster.get("status", {}).get("readyInstances") == 2
            and cluster.get("status", {}).get("currentPrimary") == f"{CLUSTER}-1",
            "Cluster did not remain ready after archive/bootstrap finalization.")
    primary, replica = wait_caught_up(r, r.kubeconfig, journal, "finalize-replica-caught-up-final")
    check_fixture(primary, replica=False, snapshot=snapshot)
    check_fixture(replica, replica=True, snapshot=snapshot)
    require(primary.get("relationFileNode") == replica.get("relationFileNode"),
            "Final primary/replica relation file node mismatch.")

    archive_absent = r.k("get", "secret", ARCHIVE_SECRET, "-n", NAMESPACE,
                        "--ignore-not-found", "-o", "name").strip() == ""
    final = r.kj("get", "cluster", CLUSTER, "-n", NAMESPACE)
    final_recovery = (final.get("spec", {}).get("bootstrap") or {}).get("recovery") or {}
    require(archive_absent and final_recovery.get("database") == "repro" and final_recovery.get("owner") == "app"
            and final_recovery.get("volumeSnapshots") == recovery.get("volumeSnapshots")
            and "source" not in final_recovery and "recoveryTarget" not in final_recovery
            and not final.get("spec", {}).get("externalClusters")
            and (final.get("spec", {}).get("backup") or {}).get("barmanObjectStore") is None
            and final.get("spec", {}).get("imageName") == IMAGE
            and final.get("spec", {}).get("instances") == 2
            and final.get("status", {}).get("readyInstances") == 2
            and final.get("status", {}).get("systemID") == EXPECTED_SYSID,
            "Final Cluster, source/HA state, or archive Secret absence check failed.")
    journal.proof("finalize-cluster-ready", final)
    journal.proof("finalize-postpatch-fixture-guc", {"primary": primary, "replica": replica})

    backup_name = f"vacuum-seed-{token[:8]}"
    source_bindings = read_json(RECOVERY / "source-bindings.json")
    assignments = (state.get("sourceSnapshotReaderAssignments") or {}).get("assignments", [])
    archive_phase = read_json(saved("*-replica-seed-wal-archive-verified-postgresArchive-*.json")[0])
    archive = {"serverName": f"hdd-pg-seed-{token[:8]}", "before": archive_phase.get("before"),
               "after": archive_phase.get("after"), "distinctFromRetainedSource": True}
    job = data["job"]
    snapshot_records = data["snapshotRecords"]
    disk_records = data["diskRecords"]
    verified = now()
    audit_path, prep_path = RECOVERY / "prepare-audit.json", STATE_DIR / "prepare.json"
    require(not audit_path.exists() and not prep_path.exists(), "Final receipt appeared during execution; refusing overwrite.")
    audit = {
        "version": 1, "status": "complete", "verifiedAtUtc": verified,
        "ownerToken": token, "subscriptionId": state["subscriptionId"], "resourceGroup": state["resourceGroup"],
        "nodeResourceGroup": state["nodeResourceGroup"], "retainedSnapshotResourceGroup": state["snapshotResourceGroup"],
        "source": {"dataSnapshotId": manifest["sourceSnapshotId"],
                   "walSnapshotId": manifest["pairedWAL"]["sourceSnapshotId"],
                   "backupName": manifest["backup"]["name"], "backupUid": manifest["backup"]["uid"],
                   "backupEndLSN": manifest["backup"]["endLSN"], "bindings": source_bindings,
                   "readerAssignments": assignments,
                   "preparedTupleEstimates": {"live": snapshot["preparedLiveTuplesEstimate"],
                                              "dead": snapshot["preparedDeadTuplesEstimate"]}},
        "offlineSeedBackup": {"name": backup_name, "uid": data["backup"]["metadata"]["uid"],
                              "online": False, "majorVersion": 16,
                              "metadataFieldsRequired": ["phase", "method", "paired DATA/WAL elements"],
                              "replicaSourceEligible": True},
        "primaryAndReplicaSnapshots": {"data": data["snapshotIds"]["PG_DATA"],
                                       "wal": data["snapshotIds"]["PG_WAL"], "records": snapshot_records},
        "snapshotRecoveryJobUid": job["metadata"]["uid"], "replicaCaughtUpToLsn": primary["flushLsn"],
        "walArchive": archive, "fixture": primary, "replicaFixture": replica,
        "observedTupleEstimates": {"primaryLive": primary.get("liveEstimate"), "primaryDead": primary.get("deadEstimate")},
        "hydratedPremiumV2Disks": disk_records, "archiveSecretAbsent": archive_absent,
        "archiveWriterAbsent": True, "clientCreated": False, "noFixtureDML": True,
        "cluster": "repro/hdd-pg", "instances": 2, "measurementImage": IMAGE,
        "allSnapshotsRetained": True, "sourceSnapshotsUntouched": True,
        "retentionAudit": {"snapshotGroupAudits": data["retentionAudits"], "allDirectCanNotDelete": True},
        "replicaSnapshotRecoveryJob": {"uid": job["metadata"]["uid"], "name": job["metadata"]["name"],
                                        "role": "snapshot-recovery", "containerImage": SYSIMAGE,
                                        "command": "restoresnapshot --immediate", "completed": True},
        "finalization": {"bootstrapSourceRemoved": True, "recoveryTargetRemoved": True,
                         "volumeSnapshotLineagePreserved": True, "archiveReferencesRemoved": True,
                         "archiveSecretRemovedAfterReferences": True},
    }
    prep = {
        "version": 1, "status": "complete", "sourceSnapshotId": manifest["sourceSnapshotId"],
        "sourceKind": "paired-retained-cnp-snapshot", "systemIdentifier": EXPECTED_SYSID,
        "sourceSnapshotHeapBytes": snapshot["heapBytes"], "sourceSnapshotTableBytes": snapshot["tableBytes"],
        "fixtureTargetHeapGiB": 23, "fixtureTargetHeapBytes": snapshot["heapBytes"],
        "heapBytes": primary["heapBytes"], "tableBytes": primary["tableBytes"],
        "estimatedVacuumMinutes": round(23 * 260.583 / 110, 1),
        "preparedLiveTuplesEstimate": snapshot["preparedLiveTuplesEstimate"],
        "preparedDeadTuplesEstimate": snapshot["preparedDeadTuplesEstimate"],
        "replicaCaughtUpToLsn": primary["flushLsn"], "cluster": "repro/hdd-pg", "instances": 2,
        "availabilityZone": state["availabilityZone"], "retainedSnapshotResourceGroup": state["snapshotResourceGroup"],
        "storage": "four separate 512-GiB PremiumV2_LRS DATA/WAL disks at 3000 IOPS / 125 MB/s",
        "backup": backup_name, "replicaSnapshots": {"data": data["snapshotIds"]["PG_DATA"],
                                                      "wal": data["snapshotIds"]["PG_WAL"]},
        "measurementImage": IMAGE, "clientCreated": False,
        "finalAudit": {"verifiedAtUtc": verified, "receipt": str(audit_path),
                       "archiveSecretAbsent": archive_absent, "archiveWriterAbsent": True,
                       "ownedRetentionSnapshotsReady": True, "allFourPremiumV2DisksHydrated": True,
                       "sourceLineageVerified": True, "snapshotSeededReplica": True,
                       "finalizationPhase": "finalize"},
    }
    audit_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    for path, value in ((audit_path, audit), (prep_path, prep)):
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.write(fd, canon(value)); os.fsync(fd)
        finally:
            os.close(fd)
    for parent in (audit_path.parent, prep_path.parent):
        fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    journal.event("finalize-preparation-complete", "run", CLUSTER,
                  f"prepare={prep_path} audit={audit_path}; no fixture DML/client work")
    print(f"Prepared snapshot-seeded baseline finalized; receipts: {prep_path} and {audit_path}")


def apply(runner: Runner, journal: Journal, data: dict[str, Any]) -> None:
    require(data["storage"].get("ownerToken") == data["state"].get("ownerToken"), "Run storage token changed.")
    validate_live(runner, journal, data, before_finalize=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="run final read-only gates and exact cleanup patch")
    parser.add_argument("--owner-token", help="exact stack token; required with --apply")
    args = parser.parse_args()
    journal = Journal()
    try:
        data = local_preflight(args.owner_token)
        state = data["state"]
        plan = {"mode": "apply" if args.apply else "local-dry-run", "phase": "finalize",
                "run": HERE.name, "ownerToken": state["ownerToken"],
                "clusterUid": data["clusterEvidence"]["metadata"]["uid"],
                "backupUid": data["backupEvidence"].get("metadata", {}).get("uid"),
                "snapshotRecoveryJobUid": data["job"]["metadata"]["uid"],
                "seedSnapshotIds": {k: v[1].get("id") for k, v in data["seedSnapshots"].items()},
                "rootAcceptedPatchProof": str(data["rootDryrunPath"]),
                "actions": ["revalidate four 100%-hydrated disks, fixture/GUCs, image, replica catch-up, and all snapshot locks",
                            "remove only completed bootstrap source/recoveryTarget and temporary archive refs via UID/RV JSON patch",
                            "delete only the run-owned archive Secret after refs are absent", "write final prepare/audit receipts"],
                "noNewBackupSnapshotDiskOrFixtureDML": True, "failurePolicy": "preserve resources and evidence; root review required"}
        if not args.apply:
            inputs = [STATE_DIR / "state.json", data["storagePath"], HERE / "source-manifest.json",
                      HERE / "snapshot.json", HERE / "run-config.json", RECOVERY / "source-bindings.json",
                      data["clusterEvidencePath"], data["backupEvidencePath"], data["jobPath"],
                      data["jobEventPath"], data["primaryFixturePath"], data["replicaFixturePath"],
                      data["podsEvidencePath"], data["rootDryrunPath"]]
            proof = {"version": 1, "phase": "finalize", "createdAtUtc": now(),
                     "ownerToken": state["ownerToken"], "helperSha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                     "inputPaths": [str(path.relative_to(HERE)) for path in inputs],
                     "inputSha256": {str(path.relative_to(HERE)): hashlib.sha256(path.read_bytes()).hexdigest()
                                     for path in inputs},
                     "remoteActionsPerformed": False}
            proof_path = journal.proof("finalize-dryrun-proof", proof)
            plan["dryRunProof"] = str(proof_path)
            print(json.dumps(plan, indent=2)); return 0
        require(args.owner_token == state["ownerToken"], "Apply requires exact --owner-token.")
        kubeconfig = Path(state["kubeconfigPath"])
        require(kubeconfig.is_file(), "Pinned kubeconfig does not exist.")
        runner = Runner(state["subscriptionId"], kubeconfig)
        apply(runner, journal, data)
        return 0
    except (Refusal, OSError, KeyError, TypeError, ValueError, json.JSONDecodeError, subprocess.SubprocessError) as exc:
        try:
            journal.event("finalize-failed", "run", HERE.name,
                          f"{str(exc)[:700]}; preserve all cloud resources and receipts; root review required")
        except OSError:
            pass
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
