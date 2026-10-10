#!/usr/bin/env python3
"""Build evidence-linked per-stack experiment timelines from local receipts.

The collector is intentionally read-only with respect to experiment state. It
writes only recovery/timelines/<stack>/timeline.{json,md}; it does not query
Azure/Kubernetes or touch credentials. Run again after each experiment phase to
include newly written final measurements, WAL-export receipts, and cleanup
receipts.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "recovery/timelines"
STACKS = ("baseline-23gib", "50gib", "100gib")


def read(path: str | Path, default: Any = None) -> Any:
    p = ROOT / path if not isinstance(path, Path) or not path.is_absolute() else path
    try:
        return json.loads(p.read_text())
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


def rel(path: str | Path) -> str:
    p = path if isinstance(path, Path) else ROOT / path
    try:
        return p.resolve().relative_to(ROOT.resolve()).as_posix()
    except ValueError:
        return str(path)


def evidence(*paths: str | Path | None) -> list[dict[str, str]]:
    rows = []
    seen = set()
    for item in paths:
        if not item:
            continue
        p = item if isinstance(item, Path) else ROOT / item
        if not p.is_file():
            continue
        name = rel(p)
        if name in seen:
            continue
        seen.add(name)
        rows.append({"path": name, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()})
    return rows


def text_first(path: str | Path, needle: str, *, before: str | None = None) -> str | None:
    p = ROOT / path if not isinstance(path, Path) or not path.is_absolute() else path
    try:
        lines = p.read_text(errors="replace").splitlines()
    except OSError:
        return None
    for line in lines:
        if needle in line:
            if before and before in line:
                return line.split(before, 1)[0].strip()[:220]
            return line.strip()[:220]
    return None


def text_last_error(path: str | Path) -> str | None:
    p = ROOT / path if not isinstance(path, Path) or not path.is_absolute() else path
    try:
        lines = p.read_text(errors="replace").splitlines()
    except OSError:
        return None
    matches = [line.strip() for line in lines if "ERROR:" in line or "RECOVERY BLOCKED:" in line]
    return matches[-1][:220] if matches else None


def add(events: list[dict[str, Any]], event_id: str, title: str,
        time_utc: str | None, precision: str, details: str,
        paths: list[str | Path | None] | None = None,
        *, end_utc: str | None = None, start_utc: str | None = None) -> None:
    row: dict[str, Any] = {
        "id": event_id,
        "title": title,
        "timeUtc": time_utc,
        "timePrecision": precision,
        "details": details,
        "evidence": evidence(*(paths or [])),
    }
    if start_utc:
        row["startUtc"] = start_utc
    if end_utc:
        row["endUtc"] = end_utc
    events.append(row)


def canonical_resource_id(value: Any) -> str:
    return str(value or "").rstrip("/").lower()


def require_equal(actual: Any, expected: Any, description: str) -> None:
    if actual != expected:
        raise RuntimeError(f"paired snapshot provenance mismatch for {description}: {actual!r} != {expected!r}")


def add_paired_arm_snapshot_creation_events(
        stack: str, events: list[dict[str, Any]], prep: dict[str, Any], backup: dict[str, Any],
        audit: dict[str, Any], backup_path: str, audit_path: str, vss_path: str,
        vss_doc: dict[str, Any]) -> None:
    """Add exact ARM creation times only for the prepared DATA/WAL snapshot pair."""
    expected_pair = prep.get("replicaSnapshots") or {}
    require_equal(set(expected_pair), {"data", "wal"}, f"{stack} prepared DATA/WAL names")
    backup_meta = backup.get("metadata", {}) or {}
    backup_name = backup_meta.get("name")
    require_equal(prep.get("backup"), backup_name, f"{stack} prepare Backup name")
    if stack == "baseline-23gib":
        require_equal(audit.get("backup"), backup_name, f"{stack} finalizer Backup name")
        require_equal(audit.get("replicaSnapshots"), expected_pair, f"{stack} finalizer snapshot names")
    else:
        audit_pair = audit.get("backupSnapshotIdentities", {}) or {}
        require_equal(set(audit_pair), set(expected_pair.values()), f"{stack} finalizer snapshot names")

    vss_items = vss_doc.get("items", []) if isinstance(vss_doc, dict) else []
    vss_by_name = {(x.get("metadata", {}) or {}).get("name"): x for x in vss_items}
    raw_rows: dict[str, tuple[dict[str, Any], Path, dict[str, Any], Path]] = {}

    if stack == "baseline-23gib":
        content_doc = read("baseline-23gib/.state/recovery/volumesnapshotcontent.json", {}) or {}
        content_by_ref = {
            ((x.get("spec", {}) or {}).get("volumeSnapshotRef", {}) or {}).get("name"): x
            for x in content_doc.get("items", [])
        }
        retained_by_id = {
            canonical_resource_id(x.get("id")): x
            for x in audit.get("retainedSnapshots", []) if x.get("id")
        }
        for role in ("data", "wal"):
            name = expected_pair[role]
            vss = vss_by_name.get(name) or {}
            vss_meta = vss.get("metadata", {}) or {}
            vsc = content_by_ref.get(name) or {}
            vsc_status = vsc.get("status", {}) or {}
            arm_id = vsc_status.get("snapshotHandle")
            arm_path = f"baseline-23gib/.state/recovery/snapshot-{vss_meta.get('uid')}.json"
            arm = read(arm_path, {}) or {}
            arm_props = arm.get("properties", {}) or {}
            retained = retained_by_id.get(canonical_resource_id(arm.get("id")), {})
            require_equal(name in vss_by_name, True, f"{stack} {role} VSS name")
            require_equal(vss_meta.get("uid"), (vsc.get("spec", {}) or {}).get("volumeSnapshotRef", {}).get("uid"),
                          f"{stack} {role} VSS/VSC UID")
            require_equal(vsc.get("metadata", {}).get("name"), vss.get("status", {}).get("boundVolumeSnapshotContentName"),
                          f"{stack} {role} VSC name")
            require_equal(canonical_resource_id(arm.get("id")), canonical_resource_id(arm_id), f"{stack} {role} ARM ID/VSC handle")
            require_equal(canonical_resource_id(retained.get("id")), canonical_resource_id(arm.get("id")),
                          f"{stack} {role} ARM ID/finalizer receipt")
            require_equal(arm_props.get("uniqueId"), retained.get("uniqueId"), f"{stack} {role} ARM uniqueId/finalizer receipt")
            require_equal(arm_props.get("timeCreated"), retained.get("timeCreated"), f"{stack} {role} ARM time/finalizer receipt")
            source_volume = ((vsc.get("spec", {}) or {}).get("source", {}) or {}).get("volumeHandle")
            require_equal(arm_props.get("creationData", {}).get("sourceResourceId"), source_volume,
                          f"{stack} {role} ARM source disk/VSC")
            raw_rows[role] = (arm, ROOT / arm_path, vss, ROOT / "baseline-23gib/.state/recovery/volumesnapshotcontent.json")
    else:
        capture_dir = ROOT / "recovery/backup-retention-review/evidence" / stack / str(backup_name)
        capture_path = capture_dir / "capture.json"
        capture = read(capture_path, {}) or {}
        require_equal(capture.get("status"), "complete", f"{stack} lineage capture status")
        require_equal(capture.get("stack"), stack, f"{stack} lineage capture stack")
        capture_backup = capture.get("backup", {}) or {}
        require_equal(capture_backup.get("name"), backup_name, f"{stack} captured Backup name")
        require_equal(capture_backup.get("uid"), backup_meta.get("uid"), f"{stack} captured Backup UID")
        require_equal(capture_backup.get("namespace"), backup_meta.get("namespace"), f"{stack} captured Backup namespace")
        require_equal(capture_backup.get("phase"), "completed", f"{stack} captured Backup phase")
        require_equal(capture.get("replicaSnapshots"), expected_pair, f"{stack} captured snapshot names")
        require_equal((capture.get("cluster", {}) or {}).get("ownerVerified"), True, f"{stack} captured cluster owner")
        require_equal((capture.get("namespace", {}) or {}).get("ownerVerified"), True, f"{stack} captured namespace owner")

        source_receipts = capture.get("sourceReceipts", {}) or {}
        source_paths = {
            "prepareMetadataSha256": ROOT / stack / ".state/prepare.json",
            "prepareProcessSha256": ROOT / stack / ".state/prepare-process.json",
            "prepareScriptSha256": ROOT / stack / "02-prepare.sh",
            "sourceManifestSha256": ROOT / stack / "source-manifest.json",
            "sourceStageHelperSha256": ROOT / "recovery/source-restore/stage-allowlisted-source.sh",
            "stateSha256": ROOT / stack / ".state/state.json",
        }
        for key, path in source_paths.items():
            require_equal(source_receipts.get(key), hashlib.sha256(path.read_bytes()).hexdigest(),
                          f"{stack} source receipt {key}")
        receipt_path = ROOT / str(source_receipts.get("sourceStageReceiptPath", ""))
        require_equal(source_receipts.get("sourceStageReceiptSha256"), hashlib.sha256(receipt_path.read_bytes()).hexdigest(),
                      f"{stack} source-stage receipt hash")

        lineage_by_name = {x.get("snapshot_name"): x for x in capture.get("snapshotLineage", [])}
        for role in ("data", "wal"):
            name = expected_pair[role]
            lineage = lineage_by_name.get(name) or {}
            vss = vss_by_name.get(name) or {}
            vss_meta = vss.get("metadata", {}) or {}
            vss_spec = vss.get("spec", {}) or {}
            vsc_path = capture_dir / f"volumesnapshotcontent-{role}.json"
            arm_path = capture_dir / f"azure-snapshot-{role}.json"
            vsc = read(vsc_path, {}) or {}
            arm = read(arm_path, {}) or {}
            vsc_meta = vsc.get("metadata", {}) or {}
            vsc_spec = vsc.get("spec", {}) or {}
            vsc_status = vsc.get("status", {}) or {}
            vsc_ref = vsc_spec.get("volumeSnapshotRef", {}) or {}
            arm_creation = arm.get("creationData", {}) or {}
            audit_identity = audit_pair.get(name, {})
            require_equal(lineage.get("snapshot_name"), name, f"{stack} {role} captured VSS name")
            require_equal(lineage.get("snapshot_uid"), vss_meta.get("uid"), f"{stack} {role} captured VSS UID")
            require_equal(lineage.get("snapshot_uid"), vsc_ref.get("uid"), f"{stack} {role} VSS/VSC UID")
            require_equal(lineage.get("snapshot_content_name"), vsc_meta.get("name"), f"{stack} {role} VSC name")
            require_equal(lineage.get("snapshot_content_uid"), vsc_meta.get("uid"), f"{stack} {role} VSC UID")
            require_equal(lineage.get("source_pvc"), vss_spec.get("source", {}).get("persistentVolumeClaimName"),
                          f"{stack} {role} source PVC")
            source_volume = (vsc_spec.get("source", {}) or {}).get("volumeHandle")
            require_equal(lineage.get("source_volume_handle"), source_volume, f"{stack} {role} source disk/VSC")
            require_equal(arm_creation.get("sourceResourceId"), source_volume, f"{stack} {role} source disk/ARM")
            require_equal(canonical_resource_id(lineage.get("azure_snapshot_id")),
                          canonical_resource_id(vsc_status.get("snapshotHandle")), f"{stack} {role} Azure ID/VSC handle")
            require_equal(canonical_resource_id(arm.get("id")), canonical_resource_id(lineage.get("azure_snapshot_id")),
                          f"{stack} {role} Azure ID/capture")
            require_equal(arm.get("uniqueId"), lineage.get("azure_snapshot_unique_id"), f"{stack} {role} Azure uniqueId")
            require_equal(canonical_resource_id(audit_identity.get("id")), canonical_resource_id(arm.get("id")),
                          f"{stack} {role} Azure ID/finalizer audit")
            require_equal(audit_identity.get("uniqueId"), arm.get("uniqueId"), f"{stack} {role} Azure uniqueId/finalizer audit")
            require_equal(audit_identity.get("volumeSnapshotUID"), vss_meta.get("uid"), f"{stack} {role} VSS UID/finalizer audit")
            require_equal(audit_identity.get("content"), vsc_meta.get("name"), f"{stack} {role} VSC name/finalizer audit")
            require_equal(audit_identity.get("sourcePVC"), lineage.get("source_pvc"), f"{stack} {role} source PVC/finalizer audit")
            require_equal(canonical_resource_id(audit_identity.get("sourceDiskId")), canonical_resource_id(source_volume),
                          f"{stack} {role} source disk/finalizer audit")
            require_equal(audit_identity.get("sourceDiskUniqueId"), arm_creation.get("sourceUniqueId"),
                          f"{stack} {role} source disk UID/finalizer audit")
            require_equal(audit_identity.get("createOption"), arm_creation.get("createOption"),
                          f"{stack} {role} copy mode/finalizer audit")
            require_equal(audit_identity.get("completionPercent"), 100.0, f"{stack} {role} copy completion/finalizer audit")
            require_equal(vsc_ref.get("name"), name, f"{stack} {role} VSC reference name")
            raw_rows[role] = (arm, arm_path, vss, vsc_path)

    for role in ("data", "wal"):
        arm, arm_path, vss, vsc_path = raw_rows[role]
        arm_props = arm.get("properties", {}) or arm
        arm_id = arm.get("id")
        arm_unique_id = arm_props.get("uniqueId") or arm.get("uniqueId")
        time_created = arm_props.get("timeCreated") or arm.get("timeCreated")
        require_equal(bool(arm_id and arm_unique_id and time_created), True, f"{stack} {role} ARM creation fields")
        vss_meta = vss.get("metadata", {}) or {}
        snapshot_name = Path(str(arm_id)).name
        add(events, f"backup-arm-snapshot-created-{role}",
            f"Azure paired Backup {role.upper()} snapshot resource created: {snapshot_name}",
            time_created, "exact-ARM-resource-creation-time",
            f"ARM snapshot uniqueId {arm_unique_id}; linked through the prepared {role.upper()} Backup snapshot name and VSS UID {vss_meta.get('uid')}. This is the Azure resource creation timestamp, not copy completion or CSI SnapshotReady time.",
            [backup_path, audit_path, f"{stack}/.state/prepare.json", vss_path, arm_path, vsc_path])


def sort_key(row: dict[str, Any]) -> tuple[str, str]:
    return (row.get("timeUtc") or row.get("startUtc") or "9999", row["id"])


def process_attempts(stack: str, events: list[dict[str, Any]]) -> None:
    base = ROOT / stack / ".state"
    candidates = list(base.glob("attempts/prepare-*/prepare-process*.json"))
    current = base / "prepare-process.json"
    if current.exists():
        candidates.append(current)
    seen: set[tuple[Any, ...]] = set()
    for p in sorted(candidates):
        obj = read(p, {}) or {}
        key = (obj.get("startedAtUtc"), obj.get("supervisorPid"), obj.get("childPid"), obj.get("status"))
        if key in seen:
            continue
        seen.add(key)
        attempt_dir = p.parent.name
        phase_num = attempt_dir.removeprefix("prepare-") if attempt_dir.startswith("prepare-") else "current"
        if p.name == "prepare-process-original.json":
            phase_num += "-original"
        log = p.with_name("prepare.log")
        terminal_log = ROOT / "recovery/backup-wait-resume/original-100gib/original-prepare-terminal.log"
        reason_source = terminal_log if stack == "100gib" and phase_num == "5" and terminal_log.exists() else log
        reason = text_last_error(reason_source) if reason_source.exists() else None
        status = obj.get("status", "unknown")
        exit_code = obj.get("exitCode")
        desc = f"Preparation attempt {phase_num}: {status}"
        if stack == "100gib" and phase_num == "current":
            desc = f"Preparation continuation v2: {status}"
        if exit_code is not None:
            desc += f" (exit {exit_code})"
        if reason:
            desc += f"; {reason}"
        script_candidates = [p.with_name("02-prepare.sh"), p.with_name("02-prepare-original.sh")]
        script = next((x for x in script_candidates if x.exists()), None)
        event_key = (str(phase_num), str(obj.get("startedAtUtc") or ""))
        suffix = hashlib.sha256("|".join(event_key).encode()).hexdigest()[:8]
        add(events, f"prepare-attempt-{phase_num}-{suffix}", desc,
            obj.get("startedAtUtc"), "process-receipt-interval",
            "Supervisor receipt gives the exact process start/finish bounds; internal SQL statement times are not logged.",
            [p, log if log.exists() else None, terminal_log if reason_source == terminal_log else None,
             script], end_utc=obj.get("finishedAtUtc"))


def source_and_provision(stack: str, events: list[dict[str, Any]]) -> None:
    state = f"{stack}/.state"
    launch = read(f"{state}/provision-launch.json", {}) or {}
    add(events, "provision-launch", "Provisioning process launched",
        launch.get("startedAtUtc"), "exact-process-receipt",
        "This is the local provisioning-launch timestamp. See the captured provision log for its terminal result; resource creation timestamps below are independent Kubernetes evidence.",
        [f"{state}/provision-launch.json", f"{stack}/01-provision.sh", f"{stack}/provision.log"])
    provision_log = ROOT / stack / "provision.log"
    if provision_log.exists() and "Cannot inspect AKS provisioning" in provision_log.read_text(errors="replace"):
        add(events, "provision-inspection-failure", "Provision launch reported AKS inspection failure; owned snapshots were preserved",
            launch.get("startedAtUtc"), "start-time-only-terminal-time-unavailable",
            "The retained stdout records `Cannot inspect AKS provisioning` and deferred cleanup because snapshot deletion was not authorized. No exact failure or cleanup timestamp was retained; the cluster and PVC creation records below document later resource state.",
            [f"{state}/provision-launch.json", f"{stack}/provision.log", f"{stack}/01-provision.sh"])

    source = read(f"{state}/source-snapshot.json", {}) or {}
    manifest = source.get("manifest", {}) or {}
    add(events, "retained-source-snapshot", "Allowlisted prepared source snapshot created",
        manifest.get("timeCreated"), "ARM-resource-creation-time",
        f"Existing source snapshot {Path(source.get('sourceSnapshotId', '')).name}; its identity is selected by the pinned source manifest. The local source metadata records a keeper → staging disk → regional incremental snapshot lineage; historical fixture properties are inherited from that chain, not direct measurements of this stack.",
        [f"{state}/source-snapshot.json", f"{stack}/source-manifest.json"])

    fixture = (read(f"{stack}/results/preflight.json", {}) or {}).get("fixture", {}) or {}
    if fixture.get("createdAtUtc"):
        keeper_id = fixture.get("snapshotId", "")
        original_id = fixture.get("originalKeeperSnapshotId", "")
        add(events, "historical-prepared-keeper-created", "Historical fixture creation timestamp inherited from source metadata",
            fixture.get("createdAtUtc"), "inherited-source-metadata-timestamp",
            f"The inherited createdAtUtc field accompanies originalKeeperSnapshotId={Path(original_id).name} and snapshotId={Path(keeper_id).name}. It is not an independently verified ARM creation time for the selected later keeper/copy. The historical source topology is {fixture.get('sourceTopology', 'topology not recorded')}; prepared live/dead tuple counts are inherited estimates. The allowlisted source manifest and saved source-snapshot provenance record the intermediate copy lineage used by this run.",
            [f"{stack}/results/preflight.json", f"{state}/source-snapshot.json",
             f"{stack}/source-manifest.json"])

    receipt_paths = list((ROOT / state / "source-stage").glob("receipt-*.json"))
    if receipt_paths:
        receipt_path = receipt_paths[0]
        receipt = read(receipt_path, {}) or {}
        disk = receipt.get("stagingDiskMetadata", {}) or {}
        snap = receipt.get("restoreSnapshotMetadata", {}) or {}
        add(events, "stage-disk-created", "Owned Standard staging disk created",
            disk.get("timeCreated"), "ARM-resource-creation-time",
            f"Temporary source restore disk {Path(receipt.get('stagingDiskId', '')).name}; copy completion for Standard sources is not exposed reliably.",
            [receipt_path, f"{state}/source-stage/plan-{receipt.get('resourceSuffix')}.json"])
        add(events, "restore-copy-snapshot-created", "Owned incremental restore snapshot created",
            snap.get("timeCreated"), "ARM-resource-creation-time",
            f"Restore copy {Path(receipt.get('restoreSnapshotId', '')).name}; preserved under CanNotDelete snapshot retention.",
            [receipt_path])
    else:
        raw = read(f"{state}/recovery/keeper-copy-arm.json", {}) or {}
        props = raw.get("properties", {}) or {}
        add(events, "restore-copy-snapshot-created", "Owned restore-copy snapshot created",
            props.get("timeCreated"), "ARM-resource-creation-time",
            f"Restore copy {Path(raw.get('id', '')).name}; no Standard staging disk was recorded for the baseline stack.",
            [f"{state}/recovery/keeper-copy-arm.json", f"{state}/prepare.json"])

    verified = read(f"{state}/provision-verified.json", {}) or {}
    if verified.get("verifiedAtUtc"):
        add(events, "provision-verified", "Provisioning verified",
            verified.get("verifiedAtUtc"), "exact-local-verification-receipt",
            "Local provisioning verification receipt.", [f"{state}/provision-verified.json"])

    # Resource creation timestamps survive longer than the provisioner's
    # terminal log and identify when the primary cluster and its initial DATA
    # volume became Kubernetes resources. WAL is deliberately tracked below as
    # a later, separate-storage transition.
    if stack == "baseline-23gib":
        cluster_path = f"{state}/recovery/cluster.json"
        pvc_path = f"{state}/recovery/pvc.json"
    elif stack == "50gib":
        cluster_path = "recovery/fixture-watch/50-finalization/live/cluster.json"
        pvc_path = "recovery/fixture-watch/50-finalization/live/pvcs.json"
    else:
        cluster_path = "recovery/fixture-watch/100-wal-move-20261007T054847Z/cluster.json"
        pvc_path = "recovery/fixture-watch/100-wal-move-20261007T055516Z/pvcs.json"
    for path, kind, wanted in ((cluster_path, "Cluster", "hdd-pg"), (pvc_path, "primary DATA PVC", "hdd-pg-1")):
        doc = read(path, {}) or {}
        items = doc.get("items", []) if isinstance(doc, dict) else []
        item = next((x for x in items if (x.get("metadata", {}) or {}).get("name") == wanted), None)
        meta = (item or {}).get("metadata", {}) or {}
        if meta.get("creationTimestamp"):
            add(events, f"resource-created-{kind.lower().replace(' ', '-')}", f"Initial {kind} resource created",
                meta["creationTimestamp"], "exact-kubernetes-creation-timestamp",
                f"Resource {wanted} creation timestamp from the preserved Kubernetes object capture.", [path])

    if stack == "baseline-23gib":
        primary_pod_path = f"{state}/recovery/pods.json"
    elif stack == "50gib":
        primary_pod_path = "recovery/fixture-watch/50-finalization/live/pods.json"
    else:
        primary_pod_path = "recovery/fixture-watch/100-wal-move-20261007T054847Z/pods.json"
    pod_doc = read(primary_pod_path, {}) or {}
    primary_pod = next((x for x in pod_doc.get("items", [])
                        if (x.get("metadata", {}) or {}).get("name") == "hdd-pg-1"), None)
    primary_meta = (primary_pod or {}).get("metadata", {}) or {}
    if primary_meta.get("creationTimestamp"):
        add(events, "primary-pod-created", "Primary PostgreSQL pod created",
            primary_meta["creationTimestamp"], "exact-kubernetes-creation-timestamp",
            f"Primary pod UID {primary_meta.get('uid')} in the preserved K8s capture. Later WAL-migration restarts are listed separately where captured.",
            [primary_pod_path])

    if stack == "baseline-23gib":
        pvc_path = f"{state}/recovery/pvc.json"
    elif stack == "50gib":
        pvc_path = "recovery/fixture-watch/50-finalization/live/pvcs.json"
    else:
        pvc_path = "recovery/fixture-watch/100-wal-move-20261007T055516Z/pvcs.json"
    pvc_doc = read(pvc_path, {}) or {}
    for pvc in pvc_doc.get("items", []) if isinstance(pvc_doc, dict) else []:
        meta = pvc.get("metadata", {}) or {}
        if meta.get("name") == "hdd-pg-1-wal" and meta.get("creationTimestamp"):
            add(events, "separate-wal-pvc-created", "Separate primary WAL PVC created",
                meta["creationTimestamp"], "exact-kubernetes-creation-timestamp",
                "The WAL claim has its own creation time and is separate from the prepared DATA claim.", [pvc_path])
            break

    if stack == "100gib":
        folder = "recovery/fixture-watch/100-wal-move-20261007T054847Z"
        add(events, "wal-migration-primary-restarted", "Primary restarted onto the separate WAL claim",
            "2026-10-07T05:15:34Z", "exact-kubernetes-event-timestamp",
            "The preserved K8s event capture records the prior primary container being stopped for the WAL-storage transition; later captures show the primary running with both DATA and WAL claims attached.",
            [f"{folder}/events.json", f"{folder}/pods.json", "recovery/fixture-watch/100-wal-move-20261007T055516Z/pvcs.json"])
        add(events, "wal-migration-new-pvc-bound", "Separate WAL PVC provisioned and attached",
            "2026-10-07T05:44:30Z", "exact-kubernetes-event-timestamp",
            "The storage provisioner reported success for the new hdd-pg-1-wal claim; subsequent pod events record the claims attached and PostgreSQL container started.",
            [f"{folder}/events.json", f"{folder}/pods.json", "recovery/fixture-watch/100-wal-move-20261007T055516Z/pvcs.json"])


def backup_and_seed(stack: str, events: list[dict[str, Any]]) -> None:
    if stack == "baseline-23gib":
        backup_path = "baseline-23gib/.state/recovery/backup.json"
        vss_path = "baseline-23gib/.state/recovery/volumesnapshot.json"
        vss_doc = read(vss_path, {}) or {}
        audit_path = "baseline-23gib/.state/recovery/baseline-finalize-audit.json"
        audit = read(audit_path, {}) or {}
        source_start = "baseline-23gib/.state/attempts/prepare-1/prepare-process.json"
        copy_watch = "baseline-23gib/.state/recovery/baseline-watch/copy-progress-latest.json"
        event_path = None
    elif stack == "50gib":
        backup_path = "recovery/fixture-watch/50-finalization/live/backups.json"
        vss_path = "recovery/fixture-watch/50-finalization/live/volumesnapshots.json"
        audit_path = "recovery/numeric-hydration-finalizer/results/50gib/audit.json"
        audit = read(audit_path, {}) or {}
        source_start = "50gib/.state/attempts/prepare-4/prepare-process-original.json"
        copy_watch = "recovery/numeric-hydration-finalizer/results/50gib/audit.json"
        vss_doc = read(vss_path, {}) or {}
        event_path = "recovery/fixture-watch/50-finalization/live/events.json"
    else:
        backup_path = "recovery/fixture-watch/100-backup-watch-20261007T071457Z/backup.json"
        vss_path = "recovery/fixture-watch/100-backup-watch-20261007T071457Z/volumesnapshots.json"
        audit_path = "recovery/backup-wait-resume/final-audit/100gib-v2/audit.json"
        audit = read(audit_path, {}) or {}
        source_start = "100gib/.state/attempts/prepare-5/prepare-process.json"
        copy_watch = "preflight/snapshot-audit-100gib-continuation-preflight.json"
        vss_doc = read(vss_path, {}) or {}
        event_path = "recovery/fixture-watch/100-backup-watch-20261007T071457Z/events.json"

    backup_doc = read(backup_path, {}) or {}
    backups = backup_doc.get("items", []) if isinstance(backup_doc, dict) else []
    if not backups and isinstance(backup_doc, dict) and backup_doc.get("kind") == "Backup":
        backups = [backup_doc]
    backup = backups[0] if backups else {}
    bm = backup.get("metadata", {}) or {}
    bs = backup.get("status", {}) or {}
    started = bs.get("startedAt") or bs.get("reconciliationStartedAt") or bm.get("creationTimestamp")
    ended = bs.get("stoppedAt") or bs.get("reconciliationTerminatedAt")
    if not started:
        # The v2 continuation receipt retains the exact CNPG Backup creation time.
        cont = read("recovery/backup-wait-resume/results/100gib-v2/continuation-evidence.json", {}) or {}
        started = cont.get("backup", {}).get("createdAt")
    if started:
        identity = bm.get("name") or (read("baseline-23gib/.state/prepare.json", {}) or {}).get("backup")
        range_note = ""
        if bs.get("beginLSN") or bs.get("endLSN"):
            range_note = f" Backup WAL range {bs.get('beginLSN')}–{bs.get('endLSN')} (segments {bs.get('beginWal')}–{bs.get('endWal')})."
        add(events, "paired-backup", "CNPG online paired DATA/WAL Backup started and completed",
            started, "exact-CNPG-status-time" if ended else "exact-object-creation-time",
            f"Backup {identity}; started {started}; completed {ended or 'completion timestamp unavailable in this object capture'}.{range_note}",
            [backup_path, vss_path, source_start], end_utc=ended)

    snapshots = vss_doc.get("items", []) if isinstance(vss_doc, dict) else []
    pair_names = set()
    prep = read(f"{stack}/.state/prepare.json", {}) or {}
    for name in (prep.get("replicaSnapshots") or {}).values():
        if name:
            pair_names.add(name)
    if stack == "100gib":
        pair_names.update(("vacuum-backup-f1b90aec", "vacuum-backup-f1b90aec-wal"))
    if stack == "50gib":
        pair_names.update(("vacuum-backup-5c241a19", "vacuum-backup-5c241a19-wal"))
    for snap in snapshots:
        m = snap.get("metadata", {}) or {}
        name = m.get("name", "")
        if pair_names and name not in pair_names:
            continue
        st = snap.get("status", {}) or {}
        creation = m.get("creationTimestamp") or st.get("creationTime")
        ready = st.get("readyToUse") is True
        if creation:
            add(events, f"backup-vss-{name}", f"Paired Backup VolumeSnapshot created: {name}",
                creation, "exact-kubernetes-resource-timestamp",
                f"VSS UID {m.get('uid')}; this timestamp is creation evidence. Later readiness is reported separately only where a SnapshotReady event was retained.",
                [vss_path])

    if backup and prep.get("replicaSnapshots"):
        add_paired_arm_snapshot_creation_events(
            stack, events, prep, backup, audit, backup_path, audit_path, vss_path, vss_doc)

    # Preserve explicit CSI/controller readiness events when captured. In
    # particular, DATA and WAL snapshots can become usable hours apart even
    # though one Backup owns the pair.
    if event_path:
        event_doc = read(event_path, {}) or {}
        for ev in event_doc.get("items", []) if isinstance(event_doc, dict) else []:
            if ev.get("reason") != "SnapshotReady":
                continue
            inv = ev.get("involvedObject", {}) or {}
            name = inv.get("name") or "snapshot"
            if pair_names and name not in pair_names:
                continue
            meta = ev.get("metadata", {}) or {}
            stamp = ev.get("firstTimestamp") or ev.get("eventTime") or meta.get("creationTimestamp")
            if stamp:
                add(events, f"backup-vss-ready-{name}", f"Paired Backup snapshot ready to use: {name}",
                    stamp, "exact-kubernetes-event-timestamp",
                    f"SnapshotReady event for VSS UID {inv.get('uid')}; readiness is later than snapshot object creation.", [event_path])

    if stack == "baseline-23gib":
        progress_paths = sorted(
            p for p in (ROOT / "baseline-23gib/.state/recovery/baseline-watch").glob("copy-progress-*.json")
            if p.name != "copy-progress-latest.json"
        )
        for p in progress_paths:
            sample = read(p, {}) or {}
            stamp = sample.get("capturedAtUtc")
            data_pct = (sample.get("data") or {}).get("completionPercent")
            wal_pct = (sample.get("wal") or {}).get("completionPercent")
            if stamp is not None:
                add(events, f"measured-disk-copy-progress-{stamp}", "Measured DATA/WAL copy progress observed",
                    stamp, "first-observed-copy-progress",
                    f"DATA={data_pct}%; WAL={wal_pct}%. Progress was observed at this poll; it does not identify the exact transition instant.", [p])
    elif stack == "100gib":
        progress_paths = sorted((ROOT / "recovery/fixture-watch").glob("100-arm-snapshots-20261007T*/snapshots.json"))
        for p in progress_paths:
            sample = read(p, {}) or {}
            stamp = sample.get("observedAtUtc")
            snaps = {x.get("volumeSnapshotName", ""): x for x in sample.get("snapshots", [])}
            data = snaps.get("vacuum-backup-f1b90aec", {})
            wal = snaps.get("vacuum-backup-f1b90aec-wal", {})
            if stamp:
                add(events, f"backup-copy-progress-{stamp}", "Paired Backup snapshot copy progress observed",
                    stamp, "first-observed-copy-progress",
                    f"DATA={data.get('completionPercent')}%/{data.get('snapshotAccessState')}; WAL={wal.get('completionPercent')}%/{wal.get('snapshotAccessState')}. A captured 100% value is the first preserved observation, not the actual Azure transition time.", [p])

    if stack == "100gib":
        # The original prepare's one-hour wait failed at 07:11:40 while the same
        # Backup continued. The next owned snapshot watch observed both copies at
        # 100%; this is a first-observed bound, not an exact backend completion.
        oldproc = read("recovery/backup-wait-resume/original-100gib/original-process-terminal.json", {}) or {}
        add(events, "original-100-backup-timeout", "Original 100 GiB preparation stopped at its bounded Backup wait",
            oldproc.get("finishedAtUtc"), "exact-process-receipt",
            "Original 02 wait expired while its owned Backup was still finalizing; it did not enter the seed/image/final audit tail.",
            ["recovery/backup-wait-resume/original-100gib/original-process-terminal.json",
             "recovery/backup-wait-resume/original-100gib/original-02-prepare.sh",
             "100gib/.state/attempts/prepare-5/prepare.log"])
        v2 = read("recovery/backup-wait-resume/results/100gib-v2/resume-progress.json", {}) or {}
        if v2.get("backupPhase") == "completed":
            add(events, "existing-backup-completed-after-timeout", "Existing paired Backup completed after original timeout",
                "2026-10-07T07:13:20Z", "exact-kubernetes-event/status-time",
                "The continuation reused the existing Backup and snapshots; no second Backup or fixture DML was submitted.",
                ["recovery/fixture-watch/100-backup-watch-20261007T071457Z/backup.json",
                 "recovery/fixture-watch/100-backup-watch-20261007T071457Z/events.json",
                 "recovery/backup-wait-resume/results/100gib-v2/resume-progress.json"])

    # The original script's fixture section precedes Backup creation. Its SQL
    # command timestamps are not persisted except for the 100-GiB remote capture.
    prep_proc = read(source_start, {}) or {}
    end_bound = started
    if prep_proc.get("startedAtUtc") and end_bound:
        detail = "Frozen 02 control flow places INSERT, index build, DELETE, and CHECKPOINT before Backup creation. Exact SQL statement timestamps/output are absent from this stack's raw capture."
        if stack == "100gib":
            detail += " The initial remote capture at 05:11:36 contained CREATE INDEX output only; by 05:15:07 the captured output contained DELETE 32768000 and CHECKPOINT with status 0. The INSERT transcript remains unavailable."
        add(events, "fixture-dml-window", "Fixture INSERT/index/DELETE/CHECKPOINT phase",
            prep_proc.get("startedAtUtc"), "bounded-by-process-and-backup",
            detail,
            [source_start, f"{stack}/.state/attempts/prepare-{'1' if stack == 'baseline-23gib' else '4' if stack == '50gib' else '5'}/02-prepare.sh",
             "recovery/fixture-watch/100-dml-output/initial-20261007T051136Z/capture-manifest.json" if stack == "100gib" else None,
             "recovery/fixture-watch/100-dml-output/final-20261007T051507Z/capture-manifest.json" if stack == "100gib" else None],
            end_utc=end_bound)
    if stack == "100gib":
        initial = "recovery/fixture-watch/100-dml-output/initial-20261007T051136Z/capture-manifest.json"
        final = "recovery/fixture-watch/100-dml-output/final-20261007T051507Z/capture-manifest.json"
        add(events, "dml-create-index-observed", "CREATE INDEX output observed in active remote psql capture",
            "2026-10-07T05:11:36.534300Z", "capture-time-upper-bound",
            "Read-only capture saw CREATE INDEX in the active command output; the actual statement completion instant is not recorded.", [initial])
        add(events, "dml-delete-checkpoint-observed", "DELETE count, CHECKPOINT, and status 0 captured",
            "2026-10-07T05:15:07.274796Z", "capture-time-upper-bound",
            "Remote output was exactly CREATE INDEX / DELETE 32768000 / CHECKPOINT; status file was 0. Capture time bounds observation, not SQL completion. INSERT output was not captured.", [final])

    # WAL migration chronology is only included where an actual read-only
    # aggregate or PVC/pod capture exists; no phase is reconstructed from size.
    if stack == "50gib":
        wal_files = sorted((ROOT / "recovery/fixture-watch").glob("wal-migration-*.json"))
        for f in wal_files:
            obj = read(f, {}) or {}
            if obj.get("stack") != stack:
                continue
            cur = obj.get("currentSample") or obj
            observed = cur.get("observedAtUtc") or obj.get("observedAtUtc")
            dest = cur.get("destinationApparentBytes") or obj.get("destinationApparentBytes")
            if dest is None:
                dest = (cur.get("destinationPgWal") or obj.get("destinationPgWal") or {}).get("apparentBytes")
            if observed:
                add(events, f"wal-migration-{observed}", "Separate WAL migration progress observed",
                    observed, "exact-read-only-sample-time",
                    f"Aggregate source/destination byte sample; destination apparent bytes={dest}. This is read-only size evidence, not file-level proof.", [f])
    if stack == "100gib":
        folder = ROOT / "recovery/fixture-watch/100-wal-move-20261007T055516Z"
        if (folder / "pvcs.json").exists():
            add(events, "wal-migration-primary-ready", "Primary PostgreSQL running with separate DATA/WAL claims",
                "2026-10-07T05:44:57Z", "exact-kubernetes-event-timestamp",
                "Kubernetes event capture records PostgreSQL container start after the new WAL PVC was provisioned and attached. This is evidence of the transition; the data-migration command's per-file completion transcript was not retained.",
                [folder / "pvcs.json", folder / "events.json", folder / "pods.json", folder / "primary.log"])
        wal_log = ROOT / "recovery/fixture-watch/100-post-wal-20261007T061139Z/primary.log"
        if wal_log.exists():
            try:
                log_rows = [json.loads(line) for line in wal_log.read_text(errors="replace").splitlines() if line]
            except json.JSONDecodeError:
                log_rows = []
            for row in log_rows:
                record = row.get("record", {}) or {}
                msg = row.get("msg") or record.get("message", "")
                stamp = row.get("ts")
                if "database system is ready to accept connections" in msg and stamp:
                    add(events, "primary-ready-after-wal-restart", "Primary accepted connections after WAL-storage restart",
                        stamp, "exact-postgresql-log-timestamp",
                        "The primary log records PostgreSQL ready to accept connections on hdd-pg-1 after the WAL-storage restart and before the online Backup began.", [wal_log])
                if row.get("logger") == "wal-archive" and msg == "Archived WAL file" and stamp:
                    add(events, f"wal-archive-before-backup-{row.get('walName', 'segment')}", "WAL segment archived before paired Backup",
                        stamp, "exact-postgresql-archive-log-timestamp",
                        f"Barman logged successful archive of WAL segment {row.get('walName')}; this supports the configured `waitForArchive` Backup boundary. No credentials or object contents are included.", [wal_log])

    # Seed Job completion and replica resource timestamps come from final audit
    # receipts that pin the exact Job UID and Completed event.
    if stack == "baseline-23gib":
        seed = audit.get("snapshotRecoveryJobEvidence", {}) or {}
        seed_paths = [audit_path, "baseline-23gib/.state/recovery/pvcs.json", "baseline-23gib/.state/recovery/pods.json"]
        pvc_time = seed.get("replicaPVCsCreatedBy")
        pod_time = seed.get("currentReplicaPodCreatedAt")
        job_time = seed.get("eventTimestamp")
        job_uid = seed.get("uid")
    else:
        checks = audit.get("checks", {}) or {}
        seed = (checks.get("backupAndReplicaSeed", {}) or {}).get("seedJob", {}) or {}
        seed_paths = [audit_path,
                      "recovery/numeric-hydration-finalizer/results/50gib/seed-events.json" if stack == "50gib" else "recovery/backup-wait-resume/results/100gib-v2/seed-events.json"]
        pvcs = seed.get("replicaPVCs", [])
        pvc_time = min((x.get("creationTimestamp") for x in pvcs if x.get("creationTimestamp")), default=None)
        pod_time = (seed.get("replicaPod", {}) or {}).get("creationTimestamp")
        job_time = seed.get("eventTimestamp")
        job_uid = seed.get("uid")
    if pvc_time:
        add(events, "replica-pvcs-created", "Replica DATA and WAL PVCs created",
            pvc_time, "exact-kubernetes-creation-timestamp",
            "Both replica PVC creation timestamps are pinned in the finalizer audit.", seed_paths)
    if job_time:
        add(events, "replica-seed-completed", "Snapshot recovery seed Job Completed event",
            job_time, "exact-kubernetes-event-timestamp",
            f"Job UID {job_uid}; completion is anchored to the Completed event rather than current-object existence after garbage collection.", seed_paths)
    if pod_time:
        add(events, "replica-pod-created", "Replica PostgreSQL pod created",
            pod_time, "exact-kubernetes-creation-timestamp",
            "Replica pod creation follows the pinned Job completion event in the finalizer audit.", seed_paths)


def finalization_gate(stack: str, events: list[dict[str, Any]]) -> None:
    if stack == "baseline-23gib":
        audit_path = "baseline-23gib/.state/recovery/baseline-finalize-audit.json"
        proc_path = "baseline-23gib/.state/prepare-process.json"
        receipt = read(audit_path, {}) or {}
        verified = receipt.get("verifiedAtUtc")
        process = read(proc_path, {}) or {}
        stamp = process.get("finishedAtUtc") or verified
        path = "baseline-23gib/.state/prepare.json"
    elif stack == "50gib":
        audit_path = "recovery/numeric-hydration-finalizer/results/50gib/audit.json"
        path = "recovery/numeric-hydration-finalizer/results/50gib/prepare.json"
        receipt = read(audit_path, {}) or {}
        verified = receipt.get("verifiedAtUtc")
        stamp = verified
    else:
        audit_path = "recovery/backup-wait-resume/final-audit/100gib-v2/audit.json"
        path = "recovery/backup-wait-resume/final-audit/100gib-v2/prepare.json"
        receipt = read(audit_path, {}) or {}
        verified = receipt.get("verifiedAtUtc")
        stamp = verified
    if stamp:
        add(events, "preparation-finalized", "Preparation finalizer exit 0; four separate DATA/WAL disks gated",
            stamp, "exact-finalizer-receipt-time",
            f"All four measured PremiumV2 disks passed numeric hydration checks and replica catch-up; audit verifiedAtUtc={verified}.",
            [audit_path, path])
        add(events, "premiumv2-hydration-100", "Measured PremiumV2 disk copies verified at 100%",
            verified, "first-complete-audit-observation",
            "The audit confirms every measured DATA Copy disk at 100%, DATA Empty disk with null progress, and every WAL/replica disk at 100% where copied. Exact underlying transition time is not recorded.",
            [audit_path])

        hydrated = receipt.get("hydratedPremiumV2Disks")
        if hydrated is None:
            hydrated = (receipt.get("checks", {}) or {}).get("hydratedPremiumV2Disks", [])
        if isinstance(hydrated, list):
            rendered = "; ".join(
                f"{Path(x.get('id', '')).name} {x.get('createOption')} {x.get('completionPercent')}%"
                for x in hydrated
            )
            add(events, "hydrated-four-disk-inventory", "Finalizer's four-disk hydration inventory",
                verified, "first-complete-audit-observation",
                f"Disk inventory at finalizer verification: {rendered}. Empty disks correctly have null copy progress. This records the first retained complete audit, not exact Azure hydration transition times.",
                [audit_path])

    if stack == "100gib":
        # These are periodic ARM reads. They show independent primary and
        # replica DATA/WAL copy progress without assigning backend completion
        # instants from asynchronous progress values.
        disk_paths = sorted((ROOT / "recovery/fixture-watch").glob("100-disks-20261007T*/summary.json"))
        for p in disk_paths:
            sample = read(p, {}) or {}
            stamp = sample.get("capturedAtUtc")
            by_claim = {x.get("claim"): x for x in sample.get("disks", [])}
            if not stamp or not by_claim:
                continue
            fields = []
            for claim in ("hdd-pg-1", "hdd-pg-1-wal", "hdd-pg-2", "hdd-pg-2-wal"):
                row = by_claim.get(claim)
                if row:
                    fields.append(f"{claim}={row.get('completionPercent')}% ({row.get('creationData', {}).get('createOption')})")
            add(events, f"replica-disk-copy-progress-{stamp}", "Primary/replica DATA and WAL disk progress observed",
                stamp, "first-observed-copy-progress",
                "; ".join(fields) + ". Progress values are first captured observations, not exact background transition times.", [p])

    retirement_path = "preflight/staging-retirement.json"
    retirement = read(retirement_path, {}) or {}
    if retirement.get("phase") == "complete":
        stamp = retirement.get("completedAtUtc")
        add(events, "staging-retired", "Temporary Standard staging disks retired; snapshots preserved",
            stamp, "exact-retirement-receipt-time",
            "Exactly the two owned unattached Standard staging disks were deleted. All before/after snapshot identities and lock protections were preserved.",
            [retirement_path, retirement.get("snapshotAudit", {}).get("beforePath"),
             retirement.get("snapshotAudit", {}).get("afterPath")])


def validate_teardown_observation(row: dict[str, Any], stack: str) -> tuple[dict[str, Any], Path]:
    run_id = row.get("runId")
    require_equal(bool(run_id), True, f"{stack} cleanup run ID")
    expected_path = f"recovery/cleanup-timeline/teardown-observations/{stack}-{run_id}.json"
    receipt_path = row.get("teardownReceiptPath")
    require_equal(receipt_path, expected_path, f"{stack} cleanup observation receipt path")
    receipt = read(receipt_path, {}) or {}
    require_equal(bool(receipt), True, f"{stack} cleanup observation receipt exists")
    for key in ("runId", "stack", "pid", "scriptSha256"):
        require_equal(receipt.get(key), row.get(key), f"{stack} {run_id} cleanup receipt {key}")
    expected_command = ["bash", str((ROOT / stack / "04-teardown.sh").resolve())]
    require_equal(receipt.get("command"), expected_command, f"{stack} {run_id} cleanup command")
    cleanup_script = ROOT / "post-measurement-cleanup-v2.py"
    require_equal(receipt.get("scriptSha256"), hashlib.sha256(cleanup_script.read_bytes()).hexdigest(),
                  f"{stack} {run_id} cleanup orchestrator hash")
    script_candidates = [ROOT / stack / "04-teardown.sh",
                         ROOT / "recovery/cleanup-cli-correction/04-teardown-before.sh"]
    known_script_hashes = {hashlib.sha256(p.read_bytes()).hexdigest() for p in script_candidates if p.is_file()}
    require_equal(receipt.get("teardownScriptSha256") in known_script_hashes, True,
                  f"{stack} {run_id} teardown script hash")

    event_name = row.get("event")
    if event_name == "teardown_process_launched":
        require_equal(receipt.get("popenCalledAtUtc"), row.get("popenCalledAtUtc"),
                      f"{stack} {run_id} cleanup Popen-call time")
        require_equal(receipt.get("popenReturnedAtUtc"), row.get("popenReturnedAtUtc"),
                      f"{stack} {run_id} cleanup Popen-return time")
        require_equal(receipt.get("phase") in ("launched", "exit_observed"), True,
                      f"{stack} {run_id} launch receipt phase")
    elif event_name == "teardown_process_exit_observed":
        journal_path = ROOT / "recovery/cleanup-timeline/phase-events.jsonl"
        launches = []
        for line in journal_path.read_text(errors="replace").splitlines():
            try:
                candidate = json.loads(line)
            except json.JSONDecodeError:
                continue
            if (candidate.get("event") == "teardown_process_launched"
                    and candidate.get("stack") == stack
                    and candidate.get("runId") == run_id
                    and candidate.get("pid") == row.get("pid")):
                launches.append(candidate)
        require_equal(len(launches), 1, f"{stack} {run_id} matching launch journal row")
        require_equal(receipt.get("popenCalledAtUtc"), launches[0].get("popenCalledAtUtc"),
                      f"{stack} {run_id} exit receipt Popen-call time")
        require_equal(receipt.get("popenReturnedAtUtc"), launches[0].get("popenReturnedAtUtc"),
                      f"{stack} {run_id} exit receipt Popen-return time")
        require_equal(receipt.get("phase"), "exit_observed", f"{stack} {run_id} exit receipt phase")
        for key in ("exitCode", "exitObservedAtUtc", "teardownReceiptStatus", "teardownCompletedAtUtc"):
            require_equal(receipt.get(key), row.get(key), f"{stack} {run_id} cleanup receipt {key}")
    else:
        raise RuntimeError(f"unsupported cleanup process event: {event_name!r}")
    return receipt, ROOT / receipt_path


def measurements_and_retention(stack: str, events: list[dict[str, Any]], open_items: list[str]) -> None:
    root = ROOT / stack
    test_receipts = sorted((root / ".state/attempts").glob("test-*/test-process*.json"))
    current = root / ".state/test-process.json"
    if current.exists():
        test_receipts.append(current)
    for idx, p in enumerate(test_receipts, 1):
        r = read(p, {}) or {}
        if not r:
            continue
        code = r.get("exitCode")
        phase = r.get("status", "unknown")
        log = p.with_name("test.log")
        failure = None
        if log.exists():
            failure = text_first(rel(log), 'data_checksum_version" does not exist')
            if failure:
                failure = 'PostgreSQL startup query failed: pg_control column "data_checksum_version" does not exist (PG16 uses data_page_checksum_version).'
        if not failure and log.exists():
            failure = text_first(rel(log), "ERROR:")
        title = "Measurement process completed" if phase in ("complete", "completed", "succeeded") or code == 0 else "Measurement process started; still running at timeline generation"
        if phase == "failed" or (code not in (None, 0)):
            title = "Measurement startup attempt failed before VACUUM" if "test-" in p.parent.name and not (root / "results/timing.json").exists() else "Measurement process failed"
        start = r.get("startedAtUtc")
        end = r.get("finishedAtUtc")
        detail = f"Test process status={phase}, exit={code}."
        if failure:
            detail += f" {failure}"
        add(events, f"measurement-process-{idx}", title, start,
            "exact-process-receipt-interval", detail,
            [p, log if log.exists() else None, p.with_name("03-test.py") if p.with_name("03-test.py").exists() else None],
            end_utc=end)

    # The first coordinated launch was intentionally preserved after all three
    # preflight queries hit a PostgreSQL 16 column-name error. Record the later
    # corrected launch as a retry boundary, distinct from the hydration gate.
    failed_attempts = []
    for p in test_receipts:
        r = read(p, {}) or {}
        if r.get("exitCode") not in (None, 0) and (r.get("startedAtUtc") or "") < "2026-10-07T08:35:43":
            failed_attempts.append((r, p))
    current_receipt = read(current, {}) or {}
    if failed_attempts and current_receipt.get("startedAtUtc"):
        r, p = max(failed_attempts, key=lambda x: x[0].get("finishedAtUtc") or "")
        add(events, "measurement-startup-retry", "Measurement restarted after preserving the pre-VACUUM startup failure",
            current_receipt["startedAtUtc"], "exact-process-receipt-time",
            "The first test attempt exited before submitting VACUUM because its PostgreSQL 16 checksum query used an unavailable column. The corrected query reads data_page_checksum_version; the new supervised process began this retry.",
            [p, current, "recovery/measurement-checksum-v2/source-review.json",
             "recovery/measurement-checksum-v2/03-test-v2.py"])

    # The test's JSONL event stream is written during the live run, before its
    # final timing/summary artifacts. It records checkpoint, observation,
    # VACUUM phases, and replay milestones with their own time semantics.
    event_files = sorted((root / "results").glob("*/events.jsonl"))
    event_files.append(root / "results/events.jsonl")
    seen_event: set[tuple[str, str]] = set()
    event_titles = {
        "preflight_complete": ("Measurement preflight complete", "exact-test-event-time"),
        "checkpoint_complete": ("Primary checkpoint complete; target LSN established", "exact-test-event-time"),
        "observation_started": ("Counter observation and VACUUM process launched", "exact-test-event-time"),
        "vacuum_started": ("VACUUM start marker observed", "exact-postgresql-log-time"),
        "vacuum_completed": ("VACUUM completion marker observed", "exact-postgresql-log-time"),
        "vacuum_progress_phase": ("VACUUM progress phase first observed", "first-observed-vacuum-phase"),
        "heap_work_complete_observed": ("VACUUM heap work completion first observed", "first-observed-progress-bound"),
        "replay_drain_started": ("Replica replay drain started after VACUUM", "exact-test-event-time"),
        "replica_caught_up": ("Replica reached the VACUUM completion LSN", "exact-test-event-sample-time"),
        "observation_finished": ("Counter observation finished", "exact-test-event-time"),
    }
    for event_path in event_files:
        if not event_path.exists():
            continue
        for line in event_path.read_text(errors="replace").splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            name, stamp = event.get("event"), event.get("time")
            if name in ("vacuum_started", "vacuum_completed"):
                stamp = ((event.get("marker") or {}).get("time") or stamp)
            if name not in event_titles or not stamp or (name, stamp) in seen_event:
                continue
            seen_event.add((name, stamp))
            title, precision = event_titles[name]
            extra = ""
            if name == "vacuum_progress_phase":
                extra = f" First sampled phase: {event.get('phase')}."
            elif name == "checkpoint_complete":
                extra = (f" Primary checkpoint target LSN {event.get('targetLsn')} was established. "
                         "The initial replica replay wait occurs after this event and before observation_started; its completion is bounded by observation_started, not separately timestamped here.")
            elif name == "replica_caught_up":
                extra = f" Stable catch-up target LSN {event.get('targetLsn')}; replay={event.get('replayLsn')}; primary flush={event.get('primaryFlushLsn')}."
            elif name in ("vacuum_started", "vacuum_completed"):
                extra = f" Marker time {((event.get('marker') or {}).get('time'))}."
            add(events, f"test-event-{name}-{stamp}", title, stamp, precision,
                "The persisted test event stream records this milestone." + extra, [event_path])

    timing = read(root / "results/timing.json", {}) or {}
    summary = read(root / "results/summary.json", {}) or {}
    for key, title in (("measurementStart", "Measurement start"), ("vacuumStart", "VACUUM started"),
                       ("vacuumComplete", "VACUUM completed"), ("replayEnd", "Replica replay catch-up complete")):
        stamp = timing.get(key)
        if stamp:
            add(events, f"measurement-{key}", title, stamp, "exact-test-timing-receipt",
                f"Result status: {summary.get('status', {}).get('testComplete')}; coverage valid={summary.get('coverage', {}).get('valid')}.",
                [f"{stack}/results/timing.json", f"{stack}/results/summary.json", f"{stack}/results/vacuum.stdout", f"{stack}/results/replay-boundaries.json"])
    if not timing.get("vacuumStart"):
        running = any((read(p, {}) or {}).get("status") == "running" for p in test_receipts)
        if running:
            persisted = set()
            for event_path in event_files:
                if not event_path.exists():
                    continue
                for line in event_path.read_text(errors="replace").splitlines():
                    try:
                        persisted.add(json.loads(line).get("event"))
                    except json.JSONDecodeError:
                        continue
            if "vacuum_completed" in persisted:
                open_items.append("VACUUM start and completion markers are persisted in the test event stream, but the final timing/summary receipt and replica catch-up result are still pending.")
            elif "vacuum_started" in persisted:
                open_items.append("VACUUM start is persisted in the test event stream and VACUUM is running; final completion, replica catch-up, and timing/summary receipts are pending.")
            else:
                open_items.append("Measurement process is running or awaiting final timing publication; VACUUM start and completion are not yet present in the persisted test events.")
        else:
            open_items.append("No final VACUUM timing receipt is available; measurement and replay completion remain unverified.")

    for gate_path in sorted((ROOT / "recovery").glob("**/measurement-gate*.json")):
        gate = read(gate_path, {}) or {}
        if stack in gate.get("stacks", []) and gate.get("releasedAtUtc"):
            add(events, f"measurement-gate-{hashlib.sha256(rel(gate_path).encode()).hexdigest()[:8]}",
                f"Fresh all-stack hydration gate released measurement startup ({gate_path.parent.name})",
                gate.get("releasedAtUtc"), "exact-gate-receipt-time",
                "The gate checked all preparation receipts, four measured PremiumV2 disks per stack, copy progress, replica readiness, and staging-retirement evidence before authorizing startup.",
                [gate_path, "preflight/staging-retirement.json"])

    export_paths = sorted((ROOT / "recovery/backup-retention-review/exports" / stack).glob("*/receipt.json"))
    if export_paths:
        for i, p in enumerate(export_paths, 1):
            r = read(p, {}) or {}
            add(events, f"wal-export-{i}", "Required paired-Backup WAL/history archive exported and verified locally",
                r.get("exportedAtUtc"), "exact-export-receipt-time",
                "Private local export receipt records selected object bytes and hashes; no credential values or WAL contents are included in this timeline.", [p])
    else:
        open_items.append("Required Backup WAL/history export receipt not found yet.")

    cleanup = read("cleanup-state.json", {}) or {}
    if cleanup.get("phase") == "complete":
        add(events, "cleanup-complete", "Authorized non-snapshot test resources cleaned up; snapshots retained",
            cleanup.get("completedAtUtc") or cleanup.get("updatedAtUtc"), "exact-cleanup-receipt-time",
            f"Cleanup receipt reports {cleanup.get('snapshotCount')} retained snapshots; every snapshot remained protected.",
            ["cleanup-state.json", "cleanup-receipt.json", "preflight/snapshot-audit-before-compute-cleanup.json",
             "preflight/snapshot-audit-after-compute-cleanup.json", "preflight/snapshot-audit-after-shared-cleanup.json"])
    else:
        cleanup_phase = cleanup.get("phase")
        gate = read("cleanup-gate.json", {}) or {}
        if cleanup_phase:
            open_items.append(f"Post-measurement cleanup is at phase `{cleanup_phase}`; no completed cleanup receipt exists. Snapshot preservation remains active.")
        elif gate.get("authorized") is True:
            open_items.append("Cleanup is authorized but has not produced a completed cleanup receipt. Snapshot preservation remains active.")
        else:
            open_items.append("Post-measurement cleanup has not started; all snapshots remain preserved under their retention protections.")

    # Teardown receipt is per stack and remains the authority for that stack's
    # resource cleanup. Current teardown scripts record a finish time; a start
    # is shown only if a future supervisor/journal explicitly captures one.
    teardown_path = root / ".state/teardown.json"
    retention_path = root / ".state/snapshot-retention.json"
    teardown = read(teardown_path, {}) or {}
    cleanup_events_path = ROOT / "recovery/cleanup-timeline/phase-events.jsonl"
    phase_rows = []
    teardown_rows = []
    if cleanup_events_path.exists():
        for line in cleanup_events_path.read_text(errors="replace").splitlines():
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if row.get("event") == "cleanup_phase":
                phase_rows.append(row)
            elif row.get("stack") == stack and row.get("event") in (
                    "teardown_process_launched", "teardown_process_exit_observed"):
                teardown_rows.append(row)
    validated_rows = [(row, *validate_teardown_observation(row, stack)) for row in teardown_rows]
    successful_rows = [row for row, _, _ in validated_rows
                       if row.get("event") == "teardown_process_exit_observed"
                       and row.get("exitCode") == 0
                       and row.get("teardownReceiptStatus") == "complete"
                       and row.get("teardownCompletedAtUtc")]
    if teardown.get("status") == "complete":
        start = teardown.get("startedAtUtc")
        finish = teardown.get("completedAtUtc")
        matching = [row for row in successful_rows if row.get("teardownCompletedAtUtc") == finish]
        if matching:
            for row in matching:
                require_equal(teardown.get("completedAtUtc"), row.get("teardownCompletedAtUtc"),
                              f"{stack} {row.get('runId')} canonical completion time")
                require_equal(teardown.get("status"), "complete", f"{stack} {row.get('runId')} canonical status")
        else:
            add(events, f"owned-teardown-{stack}", f"Owned {stack} non-snapshot resources removed; retained snapshots verified",
                finish, "exact-teardown-finish-and-start-when-recorded",
                f"Teardown status complete; finish={finish}; start={start or 'not recorded by the per-stack teardown receipt'}. Retained snapshot records are in the linked receipt.",
                [teardown_path, retention_path, root / "teardown.log",
                 "cleanup-state.json", "preflight/snapshot-audit-before-compute-cleanup.json",
                 "preflight/snapshot-audit-after-compute-cleanup.json", "preflight/snapshot-audit-after-shared-cleanup.json"],
                start_utc=start, end_utc=finish)
    elif teardown:
        open_items.append(f"{stack} teardown receipt exists with status `{teardown.get('status')}`; snapshot retention remains governed by the linked retention audit.")
    else:
        open_items.append(f"No {stack} owned teardown receipt; this stack's compute/storage remains subject to the separate cleanup gate.")

    for row in phase_rows:
        phase = row.get("phase", "unknown")
        stamp = row.get("observedAtUtc")
        run_id = row.get("runId", "unknown-run")
        if stamp:
            add(events, f"cleanup-phase-{phase}-{run_id}",
                f"Cleanup attempt {run_id}: phase recorded: {phase}", stamp,
                "exact-local-cleanup-journal-time",
                f"Cleanup run {run_id} journal records the phase after its local state receipt was saved; cloud completion requires separate resource audit evidence.",
                [cleanup_events_path, "post-measurement-cleanup-v2.py"])

    for row, receipt_obj, receipt_path in validated_rows:
        run_id = row.get("runId")
        pid = row.get("pid")
        event_name = row.get("event")
        if event_name == "teardown_process_launched":
            start = row.get("popenCalledAtUtc")
            end = row.get("popenReturnedAtUtc")
            if start:
                add(events, f"teardown-process-launched-{stack}-{run_id}-{pid}",
                    f"Cleanup attempt {run_id}: {stack} teardown process launched",
                    start, "exact-local-process-launch-bounds",
                    f"Run {run_id} launched PID {pid} with command `bash {stack}/04-teardown.sh`; Popen returned at {end}. Resource-removal completion is recorded separately.",
                    [cleanup_events_path, receipt_path], end_utc=end)
            continue

        observed = row.get("exitObservedAtUtc")
        start = row.get("popenCalledAtUtc")
        completed = row.get("teardownCompletedAtUtc")
        exit_code = row.get("exitCode")
        status = row.get("teardownReceiptStatus")
        if exit_code == 0 and status == "complete" and completed:
            if teardown.get("status") == "complete" and teardown.get("completedAtUtc") == completed:
                add(events, f"owned-teardown-completed-{stack}-{run_id}-{pid}",
                    f"Cleanup attempt {run_id}: {stack} teardown completed; retained snapshots verified",
                    completed, "exact-canonical-teardown-receipt-time",
                    f"Run {run_id} process PID {pid} exited 0 (observed {observed}); its receipt and current canonical teardown both say complete at {completed}. Snapshot retention is verified in the linked receipts.",
                    [cleanup_events_path, receipt_path, teardown_path, retention_path,
                     "preflight/snapshot-audit-before-compute-cleanup.json",
                     "preflight/snapshot-audit-after-compute-cleanup.json",
                     "preflight/snapshot-audit-after-shared-cleanup.json"],
                    start_utc=start, end_utc=observed)
            else:
                add(events, f"teardown-attempt-historical-complete-{stack}-{run_id}-{pid}",
                    f"Cleanup attempt {run_id}: per-run receipt recorded {stack} completion",
                    completed, "exact-run-specific-teardown-receipt-time",
                    f"Run {run_id} PID {pid} exited 0; its immutable per-run receipt says complete at {completed}. The current canonical receipt represents a different state, so no current resource state is inferred from this historical attempt.",
                    [cleanup_events_path, receipt_path], start_utc=start, end_utc=observed)
        elif observed:
            paths: list[str | Path | None] = [cleanup_events_path, receipt_path]
            failed_script = ROOT / "recovery/cleanup-cli-correction/04-teardown-before.sh"
            failed_logs = ROOT / "recovery/cleanup-cli-correction/failed-attempt-evidence"
            if (failed_script.is_file()
                    and receipt_obj.get("teardownScriptSha256") == hashlib.sha256(failed_script.read_bytes()).hexdigest()):
                paths += [failed_script, "recovery/cleanup-cli-correction/README.md",
                          "recovery/cleanup-cli-correction/failed-attempt-evidence/manifest.json",
                          failed_logs / stack / "teardown.log", failed_logs / "post-cleanup-v2.log"]
            add(events, f"teardown-process-exit-{stack}-{run_id}-{pid}",
                f"Cleanup attempt {run_id}: {stack} teardown process exit observed",
                observed, "exact-local-process-exit-observation-time",
                f"Run {run_id} PID {pid} exited {exit_code}; its per-run receipt status is {status!r}. No cleanup completion is inferred from this process exit.",
                paths, start_utc=start, end_utc=observed)


def make_timeline(stack: str) -> dict[str, Any]:
    events: list[dict[str, Any]] = []
    open_items: list[str] = []
    source_and_provision(stack, events)
    process_attempts(stack, events)
    backup_and_seed(stack, events)
    finalization_gate(stack, events)
    measurements_and_retention(stack, events, open_items)
    events.sort(key=sort_key)

    dml_path = "recovery/fixture-watch/100-dml-output/final-20261007T051507Z/capture-manifest.json"
    if stack != "100gib":
        open_items.append("Raw INSERT/DELETE command output was not retained for this stack; finalizer receipts and database estimates are reported as such, not presented as captured row counts.")
    else:
        open_items.append("The 100-GiB INSERT transcript is unavailable; only CREATE INDEX/DELETE/CHECKPOINT output and status 0 were preserved.")
    if stack == "100gib" and not read("preflight/staging-retirement.json", {}):
        open_items.append("Staging retirement receipt is pending; no measurement may start until retirement and the fresh all-stack hydration barrier complete.")

    return {
        "schemaVersion": 1,
        "generatedAtUtc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "stack": stack,
        "events": events,
        "timeSemantics": {
            "exact": "Timestamp comes directly from a process/Kubernetes/ARM/export receipt.",
            "first-observed": "The value was present in the cited poll or audit at that time; the underlying background transition may be earlier.",
            "bounded": "Only the supplied start/end observations are available; no per-statement instant is inferred.",
        },
        "limitations": open_items,
    }


def render_md(doc: dict[str, Any]) -> str:
    lines = [f"# {doc['stack']} experiment timeline", "",
             f"Generated from local receipts at {doc['generatedAtUtc']}. All listed times are UTC.", "",
             "Exact timestamps, first-observed copy states, and bounded intervals are labeled separately. A first-observed 100% copy status is an observation bound, not a claim about the exact instant Azure finished background work.", "",
             "| UTC / interval | Precision | Event | Evidence and qualification |",
             "|---|---|---|---|"]
    for item in doc["events"]:
        interval_start = item.get("startUtc") or item.get("timeUtc")
        if interval_start and item.get("endUtc"):
            when = f"{interval_start} → {item['endUtc']}"
        else:
            when = item.get("timeUtc") or "Pending"
        refs = ", ".join(f"[`{Path(e['path']).name}`](../../../{e['path']})" for e in item.get("evidence", [])) or "—"
        detail = item["details"].replace("|", "\\|")
        lines.append(f"| {when} | {item['timePrecision']} | {item['title']} | {detail} Sources: {refs} |")
    lines += ["", "## Outstanding evidence", ""]
    if doc["limitations"]:
        lines += [f"- {row}" for row in doc["limitations"]]
    else:
        lines.append("All required timeline phases have a receipt.")
    lines += ["", "## Interpretation", "",
              "The timeline preserves preparation retries and recovery paths. For SQL phases, exact command output is included only where a raw remote capture exists; otherwise script order, process receipts, and finalizer evidence are described as bounds or control-flow evidence. No timestamps are reconstructed from file modification times.", ""]
    return "\n".join(lines)


def main() -> None:
    for stack in STACKS:
        doc = make_timeline(stack)
        target = OUT / stack
        target.mkdir(parents=True, exist_ok=True)
        (target / "timeline.json").write_text(json.dumps(doc, indent=2, sort_keys=False) + "\n")
        (target / "timeline.md").write_text(render_md(doc))
        print(f"wrote {rel(target / 'timeline.json')} ({len(doc['events'])} events; {len(doc['limitations'])} outstanding notes)")
    index = [
        "# Experiment timeline evidence",
        "",
        "Evidence-linked per-run timelines for the baseline and two scaled fixtures. Exact timestamps come from receipts or Kubernetes/ARM metadata; copy progress is labeled as first-observed evidence. These files are regenerated as measurement, WAL export, and cleanup receipts arrive.",
        "",
        "- [Baseline 23 GiB](baseline-23gib/timeline.md) and [structured JSON](baseline-23gib/timeline.json)",
        "- [50 GiB](50gib/timeline.md) and [structured JSON](50gib/timeline.json)",
        "- [100 GiB](100gib/timeline.md) and [structured JSON](100gib/timeline.json)",
        "",
        "The standalone per-run report publisher is [publish_run_reports.py](publish_run_reports.py). Its default is read-only (`--dry-run`); after review, `--write` updates only `RUN/results/REPORT.md` and embeds each full timeline. `--require-complete` refuses publication while any run lacks exit 0, complete timing, complete summary status, or valid coverage.",
        "",
    ]
    (OUT / "README.md").write_text("\n".join(index))


if __name__ == "__main__":
    main()
