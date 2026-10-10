#!/usr/bin/env python3
"""Draft, owner-bound continuation for a 100-GiB Backup wait timeout.

This does not create a Backup, rerun SQL/DML, or write .state. By default it
only verifies and waits for the already-existing Backup and paired copies. The
optional continuation mode runs the original post-Backup seed/image tail and
writes all captures outside .state. It has not been executed.
"""
from __future__ import annotations

import argparse
import copy
import datetime as dt
import decimal
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import stat
import subprocess
import sys
import time
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
STACK = ROOT / "100gib"
NS, CLUSTER, SC = "repro", "hdd-pg", "azure-premiumv2"
IMAGE = "ghcr.io/cloudnative-pg/postgresql:16.15-202609281634-standard-trixie@sha256:1dd42563007500b94b943f2a1620f724efedfc9eea2d07a50c6db4582c2431fd"
SYSIMAGE = "ghcr.io/cloudnative-pg/postgresql:16.15-202609140815-system-trixie@sha256:a30ea813ae1c82fc94743c4e71766688158b58d8ddf1f4e20ff5d3edb569a215"
ORIGINAL_02_SHA = "7b24d869c7ef362653f38201402f5e4f297c874f66253658f884ced15055b20f"
IMMUTABLE_03_SHA = "1b6b25fdd0c49f1ec9c1b116f28c72ddb5d63202c858ffa34ec12815756e004a"
STAGE_HELPER_SHA = "0eacb0a81b30703a3b788aa745d3eca8c079e3e4e0cfca9921f500b6f5c8918b"
CAPTURE_HELPER_SHA = "1a1437306ed4bfdf8edb872b7f5e1ade475eb54fa305a67b1048eb986dad214e"
BIND_TERMINAL_HELPER_SHA = "1a7e1b48ffe91a64ec7cdd90f8c520bb4ee5eb14edf60aa1e13e02e3e260dc3d"
GIB = 1024**3
WAIT_SECONDS = 24 * 60 * 60


class ResumeError(RuntimeError):
    pass


def require(ok: bool, message: str) -> None:
    if not ok:
        raise ResumeError(message)


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ResumeError(f"Invalid local JSON evidence ({path.name}: {exc.__class__.__name__})") from None


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as exc:
        raise ResumeError(f"Cannot hash {path.name}: {exc.__class__.__name__}") from None
    return digest.hexdigest()


def local_tree_manifest(directory: Path) -> dict[str, dict[str, Any]]:
    require(directory.is_dir() and not directory.is_symlink(),
            "Preserved original TMP archive is not a regular directory")
    rows: dict[str, dict[str, Any]] = {}
    for path in sorted(directory.rglob("*")):
        rel = path.relative_to(directory).as_posix()
        require(not path.is_symlink(), f"Preserved original TMP archive contains a symlink ({rel})")
        if path.is_dir():
            continue
        require(path.is_file(), f"Preserved original TMP archive contains a non-file ({rel})")
        rows[rel] = {"sha256": sha256(path), "sizeBytes": path.stat().st_size}
    return rows


def canonical_id(value: Any) -> str:
    return str(value or "").rstrip("/").lower()


def completion_percent(value: Any) -> decimal.Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ResumeError("Copy completionPercent is non-numeric")
    try:
        result = decimal.Decimal(str(value))
    except decimal.InvalidOperation:
        raise ResumeError("Copy completionPercent is malformed") from None
    require(result.is_finite() and 0 <= result <= 100, "Copy completionPercent is outside 0..100")
    return result


def validate_backup_snapshot_access_state(value: Any) -> None:
    """Pending/Creating states remain waitable while strict lineage stays pinned."""
    require(value in ("Available", "Pending", "Creating", "Unknown", None),
            "Backup snapshot access state is not waitable")


SNAPSHOT_IDENTITY_FIELDS = (
    "id", "uniqueId", "volumeSnapshotUID", "volumeSnapshotContent",
    "volumeSnapshotContentUID", "sourcePVC", "sourcePV", "sourcePVCUID",
    "sourceDiskId", "sourceDiskUniqueId", "sourceUniqueId", "createOption",
)


def extend_snapshot_identity_pins(pins: dict[str, dict[str, Any]], name: str,
                                  row: dict[str, Any]) -> None:
    """Accumulate immutable identities as soon as each field becomes observable."""
    pinned = pins.setdefault(name, {})
    for field in SNAPSHOT_IDENTITY_FIELDS:
        value = row.get(field)
        if value in (None, ""):
            continue
        require(field not in pinned or pinned[field] == value,
                f"Backup snapshot {name} changed pinned identity field {field}")
        pinned[field] = value


def validate_snapshot_identity_pins(expected: dict[str, dict[str, Any]],
                                    observed: dict[str, dict[str, Any]]) -> None:
    require(set(expected) == set(observed) and len(expected) == 2,
            "Paired Backup snapshot identity pin set changed")
    for name, identity in expected.items():
        current = observed[name]
        require(bool(identity) and all(current.get(field) == value for field, value in identity.items()),
                f"Backup snapshot {name} changed an identity pinned before the refresh")


def validate_backup_object(backup: dict[str, Any], name: str, owner: str,
                           expected_uid: str) -> str:
    metadata, spec = backup.get("metadata", {}), backup.get("spec", {})
    require(metadata.get("name") == name and metadata.get("namespace") == NS
            and metadata.get("uid") == expected_uid,
            "Existing Backup name/UID differs from the pinned request")
    require((metadata.get("labels") or {}).get("repro.stack.token") == owner,
            "Existing Backup is not owned by this stack")
    online = spec.get("onlineConfiguration") or {}
    require(spec.get("cluster", {}).get("name") == CLUSTER
            and spec.get("method") == "volumeSnapshot" and spec.get("online") is True
            and online.get("waitForArchive") is True
            and online.get("immediateCheckpoint") is False,
            "Existing Backup is not the frozen online wait-for-archive snapshot request")
    phase = (backup.get("status") or {}).get("phase", "")
    require(phase not in ("failed", "walArchivingFailing"),
            f"Pinned Backup reached terminal phase {phase}")
    return phase


def validate_source_owner_binding(stack_token: str, manifest: dict[str, Any],
                                 stage: dict[str, Any]) -> str:
    """Keep immutable keeper ownership distinct from child-stack ownership."""
    source_owner = str(manifest.get("ownerToken", ""))
    require(re.fullmatch(r"[a-f0-9]{32}", stack_token) is not None
            and re.fullmatch(r"[a-f0-9]{32}", source_owner) is not None
            and source_owner != stack_token,
            "Pinned keeper owner token is invalid or conflated with child-stack ownership")
    require(stage.get("ownerToken") == stack_token
            and (stage.get("sourceManifest") or {}).get("ownerToken") == source_owner
            and (stage.get("sourceSnapshotMetadata", {}).get("tags") or {}).get("reprotoken") == source_owner
            and (stage.get("stagingDiskMetadata", {}).get("tags") or {}).get("reprotoken") == stack_token
            and (stage.get("restoreSnapshotMetadata", {}).get("tags") or {}).get("reprotoken") == stack_token,
            "Stage receipt does not preserve separate keeper-source and child-stack ownership")
    return source_owner


def validate_process_identity_chain(running: dict[str, Any], terminal: dict[str, Any],
                                    binding: dict[str, Any], running_sha: str,
                                    terminal_sha: str, capture_sha: str,
                                    capture: dict[str, Any]) -> None:
    identity = binding.get("runIdentity") or {}
    require(running.get("stack") == terminal.get("stack") == "100gib"
            and running.get("phase") == terminal.get("phase") == "prepare"
            and running.get("status") == "running"
            and terminal.get("status") in ("failed", "stopped")
            and running.get("supervisorPid") == terminal.get("supervisorPid")
            and running.get("childPid", running.get("childProcessId"))
                == terminal.get("childPid", terminal.get("childProcessId"))
            and running.get("startedAtUtc") == terminal.get("startedAtUtc")
            and running.get("command") == terminal.get("command")
            and running.get("scriptSha256") == terminal.get("scriptSha256")
            and running.get("sourceStageHelperSha256") == terminal.get("sourceStageHelperSha256"),
            "Running/terminal receipts do not share the same immutable preparation run identity")
    require(identity.get("stack") == "100gib" and identity.get("phase") == "prepare"
            and identity.get("startedAtUtc") == running.get("startedAtUtc")
            and identity.get("supervisorPid") == running.get("supervisorPid")
            and identity.get("childPid") == running.get("childPid", running.get("childProcessId"))
            and identity.get("command") == running.get("command")
            and identity.get("scriptSha256") == running.get("scriptSha256")
            and identity.get("sourceStageHelperSha256") == running.get("sourceStageHelperSha256"),
            "Terminal binding does not preserve the process receipt's immutable run identity")
    require(binding.get("runningProcessReceiptSha256") == running_sha
            and binding.get("terminalProcessReceiptSha256") == terminal_sha
            and binding.get("captureReceiptSha256") == capture_sha
            and binding.get("terminalStatus") == terminal.get("status"),
            "Terminal binding receipt hashes/status differ from the archived bytes")
    capture_at = utc_time(capture.get("capturedAtUtc"), "Pre-Backup capture")
    started_at = utc_time(running.get("startedAtUtc"), "Original preparation start")
    observed_at = utc_time(binding.get("terminalObservedAtUtc"), "Terminal process observation")
    require(capture_at >= started_at and observed_at >= capture_at,
            "Original process capture/terminal evidence timestamps are out of order")
    finished = terminal.get("finishedAtUtc") or terminal.get("endedAtUtc")
    if finished:
        finished_at = utc_time(finished, "Original process finish")
        require(finished_at >= capture_at and observed_at >= finished_at,
                "Original process terminal timestamp predates the active TMP capture")


def validate_capture_receipt(receipt: dict[str, Any], running: dict[str, Any],
                             file_hashes: dict[str, str], state_dir: Path,
                             running_sha: str) -> str:
    """Bind copied pre-Backup files to the exact live original attempt."""
    require(receipt.get("version") == 1 and receipt.get("stack") == "100gib"
            and receipt.get("processStatusAtCapture") == "running"
            and receipt.get("originalScriptSha256") == ORIGINAL_02_SHA
            and receipt.get("sourceStageHelperSha256") == STAGE_HELPER_SHA
            and receipt.get("runningProcessReceiptSha256") == running_sha,
            "Pre-Backup capture receipt is not bound to the original running process/script")
    child_pid = receipt.get("childPidAtCapture")
    source_temp = receipt.get("sourceTempPath")
    require(isinstance(child_pid, int) and child_pid > 1
            and isinstance(source_temp, str) and Path(source_temp).is_absolute(),
            "Pre-Backup capture receipt lacks the original child PID or TMP path")
    require(child_pid == running.get("childPid", running.get("childProcessId"))
            and receipt.get("supervisorPidAtCapture") == running.get("supervisorPid")
            and running.get("stack") == "100gib" and running.get("phase") == "prepare"
            and running.get("status") == "running"
            and running.get("scriptSha256", {}).get("02-prepare.sh") == ORIGINAL_02_SHA
            and running.get("sourceStageHelperSha256") == STAGE_HELPER_SHA,
            "Pre-Backup capture receipt differs from the archived running original process")
    source_path = Path(source_temp)
    require(source_path.parent == state_dir.resolve()
            and re.fullmatch(r"\.prepare\.[A-Za-z0-9]+", source_path.name) is not None,
            "Captured TMP source path is not the original private .state/.prepare.* directory")
    require(receipt.get("filesSha256") == file_hashes,
            "Pre-Backup capture file hashes differ from the preserved TMP copy")
    require(receipt.get("runStartedAtUtc") == running.get("startedAtUtc"),
            "Pre-Backup capture receipt run start differs from captured running process")
    captured_at = utc_time(receipt.get("capturedAtUtc"), "Pre-Backup capture")
    started_at = utc_time(running.get("startedAtUtc"), "Original preparation start")
    require(captured_at >= started_at,
            "Pre-Backup capture timestamp predates the original preparation process")
    return source_temp


def extract_backup_pair(backup: dict[str, Any]) -> dict[str, str]:
    elements = (backup.get("status", {}).get("snapshotBackupStatus") or {}).get("elements") or []
    require(len(elements) == 2 and sorted(e.get("type") for e in elements) == ["PG_DATA", "PG_WAL"],
            "Completed Backup does not have exactly one PG_DATA and one PG_WAL output")
    names = {e["type"]: e.get("name") for e in elements}
    require(all(isinstance(x, str) and x for x in names.values()) and names["PG_DATA"] != names["PG_WAL"],
            "Completed Backup output names are missing or duplicated")
    return {"data": names["PG_DATA"], "wal": names["PG_WAL"]}


def utc_time(value: Any, label: str) -> dt.datetime:
    require(isinstance(value, str) and value, f"{label} timestamp is missing")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ResumeError(f"{label} timestamp is malformed") from None
    require(parsed.tzinfo is not None and parsed.utcoffset() == dt.timedelta(0),
            f"{label} timestamp is not UTC")
    return parsed


def seed_event_timestamp(event: dict[str, Any]) -> str:
    value = event.get("lastTimestamp") or event.get("eventTime") or event.get("firstTimestamp")
    require(isinstance(value, str) and value, "Completed seed Event lacks a timestamp")
    return value


def select_completed_seed_event(events: list[dict[str, Any]], uid: str,
                                pvc_time: str, pod_time: str) -> dict[str, Any]:
    pvc_at, pod_at = utc_time(pvc_time, "replica PVC"), utc_time(pod_time, "replica pod")
    matches = []
    for event in events:
        obj = event.get("involvedObject") or {}
        if (obj.get("kind") != "Job" or obj.get("name") != f"{CLUSTER}-2-snapshot-recovery"
                or obj.get("uid") != uid or event.get("reason") != "Completed"
                or event.get("message") != "Job completed"):
            continue
        timestamp = seed_event_timestamp(event)
        at = utc_time(timestamp, "Completed seed Event")
        if pvc_at <= at < pod_at:
            matches.append(event)
    require(len(matches) == 1,
            "Pinned Completed seed Event is not unique after replica PVCs and before the current replica pod")
    return matches[0]


def validate_control_capture(control: Path, token: str, short: str, account: str,
                             container: str, backup_name: str, source_vss: str,
                             source_snapshot_id: str, restore_copy_id: str) -> dict[str, str]:
    """Pin the last pre-Backup TMP files that prove the frozen control-flow boundary."""
    expected = {"cluster.yaml", "backup.yaml", "seed-path"}
    require(control.is_dir() and not control.is_symlink(), "Preserved pre-Backup TMP is missing or a symlink")
    try:
        control.resolve().relative_to(ROOT / "recovery/backup-wait-resume")
    except ValueError:
        raise ResumeError("Preserved TMP must be copied under recovery/backup-wait-resume") from None
    require(all((control / name).is_file() and not (control / name).is_symlink() for name in expected),
            "Preserved pre-Backup TMP lacks cluster.yaml, backup.yaml, or seed-path")
    require(stat.S_IMODE(control.stat().st_mode) & 0o077 == 0
            and all(stat.S_IMODE((control / name).stat().st_mode) & 0o077 == 0 for name in expected),
            "Preserved pre-Backup TMP and files must be private to the owner")
    backup_text = (control / "backup.yaml").read_text()
    expected_backup = (
        "apiVersion: postgresql.cnpg.io/v1\n"
        "kind: Backup\n"
        f'metadata: {{name: {backup_name}, namespace: {NS}, labels: {{repro.stack.token: "{token}"}}}}\n'
        f"spec: {{cluster: {{name: {CLUSTER}}}, method: volumeSnapshot, online: true, "
        "onlineConfiguration: {waitForArchive: true, immediateCheckpoint: false}}\n"
    )
    require(backup_text == expected_backup,
            "Captured backup.yaml does not exactly match the owner-bound original request")
    cluster_text = (control / "cluster.yaml").read_text()
    fragments = (
        f'name: {CLUSTER}, namespace: {NS}, labels: {{repro.stack.token: "{token}"}}',
        f"instances: 1", f"imageName: {SYSIMAGE}",
        f"name: {source_vss}, kind: VolumeSnapshot", "storageClass: azure-premiumv2",
        f"vacuum-only-repro/source-snapshot-id: \"{source_snapshot_id}\"",
        f"vacuum-only-repro/restore-copy-id: \"{restore_copy_id}\"",
        f"destinationPath: https://{account}.blob.core.windows.net/{container}/cnpg-vacuum-only/{token}/",
        f"serverName: {CLUSTER}", "waitForArchive: true", "immediateCheckpoint: false",
        "azureCredentials: {connectionString: {name: azure-barman-credentials, key: connection-string}}",
    )
    require(all(x in cluster_text for x in fragments),
            "Captured cluster.yaml differs from the frozen owner/source/archive configuration")
    seed_path = (control / "seed-path").read_text().strip()
    require(re.fullmatch(rf"/controller/tmp/seed-{re.escape(token)}-[0-9]{{8}}T[0-9]{{6}}Z-[0-9]{{1,5}}", seed_path) is not None,
            "Captured seed-path is not the original psql_long temporary path")
    return {name: sha256(control / name) for name in sorted(expected)}


def load_inputs(args: argparse.Namespace) -> dict[str, Any]:
    state_dir = STACK / ".state"
    state = load_json(state_dir / "state.json")
    token = str(state.get("ownerToken", ""))
    sub = str(state.get("subscriptionId", ""))
    require(state.get("version") == 1 and state.get("status") == "ready"
            and re.fullmatch(r"[a-f0-9]{32}", token) and sub,
            "100 GiB stack owner state is incomplete")
    config = load_json(STACK / "run-config.json")
    require(config.get("targetHeapGiB") == 100 and config.get("location") == state.get("location")
            and str(config.get("zone")) == str(state.get("availabilityZone")) == "1",
            "100 GiB run configuration does not match owner location/zone")
    for path in (args.original_script, args.original_process_receipt, args.running_process_receipt,
                 args.prebackup_tmp, args.prebackup_capture_receipt, args.terminal_process_binding):
        require(not path.is_symlink(), f"Archived input cannot be a symlink ({path.name})")
    original = args.original_script.resolve()
    process_receipt = args.original_process_receipt.resolve()
    running_receipt_path = args.running_process_receipt.resolve()
    for path in (original, process_receipt, running_receipt_path, args.prebackup_tmp.resolve(),
                 args.prebackup_capture_receipt.resolve(), args.terminal_process_binding.resolve()):
        try:
            path.relative_to(ROOT / "recovery/backup-wait-resume")
        except ValueError:
            raise ResumeError("All archived timeout-recovery inputs must be under recovery/backup-wait-resume") from None
    require(sha256(original) == ORIGINAL_02_SHA,
            "Archived 100 GiB original 02 script is not the frozen SHA")
    require(sha256(STACK / "03-test.py") == IMMUTABLE_03_SHA
            and sha256(ROOT / "recovery/audit/03-test.py") == IMMUTABLE_03_SHA,
            "Immutable 03-test.py hash changed")
    helper = ROOT / "recovery/source-restore/stage-allowlisted-source.sh"
    require(sha256(helper) == STAGE_HELPER_SHA,
            "Pinned source-stage helper hash changed")
    process = load_json(process_receipt)
    running_process = load_json(running_receipt_path)
    require(process.get("stack") == "100gib" and process.get("phase") == "prepare"
            and process.get("status") in ("failed", "stopped")
            and process.get("scriptSha256", {}).get("02-prepare.sh") == ORIGINAL_02_SHA
            and process.get("sourceStageHelperSha256") == STAGE_HELPER_SHA,
            "Archived original process receipt is not a terminal 100 GiB frozen-script attempt")
    child = process.get("childPid", process.get("childProcessId"))
    if child is not None:
        try:
            os.kill(int(child), 0)
        except ProcessLookupError:
            pass
        except PermissionError:
            raise ResumeError("Archived original preparation child PID is still present") from None
        else:
            raise ResumeError("Archived original preparation child PID is still present")
    require(not (state_dir / "prepare.json").exists(),
            "Canonical preparation metadata already exists; refusing a second finalization")
    require(not (state_dir / "output-snapshots.jsonl").exists(),
            "Snapshot journal already exists; this is not the pre-Backup-timeout case")
    manifest_path = STACK / "source-manifest.json"
    manifest = load_json(manifest_path)
    source_meta = load_json(STACK / "snapshot.json")
    source_record = load_json(state_dir / "source-snapshot.json")
    stage_path = Path(state.get("snapshotSourceStageReceiptPath", ""))
    if not stage_path.is_absolute():
        stage_path = ROOT / stage_path
    require(stage_path.is_file() and stage_path.parent.resolve() == (state_dir / "source-stage").resolve(),
            "State does not bind the exact owned source-stage receipt")
    stage = load_json(stage_path)
    source_owner = validate_source_owner_binding(token, manifest, stage)
    copy_id = str(state.get("snapshotSourceCopyId", ""))
    stage_disk_id = str(state.get("snapshotSourceStagingDiskId", ""))
    require(manifest.get("version") == 1 and manifest.get("systemIdentifier")
            and manifest.get("subscriptionId") == sub
            and source_meta.get("systemIdentifier") == manifest.get("systemIdentifier")
            and int(source_meta.get("heapBytes", 0)) == 118_128_640_000
            and int(source_meta.get("preparedLiveTuplesEstimate", 0)) == 36_050_000,
            "Pinned 110 GiB source fixture identity/heap/statistics changed")
    require(canonical_id(manifest.get("sourceSnapshotId")) == canonical_id(stage.get("sourceSnapshotId"))
            and manifest.get("uniqueId") == stage.get("sourceSnapshotUniqueId")
            and canonical_id(stage.get("stagingDiskId")) == canonical_id(stage_disk_id)
            and canonical_id(stage.get("restoreSnapshotId")) == canonical_id(copy_id)
            and canonical_id(source_record.get("sourceSnapshotId")) == canonical_id(manifest.get("sourceSnapshotId"))
            and canonical_id(source_record.get("ownedCopySnapshotId")) == canonical_id(copy_id)
            and canonical_id(source_record.get("stagingDiskId")) == canonical_id(stage_disk_id),
            "Stack state, source manifest, source record, and stage receipt lineage differ")
    require(stage.get("status") == "ready"
            and stage.get("ownerToken") == token
            and stage.get("sourceManifest") == manifest
            and (stage.get("sourceManifest") == manifest or source_record.get("manifest") == manifest),
            "Owned stage receipt does not embed the pinned source manifest")
    require(canonical_id((stage.get("stagingDiskMetadata") or {}).get("creationData", {}).get("sourceResourceId"))
            == canonical_id(manifest.get("sourceSnapshotId"))
            and (stage.get("stagingDiskMetadata") or {}).get("creationData", {}).get("sourceUniqueId")
            == manifest.get("uniqueId"),
            "Standard staging disk does not derive from exact keeper snapshot UID")
    storage = load_json(state_dir / "storage.json")
    require(storage.get("status") == "ready" and storage.get("ownerToken") == token
            and storage.get("subscriptionId") == sub and storage.get("resourceGroup") == state.get("resourceGroup")
            and storage.get("publicAccess") is None,
            "Blob-account local owner record differs from stack state")
    account = str(storage.get("accountName", ""))
    expected_account_id = (f"/subscriptions/{sub}/resourceGroups/{state.get('resourceGroup')}"
                           f"/providers/Microsoft.Storage/storageAccounts/{account}")
    require(canonical_id(storage.get("accountId")) == canonical_id(expected_account_id),
            "Blob account local identity escapes the owned resource group")
    backup_name = f"vacuum-backup-{token[:8]}"
    control_source = args.prebackup_tmp.resolve()
    input_hashes = validate_control_capture(control_source, token, token[:8], account,
                                            str(storage.get("containerName", "")), backup_name,
                                            f"vacuum-data-{token[:8]}",
                                            str(manifest.get("sourceSnapshotId", "")), copy_id)
    capture_receipt_path = args.prebackup_capture_receipt.resolve()
    try:
        capture_receipt_path.relative_to(ROOT / "recovery/backup-wait-resume")
    except ValueError:
        raise ResumeError("Pre-Backup capture receipt must be preserved under recovery/backup-wait-resume") from None
    capture_receipt = load_json(capture_receipt_path)
    require(stat.S_IMODE(capture_receipt_path.stat().st_mode) & 0o077 == 0
            and stat.S_IMODE(running_receipt_path.stat().st_mode) & 0o077 == 0
            and stat.S_IMODE(process_receipt.stat().st_mode) & 0o077 == 0,
            "Original process and capture receipts must be private to the owner")
    binding_path = args.terminal_process_binding.resolve()
    try:
        binding_path.relative_to(ROOT / "recovery/backup-wait-resume")
    except ValueError:
        raise ResumeError("Terminal process binding must be preserved under recovery/backup-wait-resume") from None
    binding = load_json(binding_path)
    require(stat.S_IMODE(binding_path.stat().st_mode) & 0o077 == 0,
            "Terminal binding receipt must be private to the owner")
    running_sha, terminal_sha, capture_sha = (sha256(running_receipt_path), sha256(process_receipt),
                                               sha256(capture_receipt_path))
    require(capture_receipt.get("captureHelperSha256") == CAPTURE_HELPER_SHA
            and capture_receipt.get("captureHelperPath") == "recovery/backup-wait-resume/capture_prebackup_tmp.py"
            and sha256(ROOT / capture_receipt["captureHelperPath"]) == CAPTURE_HELPER_SHA
            and binding.get("bindingHelperSha256") == BIND_TERMINAL_HELPER_SHA
            and binding.get("bindingHelperPath") == "recovery/backup-wait-resume/bind_terminal_process.py"
            and sha256(ROOT / binding["bindingHelperPath"]) == BIND_TERMINAL_HELPER_SHA,
            "Read-only TMP capture or terminal-binding helper bytes differ from the pinned implementation")
    require(str((ROOT / capture_receipt.get("archivedTempPath", "")).resolve()) == str(control_source)
            and capture_receipt.get("fullTmpManifest") == local_tree_manifest(control_source),
            "Preserved full TMP archive differs from the capture receipt's file hash manifest")
    script_archive = ROOT / str(capture_receipt.get("originalScriptArchivePath", ""))
    helper_archive = ROOT / str(capture_receipt.get("sourceStageHelperArchivePath", ""))
    require(script_archive.is_file() and sha256(script_archive) == ORIGINAL_02_SHA
            and helper_archive.is_file() and sha256(helper_archive) == STAGE_HELPER_SHA
            and str(capture_receipt.get("runningProcessReceiptPath")) == str(running_receipt_path.relative_to(ROOT)),
            "Captured original script/helper/running receipt bytes or paths do not match their pins")
    source_temp_path = validate_capture_receipt(capture_receipt, running_process,
                                                input_hashes, state_dir, running_sha)
    validate_process_identity_chain(running_process, process, binding, running_sha,
                                    terminal_sha, capture_sha, capture_receipt)
    require(binding.get("runningProcessReceiptPath") == str(running_receipt_path.relative_to(ROOT))
            and binding.get("terminalProcessReceiptPath") == str(process_receipt.relative_to(ROOT))
            and binding.get("captureReceiptPath") == str(capture_receipt_path.relative_to(ROOT)),
            "Terminal binding paths differ from the exact archived running/capture/terminal receipts")
    input_hashes["captureReceiptSha256"] = sha256(capture_receipt_path)
    input_hashes["terminalProcessBindingSha256"] = sha256(binding_path)
    return {"state": state, "config": config, "manifest": manifest, "sourceMeta": source_meta,
            "sourceRecord": source_record, "stage": stage, "stagePath": stage_path,
            "process": process, "processPath": process_receipt, "originalScript": original,
            "runningProcess": running_process, "runningProcessPath": running_receipt_path,
            "terminalBinding": binding, "terminalBindingPath": binding_path,
            "storage": storage, "account": account, "backupName": backup_name,
            "token": token, "sub": sub, "short": token[:8], "inputHashes": input_hashes,
            "sourceOwnerToken": source_owner, "captureReceipt": capture_receipt,
            "captureReceiptPath": capture_receipt_path, "sourceTempPath": source_temp_path,
            "controlSource": control_source}


def invoke(argv: list[str], env: dict[str, str], *, timeout: int = 180,
           allow_fail: bool = False) -> str:
    try:
        result = subprocess.run(argv, env=env, text=True, capture_output=True,
                                timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ResumeError(f"Read-only/continuation command could not complete ({Path(argv[0]).name}: {exc.__class__.__name__})") from None
    if result.returncode and not allow_fail:
        raise ResumeError(f"Command failed ({Path(argv[0]).name}, exit {result.returncode}); see external runner log")
    return result.stdout


def port_forward_argv(pod: str, local_port: int) -> list[str]:
    require(re.fullmatch(r"[a-z0-9][a-z0-9.-]*", pod) is not None
            and isinstance(local_port, int) and 1 <= local_port <= 65535,
            "Archive-cache port-forward target or local port is invalid")
    return ["kubectl", "port-forward", "-n", NS, f"pod/{pod}",
            "--address=127.0.0.1", f"{local_port}:8010"]


def stop_port_forward(child: Any) -> None:
    child.send_signal(signal.SIGTERM)
    try:
        child.wait(timeout=5)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait(timeout=5)


class Resume:
    def __init__(self, inputs: dict[str, Any], output: Path, backup_uid: str):
        self.i = inputs
        self.output = output.resolve()
        try:
            self.output.relative_to(ROOT / "recovery/backup-wait-resume")
        except ValueError:
            raise ResumeError("All resume results must be written under recovery/backup-wait-resume") from None
        require(not self.output.exists(), "Resume output directory already exists; preserve and review it first")
        self.output.mkdir(mode=0o700, parents=True)
        os.chmod(self.output, 0o700)
        self.control_out = self.output / "control"
        self.control_out.mkdir(mode=0o700)
        for name in ("cluster.yaml", "backup.yaml", "seed-path"):
            shutil.copyfile(self.i["controlSource"] / name, self.control_out / name)
            os.chmod(self.control_out / name, 0o600)
        self.backup_uid = backup_uid
        self.state = self.i["state"]
        self.token = self.i["token"]
        self.sub = self.i["sub"]
        self.rg = self.state["resourceGroup"]
        self.node_rg = self.state["nodeResourceGroup"]
        self.snap_rg = self.state["snapshotResourceGroup"]
        self.aks = self.state["clusterName"]
        self.location = self.state["location"]
        self.zone = str(self.state["availabilityZone"])
        self.short = self.i["short"]
        self.account = self.i["account"]
        self.container = str(self.i["storage"]["containerName"])
        self.backup_name = self.i["backupName"]
        self.barman_url = f"https://{self.account}.blob.core.windows.net/{self.container}/cnpg-vacuum-only/{self.token}/"
        self.env = os.environ.copy()
        kcfg = self.state.get("kubeconfigPath") or str(STACK / ".state/kubeconfig")
        if not Path(kcfg).is_absolute():
            kcfg = str(STACK / kcfg)
        require(Path(kcfg).is_file(), "Owned stack kubeconfig is missing")
        self.env["KUBECONFIG"] = kcfg
        self.backup: dict[str, Any] | None = None
        self.pair: dict[str, str] = {}
        self.snapshot_evidence: dict[str, Any] = {}
        self.snapshot_identity_pins: dict[str, dict[str, Any]] = {}
        self.pv_pvc: dict[str, Any] = {}
        self.seed_job_uid: str | None = None
        self.seed_events_document: dict[str, Any] | None = None
        self.seed_trace: dict[str, Any] | None = None
        self.continuation_mode = False
        self.progress: dict[str, Any] = {"version": 1, "phase": "starting",
                                        "stack": "100gib", "backupName": self.backup_name,
                                        "backupUID": self.backup_uid,
                                        "originalScriptSha256": ORIGINAL_02_SHA,
                                        "originalProcessReceiptSha256": sha256(self.i["processPath"]),
                                        "originalRunningProcessReceiptSha256": sha256(self.i["runningProcessPath"]),
                                        "terminalProcessBindingSha256": sha256(self.i["terminalBindingPath"]),
                                        "controlFlowInputHashes": self.i["inputHashes"],
                                        "limitations": ["Original INSERT/DELETE output was command-substituted and removed; no raw transcript is reconstructed."]}
        self.progress["resumerScriptSha256"] = sha256(Path(__file__))
        self.progress["localEvidenceHashes"] = {
            "state": sha256(STACK / ".state/state.json"),
            "runConfig": sha256(STACK / "run-config.json"),
            "sourceManifest": sha256(STACK / "source-manifest.json"),
            "sourceMetadata": sha256(STACK / "snapshot.json"),
            "sourceSnapshotRecord": sha256(STACK / ".state/source-snapshot.json"),
            "stageReceipt": sha256(self.i["stagePath"]),
        }

    def k(self, *args: str, timeout: int = 180, allow_fail: bool = False) -> str:
        return invoke(["kubectl", *args], self.env, timeout=timeout, allow_fail=allow_fail)

    def kjson(self, *args: str) -> Any:
        try:
            return json.loads(self.k(*args, "-o", "json"))
        except json.JSONDecodeError:
            raise ResumeError("kubectl returned malformed JSON") from None

    def az(self, *args: str, timeout: int = 300) -> str:
        command = ["az", *args, "--subscription", self.sub, "--only-show-errors"]
        if "-o" not in args and "--output" not in args:
            command.extend(("-o", "json"))
        return invoke(command, self.env, timeout=timeout)

    def azjson(self, *args: str, timeout: int = 300) -> Any:
        try:
            return json.loads(self.az(*args, timeout=timeout))
        except json.JSONDecodeError:
            raise ResumeError("Azure CLI returned malformed JSON") from None

    def psql_json(self, pod: str, sql: str) -> dict[str, Any]:
        raw = self.k("exec", "-n", NS, "-c", "postgres", pod, "--", "psql", "-XAtq",
                     "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "repro", "-c", sql)
        try:
            return json.loads(raw.strip())
        except json.JSONDecodeError:
            raise ResumeError(f"PostgreSQL returned malformed JSON from {pod}") from None

    def save_json(self, path: Path, obj: Any) -> None:
        temp = path.with_name(path.name + f".tmp.{os.getpid()}")
        with temp.open("x") as stream:
            os.chmod(temp, 0o600)
            json.dump(obj, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        temp.replace(path)
        os.chmod(path, 0o600)

    def owner_preflight(self) -> None:
        require(self.az("account", "show", "--query", "id", "-o", "tsv").strip().strip('"') == self.sub,
                "Active Azure subscription differs from frozen stack owner")
        for rg in (self.rg, self.node_rg, self.snap_rg):
            group = self.azjson("group", "show", "--name", rg)
            require((group.get("tags") or {}).get("reprotoken") == self.token,
                    "Azure resource-group owner token mismatch")
        aks = self.azjson("aks", "show", "--resource-group", self.rg, "--name", self.aks)
        require((aks.get("tags") or {}).get("reprotoken") == self.token
                and aks.get("nodeResourceGroup") == self.node_rg,
                "AKS owner/node resource-group mismatch")
        current = self.k("config", "current-context").strip()
        require(self.aks in current, "Kubernetes context does not identify the owned AKS cluster")
        namespace = self.kjson("get", "namespace", NS)
        require((namespace.get("metadata", {}).get("labels") or {}).get("repro.stack.token") == self.token,
                "Namespace owner token mismatch")
        storage = self.i["storage"]
        arm = self.azjson("storage", "account", "show", "--resource-group", self.rg,
                          "--name", self.account)
        require(canonical_id(arm.get("id")) == canonical_id(storage.get("accountId"))
                and (arm.get("tags") or {}).get("reprotoken") == self.token
                and arm.get("provisioningState") == "Succeeded"
                and arm.get("allowBlobPublicAccess") is False
                and arm.get("enableHttpsTrafficOnly") is True,
                "Live Blob account identity/privacy/owner mismatch")
        cid = f"{storage['accountId']}/blobServices/default/containers/{self.container}"
        container = self.azjson("rest", "--method", "get", "--url",
                                 f"https://management.azure.com{cid}?api-version=2023-05-01")
        require(canonical_id(container.get("id")) == canonical_id(cid)
                and (container.get("properties", {}).get("metadata") or {}).get("reprotoken") == self.token
                and container.get("properties", {}).get("publicAccess") in (None, "None"),
                "Blob container ID, owner, or public access mismatch")
        sc = self.kjson("get", "storageclass", SC)
        params = sc.get("parameters") or {}
        require(sc.get("provisioner") == "disk.csi.azure.com" and params.get("skuName") == "PremiumV2_LRS"
                and params.get("cachingMode") == "None" and params.get("DiskIOPSReadWrite") == "3000"
                and params.get("DiskMBpsReadWrite") == "125" and params.get("LogicalSectorSize") == "512",
                "PremiumV2 StorageClass owner-run settings changed")
        secret = self.kjson("get", "secret", "azure-barman-credentials", "-n", NS)
        require((secret.get("metadata", {}).get("labels") or {}).get("repro.stack.token") == self.token
                and isinstance((secret.get("data") or {}).get("connection-string"), str)
                and bool(secret["data"]["connection-string"]),
                "Owned archive Secret is absent or lacks its connection-string key")
        # Only presence is retained; the Secret payload is never printed or written.
        self.progress["ownerPreflight"] = {"subscriptionId": self.sub, "resourceGroup": self.rg,
            "nodeResourceGroup": self.node_rg, "snapshotResourceGroup": self.snap_rg,
            "clusterName": self.aks, "blobAccountId": storage["accountId"], "container": self.container,
            "archiveSecretPresent": True}

    def source_lineage_preflight(self) -> None:
        """Independently bind live keeper -> stage disk -> restore-copy identities."""
        manifest, stage, state = self.i["manifest"], self.i["stage"], self.i["state"]
        src_id = str(manifest["sourceSnapshotId"])
        src_owner = str(manifest["ownerToken"])
        stage_disk_id = str(state["snapshotSourceStagingDiskId"])
        copy_id = str(state["snapshotSourceCopyId"])
        source = self.azjson("snapshot", "show", "--ids", src_id)
        require(canonical_id(source.get("id")) == canonical_id(src_id)
                and source.get("uniqueId") == manifest.get("uniqueId")
                and source.get("provisioningState") == "Succeeded"
                and source.get("snapshotAccessState") == "Available"
                and (source.get("diskSizeGB", source.get("diskSizeGb")) == 512)
                and source.get("incremental") is True
                and (source.get("sku") or {}).get("name") == "Standard_LRS"
                and canonical_id(source.get("location")) == canonical_id(manifest.get("location"))
                and (source.get("tags") or {}).get("reprotoken") == src_owner
                and (source.get("tags") or {}).get("reproPurpose") == manifest.get("purpose"),
                "Live keeper snapshot differs from immutable source manifest ownership/UID")
        stage_disk = self.azjson("disk", "show", "--ids", stage_disk_id)
        stage_creation = stage_disk.get("creationData") or {}
        require(canonical_id(stage_disk.get("id")) == canonical_id(stage_disk_id)
                and stage_disk.get("uniqueId") == stage.get("stagingDiskUniqueId")
                and stage_disk.get("provisioningState") == "Succeeded"
                and stage_disk.get("diskState") == "Unattached" and stage_disk.get("managedBy") is None
                and (stage_disk.get("sku") or {}).get("name") == "Standard_LRS"
                and int(stage_disk.get("diskSizeGB", stage_disk.get("diskSizeGb", 0))) == 512
                and canonical_id(stage_disk.get("location")) == canonical_id(self.location)
                and canonical_id(stage_creation.get("sourceResourceId")) == canonical_id(src_id)
                and stage_creation.get("sourceUniqueId") == manifest.get("uniqueId")
                and (stage_disk.get("tags") or {}).get("reprotoken") == self.token
                and (stage_disk.get("tags") or {}).get("reproPurpose") == "premiumv2-source-stage"
                and canonical_id((stage_disk.get("tags") or {}).get("sourceSnapshotId")) == canonical_id(src_id),
                "Live Standard stage disk differs from exact owner/keeper ID+UID lineage")
        copy = self.azjson("snapshot", "show", "--ids", copy_id)
        copy_creation = copy.get("creationData") or {}
        require(canonical_id(copy.get("id")) == canonical_id(copy_id)
                and copy.get("uniqueId") == stage.get("restoreSnapshotUniqueId")
                and copy.get("provisioningState") == "Succeeded"
                and copy.get("snapshotAccessState") == "Available"
                and (copy.get("diskSizeGB", copy.get("diskSizeGb")) == 512)
                and copy.get("incremental") is True
                and (copy.get("sku") or {}).get("name") == "Standard_LRS"
                and canonical_id(copy.get("location")) == canonical_id(self.location)
                and copy_creation.get("createOption") == "Copy"
                and canonical_id(copy_creation.get("sourceResourceId")) == canonical_id(stage_disk_id)
                and copy_creation.get("sourceUniqueId") == stage_disk.get("uniqueId")
                and (copy.get("tags") or {}).get("reprotoken") == self.token
                and (copy.get("tags") or {}).get("reproPurpose") == "premiumv2-keeper-copy"
                and canonical_id((copy.get("tags") or {}).get("sourceSnapshotId")) == canonical_id(src_id)
                and canonical_id((copy.get("tags") or {}).get("sourceDiskId")) == canonical_id(stage_disk_id),
                "Live restore-copy snapshot differs from exact owner/stage UID lineage")
        for identifier, label, lock_rg in ((src_id, "keeper snapshot", manifest["resourceGroup"]),
                                            (copy_id, "restore-copy snapshot", self.snap_rg)):
            lock_rows = self.azjson("lock", "list", "--resource-group", lock_rg)
            direct = []
            for lock in lock_rows:
                scope = lock.get("scope")
                if not scope and lock.get("id"):
                    lockid = canonical_id(lock["id"])
                    marker = "/providers/microsoft.authorization/locks/"
                    scope = lockid.split(marker, 1)[0] if marker in lockid else lockid
                if lock.get("level") == "CanNotDelete" and canonical_id(scope) == canonical_id(identifier):
                    direct.append(lock)
            require(bool(direct), f"Pinned {label} lacks a direct CanNotDelete lock")
        self.progress["sourceLineage"] = {"sourceSnapshotId": src_id,
            "sourceSnapshotUniqueId": source.get("uniqueId"),
            "sourceSnapshotOwnerToken": src_owner,
            "stackOwnerToken": self.token,
            "stagingDiskId": stage_disk_id, "stagingDiskUniqueId": stage_disk.get("uniqueId"),
            "restoreCopySnapshotId": copy_id, "restoreCopyUniqueId": copy.get("uniqueId"),
            "keeperAndRestoreCopyDirectLocksVerified": True}

    def cluster_preflight(self) -> None:
        c = self.kjson("get", "cluster", CLUSTER, "-n", NS)
        meta, spec, status = c.get("metadata", {}), c.get("spec", {}), c.get("status", {})
        require((meta.get("labels") or {}).get("repro.stack.token") == self.token
                and canonical_id((meta.get("annotations") or {}).get("vacuum-only-repro/source-snapshot-id"))
                    == canonical_id(self.i["manifest"].get("sourceSnapshotId"))
                and canonical_id((meta.get("annotations") or {}).get("vacuum-only-repro/restore-copy-id"))
                    == canonical_id(self.i["state"].get("snapshotSourceCopyId")),
                "Live Cluster owner/source/copy annotations differ from pinned lineage")
        barman = spec.get("backup", {}).get("barmanObjectStore") or {}
        volume_snapshot = spec.get("backup", {}).get("volumeSnapshot") or {}
        require(spec.get("instances") == 1 and status.get("readyInstances") == 1
                and status.get("currentPrimary") == f"{CLUSTER}-1"
                and spec.get("imageName") == SYSIMAGE
                and spec.get("storage", {}).get("storageClass") == SC
                and spec.get("storage", {}).get("size") == "512Gi"
                and spec.get("walStorage", {}).get("storageClass") == SC
                and spec.get("walStorage", {}).get("size") == "512Gi",
                "Live Cluster is not at the frozen one-instance pre-replica point")
        require(barman.get("destinationPath") == self.barman_url
                and barman.get("serverName") == CLUSTER
                and barman.get("azureCredentials", {}).get("connectionString", {}).get("name") == "azure-barman-credentials"
                and barman.get("azureCredentials", {}).get("connectionString", {}).get("key") == "connection-string"
                and volume_snapshot.get("className") == f"vacuum-backup-snap-{self.short}"
                and volume_snapshot.get("walClassName") == f"vacuum-backup-snap-{self.short}"
                and volume_snapshot.get("online") is True
                and volume_snapshot.get("onlineConfiguration", {}).get("waitForArchive") is True
                and volume_snapshot.get("onlineConfiguration", {}).get("immediateCheckpoint") is False,
                "Live Cluster archive path or online snapshot settings differ from the captured request")
        require(spec.get("bootstrap", {}).get("recovery", {}).get("volumeSnapshots", {})
                .get("storage", {}).get("name") == f"vacuum-data-{self.short}",
                "Cluster bootstrap does not use the exact pinned keeper VolumeSnapshot")
        pods = self.kjson("get", "pods", "-n", NS, "-l", f"cnpg.io/cluster={CLUSTER}")
        require({p.get("metadata", {}).get("name") for p in pods.get("items", [])} == {f"{CLUSTER}-1"}
                and pods["items"][0].get("status", {}).get("phase") == "Running",
                "Pre-resume Cluster unexpectedly has extra or unready pods")
        pvc_list = self.kjson("get", "pvc", "-n", NS, "-l", f"cnpg.io/cluster={CLUSTER}")
        names = {p.get("metadata", {}).get("name") for p in pvc_list.get("items", [])}
        require(names == {f"{CLUSTER}-1", f"{CLUSTER}-1-wal"},
                "Pre-resume Cluster unexpectedly has replica PVCs")
        jobs = self.kjson("get", "jobs", "-n", NS, "-l", f"cnpg.io/cluster={CLUSTER}")
        require(not [j for j in jobs.get("items", [])
                     if (j.get("metadata", {}).get("labels") or {}).get("cnpg.io/instanceName") == f"{CLUSTER}-2"
                     and (j.get("metadata", {}).get("labels") or {}).get("cnpg.io/jobRole") == "snapshot-recovery"],
                "Pre-resume Cluster already has a replica snapshot-recovery Job")
        self.progress["clusterPreflight"] = {"instances": 1, "readyInstances": 1,
            "primary": f"{CLUSTER}-1", "image": SYSIMAGE, "archiveDestination": self.barman_url,
            "replicaPVCsAbsent": True, "replicaSeedJobsAbsent": True}

    def fixture_and_archiver_preflight(self) -> None:
        primary = self.psql_json(f"{CLUSTER}-1", """SELECT json_build_object(
          'systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),
          'inRecovery',pg_is_in_recovery(),'heapBytes',pg_relation_size('public.vacuum_target'),
          'totalBytes',pg_total_relation_size('public.vacuum_target'),
          'liveEstimate',(SELECT n_live_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),
          'deadEstimate',(SELECT n_dead_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),
          'minId',(SELECT id FROM public.vacuum_target ORDER BY id ASC LIMIT 1),
          'maxId',(SELECT id FROM public.vacuum_target ORDER BY id DESC LIMIT 1),
          'payloadBytes',(SELECT octet_length(payload) FROM public.vacuum_target WHERE id%2=0 ORDER BY id LIMIT 1),
          'payloadStoragePlain',(SELECT attstorage='p' FROM pg_attribute WHERE attrelid='public.vacuum_target'::regclass AND attname='payload'),
          'tableAutovacuumDisabled',(SELECT 'autovacuum_enabled=false'=ANY(reloptions) FROM pg_class WHERE oid='public.vacuum_target'::regclass),
          'hasIndex',to_regclass('public.vacuum_target_id_idx') IS NOT NULL,
          'stageTableAbsent',to_regclass('public.vacuum_target_stage') IS NULL,
          'appSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend'))::text;""")
        meta = self.i["sourceMeta"]
        require(str(primary.get("systemIdentifier")) == str(meta.get("systemIdentifier"))
                and primary.get("inRecovery") is False,
                "Current primary does not match the original source system")
        heap = int(primary["heapBytes"])
        target = 100 * GIB
        require(abs(heap - target) <= target // 100 and int(primary["totalBytes"]) >= heap,
                "Existing fixture heap/table bytes are outside the assigned 100-GiB target")
        expected = (target * int(meta["preparedLiveTuplesEstimate"])
                    + int(meta["heapBytes"]) - 1) // int(meta["heapBytes"])
        require(int(primary["liveEstimate"]) == expected == 32_768_000
                and int(primary["deadEstimate"]) == expected,
                "Existing fixture live/dead estimates differ from the exact pinned source-prefix formula")
        require(int(primary["minId"]) > 0 and int(primary["maxId"]) > int(primary["minId"])
                and int(primary["minId"]) % 2 == 0 and int(primary["maxId"]) % 2 == 0
                and primary.get("payloadBytes") == 1536 and primary.get("payloadStoragePlain") is True
                and primary.get("tableAutovacuumDisabled") is True and primary.get("hasIndex") is True
                and primary.get("stageTableAbsent") is True and primary.get("appSessions") == 0,
                "Existing fixture row/index/table/workload checks changed")
        archiver = self.psql_json(f"{CLUSTER}-1", """SELECT json_build_object(
          'inRecovery',pg_is_in_recovery(),'archiveMode',current_setting('archive_mode'),
          'archivedCount',archived_count,'failedCount',failed_count,'lastArchivedWal',last_archived_wal)::text
          FROM pg_stat_archiver;""")
        require(archiver.get("inRecovery") is False and archiver.get("archiveMode") == "on"
                and int(archiver.get("archivedCount", -1)) > 0
                and int(archiver.get("failedCount", -1)) == 0
                and re.fullmatch(r"[0-9A-F]{24}", str(archiver.get("lastArchivedWal", ""))) is not None,
                "Current primary archiver has no successful history or reports failures")
        self.progress["fixtureAndArchiver"] = {"fixture": primary, "sourceHeapBytes": int(meta["heapBytes"]),
            "sourceLiveTuplesEstimate": int(meta["preparedLiveTuplesEstimate"]),
            "expectedTupleEstimate": expected, "tupleCountsAreEstimates": True,
            "archiver": archiver,
            "controlFlowInference": "The exact frozen original script writes captured backup.yaml only after its fixture count/heap/payload guards and archiver/WAL checks; this inference is bound to the old process/script hashes and the live Backup UID/spec. Raw DML output is unavailable."}

    def verify_backup_yaml_and_live(self) -> dict[str, Any]:
        all_backups = self.kjson("get", "backups", "-n", NS)
        owned = [b for b in all_backups.get("items", [])
                 if (b.get("metadata", {}).get("labels") or {}).get("repro.stack.token") == self.token]
        require(len(owned) == 1 and owned[0].get("metadata", {}).get("uid") == self.backup_uid,
                "Expected exactly one existing owner-scoped Backup with the pinned UID")
        backup = self.kjson("get", "backup", self.backup_name, "-n", NS)
        validate_backup_object(backup, self.backup_name, self.token, self.backup_uid)
        phase = backup.get("status", {}).get("phase")
        require(phase != "", "Pinned Backup has no status phase")
        self.backup = backup
        return backup

    def source_pv_disk(self) -> None:
        vsc = self.kjson("get", "volumesnapshotclass", f"vacuum-backup-snap-{self.short}")
        require((vsc.get("metadata", {}).get("labels") or {}).get("repro.stack.token") == self.token
                and vsc.get("driver") == "disk.csi.azure.com" and vsc.get("deletionPolicy") == "Retain",
                "Backup VolumeSnapshotClass is not owner-scoped with Retain")
        pvcs = self.kjson("get", "pvc", "-n", NS, "-l", f"cnpg.io/cluster={CLUSTER}")
        by_pvc = {p.get("metadata", {}).get("name"): p for p in pvcs.get("items", [])}
        require(set(by_pvc) == {f"{CLUSTER}-1", f"{CLUSTER}-1-wal"},
                "Primary source PVC set changed before paired Backup snapshot audit")
        self.pv_pvc = {}
        for claim in (f"{CLUSTER}-1", f"{CLUSTER}-1-wal"):
            pvc = by_pvc[claim]
            require(pvc.get("status", {}).get("phase") == "Bound"
                    and pvc.get("spec", {}).get("storageClassName") == SC
                    and pvc.get("spec", {}).get("resources", {}).get("requests", {}).get("storage") == "512Gi",
                    f"Primary source PVC {claim} is not a bound PremiumV2 512-GiB claim")
            pv_name = pvc.get("spec", {}).get("volumeName")
            pv = self.kjson("get", "pv", str(pv_name))
            ref, csi = pv.get("spec", {}).get("claimRef", {}), pv.get("spec", {}).get("csi", {})
            disk_id = csi.get("volumeHandle")
            require(ref.get("name") == claim and ref.get("namespace") == NS
                    and ref.get("uid") == pvc.get("metadata", {}).get("uid")
                    and csi.get("driver") == "disk.csi.azure.com" and disk_id,
                    f"Primary PV {pv_name} does not bind the exact PVC UID/Azure handle")
            disk = self.azjson("disk", "show", "--ids", disk_id)
            zones = [str(x) for x in disk.get("zones", [])]
            require(canonical_id(disk.get("id")) == canonical_id(disk_id) and disk.get("uniqueId")
                    and canonical_id(disk.get("location")) == canonical_id(self.location)
                    and (disk.get("sku") or {}).get("name") == "PremiumV2_LRS"
                    and int(disk.get("diskSizeGB", disk.get("diskSizeGb", 0))) == 512
                    and disk.get("diskIOPSReadWrite") == 3000 and disk.get("diskMBpsReadWrite") == 125
                    and disk.get("managedBy") and self.zone in zones
                    and canonical_id(disk_id).startswith(canonical_id(
                        f"/subscriptions/{self.sub}/resourceGroups/{self.node_rg}/providers/Microsoft.Compute/disks/")),
                    f"Primary source disk for {claim} violates the exact PremiumV2 owner/settings")
            self.pv_pvc[claim] = {"pvc": pvc, "pv": pv, "disk": disk}

    def snapshot_pair(self, backup: dict[str, Any]) -> dict[str, Any]:
        pair = extract_backup_pair(backup)
        self.pair = pair
        found: dict[str, Any] = {}
        for key, claim in (("data", f"{CLUSTER}-1"), ("wal", f"{CLUSTER}-1-wal")):
            snap_name = pair[key]
            vs = self.kjson("get", "volumesnapshot", snap_name, "-n", NS)
            vm = vs.get("metadata", {})
            labels = vm.get("labels") or {}
            require(labels.get("cnpg.io/cluster") == CLUSTER
                    and labels.get("cnpg.io/backupName") == self.backup_name
                    and labels.get("repro.stack.token") == self.token
                    and vs.get("spec", {}).get("source", {}).get("persistentVolumeClaimName") == claim
                    and vs.get("spec", {}).get("volumeSnapshotClassName") == f"vacuum-backup-snap-{self.short}"
                    and vs.get("spec", {}).get("deletionPolicy", "Retain") == "Retain",
                    f"Backup VolumeSnapshot {snap_name} owner/request/source mismatch")
            content_name = vs.get("status", {}).get("boundVolumeSnapshotContentName")
            source = self.pv_pvc[claim]
            disk = source["disk"]
            disk_id = source["pv"]["spec"]["csi"]["volumeHandle"]
            if not content_name:
                partial = {"type": key, "volumeSnapshotUID": vm.get("uid"),
                           "volumeSnapshotContent": None, "sourcePVC": claim,
                           "sourcePV": source["pv"].get("metadata", {}).get("name"),
                           "sourcePVCUID": source["pvc"].get("metadata", {}).get("uid"),
                           "sourceDiskId": disk_id, "sourceDiskUniqueId": disk.get("uniqueId"),
                           "readyToUse": vs.get("status", {}).get("readyToUse") is True,
                           "completionPercent": None}
                extend_snapshot_identity_pins(self.snapshot_identity_pins, snap_name, partial)
                found[snap_name] = partial
                continue
            content = self.kjson("get", "volumesnapshotcontent", str(content_name))
            cm, cs = content.get("metadata", {}), content.get("spec", {})
            status = content.get("status", {})
            snap_id = status.get("snapshotHandle")
            require((cs.get("volumeSnapshotRef") or {}).get("uid") == vm.get("uid")
                    and cs.get("deletionPolicy") == "Retain" and cs.get("driver") == "disk.csi.azure.com"
                    and canonical_id((cs.get("source") or {}).get("volumeHandle")) == canonical_id(disk_id)
                    and (snap_id is None or canonical_id(status.get("snapshotHandle")) == canonical_id(snap_id)),
                    f"Backup VolumeSnapshotContent {content_name} UID/source/handle mismatch")
            partial = {"type": key, "volumeSnapshotUID": vm.get("uid"),
                "volumeSnapshotContent": content_name, "volumeSnapshotContentUID": cm.get("uid"),
                "sourcePVC": claim, "sourcePV": source["pv"].get("metadata", {}).get("name"),
                "sourcePVCUID": source["pvc"].get("metadata", {}).get("uid"),
                "id": snap_id, "sourceDiskId": disk_id, "sourceDiskUniqueId": disk.get("uniqueId"),
                "readyToUse": vs.get("status", {}).get("readyToUse") is True,
                "completionPercent": None}
            extend_snapshot_identity_pins(self.snapshot_identity_pins, snap_name, partial)
            if (vs.get("status", {}).get("readyToUse") is not True
                    or status.get("readyToUse") is not True or not snap_id):
                found[snap_name] = partial
                continue
            arm = self.azjson("snapshot", "show", "--ids", snap_id)
            require(canonical_id(arm.get("id")) == canonical_id(snap_id) and arm.get("uniqueId")
                    and canonical_id(snap_id).startswith(canonical_id(
                        f"/subscriptions/{self.sub}/resourceGroups/{self.snap_rg}/providers/Microsoft.Compute/snapshots/"))
                    and canonical_id(arm.get("location")) == canonical_id(self.location)
                    and arm.get("provisioningState") != "Failed"
                    and int(arm.get("diskSizeGB", arm.get("diskSizeGb", 0))) == 512
                    and (arm.get("sku") or {}).get("name") in ("Standard_LRS", "Standard_ZRS")
                    and arm.get("incremental") is True
                    and (arm.get("tags") or {}).get("reprotoken") == self.token
                    and (arm.get("tags") or {}).get("reproPurpose") == "premiumv2-vacuum-backup"
                    and canonical_id((arm.get("creationData") or {}).get("sourceResourceId")) == canonical_id(disk_id)
                    and (arm.get("creationData") or {}).get("sourceUniqueId") == disk.get("uniqueId"),
                    f"Backup ARM snapshot {snap_name} is outside exact owner/source lineage")
            validate_backup_snapshot_access_state(arm.get("snapshotAccessState"))
            option = (arm.get("creationData") or {}).get("createOption")
            require(option in ("Copy", "CopyStart"),
                    f"Backup snapshot {snap_name} is not a Copy/CopyStart from its PremiumV2 source")
            row = {"type": key, "id": snap_id, "uniqueId": arm["uniqueId"],
                "volumeSnapshotUID": vm.get("uid"), "volumeSnapshotContent": content_name,
                "volumeSnapshotContentUID": cm.get("uid"), "sourcePVC": claim,
                "sourcePV": self.pv_pvc[claim]["pv"].get("metadata", {}).get("name"),
                "sourcePVCUID": self.pv_pvc[claim]["pvc"].get("metadata", {}).get("uid"),
                "sourceDiskId": disk_id, "sourceDiskUniqueId": disk.get("uniqueId"),
                "sourceUniqueId": (arm.get("creationData") or {}).get("sourceUniqueId"),
                "createOption": option}
            extend_snapshot_identity_pins(self.snapshot_identity_pins, snap_name, row)
            self.protect_snapshot_promptly(snap_id, disk_id, str(disk.get("uniqueId")))
            percent = completion_percent(arm.get("completionPercent"))
            found[snap_name] = {**row, "provisioningState": arm.get("provisioningState"),
                "snapshotAccessState": arm.get("snapshotAccessState"),
                "readyToUse": True, "completionPercent": None if percent is None else float(percent)}
        require(len(found) == 2, "Paired Backup outputs are incomplete")
        self.pair, self.snapshot_evidence = pair, found
        return {"pair": pair, "snapshots": found}

    def _lock_scope(self, lock: dict[str, Any]) -> str:
        scope = lock.get("scope")
        if not scope and lock.get("id"):
            lock_id = canonical_id(lock["id"])
            marker = "/providers/microsoft.authorization/locks/"
            scope = lock_id.split(marker, 1)[0] if marker in lock_id else lock_id
        return canonical_id(scope)

    def verify_snapshot_group_lock(self) -> None:
        locks = self.azjson("lock", "list", "--resource-group", self.snap_rg)
        group_id = canonical_id(f"/subscriptions/{self.sub}/resourceGroups/{self.snap_rg}")
        require(any(lock.get("level") == "CanNotDelete" and self._lock_scope(lock) == group_id
                    for lock in locks),
                "Snapshot resource group lacks inherited CanNotDelete protection for not-yet-discovered Backup snapshots")

    def protect_snapshot_promptly(self, snapshot_id: str, source_disk_id: str,
                                  source_disk_unique_id: str) -> None:
        """Add a direct lock as soon as an exact owner/source-bound ID appears."""
        require(self.continuation_mode,
                "Read-only preflight cannot create snapshot locks")
        locks = self.azjson("lock", "list", "--resource-group", self.snap_rg)
        if any(lock.get("level") == "CanNotDelete" and self._lock_scope(lock) == canonical_id(snapshot_id)
               for lock in locks):
            return
        # These checks are deliberately repeated immediately before adding a
        # lock. The group lock already protects not-yet-discovered children.
        self.owner_preflight()
        self.source_lineage_preflight()
        self.cluster_preflight()
        self.fixture_and_archiver_preflight()
        self.source_pv_disk()
        require(any(canonical_id(data["pv"]["spec"]["csi"]["volumeHandle"]) == canonical_id(source_disk_id)
                    and data["disk"].get("uniqueId") == source_disk_unique_id
                    for data in self.pv_pvc.values()),
                "Primary source disk ID/UID changed before protecting its Backup snapshot")
        backup = self.verify_backup_yaml_and_live()
        require(backup.get("status", {}).get("phase") == "completed"
                and extract_backup_pair(backup) == self.pair,
                "Pinned Backup/pair changed before protecting a discovered snapshot")
        self.verify_snapshot_group_lock()
        require(canonical_id(snapshot_id).startswith(canonical_id(
                    f"/subscriptions/{self.sub}/resourceGroups/{self.snap_rg}/providers/Microsoft.Compute/snapshots/")),
                "Refusing to lock a snapshot outside the owned retained RG")
        name = snapshot_id.rstrip("/").rsplit("/", 1)[-1]
        self.az("lock", "create", "--name", f"retain-{name}", "--lock-type", "CanNotDelete",
                "--resource-group", self.snap_rg, "--resource-name", name,
                "--resource-type", "Microsoft.Compute/snapshots", "--notes",
                "Snapshot retained by the Premium SSD v2 VACUUM experiment until explicit user authorization.",
                "-o", "none")
        locks = self.azjson("lock", "list", "--resource-group", self.snap_rg)
        require(any(lock.get("level") == "CanNotDelete" and self._lock_scope(lock) == canonical_id(snapshot_id)
                    for lock in locks),
                f"Direct snapshot retention lock did not verify for {name}")

    def wait_backup_and_copy(self) -> None:
        end = time.monotonic() + WAIT_SECONDS
        last_log = 0.0
        self.progress["phase"] = "waiting-for-existing-backup"
        self.save_json(self.output / "resume-progress.json", self.progress)
        while time.monotonic() < end:
            backup = self.verify_backup_yaml_and_live()
            phase = backup.get("status", {}).get("phase")
            if phase == "completed":
                break
            now = time.monotonic()
            if now - last_log >= 900:
                print(f"Pinned existing Backup {self.backup_name} remains in phase {phase!r}; waiting.", flush=True)
                last_log = now
                self.progress["backupPhase"] = phase
                self.progress["lastWaitAtUtc"] = dt.datetime.now(dt.timezone.utc).isoformat()
                self.save_json(self.output / "resume-progress.json", self.progress)
            time.sleep(60)
        else:
            raise ResumeError("Pinned existing Backup did not complete within 24 hours")
        require(self.backup is not None, "Existing Backup disappeared")
        self.pair = extract_backup_pair(self.backup)
        self.source_pv_disk()
        end = time.monotonic() + WAIT_SECONDS
        last_log = 0.0
        self.progress["phase"] = "waiting-for-paired-copy-100"
        self.progress["backupPhase"] = "completed"
        self.save_json(self.output / "resume-progress.json", self.progress)
        while time.monotonic() < end:
            evidence = self.snapshot_pair(self.backup)
            not_ready = [name for name, row in evidence["snapshots"].items()
                         if row.get("provisioningState") != "Succeeded"
                         or row.get("snapshotAccessState") != "Available"
                         or row.get("readyToUse") is not True
                         or row.get("completionPercent") != 100.0
                         or not row.get("id") or not row.get("uniqueId")]
            if not not_ready:
                require(len({x["id"].lower() for x in evidence["snapshots"].values()}) == 2
                        and len({x["uniqueId"] for x in evidence["snapshots"].values()}) == 2
                        and set(self.snapshot_identity_pins) == set(evidence["snapshots"]),
                        "Paired Backup snapshots are not distinct ARM resources/UIDs")
                require(all(set(SNAPSHOT_IDENTITY_FIELDS).issubset(self.snapshot_identity_pins.get(name, {}))
                            for name in evidence["snapshots"]),
                        "Paired Backup snapshot identity was not completely pinned before Copy100")
                break
            now = time.monotonic()
            if now - last_log >= 900:
                values = ", ".join(f"{name}={evidence['snapshots'][name].get('completionPercent')}%"
                                    for name in not_ready)
                print(f"Existing paired Backup snapshot copies remain incomplete ({values}); waiting.", flush=True)
                last_log = now
                self.progress["lastCopyProgress"] = {name: evidence["snapshots"][name]["completionPercent"]
                                                       for name in not_ready}
                self.progress["lastWaitAtUtc"] = dt.datetime.now(dt.timezone.utc).isoformat()
                self.save_json(self.output / "resume-progress.json", self.progress)
            time.sleep(60)
        else:
            raise ResumeError("Existing Backup snapshots did not reach Copy 100 within 24 hours")
        self.progress["phase"] = "backup-and-copy-100-complete"
        self.progress["backup"] = {"name": self.backup_name, "uid": self.backup_uid,
                                    "phase": "completed", "pair": self.pair,
                                    "createdAt": self.backup.get("metadata", {}).get("creationTimestamp")}
        self.progress["pairedSnapshots"] = self.snapshot_evidence
        self.progress["pinnedSnapshotIdentity"] = copy.deepcopy(self.snapshot_identity_pins)
        self.save_json(self.output / "resume-progress.json", self.progress)

    def ensure_snapshot_locks(self) -> None:
        """Read-only verification after each direct lock was installed promptly."""
        for row in self.snapshot_evidence.values():
            snapshot_id, name = row["id"], row["id"].rsplit("/", 1)[-1]
            locks = self.azjson("lock", "list", "--resource-group", self.snap_rg)
            require(any(lock.get("level") == "CanNotDelete" and self._lock_scope(lock) == canonical_id(snapshot_id)
                        for lock in locks),
                    f"Direct snapshot retention lock did not verify for {name}")
        rows = [{"id": item["id"], "uniqueId": item["uniqueId"], "completionPercent": item["completionPercent"]}
                for item in self.snapshot_evidence.values()]
        self.progress["retentionLocks"] = rows
        self.save_json(self.output / "resume-progress.json", self.progress)

    def apply_cluster_patch(self, patch: str, mode: str = "merge") -> None:
        self.k("patch", "cluster", CLUSTER, "-n", NS, f"--type={mode}", "-p", patch)

    def wait_ready(self, count: int, timeout_seconds: int = 10800) -> dict[str, Any]:
        end = time.monotonic() + timeout_seconds
        while time.monotonic() < end:
            cluster = self.kjson("get", "cluster", CLUSTER, "-n", NS)
            pods = self.kjson("get", "pods", "-n", NS, "-l", f"cnpg.io/cluster={CLUSTER}")
            ready = [p for p in pods.get("items", []) if p.get("status", {}).get("phase") == "Running"
                     and any(s.get("name") == "postgres" and s.get("ready") is True
                             for s in p.get("status", {}).get("containerStatuses", []))]
            if cluster.get("status", {}).get("readyInstances") == count and len(ready) == count:
                return cluster
            time.sleep(10)
        raise ResumeError(f"Cluster did not reach {count} ready instances")

    def start_and_verify_seed(self) -> tuple[str, dict[str, Any]]:
        c = self.kjson("get", "cluster", CLUSTER, "-n", NS)
        spec = c.get("spec", {})
        require(spec.get("instances") == 1 and c.get("status", {}).get("readyInstances") == 1,
                "Cluster moved from the pinned pre-replica boundary; refusing to duplicate seed work")
        pvc_list = self.kjson("get", "pvc", "-n", NS, "-l", f"cnpg.io/cluster={CLUSTER}")
        require({p.get("metadata", {}).get("name") for p in pvc_list.get("items", [])}
                == {f"{CLUSTER}-1", f"{CLUSTER}-1-wal"},
                "Replica PVCs appeared before the continuation; stop for manual review")
        seed_started = dt.datetime.now(dt.timezone.utc)
        self.apply_cluster_patch('{"spec":{"instances":2,"postgresql":{"synchronous":{"method":"any","number":1,"dataDurability":"preferred"}}}}')
        end = time.monotonic() + 10800
        job: dict[str, Any] | None = None
        job_uid = ""
        event_uid = ""
        seed_events: dict[str, Any] | None = None
        cluster: dict[str, Any] = {}
        while time.monotonic() < end:
            jobs = self.kjson("get", "jobs", "-n", NS, "-l", f"cnpg.io/cluster={CLUSTER}")
            candidates = [j for j in jobs.get("items", [])
                          if (j.get("metadata", {}).get("labels") or {}).get("cnpg.io/instanceName") == f"{CLUSTER}-2"
                          and (j.get("metadata", {}).get("labels") or {}).get("cnpg.io/jobRole") == "snapshot-recovery"]
            require(len(candidates) <= 1, "Duplicate replica snapshot-recovery Jobs appeared")
            if candidates:
                job = candidates[0]
                job_uid = str(job.get("metadata", {}).get("uid", ""))
                containers = [x for x in job.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
                              if x.get("name") == "snapshot-recovery"]
                require(len(containers) == 1 and containers[0].get("image") == SYSIMAGE
                        and "restoresnapshot" in " ".join(containers[0].get("command", [])).lower(),
                        "Replica seed Job differs from pinned CNPG restoresnapshot image/command")
                require((job.get("status", {}).get("failed") or 0) == 0,
                        "Replica snapshot-recovery Job reports failure")
                require(not event_uid or not job_uid or event_uid == job_uid,
                        "Completed seed Event UID differs from the live Job UID")
            events_now = self.kjson("get", "events", "-n", NS)
            candidates_events = []
            for event in events_now.get("items", []):
                obj = event.get("involvedObject") or {}
                if (obj.get("kind") == "Job" and obj.get("name") == f"{CLUSTER}-2-snapshot-recovery"
                        and event.get("reason") == "Completed" and event.get("message") == "Job completed"):
                    at = utc_time(seed_event_timestamp(event), "Completed seed Event")
                    if at >= seed_started:
                        candidates_events.append(event)
            event_uids = {str((event.get("involvedObject") or {}).get("uid")) for event in candidates_events
                          if (event.get("involvedObject") or {}).get("uid")}
            require(len(event_uids) <= 1, "More than one new Completed seed Job Event appeared")
            if event_uids:
                event_uid = next(iter(event_uids))
                seed_events = events_now
                require(not job_uid or event_uid == job_uid,
                        "Completed seed Event UID differs from the observed Job UID")
            cluster = self.kjson("get", "cluster", CLUSTER, "-n", NS)
            job_complete = job is not None and job.get("status", {}).get("succeeded", 0) >= 1
            event_complete = bool(event_uid)
            if cluster.get("status", {}).get("readyInstances") == 2 and (job_complete or event_complete) and event_complete:
                break
            time.sleep(5)
        require(job_uid or event_uid, "Snapshot-recovery Job UID was not observed live or in its exact completion Event")
        if job_uid and event_uid:
            require(job_uid == event_uid, "Completed seed Event UID differs from the observed Job UID")
        if not job_uid:
            job_uid = event_uid
        require(cluster.get("status", {}).get("readyInstances") == 2,
                "Replica did not become ready after snapshot recovery")
        c = self.wait_ready(2)
        require(c.get("status", {}).get("currentPrimary") == f"{CLUSTER}-1",
                "Replica seed changed primary unexpectedly")
        pvcs = self.kjson("get", "pvc", "-n", NS, "-l", f"cnpg.io/cluster={CLUSTER}")
        by = {p.get("metadata", {}).get("name"): p for p in pvcs.get("items", [])}
        require(set(by) == {f"{CLUSTER}-1", f"{CLUSTER}-1-wal", f"{CLUSTER}-2", f"{CLUSTER}-2-wal"},
                "Replica seed did not produce exactly four DATA/WAL PVCs")
        for claim, snap in ((f"{CLUSTER}-2", self.pair["data"]), (f"{CLUSTER}-2-wal", self.pair["wal"])):
            pvc = by[claim]
            require(pvc.get("status", {}).get("phase") == "Bound"
                    and pvc.get("spec", {}).get("dataSource", {}).get("kind") == "VolumeSnapshot"
                    and pvc.get("spec", {}).get("dataSource", {}).get("name") == snap,
                    f"Replica PVC {claim} is not bound from its exact paired Backup snapshot")
        require(seed_events is not None, "Completed seed Event was not captured while monitoring the Job")
        pods = self.kjson("get", "pods", "-n", NS, "-l", f"cnpg.io/cluster={CLUSTER}")
        pvc_time = max(p.get("metadata", {}).get("creationTimestamp", "") for p in by.values()
                       if p.get("metadata", {}).get("name") in (f"{CLUSTER}-2", f"{CLUSTER}-2-wal"))
        pod_time = next(p.get("metadata", {}).get("creationTimestamp", "") for p in pods.get("items", [])
                        if p.get("metadata", {}).get("name") == f"{CLUSTER}-2")
        completed_event = select_completed_seed_event(seed_events.get("items", []), job_uid, pvc_time, pod_time)
        self.seed_job_uid = job_uid
        self.seed_events_document = seed_events
        self.save_json(self.output / "seed-events.json", self.seed_events_document)
        self.save_json(self.output / "seed-job.json", {"uid": job_uid,
            "source": "live-job" if job is not None else "completed-event-after-job-gc",
            "jobSucceeded": job is not None and job.get("status", {}).get("succeeded", 0) >= 1,
            "completedEvent": completed_event, "systemImage": SYSIMAGE,
            "sourceBackupSnapshots": self.pair})
        self.progress["seed"] = {"jobUID": job_uid, "completed": True,
                                  "completedEventTimestamp": seed_event_timestamp(completed_event),
                                  "eventsSha256": sha256(self.output / "seed-events.json")}
        self.seed_trace = {"jobUID": job_uid, "completedEvent": completed_event,
                           "completedEventTimestamp": seed_event_timestamp(completed_event),
                           "jobEvidence": "live-job" if job is not None else "completed-event-after-job-gc",
                           "jobSucceeded": job is not None and job.get("status", {}).get("succeeded", 0) >= 1,
                           "sourceBackupSnapshots": self.pair,
                           "replicaPVCs": {name: {"uid": by[name].get("metadata", {}).get("uid"),
                                                  "creationTimestamp": by[name].get("metadata", {}).get("creationTimestamp"),
                                                  "dataSource": by[name].get("spec", {}).get("dataSource")}
                                           for name in (f"{CLUSTER}-2", f"{CLUSTER}-2-wal")},
                           "replicaPod": {"name": f"{CLUSTER}-2", "uid": next(
                               p.get("metadata", {}).get("uid") for p in pods.get("items", [])
                               if p.get("metadata", {}).get("name") == f"{CLUSTER}-2"),
                               "creationTimestamp": pod_time}}
        self.save_json(self.output / "resume-progress.json", self.progress)
        return job_uid, c

    def archive_cache_status(self, pod: str) -> str:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        log = self.output / f"port-forward-{pod}.log"
        with log.open("w") as stream:
            os.chmod(log, 0o600)
            child = subprocess.Popen(port_forward_argv(pod, port),
                                     env=self.env, stdout=stream, stderr=stream)
            try:
                end = time.monotonic() + 30
                while time.monotonic() < end:
                    code = invoke(["curl", "--silent", "--show-error", "--max-time", "5", "-o", "/dev/null",
                                   "-w", "%{http_code}", f"http://127.0.0.1:{port}/cache/wal-archive"],
                                  self.env, timeout=7, allow_fail=True).strip()
                    if code in ("200", "404"):
                        return code
                    time.sleep(1)
                raise ResumeError(f"Archive cache endpoint did not respond for {pod}")
            finally:
                stop_port_forward(child)

    def remove_archive_and_switch_image(self) -> None:
        c = self.kjson("get", "cluster", CLUSTER, "-n", NS)
        pods = self.kjson("get", "pods", "-n", NS, "-l", f"cnpg.io/cluster={CLUSTER}")
        before = {p.get("metadata", {}).get("name"): p.get("metadata", {}).get("uid")
                  for p in pods.get("items", [])}
        require(set(before) == {f"{CLUSTER}-1", f"{CLUSTER}-2"},
                "Cannot pin the two running pod UIDs before archive writer removal")
        require(c.get("spec", {}).get("backup", {}).get("barmanObjectStore") is not None,
                "Archive writer is already absent; stop instead of repeating tail mutations")
        self.k("patch", "cluster", CLUSTER, "-n", NS, "--type=json", "-p",
               '[{"op":"remove","path":"/spec/backup/barmanObjectStore"}]')
        end = time.monotonic() + 180
        while time.monotonic() < end:
            c = self.kjson("get", "cluster", CLUSTER, "-n", NS)
            pods = self.kjson("get", "pods", "-n", NS, "-l", f"cnpg.io/cluster={CLUSTER}")
            after = {p.get("metadata", {}).get("name"): p.get("metadata", {}).get("uid")
                     for p in pods.get("items", [])}
            if c.get("status", {}).get("readyInstances") == 2 and after == before:
                break
            time.sleep(5)
        else:
            raise ResumeError("Removing archive writer changed pods or prevented cluster readiness")
        require(c.get("spec", {}).get("backup", {}).get("barmanObjectStore") is None,
                "Archive writer remains configured")
        cache_status = {}
        end = time.monotonic() + 180
        while time.monotonic() < end:
            cache_status = {name: self.archive_cache_status(name) for name in (f"{CLUSTER}-1", f"{CLUSTER}-2")}
            require(all(code in ("200", "404") for code in cache_status.values()),
                    "Archive cache returned an unexpected status")
            if all(code == "404" for code in cache_status.values()):
                break
            time.sleep(5)
        require(cache_status == {f"{CLUSTER}-1": "404", f"{CLUSTER}-2": "404"},
                "Archive cache did not clear before removing the credential Secret")
        secret = self.kjson("get", "secret", "azure-barman-credentials", "-n", NS)
        require((secret.get("metadata", {}).get("labels") or {}).get("repro.stack.token") == self.token
                and isinstance((secret.get("data") or {}).get("connection-string"), str)
                and bool(secret["data"]["connection-string"]),
                "Archive Secret owner/key changed before its original tail removal")
        self.k("delete", "secret", "azure-barman-credentials", "-n", NS, "--wait=true")
        self.k("patch", "cluster", CLUSTER, "-n", NS, "--type=merge", "-p",
               json.dumps({"spec": {"imageName": IMAGE, "primaryUpdateMethod": "restart"}}))
        digest = IMAGE.split("@", 1)[1]
        end = time.monotonic() + 10800
        while time.monotonic() < end:
            c = self.kjson("get", "cluster", CLUSTER, "-n", NS)
            pods = self.kjson("get", "pods", "-n", NS, "-l", f"cnpg.io/cluster={CLUSTER}")
            ready = []
            for p in pods.get("items", []):
                if p.get("status", {}).get("phase") != "Running":
                    continue
                containers = p.get("spec", {}).get("containers", [])
                status = p.get("status", {}).get("containerStatuses", [])
                if (any(x.get("name") == "postgres" and x.get("image") == IMAGE for x in containers)
                        and any(x.get("name") == "postgres" and x.get("ready") is True
                                and str(x.get("imageID", "")).endswith(digest) for x in status)):
                    ready.append(p)
            if c.get("status", {}).get("readyInstances") == 2 and len(ready) == 2:
                break
            time.sleep(10)
        else:
            raise ResumeError("Instances did not reach the pinned measurement image")
        require(c.get("status", {}).get("currentPrimary") == f"{CLUSTER}-1"
                and c.get("spec", {}).get("backup", {}).get("barmanObjectStore") is None
                and c.get("spec", {}).get("walStorage", {}).get("size") == "512Gi",
                "Post-image Cluster settings differ from the original final gate")
        self.progress["archiveAndImageTail"] = {"archiveWriterRemoved": True,
            "cacheStatus": cache_status, "archiveSecretRemoved": True,
            "image": IMAGE, "readyInstances": 2, "currentPrimary": f"{CLUSTER}-1"}
        self.save_json(self.output / "resume-progress.json", self.progress)

    def capture_final_control(self, seed_uid: str) -> None:
        pods = self.kjson("get", "pods", "-n", NS, "-l", f"cnpg.io/cluster={CLUSTER}")
        pvcs = self.kjson("get", "pvc", "-n", NS, "-l", f"cnpg.io/cluster={CLUSTER}")
        self.save_json(self.control_out / "pods-final.json", pods)
        self.save_json(self.control_out / "pvcs-final.json", pvcs)
        cfg_sql = """SELECT json_build_object(
          'systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),
          'inRecovery',pg_is_in_recovery(),'heapBytes',pg_relation_size('public.vacuum_target'),
          'totalBytes',pg_total_relation_size('public.vacuum_target'),
          'payloadBytes',(SELECT octet_length(payload) FROM public.vacuum_target WHERE id%2=0 ORDER BY id LIMIT 1),
          'payloadStoragePlain',(SELECT attstorage='p' FROM pg_attribute WHERE attrelid='public.vacuum_target'::regclass AND attname='payload'),
          'tableAutovacuumDisabled',(SELECT 'autovacuum_enabled=false'=ANY(reloptions) FROM pg_class WHERE oid='public.vacuum_target'::regclass),
          'hasIndex',to_regclass('public.vacuum_target_id_idx') IS NOT NULL,
          'stageTableAbsent',to_regclass('public.vacuum_target_stage') IS NULL,
          'liveEstimate',(SELECT n_live_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),
          'deadEstimate',(SELECT n_dead_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),
          'minId',(SELECT id FROM public.vacuum_target ORDER BY id ASC LIMIT 1),
          'maxId',(SELECT id FROM public.vacuum_target ORDER BY id DESC LIMIT 1),
          'autovacuum',current_setting('autovacuum'),'walLevel',current_setting('wal_level'),
          'fullPageWrites',current_setting('full_page_writes'),'recoveryPrefetch',current_setting('recovery_prefetch'),
          'vacuumCostDelay',current_setting('vacuum_cost_delay'),'sharedBuffers',current_setting('shared_buffers'),
          'maintenanceIoConcurrency',current_setting('maintenance_io_concurrency'),
          'checkpointTimeout',current_setting('checkpoint_timeout'),'maxWalSize',current_setting('max_wal_size'),
          'archiveMode',current_setting('archive_mode'),'trackIoTiming',current_setting('track_io_timing'),
          'trackWalIoTiming',current_setting('track_wal_io_timing'),
          'tablespaceOverride',EXISTS(SELECT 1 FROM pg_tablespace t,unnest(t.spcoptions) o WHERE t.spcname='pg_default' AND o LIKE 'maintenance_io_concurrency=%'),
          'appSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend'))::text;"""
        for pod in (f"{CLUSTER}-1", f"{CLUSTER}-2"):
            row = self.psql_json(pod, cfg_sql)
            self.save_json(self.control_out / f"{pod}-config.json", row)
        by = {p.get("metadata", {}).get("name"): p for p in pvcs.get("items", [])}
        claim = f"{CLUSTER}-1"
        require(claim in by, "Final primary DATA PVC is missing")
        pvname = by[claim].get("spec", {}).get("volumeName")
        pv = self.kjson("get", "pv", str(pvname))
        self.save_json(self.control_out / "hdd-pg-1-pv.json", pv)
        disk = self.azjson("disk", "show", "--ids", pv.get("spec", {}).get("csi", {}).get("volumeHandle"))
        self.save_json(self.control_out / "hdd-pg-1-disk.json", disk)
        # The seed Event list is captured before Kubernetes' short-lived Event
        # retention can erase the exact Job UID. No Secret data is read here.
        events = self.kjson("get", "events", "-n", NS)
        completed = [e for e in events.get("items", [])
                     if (e.get("involvedObject") or {}).get("uid") == seed_uid
                     and e.get("reason") == "Completed" and e.get("message") == "Job completed"]
        if completed:
            require(len(completed) == 1, "Exact completed replica seed Event is ambiguous")
            self.seed_events_document = events
        require(self.seed_events_document is not None,
                "Exact completed replica seed Event was not preserved before Event expiry")
        self.save_json(self.output / "seed-events.json", self.seed_events_document)
        self.progress["finalControlCapture"] = {
            "path": str(self.control_out.relative_to(ROOT)),
            "sha256": {p.name: sha256(p) for p in sorted(self.control_out.iterdir()) if p.is_file()},
            "seedJobUID": seed_uid,
            "seedEventsPath": str((self.output / "seed-events.json").relative_to(ROOT)),
            "seedEventsSha256": sha256(self.output / "seed-events.json")}
        self.save_json(self.output / "resume-progress.json", self.progress)

    def continue_original_tail(self) -> None:
        # Recheck the original Backup UID and both full-copy records immediately
        # before any Cluster mutation; never submit Backup YAML here.
        self.owner_preflight()
        self.source_lineage_preflight()
        self.cluster_preflight()
        self.fixture_and_archiver_preflight()
        self.source_pv_disk()
        backup = self.verify_backup_yaml_and_live()
        require(backup.get("status", {}).get("phase") == "completed"
                and extract_backup_pair(backup) == self.pair,
                "Pinned Backup changed before tail continuation")
        expected_snapshot_identity = copy.deepcopy(self.snapshot_identity_pins)
        latest = self.snapshot_pair(backup)["snapshots"]
        validate_snapshot_identity_pins(expected_snapshot_identity, self.snapshot_identity_pins)
        require(all(row.get("provisioningState") == "Succeeded"
                    and row.get("snapshotAccessState") == "Available"
                    and row.get("completionPercent") == 100.0
                    and row.get("readyToUse") is True
                    and all(row.get(field) == expected_snapshot_identity.get(name, {}).get(field)
                            for field in SNAPSHOT_IDENTITY_FIELDS)
                    for name, row in latest.items()),
                "Paired Backup snapshot identities or Copy100 readiness changed before tail continuation")
        self.snapshot_evidence = latest
        self.ensure_snapshot_locks()
        uid, _ = self.start_and_verify_seed()
        self.remove_archive_and_switch_image()
        # The original tail checks streaming and replay before creating metadata.
        end = time.monotonic() + 1800
        while time.monotonic() < end:
            streaming = self.k("exec", "-n", NS, "-c", "postgres", f"{CLUSTER}-1", "--", "psql", "-XAtq",
                               "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "repro", "-c",
                               "SELECT count(*) FROM pg_stat_replication WHERE state='streaming' AND replay_lsn>=sent_lsn;").strip()
            if streaming == "1":
                break
            time.sleep(5)
        else:
            raise ResumeError("Replica did not reach streaming/caught-up state")
        primary_lsn = self.k("exec", "-n", NS, "-c", "postgres", f"{CLUSTER}-1", "--", "psql", "-XAtq",
                            "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "repro", "-c",
                            "SELECT pg_current_wal_flush_lsn();").strip()
        end = time.monotonic() + 3600
        while time.monotonic() < end:
            replay = self.k("exec", "-n", NS, "-c", "postgres", f"{CLUSTER}-2", "--", "psql", "-XAtq",
                            "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "repro", "-c",
                            f"SELECT pg_last_wal_replay_lsn()>='{primary_lsn}'::pg_lsn;").strip()
            if replay == "t":
                break
            time.sleep(5)
        else:
            raise ResumeError("Replica did not replay through the sampled primary flush LSN")
        self.progress["replicaCatchup"] = {"streaming": True, "primaryFlushLsn": primary_lsn,
                                           "replicaCaughtUp": True}
        self.capture_final_control(uid)
        # The finalizer is pinned separately by root. Its read-only live audit
        # builds canonical prepare.json only after all four measured disks and
        # paired snapshots are Copy100, all GUC/fixture/owner guards pass, and
        # no global-quiescence claim is made.
        records = [
            {"id": self.snapshot_evidence[name]["id"], "volumeSnapshot": name,
             "sourcePVC": self.snapshot_evidence[name]["sourcePVC"]}
            for name in (self.pair["data"], self.pair["wal"])
        ]
        journal = self.output / "output-snapshots.jsonl"
        with journal.open("x") as stream:
            os.chmod(journal, 0o600)
            for row in records:
                json.dump(row, stream, indent=2, sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
        self.progress["externalSnapshotJournal"] = {"path": str(journal.relative_to(ROOT)),
                                                     "sha256": sha256(journal), "documents": 2}
        self.progress["phase"] = "tail-complete-awaiting-frozen-finalizer"
        self.progress["nextStep"] = "Root may install external journal into .state and invoke frozen hydration finalizer with archived process/script and captured seed UID/Event."
        require(sha256(Path(__file__)) == self.progress["resumerScriptSha256"],
                "Resumer script bytes changed during continuation")
        self.save_json(self.output / "continuation-evidence.json", {
            "version": 1, "phase": "tail-complete-awaiting-frozen-finalizer",
            "executionAttribution": "Original 02 reached only the captured pre-Backup control-flow boundary. Seed, archive removal, image switch, and final captures were performed by this pinned resumer continuation.",
            "resumerScriptSha256": self.progress["resumerScriptSha256"],
            "originalScriptSha256": ORIGINAL_02_SHA,
            "originalProcessReceiptSha256": self.progress["originalProcessReceiptSha256"],
            "originalProcessReceiptPath": str(self.i["processPath"].relative_to(ROOT)),
            "originalRunningProcessReceiptPath": str(self.i["runningProcessPath"].relative_to(ROOT)),
            "originalRunningProcessReceiptSha256": self.progress["originalRunningProcessReceiptSha256"],
            "terminalProcessBindingPath": str(self.i["terminalBindingPath"].relative_to(ROOT)),
            "terminalProcessBindingSha256": self.progress["terminalProcessBindingSha256"],
            "originalScriptPath": str(self.i["originalScript"].relative_to(ROOT)),
            "preBackupCapturePath": str(self.i["controlSource"].relative_to(ROOT)),
            "preBackupCaptureFileSha256": self.i["inputHashes"],
            "preBackupCaptureReceiptPath": str(self.i["captureReceiptPath"].relative_to(ROOT)),
            "preBackupCaptureReceiptSha256": self.i["inputHashes"]["captureReceiptSha256"],
            "captureHelperPath": "recovery/backup-wait-resume/capture_prebackup_tmp.py",
            "captureHelperSha256": CAPTURE_HELPER_SHA,
            "terminalBindingHelperPath": "recovery/backup-wait-resume/bind_terminal_process.py",
            "terminalBindingHelperSha256": BIND_TERMINAL_HELPER_SHA,
            "localEvidenceHashes": self.progress["localEvidenceHashes"],
            "inputHashes": self.i["inputHashes"],
            "originalTempPath": self.i["sourceTempPath"],
            "backup": self.progress["backup"],
            "stackOwnerToken": self.token,
            "keeperSnapshotOwnerToken": self.i["sourceOwnerToken"],
            "pairedSnapshots": self.snapshot_evidence,
            "pinnedSnapshotIdentity": copy.deepcopy(self.snapshot_identity_pins),
            "seedValidation": self.seed_trace,
            "seedEventsPath": str((self.output / "seed-events.json").relative_to(ROOT)),
            "seedEventsSha256": sha256(self.output / "seed-events.json"),
            "seedJobReceiptPath": str((self.output / "seed-job.json").relative_to(ROOT)),
            "seedJobReceiptSha256": sha256(self.output / "seed-job.json"),
            "seedSystemImage": SYSIMAGE,
            "finalControlPath": str(self.control_out.relative_to(ROOT)),
            "finalControlFileSha256": {p.name: sha256(p) for p in sorted(self.control_out.iterdir()) if p.is_file()},
            "snapshotJournalPath": str(journal.relative_to(ROOT)),
            "snapshotJournalSha256": sha256(journal),
            "replicaCatchup": self.progress["replicaCatchup"],
            "limits": ["No fixture SQL/DML or Backup request was rerun.",
                       "The original raw INSERT/DELETE output was not preserved and is not reconstructed."]})
        self.save_json(self.output / "resume-progress.json", self.progress)

    def run(self, continue_tail: bool) -> None:
        self.owner_preflight()
        self.source_lineage_preflight()
        self.cluster_preflight()
        self.fixture_and_archiver_preflight()
        self.verify_backup_yaml_and_live()
        self.source_pv_disk()
        if continue_tail:
            self.continuation_mode = True
            self.verify_snapshot_group_lock()
            self.wait_backup_and_copy()
            self.continue_original_tail()
        else:
            self.progress["phase"] = "read-only-preflight-complete"
            self.progress["backupPhase"] = (self.backup or {}).get("status", {}).get("phase")
            self.progress["nextStep"] = "After root review, explicit --continue-original-tail waits on the exact existing Backup, protects snapshots as soon as observed, then resumes the tail; default mode does not wait or mutate Azure/Kubernetes."
            self.save_json(self.output / "resume-progress.json", self.progress)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-process-receipt", type=Path, required=True,
                        help="externally preserved terminal failed/stopped original prepare-process.json")
    parser.add_argument("--running-process-receipt", type=Path, required=True,
                        help="byte-preserved process receipt captured while the original 02 process was running")
    parser.add_argument("--original-script", type=Path, required=True,
                        help="externally preserved frozen 100 GiB 02-prepare.sh")
    parser.add_argument("--prebackup-tmp", type=Path, required=True,
                        help="preserved original TMP copied under recovery/backup-wait-resume")
    parser.add_argument("--prebackup-capture-receipt", type=Path, required=True,
                        help="private receipt binding copied TMP files to the original running PID/script/receipt")
    parser.add_argument("--terminal-process-binding", type=Path, required=True,
                        help="separate receipt binding the later terminal receipt to the captured running identity")
    parser.add_argument("--backup-uid", required=True,
                        help="exact UID captured from the existing owned Backup; never creates a Backup")
    parser.add_argument("--output-dir", type=Path, required=True,
                        help="new private results directory under recovery/backup-wait-resume")
    parser.add_argument("--continue-original-tail", action="store_true",
                        help="after the exact Backup and paired Copy100 gate, perform the remaining original tail")
    args = parser.parse_args(argv)
    try:
        require(re.fullmatch(r"[a-f0-9-]{36}", args.backup_uid) is not None,
                "--backup-uid must be a UUID")
        inputs = load_inputs(args)
        runner = Resume(inputs, args.output_dir, args.backup_uid)
        runner.run(args.continue_original_tail)
        phase = runner.progress.get("phase")
        print(f"100 GiB existing-Backup recovery reached {phase}; private evidence is under {runner.output.relative_to(ROOT)}")
        return 0
    except ResumeError as exc:
        print(f"RECOVERY BLOCKED: {exc}", file=sys.stderr)
        return 2
    except (KeyError, TypeError, ValueError, decimal.InvalidOperation) as exc:
        print(f"RECOVERY BLOCKED: malformed evidence ({exc.__class__.__name__})", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
