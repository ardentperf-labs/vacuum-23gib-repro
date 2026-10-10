#!/usr/bin/env python3
"""Safely orchestrate post-measurement cleanup for this experiment stack.

The default invocation is local-only and read-only. ``--apply`` is required to
run Azure-backed audits and the existing teardown/cleanup helpers. An owned
regional Network Watcher is an optional final cleanup phase. A root review gate
can authorize its deletion only after this stack's teardown receipt proves its
AKS and node resource groups are gone. A missing or unsafe gate preserves the
watcher while allowing the rest of cleanup to finish.
"""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import uuid
from typing import Any


ROOT = Path(__file__).resolve().parent
RUN = ROOT / "run"
STATE_DIR = RUN / ".state"
RECEIPT = STATE_DIR / "cleanup-run.json"
ATTEMPT_DIR = STATE_DIR / "cleanup-run"
TIMELINE = ROOT / "timeline-events.jsonl"
WATCHER_GATE_DEFAULT = STATE_DIR / "regional-watcher-safety-gate.json"
READER_HELPER = ROOT / "recovery" / "cleanup-source-readers.py"
WATCHER_HELPER = ROOT / "recovery" / "cleanup-owned-watcher.py"
TEARDOWN = RUN / "04-teardown.sh"
RETENTION_AUDIT = ROOT / "retention-audit.py"
SOURCE_VALIDATOR = RUN / "recovery" / "prepare-source.py"
TEARDOWN_AUTH_HELPER = RUN / "recovery" / "verify-teardown-authorization.py"
TEARDOWN_AUTH = STATE_DIR / "teardown-authorization.json"
TEARDOWN_AUTH_CONSUMED = STATE_DIR / "teardown-authorization-consumed.json"


class Refusal(RuntimeError):
    """A local safety gate failed; no later cleanup step may proceed."""


def now_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_utc(value: Any, label: str) -> dt.datetime:
    if not isinstance(value, str) or not value:
        raise Refusal(f"{label} must be an aware UTC timestamp.")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise Refusal(f"{label} is not a valid ISO timestamp.") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise Refusal(f"{label} must include a timezone offset.")
    return parsed.astimezone(dt.timezone.utc)


def read_json(path: Path, label: str | None = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise Refusal(f"Required {label or path.name} is missing.") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise Refusal(f"Required {label or path.name} is unreadable or invalid JSON.") from exc


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def proof_file(path: Path, kind: str, stack: Path) -> dict[str, Any]:
    resolved_root = stack.resolve()
    resolved_path = path.resolve()
    if not resolved_path.is_relative_to(resolved_root) or not resolved_path.is_file():
        raise Refusal("A teardown proof input is missing or escapes the experiment directory.")
    return {"path": str(resolved_path.relative_to(resolved_root)), "kind": kind,
            "bytes": resolved_path.stat().st_size, "sha256": sha256(resolved_path)}


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{uuid.uuid4().hex}.tmp")
    with temporary.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def append_timeline(event: str, receipt_path: Path, details: dict[str, Any]) -> None:
    TIMELINE.parent.mkdir(parents=True, exist_ok=True)
    lock_path = STATE_DIR / "timeline-events.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    record = {"occurredAtUtc": now_utc(), "event": event,
              "precision": "exact-local-cleanup-receipt",
              "source": str(receipt_path.relative_to(ROOT)), "details": details}
    with lock_path.open("a", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        with TIMELINE.open("a", encoding="utf-8") as timeline:
            timeline.write(json.dumps(record, sort_keys=True) + "\n")
            timeline.flush()
            os.fsync(timeline.fileno())
    os.chmod(TIMELINE, 0o600)


def import_helper(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise Refusal(f"Cannot load required local helper {path.name}.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def phase_receipt_path(stack: Path, phase: str) -> Path:
    candidates = [stack / ".state" / f"{phase}-process.json",
                  stack / "run" / ".state" / f"{phase}-process.json"]
    existing = [path for path in candidates if path.is_file()]
    if not existing:
        raise Refusal(f"No durable {phase} process receipt exists.")
    if len(existing) > 1:
        receipts = [read_json(path, f"{phase} process receipt") for path in existing]
        signatures = {(row.get("phase"), row.get("status"), row.get("exitCode")) for row in receipts}
        if len(signatures) != 1:
            raise Refusal(f"Conflicting {phase} process receipts exist; refusing to select one.")
        if not all(row.get("phase") == phase for row in receipts):
            raise Refusal(f"A {phase} process receipt has the wrong phase identity.")
        # The runner-level receipt at stack/.state is the outermost supervisor
        # record when both it and a script-local receipt are present.
        return existing[0]
    return existing[0]


def latest_attempt_receipt_path(stack: Path, phase: str) -> tuple[Path, dict[str, Any], int]:
    """Select the newest durable attempt receipt without falling back on failure."""
    candidates: list[tuple[Path, dict[str, Any], int]] = []
    for base in (stack / ".state", stack / "run" / ".state"):
        paths = [base / f"{phase}-process.json", *sorted(base.glob(f"{phase}-process-attempt-*.json"))]
        for path in paths:
            if not path.is_file():
                continue
            receipt = read_json(path, f"{phase} process receipt")
            if receipt.get("phase") != phase:
                raise Refusal(f"A {phase} process receipt has the wrong phase identity.")
            attempt = receipt.get("attempt")
            if attempt is None and path.name == f"{phase}-process.json":
                attempt = 1
            if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 1:
                raise Refusal(f"A {phase} process receipt has an invalid attempt number.")
            candidates.append((path, receipt, attempt))
    if not candidates:
        raise Refusal(f"No durable {phase} process receipt exists.")
    newest = max(item[2] for item in candidates)
    matches = [item for item in candidates if item[2] == newest]
    if len(matches) != 1:
        hashes = {sha256(item[0]) for item in matches}
        if len(hashes) != 1:
            raise Refusal(f"Conflicting latest {phase} process receipts exist; refusing to select one.")
    return matches[0][0], matches[0][1], newest


def verify_receipt_input_hashes(stack: Path, receipt: dict[str, Any], required: set[str],
                                label: str, kind: str) -> list[dict[str, Any]]:
    """Verify every recorded frozen input and return teardown-bindable file proofs."""
    frozen = receipt.get("scriptSha256")
    if not isinstance(frozen, dict) or not required.issubset(frozen):
        raise Refusal(f"{label} receipt lacks its complete frozen helper/config/manifest hash set.")
    run_dir = (stack / "run").resolve()
    proofs = []
    for relative, expected in sorted(frozen.items()):
        if (not isinstance(relative, str) or not relative or Path(relative).is_absolute() or
            not isinstance(expected, str) or len(expected) != 64 or
            any(char not in "0123456789abcdefABCDEF" for char in expected)):
            raise Refusal(f"{label} receipt contains a malformed frozen-input hash.")
        path = (run_dir / relative).resolve()
        if not path.is_relative_to(run_dir) or not path.is_file():
            raise Refusal(f"A frozen {label} input is missing or escapes the run directory.")
        if sha256(path) != expected.casefold():
            raise Refusal(f"A frozen {label} helper/config/manifest input changed after its process receipt.")
        proofs.append(proof_file(path, kind, stack))
    return proofs


PREPARE_FROZEN_INPUTS = {
    "02-prepare.sh", "recovery/prepare-source.py", "run-config.json",
    "source-manifest.json", "snapshot.json",
}
FINALIZE_FROZEN_INPUTS = {
    "recovery/finalize-prepared-seed-attempt-2.py",
    "recovery/seed-replica-and-finalize.py", "run-config.json",
    "source-manifest.json", "snapshot.json",
}


def validate_preparation_receipt(stack: Path, state: dict[str, Any],
                                 process: dict[str, Any]) -> dict[str, Any]:
    """Accept original preparation or a hash-bound successful finalize continuation."""
    if process.get("phase") != "prepare":
        raise Refusal("Preparation process receipt has the wrong phase identity.")
    if process.get("status") == "complete" and process.get("exitCode") == 0:
        return {"mode": "original-prepare", "verified": True, "proofFiles": []}
    original_exit = process.get("exitCode")
    if (process.get("status") != "failed" or not isinstance(original_exit, int) or
        isinstance(original_exit, bool) or original_exit == 0):
        raise Refusal("Original preparation must either complete successfully or have a verified finalizer continuation.")

    expected_prepare_command = ["bash", str((stack / "run" / "02-prepare.sh").resolve())]
    if process.get("command") != expected_prepare_command:
        raise Refusal("Failed original preparation receipt does not bind the frozen 02-prepare.sh invocation.")
    prepare_input_proofs = verify_receipt_input_hashes(
        stack, process, PREPARE_FROZEN_INPUTS, "original preparation", "prepareFrozenInput")

    finalize_path, finalize, attempt_number = latest_attempt_receipt_path(stack, "finalize")
    finalize_exit = finalize.get("exitCode")
    if (finalize.get("phase") != "finalize" or finalize.get("status") != "complete" or
        not isinstance(finalize_exit, int) or isinstance(finalize_exit, bool) or finalize_exit != 0 or
        finalize.get("attempt") != attempt_number):
        raise Refusal("Latest finalizer process receipt must be phase finalize, complete, and exit code 0.")
    finalizer_script = (stack / "run" / "recovery" / "finalize-prepared-seed-attempt-2.py").resolve()
    command = finalize.get("command")
    token = state.get("ownerToken")
    if (not isinstance(token, str) or len(token) != 32 or
        command != ["python3", str(finalizer_script), "--apply", "--owner-token", token]):
        raise Refusal("Latest finalizer command does not bind the exact owner-token apply invocation.")
    if finalize.get("stack") not in ("run", str((stack / "run").resolve())):
        raise Refusal("Latest finalizer receipt does not identify this experiment's run directory.")
    finalize_input_proofs = verify_receipt_input_hashes(
        stack, finalize, FINALIZE_FROZEN_INPUTS, "finalizer", "finalizerFrozenInput")
    finalize_process_proof = proof_file(finalize_path, "finalizeProcess", stack)
    return {"mode": "verified-finalizer-continuation", "verified": True,
            "originalPrepareExitCode": process["exitCode"],
            "finalizerAttempt": attempt_number,
            "finalizerReceiptPath": str(finalize_path.relative_to(stack)),
            "finalizerReceiptSha256": sha256(finalize_path),
            "finalizerFrozenInputCount": len(finalize_input_proofs),
            "proofFiles": [finalize_process_proof, *prepare_input_proofs, *finalize_input_proofs]}


def results_dir_from_receipt(stack: Path, process: dict[str, Any]) -> Path:
    command = process.get("command")
    if not isinstance(command, list) or not all(isinstance(item, str) for item in command):
        raise Refusal("Test process receipt has no usable command vector.")
    try:
        index = command.index("--out")
        output = Path(command[index + 1])
    except (ValueError, IndexError) as exc:
        raise Refusal("Test process receipt does not identify its results directory.") from exc
    cwd = process.get("workingDirectory") or process.get("cwd") or str(stack)
    working_directory = Path(cwd)
    if not working_directory.is_absolute():
        working_directory = (stack / working_directory).resolve()
    resolved = (working_directory / output).resolve() if not output.is_absolute() else output.resolve()
    if not resolved.is_relative_to(stack.resolve()):
        raise Refusal("Test results path escapes this owned experiment directory.")
    return resolved


def validate_test_outputs(stack: Path) -> dict[str, Any]:
    process_path = phase_receipt_path(stack, "test")
    process = read_json(process_path, "test process receipt")
    if (process.get("phase") != "test" or process.get("status") != "complete" or
        process.get("exitCode") != 0):
        raise Refusal("Test process must have a durable complete receipt with exit code 0.")
    results = results_dir_from_receipt(stack, process)
    summary = read_json(results / "summary.json", "test summary")
    timing = read_json(results / "timing.json", "test timing receipt")
    native = read_json(results / "client-summary.json", "native pgbench client summary")
    status = summary.get("status") or {}
    coverage = summary.get("coverage") or {}
    if status.get("testComplete") is not True or timing.get("testComplete") is not True:
        raise Refusal("Test summary and timing receipt must both mark the measurement complete.")
    if coverage.get("valid") is not True or status.get("vacuumWindowCoverageValid") is not True:
        raise Refusal("Required vacuum-window storage sampling coverage is not valid.")
    if (native.get("pgbenchExitCode") != 0 or native.get("failedTransactions") != 0 or
        native.get("validationErrors") not in ([], None)):
        raise Refusal("Native pgbench exit status, failed count, or validation errors do not pass.")
    completed = native.get("completedTransactions")
    logged = native.get("loggedCompletedTransactions")
    committed = native.get("committedTransactions")
    if (not isinstance(completed, int) or isinstance(completed, bool) or completed <= 0 or
        not isinstance(logged, int) or logged != completed):
        raise Refusal("Native completed transactions and logged completion rows do not reconcile.")
    if not isinstance(committed, int) or isinstance(committed, bool):
        raise Refusal("Server committed-update count is missing from the drained client receipt.")
    pending = max(int(native.get("pendingTransactionsAtStopRequest") or 0),
                  int(native.get("inFlightActiveSessionsAtStopRequest") or 0))
    difference = committed - completed
    if difference < 0 or difference > pending:
        raise Refusal("Server committed updates cannot be reconciled with completed transactions and stop-time in-flight work.")
    fixed = {results / "summary.json", results / "timing.json", results / "client-summary.json"}
    artifacts = []
    for candidate in sorted(results.rglob("*")):
        if candidate.is_symlink():
            raise Refusal("Test result tree contains a symlink; refusing to authorize teardown.")
        if candidate.is_file() and candidate not in fixed:
            artifacts.append(proof_file(candidate, "testArtifact", stack))
    return {"resultsDirectory": str(results), "testProcessExitCode": 0,
            "testComplete": True, "coverageValid": True,
            "completedTransactions": completed, "loggedCompletedTransactions": logged,
            "committedTransactions": committed, "stopDrainDifference": difference,
            "inFlightUpperBound": pending,
            "proofFiles": [proof_file(process_path, "testProcess", stack),
                           proof_file(results / "summary.json", "testSummary", stack),
                           proof_file(results / "timing.json", "testTiming", stack),
                           proof_file(results / "client-summary.json", "nativeClientSummary", stack),
                           *artifacts]}


def validate_file_manifest(stack: Path, manifest: dict[str, Any]) -> list[dict[str, Any]]:
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise Refusal("Pinned source-evidence manifest has no file inventory.")
    root = stack.resolve()
    seen: set[str] = set()
    verified: list[dict[str, Any]] = []
    for item in files:
        if not isinstance(item, dict):
            raise Refusal("Pinned source-evidence file entry is malformed.")
        relative = item.get("path")
        expected_bytes = item.get("bytes")
        expected_hash = item.get("sha256")
        if (not isinstance(relative, str) or not relative or not isinstance(expected_bytes, int) or
            isinstance(expected_bytes, bool) or expected_bytes < 0 or
            not isinstance(expected_hash, str) or len(expected_hash) != 64):
            raise Refusal("Pinned source-evidence file entry is missing a valid path, size, or SHA-256.")
        key = relative.casefold()
        if key in seen:
            raise Refusal("Pinned source-evidence manifest contains duplicate paths.")
        seen.add(key)
        path = (stack / relative).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise Refusal("A pinned source-evidence path is missing or escapes the experiment directory.")
        if path.stat().st_size != expected_bytes or sha256(path) != expected_hash.casefold():
            raise Refusal("A source archive input or evidence file differs from its pinned size or SHA-256.")
        verified.append({"path": relative, "bytes": expected_bytes, "sha256": expected_hash.casefold()})
    return verified


def validate_archive_manifest_binding(stack: Path, source_manifest: dict[str, Any],
                                      receipt: dict[str, Any]) -> list[dict[str, Any]]:
    archive = source_manifest.get("archive") or {}
    receipt_path = archive.get("sourceReceipt")
    if receipt_path != "recovery/source-evidence/archive-receipt.json":
        raise Refusal("Pinned source manifest does not identify the expected original archive receipt.")
    pinned_receipt = read_json(stack / receipt_path, "pinned original archive receipt")
    private_receipt = read_json(stack / "recovery" / "archive-input" / "receipt.json",
                                "private archive-input receipt")
    if pinned_receipt != private_receipt or receipt != private_receipt:
        raise Refusal("Private archive-input receipt differs from the pinned original source receipt.")
    manifest_objects = archive.get("objects")
    receipt_objects = receipt.get("exportedObjects")
    if not isinstance(manifest_objects, list) or not isinstance(receipt_objects, list) or not manifest_objects:
        raise Refusal("Pinned source manifest or archive receipt lacks an object inventory.")
    project = lambda rows: {(row.get("objectName"), row.get("bytes"), row.get("sha256"))
                            for row in rows if isinstance(row, dict)}
    if len(project(manifest_objects)) != len(manifest_objects) or project(manifest_objects) != project(receipt_objects):
        raise Refusal("Private archive objects do not exactly match the pinned manifest names, sizes, and SHA-256s.")
    if receipt.get("status") not in {"complete", "verified", "exported"}:
        raise Refusal("Original private archive receipt is not marked verified/complete.")
    if not receipt.get("verification", {}).get("allStartThroughEndSegmentsPresent") or not receipt.get("verification", {}).get("rangeContiguous"):
        raise Refusal("Original private WAL receipt does not prove a contiguous required source range.")
    expected_object_paths = {Path(row["objectName"]).name for row in receipt_objects}
    evidence_manifest = read_json(stack / "recovery" / "source-evidence-manifest.json",
                                  "pinned source-evidence manifest")
    verified_files = validate_file_manifest(stack, evidence_manifest)
    private_rows = {Path(item["path"]).name for item in verified_files
                    if item["path"].startswith("recovery/archive-input/")}
    if not expected_object_paths.issubset(private_rows):
        raise Refusal("Pinned source-evidence manifest does not cover every private WAL input.")
    if "backup_label.txt" not in private_rows or "receipt.json" not in private_rows:
        raise Refusal("Pinned source-evidence manifest does not cover the backup label and private receipt.")
    return verified_files


def validate_archive_inputs(stack: Path, state: dict[str, Any], prepare_process: dict[str, Any]) -> dict[str, Any]:
    run_dir = stack / "run"
    source_manifest_path = run_dir / "source-manifest.json"
    recorded_hash = (prepare_process.get("scriptSha256") or {}).get("source-manifest.json")
    if not isinstance(recorded_hash, str) or sha256(source_manifest_path) != recorded_hash:
        raise Refusal("Pinned source-manifest file differs from the durable preparation process hash.")
    source_manifest = read_json(source_manifest_path, "pinned source manifest")
    receipt = read_json(run_dir / "recovery" / "archive-input" / "receipt.json", "private archive receipt")
    verified_files = validate_archive_manifest_binding(run_dir, source_manifest, receipt)
    # Reuse the preparation validator for its deeper Backup/VSS/VSC/LSN and
    # backup-label binding checks. It writes output only to a temporary folder
    # and has no Azure calls or credential inputs.
    token = state.get("ownerToken")
    if not isinstance(token, str) or len(token) != 32:
        raise Refusal("Saved owner token is unavailable for local archive validation.")
    with tempfile.TemporaryDirectory(prefix="cleanup-source-verify-") as temporary:
        output = Path(temporary) / "source-snapshots.json"
        command = [sys.executable, str(SOURCE_VALIDATOR), "--token", token,
                   "--short", token[:8], "--output", str(output)]
        completed = subprocess.run(command, cwd=ROOT, stdin=subprocess.DEVNULL,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True, check=False)
        if completed.returncode != 0 or not output.is_file():
            raise Refusal("Existing local source validator rejected the original private archive inputs or pinned backup evidence.")
    pinned_records = [{"path": "run/" + item["path"],
                      "kind": ("archiveInput" if item["path"].startswith("recovery/archive-input/")
                               else "archiveSourceEvidence"),
                      "bytes": item["bytes"], "sha256": item["sha256"]}
                     for item in verified_files]
    pinned_records.extend([
        proof_file(source_manifest_path, "sourceManifest", stack),
        proof_file(run_dir / "recovery" / "source-evidence-manifest.json",
                   "sourceEvidenceManifest", stack),
    ])
    return {"verified": True, "verifiedFileCount": len(verified_files),
            "archiveObjectCount": len(receipt.get("exportedObjects", [])),
            "sourceManifestSha256": recorded_hash,
            "originalReceiptSha256": sha256(run_dir / "recovery" / "archive-input" / "receipt.json"),
            "newBlobDeletionSafeAfterOfflineSeed": True,
            "verifiedFiles": pinned_records}


def validate_new_blob_ownership(stack: Path, state: dict[str, Any]) -> dict[str, Any]:
    storage = read_json(stack / "run" / ".state" / "storage.json", "private new-Blob storage receipt")
    container = read_json(stack / "run" / ".state" / "storage-container.json", "private new-Blob container receipt")
    if not isinstance(storage, dict) or not isinstance(container, dict):
        raise Refusal("New Blob storage/container receipts are malformed.")
    if (storage.get("status") != "ready" or storage.get("subscriptionId") != state.get("subscriptionId") or
        storage.get("resourceGroup") != state.get("resourceGroup") or
        storage.get("ownerToken") != state.get("ownerToken") or not storage.get("accountName") or
        not storage.get("containerName") or storage.get("publicAccess") is not None):
        raise Refusal("New private Blob storage receipt does not bind to this exact owned test resource group.")
    metadata = container.get("metadata") or {}
    if (metadata.get("reprotoken") != state.get("ownerToken") or
        container.get("publicAccess") not in (None, "off")):
        raise Refusal("New Blob container is not bound to this stack owner token or is not private.")
    return {"accountName": storage["accountName"], "containerName": storage["containerName"],
            "resourceGroup": storage["resourceGroup"], "private": True,
            "willBeRemovedByOwnedTestResourceGroupTeardown": True,
            "proofFiles": [proof_file(stack / "run" / ".state" / "storage.json", "newBlobStorage", stack),
                           proof_file(stack / "run" / ".state" / "storage-container.json", "newBlobContainer", stack)]}


def validate_offline_seed(stack: Path, state: dict[str, Any], prepare: dict[str, Any],
                          preparation_verification: dict[str, Any] | None = None) -> dict[str, Any]:
    process = read_json(phase_receipt_path(stack, "prepare"), "preparation process receipt")
    current_verification = validate_preparation_receipt(stack, state, process)
    if preparation_verification is not None and preparation_verification != current_verification:
        raise Refusal("Supplied preparation verification differs from the current process and frozen-input receipts.")
    preparation_verification = current_verification
    if preparation_verification.get("verified") is not True:
        raise Refusal("Preparation or its verified finalizer continuation did not pass local process checks.")
    if (prepare.get("status") != "complete" or prepare.get("ownerToken") != state.get("ownerToken") or
        prepare.get("subscriptionId") != state.get("subscriptionId") or
        prepare.get("sourceSnapshotsUntouched") is not True or prepare.get("allSnapshotsRetained") is not True or
        prepare.get("archiveSecretAbsent") is not True or prepare.get("archiveWriterAbsent") is not True):
        raise Refusal("Preparation audit ownership or source/snapshot preservation evidence does not pass.")
    if preparation_verification.get("mode") == "verified-finalizer-continuation":
        finalization = prepare.get("finalization") or {}
        required_finalization = ("archiveReferencesRemoved", "archiveSecretRemovedAfterReferences",
                                 "bootstrapSourceRemoved", "recoveryTargetRemoved",
                                 "volumeSnapshotLineagePreserved")
        if any(finalization.get(key) is not True for key in required_finalization):
            raise Refusal("Completed finalizer continuation lacks its complete archive/lineage preparation audit.")
    backup_receipt = prepare.get("offlineSeedBackup") or {}
    backup_name, backup_uid = backup_receipt.get("name"), backup_receipt.get("uid")
    if not backup_name or not backup_uid or backup_receipt.get("online") is not False:
        raise Refusal("Preparation audit does not bind a completed offline seed Backup UID.")
    snapshot_records = (prepare.get("primaryAndReplicaSnapshots") or {}).get("records")
    if not isinstance(snapshot_records, list) or len(snapshot_records) != 2:
        raise Refusal("Preparation audit must retain the paired DATA/WAL seed snapshots.")
    if {record.get("backupUid") for record in snapshot_records} != {backup_uid} or any(
        not record.get("id") or record.get("retentionLock") != "CanNotDelete" for record in snapshot_records):
        raise Refusal("Prepared DATA/WAL snapshots are not both bound to the offline seed Backup and retained lock.")
    evidence_dir = stack / "run" / ".state" / "recovery" / "evidence"
    candidates = []
    for path in sorted(evidence_dir.glob("*.json")):
        try:
            item = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(item, dict):
            continue
        metadata = item.get("metadata") or {}
        status = item.get("status") or {}
        spec = item.get("spec") or {}
        elements = status.get("snapshotBackupStatus", {}).get("elements") or []
        types = sorted(element.get("type") for element in elements if isinstance(element, dict))
        if (metadata.get("name") == backup_name and metadata.get("uid") == backup_uid and
            spec.get("online") is False and status.get("phase") == "completed" and
            status.get("online") in (None, False) and
            status.get("method") == "volumeSnapshot" and types == ["PG_DATA", "PG_WAL"]):
            candidates.append({"file": str(path.relative_to(stack)), "sha256": sha256(path),
                              "semanticSha256": hashlib.sha256(json.dumps(
                                  item, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest(),
                              "backupName": backup_name, "backupUid": backup_uid,
                              "online": False, "phase": "completed", "method": "volumeSnapshot",
                              "pairedElementTypes": types})
    if not candidates:
        raise Refusal("No captured completed offline paired DATA/WAL seed Backup evidence matches the preparation receipt.")
    unique_receipts = {(item["backupUid"], item["semanticSha256"]) for item in candidates}
    if len(unique_receipts) != 1:
        raise Refusal("Captured offline seed Backup evidence is inconsistent across preparation observations.")
    chosen = candidates[-1]
    chosen.pop("semanticSha256", None)
    chosen["bytes"] = (stack / chosen["file"]).stat().st_size
    chosen["preparationVerification"] = {
        "mode": preparation_verification["mode"],
        "verified": True,
        "finalizerAttempt": preparation_verification.get("finalizerAttempt"),
        "finalizerReceiptPath": preparation_verification.get("finalizerReceiptPath"),
        "finalizerReceiptSha256": preparation_verification.get("finalizerReceiptSha256"),
    }
    return chosen


def load_reader_contract(stack: Path, state: dict[str, Any]):
    helper = import_helper(stack / "recovery" / "cleanup-source-readers.py", "cleanup_source_readers_contract")
    manifest = read_json(stack / "run" / "source-manifest.json", "pinned source manifest")
    try:
        contract = helper.validate_state(state, manifest)
    except Exception as exc:
        raise Refusal("Recorded source-reader grants failed the existing exact-scope helper contract.") from exc
    if contract is not None and len(contract["records"]) != 2:
        raise Refusal("Source-reader cleanup must be pinned to exactly the DATA and WAL source snapshots.")
    return helper, contract


def watcher_ownership(stack: Path, state: dict[str, Any]):
    watcher = state.get("networkWatcher")
    if not isinstance(watcher, dict) or watcher.get("owned") is not True:
        return None, None
    helper = import_helper(stack / "recovery" / "cleanup-owned-watcher.py", "cleanup_owned_watcher_contract")
    try:
        contract = helper.validate_state(state)
    except Exception as exc:
        raise Refusal("Owned Network Watcher state failed the existing ownership helper contract.") from exc
    # The ownership helper returns a flat validated identity. Normalize it for
    # the orchestrator's gate and timeline consumers, retaining its pinned ETag.
    return helper, {**contract, "networkWatcher": {
        "id": contract["id"], "location": contract["location"]}}


def validate_watcher_gate(path: Path, watcher_contract: dict[str, Any] | None,
                          stack: Path, state: dict[str, Any]) -> dict[str, Any] | None:
    if watcher_contract is None:
        return None
    gate = read_json(path, "root regional-watcher safety gate")
    if gate.get("version") != 1 or gate.get("authorized") is not True:
        raise Refusal("Root regional-watcher safety gate is not explicitly authorized.")
    if gate.get("authorizedBy") != "root":
        raise Refusal("Regional-watcher safety gate must be issued by the root cleanup reviewer.")
    approved = parse_utc(gate.get("authorizedAtUtc"), "watcher gate authorizedAtUtc")
    age = (dt.datetime.now(dt.timezone.utc) - approved).total_seconds()
    if age < -300 or age > 3600:
        raise Refusal("Root regional-watcher review is future-dated or older than one hour.")
    watcher = watcher_contract["networkWatcher"]
    watcher_id = gate.get("networkWatcherId")
    region = gate.get("region")
    resource_review = gate.get("watcherResourceReview") or {}
    resource_ids = resource_review.get("resourceGroupResourceIds")
    flow_log_ids = resource_review.get("childFlowLogIds")
    safe_to_delete = gate.get("sharedRegionalUseReviewedSafeToDelete")
    if (gate.get("stackName") != stack.name or gate.get("ownerToken") != state.get("ownerToken") or
        not isinstance(watcher_id, str) or watcher_id.casefold() != watcher["id"].casefold() or
        not isinstance(region, str) or region.casefold() != watcher["location"].casefold() or
        not isinstance(safe_to_delete, bool) or
        not isinstance(gate.get("sharedRegionalUseAssessment"), str) or
        len(gate["sharedRegionalUseAssessment"].strip()) < 20):
        raise Refusal("Root watcher gate must bind the exact owned watcher and record a reviewed shared-regional-use decision.")
    review_time = parse_utc(resource_review.get("observedAtUtc"), "watcher resource-review observedAtUtc")
    review_age = (dt.datetime.now(dt.timezone.utc) - review_time).total_seconds()
    if review_age < -300 or review_age > 3600:
        raise Refusal("Root watcher resource inventory review is future-dated or older than one hour.")
    if (not isinstance(resource_ids, list) or not all(isinstance(item, str) and item for item in resource_ids) or
        not isinstance(flow_log_ids, list) or not all(isinstance(item, str) and item for item in flow_log_ids) or
        watcher["id"].casefold() not in {item.casefold() for item in resource_ids}):
        raise Refusal("Root watcher resource review must list the exact owned resource group contents and child flow logs.")
    reviewed_etag = resource_review.get("networkWatcherEtag")
    if not isinstance(reviewed_etag, str) or not reviewed_etag or reviewed_etag != watcher_contract.get("savedEtag"):
        raise Refusal("Root watcher resource review ETag differs from the saved owned-watcher ETag.")
    if safe_to_delete and (flow_log_ids or
        sorted(item.casefold() for item in resource_ids) != [watcher["id"].casefold()]):
        raise Refusal("Root watcher gate cannot authorize deletion while any child or unexpected resource remains in its group.")
    return {"path": str(path.relative_to(stack)), "sha256": sha256(path),
            "authorizedBy": gate["authorizedBy"], "authorizedAtUtc": approved.isoformat().replace("+00:00", "Z"),
            "networkWatcherId": watcher["id"], "region": watcher["location"],
            "sharedRegionalUseReviewedSafeToDelete": safe_to_delete,
            "sharedRegionalUseAssessment": gate["sharedRegionalUseAssessment"],
            "resourceReviewObservedAtUtc": review_time.isoformat().replace("+00:00", "Z"),
            "networkWatcherEtag": reviewed_etag,
            "resourceGroupResourceIds": resource_ids, "childFlowLogIds": flow_log_ids}


def validate_baseline(stack: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    prior = read_json(stack / "preflight" / "all-snapshots-before.json", "pre-run snapshot identity inventory")
    prior_locks = read_json(stack / "preflight" / "locks.json", "pre-run lock inventory")
    if not isinstance(prior, list) or len(prior) != 42:
        raise Refusal("Pinned pre-run snapshot inventory must contain all 42 prior snapshot identities.")
    if not isinstance(prior_locks, list) or not prior_locks:
        raise Refusal("Pinned pre-run lock inventory is missing or empty.")
    if any(not isinstance(item, dict) for item in prior):
        raise Refusal("Pinned pre-run snapshot identity inventory contains malformed entries.")
    identities = [(str(item.get("id", "")).casefold(), item.get("uniqueId")) for item in prior]
    if any(not item_id or not uid for item_id, uid in identities) or len({item_id for item_id, _ in identities}) != 42:
        raise Refusal("Pinned pre-run snapshot identity inventory has missing or duplicate IDs/unique IDs.")
    locks = normalize_locks(prior_locks)
    if len(locks) != len(prior_locks):
        raise Refusal("Pinned pre-run lock inventory has duplicate or malformed lock identities.")
    return prior, locks


def normalize_locks(records: Any) -> dict[str, dict[str, str]]:
    if not isinstance(records, list):
        raise Refusal("Azure lock inventory is not a list.")
    normalized: dict[str, dict[str, str]] = {}
    for item in records:
        if not isinstance(item, dict):
            raise Refusal("Azure lock inventory contains a malformed record.")
        lock_id = item.get("id")
        level = item.get("level") or (item.get("properties") or {}).get("level")
        scope = item.get("scope")
        if not scope and isinstance(lock_id, str):
            lower = lock_id.casefold()
            marker = "/providers/microsoft.authorization/locks/"
            if marker in lower:
                index = lower.index(marker)
                scope = lock_id[:index]
        if not isinstance(lock_id, str) or not lock_id or not isinstance(level, str) or not level or not isinstance(scope, str) or not scope:
            raise Refusal("Azure lock inventory is missing exact ID, scope, or level fields.")
        key = lock_id.casefold()
        if key in normalized:
            raise Refusal("Azure returned duplicate lock resource IDs.")
        normalized[key] = {"id": lock_id, "scope": scope.casefold(), "level": level}
    return normalized


def prepared_snapshot_uids(stack: Path, prepare: dict[str, Any]) -> tuple[dict[str, str], list[dict[str, Any]]]:
    records = (prepare.get("primaryAndReplicaSnapshots") or {}).get("records")
    if not isinstance(records, list) or len(records) != 2:
        raise Refusal("Preparation audit must identify exactly two DATA/WAL snapshots for UID verification.")
    if any(not isinstance(row, dict) or not isinstance(row.get("id"), str) or not row["id"]
           for row in records):
        raise Refusal("Preparation audit has a malformed paired snapshot identity.")
    ids = [row["id"].casefold() for row in records]
    if len(set(ids)) != 2:
        raise Refusal("Preparation audit has missing or duplicate paired snapshot IDs.")
    evidence: dict[str, tuple[str, Path]] = {}
    evidence_proofs: list[dict[str, Any]] = []
    evidence_dir = stack / "run" / ".state" / "recovery" / "evidence"
    for path in sorted(evidence_dir.glob("*arm-snapshot-observed-azureSnapshot*.json")):
        try:
            item = read_json(path, "prepared snapshot ARM evidence")
        except Refusal:
            continue
        item_id = item.get("id")
        unique_id = item.get("uniqueId")
        if not isinstance(item_id, str) or item_id.casefold() not in set(ids):
            continue
        if not isinstance(unique_id, str) or not unique_id:
            raise Refusal("Captured prepared snapshot ARM evidence lacks its uniqueId.")
        key = item_id.casefold()
        prior = evidence.get(key)
        if prior is not None and prior[0] != unique_id:
            raise Refusal("Captured prepared snapshot ARM evidence disagrees about a snapshot uniqueId.")
        evidence[key] = (unique_id, path)

    expected: dict[str, str] = {}
    seen_uids: set[str] = set()
    for row in records:
        snapshot_id = row["id"].casefold()
        recorded_uid = row.get("uniqueId")
        captured = evidence.get(snapshot_id)
        if recorded_uid is not None and (not isinstance(recorded_uid, str) or not recorded_uid):
            raise Refusal("Preparation record contains a malformed snapshot uniqueId.")
        if captured is None and recorded_uid is None:
            raise Refusal("Prepared snapshot uniqueId is absent from both preparation record and captured ARM evidence.")
        unique_id = recorded_uid or captured[0]
        if captured is not None and unique_id != captured[0]:
            raise Refusal("Preparation record uniqueId differs from captured ARM snapshot evidence.")
        if unique_id in seen_uids:
            raise Refusal("Paired prepared snapshots have duplicate uniqueIds.")
        seen_uids.add(unique_id)
        expected[snapshot_id] = unique_id
        if captured is not None:
            evidence_proofs.append(proof_file(captured[1], "preparedSeedSnapshotEvidence", stack))
    return expected, evidence_proofs


def assert_locks_preserved(before: dict[str, dict[str, str]], after: dict[str, dict[str, str]],
                           baseline: dict[str, dict[str, str]]) -> None:
    missing_baseline = [key for key in baseline if key not in after or
                        after[key]["scope"] != baseline[key]["scope"] or after[key]["level"] != baseline[key]["level"]]
    missing_before = [key for key in before if key not in after or
                      after[key]["scope"] != before[key]["scope"] or after[key]["level"] != before[key]["level"]]
    if missing_baseline:
        raise Refusal("One or more pre-run protection locks changed or disappeared.")
    if missing_before:
        raise Refusal("One or more before-cleanup protection locks changed or disappeared during cleanup.")


def validate_snapshot_audit(audit: dict[str, Any], prior: list[dict[str, Any]],
                            prepare: dict[str, Any], token: str,
                            stack: Path = ROOT) -> dict[str, Any]:
    if audit.get("preservedPriorSnapshots") is not True or audit.get("allSnapshotsProtected") is not True:
        raise Refusal("Snapshot audit did not prove prior identity preservation and protection of every current snapshot.")
    inventory = audit.get("snapshots")
    if not isinstance(inventory, list):
        raise Refusal("Snapshot audit lacks its complete live inventory.")
    by_id = {str(item.get("id", "")).casefold(): item for item in inventory if isinstance(item, dict)}
    if len(by_id) != len(inventory):
        raise Refusal("Snapshot audit contains duplicate or malformed resource IDs.")
    missing, changed = [], []
    for old in prior:
        live = by_id.get(old["id"].casefold())
        if live is None:
            missing.append(old["id"])
        elif live.get("uniqueId") != old["uniqueId"]:
            changed.append(old["id"])
    if missing or changed:
        raise Refusal("Snapshot audit lost or changed one of the 42 prior snapshot identities.")
    records = (prepare.get("primaryAndReplicaSnapshots") or {}).get("records") or []
    expected_uids, _ = prepared_snapshot_uids(stack, prepare)
    new_records = []
    for expected in records:
        expected_id = str(expected.get("id", "")).casefold()
        live = by_id.get(expected_id)
        if live is None or not live.get("uniqueId"):
            raise Refusal("Before-cleanup snapshot audit is missing a prepared DATA/WAL seed snapshot identity.")
        if live.get("uniqueId") != expected_uids.get(expected_id):
            raise Refusal("Prepared DATA/WAL snapshot uniqueId differs from its preparation evidence.")
        tags = live.get("tags") or {}
        if (tags.get("reprotoken") != token or
            tags.get("reproPurpose") != "premiumv2-vacuum-backup" or
            not (live.get("directCanNotDelete") or live.get("groupCanNotDelete"))):
            raise Refusal("Prepared DATA/WAL snapshot owner tag, purpose, or retention lock failed live audit.")
        new_records.append({"id": live["id"], "uniqueId": live["uniqueId"],
                            "expectedUniqueId": expected_uids[expected_id],
                            "directCanNotDelete": live["directCanNotDelete"],
                            "groupCanNotDelete": live["groupCanNotDelete"],
                            "provisioningState": live["state"], "accessState": live["accessState"],
                            "sizeGiB": live["sizeGiB"], "purpose": tags.get("reproPurpose")})
    return {"checkedAtUtc": audit.get("checkedAtUtc"), "priorSnapshotCount": len(prior),
            "currentSnapshotCount": len(inventory), "priorIdentitiesPreserved": True,
            "allCurrentSnapshotsProtected": True, "preparedSnapshots": new_records}


def compare_snapshot_audits(before: dict[str, Any], after: dict[str, Any]) -> None:
    before_rows, after_rows = before.get("snapshots"), after.get("snapshots")
    if not isinstance(before_rows, list) or not isinstance(after_rows, list):
        raise Refusal("Before/final snapshot audits lack full inventories for comparison.")
    after_map = {str(row.get("id", "")).casefold(): row for row in after_rows if isinstance(row, dict)}
    for row in before_rows:
        current = after_map.get(str(row.get("id", "")).casefold())
        if current is None or current.get("uniqueId") != row.get("uniqueId"):
            raise Refusal("A snapshot present before cleanup is absent or has a changed unique ID in the final audit.")


def validate_gate_inputs(stack: Path, watcher_gate_path: Path | None = None) -> dict[str, Any]:
    state_path = stack / "run" / ".state" / "state.json"
    state = read_json(state_path, "owned stack state")
    plan_path = stack / "experiment-plan.json"
    plan = read_json(plan_path, "experiment plan")
    plan_subscription = plan.get("subscriptionId") or "00000000-0000-0000-0000-000000000000"
    if str(plan_subscription).casefold() != str(state.get("subscriptionId", "")).casefold():
        raise Refusal("Experiment plan subscription differs from the saved stack subscription.")
    prepare_path = stack / "run" / ".state" / "recovery" / "prepare-audit.json"
    prepare = read_json(prepare_path, "preparation audit")
    prepare_process_path = phase_receipt_path(stack, "prepare")
    prepare_process = read_json(prepare_process_path, "preparation process receipt")
    preparation_verification = validate_preparation_receipt(stack, state, prepare_process)
    test = validate_test_outputs(stack)
    archive = validate_archive_inputs(stack, state, prepare_process)
    seed = validate_offline_seed(stack, state, prepare, preparation_verification)
    _, prepared_snapshot_proofs = prepared_snapshot_uids(stack, prepare)
    prior, baseline_locks = validate_baseline(stack)
    reader_helper, reader_contract = load_reader_contract(stack, state)
    watcher_helper, watcher_contract = watcher_ownership(stack, state)
    gate = None
    gate_error = None
    gate_path = (watcher_gate_path or WATCHER_GATE_DEFAULT).resolve()
    if not gate_path.is_relative_to(stack.resolve()):
        raise Refusal("Root watcher safety gate must be stored inside this experiment directory.")
    if watcher_contract is not None and gate_path.is_file():
        try:
            gate = validate_watcher_gate(gate_path, watcher_contract, stack, state)
        except Refusal as exc:
            # A bad or stale optional deletion gate never blocks compute/blob
            # cleanup; it only means the shared watcher remains untouched.
            gate_error = str(exc)
    teardown = existing_teardown_receipt(stack, state)
    return {"state": state, "prepare": prepare, "prepareProcess": prepare_process,
            "stateProof": proof_file(state_path, "state", stack),
            "planProof": proof_file(plan_path, "experimentPlan", stack),
            "prepareAuditProof": proof_file(prepare_path, "prepareAudit", stack),
            "prepareProcessProof": proof_file(prepare_process_path, "prepareProcess", stack),
            "preparationVerification": preparation_verification,
            "preparedSnapshotEvidenceProofs": prepared_snapshot_proofs,
            "offlineSeedBackupProof": proof_file(stack / seed["file"], "offlineSeedBackup", stack),
            "baselineProofFiles": [proof_file(stack / "preflight" / "all-snapshots-before.json",
                                               "priorSnapshotInventory", stack),
                                   proof_file(stack / "preflight" / "locks.json",
                                              "priorLockInventory", stack)],
            "test": test, "archive": archive, "offlineSeedBackup": seed,
            "newBlob": validate_new_blob_ownership(stack, state),
            "priorSnapshots": prior, "baselineLocks": baseline_locks,
            "readerHelper": reader_helper, "readerContract": reader_contract,
            "watcherHelper": watcher_helper, "watcherContract": watcher_contract,
            "watcherGate": gate, "watcherGateError": gate_error, "watcherGatePath": gate_path,
            "existingTeardownReceipt": teardown}


def run_logged(command: list[str], log_path: Path, cwd: Path) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("ab", buffering=0) as log:
        os.chmod(log_path, 0o600)
        result = subprocess.run(command, cwd=cwd, stdin=subprocess.DEVNULL,
                                stdout=log, stderr=subprocess.STDOUT, check=False)
    return result.returncode


def run_capture(command: list[str], log_path: Path, cwd: Path) -> tuple[int, str]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(command, cwd=cwd, stdin=subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               text=True, check=False)
    with log_path.open("a", encoding="utf-8") as log:
        os.chmod(log_path, 0o600)
        log.write(completed.stdout)
        log.flush()
        os.fsync(log.fileno())
    return completed.returncode, completed.stdout


def validate_watcher_child_list(payload: Any, child: str) -> list[str]:
    if not isinstance(payload, dict) or not isinstance(payload.get("value"), list):
        raise Refusal(f"Live Network Watcher {child} inventory is malformed.")
    if payload.get("nextLink") not in (None, ""):
        raise Refusal(f"Live Network Watcher {child} inventory is paginated and incomplete.")
    ids = []
    for item in payload["value"]:
        if not isinstance(item, dict) or not isinstance(item.get("id"), str) or not item["id"]:
            raise Refusal(f"Live Network Watcher {child} inventory contains a malformed child.")
        ids.append(item["id"])
    return ids


def live_watcher_child_inventory(subscription: str, watcher_id: str,
                                 output_path: Path) -> dict[str, Any]:
    """Freshly read watcher child collections without retaining raw CLI errors."""
    children = ("connectionMonitors", "flowLogs", "packetCaptures")
    result: dict[str, Any] = {"observedAtUtc": now_utc(), "watcherId": watcher_id,
                              "status": "clear", "children": {}}
    for child in children:
        url = f"https://management.azure.com{watcher_id}/{child}?api-version=2025-09-01"
        command = ["az", "rest", "--method", "get", "--url", url,
                   "--subscription", subscription, "--only-show-errors", "-o", "json"]
        try:
            completed = subprocess.run(command, cwd=ROOT, stdin=subprocess.DEVNULL,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       text=True, timeout=180, check=False)
        except (OSError, subprocess.TimeoutExpired):
            result["status"] = "unavailable"
            result["failedChild"] = child
            result["failure"] = "Azure CLI read failed or timed out."
            break
        if completed.returncode != 0:
            result["status"] = "unavailable"
            result["failedChild"] = child
            result["failure"] = f"Azure CLI returned exit code {completed.returncode}."
            break
        try:
            payload = json.loads(completed.stdout)
            ids = validate_watcher_child_list(payload, child)
        except (json.JSONDecodeError, Refusal) as exc:
            result["status"] = "unavailable"
            result["failedChild"] = child
            result["failure"] = str(exc) if isinstance(exc, Refusal) else "Azure CLI returned invalid child-inventory JSON."
            break
        result["children"][child] = ids
        if ids:
            result["status"] = "has-child-resources"
    result["completedAtUtc"] = now_utc()
    atomic_json(output_path, result)
    return result


def capture_global_locks(subscription: str) -> dict[str, dict[str, str]]:
    completed = subprocess.run(["az", "lock", "list", "--subscription", subscription,
                                "--only-show-errors", "-o", "json"],
                               cwd=ROOT, stdin=subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, check=False)
    if completed.returncode != 0:
        raise Refusal("Cannot read the global Azure lock inventory; preserving resources.")
    try:
        return normalize_locks(json.loads(completed.stdout))
    except json.JSONDecodeError as exc:
        raise Refusal("Azure returned invalid JSON for the global lock inventory.") from exc


def write_lock_receipt(path: Path, locks: dict[str, dict[str, str]]) -> None:
    rows = sorted(locks.values(), key=lambda item: item["id"].casefold())
    atomic_json(path, {"capturedAtUtc": now_utc(), "lockCount": len(rows), "locks": rows})


def run_retention_audit(label: str, log_path: Path) -> dict[str, Any]:
    command = [sys.executable, str(RETENTION_AUDIT), "--label", label]
    status = run_logged(command, log_path, ROOT)
    if status != 0:
        raise Refusal(f"Read-only snapshot retention audit failed ({label}); teardown is blocked.")
    path = ROOT / "preflight" / f"snapshot-audit-{label}.json"
    os.chmod(path, 0o600)
    audit = read_json(path, f"{label} snapshot retention audit")
    parse_utc(audit.get("checkedAtUtc"), f"snapshot-audit-{label}.checkedAtUtc")
    return audit


def verify_teardown_receipt(stack: Path, state: dict[str, Any]) -> dict[str, Any]:
    receipt = read_json(stack / "run" / ".state" / "teardown.json", "durable teardown receipt")
    if (receipt.get("status") != "complete" or receipt.get("ownerToken") != state.get("ownerToken") or
        receipt.get("subscriptionId") != state.get("subscriptionId") or
        receipt.get("deletedTestResourceGroup") != state.get("resourceGroup") or
        receipt.get("deletedNodeResourceGroup") != state.get("requestedNodeResourceGroup") or
        receipt.get("retainedSnapshotResourceGroupId", "").casefold() != state.get("snapshotResourceGroupId", "").casefold()):
        raise Refusal("04-teardown.sh did not produce a matching complete ownership receipt.")
    parse_utc(receipt.get("completedAtUtc"), "teardown.completedAtUtc")
    return receipt


def existing_teardown_receipt(stack: Path, state: dict[str, Any]) -> dict[str, Any] | None:
    """Return a valid completed 04 receipt for a safe phased resume, if present."""
    path = stack / "run" / ".state" / "teardown.json"
    if not path.exists():
        return None
    return verify_teardown_receipt(stack, state)


def watcher_cleanup_decision(watcher_contract: dict[str, Any] | None,
                             watcher_gate: dict[str, Any] | None,
                             watcher_gate_error: str | None = None) -> str:
    if watcher_contract is None:
        return "not-owned"
    if watcher_gate is None:
        return "preserve-invalid-gate" if watcher_gate_error else "preserve-gate-absent"
    if watcher_gate.get("sharedRegionalUseReviewedSafeToDelete") is not True:
        return "preserve-shared-use-review"
    return "delete-after-owned-stack-teardown"


def can_resume_watcher_only(previous: dict[str, Any], watcher_contract: dict[str, Any] | None,
                            watcher_gate: dict[str, Any] | None) -> bool:
    attempts = previous.get("attempts")
    if not isinstance(attempts, list) or not attempts or not isinstance(attempts[-1], dict):
        return False
    prior_watcher = attempts[-1].get("ownedWatcherCleanup") or {}
    return (watcher_contract is not None and
            watcher_cleanup_decision(watcher_contract, watcher_gate) == "delete-after-owned-stack-teardown" and
            prior_watcher.get("status") in {"preserve-gate-absent", "preserve-invalid-gate",
                                             "preserve-shared-use-review",
                                             "preserve-live-child-resources",
                                             "preserve-live-review-failed",
                                             "watcher-review-required-after-helper-refusal"})


def verify_teardown_preserved_new(stack: Path, teardown: dict[str, Any],
                                 before_audit: dict[str, Any], prepare: dict[str, Any]) -> None:
    expected = {str(row.get("id", "")).casefold() for row in
                (prepare.get("primaryAndReplicaSnapshots") or {}).get("records", [])}
    expected_uids, _ = prepared_snapshot_uids(stack, prepare)
    retained = teardown.get("retainedSnapshots")
    if not isinstance(retained, list):
        raise Refusal("04-teardown receipt lacks its retained snapshot records.")
    retained_by_id = {str(row.get("id", "")).casefold(): row for row in retained if isinstance(row, dict)}
    if not expected.issubset(retained_by_id):
        raise Refusal("04-teardown receipt does not retain every prepared DATA/WAL seed snapshot.")
    live = {str(row.get("id", "")).casefold(): row for row in before_audit.get("snapshots", []) if isinstance(row, dict)}
    for identity in expected:
        if identity not in live:
            raise Refusal("Before-cleanup snapshot audit lacks a prepared seed snapshot.")
        record = retained_by_id[identity]
        if record.get("retained") is not True or record.get("resourceLock") != "CanNotDelete":
            raise Refusal("04-teardown receipt does not show the prepared seed snapshot remains protected.")
        expected_uid = expected_uids.get(identity)
        if (not expected_uid or live[identity].get("uniqueId") != expected_uid or
            record.get("uniqueId") != expected_uid):
            raise Refusal("04-teardown snapshot uniqueId differs from the pinned prepared DATA/WAL snapshot.")


def validate_reader_cleanup(stdout: str, contract: dict[str, Any] | None) -> dict[str, Any]:
    try:
        result = json.loads(stdout.strip().splitlines()[-1])
    except (json.JSONDecodeError, IndexError) as exc:
        raise Refusal("Exact source-reader cleanup returned no parseable receipt.") from exc
    if contract is None:
        if result.get("status") != "not-configured" or result.get("mutated") is not False:
            raise Refusal("Source-reader cleanup attempted an unrecorded or unexpected scope.")
        return {"status": "not-configured", "residualScopeLocked": []}
    if result.get("status") != "complete" or result.get("mutated") is not True:
        raise Refusal("Exact source-reader cleanup did not complete its recorded assignment pass.")
    assignments = result.get("assignments")
    expected = {record["id"].casefold() for record in contract["records"]}
    if not isinstance(assignments, list):
        raise Refusal("Source-reader cleanup returned a malformed assignment list.")
    returned = {row.get("id", "").casefold() for row in assignments if isinstance(row, dict)}
    if len(returned) != len(assignments) or not returned.issubset(expected):
        raise Refusal("Source-reader cleanup receipt does not match the two exact recorded source assignment IDs.")
    allowed = {"deleted", "already-absent", "retained-scope-locked", "verified-dry-run"}
    if any(row.get("status") not in allowed for row in assignments):
        raise Refusal("Source-reader cleanup reported an unknown assignment outcome.")
    # The helper only emits IDs found live at that attempt. A recorded ID omitted
    # from a successful later retry is verified absent by its exact-scope listing.
    for assignment_id in sorted(expected-returned):
        assignments.append({"id": next(record["id"] for record in contract["records"]
                                       if record["id"].casefold() == assignment_id),
                            "status": "already-absent"})
    residual = [{"id": row["id"], "status": row["status"]} for row in assignments
                if row["status"] == "retained-scope-locked"]
    return {"status": "complete", "assignmentCount": len(assignments),
            "assignments": assignments, "residualScopeLocked": residual}


def attempt_record(receipt: dict[str, Any]) -> dict[str, Any]:
    attempts = receipt.get("attempts")
    if not isinstance(attempts, list):
        attempts = []
        receipt["attempts"] = attempts
    attempt = {"attempt": len(attempts) + 1, "id": uuid.uuid4().hex,
               "startedAtUtc": now_utc(), "status": "running", "stage": "local-gates-passed"}
    attempts.append(attempt)
    receipt.update({"version": 1, "status": "running", "updatedAtUtc": now_utc()})
    return attempt


def save_receipt(receipt: dict[str, Any]) -> None:
    receipt["updatedAtUtc"] = now_utc()
    atomic_json(RECEIPT, receipt)


def teardown_authorization_proofs(gates: dict[str, Any], before_snapshot_path: Path,
                                 before_lock_path: Path, stack: Path = ROOT) -> list[dict[str, Any]]:
    records = [gates["stateProof"], gates["planProof"], gates["prepareAuditProof"],
               gates["prepareProcessProof"], *gates["test"]["proofFiles"],
               *gates["archive"]["verifiedFiles"], gates["offlineSeedBackupProof"],
               *gates["newBlob"]["proofFiles"], *gates["baselineProofFiles"],
               *gates["preparedSnapshotEvidenceProofs"],
               proof_file(before_snapshot_path, "beforeCleanupSnapshotAudit", stack),
               proof_file(before_lock_path, "beforeCleanupLocks", stack),
               proof_file(stack / "run" / "04-teardown.sh", "04Script", stack),
               proof_file(stack / "run" / "recovery" / "verify-teardown-authorization.py",
                          "authorizationHelper", stack),
               proof_file(stack / "cleanup-run.py", "cleanupRunScript", stack)]
    paths = {row["path"].casefold(): row for row in records}
    if len(paths) != len(records):
        raise Refusal("Teardown authorization proof set contains duplicate file paths.")
    continuation = gates.get("preparationVerification") or {}
    extra = continuation.get("proofFiles", [])
    if not isinstance(extra, list):
        raise Refusal("Preparation-continuation teardown proof set is malformed.")
    for proof in extra:
        if not isinstance(proof, dict) or not isinstance(proof.get("path"), str):
            raise Refusal("Preparation-continuation teardown proof entry is malformed.")
        key = proof["path"].casefold()
        existing = paths.get(key)
        if existing is not None:
            if (existing.get("bytes") != proof.get("bytes") or
                existing.get("sha256") != proof.get("sha256")):
                raise Refusal("Overlapping preparation-continuation proof differs from the existing teardown proof.")
            continue
        paths[key] = proof
        records.append(proof)
    return records


def issue_teardown_authorization(gates: dict[str, Any], receipt: dict[str, Any],
                                 attempt: dict[str, Any], proof_files: list[dict[str, Any]]) -> dict[str, Any]:
    state = gates["state"]
    nonce = uuid.uuid4().hex
    issued = dt.datetime.now(dt.timezone.utc)
    script_record = next((row for row in proof_files if row.get("kind") == "04Script"), None)
    if script_record is None:
        raise Refusal("Teardown authorization proof set does not bind 04-teardown.sh.")
    if TEARDOWN_AUTH.is_symlink():
        raise Refusal("Existing one-shot authorization path is a symlink; refusing to overwrite it.")
    attempt["stage"] = "04-teardown-authorized"
    attempt["teardownAuthorization"] = {"status": "issued", "nonce": nonce,
        "issuedAtUtc": issued.isoformat().replace("+00:00", "Z"),
        "path": str(TEARDOWN_AUTH.relative_to(ROOT)), "proofFiles": proof_files,
        "proofFileCount": len(proof_files), "teardownScriptSha256": script_record["sha256"]}
    save_receipt(receipt)
    cleanup_bytes = RECEIPT.stat().st_size
    cleanup_sha = sha256(RECEIPT)
    authorization = {"version": 1, "status": "authorized", "nonce": nonce,
        "issuedAtUtc": issued.isoformat().replace("+00:00", "Z"),
        "expiresAtUtc": (issued + dt.timedelta(minutes=10)).isoformat().replace("+00:00", "Z"),
        "cleanupRunPid": os.getpid(), "ownerToken": state["ownerToken"],
        "subscriptionId": state["subscriptionId"], "resourceGroup": state["resourceGroup"],
        "requestedNodeResourceGroup": state["requestedNodeResourceGroup"],
        "attempt": {"id": attempt["id"], "number": attempt["attempt"]},
        "cleanupRunReceiptPath": str(RECEIPT.relative_to(ROOT)),
        "cleanupRunReceiptBytes": cleanup_bytes, "cleanupRunReceiptSha256": cleanup_sha,
        "teardownScriptBytes": script_record["bytes"],
        "teardownScriptSha256": script_record["sha256"], "proofFiles": proof_files}
    atomic_json(TEARDOWN_AUTH, authorization)
    return {"nonce": nonce, "path": str(TEARDOWN_AUTH.relative_to(ROOT)),
            "sha256": sha256(TEARDOWN_AUTH), "cleanupRunReceiptSha256": cleanup_sha,
            "proofFileCount": len(proof_files), "issuedAtUtc": authorization["issuedAtUtc"]}


def apply_cleanup(gates: dict[str, Any], stack: Path) -> dict[str, Any]:
    state = gates["state"]
    prepare = gates["prepare"]
    prior = gates["priorSnapshots"]
    baseline_locks = gates["baselineLocks"]
    watcher_gate = gates["watcherGate"]
    watcher_gate_error = gates.get("watcherGateError")
    watcher_decision = watcher_cleanup_decision(gates["watcherContract"], watcher_gate,
                                                watcher_gate_error)

    lock_path = STATE_DIR / "cleanup-run.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as run_lock:
        try:
            fcntl.flock(run_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise Refusal("Another cleanup-run orchestrator is active.") from exc

        previous = read_json(RECEIPT, "existing cleanup-run receipt") if RECEIPT.exists() else {}
        if not isinstance(previous, dict):
            raise Refusal("Existing cleanup-run receipt is malformed; refusing resume.")
        if previous.get("status") == "complete":
            if not can_resume_watcher_only(previous, gates["watcherContract"], watcher_gate):
                raise Refusal("Cleanup is already complete; a new attempt is allowed only to resume an owned watcher deletion after a new safe root review.")
        attempts_before = previous.get("attempts") if isinstance(previous.get("attempts"), list) else []
        prior_watcher_status = ((attempts_before[-1].get("ownedWatcherCleanup") or {}).get("status")
                                if attempts_before and isinstance(attempts_before[-1], dict) else None)
        if (gates["watcherContract"] is not None and
            watcher_decision != "delete-after-owned-stack-teardown" and
            prior_watcher_status == "watcher-review-required-after-helper-refusal"):
            watcher_decision = "watcher-review-required-after-helper-refusal"
        receipt = previous if isinstance(previous, dict) else {}
        if receipt and (receipt.get("ownerToken") != state.get("ownerToken") or
                        receipt.get("stack") != stack.name):
            raise Refusal("Existing cleanup-run receipt belongs to a different stack identity.")
        prior_teardown = existing_teardown_receipt(stack, state)
        attempt = attempt_record(receipt)
        receipt.update({"stack": stack.name, "ownerToken": state.get("ownerToken"),
                        "subscriptionId": state.get("subscriptionId"),
                  "preflight": {"test": gates["test"], "archive": gates["archive"],
                                      "newBlob": gates["newBlob"],
                                      "preparationVerification": {
                                          key: value for key, value in gates["preparationVerification"].items()
                                          if key != "proofFiles"},
                                      "offlineSeedBackup": gates["offlineSeedBackup"],
                                      "rootWatcherGate": watcher_gate,
                                      "rootWatcherGateError": watcher_gate_error,
                                      "watcherCleanupDecision": watcher_decision,
                                      "priorSnapshotCount": len(prior), "priorLockCount": len(baseline_locks)}})
        save_receipt(receipt)
        append_timeline("cleanup-local-gates-passed", RECEIPT,
                        {"attempt": attempt["attempt"], "newBlobDeletionSafe": True,
                         "offlineSeedBackupUid": gates["offlineSeedBackup"]["backupUid"]})
        attempt_dir = ATTEMPT_DIR / f"attempt-{attempt['attempt']:03d}"
        attempt_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(attempt_dir, 0o700)

        try:
            before_audit = run_retention_audit("before-cleanup", attempt_dir / "snapshot-audit-before.log")
            live_locks_before = capture_global_locks(state["subscriptionId"])
            assert_locks_preserved(baseline_locks, live_locks_before, baseline_locks)
            before_summary = validate_snapshot_audit(before_audit, prior, prepare, state["ownerToken"], stack)
            before_lock_path = attempt_dir / "locks-before-cleanup.json"
            write_lock_receipt(before_lock_path, live_locks_before)
            before_snapshot_path = attempt_dir / "snapshot-audit-before-cleanup.json"
            shutil.copy2(ROOT / "preflight" / "snapshot-audit-before-cleanup.json", before_snapshot_path)
            os.chmod(before_snapshot_path, 0o600)
            attempt.update({"stage": "before-cleanup-audit-passed", "beforeCleanupAudit": before_summary,
                            "beforeCleanupAuditPath": str(before_snapshot_path.relative_to(stack)),
                            "beforeCleanupAuditSha256": sha256(before_snapshot_path),
                            "beforeCleanupLocksPath": str(before_lock_path.relative_to(stack)),
                            "beforeCleanupLocksSha256": sha256(before_lock_path)})
            save_receipt(receipt)
            append_timeline("cleanup-before-audit-complete", RECEIPT,
                            {"attempt": attempt["attempt"], "audit": before_summary,
                             "auditSha256": attempt["beforeCleanupAuditSha256"]})

            teardown_receipt_path = STATE_DIR / "teardown.json"
            if prior_teardown is not None:
                teardown = prior_teardown
                invocation = {"status": "already-complete", "command": None,
                              "startedAtUtc": None, "finishedAtUtc": teardown["completedAtUtc"],
                              "receiptPath": str(teardown_receipt_path.relative_to(stack)),
                              "receiptSha256": sha256(teardown_receipt_path),
                              "receiptCompletedAtUtc": teardown["completedAtUtc"],
                              "resumedFromExistingReceipt": True}
                attempt["teardownInvocation"] = invocation
                attempt["stage"] = "04-teardown-resumed"
                save_receipt(receipt)
                append_timeline("04-teardown-resumed-from-receipt", RECEIPT,
                                {"attempt": attempt["attempt"],
                                 "teardownReceiptSha256": invocation["receiptSha256"],
                                 "teardownCompletedAtUtc": teardown["completedAtUtc"]})
            else:
                teardown_log = attempt_dir / "04-teardown.log"
                command = ["bash", str(TEARDOWN)]
                proof_files = teardown_authorization_proofs(gates, before_snapshot_path, before_lock_path)
                attempt["teardownInvocation"] = {"command": command, "startedAtUtc": now_utc(),
                                                  "logPath": str(teardown_log.relative_to(stack)),
                                                  "resumedFromExistingReceipt": False,
                                                  "authorizationPath": str(TEARDOWN_AUTH.relative_to(stack)),
                                                  "proofFileCount": len(proof_files)}
                authorization = issue_teardown_authorization(gates, receipt, attempt, proof_files)
                append_timeline("04-teardown-authorized", RECEIPT,
                                {"attempt": attempt["attempt"], "authorizationSha256": authorization["sha256"],
                                 "cleanupRunReceiptSha256": authorization["cleanupRunReceiptSha256"],
                                 "proofFileCount": authorization["proofFileCount"]})
                append_timeline("04-teardown-started", RECEIPT,
                                {"attempt": attempt["attempt"], "command": ["bash", "run/04-teardown.sh"]})
                status = run_logged(command, teardown_log, stack)
                invocation = attempt["teardownInvocation"]
                consumed = None
                if TEARDOWN_AUTH_CONSUMED.is_file():
                    try:
                        candidate = read_json(TEARDOWN_AUTH_CONSUMED, "consumed teardown authorization")
                        if candidate.get("nonce") == authorization["nonce"] and candidate.get("status") == "consumed":
                            consumed = candidate
                    except Refusal:
                        consumed = None
                invocation.update({"finishedAtUtc": now_utc(), "exitCode": status,
                                   "logSha256": sha256(teardown_log) if teardown_log.exists() else None,
                                   "authorizationNonce": authorization["nonce"],
                                   "authorizationSha256": authorization["sha256"],
                                   "cleanupRunReceiptSha256AtAuthorization": authorization["cleanupRunReceiptSha256"],
                                   "authorizationConsumed": consumed is not None,
                                   "authorizationConsumedReceiptPath": (str(TEARDOWN_AUTH_CONSUMED.relative_to(stack))
                                                                         if consumed else None),
                                   "authorizationConsumedReceiptSha256": (sha256(TEARDOWN_AUTH_CONSUMED)
                                                                            if consumed else None)})
                attempt["teardownAuthorization"].update({"status": "consumed" if consumed else "not-consumed",
                    "authorizationSha256": authorization["sha256"],
                    "consumedReceiptSha256": sha256(TEARDOWN_AUTH_CONSUMED) if consumed else None})
                if status == 0 and consumed is None:
                    raise Refusal("04-teardown.sh returned success without consuming its one-shot authorization.")
                if status != 0:
                    raise Refusal("04-teardown.sh returned nonzero; source-reader and watcher cleanup remain blocked.")
                if not teardown_receipt_path.exists():
                    raise Refusal("04-teardown.sh did not write a durable teardown receipt.")
                teardown = verify_teardown_receipt(stack, state)
                invocation["receiptPath"] = str(teardown_receipt_path.relative_to(stack))
                invocation["receiptSha256"] = sha256(teardown_receipt_path)
                invocation["receiptCompletedAtUtc"] = teardown["completedAtUtc"]
            verify_teardown_preserved_new(stack, teardown, before_audit, prepare)
            attempt["stage"] = "04-teardown-complete"
            save_receipt(receipt)
            append_timeline("04-teardown-complete", RECEIPT,
                            {"attempt": attempt["attempt"], "teardownReceiptSha256": invocation["receiptSha256"],
                             "newSnapshotsRetained": len((prepare.get("primaryAndReplicaSnapshots") or {}).get("records", []))})

            reader_log = attempt_dir / "source-reader-cleanup.log"
            reader_command = [sys.executable, str(READER_HELPER), "--state", str(STATE_DIR / "state.json"), "--apply"]
            status, output = run_capture(reader_command, reader_log, stack)
            if status != 0:
                raise Refusal("Exact source-reader cleanup helper failed; inspect its restricted local log.")
            reader_result = validate_reader_cleanup(output, gates["readerContract"])
            reader_path = attempt_dir / "source-reader-cleanup.json"
            atomic_json(reader_path, reader_result)
            attempt["sourceReaderCleanup"] = {"resultPath": str(reader_path.relative_to(stack)),
                                              "resultSha256": sha256(reader_path),
                                              "logPath": str(reader_log.relative_to(stack)),
                                              "logSha256": sha256(reader_log), **reader_result}
            attempt["stage"] = "source-reader-cleanup-complete"
            save_receipt(receipt)
            append_timeline("source-reader-cleanup-complete", RECEIPT,
                            {"attempt": attempt["attempt"], "status": reader_result["status"],
                             "residualScopeLocked": reader_result["residualScopeLocked"]})

            if watcher_decision == "delete-after-owned-stack-teardown":
                try:
                    watcher_gate = validate_watcher_gate(gates["watcherGatePath"],
                        gates["watcherContract"], stack, state)
                    watcher_decision = watcher_cleanup_decision(gates["watcherContract"], watcher_gate)
                except Refusal as exc:
                    watcher_gate_error = str(exc)
                    watcher_decision = "preserve-invalid-gate"
                if watcher_decision == "delete-after-owned-stack-teardown":
                    attempt["currentRootWatcherGate"] = watcher_gate
                    save_receipt(receipt)

            if watcher_decision == "delete-after-owned-stack-teardown":
                # 04's complete receipt is only written after AKS and the node/test
                # groups have reached absence. The helper then rechecks ownership,
                # ETag, regional inventory, and exact watcher-group contents. Do
                # a fresh child-resource read here as well; the root review gate
                # records the prior inventory/ETag, and its unrelated subscription
                # AKS clusters are not assumed to be ours.
                watcher_id = gates["watcherContract"]["networkWatcher"]["id"]
                child_review_path = attempt_dir / "owned-watcher-child-review.json"
                child_review = live_watcher_child_inventory(state["subscriptionId"], watcher_id,
                                                            child_review_path)
                watcher_child_summary = {"resultPath": str(child_review_path.relative_to(stack)),
                                         "resultSha256": sha256(child_review_path),
                                         "status": child_review["status"],
                                         "childCounts": {name: len(ids) for name, ids in
                                                         child_review.get("children", {}).items()},
                                         "observedAtUtc": child_review["observedAtUtc"],
                                         "completedAtUtc": child_review["completedAtUtc"]}
                attempt["ownedWatcherChildReview"] = watcher_child_summary
                if child_review["status"] == "clear":
                    watcher_log = attempt_dir / "owned-watcher-cleanup.log"
                    watcher_command = [sys.executable, str(WATCHER_HELPER), "--state",
                                       str(STATE_DIR / "state.json"), "--apply", "--all-stacks-gone"]
                    status, output = run_capture(watcher_command, watcher_log, stack)
                    if status != 0:
                        watcher_result = {"status": "watcher-review-required-after-helper-refusal",
                                          "mutated": "unknown", "helperExitCode": status,
                                          "reason": "The exact watcher helper refused or did not finish; inspect its log before any further watcher action."}
                    else:
                        try:
                            watcher_result = json.loads(output.strip().splitlines()[-1])
                        except (json.JSONDecodeError, IndexError):
                            watcher_result = {"status": "watcher-review-required-after-helper-refusal",
                                              "mutated": "unknown", "helperExitCode": status,
                                              "reason": "The exact watcher helper returned no parseable receipt; inspect its log."}
                        if (watcher_result.get("mutated") is not True or
                            watcher_result.get("status") not in {"deleted", "already-absent"}):
                            watcher_result = {"status": "watcher-review-required-after-helper-refusal",
                                              "mutated": "unknown", "helperExitCode": status,
                                              "reason": "The exact watcher helper returned an unverified outcome; inspect its log."}
                    watcher_result.update({"logPath": str(watcher_log.relative_to(stack)),
                                           "logSha256": sha256(watcher_log)})
                else:
                    has_children = child_review["status"] == "has-child-resources"
                    watcher_result = {"status": ("preserve-live-child-resources" if has_children
                                                  else "preserve-live-review-failed"),
                                      "mutated": False,
                                      "reason": ("Live watcher child resources appeared after the root review; preserving the watcher."
                                                 if has_children else
                                                 "Fresh watcher child inventory failed or was incomplete; preserving the watcher."),
                                      "childCounts": watcher_child_summary["childCounts"],
                                      "failedChild": child_review.get("failedChild"),
                                      "failure": child_review.get("failure")}
                watcher_result["rootGate"] = watcher_gate
                watcher_result["childReview"] = watcher_child_summary
                if "logPath" not in watcher_result:
                    watcher_result["logPath"] = None
                    watcher_result["logSha256"] = None
                watcher_succeeded = watcher_result["status"] in {"deleted", "already-absent"}
                watcher_needs_review = watcher_result["status"] in {
                    "watcher-review-required-after-helper-refusal", "preserve-live-review-failed"}
                attempt["ownedWatcherCleanup"] = watcher_result
                attempt["stage"] = ("owned-watcher-cleanup-complete" if watcher_succeeded
                                    else "owned-watcher-cleanup-needs-review" if watcher_needs_review
                                    else "owned-watcher-preserved")
                save_receipt(receipt)
                append_timeline(("owned-watcher-cleanup-complete" if watcher_succeeded else
                                 "owned-watcher-cleanup-needs-review" if watcher_needs_review else
                                 "owned-watcher-preserved"), RECEIPT,
                                {"attempt": attempt["attempt"], "status": watcher_result["status"],
                                 "watcherId": watcher_id,
                                 "rootGateSha256": watcher_gate["sha256"]})
            else:
                unresolved = watcher_decision == "watcher-review-required-after-helper-refusal"
                reason = ("An earlier watcher helper attempt had an unverified outcome; a fresh safe root gate is required to recheck it."
                          if unresolved else
                          "No root review gate was supplied; preserving the regional watcher."
                          if watcher_decision == "preserve-gate-absent" else
                          "Root watcher review gate was invalid or stale; preserving the regional watcher."
                          if watcher_decision == "preserve-invalid-gate" else
                          "Root reviewed shared regional use as unsafe to delete; preserving the regional watcher."
                          if watcher_decision == "preserve-shared-use-review" else
                          "This experiment did not create a Network Watcher.")
                attempt["ownedWatcherCleanup"] = {"status": watcher_decision,
                    "mutated": "unknown" if unresolved else False, "rootGate": watcher_gate,
                    "rootGateError": watcher_gate_error, "reason": reason}
                if gates["watcherContract"] is not None:
                    append_timeline(("owned-watcher-cleanup-needs-review" if unresolved
                                     else "owned-watcher-preserved"), RECEIPT,
                                    {"attempt": attempt["attempt"], "status": watcher_decision,
                                     "watcherId": gates["watcherContract"]["networkWatcher"]["id"],
                                     "reason": reason})

            final_audit = run_retention_audit("final", attempt_dir / "snapshot-audit-final.log")
            final_summary = validate_snapshot_audit(final_audit, prior, prepare,
                                                    state["ownerToken"], stack)
            compare_snapshot_audits(before_audit, final_audit)
            if final_audit.get("preservedPriorSnapshots") is not True or final_audit.get("allSnapshotsProtected") is not True:
                raise Refusal("Final audit did not preserve every snapshot identity and lock.")
            final_locks = capture_global_locks(state["subscriptionId"])
            assert_locks_preserved(live_locks_before, final_locks, baseline_locks)
            final_lock_path = attempt_dir / "locks-final.json"
            write_lock_receipt(final_lock_path, final_locks)
            final_snapshot_path = ROOT / "preflight" / "snapshot-audit-final.json"
            attempt.update({"stage": "final-retention-audit-complete",
                            "finalRetentionAuditPath": str(final_snapshot_path.relative_to(stack)),
                            "finalRetentionAuditSha256": sha256(final_snapshot_path),
                            "finalRetentionAudit": {"priorSnapshotCount": final_audit["priorSnapshotCount"],
                                "currentSnapshotCount": final_audit["currentSnapshotCount"],
                                "preservedPriorSnapshots": True, "allSnapshotsProtected": True,
                                "preparedSnapshots": final_summary["preparedSnapshots"]},
                            "finalLocksPath": str(final_lock_path.relative_to(stack)),
                            "finalLocksSha256": sha256(final_lock_path),
                            "completionDisposition": ("complete-watcher-preserved" if
                                attempt["ownedWatcherCleanup"]["status"].startswith("preserve-") else
                                "complete-watcher-review-required" if attempt["ownedWatcherCleanup"]["status"].startswith("watcher-review-required") else
                                "complete"),
                            "residualScopeLockedReaderAssignments": attempt["sourceReaderCleanup"]["residualScopeLocked"]})
            attempt.update({"status": "complete", "stage": "complete", "finishedAtUtc": now_utc()})
            receipt["status"] = "complete"
            save_receipt(receipt)
            append_timeline("cleanup-run-complete", RECEIPT,
                            {"attempt": attempt["attempt"], "finalRetentionAuditSha256": attempt["finalRetentionAuditSha256"],
                             "residualScopeLockedReaderAssignments": attempt["sourceReaderCleanup"]["residualScopeLocked"],
                             "watcherStatus": attempt["ownedWatcherCleanup"]["status"]})
            return receipt
        except Exception as exc:
            attempt.update({"status": "failed", "stage": attempt.get("stage", "failed"),
                            "failedAtUtc": now_utc(), "failure": str(exc)})
            receipt["status"] = "failed"
            save_receipt(receipt)
            try:
                append_timeline("cleanup-run-failed", RECEIPT,
                                {"attempt": attempt["attempt"], "stage": attempt["stage"], "failure": str(exc)})
            except Exception:
                pass
            if isinstance(exc, Refusal):
                raise
            raise Refusal("Cleanup orchestration failed; the durable receipt and per-step logs were preserved.") from exc


def _self_test() -> None:
    assert parse_utc("2026-10-09T00:00:00Z", "test").tzinfo is not None
    try:
        parse_utc("2026-10-09T00:00:00", "test")
    except Refusal:
        pass
    else:
        raise AssertionError("naive timestamp accepted")
    print("cleanup-run self-test: PASS (local-only)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="run read-only Azure audits and authorized cleanup helpers")
    parser.add_argument("--root-watcher-gate", type=Path, default=WATCHER_GATE_DEFAULT,
                        help="root-written gate for deleting this stack's owned regional Network Watcher")
    parser.add_argument("--self-test", action="store_true", help="run local-only smoke checks")
    args = parser.parse_args()
    if args.self_test:
        _self_test()
        return 0
    stack = ROOT
    gates = validate_gate_inputs(stack, args.root_watcher_gate)
    if not args.apply:
        watcher_decision = watcher_cleanup_decision(gates["watcherContract"], gates["watcherGate"],
                                                   gates.get("watcherGateError"))
        teardown_resume = gates["existingTeardownReceipt"] is not None
        planned_steps = ["retention-audit.py --label before-cleanup",
                         ("verify existing run/.state/teardown.json" if teardown_resume
                          else "run/04-teardown.sh"),
                         "cleanup-source-readers.py --apply"]
        if watcher_decision == "delete-after-owned-stack-teardown":
            planned_steps.append("cleanup-owned-watcher.py --apply --all-stacks-gone (after verified 04 receipt)")
        elif watcher_decision != "not-owned":
            planned_steps.append("preserve owned regional Network Watcher")
        planned_steps.append("retention-audit.py --label final")
        output = {"status": "dry-run-ready", "mutated": False,
                  "stack": stack.name, "test": gates["test"], "archive": gates["archive"],
                  "newBlob": gates["newBlob"],
                  "preparationVerification": {
                      key: value for key, value in gates["preparationVerification"].items()
                      if key != "proofFiles"},
                  "offlineSeedBackup": gates["offlineSeedBackup"],
                  "priorSnapshotCount": len(gates["priorSnapshots"]),
                  "priorLockCount": len(gates["baselineLocks"]),
                  "watcherCleanup": watcher_decision,
                  "watcherGateError": gates.get("watcherGateError"),
                  "teardownResume": ("existing-complete-receipt" if teardown_resume else "invoke-04"),
                  "plannedSteps": planned_steps}
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    result = apply_cleanup(gates, stack)
    print(json.dumps({"status": result["status"], "receipt": str(RECEIPT.relative_to(stack)),
                      "attempts": len(result.get("attempts", []))}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Refusal as error:
        print(f"REFUSED: {error}", file=sys.stderr)
        raise SystemExit(2)
