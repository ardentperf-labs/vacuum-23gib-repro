#!/usr/bin/env python3
"""Validate and consume the one-shot local authorization for owned teardown.

This helper performs no Azure calls. It prevents accidental direct invocation
of 04-teardown.sh unless cleanup-run.py issued a fresh, private receipt bound to
the exact stack state, proof artifacts, cleanup receipt, and teardown script.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
import uuid
from typing import Any


SHA_RE = re.compile(r"^[a-f0-9]{64}$")
NONCE_RE = re.compile(r"^[a-f0-9]{32}$")
REQUIRED_KINDS = {
    "state", "experimentPlan", "prepareAudit", "prepareProcess",
    "sourceManifest", "sourceEvidenceManifest", "archiveInput",
    "offlineSeedBackup", "testProcess", "testSummary", "testTiming",
    "nativeClientSummary", "newBlobStorage", "newBlobContainer",
    "priorSnapshotInventory", "priorLockInventory",
    "beforeCleanupSnapshotAudit", "beforeCleanupLocks", "04Script",
    "authorizationHelper", "cleanupRunScript",
}


class Refusal(RuntimeError):
    pass


def read_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Refusal(f"Required {label} is missing, unreadable, or invalid JSON.") from exc


def digest(path: Path) -> tuple[int, str]:
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            size += len(block)
            h.update(block)
    return size, h.hexdigest()


def parse_time(value: Any, label: str) -> dt.datetime:
    if not isinstance(value, str) or not value:
        raise Refusal(f"Authorization {label} is missing.")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise Refusal(f"Authorization {label} is invalid.") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise Refusal(f"Authorization {label} must include a timezone.")
    return parsed.astimezone(dt.timezone.utc)


def ensure_private_file(path: Path, label: str) -> None:
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
    except OSError as exc:
        raise Refusal(f"Required {label} file is unavailable.") from exc
    if mode & 0o077:
        raise Refusal(f"{label} file permissions are not private.")


def expected_path(root: Path, path: Path, relative: str, label: str) -> Path:
    resolved = path.resolve()
    expected = (root / relative).resolve()
    if resolved != expected or not resolved.is_relative_to(root):
        raise Refusal(f"{label} path is not the exact experiment-local path.")
    return resolved


def validate_proof_files(root: Path, records: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(records, list) or not records:
        raise Refusal("Authorization has no proof-file inventory.")
    by_path: dict[str, dict[str, Any]] = {}
    kinds: set[str] = set()
    for row in records:
        if not isinstance(row, dict):
            raise Refusal("Authorization proof-file entry is malformed.")
        relative = row.get("path")
        kind = row.get("kind")
        size = row.get("bytes")
        expected_sha = row.get("sha256")
        if (not isinstance(relative, str) or not relative or Path(relative).is_absolute() or
            not isinstance(kind, str) or not kind or not isinstance(size, int) or
            isinstance(size, bool) or size < 0 or not isinstance(expected_sha, str) or
            not SHA_RE.fullmatch(expected_sha)):
            raise Refusal("Authorization proof-file entry lacks a safe path, kind, size, or SHA-256.")
        key = relative.casefold()
        if key in by_path:
            raise Refusal("Authorization proof-file inventory contains duplicate paths.")
        path = (root / relative).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise Refusal("An authorized proof file is missing or escapes the experiment directory.")
        actual_size, actual_sha = digest(path)
        if actual_size != size or actual_sha != expected_sha:
            raise Refusal("An authorized proof file changed size or SHA-256 after cleanup approval.")
        by_path[key] = {**row, "resolvedPath": path}
        kinds.add(kind)
    missing = sorted(REQUIRED_KINDS - kinds)
    if missing:
        raise Refusal("Authorization is missing required proof categories: " + ", ".join(missing))

    manifest = by_path.get("run/recovery/source-evidence-manifest.json")
    if manifest is None or manifest["kind"] != "sourceEvidenceManifest":
        raise Refusal("Authorization does not bind the pinned source-evidence manifest.")
    manifest_json = read_json(manifest["resolvedPath"], "source-evidence manifest")
    files = manifest_json.get("files")
    if not isinstance(files, list) or not files:
        raise Refusal("Pinned source-evidence manifest has no file inventory.")
    for item in files:
        if not isinstance(item, dict) or not isinstance(item.get("path"), str):
            raise Refusal("Pinned source-evidence manifest contains a malformed file entry.")
        relative = "run/" + item["path"]
        proof = by_path.get(relative.casefold())
        if (proof is None or proof["bytes"] != item.get("bytes") or
            proof["sha256"] != item.get("sha256")):
            raise Refusal("Authorization does not bind every pinned original archive/evidence input.")
        if item["path"].startswith("recovery/archive-input/") and proof["kind"] != "archiveInput":
            raise Refusal("Private archive input is not classified in the authorization proof set.")
    prepare_audit = by_path.get("run/.state/recovery/prepare-audit.json")
    if prepare_audit is None or prepare_audit["kind"] != "prepareAudit":
        raise Refusal("Authorization does not bind the preparation audit used for snapshot UID checks.")
    prepared = read_json(prepare_audit["resolvedPath"], "preparation audit")
    snapshot_records = (prepared.get("primaryAndReplicaSnapshots") or {}).get("records")
    if not isinstance(snapshot_records, list) or len(snapshot_records) != 2:
        raise Refusal("Preparation audit does not contain the exact DATA/WAL snapshot pair.")
    captured_uids: dict[str, str] = {}
    for proof in records:
        if proof.get("kind") != "preparedSeedSnapshotEvidence":
            continue
        captured = read_json(by_path[proof["path"].casefold()]["resolvedPath"],
                             "prepared snapshot ARM evidence")
        if isinstance(captured.get("id"), str) and isinstance(captured.get("uniqueId"), str):
            captured_uids[captured["id"].casefold()] = captured["uniqueId"]
    expected_uids: set[str] = set()
    for row in snapshot_records:
        if not isinstance(row, dict) or not isinstance(row.get("id"), str):
            raise Refusal("Preparation audit contains a malformed prepared snapshot identity.")
        uid = row.get("uniqueId") or captured_uids.get(row["id"].casefold())
        if not isinstance(uid, str) or not uid:
            raise Refusal("Authorization lacks a uniqueId for a prepared DATA/WAL snapshot.")
        captured = captured_uids.get(row["id"].casefold())
        if captured is not None and captured != uid:
            raise Refusal("Captured prepared snapshot ARM uniqueId differs from the preparation record.")
        expected_uids.add(uid)
    if len(expected_uids) != 2:
        raise Refusal("Prepared DATA/WAL snapshots do not have two distinct uniqueIds.")
    return by_path


def validate_authorization(root: Path, state_path: Path, auth_path: Path,
                           cleanup_receipt_path: Path, script_path: Path,
                           consumed_path: Path, parent_pid: int, script_pid: int,
                           check_process: bool = False) -> dict[str, Any]:
    root = root.resolve()
    state_path = expected_path(root, state_path, "run/.state/state.json", "ownership state")
    auth_path = expected_path(root, auth_path, "run/.state/teardown-authorization.json", "authorization")
    cleanup_receipt_path = expected_path(root, cleanup_receipt_path,
                                         "run/.state/cleanup-run.json", "cleanup receipt")
    script_path = expected_path(root, script_path, "run/04-teardown.sh", "teardown script")
    consumed_path = expected_path(root, consumed_path,
                                  "run/.state/teardown-authorization-consumed.json", "consumed receipt")
    ensure_private_file(auth_path, "Authorization")
    auth = read_json(auth_path, "one-shot teardown authorization")
    if auth.get("version") != 1 or auth.get("status") != "authorized":
        raise Refusal("One-shot teardown authorization has an unknown version or state.")
    nonce = auth.get("nonce")
    if not isinstance(nonce, str) or not NONCE_RE.fullmatch(nonce):
        raise Refusal("One-shot teardown authorization nonce is invalid.")
    if consumed_path.exists():
        consumed = read_json(consumed_path, "consumed authorization receipt")
        if consumed.get("nonce") == nonce:
            raise Refusal("One-shot teardown authorization was already consumed.")
    now = dt.datetime.now(dt.timezone.utc)
    issued = parse_time(auth.get("issuedAtUtc"), "issuedAtUtc")
    expires = parse_time(auth.get("expiresAtUtc"), "expiresAtUtc")
    if issued > now + dt.timedelta(minutes=2) or expires <= now or expires <= issued or expires - issued > dt.timedelta(minutes=20):
        raise Refusal("One-shot teardown authorization is expired, future-dated, or has an invalid lifetime.")
    if (not isinstance(parent_pid, int) or parent_pid <= 0 or
        not isinstance(script_pid, int) or script_pid <= 0 or
        auth.get("cleanupRunPid") != parent_pid):
        raise Refusal("Teardown was not launched by the cleanup-run process recorded in the authorization.")
    if check_process:
        if os.getppid() != script_pid:
            raise Refusal("Teardown authorization does not match the running 04-teardown.sh process.")
        stat_path = Path(f"/proc/{script_pid}/stat")
        try:
            stat_text = stat_path.read_text(encoding="utf-8")
            fields = stat_text[stat_text.rfind(")") + 2:].split()
            actual_parent = int(fields[1])
        except (OSError, ValueError, IndexError) as exc:
            raise Refusal("Cannot verify the cleanup-run parent process for teardown.") from exc
        if actual_parent != parent_pid:
            raise Refusal("Running teardown process is not a child of the authorized cleanup-run process.")
        try:
            command_line = Path(f"/proc/{parent_pid}/cmdline").read_bytes().split(b"\0")
        except OSError as exc:
            raise Refusal("Cannot inspect the cleanup-run parent process.") from exc
        if not any(b"cleanup-run.py" in part for part in command_line) or b"--apply" not in command_line:
            raise Refusal("Teardown parent is not an active cleanup-run.py --apply process.")

    state = read_json(state_path, "owned stack state")
    token = state.get("ownerToken")
    subscription = state.get("subscriptionId")
    resource_group = state.get("resourceGroup")
    requested_nodes = state.get("requestedNodeResourceGroup")
    if (auth.get("ownerToken") != token or auth.get("subscriptionId") != subscription or
        auth.get("resourceGroup") != resource_group or
        auth.get("requestedNodeResourceGroup") != requested_nodes):
        raise Refusal("Authorization token, subscription, or resource-group identity differs from saved stack state.")

    cleanup_receipt = read_json(cleanup_receipt_path, "cleanup-run receipt")
    if (cleanup_receipt.get("status") != "running" or
        cleanup_receipt.get("ownerToken") != token or
        cleanup_receipt.get("subscriptionId") != subscription or
        cleanup_receipt.get("stack") != root.name):
        raise Refusal("Cleanup-run receipt does not bind an active attempt for this exact stack.")
    attempts = cleanup_receipt.get("attempts")
    if not isinstance(attempts, list) or not attempts or not isinstance(attempts[-1], dict):
        raise Refusal("Cleanup-run receipt has no active cleanup attempt.")
    attempt = attempts[-1]
    expected_attempt = auth.get("attempt") or {}
    intent = attempt.get("teardownAuthorization") or {}
    if (attempt.get("id") != expected_attempt.get("id") or
        attempt.get("attempt") != expected_attempt.get("number") or
        attempt.get("stage") != "04-teardown-authorized" or
        intent.get("status") != "issued" or intent.get("nonce") != nonce or
        intent.get("proofFiles") != auth.get("proofFiles") or
        intent.get("teardownScriptSha256") != auth.get("teardownScriptSha256")):
        raise Refusal("Cleanup-run receipt does not contain this pending teardown authorization intent.")

    receipt_size, receipt_sha = digest(cleanup_receipt_path)
    if (auth.get("cleanupRunReceiptPath") != "run/.state/cleanup-run.json" or
        auth.get("cleanupRunReceiptBytes") != receipt_size or
        auth.get("cleanupRunReceiptSha256") != receipt_sha):
        raise Refusal("Cleanup-run receipt changed after teardown authorization was issued.")
    script_size, script_sha = digest(script_path)
    if auth.get("teardownScriptSha256") != script_sha or auth.get("teardownScriptBytes") != script_size:
        raise Refusal("04-teardown.sh changed after teardown authorization was issued.")
    proof_files = validate_proof_files(root, auth.get("proofFiles"))
    expected_state_proof = proof_files.get("run/.state/state.json")
    if (expected_state_proof is None or expected_state_proof["kind"] != "state" or
        expected_state_proof["resolvedPath"] != state_path):
        raise Refusal("Authorization proof set does not bind the exact saved ownership state.")
    helper_proof = proof_files.get("run/recovery/verify-teardown-authorization.py")
    if (helper_proof is None or helper_proof["kind"] != "authorizationHelper" or
        helper_proof["resolvedPath"] != (root / "run/recovery/verify-teardown-authorization.py").resolve()):
        raise Refusal("Authorization proof set does not bind the exact authorization helper.")
    runner_proof = proof_files.get("cleanup-run.py")
    if (runner_proof is None or runner_proof["kind"] != "cleanupRunScript" or
        runner_proof["resolvedPath"] != (root / "cleanup-run.py").resolve()):
        raise Refusal("Authorization proof set does not bind the cleanup-run script.")

    return {"nonce": nonce, "attemptId": attempt["id"], "attemptNumber": attempt["attempt"],
            "proofFileCount": len(proof_files), "cleanupRunReceiptSha256": receipt_sha,
            "teardownScriptSha256": script_sha}


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


def consume_authorization(root: Path, auth_path: Path, consumed_path: Path,
                          verified: dict[str, Any]) -> None:
    auth_sha = digest(auth_path)[1]
    atomic_json(consumed_path, {"version": 1, "status": "consumed",
                                "nonce": verified["nonce"], "attemptId": verified["attemptId"],
                                "attemptNumber": verified["attemptNumber"],
                                "consumedAtUtc": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
                                "authorizationSha256": auth_sha,
                                "cleanupRunReceiptSha256": verified["cleanupRunReceiptSha256"]})
    try:
        auth_path.unlink()
    except FileNotFoundError as exc:
        raise Refusal("Teardown authorization disappeared before it could be consumed.") from exc
    dir_fd = os.open(auth_path.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _self_test() -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root / "run/.state").mkdir(parents=True)
        (root / "run/recovery").mkdir(parents=True)
        (root / "preflight").mkdir()
        (root / "verification").mkdir()
        (root / "run/04-teardown.sh").write_text("#!/bin/sh\n", encoding="utf-8")
        (root / "cleanup-run.py").write_text("# authorized cleanup runner fixture\n", encoding="utf-8")
        import shutil
        shutil.copy2(Path(__file__), root / "run/recovery/verify-teardown-authorization.py")
        state_path = root / "run/.state/state.json"
        state = {"ownerToken": "a" * 32, "subscriptionId": "sub", "resourceGroup": "rg",
                 "requestedNodeResourceGroup": "nodes"}
        state_path.write_text(json.dumps(state), encoding="utf-8")
        cleanup_path = root / "run/.state/cleanup-run.json"
        nonce = uuid.uuid4().hex
        cleanup = {"status": "running", "ownerToken": state["ownerToken"],
                   "subscriptionId": "sub", "stack": root.name,
                   "attempts": [{"id": "attempt-id", "attempt": 1, "stage": "04-teardown-authorized",
                                 "teardownAuthorization": {"status": "issued", "nonce": nonce}}]}
        file_payloads = {
            "experimentPlan": ("experiment-plan.json", {"subscriptionId": "sub"}),
            "prepareAudit": ("run/.state/recovery/prepare-audit.json",
                             {"primaryAndReplicaSnapshots": {"records": [
                                 {"id": "/snapshots/data", "uniqueId": "data-uid"},
                                 {"id": "/snapshots/wal", "uniqueId": "wal-uid"}]}}),
            "prepareProcess": ("run/.state/prepare-process.json", {}),
            "sourceManifest": ("run/source-manifest.json", {}),
            "offlineSeedBackup": ("run/.state/recovery/evidence/offline.json", {}),
            "testProcess": ("run/.state/test-process.json", {}),
            "testSummary": ("results/summary.json", {}),
            "testTiming": ("results/timing.json", {}),
            "nativeClientSummary": ("results/client-summary.json", {}),
            "newBlobStorage": ("run/.state/storage.json", {}),
            "newBlobContainer": ("run/.state/storage-container.json", {}),
            "priorSnapshotInventory": ("preflight/all-snapshots-before.json", []),
            "priorLockInventory": ("preflight/locks.json", []),
            "beforeCleanupSnapshotAudit": ("verification/before.json", {}),
            "beforeCleanupLocks": ("verification/locks.json", {}),
        }
        proof: list[dict[str, Any]] = []
        def add(path: Path, kind: str) -> None:
            data = path.read_bytes()
            proof.append({"path": str(path.relative_to(root)), "kind": kind,
                          "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
        add(state_path, "state")
        for kind, (relative, value) in file_payloads.items():
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(value), encoding="utf-8")
            add(path, kind)
        source_input = root / "run/recovery/archive-input/receipt.json"
        source_input.parent.mkdir(parents=True, exist_ok=True)
        source_input.write_text("{}", encoding="utf-8")
        add(source_input, "archiveInput")
        script_path = root / "run/04-teardown.sh"
        evidence_manifest = {"files": [{"path": "recovery/archive-input/receipt.json",
                                         "bytes": source_input.stat().st_size,
                                         "sha256": hashlib.sha256(source_input.read_bytes()).hexdigest()}]}
        manifest_path = root / "run/recovery/source-evidence-manifest.json"
        manifest_path.write_text(json.dumps(evidence_manifest), encoding="utf-8")
        add(manifest_path, "sourceEvidenceManifest")
        add(script_path, "04Script")
        add(root / "run/recovery/verify-teardown-authorization.py", "authorizationHelper")
        add(root / "cleanup-run.py", "cleanupRunScript")
        cleanup["attempts"][-1]["teardownAuthorization"].update({
            "proofFiles": proof,
            "teardownScriptSha256": hashlib.sha256(script_path.read_bytes()).hexdigest()})
        cleanup_path.write_text(json.dumps(cleanup), encoding="utf-8")
        auth_path = root / "run/.state/teardown-authorization.json"
        auth = {"version": 1, "status": "authorized", "nonce": nonce,
                "issuedAtUtc": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
                "expiresAtUtc": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=10)).isoformat().replace("+00:00", "Z"),
                "cleanupRunPid": os.getpid(), "ownerToken": state["ownerToken"], "subscriptionId": "sub",
                "resourceGroup": "rg", "requestedNodeResourceGroup": "nodes",
                "attempt": {"id": "attempt-id", "number": 1},
                "cleanupRunReceiptPath": "run/.state/cleanup-run.json",
                "cleanupRunReceiptBytes": cleanup_path.stat().st_size,
                "cleanupRunReceiptSha256": hashlib.sha256(cleanup_path.read_bytes()).hexdigest(),
                "teardownScriptBytes": script_path.stat().st_size,
                "teardownScriptSha256": hashlib.sha256(script_path.read_bytes()).hexdigest(),
                "proofFiles": proof}
        auth_path.write_text(json.dumps(auth), encoding="utf-8")
        os.chmod(auth_path, 0o600)
        consumed = root / "run/.state/teardown-authorization-consumed.json"
        verified = validate_authorization(root, state_path, auth_path, cleanup_path, script_path,
                                          consumed, os.getpid(), os.getpid(), check_process=False)
        assert verified["proofFileCount"] == len(proof)
        try:
            validate_authorization(root, state_path, auth_path, cleanup_path, script_path,
                                   consumed, os.getpid() + 1, os.getpid(), check_process=False)
        except Refusal as exc:
            if "not launched by" not in str(exc):
                raise
        else:
            raise AssertionError("wrong launcher PID was accepted")
        # A proof edit is rejected before any consume/mutation step.
        summary_path = root / "results/summary.json"
        original_summary = summary_path.read_bytes()
        summary_path.write_text("changed", encoding="utf-8")
        try:
            validate_authorization(root, state_path, auth_path, cleanup_path, script_path,
                                   consumed, os.getpid(), os.getpid(), check_process=False)
        except Refusal:
            pass
        else:
            raise AssertionError("changed proof file was accepted")
        summary_path.write_bytes(original_summary)
        old_receipt = cleanup_path.read_bytes()
        cleanup_path.write_bytes(old_receipt + b"\n")
        try:
            validate_authorization(root, state_path, auth_path, cleanup_path, script_path,
                                   consumed, os.getpid(), os.getpid(), check_process=False)
        except Refusal as exc:
            if "changed after teardown authorization" not in str(exc):
                raise
        else:
            raise AssertionError("changed cleanup-run receipt was accepted")
        cleanup_path.write_bytes(old_receipt)
        old_script = script_path.read_bytes()
        script_path.write_bytes(old_script + b"# changed\n")
        try:
            validate_authorization(root, state_path, auth_path, cleanup_path, script_path,
                                   consumed, os.getpid(), os.getpid(), check_process=False)
        except Refusal as exc:
            if "changed after teardown authorization" not in str(exc):
                raise
        else:
            raise AssertionError("changed 04 script was accepted")
        script_path.write_bytes(old_script)
        consume_authorization(root, auth_path, consumed, verified)
        if auth_path.exists():
            raise AssertionError("authorization was not consumed")
        auth_path.write_text(json.dumps(auth), encoding="utf-8")
        os.chmod(auth_path, 0o600)
        try:
            validate_authorization(root, state_path, auth_path, cleanup_path, script_path,
                                   consumed, os.getpid(), os.getpid(), check_process=False)
        except Refusal as exc:
            if "already consumed" not in str(exc):
                raise
        else:
            raise AssertionError("consumed authorization was replayed")
    print("verify-teardown-authorization self-test: PASS (local-only)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--state", type=Path, required=False)
    parser.add_argument("--authorization", type=Path, required=False)
    parser.add_argument("--cleanup-receipt", type=Path, required=False)
    parser.add_argument("--script", type=Path, required=False)
    parser.add_argument("--consumed-receipt", type=Path, required=False)
    parser.add_argument("--parent-pid", type=int, required=False)
    parser.add_argument("--script-pid", type=int, required=False)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        _self_test()
        return 0
    root = args.root.resolve()
    try:
        verified = validate_authorization(
            root,
            args.state or root / "run/.state/state.json",
            args.authorization or root / "run/.state/teardown-authorization.json",
            args.cleanup_receipt or root / "run/.state/cleanup-run.json",
            args.script or root / "run/04-teardown.sh",
            args.consumed_receipt or root / "run/.state/teardown-authorization-consumed.json",
            args.parent_pid if args.parent_pid is not None else -1,
            args.script_pid if args.script_pid is not None else -1,
            check_process=True)
        consume_authorization(root, args.authorization or root / "run/.state/teardown-authorization.json",
                              args.consumed_receipt or root / "run/.state/teardown-authorization-consumed.json",
                              verified)
    except Refusal as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return 2
    print(json.dumps({"status": "consumed", **{key: value for key, value in verified.items()
                                               if key != "nonce"}}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
