#!/usr/bin/env python3
"""Preserve read-only PostgreSQL recovery files and control metadata locally.

All raw files and the private receipt are written with owner-only permissions
under a stack's ignored ``.state`` directory. A small public receipt contains
only hashes, sizes, recovery timeline/LSNs, system identifier, and source-pair
proof hash. This helper makes no PostgreSQL, Kubernetes, or Azure mutations.
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
STACKS = ("sigterm", "control")
NS = "repro"
CLUSTER = "hdd-pg"
TOKEN_RE = re.compile(r"^[a-f0-9]{32}$")
POD_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{0,62}$")
HISTORY_RE = re.compile(r"^[0-9A-F]{8}\.history$")
WAL_RE = re.compile(r"^[0-9A-F]{24}$")
WAL_COPY_CHUNK_BYTES = 2 * 1024 * 1024
WAL_COPY_CHUNK_ATTEMPTS = 3
TIMESTAMP = lambda: datetime.now(timezone.utc).isoformat()
SQL = r"""
SELECT json_build_object(
  'systemIdentifier', (SELECT system_identifier::text FROM pg_control_system()),
  'checkpointLsn', (SELECT checkpoint_lsn::text FROM pg_control_checkpoint()),
  'checkpointTimeline', (SELECT timeline_id FROM pg_control_checkpoint()),
  'isInRecovery', pg_is_in_recovery(),
  'appClientSessions', (SELECT count(*) FROM pg_stat_activity WHERE datname = 'repro' AND usename = 'app' AND backend_type = 'client backend'),
  'vacuumClientSessions', (SELECT count(*) FROM pg_stat_activity WHERE datname = 'repro' AND usename = 'postgres' AND backend_type = 'client backend' AND application_name LIKE 'vacuum-only-%'),
  'currentFlushLsn', CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_current_wal_flush_lsn()::text END,
  'receiveLsn', pg_last_wal_receive_lsn()::text,
  'replayLsn', pg_last_wal_replay_lsn()::text,
  'walReceiverTimeline', CASE WHEN pg_is_in_recovery()
    THEN (SELECT received_tli FROM pg_stat_wal_receiver LIMIT 1) ELSE NULL END,
  'currentWalFile', CASE WHEN pg_is_in_recovery() THEN NULL
    ELSE pg_walfile_name(pg_current_wal_flush_lsn()) END,
  'lastWalSegment', CASE WHEN pg_is_in_recovery() THEN NULL
    ELSE CASE WHEN pg_current_wal_flush_lsn() = '0/0'::pg_lsn THEN NULL
         ELSE pg_walfile_name(pg_current_wal_flush_lsn() - 1) END END,
  'dataDirectory', current_setting('data_directory'),
  'walSegmentSize', current_setting('wal_segment_size')
)::text;
""".strip()


class Refusal(RuntimeError):
    """A failed ownership or evidence check."""


def require(ok: bool, message: str) -> None:
    if not ok:
        raise Refusal(message)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_json(path: Path, value: Any, mode: int = 0o600) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    require(not path.parent.is_symlink() and not path.is_symlink(), "Refusing a symlinked recovery receipt path.")
    fd, raw = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(raw)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def hash_record(relative: str, data: bytes, status: str = "present") -> dict[str, Any]:
    return {"path": relative, "status": status, "bytes": len(data), "sha256": sha256_bytes(data)}


def safe_relative(path: str) -> Path:
    parts = path.split("/")
    require(bool(path) and not path.startswith("/") and all(p not in ("", ".", "..") for p in parts)
            and "\\" not in path and "\x00" not in path,
            "Unsafe recovery metadata path.")
    return Path(*parts)


def write_private(root: Path, relative: str, data: bytes, max_bytes: int) -> dict[str, Any]:
    require(len(data) <= max_bytes, f"Recovery metadata file exceeded its size bound: {relative}")
    rel = safe_relative(relative)
    target = root / rel
    cursor = root
    for part in rel.parts[:-1]:
        cursor = cursor / part
        cursor.mkdir(mode=0o700, exist_ok=True)
        require(not cursor.is_symlink() and cursor.is_dir(), "Recovery metadata path contains a symlink/non-directory.")
    require(not target.is_symlink(), "Recovery metadata target is a symlink.")
    fd, raw = tempfile.mkstemp(prefix=".metadata-", dir=target.parent)
    temporary = Path(raw)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return hash_record(relative, data)


def call(argv: list[str], *, input_bytes: bytes | None = None, timeout: int = 120,
         check: bool = True) -> subprocess.CompletedProcess[bytes]:
    try:
        result = subprocess.run(argv, input=input_bytes, capture_output=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired as exc:
        raise Refusal(f"Read-only command timed out: {argv[0]}") from exc
    if check and result.returncode != 0:
        raise Refusal(f"Read-only command failed: {argv[0]} (details suppressed).")
    return result


def parse_json_line(raw: bytes, what: str) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8").strip())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Refusal(f"Could not parse PostgreSQL {what} JSON.") from exc
    require(isinstance(value, dict), f"PostgreSQL {what} result is not an object.")
    return value


def kubectl(base: list[str], args: list[str], *, timeout: int = 180,
            input_bytes: bytes | None = None, check: bool = True) -> subprocess.CompletedProcess[bytes]:
    return call([*base, *args], input_bytes=input_bytes, timeout=timeout, check=check)


def sql_snapshot(base: list[str], pod: str) -> dict[str, Any]:
    require(bool(POD_RE.fullmatch(pod)), "Unsafe PostgreSQL pod name.")
    result = kubectl(base, ["exec", "-n", NS, pod, "-c", "postgres", "--", "psql", "-XAt",
                            "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "repro", "-c", SQL])
    value = parse_json_line(result.stdout, "recovery snapshot")
    require(bool(re.fullmatch(r"[0-9]+", str(value.get("systemIdentifier", "")))),
            "PostgreSQL returned an invalid system identifier.")
    datadir = value.get("dataDirectory", "")
    require(isinstance(datadir, str) and datadir.startswith("/") and "\n" not in datadir
            and "\x00" not in datadir and len(datadir) < 512,
            "PostgreSQL returned an unsafe data directory.")
    require(value.get("isInRecovery") in (True, False), "PostgreSQL recovery state is missing.")
    if value["isInRecovery"]:
        # pg_walfile_name() and pg_walfile_name_offset() error during recovery.
        # Use the receiver's TLI with its flushed receive LSN. If streaming is
        # temporarily disconnected, use checkpoint TLI with the replay LSN.
        if value.get("walReceiverTimeline") and value.get("receiveLsn"):
            timeline, position = value["walReceiverTimeline"], value["receiveLsn"]
        else:
            timeline, position = value.get("checkpointTimeline"), value.get("replayLsn")
        value["walFilenameTimeline"] = timeline
        value["walFilenameLsn"] = position
        value["currentWalFile"] = wal_filename(timeline, position, value.get("walSegmentSize"))
        previous = lsn_minus_one(position)
        value["lastWalSegment"] = wal_filename(timeline, previous, value.get("walSegmentSize"))
    require(value.get("appClientSessions") == 0 and value.get("vacuumClientSessions") == 0,
            "Application client or VACUUM sessions remain; wait until client and VACUUM have stopped.")
    current_file = value.get("currentWalFile")
    if current_file is not None:
        require(bool(WAL_RE.fullmatch(current_file)), "PostgreSQL returned an invalid current WAL filename.")
    candidate = value.get("lastWalSegment")
    if candidate is not None:
        require(bool(WAL_RE.fullmatch(candidate)), "PostgreSQL returned an invalid last WAL segment filename.")
    return value


def remote_read(base: list[str], pod: str, datadir: str, relative: str, *, max_bytes: int,
                missing_ok: bool = False) -> bytes | None:
    rel = safe_relative(relative)
    target = f"{datadir}/{rel.as_posix()}"
    # Paths are arguments, not interpolated into the command string.
    probe = kubectl(base, ["exec", "-n", NS, pod, "-c", "postgres", "--", "sh", "-c",
                           'if [ -f "$1" ]; then printf present; else printf absent; fi', "sh", target])
    status = probe.stdout.decode("ascii", errors="strict").strip()
    require(status in ("present", "absent"), "Could not determine whether an optional PostgreSQL file exists.")
    if status == "absent":
        require(missing_ok, f"Required PostgreSQL recovery file is absent: {relative}")
        return None
    result = kubectl(base, ["exec", "-n", NS, pod, "-c", "postgres", "--", "cat", target], timeout=180)
    require(len(result.stdout) <= max_bytes, f"PostgreSQL recovery file exceeds its size bound: {relative}")
    return result.stdout


def remote_wal_fingerprint(base: list[str], pod: str, target: str) -> tuple[int, str]:
    script = 'set -eu; f="$1"; test -f "$f"; stat -c %s -- "$f"; sha256sum -- "$f" | cut -d " " -f1'
    result = kubectl(base, ["exec", "-n", NS, pod, "-c", "postgres", "--", "sh", "-c",
                            script, "sh", target], timeout=120)
    try:
        lines = result.stdout.decode("ascii").splitlines()
        size = int(lines[0]) if len(lines) == 2 else -1
    except (UnicodeDecodeError, ValueError):
        size = -1
        lines = []
    require(size >= 0 and len(lines) == 2 and re.fullmatch(r"[a-f0-9]{64}", lines[1]) is not None,
            "Could not verify the remote WAL segment size and hash.")
    return size, lines[1]


def remote_read_wal_segment(base: list[str], pod: str, datadir: str, wal_name: str,
                            expected_bytes: int) -> bytes:
    """Read a full WAL segment in bounded chunks and verify remote/local hashes."""
    require(bool(WAL_RE.fullmatch(wal_name)), "Refusing an invalid remote WAL filename.")
    require(expected_bytes in {16 * 1024 * 1024, 32 * 1024 * 1024, 64 * 1024 * 1024},
            "Unexpected WAL segment size; refusing a large local copy.")
    target = f"{datadir}/pg_wal/{wal_name}"
    before_size, before_hash = remote_wal_fingerprint(base, pod, target)
    require(before_size == expected_bytes, "Remote WAL segment is not the expected complete size.")

    parts: list[bytes] = []
    chunk_count = (expected_bytes + WAL_COPY_CHUNK_BYTES - 1) // WAL_COPY_CHUNK_BYTES
    for index in range(chunk_count):
        offset = index * WAL_COPY_CHUNK_BYTES
        expected_chunk = min(WAL_COPY_CHUNK_BYTES, expected_bytes - offset)
        script = 'dd if="$1" bs=2097152 skip="$2" count=1 iflag=fullblock 2>/dev/null'
        chunk: bytes | None = None
        for _attempt in range(1, WAL_COPY_CHUNK_ATTEMPTS + 1):
            try:
                result = kubectl(base, ["exec", "-n", NS, pod, "-c", "postgres", "--", "sh", "-c",
                                        script, "sh", target, str(index)], timeout=120, check=False)
            except Refusal:
                continue
            if result.returncode == 0 and len(result.stdout) == expected_chunk:
                chunk = result.stdout
                break
        require(chunk is not None,
                f"Could not read remote WAL chunk {index + 1}/{chunk_count} after bounded retries.")
        parts.append(chunk)

    data = b"".join(parts)
    after_size, after_hash = remote_wal_fingerprint(base, pod, target)
    local_hash = sha256_bytes(data)
    require(len(data) == expected_bytes and (before_size, before_hash) == (after_size, after_hash)
            and local_hash == before_hash,
            "Remote WAL segment changed during chunked capture or failed local hash verification.")
    return data


def history_names(base: list[str], pod: str, datadir: str) -> list[str]:
    script = 'for f in "$1"/pg_wal/*.history; do [ -f "$f" ] && printf "%s\\n" "${f##*/}"; done'
    result = kubectl(base, ["exec", "-n", NS, pod, "-c", "postgres", "--", "sh", "-c",
                            script, "sh", datadir])
    try:
        names = [line for line in result.stdout.decode("ascii").splitlines() if line]
    except UnicodeDecodeError as exc:
        raise Refusal("PostgreSQL history file listing was not ASCII.") from exc
    require(all(HISTORY_RE.fullmatch(name) for name in names), "PostgreSQL returned an unsafe timeline history filename.")
    require(len(names) == len(set(names)), "PostgreSQL returned duplicate timeline history names.")
    return sorted(names)


def wal_segment_size_bytes(value: Any) -> int:
    match = re.fullmatch(r"([0-9]+)([kMGT]B)", str(value or ""))
    require(match is not None, "PostgreSQL returned an unsupported WAL segment size.")
    amount = int(match.group(1))
    multiplier = {"kB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}[match.group(2)]
    size = amount * multiplier
    require(size in (16 * 1024 * 1024, 32 * 1024 * 1024, 64 * 1024 * 1024),
            "Unexpected WAL segment size; refusing a large local copy.")
    return size


def lsn_minus_one(lsn: Any) -> str | None:
    if lsn is None:
        return None
    match = re.fullmatch(r"([0-9A-Fa-f]{1,8})/([0-9A-Fa-f]{1,8})", str(lsn))
    require(match is not None, "PostgreSQL returned an invalid LSN for WAL filename calculation.")
    location = ((int(match.group(1), 16) << 32) | int(match.group(2), 16)) - 1
    if location < 0:
        return None
    return f"{location >> 32:X}/{location & 0xFFFFFFFF:X}"


def wal_filename(timeline: Any, lsn: Any, segment_size: Any) -> str | None:
    """Calculate a WAL filename without calling recovery-prohibited SQL helpers."""
    if lsn is None:
        return None
    require(isinstance(timeline, int) and 0 < timeline <= 0xFFFFFFFF,
            "PostgreSQL returned an invalid WAL timeline for filename calculation.")
    match = re.fullmatch(r"([0-9A-Fa-f]{1,8})/([0-9A-Fa-f]{1,8})", str(lsn))
    require(match is not None, "PostgreSQL returned an invalid LSN for WAL filename calculation.")
    location = (int(match.group(1), 16) << 32) | int(match.group(2), 16)
    size = wal_segment_size_bytes(segment_size)
    # PostgreSQL's pg_walfile_name() assigns an exact segment boundary to
    # the previous segment (XLByteToPrevSeg), so mirror that behavior here.
    segment_number = (location - 1) // size if location else 0
    segments_per_log = 0x100000000 // size
    log_number, segment_in_log = divmod(segment_number, segments_per_log)
    require(log_number <= 0xFFFFFFFF, "LSN exceeds the WAL filename range.")
    return f"{timeline:08X}{log_number:08X}{segment_in_log:08X}"


def capture_stable_wal(base: list[str], pod: str, role_root: Path, role: str,
                       before: dict[str, Any], max_attempts: int = 3) -> dict[str, Any]:
    wal_name = before.get("lastWalSegment")
    if not wal_name:
        return {"status": "unavailable", "reason": "no-replay-or-flush-lsn", "file": None}
    require(bool(WAL_RE.fullmatch(wal_name)), "Unsafe WAL filename for local metadata capture.")
    expected_size = wal_segment_size_bytes(before.get("walSegmentSize"))
    attempts = []
    for attempt in range(1, max_attempts + 1):
        current_before = sql_snapshot(base, pod)
        if current_before != before:
            before = current_before
            wal_name = before.get("lastWalSegment")
            if not wal_name or not WAL_RE.fullmatch(wal_name):
                return {"status": "unavailable", "reason": "wal-position-changed-to-unavailable", "file": None}
            expected_size = wal_segment_size_bytes(before.get("walSegmentSize"))
        try:
            data = remote_read_wal_segment(base, pod, str(before["dataDirectory"]), wal_name,
                                           expected_size)
        except Refusal as exc:
            attempts.append({"attempt": attempt, "result": "read-error", "errorType": type(exc).__name__})
            continue
        current_after = sql_snapshot(base, pod)
        stable_fields = ("systemIdentifier", "currentFlushLsn", "replayLsn", "receiveLsn", "currentWalFile", "lastWalSegment")
        stable = all(current_before.get(field) == current_after.get(field) for field in stable_fields)
        if data is not None and len(data) == expected_size and stable:
            record = write_private(role_root, f"wal/{wal_name}", data, max_bytes=64 * 1024 * 1024)
            return {"status": "captured", "attempts": attempt, "before": current_before,
                    "after": current_after, "file": record}
        attempts.append({"attempt": attempt, "result": "unstable-or-incomplete",
                         "bytes": len(data) if data is not None else None,
                         "stablePositions": stable})
        before = current_after
        wal_name = before.get("lastWalSegment")
        if not wal_name:
            break
        expected_size = wal_segment_size_bytes(before.get("walSegmentSize"))
    return {"status": "not-captured", "reason": "position-did-not-stabilize-or-segment-unavailable",
            "attempts": attempts, "file": None}


def role_capture(base: list[str], pod_obj: dict[str, Any], role: str, stage_root: Path) -> dict[str, Any]:
    metadata = pod_obj.get("metadata", {})
    pod = metadata.get("name", "")
    uid = metadata.get("uid", "")
    require(bool(POD_RE.fullmatch(pod)) and bool(uid), f"Invalid {role} pod identity.")
    role_root = stage_root / role
    role_root.mkdir(mode=0o700, parents=True)
    os.chmod(role_root, 0o700)
    first = sql_snapshot(base, pod)
    datadir = str(first["dataDirectory"])

    control_text = kubectl(base, ["exec", "-n", NS, pod, "-c", "postgres", "--", "sh", "-c",
                                  'pg_controldata "$1"', "sh", datadir], timeout=120).stdout
    require(0 < len(control_text) <= 128 * 1024, "pg_controldata output is empty or unexpectedly large.")
    control_record = write_private(role_root, "pg_controldata.txt", control_text, 128 * 1024)
    try:
        pg_control = remote_read(base, pod, datadir, "global/pg_control", max_bytes=8192, missing_ok=True)
        pg_control_status = "absent" if pg_control is None else "present"
    except Refusal:
        pg_control = None
        pg_control_status = "unreadable"
    if pg_control is not None:
        require(len(pg_control) == 8192, "pg_control binary size differs from PostgreSQL's 8-KiB control file.")
        pg_control_record = write_private(role_root, "global/pg_control", pg_control, 8192)
    else:
        pg_control_record = {"path": "global/pg_control", "status": pg_control_status, "bytes": 0, "sha256": None}

    files: list[dict[str, Any]] = [control_record]
    if pg_control_record.get("sha256"):
        files.append(pg_control_record)
    history = []
    for name in history_names(base, pod, datadir):
        content = remote_read(base, pod, datadir, f"pg_wal/{name}", max_bytes=1024 * 1024)
        assert content is not None
        item = write_private(role_root, f"history/{name}", content, 1024 * 1024)
        history.append(item)
        files.append(item)

    optional = {}
    for name in ("backup_label", "tablespace_map"):
        try:
            content = remote_read(base, pod, datadir, name, max_bytes=1024 * 1024, missing_ok=True)
            optional_status = "absent" if content is None else "present"
        except Refusal:
            content = None
            optional_status = "unreadable"
        if content is None:
            optional[name] = {"status": optional_status, "path": name, "bytes": 0, "sha256": None}
        else:
            item = write_private(role_root, name, content, 1024 * 1024)
            optional[name] = {"status": "present", **item}
            files.append(item)

    wal = capture_stable_wal(base, pod, role_root, role, first)
    if wal.get("file"):
        files.append(wal["file"])
    final = wal.get("after") or sql_snapshot(base, pod)
    metadata_record = {
        "version": 1, "role": role, "pod": pod, "podUid": uid,
        "container": "postgres", "systemIdentifier": final["systemIdentifier"],
        "sql": final, "pgControlOutput": control_record,
        "pgControlBinary": pg_control_record, "timelineHistoryFiles": history,
        "optionalRecoveryFiles": optional, "stableWalCapture": wal,
        "files": files,
    }
    encoded = (json.dumps(metadata_record, indent=2, sort_keys=True) + "\n").encode()
    meta_file = write_private(role_root, "metadata.json", encoded, 512 * 1024)
    return {"role": role, "pod": pod, "podUid": uid, "systemIdentifier": final["systemIdentifier"],
            "checkpointTimeline": final.get("checkpointTimeline"),
            "currentWalFile": final.get("currentWalFile"),
            "timelineHex": final.get("currentWalFile", "")[:8] if final.get("currentWalFile") else None,
            "isInRecovery": final["isInRecovery"], "currentFlushLsn": final.get("currentFlushLsn"),
            "receiveLsn": final.get("receiveLsn"), "replayLsn": final.get("replayLsn"),
            "checkpointLsn": final.get("checkpointLsn"), "lastWalSegment": final.get("lastWalSegment"),
            "appClientSessions": final.get("appClientSessions"),
            "vacuumClientSessions": final.get("vacuumClientSessions"),
            "pgControlBinary": pg_control_record,
            "historyFiles": history, "optionalRecoveryFiles": optional,
            "stableWalCapture": {"status": wal["status"], "file": wal.get("file")},
            "files": files + [meta_file]}


def validate_stack(stack: str) -> dict[str, Any]:
    require(stack in STACKS, "Select the owned sigterm or control stack.")
    stack_dir = EXPERIMENT / stack
    sd = stack_dir / ".state"
    state = json.loads((sd / "state.json").read_text())
    pair_path = HERE / "source-pair.json"
    proof_path = HERE / "source-pair-proof.json"
    pair = json.loads(pair_path.read_text())
    proof = json.loads(proof_path.read_text())
    token = state.get("ownerToken", "")
    require(bool(TOKEN_RE.fullmatch(token)) and state.get("status") == "ready", "Stack state is not ready or owner token is invalid.")
    require(state.get("resourceGroup") == f"rg-vacuum-only-{token}"
            and state.get("nodeResourceGroup") == f"rg-vacuum-nodes-{token}",
            "Stack resource group names do not match its exact owner token.")
    require(not sd.is_symlink(), "Stack state directory is a symlink.")
    require(state.get("subscriptionId") == pair.get("subscriptionId")
            and state.get("location", "").casefold() == pair.get("region", "").casefold() == "eastus",
            "Stack and pinned source DATA/WAL pair are not on the expected subscription/region.")
    require(proof.get("provenance", {}).get("copiedPairSha256") == sha256_file(pair_path),
            "Pinned source-pair proof hash differs from the manifest.")
    prepare = json.loads((sd / "prepare.json").read_text())
    require(prepare.get("status") == "complete" and prepare.get("ownerToken") == token
            and prepare.get("subscriptionId") == state.get("subscriptionId"),
            "Preparation receipt does not match this stack.")
    require(prepare.get("sourcePair", {}).get("dataSnapshotId", "").casefold() == pair["data"]["id"].casefold()
            and prepare.get("sourcePair", {}).get("walSnapshotId", "").casefold() == pair["wal"]["id"].casefold(),
            "Preparation receipt no longer references the pinned source snapshot pair.")
    test_path = sd / "test-state.json"
    require(test_path.is_file(), "Measurement terminal-state receipt is missing; stop the client/test before metadata capture.")
    test = json.loads(test_path.read_text())
    require(test.get("phase") in ("complete", "failed") and test.get("vacuumLeftRunning") is not True,
            "Measurement is still running or reports a detached VACUUM.")
    kubeconfig = Path(state.get("kubeconfigPath", ""))
    require(kubeconfig == sd / "kubeconfig" and kubeconfig.is_file() and not kubeconfig.is_symlink(),
            "Stack kubeconfig is outside the exact private state path.")
    base = ["kubectl", "--kubeconfig", str(kubeconfig)]
    return {"stack": stack, "stackDir": stack_dir, "stateDir": sd, "state": state,
            "pair": pair, "pairSha256": sha256_file(pair_path), "token": token,
            "base": base, "testState": test}


def safe_public(ctx: dict[str, Any], roles: list[dict[str, Any]], timeline: int) -> dict[str, Any]:
    public_roles = []
    for role in roles:
        public_roles.append({
            "role": role["role"], "systemIdentifier": role["systemIdentifier"],
            "checkpointTimeline": role["checkpointTimeline"], "timelineHex": role["timelineHex"],
            "isInRecovery": role["isInRecovery"], "currentFlushLsn": role["currentFlushLsn"],
            "appClientSessions": role["appClientSessions"], "vacuumClientSessions": role["vacuumClientSessions"],
            "receiveLsn": role["receiveLsn"], "replayLsn": role["replayLsn"],
            "checkpointLsn": role["checkpointLsn"],
            "pgControlBinary": {key: role["pgControlBinary"].get(key) for key in ("path", "status", "bytes", "sha256")},
            "files": [{key: f.get(key) for key in ("path", "status", "bytes", "sha256")} for f in role["files"]],
            "optionalRecoveryFiles": {name: {key: record.get(key) for key in ("status", "bytes", "sha256")}
                                      for name, record in role["optionalRecoveryFiles"].items()},
            "stableWalCapture": {"status": role["stableWalCapture"]["status"],
                                 "file": ({key: role["stableWalCapture"]["file"].get(key)
                                           for key in ("path", "status", "bytes", "sha256")}
                                          if role["stableWalCapture"].get("file") else None)},
        })
    return {
        "version": 1, "status": "complete", "capturedAtUtc": TIMESTAMP(),
        "stack": ctx["stack"], "systemIdentifier": roles[0]["systemIdentifier"],
        "timeline": timeline, "roles": public_roles,
        "sourcePairProofSha256": ctx["pairSha256"],
        "sourceSnapshotsPreserved": True,
        "redactions": {"ownerToken": True, "subscriptionId": True, "resourceGroup": True,
                       "sourceSnapshotIds": True, "podUids": True, "privatePaths": True},
    }


def capture(ctx: dict[str, Any]) -> dict[str, Any]:
    state_dir = ctx["stateDir"]
    dest = state_dir / "recovery-metadata"
    receipt_path = state_dir / "recovery-metadata.json"
    require(not dest.exists() and not receipt_path.exists(),
            "Recovery metadata already exists; refusing to overwrite evidence.")
    state = ctx["state"]
    cluster = json.loads(kubectl(ctx["base"], ["get", "cluster", CLUSTER, "-n", NS, "-o", "json"]).stdout)
    cluster_meta = cluster.get("metadata", {})
    require(cluster_meta.get("labels", {}).get("repro.stack.token") == ctx["token"],
            "CNPG Cluster ownership label does not match the stack token.")
    annotations = cluster_meta.get("annotations", {})
    require(annotations.get("vacuum-only-repro/source-data-snapshot-id", "").casefold()
            == ctx["pair"]["data"]["id"].casefold()
            and annotations.get("vacuum-only-repro/source-wal-snapshot-id", "").casefold()
            == ctx["pair"]["wal"]["id"].casefold(),
            "Live Cluster no longer references the exact pinned source snapshots.")
    pods_result = kubectl(ctx["base"], ["get", "pods", "-n", NS, "-l", f"cnpg.io/cluster={CLUSTER}", "-o", "json"])
    pods_json = json.loads(pods_result.stdout)
    pods = pods_json.get("items", [])
    by_role: dict[str, list[dict[str, Any]]] = {"primary": [], "replica": []}
    for pod in pods:
        labels = pod.get("metadata", {}).get("labels", {})
        role = labels.get("cnpg.io/instanceRole")
        if role in by_role and pod.get("status", {}).get("phase") == "Running":
            require(labels.get("cnpg.io/cluster") == CLUSTER, "Pod cluster label changed during selection.")
            by_role[role].append(pod)
    require(len(by_role["primary"]) == 1 and len(by_role["replica"]) == 1,
            "Expected exactly one Running primary and replica for recovery metadata capture.")

    stage = Path(tempfile.mkdtemp(prefix=".recovery-metadata.partial.", dir=state_dir))
    os.chmod(stage, 0o700)
    try:
        roles = [role_capture(ctx["base"], by_role[role][0], role, stage)
                 for role in ("primary", "replica")]
        require(roles[0]["systemIdentifier"] == roles[1]["systemIdentifier"]
                == ctx["pair"]["fixture"]["systemIdentifier"],
                "Primary, replica, and pinned source-pair system identifiers do not match.")
        require(roles[0]["isInRecovery"] is False and roles[1]["isInRecovery"] is True,
                "Primary/replica recovery states are not correct.")
        timeline_hex = roles[0].get("timelineHex")
        require(timeline_hex and timeline_hex == roles[1].get("timelineHex"),
                "Primary and replica report different or unavailable current WAL timelines.")
        timeline = int(timeline_hex, 16)
        history_names_found = {file["path"].split("/")[-1]
                               for role in roles for file in role["historyFiles"]}
        if timeline > 1:
            require(f"{timeline_hex}.history" in history_names_found,
                    "Current timeline is greater than 1 but its matching .history file was not captured.")

        private = {
            "version": 1, "status": "complete", "capturedAtUtc": TIMESTAMP(),
            "stack": ctx["stack"], "ownerToken": ctx["token"],
            "subscriptionId": state["subscriptionId"], "resourceGroup": state["resourceGroup"],
            "sourcePairProofSha256": ctx["pairSha256"],
            "sourceSnapshotIds": {"data": ctx["pair"]["data"]["id"], "wal": ctx["pair"]["wal"]["id"]},
            "systemIdentifier": roles[0]["systemIdentifier"], "timeline": timeline,
            "historyFileRequired": timeline > 1,
            "roles": roles,
            "checks": {"primaryReplicaSystemIdentifiersMatch": True,
                       "sourcePairSystemIdentifierMatches": True,
                       "primaryNotInRecovery": True, "replicaInRecovery": True,
                       "clientAndVacuumStopped": all(role["appClientSessions"] == 0 and role["vacuumClientSessions"] == 0 for role in roles),
                       "matchingTimelineHistoryRetained": timeline <= 1 or f"{timeline_hex}.history" in history_names_found,
                       "allRawFilesPrivate": True, "azureMutations": 0, "postgresqlMutations": 0},
        }
        atomic_json(stage / "receipt.json", private)
        os.replace(stage, dest)
        public_path = EXPERIMENT / "results" / "diagnosis" / "public" / f"recovery-metadata-{ctx['stack']}.json"
        public = safe_public(ctx, roles, timeline)
        atomic_json(public_path, public, mode=0o644)
        public_hash = sha256_file(public_path)
        private_receipt_hash = sha256_file(dest / "receipt.json")
        receipt = {"version": 1, "status": "complete", "capturedAtUtc": private["capturedAtUtc"],
                   "ownerToken": ctx["token"], "stack": ctx["stack"],
                   "sourcePairProofSha256": ctx["pairSha256"],
                   "privateDirectory": ".state/recovery-metadata",
                   "privateReceipt": ".state/recovery-metadata/receipt.json",
                   "privateReceiptSha256": private_receipt_hash,
                   "publicReceiptPath": str(public_path.relative_to(EXPERIMENT)),
                   "publicReceiptSha256": public_hash, "systemIdentifier": private["systemIdentifier"],
                   "timeline": timeline, "historyFileRequired": timeline > 1,
                   "historyFilePresent": not (timeline > 1) or f"{timeline_hex}.history" in history_names_found,
                   "roles": [{"role": role["role"], "historyFiles": role["historyFiles"],
                              "stableWalCaptureStatus": role["stableWalCapture"]["status"],
                              "podUid": role["podUid"]} for role in roles],
                   "checks": private["checks"]}
        atomic_json(receipt_path, receipt)
        return receipt
    except BaseException:
        if stage.exists():
            # Preserve failed raw capture attempts privately for diagnosis.
            failed = state_dir / f"recovery-metadata-failed-{int(time.time())}"
            if not failed.exists():
                os.replace(stage, failed)
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stack", required=True, choices=STACKS)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ctx = validate_stack(args.stack)
    ctx["stack"] = args.stack
    lock_path = ctx["stateDir"] / "recovery-metadata.lock"
    with lock_path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise Refusal("Recovery metadata capture is already running for this stack.") from exc
        result = capture(ctx)
    print(json.dumps({"status": result["status"], "stack": result["stack"],
                      "timeline": result["timeline"], "historyFilePresent": result["historyFilePresent"],
                      "publicReceipt": result["publicReceiptPath"], "secretsPrinted": False}, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Refusal as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
