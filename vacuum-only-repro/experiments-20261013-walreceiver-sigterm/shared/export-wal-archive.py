#!/usr/bin/env python3
"""Export one owned stack's archived WAL prefix before infrastructure cleanup.

The command is plan-only unless ``--apply`` is present. It reads the stack's
Azure account/container and downloads only
``cnpg-wal-archive/<owner-token>/<server-name>/``. Downloads are conditional on
the ETag from the paginated inventory. Payloads go under the stack's ignored
``.state/archived-wal/run`` directory; a redacted inventory goes under the
experiment's ``results/diagnosis/public`` directory. It never changes Azure.
"""

from __future__ import annotations

import argparse
import base64
import fcntl
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from remote_chunk_download import download_verified_file

HERE = Path(__file__).resolve().parent
EXPERIMENT = HERE.parent
STACK_NAMES = ("sigterm", "control")
NAMESPACE = "repro"
CLUSTER = "hdd-pg"
TOKEN_RE = re.compile(r"^[a-f0-9]{32}$")
HASH_RE = re.compile(r"^[a-f0-9]{64}$")
WAL_RE = re.compile(r"^[0-9A-F]{24}$")
ACCOUNT_RE = re.compile(r"^cnpgwal[a-f0-9]{17}$")
CONTAINER_RE = re.compile(r"^wal-[a-f0-9]{12}$")
DEFAULT_WORKERS = 12
MAX_WORKERS = 16
MAX_INVENTORY_PASSES = 8
REMOTE_CHUNK_BYTES = 2 * 1024 * 1024
REMOTE_BATCH_OBJECTS = 100
REMOTE_MAX_WORKERS = 4

# Connection credentials are JSON stdin only. This static program, the stack
# token, and object metadata may appear in argv/stdout; the connection string
# never does. Each batch is staged beneath a unique token-owned /controller/tmp
# directory, and each original object's ETag/size/SHA-256 is carried in the tar.
REMOTE_STAGE_SCRIPT = r'''import gzip, hashlib, importlib.util, io, json, os, re, shutil, sys, tarfile, time

class GuardFailure(RuntimeError):
    def __init__(self, line):
        self.guard_line = line
        super().__init__("remote WAL export check failed")

def fail(): raise GuardFailure(sys._getframe(1).f_lineno)
def canonical_etag(value):
    text = str(value)
    return text[1:-1] if len(text) >= 2 and text[0] == '"' and text[-1] == '"' else text
def azure_etag(value):
    text = canonical_etag(value)
    if not text or '"' in text: fail()
    return '"' + text + '"'

try:
    request = json.load(sys.stdin)
    op = request.get("operation")
    token = request.get("ownerToken", "")
    stack = request.get("stack", "")
    if not re.fullmatch(r"[a-f0-9]{32}", token) or stack not in ("sigterm", "control"):
        fail()
    scratch = "/controller/tmp"
    if os.path.realpath(scratch) != scratch or not os.path.isdir(scratch) or not os.access(scratch, os.W_OK):
        fail()
    if op == "preflight":
        if importlib.util.find_spec("azure.storage.blob") is None or shutil.which("gzip") is None:
            fail()
        usage = shutil.disk_usage(scratch)
        print(json.dumps({"status":"ready", "scratchFreeBytes":usage.free,
                          "azureBlobSdk":True, "gzip":True}))
        sys.exit(0)
    stage_id = request.get("stageId", "")
    if not re.fullmatch(r"[a-f0-9]{32}", stage_id):
        fail()
    stage = os.path.join(scratch, f"wal-export-{token}-{stack}-{stage_id}")
    if os.path.dirname(stage) != scratch:
        fail()
    owner = {"version":1, "ownerToken":token, "stack":stack,
             "purpose":"post-measurement-wal-export", "stageId":stage_id}
    marker_path = os.path.join(stage, ".owner.json")
    if op == "cleanup":
        if not os.path.lexists(stage):
            print(json.dumps({"status":"absent", "stageId":stage_id}))
            sys.exit(0)
        with open(marker_path, "r", encoding="utf-8") as marker:
            if json.load(marker) != owner: fail()
        shutil.rmtree(stage)
        print(json.dumps({"status":"removed", "stageId":stage_id}))
        sys.exit(0)
    if op != "stage" or os.path.lexists(stage):
        fail()
    created = False
    try:
        os.mkdir(stage, 0o700); created = True
        fd = os.open(marker_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as marker:
            json.dump(owner, marker, sort_keys=True)
            marker.flush(); os.fsync(marker.fileno())
        items = request.get("items")
        if not isinstance(items, list) or not items or len(items) > 100:
            fail()
        connection = request.get("connectionString", "")
        if not isinstance(connection, str) or "AccountName=" not in connection or "AccountKey=" not in connection:
            fail()
        from azure.core import MatchConditions
        from azure.storage.blob import BlobServiceClient
        service = BlobServiceClient.from_connection_string(connection, max_single_get_size=32*1024*1024,
            max_chunk_get_size=16*1024*1024, connection_timeout=30, read_timeout=120)
        if service.account_name != request.get("accountName"):
            fail()
        container = service.get_container_client(request.get("containerName", ""))
        records = []; raw_root = os.path.join(stage, "raw"); os.mkdir(raw_root, 0o700)
        prefix = f"cnpg-wal-archive/{token}/hdd-pg/"
        seen = set()
        required_bytes = sum(int(item.get("bytes", 0)) for item in items)
        if shutil.disk_usage(scratch).free < required_bytes * 2 + 64*1024*1024:
            fail()
        for item in items:
            name = item.get("name", ""); etag = item.get("etag", ""); size = item.get("bytes")
            relative = item.get("relativeName", "")
            match = re.fullmatch(r"wals/([0-9A-F]{16})/([0-9A-F]{24})", relative)
            if (not name.startswith(prefix) or name != prefix + relative or name in seen or not match or
                    relative != f"wals/{match.group(2)[:16]}/{match.group(2)}" or
                    match.group(1) != match.group(2)[:16] or not etag or not isinstance(size, int) or size <= 0):
                fail()
            seen.add(name)
            blob = container.get_blob_client(name)
            before = blob.get_blob_properties()
            if canonical_etag(before.etag) != canonical_etag(etag) or int(before.size) != size:
                fail()
            target = os.path.join(raw_root, relative)
            os.makedirs(os.path.dirname(target), mode=0o700, exist_ok=True)
            digest = hashlib.sha256(); received = 0
            downloader = blob.download_blob(etag=azure_etag(etag), match_condition=MatchConditions.IfNotModified, max_concurrency=1)
            with open(target, "xb") as output:
                for block in downloader.chunks():
                    output.write(block); digest.update(block); received += len(block)
                output.flush(); os.fsync(output.fileno())
            after = blob.get_blob_properties()
            if received != size or canonical_etag(after.etag) != canonical_etag(etag) or int(after.size) != size:
                fail()
            records.append({"name":name, "relativeName":relative, "etag":etag,
                            "bytes":size, "sha256":digest.hexdigest()})
        records.sort(key=lambda row: row["name"])
        bundle = os.path.join(stage, "batch.tar.gz")
        manifest = {"version":1, "ownerToken":token, "stack":stack, "files":records}
        with tarfile.open(bundle, mode="w:gz", compresslevel=1) as archive:
            for row in records:
                path = os.path.join(raw_root, row["relativeName"])
                archive.add(path, arcname=row["relativeName"], recursive=False)
            raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
            info = tarfile.TarInfo("_wal-export-manifest.json"); info.size = len(raw); info.mode = 0o600
            info.mtime = int(time.time()); archive.addfile(info, io.BytesIO(raw))
        shutil.rmtree(raw_root)
        with open(bundle, "rb") as stream:
            digest = hashlib.sha256()
            for block in iter(lambda: stream.read(1024*1024), b""): digest.update(block)
        print(json.dumps({"status":"staged", "stageId":stage_id, "ownerToken":token,"stack":stack,
            "bundleName":"batch.tar.gz",
            "bundleBytes":os.path.getsize(bundle), "bundleSha256":digest.hexdigest(), "files":records}, sort_keys=True))
    except BaseException:
        if created:
            try:
                with open(marker_path, "r", encoding="utf-8") as marker:
                    if json.load(marker) == owner: shutil.rmtree(stage)
            except Exception: pass
        raise
except SystemExit:
    raise
except BaseException as exc:
    guard = ", guard line " + str(exc.guard_line) if isinstance(exc, GuardFailure) else ""
    print("remote WAL staging failed (" + type(exc).__name__ + guard + ")", file=sys.stderr)
    sys.exit(2)
'''

try:
    from azure.core import MatchConditions
    from azure.storage.blob import BlobServiceClient
except ImportError:  # plan and offline tests remain usable outside the workspace venv
    MatchConditions = None  # type: ignore[assignment,misc]
    BlobServiceClient = None  # type: ignore[assignment,misc]


class Refusal(RuntimeError):
    """A failed ownership, lineage, or export-completeness check."""


def require(ok: bool, message: str) -> None:
    if not ok:
        raise Refusal(message)


def lower(value: Any) -> str:
    return str(value or "").casefold().rstrip("/")


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise Refusal(f"Cannot read valid JSON metadata at {path}: {exc}") from exc


def atomic_json(path: Path, value: Any, mode: int = 0o600) -> str:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    require(not path.parent.is_symlink() and not path.is_symlink(), f"Refusing symlinked receipt path: {path}")
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
    return sha256_file(path)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def expected_storage(token: str) -> tuple[str, str]:
    require(bool(TOKEN_RE.fullmatch(token)), "Invalid stack owner token.")
    return f"cnpgwal{token[:17]}", f"wal-{token[:12]}"


def archive_prefix(token: str, server_name: str) -> str:
    require(bool(TOKEN_RE.fullmatch(token)), "Invalid stack owner token.")
    require(server_name == CLUSTER, "Archive server name differs from the expected CNPG Cluster.")
    return f"cnpg-wal-archive/{token}/{server_name}/"


def relative_blob_name(name: str, prefix: str) -> str:
    require(name.startswith(prefix), "Blob listing returned an object outside the exact stack/server prefix.")
    relative = name[len(prefix):]
    raw_parts = relative.split("/")
    posix = PurePosixPath(relative)
    require(relative.startswith("wals/") and not posix.is_absolute()
            and bool(posix.parts) and all(part not in ("", ".", "..") for part in raw_parts)
            and "\\" not in relative and "\x00" not in relative,
            "Archive prefix contains a non-WAL or unsafe object path; refusing to export it.")
    return relative


def canonical_etag(value: Any) -> str:
    """Compare Azure's opaque ETag independent of optional HTTP quotes.

    Keep the original listing representation in receipts; quote it only when
    passing it as an If-Match precondition to the Blob SDK.
    """
    text = str(value)
    if len(text) >= 2 and text[0] == '"' and text[-1] == '"':
        text = text[1:-1]
    return text


def etags_match(left: Any, right: Any) -> bool:
    return bool(canonical_etag(left)) and canonical_etag(left) == canonical_etag(right)


def azure_etag(value: Any) -> str:
    text = canonical_etag(value)
    require(bool(text) and '"' not in text, "Azure returned an invalid ETag representation.")
    return f'"{text}"'


def item_identity(item: dict[str, Any]) -> tuple[str, str, int, str]:
    return (item["name"], canonical_etag(item["etag"]), int(item["bytes"]), item["lastModified"])


def compare_monotonic(previous: list[dict[str, Any]], current: list[dict[str, Any]]) -> None:
    current_by_name = {item["name"]: item for item in current}
    require(len(current_by_name) == len(current), "Blob inventory contains duplicate object names.")
    for old in previous:
        fresh = current_by_name.get(old["name"])
        require(fresh is not None, "A previously inventoried WAL object disappeared during export.")
        require(item_identity(old) == item_identity(fresh),
                "A previously inventoried WAL object's ETag, size, or modified time changed during export.")


def local_file_for(root: Path, relative: str, *, create_parents: bool = False) -> Path:
    raw_parts = relative.split("/")
    posix = PurePosixPath(relative)
    require(not posix.is_absolute() and bool(posix.parts)
            and all(part not in ("", ".", "..") for part in raw_parts)
            and "\\" not in relative and "\x00" not in relative,
            "Unsafe relative archive file path.")
    if create_parents:
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
    require(not root.is_symlink() and (not root.exists() or root.is_dir()),
            "Refusing symlinked or non-directory local archive root.")
    current = root
    for part in posix.parts[:-1]:
        current = current / part
        if create_parents:
            current.mkdir(mode=0o700, exist_ok=True)
        if current.exists():
            require(not current.is_symlink() and current.is_dir(), "Local archive path contains a symlink or non-directory.")
    target = current / posix.parts[-1]
    require(not target.is_symlink(), "Refusing to use a symlink as a local WAL file.")
    return target


def verify_local_inventory(export_root: Path, objects: list[dict[str, Any]], total_bytes: int | None = None) -> None:
    expected: set[str] = set()
    counted = 0
    for item in objects:
        relative = item["localRelativePath"]
        expected.add(relative)
        target = local_file_for(export_root, relative)
        require(target.is_file(), f"Required exported WAL file is missing: {relative}")
        stat = target.stat()
        require(stat.st_size == int(item["bytes"]), f"Exported WAL byte count changed: {relative}")
        require(sha256_file(target) == item["sha256"], f"Exported WAL SHA-256 changed: {relative}")
        counted += stat.st_size
    actual: set[str] = set()
    if export_root.exists():
        for directory, dirs, files in os.walk(export_root, followlinks=False):
            base = Path(directory)
            for name in dirs:
                child = base / name
                require(not child.is_symlink(), "Export directory contains a symlink.")
            for name in files:
                child = base / name
                require(not child.is_symlink() and child.is_file(), "Export directory contains a non-regular file.")
                actual.add(str(child.relative_to(export_root)).replace(os.sep, "/"))
    require(actual == expected, "Local WAL archive directory contains missing or unmanifested files.")
    if total_bytes is not None:
        require(counted == total_bytes, "Exported WAL total byte count differs from its receipt.")


def validate_local(stack: str) -> dict[str, Any]:
    require(stack in STACK_NAMES, "Select the owned sigterm or control stack.")
    stack_dir = EXPERIMENT / stack
    state_dir = stack_dir / ".state"
    state = read_json(state_dir / "state.json")
    prepare = read_json(state_dir / "prepare.json")
    storage = read_json(state_dir / "storage.json")
    archive = read_json(state_dir / "wal-archive.json")
    smoke = read_json(state_dir / "wal-archive-smoke.json")
    test_state_path = state_dir / "test-state.json"
    test_state = read_json(test_state_path) if test_state_path.exists() else None

    token = state.get("ownerToken", "")
    account_name, container_name = expected_storage(token)
    pair_path = HERE / "source-pair.json"
    proof_path = HERE / "source-pair-proof.json"
    pair = read_json(pair_path)
    proof = read_json(proof_path)
    pair_sha = sha256_file(pair_path)
    expected_data = pair.get("data", {}).get("id")
    expected_wal = pair.get("wal", {}).get("id")
    require(state.get("status") == "ready" and state.get("subscriptionId") == pair.get("subscriptionId"),
            "Stack is not ready on the pinned source-pair subscription.")
    require(state.get("resourceGroup") == f"rg-vacuum-only-{token}"
            and state.get("nodeResourceGroup") == f"rg-vacuum-nodes-{token}"
            and lower(state.get("location")) == lower(pair.get("region")) == "eastus",
            "Stack resource-group ownership or region changed.")
    require(proof.get("provenance", {}).get("copiedPairSha256") == pair_sha,
            "Pinned source-pair proof hash changed.")
    require(prepare.get("status") == "complete" and prepare.get("ownerToken") == token
            and prepare.get("subscriptionId") == state.get("subscriptionId")
            and prepare.get("resourceGroup") == state.get("resourceGroup"),
            "Prepared source-pair restoration does not match this stack.")
    source = prepare.get("sourcePair", {})
    require(lower(source.get("dataSnapshotId")) == lower(expected_data)
            and lower(source.get("walSnapshotId")) == lower(expected_wal),
            "Preparation receipt no longer references the exact paired source snapshots.")
    permissions = prepare.get("sourceSnapshotRoleAssignments", {})
    require(permissions.get("status") == "not-created" and permissions.get("locksModified") is False
            and permissions.get("sourceLocksVerifiedUnmodified") is True,
            "Preparation did not preserve the source snapshot locks and permissions.")
    require(storage.get("version") == 1 and storage.get("status") == "ready"
            and storage.get("ownerToken") == token and storage.get("subscriptionId") == state.get("subscriptionId")
            and storage.get("resourceGroup") == state.get("resourceGroup")
            and storage.get("accountName") == account_name and storage.get("containerName") == container_name
            and storage.get("purpose") == "private-cnpg-wal-archive" and storage.get("publicAccess") is None,
            "Storage metadata does not prove exact ownership by this stack.")
    require(archive.get("version") == 1 and archive.get("status") == "ready"
            and archive.get("ownerToken") == token and archive.get("subscriptionId") == state.get("subscriptionId")
            and archive.get("resourceGroup") == state.get("resourceGroup")
            and archive.get("accountName") == account_name and archive.get("accountId") == storage.get("accountId")
            and archive.get("containerName") == container_name and archive.get("serverName") == CLUSTER
            and archive.get("connectionStringPersistedLocally") is False,
            "WAL archive receipt is incomplete or outside this stack's ownership scope.")
    destination = f"https://{account_name}.blob.core.windows.net/{container_name}/cnpg-wal-archive/{token}/"
    require(archive.get("destinationPath") == destination,
            "Archive destination is not the exact token-scoped private account/container prefix.")
    archive_sources = archive.get("sourceSnapshotIds", {})
    require(lower(archive_sources.get("data")) == lower(expected_data)
            and lower(archive_sources.get("wal")) == lower(expected_wal),
            "Archive receipt lost the source DATA/WAL snapshot lineage.")
    locks = archive.get("sourceLockVerification", {})
    require(lower(locks.get("data", {}).get("snapshotId")) == lower(expected_data)
            and lower(locks.get("wal", {}).get("snapshotId")) == lower(expected_wal)
            and locks.get("data", {}).get("level") == "CanNotDelete"
            and locks.get("wal", {}).get("level") == "CanNotDelete"
            and locks.get("resourceGroup", {}).get("level") == "CanNotDelete",
            "Archive receipt does not prove the retained source locks were preserved.")
    require(smoke.get("status") == "complete" and smoke.get("ownerToken") == token
            and smoke.get("stack") == stack and smoke.get("serverName") == CLUSTER
            and smoke.get("restorePodRole") == "replica"
            and smoke.get("checks", {}).get("allThreeHashesMatch") is True
            and smoke.get("checks", {}).get("sourceSnapshotsReadOnly") is True
            and smoke.get("checks", {}).get("snapshotLineagePreserved") is True,
            "WAL archive/replica restore smoke receipt is incomplete.")
    smoke_object = smoke.get("archiveObject", "")
    prefix = archive_prefix(token, CLUSTER)
    smoke_relative = relative_blob_name(smoke_object, prefix)
    wal_name = smoke.get("walName", "")
    require(bool(WAL_RE.fullmatch(wal_name))
            and smoke_relative == f"wals/{wal_name[:16]}/{wal_name}"
            and smoke.get("retainedLocalWALFile") == f"archived-wal/{wal_name}",
            "Smoke WAL object or retained local path is not the exact safe Barman segment path.")
    smoke_hash = smoke.get("hashes", {}).get("independentAzureDownloadSha256", "")
    require(HASH_RE.fullmatch(smoke_hash) is not None and smoke.get("bytes", 0) == 16 * 1024 * 1024,
            "Smoke WAL byte count or Azure SHA-256 is invalid.")
    smoke_local = state_dir / smoke.get("retainedLocalWALFile", "")
    require(smoke_local.is_file() and not smoke_local.is_symlink()
            and smoke_local.stat().st_size == smoke["bytes"]
            and sha256_file(smoke_local) == smoke_hash
            and smoke.get("retainedLocalWALSha256") == smoke_hash,
            "The independently verified smoke WAL is not retained locally with its recorded hash.")
    require(test_state is None or test_state.get("phase") in ("complete", "failed"),
            "Measurement is still running; refusing to freeze/export its WAL prefix.")
    require(test_state is None or test_state.get("vacuumLeftRunning") is not True,
            "Measurement reports a detached VACUUM still running; export after it stops.")
    if test_state and test_state.get("phase") == "complete":
        require(test_state.get("testComplete") is True,
                "Measurement phase says complete but its testComplete flag is false.")

    return {
        "stack": stack,
        "stackDir": stack_dir,
        "stateDir": state_dir,
        "state": state,
        "prepare": prepare,
        "storage": storage,
        "archive": archive,
        "smoke": smoke,
        "pair": pair,
        "pairSha256": pair_sha,
        "ownerToken": token,
        "subscriptionId": state["subscriptionId"],
        "resourceGroup": state["resourceGroup"],
        "location": state["location"],
        "accountName": account_name,
        "accountId": storage["accountId"],
        "containerName": container_name,
        "serverName": CLUSTER,
        "destinationPath": destination,
        "prefix": prefix,
        "smokeObject": smoke_object,
        "smokeRelative": smoke_relative,
        "smokeSha256": smoke_hash,
        "smokeBytes": int(smoke["bytes"]),
        "runDir": state_dir / "archived-wal" / "run",
        "pendingPath": state_dir / "wal-archive-export-pending.json",
        "progressDir": state_dir / "wal-archive-export-progress",
        "privateReceiptPath": state_dir / "wal-archive-export.json",
        "publicReceiptPath": EXPERIMENT / "results" / "diagnosis" / "public" / f"wal-archive-export-{stack}.json",
    }


def build_plan(ctx: dict[str, Any], workers: int, backend: str = "host",
               remote_workers: int = 2, remote_batch_objects: int = REMOTE_BATCH_OBJECTS) -> dict[str, Any]:
    return {
        "mode": "plan-only",
        "stack": ctx["stack"],
        "resourceGroup": ctx["resourceGroup"],
        "storageAccount": ctx["accountName"],
        "container": ctx["containerName"],
        "serverName": ctx["serverName"],
        "blobPrefix": ctx["prefix"],
        "localPayloadDirectory": str(ctx["runDir"]),
        "publicInventoryPath": str(ctx["publicReceiptPath"]),
        "workers": workers,
        "maxConcurrentDownloads": workers,
        "transferBackend":backend,
        "remoteWorkers":remote_workers if backend=="in-cluster" else 0,
        "remoteBatchObjects":remote_batch_objects if backend=="in-cluster" else None,
        "remoteChunkBytes":REMOTE_CHUNK_BYTES if backend=="in-cluster" else None,
        "azureMutations": 0,
        "sourceSnapshotMutations": 0,
        "secretsPrintedOrPersisted": False,
    }


def az_run(ctx: dict[str, Any], args: list[str], *, timeout: int = 180) -> str:
    command = ["az", *args, "--subscription", ctx["subscriptionId"]]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired as exc:
        raise Refusal(f"Azure CLI read timed out ({args[0]} {args[1] if len(args) > 1 else ''}).") from exc
    if result.returncode != 0:
        raise Refusal(f"Azure CLI read failed ({args[0]} {args[1] if len(args) > 1 else ''}); details suppressed.")
    return result.stdout.strip()


def az_json(ctx: dict[str, Any], args: list[str]) -> Any:
    raw = az_run(ctx, [*args, "-o", "json"])
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise Refusal(f"Azure CLI returned invalid JSON for {args[0]}.") from exc


def verify_live_owner(ctx: dict[str, Any]) -> tuple[Any, str]:
    if BlobServiceClient is None or MatchConditions is None:
        raise Refusal("Azure Blob SDK is missing; run `.venv/bin/pip install -r shared/requirements-wal-archive-export.txt`.")
    active = az_run(ctx, ["account", "show", "--query", "id", "-o", "tsv"], timeout=60)
    require(active.casefold() == ctx["subscriptionId"].casefold(), "Active Azure subscription differs from this stack.")
    rg = az_json(ctx, ["group", "show", "-n", ctx["resourceGroup"]])
    require(rg.get("name", "").casefold() == ctx["resourceGroup"].casefold()
            and (rg.get("tags") or {}).get("reprotoken") == ctx["ownerToken"],
            "Stack resource group owner tag does not match the local token.")
    account = az_json(ctx, ["storage", "account", "show", "-g", ctx["resourceGroup"], "-n", ctx["accountName"]])
    tags = account.get("tags") or {}
    require(lower(account.get("id")) == lower(ctx["accountId"])
            and account.get("name", "").casefold() == ctx["accountName"]
            and account.get("resourceGroup", "").casefold() == ctx["resourceGroup"].casefold()
            and account.get("location", "").casefold() == ctx["location"].casefold()
            and account.get("provisioningState") == "Succeeded"
            and account.get("kind") == "StorageV2"
            and (account.get("sku") or {}).get("name") == "Standard_LRS"
            and account.get("allowBlobPublicAccess") is False
            and account.get("enableHttpsTrafficOnly") is True
            and account.get("minimumTlsVersion") == "TLS1_2"
            and tags.get("reprotoken") == ctx["ownerToken"]
            and tags.get("reproPurpose") == "private-cnpg-wal-archive"
            and tags.get("reproStack") == "walreceiver-sigterm",
            "Live Storage account identity/security/owner tags differ from this stack's receipt.")

    connection = az_run(ctx, ["storage", "account", "show-connection-string", "-g", ctx["resourceGroup"],
                              "-n", ctx["accountName"], "--query", "connectionString", "-o", "tsv"], timeout=120)
    require("AccountName=" in connection and "AccountKey=" in connection,
            "Azure CLI did not return the expected Storage connection string; value suppressed.")
    try:
        service = BlobServiceClient.from_connection_string(
            connection, max_single_get_size=2 * 1024 * 1024,
            max_chunk_get_size=2 * 1024 * 1024, connection_timeout=30, read_timeout=60)
        container = service.get_container_client(ctx["containerName"])
        properties = container.get_container_properties()
    except Exception as exc:
        raise Refusal(f"Cannot verify the owned Blob container ({type(exc).__name__}); details suppressed.") from exc
    metadata = {str(k).casefold(): str(v) for k, v in (properties.metadata or {}).items()}
    public_access = getattr(properties, "public_access", None)
    require(properties.name == ctx["containerName"]
            and metadata.get("reprotoken") == ctx["ownerToken"]
            and metadata.get("reprostack") == "walreceiver-sigterm"
            and public_access is None,
            "Blob container owner metadata or private-access setting did not verify.")
    return container, connection


def list_inventory(container: Any, ctx: dict[str, Any]) -> list[dict[str, Any]]:
    inventory: list[dict[str, Any]] = []
    try:
        # Azure's list_blobs owns the per-page setting; ItemPaged.by_page only
        # accepts a continuation token in azure-storage-blob 12.31.0.
        pages = container.list_blobs(name_starts_with=ctx["prefix"], results_per_page=500).by_page()
        for page in pages:
            for blob in page:
                relative = relative_blob_name(blob.name, ctx["prefix"])
                etag = str(getattr(blob, "etag", "") or "")
                size = getattr(blob, "size", None)
                last_modified = getattr(blob, "last_modified", None)
                require(bool(etag) and isinstance(size, int) and size > 0 and last_modified is not None,
                        "Azure returned incomplete ETag/size/last-modified metadata for a WAL object.")
                require(last_modified.tzinfo is not None,
                        "Azure returned a WAL last-modified value without timezone information.")
                modified = last_modified.astimezone(timezone.utc).isoformat()
                content_md5 = getattr(getattr(blob, "content_settings", None), "content_md5", None)
                inventory.append({
                    "name": blob.name,
                    "relativeName": relative,
                    "etag": etag,
                    "bytes": size,
                    "lastModified": modified,
                    "contentMd5Base64": base64.b64encode(content_md5).decode("ascii") if content_md5 else None,
                })
    except Refusal:
        raise
    except Exception as exc:
        raise Refusal(f"Azure Blob inventory listing failed ({type(exc).__name__}); details suppressed.") from exc
    inventory.sort(key=lambda item: item["name"])
    require(len({item["name"] for item in inventory}) == len(inventory),
            "Azure Blob inventory contains duplicate names.")
    return inventory


def inventory_map(inventory: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {item["name"]: item for item in inventory}


def verify_previous_inventory(previous: list[dict[str, Any]], current: list[dict[str, Any]]) -> None:
    compare_monotonic(previous, current)


def progress_path(ctx: dict[str, Any], item: dict[str, Any]) -> Path:
    return ctx["progressDir"] / (hashlib.sha256(item["name"].encode()).hexdigest() + ".json")


def verified_progress(ctx: dict[str, Any], item: dict[str, Any]) -> dict[str, Any] | None:
    path = progress_path(ctx, item)
    if not path.exists():
        return None
    progress = read_json(path)
    require(progress.get("ownerToken") == ctx["ownerToken"] and progress.get("stack") == ctx["stack"]
            and progress.get("name") == item["name"] and etags_match(progress.get("etag"), item["etag"])
            and progress.get("bytes") == item["bytes"] and HASH_RE.fullmatch(progress.get("sha256", "")) is not None,
            "WAL export progress file has different ownership or remote ETag.")
    target = local_file_for(ctx["runDir"], item["relativeName"])
    require(target.is_file() and target.stat().st_size == item["bytes"]
            and sha256_file(target) == progress["sha256"],
            "WAL export progress receipt does not match its local bytes.")
    return progress


def ensure_export_roots(ctx: dict[str, Any]) -> None:
    state_dir = ctx["stateDir"]
    run_dir = ctx["runDir"]
    progress_dir = ctx["progressDir"]
    require(not state_dir.is_symlink(), "Stack state directory is a symlink.")
    owner_path = state_dir / "wal-archive-export-owner.json"
    if run_dir.exists():
        require(not run_dir.is_symlink() and run_dir.is_dir() and owner_path.is_file() and not owner_path.is_symlink(),
                "Existing run export directory has no private ownership marker.")
        owner = read_json(owner_path)
        require(owner.get("ownerToken") == ctx["ownerToken"] and owner.get("stack") == ctx["stack"]
                and owner.get("purpose") == "cnpg-wal-archive-export",
                "Existing run export directory belongs to another stack.")
    else:
        run_dir.mkdir(mode=0o700, parents=True)
        atomic_json(owner_path, {"version": 1, "ownerToken": ctx["ownerToken"], "stack": ctx["stack"],
                                 "purpose": "cnpg-wal-archive-export"})
    os.chmod(run_dir, 0o700)
    progress_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    require(not progress_dir.is_symlink(), "Export progress directory is a symlink.")
    os.chmod(progress_dir, 0o700)


def download_one(container: Any, ctx: dict[str, Any], item: dict[str, Any], attempts: int) -> dict[str, Any]:
    prior = verified_progress(ctx, item)
    if prior:
        return prior
    target = local_file_for(ctx["runDir"], item["relativeName"], create_parents=True)
    require(not target.is_symlink(), "Refusing to replace a symlink in the local archive tree.")
    last_error = ""
    for attempt in range(1, attempts + 1):
        fd, raw = tempfile.mkstemp(prefix=".wal-export-", suffix=".partial", dir=target.parent)
        temporary = Path(raw)
        digest = hashlib.sha256()
        received = 0
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb") as output:
                blob_client = container.get_blob_client(item["name"])
                downloader = blob_client.download_blob(etag=azure_etag(item["etag"]), match_condition=MatchConditions.IfNotModified,
                                                       max_concurrency=1)
                for chunk in downloader.chunks():
                    output.write(chunk)
                    digest.update(chunk)
                    received += len(chunk)
                output.flush()
                os.fsync(output.fileno())
            require(received == item["bytes"], "Downloaded WAL byte count differs from its Azure inventory.")
            after = blob_client.get_blob_properties()
            require(etags_match(after.etag, item["etag"]) and int(after.size) == item["bytes"],
                    "WAL blob changed while its ETag-pinned download was in progress.")
            sha = digest.hexdigest()
            os.replace(temporary, target)
            progress = {"version": 1, "ownerToken": ctx["ownerToken"], "stack": ctx["stack"],
                        "name": item["name"], "etag": item["etag"], "bytes": received,
                        "sha256": sha, "lastModified": item["lastModified"]}
            atomic_json(progress_path(ctx, item), progress)
            return progress
        except Exception as exc:
            last_error = type(exc).__name__
            temporary.unlink(missing_ok=True)
            status = getattr(exc, "status_code", None)
            if status in (404, 412) or attempt == attempts:
                break
            time.sleep(min(2 ** attempt, 12))
    raise Refusal(f"WAL download failed for {item['relativeName']} after {attempts} attempts ({last_error}); details suppressed.")


def expected_local_files(ctx: dict[str, Any], inventory: list[dict[str, Any]]) -> set[str]:
    return {item["relativeName"] for item in inventory}


def check_no_unexpected_local_files(ctx: dict[str, Any], inventory: list[dict[str, Any]]) -> None:
    expected = expected_local_files(ctx, inventory)
    expected_parents = {str(PurePosixPath(relative).parent) for relative in expected}
    actual: set[str] = set()
    for directory, dirs, files in os.walk(ctx["runDir"], followlinks=False):
        base = Path(directory)
        for name in dirs:
            child = base / name
            require(not child.is_symlink(), "Local run archive contains a symlink directory.")
        for name in files:
            child = base / name
            require(not child.is_symlink() and child.is_file(), "Local run archive contains a symlink/non-file.")
            rel = str(child.relative_to(ctx["runDir"])).replace(os.sep, "/")
            parent_rel = str(PurePosixPath(rel).parent)
            if (re.fullmatch(r"\.wal-export-[A-Za-z0-9_-]+\.partial", name)
                    and parent_rel in expected_parents):
                child.unlink(missing_ok=True)
                continue
            actual.add(rel)
    require(actual <= expected, "Local run archive contains files outside the owned server WAL inventory.")


def kubectl_exec(ctx: dict[str, Any], pod: str, argv: list[str], *, input_bytes: bytes | None = None,
                 timeout: int = 120) -> subprocess.CompletedProcess[bytes]:
    command = ["kubectl", "--kubeconfig", str(ctx["stateDir"] / "kubeconfig"),
               "exec", *(["-i"] if input_bytes is not None else []), "-n", NAMESPACE, pod, "-c", "postgres", "--", *argv]
    try:
        result = subprocess.run(command, input=input_bytes, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=timeout, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        raise Refusal(f"In-cluster WAL transfer command failed ({type(exc).__name__}).") from exc
    return result


def resolve_primary_pod(ctx: dict[str, Any]) -> str:
    kube = ["kubectl", "--kubeconfig", str(ctx["stateDir"] / "kubeconfig")]
    try:
        result = subprocess.run(kube + ["get", "pods", "-n", NAMESPACE,
            "-l", f"cnpg.io/cluster={CLUSTER}", "-o", "json"], capture_output=True,
            timeout=60, check=False)
        if result.returncode:
            raise Refusal("Cannot inspect the live stack-owned PostgreSQL pods for export.")
        pods = json.loads(result.stdout)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        raise Refusal(f"Cannot validate the live PostgreSQL primary ({type(exc).__name__}).") from exc
    candidates=[]
    expected_digest=ctx["archive"].get("postgresImage", "").split("@")[-1]
    for pod in pods.get("items", []):
        metadata=pod.get("metadata", {}); labels=metadata.get("labels", {})
        if (labels.get("repro.stack.token") != ctx["ownerToken"] or
                labels.get("cnpg.io/instanceRole") != "primary"):
            continue
        if pod.get("status", {}).get("phase") != "Running": continue
        containers=[item for item in pod.get("spec", {}).get("containers", []) if item.get("name")=="postgres"]
        statuses=[item for item in pod.get("status", {}).get("containerStatuses", []) if item.get("name")=="postgres"]
        if len(containers)!=1 or len(statuses)!=1 or statuses[0].get("ready") is not True: continue
        if expected_digest and not str(statuses[0].get("imageID", "")).endswith(expected_digest): continue
        candidates.append(metadata.get("name"))
    require(len(candidates)==1 and candidates[0],
            "Could not verify exactly one ready stack-owned PostgreSQL primary pod for WAL staging.")
    return str(candidates[0])


def remote_json(ctx: dict[str, Any], pod: str, payload: dict[str, Any], *, timeout: int = 120) -> dict[str, Any]:
    encoded=json.dumps(payload, separators=(",", ":")).encode()
    result=kubectl_exec(ctx,pod,["env","PYTHONDONTWRITEBYTECODE=1","python3","-c",REMOTE_STAGE_SCRIPT],
                        input_bytes=encoded,timeout=timeout)
    if result.returncode:
        # The remote helper suppresses exception messages, including any values
        # that might be embedded in SDK request details.
        detail=result.stderr.decode(errors="replace")[-400:]
        raise Refusal(f"In-cluster WAL helper failed (exit {result.returncode}): {detail}")
    try:
        value=json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise Refusal("In-cluster WAL helper returned malformed JSON.") from exc
    require(isinstance(value, dict), "In-cluster WAL helper returned a non-object response.")
    return value


def remote_stage_preflight(ctx: dict[str, Any], pod: str, workers: int = 1) -> dict[str, Any]:
    result=remote_json(ctx,pod,{"operation":"preflight","ownerToken":ctx["ownerToken"],"stack":ctx["stack"]})
    required=workers*(2*REMOTE_BATCH_OBJECTS*16*1024*1024+64*1024*1024)
    require(result.get("status")=="ready" and result.get("azureBlobSdk") is True and result.get("gzip") is True
            and int(result.get("scratchFreeBytes",0))>=required,
            "In-cluster Azure SDK or token-owned scratch capacity did not verify.")
    return result


def remote_bundle_path(stage_id: str, ctx: dict[str, Any]) -> str:
    require(bool(re.fullmatch(r"[a-f0-9]{32}",stage_id)),"Invalid remote WAL stage ID.")
    return f"/controller/tmp/wal-export-{ctx['ownerToken']}-{ctx['stack']}-{stage_id}/batch.tar.gz"


def remote_bundle_fingerprint(ctx: dict[str, Any], pod: str, stage_id: str) -> tuple[int,str]:
    path=shlex.quote(remote_bundle_path(stage_id,ctx))
    script=f"set -eu; f={path}; stat -c %s -- \"$f\"; sha256sum -- \"$f\" | cut -d ' ' -f1"
    result=kubectl_exec(ctx,pod,["sh","-c",script],timeout=180)
    if result.returncode:
        raise Refusal("Could not fingerprint the staged compressed WAL batch.")
    lines=result.stdout.decode("ascii","strict").splitlines()
    require(len(lines)==2 and lines[0].isdecimal() and HASH_RE.fullmatch(lines[1]) is not None,
            "Remote compressed WAL fingerprint is malformed.")
    return int(lines[0]),lines[1]


def record_remote_chunk_failure(ctx: dict[str, Any], stage_id: str, offset: int, expected: int,
                               result: subprocess.CompletedProcess[bytes] | None = None,
                               exc: Exception | None = None) -> None:
    receipt = {"atUtc":datetime.now(timezone.utc).isoformat(),"stack":ctx["stack"],
        "stageId":stage_id,"offsetBytes":offset,"expectedBytes":expected,
        "returnCode":result.returncode if result is not None else None,
        "actualBytes":len(result.stdout) if result is not None else None,
        "stderrBytes":len(result.stderr) if result is not None else None,
        "stderrDisposition":("present-redacted" if result.stderr else "empty") if result is not None else "unavailable",
        "exceptionType":type(exc).__name__ if exc is not None else None,
        "stderrContentsPersisted":False}
    path=ctx["stateDir"]/"wal-archive-export-chunk-failures.jsonl"
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
    try:
        os.fchmod(fd,0o600)
        with os.fdopen(fd,"a") as stream:
            fd=-1
            stream.write(json.dumps(receipt,sort_keys=True)+"\n")
            stream.flush(); os.fsync(stream.fileno())
    finally:
        if fd>=0: os.close(fd)


def remote_bundle_chunk(ctx: dict[str, Any], pod: str, stage_id: str, offset: int, expected: int) -> bytes:
    path=shlex.quote(remote_bundle_path(stage_id,ctx))
    command=f"dd if={path} bs={REMOTE_CHUNK_BYTES} skip={offset//REMOTE_CHUNK_BYTES} count=1 iflag=fullblock 2>/dev/null"
    try:
        result=kubectl_exec(ctx,pod,["sh","-c",command],timeout=180)
    except Exception as exc:
        try: record_remote_chunk_failure(ctx,stage_id,offset,expected,exc=exc)
        except Exception as receipt_exc:
            print(f"{ctx['stack']}: failed to write chunk diagnostic ({type(receipt_exc).__name__})",
                  file=sys.stderr,flush=True)
        raise RuntimeError("Remote compressed WAL range command failed.") from exc
    if result.returncode or len(result.stdout)!=expected:
        try: record_remote_chunk_failure(ctx,stage_id,offset,expected,result=result)
        except Exception as exc:
            print(f"{ctx['stack']}: failed to write chunk diagnostic ({type(exc).__name__})",
                  file=sys.stderr,flush=True)
        raise RuntimeError("Remote compressed WAL range was interrupted or short.")
    return result.stdout


def safe_wal_relative(relative: str) -> bool:
    match=re.fullmatch(r"wals/([0-9A-F]{16})/([0-9A-F]{24})",relative)
    return bool(match and match.group(1)==match.group(2)[:16])


def extract_verified_remote_bundle(bundle_path: Path, ctx: dict[str, Any] | None,
                                   expected_items: list[dict[str, Any]], stage_response: dict[str, Any],
                                   destination_root: Path, *, save_progress: bool,
                                   skip_verified: bool = False) -> list[dict[str, Any]]:
    expected={item["relativeName"]:item for item in expected_items}
    response_files=stage_response.get("files")
    require(isinstance(response_files,list) and len(response_files)==len(expected),
            "Remote batch manifest object count differs from the selected inventory.")
    response_by_relative={}
    for row in response_files:
        relative=row.get("relativeName","")
        require(safe_wal_relative(relative) and relative not in response_by_relative,
                "Remote batch manifest contains an unsafe or duplicate file path.")
        response_by_relative[relative]=row
    require(set(response_by_relative)==set(expected),"Remote batch manifest file set differs from Azure inventory.")
    for relative,item in expected.items():
        row=response_by_relative[relative]
        require(row.get("name")==item["name"] and row.get("etag")==item["etag"]
                and row.get("bytes")==item["bytes"] and HASH_RE.fullmatch(row.get("sha256","")) is not None,
                "Remote batch manifest metadata differs from the exact ETag inventory.")
    allowed={*expected,"_wal-export-manifest.json"}
    progress_records=[]; seen=set()
    with tarfile.open(bundle_path,mode="r:gz") as archive:
        members=archive.getmembers()
        for member in members:
            require(member.isfile() and not member.issym() and not member.islnk()
                    and member.name in allowed and member.name not in seen,
                    "Compressed WAL archive contains an unsafe member.")
            seen.add(member.name)
        require(seen==allowed,"Compressed WAL archive member set is incomplete or unexpected.")
        manifest_member=next(member for member in members if member.name=="_wal-export-manifest.json")
        require(manifest_member.size<=2*1024*1024,"Embedded WAL manifest is too large.")
        manifest_stream=archive.extractfile(manifest_member)
        require(manifest_stream is not None,"Embedded WAL manifest could not be read.")
        try: manifest=json.loads(manifest_stream.read().decode("utf-8"))
        except (UnicodeError,json.JSONDecodeError) as exc: raise Refusal("Embedded WAL manifest is invalid.") from exc
        require(manifest.get("version")==1 and manifest.get("ownerToken")==stage_response.get("ownerToken")
                and manifest.get("stack")==stage_response.get("stack") and manifest.get("files")==response_files,
                "Embedded WAL manifest does not match the verified remote response.")
        for member in members:
            if member.name=="_wal-export-manifest.json": continue
            item=expected[member.name]; remote=response_by_relative[member.name]
            require(member.size==int(item["bytes"]),"Compressed WAL member size differs from its Azure inventory.")
            if skip_verified:
                require(ctx is not None,"Verified-file skipping requires the owned local export context.")
                prior=verified_progress(ctx,item)
                if prior is not None:
                    require(prior.get("sha256")==remote["sha256"],
                            "Staged WAL hash differs from an already verified local progress receipt.")
                    progress_records.append(prior)
                    continue
            target=local_file_for(destination_root,member.name,create_parents=True)
            temporary=target.with_name(f".{target.name}.wal-stage-{os.getpid()}-{time.time_ns()}.partial")
            source=archive.extractfile(member)
            require(source is not None,"Compressed WAL member could not be read.")
            digest=hashlib.sha256(); received=0
            try:
                fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
                with os.fdopen(fd,"wb") as output:
                    for block in iter(lambda:source.read(1024*1024),b""):
                        output.write(block); digest.update(block); received+=len(block)
                    output.flush(); os.fsync(output.fileno())
                require(received==item["bytes"]==remote["bytes"] and digest.hexdigest()==remote["sha256"],
                        "Decompressed WAL bytes do not match the in-cluster original hash.")
                os.replace(temporary,target)
            finally:
                source.close(); temporary.unlink(missing_ok=True)
            progress={"version":1,"ownerToken":stage_response["ownerToken"],"stack":stage_response["stack"],
                "name":item["name"],"etag":item["etag"],"bytes":received,"sha256":digest.hexdigest(),
                "lastModified":item["lastModified"]}
            if save_progress:
                require(ctx is not None,"Local export context is required to write progress.")
                atomic_json(progress_path(ctx,item),progress)
            progress_records.append(progress)
    return progress_records


def _remote_batch_transfer(ctx: dict[str, Any], pod: str, connection: str,
                           items: list[dict[str, Any]], attempts: int, stage_id: str,
                           destination_root: Path | None = None) -> list[dict[str, Any]]:
    require(bool(items) and len(items)<=REMOTE_BATCH_OBJECTS,"Remote WAL batch size is outside the allowed bound.")
    payload={"operation":"stage","ownerToken":ctx["ownerToken"],"stack":ctx["stack"],"stageId":stage_id,
        "accountName":ctx["accountName"],"containerName":ctx["containerName"],"connectionString":connection,
        "items":[{key:item[key] for key in ("name","relativeName","etag","bytes")} for item in items]}
    stage_response=remote_json(ctx,pod,payload,timeout=1800)
    require(stage_response.get("status")=="staged" and stage_response.get("stageId")==stage_id
            and stage_response.get("bundleName")=="batch.tar.gz" and stage_response.get("ownerToken")==ctx["ownerToken"]
            and stage_response.get("stack")==ctx["stack"] and HASH_RE.fullmatch(stage_response.get("bundleSha256","")) is not None,
            "In-cluster staging response does not prove the expected token-owned batch.")
    bundle_bytes=int(stage_response.get("bundleBytes",0))
    require(0<bundle_bytes<=sum(item["bytes"] for item in items)+16*1024*1024,
            "Compressed WAL batch size is implausible.")
    root=destination_root or ctx["runDir"]
    local_bundle_dir=Path(tempfile.mkdtemp(prefix="wal-export-bundle-",dir=ctx["stateDir"]))
    os.chmod(local_bundle_dir,0o700)
    local_bundle=local_bundle_dir/"batch.tar.gz"
    transfer=None
    try:
        def fingerprint(): return remote_bundle_fingerprint(ctx,pod,stage_id)
        def read_chunk(offset,expected): return remote_bundle_chunk(ctx,pod,stage_id,offset,expected)
        transfer=download_verified_file(local_bundle,fingerprint,read_chunk,chunk_bytes=REMOTE_CHUNK_BYTES,
            attempts_per_chunk=attempts,retry_backoff_seconds=0.5,max_retry_backoff_seconds=2.0)
        require(transfer["bytes"]==bundle_bytes and transfer["sha256"]==stage_response["bundleSha256"],
                "Compressed WAL bundle differs from its remote stage receipt.")
        records=extract_verified_remote_bundle(local_bundle,ctx if destination_root is None else None,items,
            stage_response,root,save_progress=destination_root is None)
    finally:
        try:
            cleanup=remote_json(ctx,pod,{"operation":"cleanup","ownerToken":ctx["ownerToken"],
                "stack":ctx["stack"],"stageId":stage_id},timeout=120)
            require(cleanup.get("status") in ("removed","absent") and cleanup.get("stageId")==stage_id,
                    "Remote WAL staging cleanup receipt did not verify.")
        except Exception as exc:
            receipt={"atUtc":datetime.now(timezone.utc).isoformat(),"stack":ctx["stack"],"pod":pod,
                "stageId":stage_id,"stageDirectory":f"/controller/tmp/wal-export-{ctx['ownerToken']}-{ctx['stack']}-{stage_id}",
                "ownerToken":ctx["ownerToken"],"remoteStagePreserved":True,"errorType":type(exc).__name__,
                "files":[{k:item[k] for k in ("name","etag","bytes")} for item in items]}
            path=ctx["stateDir"]/"wal-archive-export-remote-stage-preserved.jsonl"
            with path.open("a") as stream:
                os.chmod(path,0o600); stream.write(json.dumps(receipt,sort_keys=True)+"\n")
            raise Refusal("Could not remove the verified token-owned remote WAL staging directory; preserving its receipt.") from exc
        finally:
            shutil.rmtree(local_bundle_dir,ignore_errors=True)
    receipt={"atUtc":datetime.now(timezone.utc).isoformat(),"stack":ctx["stack"],"pod":pod,
        "stageId":stage_id,"rawBytes":sum(item["bytes"] for item in items),"bundleBytes":bundle_bytes,
        "bundleSha256":transfer["sha256"],"chunkBytes":REMOTE_CHUNK_BYTES,"chunks":transfer["chunks"],
        "chunkRetries":transfer["retries"],"compressionRatio":bundle_bytes/sum(item["bytes"] for item in items),
        "files":records,"remoteTemporaryRemoved":True,"connectionStringPrintedOrPersisted":False}
    path=ctx["stateDir"]/"wal-archive-export-remote-batches.jsonl"
    with path.open("a") as stream:
        os.chmod(path,0o600); stream.write(json.dumps(receipt,sort_keys=True)+"\n")
    return records


def safe_exception_detail(exc: Exception) -> str | None:
    if not isinstance(exc, Refusal):
        return None
    detail = str(exc)
    detail = re.sub(r"(?i)(Account(?:Name|Key)\s*=\s*)[^;,\s]+", r"\1<redacted>", detail)
    detail = re.sub(r"https?://\S+", "<url-redacted>", detail)
    return detail[:600]


def append_remote_batch_failure(ctx: dict[str, Any], stage_id: str,
                                items: list[dict[str, Any]], exc: Exception) -> None:
    receipt = {"atUtc": datetime.now(timezone.utc).isoformat(), "stack": ctx["stack"],
        "stageId": stage_id, "objectCount": len(items), "errorType": type(exc).__name__,
        "error": safe_exception_detail(exc), "connectionStringLoggedOrPersisted": False}
    path = ctx["stateDir"] / "wal-archive-export-remote-batch-failures.jsonl"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "a") as stream:
            fd = -1
            stream.write(json.dumps(receipt, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        if fd >= 0:
            os.close(fd)


def remote_batch_transfer(ctx: dict[str, Any], pod: str, connection: str,
                          items: list[dict[str, Any]], attempts: int,
                          destination_root: Path | None = None) -> list[dict[str, Any]]:
    stage_id = uuid.uuid4().hex
    try:
        return _remote_batch_transfer(ctx, pod, connection, items, attempts, stage_id,
                                      destination_root=destination_root)
    except Exception as exc:
        try:
            append_remote_batch_failure(ctx, stage_id, items, exc)
        except Exception as receipt_exc:
            print(f"{ctx['stack']}: failed to write WAL batch failure receipt ({type(receipt_exc).__name__})",
                  file=sys.stderr, flush=True)
        raise


def cancel_unstarted_futures(futures: dict[Any, Any], failed_future: Any,
                             stack: str, description: str, exc: Exception) -> int:
    cancelled = sum(1 for future in futures if future is not failed_future and future.cancel())
    detail = safe_exception_detail(exc)
    suffix = f": {detail}" if detail else ""
    print(f"{stack}: {description} failed ({type(exc).__name__}{suffix}); "
          f"cancelled {cancelled} queued tasks; in-flight tasks may finish.", file=sys.stderr, flush=True)
    return cancelled


def run_bounded_futures(pool: Any, jobs: list[Any], max_in_flight: int, submit: Any, consume: Any,
                        stack: str, description: str) -> None:
    """Keep only active work submitted so a failure cannot drain a queued backlog."""
    require(max_in_flight > 0, "In-flight export worker limit must be positive.")
    job_iterator = iter(jobs)
    futures: dict[Any, Any] = {}

    def fill_window() -> None:
        while len(futures) < max_in_flight:
            try:
                job = next(job_iterator)
            except StopIteration:
                return
            futures[submit(job)] = job

    fill_window()
    while futures:
        future = next(as_completed(tuple(futures)))
        futures.pop(future)
        try:
            result = future.result()
        except Exception as exc:
            cancel_unstarted_futures(futures, future, stack, description, exc)
            raise
        consume(result)
        fill_window()


def receipt_objects(inventory: list[dict[str, Any]], progress: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    objects = []
    for item in inventory:
        record = progress.get(item["name"])
        require(record is not None and etags_match(record.get("etag"), item["etag"])
                and record.get("bytes") == item["bytes"],
                "A WAL inventory entry is missing its ETag-pinned local transfer receipt.")
        objects.append({**item, "sha256": record["sha256"],
                        "localRelativePath": f"archived-wal/run/{item['relativeName']}"})
    return objects


def save_public_receipt(ctx: dict[str, Any], objects: list[dict[str, Any]], total_bytes: int,
                        stable_listing_passes: int) -> str:
    public_objects = [{
        "path": item["relativeName"],
        "bytes": item["bytes"],
        "sha256": item["sha256"],
        "etag": item["etag"],
        "lastModified": item["lastModified"],
        "contentMd5Base64": item.get("contentMd5Base64"),
    } for item in objects]
    receipt = {
        "version": 1,
        "status": "complete",
        "exportedAtUtc": datetime.now(timezone.utc).isoformat(),
        "stack": ctx["stack"],
        "backend": "CNPG BarmanObjectStore to Azure Blob",
        "serverName": ctx["serverName"],
        "sourcePairProofSha256": ctx["pairSha256"],
        "sourceSnapshotLineageVerified": True,
        "sourceRetentionLocksVerified": True,
        "smokeWalObjectIncluded": True,
        "objectCount": len(objects),
        "totalBytes": total_bytes,
        "stableListingPasses": stable_listing_passes,
        "downloadTransport": ctx.get("transferBackend", "host Azure SDK conditional Blob download"),
        "objects": public_objects,
        "redactions": {"ownerToken": True, "accountId": True, "containerName": True,
                       "connectionString": True, "accountKey": True},
    }
    return atomic_json(ctx["publicReceiptPath"], receipt, mode=0o644)


def load_previous(ctx: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    path = ctx["privateReceiptPath"]
    if path.exists():
        previous = read_json(path)
        require(previous.get("status") == "complete" and previous.get("ownerToken") == ctx["ownerToken"]
                and previous.get("stack") == ctx["stack"] and previous.get("prefix") == ctx["prefix"],
                "Existing export receipt is incomplete or owned by another stack.")
        verify_local_inventory(ctx["runDir"], previous.get("objects", []), previous.get("totalBytes"))
        progress = {item["name"]: {"ownerToken": ctx["ownerToken"], "stack": ctx["stack"],
                                   "name": item["name"], "etag": item["etag"], "bytes": item["bytes"],
                                   "sha256": item["sha256"], "lastModified": item["lastModified"]}
                    for item in previous.get("objects", [])}
        return previous.get("objects", []), progress

    pending_path = ctx["pendingPath"]
    if pending_path.exists():
        pending = read_json(pending_path)
        require(pending.get("status") == "in-progress" and pending.get("ownerToken") == ctx["ownerToken"]
                and pending.get("stack") == ctx["stack"] and pending.get("prefix") == ctx["prefix"],
                "Existing pending export receipt has a different stack/prefix owner.")
        previous = pending.get("objects", [])
    else:
        previous = []
    progress: dict[str, dict[str, Any]] = {}
    if ctx["progressDir"].exists():
        for path in ctx["progressDir"].glob("*.json"):
            item = read_json(path)
            require(item.get("ownerToken") == ctx["ownerToken"] and item.get("stack") == ctx["stack"],
                    "Export progress directory contains a receipt from another stack.")
            progress[item["name"]] = item
    return previous, progress


def acquire_export_locks(state_dir: Path) -> list[Any]:
    lock_files=[]
    for name in ("wal-archive-export.lock","test.lock","teardown.lock","wal-archive.lock"):
        fd=(state_dir/name).open("a")
        try: fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError as exc:
            fd.close()
            raise Refusal(f"Cannot export WAL while {name} is locked.") from exc
        lock_files.append(fd)
    return lock_files


def export(ctx: dict[str, Any], workers: int, attempts: int, *, backend: str = "host",
           remote_workers: int = 2, remote_batch_objects: int = REMOTE_BATCH_OBJECTS) -> None:
    state_dir = ctx["stateDir"]
    lock_files=acquire_export_locks(state_dir)
    require(len(lock_files)==4,"WAL export locks were not all acquired.")
    ctx = validate_local(ctx["stack"])
    container, connection_string = verify_live_owner(ctx)
    require(backend in ("host","in-cluster"),"Unsupported WAL export backend.")
    require(1<=remote_workers<=REMOTE_MAX_WORKERS,"In-cluster worker count must be between 1 and 4.")
    require(1<=remote_batch_objects<=REMOTE_BATCH_OBJECTS,"In-cluster batch size must be between 1 and 100 WAL objects.")
    remote_pod=None; remote_preflight=None
    if backend=="in-cluster":
        remote_pod=resolve_primary_pod(ctx)
        remote_preflight=remote_stage_preflight(ctx,remote_pod,remote_workers)
        ctx["transferBackend"]="in-cluster Azure SDK, gzip-tar staging, verified bounded kubectl transfer"
    else:
        # Do not retain the connection string for the direct-host SDK path.
        del connection_string
        ctx["transferBackend"]="host Azure SDK conditional Blob download"
    ensure_export_roots(ctx)
    previous, progress = load_previous(ctx)
    current = list_inventory(container, ctx)
    verify_previous_inventory(previous, current)
    check_no_unexpected_local_files(ctx, current)
    require(any(item["name"] == ctx["smokeObject"] and item["bytes"] == ctx["smokeBytes"] for item in current),
            "The independently verified smoke WAL object is missing from the exact Azure server prefix.")

    stable_passes = 0
    stable_inventory: list[dict[str, Any]] | None = None
    for pass_number in range(1, MAX_INVENTORY_PASSES + 1):
        previous, _ = load_previous(ctx)
        verify_previous_inventory(previous, current)
        atomic_json(ctx["pendingPath"], {
            "version": 1, "status": "in-progress", "ownerToken": ctx["ownerToken"], "stack": ctx["stack"],
            "accountName": ctx["accountName"], "containerName": ctx["containerName"], "prefix": ctx["prefix"],
            "sourcePairSha256": ctx["pairSha256"], "updatedAtUtc": datetime.now(timezone.utc).isoformat(),
            "objects": current,
        })
        check_no_unexpected_local_files(ctx, current)
        todo = [item for item in current if not verified_progress(ctx, item)]
        if todo:
            completed = 0
            if backend=="host":
                with ThreadPoolExecutor(max_workers=workers, thread_name_prefix=f"wal-{ctx['stack']}") as pool:
                    def consume_host_record(record: dict[str, Any]) -> None:
                        nonlocal completed
                        progress[record["name"]] = record
                        completed += 1
                        if completed % 100 == 0:
                            print(f"{ctx['stack']}: exported {completed}/{len(todo)} new WAL objects in this pass", flush=True)

                    run_bounded_futures(pool, todo, workers,
                        lambda item: pool.submit(download_one, container, ctx, item, attempts),
                        consume_host_record, ctx["stack"], "WAL download")
            else:
                batches=[todo[i:i+remote_batch_objects] for i in range(0,len(todo),remote_batch_objects)]
                with ThreadPoolExecutor(max_workers=remote_workers,thread_name_prefix=f"wal-remote-{ctx['stack']}") as pool:
                    def consume_remote_records(records: list[dict[str, Any]]) -> None:
                        nonlocal completed
                        for record in records:
                            progress[record["name"]]=record
                            completed+=1
                            if completed%100==0 or completed==len(todo):
                                print(f"{ctx['stack']}: exported {completed}/{len(todo)} new WAL objects in this pass",flush=True)

                    run_bounded_futures(pool, batches, remote_workers,
                        lambda batch: pool.submit(remote_batch_transfer,ctx,remote_pod,connection_string,batch,attempts),
                        consume_remote_records, ctx["stack"], "remote WAL batch")
        after = list_inventory(container, ctx)
        verify_previous_inventory(current, after)
        if len(after) == len(current) and all(item_identity(a) == item_identity(b) for a, b in zip(current, after)):
            stable_passes = pass_number
            stable_inventory = after
            break
        current = after
    require(stable_inventory is not None, "Azure WAL prefix did not remain stable during export; retry after archiving settles.")

    # Validate the local smoke object against the earlier independent Blob/replica proof.
    smoke_item = next((item for item in stable_inventory if item["name"] == ctx["smokeObject"]), None)
    require(smoke_item is not None and smoke_item["bytes"] == ctx["smokeBytes"],
            "Final stable inventory no longer contains the smoke WAL object.")
    smoke_progress = progress.get(ctx["smokeObject"]) or verified_progress(ctx, smoke_item)
    require(smoke_progress is not None and smoke_progress["sha256"] == ctx["smokeSha256"],
            "Exported smoke WAL SHA-256 differs from the previously verified Azure/replica smoke.")

    # Require a second identical final listing after all bytes have been flushed.
    final_listing = list_inventory(container, ctx)
    require(len(final_listing) == len(stable_inventory)
            and all(item_identity(a) == item_identity(b) for a, b in zip(stable_inventory, final_listing)),
            "Azure WAL prefix changed after the final file verification; retry export before teardown.")

    objects = receipt_objects(stable_inventory, progress)
    total_bytes = sum(int(item["bytes"]) for item in objects)
    verify_local_inventory(ctx["runDir"], objects, total_bytes)
    public_hash = save_public_receipt(ctx, objects, total_bytes, stable_passes)
    private = {
        "version": 1,
        "status": "complete",
        "exportedAtUtc": datetime.now(timezone.utc).isoformat(),
        "ownerToken": ctx["ownerToken"],
        "stack": ctx["stack"],
        "subscriptionId": ctx["subscriptionId"],
        "resourceGroup": ctx["resourceGroup"],
        "accountName": ctx["accountName"],
        "accountId": ctx["accountId"],
        "containerName": ctx["containerName"],
        "destinationPath": ctx["destinationPath"],
        "prefix": ctx["prefix"],
        "serverName": ctx["serverName"],
        "sourcePairSha256": ctx["pairSha256"],
        "sourceSnapshotIds": {"data": ctx["pair"]["data"]["id"], "wal": ctx["pair"]["wal"]["id"]},
        "sourceSnapshotLineage": ctx["smoke"].get("snapshotLineage"),
        "sourceLocksPreserved": True,
        "restoreSmoke": {"walName": ctx["smoke"].get("walName"), "archiveObject": ctx["smokeObject"],
                         "bytes": ctx["smokeBytes"], "sha256": ctx["smokeSha256"],
                         "restorePod": ctx["smoke"].get("restorePod"), "restorePodRole": "replica"},
        "localPayloadDirectory": str(ctx["runDir"].relative_to(EXPERIMENT / ctx["stack"])),
        "publicReceiptPath": str(ctx["publicReceiptPath"].relative_to(EXPERIMENT)),
        "publicReceiptSha256": public_hash,
        "objectCount": len(objects),
        "totalBytes": total_bytes,
        "transferBackend":ctx.get("transferBackend","host Azure SDK conditional Blob download"),
        "remoteStagePod":remote_pod,
        "remoteStagePreflight":remote_preflight,
        "stableListingPasses": stable_passes,
        "objects": objects,
        "checks": {"exactTokenServerPrefixOnly": True, "sourcePairLineageVerified": True,
                   "sourceLocksVerifiedUnmodified": True, "allDownloadsEtagPinned": True,
                   "everyObjectSizeVerified": True, "everyLocalSha256Verified": True,
                   "azureListingStableAfterDownload": True, "azureMutations": 0},
    }
    atomic_json(ctx["privateReceiptPath"], private)
    ctx["pendingPath"].unlink(missing_ok=True)
    print(json.dumps({"status": "complete", "stack": ctx["stack"], "objectCount": len(objects),
                      "totalBytes": total_bytes, "publicReceipt": str(ctx["publicReceiptPath"]),
                      "privatePayloadDirectory": str(ctx["runDir"]),"transferBackend":ctx["transferBackend"],
                      "secretsPrinted": False}, indent=2))


def validate_in_cluster_transport(stack: str, attempts: int = 4) -> dict[str, Any]:
    initial=validate_local(stack)
    lock_files=acquire_export_locks(initial["stateDir"])
    try:
        ctx=validate_local(stack)
        container,connection=verify_live_owner(ctx)
        inventory=list_inventory(container,ctx)
        by_name=inventory_map(inventory)
        smoke=by_name.get(ctx["smokeObject"])
        require(smoke is not None and smoke["bytes"]==ctx["smokeBytes"],
                "Known smoke WAL object is absent from the current owner-scoped Blob inventory.")
        newest=max(inventory,key=lambda item:(item["lastModified"],item["name"]))
        selected={item["name"]:item for item in (smoke,newest)}
        selected_items=list(selected.values())
        pod=resolve_primary_pod(ctx)
        preflight=remote_stage_preflight(ctx,pod,1)
        with tempfile.TemporaryDirectory(prefix="wal-export-validation-",dir=ctx["stateDir"]) as raw_root:
            destination=Path(raw_root)
            records=remote_batch_transfer(ctx,pod,connection,selected_items,attempts,destination_root=destination)
            record_by_name={item["name"]:item for item in records}
            for item in selected_items:
                path=destination/item["relativeName"]
                record=record_by_name.get(item["name"])
                require(path.is_file() and path.stat().st_size==item["bytes"] and record is not None
                        and sha256_file(path)==record["sha256"],
                        "In-cluster staged WAL did not round-trip with its original byte count and SHA-256.")
            require(record_by_name[ctx["smokeObject"]]["sha256"]==ctx["smokeSha256"],
                    "In-cluster staged smoke WAL hash differs from the independent archive/restore proof.")
        receipt={"version":1,"status":"complete","validatedAtUtc":datetime.now(timezone.utc).isoformat(),
            "stack":stack,"ownerToken":ctx["ownerToken"],"pod":pod,"remoteStagePreflight":preflight,
            "objects":[{**record_by_name[item["name"]],"matchesIndependentSmokeHash":
                         record_by_name[item["name"]]["sha256"]==ctx["smokeSha256"] if item["name"]==ctx["smokeObject"] else None}
                        for item in selected_items],
            "checks":{"remoteSdkEtagPinned":True,"etagAndSizeRecheckedAfterDownload":True,
                "compressedTransferSha256Verified":True,"decompressedOriginalSha256Verified":True,
                "smokeWalMatchesIndependentHash":True,"azureMutations":0,
                "postgresqlWrites":0,"connectionStringPrintedOrPersisted":False}}
        path=ctx["stateDir"]/"wal-archive-export-remote-validation.json"
        atomic_json(path,receipt)
        receipt["privateReceiptPath"]=str(path)
        return receipt
    finally:
        for fd in lock_files: fd.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stack", required=True, choices=STACK_NAMES)
    parser.add_argument("--apply", action="store_true", help="Read and download the exact owned Azure Blob WAL prefix.")
    parser.add_argument("--backend", choices=("host","in-cluster"), default="host",
                        help="Download via host SDK or stage ETag-verified gzip batches in the PostgreSQL primary scratch area.")
    parser.add_argument("--remote-workers", type=int, default=2)
    parser.add_argument("--remote-batch-objects", type=int, default=REMOTE_BATCH_OBJECTS)
    parser.add_argument("--validate-in-cluster", action="store_true",
                        help="Round-trip the known smoke WAL and newest WAL through in-cluster gzip staging; requires --apply --backend in-cluster.")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--attempts", type=int, default=4)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    require(8 <= args.workers <= MAX_WORKERS, f"Worker count must be between 8 and {MAX_WORKERS}.")
    require(1 <= args.attempts <= 8, "Retry count must be between 1 and 8.")
    require(1 <= args.remote_workers <= REMOTE_MAX_WORKERS, "Remote worker count must be between 1 and 4.")
    require(1 <= args.remote_batch_objects <= REMOTE_BATCH_OBJECTS,
            "Remote batch size must be between 1 and 100 WAL objects.")
    require(not args.validate_in_cluster or (args.apply and args.backend=="in-cluster"),
            "--validate-in-cluster requires --apply --backend in-cluster.")
    ctx = validate_local(args.stack)
    if not args.apply:
        print(json.dumps(build_plan(ctx,args.workers,args.backend,args.remote_workers,args.remote_batch_objects),indent=2,sort_keys=True))
        return 0
    if args.validate_in_cluster:
        print(json.dumps(validate_in_cluster_transport(args.stack,args.attempts),indent=2,sort_keys=True))
        return 0
    # Preserve local timeline history and recovery control files as well as
    # Blob WAL. A history file archived before the backend was configured
    # can be absent from the Blob prefix despite being required for recovery.
    if not (ctx["stateDir"] / "recovery-metadata.json").exists():
        subprocess.run([sys.executable, str(HERE / "capture-recovery-metadata.py"),
                        "--stack", args.stack], check=True)
    export(ctx,args.workers,args.attempts,backend=args.backend,remote_workers=args.remote_workers,
           remote_batch_objects=args.remote_batch_objects)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Refusal as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
