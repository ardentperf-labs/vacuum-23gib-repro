#!/usr/bin/env python3
"""Set up and prove the per-stack CNPG Azure Blob WAL archive.

The default is a local plan only. ``--apply`` creates an owner-tagged private
StorageV2 account/container in the selected stack resource group, writes its
connection string to the stack's Kubernetes Secret via stdin (never argv or a
file), enables the already-pinned CNPG in-tree Barman backend, then proves a
real primary WAL segment can be independently downloaded from Blob and fetched
again through CNPG's ``manager wal-restore`` into a temporary path outside
PGDATA. It never changes a snapshot, snapshot lock, VolumeSnapshot, source
lineage, or ``pg_wal``.
"""

from __future__ import annotations

import argparse
import base64
import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
EXPERIMENT = HERE.parent
NAMESPACE = "repro"
CLUSTER = "hdd-pg"
SECRET_NAME = "azure-barman-credentials"
SECRET_KEY = "connection-string"
SYSTEM_IMAGE = (
    "ghcr.io/cloudnative-pg/postgresql:16.15-202609140815-system-trixie"
    "@sha256:a30ea813ae1c82fc94743c4e71766688158b58d8ddf1f4e20ff5d3edb569a215"
)
SOURCE_IDS = {
    "data": "vacuum-only-repro/source-data-snapshot-id",
    "wal": "vacuum-only-repro/source-wal-snapshot-id",
}
WAL_RE = re.compile(r"^[0-9A-F]{24}$")
TOKEN_RE = re.compile(r"^[a-f0-9]{32}$")
MIB = 1024 * 1024


class Refusal(RuntimeError):
    """A failed safety or evidence check."""


def require(ok: bool, message: str) -> None:
    if not ok:
        raise Refusal(message)


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise Refusal(f"Cannot read valid JSON metadata at {path}: {exc}") from exc


def atomic_json(path: Path, value: Any, mode: int = 0o600) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
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


def lower(value: Any) -> str:
    return str(value or "").casefold().rstrip("/")


def expected_storage(token: str) -> dict[str, str]:
    require(bool(TOKEN_RE.fullmatch(token)), "Owner token must be 32 lowercase hexadecimal characters.")
    return {
        "accountName": f"cnpgwal{token[:17]}",
        "containerName": f"wal-{token[:12]}",
    }


def destination_path(account: str, container: str, token: str) -> str:
    require(bool(re.fullmatch(r"[a-z0-9]{3,24}", account)), "Invalid Storage account name.")
    require(bool(re.fullmatch(r"[a-z0-9-]{3,63}", container)), "Invalid Blob container name.")
    require(bool(TOKEN_RE.fullmatch(token)), "Invalid owner token.")
    return f"https://{account}.blob.core.windows.net/{container}/cnpg-wal-archive/{token}/"


def expected_blob_name(token: str, server: str, wal_name: str) -> str:
    require(bool(TOKEN_RE.fullmatch(token)), "Invalid owner token.")
    require(server == CLUSTER, "Archive server name must equal the CNPG Cluster name.")
    require(bool(WAL_RE.fullmatch(wal_name)), "Invalid PostgreSQL WAL segment name.")
    return f"cnpg-wal-archive/{token}/{server}/wals/{wal_name[:16]}/{wal_name}"


def validate_restore_destination(remote_path: str) -> None:
    require(bool(re.fullmatch(r"/run/wal-archive-smoke-[a-f0-9]{12}-[0-9A-F]{24}", remote_path)),
            "CNPG restore smoke destination must be an isolated writable scratch file outside PGDATA and pg_wal.")


def validate_restore_log_destination(remote_log: str) -> None:
    require(bool(re.fullmatch(r"/run/wal-archive-smoke-[a-f0-9]{12}\.json", remote_log)),
            "CNPG restore smoke log must be an isolated writable scratch file outside pg_wal.")


def require_matching_smoke_hashes(primary_hash: str, azure_hash: str, manager_hash: str) -> None:
    require(bool(re.fullmatch(r"[a-f0-9]{64}", primary_hash))
            and primary_hash == azure_hash == manager_hash,
            "Primary, independent Azure download, and CNPG manager restore hashes must match.")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def retain_smoke_wal(state_dir: Path, wal_name: str, source: Path, expected_sha256: str) -> str:
    require(bool(WAL_RE.fullmatch(wal_name)), "Refusing to retain an invalid WAL filename.")
    require(bool(re.fullmatch(r"[a-f0-9]{64}", expected_sha256)) and source.is_file(),
            "Refusing to retain an unverified WAL object.")
    directory = state_dir / "archived-wal"
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    require(not directory.is_symlink(), "Refusing to retain WAL data through a symlinked directory.")
    os.chmod(directory, 0o700)
    destination = directory / wal_name
    require(not destination.is_symlink(), "Refusing to replace or adopt a symlink as retained WAL data.")
    if destination.exists():
        require(destination.is_file() and sha256_file(destination) == expected_sha256,
                "A retained WAL filename already exists with different content.")
    else:
        fd, raw = tempfile.mkstemp(prefix=".wal-segment.", dir=directory)
        temporary = Path(raw)
        try:
            with os.fdopen(fd, "wb") as target, source.open("rb") as incoming:
                shutil.copyfileobj(incoming, target, 1024 * 1024)
                target.flush()
                os.fsync(target.fileno())
            os.chmod(temporary, 0o600)
            require(sha256_file(temporary) == expected_sha256,
                    "Retained local WAL bytes changed during the copy.")
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
    return str(destination.relative_to(state_dir))


def make_secret_manifest(token: str, connection_string: str) -> dict[str, Any]:
    require(bool(TOKEN_RE.fullmatch(token)), "Invalid owner token.")
    require(bool(connection_string.strip()), "Azure connection string is empty.")
    encoded = base64.b64encode(connection_string.encode()).decode("ascii")
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": SECRET_NAME,
            "namespace": NAMESPACE,
            "labels": {
                "repro.stack.token": token,
                "app.kubernetes.io/managed-by": "walreceiver-repro",
            },
        },
        "type": "Opaque",
        "data": {SECRET_KEY: encoded},
    }


def make_barman_patch(account: str, container: str, token: str) -> dict[str, Any]:
    return {
        "spec": {
            "backup": {
                "barmanObjectStore": {
                    "destinationPath": destination_path(account, container, token),
                    "serverName": CLUSTER,
                    "azureCredentials": {
                        "connectionString": {"name": SECRET_NAME, "key": SECRET_KEY}
                    },
                }
            }
        }
    }


def validate_configuring_receipt(receipt: dict[str, Any], ctx: dict[str, Any], *,
                                 account_id: str | None = None,
                                 source_locks: dict[str, Any] | None = None,
                                 lineage: dict[str, Any] | None = None) -> None:
    """Accept only this stack's exact, incomplete archive setup receipt."""
    require(receipt.get("version") == 1 and receipt.get("status") == "configuring"
            and isinstance(receipt.get("configuredAtUtc"), str) and bool(receipt["configuredAtUtc"]),
            "Only an owned configuring WAL archive receipt can be resumed; ready/unknown receipts are not repeatable.")
    expected_destination = destination_path(ctx["accountName"], ctx["containerName"], ctx["ownerToken"])
    require(receipt.get("ownerToken") == ctx["ownerToken"]
            and receipt.get("subscriptionId") == ctx["subscriptionId"]
            and receipt.get("resourceGroup") == ctx["resourceGroup"]
            and lower(receipt.get("location")) == lower(ctx["location"]),
            "WAL archive receipt belongs to a different stack or Azure scope.")
    require(receipt.get("accountName") == ctx["accountName"]
            and receipt.get("containerName") == ctx["containerName"]
            and receipt.get("containerPublicAccess") is None
            and lower(receipt.get("accountId")) == lower(account_id or expected_account_id(ctx))
            and receipt.get("destinationPath") == expected_destination,
            "WAL archive receipt account/container/destination differs from this stack's exact owned target.")
    require(receipt.get("serverName") == CLUSTER
            and receipt.get("secretReference") == {"namespace": NAMESPACE, "name": SECRET_NAME, "key": SECRET_KEY}
            and receipt.get("postgresImage") == SYSTEM_IMAGE
            and receipt.get("backend") == "CNPG 1.30.1 in-tree BarmanObjectStore"
            and receipt.get("plugin") is None
            and receipt.get("connectionStringStoredIn") == f"{NAMESPACE}/{SECRET_NAME}:{SECRET_KEY}"
            and receipt.get("connectionStringPersistedLocally") is False,
            "WAL archive receipt contains an unexpected server, Secret, backend, or image configuration.")
    expected_sources = {kind: ctx["pair"][kind]["id"] for kind in ("data", "wal")}
    actual_sources = receipt.get("sourceSnapshotIds") or {}
    require(set(actual_sources) == {"data", "wal"}
            and all(lower(actual_sources.get(kind)) == lower(value) for kind, value in expected_sources.items()),
            "WAL archive receipt does not reference the exact pinned DATA/WAL source pair.")
    if source_locks is not None:
        require(receipt.get("sourceLockVerification") == source_locks,
                "WAL archive receipt source-lock evidence differs from the current read-only verification.")
    if lineage is not None:
        require(receipt.get("snapshotLineage") == lineage,
                "WAL archive receipt snapshot lineage differs from the live pinned DATA/WAL lineage.")


def validate_backup_store(backup: dict[str, Any], expected_store: dict[str, Any] | None = None) -> None:
    configured = backup.get("barmanObjectStore")
    if expected_store is None:
        require(not configured, "A Barman object store is already configured; refusing to adopt an unknown backend.")
    else:
        require(configured == expected_store,
                "Configured Barman object store does not exactly match this stack's owned resume receipt.")
    require(not (backup.get("volumeSnapshot") or backup.get("pluginConfiguration")),
            "Unexpected backup backend is configured.")


def validate_archive_secret(secret: dict[str, Any], ctx: dict[str, Any], connection: str) -> None:
    metadata = secret.get("metadata", {})
    data = secret.get("data") or {}
    require(secret.get("kind") == "Secret" and secret.get("type") == "Opaque"
            and metadata.get("name") == SECRET_NAME and metadata.get("namespace") == NAMESPACE
            and metadata.get("labels", {}).get("repro.stack.token") == ctx["ownerToken"]
            and SECRET_KEY in data,
            "Existing archive Secret is not the exact stack-owned Secret reference.")
    try:
        value = base64.b64decode(data[SECRET_KEY], validate=True).decode()
    except (ValueError, UnicodeDecodeError) as exc:
        raise Refusal("Existing archive Secret data is not valid base64 text.") from exc
    require(value == connection, "Existing stack-owned archive Secret differs from the verified Storage account credential.")


def secret_access_argv(ctx: dict[str, Any], service_account: str) -> list[str]:
    require(bool(re.fullmatch(r"[a-z0-9](?:[-a-z0-9.]{0,251}[a-z0-9])?", service_account)),
            "CNPG pod reported an invalid service-account name.")
    identity = f"system:serviceaccount:{NAMESPACE}:{service_account}"
    return k_argv(ctx, "auth", "can-i", "get", f"secrets/{SECRET_NAME}",
                  "-n", NAMESPACE, "--as", identity)


def wait_for_secret_access(ctx: dict[str, Any], pods: list[dict[str, Any]], timeout_seconds: int = 300,
                           interval_seconds: int = 5) -> dict[str, Any]:
    """Wait for each PostgreSQL pod identity to have Secret read access twice."""
    service_accounts = sorted({p.get("spec", {}).get("serviceAccountName", "") for p in pods})
    require(bool(service_accounts) and all(service_accounts),
            "Cannot verify archive Secret access because a PostgreSQL pod has no service-account identity.")
    deadline = time.monotonic() + timeout_seconds
    consecutive = 0
    while time.monotonic() < deadline:
        allowed = True
        for service_account in service_accounts:
            result = run(secret_access_argv(ctx, service_account), timeout=60, allow_failure=True)
            if result.returncode != 0 or result.stdout.strip().casefold() != "yes":
                allowed = False
        consecutive = consecutive + 1 if allowed else 0
        if consecutive >= 2:
            return {"status": "ready", "checkedServiceAccounts": service_accounts,
                    "consecutiveReadOnlyAccessReviews": consecutive, "secretReference": f"{NAMESPACE}/{SECRET_NAME}"}
        time.sleep(interval_seconds)
    raise Refusal("PostgreSQL service accounts did not show stable read access to the archive Secret; smoke was not started.")


def archiver_counts_advanced(before: dict[str, Any], after: dict[str, Any]) -> bool:
    try:
        archived_before = int(before["archivedCount"])
        archived_after = int(after["archivedCount"])
        failed_before = int(before["failedCount"])
        failed_after = int(after["failedCount"])
    except (KeyError, TypeError, ValueError) as exc:
        raise Refusal("PostgreSQL archiver counters are missing or invalid.") from exc
    require(min(archived_before, archived_after, failed_before, failed_after) >= 0,
            "PostgreSQL archiver counters must be nonnegative.")
    require(failed_after == failed_before,
            "The WAL archive smoke increased failed_count from its preserved baseline.")
    return archived_after > archived_before


def validate_local(stack: str) -> dict[str, Any]:
    stack_dir = EXPERIMENT / stack
    sd = stack_dir / ".state"
    state_path = sd / "state.json"
    prepare_path = sd / "prepare.json"
    pair_path = HERE / "source-pair.json"
    proof_path = HERE / "source-pair-proof.json"
    require(stack_dir.parent == EXPERIMENT and stack in ("sigterm", "control"), "Select the owned sigterm or control stack.")
    state = read_json(state_path)
    pair = read_json(pair_path)
    proof = read_json(proof_path)
    prepare = read_json(prepare_path)
    pair_sha = hashlib.sha256(pair_path.read_bytes()).hexdigest()
    token = state.get("ownerToken", "")
    storage = expected_storage(token)

    require(state.get("status") == "ready" and state.get("ownerToken") == token, "Stack is not ready or its ownership token is invalid.")
    require(state.get("subscriptionId") == pair.get("subscriptionId"), "Stack subscription differs from the pinned snapshot pair.")
    require(lower(state.get("location")) == lower(pair.get("region")) == "eastus", "WAL archive stack/pair must both be in EastUS.")
    require(state.get("resourceGroup") == f"rg-vacuum-only-{token}", "Stack RG name is outside its token ownership scope.")
    require(state.get("nodeResourceGroup") == f"rg-vacuum-nodes-{token}", "Node RG name is outside its token ownership scope.")
    kubeconfig = Path(state.get("kubeconfigPath", ""))
    require(kubeconfig == sd / "kubeconfig" and kubeconfig.is_file(), "Isolated per-stack kubeconfig is missing or changed.")
    require(proof.get("provenance", {}).get("copiedPairSha256") == pair_sha, "Pinned source-pair hash differs from its proof.")
    require(pair.get("version") == 1 and pair.get("kind") == "offline-postgresql-pair", "Pinned source-pair kind/schema changed.")
    require(pair.get("diskSizeGiB") == 512 and pair.get("fixture", {}).get("heapBytes") == 24696061952,
            "Pinned source is not the verified 512-GiB, 23-GiB pre-VACUUM pair.")
    require(pair.get("fixture", {}).get("sourceSnapshotBeforeVacuum") is True, "Pinned source is not pre-VACUUM.")
    require(pair.get("restoreEvidence", {}).get("status") == "complete"
            and pair.get("restoreEvidence", {}).get("sourceSnapshotsUntouched") is True
            and pair.get("restoreEvidence", {}).get("replicaRestoreCompleted") is True,
            "Pinned pair lacks successful, unchanged-source restore evidence.")
    require(prepare.get("status") == "complete" and prepare.get("ownerToken") == token,
            "The restored primary/replica preparation receipt is not complete for this stack.")
    require(prepare.get("subscriptionId") == state.get("subscriptionId")
            and prepare.get("resourceGroup") == state.get("resourceGroup")
            and prepare.get("clusterName") == state.get("clusterName")
            and prepare.get("cluster") == f"{NAMESPACE}/{CLUSTER}",
            "Preparation receipt does not match this stack.")
    require(prepare.get("systemIdentifier") == pair.get("fixture", {}).get("systemIdentifier")
            and prepare.get("heapBytes") == pair.get("fixture", {}).get("heapBytes"),
            "Prepared primary identity/heap differs from the source fixture.")
    source = prepare.get("sourcePair", {})
    require(lower(source.get("dataSnapshotId")) == lower(pair.get("data", {}).get("id"))
            and lower(source.get("walSnapshotId")) == lower(pair.get("wal", {}).get("id")),
            "Preparation receipt does not preserve the exact paired snapshot IDs.")
    permissions = prepare.get("sourceSnapshotRoleAssignments", {})
    require(permissions.get("status") == "not-created"
            and permissions.get("locksModified") is False
            and permissions.get("sourceLocksVerifiedUnmodified") is True,
            "Source permission/lock receipt does not prove read-only direct-copy restore.")
    require(not (sd / "test-state.json").exists(), "Measurement already started; archive smoke must finish before the test.")
    require(not (sd / "wal-archive-smoke.json").exists(), "A prior archive smoke receipt exists; refusing to run a second smoke implicitly.")

    ctx = {
        "stack": stack,
        "stackDir": stack_dir,
        "stateDir": sd,
        "state": state,
        "pair": pair,
        "prepare": prepare,
        "pairSha256": pair_sha,
        "subscriptionId": state["subscriptionId"],
        "ownerToken": token,
        "resourceGroup": state["resourceGroup"],
        "location": state["location"],
        "kubeconfig": kubeconfig,
        **storage,
    }
    archive_path = sd / "wal-archive.json"
    if archive_path.exists() or archive_path.is_symlink():
        require(not sd.is_symlink() and not archive_path.is_symlink(),
                "WAL archive resume receipt path is symlinked.")
        archive_receipt = read_json(archive_path)
        validate_configuring_receipt(archive_receipt, ctx)
        ctx["resumeArchive"] = archive_receipt
    else:
        ctx["resumeArchive"] = None
    return ctx


def build_plan(ctx: dict[str, Any]) -> dict[str, Any]:
    return {
        "mode": "plan-only",
        "stack": ctx["stack"],
        "ownerToken": ctx["ownerToken"],
        "resourceGroup": ctx["resourceGroup"],
        "location": ctx["location"],
        "storageAccount": ctx["accountName"],
        "container": ctx["containerName"],
        "storageSku": "Standard_LRS",
        "anonymousAccess": False,
        "destinationPath": destination_path(ctx["accountName"], ctx["containerName"], ctx["ownerToken"]),
        "serverName": CLUSTER,
        "secretReference": {"namespace": NAMESPACE, "name": SECRET_NAME, "key": SECRET_KEY},
        "postgresImage": SYSTEM_IMAGE,
        "backend": "CNPG 1.30.1 in-tree BarmanObjectStore; no plugin CRDs or sidecars",
        "smoke": ["closed primary WAL file hash", "independent Azure Blob download hash", "CNPG manager wal-restore into writable /run scratch hash"],
        "sourceSnapshotMutations": 0,
        "sourceLockMutations": 0,
        "pgWalRestoreTarget": None,
    }


def run(
    argv: list[str],
    *,
    timeout: int = 240,
    env: dict[str, str] | None = None,
    stdin: str | None = None,
    sensitive: bool = False,
    allow_failure: bool = False,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(argv, input=stdin, text=True, capture_output=True, timeout=timeout, env=env, check=False)
    except subprocess.TimeoutExpired as exc:
        raise Refusal(f"Command timed out: {' '.join(argv[:5])}") from exc
    if result.returncode and not allow_failure:
        if sensitive:
            detail = " (sensitive command output suppressed)"
        else:
            detail = f": {(result.stderr or result.stdout).strip()[-1200:]}"
        raise Refusal(f"Command failed ({result.returncode}): {' '.join(argv[:5])}{detail}")
    return result


def az_argv(ctx: dict[str, Any], *args: str) -> list[str]:
    return ["az", *args, "--subscription", ctx["subscriptionId"]]


def az_json(ctx: dict[str, Any], *args: str, timeout: int = 240) -> Any:
    result = run(az_argv(ctx, *args, "-o", "json"), timeout=timeout)
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise Refusal(f"Azure CLI returned invalid JSON for {' '.join(args[:4])}.") from exc


def k_argv(ctx: dict[str, Any], *args: str) -> list[str]:
    return ["kubectl", "--kubeconfig", str(ctx["kubeconfig"]), *args]


def k_json(ctx: dict[str, Any], *args: str) -> Any:
    result = run(k_argv(ctx, *args, "-o", "json"), timeout=180)
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise Refusal(f"kubectl returned invalid JSON for {' '.join(args[:4])}.") from exc


def expected_account_id(ctx: dict[str, Any]) -> str:
    return (
        f"/subscriptions/{ctx['subscriptionId']}/resourceGroups/{ctx['resourceGroup']}"
        f"/providers/Microsoft.Storage/storageAccounts/{ctx['accountName']}"
    )


def account_matches(live: dict[str, Any], ctx: dict[str, Any]) -> bool:
    tags = live.get("tags") or {}
    return (
        lower(live.get("id")) == lower(expected_account_id(ctx))
        and live.get("name", "").casefold() == ctx["accountName"]
        and live.get("resourceGroup", "").casefold() == ctx["resourceGroup"].casefold()
        and live.get("location", "").casefold() == ctx["location"].casefold()
        and live.get("provisioningState") == "Succeeded"
        and live.get("kind") == "StorageV2"
        and (live.get("sku") or {}).get("name") == "Standard_LRS"
        and live.get("allowBlobPublicAccess") is False
        and live.get("enableHttpsTrafficOnly") is True
        and live.get("minimumTlsVersion") == "TLS1_2"
        and tags.get("reprotoken") == ctx["ownerToken"]
        and tags.get("reproPurpose") == "private-cnpg-wal-archive"
        and tags.get("reproStack") == "walreceiver-sigterm"
    )


def lineage_objects(ctx: dict[str, Any], cluster: dict[str, Any], vss_items: list[dict[str, Any]], vsc_items: list[dict[str, Any]]) -> dict[str, Any]:
    pair = ctx["pair"]
    token = ctx["ownerToken"]
    short = token[:8]
    vss_by_name = {x.get("metadata", {}).get("name"): x for x in vss_items}
    vsc_by_name = {x.get("metadata", {}).get("name"): x for x in vsc_items}
    records: dict[str, Any] = {}
    for kind in ("data", "wal"):
        lower_kind = kind
        vss_name = f"source-{kind}-{short}"
        vsc_name = f"source-{kind}-content-{short}"
        expected_id = pair[kind]["id"]
        require(lower(cluster.get("metadata", {}).get("annotations", {}).get(SOURCE_IDS[kind])) == lower(expected_id),
                f"Cluster {kind.upper()} snapshot annotation changed.")
        vss = vss_by_name.get(vss_name)
        vsc = vsc_by_name.get(vsc_name)
        require(vss is not None and vsc is not None, f"Stack-owned {kind.upper()} VolumeSnapshot lineage is missing.")
        vss_meta = vss.get("metadata", {})
        vsc_meta = vsc.get("metadata", {})
        require((vss_meta.get("labels") or {}).get("repro.stack.token") == token, f"VolumeSnapshot {vss_name} is not stack-owned.")
        require((vsc_meta.get("labels") or {}).get("repro.stack.token") == token, f"VolumeSnapshotContent {vsc_name} is not stack-owned.")
        require(vss.get("spec", {}).get("source", {}).get("volumeSnapshotContentName") == vsc_name,
                f"VolumeSnapshot {vss_name} no longer points to the expected Content.")
        spec = vsc.get("spec", {})
        require(spec.get("deletionPolicy") == "Retain" and spec.get("driver") == "disk.csi.azure.com",
                f"VolumeSnapshotContent {vsc_name} lost Retain/CSI lineage.")
        require(lower(spec.get("source", {}).get("snapshotHandle")) == lower(expected_id),
                f"VolumeSnapshotContent {vsc_name} snapshot ID differs from the pinned source pair.")
        ref = spec.get("volumeSnapshotRef", {})
        require(ref.get("name") == vss_name and ref.get("namespace") == NAMESPACE,
                f"VolumeSnapshotContent {vsc_name} binding reference changed.")
        records[lower_kind] = {
            "volumeSnapshot": vss_name,
            "volumeSnapshotUid": vss_meta.get("uid"),
            "volumeSnapshotContent": vsc_name,
            "volumeSnapshotContentUid": vsc_meta.get("uid"),
            "snapshotHandle": expected_id,
            "deletionPolicy": "Retain",
        }
    return records


def verify_source_locks(ctx: dict[str, Any]) -> dict[str, Any]:
    pair = ctx["pair"]
    evidence: dict[str, Any] = {}
    source_group = az_json(ctx, "group", "show", "-n", pair["resourceGroup"])
    require(source_group.get("name", "").casefold() == pair["resourceGroup"].casefold()
            and source_group.get("tags", {}).get("reprotoken") == pair["resourceGroupOwnerToken"],
            "Pinned snapshot resource group owner tag changed.")
    expected_group_lock = pair["resourceGroupLock"]
    group_locks = az_json(ctx, "lock", "list", "--resource-group", pair["resourceGroup"])
    group_match = [x for x in group_locks if lower(x.get("id")) == lower(expected_group_lock["id"])]
    require(len(group_match) == 1, "Pinned snapshot resource-group retention lock is missing or ambiguous.")
    live_group_lock = group_match[0]
    require(live_group_lock.get("level") == expected_group_lock.get("level") == "CanNotDelete"
            and live_group_lock.get("name") == expected_group_lock.get("name")
            and live_group_lock.get("notes") == expected_group_lock.get("notes"),
            "Pinned snapshot resource-group lock differs from its retained definition.")
    evidence["resourceGroup"] = {"name": pair["resourceGroup"], "lockId": live_group_lock["id"], "level": live_group_lock["level"]}
    for kind in ("data", "wal"):
        expected = pair[kind]
        live = az_json(ctx, "snapshot", "show", "--ids", expected["id"])
        require(lower(live.get("id")) == lower(expected["id"])
                and live.get("uniqueId") == expected["uniqueId"]
                and live.get("provisioningState") == "Succeeded"
                and live.get("snapshotAccessState") == "Available"
                and lower(live.get("location")) == lower(ctx["location"])
                and int(live.get("diskSizeGB", live.get("diskSizeGb", 0))) == 512
                and lower((live.get("creationData") or {}).get("sourceResourceId")) == lower(expected["sourceDiskId"])
                and (live.get("sku") or {}).get("name") == expected["sku"]
                and (live.get("completionPercent") is None or float(live["completionPercent"]) == 100.0),
                f"Pinned {kind.upper()} source snapshot identity/readiness changed.")
        expected_lock = expected["lock"]
        locks = az_json(ctx, "lock", "list", "--resource", expected["id"])
        match = [x for x in locks if lower(x.get("id")) == lower(expected_lock["id"])]
        require(len(match) == 1, f"Pinned {kind.upper()} CanNotDelete lock is missing or ambiguous.")
        live_lock = match[0]
        require(live_lock.get("level") == expected_lock.get("level") == "CanNotDelete"
                and live_lock.get("name") == expected_lock.get("name")
                and live_lock.get("notes") == expected_lock.get("notes"),
                f"Pinned {kind.upper()} source lock differs from its retained definition.")
        evidence[kind] = {"snapshotId": expected["id"], "uniqueId": expected["uniqueId"], "lockId": live_lock["id"], "level": live_lock["level"]}
    return evidence


def verify_cluster_before(ctx: dict[str, Any], expected_store: dict[str, Any] | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    cluster = k_json(ctx, "get", "cluster", CLUSTER, "-n", NAMESPACE)
    meta = cluster.get("metadata", {})
    spec = cluster.get("spec", {})
    require(meta.get("labels", {}).get("repro.stack.token") == ctx["ownerToken"], "CNPG Cluster owner token mismatch.")
    require(spec.get("instances") == 2 and cluster.get("status", {}).get("readyInstances") == 2,
            "The prepared two-instance CNPG Cluster is not ready.")
    sync = spec.get("postgresql", {}).get("synchronous", {})
    require(sync.get("method") == "any" and sync.get("number") == 1 and sync.get("dataDurability") == "preferred",
            "CNPG synchronous replica settings differ from the prepared experiment.")
    require(spec.get("imageName") == SYSTEM_IMAGE, "The Cluster is not using the pinned PG 16.15 system image with Barman Cloud support.")
    recovery = (spec.get("bootstrap") or {}).get("recovery")
    require(not recovery and not spec.get("externalClusters") and not spec.get("plugins"),
            "Snapshot bootstrap/recovery configuration has not been finalized.")
    backup = spec.get("backup") or {}
    validate_backup_store(backup, expected_store)
    ns = k_json(ctx, "get", "volumesnapshots", "-n", NAMESPACE)
    contents = k_json(ctx, "get", "volumesnapshotcontents")
    lineage = lineage_objects(ctx, cluster, ns.get("items", []), contents.get("items", []))
    return cluster, lineage


def verify_cluster_after(ctx: dict[str, Any], expected_destination: str, lineage: dict[str, Any],
                         expected_store: dict[str, Any] | None = None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    cluster = k_json(ctx, "get", "cluster", CLUSTER, "-n", NAMESPACE)
    spec = cluster.get("spec", {})
    store = (spec.get("backup") or {}).get("barmanObjectStore") or {}
    require(cluster.get("metadata", {}).get("labels", {}).get("repro.stack.token") == ctx["ownerToken"], "CNPG Cluster owner changed.")
    require(spec.get("imageName") == SYSTEM_IMAGE, "CNPG image changed during archive setup.")
    if expected_store is not None:
        require(store == expected_store and store.get("destinationPath") == expected_destination,
                "CNPG Barman archive configuration does not exactly match the stack-owned resume receipt.")
    else:
        require(store.get("destinationPath") == expected_destination and store.get("serverName") == CLUSTER,
                "CNPG Barman archive destination/server name did not verify.")
    cred = (store.get("azureCredentials") or {}).get("connectionString") or {}
    require(cred == {"name": SECRET_NAME, "key": SECRET_KEY}, "CNPG Azure credential Secret reference changed.")
    require(not spec.get("externalClusters") and not spec.get("plugins"), "Unexpected alternate CNPG recovery source was added.")
    pods = k_json(ctx, "get", "pods", "-n", NAMESPACE, "-l", f"cnpg.io/cluster={CLUSTER}").get("items", [])
    ready = [p for p in pods if p.get("status", {}).get("phase") == "Running"
             and any(c.get("name") == "postgres" and c.get("ready") for c in p.get("status", {}).get("containerStatuses", []))]
    digest = SYSTEM_IMAGE.split("@", 1)[1]
    require(cluster.get("status", {}).get("readyInstances") == 2 and len(ready) == 2,
            "Both CNPG instances must be ready after the archive configuration rollout.")
    for pod in ready:
        require(pod.get("metadata", {}).get("labels", {}).get("repro.stack.token") == ctx["ownerToken"], "PostgreSQL pod owner token mismatch.")
        postgres = [c for c in pod.get("status", {}).get("containerStatuses", []) if c.get("name") == "postgres"]
        require(len(postgres) == 1 and postgres[0].get("imageID", "").endswith(digest),
                "A PostgreSQL pod is not running the pinned system image digest.")
    now_ns = k_json(ctx, "get", "volumesnapshots", "-n", NAMESPACE)
    now_contents = k_json(ctx, "get", "volumesnapshotcontents")
    after_lineage = lineage_objects(ctx, cluster, now_ns.get("items", []), now_contents.get("items", []))
    require(after_lineage == lineage, "Snapshot lineage changed during WAL archive setup.")
    return cluster, ready


def sql(ctx: dict[str, Any], pod: str, query: str, *, timeout: int = 180) -> str:
    argv = k_argv(ctx, "exec", "-n", NAMESPACE, pod, "-c", "postgres", "--",
                  "psql", "-XAtq", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "repro", "-c", query)
    return run(argv, timeout=timeout).stdout.strip()


def sql_json(ctx: dict[str, Any], pod: str, query: str) -> dict[str, Any]:
    try:
        value = json.loads(sql(ctx, pod, query))
    except json.JSONDecodeError as exc:
        raise Refusal("PostgreSQL did not return the expected archive-smoke JSON.") from exc
    require(isinstance(value, dict), "PostgreSQL archive-smoke result is not an object.")
    return value


def copy_primary_wal(ctx: dict[str, Any], pod: str, wal_name: str, destination: Path) -> None:
    require(bool(WAL_RE.fullmatch(wal_name)), "Refusing an invalid WAL file path.")
    remote_file = f"/var/lib/postgresql/data/pgdata/pg_wal/{wal_name}"
    hash_argv = k_argv(ctx, "exec", "-n", NAMESPACE, pod, "-c", "postgres", "--", "sha256sum", remote_file)
    expected_hash = run(hash_argv, timeout=120).stdout.split()[0]
    require(bool(re.fullmatch(r"[a-f0-9]{64}", expected_hash)), "Invalid remote closed-WAL hash.")
    # Large API WebSocket streams can reset after partial output. Each bounded
    # read is independently retried and checked, then the full bytes are hashed.
    with destination.open("wb") as target:
        os.chmod(destination, 0o600)
        for offset_mib in range(0, 16, 2):
            argv = k_argv(ctx, "exec", "-n", NAMESPACE, pod, "-c", "postgres", "--", "dd",
                          f"if={remote_file}", "bs=1048576", f"skip={offset_mib}", "count=2", "status=none")
            for attempt in range(1, 4):
                try:
                    result = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60, check=False)
                    code, data = result.returncode, result.stdout
                    error = result.stderr.decode(errors="replace")[:4000]
                except subprocess.TimeoutExpired:
                    code, data, error = -1, b"", "kubectl WAL chunk copy timed out"
                receipt = ctx["stateDir"] / "wal-archive-copy-attempts.jsonl"
                with receipt.open("a") as stream:
                    os.chmod(receipt, 0o600)
                    stream.write(json.dumps({"atUtc": datetime.now(timezone.utc).isoformat(),
                                             "walName": wal_name, "offsetMiB": offset_mib,
                                             "attempt": attempt, "exitCode": code,
                                             "bytes": len(data), "error": error}) + "\n")
                if code == 0 and len(data) == 2 * MIB:
                    target.write(data)
                    break
                if attempt < 3:
                    time.sleep(2)
            else:
                raise Refusal("Could not read an exact WAL chunk after three bounded attempts; private copy receipts retained.")
        target.flush()
        os.fsync(target.fileno())
    after_hash = run(hash_argv, timeout=120).stdout.split()[0]
    require(destination.stat().st_size == 16 * MIB and sha256_file(destination) == expected_hash == after_hash,
            "Closed WAL changed or its assembled local bytes failed the remote SHA-256 check.")


def azure_connection_string(ctx: dict[str, Any]) -> str:
    result = run(az_argv(ctx, "storage", "account", "show-connection-string", "-g", ctx["resourceGroup"],
                        "-n", ctx["accountName"], "--query", "connectionString", "-o", "tsv"),
                 sensitive=True, timeout=120)
    connection = result.stdout.strip()
    require("AccountName=" in connection and "AccountKey=" in connection,
            "Azure CLI returned an unexpected storage connection string.")
    return connection


def blob_list(ctx: dict[str, Any], connection: str, prefix: str) -> list[dict[str, Any]]:
    env = os.environ.copy()
    env["AZURE_STORAGE_CONNECTION_STRING"] = connection
    result = run(az_argv(ctx, "storage", "blob", "list", "--container-name", ctx["containerName"],
                         "--account-name", ctx["accountName"], "--prefix", prefix, "--only-show-errors", "-o", "json"),
                 env=env, sensitive=True, timeout=180)
    try:
        rows = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise Refusal("Azure CLI returned invalid Blob listing JSON (details suppressed).") from exc
    require(isinstance(rows, list), "Azure Blob listing is not an array.")
    return rows


def download_blob(ctx: dict[str, Any], connection: str, name: str, path: Path) -> None:
    env = os.environ.copy()
    env["AZURE_STORAGE_CONNECTION_STRING"] = connection
    run(az_argv(ctx, "storage", "blob", "download", "--container-name", ctx["containerName"],
                "--account-name", ctx["accountName"], "--name", name, "--file", str(path),
                "--overwrite", "true", "--only-show-errors", "-o", "none"),
        env=env, sensitive=True, timeout=300)
    require(path.is_file() and path.stat().st_size > 0, "Azure CLI download did not create the expected WAL object.")


def restore_with_cnpg(ctx: dict[str, Any], pod: str, wal_name: str, remote_path: str, remote_log: str) -> dict[str, Any]:
    require(bool(WAL_RE.fullmatch(wal_name)), "Refusing an invalid WAL restore request.")
    validate_restore_destination(remote_path)
    validate_restore_log_destination(remote_log)
    try:
        run(k_argv(ctx, "exec", "-n", NAMESPACE, pod, "-c", "postgres", "--", "test", "!", "-e", remote_path), timeout=120)
        run(k_argv(ctx, "exec", "-n", NAMESPACE, pod, "-c", "postgres", "--",
                   "/controller/manager", "wal-restore", "--rewind", "--log-destination", remote_log, wal_name, remote_path), timeout=300)
        result = run(k_argv(ctx, "exec", "-n", NAMESPACE, pod, "-c", "postgres", "--", "sha256sum", remote_path), timeout=120)
        match = re.fullmatch(r"([a-f0-9]{64})\s+\S+\s*", result.stdout)
        require(match is not None, "CNPG manager restore did not produce a readable destination file.")
        size_result = run(k_argv(ctx, "exec", "-n", NAMESPACE, pod, "-c", "postgres", "--", "stat", "-c", "%s", remote_path), timeout=120)
        try:
            size = int(size_result.stdout.strip())
        except ValueError as exc:
            raise Refusal("CNPG manager restore destination has an invalid size.") from exc
        return {"sha256": match.group(1), "bytes": size, "destination": "temporary-file-outside-PGDATA"}
    finally:
        run(k_argv(ctx, "exec", "-n", NAMESPACE, pod, "-c", "postgres", "--", "rm", "-f", remote_path, remote_log),
            timeout=120, allow_failure=True)


def run_smoke(ctx: dict[str, Any], connection: str, lineage: dict[str, Any], timeout_seconds: int) -> dict[str, Any]:
    sd = ctx["stateDir"]
    tmp_dir = Path(tempfile.mkdtemp(prefix=".wal-archive-smoke.", dir=sd))
    os.chmod(tmp_dir, 0o700)
    token = ctx["ownerToken"]
    try:
        cluster = k_json(ctx, "get", "cluster", CLUSTER, "-n", NAMESPACE)
        pod = cluster.get("status", {}).get("currentPrimary", "")
        require(pod == f"{CLUSTER}-1", "Expected the prepared primary to remain hdd-pg-1 before client startup.")
        pod_items = k_json(ctx, "get", "pods", "-n", NAMESPACE, "-l", f"cnpg.io/cluster={CLUSTER}").get("items", [])
        pod_obj = next((p for p in pod_items if p.get("metadata", {}).get("name") == pod), None)
        require(pod_obj is not None, "Current primary pod metadata is missing.")
        pod_uid = pod_obj.get("metadata", {}).get("uid", "")
        replicas = [p for p in pod_items
                    if p.get("metadata", {}).get("labels", {}).get("cnpg.io/instanceRole") == "replica"
                    and p.get("status", {}).get("phase") == "Running"
                    and any(c.get("name") == "postgres" and c.get("ready")
                            for c in p.get("status", {}).get("containerStatuses", []))]
        require(len(replicas) == 1, "WAL restore smoke requires exactly one ready CNPG replica.")
        restore_pod_obj = replicas[0]
        restore_pod = restore_pod_obj.get("metadata", {}).get("name", "")
        restore_pod_uid = restore_pod_obj.get("metadata", {}).get("uid", "")
        require(restore_pod and restore_pod != pod, "WAL restore smoke replica identity is invalid.")
        require(restore_pod_obj.get("metadata", {}).get("labels", {}).get("repro.stack.token") == token,
                "WAL restore smoke replica owner token mismatch.")
        restore_containers = [c for c in restore_pod_obj.get("status", {}).get("containerStatuses", []) if c.get("name") == "postgres"]
        require(len(restore_containers) == 1 and restore_containers[0].get("imageID", "").endswith(SYSTEM_IMAGE.split("@", 1)[1]),
                "WAL restore smoke replica is not running the pinned system image digest.")

        pre = sql_json(ctx, pod, "SELECT json_build_object('archivedCount',archived_count,'failedCount',failed_count,'activeAppSessions',(SELECT count(*) FROM pg_stat_activity WHERE usename='app' AND backend_type='client backend'),'activeVacuum',(SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend' AND query ~* '^\\s*VACUUM'))::text FROM pg_stat_archiver;")
        require(pre.get("activeAppSessions") == 0 and pre.get("activeVacuum") == 0,
                "WAL archive smoke requires no client activity or VACUUM in progress.")
        label = "wal_archive_smoke_" + token[:8]
        created = sql_json(ctx, pod, f"WITH restore_point AS (SELECT pg_create_restore_point('{label}') AS lsn) SELECT json_build_object('lsn',lsn,'segmentSize',current_setting('wal_segment_size'),'restorePointWal',pg_walfile_name(lsn))::text FROM restore_point;")
        require(created.get("lsn") and created.get("segmentSize") == "16MB" and WAL_RE.fullmatch(str(created.get("restorePointWal", ""))) is not None,
                "Could not create a known 16-MiB WAL segment for the archive smoke.")
        switched = sql_json(ctx, pod, "WITH switched AS (SELECT pg_switch_wal() AS lsn) SELECT json_build_object('switchLsn',lsn,'closedWal',pg_walfile_name(lsn-1))::text FROM switched;")
        wal_name = str(switched.get("closedWal", ""))
        require(switched.get("switchLsn") and wal_name == created["restorePointWal"] and WAL_RE.fullmatch(wal_name) is not None,
                "The known WAL segment was not the segment just closed by pg_switch_wal().")

        local_path = tmp_dir / "primary-wal-segment"
        copy_primary_wal(ctx, pod, wal_name, local_path)
        local_size = local_path.stat().st_size
        require(local_size == 16 * MIB, f"Primary WAL segment size was {local_size}, expected 16 MiB.")
        local_hash = sha256_file(local_path)

        expected_blob = expected_blob_name(token, CLUSTER, wal_name)
        prefix = expected_blob.rsplit("/", 1)[0] + "/"
        deadline = time.monotonic() + timeout_seconds
        archive_after: dict[str, Any] = {}
        selected: dict[str, Any] | None = None
        successful_counter_polls = 0
        while time.monotonic() < deadline:
            archive_after = sql_json(ctx, pod, "SELECT json_build_object('archivedCount',archived_count,'failedCount',failed_count,'lastWal',last_archived_wal,'lastArchivedTime',last_archived_time)::text FROM pg_stat_archiver;")
            rows = blob_list(ctx, connection, prefix)
            selected = next((x for x in rows if x.get("name") == expected_blob), None)
            counters_advanced = archiver_counts_advanced(pre, archive_after)
            if selected is not None and counters_advanced:
                successful_counter_polls += 1
                if successful_counter_polls >= 2:
                    break
                time.sleep(5)
                continue
            else:
                successful_counter_polls = 0
            # Azure can expose the Blob before pg_stat_archiver's stats cache
            # reflects the successful archive. Keep polling rather than
            # treating that transient view as a failed smoke.
            if selected is not None and not counters_advanced:
                time.sleep(5)
                continue
            time.sleep(10)
        require(selected is not None, "The exact test WAL segment did not appear in the stack's private Azure Blob container within 15 minutes.")
        require(archiver_counts_advanced(pre, archive_after),
                "PostgreSQL archiver counters did not confirm a successful new archive.")

        azure_path = tmp_dir / "azure-cli-downloaded-wal"
        download_blob(ctx, connection, expected_blob, azure_path)
        azure_size = azure_path.stat().st_size
        azure_hash = sha256_file(azure_path)
        require(azure_size == local_size and azure_hash == local_hash,
                "The independently downloaded Azure Blob bytes do not match the primary's closed WAL segment.")
        retained_wal_path = retain_smoke_wal(sd, wal_name, azure_path, azure_hash)

        remote_path = f"/run/wal-archive-smoke-{token[:12]}-{wal_name}"
        remote_log = f"/run/wal-archive-smoke-{token[:12]}.json"
        restored = restore_with_cnpg(ctx, restore_pod, wal_name, remote_path, remote_log)
        require(restored["bytes"] == local_size and restored["sha256"] == local_hash,
                "CNPG manager wal-restore did not fetch the exact archived bytes.")
        require_matching_smoke_hashes(local_hash, azure_hash, restored["sha256"])
        final_lineage = verify_live_lineage(ctx, cluster=None)
        require(final_lineage == lineage, "Source snapshot VolumeSnapshot lineage changed during the WAL smoke.")
        return {
            "version": 1,
            "status": "complete",
            "verifiedAtUtc": datetime.now(timezone.utc).isoformat(),
            "ownerToken": token,
            "stack": ctx["stack"],
            "cluster": f"{NAMESPACE}/{CLUSTER}",
            "primaryPod": pod,
            "primaryPodUid": pod_uid,
            "primaryImage": SYSTEM_IMAGE,
            "restorePod": restore_pod,
            "restorePodUid": restore_pod_uid,
            "restorePodRole": "replica",
            "restoreSmokePrefetchDisabled": True,
            "restoreSmokeDestination": "isolated writable /run scratch outside PGDATA",
            "restorePodImage": SYSTEM_IMAGE,
            "systemIdentifier": ctx["prepare"]["systemIdentifier"],
            "serverName": CLUSTER,
            "walName": wal_name,
            "archiveObject": expected_blob,
            "bytes": local_size,
            "retainedLocalWALFile": retained_wal_path,
            "retainedLocalWALSha256": azure_hash,
            "hashes": {"closedPrimarySegmentSha256": local_hash, "independentAzureDownloadSha256": azure_hash,
                       "cnpgManagerWalRestoreSha256": restored["sha256"]},
            "archiver": {"before": pre, "after": archive_after},
            "checks": {"blobListedByAzureCli": True, "blobDownloadedByAzureCli": True,
                       "cnpgManagerWalRestoreSucceeded": True, "allThreeHashesMatch": True,
                       "restoreDestinationOutsidePgData": True, "livePgWalTargeted": False,
                       "sourceSnapshotsReadOnly": True, "sourceLocksPreserved": True,
                       "snapshotLineagePreserved": True},
            "snapshotLineage": lineage,
        }
    finally:
        for child in tmp_dir.iterdir():
            child.unlink(missing_ok=True)
        tmp_dir.rmdir()


def verify_live_lineage(ctx: dict[str, Any], cluster: dict[str, Any] | None) -> dict[str, Any]:
    if cluster is None:
        cluster = k_json(ctx, "get", "cluster", CLUSTER, "-n", NAMESPACE)
    snapshots = k_json(ctx, "get", "volumesnapshots", "-n", NAMESPACE).get("items", [])
    contents = k_json(ctx, "get", "volumesnapshotcontents").get("items", [])
    return lineage_objects(ctx, cluster, snapshots, contents)


def save_storage_metadata(ctx: dict[str, Any], status: str, account_id: str | None = None) -> None:
    atomic_json(ctx["stateDir"] / "storage.json", {
        "version": 1,
        "status": status,
        "subscriptionId": ctx["subscriptionId"],
        "resourceGroup": ctx["resourceGroup"],
        "location": ctx["location"],
        "ownerToken": ctx["ownerToken"],
        "accountName": ctx["accountName"],
        "accountId": account_id,
        "containerName": ctx["containerName"],
        "publicAccess": None,
        "purpose": "private-cnpg-wal-archive",
        "secret": {"namespace": NAMESPACE, "name": SECRET_NAME, "key": SECRET_KEY},
    })


def existing_storage_metadata(ctx: dict[str, Any]) -> dict[str, Any] | None:
    path = ctx["stateDir"] / "storage.json"
    if not path.exists():
        return None
    value = read_json(path)
    require(value.get("ownerToken") == ctx["ownerToken"]
            and value.get("subscriptionId") == ctx["subscriptionId"]
            and value.get("resourceGroup") == ctx["resourceGroup"]
            and value.get("accountName") == ctx["accountName"]
            and value.get("containerName") == ctx["containerName"]
            and value.get("purpose") == "private-cnpg-wal-archive"
            and value.get("publicAccess") is None
            and value.get("status") in ("creating", "ready"),
            "Existing storage metadata does not prove ownership by this exact stack.")
    return value


def check_account_absence(ctx: dict[str, Any]) -> bool:
    result = run(az_argv(ctx, "storage", "account", "show", "-g", ctx["resourceGroup"], "-n", ctx["accountName"], "-o", "json"),
                 allow_failure=True, timeout=120)
    if result.returncode == 0:
        try:
            live = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise Refusal("Existing Azure Storage account show returned invalid JSON.") from exc
        require(account_matches(live, ctx), "A same-name Azure Storage account exists but its exact owner/security scope does not match.")
        return False
    stderr = (result.stderr or "").casefold()
    require(any(marker in stderr for marker in ("resourcenotfound", "not found", "could not be found", "404")),
            "Could not prove the token-scoped Storage account absent or owned.")
    return True


def ensure_account(ctx: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    sub = ctx["subscriptionId"]
    active = run(["az", "account", "show", "--query", "id", "-o", "tsv"], timeout=60).stdout.strip()
    require(active.casefold() == sub.casefold(), "Active Azure subscription differs from the stack.")
    group = az_json(ctx, "group", "show", "-n", ctx["resourceGroup"])
    require(group.get("tags", {}).get("reprotoken") == ctx["ownerToken"], "Stack Azure resource group ownership tag changed.")
    provider = run(az_argv(ctx, "provider", "show", "--namespace", "Microsoft.Storage", "--query", "registrationState", "-o", "tsv"), timeout=60).stdout.strip()
    require(provider == "Registered", "Microsoft.Storage provider must already be registered; this helper will not make subscription-wide changes.")

    metadata = existing_storage_metadata(ctx)
    absent = check_account_absence(ctx)
    require(absent or metadata is not None,
            "Storage account already exists without a local ownership receipt; refusing to adopt it.")
    if absent:
        save_storage_metadata(ctx, "creating")
        run(az_argv(ctx, "storage", "account", "create", "-g", ctx["resourceGroup"], "-n", ctx["accountName"],
                    "-l", ctx["location"], "--kind", "StorageV2", "--sku", "Standard_LRS",
                    "--https-only", "true", "--allow-blob-public-access", "false", "--min-tls-version", "TLS1_2",
                    "--tags", f"reprotoken={ctx['ownerToken']}", "reproStack=walreceiver-sigterm",
                    "reproPurpose=private-cnpg-wal-archive", "-o", "none"), timeout=1800)
    deadline = time.monotonic() + 2400
    account: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        if not check_account_absence(ctx):
            account = az_json(ctx, "storage", "account", "show", "-g", ctx["resourceGroup"], "-n", ctx["accountName"])
            if account.get("provisioningState") == "Succeeded":
                break
        time.sleep(10)
    require(account is not None and account_matches(account, ctx), "Owned private StorageV2 account failed identity/security verification.")
    account_id = account["id"]
    connection = azure_connection_string(ctx)
    env = os.environ.copy()
    env["AZURE_STORAGE_CONNECTION_STRING"] = connection
    exists = run(az_argv(ctx, "storage", "container", "exists", "--name", ctx["containerName"],
                        "--account-name", ctx["accountName"], "-o", "json"), env=env, sensitive=True, timeout=120)
    try:
        container_exists = json.loads(exists.stdout).get("exists") is True
    except (json.JSONDecodeError, AttributeError) as exc:
        raise Refusal("Azure CLI returned invalid container-exists output (details suppressed).") from exc
    if not container_exists:
        run(az_argv(ctx, "storage", "container", "create", "--name", ctx["containerName"],
                    "--account-name", ctx["accountName"], "--public-access", "off",
                    "--metadata", f"reprotoken={ctx['ownerToken']}", "reprostack=walreceiver-sigterm",
                    "--fail-on-exist", "--only-show-errors", "-o", "none"), env=env, sensitive=True, timeout=300)
    container = run(az_argv(ctx, "storage", "container", "show", "--name", ctx["containerName"],
                            "--account-name", ctx["accountName"], "-o", "json"), env=env, sensitive=True, timeout=120)
    try:
        container_json = json.loads(container.stdout)
    except json.JSONDecodeError as exc:
        raise Refusal("Azure CLI returned invalid container metadata (details suppressed).") from exc
    require(container_json.get("name") == ctx["containerName"]
            and (container_json.get("metadata") or {}).get("reprotoken") == ctx["ownerToken"]
            and (container_json.get("metadata") or {}).get("reprostack") == "walreceiver-sigterm"
            and container_json.get("publicAccess") in (None, "off", "None"),
            "Blob container owner or anonymous-access settings did not verify.")
    save_storage_metadata(ctx, "ready", account_id)
    return connection, account


def ensure_secret(ctx: dict[str, Any], connection: str, *, resume: bool = False) -> None:
    lookup = run(k_argv(ctx, "get", "secret", SECRET_NAME, "-n", NAMESPACE, "-o", "json"), allow_failure=True, timeout=120)
    if lookup.returncode == 0:
        try:
            existing = json.loads(lookup.stdout)
        except json.JSONDecodeError as exc:
            raise Refusal("Existing Azure credential Secret is not readable JSON.") from exc
        require(existing.get("metadata", {}).get("labels", {}).get("repro.stack.token") == ctx["ownerToken"],
                "An unowned Secret already uses the Barman credential name.")
        if resume:
            validate_archive_secret(existing, ctx, connection)
            return
    else:
        error = (lookup.stderr or "").casefold()
        require("notfound" in error or "not found" in error, "Could not prove the Barman credential Secret absent.")
        require(not resume, "The configuring receipt exists but its owned archive Secret is missing; refusing partial adoption.")
    manifest = make_secret_manifest(ctx["ownerToken"], connection)
    payload = json.dumps(manifest, separators=(",", ":"))
    run(k_argv(ctx, "apply", "-f", "-"), stdin=payload, sensitive=True, timeout=120)
    written = k_json(ctx, "get", "secret", SECRET_NAME, "-n", NAMESPACE)
    validate_archive_secret(written, ctx, connection)


def wait_for_archive_ready(ctx: dict[str, Any], destination: str, lineage: dict[str, Any], timeout_seconds: int,
                           expected_store: dict[str, Any] | None = None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    deadline = time.monotonic() + timeout_seconds
    last_error = ""
    while time.monotonic() < deadline:
        try:
            cluster, pods = verify_cluster_after(ctx, destination, lineage, expected_store)
            if cluster.get("status", {}).get("currentPrimary") == f"{CLUSTER}-1":
                return cluster, pods
        except Refusal as exc:
            last_error = str(exc)
        time.sleep(10)
    raise Refusal(f"Cluster did not become ready on the pinned system image with Barman configuration: {last_error}")


def apply_setup(ctx: dict[str, Any], smoke_timeout_seconds: int) -> None:
    sd = ctx["stateDir"]
    for name in ("wal-archive.lock", "test.lock", "teardown.lock"):
        fd = (sd / name).open("a")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            fd.close()
            raise Refusal(f"Cannot set up WAL archive while {name} is locked.") from exc
        # Keep descriptors live for the entire operation.
        if name == "wal-archive.lock": locks = [fd]
        else: locks.append(fd)

    # Source verification is read-only and intentionally has no shared lock-cycle logic.
    source_locks = verify_source_locks(ctx)
    group = az_json(ctx, "group", "show", "-n", ctx["resourceGroup"])
    require(group.get("tags", {}).get("reprotoken") == ctx["ownerToken"], "Stack resource group ownership changed.")
    resume_receipt = ctx.get("resumeArchive")
    resume = resume_receipt is not None
    expected_store = (make_barman_patch(ctx["accountName"], ctx["containerName"], ctx["ownerToken"])
                      ["spec"]["backup"]["barmanObjectStore"] if resume else None)
    before_cluster, lineage = verify_cluster_before(ctx, expected_store)
    before_lineage = verify_live_lineage(ctx, before_cluster)
    require(before_lineage == lineage, "Snapshot lineage preflight is inconsistent.")

    connection, account = ensure_account(ctx)
    ensure_secret(ctx, connection, resume=resume)
    destination = destination_path(ctx["accountName"], ctx["containerName"], ctx["ownerToken"])
    archive_meta_path = sd / "wal-archive.json"
    if resume:
        validate_configuring_receipt(resume_receipt, ctx, account_id=account["id"],
                                     source_locks=source_locks, lineage=lineage)
        configured_at = resume_receipt["configuredAtUtc"]
    else:
        configured_at = datetime.now(timezone.utc).isoformat()
        atomic_json(archive_meta_path, {
            "version": 1,
            "status": "configuring",
            "configuredAtUtc": configured_at,
            "ownerToken": ctx["ownerToken"],
            "subscriptionId": ctx["subscriptionId"],
            "resourceGroup": ctx["resourceGroup"],
            "location": ctx["location"],
            "accountName": ctx["accountName"],
            "accountId": account["id"],
            "containerName": ctx["containerName"],
            "containerPublicAccess": None,
            "destinationPath": destination,
            "serverName": CLUSTER,
            "secretReference": {"namespace": NAMESPACE, "name": SECRET_NAME, "key": SECRET_KEY},
            "postgresImage": SYSTEM_IMAGE,
            "backend": "CNPG 1.30.1 in-tree BarmanObjectStore",
            "plugin": None,
            "sourceSnapshotIds": {"data": ctx["pair"]["data"]["id"], "wal": ctx["pair"]["wal"]["id"]},
            "sourceLockVerification": source_locks,
            "snapshotLineage": lineage,
            "connectionStringStoredIn": f"{NAMESPACE}/{SECRET_NAME}:{SECRET_KEY}",
            "connectionStringPersistedLocally": False,
        })

    # This patches only the backup destination. The image is required to match
    # the previously validated system digest, which carries barman-cloud tools.
    patch = make_barman_patch(ctx["accountName"], ctx["containerName"], ctx["ownerToken"])
    if not resume:
        run(k_argv(ctx, "patch", "cluster", CLUSTER, "-n", NAMESPACE, "--type=merge", "-p", json.dumps(patch, separators=(",", ":"))), timeout=180)
    expected_store = patch["spec"]["backup"]["barmanObjectStore"]
    cluster, pods = wait_for_archive_ready(ctx, destination, lineage, timeout_seconds=1800,
                                           expected_store=expected_store if resume else None)
    require(cluster.get("spec", {}).get("imageName") == SYSTEM_IMAGE, "CNPG Barman archive requires the pinned system image.")
    secret_access = wait_for_secret_access(ctx, pods)
    smoke = run_smoke(ctx, connection, lineage, smoke_timeout_seconds)
    smoke["secretAccessReadiness"] = secret_access
    final_lock_evidence = verify_source_locks(ctx)
    require(final_lock_evidence == source_locks, "Source lock definitions changed during the archive smoke.")
    final_lineage = verify_live_lineage(ctx, cluster)
    require(final_lineage == lineage, "Source snapshot lineage changed during the archive smoke.")
    atomic_json(sd / "wal-archive-smoke.json", smoke)
    atomic_json(archive_meta_path, {
        "version": 1,
        "status": "ready",
        "configuredAtUtc": configured_at,
        "ownerToken": ctx["ownerToken"],
        "subscriptionId": ctx["subscriptionId"],
        "resourceGroup": ctx["resourceGroup"],
        "location": ctx["location"],
        "accountName": ctx["accountName"],
        "accountId": account["id"],
        "containerName": ctx["containerName"],
        "containerPublicAccess": None,
        "destinationPath": destination,
        "serverName": CLUSTER,
        "secretReference": {"namespace": NAMESPACE, "name": SECRET_NAME, "key": SECRET_KEY},
        "postgresImage": SYSTEM_IMAGE,
        "backend": "CNPG 1.30.1 in-tree BarmanObjectStore",
        "plugin": None,
        "smokeReceipt": ".state/wal-archive-smoke.json",
        "smokeStatus": smoke["status"],
        "sourceSnapshotIds": {"data": ctx["pair"]["data"]["id"], "wal": ctx["pair"]["wal"]["id"]},
        "sourceLockVerification": final_lock_evidence,
        "snapshotLineage": final_lineage,
        "connectionStringStoredIn": f"{NAMESPACE}/{SECRET_NAME}:{SECRET_KEY}",
        "connectionStringPersistedLocally": False,
        "accountOwnerScope": ctx["resourceGroup"],
    })


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stack", required=True, choices=("sigterm", "control"))
    parser.add_argument("--apply", action="store_true", help="Create the private archive account, configure CNPG, and run the Blob/restore smoke gate.")
    parser.add_argument("--smoke-timeout-seconds", type=int, default=900)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    require(120 <= args.smoke_timeout_seconds <= 3600, "Smoke timeout must be between 120 and 3600 seconds.")
    ctx = validate_local(args.stack)
    if not args.apply:
        print(json.dumps(build_plan(ctx), indent=2, sort_keys=True))
        return 0
    apply_setup(ctx, args.smoke_timeout_seconds)
    print(json.dumps({"status": "complete", "stack": args.stack,
                      "archiveReceipt": str(ctx["stateDir"] / "wal-archive.json"),
                      "smokeReceipt": str(ctx["stateDir"] / "wal-archive-smoke.json"),
                      "connectionStringPrinted": False}, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Refusal as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
