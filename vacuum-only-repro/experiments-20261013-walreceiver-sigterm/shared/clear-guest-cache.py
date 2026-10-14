#!/usr/bin/env python3
"""Clear Linux guest page cache on the verified primary and replica nodes."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import select
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
STATE_DIR = ROOT / ".state"
EXPECTED_CLUSTER = "hdd-pg"
TOKEN_LABEL = "repro.stack.token"
RUN_LABEL = "repro.guest-cache/run"
UTC = dt.timezone.utc


def now() -> str:
    return dt.datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def invoke(argv: list[str], *, input_text: str | None = None, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, input=input_text, text=True, capture_output=True, timeout=timeout, check=False)


def checked(argv: list[str], *, input_text: str | None = None, timeout: int = 120) -> str:
    result = invoke(argv, input_text=input_text, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"{argv[0]} command failed ({result.returncode}): {(result.stderr or result.stdout)[-2000:]}")
    return result.stdout


def parse_json(out: str, description: str) -> dict[str, Any]:
    try:
        value = json.loads(out)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid JSON returned for {description}: {out[-1000:]}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"{description} did not return a JSON object")
    return value


def normalize_azure_id(value: str) -> str:
    value = value.strip()
    if value.lower().startswith("azure://"):
        value = value[len("azure://"):]
    return value.rstrip("/")


def azure_subscription_and_rg(resource_id: str) -> tuple[str, str]:
    parts = normalize_azure_id(resource_id).strip("/").split("/")
    if (len(parts) < 8 or parts[0].casefold() != "subscriptions"
            or parts[2].casefold() != "resourcegroups" or parts[4].casefold() != "providers"
            or parts[5].casefold() != "microsoft.compute"):
        raise RuntimeError(f"verified node provider ID is not a Compute resource ID: {resource_id}")
    provider_path = [x.casefold() for x in parts[6:]]
    vm = (provider_path[0] == "virtualmachines" and len(provider_path) == 2) or (
        provider_path[0] == "virtualmachinescalesets" and len(provider_path) == 4
        and provider_path[2] == "virtualmachines")
    if not vm:
        raise RuntimeError(f"verified node provider ID is not a VM/VMSS instance: {resource_id}")
    return parts[1], parts[3]


def kubectl_base(args: argparse.Namespace) -> list[str]:
    return [args.kubectl, "--kubeconfig", str(args.kubeconfig)]


def verified_targets(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    state_path = STATE_DIR / "state.json"
    manifest_path = STATE_DIR / "attachment-verification" / "verified.json"
    if not state_path.is_file() or not manifest_path.is_file():
        raise RuntimeError("provision state or verified attachment manifest is missing")
    state = json.loads(state_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    token = state.get("ownerToken")
    node_rg = state.get("nodeResourceGroup")
    subscription = state.get("subscriptionId")
    if state.get("status") != "ready" or not re.fullmatch(r"[a-f0-9]{32}", str(token or "")):
        raise RuntimeError("AKS state is not ready or its ownership token is invalid")
    if not node_rg or not subscription:
        raise RuntimeError("AKS state has no verified node resource group/subscription")
    if manifest.get("passed") is not True:
        raise RuntimeError("attachment verification did not pass")
    roles = manifest.get("roles")
    if not isinstance(roles, dict) or set(roles) != {"primary", "replica"}:
        raise RuntimeError("verified attachment manifest does not identify both PostgreSQL roles")

    base = kubectl_base(args)
    namespace = parse_json(checked(base + ["get", "namespace", args.namespace, "-o", "json"]), "namespace")
    if namespace.get("metadata", {}).get("labels", {}).get(TOKEN_LABEL) != token:
        raise RuntimeError("namespace ownership token does not match provision state")
    node_items = parse_json(checked(base + ["get", "nodes", "-o", "json"]), "nodes").get("items", [])
    nodes = {n.get("metadata", {}).get("name"): n for n in node_items}
    pod_items = parse_json(checked(base + ["get", "pods", "-n", args.namespace,
                                            "-l", "cnpg.io/cluster=" + EXPECTED_CLUSTER, "-o", "json"]), "cluster pods").get("items", [])
    current_roles: dict[str, dict[str, Any]] = {}
    for pod in pod_items:
        labels = pod.get("metadata", {}).get("labels", {})
        role = labels.get("cnpg.io/instanceRole")
        if role in ("primary", "replica"):
            if role in current_roles:
                raise RuntimeError(f"duplicate {role} pod while preparing cache clear")
            current_roles[role] = pod
    if set(current_roles) != {"primary", "replica"}:
        raise RuntimeError("exactly one primary and one replica are required for cache clearing")

    result: dict[str, dict[str, Any]] = {}
    for role in ("primary", "replica"):
        expected = roles[role]
        pod = current_roles[role]
        meta = pod.get("metadata", {})
        labels = meta.get("labels", {})
        pod_name = expected.get("pod")
        node_name = expected.get("node")
        node = nodes.get(node_name)
        if not node:
            raise RuntimeError(f"verified {role} node is missing: {node_name}")
        if meta.get("name") != pod_name or pod.get("spec", {}).get("nodeName") != node_name:
            raise RuntimeError(f"live {role} pod/node differs from attachment verification")
        if labels.get(TOKEN_LABEL) != token:
            raise RuntimeError(f"live {role} pod ownership token does not match provision state")
        if not meta.get("uid"):
            raise RuntimeError(f"live {role} PostgreSQL pod has no UID for identity verification")
        if pod.get("status", {}).get("phase") != "Running" or not any(
                c.get("type") == "Ready" and c.get("status") == "True"
                for c in pod.get("status", {}).get("conditions", [])):
            raise RuntimeError(f"live {role} PostgreSQL pod is not Running/Ready")
        provider_id = normalize_azure_id(node.get("spec", {}).get("providerID", ""))
        verified_id = normalize_azure_id(expected.get("vmResourceId", ""))
        if not provider_id or provider_id.casefold() != verified_id.casefold():
            raise RuntimeError(f"{role} node providerID differs from verified Azure VM ID")
        provider_sub, provider_rg = azure_subscription_and_rg(provider_id)
        if provider_sub.casefold() != subscription.casefold() or provider_rg.casefold() != node_rg.casefold():
            raise RuntimeError(f"{role} node providerID is outside the owned subscription/node resource group")
        if node.get("metadata", {}).get("labels", {}).get("node.kubernetes.io/instance-type") != state.get("nodeVmSize"):
            raise RuntimeError(f"{role} node VM size differs from provision state")
        postgres = next((c for c in pod.get("spec", {}).get("containers", []) if c.get("name") == "postgres"), None)
        if not postgres or not postgres.get("image"):
            raise RuntimeError(f"{role} PostgreSQL image cannot be used for the local cache-clear pod")
        result[role] = {"pod": pod_name, "node": node_name, "vmResourceId": verified_id,
                        "postgresImage": postgres["image"], "sourcePodUid": meta.get("uid")}
    if result["primary"]["node"] == result["replica"]["node"]:
        raise RuntimeError("primary and replica do not use distinct verified nodes")
    return state, result


def cache_clear_pod(args: argparse.Namespace, token: str, run_id: str,
                    role: str, target: dict[str, Any]) -> dict[str, Any]:
    name = f"guest-cache-{role}-{token[:8]}"
    script = r'''set -eu
sync
printf '%s\n' '=== GUEST_CACHE_BEFORE ==='
printf 'time_utc='; date -u +'%Y-%m-%dT%H:%M:%S.%NZ'
printf 'role=%s\nnode=%s\npod=%s\npod_uid=%s\n' "$ROLE" "$NODE_NAME" "$POD_NAME" "$POD_UID"
cat /host-proc/meminfo
printf '%s\n' '=== GUEST_CACHE_CLEAR ==='
printf 3 > /host-proc/sys/vm/drop_caches
sync
printf '%s\n' '=== GUEST_CACHE_AFTER ==='
printf 'time_utc='; date -u +'%Y-%m-%dT%H:%M:%S.%NZ'
printf 'role=%s\nnode=%s\npod=%s\npod_uid=%s\n' "$ROLE" "$NODE_NAME" "$POD_NAME" "$POD_UID"
cat /host-proc/meminfo
'''
    return {
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "namespace": args.namespace,
                     "labels": {TOKEN_LABEL: token, RUN_LABEL: run_id,
                                "app.kubernetes.io/name": "vacuum-guest-cache-clear",
                                "vacuum-only-repro/role": role}},
        "spec": {
            "nodeName": target["node"], "hostPID": True, "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "containers": [{
                "name": "clear-cache", "image": target["postgresImage"], "imagePullPolicy": "IfNotPresent",
                "command": ["/bin/sh", "-e", "-u", "-c", script],
                "env": [{"name": "ROLE", "value": role},
                        {"name": "NODE_NAME", "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}},
                        {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
                        {"name": "POD_UID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}}}],
                "securityContext": {"privileged": True, "runAsUser": 0},
                "volumeMounts": [{"name": "host-proc", "mountPath": "/host-proc", "readOnly": False}],
            }],
            "volumes": [{"name": "host-proc", "hostPath": {"path": "/proc", "type": "Directory"}}],
        },
    }


def get_owned_pod(base: list[str], namespace: str, record: dict[str, Any], token: str,
                  run_id: str) -> dict[str, Any] | None:
    result = invoke(base + ["get", "pod", "-n", namespace, record["pod"], "-o", "json"])
    if result.returncode:
        if "notfound" in (result.stderr + result.stdout).casefold():
            return None
        raise RuntimeError(f"cannot recheck temporary pod {record['pod']}: {(result.stderr or result.stdout)[-1500:]}")
    pod = parse_json(result.stdout, "temporary cache-clear pod")
    meta = pod.get("metadata", {})
    labels = meta.get("labels", {})
    if meta.get("uid") != record.get("uid") or labels.get(TOKEN_LABEL) != token or labels.get(RUN_LABEL) != run_id:
        raise RuntimeError(f"refusing to read/delete temporary pod with changed UID or ownership: {record['pod']}")
    if pod.get("spec", {}).get("nodeName") != record.get("node"):
        raise RuntimeError(f"temporary pod moved from verified node: {record['pod']}")
    return pod


def retrieve_logs(base: list[str], namespace: str, record: dict[str, Any], token: str,
                  run_id: str, log_path: Path) -> str:
    pod = get_owned_pod(base, namespace, record, token, run_id)
    if pod is None:
        log_path.write_text("Temporary cache-clear pod disappeared before logs were retrieved.\n", encoding="utf-8")
        raise RuntimeError(f"temporary pod disappeared before logs were retrieved: {record['pod']}")
    result = invoke(base + ["logs", "-n", namespace, record["pod"], "-c", "clear-cache"], timeout=120)
    contents = result.stdout
    if result.stderr:
        contents += "\n[ kubectl logs stderr ]\n" + result.stderr
    log_path.write_text(contents, encoding="utf-8")
    record["logsRetrieved"] = result.returncode == 0
    record["localLog"] = str(log_path)
    if result.returncode:
        raise RuntimeError(f"could not retrieve {record['role']} guest-cache log: {result.stderr[-1500:]}")
    if "=== GUEST_CACHE_BEFORE ===" not in contents or "=== GUEST_CACHE_AFTER ===" not in contents:
        raise RuntimeError(f"{record['role']} guest-cache log lacks before/after host meminfo markers")
    return contents


def delete_owned_pod(base: list[str], namespace: str, record: dict[str, Any], token: str,
                     run_id: str) -> None:
    pod = get_owned_pod(base, namespace, record, token, run_id)
    if pod is None:
        record["deleted"] = True
        return
    uid = str(record["uid"])
    proxy = subprocess.Popen(
        base + ["proxy", "--address=127.0.0.1", "--port=0"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
    )
    try:
        startup_lines: list[str] = []
        port: int | None = None
        startup_deadline = time.monotonic() + 10
        while proxy.stdout is not None and time.monotonic() < startup_deadline:
            remaining = max(0.0, startup_deadline - time.monotonic())
            if not select.select([proxy.stdout], [], [], remaining)[0]:
                break
            startup = proxy.stdout.readline().strip()
            startup_lines.append(startup)
            match = re.search(r"(?:127\.0\.0\.1|localhost):(\d+)", startup)
            if match and int(match.group(1)) != 0:
                port = int(match.group(1))
                break
            if proxy.poll() is not None:
                break
        if port is None:
            raise RuntimeError(f"kubectl proxy did not report a local port: {startup_lines[-5:]}")
        path = "/api/v1/namespaces/{}/pods/{}".format(
            urllib.parse.quote(namespace, safe=""),
            urllib.parse.quote(str(record["pod"]), safe=""),
        )
        url = f"http://127.0.0.1:{port}{path}"
        delete_options = {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "gracePeriodSeconds": 30,
            "preconditions": {"uid": uid},
        }
        request = urllib.request.Request(
            url,
            data=json.dumps(delete_options).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="DELETE",
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                status = response.status
                body = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"UID-preconditioned deletion failed for {record['pod']} "
                f"(HTTP {exc.code}): {body[-1500:]}"
            ) from exc
        if status not in (200, 202):
            raise RuntimeError(
                f"UID-preconditioned deletion returned HTTP {status} for {record['pod']}: {body[-1500:]}"
            )
    finally:
        if proxy.poll() is None:
            proxy.terminate()
            try:
                proxy.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proxy.kill()
                proxy.wait(timeout=5)

    deadline = time.monotonic() + 90
    while True:
        after = get_owned_pod(base, namespace, record, token, run_id)
        if after is None:
            record["deleted"] = True
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(f"temporary cache-clear pod remains after deletion: {record['pod']}")
        time.sleep(1)


def execute_role(args: argparse.Namespace, base: list[str], token: str, run_id: str,
                 role: str, target: dict[str, Any], summary: dict[str, Any]) -> None:
    pod_obj = cache_clear_pod(args, token, run_id, role, target)
    name = pod_obj["metadata"]["name"]
    create_result = invoke(base + ["create", "-f", "-", "-o", "json"],
                           input_text=json.dumps(pod_obj), timeout=120)
    if create_result.returncode:
        lookup = invoke(base + ["get", "pod", "-n", args.namespace, name, "-o", "json"])
        if lookup.returncode:
            raise RuntimeError(f"could not create {role} guest-cache pod: {(create_result.stderr or create_result.stdout)[-2000:]}")
        created = parse_json(lookup.stdout, "cache-clear pod after ambiguous create")
        recovered_after_create_error = True
    else:
        created = parse_json(create_result.stdout, "created cache-clear pod")
        recovered_after_create_error = False
    meta = created.get("metadata", {})
    labels = meta.get("labels", {})
    uid = meta.get("uid")
    if (not uid or meta.get("name") != name or labels.get(TOKEN_LABEL) != token
            or labels.get(RUN_LABEL) != run_id or created.get("spec", {}).get("nodeName") != target["node"]):
        raise RuntimeError(f"created {role} cache-clear pod identity/ownership did not verify")
    record = {"role": role, "pod": name, "uid": uid, "node": target["node"],
              "vmResourceId": target["vmResourceId"], "sourcePostgresPod": target["pod"],
              "sourcePostgresPodUid": target["sourcePodUid"], "createdAtUtc": now(),
              "logsRetrieved": False, "deleted": False}
    summary["roles"][role] = record
    if recovered_after_create_error:
        record["createResponseError"] = (create_result.stderr or create_result.stdout)[-2000:]
    save_path = Path(args.out).resolve() / f"{role}-guest-cache.log"
    deadline = time.monotonic() + args.timeout
    phase = ""
    while time.monotonic() < deadline:
        current = get_owned_pod(base, args.namespace, record, token, run_id)
        if current is None:
            raise RuntimeError(f"temporary {role} cache-clear pod disappeared")
        phase = current.get("status", {}).get("phase", "")
        statuses = current.get("status", {}).get("containerStatuses", [])
        waiting = next((x.get("state", {}).get("waiting") for x in statuses
                        if x.get("state", {}).get("waiting")), None)
        if phase in ("Succeeded", "Failed"):
            break
        if waiting and waiting.get("reason") in ("ErrImagePull", "ImagePullBackOff", "CreateContainerConfigError"):
            break
        time.sleep(2)
    try:
        log_contents = retrieve_logs(base, args.namespace, record, token, run_id, save_path)
    except Exception as exc:
        record["logError"] = str(exc)
        raise
    if phase != "Succeeded":
        raise RuntimeError(f"{role} cache-clear pod ended in phase {phase or 'unknown'}")
    current = get_owned_pod(base, args.namespace, record, token, run_id)
    terminations = (current or {}).get("status", {}).get("containerStatuses", [])
    exit_code = next((x.get("state", {}).get("terminated", {}).get("exitCode")
                      for x in terminations if x.get("name") == "clear-cache"), None)
    if exit_code != 0:
        raise RuntimeError(f"{role} guest-cache clear container exited with {exit_code}")
    marker_time = re.findall(r"time_utc=([^\s]+)", log_contents)
    record["guestClearExitCode"] = exit_code
    record["guestCacheClearTimesUtc"] = marker_time[:2]
    record["completedAtUtc"] = now()
    delete_owned_pod(base, args.namespace, record, token, run_id)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", type=Path, default=STATE_DIR / "kubeconfig")
    parser.add_argument("--kubectl", default="kubectl")
    parser.add_argument("--namespace", default="repro")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()
    if args.timeout < 30:
        parser.error("--timeout must be at least 30 seconds")
    args.kubeconfig = args.kubeconfig.resolve()
    if not args.kubeconfig.is_file():
        parser.error(f"kubeconfig is missing: {args.kubeconfig}")
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    manifest_path = out / "guest-cache-clearing.json"
    run_id = uuid.uuid4().hex[:16]
    summary: dict[str, Any] = {
        "phase": "starting", "startedAtUtc": now(), "runId": run_id,
        "preparationChange": "Cleared Linux guest page cache on both verified PostgreSQL nodes with sync and drop_caches=3.",
        "azureReadOnlyHostCachePurged": False,
        "hostCacheNote": "Azure host caching is disabled on verified DATA/WAL disks; this operation clears guest Linux caches after replica seeding.",
        "roles": {}, "errors": [],
    }
    created_records: list[dict[str, Any]] = []
    cleanup_base: list[str] | None = None
    cleanup_token: str | None = None
    try:
        state, targets = verified_targets(args)
        summary.update({"ownerTokenVerified": True, "nodeResourceGroup": state["nodeResourceGroup"],
                        "subscriptionId": state["subscriptionId"], "nodeVmSize": state["nodeVmSize"]})
        base = kubectl_base(args)
        token = state["ownerToken"]
        cleanup_base = base
        cleanup_token = token
        summary["phase"] = "clearing"
        atomic_json(manifest_path, summary)
        for role in ("primary", "replica"):
            before = set(summary["roles"])
            try:
                execute_role(args, base, token, run_id, role, targets[role], summary)
            finally:
                created_records.extend(v for k, v in summary["roles"].items() if k not in before)
                summary["updatedAtUtc"] = now()
                atomic_json(manifest_path, summary)
        summary["phase"] = "complete"
    except Exception as exc:
        summary["errors"].append(str(exc))
        summary["phase"] = "failed"
    finally:
        # Preserve a log before deleting any pod left by a failed wait/clear.
        for record in created_records:
            if record.get("deleted"):
                continue
            if cleanup_base and cleanup_token:
                if not record.get("logsRetrieved"):
                    try:
                        retrieve_logs(cleanup_base, args.namespace, record, cleanup_token, run_id,
                                      out / f"{record['role']}-guest-cache.log")
                    except Exception as exc:
                        record["logError"] = str(exc)
                        summary["errors"].append(str(exc))
                try:
                    delete_owned_pod(cleanup_base, args.namespace, record, cleanup_token, run_id)
                except Exception as exc:
                    record["cleanupError"] = str(exc)
                    summary["errors"].append(str(exc))
        if summary["errors"]:
            summary["phase"] = "failed"
        summary["finishedAtUtc"] = now()
        atomic_json(manifest_path, summary)
    if summary["phase"] != "complete":
        print(json.dumps(summary, sort_keys=True), file=sys.stderr)
        return 2
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
