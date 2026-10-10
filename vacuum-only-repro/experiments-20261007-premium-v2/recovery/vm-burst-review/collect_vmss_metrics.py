#!/usr/bin/env python3
"""Read Azure VMSS storage metrics for the three locally owned experiment stacks.

This collector is supplementary evidence only. It never changes Azure resources
or experiment gates. --once samples the last 30 minutes; --until-test-receipts
refreshes its in-memory management token for every batch and repeats every five
minutes until each test-process receipt is terminal.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[2]
RUNS = ("baseline-23gib", "50gib", "100gib")
ARM = "https://management.azure.com"
METRICS = (
    "VM Remote Used Burst IO Credits Percentage",
    "VM Remote Used Burst BPS Credits Percentage",
    "VM Uncached IOPS Consumed Percentage",
    "VM Uncached Bandwidth Consumed Percentage",
)
EXPECTED_VMSS = {
    "baseline-23gib": {"name": "aks-nodepool1-34976006-vmss", "size": "Standard_D2s_v4", "capacity": 2},
    "50gib": {"name": "aks-nodepool1-30685692-vmss", "size": "Standard_D2s_v4", "capacity": 2},
    "100gib": {"name": "aks-nodepool1-15388874-vmss", "size": "Standard_D2s_v4", "capacity": 2},
}
GROUP_API = "2024-03-01"
VMSS_API = "2023-09-01"
METRICS_API = "2023-10-01"
METRICS_NAMESPACE = "microsoft.compute/virtualmachinescalesets"
INTERVAL = "PT5M"
DEFAULT_WINDOW_MINUTES = 30
POLL_SECONDS = 300


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_z(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object at {path}")
    return value


def local_scope() -> dict[str, dict[str, str]]:
    """Read local state and pin only the known experiment VMSS names."""
    result: dict[str, dict[str, str]] = {}
    subscriptions: set[str] = set()
    for name in RUNS:
        stack = ROOT / name
        state = read_json(stack / ".state/state.json")
        config = read_json(stack / "run-config.json")
        expected = EXPECTED_VMSS[name]
        subscription = state.get("subscriptionId")
        owner = state.get("ownerToken")
        node_group = state.get("nodeResourceGroup")
        if not all(isinstance(item, str) and item for item in (subscription, owner, node_group)):
            raise ValueError(f"{name}: local state is missing subscription, owner, or node resource group")
        if not re.fullmatch(r"[0-9a-f]{32}", owner):
            raise ValueError(f"{name}: local owner token has an unexpected shape")
        if node_group != f"rg-vacuum-nodes-{owner}":
            raise ValueError(f"{name}: local node resource group does not match its owner token")
        if state.get("status") != "ready" or config.get("name") != name:
            raise ValueError(f"{name}: local preparation identity or infrastructure state is unexpected")
        subscriptions.add(subscription.lower())
        result[name] = {
            "subscriptionId": subscription,
            "ownerToken": owner,
            "nodeResourceGroup": node_group,
            "expectedVmssName": expected["name"],
            "expectedVmSize": expected["size"],
            "expectedCapacity": str(expected["capacity"]),
        }
    if len(subscriptions) != 1:
        raise ValueError("the three local stacks do not share one subscription")
    return result


def get_token() -> str:
    """Acquire a management token through Azure CLI and keep it in memory."""
    try:
        completed = subprocess.run(
            ["az", "account", "get-access-token", "--resource", ARM + "/", "-o", "json"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=120,
        )
        token = json.loads(completed.stdout).get("accessToken")
    except Exception as exc:
        raise RuntimeError(f"could not acquire the Azure management token ({type(exc).__name__})") from None
    if not isinstance(token, str) or not token:
        raise RuntimeError("Azure CLI returned no management access token")
    return token


def ensure_arm_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.netloc.lower() != "management.azure.com":
        raise ValueError("refusing to send the Azure bearer token outside management.azure.com")
    return url


def arm_get(url_or_path: str, token: str, timeout: int = 60) -> dict[str, Any]:
    url = ARM + url_or_path if url_or_path.startswith("/") else url_or_path
    url = ensure_arm_url(url)
    request = Request(url, headers={"Authorization": "Bearer " + token, "Accept": "application/json"})
    with urlopen(request, timeout=timeout) as response:
        body = json.load(response)
    if not isinstance(body, dict):
        raise ValueError("ARM returned a non-object JSON response")
    return body


def arm_collection(path: str, token: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Read every page and retain the public response bodies for audit."""
    pages: list[dict[str, Any]] = []
    items: list[dict[str, Any]] = []
    current = ARM + path
    for _ in range(10):
        page = arm_get(current, token)
        pages.append(page)
        value = page.get("value")
        if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
            raise ValueError("ARM collection response has an invalid value array")
        items.extend(value)
        next_link = page.get("nextLink")
        if not next_link:
            return items, pages
        current = ensure_arm_url(str(next_link))
    raise ValueError("ARM collection exceeded ten pages")


def summarize_metrics(body: dict[str, Any]) -> list[dict[str, Any]]:
    summaries = []
    for metric in body.get("value", []):
        name = (metric.get("name") or {}).get("value")
        datapoints = []
        for series in metric.get("timeseries", []):
            for point in series.get("data", []):
                average = point.get("average")
                if isinstance(average, (int, float)):
                    datapoints.append((point.get("timeStamp"), float(average)))
        values = [value for _, value in datapoints]
        summaries.append({
            "name": name,
            "unit": metric.get("unit"),
            "seriesCount": len(metric.get("timeseries", [])),
            "pointCount": len(datapoints),
            "first": datapoints[0] if datapoints else None,
            "last": datapoints[-1] if datapoints else None,
            "minimumOfAverages": min(values) if values else None,
            "maximumOfAverages": max(values) if values else None,
        })
    return summaries


def safe_error(exc: Exception, token: str) -> dict[str, Any]:
    """Persist a concise error without headers, raw bodies, or bearer material."""
    status = exc.code if isinstance(exc, HTTPError) else None
    code = None
    message = None
    if isinstance(exc, HTTPError):
        try:
            body = json.loads(exc.read().decode("utf-8", errors="replace"))
            error = body.get("error") or {}
            code = error.get("code")
            message = error.get("message")
        except Exception:
            pass
    if not message:
        message = str(exc)[:1000]
    message = message.replace(token, "<redacted>") if token else message
    message = re.sub(r"(?i)bearer\s+[^\s,;]+", "Bearer <redacted>", message)
    message = re.sub(r"(?i)(access_token|token)=([^&\s]+)", r"\1=<redacted>", message)
    return {"type": type(exc).__name__, "httpStatus": status, "code": code, "message": message}


def inspect_and_sample(name: str, local: dict[str, str], token: str,
                       start_utc: str, end_utc: str) -> dict[str, Any]:
    subscription = local["subscriptionId"]
    node_group = local["nodeResourceGroup"]
    group_id = f"/subscriptions/{subscription}/resourceGroups/{node_group}"
    vmss_path = group_id + "/providers/Microsoft.Compute/virtualMachineScaleSets"
    raw: dict[str, Any] = {}
    scope_check: dict[str, Any] = {
        "subscriptionId": subscription,
        "nodeResourceGroup": node_group,
        "expectedVmssName": local["expectedVmssName"],
        "expectedVmSize": local["expectedVmSize"],
        "expectedCapacity": int(local["expectedCapacity"]),
    }
    try:
        group = arm_get(group_id + "?api-version=" + GROUP_API, token)
        raw["resourceGroup"] = group
        actual_owner = (group.get("tags") or {}).get("reprotoken")
        scope_check["liveOwnerTagMatches"] = actual_owner == local["ownerToken"]
        if not scope_check["liveOwnerTagMatches"]:
            raise ValueError("live node resource group reprotoken does not match local owner token")

        vmss_items, vmss_pages = arm_collection(vmss_path + "?api-version=" + VMSS_API, token)
        raw["vmScaleSetPages"] = vmss_pages
        if len(vmss_items) != 1:
            raise ValueError(f"expected exactly one VMSS in the owned node resource group; found {len(vmss_items)}")
        vmss = vmss_items[0]
        vmss_name = vmss.get("name")
        sku = vmss.get("sku") or {}
        live_size = sku.get("name")
        live_capacity = sku.get("capacity")
        scope_check.update({
            "liveVmssName": vmss_name,
            "liveVmSize": live_size,
            "liveCapacity": live_capacity,
            "vmssNameMatches": vmss_name == local["expectedVmssName"],
            "vmSizeMatches": live_size == local["expectedVmSize"],
            "capacityMatches": live_capacity == int(local["expectedCapacity"]),
        })
        if not all(scope_check[key] for key in ("vmssNameMatches", "vmSizeMatches", "capacityMatches")):
            raise ValueError("live VMSS name, size, or capacity differs from the reviewed local scope")

        params = urlencode({
            "api-version": METRICS_API,
            "metricnamespace": METRICS_NAMESPACE,
            "metricnames": ",".join(METRICS),
            "timespan": start_utc + "/" + end_utc,
            "interval": INTERVAL,
            "aggregation": "Average",
        })
        metric_path = (vmss_path + "/" + str(vmss_name) +
                       "/providers/Microsoft.Insights/metrics?" + params)
        metric_body = arm_get(metric_path, token, timeout=90)
        raw["metrics"] = metric_body
        return {
            "stack": name,
            "httpStatus": 200,
            "scopeCheck": scope_check,
            "metricSummaries": summarize_metrics(metric_body),
            "rawResponses": raw,
        }
    except Exception as exc:
        return {
            "stack": name,
            "httpStatus": exc.code if isinstance(exc, HTTPError) else None,
            "scopeCheck": scope_check,
            "rawResponses": raw,
            "error": safe_error(exc, token),
        }


def sample_once(scopes: dict[str, dict[str, str]], token: str,
                window_minutes: int) -> dict[str, Any]:
    end = utc_now()
    start = end - dt.timedelta(minutes=window_minutes)
    start_text, end_text = iso_z(start), iso_z(end)
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futures = {
            pool.submit(inspect_and_sample, name, scopes[name], token, start_text, end_text): name
            for name in RUNS
        }
        stack_results = [future.result() for future in concurrent.futures.as_completed(futures)]
    by_name = {item["stack"]: item for item in stack_results}
    return {
        "collectionStatus": "sampled",
        "capturedAtUtc": iso_z(utc_now()),
        "scope": "one aggregate time series per metric on each exact parent VMSS",
        "timespan": {"startUtc": start_text, "endUtc": end_text},
        "metricNamespace": METRICS_NAMESPACE,
        "metricsApiVersion": METRICS_API,
        "interval": INTERVAL,
        "aggregation": "Average",
        "metrics": list(METRICS),
        "stacks": [by_name[name] for name in RUNS],
        "limitations": [
            "VMSS metrics are host-level scale-set aggregates and do not attribute activity to a specific VM.",
            "VM uncached utilization percentages are supplemental host telemetry; they are not converted to a Premium SSD v2 IOPS or MB/s limit.",
            "Burst-credit metrics are recorded as reported; their presence does not prove D2s_v4/Premium SSD v2 burst eligibility.",
            "Each polling sample queries a rolling window, so adjacent five-minute samples overlap.",
        ],
        "measurementGateTouched": False,
    }


def token_error_sample(scopes: dict[str, dict[str, str]], window_minutes: int,
                       exc: Exception) -> dict[str, Any]:
    """Record a failed supplementary refresh without storing CLI output/token material."""
    end = utc_now()
    start = end - dt.timedelta(minutes=window_minutes)
    message = str(exc)
    message = re.sub(r"(?i)bearer\s+[^\s,;]+", "Bearer <redacted>", message)
    message = re.sub(r"(?i)(access_token|token)=([^&\s]+)", r"\1=<redacted>", message)
    return {
        "collectionStatus": "management-token-refresh-failed",
        "capturedAtUtc": iso_z(end),
        "timespan": {"startUtc": iso_z(start), "endUtc": iso_z(end)},
        "metrics": list(METRICS),
        "stacks": [{
            "stack": name,
            "httpStatus": None,
            "scopeCheck": {
                "subscriptionId": scopes[name]["subscriptionId"],
                "nodeResourceGroup": scopes[name]["nodeResourceGroup"],
                "expectedVmssName": scopes[name]["expectedVmssName"],
                "expectedVmSize": scopes[name]["expectedVmSize"],
                "expectedCapacity": int(scopes[name]["expectedCapacity"]),
                "liveScopeChecked": False,
            },
            "metricSummaries": [],
            "rawResponses": {},
            "error": {"type": type(exc).__name__, "message": message[:500]},
        } for name in RUNS],
        "supplementaryOnly": True,
        "measurementGateTouched": False,
        "limitations": [
            "No Azure request was sent because management-token refresh failed.",
            "This supplementary collector failure does not update or block experiment gates.",
        ],
    }


def test_receipt_statuses() -> tuple[bool, dict[str, Any]]:
    statuses: dict[str, Any] = {}
    terminal = True
    for name in RUNS:
        path = ROOT / name / ".state/test-process.json"
        if not path.exists():
            statuses[name] = {"status": "missing"}
            terminal = False
            continue
        try:
            receipt = read_json(path)
        except Exception as exc:
            statuses[name] = {"status": "invalid", "error": type(exc).__name__}
            terminal = False
            continue
        status = receipt.get("status")
        exit_code = receipt.get("exitCode")
        finished = status in ("complete", "failed") and isinstance(exit_code, int)
        statuses[name] = {
            "status": status,
            "exitCode": exit_code,
            "finishedAtUtc": receipt.get("finishedAtUtc"),
            "terminal": finished,
        }
        terminal = terminal and finished
    return terminal, statuses


def output_path(output_dir: Path, captured: str) -> Path:
    stamp = captured.replace(":", "").replace("-", "").replace("T", "T").replace("Z", "Z")
    return output_dir / f"vmss-metrics-{stamp}.json"


def save_private(path: Path, data: dict[str, Any]) -> None:
    created_directory = not path.parent.exists()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if created_directory:
        os.chmod(path.parent, 0o700)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    os.chmod(temporary, 0o600)
    temporary.replace(path)


def display_result(path: Path, data: dict[str, Any]) -> None:
    print(json.dumps({
        "file": str(path),
        "collectionStatus": data.get("collectionStatus"),
        "capturedAtUtc": data["capturedAtUtc"],
        "stacks": [{
            "name": stack["stack"],
            "httpStatus": stack.get("httpStatus"),
            "ownerTagMatches": stack.get("scopeCheck", {}).get("liveOwnerTagMatches"),
            "vmssNameMatches": stack.get("scopeCheck", {}).get("vmssNameMatches"),
            "vmSizeMatches": stack.get("scopeCheck", {}).get("vmSizeMatches"),
            "capacityMatches": stack.get("scopeCheck", {}).get("capacityMatches"),
            "metrics": [{"name": item["name"], "points": item["pointCount"],
                         "minAverage": item["minimumOfAverages"],
                         "maxAverage": item["maximumOfAverages"]}
                        for item in stack.get("metricSummaries", [])],
            "error": stack.get("error"),
        } for stack in data["stacks"]],
        "measurementGateTouched": False,
    }, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--once", action="store_true", help="capture one rolling metrics window")
    modes.add_argument("--until-test-receipts", action="store_true",
                       help="sample every five minutes until all test process receipts are terminal")
    parser.add_argument("--window-minutes", type=int, default=DEFAULT_WINDOW_MINUTES,
                        help="rolling lookback window (default: 30 minutes)")
    parser.add_argument("--output-dir", type=Path,
                        default=Path(__file__).resolve().parent / "collected")
    args = parser.parse_args()
    if not 5 <= args.window_minutes <= 24 * 60:
        parser.error("--window-minutes must be between 5 and 1440")

    try:
        scopes = local_scope()
    except Exception as exc:
        print(f"collector preflight failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2

    if args.until_test_receipts:
        gate_path = ROOT / "measurement-gate.json"
        if not gate_path.exists():
            print("loop mode requires the existing measurement-gate.json", file=sys.stderr)
            return 2
        gate = read_json(gate_path)
        if gate.get("authorized") is not True or gate.get("stacks") != list(RUNS):
            print("loop mode requires an already authorized gate naming exactly these stacks", file=sys.stderr)
            return 2

    if args.once:
        try:
            token = get_token()
        except Exception as exc:
            data = token_error_sample(scopes, args.window_minutes, exc)
        else:
            try:
                data = sample_once(scopes, token, args.window_minutes)
            finally:
                token = None
        destination = output_path(args.output_dir, data["capturedAtUtc"])
        save_private(destination, data)
        display_result(destination, data)
        return 0

    next_sample = time.monotonic()
    while True:
        token = None
        try:
            token = get_token()
        except Exception as exc:
            data = token_error_sample(scopes, args.window_minutes, exc)
        else:
            try:
                data = sample_once(scopes, token, args.window_minutes)
            finally:
                token = None
        terminal, statuses = test_receipt_statuses()
        data["testReceipts"] = statuses
        destination = output_path(args.output_dir, data["capturedAtUtc"])
        save_private(destination, data)
        display_result(destination, data)
        if terminal:
            return 0
        next_sample += POLL_SECONDS
        time.sleep(max(0, next_sample - time.monotonic()))


if __name__ == "__main__":
    raise SystemExit(main())
