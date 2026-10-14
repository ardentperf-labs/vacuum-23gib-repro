#!/usr/bin/env python3
"""Collect exact-node Azure VM and managed-disk metrics at native PT1M grain.

Definitions and metric responses are retained raw. Missing definitions, failed
queries, empty series, and dimension mismatches are recorded as coverage gaps.
"""

from __future__ import annotations

import datetime as dt
import json
import re
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

UTC = dt.timezone.utc

VM_METRICS = (
    "Percentage CPU",
    "Available Memory Bytes",
    "VM Uncached IOPS Consumed Percentage",
    "VM Uncached Bandwidth Consumed Percentage",
    "VM Cached IOPS Consumed Percentage",
    "VM Cached Bandwidth Consumed Percentage",
)
VM_DISK_METRICS = (
    "Data Disk Read Bytes/sec",
    "Data Disk Write Bytes/sec",
    "Data Disk Read Operations/Sec",
    "Data Disk Write Operations/Sec",
    "Data Disk IOPS Consumed Percentage",
    "Data Disk Bandwidth Consumed Percentage",
    "Data Disk Queue Depth",
    "Data Disk Latency",
    "Data Disk Target IOPS",
    "Data Disk Target Bandwidth",
    "Data Disk Max Burst IOPS",
    "Data Disk Max Burst Bandwidth",
    "Data Disk Used Burst IO Credits Percentage",
    "Data Disk Used Burst BPS Credits Percentage",
)
MANAGED_DISK_METRICS = (
    "Composite Disk Read Operations/sec",
    "Composite Disk Write Operations/sec",
    "Composite Disk Read Bytes/sec",
    "Composite Disk Write Bytes/sec",
)


def utc_now() -> dt.datetime:
    return dt.datetime.now(UTC)


def iso(value: dt.datetime | None = None) -> str:
    return (value or utc_now()).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_")[-100:] or "resource"


def dimension_values(series: dict[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for raw in series.get("metadatavalues", []) or []:
        name = raw.get("name") or {}
        key = name.get("value") or name.get("localizedValue") or ""
        if key:
            result[str(key).casefold()] = str(raw.get("value", ""))
    return result


class AzureMetrics:
    """Metric-definition discovery and polling for one owned AKS run."""

    def __init__(self, manifest: dict[str, Any], out: Path, az: str = "az",
                 timeout: int = 60, subscription: str | None = None):
        self.manifest = manifest
        self.out = out
        self.out.mkdir(parents=True, exist_ok=True)
        self.az = az
        self.timeout = timeout
        self.subscription = subscription or manifest.get("subscriptionId")
        self.targets: list[dict[str, Any]] = []
        self.definitions: dict[str, dict[str, Any]] = {}
        self.discovered = False
        self.polls = 0
        self.errors = 0
        self.gaps: list[dict[str, Any]] = []
        self._gap_index: dict[str, dict[str, Any]] = {}
        self._make_targets()

    def record_gap(self, item: dict[str, Any]) -> None:
        key = json.dumps({name: item.get(name) for name in
                          ("role", "kind", "resourceId", "metric", "gap")}, sort_keys=True)
        now = iso()
        existing = self._gap_index.get(key)
        if existing is None:
            value = dict(item)
            value.update({"firstSeenAtUtc": now, "lastSeenAtUtc": now, "occurrences": 1})
            self._gap_index[key] = value
            self.gaps.append(value)
        else:
            existing["lastSeenAtUtc"] = now
            existing["occurrences"] += 1
            if item.get("dimensionMismatches"):
                existing["lastDimensionMismatches"] = item["dimensionMismatches"]

    def _make_targets(self) -> None:
        roles = self.manifest.get("roles") or {}
        for role in ("primary", "replica"):
            item = roles.get(role) or {}
            vm_id = item.get("vmResourceId")
            if vm_id:
                vm_id = str(vm_id).removeprefix("azure://")
                match = re.match(
                    r"(?P<parent>.+/providers/Microsoft\.Compute/virtualMachineScaleSets/"
                    r"(?P<scale>[^/]+))/virtualMachines/(?P<instance>[^/]+)$",
                    vm_id, flags=re.IGNORECASE,
                )
                vm_name = f"{match.group('scale')}_{match.group('instance')}" if match else None
                self.targets.append({
                    "role": role, "kind": "vm", "resourceId": vm_id,
                    "parentId": match.group("parent") if match else None,
                    "vmName": vm_name, "expectedVmName": vm_name,
                    "metrics": list(VM_METRICS + VM_DISK_METRICS),
                })
            for disk_kind in ("data", "wal"):
                disk = (item.get("disks") or {}).get(disk_kind) or {}
                disk_id = disk.get("id") or disk.get("resourceId")
                if not disk_id:
                    self.record_gap({"role": role, "kind": disk_kind, "gap": "attachment manifest has no disk resource ID"})
                    continue
                self.targets.append({
                    "role": role, "kind": f"{disk_kind}-disk", "diskKind": disk_kind,
                    "resourceId": str(disk_id).removeprefix("azure://"),
                    "lun": str(disk.get("lun", "")), "metrics": list(MANAGED_DISK_METRICS),
                })
                vm_target = next((x for x in self.targets if x.get("role") == role and x.get("kind") == "vm"), None)
                if vm_target:
                    vm_target.setdefault("diskLuns", []).append({"kind": disk_kind, "lun": str(disk.get("lun", ""))})

    def _argv(self, *args: str) -> list[str]:
        result = [self.az, *args]
        if self.subscription:
            result += ["--subscription", str(self.subscription)]
        return result

    def _call_json(self, argv: list[str]) -> tuple[Any, int, str, str]:
        try:
            p = subprocess.run(argv, text=True, capture_output=True, timeout=self.timeout, check=False)
        except subprocess.TimeoutExpired as exc:
            return None, 124, str(exc.stderr or ""), str(exc.stdout or "")
        try:
            value = json.loads(p.stdout) if p.stdout.strip() else None
        except json.JSONDecodeError:
            value = None
        return value, p.returncode, p.stderr, p.stdout

    def _definitions(self, target: dict[str, Any], resource_id: str) -> dict[str, Any]:
        argv = self._argv("monitor", "metrics", "list-definitions", "--resource", resource_id,
                          "--output", "json", "--only-show-errors")
        rows, code, stderr, stdout = self._call_json(argv)
        definitions = rows if isinstance(rows, list) else []
        available = {str(((x.get("name") or {}).get("value"))): x for x in definitions}
        wanted = target["metrics"]
        found = [name for name in wanted if name in available]
        missing = [name for name in wanted if name not in available]
        namespaces = sorted({str(x.get("namespace")) for x in available.values() if x.get("namespace")})
        record = {
            "capturedAtUtc": iso(), "role": target["role"], "kind": target["kind"],
            "resourceId": resource_id, "command": argv, "exitCode": code,
            "stderr": stderr[-4000:], "stdoutTail": stdout[-4000:],
            "requestedMetrics": wanted, "availableMetrics": found, "missingMetrics": missing,
            "metricDefinitions": [available[name] for name in found], "namespaces": namespaces,
        }
        path = self.out / ("definitions-" + safe_name(f"{target['role']}-{target['kind']}-{resource_id}") + ".json")
        path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        if code != 0:
            self.record_gap({"role": target["role"], "kind": target["kind"], "resourceId": resource_id,
                             "gap": "metric definition query failed", "exitCode": code, "stderr": stderr[-1000:]})
        return {"rows": definitions, "available": found, "missing": missing,
                "namespaces": namespaces, "rawPath": str(path), "exitCode": code,
                "stderr": stderr, "resourceId": resource_id}

    def discover(self) -> dict[str, Any]:
        inventory = []
        for target in self.targets:
            candidates: list[tuple[str, str]] = [(target["resourceId"], "resource")]
            if target["kind"] == "vm" and target.get("parentId"):
                candidates.append((target["parentId"], "vmss-parent"))
            attempts = []
            candidate_results = []
            for resource_id, scope in candidates:
                result = self._definitions(target, resource_id)
                eligible = list(result["available"])
                dimension_gaps = []
                if target["kind"] == "vm":
                    definitions_by_name = {str((x.get("name") or {}).get("value")): x
                                           for x in result["rows"]}
                    for metric in list(eligible):
                        dims = {str(x.get("value") or x.get("localizedValue") or "").casefold()
                                for x in (definitions_by_name.get(metric) or {}).get("dimensions", [])}
                        required = set()
                        if scope == "vmss-parent":
                            required.add("vmname")
                        if metric.startswith("Data Disk"):
                            required.add("lun")
                        if not required.issubset(dims):
                            eligible.remove(metric)
                            dimension_gaps.append({"metric": metric, "requiredDimensions": sorted(required),
                                                   "availableDimensions": sorted(dims)})
                proof = {"resourceId": resource_id, "scope": scope,
                         "exitCode": result["exitCode"], "availableMetrics": result["available"],
                         "eligibleMetrics": eligible,
                         "missingMetrics": sorted(set(target["metrics"]) - set(eligible)),
                         "metricNamespaces": {
                             name: (definitions_by_name.get(name) or {}).get("namespace")
                             for name in eligible
                         },
                         "dimensionGaps": dimension_gaps,
                         "namespaces": result["namespaces"], "definitionsPath": result["rawPath"]}
                attempts.append(proof)
                if result["exitCode"] == 0 and eligible:
                    candidate_results.append((len(eligible), -len(candidate_results), resource_id,
                                              scope, result, eligible, dimension_gaps))
            # The VMSS parent metric endpoint accepts dimension filters for an
            # exact VMName. VMSS instance resource IDs can return definitions
            # while the metrics endpoint itself returns NotFound, so prefer
            # the parent scope when both scopes expose the same definitions.
            selected_entry = max(candidate_results, default=None,
                                 key=lambda x: (x[0], x[3] == "vmss-parent", x[1]))
            selected = selected_entry[4] if selected_entry else None
            if selected is None:
                target["availableMetrics"] = []
                target["missingMetrics"] = list(target["metrics"])
                target["namespaces"] = []
                target["definitionAttempts"] = attempts
                for metric in target["metrics"]:
                    self.record_gap({"role": target["role"], "kind": target["kind"],
                                     "resourceId": target["resourceId"],
                                     "gap": "no resource scope exposes an exact usable definition", "metric": metric})
            else:
                # Prefer the scope exposing the most requested metric series;
                # VMSS parent metrics are usable only when their dimensions
                # support the exact VMName and, for disk metrics, LUN filters.
                target["availableMetrics"] = selected_entry[5]
                target["missingMetrics"] = sorted(set(target["metrics"]) - set(selected_entry[5]))
                target["namespaces"] = selected["namespaces"]
                target["definitionAttempts"] = attempts
                target["definitionsPath"] = selected["rawPath"]
                target["selectedResourceId"] = selected_entry[2]
                target["selectedScope"] = selected_entry[3]
                for metric in target["missingMetrics"]:
                    self.record_gap({"role": target["role"], "kind": target["kind"],
                                     "resourceId": target["selectedResourceId"],
                                     "gap": "metric unavailable or lacks exact filter dimensions", "metric": metric})
                for item in selected_entry[6]:
                    self.record_gap({"role": target["role"], "kind": target["kind"],
                                     "resourceId": target["selectedResourceId"],
                                     "gap": "metric lacks exact dimension", **item})
                definitions_by_name = {str((x.get("name") or {}).get("value")): x for x in selected["rows"]}
                target["metricNamespaces"] = {name: (definitions_by_name.get(name) or {}).get("namespace")
                                               for name in target["availableMetrics"]}
            inventory.append({"role": target["role"], "kind": target["kind"],
                              "resourceId": target["resourceId"],
                              "selectedResourceId": target.get("selectedResourceId"),
                              "selectedScope": target.get("selectedScope"),
                              "expectedVmName": target.get("expectedVmName"), "lun": target.get("lun"),
                              "availableMetrics": target.get("availableMetrics", []),
                              "missingMetrics": target.get("missingMetrics", target["metrics"]),
                              "definitionAttempts": attempts})
        self.discovered = True
        result = {"discoveredAtUtc": iso(), "nativeInterval": "PT1M", "targets": inventory,
                  "gaps": self.gaps}
        (self.out / "definitions-inventory.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        return result

    def _query_target(self, target: dict[str, Any], start: dt.datetime, end: dt.datetime,
                      poll_id: str, record_gaps: bool = True) -> dict[str, Any]:
        resource_id = target.get("selectedResourceId")
        if not resource_id:
            return {"role": target["role"], "kind": target["kind"], "gap": "no metric scope with usable definitions"}
        names = target.get("availableMetrics", [])
        if target["kind"] == "vm":
            # Query VM-level metrics once and attached-disk metrics once per
            # exact LUN, including when the resource scope is an exact VM.
            groups = [([n for n in names if not n.startswith("Data Disk")], None)]
            for mapping in target.get("diskLuns", []):
                groups.append(([n for n in names if n.startswith("Data Disk")], mapping))
        else:
            groups = [(names, None)]
        namespace_groups = []
        for metrics, disk in groups:
            by_namespace: dict[str | None, list[str]] = {}
            for metric in metrics:
                namespace = target.get("metricNamespaces", {}).get(metric)
                by_namespace.setdefault(namespace, []).append(metric)
            namespace_groups.extend((items, disk, namespace)
                                    for namespace, items in by_namespace.items())
        results = []
        for metrics, disk, metric_namespace in namespace_groups:
            if not metrics:
                continue
            metric_arg = ",".join(metrics)
            argv = self._argv("monitor", "metrics", "list", "--resource", resource_id,
                              "--metric", metric_arg, "--interval", "PT1M", "--aggregation", "Average",
                              "--start-time", iso(start).replace(".000Z", "Z"),
                              "--end-time", iso(end).replace(".000Z", "Z"),
                              "--output", "json", "--only-show-errors")
            scope = target.get("selectedScope")
            if metric_namespace:
                argv += ["--namespace", str(metric_namespace)]
            filters = []
            if scope == "vmss-parent":
                filters.append(f"VMName eq '{target['expectedVmName']}'")
            if target["kind"] == "vm" and disk is not None:
                filters.append(f"LUN eq '{disk['lun']}'")
            elif target["kind"] in ("data-disk", "wal-disk") and target.get("lun"):
                # Managed disk resources are already exact scope; no VM LUN dimension applies.
                pass
            if filters:
                argv += ["--filter", " and ".join(filters)]
            response, code, stderr, stdout = self._call_json(argv)
            expected_dims = ({"vmname": target["expectedVmName"]}
                             if scope == "vmss-parent" else {})
            if target["kind"] == "vm" and disk is not None:
                expected_dims["lun"] = disk["lun"]
            parsed_by_metric = {}
            for metric in metrics:
                metric_rows = [x for x in (response or {}).get("value", [])
                               if (x.get("name") or {}).get("value") == metric] if isinstance(response, dict) else []
                series_rows = []
                mismatch = []
                for mrow in metric_rows:
                    for series in mrow.get("timeseries", []) or []:
                        dims = dimension_values(series)
                        bad = {key: {"expected": value, "actual": dims.get(key)}
                               for key, value in expected_dims.items()
                               if dims.get(key, "").casefold() != str(value).casefold()}
                        if bad:
                            mismatch.append(bad)
                            continue
                        points = series.get("data", []) or []
                        series_rows.append({"dimensions": dims, "points": points})
                        for point in points:
                            append_jsonl(self.out / "azure-metrics.jsonl", {
                                "capturedAtUtc": iso(), "pollId": poll_id,
                                "role": target["role"], "targetKind": target["kind"],
                                "resourceId": resource_id, "metric": metric,
                                "nativeInterval": (response or {}).get("interval", "PT1M"),
                                "dimensions": dims, "point": point,
                            })
                parsed_by_metric[metric] = {"series": series_rows, "mismatches": mismatch}
            record = {
                "pollId": poll_id, "capturedAtUtc": iso(), "role": target["role"],
                "targetKind": target["kind"], "resourceId": resource_id, "scope": scope,
                "metrics": metrics, "expectedDimensions": expected_dims, "command": argv,
                "exitCode": code, "stderr": stderr[-2500:],
                "timespan": (response or {}).get("timespan") if isinstance(response, dict) else None,
                "interval": (response or {}).get("interval") if isinstance(response, dict) else None,
                "metricResults": {name: {"seriesCount": len(value["series"]),
                                         "pointCount": sum(len(x["points"]) for x in value["series"]),
                                         "dimensionMismatches": value["mismatches"]}
                                  for name, value in parsed_by_metric.items()},
                "rawResponse": response, "stdoutTail": stdout[-2000:],
            }
            append_jsonl(self.out / "azure-metric-polls.jsonl", record)
            for metric, value in parsed_by_metric.items():
                series_rows = value["series"]
                mismatch = value["mismatches"]
                if code != 0:
                    self.errors += 1
                    if record_gaps:
                        self.record_gap({"role": target["role"], "kind": target["kind"], "metric": metric,
                                         "gap": "metric query failed", "exitCode": code, "stderr": stderr[-1000:]})
                elif not series_rows:
                    if record_gaps:
                        self.record_gap({"role": target["role"], "kind": target["kind"], "metric": metric,
                                         "gap": "query returned no exact-dimension series", "dimensionMismatches": mismatch})
                elif mismatch:
                    if record_gaps:
                        self.record_gap({"role": target["role"], "kind": target["kind"], "metric": metric,
                                         "gap": "query included unexpected dimensions", "dimensionMismatches": mismatch})
                results.append({"metric": metric, "exitCode": code, "seriesCount": len(series_rows),
                                "pointCount": sum(len(x["points"]) for x in series_rows),
                                "expectedDimensions": expected_dims,
                                "matchedDimensions": [x["dimensions"] for x in series_rows],
                                "dimensionMismatches": mismatch,
                                "resourceId": resource_id, "scope": scope})
        return {"role": target["role"], "kind": target["kind"], "resourceId": resource_id,
                "scope": target.get("selectedScope"), "metrics": results}

    @staticmethod
    def _has_initial_vm_metric_proof(target: dict[str, Any], result: dict[str, Any]) -> bool:
        """True only when required VM/LUN series have points at exact dimensions."""
        metrics = target.get("availableMetrics", [])
        # CPU and uncached IOPS are mandatory for the run. Treat a scope that
        # does not advertise either metric as an unproven scope so discovery's
        # alternate (VMSS parent vs. instance) can be tried before preflight
        # fails. Bandwidth and disk latency/queue are required when advertised.
        required_vm = ["Percentage CPU", "VM Uncached IOPS Consumed Percentage"]
        required_vm.extend(name for name in
                           ("VM Uncached Bandwidth Consumed Percentage",) if name in metrics)
        if any(name not in metrics for name in required_vm):
            return False
        for name in required_vm:
            exact = [row for row in result.get("metrics", []) if row.get("metric") == name]
            if not any(row.get("exitCode") == 0 and row.get("pointCount", 0) > 0
                       and (target.get("selectedScope") != "vmss-parent" or
                            any((dims.get("vmname", "").casefold() ==
                                 str(target.get("expectedVmName", "")).casefold())
                                for dims in row.get("matchedDimensions", [])))
                       for row in exact):
                return False
        for disk in target.get("diskLuns", []):
            for name in ("Data Disk Latency", "Data Disk Queue Depth"):
                if name not in metrics:
                    continue
                exact = [row for row in result.get("metrics", [])
                         if row.get("metric") == name
                         and str(row.get("expectedDimensions", {}).get("lun", "")) == str(disk["lun"])]
                if not any(row.get("exitCode") == 0 and row.get("pointCount", 0) > 0
                           and any(str(dims.get("lun", "")) == str(disk["lun"])
                                   and (target.get("selectedScope") != "vmss-parent" or
                                        dims.get("vmname", "").casefold() ==
                                        str(target.get("expectedVmName", "")).casefold())
                                   for dims in row.get("matchedDimensions", []))
                           for row in exact):
                    return False
        return True

    def _query_target_with_fallback(self, target: dict[str, Any], start: dt.datetime,
                                    end: dt.datetime, poll_id: str) -> dict[str, Any]:
        result = self._query_target(target, start, end, poll_id)
        if target.get("kind") != "vm" or self._has_initial_vm_metric_proof(target, result):
            return result
        attempts = target.get("definitionAttempts", [])
        for attempt in attempts:
            if attempt.get("scope") == target.get("selectedScope") or not attempt.get("eligibleMetrics"):
                continue
            fallback = dict(target)
            fallback.update({"selectedResourceId": attempt["resourceId"],
                             "selectedScope": attempt["scope"],
                             "availableMetrics": attempt["eligibleMetrics"],
                             "missingMetrics": attempt.get("missingMetrics", []),
                             "metricNamespaces": attempt.get("metricNamespaces", {})})
            candidate = self._query_target(fallback, start, end, poll_id, record_gaps=False)
            if self._has_initial_vm_metric_proof(fallback, candidate):
                previous = {"resourceId": target.get("selectedResourceId"),
                            "scope": target.get("selectedScope")}
                target.update({key: fallback[key] for key in
                               ("selectedResourceId", "selectedScope", "availableMetrics",
                                "missingMetrics", "metricNamespaces")})
                candidate["fallbackFrom"] = previous
                candidate["fallbackReason"] = "preferred VM metric scope returned no exact required series"
                return candidate
        result["fallbackAttemptsFailed"] = True
        return result

    def poll(self, start: dt.datetime | None = None, end: dt.datetime | None = None) -> dict[str, Any]:
        if not self.discovered:
            self.discover()
        end = end or utc_now()
        start = start or end - dt.timedelta(minutes=12)
        poll_id = uuid.uuid4().hex[:12]
        results = [self._query_target_with_fallback(target, start, end, poll_id)
                   for target in self.targets]
        self.polls += 1
        return {"pollId": poll_id, "capturedAtUtc": iso(end), "windowStartUtc": iso(start),
                "windowEndUtc": iso(end), "nativeInterval": "PT1M", "targets": results}

    def summary(self) -> dict[str, Any]:
        return {"nativeInterval": "PT1M", "polls": self.polls, "queryErrors": self.errors,
                "targets": [{"role": x["role"], "kind": x["kind"],
                             "resourceId": x.get("selectedResourceId", x["resourceId"]),
                             "scope": x.get("selectedScope"),
                             "availableMetrics": x.get("availableMetrics", []),
                             "missingMetrics": x.get("missingMetrics", x["metrics"])} for x in self.targets],
                "gaps": self.gaps}


def append_jsonl(path: Path, value: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str) + "\n")
        stream.flush()
