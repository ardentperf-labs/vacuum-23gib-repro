#!/usr/bin/env python3
"""Read completed experiment artifacts; write only to stdout."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNS = {
    "baseline": ROOT / "vacuum-only-repro/experiments-20261005-five/runs/baseline/results/20261005T062051Z",
    "prefetch_off": ROOT / "vacuum-only-repro/experiments-20261005-five/runs/prefetch-off/results/20261005T072928Z-prefetch-off",
}
result = {}
settings = {}
for name, path in RUNS.items():
    preflight = json.loads((path / "preflight.json").read_text())
    summary = json.loads((path / "summary.json").read_text())
    settings[name] = {
        role: {key: value["setting"] for key, value in sql["settings"].items()}
        for role, sql in preflight["sql"].items()
    }
    totals = {}
    for device, window in summary["windows"]["vacuumAndReplay"].items():
        duration = window["coveredSeconds"]
        read_ops = window["readIops"] * duration
        read_bytes = window["readMBps"] * 1_000_000 * duration
        totals[device] = {
            "readOperations": read_ops,
            "readBytes": read_bytes,
            "writeBytes": window["writeMBps"] * 1_000_000 * duration,
            "meanReadBytes": read_bytes / read_ops if read_ops else None,
            "readAwaitMs": window["readAwaitMs"],
        }
    result[name] = {
        "path": str(path.relative_to(ROOT)),
        "sha256": {
            filename: hashlib.sha256((path / filename).read_bytes()).hexdigest()
            for filename in ("preflight.json", "summary.json", "vacuum.log", "timing.json")
        },
        "table": preflight["sql"]["primary"]["table"],
        "walDelta": summary["walCounterDeltaFromBeforeVacuum"],
        "wholeTimelineDeviceTotals": totals,
        "status": summary["status"],
    }
result["capturedSettingDifferences"] = {
    role: {
        key: {name: settings[name][role].get(key) for name in RUNS}
        for key in settings["baseline"][role].keys() | settings["prefetch_off"][role].keys()
        if settings["baseline"][role].get(key) != settings["prefetch_off"][role].get(key)
    }
    for role in ("primary", "replica")
}
heap = result["baseline"]["table"]["heapBytes"]
baseline = result["baseline"]["wholeTimelineDeviceTotals"]["replica_data"]
result["twoHeapPassPrediction"] = {
    "heapPages": heap // 8192,
    "twoPass8KiBRequests": 2 * (heap // 8192),
    "twoPassBytes": 2 * heap,
    "baselineReadBytesDividedByTwoPassBytes": baseline["readBytes"] / (2 * heap),
}
print(json.dumps(result, indent=2, sort_keys=True))
