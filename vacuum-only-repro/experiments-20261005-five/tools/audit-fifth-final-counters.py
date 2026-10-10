#!/usr/bin/env python3
"""Independent, read-only recount of the completed fifth capture.

Does not import the collector or plotting implementation. Writes a receipt only
with --write and only after all checks pass.
"""
import argparse
import datetime as dt
import hashlib
import json
import math
from pathlib import Path


def epoch(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def lsn(value):
    hi, lo = value.split("/")
    return (int(hi, 16) << 32) + int(lo, 16)


def main():
    if not __debug__:
        raise RuntimeError("Run without Python optimization; audit assertions must remain enabled")
    parser = argparse.ArgumentParser()
    parser.add_argument("result", type=Path)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    root = args.result
    timing = json.loads((root / "timing.json").read_text())
    summary = json.loads((root / "summary.json").read_text())
    assert timing["testComplete"] and timing["replayCaughtUp"]
    assert timing["remoteVacuumExitCode"] == 0
    assert summary["status"]["testComplete"] and not summary["status"]["error"]
    raw = (root / "samples.jsonl").read_bytes()
    rows = [json.loads(line) for line in raw.splitlines()]
    assert len(rows) == summary["sampleCount"]
    start, end, catchup = map(epoch, (timing["vacuumStart"], timing["vacuumComplete"], timing["replayEnd"]))
    windows = {"vacuum": (start, end), "replayDrain": (end, catchup), "vacuumAndReplay": (start, catchup)}
    estimates = {}
    checked = 0
    max_gap = 0
    for role in ("primary", "replica"):
        assert all(row[role]["appSessions"] == 0 for row in rows)
        for disk in ("data", "wal"):
            identity = rows[0][role]["devices"][disk]
            device = identity["majorMinor"]
            for row in rows:
                current = row[role]["devices"][disk]
                for field in ("majorMinor", "device", "pv", "claim", "mount", "node", "lun"):
                    assert current[field] == identity[field], (role, disk, field)
            for window, (left, right) in windows.items():
                total = {key: 0.0 for key in ("r", "rs", "rm", "w", "ws", "wm")}
                covered = 0.0
                for before, after in zip(rows, rows[1:]):
                    a, b = before[role], after[role]
                    gap = b["epoch"] - a["epoch"]
                    max_gap = max(max_gap, gap)
                    assert 0 < gap <= 12.5, (role, gap)
                    overlap = max(0, min(right, b["epoch"]) - max(left, a["epoch"]))
                    for key in total:
                        delta = b["disks"][device][key] - a["disks"][device][key]
                        assert delta >= 0, (role, disk, key, "reset/rollover requires separate proof")
                        total[key] += delta * overlap / gap
                    covered += overlap
                assert covered / (right - left) >= 0.9
                actual = {
                    "readIops": total["r"] / covered,
                    "writeIops": total["w"] / covered,
                    "readMBps": total["rs"] * 512 / 1e6 / covered,
                    "writeMBps": total["ws"] * 512 / 1e6 / covered,
                    "readAwaitMs": total["rm"] / total["r"] if total["r"] else None,
                    "writeAwaitMs": total["wm"] / total["w"] if total["w"] else None,
                }
                saved = summary["windows"][window][role + "_" + disk]
                for key, value in actual.items():
                    assert (value is None and saved[key] is None) or (
                        value is not None and saved[key] is not None
                        and math.isclose(value, saved[key], rel_tol=1e-7, abs_tol=1e-6)
                    ), (window, role, disk, key, value, saved[key])
                    checked += 1
                if disk == "data" and ((role == "primary" and window == "vacuum") or (role == "replica" and window == "vacuumAndReplay")):
                    estimates[role] = {"averageReadIops": actual["readIops"], "durationSeconds": right - left,
                                       "estimatedReadOperations": actual["readIops"] * (right - left)}
    target = lsn(timing["replayTargetLsn"])
    assert all(lsn(row["replica"]["replayLsn"]) >= max(target, lsn(row["primary"]["flushLsn"]))
               and row["replica"]["receiver"]["status"] == "streaming" for row in rows[-3:])
    estimates["ratio"] = estimates["replica"]["estimatedReadOperations"] / estimates["primary"]["estimatedReadOperations"]
    if "estimatedTotalDataReadOperations" in summary:
        assert math.isclose(estimates["ratio"], summary["estimatedTotalDataReadOperations"]["ratio"], rel_tol=1e-7)
    receipt = {"auditPassed": True, "independentCalculation": "raw OS device counter deltas, prorated by timestamp overlap; no collector/plot imports",
               "rawSha256": hashlib.sha256(raw).hexdigest(), "sampleCount": len(rows), "metricsCompared": checked,
               "maximumSampleGapSeconds": max_gap, "stableDeviceIdentities": True, "applicationSessionsZero": True,
               "lastThreeSamplesReachedCurrentFlush": True, "vacuumSeconds": end - start,
               "additionalReplaySeconds": catchup - end, "vacuumStartToCatchupSeconds": catchup - start,
               "estimatedTotalDataReadOperations": estimates, "verifiedAtUtc": dt.datetime.now(dt.timezone.utc).isoformat()}
    if args.write:
        path = root / "root-independent-counter-audit.json"
        assert not path.exists(), "do not overwrite a completed audit"
        path.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
