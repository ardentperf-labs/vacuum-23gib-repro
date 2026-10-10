#!/usr/bin/env python3
"""Regenerate the three graphs from packaged sample counters, entirely offline."""
import argparse, gzip, hashlib, importlib.util, json, math, shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("run", choices=("baseline", "prefetch-off", "100gib", "prefetch-off-fpw-off", "read-pressure"))
parser.add_argument("--out", type=Path)
args = parser.parse_args()
run = ROOT / "runs" / args.run
sample_files = list((run / "results").glob("*/samples.jsonl.gz"))
if len(sample_files) != 1:
    raise SystemExit(f"expected one packaged raw capture under {run / 'results'}, found {len(sample_files)}")
result = sample_files[0].parent
timing_path = result / ("partial-timing.json" if args.run == "read-pressure" else "timing.json")
timing = json.loads(timing_path.read_text())
with gzip.open(sample_files[0], "rt", encoding="utf-8") as stream:
    rows = [json.loads(line) for line in stream if line.strip()]
raw_bytes = gzip.decompress(sample_files[0].read_bytes())
source = run / "source/as-run/plot.py"
spec = importlib.util.spec_from_file_location("vacuum_plot", source)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
out = args.out or (run / "replots")
out.mkdir(parents=True, exist_ok=True)
events = result / "replication-events.json"
if events.is_file():
    shutil.copyfile(events, out / "replication-events.json")
report = module.analyze(rows, timing, 5)
if args.run == "100gib":
    helper = ROOT / "tools/diskstats_rollover.py"
    helper_spec = importlib.util.spec_from_file_location("diskstats_rollover", helper)
    rollover = importlib.util.module_from_spec(helper_spec)
    helper_spec.loader.exec_module(rollover)
    proof = json.loads((ROOT / "evidence/diskstats-rollover-dryrun-proof.json").read_text())
    correction = json.loads((result / "counter-rollover-correction.json").read_text())
    rollover_audit = json.loads((result / "counter-rollover-audit.json").read_text())
    if hashlib.sha256(helper.read_bytes()).hexdigest() != proof.get("helperSha256"):
        raise SystemExit("packaged rollover helper hash does not match its offline proof")
    raw_hash = hashlib.sha256(raw_bytes).hexdigest()
    proof_run = proof.get("100GiB", {})
    source_evidence = rollover_audit.get("sourceEvidence", {})
    if (source_evidence.get("samplesSha256") != raw_hash
            or proof_run.get("rawSha256BeforeAndAfter") != raw_hash
            or proof_run.get("rawUnchanged") is not True
            or proof_run.get("expectedPriorCorrectionEventsMatchedExactly") is not True
            or proof_run.get("allThreeWindowsFourDevicesSixReadWriteMetricsMatchSavedCorrectedSummary") is not True):
        raise SystemExit("rollover proof does not bind the immutable raw capture and corrected summary")
    normalized, audit = rollover.normalize_rows(rows)
    if audit.get("status") != "passed-with-corrections" or audit.get("rejections"):
        raise SystemExit(f"rollover normalizer did not pass cleanly: {audit.get('status')}")
    expected = correction.get("events", [])
    actual = audit.get("events", [])
    if len(expected) != 2 or len(actual) != 2:
        raise SystemExit("expected exactly the two independently audited 100-GiB read-time wraps")
    for want, got in zip(expected, actual):
        checks = (
            (want.get("role"), got.get("role")),
            (want.get("device"), got.get("device")),
            (want.get("rowIndex"), got.get("rowIndex")),
            (want.get("majorMinor"), got.get("majorMinor")),
            (want.get("beforeTime"), got.get("beforeTime")),
            (want.get("afterTime"), got.get("afterTime")),
            (want.get("beforeReadMs"), got.get("beforeValue")),
            (want.get("afterReadMs"), got.get("afterValue")),
            (want.get("correctedDeltaReadMs"), got.get("correctedDelta")),
            (want.get("deltaReadOps"), got.get("deltaOperations")),
        )
        if any(left != right for left, right in checks) or want.get("device") != "data" or got.get("counter") != "rm":
            raise SystemExit("observed rollover events do not exactly match the published counter correction")
    raw_report = module.analyze(rows, timing, 5)
    if raw_report.get("coverage", {}).get("valid") is not False:
        raise SystemExit("raw 100-GiB analysis unexpectedly differs from its audited pre-correction invalid coverage")
    report = module.analyze(normalized, timing, 5)
    saved = json.loads((result / "summary.json").read_text())
    if report.get("coverage", {}).get("valid") is not True or saved.get("coverage", {}).get("valid") is not True:
        raise SystemExit("corrected 100-GiB analysis does not have valid coverage")
    metric_names = ("readIops", "readMBps", "readAwaitMs", "writeIops", "writeMBps", "writeAwaitMs")
    for window in ("vacuum", "replayDrain", "vacuumAndReplay"):
        for device in ("primary_data", "primary_wal", "replica_data", "replica_wal"):
            for metric in metric_names:
                left = report["windows"][window][device].get(metric)
                right = saved["windows"][window][device].get(metric)
                if left is None or right is None:
                    if left is not right:
                        raise SystemExit(f"corrected metric mismatch: {window}/{device}/{metric}")
                elif not math.isclose(float(left), float(right), rel_tol=1e-12, abs_tol=1e-12):
                    raise SystemExit(f"corrected metric mismatch: {window}/{device}/{metric}: {left} != {right}")
    rows = normalized
    print("100-GiB unsigned-32-bit correction verified against exactly two audited events and all 72 saved read/write metrics")
module.render(out, rows, timing, report)
print(f"wrote await.png, iops.png, throughput.png under {out}")
