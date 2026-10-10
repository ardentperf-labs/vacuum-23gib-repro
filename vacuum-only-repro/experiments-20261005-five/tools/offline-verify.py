#!/usr/bin/env python3
"""Offline integrity/link check for an assembled experiment archive."""
import gzip, hashlib, json, re, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
manifest = json.loads((ROOT / "evidence/raw-capture-manifest.json").read_text())
errors = []
for row in manifest.get("rawCaptures", []):
    path = ROOT / row["destination"]
    if not path.is_file():
        errors.append(f"missing compressed raw capture: {row['destination']}")
        continue
    compressed = path.read_bytes()
    if hashlib.sha256(compressed).hexdigest() != row["gzipSha256"]:
        errors.append(f"compressed hash mismatch: {row['destination']}")
        continue
    raw = gzip.decompress(compressed)
    if hashlib.sha256(raw).hexdigest() != row["sourceSha256"]:
        errors.append(f"decompressed hash mismatch: {row['destination']}")
    if sum(bool(x.strip()) for x in raw.splitlines()) != row["rowCount"]:
        errors.append(f"raw row count mismatch: {row['destination']}")
link = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
for md in ROOT.rglob("*.md"):
    for target in link.findall(md.read_text(errors="replace")):
        target = target.split()[0].strip("<>")
        if not target or target.startswith(("https://", "http://", "mailto:", "#")):
            continue
        target = target.split("#", 1)[0]
        if target:
            resolved = (md.parent / target).resolve()
            try:
                resolved.relative_to(ROOT.resolve())
            except ValueError:
                errors.append(f"local link escapes archive in {md.relative_to(ROOT)}: {target}")
                continue
            if not resolved.is_file():
                errors.append(f"broken local link in {md.relative_to(ROOT)}: {target}")
for slug in ("baseline", "prefetch-off", "100gib", "prefetch-off-fpw-off", "read-pressure"):
    plot = ROOT / "runs" / slug / "source/as-run/plot.py"
    if not plot.is_file() or 'set_yscale("linear")' not in plot.read_text(errors="replace"):
        errors.append(f"linear-axis source check failed: {plot.relative_to(ROOT)}")
rollover_helper = ROOT / "tools/diskstats_rollover.py"
rollover_proof = ROOT / "evidence/diskstats-rollover-dryrun-proof.json"
if rollover_helper.is_file() and rollover_proof.is_file():
    proof = json.loads(rollover_proof.read_text())
    if hashlib.sha256(rollover_helper.read_bytes()).hexdigest() != proof.get("helperSha256"):
        errors.append("bundled unsigned-32-bit rollover helper hash does not match its offline proof")
    for name in ("await.png", "iops.png", "throughput.png"):
        old_graph = ROOT / "runs/100gib/results/100gib-20261005T130618Z/original-analysis-before-counter-rollover-correction" / name
        if not old_graph.is_file():
            errors.append(f"missing preserved pre-correction graph: {old_graph.relative_to(ROOT)}")
if errors:
    print("\n".join(errors), file=sys.stderr)
    raise SystemExit(1)
print(f"offline archive verification passed: {len(manifest.get('rawCaptures', []))} raw captures")
