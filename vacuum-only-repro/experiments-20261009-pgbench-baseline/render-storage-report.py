#!/usr/bin/env python3
"""Render final storage plots with first target arrival marked, preserving raw timing."""
from pathlib import Path
import datetime as dt
import hashlib
import json
import shutil
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT/'run'))
import plot


def main():
    results = ROOT/'run/results'
    timing_path = results/'timing.json'
    timing = json.loads(timing_path.read_text())
    if not timing.get('testComplete'):
        raise RuntimeError('Measurement must be complete before final rendering')
    events_path = results/'events.jsonl'
    events = [json.loads(line) for line in events_path.read_text().splitlines() if line.strip()]
    target = next(event for event in events if event.get('event') == 'replica_reached_vacuum_target_with_client')
    rows = [json.loads(line) for line in (results/'samples.jsonl').read_text().splitlines() if line.strip()]
    original_plots = results/'raw/collector-plots'
    original_plots.mkdir(exist_ok=True)
    for name in ('await.png', 'iops.png', 'throughput.png'):
        destination = original_plots/name
        if not destination.exists():
            shutil.copy2(results/name, destination)
    # Derive full-window counter series with the original measurement boundaries.
    # Only the displayed catch-up marker changes; raw timing/summary stay intact.
    report = plot.analyze(rows, timing, 5)
    displayed = dict(timing, replayEnd=target['time'])
    plot.render(results, rows, displayed, report)
    evidence = {'generatedAtUtc': dt.datetime.now(dt.timezone.utc).isoformat(),
                'markerMeaning': 'First observed replica arrival at the VACUUM completion target with the client active, confirmed by three healthy samples',
                'firstReplayTargetUtc': target['time'],
                'finalReplayVerificationUtc': timing['replayEnd'],
                'rawTimingPreserved': True, 'rawSummaryPreserved': True,
                'fullCounterWindowPreserved': True,
                'originalCollectorPlots': 'raw/collector-plots/',
                'inputSha256': {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in (timing_path, events_path, ROOT/'run/plot.py', Path(__file__))}}
    (results/'storage-plot-boundaries.json').write_text(json.dumps(evidence, indent=2)+'\n')
    print(json.dumps(evidence, indent=2))


if __name__ == '__main__':
    main()
