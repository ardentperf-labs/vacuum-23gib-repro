"""Offline analysis only; retain raw samples and original analysis artifacts."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil

BASE = Path(__file__).resolve().parent
STACK = BASE / '100gib'
RUN = STACK / 'results/100gib-20261005T130618Z'
MOD = 2**32
spec = importlib.util.spec_from_file_location('unchanged_plot', STACK / 'plot.py')
plot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(plot)

def corrected_rows(rows):
    result = copy.deepcopy(rows)
    events = []
    for role in plot.ROLES:
        for dev in plot.DEVICES:
            offset = 0
            initial = rows[0][role]['devices'][dev]
            identity = lambda d: tuple(d.get(k) for k in ('majorMinor', 'path', 'claim', 'source'))
            key = initial['majorMinor']
            for i, row in enumerate(rows):
                sample = row[role]
                assert identity(sample['devices'][dev]) == identity(initial)
                current = sample['disks'][key]
                if i:
                    prior = rows[i-1][role]
                    previous = prior['disks'][key]
                    delta = current['rm'] - previous['rm']
                    if delta < 0:
                        seconds = sample['epoch'] - prior['epoch']
                        assert 0 < seconds <= 12.5
                        assert all(current[k] >= previous[k] for k in ('r', 'rs', 'w', 'ws', 'wm'))
                        assert 3*MOD/4 <= previous['rm'] < MOD and 0 <= current['rm'] < MOD/4
                        assert 0 < delta + MOD < 10_000_000
                        assert current['r'] > previous['r']
                        events.append({'role': role, 'device': dev, 'rowIndex': i,
                            'beforeTime': prior['ts'], 'afterTime': sample['ts'],
                            'seconds': seconds, 'majorMinor': key,
                            'beforeReadMs': previous['rm'], 'afterReadMs': current['rm'],
                            'rawDeltaReadMs': delta, 'correctedDeltaReadMs': delta + MOD,
                            'deltaReadOps': current['r'] - previous['r'],
                            'correctedReadAwaitMs': (delta+MOD)/(current['r']-previous['r'])})
                        offset += MOD
                result[i][role]['disks'][key]['rm'] += offset
    assert len(events) == 2 and all(e['role'] == 'replica' and e['device'] == 'data' for e in events)
    return result, events

def main():
    archive = RUN / 'original-analysis-before-counter-rollover-correction'
    assert not archive.exists(), 'Refuse to overwrite preserved original analysis'
    rows = [json.loads(line) for line in (RUN/'samples.jsonl').read_text().splitlines()]
    corrected, events = corrected_rows(rows)
    timing = json.loads((RUN/'timing.json').read_text())
    old = json.loads((RUN/'summary.json').read_text())
    report = plot.analyze(corrected, timing, 5)
    assert report['coverage']['valid']
    for key in ('status', 'walDeltaVacuumMarkers', 'walCounterDeltaFromBeforeVacuum'):
        report[key] = old[key]
    audit = {'status': 'passed-offline-counter-analysis', 'measurementRerun': False,
        'rawSamplesModified': False, 'correction': 'Unwrap only the two validated unsigned 32-bit replica DATA read-time-ms counter rollovers; all other counters and timestamps unchanged.',
        'reference': 'https://www.kernel.org/doc/html/v6.7/admin-guide/iostats.html',
        'events': events, 'sourceSha256': {str(p.relative_to(BASE)): hashlib.sha256(p.read_bytes()).hexdigest() for p in (RUN/'samples.jsonl', STACK/'plot.py', Path(__file__))},
        'originalCoverageValid': old['coverage']['valid'], 'correctedCoverageValid': report['coverage']['valid']}
    archive.mkdir()
    for name in ('summary.json', 'await.png', 'iops.png', 'throughput.png'):
        shutil.copy2(RUN/name, archive/name)
    report['offlineCounterRolloverCorrection'] = audit
    plot.render(RUN, corrected, timing, report)
    report.pop('_series')
    (RUN/'summary.json').write_text(json.dumps(report, indent=2, sort_keys=True)+'\n')
    (RUN/'counter-rollover-correction.json').write_text(json.dumps(audit, indent=2, sort_keys=True)+'\n')
    print(json.dumps(audit, indent=2))

if __name__ == '__main__':
    main()
