#!/usr/bin/env python3
"""Download remaining experiment evidence and checksum all local result files."""
import datetime, hashlib, json, subprocess, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SD = ROOT / '.state'
dest = Path(sys.argv[1]).resolve()
dest.mkdir(parents=True, exist_ok=True)
base = ['kubectl', '--kubeconfig', str(SD / 'kubeconfig')]

def run(args, filename, timeout=120):
    p = subprocess.run(base + args, capture_output=True, timeout=timeout)
    (dest / filename).write_bytes(p.stdout)
    if p.stderr: (dest / (filename + '.stderr')).write_bytes(p.stderr)
    if p.returncode: raise RuntimeError(f'Harvest failed for {filename}: {p.stderr.decode()[-1500:]}')

run(['get', 'cluster', 'hdd-pg', '-n', 'repro', '-o', 'json'], 'cluster-final.json')
run(['get', 'pods', '-n', 'repro', '-o', 'json'], 'pods-final.json')
run(['get', 'pvc', '-n', 'repro', '-o', 'json'], 'pvcs-final.json')
run(['get', 'pv', '-o', 'json'], 'pvs-final.json')
run(['get', 'volumeattachments', '-o', 'json'], 'volumeattachments-final.json')
run(['get', 'events', '-A', '-o', 'json'], 'kubernetes-events-final.json')
pods = json.loads((dest / 'pods-final.json').read_text())['items']
for pod in pods:
    if pod['metadata'].get('labels', {}).get('cnpg.io/cluster') != 'hdd-pg': continue
    name = pod['metadata']['name']
    containers = [item['name'] for item in pod['spec']['containers']]
    if 'postgres' not in containers:
        for container in containers:
            run(['logs', '-n', 'repro', name, '-c', container, '--timestamps'],
                name + '-' + container + '.log')
        continue
    run(['logs', '-n', 'repro', name, '-c', 'postgres', '--timestamps'], name + '-postgres.log')
    # Download CNPG's structured log files as well as the kubectl log stream.
    run(['exec', '-n', 'repro', name, '-c', 'postgres', '--', 'sh', '-c',
         'if [ -d /controller/log ]; then tar -czf - -C /controller log; else tar -czf - --files-from /dev/null; fi'],
        name + '-controller-logs.tar.gz', timeout=300)

checksums = []
stable_roots = [dest, dest.parent / 'measurement', dest.parent / 'replica-instrumentation', SD / 'attachment-verification']
stable_files = {p for directory in stable_roots for p in directory.rglob('*')}
stable_files.update(dest.parent.glob('*-controller-queues.log'))
stable_files.update(dest.parent.glob('*-guest-cache.log'))
stable_files.update(dest.parent.glob('guest-cache-*.json'))
stable_files.update(dest.parent.glob('guest-cache-clear.log'))
stable_files.update(dest.parent.glob('instrumentation-readiness-checkpoint.log'))
for p in sorted(stable_files):
    if not p.is_file() or any(x in p.parts for x in ('__pycache__', 'instrumentation-pod')): continue
    if p.name in ('kubeconfig', 'downloads.json') or p.name.startswith('.connection'): continue
    h = hashlib.sha256()
    with p.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''): h.update(block)
    checksums.append({'path': str(p.relative_to(ROOT)), 'bytes': p.stat().st_size, 'sha256': h.hexdigest()})
receipt = {'finishedAtUtc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
           'remoteArtifactsDownloaded': True, 'files': checksums}
(SD / 'downloads.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(f'Harvested logs and metadata; checksummed {len(checksums)} local files.')
