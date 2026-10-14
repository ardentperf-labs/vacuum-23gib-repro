#!/usr/bin/env python3
"""Download remaining experiment evidence and checksum all local result files."""
import datetime, hashlib, json, subprocess, sys, os, signal, time, uuid
from pathlib import Path
from remote_chunk_download import download_verified_file

ROOT = Path(__file__).resolve().parent
SD = ROOT / '.state'
# A recovered collector is started independently after a stream failure.
# Stop and drain its Azure ingestion before checksums or infrastructure cleanup.
recovery_launch = SD / 'instrumentation-recovery-launch.json'
if recovery_launch.exists():
    recovered = json.loads(recovery_launch.read_text())
    collector_pid = int(recovered['pid'])
    process_cmdline = Path(f'/proc/{collector_pid}/cmdline')
    if process_cmdline.exists() and process_cmdline.read_bytes():
        expected_collector = str(ROOT / 'instrumentation/collect.py').encode()
        if expected_collector not in process_cmdline.read_bytes().split(b'\0'):
            raise RuntimeError('Recovery collector PID identity changed; preserving infrastructure')
        try: os.kill(collector_pid, signal.SIGTERM)
        except ProcessLookupError: pass
    deadline = time.monotonic() + 1800
    collector_status = ROOT / 'results/instrumentation/collector-status.json'
    while time.monotonic() < deadline:
        if collector_status.exists():
            status = json.loads(collector_status.read_text())
            if status.get('phase') in ('stopped', 'failed'):
                break
        time.sleep(3)
    else: raise RuntimeError('Recovered collector did not finish draining; preserving infrastructure')
    (ROOT / 'results/instrumentation/recovery-stop-receipt.json').write_text(json.dumps({
        'stoppedAtUtc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'launch': recovered, 'finalStatus': status}, indent=2) + '\n')
watcher_launch = SD / 'watcher-recovery-launch.json'
if watcher_launch.exists():
    watchers = json.loads(watcher_launch.read_text())['watchers']
    stopped_watchers = []
    for watcher in watchers:
        pid = int(watcher['pid']); process_file = Path(f'/proc/{pid}/cmdline')
        if process_file.exists() and process_file.read_bytes():
            if watcher['expectedScriptPath'].encode() not in process_file.read_bytes().split(b'\0'):
                raise RuntimeError('Recovery watcher PID identity changed; preserving infrastructure')
            try: os.kill(pid, signal.SIGTERM)
            except ProcessLookupError: pass
        end = time.monotonic() + 60
        while process_file.exists() and process_file.read_bytes() and time.monotonic() < end:
            time.sleep(1)
        if process_file.exists() and process_file.read_bytes():
            raise RuntimeError('Recovery watcher did not stop; preserving infrastructure')
        stopped_watchers.append(watcher)
    (ROOT/'results/preparation/watcher-stop-receipt.json').write_text(json.dumps({
        'stoppedAtUtc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'watchers': stopped_watchers}, indent=2) + '\n')
dest = Path(sys.argv[1]).resolve()
dest.mkdir(parents=True, exist_ok=True)
base = ['kubectl', '--kubeconfig', str(SD / 'kubeconfig')]
remote_transfers = []

def run(args, filename, timeout=120):
    last = None
    for _attempt in range(3):
        try:
            p = subprocess.run(base + args, capture_output=True, timeout=timeout)
        except (OSError, subprocess.TimeoutExpired) as exc:
            last = str(exc)
            continue
        if p.returncode == 0:
            temporary = dest / ('.' + filename + '.partial')
            temporary.write_bytes(p.stdout)
            os.replace(temporary, dest / filename)
            if p.stderr: (dest / (filename + '.stderr')).write_bytes(p.stderr)
            return
        last = p.stderr.decode(errors='replace')[-1500:]
    raise RuntimeError(f'Harvest failed for {filename} after 3 attempts: {last}')

def exec_remote(pod, container, args, timeout=120):
    command = base + ['exec', '-n', 'repro', pod, '-c', container, '--'] + args
    return subprocess.run(command, capture_output=True, timeout=timeout)

def download_controller_logs(pod, destination):
    """Build the archive remotely, then fetch verified 2 MiB ranges.

    Keeping the archive in the pod lets every WebSocket exec response remain
    small. Remote and local size/SHA-256 checks reject truncated or changing
    data before the final local filename becomes visible.
    """
    remote_path = '/run/cnpg-controller-logs-' + uuid.uuid4().hex + '.tar.gz'
    create_script = r'''set -eu
f="$1"
umask 077
if [ -d /controller/log ]; then
  tar -czf "$f" -C /controller log
else
  tar -czf "$f" --files-from /dev/null
fi
stat -c '%s' -- "$f"
sha256sum -- "$f" | cut -d ' ' -f1
'''
    fingerprint_script = 'set -eu; stat -c %s -- "$1"; sha256sum -- "$1" | cut -d " " -f1'
    def fingerprint():
        last_error = None
        for _attempt in range(3):
            try:
                result = exec_remote(pod, 'postgres', ['sh', '-c', fingerprint_script, 'sh', remote_path], timeout=120)
                if result.returncode:
                    last_error = 'remote command returned nonzero'
                    continue
                lines = result.stdout.decode('ascii', errors='strict').splitlines()
                if len(lines) != 2:
                    last_error = 'remote output was malformed'
                    continue
                return int(lines[0]), lines[1]
            except (OSError, subprocess.SubprocessError, UnicodeError, ValueError) as exc:
                last_error = str(exc)
        raise RuntimeError(f'Remote archive fingerprint failed after 3 attempts: {last_error}')

    chunk_script = 'dd if="$1" bs=2097152 skip="$2" count=1 iflag=fullblock 2>/dev/null'
    def read_chunk(offset, expected_length):
        result = exec_remote(pod, 'postgres', ['sh', '-c', chunk_script, 'sh', remote_path,
                                                str(offset // (2 * 1024 * 1024))], timeout=120)
        if result.returncode or len(result.stdout) != expected_length:
            raise RuntimeError('Remote archive chunk was interrupted or short')
        return result.stdout

    try:
        created = exec_remote(pod, 'postgres', ['sh', '-c', create_script, 'sh', remote_path], timeout=300)
        if created.returncode:
            raise RuntimeError(f'Could not create controller-log archive in {pod}: {created.stderr.decode(errors="replace")[-1500:]}')
        transfer = download_verified_file(destination, fingerprint, read_chunk)
    finally:
        cleanup = None
        for _attempt in range(3):
            try:
                candidate = exec_remote(pod, 'postgres', ['rm', '-f', '--', remote_path], timeout=60)
            except (OSError, subprocess.SubprocessError):
                continue
            if candidate.returncode == 0:
                cleanup = candidate
                break
            cleanup = candidate
        if cleanup is None:
            raise RuntimeError(f'Remote temporary archive cleanup could not reach pod {pod}')
    transfer.update({'pod': pod, 'archive': destination.name,
                     'remoteTemporaryRemoved': cleanup.returncode == 0})
    if cleanup.returncode:
        raise RuntimeError(f'Remote temporary controller archive could not be removed from {pod}')
    remote_transfers.append(transfer)

run(['get', 'cluster', 'hdd-pg', '-n', 'repro', '-o', 'json'], 'cluster-final.json')
run(['get', 'pods', '-n', 'repro', '-o', 'json'], 'pods-final.json')
run(['get', 'pvc', '-n', 'repro', '-o', 'json'], 'pvcs-final.json')
run(['get', 'pv', '-o', 'json'], 'pvs-final.json')
run(['get', 'volumeattachments', '-o', 'json'], 'volumeattachments-final.json')
run(['get', 'events', '-A', '-o', 'json'], 'kubernetes-events-final.json')
run(['logs', '-n', 'cnpg-system', 'deployment/cnpg-controller-manager', '--timestamps'],
    'cnpg-operator-final.log', timeout=300)
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
    run(['logs', '-n', 'repro', name, '-c', 'postgres', '--timestamps'],
        name + '-postgres.log', timeout=300)
    # Download CNPG's structured log files as well as the kubectl log stream.
    download_controller_logs(pod=name, destination=dest / (name + '-controller-logs.tar.gz'))

checksums = []
stable_roots = [dest.parent, SD / 'attachment-verification']
stable_files = {p for directory in stable_roots for p in directory.rglob('*')}
stable_files.update(dest.parent.glob('*-controller-queues.log'))
stable_files.update(dest.parent.glob('cluster-watch.json'))
stable_files.update(dest.parent.glob('cnpg-operator.log'))
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
           'remoteArtifactsDownloaded': True, 'files': checksums,
           'chunkedRemoteTransfers': remote_transfers}
(SD / 'downloads.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(f'Harvested logs and metadata; checksummed {len(checksums)} local files.')
