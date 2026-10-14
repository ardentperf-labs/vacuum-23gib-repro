#!/usr/bin/env python3
"""Verify downloaded bytes and record experiment completeness before teardown."""
import argparse, datetime, hashlib, json, tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
p = argparse.ArgumentParser(); p.add_argument('run_directory'); p.add_argument('--allow-incomplete', action='store_true'); a = p.parse_args()
run = Path(a.run_directory).resolve()
receipt = json.loads((ROOT / '.state/downloads.json').read_text())
assert receipt['remoteArtifactsDownloaded'] and receipt['files'], 'No download receipt'
local_receipts = {Path(item['path']).name: item for item in receipt['files']}
for item in receipt['files']:
    file = ROOT / item['path']
    assert file.stat().st_size == item['bytes']
    assert hashlib.sha256(file.read_bytes()).hexdigest() == item['sha256'], 'Checksum mismatch: ' + str(file)
downloaded = run / 'downloaded'
remote_transfers = receipt.get('chunkedRemoteTransfers', [])
remote_transfer_by_archive = {item.get('archive'): item for item in remote_transfers}
transfer_errors = []
for transfer in remote_transfers:
    archive_name = transfer.get('archive')
    local = local_receipts.get(archive_name)
    if (not archive_name or not local or not transfer.get('remoteTemporaryRemoved')
            or transfer.get('bytes') != local.get('bytes')
            or transfer.get('sha256') != local.get('sha256')
            or transfer.get('chunks', 0) < 1):
        transfer_errors.append(archive_name or '<unnamed transfer>')
for name in ('cluster-final.json', 'pods-final.json', 'pvcs-final.json', 'pvs-final.json', 'kubernetes-events-final.json'):
    json.loads((downloaded / name).read_text())
pods = json.loads((downloaded / 'pods-final.json').read_text())['items']
for pod in pods:
    if pod['metadata'].get('labels', {}).get('cnpg.io/cluster') != 'hdd-pg': continue
    if not any(item['name'] == 'postgres' for item in pod['spec']['containers']): continue
    name = pod['metadata']['name']
    assert (downloaded / (name + '-postgres.log')).stat().st_size > 0
    with tarfile.open(downloaded / (name + '-controller-logs.tar.gz'), 'r:gz') as archive:
        archive.getmembers()
    expected_archive = name + '-controller-logs.tar.gz'
    if expected_archive not in remote_transfer_by_archive:
        transfer_errors.append(expected_archive)
required = [
    'primary-controller-queues.log', 'replica-controller-queues.log',
    'primary-guest-cache.log', 'replica-guest-cache.log', 'guest-cache-clearing.json',
    'measurement/samples.jsonl', 'measurement/timing.json', 'measurement/summary.json', 'measurement/vacuum.log',
    'replica-instrumentation/pgstat-receiver.jsonl', 'replica-instrumentation/diskstats.jsonl',
    'replica-instrumentation/biolatency.raw.log', 'replica-instrumentation/biolatency-histograms.jsonl',
    'replica-instrumentation/azure-monitor-polls.jsonl', 'replica-instrumentation/azure-vm-uncached-iops.jsonl',
    'replica-instrumentation/coverage.json', 'replica-instrumentation/collector-status.json',
]
missing = [name for name in required if not (run / name).exists() or (run / name).stat().st_size == 0]
timing = json.loads((run / 'measurement/timing.json').read_text()) if (run / 'measurement/timing.json').exists() else {}
coverage_file = run / 'replica-instrumentation/coverage.json'
coverage = json.loads(coverage_file.read_text()) if coverage_file.exists() else {}
counts = {
    'pgStatSamples': coverage.get('pgStatActivityAndWalReceiver', {}).get('successfulSamples', 0),
    'diskstatsSamples': coverage.get('diskstats', {}).get('successfulSamples', 0),
    'biolatencyHistograms': coverage.get('biolatency', {}).get('deviceHistogramsParsed', 0),
    'azureMetricPoints': coverage.get('azureMonitor', {}).get('uniquePoints', 0),
}
def stamp(value): return datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
valid_azure_times = []
azure_file = run / 'replica-instrumentation/azure-vm-uncached-iops.jsonl'
if azure_file.exists():
    for line in azure_file.read_text().splitlines():
        point = json.loads(line)
        if point.get('averagePercent') is not None: valid_azure_times.append(stamp(point['timeStamp']))
azure_covers_run = False
if valid_azure_times and timing.get('vacuumStart') and timing.get('observationEnd'):
    first = stamp(timing['vacuumStart']).replace(second=0, microsecond=0)
    final = (stamp(timing['observationEnd']) - datetime.timedelta(minutes=1)).replace(second=0, microsecond=0)
    azure_covers_run = any(x >= first for x in valid_azure_times) and max(valid_azure_times) >= final
histograms = coverage.get('biolatency', {})
both_devices_observed = histograms.get('dataDeviceObservedInHistogram') and histograms.get('walDeviceObservedInHistogram')
complete = (not missing and not transfer_errors and bool(timing.get('testComplete')) and all(counts.values()) and not coverage.get('fatalErrors')
            and azure_covers_run and coverage.get('azureMetricDrain', {}).get('complete') and both_devices_observed)
report = {'checksumsVerified': True, 'remoteLogsVerified': not transfer_errors, 'experimentComplete': complete,
          'missingDatasets': missing, 'remoteTransferErrors': transfer_errors,
          'counts': counts, 'measurementTestComplete': timing.get('testComplete', False),
          'incompleteExperimentRecorded': a.allow_incomplete, 'collectorCoverage': coverage,
          'azureValidPointsCoverRun': azure_covers_run, 'bothDevicesObservedInHistograms': bool(both_devices_observed)}
(run / 'download-verification.json').write_text(json.dumps(report, indent=2) + '\n')
assert complete or a.allow_incomplete, 'Incomplete experiment; see download-verification.json'
print('Downloaded bytes verified; experiment completeness: ' + str(complete))
