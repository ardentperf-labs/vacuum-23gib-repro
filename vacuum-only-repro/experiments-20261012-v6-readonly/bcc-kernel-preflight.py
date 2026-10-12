#!/usr/bin/env python3
"""Validate BCC on an owned empty node before restoring the large fixture."""
import importlib.util, json, subprocess, time, uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SD = ROOT / '.state'
spec = importlib.util.spec_from_file_location('collector', ROOT / 'instrumentation/collect.py')
collector = importlib.util.module_from_spec(spec)
spec.loader.exec_module(collector)
s = json.loads((SD / 'state.json').read_text())
assert s['status'] == 'ready'
token = s['ownerToken']
base = ['kubectl', '--kubeconfig', str(SD / 'kubeconfig')]
out = ROOT / 'preflight/bcc-kernel'
out.mkdir(parents=True, exist_ok=True)

def k(*args, body=None):
    p = subprocess.run(base + list(args), input=body, text=True, capture_output=True, timeout=120)
    if p.returncode: raise RuntimeError(p.stderr[-2000:])
    return p.stdout

assert json.loads(k('get', 'namespace', 'repro', '-o', 'json'))['metadata']['labels']['repro.stack.token'] == token
nodes = json.loads(k('get', 'nodes', '-o', 'json'))['items']
assert len(nodes) == 2
node = nodes[0]
prefix = f"azure:///subscriptions/{s['subscriptionId']}/resourceGroups/{s['nodeResourceGroup']}/providers/Microsoft.Compute/virtualMachineScaleSets/"
assert node['spec']['providerID'].lower().startswith(prefix.lower())
assert node['metadata']['labels']['node.kubernetes.io/instance-type'] == s['nodeVmSize']
name = 'bcc-kernel-probe-' + uuid.uuid4().hex[:10]
pod = collector.build_bcc_pod(name, 'repro', node['metadata']['name'], 'ubuntu:24.04')
pod['metadata']['labels']['repro.stack.token'] = token
command = pod['spec']['containers'][0]['args'][0]
assert 'exec "$tool" -mTD -j 1' in command
command = command.replace('exec "$tool" -mTD -j 1', '''"$tool" -mTD -j 1 20 >/tmp/probe.log 2>&1 &
trace_pid=$!
sleep 12
dd if=/proc/1/root/dev/nvme0n1 of=/dev/null bs=4096 count=4 iflag=direct
trace_rc=0
wait "$trace_pid" || trace_rc=$?
cat /tmp/probe.log
exit "$trace_rc"
''')
pod['spec']['containers'][0]['args'][0] = command
(out / 'pod-manifest.json').write_text(json.dumps(pod, indent=2)+'\n')
created = json.loads(k('create', '-f', '-', '-o', 'json', body=json.dumps(pod)))
uid = created['metadata']['uid']
try:
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        info = json.loads(k('get', 'pod', name, '-n', 'repro', '-o', 'json'))
        assert info['metadata']['uid'] == uid
        if info['status']['phase'] in ('Succeeded', 'Failed'): break
        time.sleep(2)
    logs = k('logs', name, '-n', 'repro', '--timestamps')
    (out / 'probe.log').write_text(logs)
    (out / 'pod-final.json').write_text(json.dumps(info, indent=2)+'\n')
    assert info['status']['phase'] == 'Succeeded', 'BCC kernel probe failed; inspect its logs'
    histograms = []
    for line in logs.splitlines():
        _, _, raw = line.partition(' ')
        parsed = collector.parse_bcc_histogram_line(raw)
        if parsed: histograms.append(parsed[0])
    assert 'nvme0n1' in histograms, 'No real OS-disk NVMe histogram from BCC kernel probe'
    (out / 'verified.json').write_text(json.dumps({'passed':True,'node':node['metadata']['name'],
        'vmSize':s['nodeVmSize'],'podUid':uid,'devicesObserved':sorted(set(histograms))},indent=2)+'\n')
finally:
    current = json.loads(k('get', 'pod', name, '-n', 'repro', '-o', 'json'))
    assert current['metadata']['uid'] == uid and current['metadata']['labels']['repro.stack.token'] == token
    k('delete', 'pod', name, '-n', 'repro', '--wait=true', '--timeout=90s')
print('BCC kernel/NVMe preflight passed before fixture restore.')
