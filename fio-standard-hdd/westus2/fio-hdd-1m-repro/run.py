#!/usr/bin/env python3
"""Wait for the fixture, run the read sweep and repeat, then collect/analyze.

Leaves all infrastructure intact for verification and explicit teardown.
"""
import argparse
import datetime as dt
import json
import pathlib
import shutil
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parent
ART = ROOT / 'artifacts'
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--vm-size', default='Standard_E2bs_v5')
parser.add_argument('--analysis-python', required=True, type=pathlib.Path)
parser.add_argument('--fixture-manifest', type=pathlib.Path,
                    default=ROOT.parent / '.state/fio-hdd-westus2-fixture.json')
args = parser.parse_args()
analysis_python = args.analysis_python.absolute()
assert analysis_python.is_file()
ART.mkdir(exist_ok=True)

def log(message):
    print(dt.datetime.now(dt.timezone.utc).isoformat(), message, flush=True)

while not args.fixture_manifest.is_file():
    time.sleep(30)
log('Verified fixture manifest is available; attaching the data disk')
subprocess.run(['python3', str(ROOT / 'provision.py'), '--fixture-manifest',
                str(args.fixture_manifest), '--vm-size', args.vm_size], check=True)
state = json.loads((ROOT / 'state.json').read_text())
vm = json.loads((ART / 'vm.json').read_text())
assert vm['storageProfile']['diskControllerType'].lower() == 'scsi', 'Azure latency requires SCSI'
assert len(vm['storageProfile']['dataDisks']) == 1
assert vm['storageProfile']['dataDisks'][0]['lun'] == 0
assert vm['storageProfile']['dataDisks'][0]['caching'] == 'None'
ssh = ['ssh', '-o', 'StrictHostKeyChecking=accept-new', '-o', 'ConnectTimeout=20',
       '-i', str(ROOT / 'ssh-key'), 'fioadmin@' + state['ip']]
scp = ['scp', '-q', '-o', 'StrictHostKeyChecking=accept-new', '-i', str(ROOT / 'ssh-key')]
subprocess.run([*ssh, 'sudo cloud-init status --wait; test -f /var/tmp/fio-ready; fio --version'], check=True)
subprocess.run([*scp, str(ROOT / 'guest-test.py'), 'fioadmin@' + state['ip'] + ':/home/fioadmin/guest-test.py'], check=True)

def launch(extra, logfile):
    command = ('nohup sudo python3 -u /home/fioadmin/guest-test.py ' + extra
               + ' > /home/fioadmin/' + logfile + ' 2>&1 < /dev/null &')
    subprocess.run([*ssh, command], check=True)

def await_guest(directory, logfile):
    code = """import json,pathlib
r=pathlib.Path('/home/fioadmin/DIRECTORY')
metas=[json.loads(p.read_text()) for p in r.glob('run-*/meta.json')
       if p.parent.name.removeprefix('run-').isdigit()]
import subprocess
running=subprocess.run(['pgrep','-fc','python3 -u /home/fioadmin/guest-test.py'],capture_output=True,text=True).stdout.strip()
print(json.dumps(dict(complete=(r/'complete.json').exists(),
 running=int(running or 0),
 completed=[m['threads'] for m in metas if m['returncode']==0],
 errors=[m['threads'] for m in metas if m['returncode']!=0],
 tail=pathlib.Path('/home/fioadmin/LOGFILE').read_text().splitlines()[-1:])))
""".replace('DIRECTORY', directory).replace('LOGFILE', logfile)
    failures = 0
    while True:
        time.sleep(30)
        result = subprocess.run([*ssh, 'python3 -'], input=code, capture_output=True, text=True)
        if result.returncode:
            failures += 1
            assert failures < 5, 'Five consecutive guest status queries failed'
            log('Guest status query failed; retrying')
            continue
        failures = 0
        status = json.loads(result.stdout)
        log(status)
        assert not status['errors'], status
        assert status['complete'] or status['running'], 'Guest test exited without completion: ' + str(status)
        if status['complete']:
            return status

launch('', 'guest-test.log')
log('Main read-only sweep launched')
status = await_guest('results', 'guest-test.log')
if '1m' in ROOT.name:
    assert {768, 1024} <= set(status['completed']), 'Requested extra points are missing'
subprocess.run(['python3', str(ROOT / 'collect.py')], check=True)
launch('--single-validation', 'repeat-test.log')
log('Single-thread repeat launched')
await_guest('repeat-single', 'repeat-test.log')
repeat = ART / 'single-validation'
repeat.mkdir(exist_ok=True)
subprocess.run([*scp, '-r', 'fioadmin@' + state['ip'] + ':/home/fioadmin/repeat-single/.', str(repeat)], check=True)
subprocess.run([*scp, 'fioadmin@' + state['ip'] + ':/home/fioadmin/repeat-test.log', str(repeat)], check=True)
log('Waiting four minutes for final Azure metric ingestion')
time.sleep(240)
subprocess.run(['python3', str(ROOT / 'collect.py')], check=True)
for name in ('vm-metrics.json', 'disk-metrics.json', 'host-metrics.json'):
    shutil.copy2(ART / name, repeat / name)
for folder in (ART, repeat):
    subprocess.run([str(analysis_python), str(ROOT / 'analyze.py'), '--artifacts', str(folder)], check=True)
(ART / 'controller-complete.json').write_text(json.dumps(dict(
    end=dt.datetime.now(dt.timezone.utc).isoformat(), vm_size=state['vm_size']), indent=2))
log('Collection and analysis complete; resources retained for verification')
