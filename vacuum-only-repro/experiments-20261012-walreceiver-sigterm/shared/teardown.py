#!/usr/bin/env python3
"""Delete only verified owned snapshot-free experiment groups, preserving all snapshots."""
import datetime, fcntl, json, re, subprocess, time
from pathlib import Path
from contextlib import ExitStack

ROOT = Path(__file__).resolve().parent
SD = ROOT / '.state'

def save(name, data):
    (SD / name).write_text(json.dumps(data, indent=2) + '\n')

def teardown(stack):
    s = json.loads((SD / 'state.json').read_text())
    sub, token = s['subscriptionId'], s['ownerToken']
    assert re.fullmatch('[a-f0-9]{32}', token)
    expected = ['rg-vacuum-only-' + token, 'rg-vacuum-nodes-' + token]
    assert s['resourceGroup'] == expected[0]
    assert s['requestedNodeResourceGroup'] == expected[1]
    assert s.get('nodeResourceGroup', '') in ('', expected[1])
    locks = []
    for name in ('teardown.lock', 'test.lock', '02-prepare.lock', 'provision.lock'):
        f = stack.enter_context((SD / name).open('a')); fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB); locks.append(f)

    def az(*args, timeout=300):
        p = subprocess.run(['az', *args, '--subscription', sub, '-o', 'json'], capture_output=True, text=True, timeout=timeout)
        if p.returncode: raise RuntimeError(p.stderr[-3000:])
        return json.loads(p.stdout) if p.stdout.strip() else None

    assert az('account', 'show')['id'].lower() == sub.lower()
    before = az('snapshot', 'list')
    save('teardown-snapshots-before.json', before)
    ids = {x['id'].lower() for x in before}
    watcher = s.get('networkWatcher', {})
    if watcher.get('owned'):
        assert watcher['ownerToken'] == token
        assert watcher['resourceGroup'] == 'rg-vacuum-watcher-' + token
        expected.append(watcher['resourceGroup'])

    def verify_group(group):
        exists = az('group', 'exists', '-n', group)
        if not exists: return False
        g = az('group', 'show', '-n', group)
        assert g.get('tags', {}).get('reprotoken') == token, f'ownership mismatch: {group}'
        snapshots = az('snapshot', 'list', '-g', group)
        assert not snapshots, f'Cannot delete group containing snapshots: {group}'
        resources = az('resource', 'list', '-g', group)
        assert not any(x['type'].lower() == 'microsoft.compute/snapshots' for x in resources)
        return True

    # Recheck every scope before AKS deletion can cascade into the node group.
    for group in expected: verify_group(group)
    if (SD / 'test-state.json').exists() and az('group', 'exists', '-n', expected[1]):
        assert az('group', 'exists', '-n', expected[0]), 'Test nodes remain but the AKS resource group is absent; cannot prove VACUUM stopped'
        present = az('aks', 'list', '-g', expected[0])
        assert len(present) == 1 and present[0]['name'] == s['clusterName'], 'Test nodes remain without the expected AKS cluster; cannot prove VACUUM stopped'
    if az('group', 'exists', '-n', expected[0]):
        clusters = az('aks', 'list', '-g', expected[0])
        for c in clusters:
            assert c['name'] == s['clusterName'] and c.get('tags', {}).get('reprotoken') == token
            assert c['nodeResourceGroup'].lower() == expected[1].lower()
            if (SD / 'test-state.json').exists():
                assert c['provisioningState'] == 'Succeeded', 'Cannot verify VACUUM absence on an unavailable cluster'
                base = ['kubectl', '--kubeconfig', str(SD / 'kubeconfig')]
                p = subprocess.run(base + ['get', 'pods', '-n', 'repro', '-l', 'cnpg.io/cluster=hdd-pg', '-o', 'json'],
                                   capture_output=True, text=True, timeout=120, check=True)
                primaries = [x for x in json.loads(p.stdout)['items'] if x['metadata'].get('labels', {}).get('cnpg.io/instanceRole') == 'primary']
                assert len(primaries) == 1, 'Cannot verify VACUUM absence without the primary'
                name = primaries[0]['metadata']['name']
                p = subprocess.run(base + ['exec', '-n', 'repro', name, '-c', 'postgres', '--',
                    'psql', '-XAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'repro', '-c',
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'vacuum-only-%' AND backend_type='client backend';"],
                    capture_output=True, text=True, timeout=120, check=True)
                assert p.stdout.strip() == '0', 'VACUUM session still exists; let it finish before teardown'
            verify_group(expected[1])
            az('aks', 'delete', '-g', expected[0], '-n', c['name'], '--yes', timeout=3600)
    for group in expected:
        if verify_group(group):
            az('group', 'delete', '-n', group, '--yes', '--no-wait')
            deadline = time.monotonic() + 3600
            while az('group', 'exists', '-n', group):
                if time.monotonic() > deadline: raise RuntimeError('Deletion timeout: ' + group)
                time.sleep(15)
    after = az('snapshot', 'list')
    save('teardown-snapshots-after.json', after)
    missing = ids - {x['id'].lower() for x in after}
    assert not missing, f'Snapshots missing after cleanup: {missing}'
    save('cleanup.json', {'finishedAtUtc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
         'deletedGroups': expected, 'snapshotsBefore': len(before), 'snapshotsAfter': len(after),
         'allSnapshotsPreserved': True})
    s['status'] = 'deleted'; save('state.json', s)
    print('Owned infrastructure removed; all snapshots preserved.')

def main():
    with ExitStack() as stack:
        return teardown(stack)

if __name__ == '__main__': main()
