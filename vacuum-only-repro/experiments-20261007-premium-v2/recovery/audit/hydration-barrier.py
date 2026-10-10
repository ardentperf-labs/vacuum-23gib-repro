#!/usr/bin/env python3
"""Read live Azure copy states before starting any measurement."""
import concurrent.futures
import datetime as dt
import json
from pathlib import Path
import subprocess
import urllib.request

ROOT = Path(__file__).resolve().parent
RUNS = ('baseline-23gib', '50gib', '100gib')


def verify():
    token = json.loads(subprocess.check_output(['az', 'account', 'get-access-token',
        '--resource', 'https://management.azure.com/', '-o', 'json'], text=True, timeout=900))['accessToken']

    def get(path):
        request = urllib.request.Request('https://management.azure.com' + path,
            headers={'Authorization': 'Bearer ' + token})
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.load(response)

    def collection(path):
        body = get(path)
        if body.get('nextLink'):
            raise RuntimeError('Unexpected pagination in isolated experiment resource group')
        return body['value']

    def failed_copy_pins(name, owner, source_snapshot_id, snapshot_group_id):
        path = ROOT / name / '.state/known-failed-source-copies.json'
        data = json.loads(path.read_text())
        if data.get('version') != 1 or not isinstance(data.get('snapshots'), list):
            raise RuntimeError(name + ': malformed known failed-copy pin inventory')
        pins = {}
        expected_group = snapshot_group_id.lower() + '/providers/microsoft.compute/snapshots/'
        for item in data['snapshots']:
            resource_id = item.get('id', '')
            unique_id = item.get('uniqueId')
            if (not resource_id.lower().startswith(expected_group) or not unique_id or
                item.get('ownerToken') != owner or item.get('reprotoken') != owner or
                item.get('sourceSnapshotId', '').lower() != source_snapshot_id.lower() or
                item.get('reproPurpose') != 'premiumv2-keeper-copy' or
                item.get('provisioningState') != 'Failed' or item.get('completionPercent') != 0.0):
                raise RuntimeError(name + ': invalid known failed-copy pin')
            key = resource_id.lower()
            if key in pins:
                raise RuntimeError(name + ': duplicate known failed-copy pin')
            pins[key] = item
        return pins

    def inspect(name):
        state = json.loads((ROOT / name / '.state/state.json').read_text())
        source = json.loads((ROOT / name / 'source-manifest.json').read_text())
        prefix = '/subscriptions/' + state['subscriptionId'] + '/resourceGroups/'
        node_rg = prefix + state['nodeResourceGroup']
        snap_rg = state['snapshotResourceGroupId']
        owner = state['ownerToken']
        source_snapshot_id = source['sourceSnapshotId']
        if (not snap_rg.lower().startswith(prefix.lower()) or
            len(snap_rg.split('/')) != 5 or
            source_snapshot_id.split('/')[2].lower() != state['subscriptionId'].lower()):
            raise RuntimeError(name + ': subscription or snapshot-group scope changed')
        for group in (node_rg, snap_rg):
            metadata = get(group + '?api-version=2024-03-01')
            if metadata.get('tags', {}).get('reprotoken') != owner:
                raise RuntimeError(name + ': live resource-group ownership changed')
        disks = collection(node_rg + '/providers/Microsoft.Compute/disks?api-version=2025-01-02')
        snapshots = collection(snap_rg + '/providers/Microsoft.Compute/snapshots?api-version=2025-01-02')
        pins = failed_copy_pins(name, owner, source_snapshot_id, snap_rg)
        evidence, pending, premium_count, seen_pins = [], [], 0, set()
        for disk in disks:
            is_premium = disk['sku']['name'] == 'PremiumV2_LRS'
            is_stage = disk.get('tags', {}).get('reproPurpose') == 'premiumv2-source-stage'
            props = disk['properties']
            resource_id = disk.get('id', '')
            if not resource_id.lower().startswith(node_rg.lower() + '/providers/microsoft.compute/disks/'):
                raise RuntimeError(name + ': disk escaped its owned node resource group')
            if is_stage and disk.get('tags', {}).get('reprotoken') != owner:
                raise RuntimeError(name + ': staging disk ownership changed')
            if is_premium:
                premium_count += 1
                sector = props.get('logicalSectorSize')
                if sector is None:
                    sector = (props.get('creationData') or {}).get('logicalSectorSize')
                size_gib = props.get('diskSizeGB')
                if size_gib is None:
                    size_gib = props.get('diskSizeGb')
                if (size_gib != 512 or props.get('diskIOPSReadWrite') != 3000 or
                    props.get('diskMBpsReadWrite') != 125 or str(sector) != '512' or
                    str(state['availabilityZone']) not in [str(zone) for zone in disk.get('zones', [])]):
                    raise RuntimeError(name + ': Premium disk settings changed')
            if is_stage and disk['sku']['name'] != 'Standard_LRS':
                raise RuntimeError(name + ': source staging disk is not Standard_LRS')
            option = props.get('creationData', {}).get('createOption')
            percent = props.get('completionPercent')
            provision = props.get('provisioningState')
            if provision == 'Failed' or props.get('copyCompletionError') is not None:
                raise RuntimeError(name + ': disk copy failed: ' + disk['id'])
            if is_premium:
                complete = provision == 'Succeeded' and (
                    percent == 100 if option in ('Copy', 'CopyStart') else
                    percent in (None, 100) if option == 'Empty' else False)
            elif is_stage:
                complete = (option == 'Copy' and provision == 'Succeeded' and
                            percent in (None, 100))
            elif option in ('Copy', 'CopyStart'):
                # Scan every disk in the owned node RG, not only PremiumV2 and
                # the tagged source stage. Other disk SKUs can still be copying.
                complete = provision == 'Succeeded' and percent == 100
            else:
                # Ordinary FromImage/Empty disks have no background copy field.
                complete = provision == 'Succeeded' and percent in (None, 100)
            if not complete:
                pending.append(disk['id'])
            evidence.append({'kind': 'disk', 'id': disk['id'], 'uniqueId': props.get('uniqueId'),
                'sku': disk['sku']['name'], 'createOption': option, 'state': provision,
                'completionPercent': percent, 'complete': complete,
                'premiumTarget': is_premium, 'sourceStage': is_stage})
        if premium_count != 4:
            raise RuntimeError(name + ': expected exactly four Premium SSD v2 disks')
        for snapshot in snapshots:
            props = snapshot['properties']
            tags = snapshot.get('tags', {})
            if tags.get('reprotoken') != owner:
                raise RuntimeError(name + ': retained snapshot ownership changed')
            option = props.get('creationData', {}).get('createOption')
            percent = props.get('completionPercent')
            provision = props.get('provisioningState')
            snapshot_id = snapshot.get('id', '')
            if not snapshot_id.lower().startswith(
                    snap_rg.lower() + '/providers/microsoft.compute/snapshots/'):
                raise RuntimeError(name + ': retained snapshot escaped its owned resource group')
            pin = pins.get(snapshot_id.lower())
            unique_id = props.get('uniqueId')
            failed_known_copy = (pin is not None and unique_id == pin.get('uniqueId') and
                pin.get('ownerToken') == owner and pin.get('reprotoken') == owner and
                provision == 'Failed' and props.get('snapshotAccessState') == 'Unknown' and
                option == 'CopyStart' and percent == pin.get('completionPercent') == 0.0 and
                tags.get('reproPurpose') == pin.get('reproPurpose') == 'premiumv2-keeper-copy' and
                tags.get('sourceSnapshotId', '').lower() == source_snapshot_id.lower() ==
                    pin.get('sourceSnapshotId', '').lower())
            if provision == 'Failed' and not failed_known_copy:
                raise RuntimeError(name + ': unexpected failed retained snapshot')
            if failed_known_copy:
                seen_pins.add(snapshot_id.lower())
            copy_error = props.get('copyCompletionError')
            complete = failed_known_copy or (provision == 'Succeeded' and
                props.get('snapshotAccessState') == 'Available' and
                option in ('Copy', 'CopyStart') and copy_error is None and
                percent in (None, 100) and
                (option != 'CopyStart' or percent == 100))
            if not complete:
                pending.append(snapshot_id)
            evidence.append({'kind': 'snapshot', 'id': snapshot_id, 'uniqueId': unique_id,
                'sku': snapshot['sku']['name'], 'createOption': option, 'state': provision,
                'completionPercent': percent, 'complete': complete,
                'copyCompletionError': copy_error,
                'terminalFailedCopyPreserved': failed_known_copy})
        missing_pins = set(pins) - seen_pins
        if missing_pins:
            raise RuntimeError(name + ': pinned failed source-copy snapshot is missing or changed')
        return {'stack': name, 'complete': not pending, 'pending': pending, 'resources': evidence}

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        stacks = list(pool.map(inspect, RUNS))
    receipt = {'checkedAtUtc': dt.datetime.now(dt.timezone.utc).isoformat(),
        'complete': all(stack['complete'] for stack in stacks), 'stacks': stacks,
        'basis': 'All measured restored disks require 100% copy completion; owned staging disks and retained snapshots must have no active copy. Known terminal failed copies are preserved.'}
    destination = ROOT / 'preflight/hydration-barrier.json'
    temporary = destination.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(receipt, indent=2) + '\n')
    temporary.replace(destination)
    return receipt


if __name__ == '__main__':
    receipt = verify()
    print(json.dumps(receipt, indent=2))
    raise SystemExit(0 if receipt['complete'] else 1)
