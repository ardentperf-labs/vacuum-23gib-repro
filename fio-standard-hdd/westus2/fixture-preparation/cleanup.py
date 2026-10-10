#!/usr/bin/env python3
"""Safely remove only the temporary VHD-transfer resource group.

This script never deletes snapshots. It first checks that both West US 2
experiment groups are gone and that the regional full snapshot is verified in
its separate keeper resource group. Without --execute it performs preflight
checks only.
"""
import argparse
import datetime as dt
import json
import pathlib
import re
import subprocess
import sys
import time


ROOT = pathlib.Path(__file__).resolve().parent
ART = ROOT / 'artifacts'
STATE_PATH = ROOT / 'state.json'
FIXTURE_MANIFEST_PATH = ROOT.parent / '.state/fio-hdd-westus2-fixture.json'
SOURCE_MANIFEST_PATH = ROOT.parent / '.state/prepared-snapshot-keeper.json'
if not STATE_PATH.is_file():
    STATE_PATH = ART / 'infrastructure-state.json'
if not FIXTURE_MANIFEST_PATH.is_file():
    FIXTURE_MANIFEST_PATH = ROOT.parent / 'fixture.json'
if not SOURCE_MANIFEST_PATH.is_file():
    SOURCE_MANIFEST_PATH = ROOT.parent.parent / 'eastus/fixture.json'
TOKEN_RE = re.compile(r'^[a-f0-9]{24}$')


class CleanupRefused(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise CleanupRefused(message)


def read_json(path):
    require(path.is_file(), f'Required local evidence is missing: {path.name}')
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise CleanupRefused(f'Could not read valid JSON from {path.name}') from exc


def az_json(*args):
    result = subprocess.run(['az', *args, '-o', 'json'], text=True,
                            capture_output=True, check=False)
    if result.returncode:
        # Do not forward arbitrary CLI output into this cleanup's artifacts.
        raise CleanupRefused(f'Azure CLI read/operation failed: az {args[0]} {args[1] if len(args) > 1 else ""} (exit {result.returncode})')
    if not result.stdout.strip():
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise CleanupRefused(f'Azure CLI returned invalid JSON for az {args[0]}') from exc


def az_delete_group(name):
    result = subprocess.run(['az', 'group', 'delete', '--name', name, '--yes', '-o', 'none'],
                            text=True, capture_output=True, check=False)
    if result.returncode:
        raise CleanupRefused(f'Azure CLI failed to delete the owned transport group (exit {result.returncode})')


def id_key(value):
    require(isinstance(value, str) and value.startswith('/subscriptions/'),
            'Encountered a missing or malformed Azure resource ID')
    return value.rstrip('/').lower()


def tags_of(resource, label):
    tags = resource.get('tags') or {}
    require(isinstance(tags, dict), f'{label} has malformed tags')
    return tags


def require_owned_group(group, name, token, managed_by, label):
    require(group is not None, f'{label} resource group does not exist')
    tags = tags_of(group, label)
    require(group.get('name', '').lower() == name.lower(), f'{label} resource-group name changed')
    require(tags.get('managed-by') == managed_by, f'{label} managed-by tag does not match')
    require(tags.get('reprotoken') == token, f'{label} ownership token does not match')
    return group


def group_exists(name):
    result = az_json('group', 'exists', '--name', name)
    require(isinstance(result, bool), f'Could not verify whether resource group {name} exists')
    return result


def experiment_groups_gone(subscription):
    checked = []
    for local_name, archive_name, label in (
            ('fio-hdd-westus2-4k-repro', 'fio-hdd-repro', '4 KiB'),
            ('fio-hdd-westus2-1m-repro', 'fio-hdd-1m-repro', '1 MiB')):
        directory = ROOT.parent / local_name
        if not directory.is_dir():
            directory = ROOT.parent / archive_name
        state_path = directory / 'state.json'
        if not state_path.is_file():
            state_path = directory / 'artifacts/infrastructure-state.json'
        state = read_json(state_path)
        token = state.get('token')
        rg = state.get('rg')
        require(state.get('subscription') == subscription,
                f'{label} experiment state belongs to another subscription')
        require(state.get('location', '').lower() == 'westus2',
                f'{label} experiment is not recorded in westus2')
        require(isinstance(token, str) and TOKEN_RE.fullmatch(token),
                f'{label} experiment ownership token is malformed')
        require(rg == 'rg-fio-hdd-' + token,
                f'{label} experiment resource-group name does not match its ownership token')
        require(not group_exists(rg),
                f'{label} West US 2 experiment group still exists: {rg}')
        checked.append(dict(label=label, resource_group=rg, ownership_token=token,
                            verified_absent=True))
    return checked


def snapshot_inventory():
    snapshots = az_json('snapshot', 'list')
    require(isinstance(snapshots, list), 'Could not read the subscription snapshot inventory')
    ids = [id_key(snapshot.get('id')) for snapshot in snapshots]
    require(len(ids) == len(set(ids)), 'Azure returned duplicate snapshot IDs')
    return snapshots, set(ids)


def validate_snapshot(snapshot, *, expected_id, expected_rg, token, label):
    require(id_key(snapshot.get('id')) == id_key(expected_id), f'{label} snapshot ID does not match its manifest')
    require(snapshot.get('resourceGroup', '').lower() == expected_rg.lower(),
            f'{label} snapshot is not in its separate keeper resource group')
    require(snapshot.get('location', '').lower() == 'westus2', f'{label} snapshot is not in westus2')
    require(snapshot.get('provisioningState') == 'Succeeded', f'{label} snapshot is not provisioned')
    require(snapshot.get('diskSizeGB') == 512, f'{label} snapshot is not 512 GiB')
    require(not snapshot.get('incremental', False), f'{label} snapshot is not full')
    tags = tags_of(snapshot, label)
    require(tags.get('reprotoken') == token, f'{label} snapshot ownership token does not match')
    require(tags.get('preserve-all-snapshots') == 'true', f'{label} snapshot is missing the preservation tag')


def load_and_validate_manifests(subscription):
    state = read_json(STATE_PATH)
    manifest = read_json(FIXTURE_MANIFEST_PATH)
    source_manifest = read_json(SOURCE_MANIFEST_PATH)
    require(state.get('subscription') == subscription == manifest.get('subscriptionId'),
            'Current subscription does not match the fixture state')
    token = state.get('token')
    require(isinstance(token, str) and TOKEN_RE.fullmatch(token), 'Fixture ownership token is malformed')
    require(state.get('transport_rg') == 'rg-fio-hdd-transport-' + token,
            'Transport resource-group name does not match its ownership token')
    require(state.get('keeper_rg') == 'rg-fio-hdd-keeper-westus2-' + token,
            'Regional keeper resource-group name does not match its ownership token')
    require(state.get('location', '').lower() == 'westus2' and manifest.get('location', '').lower() == 'westus2',
            'Regional fixture is not recorded in westus2')
    require(manifest.get('originalSourceSnapshot', '').lower() == state.get('source_snapshot', '').lower(),
            'The source snapshot IDs in the fixture state and manifest differ')
    require(manifest.get('keeperResourceGroup') == state.get('keeper_rg'),
            'Regional keeper resource groups in state and manifest differ')
    require(manifest.get('keeperOwnerToken') == token, 'Regional keeper manifest has a different owner token')

    retained = manifest.get('retainedSnapshot') or {}
    target_id = retained.get('id')
    require(isinstance(target_id, str), 'Regional keeper snapshot ID is missing from its manifest')
    require(retained.get('diskSizeGiB') == 512 and retained.get('incremental') is False,
            'Regional keeper manifest does not describe a verified full 512-GiB snapshot')
    require(id_key(target_id) != id_key(state.get('source_snapshot')),
            'The regional copy unexpectedly aliases the original keeper snapshot')

    complete = read_json(ART / 'complete.json')
    require(isinstance(complete.get('end'), str), 'Fixture completion evidence is incomplete')
    regional_evidence = read_json(ART / 'retained-regional-snapshot.json')
    validate_snapshot(regional_evidence, expected_id=target_id, expected_rg=state['keeper_rg'],
                      token=token, label='Regional keeper evidence')
    require((regional_evidence.get('id') or '').lower() == target_id.lower(),
            'Regional snapshot artifact and manifest IDs differ')

    source_id = source_manifest.get('retainedSnapshot', {}).get('id')
    require(isinstance(source_id, str) and source_id.lower() == state['source_snapshot'].lower(),
            'Original source snapshot ID does not match the prepared keeper manifest')
    source_before = read_json(ART / 'source-snapshot.json')
    require(id_key(source_before.get('id')) == id_key(source_id),
            'Captured source snapshot evidence has a different ID')
    require(source_before.get('location', '').lower() == 'eastus' and
            source_before.get('diskSizeGB') == 512 and not source_before.get('incremental', False),
            'Captured source snapshot evidence is not the expected full eastus 512-GiB snapshot')
    require(source_before.get('provisioningState') == 'Succeeded',
            'Captured source snapshot is not provisioned')

    baseline = read_json(ART / 'snapshots-before.json')
    require(isinstance(baseline, list) and baseline, 'Original snapshot inventory evidence is missing')
    baseline_ids = {id_key(item.get('id')) for item in baseline}
    require(id_key(source_id) in baseline_ids, 'Original keeper snapshot is absent from the before-copy inventory')
    require(id_key(target_id) not in baseline_ids,
            'Regional keeper snapshot already appears in the before-copy inventory')
    return state, manifest, token, source_id, target_id, baseline_ids


def validate_transport_resources(state, token, group):
    transport_rg = state['transport_rg']
    require_owned_group(group, transport_rg, token, 'fio-hdd-region-transport', 'Transport')
    require(group.get('location', '').lower() == 'westus2', 'Transport resource group has an unexpected location')
    require(tags_of(group, 'Transport').get('preserve-all-snapshots') == 'true',
            'Transport group is missing the snapshot-preservation tag')
    require(transport_rg != state['keeper_rg'], 'Transport and keeper groups must be separate')

    disk = az_json('disk', 'show', '--ids', state.get('export_disk_id', ''))
    require(disk is not None, 'Owned export disk is missing')
    disk_id = id_key(disk.get('id'))
    require(disk_id == id_key(state.get('export_disk_id')), 'Export disk ID differs from local state')
    require(disk.get('resourceGroup', '').lower() == transport_rg.lower(),
            'Export disk is outside the transport resource group')
    require(disk.get('name') == state.get('disk'), 'Export disk name differs from local state')
    require(disk.get('location', '').lower() == 'eastus' and disk.get('diskSizeGB') == 512,
            'Export disk is not the expected 512-GiB eastus disk')
    require(not disk.get('managedBy'), 'Export disk is attached; refusing to remove its resource group')
    require(tags_of(disk, 'Export disk').get('reprotoken') == token,
            'Export disk ownership token does not match')

    storage = az_json('storage', 'account', 'show', '--ids', state.get('storage_id', ''))
    require(storage is not None, 'Owned transfer storage account is missing')
    storage_id = id_key(storage.get('id'))
    require(storage_id == id_key(state.get('storage_id')), 'Storage account ID differs from local state')
    require(storage.get('resourceGroup', '').lower() == transport_rg.lower(),
            'Storage account is outside the transport resource group')
    require(storage.get('name') == state.get('account'), 'Storage account name differs from local state')
    require(storage.get('location', '').lower() == 'westus2', 'Transfer storage account is not in westus2')
    require(tags_of(storage, 'Storage account').get('reprotoken') == token,
            'Storage account ownership token does not match')
    require(storage.get('allowBlobPublicAccess') is False,
            'Transfer storage account is not confirmed private')

    snapshots = az_json('snapshot', 'list', '--resource-group', transport_rg)
    require(isinstance(snapshots, list), 'Could not inspect snapshots in the transport group')
    require(not snapshots, 'Transport group contains a snapshot; refusing resource-group deletion')

    resources = az_json('resource', 'list', '--resource-group', transport_rg)
    require(isinstance(resources, list), 'Could not inspect all transport-group resources')
    require(not any((item.get('type') or '').lower() == 'microsoft.compute/snapshots'
                    for item in resources),
            'Transport group contains a snapshot resource; refusing deletion')

    direct = {disk_id, storage_id}
    listed_direct = set()
    known_storage_children = {
        storage_id + '/blobservices/default',
        storage_id + '/blobservices/default/containers/vhds',
    }
    for item in resources:
        resource_id = id_key(item.get('id'))
        if resource_id in direct:
            tags = tags_of(item, item.get('name', 'Transport resource'))
            require(tags.get('reprotoken') == token,
                    f'Owned transport resource {item.get("name", "unknown")} has a different owner token')
            listed_direct.add(resource_id)
            continue
        require(resource_id in known_storage_children,
                f'Unexpected or unowned resource exists in the transport group: {item.get("name", "unknown")}')
        require(resource_id.startswith(storage_id + '/'),
                'Unexpected storage child resource is outside the owned storage account')
    require(listed_direct == direct,
            'Transport resource inventory does not contain exactly the expected disk and storage account')
    return disk, storage, [dict(id=item['id'], type=item.get('type'), name=item.get('name'))
                           for item in resources]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true',
                        help='revoke active export access and delete only the verified transport resource group')
    args = parser.parse_args()

    state_hint = read_json(STATE_PATH)
    account = az_json('account', 'show')
    require(account and account.get('id') == state_hint.get('subscription'),
            'The active Azure subscription does not match the fixture state')
    subscription = account['id']
    experiment_checks = experiment_groups_gone(subscription)
    state, manifest, token, source_id, target_id, baseline_ids = load_and_validate_manifests(subscription)
    transport_group = az_json('group', 'show', '--name', state['transport_rg'])
    require_owned_group(transport_group, state['transport_rg'], token,
                        'fio-hdd-region-transport', 'Transport')

    keeper_group = az_json('group', 'show', '--name', state['keeper_rg'])
    require_owned_group(keeper_group, state['keeper_rg'], token,
                        'fio-hdd-regional-fixture', 'Regional keeper')
    require(keeper_group.get('location', '').lower() == 'westus2',
            'Regional keeper group is not in westus2')
    require(tags_of(keeper_group, 'Regional keeper').get('preserve-all-snapshots') == 'true',
            'Regional keeper group is missing the snapshot-preservation tag')

    source_live = az_json('snapshot', 'show', '--ids', source_id)
    require(id_key(source_live.get('id')) == id_key(source_id), 'Original keeper snapshot is missing')
    require(source_live.get('resourceGroup', '').lower() != state['transport_rg'].lower(),
            'Original keeper snapshot is inside the transport group')
    target_live = az_json('snapshot', 'show', '--ids', target_id)
    validate_snapshot(target_live, expected_id=target_id, expected_rg=state['keeper_rg'],
                      token=token, label='Regional keeper snapshot')

    before, before_ids = snapshot_inventory()
    require(baseline_ids <= before_ids, 'At least one pre-copy snapshot is missing; refusing cleanup')
    require(id_key(source_id) in before_ids and id_key(target_id) in before_ids,
            'Original or regional keeper snapshot is missing; refusing cleanup')

    disk, storage, resource_evidence = validate_transport_resources(state, token, transport_group)
    preflight = dict(subscription=subscription, transport_group=state['transport_rg'],
                     transport_group_id=transport_group.get('id'), export_disk_id=disk['id'],
                     storage_account_id=storage['id'], keeper_group=state['keeper_rg'],
                     original_snapshot_id=source_id, regional_snapshot_id=target_id,
                     experiment_groups=experiment_checks, resource_inventory=resource_evidence,
                     snapshot_count_before=len(before_ids), execute=args.execute)
    if not args.execute:
        print(json.dumps(dict(status='preflight-passed-no-changes', **preflight), indent=2))
        return 0

    # The Azure disk resource exposes ActiveSAS in diskState while a disk export
    # SAS is live. Revoke only when that state is present, then verify it cleared.
    disk_live = az_json('disk', 'show', '--ids', state['export_disk_id'])
    disk_state = (disk_live.get('diskState') or '').lower()
    require(disk_state in {'unattached', 'activesas'},
            f'Export disk has unexpected state {disk_live.get("diskState")!r}; refusing deletion')
    if disk_state == 'activesas':
        az_json('disk', 'revoke-access', '--ids', state['export_disk_id'])
        deadline = time.monotonic() + 120
        while True:
            disk_live = az_json('disk', 'show', '--ids', state['export_disk_id'])
            disk_state = (disk_live.get('diskState') or '').lower()
            require(disk_state in {'unattached', 'activesas'},
                    f'Export disk entered unexpected state {disk_live.get("diskState")!r}')
            if disk_state == 'unattached':
                break
            require(time.monotonic() < deadline,
                    'Export SAS remained active after revoke-access; refusing deletion')
            time.sleep(5)

    # Revalidate all deletion gates immediately before deleting the sole target.
    require(group_exists(state['transport_rg']), 'Transport group unexpectedly disappeared before deletion')
    experiment_checks = experiment_groups_gone(subscription)
    transport_group = az_json('group', 'show', '--name', state['transport_rg'])
    require_owned_group(transport_group, state['transport_rg'], token,
                        'fio-hdd-region-transport', 'Transport')
    disk, storage, resource_evidence = validate_transport_resources(state, token, transport_group)
    before, before_ids = snapshot_inventory()
    require(baseline_ids <= before_ids, 'Snapshot inventory changed before transport deletion')
    require(id_key(source_id) in before_ids and id_key(target_id) in before_ids,
            'A keeper snapshot disappeared before transport deletion')

    az_delete_group(state['transport_rg'])
    require(not group_exists(state['transport_rg']),
            'Azure did not confirm deletion of the transport resource group')
    after, after_ids = snapshot_inventory()
    evidence = dict(
        completed_at_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
        subscription=subscription,
        deleted_resource_group=state['transport_rg'],
        deleted_resource_group_id=transport_group.get('id'),
        deletion_scope='transport resource group only',
        experiment_groups=experiment_checks,
        deleted_resources=resource_evidence,
        original_snapshot_id=source_id,
        regional_keeper_snapshot_id=target_id,
        regional_keeper_resource_group=state['keeper_rg'],
        pre_copy_snapshot_ids=sorted(baseline_ids),
        snapshot_ids_before=sorted(s['id'] for s in before),
        snapshot_ids_after=sorted(s['id'] for s in after),
        pre_copy_snapshot_ids_preserved=baseline_ids <= after_ids,
        all_pre_delete_snapshot_ids_preserved=before_ids <= after_ids,
    )
    (ART / 'transport-cleanup.json').write_text(json.dumps(evidence, indent=2))
    require(baseline_ids <= after_ids,
            'Post-deletion inventory is missing a pre-copy snapshot; evidence was recorded')
    require(before_ids <= after_ids,
            'Post-deletion inventory is missing a pre-delete snapshot; evidence was recorded')
    require(id_key(source_id) in after_ids and id_key(target_id) in after_ids,
            'Post-deletion inventory is missing a keeper snapshot; evidence was recorded')
    print(json.dumps(dict(status='transport-group-deleted-snapshots-preserved',
                          transport_group=state['transport_rg'],
                          retained_snapshot_count=len(after_ids),
                          evidence='artifacts/transport-cleanup.json'), indent=2))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except CleanupRefused as exc:
        print(f'Cleanup refused: {exc}', file=sys.stderr)
        raise SystemExit(2)
