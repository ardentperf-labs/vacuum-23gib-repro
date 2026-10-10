#!/usr/bin/env python3
"""Copy the existing exported VHD to a separate blob with AzCopy.

This is a data-transfer helper only. It obtains read-only export access for
the owned temporary source clone, after resetting its failed transfer grant. It never
changes the existing `prepared.vhd` copy, creates snapshots, or deletes resources. Azure SAS URLs and storage keys stay in memory; only a
sanitized completion record is written after successful transfer and checks.
"""
import argparse
import datetime as dt
import json
import os
import pathlib
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse


ROOT = pathlib.Path(__file__).resolve().parent
ART = ROOT / 'artifacts'
STATE_PATH = ROOT / 'state.json'
SOURCE_MANIFEST_PATH = ROOT.parent / '.state/prepared-snapshot-keeper.json'
MARKER_PATH = ART / 'fast-copy-complete.json'
CONTAINER = 'vhds'
SOURCE_BLOB = 'prepared.vhd'
DESTINATION_BLOB = 'prepared-azcopy-retry.vhd'
EXPECTED_SIZE_GIB = 512
EXPECTED_VHD_BYTES = EXPECTED_SIZE_GIB * (1024 ** 3) + 512
TOKEN_RE = re.compile(r'^[a-f0-9]{24}$')
QUERY_RE = re.compile(r'\?[^\s"\'<>}]*')

# Exact credential values and complete SAS URLs are redacted before any
# subprocess output can reach the terminal. Values are never written to disk.
HIDDEN = []


class FastCopyRefused(RuntimeError):
    pass


def emit(value):
    print(json.dumps(value, separators=(',', ':'), sort_keys=True), flush=True)


def sanitized_text(value):
    if not isinstance(value, str):
        value = str(value)
    for secret in HIDDEN:
        if secret:
            value = value.replace(secret, '<redacted>')
    return QUERY_RE.sub('?<redacted>', value)


def sanitized_tree(value):
    if isinstance(value, dict):
        return {sanitized_text(k): sanitized_tree(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitized_tree(v) for v in value]
    if isinstance(value, str):
        return sanitized_text(value)
    return value


def read_json(path):
    if not path.is_file():
        raise FastCopyRefused(f'Required local evidence is missing: {path.name}')
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise FastCopyRefused(f'Could not read valid JSON from {path.name}') from exc


def require(condition, message):
    if not condition:
        raise FastCopyRefused(message)


def id_key(value, label):
    require(isinstance(value, str) and value.startswith('/subscriptions/'),
            f'{label} ID is missing or malformed')
    return value.rstrip('/').lower()


def tags_of(resource, label):
    tags = resource.get('tags') or {}
    require(isinstance(tags, dict), f'{label} tags are malformed')
    return tags


def az_json(*args):
    """Run an Azure read that returns JSON, without forwarding raw diagnostics."""
    result = subprocess.run(['az', *args, '-o', 'json'], text=True,
                            capture_output=True, check=False)
    if result.returncode:
        raise FastCopyRefused(
            f'Azure read failed: az {args[0]} {args[1] if len(args) > 1 else ""} '
            f'(exit {result.returncode}); details suppressed')
    if not result.stdout.strip():
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise FastCopyRefused(
            f'Azure returned invalid JSON for az {args[0]}; details suppressed') from exc


def az_text(*args):
    """Run a credential-bearing CLI operation and keep its result in memory."""
    result = subprocess.run(['az', *args, '-o', 'tsv'], text=True,
                            capture_output=True, check=False)
    if result.returncode:
        raise FastCopyRefused(
            f'Azure operation failed: az {args[0]} {args[1] if len(args) > 1 else ""} '
            f'(exit {result.returncode}); details suppressed')
    return result.stdout.strip()


def parse_sas_expiry(uri, label):
    try:
        parsed = urllib.parse.urlsplit(uri)
        params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        values = params.get('se', [])
        require(len(values) == 1 and values[0], f'{label} SAS has no unique expiry')
        value = values[0]
        expiry = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=dt.timezone.utc)
        return expiry.astimezone(dt.timezone.utc)
    except (ValueError, TypeError) as exc:
        raise FastCopyRefused(f'{label} SAS expiry is missing or invalid') from exc


def get_storage_key(state):
    result = az_json('storage', 'account', 'keys', 'list',
                     '--resource-group', state['transport_rg'],
                     '--account-name', state['account'])
    require(isinstance(result, list) and result,
            'Could not obtain the owned storage-account key in memory')
    key = result[0].get('value') if isinstance(result[0], dict) else None
    require(isinstance(key, str) and key, 'Storage-account key is missing')
    HIDDEN.append(key)
    return key


def validate_owned_resources(state, keeper_manifest, account):
    token = state.get('token')
    require(isinstance(token, str) and TOKEN_RE.fullmatch(token),
            'Fixture ownership token is malformed')
    require(account and account.get('id') == state.get('subscription'),
            'Active Azure subscription does not match fixture state')
    require(account.get('id') == keeper_manifest.get('subscriptionId'),
            'Active subscription does not match the prepared snapshot manifest')
    require(state.get('location', '').lower() == 'westus2',
            'Fixture state does not identify westus2')
    require(state.get('source_location', '').lower() == 'eastus',
            'Fixture source location does not identify eastus')
    require(state.get('transport_rg') == 'rg-fio-hdd-transport-' + token,
            'Transport resource-group name does not match its owner token')
    require(state.get('keeper_rg') == 'rg-fio-hdd-keeper-westus2-' + token,
            'Keeper resource-group name does not match its owner token')
    require(state.get('disk') == 'fixture-export' and state.get('account'),
            'Owned transfer disk or storage account name is missing')
    require(state.get('source_snapshot') ==
            keeper_manifest.get('retainedSnapshot', {}).get('id'),
            'Source snapshot ID differs from the prepared snapshot manifest')

    group = az_json('group', 'show', '--name', state['transport_rg'])
    require(group and group.get('name', '').lower() == state['transport_rg'].lower(),
            'Owned transport resource group is missing or has a different name')
    group_tags = tags_of(group, 'Transport group')
    require(group_tags.get('managed-by') == 'fio-hdd-region-transport' and
            group_tags.get('reprotoken') == token and
            group_tags.get('preserve-all-snapshots') == 'true',
            'Transport resource-group ownership/preservation tags do not match')
    require(group.get('location', '').lower() == 'westus2',
            'Transport resource group has an unexpected location')

    disk = az_json('disk', 'show', '--ids', state.get('export_disk_id', ''))
    require(disk is not None, 'Owned export disk is missing')
    require(id_key(disk.get('id'), 'Export disk') ==
            id_key(state.get('export_disk_id'), 'State export disk'),
            'Export disk ID differs from fixture state')
    require(disk.get('resourceGroup', '').lower() == state['transport_rg'].lower() and
            disk.get('name') == state['disk'],
            'Export disk is outside its owned transport group or has a different name')
    require(disk.get('location', '').lower() == 'eastus' and
            disk.get('diskSizeGB') == EXPECTED_SIZE_GIB,
            'Export disk is not the expected 512-GiB eastus disk')
    require(not disk.get('managedBy'), 'Export disk is attached; refusing to reuse its SAS')
    require(tags_of(disk, 'Export disk').get('reprotoken') == token,
            'Export disk ownership tag does not match')
    require(id_key((disk.get('creationData') or {}).get('sourceResourceId'),
                   'Export disk source snapshot') ==
            id_key(state['source_snapshot'], 'Fixture source snapshot'),
            'Export disk was not created from the recorded keeper snapshot')

    storage = az_json('storage', 'account', 'show', '--ids', state.get('storage_id', ''))
    require(storage is not None, 'Owned transfer storage account is missing')
    require(id_key(storage.get('id'), 'Storage account') ==
            id_key(state.get('storage_id'), 'State storage account'),
            'Storage-account ID differs from fixture state')
    require(storage.get('resourceGroup', '').lower() == state['transport_rg'].lower() and
            storage.get('name') == state['account'],
            'Storage account is outside its owned transport group or has a different name')
    require(storage.get('location', '').lower() == 'westus2',
            'Storage account is not in westus2')
    require(storage.get('kind') == 'StorageV2' and
            (storage.get('sku') or {}).get('name') == 'Standard_LRS',
            'Transfer storage account type/SKU differs from the owned fixture account')
    require(tags_of(storage, 'Storage account').get('reprotoken') == token,
            'Storage-account ownership tag does not match')
    require(storage.get('allowBlobPublicAccess') is False and
            storage.get('enableHttpsTrafficOnly') is True,
            'Transfer storage account is not confirmed private and HTTPS-only')
    endpoints = storage.get('primaryEndpoints') or {}
    endpoint = endpoints.get('blob')
    require(isinstance(endpoint, str) and endpoint.startswith('https://') and
            '?' not in endpoint,
            'Storage account has no plain HTTPS blob endpoint')
    endpoint = endpoint.rstrip('/') + '/'

    source_snapshot = az_json('snapshot', 'show', '--ids', state['source_snapshot'])
    require(source_snapshot is not None and
            id_key(source_snapshot.get('id'), 'Source snapshot') ==
            id_key(state['source_snapshot'], 'Fixture source snapshot'),
            'Recorded source snapshot is missing or has a different ID')
    require(source_snapshot.get('location', '').lower() == 'eastus' and
            source_snapshot.get('diskSizeGB') == EXPECTED_SIZE_GIB and
            source_snapshot.get('provisioningState') == 'Succeeded' and
            not source_snapshot.get('incremental', False),
            'Source snapshot is not the expected full 512-GiB eastus snapshot')

    return disk, storage, endpoint, token


def blob_show(state, key, blob_name):
    return az_json('storage', 'blob', 'show',
                   '--account-name', state['account'], '--account-key', key,
                   '--container-name', CONTAINER, '--name', blob_name,
                   '--auth-mode', 'key')


def blob_exists(state, key, blob_name):
    result = az_json('storage', 'blob', 'exists',
                     '--account-name', state['account'], '--account-key', key,
                     '--container-name', CONTAINER, '--name', blob_name,
                     '--auth-mode', 'key')
    require(isinstance(result, dict) and isinstance(result.get('exists'), bool),
            f'Could not safely determine whether {blob_name} exists')
    return result['exists']


def validate_private_container(state, key):
    container = az_json('storage', 'container', 'show',
                        '--account-name', state['account'], '--account-key', key,
                        '--name', CONTAINER, '--auth-mode', 'key')
    require(isinstance(container, dict), 'Owned destination container is missing')
    properties = container.get('properties') or {}
    public_access = container.get('publicAccess', properties.get('publicAccess'))
    require(public_access in (None, '', 'off', 'Off'),
            'Destination container is publicly accessible; refusing SAS copy')


def validate_page_blob(blob, blob_name, label):
    require(isinstance(blob, dict), f'{label} blob is missing')
    require(blob.get('name') == blob_name and blob.get('container') == CONTAINER,
            f'{label} blob name/container differs from the expected owned path')
    props = blob.get('properties') or {}
    require(props.get('blobType', '').lower() == 'pageblob',
            f'{label} is not a page blob')
    require(props.get('contentLength') == EXPECTED_VHD_BYTES,
            f'{label} length is not the expected 512-GiB VHD plus footer')
    return props


def validate_existing_marker(state, token, storage, key):
    marker = read_json(MARKER_PATH)
    require(marker.get('status') == 'Succeeded' and
            marker.get('subscriptionId') == state['subscription'] and
            marker.get('ownershipToken') == token and
            marker.get('sourceDiskId') == state['export_disk_id'] and
            marker.get('sourceSnapshotId') == state['source_snapshot'] and
            marker.get('storageAccountId') == state['storage_id'] and
            marker.get('destinationBlob') == DESTINATION_BLOB and
            marker.get('container') == CONTAINER and
            marker.get('contentLengthBytes') == EXPECTED_VHD_BYTES,
            'Existing completion marker does not match current owned state')
    endpoint = ((storage.get('primaryEndpoints') or {}).get('blob') or '').rstrip('/') + '/'
    expected_uri = endpoint + CONTAINER + '/' + DESTINATION_BLOB
    require(marker.get('destinationBlobUri') == expected_uri,
            'Existing completion marker destination URI does not match owned storage')
    blob = blob_show(state, key, DESTINATION_BLOB)
    props = validate_page_blob(blob, DESTINATION_BLOB, 'Completed AzCopy destination')
    require((props.get('copy') or {}).get('status') not in ('pending', 'failed', 'aborted'),
            'Completed AzCopy destination reports an unfinished/failed blob copy')
    emit(dict(status='Succeeded', idempotent=True, destinationBlobUri=expected_uri,
              contentLengthBytes=EXPECTED_VHD_BYTES))


def build_source_sas(state, key):
    original = blob_show(state, key, SOURCE_BLOB)
    copy = ((original.get('properties') or {}).get('copy') or {})
    require(copy.get('status') == 'failed',
            'This retry requires the original server-side copy to have failed')
    require(original.get('name') == SOURCE_BLOB and original.get('container') == CONTAINER,
            'Unexpected original transfer blob')
    recorded_source = copy.get('source')
    require(isinstance(recorded_source, str) and recorded_source,
            'Existing prepared.vhd copy has no source URI')
    # Blob copy metadata omits the signature; obtain a usable read-only URL
    # directly from the owned export disk. No other source transfer is active.
    az_json('disk', 'revoke-access', '--ids', state['export_disk_id'])
    grant = az_json('disk', 'grant-access', '--ids', state['export_disk_id'],
                    '--access-level', 'Read', '--duration-in-seconds', '21600')
    source_sas = grant.get('accessSAS') or grant.get('accessSas')
    require(isinstance(source_sas, str) and source_sas,
            'Owned disk did not return read-only export access')
    HIDDEN.append(source_sas)
    require(source_sas.split('?')[0] == recorded_source.split('?')[0],
            'Export source URI differs from the original copy source')
    parsed = urllib.parse.urlsplit(source_sas)
    require(parsed.scheme == 'https' and parsed.hostname and
            (parsed.hostname.endswith('.blob.storage.azure.net') or
             parsed.hostname.endswith('.blob.core.windows.net')) and
            parsed.query,
            'Existing copy source is not a private HTTPS blob SAS URL')
    HIDDEN.append(source_sas)
    HIDDEN.append(parsed.query)
    # Managed-disk exports may use a stored access policy (`si`) rather than
    # an explicit `se` query value. The existing live copy uses the same grant;
    # AzCopy authenticates it and fails if it expires during the transfer.
    params = urllib.parse.parse_qs(parsed.query)
    if 'se' in params:
        expiry = parse_sas_expiry(source_sas, 'Existing source')
        require(expiry >= dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=30),
                'Existing source SAS expires too soon for a safe copy attempt')
    else:
        require('si' in params,
                'Source SAS has neither expiry nor a managed-disk access policy')
    return source_sas, copy.get('id'), copy.get('status')


def create_destination_sas(state, key, endpoint):
    now = dt.datetime.now(dt.timezone.utc)
    start = now - dt.timedelta(minutes=5)
    expiry = now + dt.timedelta(hours=6)
    sas = az_text('storage', 'blob', 'generate-sas',
                  '--account-name', state['account'], '--account-key', key,
                  '--container-name', CONTAINER, '--name', DESTINATION_BLOB,
                  '--permissions', 'rcw',
                  '--start', start.strftime('%Y-%m-%dT%H:%MZ'),
                  '--expiry', expiry.strftime('%Y-%m-%dT%H:%MZ'),
                  '--https-only')
    sas = sas[1:] if sas.startswith('?') else sas
    require(sas and '=' in sas, 'Azure did not return a valid destination SAS')
    HIDDEN.append(sas)
    destination = (endpoint + CONTAINER + '/' + DESTINATION_BLOB + '?' + sas)
    HIDDEN.append(destination)
    require(parse_sas_expiry(destination, 'Destination') >=
            dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=50),
            'Destination SAS lifetime is shorter than the requested six-hour window')
    return destination, endpoint + CONTAINER + '/' + DESTINATION_BLOB


def relay_azcopy_output(stream, label, messages):
    try:
        for line in iter(stream.readline, ''):
            value = line.strip()
            if not value:
                continue
            try:
                data = json.loads(value)
            except json.JSONDecodeError:
                data = sanitized_text(value)
            else:
                data = sanitized_tree(data)
            messages.put((label, data))
    finally:
        stream.close()
        messages.put((label, None))


def run_azcopy(executable, source_sas, destination_sas):
    azcopy_path = shutil.which(executable) if os.sep not in executable else executable
    require(azcopy_path and os.path.isfile(azcopy_path) and os.access(azcopy_path, os.X_OK),
            'AzCopy executable was not found or is not executable')

    with tempfile.TemporaryDirectory(prefix='fio-hdd-azcopy-') as private_dir:
        os.chmod(private_dir, 0o700)
        log_dir = pathlib.Path(private_dir) / 'logs'
        plan_dir = pathlib.Path(private_dir) / 'plans'
        log_dir.mkdir(mode=0o700)
        plan_dir.mkdir(mode=0o700)
        env = os.environ.copy()
        env['AZCOPY_LOG_LOCATION'] = str(log_dir)
        env['AZCOPY_JOB_PLAN_LOCATION'] = str(plan_dir)
        args = [azcopy_path, 'copy', source_sas, destination_sas,
                '--from-to=BlobBlob', '--blob-type=PageBlob', '--overwrite=false',
                '--log-level=NONE', '--output-type=json']
        try:
            process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       text=True, encoding='utf-8', errors='replace', bufsize=1, env=env)
        except OSError as exc:
            raise FastCopyRefused('Could not start AzCopy; command details suppressed') from exc

        messages = queue.Queue()
        threads = [
            threading.Thread(target=relay_azcopy_output,
                             args=(process.stdout, 'stdout', messages), daemon=True),
            threading.Thread(target=relay_azcopy_output,
                             args=(process.stderr, 'stderr', messages), daemon=True),
        ]
        for thread in threads:
            thread.start()
        streams_open = len(threads)
        while streams_open:
            try:
                label, data = messages.get(timeout=1)
            except queue.Empty:
                continue
            if data is None:
                streams_open -= 1
            else:
                emit(dict(event='azcopy-output', stream=label, data=data))
        return_code = process.wait()
        for thread in threads:
            thread.join()
        require(return_code == 0, f'AzCopy exited with status {return_code}')


def write_marker(state, token, source_copy_id, source_copy_status, destination_uri):
    marker = dict(
        status='Succeeded',
        completedAtUtc=dt.datetime.now(dt.timezone.utc).isoformat(),
        subscriptionId=state['subscription'],
        ownershipToken=token,
        location='westus2',
        sourceSnapshotId=state['source_snapshot'],
        sourceDiskId=state['export_disk_id'],
        sourceCopyId=source_copy_id,
        sourceCopyStatusAtStart=source_copy_status,
        storageAccountId=state['storage_id'],
        container=CONTAINER,
        destinationBlob=DESTINATION_BLOB,
        destinationBlobUri=destination_uri,
        contentLengthBytes=EXPECTED_VHD_BYTES,
        vhdSizeGiB=EXPECTED_SIZE_GIB,
        method='AzCopy BlobBlob PageBlob from owned read-only managed-disk export SAS',
    )
    # Use exclusive creation: this script never overwrites evidence or an
    # earlier completion marker.
    try:
        with MARKER_PATH.open('x') as marker_file:
            marker_file.write(json.dumps(marker, indent=2) + '\n')
            marker_file.flush()
            os.fsync(marker_file.fileno())
    except FileExistsError as exc:
        raise FastCopyRefused('Completion marker appeared during transfer; refusing overwrite') from exc
    except OSError as exc:
        raise FastCopyRefused('Could not write the completion marker') from exc
    emit(dict(status='Succeeded', idempotent=False, destinationBlobUri=destination_uri,
              contentLengthBytes=EXPECTED_VHD_BYTES, marker=MARKER_PATH.name))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--azcopy', required=True,
                        help='Path to the AzCopy executable installed by the caller')
    args = parser.parse_args()

    state = read_json(STATE_PATH)
    keeper_manifest = read_json(SOURCE_MANIFEST_PATH)
    account = az_json('account', 'show')
    disk, storage, endpoint, token = validate_owned_resources(state, keeper_manifest, account)
    key = get_storage_key(state)
    validate_private_container(state, key)

    if MARKER_PATH.exists():
        validate_existing_marker(state, token, storage, key)
        return

    require(not blob_exists(state, key, DESTINATION_BLOB),
            'Destination blob already exists without a completion marker; refusing overwrite')

    source_sas, source_copy_id, source_copy_status = build_source_sas(state, key)
    destination_sas, destination_uri = create_destination_sas(state, key, endpoint)
    require(destination_uri == endpoint + CONTAINER + '/' + DESTINATION_BLOB,
            'Destination SAS URI does not point into the validated private storage account')

    emit(dict(event='azcopy-start', status='Running', sourceBlob=SOURCE_BLOB,
              destinationBlob=DESTINATION_BLOB, contentLengthBytes=EXPECTED_VHD_BYTES,
              sourceCopyStatus=source_copy_status))
    emit(dict(event='grant-propagation-wait', seconds=60))
    time.sleep(60)
    run_azcopy(args.azcopy, source_sas, destination_sas)

    # Re-read and verify the destination through the owned account key. No SAS
    # value or raw Azure response is written to the artifact.
    copied = blob_show(state, key, DESTINATION_BLOB)
    props = validate_page_blob(copied, DESTINATION_BLOB, 'AzCopy destination')
    require((props.get('copy') or {}).get('status') not in ('pending', 'failed', 'aborted'),
            'Destination reports an unfinished or failed server-side copy')
    require(not copied.get('deleted', False), 'Destination blob is marked deleted')
    write_marker(state, token, source_copy_id, source_copy_status, destination_uri)


if __name__ == '__main__':
    try:
        main()
    except FastCopyRefused as exc:
        emit(dict(status='Refused', reason=sanitized_text(str(exc))))
        sys.exit(2)
    except Exception:
        # Avoid a Python traceback containing subprocess arguments or secrets.
        emit(dict(status='Error', reason='Unexpected failure; sensitive details suppressed'))
        sys.exit(1)
