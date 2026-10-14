#!/usr/bin/env python3
"""Delete only verified owned snapshot-free experiment groups, preserving all snapshots."""
import datetime, fcntl, hashlib, json, os, re, subprocess, time
from pathlib import Path, PurePosixPath
from contextlib import ExitStack

ROOT = Path(__file__).resolve().parent
SD = ROOT / '.state'

def save(name, data):
    (SD / name).write_text(json.dumps(data, indent=2) + '\n')

def verify_declined_wal_archive_export(token, stack_name, sub, resource_group, storage_meta):
    """Accept only a token- and source-pair-bound explicit user waiver.

    A waiver bypasses the full Blob archive copy requirement only. The already
    completed archive smoke, source-pair lineage/locks, and retained smoke WAL
    remain required, as do the live account ownership/security checks in the
    caller.
    """
    receipt_path = SD / 'wal-export-declined.json'
    assert receipt_path.is_file() and not receipt_path.is_symlink(), \
        'WAL export decline receipt is missing or unsafe'
    receipt = json.loads(receipt_path.read_text())
    expected_account = 'cnpgwal' + token[:17]
    expected_prefix = f'cnpg-wal-archive/{token}/hdd-pg/'
    pair_path = ROOT / 'source-pair.json'
    proof_path = ROOT / 'source-pair-proof.json'
    archive_path = SD / 'wal-archive.json'
    smoke_path = SD / 'wal-archive-smoke.json'
    assert pair_path.is_file() and proof_path.is_file() and archive_path.is_file() and smoke_path.is_file()
    pair = json.loads(pair_path.read_text())
    proof = json.loads(proof_path.read_text())
    archive = json.loads(archive_path.read_text())
    smoke = json.loads(smoke_path.read_text())
    pair_hash = hashlib.sha256(pair_path.read_bytes()).hexdigest()
    expected_fields = {
        'version': 1,
        'status': 'user-declined-full-export',
        'ownerToken': token,
        'stack': stack_name,
        'subscriptionId': sub,
        'resourceGroup': resource_group,
        'accountId': storage_meta.get('accountId'),
        'accountName': expected_account,
        'containerName': 'wal-' + token[:12],
        'prefix': expected_prefix,
        'sourcePairSha256': pair_hash,
    }
    assert all(receipt.get(key) == value for key, value in expected_fields.items()), \
        'WAL export decline receipt does not match this owned account, stack, and pinned source pair'
    assert receipt.get('userInstructions') == [
        'i dont think we need to download the WAL',
        'please finish cleanup',
    ], 'WAL export decline receipt does not preserve the explicit user instructions verbatim'
    assert receipt.get('localExporterStopped') is True
    assert receipt.get('fullExportComplete') is False
    assert receipt.get('existingVerifiedPayloadsRetained') is True
    assert receipt.get('recoveryMetadataRetained') is True
    assert receipt.get('postgresqlSignals') == 0
    assert isinstance(receipt.get('kubectlChildrenStopped'), list)
    assert pair.get('kind') == 'offline-postgresql-pair'
    assert pair.get('backup', {}).get('phase') == 'completed'
    assert pair.get('backup', {}).get('online') is False
    assert pair.get('backup', {}).get('beginLSN') == pair.get('backup', {}).get('endLSN')
    assert pair.get('backup', {}).get('beginWal') == pair.get('backup', {}).get('endWal')
    proof_assertions = proof.get('assertions', {})
    assert proof_assertions.get('backupCompletedOfflineWithPairedRoles') is True
    assert proof_assertions.get('externalWalArchiveRequired') is False
    assert proof.get('provenance', {}).get('copiedPairSha256') == pair_hash
    expected_ids = {'data': pair['data']['id'], 'wal': pair['wal']['id']}
    expected_account_id = (f'/subscriptions/{sub}/resourceGroups/{resource_group}'
                           f'/providers/Microsoft.Storage/storageAccounts/{expected_account}')
    expected_destination = (f'https://{expected_account}.blob.core.windows.net/'
                            f"wal-{token[:12]}/cnpg-wal-archive/{token}/")
    assert storage_meta.get('accountName') == expected_account
    assert storage_meta.get('containerName') == 'wal-' + token[:12]
    assert storage_meta.get('accountId', '').lower() == expected_account_id.lower()
    assert receipt.get('accountId', '').lower() == expected_account_id.lower()
    assert archive.get('status') == 'ready' and archive.get('ownerToken') == token
    assert archive.get('accountId', '').lower() == expected_account_id.lower()
    assert archive.get('sourceSnapshotIds') == expected_ids
    assert archive.get('destinationPath') == expected_destination
    assert archive.get('snapshotLineage') == smoke.get('snapshotLineage')
    source_locks = archive.get('sourceLockVerification', {})
    assert source_locks.get('data', {}).get('snapshotId', '').lower() == pair['data']['id'].lower()
    assert source_locks.get('wal', {}).get('snapshotId', '').lower() == pair['wal']['id'].lower()
    assert source_locks.get('data', {}).get('level') == 'CanNotDelete'
    assert source_locks.get('wal', {}).get('level') == 'CanNotDelete'
    assert source_locks.get('resourceGroup', {}).get('level') == 'CanNotDelete'
    assert smoke.get('status') == 'complete' and smoke.get('ownerToken') == token
    assert smoke.get('stack') == stack_name and smoke.get('restorePodRole') == 'replica'
    assert smoke.get('checks', {}).get('allThreeHashesMatch') is True
    assert smoke.get('checks', {}).get('sourceSnapshotsReadOnly') is True
    assert smoke.get('checks', {}).get('snapshotLineagePreserved') is True
    wal_name = smoke.get('walName', '')
    assert re.fullmatch(r'[0-9A-F]{24}', wal_name)
    assert smoke.get('archiveObject') == f'{expected_prefix}wals/{wal_name[:16]}/{wal_name}'
    assert smoke.get('snapshotLineage') == archive.get('snapshotLineage')
    retained_rel = smoke.get('retainedLocalWALFile', '')
    assert retained_rel == f'archived-wal/{wal_name}'
    retained_path = SD / retained_rel
    assert not retained_path.is_symlink() and retained_path.is_file()
    digest = hashlib.sha256(retained_path.read_bytes()).hexdigest()
    assert (retained_path.stat().st_size == smoke.get('bytes') == 16 * 1024 * 1024
            and digest == smoke.get('retainedLocalWALSha256')
            == smoke.get('hashes', {}).get('independentAzureDownloadSha256'))
    return {'status': receipt['status'], 'prefix': expected_prefix,
            'sourcePairSha256': pair_hash, 'fullArchiveDownloadRequired': False}


def verify_local_archive_export(token, stack, sub, resource_group, storage_meta):
    """Require a complete, hash-verified local copy before deleting the Blob account."""
    declined_path = SD / 'wal-export-declined.json'
    if declined_path.exists():
        assert not declined_path.is_symlink(), 'WAL export decline receipt is a symlink'
        assert not (SD / 'wal-archive-export.json').exists(), \
            'Both complete-export and user-decline receipts exist; refusing ambiguous disposition'
        verify_declined_wal_archive_export(token, stack, sub, resource_group, storage_meta)
        return
    private_path = SD / 'wal-archive-export.json'
    assert private_path.is_file() and not private_path.is_symlink(), \
        'WAL archive export receipt is missing; refusing Storage-account/resource-group deletion'
    private = json.loads(private_path.read_text())
    expected_account = 'cnpgwal' + token[:17]
    expected_container = 'wal-' + token[:12]
    expected_prefix = f'cnpg-wal-archive/{token}/hdd-pg/'
    expected_destination = f'https://{expected_account}.blob.core.windows.net/{expected_container}/cnpg-wal-archive/{token}/'
    assert private.get('version') == 1 and private.get('status') == 'complete'
    assert private.get('ownerToken') == token and private.get('stack') == stack
    assert private.get('subscriptionId') == sub and private.get('resourceGroup') == resource_group
    assert private.get('accountName') == expected_account and private.get('accountId', '').lower() == storage_meta.get('accountId', '').lower()
    assert private.get('containerName') == expected_container and private.get('serverName') == 'hdd-pg'
    assert private.get('destinationPath') == expected_destination and private.get('prefix') == expected_prefix
    assert storage_meta.get('accountName') == expected_account and storage_meta.get('containerName') == expected_container

    pair_path = ROOT / 'source-pair.json'
    proof_path = ROOT / 'source-pair-proof.json'
    smoke_path = SD / 'wal-archive-smoke.json'
    archive_path = SD / 'wal-archive.json'
    assert pair_path.is_file() and proof_path.is_file() and smoke_path.is_file() and archive_path.is_file()
    pair = json.loads(pair_path.read_text())
    proof = json.loads(proof_path.read_text())
    smoke = json.loads(smoke_path.read_text())
    archive = json.loads(archive_path.read_text())
    pair_hash = hashlib.sha256(pair_path.read_bytes()).hexdigest()
    assert proof.get('provenance', {}).get('copiedPairSha256') == pair_hash
    assert private.get('sourcePairSha256') == pair_hash
    assert private.get('sourceSnapshotIds') == {'data': pair['data']['id'], 'wal': pair['wal']['id']}
    assert private.get('sourceLocksPreserved') is True
    expected_id = (f"/subscriptions/{sub}/resourceGroups/{resource_group}"
                   f"/providers/Microsoft.Storage/storageAccounts/{expected_account}")
    assert storage_meta.get('accountId', '').lower() == expected_id.lower()
    assert archive.get('status') == 'ready' and archive.get('ownerToken') == token
    assert archive.get('accountId', '').lower() == expected_id.lower()
    assert archive.get('sourceSnapshotIds') == {'data': pair['data']['id'], 'wal': pair['wal']['id']}
    assert archive.get('destinationPath') == expected_destination
    assert archive.get('snapshotLineage') == smoke.get('snapshotLineage')
    assert private.get('sourcePairLineage') == archive.get('snapshotLineage')
    source_locks = archive.get('sourceLockVerification', {})
    assert source_locks.get('data', {}).get('snapshotId', '').lower() == pair['data']['id'].lower()
    assert source_locks.get('wal', {}).get('snapshotId', '').lower() == pair['wal']['id'].lower()
    assert source_locks.get('data', {}).get('level') == 'CanNotDelete'
    assert source_locks.get('wal', {}).get('level') == 'CanNotDelete'
    assert source_locks.get('resourceGroup', {}).get('level') == 'CanNotDelete'
    checks = private.get('checks', {})
    assert checks.get('exactTokenServerPrefixOnly') is True
    assert checks.get('sourcePairLineageVerified') is True
    assert checks.get('sourceLocksVerifiedUnmodified') is True
    assert checks.get('allDownloadsEtagPinned') is True
    assert checks.get('everyObjectSizeVerified') is True
    assert checks.get('everyLocalSha256Verified') is True
    assert checks.get('azureListingStableAfterDownload') is True
    assert checks.get('azureMutations') == 0

    assert smoke.get('status') == 'complete' and smoke.get('ownerToken') == token
    assert smoke.get('stack') == stack and smoke.get('restorePodRole') == 'replica'
    assert smoke.get('checks', {}).get('allThreeHashesMatch') is True
    assert smoke.get('checks', {}).get('sourceSnapshotsReadOnly') is True
    assert smoke.get('checks', {}).get('snapshotLineagePreserved') is True
    smoke_wal_name = smoke.get('walName', '')
    assert re.fullmatch(r'[0-9A-F]{24}', smoke_wal_name)
    assert smoke.get('archiveObject') == f'{expected_prefix}wals/{smoke_wal_name[:16]}/{smoke_wal_name}'
    assert smoke.get('retainedLocalWALFile') == f'archived-wal/{smoke_wal_name}'
    restore_smoke = private.get('restoreSmoke', {})
    assert restore_smoke.get('walName') == smoke_wal_name
    assert restore_smoke.get('archiveObject') == smoke.get('archiveObject')
    assert restore_smoke.get('bytes') == smoke.get('bytes')
    assert restore_smoke.get('sha256') == smoke.get('hashes', {}).get('independentAzureDownloadSha256')
    assert restore_smoke.get('restorePodRole') == 'replica'
    retained = PurePosixPath(smoke.get('retainedLocalWALFile', ''))
    retained_raw = smoke.get('retainedLocalWALFile', '').split('/')
    assert not retained.is_absolute() and retained.parts and all(part not in ('', '.', '..') for part in retained_raw)
    retained_path = SD / Path(*retained.parts)
    cursor = ROOT
    for part in ('.state', *retained.parts):
        cursor = cursor / part
        assert not cursor.is_symlink(), 'Retained smoke WAL traverses a symlink; refusing teardown'
    retained_hash = smoke.get('hashes', {}).get('independentAzureDownloadSha256')
    assert retained_path.is_file() and retained_path.stat().st_size == smoke.get('bytes')
    digest = hashlib.sha256()
    with retained_path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    assert digest.hexdigest() == retained_hash == smoke.get('retainedLocalWALSha256')
    objects = private.get('objects')
    assert isinstance(objects, list) and objects
    names = set()
    expected_files = set()
    total = 0
    smoke_found = False
    smoke_object = smoke.get('archiveObject')
    smoke_sha = smoke.get('hashes', {}).get('independentAzureDownloadSha256')
    for item in objects:
        name = item.get('name', '')
        assert name.startswith(expected_prefix)
        relative = name[len(expected_prefix):]
        relative_raw = relative.split('/')
        path = PurePosixPath(relative)
        assert relative.startswith('wals/') and not path.is_absolute()
        assert all(part not in ('', '.', '..') for part in relative_raw) and '\\' not in relative
        assert item.get('relativeName') == relative and item.get('etag') and int(item.get('bytes', 0)) > 0
        assert name not in names
        names.add(name)
        local_relative = item.get('localRelativePath')
        assert local_relative == f'archived-wal/run/{relative}'
        local_raw = local_relative.split('/')
        local_path = PurePosixPath(local_relative)
        assert not local_path.is_absolute() and all(part not in ('', '.', '..') for part in local_raw)
        target = ROOT / '.state' / Path(*local_path.parts)
        cursor = ROOT
        for part in ('.state', *local_path.parts):
            cursor = cursor / part
            assert not cursor.is_symlink(), 'Local WAL archive contains a symlink; refusing teardown'
        assert target.is_file()
        size = target.stat().st_size
        assert size == int(item.get('bytes', -1))
        digest = hashlib.sha256()
        with target.open('rb') as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(block)
        assert digest.hexdigest() == item.get('sha256')
        total += size
        expected_files.add(local_relative.removeprefix('archived-wal/run/'))
        if name == smoke_object:
            assert item.get('sha256') == smoke_sha and size == smoke.get('bytes')
            smoke_found = True
    assert smoke_found and len(objects) == private.get('objectCount')
    assert total == private.get('totalBytes')

    run_dir = SD / 'archived-wal' / 'run'
    assert run_dir.is_dir() and not run_dir.is_symlink()
    owner_marker = SD / 'wal-archive-export-owner.json'
    assert owner_marker.is_file() and not owner_marker.is_symlink()
    owner = json.loads(owner_marker.read_text())
    assert owner.get('ownerToken') == token and owner.get('stack') == stack
    assert owner.get('purpose') == 'cnpg-wal-archive-export'
    actual_files = set()
    for directory, dirs, files in os.walk(run_dir, followlinks=False):
        base = Path(directory)
        for dirname in dirs:
            assert not (base / dirname).is_symlink()
        for filename in files:
            file_path = base / filename
            assert file_path.is_file() and not file_path.is_symlink()
            actual_files.add(file_path.relative_to(run_dir).as_posix())
    assert actual_files == expected_files, 'Local WAL archive has missing or unmanifested files'

    experiment = ROOT.parent
    public_rel = private.get('publicReceiptPath', '')
    public_raw = public_rel.split('/')
    public_posix = PurePosixPath(public_rel)
    assert not public_posix.is_absolute() and all(part not in ('', '.', '..') for part in public_raw)
    public_path = experiment / Path(*public_posix.parts)
    assert public_path.is_file() and not public_path.is_symlink()
    public_bytes = public_path.read_bytes()
    assert hashlib.sha256(public_bytes).hexdigest() == private.get('publicReceiptSha256')
    public_text = public_bytes.decode('utf-8')
    assert token not in public_text and expected_account not in public_text and expected_container not in public_text
    assert storage_meta.get('accountId', '') not in public_text and 'AccountKey=' not in public_text
    public = json.loads(public_bytes)
    assert public.get('status') == 'complete' and public.get('stack') == stack
    assert public.get('serverName') == 'hdd-pg' and public.get('sourcePairProofSha256') == pair_hash
    assert public.get('objectCount') == len(objects) and public.get('totalBytes') == total
    public_objects = public.get('objects', [])
    expected_public = [{key: item.get(source_key) for key, source_key in (
        ('path', 'relativeName'), ('bytes', 'bytes'), ('sha256', 'sha256'), ('etag', 'etag'),
        ('lastModified', 'lastModified'), ('contentMd5Base64', 'contentMd5Base64'))} for item in objects]
    assert public_objects == expected_public
    redactions = public.get('redactions', {})
    assert all(redactions.get(key) is True for key in ('ownerToken', 'accountId', 'containerName', 'connectionString', 'accountKey'))

def verify_local_recovery_metadata(token, stack, sub, resource_group):
    """Require hashes for every private recovery file and the active timeline history."""
    top_path = SD / 'recovery-metadata.json'
    root = SD / 'recovery-metadata'
    private_path = root / 'receipt.json'
    assert top_path.is_file() and not top_path.is_symlink(), \
        'Recovery metadata receipt is missing; refusing teardown'
    assert root.is_dir() and not root.is_symlink() and private_path.is_file() and not private_path.is_symlink()
    top = json.loads(top_path.read_text())
    private_bytes = private_path.read_bytes()
    private = json.loads(private_bytes)
    pair_path = ROOT / 'source-pair.json'
    proof_path = ROOT / 'source-pair-proof.json'
    assert pair_path.is_file() and proof_path.is_file()
    pair = json.loads(pair_path.read_text())
    proof = json.loads(proof_path.read_text())
    pair_hash = hashlib.sha256(pair_path.read_bytes()).hexdigest()
    assert proof.get('provenance', {}).get('copiedPairSha256') == pair_hash
    assert top.get('version') == 1 and top.get('status') == 'complete'
    assert top.get('ownerToken') == token and top.get('stack') == stack
    assert top.get('subscriptionId', private.get('subscriptionId')) == sub and top.get('resourceGroup', private.get('resourceGroup')) == resource_group
    assert private.get('status') == 'complete' and private.get('ownerToken') == token
    assert private.get('stack') == stack and private.get('subscriptionId') == sub
    assert private.get('resourceGroup') == resource_group
    assert top.get('sourcePairProofSha256') == private.get('sourcePairProofSha256') == pair_hash
    assert private.get('sourceSnapshotIds') == {'data': pair['data']['id'], 'wal': pair['wal']['id']}
    assert top.get('privateDirectory') == '.state/recovery-metadata'
    assert top.get('privateReceipt') == '.state/recovery-metadata/receipt.json'
    assert hashlib.sha256(private_bytes).hexdigest() == top.get('privateReceiptSha256')
    timeline = private.get('timeline')
    assert isinstance(timeline, int) and timeline >= 1 and top.get('timeline') == timeline
    assert private.get('historyFileRequired') is (timeline > 1)
    assert top.get('historyFileRequired') is (timeline > 1)
    assert private.get('systemIdentifier') == pair.get('fixture', {}).get('systemIdentifier')
    assert top.get('systemIdentifier') == private.get('systemIdentifier')
    checks = private.get('checks', {})
    assert checks.get('primaryReplicaSystemIdentifiersMatch') is True
    assert checks.get('sourcePairSystemIdentifierMatches') is True
    assert checks.get('primaryNotInRecovery') is True and checks.get('replicaInRecovery') is True
    assert checks.get('clientAndVacuumStopped') is True
    assert checks.get('matchingTimelineHistoryRetained') is (timeline <= 1 or top.get('historyFilePresent') is True)
    assert checks.get('allRawFilesPrivate') is True
    roles = private.get('roles', [])
    assert {role.get('role') for role in roles} == {'primary', 'replica'}
    assert len(roles) == 2
    timeline_hex = f'{timeline:08X}'
    history_name = f'{timeline_hex}.history'
    matching_history = False
    expected_files = {'receipt.json'}
    for role in roles:
        name = role.get('role')
        assert role.get('systemIdentifier') == private.get('systemIdentifier')
        assert role.get('isInRecovery') is (name == 'replica')
        assert role.get('timelineHex') == timeline_hex
        assert role.get('appClientSessions') == 0 and role.get('vacuumClientSessions') == 0
        files = role.get('files', [])
        paths = set()
        for item in files:
            rel = item.get('path', '')
            raw_parts = rel.split('/')
            assert rel and not rel.startswith('/') and all(part not in ('', '.', '..') for part in raw_parts)
            assert '\\' not in rel and '\x00' not in rel and rel not in paths
            paths.add(rel)
            path = root / name / Path(*raw_parts)
            cursor = root
            for part in (name, *raw_parts):
                cursor = cursor / part
                assert not cursor.is_symlink(), 'Recovery metadata contains a symlink; refusing teardown'
            assert path.is_file() and path.stat().st_size == item.get('bytes')
            digest = hashlib.sha256()
            with path.open('rb') as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(block)
            assert digest.hexdigest() == item.get('sha256')
            expected_files.add(f'{name}/{rel}')
            if rel == f'history/{history_name}':
                matching_history = True
        assert 'pg_controldata.txt' in paths
        assert 'metadata.json' in paths
    if timeline > 1:
        assert matching_history, 'Current timeline history file is missing; refusing teardown'
        assert top.get('historyFilePresent') is True

    actual_files = set()
    for directory, dirs, files in os.walk(root, followlinks=False):
        base = Path(directory)
        for dirname in dirs:
            assert not (base / dirname).is_symlink()
        for filename in files:
            path = base / filename
            assert path.is_file() and not path.is_symlink()
            actual_files.add(path.relative_to(root).as_posix())
    assert actual_files == expected_files, 'Recovery metadata directory contains missing or unmanifested files'

    public_rel = top.get('publicReceiptPath', '')
    raw_parts = public_rel.split('/')
    assert public_rel and not public_rel.startswith('/') and all(part not in ('', '.', '..') for part in raw_parts)
    public_path = ROOT.parent / Path(*raw_parts)
    assert public_path.is_file() and not public_path.is_symlink()
    public_bytes = public_path.read_bytes()
    assert hashlib.sha256(public_bytes).hexdigest() == top.get('publicReceiptSha256')
    public_text = public_bytes.decode('utf-8')
    assert token not in public_text and pair['data']['id'] not in public_text and pair['wal']['id'] not in public_text
    public = json.loads(public_bytes)
    assert public.get('status') == 'complete' and public.get('stack') == stack
    assert public.get('timeline') == timeline and public.get('systemIdentifier') == private.get('systemIdentifier')
    assert public.get('sourcePairProofSha256') == pair_hash
    assert all(public.get('redactions', {}).get(key) is True for key in
               ('ownerToken', 'subscriptionId', 'resourceGroup', 'sourceSnapshotIds', 'podUids', 'privatePaths'))

def teardown(stack):
    s = json.loads((SD / 'state.json').read_text())
    sub, token = s['subscriptionId'], s['ownerToken']
    assert re.fullmatch('[a-f0-9]{32}', token)
    expected = ['rg-vacuum-only-' + token, 'rg-vacuum-nodes-' + token]
    assert s['resourceGroup'] == expected[0]
    assert s['requestedNodeResourceGroup'] == expected[1]
    assert s.get('nodeResourceGroup', '') in ('', expected[1])
    locks = []
    for name in ('wal-archive-export.lock', 'wal-archive.lock', 'test.lock',
                 'teardown.lock', '02-prepare.lock', 'provision.lock'):
        f = stack.enter_context((SD / name).open('a')); fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB); locks.append(f)

    def az(*args, timeout=300):
        p = subprocess.run(['az', *args, '--subscription', sub, '-o', 'json'], capture_output=True, text=True, timeout=timeout)
        if p.returncode: raise RuntimeError(p.stderr[-3000:])
        return json.loads(p.stdout) if p.stdout.strip() else None

    assert az('account', 'show')['id'].lower() == sub.lower()

    def verify_archive_account_scope():
        """Refuse RG cleanup if the archive account lacks this stack's local ownership receipt."""
        storage_path = SD / 'storage.json'
        expected_name = 'cnpgwal' + token[:17]
        account_rows = az('storage', 'account', 'list', '-g', expected[0])
        # The CLI call is already scoped to Storage accounts, so inspect every
        # returned account rather than depending on optional `type` fields.
        storage_rows = account_rows
        if not storage_path.exists():
            assert not storage_rows, 'Storage account exists without a WAL archive ownership receipt; refusing RG deletion'
            return

        meta = json.loads(storage_path.read_text())
        assert meta.get('version') == 1 and meta.get('ownerToken') == token
        assert meta.get('subscriptionId') == sub and meta.get('resourceGroup') == expected[0]
        assert meta.get('accountName') == expected_name
        assert meta.get('containerName') == 'wal-' + token[:12]
        assert meta.get('purpose') == 'private-cnpg-wal-archive'
        assert meta.get('publicAccess') is None and meta.get('status') in ('creating', 'ready')
        assert len(storage_rows) <= 1, 'Unexpected extra Storage account exists in the owned stack RG'
        assert all(x.get('name', '').casefold() == expected_name.casefold() for x in storage_rows), \
            'An unrelated Storage account exists in the owned stack RG'
        live = next((x for x in storage_rows if x.get('name', '').casefold() == expected_name.casefold()), None)
        if live is None:
            assert meta.get('status') == 'creating', 'Ready archive account is missing from its owned RG'
            return
        expected_id = (f"/subscriptions/{sub}/resourceGroups/{expected[0]}"
                       f"/providers/Microsoft.Storage/storageAccounts/{expected_name}")
        assert live.get('id', '').lower() == expected_id.lower()
        assert meta.get('accountId') in (None, expected_id, live.get('id'))
        tags = live.get('tags', {})
        assert tags.get('reprotoken') == token
        assert tags.get('reproPurpose') == 'private-cnpg-wal-archive'
        assert tags.get('reproStack') == 'walreceiver-sigterm'
        assert live.get('kind') == 'StorageV2'
        assert live.get('sku', {}).get('name') == 'Standard_LRS'
        assert live.get('allowBlobPublicAccess') is False
        assert live.get('enableHttpsTrafficOnly') is True
        assert live.get('minimumTlsVersion') == 'TLS1_2'
        if meta.get('status') == 'ready':
            assert live.get('provisioningState') == 'Succeeded'
            assert meta.get('accountId', '').lower() == live['id'].lower()
        if live is not None or meta.get('status') == 'ready':
            verify_local_archive_export(token, ROOT.name, sub, expected[0], meta)

    verify_local_recovery_metadata(token, ROOT.name, sub, expected[0])
    verify_archive_account_scope()
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
