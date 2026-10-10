# Local WAL cleanup guard

This guard makes no Azure or Kubernetes calls. It verifies that the exact
begin-to-end WAL archive required by each paired online VolumeSnapshot Backup
is present locally before cleanup can remove the stacks' Blob accounts.

Root can integrate `verify()` from `verify_wal_retention.py` immediately after
`post-measurement-cleanup.py` creates `snapshot-audit-before-compute-cleanup.json`
and before it records `removing-owned-compute` or starts any teardown. The
guard expects that current protected-snapshot inventory at
`preflight/snapshot-audit-before-compute-cleanup.json`.

It binds each export to `.state/prepare.json` and `.state/storage.json`, checks
the completed process receipt, and requires the exported DATA/WAL Azure
snapshot IDs and `uniqueId` values to remain present and protected in the
current retention audit. The 50-GiB and 100-GiB receipts must also match their
capture receipt's exact prepare/process hashes and snapshot lineage. The
baseline is bound to its completed finalization and saved Backup evidence.
For each stack, the guard binds export begin/end LSN, begin/end WAL, and
timeline to the frozen completed CNPG Backup status and its backup label. For
50/100-GiB, the captured Backup document must match the capture receipt's
recorded byte length and SHA-256.

For every archive it recomputes the PostgreSQL WAL segment sequence, checks the
exact object set and archive prefix, verifies each file's recorded byte length
and SHA-256, fully decompresses each WAL segment to the expected 16-MiB size,
and checks backup-history and `backup_label.txt` LSN/WAL/timeline/name fields.
It follows the exporter's actual compression metadata values (`gz`, `bz2`, and
`xz`) and archive layout, including timeline-history objects stored directly
under the archive prefix. Downloaded objects are flat local files under each
backup export directory.
The parser/decompression helpers are loaded only if the WAL exporter source
still matches the reviewed SHA-256
`915b5972c845597865b51d10e97604ecc9e21dfcd662be5c998f936fe77f0a85`.
Only regular files under the fixed stack export paths are accepted; symlinks,
traversal, missing files, stale retention audits, mismatched identities, and
unsupported compression fail closed. The check prints a small summary and
never writes credentials or evidence.

Run the focused local tests with:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v recovery/wal-cleanup-guard/test_verify_wal_retention.py
```

The check is expected to fail until all three exports, including the pending
50-GiB and 100-GiB exports, are complete and the current pre-cleanup retention
audit has been produced.
