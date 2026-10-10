# Hydration barrier review

Reviewed `hydration-barrier.py`, its coordinator call before `measurement-gate.json`,
the saved baseline watcher captures, and the per-stack known failed-copy pins. No
Azure requests or mutations were made during this review; tests use mocked ARM
responses.

## Baseline evidence

The latest saved normalized ARM capture at review time was
`baseline-23gib/.state/recovery/baseline-watch/copy-progress-latest.json`, captured
at `2026-10-06T21:01:28Z`. It reports the replica DATA disk
`pvc-1b8dc8b3-93f5-40a2-8f17-d99854e6a82a` at 12%, and its WAL disk at 100%;
both report `provisioningState: Succeeded` and `copyCompletionError: null`. The
coordinator's last saved state I read was `waiting-for-preparation` at
`2026-10-06T20:57:39Z`. The 12% replica DATA copy must keep measurement gated.

These watcher files are normalized captures rather than full raw disk objects, so
they do not establish the live disk's logical-sector property shape. The existing
`verification/test_lun_metadata.py` does cover the measurement script's Azure CLI
case: `diskSizeGB` null with `diskSizeGb` populated, and `logicalSectorSize` null
with `creationData.logicalSectorSize` populated.

## Staged barrier changes

`recovery/audit/hydration-barrier.py` now checks all four PremiumV2 disks per stack
for Succeeded state, 512 GiB, 3,000 IOPS, 125 MB/s, zone, 512-byte sector, and
completion rules (`Copy`/`CopyStart` at 100%; `Empty` at null or 100%). It handles
null sector and size aliases. A tagged source stage must be Standard_LRS with
`Copy`/Succeeded and completion null or 100%. Every disk in the owned node group
is inspected for Copy/CopyStart progress, including non-Premium disks; ordinary
FromImage disks with no completion percent pass, while a nonstage Standard Copy
with an omitted percent remains pending.

The snapshot loop validates snapshot IDs stay inside the owned snapshot group,
requires matching owner tags, accepts only known `Copy` or `CopyStart` success
states, requires `Available`, and blocks on any non-null `copyCompletionError`.
The one known 100-GiB failed source copy is accepted only when live ID and unique
ID match the local pin, its owner and source tags match the stack owner and
manifest, and it is `CopyStart`, `Failed`, `Unknown`, and 0%. The pin inventory
must exist and be internally consistent; a missing or changed pinned snapshot
fails closed. Its actual ARM create option is included in barrier evidence. The
failed snapshot remains preserved and does not hold the measurement gate because
its copy is terminal.

## Verification

The existing plus new local suite passed 45 tests, exit 0. Exact command:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/workspace/vacuum-only-repro/experiments-20261007-premium-v2/baseline-23gib python3 -m unittest discover -s verification -v
```

The run started at `2026-10-06 21:02:14 UTC` and reported 45 tests in 3.766s.
After adding the final unknown-snapshot-option case, the focused hydration suite
also passed all 11 tests with:

```text
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest verification.test_hydration_barrier -v
```

No broad suite was rerun after that last one-line fail-closed predicate change.
The focused tests cover a fully complete three-stack fixture, partial Premium
disk and snapshot copies, null/alias fallbacks, an active non-Premium copy, the
pinned terminal failure, changed owner/source rejection, copy errors, unknown
snapshot create options, and staging-SKU enforcement.

SHA256 of staged `hydration-barrier.py`:
`05c9b50609e8ee093e2f66ab40ba314e082a52f83f31e1aaa285d45707af1a57`.

After root copied the staged code to `ROOT/hydration-barrier.py`, the test module
was pointed at that production path and all 11 focused tests passed again with
`PYTHONDONTWRITEBYTECODE=1 python3 -m unittest verification.test_hydration_barrier -v`.

SHA256 of production `hydration-barrier.py` and its staged review copy:
`05c9b50609e8ee093e2f66ab40ba314e082a52f83f31e1aaa285d45707af1a57`.

SHA256 of `verification/test_hydration_barrier.py` after the production import:
`5f59162f609561ea2b441e3d69ad29ca9268fbc1a12fc40115257feba6a86ff0`.

The earlier 35-test run recorded in `AUDIT.md` had no captured process timestamp;
the 45-test follow-up above has a timestamped record.
