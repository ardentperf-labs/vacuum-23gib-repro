# Measurement and cleanup audit

This review covered the three stack copies of `03-test.py` and `04-teardown.sh`,
plus `coordinate-measurements.py` and `post-measurement-cleanup.py`. The three
stack copies of each script were byte-identical before the staged edits.

## Findings and staged fixes

- `03-test.py` checked Azure disk size only through `diskSizeGB`, although the
  preparation gate already accepts `diskSizeGb` too. It also used
  `dict.get("logicalSectorSize", fallback)`, which does not use the fallback
  when the first key is present with JSON `null`. Both conditions could reject
  correctly prepared PremiumV2 disks during measurement preflight. The staged
  [03-test.py](03-test.py) handles both size spellings and null sector values.
- `04-teardown.sh` required every retained snapshot to be Succeeded, 512 GiB,
  Available, and Standard_LRS. This blocked cleanup for the known terminal
  failed keeper copy whose size is absent, and for valid CNPG backup snapshots
  using Standard_ZRS. The staged [04-teardown.sh](04-teardown.sh) accepts
  successful keeper copies only as Standard_LRS, successful backup snapshots
  as Standard_LRS or Standard_ZRS, and records an owner/source-verified terminal
  failed copy as preserved but unverified with nullable size. It applies or
  verifies a direct CanNotDelete lock for every accepted snapshot.
- The staged teardown rechecks Copy/CopyStart states before AKS deletion and
  again before node/test resource-group deletion. A nonterminal or unknown
  copy that may reference an owned managed disk blocks compute cleanup. The
  existing snapshot inventory guards still prevent indirect deletion through
  node/test resource-group removal.

## Gates reviewed

`03-test.py` checks the effective values of the four CNPG defaults named in the
experiment instructions (`wal_level=logical`, `full_page_writes=on`,
`recovery_prefetch=try`, `vacuum_cost_delay=0`). These values match the active
experiment plan and the prior prepared-source manifest. The same preflight and
every sample reject app-user client sessions; the test creates no client load.
VACUUM completion requires its start and completion markers and exit code 0.
Replay drain requires three consecutive samples with the replica at or beyond
the completion and current primary flush LSN. The aggregator additionally
requires valid coverage for VACUUM, replay drain, and the combined observation
window before cleanup can run.

`coordinate-measurements.py` fails immediately if any preparation receipt still
says `failed`; after root repairs preparation state, it must be relaunched with
all three process receipts complete and metadata consistent. The existing
`cleanup-gate.json` hashes the original teardown script, so root must refresh
`teardownScriptSha256` after integrating the staged script into all three
stacks. `post-measurement-cleanup.py` checks measurement completion, verifies
each teardown-script hash, audits all subscription snapshots before and after
compute cleanup, then removes shared watchers only after all experiment and
node resource groups are gone.

## Validation

The integrated measurement and teardown scripts passed 35 verification tests:
`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=baseline-23gib python3 -m unittest discover -s verification -v`.
The added cases check alternate Azure disk metadata spellings, LRS/ZRS backups,
failed owner/source-bound snapshot preservation, incomplete copy blocking, and
wrong-source/owner rejection before deletion. Bash syntax checks passed for
all four teardown copies. No Azure mutations were performed by this audit.

Integrated `03-test.py` SHA256: `1b6b25fdd0c49f1ec9c1b116f28c72ddb5d63202c858ffa34ec12815756e004a`.
Integrated `04-teardown.sh` SHA256: `c49fc87cf125b3dc85f2219c39fbc0d0838903bb9f03d96ffbec66f9376f02ca`.
Root refreshed the cleanup gate hash after review; authorization remains false
until the completed measurements and retention inventory are reviewed.
