# Numeric-hydration recovery finalizer

This is a staged read-only finalizer for the existing 50 GiB and 100 GiB runs.
It addresses the frozen `02-prepare.sh` gate comparing jq's string `100.0` to
`100`. It does not edit either preparation script, `.state`, PostgreSQL, Azure
resources, or Kubernetes objects. A passing run writes a normal-schema
`prepare.json` plus an audit receipt under
`recovery/numeric-hydration-finalizer/results/<stack>/`, both mode `0600` in a
mode `0700` directory. Root can review and integrate those receipts; this tool
never copies them into `.state`.

Run only after the original supervisor has stopped and its final-audit temporary
directory has been privately preserved:

```sh
python3 recovery/numeric-hydration-finalizer/finalize_hydration.py \
  --stack 50gib \
  --control-flow-dir recovery/fixture-watch/50-finalization/active-prepare-tmp.a7vQvw \
  --original-process-receipt recovery/numeric-hydration-finalizer/original-50gib/prepare-process-stopped.json \
  --original-prepare-script recovery/numeric-hydration-finalizer/original-50gib/02-prepare.sh \
  --seed-job-uid bc0de226-e948-4186-90c8-573c8b0be2ed \
  --seed-event-evidence recovery/fixture-watch/50-finalization/live/events.json \
  --seed-event-sha256 4e96d8a122c0f8adc2308d76f45e457d4ad8fd2681d78accd81c5f8957c0aa4f
```

For 100 GiB, pass `--stack 100gib`, that run's preserved final-audit temporary
directory, stopped process receipt, frozen original script, and captured seed
Job UID. The tool loads the stack's saved kubeconfig and uses
read-only `az`, `kubectl get`, `kubectl exec` SQL/inspection commands. It never
reads the archive Secret's contents and never requests storage credentials.
The archived process receipt must say `failed` or `stopped`, and its original
child PID must no longer exist; the finalizer does not inspect or reject a new
wrapper process. It verifies the frozen 02 script SHA and unchanged `03-test.py`
SHA before using any captured seed-event fallback. For a GC'd seed Job, its
exact Job UID and Completed Event chronology must match both the live replica
PVC/pod records and their finalization capture. If Kubernetes has expired that
Event, the archived Event list must be supplied with an independently pinned
SHA-256; the receipt binds it to the stopped-process receipt, captured
finalization hashes, and unchanged replica PVC/pod UIDs and creation times.

The finalizer checks the subscription, owner-tagged resource groups, AKS and
Blob-account identity; exact pinned source manifest, stage receipt, helper and
prepare-script hashes; source snapshot UID and restore-copy source staging-disk
ID/UID; source VolumeSnapshot/Content; completed paired Backup; backup
VolumeSnapshot/Content/PV/ARM lineage; completed replica seed Job or its
GC-safe retained completion Event; four exact DATA/WAL PVC and PremiumV2 disk
identities; image, synchronous settings, GUCs, fixture shape, independent WAL
mounts, and replica catch-up. Copy/CopyStart disk completion is parsed as a
number, so `100` and `100.0` both pass. Logical sector size may fall back
to the validated PV CSI `LogicalSectorSize=512` attribute when ARM omits it.

Live tuple-statistics checks remain estimates. The original script captures
INSERT/DELETE output through command substitution and then removes the
remote output. The 50-GiB raw DML and 100-GiB INSERT output are unavailable.
A later read-only watch preserved the original 100-GiB `CREATE INDEX`,
`DELETE 32768000`, `CHECKPOINT` output and exit status 0 before deletion;
see the private [capture](../fixture-watch/100-dml-output/). The frozen
auditor predates that additional evidence, which must be identified in the
installed 100-GiB audit without changing the frozen auditor. The finalizer
binds the unchanged frozen script and captured final-audit temporary files
(evidence that execution passed the original fixture SQL equality guards) to
fresh indexed min/max ID probes, absence of the temporary stage table, target
heap size, and live `pg_stat_all_tables.n_live_tup`/`n_dead_tup`. It requires
the live estimates to equal `ceil(targetBytes * pinnedSourceLiveEstimate /
pinnedSourceHeapBytes)`: 16,384,000 for 50 GiB and 32,768,000 for 100 GiB. It
records these as estimates and does not infer a source ID stride or run
`COUNT(*)`, `ANALYZE`, or `VACUUM`.

The Standard-origin restore-copy snapshot is verified as Available/Succeeded
and tied to the exact saved staging-disk UID. Its reported copy progress is
recorded as returned; this finalizer does not interpret Standard-origin
progress as proof of full hydration. The four measured PremiumV2 disks must be
Succeeded and Copy/CopyStart at numeric 100 (or Empty with null/100). Paired
backup snapshots made from those measured PremiumV2 disks must also report
Copy or CopyStart at numeric 100. Only a fully identity-validated Copy or
CopyStart resource in Succeeded state with a numeric percentage below 100
returns retryable exit status 3 (`HYDRATION PENDING`). Missing, malformed, or
out-of-range progress, unsupported create options, and all other readiness,
identity, ownership, or configuration failures return exit status 2 and are
not retryable as hydration. An Empty disk allows a null or 100 completion
value. The audit makes no global-cloud-quiescence claim.

Focused offline tests:

```sh
python3 -m unittest discover -s recovery/numeric-hydration-finalizer -v
```
