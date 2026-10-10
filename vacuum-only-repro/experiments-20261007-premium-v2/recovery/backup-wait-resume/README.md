# Draft: resume an existing 100 GiB CNPG Backup after a wait timeout

Current execution record (2026-10-07): the original preparation timed out at
07:11:40 UTC while DATA snapshot readiness was still reconciling; its paired
Backup later completed without recreation. The first supervised continuation
stopped before replica setup because generated CNPG VolumeSnapshots do not
inherit `repro.stack.token`. Its code and failed attempt remain preserved.
The reviewed v2 resumer accepts an absent custom token while rejecting a
conflicting one and retaining namespace, owned Backup/class, CNPG labels,
PVC/VSC/ARM source and UID checks. `resume_after_backup_v2.py`,
`finalize_continuation_v2.py`, and `continue-100-02-v2.sh` are the deployed
versioned path. Seven tests against captured live metadata passed in root
and independent review. A fresh supervisor started v2 at approximately
07:28 UTC; it reached the same Backup/Copy100 gate and started replica seeding
at 07:29:59 UTC. Its outputs are in `results/100gib-v2` and
`final-audit/100gib-v2`; all older outputs remain separate. Completion is
still subject to live hydration, configuration and replay checks.


This is an unexecuted recovery draft for the case where frozen
`100gib/02-prepare.sh` terminates after its 3,600-second Backup wait while the
existing owner-bound online Backup and paired snapshot copies continue. It
never repeats fixture SQL/DML or creates another Backup. The continuation and
adapter do not write `.state`; a root-owned wrapper may install the exact
external journal and canonical prepare receipt only after review. No cloud
cleanup or global-quiescence claim is part of this path.

The original script SHA is
`7b24d869c7ef362653f38201402f5e4f297c874f66253658f884ced15055b20f`. Before
its EXIT trap removes `.state/.prepare.*`, the runner must preserve the full
TMP directory, a process receipt captured while it is running, and exact
original script/helper bytes. At the actual Backup-request boundary, the
reviewed local capture helper is invoked as follows:

```sh
python3 recovery/backup-wait-resume/capture_prebackup_tmp.py \
  --source-tmp /workspace/vacuum-only-repro/experiments-20261007-premium-v2/100gib/.state/.prepare.<suffix> \
  --output-dir recovery/backup-wait-resume/original-100gib
```

The helper requires the exact active `run-phase.py <absolute-100gib-path>
prepare` argv, verifies the frozen script/helper hashes before and after
copying, and records full source/destination manifests. After the old process
has failed or been stopped, preserve its terminal receipt separately and bind
it to the earlier running snapshot:

```sh
python3 recovery/backup-wait-resume/bind_terminal_process.py \
  --capture-receipt recovery/backup-wait-resume/original-100gib/prebackup-capture.json \
  --terminal-process-receipt recovery/backup-wait-resume/original-100gib/original-process-terminal.json \
  --output recovery/backup-wait-resume/original-100gib/terminal-process-binding.json
```

The running/terminal receipt chain preserves the same run identity across the
status transition. It is corroborating local evidence, not a signed execution
attestation. If the TMP was not captured before the EXIT trap, this path must
not be used. The original INSERT output was already removed and is not
reconstructed. A later read-only watch captured the original 100-GiB
`CREATE INDEX`, `DELETE 32768000`, `CHECKPOINT` output and exit status 0 at
05:15:07 UTC on 2026-10-07, before the original script removed that remote
directory. The private [DML capture](../fixture-watch/100-dml-output/)
binds those bytes to the original query, process/script/helper, path and pod
UID. It is separate from the required Backup-boundary capture. The frozen
resumer and adapter predate this capture and contain generic raw-output
absence limitations; any installed audit must clarify the additional
captured DELETE evidence while retaining their original external receipts.
The original attempt reaching
the captured `backup.yaml` boundary is corroborated by its frozen script and
process receipts; the exact existing owner-bound Backup is separately checked
against that request. This does not prove that a particular PID issued the
Kubernetes apply call, and it says nothing about post-Backup seed/image gates.

The resumer requires all archived inputs under this recovery directory and
performs a one-shot read-only owner/source/Cluster/fixture/archiver/Backup
preflight by default. That mode does not wait or add locks. Explicit
`--continue-original-tail` waits up to 24 hours for the exact existing Backup
UID and paired DATA/WAL Copy100 outputs. It first verifies the snapshot
resource group's inherited `CanNotDelete` lock, then adds/verifies a direct
lock as soon as each exact owner/source-bound snapshot ARM ID appears. It
pins each snapshot/VSS/content UID, ARM ID, source PVC/PV/disk ID and UID, and
create option as soon as observed; later copy polls and the pre-mutation
refresh must match those independent pins. It rechecks owner, source lineage,
fixture/archiver, source disks, Backup UID/pair, and snapshot identities before
the first Cluster mutation. Only
`Succeeded`/`Available` with numeric `completionPercent=100` is complete.

After Copy100, the explicit continuation performs the remaining replica seed,
archive removal, measurement-image switch, and catch-up gates. It preserves the
exact seed Job UID/Event, PVC/pod chronology, final controls, and snapshot
journal outside `.state`. Those actions and final captures are attributed to
the pinned continuation, not to original 02. Partial continuation evidence is
preserved for review and must not be automatically restarted.

Example shape for a future reviewed call (all paths and the UID must come from
the preserved receipts; do not execute this example as-is):

```sh
python3 recovery/backup-wait-resume/resume_after_backup.py \
  --original-process-receipt recovery/backup-wait-resume/original-100gib/original-process-terminal.json \
  --running-process-receipt recovery/backup-wait-resume/original-100gib/original-process-running.json \
  --original-script recovery/backup-wait-resume/original-100gib/original-02-prepare.sh \
  --prebackup-tmp recovery/backup-wait-resume/original-100gib/prebackup-tmp \
  --prebackup-capture-receipt recovery/backup-wait-resume/original-100gib/prebackup-capture.json \
  --terminal-process-binding recovery/backup-wait-resume/original-100gib/terminal-process-binding.json \
  --backup-uid '<exact existing Backup UID>' \
  --output-dir recovery/backup-wait-resume/results/100gib \
  --continue-original-tail
```

After root installs the exact external paired-snapshot journal for the frozen
read-only audit, a separately pinned adapter verifies the local receipt chain,
runs the unchanged hydration finalizer, and cross-checks its live Backup,
snapshot, seed, and capture identities against continuation evidence. It
stores original pre-Backup corroboration separately from continuation-created
final captures and corrects the audit attribution. Only a validated
sub-100%-hydration state is retryable; other provenance or identity failures
stop the finalization.

```sh
python3 recovery/backup-wait-resume/finalize_continuation.py \
  --continuation-evidence recovery/backup-wait-resume/results/100gib/continuation-evidence.json \
  --output-dir recovery/backup-wait-resume/final-audit/100gib
```

Focused unit tests use synthetic local evidence only. They do not call Azure
or Kubernetes and do not launch the resumer or adapter.

The original unbound wrapper remains in `reviewed-drafts/`. For any continuation,
root must first preserve the original terminal receipt, script and logs,
bind the terminal receipt to the running capture, and review a read-only
preflight for the exact existing Backup. Only then may root bind that UID in
the wrapper, review its final bytes, install it as `100gib/02-prepare.sh`,
and archive the old canonical process receipt in a separate attempt folder.
Run the wrapper through a **fresh** `run-phase.py` prepare invocation with
the absolute stack path. Its startup guard verifies the new supervisor,
child PID, command, script hash and helper hash. The supervisor writes the
new complete/exit-0 receipt only after the wrapper succeeds; the original
failure remains separate and is never relabeled as successful.

The wrapper continues the original tail once, installs the exact externally
captured snapshot journal with exclusive file creation, invokes the frozen
adapter, and installs the canonical audit/prepare metadata only after full
validation. Only adapter exit 3 (validated remaining measured-disk hydration)
is retryable. Partial continuation or finalization evidence requires review
and prevents an automatic restart. Restart the coordinator only after the
fresh supervised preparation is running and the measurement gate is absent.
