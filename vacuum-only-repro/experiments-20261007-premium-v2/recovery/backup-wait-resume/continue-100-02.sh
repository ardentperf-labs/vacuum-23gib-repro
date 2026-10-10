#!/usr/bin/env bash
# Unexecuted integration draft. Root must bind the actual existing Backup UID.
set -Eeuo pipefail
umask 077
HERE="$(cd -- "$(dirname -- "$0")" && pwd)"
if [[ -f "$HERE/run-config.json" ]]; then
  STACK="$HERE"; ROOT="$(cd -- "$HERE/.." && pwd)"
else
  ROOT="$(cd -- "$HERE/../.." && pwd)"; STACK="$ROOT/100gib"
fi
SD="$STACK/.state"
RECOVERY="$ROOT/recovery/backup-wait-resume"
ORIGINAL="$RECOVERY/original-100gib"
TAIL_OUTPUT="$RECOVERY/results/100gib"
FINAL_OUTPUT="$RECOVERY/final-audit/100gib"
BACKUP_UID=597e0f7d-c87e-4455-b01a-42120f969859
RESUMER="$RECOVERY/resume_after_backup.py"
RESUMER_SHA=9278b938a3adb8cb5f18c4d2a65f8aff2792f5c4029e77902d3af6b170de56a2
ADAPTER="$RECOVERY/finalize_continuation.py"
ADAPTER_SHA=873da9c7c32ef6980d4bf2856109fbd7ef53706d624fff634bffd735615af280
FINALIZER="$ROOT/recovery/numeric-hydration-finalizer/finalize_hydration.py"
FINALIZER_SHA=78fc66344fd105ef96ac4299a9ddbf52ec5f74cff081b2b74898b099b0c6d2e1
STAGE_HELPER="$ROOT/recovery/source-restore/stage-allowlisted-source.sh"
HELPER_SHA=0eacb0a81b30703a3b788aa745d3eca8c079e3e4e0cfca9921f500b6f5c8918b
DML_PROOF="$ROOT/recovery/fixture-watch/100-dml-output/verify_delete_capture.py"
DML_PROOF_SHA=271343456b365e88fef64fe0990420107fe76dd052f05b9eec3a84d831756525
die(){ echo "ERROR: $*" >&2; exit 1; }
verify_sources(){
  [[ "$(sha256sum "$RESUMER" | awk '{print $1}')" == "$RESUMER_SHA" ]] || die 'Reviewed resumer changed.'
  [[ "$(sha256sum "$ADAPTER" | awk '{print $1}')" == "$ADAPTER_SHA" ]] || die 'Reviewed adapter changed.'
  [[ "$(sha256sum "$FINALIZER" | awk '{print $1}')" == "$FINALIZER_SHA" ]] || die 'Frozen finalizer changed.'
  [[ "$(sha256sum "$STAGE_HELPER" | awk '{print $1}')" == "$HELPER_SHA" ]] || die 'Source helper changed.'
  [[ "$(sha256sum "$DML_PROOF" | awk '{print $1}')" == "$DML_PROOF_SHA" ]] || die 'Reviewed original DELETE proof helper changed.'
}
[[ $BACKUP_UID =~ ^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$ ]] || die 'Draft has no verified existing Backup UID; execution is disabled.'
[[ ${TEST_HEAP_GIB:-100} == 100 && "$(basename "$STACK")" == 100gib ]] || die 'Wrong fixture assigned.'
[[ "$(jq -er .targetHeapGiB "$STACK/run-config.json")" == 100 ]] || die 'Run target changed.'
[[ ! -e "$ROOT/measurement-gate.json" && ! -e "$SD/prepare.json" ]] || die 'Measurement released or preparation already complete.'
[[ ! -e "$SD/output-snapshots.jsonl" && ! -e "$TAIL_OUTPUT" && ! -e "$FINAL_OUTPUT" ]] || die 'Previous continuation evidence exists; review it instead of restarting.'
exec 9>"$SD/backup-continuation.lock"
flock -n 9 || die 'Another continuation is active.'
verify_sources
# run-phase.py owns the new process receipt; the original failure stays archived.
python3 - "$STACK" "$$" "$HELPER_SHA" <<'PY'
import hashlib,json,os,sys,time
from pathlib import Path
stack=Path(sys.argv[1]).resolve();child=int(sys.argv[2]);helper_sha=sys.argv[3]
path=stack/'.state/prepare-process.json'
deadline=time.monotonic()+2
while True:
    process=json.loads(path.read_text())
    if process.get('status')!='starting' or time.monotonic()>=deadline: break
    time.sleep(.05)
assert process['stack']=='100gib' and process['phase']=='prepare' and process['status']=='running'
assert process['childPid']==child==os.getppid()
assert process['command']==['bash',str(stack/'02-prepare.sh')]
assert process['scriptSha256']['02-prepare.sh']==hashlib.sha256((stack/'02-prepare.sh').read_bytes()).hexdigest()
assert process['sourceStageHelperSha256']==helper_sha
supervisor=int(process['supervisorPid'])
status=(Path('/proc')/str(child)/'status').read_text().splitlines()
assert int(next(line.split()[1] for line in status if line.startswith('PPid:')))==supervisor
argv=(Path('/proc')/str(supervisor)/'cmdline').read_bytes().rstrip(b'\0').split(b'\0')
assert len(argv)==4 and Path(os.fsdecode(argv[0])).name in ('python','python3')
assert [os.fsdecode(v) for v in argv[1:]]==[str(stack.parent/'run-phase.py'),str(stack),'prepare']
print('Verified fresh supervised recovery attempt; original failure remains archived.')
PY
python3 -B "$RESUMER" \
  --original-process-receipt "$ORIGINAL/original-process-terminal.json" \
  --running-process-receipt "$ORIGINAL/original-process-running.json" \
  --original-script "$ORIGINAL/original-02-prepare.sh" \
  --prebackup-tmp "$ORIGINAL/prebackup-tmp" \
  --prebackup-capture-receipt "$ORIGINAL/prebackup-capture.json" \
  --terminal-process-binding "$ORIGINAL/terminal-process-binding.json" \
  --backup-uid "$BACKUP_UID" --output-dir "$TAIL_OUTPUT" --continue-original-tail
verify_sources
# Install only the exact, completed continuation's paired-snapshot journal.
python3 - "$ROOT" "$TAIL_OUTPUT" "$BACKUP_UID" "$RESUMER_SHA" "$FINALIZER_SHA" <<'PY'
import hashlib,importlib.util,json,os,sys
from pathlib import Path
root,output=map(Path,sys.argv[1:3]); backup_uid,resumer_sha,finalizer_sha=sys.argv[3:]
def digest(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def require(ok,message):
    if not ok: raise RuntimeError(message)
evidence=json.loads((output/'continuation-evidence.json').read_text())
progress=json.loads((output/'resume-progress.json').read_text())
require(evidence['phase']==progress['phase']=='tail-complete-awaiting-frozen-finalizer','Tail is incomplete.')
require(evidence['backup']['uid']==progress['backup']['uid']==backup_uid,'Backup UID differs from root pin.')
require(evidence['resumerScriptSha256']==progress['resumerScriptSha256']==resumer_sha,'Continuation source differs.')
require(evidence['pairedSnapshots']==progress['pairedSnapshots'] and evidence['pinnedSnapshotIdentity']==progress['pinnedSnapshotIdentity'],'Snapshot evidence differs.')
relative=Path(evidence['snapshotJournalPath'])
require(not relative.is_absolute() and '..' not in relative.parts,'Unsafe journal path.')
journal=root/relative
require(not journal.is_symlink() and journal.resolve().parent==output.resolve(),'Journal escaped continuation output.')
payload=journal.read_bytes(); journal_sha=hashlib.sha256(payload).hexdigest()
require(journal_sha==evidence['snapshotJournalSha256']==progress['externalSnapshotJournal']['sha256'],'Journal hash differs.')
path=root/'recovery/numeric-hydration-finalizer/finalize_hydration.py'
require(digest(path)==finalizer_sha,'Frozen parser changed.')
spec=importlib.util.spec_from_file_location('frozen_journal_parser',path); module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
records=module.load_output_snapshot_records(journal)
pair=evidence['backup']['pair']
require(set(pair)=={'data','wal'} and set(records)==set(pair.values()),'Journal outputs differ from Backup pair.')
for kind,name in pair.items():
    row=evidence['pairedSnapshots'][name]; pin=evidence['pinnedSnapshotIdentity'][name]; record=records[name]
    require(record['id'].rstrip('/').lower()==row['id'].rstrip('/').lower()==pin['id'].rstrip('/').lower(),'Snapshot ARM IDs differ.')
    require(row['uniqueId']==pin['uniqueId'] and row['completionPercent']==100,'Snapshot UID/copy is incomplete.')
    require(record['sourcePVC']==row['sourcePVC']==('hdd-pg-1' if kind=='data' else 'hdd-pg-1-wal'),'Journal source differs.')
destination=root/'100gib/.state/output-snapshots.jsonl'
fd=os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
with os.fdopen(fd,'wb') as stream:
    stream.write(payload);stream.flush();os.fsync(stream.fileno())
require(digest(destination)==journal_sha,'Installed journal changed.')
print('Installed exact continuation snapshot journal for the frozen live audit.')
PY
deadline=$((SECONDS+86400))
completed=false
while ((SECONDS < deadline)); do
  verify_sources
  if python3 -B "$ADAPTER" --continuation-evidence "$TAIL_OUTPUT/continuation-evidence.json" --output-dir "$FINAL_OUTPUT"; then
    completed=true
    break
  else
    result=$?
    [[ $result == 3 ]] || die "Continuation final audit failed (exit $result); evidence retained."
  fi
  sleep 60
done
[[ $completed == true ]] || die 'Actual measured PremiumV2 hydration did not complete within 24 hours.'
verify_sources
python3 - "$STACK" "$FINAL_OUTPUT" "$ADAPTER_SHA" "$FINALIZER_SHA" "$DML_PROOF" "$DML_PROOF_SHA" <<'PY'
import hashlib,importlib.util,json,os,sys
from pathlib import Path
stack,output=map(Path,sys.argv[1:3]); adapter_sha,finalizer_sha=sys.argv[3:5]
proof_path=Path(sys.argv[5]);proof_sha=sys.argv[6]
meta=json.loads((output/'prepare.json').read_text()); audit=json.loads((output/'audit.json').read_text())
assert meta['status']=='complete' and meta['fixtureTargetHeapGiB']==100
assert meta['heapBytes']==100*1024**3 and meta['instances']==2 and meta['clientCreated'] is False
assert meta['preparedLiveTuplesEstimate']==meta['preparedDeadTuplesEstimate']==32768000
assert audit['status']=='complete' and audit['fourHydratedPremiumV2Disks']==4
assert audit['checks']['postgres']['replicaCaughtUp'] is True
assert audit['checks']['frozenPreparation']['stageHelperSha256']=='0eacb0a81b30703a3b788aa745d3eca8c079e3e4e0cfca9921f500b6f5c8918b'
assert audit['continuationReceiptChain']['adapterScriptSha256']==adapter_sha
assert audit['continuationReceiptChain']['frozenFinalizerSha256']==finalizer_sha
assert 'continuationFinalizationCapture' in audit['checks'] and 'originalFinalizationCapture' not in audit['checks']
assert hashlib.sha256(proof_path.read_bytes()).hexdigest()==proof_sha
spec=importlib.util.spec_from_file_location('verified_original_delete',proof_path);proof_module=importlib.util.module_from_spec(spec);spec.loader.exec_module(proof_module)
audit=proof_module.supplement_audit(audit)
audit['originalDeleteCaptureVerifierSha256']=proof_sha
sd=stack/'.state'; recovery=sd/'recovery';recovery.mkdir(mode=0o700,exist_ok=True)
def install(path,value):
    if path.exists(): raise RuntimeError('Refusing to overwrite finalization evidence: '+str(path))
    temporary=path.with_name(path.name+'.tmp.'+str(os.getpid()))
    with temporary.open('x') as stream:
        os.chmod(temporary,0o600);json.dump(value,stream,indent=2);stream.write('\n');stream.flush();os.fsync(stream.fileno())
    temporary.replace(path);path.chmod(0o600)
install(recovery/'continuation-finalize-audit.json',audit)
install(sd/'prepare.json',meta)
print('Prepared existing 100 GiB fixture: four hydrated PremiumV2 disks and verified replica catch-up.')
PY
