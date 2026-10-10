#!/usr/bin/env bash
# Complete local preparation receipts after a read-only audit of the existing stack.
set -Eeuo pipefail
umask 077
HERE="$(cd -- "$(dirname -- "$0")" && pwd)"
if [[ -f "$HERE/run-config.json" ]]; then
  STACK="$HERE"; ROOT="$(cd -- "$HERE/.." && pwd)"
else
  ROOT="$(cd -- "$HERE/../.." && pwd)"; STACK="$ROOT/50gib"
fi
SD="$STACK/.state"
die(){ echo "ERROR: $*" >&2; exit 1; }
[[ ${TEST_HEAP_GIB:-50} == 50 && "$(basename "$STACK")" == 50gib ]] || die 'Wrong fixture assigned to this finalizer.'
[[ "$(jq -er .targetHeapGiB "$STACK/run-config.json")" == 50 ]] || die 'Run target changed.'
[[ ! -e "$ROOT/measurement-gate.json" ]] || die 'Preparation cannot run after measurement release.'
[[ ! -e "$SD/prepare.json" ]] || die 'Prepared metadata already exists; refusing overwrite.'
exec 9>"$SD/numeric-finalize.lock"
flock -n 9 || die 'Another numeric finalizer is active.'
FINALIZER="$ROOT/recovery/numeric-hydration-finalizer/finalize_hydration.py"
FINALIZER_SHA=78fc66344fd105ef96ac4299a9ddbf52ec5f74cff081b2b74898b099b0c6d2e1
STAGE_HELPER="$ROOT/recovery/source-restore/stage-allowlisted-source.sh"
HELPER_SHA=0eacb0a81b30703a3b788aa745d3eca8c079e3e4e0cfca9921f500b6f5c8918b
ORIGINAL="$ROOT/recovery/numeric-hydration-finalizer/original-50gib"
CONTROL="$ROOT/recovery/fixture-watch/50-finalization/active-prepare-tmp.a7vQvw"
OUTPUT="$ROOT/recovery/numeric-hydration-finalizer/results/50gib"
deadline=$((SECONDS+86400))
while ((SECONDS < deadline)); do
  [[ "$(sha256sum "$FINALIZER" | awk '{print $1}')" == "$FINALIZER_SHA" ]] || die 'Reviewed finalizer changed.'
  [[ "$(sha256sum "$STAGE_HELPER" | awk '{print $1}')" == "$HELPER_SHA" ]] || die 'Source helper changed.'
  if python3 -B "$FINALIZER" --stack 50gib --control-flow-dir "$CONTROL" \
    --original-process-receipt "$ORIGINAL/prepare-process-stopped.json" \
    --original-prepare-script "$ORIGINAL/02-prepare.sh" \
    --seed-job-uid bc0de226-e948-4186-90c8-573c8b0be2ed \
    --seed-event-evidence "$ROOT/recovery/fixture-watch/50-finalization/live/events.json" \
    --seed-event-sha256 4e96d8a122c0f8adc2308d76f45e457d4ad8fd2681d78accd81c5f8957c0aa4f \
    --output-dir "$OUTPUT"; then
    break
  else
    result=$?
    [[ $result == 3 ]] || die "Read-only final audit failed (exit $result); evidence retained."
  fi
  sleep 60
done
((SECONDS < deadline)) || die 'Actual PremiumV2 hydration did not complete within 24 hours.'
python3 - "$STACK" "$OUTPUT" "$FINALIZER_SHA" <<'PY'
import hashlib,json,os,sys
from pathlib import Path
stack,output=map(Path,sys.argv[1:3]); digest=sys.argv[3]
meta=json.loads((output/'prepare.json').read_text()); audit=json.loads((output/'audit.json').read_text())
assert meta['status']=='complete' and meta['fixtureTargetHeapGiB']==50
assert meta['heapBytes']==50*1024**3 and meta['instances']==2 and meta['clientCreated'] is False
assert meta['preparedLiveTuplesEstimate']==meta['preparedDeadTuplesEstimate']==16384000
assert audit['status']=='complete' and audit['fourHydratedPremiumV2Disks']==4
assert audit['checks']['postgres']['replicaCaughtUp'] is True
assert audit['checks']['frozenPreparation']['stageHelperSha256']=='0eacb0a81b30703a3b788aa745d3eca8c079e3e4e0cfca9921f500b6f5c8918b'
assert hashlib.sha256((stack.parent/'recovery/numeric-hydration-finalizer/finalize_hydration.py').read_bytes()).hexdigest()==digest
sd=stack/'.state'; recovery=sd/'recovery'; recovery.mkdir(mode=0o700,exist_ok=True)
def install(path,value):
    if path.exists(): raise RuntimeError('Refusing to overwrite local finalization evidence: '+str(path))
    temporary=path.with_name(path.name+'.tmp.'+str(os.getpid()))
    with temporary.open('x') as stream:
        os.chmod(temporary,0o600); json.dump(value,stream,indent=2); stream.write('\n'); stream.flush(); os.fsync(stream.fileno())
    temporary.replace(path); path.chmod(0o600)
audit['reviewedFinalizerSha256']=digest
install(recovery/'numeric-finalize-audit.json',audit)
install(sd/'prepare.json',meta)
print('Prepared existing 50 GiB fixture: four numerically hydrated PremiumV2 disks and verified replica catch-up.')
PY
