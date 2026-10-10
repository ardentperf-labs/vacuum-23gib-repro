#!/usr/bin/env bash
# After measured VACUUM completes, prove the retained cold DATA/WAL pair can
# recover the 100-GiB fixture without a Blob archive, then remove only its test
# Cluster/PVCs. Azure snapshots and all VolumeSnapshot objects remain retained.
set -Eeuo pipefail
umask 077
ROOT="$(cd -- "$(dirname -- "$0")" && pwd)"; SD="$ROOT/.state"; STATE="$SD/state.json"
die(){ echo "ERROR: $*" >&2; exit 1; }
for x in az kubectl jq timeout flock; do command -v "$x" >/dev/null || die "Missing $x"; done
jq -e '.version==1 and .status=="ready"' "$STATE" >/dev/null || die "Stack is not provisioned."
SUB=$(jq -er .subscriptionId "$STATE"); RG=$(jq -er .resourceGroup "$STATE"); AKS=$(jq -er .clusterName "$STATE")
NODE_RG=$(jq -er .nodeResourceGroup "$STATE"); LOC=$(jq -er .location "$STATE"); TOKEN=$(jq -er .ownerToken "$STATE")
SHORT=$(cut -c1-8 <<<"$TOKEN"); NS=repro; C=hdd-pg; VERIFY="hdd-pg-cold-check-$SHORT"
SNAPSHOT_RG=$(jq -er .snapshotRetention.resourceGroup "$STATE")
IMAGE='ghcr.io/cloudnative-pg/postgresql:16.15-202609281634-standard-trixie@sha256:1dd42563007500b94b943f2a1620f724efedfc9eea2d07a50c6db4582c2431fd'
SYSIMAGE='ghcr.io/cloudnative-pg/postgresql:16.15-202609140815-system-trixie@sha256:a30ea813ae1c82fc94743c4e71766688158b58d8ddf1f4e20ff5d3edb569a215'
[[ "$TOKEN" =~ ^[a-f0-9]{32}$ ]] || die "Bad owner token."
export KUBECONFIG="$(jq -er .kubeconfigPath "$STATE")"
[[ "$KUBECONFIG" == /* && -f "$KUBECONFIG" ]] || die "Stack kubeconfig is missing or not isolated."
[[ $(kubectl config current-context) == *"$AKS"* ]] || die "Wrong Kubernetes context."
azc(){ timeout 240s az "$@" --subscription "$SUB"; }
k(){ timeout 120s kubectl "$@"; }
mkdir -p "$SD"; chmod 700 "$SD"
exec 8>"$SD/cold-validation.lock"; flock -n 8 || die "Cold restore validation already active."
exec 9>"$SD/test.lock"; flock -n 9 || die "Measurement is still active."
exec 7>"$SD/02-prepare.lock"; flock -n 7 || die "Preparation is still active."
PREP="$SD/prepare.json"; COLD="$SD/cold-snapshot.json"; TEST="$SD/test-state.json"
[[ -f "$PREP" && -f "$COLD" && -f "$TEST" ]] || die "Preparation, cold snapshot, or test evidence is missing."
jq -e '.status=="complete" and .fixtureTargetHeapGiB==100 and (.coldSnapshotPair|length)==2' "$PREP" >/dev/null || die "The 100-GiB cold snapshot pair is not in prepare state."
jq -e '.phase=="complete" and .testComplete==true and .replayCaughtUp==true' "$TEST" >/dev/null || die "Full VACUUM and replica replay have not both completed."
RUN=$(jq -er .runId "$TEST"); OUT=$(jq -er .output "$TEST")
[[ -f "$OUT/summary.json" && -f "$OUT/timing.json" ]] || die "Measured report/timing evidence is incomplete."
jq -e '.status.testComplete==true and .status.vacuumComplete==true and .status.replayCaughtUp==true' "$OUT/summary.json" >/dev/null || die "Measured report does not confirm VACUUM and replay completion."
jq -e '.coverage.valid==true and .coverage.windows.vacuum.valid==true and .coverage.windows.replayDrain.valid==true and .coverage.windows.vacuumAndReplay.valid==true' "$OUT/summary.json" >/dev/null || die "Measured DATA/WAL counter coverage is invalid for one or more required windows; preserve the measured Cluster for review."
# The external log follower may flush its final monitoring summary a few
# seconds after 03-test.py publishes test-state.json.
MONITOR="$OUT/monitoring/monitor-summary.json"; monitor_ready=false; end=$((SECONDS+180))
while ((SECONDS<end)); do
  if [[ -s "$MONITOR" ]] && jq -e '.testState.phase=="complete" and .testState.testComplete==true and .testState.replayCaughtUp==true and .measurementWindow.start!=null and .measurementWindow.end!=null and .logFiles.primary.bytes>0 and .logFiles.replica.bytes>0' "$MONITOR" >/dev/null; then monitor_ready=true; break; fi
  sleep 1
done
[[ "$monitor_ready" == true ]] || die "Continuous replication-log monitor has not flushed a complete primary/replica summary."
COLD_BACKUP=$(jq -er .name "$COLD"); DATA=$(jq -er '.snapshotPair[]|select(.type=="PG_DATA")|.volumeSnapshot' "$COLD"); WAL=$(jq -er '.snapshotPair[]|select(.type=="PG_WAL")|.volumeSnapshot' "$COLD")
jq -e --arg n "$COLD_BACKUP" --arg d "$DATA" --arg w "$WAL" '.coldBackup.name==$n and .coldBackup.phase=="completed" and .coldBackup.online==false and .coldBackup.target=="primary" and ([.coldSnapshotPair[].type]|sort)==["PG_DATA","PG_WAL"] and ([.coldSnapshotPair[].volumeSnapshot]|sort)==([$d,$w]|sort)' "$PREP" >/dev/null || die "Cold snapshot metadata does not match its backup."
[[ $(azc account show --query id -o tsv) == "$SUB" ]] || die "Azure subscription mismatch."
if k get secret azure-barman-credentials -n "$NS" -o json >/dev/null 2>&1; then die "Temporary Blob archive credentials still exist; cannot prove archive-independent recovery."; fi
rg=$(azc group show -n "$SNAPSHOT_RG" -o json)
jq -e --arg t "$TOKEN" --arg n "$SNAPSHOT_RG" --arg l "${LOC,,}" '.name==$n and ((.location|ascii_downcase)==$l) and .tags.reprotoken==$t and .tags["managed-by"]=="vacuum-only-repro" and .tags.reproPurpose=="retained-cnpg-volume-snapshots"' <<<"$rg" >/dev/null || die "Snapshot-retention resource group is not owned by this stack."
jq -e -n --argjson pair "$(jq -c .snapshotPair "$COLD")" '($pair|length)==2 and all($pair[]; .deletionPolicy=="Retain" and (.azureSnapshotId|type)=="string")' >/dev/null || die "Cold snapshot pair metadata is incomplete."

# Save API evidence before deleting the original measured Cluster. A resumed
# invocation must preserve the original pre-delete manifests rather than
# replacing them with post-delete empty inventories.
if [[ ! -s "$SD/volume-snapshots-before-restore.json" ]]; then
  k get volumesnapshot -n "$NS" -o json >"$SD/volume-snapshots-before-restore.json"
fi
if [[ ! -s "$SD/volume-snapshot-contents-before-restore.json" ]]; then
  k get volumesnapshotcontent -o json | jq --arg ns "$NS" '[.items[]|select(.spec.volumeSnapshotRef.namespace==$ns)]' >"$SD/volume-snapshot-contents-before-restore.json"
fi
if [[ ! -s "$SD/azure-retained-snapshots-before-restore.json" ]]; then
  azc snapshot list -g "$SNAPSHOT_RG" -o json >"$SD/azure-retained-snapshots-before-restore.json"
fi
chmod 600 "$SD/volume-snapshots-before-restore.json" "$SD/volume-snapshot-contents-before-restore.json" "$SD/azure-retained-snapshots-before-restore.json"
for item in "$DATA" "$WAL"; do
  v=$(k get volumesnapshot "$item" -n "$NS" -o json)
  jq -e '.status.readyToUse==true and .spec.volumeSnapshotClassName!=null and .status.boundVolumeSnapshotContentName!=null and ((.metadata.ownerReferences // [])|length==0)' <<<"$v" >/dev/null || die "Cold VolumeSnapshot/$item is not ready or is not retained independently of the measured Cluster."
done
PREDELETE_PVCS="$SD/measured-pvcs-before-restore-validation.json"
MEASURED_CLUSTER="$SD/measured-cluster-before-restore-validation.json"
MEASURED_CLUSTER_LIVE="$SD/measured-cluster-live-before-deletion.json"
EXPECTED_CLUSTER_UID=$(jq -er '.clusterManifest.metadata.uid // .metadata.uid' "$MEASURED_CLUSTER") || die "The measured Cluster UID is not recoverable from the saved pre-delete CNPG snapshot manifest."
jq -e --arg t "$TOKEN" --arg c "$C" --arg u "$EXPECTED_CLUSTER_UID" '.clusterManifest.metadata.name==$c and .clusterManifest.metadata.namespace=="repro" and .clusterManifest.metadata.labels["repro.stack.token"]==$t and .clusterManifest.metadata.uid==$u' "$MEASURED_CLUSTER" >/dev/null || die "The saved measured Cluster manifest is not owned by this stack."

# Persist the exact pre-delete state once. A previous failed cleanup may have
# removed some PVCs asynchronously, so subsequent attempts compare against the
# preserved identities and accept only exact NotFound cleanup for those PVCs.
if [[ ! -s "$PREDELETE_PVCS" ]]; then
  cluster=$(k get cluster "$C" -n "$NS" -o json) || die "Original measured Cluster is absent and no preserved pre-delete PVC manifest exists."
  jq -e --arg t "$TOKEN" --arg u "$EXPECTED_CLUSTER_UID" '.metadata.labels["repro.stack.token"]==$t and .metadata.uid==$u and .spec.instances==2' <<<"$cluster" >/dev/null || die "Original Cluster owner token or UID mismatch."
  printf '%s\n' "$cluster" | jq . >"$MEASURED_CLUSTER_LIVE"; chmod 600 "$MEASURED_CLUSTER_LIVE"
  pvc_before=$(k get pvc -n "$NS" -l "cnpg.io/cluster=$C" -o json)
  jq -e --arg c "$C" '([.items[]|select(.metadata.labels["cnpg.io/cluster"]==$c and .status.phase=="Bound")|.metadata.name]|sort)==([($c+"-1"),($c+"-1-wal"),($c+"-2"),($c+"-2-wal")]|sort)' <<<"$pvc_before" >/dev/null || die "Measured Cluster PVC set is not exactly the verified four disks."
  printf '%s\n' "$pvc_before" | jq . >"$PREDELETE_PVCS"; chmod 600 "$PREDELETE_PVCS"
fi
jq -e --arg c "$C" '(.items|length)==4 and ([.items[].metadata.name]|sort)==([($c+"-1"),($c+"-1-wal"),($c+"-2"),($c+"-2-wal")]|sort) and all(.items[]; .status.phase=="Bound" and .metadata.labels["cnpg.io/cluster"]==$c and (.metadata.uid|type)=="string" and .spec.volumeName!=null)' "$PREDELETE_PVCS" >/dev/null || die "Preserved measured PVC manifest is not the exact four-disk set."
jq -e --slurpfile old "$SD/prepared-pvcs.json" '[.items[]|{name:.metadata.name,uid:.metadata.uid,pv:.spec.volumeName}]|sort_by(.name)==([$old[0].items[]|{name:.metadata.name,uid:.metadata.uid,pv:.spec.volumeName}]|sort_by(.name))' "$PREDELETE_PVCS" >/dev/null || die "Preserved pre-delete PVC UIDs/PVs differ from preparation evidence."

cluster=''; cluster_read_err="$SD/measured-cluster-current-read.stderr"
if cluster=$(k get cluster "$C" -n "$NS" -o json 2>"$cluster_read_err"); then
  jq -e --arg t "$TOKEN" --arg u "$EXPECTED_CLUSTER_UID" '.metadata.labels["repro.stack.token"]==$t and .metadata.uid==$u and .spec.instances==2' <<<"$cluster" >/dev/null || die "Live measured Cluster owner token or UID mismatch."
  if [[ ! -s "$MEASURED_CLUSTER_LIVE" ]]; then printf '%s\n' "$cluster" | jq . >"$MEASURED_CLUSTER_LIVE"; chmod 600 "$MEASURED_CLUSTER_LIVE"; fi
  pvc_live=$(k get pvc -n "$NS" -l "cnpg.io/cluster=$C" -o json)
  jq -e --slurpfile saved "$PREDELETE_PVCS" '[.items[]|{name:.metadata.name,uid:.metadata.uid,pv:.spec.volumeName}]|sort_by(.name)==([$saved[0].items[]|{name:.metadata.name,uid:.metadata.uid,pv:.spec.volumeName}]|sort_by(.name))' <<<"$pvc_live" >/dev/null || die "Live measured PVCs differ from the preserved pre-delete identities."
else
  grep -qi 'notfound' "$cluster_read_err" || die "Could not determine measured Cluster state: $(tail -3 "$cluster_read_err" | tr '\n' ' ')"
  clusters_now=$(k get clusters -n "$NS" -o json)
  jq -e --arg c "$C" '([.items[]|select(.metadata.name==$c)]|length)==0' <<<"$clusters_now" >/dev/null || die "Measured Cluster name still appears in the namespace inventory."
  pvc_names_now=$(k get pvc -n "$NS" -o json)
  jq -e --arg c "$C" '([.items[]|select(.metadata.name==($c+"-1") or .metadata.name==($c+"-1-wal") or .metadata.name==($c+"-2") or .metadata.name==($c+"-2-wal") or .metadata.labels["cnpg.io/cluster"]==$c)|.metadata.name]|length)==0' <<<"$pvc_names_now" >/dev/null || die "Measured Cluster PVCs are not all absent before resuming snapshot recovery."
  pods_now=$(k get pods -n "$NS" -o json)
  jq -e --arg c "$C" '([.items[]|select(.metadata.labels["cnpg.io/cluster"]==$c)]|length)==0' <<<"$pods_now" >/dev/null || die "Measured Cluster pods remain after deletion."
fi
if k get cluster "$VERIFY" -n "$NS" -o json >/dev/null 2>&1; then die "Validation Cluster/$VERIFY already exists; inspect it before continuing."; fi

# VACUUM evidence and the immutable cold pair are already saved. Releasing the
# measured Cluster lets the same two-node stack attach only the two restore
# disks, avoiding another full AKS stack or measurement interference.
if [[ -n "$cluster" ]]; then
  k delete cluster "$C" -n "$NS" --wait=true --timeout=40m >/dev/null
  end=$((SECONDS+3600))
  while ((SECONDS<end)); do
    if ! k get cluster "$C" -n "$NS" -o json >/dev/null 2>"$cluster_read_err"; then
      grep -qi 'notfound' "$cluster_read_err" && break
      die "Could not verify measured Cluster deletion: $(tail -3 "$cluster_read_err" | tr '\n' ' ')"
    fi
    sleep 10
  done
  if k get cluster "$C" -n "$NS" -o json >/dev/null 2>"$cluster_read_err"; then die "Measured Cluster object remains after deletion."; fi
  grep -qi 'notfound' "$cluster_read_err" || die "Measured Cluster deletion status is ambiguous."
fi

# CNPG may remove PVCs asynchronously with its Cluster finalizer. Delete only
# an exact remaining pre-delete UID, and treat the exact expected NotFound as
# successful cleanup; never replace the preserved pre-delete inventory.
for claim in "$C-1" "$C-1-wal" "$C-2" "$C-2-wal"; do
  expected_uid=$(jq -er --arg n "$claim" '.items[]|select(.metadata.name==$n)|.metadata.uid' "$PREDELETE_PVCS")
  pvc_read_err="$SD/pvc-${claim}-read.stderr"
  if pvc_live=$(k get pvc "$claim" -n "$NS" -o json 2>"$pvc_read_err"); then
    jq -e --arg c "$C" --arg u "$expected_uid" '.metadata.labels["cnpg.io/cluster"]==$c and .metadata.uid==$u' <<<"$pvc_live" >/dev/null || die "Remaining PVC/$claim identity differs from its preserved pre-delete UID."
    k delete pvc "$claim" -n "$NS" --ignore-not-found=true --wait=true --timeout=20m >/dev/null
  else
    grep -qi 'notfound' "$pvc_read_err" || die "Could not verify PVC/$claim cleanup: $(tail -3 "$pvc_read_err" | tr '\n' ' ')"
  fi
done
end=$((SECONDS+1800)); remaining_absent=false
while ((SECONDS<end)); do
  pvc_names_now=$(k get pvc -n "$NS" -o json)
  jq -e --arg c "$C" '([.items[]|select(.metadata.name==($c+"-1") or .metadata.name==($c+"-1-wal") or .metadata.name==($c+"-2") or .metadata.name==($c+"-2-wal") or .metadata.labels["cnpg.io/cluster"]==$c)|.metadata.name]|length)==0' <<<"$pvc_names_now" >/dev/null && { remaining_absent=true; break; }
  sleep 10
done
[[ "$remaining_absent" == true ]] || die "Measured Cluster PVCs remain; keep stack for safe review."
pods_now=$(k get pods -n "$NS" -o json)
jq -e --arg c "$C" '([.items[]|select(.metadata.labels["cnpg.io/cluster"]==$c)]|length)==0' <<<"$pods_now" >/dev/null || die "Measured Cluster pods remain after deletion."
printf '%s\n' "$(jq -cn --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg uid "$EXPECTED_CLUSTER_UID" --arg pvc "$PREDELETE_PVCS" --arg manifest "$MEASURED_CLUSTER" '{status:"measured-cluster-and-exact-pvcs-absent",verifiedAtUtc:$at,measuredClusterUid:$uid,preservedPredeletePvcManifest:$pvc,preservedClusterManifest:$manifest,onlyExpectedClusterAndPvcNamesChecked:true}')" >"$SD/measured-cluster-cleanup-verification.json"; chmod 600 "$SD/measured-cluster-cleanup-verification.json"

cat >"$SD/restore-cluster.yaml" <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: $VERIFY
  namespace: $NS
  labels: {repro.stack.token: "$TOKEN", repro.validation: "cold-snapshot-restore"}
spec:
  instances: 1
  imageName: $SYSIMAGE
  storage: {storageClass: azure-standard-hdd, size: 512Gi}
  walStorage: {storageClass: azure-standard-hdd, size: 512Gi}
  bootstrap:
    recovery:
      database: repro
      owner: app
      volumeSnapshots:
        storage: {name: $DATA, kind: VolumeSnapshot, apiGroup: snapshot.storage.k8s.io}
        walStorage: {name: $WAL, kind: VolumeSnapshot, apiGroup: snapshot.storage.k8s.io}
  affinity: {enablePodAntiAffinity: true, podAntiAffinityType: required, topologyKey: kubernetes.io/hostname}
  resources: {requests: {cpu: "1", memory: 2Gi}, limits: {cpu: "2", memory: 3Gi}}
EOF
RESTORE_JOB_UID=''; RESTORE_JOB_NAME=''
capture_restore_job_evidence(){
  local jobs observed events event pods restore_pod exited tmp
  jobs=$(k get jobs -n "$NS" -o json 2>/dev/null || printf '{"items":[]}')
  observed=$(jq -c --arg i "$VERIFY-1" '[.items[]|select(.metadata.labels["cnpg.io/instanceName"]==$i and .metadata.labels["cnpg.io/jobRole"]=="snapshot-recovery")]|sort_by(.metadata.creationTimestamp)|last // empty' <<<"$jobs")
  if [[ -n "$observed" ]]; then
    RESTORE_JOB_UID=$(jq -er .metadata.uid <<<"$observed")
    RESTORE_JOB_NAME=$(jq -er .metadata.name <<<"$observed")
    tmp="$SD/cold-snapshot-recovery-job.json.tmp"
    printf '%s\n' "$observed" | jq . >"$tmp"; chmod 600 "$tmp"; mv -f "$tmp" "$SD/cold-snapshot-recovery-job.json"
  elif [[ -s "$SD/cold-snapshot-recovery-job.json" ]]; then
    RESTORE_JOB_UID=$(jq -er .metadata.uid "$SD/cold-snapshot-recovery-job.json")
    RESTORE_JOB_NAME=$(jq -er .metadata.name "$SD/cold-snapshot-recovery-job.json")
  fi
  [[ -n "$RESTORE_JOB_UID" ]] || return 0

  events=$(k get events -n "$NS" -o json 2>/dev/null || printf '{"items":[]}')
  event=$(jq -c --arg u "$RESTORE_JOB_UID" '[.items[]|select(.involvedObject.kind=="Job" and .involvedObject.uid==$u and .reason=="Completed" and .message=="Job completed")]|sort_by(.lastTimestamp // .eventTime // .metadata.creationTimestamp)|last // empty' <<<"$events")
  if [[ -n "$event" ]]; then
    tmp="$SD/cold-snapshot-recovery-completed-event.json.tmp"
    printf '%s\n' "$event" | jq . >"$tmp"; chmod 600 "$tmp"; mv -f "$tmp" "$SD/cold-snapshot-recovery-completed-event.json"
  fi

  # Capture the Job pod manifest/log while the operator may still retain it.
  pods=$(k get pods -n "$NS" -o json 2>/dev/null || printf '{"items":[]}')
  restore_pod=$(jq -c --arg u "$RESTORE_JOB_UID" '[.items[]|select(any(.metadata.ownerReferences[]?;.kind=="Job" and .uid==$u))]|last // empty' <<<"$pods")
  if [[ -n "$restore_pod" ]]; then
    RESTORE_JOB_POD_NAME=$(jq -er .metadata.name <<<"$restore_pod")
    tmp="$SD/cold-snapshot-recovery-pod.json.tmp"
    printf '%s\n' "$restore_pod" | jq . >"$tmp"; chmod 600 "$tmp"; mv -f "$tmp" "$SD/cold-snapshot-recovery-pod.json"
    exited=$(jq -r '[.status.containerStatuses[]?|select(.name=="snapshot-recovery" and .state.terminated.exitCode==0)]|length>0' <<<"$restore_pod")
    if [[ "$exited" == true ]]; then
      k logs "$RESTORE_JOB_POD_NAME" -n "$NS" -c snapshot-recovery >"$SD/cold-recovery-job.log.tmp" 2>/dev/null && { chmod 600 "$SD/cold-recovery-job.log.tmp"; mv -f "$SD/cold-recovery-job.log.tmp" "$SD/cold-recovery-job.log"; } || rm -f "$SD/cold-recovery-job.log.tmp"
    fi
  fi
}
k apply -f "$SD/restore-cluster.yaml" >/dev/null
end=$((SECONDS+10800)); cluster='{}'; pod=''
while ((SECONDS<end)); do
  capture_restore_job_evidence
  cluster=$(k get cluster "$VERIFY" -n "$NS" -o json 2>/dev/null || true)
  if [[ -n "$cluster" && $(jq -r '.status.readyInstances // 0' <<<"$cluster") == 1 ]]; then
    pods=$(k get pods -n "$NS" -l "cnpg.io/cluster=$VERIFY" -o json)
    pod=$(jq -r --arg c "$VERIFY" '[.items[]|select(.metadata.name==($c+"-1") and .status.phase=="Running" and any(.status.containerStatuses[]?;.name=="postgres" and .ready))]|first|.metadata.name // empty' <<<"$pods")
    [[ -n "$pod" ]] && break
  fi
jq -e '.status.phase=="failed"' <<<"$cluster" >/dev/null 2>&1 && die "Cold snapshot restore Cluster entered failed status."
  sleep 2
done
[[ -n "$pod" ]] || die "Cold snapshot restore did not become ready."

# Wait briefly for the exact-UID completion event if Job garbage collection
# removed the successful Job before its final status could be observed.
for _ in {1..30}; do
  capture_restore_job_evidence
  job_succeeded=false; event_matches=false
  if [[ -s "$SD/cold-snapshot-recovery-job.json" ]]; then
    job_succeeded=$(jq -r '.status.succeeded==1' "$SD/cold-snapshot-recovery-job.json")
    RESTORE_JOB_UID=$(jq -er .metadata.uid "$SD/cold-snapshot-recovery-job.json")
    RESTORE_JOB_NAME=$(jq -er .metadata.name "$SD/cold-snapshot-recovery-job.json")
  fi
  if [[ -s "$SD/cold-snapshot-recovery-completed-event.json" ]]; then
    event_matches=$(jq -e --arg u "$RESTORE_JOB_UID" '.involvedObject.kind=="Job" and .involvedObject.uid==$u and .reason=="Completed" and .message=="Job completed"' "$SD/cold-snapshot-recovery-completed-event.json" >/dev/null && echo true || echo false)
  fi
  [[ "$job_succeeded" == true || "$event_matches" == true ]] && break
  sleep 1
done
[[ -s "$SD/cold-snapshot-recovery-job.json" ]] || die "CNPG snapshot recovery Job was not captured during activation; preserve the validation Cluster and inspect it."
restore_job=$(cat "$SD/cold-snapshot-recovery-job.json")
RESTORE_JOB_UID=$(jq -er .metadata.uid <<<"$restore_job"); RESTORE_JOB_NAME=$(jq -er .metadata.name <<<"$restore_job")
job_succeeded=$(jq -r '.status.succeeded==1' <<<"$restore_job")
event_matches=false
restore_job_event='{}'
if [[ -s "$SD/cold-snapshot-recovery-completed-event.json" ]]; then
  restore_job_event=$(cat "$SD/cold-snapshot-recovery-completed-event.json")
  event_matches=$(jq -e --arg u "$RESTORE_JOB_UID" '.involvedObject.kind=="Job" and .involvedObject.uid==$u and .reason=="Completed" and .message=="Job completed"' <<<"$restore_job_event" >/dev/null && echo true || echo false)
fi
jq -e --arg i "$SYSIMAGE" --arg inst "$VERIFY-1" '(.metadata.uid|type)=="string" and .metadata.labels["cnpg.io/instanceName"]==$inst and .metadata.labels["cnpg.io/jobRole"]=="snapshot-recovery" and (.spec.template.spec.containers[]|select(.name=="snapshot-recovery")|.image)==$i and (([.spec.template.spec.containers[]|select(.name=="snapshot-recovery")|.command[]]|join(" "))|contains("restoresnapshot"))' <<<"$restore_job" >/dev/null || die "Captured cold restore Job spec/UID does not match the pinned system-image snapshot-recovery job."
[[ "$job_succeeded" == true || "$event_matches" == true ]] || die "Captured snapshot-recovery Job has neither successful status nor its exact-UID Completed event."
if [[ "$job_succeeded" == true ]]; then RESTORE_JOB_COMPLETION_EVIDENCE=job-status-succeeded; else RESTORE_JOB_COMPLETION_EVIDENCE=exact-uid-completed-event; fi
[[ -s "$SD/cold-recovery-job.log" ]] || printf 'CNPG snapshot recovery completed; the bootstrap Job pod log was not retained. Exact Job UID/spec and completion evidence are in cold-snapshot-recovery-job.json and cold-snapshot-recovery-completed-event.json.\n' >"$SD/cold-recovery-job.log"
chmod 600 "$SD/cold-recovery-job.log"
restore_pvcs=$(k get pvc -n "$NS" -l "cnpg.io/cluster=$VERIFY" -o json)
jq -e --arg c "$VERIFY" --arg d "$DATA" --arg w "$WAL" '(.items|length)==2 and ([.items[]|select(.metadata.name==($c+"-1") and .status.phase=="Bound" and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$d and .spec.dataSourceRef.kind=="VolumeSnapshot" and .spec.dataSourceRef.name==$d)]|length)==1 and ([.items[]|select(.metadata.name==($c+"-1-wal") and .status.phase=="Bound" and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$w and .spec.dataSourceRef.kind=="VolumeSnapshot" and .spec.dataSourceRef.name==$w)]|length)==1' <<<"$restore_pvcs" >/dev/null || die "Restored DATA/WAL PVCs do not have the exact expected VolumeSnapshot data sources."
printf '%s\n' "$restore_pvcs" | jq . >"$SD/cold-restore-pvcs.json"; chmod 600 "$SD/cold-restore-pvcs.json"

# CNPG snapshot recovery runs its restore job with the system image, then the
# Cluster is explicitly advanced to the measurement image. This mirrors the
# production preparation flow while proving the cold pair through the same
# bootstrap path.
k logs "$pod" -n "$NS" -c postgres --since=2h >"$SD/cold-recovery-system-image.log" || die "Could not capture system-image bootstrap Postgres log."
k patch cluster "$VERIFY" -n "$NS" --type=merge -p "$(jq -cn --arg i "$IMAGE" '{spec:{imageName:$i,primaryUpdateMethod:"restart"}}')" >/dev/null
digest=$(sed 's/.*@//' <<<"$IMAGE"); end=$((SECONDS+7200)); ready_measurement=false
while ((SECONDS<end)); do
  cluster=$(k get cluster "$VERIFY" -n "$NS" -o json)
  pods=$(k get pods -n "$NS" -l "cnpg.io/cluster=$VERIFY" -o json)
  if [[ $(jq -r '.status.readyInstances // 0' <<<"$cluster") == 1 ]] && jq -e --arg i "$IMAGE" --arg d "$digest" '[.items[]|select(.status.phase=="Running" and any(.spec.containers[];.name=="postgres" and .image==$i) and any(.status.containerStatuses[]?;.name=="postgres" and .ready and (.imageID|endswith($d))))]|length==1' <<<"$pods" >/dev/null; then ready_measurement=true; break; fi
  sleep 10
done
[[ "$ready_measurement" == true ]] || die "Restored Cluster did not reach the pinned measurement image."

EXPECTED_SYS=$(jq -er '.fixtureAtSnapshot.systemIdentifier' "$COLD")
EXPECTED_HEAP=$(jq -er '.fixtureAtSnapshot.heapBytes' "$COLD")
EXPECTED_LIVE=$(jq -er '.fixtureAtSnapshot.expectedLiveTuples|select(type=="number" and .>0)' "$COLD")
EXPECTED_DEAD=$(jq -er '.fixtureAtSnapshot.expectedDeadTuples|select(type=="number" and .>0)' "$COLD")
((EXPECTED_LIVE==EXPECTED_DEAD)) || die "Cold snapshot metadata does not contain matching prepared live/dead counts."
low_plan=$(k exec -n "$NS" -c postgres "$pod" -- psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d repro -c 'SET enable_seqscan=off; EXPLAIN (FORMAT JSON) SELECT id FROM public.vacuum_target ORDER BY id ASC LIMIT 1;')
high_plan=$(k exec -n "$NS" -c postgres "$pod" -- psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d repro -c 'SET enable_seqscan=off; EXPLAIN (FORMAT JSON) SELECT id FROM public.vacuum_target ORDER BY id DESC LIMIT 1;')
for plan in "$low_plan" "$high_plan"; do
  jq -e '([..|objects|select((."Node Type"?=="Index Scan" or ."Node Type"?=="Index Only Scan" or ."Node Type"?=="Bitmap Index Scan") and ."Index Name"?=="vacuum_target_id_idx")]|length)>0 and ([..|objects|select(."Node Type"?=="Seq Scan")]|length)==0' <<<"$plan" >/dev/null || die "Cold restore tuple-bound query plan is not indexed."
done
sentinel_plan=$(jq -cn --argjson low "$low_plan" --argjson high "$high_plan" '{lowId:$low,highId:$high}')
fixture=$(k exec -n "$NS" -c postgres "$pod" -- psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d repro -c "SET enable_seqscan=off; WITH bounds AS (SELECT (SELECT id FROM public.vacuum_target ORDER BY id ASC LIMIT 1) AS low_id,(SELECT id FROM public.vacuum_target ORDER BY id DESC LIMIT 1) AS high_id) SELECT json_build_object('systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),'inRecovery',pg_is_in_recovery(),'heapBytes',pg_relation_size('public.vacuum_target'),'totalBytes',pg_total_relation_size('public.vacuum_target'),'payloadBytes',(SELECT octet_length(payload) FROM public.vacuum_target ORDER BY id ASC LIMIT 1),'payloadStoragePlain',(SELECT attstorage='p' FROM pg_attribute WHERE attrelid='public.vacuum_target'::regclass AND attname='payload'),'tableAutovacuumDisabled',(SELECT 'autovacuum_enabled=false'=ANY(reloptions) FROM pg_class WHERE oid='public.vacuum_target'::regclass),'hasIndex',to_regclass('public.vacuum_target_id_idx') IS NOT NULL,'lowestLiveId',low_id,'highestLiveId',high_id,'lowIdIsEven',low_id%2=0,'highIdIsEven',high_id%2=0,'lowLiveIdPresent',EXISTS(SELECT 1 FROM public.vacuum_target WHERE id=low_id),'lowAdjacentOddAbsent',NOT EXISTS(SELECT 1 FROM public.vacuum_target WHERE id=low_id+1),'highLiveIdPresent',EXISTS(SELECT 1 FROM public.vacuum_target WHERE id=high_id),'highAdjacentOddAbsent',NOT EXISTS(SELECT 1 FROM public.vacuum_target WHERE id=high_id+1),'statsLiveTuplesEstimate',(SELECT n_live_tup FROM pg_stat_user_tables WHERE relid='public.vacuum_target'::regclass),'statsDeadTuplesEstimate',(SELECT n_dead_tup FROM pg_stat_user_tables WHERE relid='public.vacuum_target'::regclass),'statsLastAnalyze',(SELECT last_analyze FROM pg_stat_user_tables WHERE relid='public.vacuum_target'::regclass),'walLevel',current_setting('wal_level'),'fullPageWrites',current_setting('full_page_writes'),'recoveryPrefetch',current_setting('recovery_prefetch'),'vacuumCostDelay',current_setting('vacuum_cost_delay'),'archiveMode',current_setting('archive_mode'),'restoreCommand',current_setting('restore_command'),'barmanConfig',EXISTS(SELECT 1 FROM pg_settings WHERE name LIKE 'barman%'))::text FROM bounds;")
fixture=$(jq --argjson expectedLive "$EXPECTED_LIVE" --argjson expectedDead "$EXPECTED_DEAD" --argjson plan "$sentinel_plan" '. + {expectedLiveTuples:$expectedLive,expectedDeadTuples:$expectedDead,tupleStatisticsNote:"pg_stat_user_tables live/dead values are advisory estimates and can reset or be stale after restart/restore; expected counts come from recorded fixture insert/delete operations",indexedSentinelPlan:$plan}' <<<"$fixture")
jq -e --arg s "$EXPECTED_SYS" --argjson h "$EXPECTED_HEAP" '.systemIdentifier==$s and .inRecovery==false and (.heapBytes >= ($h*0.99) and .heapBytes <= ($h*1.01)) and .payloadBytes==1536 and .payloadStoragePlain and .tableAutovacuumDisabled and .hasIndex and .expectedLiveTuples==.expectedDeadTuples and .lowIdIsEven and .highIdIsEven and .lowLiveIdPresent and .lowAdjacentOddAbsent and .highLiveIdPresent and .highAdjacentOddAbsent and .walLevel=="logical" and .fullPageWrites=="on" and .recoveryPrefetch=="try" and .vacuumCostDelay=="0" and (.barmanConfig==false)' <<<"$fixture" >/dev/null || die "Cold snapshot restore fixture/defaults or indexed tuple sentinels failed: $fixture"
verify_cluster=$(k get cluster "$VERIFY" -n "$NS" -o json)
jq -e '(.spec.backup.barmanObjectStore // null)==null and ((.spec.externalClusters // [])|length)==0' <<<"$verify_cluster" >/dev/null || die "Restore Cluster unexpectedly has external ObjectStore/archive sources."
k logs "$pod" -n "$NS" -c postgres --since=2h >"$SD/cold-recovery-postgres.log" || die "Could not capture validation Postgres startup log."
chmod 600 "$SD/cold-recovery-system-image.log" "$SD/cold-recovery-postgres.log" "$SD/cold-recovery-job.log"

result=$(jq -cn --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg backup "$COLD_BACKUP" --arg cluster "$NS/$VERIFY" --arg job "$RESTORE_JOB_UID" --arg jobName "$RESTORE_JOB_NAME" --arg completion "$RESTORE_JOB_COMPLETION_EVIDENCE" --arg jobJson "$SD/cold-snapshot-recovery-job.json" --arg eventJson "$SD/cold-snapshot-recovery-completed-event.json" --arg pvcJson "$SD/cold-restore-pvcs.json" --arg restoreImage "$SYSIMAGE" --arg measurementImage "$IMAGE" --arg data "$DATA" --arg wal "$WAL" --arg sys "$EXPECTED_SYS" --arg monitor "$MONITOR" --arg systemlog "$SD/cold-recovery-system-image.log" --arg pglog "$SD/cold-recovery-postgres.log" --arg joblog "$SD/cold-recovery-job.log" --argjson pvcRefs "$restore_pvcs" --argjson jobEvent "$restore_job_event" --argjson heap "$EXPECTED_HEAP" --argjson fixture "$fixture" '{status:"passed",validatedAtUtc:$at,coldBackup:$backup,restoreCluster:$cluster,bootstrap:{recoveryJobUid:$job,recoveryJobName:$jobName,recoveryImage:$restoreImage,measurementImage:$measurementImage,snapshotRecoveryJobEvidence:$completion,snapshotRecoveryJobJson:$jobJson,snapshotRecoveryCompletedEvent:$jobEvent,snapshotRecoveryEventJson:$eventJson,snapshotRecoveryPVCRefsJson:$pvcJson,pvcSnapshotRefsVerified:true,snapshotRestoreJobCompleted:true,systemImagePostgresLog:$systemlog,measurementImagePostgresLog:$pglog,bootstrapJobLog:$joblog},restorePVCRefs:$pvcRefs,azureSnapshotPair:{data:$data,wal:$wal},archiveDependency:{barmanObjectStoreConfigured:false,externalClustersConfigured:false,temporaryCredentialSecretPresent:false,restoreCommand:$fixture.restoreCommand,archiveMode:$fixture.archiveMode,successfulColdSnapshotRecoveryWithoutObjectStore:true},fixture:{systemIdentifier:$fixture.systemIdentifier,heapBytes:$fixture.heapBytes,totalBytes:$fixture.totalBytes,payloadBytes:$fixture.payloadBytes,tableAutovacuumDisabled:$fixture.tableAutovacuumDisabled,hasIndex:$fixture.hasIndex,expectedSystemIdentifier:$sys,expectedHeapBytes:$heap,fullFixtureDetails:$fixture},measurementLogMonitor:$monitor,sourceSnapshotsPreserved:true,validationClusterCleanup:"pending"}')
printf '%s\n' "$result" | jq . >"$SD/cold-snapshot-validation.json.tmp"; chmod 600 "$SD/cold-snapshot-validation.json.tmp"; mv -f "$SD/cold-snapshot-validation.json.tmp" "$SD/cold-snapshot-validation.json"

# Delete only the token-labeled one-instance validation Cluster/PVCs. Snapshot
# and VolumeSnapshot resources are never touched by this script.
k delete cluster "$VERIFY" -n "$NS" --wait=true --timeout=40m >/dev/null
end=$((SECONDS+1800)); remains=true
while ((SECONDS<end)); do
  pvc=$(k get pvc -n "$NS" -l "cnpg.io/cluster=$VERIFY" -o json)
  [[ $(jq '.items|length' <<<"$pvc") == 0 ]] && { remains=false; break; }
  sleep 10
done
[[ "$remains" == false ]] || die "Validation Cluster PVCs remain; keep stack for safe review."
for item in "$DATA" "$WAL"; do
  k get volumesnapshot "$item" -n "$NS" -o json | jq -e '.status.readyToUse==true' >/dev/null || die "Cold VolumeSnapshot/$item disappeared after verification cluster cleanup."
done
azc snapshot list -g "$SNAPSHOT_RG" -o json >"$SD/azure-retained-snapshots-after-restore.json"
for id in $(jq -r '.snapshotPair[].azureSnapshotId' "$COLD"); do
  still=$(azc snapshot show --ids "$id" -o json) || die "Retained snapshot disappeared after verification cleanup: $id"
  jq -e --arg i "${id,,}" '.provisioningState=="Succeeded" and ((.id|ascii_downcase)==$i)' <<<"$still" >/dev/null || die "Retained snapshot is not ready after verification cleanup: $id"
done
jq '.validationClusterCleanup="deleted; only its Cluster and token-labeled PVCs were removed" | .sourceSnapshotsPreserved=true' "$SD/cold-snapshot-validation.json" >"$SD/cold-snapshot-validation.json.tmp"
chmod 600 "$SD/cold-snapshot-validation.json.tmp"; mv -f "$SD/cold-snapshot-validation.json.tmp" "$SD/cold-snapshot-validation.json"
jq --slurpfile validation "$SD/cold-snapshot-validation.json" '.restoreValidation=$validation[0]' "$COLD" >"$COLD.tmp"
chmod 600 "$COLD.tmp"; mv -f "$COLD.tmp" "$COLD"
MANIFEST=$(jq -er .snapshotManifest "$PREP")
jq --slurpfile validation "$SD/cold-snapshot-validation.json" '.coldBackup.restoreValidation=$validation[0]' "$MANIFEST" >"$MANIFEST.tmp"
chmod 600 "$MANIFEST.tmp"; mv -f "$MANIFEST.tmp" "$MANIFEST"
jq --slurpfile validation "$SD/cold-snapshot-validation.json" '.coldBackup.restoreValidation=$validation[0]' "$PREP" >"$PREP.tmp"
chmod 600 "$PREP.tmp"; mv -f "$PREP.tmp" "$PREP"
echo "Verified cold snapshot recovery without Blob archive; 100-GiB fixture restored. Temporary validation Cluster/PVCs were cleaned up. State: $SD/cold-snapshot-validation.json"
