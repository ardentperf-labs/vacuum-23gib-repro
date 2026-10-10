#!/usr/bin/env bash
# Add a cold paired snapshot after fixture preparation and before measurement.
# The primary target is fenced only for snapshot consistency; run no workload.
set -Eeuo pipefail
umask 077
ROOT="$(cd -- "$(dirname -- "$0")" && pwd)"; SD="$ROOT/.state"; STATE="$SD/state.json"
die(){ echo "ERROR: $*" >&2; exit 1; }
for x in az kubectl jq timeout flock; do command -v "$x" >/dev/null || die "Missing $x"; done
jq -e '.version==1 and .status=="ready"' "$STATE" >/dev/null || die "Stack is not provisioned."
SUB=$(jq -er .subscriptionId "$STATE"); RG=$(jq -er .resourceGroup "$STATE"); AKS=$(jq -er .clusterName "$STATE")
NODE_RG=$(jq -er .nodeResourceGroup "$STATE"); LOC=$(jq -er .location "$STATE"); TOKEN=$(jq -er .ownerToken "$STATE")
SHORT=$(cut -c1-8 <<<"$TOKEN"); NS=repro; C=hdd-pg; BK="vacuum-cold-$SHORT"
SNAPSHOT_RG=$(jq -er .snapshotRetention.resourceGroup "$STATE")
SNAPSHOT_RG_ID=$(jq -er .snapshotRetention.resourceGroupId "$STATE")
ROLE_ASSIGNMENT=$(jq -er .snapshotRetention.roleAssignmentId "$STATE")
[[ "$TOKEN" =~ ^[a-f0-9]{32}$ ]] || die "Bad owner token."
jq -e --arg t "$TOKEN" --arg n "rg-vacuum-retained-$TOKEN" --arg rg "$SNAPSHOT_RG" '.snapshotRetention.ownerToken==$t and .snapshotRetention.resourceGroup==$rg and $rg==$n and .snapshotRetention.purpose=="cnpg-pre-vacuum-snapshot-retention"' "$STATE" >/dev/null || die "Snapshot-retention ownership mismatch."
export KUBECONFIG="$(jq -er .kubeconfigPath "$STATE")"
[[ "$KUBECONFIG" == /* && -f "$KUBECONFIG" ]] || die "Stack kubeconfig is missing or not isolated."
[[ $(kubectl config current-context) == *"$AKS"* ]] || die "Wrong Kubernetes context."
azc(){ timeout 240s az "$@" --subscription "$SUB"; }
k(){ timeout 120s kubectl "$@"; }
mkdir -p "$SD"; chmod 700 "$SD"
exec 8>"$SD/cold-snapshot.lock"; flock -n 8 || die "Cold snapshot step already active."
# Hold the same lock as 03-test.py so the VACUUM run cannot start mid-checkpoint.
exec 9>"$SD/test.lock"; flock -n 9 || die "Measurement is active; cold snapshot must precede it."
exec 7>"$SD/02-prepare.lock"; flock -n 7 || die "Preparation is still active."
PREP="$SD/prepare.json"; [[ -f "$PREP" ]] || die "Run 02-prepare.sh first."
jq -e --arg t "$TOKEN" '.status=="complete" and .fixtureTargetHeapGiB==100 and (.snapshotManifest|type)=="string"' "$PREP" >/dev/null || die "Expected a complete 100-GiB prepared stack."
MANIFEST=$(jq -er .snapshotManifest "$PREP"); [[ -f "$MANIFEST" ]] || die "Prepared online-pair manifest is missing."
jq -e --arg t "$TOKEN" '.status=="verified-pre-vacuum" and .ownerToken==$t and .fixture.targetHeapGiB==100 and (.preparedPreVacuumSnapshotPair|length)==2' "$MANIFEST" >/dev/null || die "Online snapshot manifest is incomplete."
EXPECTED_LIVE=$(jq -er '.preparedLiveTuplesEstimate|select(type=="number" and .>0)' "$PREP")
EXPECTED_DEAD=$(jq -er '.preparedDeadTuplesEstimate|select(type=="number" and .>0)' "$PREP")
((EXPECTED_LIVE==EXPECTED_DEAD)) || die "Prepared fixture metadata does not report equal live/dead tuple counts."
[[ $(azc account show --query id -o tsv) == "$SUB" ]] || die "Azure subscription mismatch."
rg=$(azc group show -n "$SNAPSHOT_RG" -o json)
jq -e --arg t "$TOKEN" --arg n "$SNAPSHOT_RG" --arg l "${LOC,,}" '.name==$n and ((.location|ascii_downcase)==$l) and .tags.reprotoken==$t and .tags["managed-by"]=="vacuum-only-repro" and .tags.reproPurpose=="retained-cnpg-volume-snapshots"' <<<"$rg" >/dev/null || die "Snapshot-retention resource group is not owned by this stack."
[[ "$(jq -er .snapshotRetention.resourceGroupId "$STATE" | tr '[:upper:]' '[:lower:]')" == "/subscriptions/${SUB,,}/resourcegroups/${SNAPSHOT_RG,,}" ]] || die "Retention group ID mismatch."
[[ "$(jq -er .snapshotRetention.roleAssignmentId "$STATE")" == "$ROLE_ASSIGNMENT" ]] || die "Retention role assignment state mismatch."
cluster=$(k get cluster "$C" -n "$NS" -o json)
jq -e --arg t "$TOKEN" --arg rg "$SNAPSHOT_RG" '.metadata.labels["repro.stack.token"]==$t and .spec.backup.volumeSnapshot.online==true and .spec.backup.volumeSnapshot.className!=null and .spec.backup.volumeSnapshot.walClassName!=null' <<<"$cluster" >/dev/null || die "The prepared cluster has no paired snapshot configuration."
[[ $(jq -r '.status.readyInstances // 0' <<<"$cluster") == 2 ]] || die "Prepared two-instance cluster is not ready."
[[ $(jq -r '.spec.backup.barmanObjectStore // null' <<<"$cluster") == null ]] || die "Temporary archive writer must be removed before this retained cold checkpoint."
pods=$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json)
jq -e --arg p "$C-1" --arg r "$C-2" '[.items[]|select((.metadata.name==$p or .metadata.name==$r) and .status.phase=="Running" and any(.status.containerStatuses[]?;.name=="postgres" and .ready))]|length==2' <<<"$pods" >/dev/null || die "Both prepared PostgreSQL pods must be running."
PRIMARY_POD_BEFORE=$(k get pod "$C-1" -n "$NS" -o json)
PRIMARY_UID_BEFORE=$(jq -er .metadata.uid <<<"$PRIMARY_POD_BEFORE")
POSTMASTER_BEFORE=$(k exec -n "$NS" -c postgres "$C-1" -- psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d repro -c 'SELECT pg_postmaster_start_time();')
[[ -n "$POSTMASTER_BEFORE" ]] || die "Cannot record primary postmaster time before cold snapshot."
if k get backup "$BK" -n "$NS" -o json >/dev/null 2>&1; then die "Backup/$BK already exists; inspect its prior result."; fi

# Make retention semantics explicit on CNPG-generated VolumeSnapshot objects.
# `none` is the current CRD default, but spelling it out prevents deletion of
# retained snapshot objects if the Cluster is later removed for restore proof.
k patch cluster "$C" -n "$NS" --type=merge -p '{"spec":{"backup":{"volumeSnapshot":{"snapshotOwnerReference":"none"}}}}' >/dev/null

# A checkpoint ensures the target's data and separate WAL volumes are at a clean,
# consistent recovery point. This is after fixture preparation and before VACUUM.
# It may fence/restart the primary; record the effect for report interpretation.
cat >"$SD/cold-backup.yaml" <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata:
  name: $BK
  namespace: $NS
  labels: {repro.stack.token: "$TOKEN"}
  annotations: {vacuum-only-repro/purpose: "recoverable cold pre-VACUUM fixture snapshot"}
spec:
  cluster: {name: $C}
  method: volumeSnapshot
  online: false
  target: primary
EOF
k apply -f "$SD/cold-backup.yaml" >/dev/null
end=$((SECONDS+7200)); backup='{}'; phase=''
while ((SECONDS<end)); do
  backup=$(k get backup "$BK" -n "$NS" -o json); phase=$(jq -r '.status.phase // empty' <<<"$backup")
  [[ "$phase" == completed ]] && break
  [[ "$phase" != failed && "$phase" != walArchivingFailing ]] || die "Cold CNPG snapshot backup failed: $(jq -c '.status' <<<"$backup")"
  sleep 10
done
[[ "$phase" == completed ]] || die "Cold CNPG snapshot backup timed out."
jq -e --arg p "$C-1" '.spec.method=="volumeSnapshot" and .spec.online==false and .spec.target=="primary" and .status.phase=="completed" and .status.instanceID.podName==$p and ([.status.snapshotBackupStatus.elements[].type]|sort)==["PG_DATA","PG_WAL"]' <<<"$backup" >/dev/null || die "Cold Backup status lacks explicit primary provenance and paired snapshots."
DATA=$(jq -er '.status.snapshotBackupStatus.elements[]|select(.type=="PG_DATA")|.name' <<<"$backup")
WAL=$(jq -er '.status.snapshotBackupStatus.elements[]|select(.type=="PG_WAL")|.name' <<<"$backup")
PAIR='[]'
for spec in "PG_DATA:$DATA:$C-1" "PG_WAL:$WAL:$C-1-wal"; do
  IFS=: read -r kind snap pvc <<<"$spec"
  vs=$(k get volumesnapshot "$snap" -n "$NS" -o json)
  jq -e --arg p "$pvc" '.status.readyToUse==true and .spec.source.persistentVolumeClaimName==$p' <<<"$vs" >/dev/null || die "$kind cold VolumeSnapshot is not ready or references the wrong PVC."
  content_name=$(jq -er '.status.boundVolumeSnapshotContentName' <<<"$vs")
  content=$(k get volumesnapshotcontent "$content_name" -o json)
  jq -e --arg ns "$NS" --arg n "$snap" '.spec.deletionPolicy=="Retain" and .spec.driver=="disk.csi.azure.com" and .spec.volumeSnapshotRef.namespace==$ns and .spec.volumeSnapshotRef.name==$n and ((.status.snapshotHandle // .spec.source.snapshotHandle)|type=="string" and length>0)' <<<"$content" >/dev/null || die "$kind snapshot content is not retained Azure Disk content."
  jq -e '(.metadata.ownerReferences // [])|length==0' <<<"$vs" >/dev/null || die "$kind VolumeSnapshot unexpectedly has an owner reference; preserving Cluster deletion could be unsafe."
  sid=$(jq -er '.status.snapshotHandle // .spec.source.snapshotHandle' <<<"$content")
  [[ "${sid,,}" == "/subscriptions/${SUB,,}/resourcegroups/${SNAPSHOT_RG,,}/providers/microsoft.compute/snapshots/"* ]] || die "$kind snapshot handle is outside the retained RG."
  pv_name=$(k get pvc "$pvc" -n "$NS" -o json | jq -er '.spec.volumeName')
  pv=$(k get pv "$pv_name" -o json); disk_id=$(jq -er '.spec.csi.volumeHandle' <<<"$pv")
  [[ "${disk_id,,}" == "/subscriptions/${SUB,,}/resourcegroups/${NODE_RG,,}/providers/microsoft.compute/disks/"* ]] || die "$kind source disk is outside this stack's node RG."
  azs=$(azc snapshot show --ids "$sid" -o json) || die "Cannot inspect $kind cold Azure snapshot."
  jq -e --arg id "${sid,,}" --arg disk "${disk_id,,}" --arg loc "${LOC,,}" --arg t "$TOKEN" '((.id|ascii_downcase)==$id) and .provisioningState=="Succeeded" and (.snapshotAccessState//"Available")=="Available" and ((.location|ascii_downcase)==$loc) and (.diskSizeGB//.diskSizeGb)==512 and (.sku.name=="Standard_LRS" or .sku.name=="Standard_ZRS") and .incremental==true and ((.creationData.sourceResourceId|ascii_downcase)==$disk) and .tags.reprotoken==$t and .tags.reproPurpose=="cnpg-pre-vacuum-snapshot"' <<<"$azs" >/dev/null || die "$kind cold Azure snapshot identity, source, readiness, or retention tag failed."
  printf '%s\n' "$azs" >"$SD/cold-${kind,,}-snapshot.json"
  item=$(jq -cn --arg kind "$kind" --arg vs "$snap" --arg content "$content_name" --arg id "$sid" --arg pvc "$pvc" --arg pv "$pv_name" --arg disk "$disk_id" --arg sku "$(jq -r .sku.name <<<"$azs")" '{type:$kind,volumeSnapshot:$vs,volumeSnapshotContent:$content,azureSnapshotId:$id,sourcePVC:$pvc,sourcePV:$pv,sourceDiskId:$disk,diskSizeGiB:512,azureSku:$sku,deletionPolicy:"Retain"}')
  PAIR=$(jq -cn --argjson pair "$PAIR" --argjson item "$item" '$pair+[$item]')
done
end=$((SECONDS+10800)); cluster='{}'
while ((SECONDS<end)); do
  cluster=$(k get cluster "$C" -n "$NS" -o json)
  [[ $(jq -r '.status.readyInstances // 0' <<<"$cluster") == 2 ]] && break
  sleep 10
done
[[ $(jq -r '.status.readyInstances // 0' <<<"$cluster") == 2 ]] || die "Cluster did not return to two ready instances after cold backup."
PRIMARY_POD_AFTER=$(k get pod "$C-1" -n "$NS" -o json)
PRIMARY_UID_AFTER=$(jq -er .metadata.uid <<<"$PRIMARY_POD_AFTER")
POSTMASTER_AFTER=$(k exec -n "$NS" -c postgres "$C-1" -- psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d repro -c 'SELECT pg_postmaster_start_time();')
[[ -n "$POSTMASTER_AFTER" ]] || die "Cannot record primary postmaster time after cold snapshot."
end=$((SECONDS+1800)); caught=false; primary_lsn=''
while ((SECONDS<end)); do
  primary_lsn=$(k exec -n "$NS" -c postgres "$C-1" -- psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d repro -c "SELECT pg_current_wal_flush_lsn();" 2>/dev/null || true)
  [[ -n "$primary_lsn" ]] || { sleep 5; continue; }
  caught=$(k exec -n "$NS" -c postgres "$C-2" -- psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d repro -c "SELECT pg_last_wal_replay_lsn()>='$primary_lsn'::pg_lsn;" 2>/dev/null || true)
  [[ "$caught" == t ]] && break
  sleep 5
done
[[ "$caught" == t ]] || die "Replica did not catch up after cold backup."

low_plan=$(k exec -n "$NS" -c postgres "$C-1" -- psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d repro -c 'SET enable_seqscan=off; EXPLAIN (FORMAT JSON) SELECT id FROM public.vacuum_target ORDER BY id ASC LIMIT 1;')
high_plan=$(k exec -n "$NS" -c postgres "$C-1" -- psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d repro -c 'SET enable_seqscan=off; EXPLAIN (FORMAT JSON) SELECT id FROM public.vacuum_target ORDER BY id DESC LIMIT 1;')
for plan in "$low_plan" "$high_plan"; do
  jq -e '([..|objects|select((."Node Type"?=="Index Scan" or ."Node Type"?=="Index Only Scan" or ."Node Type"?=="Bitmap Index Scan") and ."Index Name"?=="vacuum_target_id_idx")]|length)>0 and ([..|objects|select(."Node Type"?=="Seq Scan")]|length)==0' <<<"$plan" >/dev/null || die "Cold-snapshot tuple-bound query plan is not indexed."
done
sentinel_plan=$(jq -cn --argjson low "$low_plan" --argjson high "$high_plan" '{lowId:$low,highId:$high}')
fixture=$(k exec -n "$NS" -c postgres "$C-1" -- psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d repro -c "SET enable_seqscan=off; WITH bounds AS (SELECT (SELECT id FROM public.vacuum_target ORDER BY id ASC LIMIT 1) AS low_id,(SELECT id FROM public.vacuum_target ORDER BY id DESC LIMIT 1) AS high_id) SELECT json_build_object('systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),'inRecovery',pg_is_in_recovery(),'heapBytes',pg_relation_size('public.vacuum_target'),'payloadBytes',(SELECT octet_length(payload) FROM public.vacuum_target ORDER BY id ASC LIMIT 1),'payloadStoragePlain',(SELECT attstorage='p' FROM pg_attribute WHERE attrelid='public.vacuum_target'::regclass AND attname='payload'),'tableAutovacuumDisabled',(SELECT 'autovacuum_enabled=false'=ANY(reloptions) FROM pg_class WHERE oid='public.vacuum_target'::regclass),'hasIndex',to_regclass('public.vacuum_target_id_idx') IS NOT NULL,'lowestLiveId',low_id,'highestLiveId',high_id,'lowIdIsEven',low_id%2=0,'highIdIsEven',high_id%2=0,'lowLiveIdPresent',EXISTS(SELECT 1 FROM public.vacuum_target WHERE id=low_id),'lowAdjacentOddAbsent',NOT EXISTS(SELECT 1 FROM public.vacuum_target WHERE id=low_id+1),'highLiveIdPresent',EXISTS(SELECT 1 FROM public.vacuum_target WHERE id=high_id),'highAdjacentOddAbsent',NOT EXISTS(SELECT 1 FROM public.vacuum_target WHERE id=high_id+1),'statsLiveTuplesEstimate',(SELECT n_live_tup FROM pg_stat_user_tables WHERE relid='public.vacuum_target'::regclass),'statsDeadTuplesEstimate',(SELECT n_dead_tup FROM pg_stat_user_tables WHERE relid='public.vacuum_target'::regclass),'statsLastAnalyze',(SELECT last_analyze FROM pg_stat_user_tables WHERE relid='public.vacuum_target'::regclass))::text FROM bounds;")
fixture=$(jq --argjson expectedLive "$EXPECTED_LIVE" --argjson expectedDead "$EXPECTED_DEAD" --argjson plan "$sentinel_plan" '. + {expectedLiveTuples:$expectedLive,expectedDeadTuples:$expectedDead,tupleStatisticsNote:"pg_stat_user_tables live/dead values are advisory estimates and can be reset or stale after restart/restore; expected counts come from recorded fixture insert/delete operations",indexedSentinelPlan:$plan}' <<<"$fixture")
jq -e --arg s "$(jq -er .fixture.systemIdentifier "$MANIFEST")" --argjson target "$(jq -er .fixture.targetHeapBytes "$MANIFEST")" '.systemIdentifier==$s and .inRecovery==false and (.heapBytes >= ($target*0.99) and .heapBytes <= ($target*1.01)) and .payloadBytes==1536 and .payloadStoragePlain and .tableAutovacuumDisabled and .hasIndex and .expectedLiveTuples==.expectedDeadTuples and .lowIdIsEven and .highIdIsEven and .lowLiveIdPresent and .lowAdjacentOddAbsent and .highLiveIdPresent and .highAdjacentOddAbsent' <<<"$fixture" >/dev/null || die "Prepared 100-GiB fixture or indexed tuple sentinels changed while taking the cold snapshot."

COLD_UID=$(jq -er '.metadata.uid' <<<"$backup")
COLD_META=$(jq -cn --arg backup "$BK" --arg uid "$COLD_UID" --arg target "$C-1" --arg uidBefore "$PRIMARY_UID_BEFORE" --arg uidAfter "$PRIMARY_UID_AFTER" --arg pmBefore "$POSTMASTER_BEFORE" --arg pmAfter "$POSTMASTER_AFTER" --argjson pair "$PAIR" --arg started "$(jq -r '.status.startedAt // empty' <<<"$backup")" --arg stopped "$(jq -r '.status.stoppedAt // empty' <<<"$backup")" --arg created "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson fixture "$fixture" --argjson restarted "$([[ "$POSTMASTER_BEFORE" != "$POSTMASTER_AFTER" ]] && echo true || echo false)" '{name:$backup,uid:$uid,method:"volumeSnapshot",online:false,target:"primary",sourceInstance:$target,phase:"completed",startedAt:$started,stoppedAt:$stopped,primaryPodUidBefore:$uidBefore,primaryPodUidAfter:$uidAfter,primaryPostmasterStartTimeBefore:$pmBefore,primaryPostmasterStartTimeAfter:$pmAfter,primaryPostmasterRestarted:$restarted,cacheEffect:(if $restarted then "Primary PostgreSQL postmaster restarted during the cold checkpoint; shared-buffer cache started cold before measurement." else "Primary was fenced for the cold checkpoint, but no postmaster restart timestamp change was observed." end),fixtureAtSnapshot:$fixture,snapshotOwnerReference:"none",snapshotPair:$pair,restoreValidation:{status:"pending-after-measurement",archiveConfigRemoved:true}}')
printf '%s\n' "$COLD_META" | jq . >"$SD/cold-snapshot.json.tmp"; chmod 600 "$SD/cold-snapshot.json.tmp"; mv -f "$SD/cold-snapshot.json.tmp" "$SD/cold-snapshot.json"
jq --argjson cold "$COLD_META" '.coldPreVacuumSnapshotPair=$cold.snapshotPair | .coldBackup={name:$cold.name,uid:$cold.uid,method:$cold.method,online:$cold.online,target:$cold.target,sourceInstance:$cold.sourceInstance,phase:$cold.phase,startedAt:$cold.startedAt,stoppedAt:$cold.stoppedAt,snapshotOwnerReference:$cold.snapshotOwnerReference,primaryPodUidBefore:$cold.primaryPodUidBefore,primaryPodUidAfter:$cold.primaryPodUidAfter,primaryPostmasterStartTimeBefore:$cold.primaryPostmasterStartTimeBefore,primaryPostmasterStartTimeAfter:$cold.primaryPostmasterStartTimeAfter,primaryPostmasterRestarted:$cold.primaryPostmasterRestarted,cacheEffect:$cold.cacheEffect,fixtureAtSnapshot:$cold.fixtureAtSnapshot} | .cacheEffect=$cold.cacheEffect' "$MANIFEST" >"$MANIFEST.tmp"
chmod 600 "$MANIFEST.tmp"; mv -f "$MANIFEST.tmp" "$MANIFEST"
jq --argjson cold "$COLD_META" '.coldBackup=$cold | .coldSnapshotPair=$cold.snapshotPair' "$PREP" >"$PREP.tmp"
chmod 600 "$PREP.tmp"; mv -f "$PREP.tmp" "$PREP"
echo "Verified cold pre-VACUUM 100-GiB DATA/WAL snapshot pair in $SNAPSHOT_RG; restore check is scheduled after measurement. State: $SD/cold-snapshot.json"
