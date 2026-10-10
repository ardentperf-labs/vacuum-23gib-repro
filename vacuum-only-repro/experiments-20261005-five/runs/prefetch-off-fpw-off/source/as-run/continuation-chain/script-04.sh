#!/usr/bin/env bash
# Resume only after the initial exact DATA/WAL snapshot restore succeeded.
# 02-prepare.sh stopped before any tuning because its jq DREF/WREF commands
# omitted their captured PVC JSON stdin. This continuation re-verifies every
# source/restore gate before it changes PostgreSQL or starts a backup/replica.
set -Eeuo pipefail
umask 077
ROOT="$(cd -- "$(dirname -- "$0")/.." && pwd)"
SD="$ROOT/.state"; STATE="$SD/state.json"; PAIR="$SD/source-pair.json"
NS=repro; C=hdd-pg; SECRET=azure-barman-credentials
SYSIMAGE='ghcr.io/cloudnative-pg/postgresql:16.15-202609140815-system-trixie@sha256:a30ea813ae1c82fc94743c4e71766688158b58d8ddf1f4e20ff5d3edb569a215'
IMAGE='ghcr.io/cloudnative-pg/postgresql:16.15-202609281634-standard-trixie@sha256:1dd42563007500b94b943f2a1620f724efedfc9eea2d07a50c6db4582c2431fd'
ROLE_ID='7efff54f-a5b4-42b5-a1c5-5411624893ce'
SCRIPT_PATH="${BASH_SOURCE[0]}"; SCRIPT_BASENAME="$(basename -- "$SCRIPT_PATH")"
EXEC_LOG="$SD/continuation-execution-v2.jsonl"; CURRENT_STEP=initial_checks; FAILURE_REASON=''; EXEC_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
record_step(){ local status="$1" detail="$2" rc="${3:-0}"; jq -cn --arg id "$EXEC_ID" --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg step "$CURRENT_STEP" --arg status "$status" --arg detail "$detail" --argjson rc "$rc" '{executionId:$id,timeUtc:$at,step:$step,status:$status,detail:$detail,exitCode:$rc}' >>"$EXEC_LOG"; chmod 600 "$EXEC_LOG"; }
phase(){ CURRENT_STEP="$1"; record_step "entered" "" 0; }
finish(){ local rc=$?; trap - EXIT; if ((rc==0)); then record_step "completed" "" 0; else record_step "failed" "${FAILURE_REASON:-unexpected command exit during ${CURRENT_STEP}}" "$rc"; fi; exit "$rc"; }
trap finish EXIT
die(){ FAILURE_REASON="$*"; echo "ERROR: $*" >&2; exit 1; }
phase "initial_revalidation"
for x in az kubectl jq timeout flock; do command -v "$x" >/dev/null || die "Missing $x"; done
jq -e '.data.id and .wal.id and .data.uniqueId and .wal.uniqueId and .systemIdentifier=="7692518806614212629" and .fixture.heapBytes==24696061952 and .sourceResourceGroupAdopted==false' "$PAIR" >/dev/null || die 'Bad source pair metadata.'
jq -e '.version==1 and .status=="ready"' "$STATE" >/dev/null || die 'Owned stack state is not ready.'
SUB=$(jq -er .subscriptionId "$STATE"); RG=$(jq -er .resourceGroup "$STATE"); AKS=$(jq -er .clusterName "$STATE"); NODE_RG=$(jq -er .nodeResourceGroup "$STATE")
LOC=$(jq -er .location "$STATE"); TOKEN=$(jq -er .ownerToken "$STATE"); SHORT=$(cut -c1-8 <<<"$TOKEN")
[[ "$LOC" == westus3 ]] || die 'Source pair and owned stack must both be West US 3.'
KCFG=$(jq -r '.kubeconfigPath // empty' "$STATE"); [[ -n "$KCFG" && -f "$KCFG" ]] || die 'Isolated kubeconfig missing.'
export KUBECONFIG="$KCFG"
exec 9>"$SD/${SCRIPT_BASENAME}.lock"; flock -n 9 || die 'Continuation v2 already active.'
k(){ timeout --foreground --kill-after=10s 180s kubectl --kubeconfig "$KCFG" "$@"; }
azc(){ timeout --foreground --kill-after=15s 240s az "$@" --subscription "$SUB"; }
azlong(){ timeout --foreground --kill-after=15s 1800s az "$@" --subscription "$SUB"; }
sql(){ k exec -n "$NS" -c postgres "$1" -- psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d repro -c "$2"; }
auto_alter(){ local pod="$1" statement="$2"; k exec -n "$NS" -c postgres "$pod" -- sh -eu -c 'd=$(printenv PGDATA); [ -n "$d" ] || d=/var/lib/postgresql/data/pgdata; f=$d/postgresql.auto.conf; test -f "$f" && test ! -L "$f"; chmod u+w "$f"; trap '\''chmod 400 "$f"'\'' EXIT; psql -X -v ON_ERROR_STOP=1 -U postgres -d repro -c "$1" >/dev/null; psql -X -v ON_ERROR_STOP=1 -U postgres -d repro -c "SELECT pg_reload_conf()" >/dev/null' sh "$statement"; }
wait_cluster(){ local want="$1" end=$((SECONDS+7200)) c p; while ((SECONDS<end)); do c=$(k get cluster "$C" -n "$NS" -o json 2>/dev/null || true); if [[ -n "$c" ]] && jq -e --argjson n "$want" '.status.readyInstances==$n and .status.phase=="Cluster in healthy state"' <<<"$c" >/dev/null; then p=$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json); if jq -e --argjson n "$want" '[.items[]|select(.status.phase=="Running" and any(.status.containerStatuses[]?;.name=="postgres" and .ready))]|length==$n' <<<"$p" >/dev/null; then printf '%s' "$c"; return; fi; fi; sleep 5; done; die "Cluster did not become healthy with $want instances."; }
wait_replay(){ local target="$1" end=$((SECONDS+3600)) x; while ((SECONDS<end)); do x=$(sql "$C-2" "SELECT pg_last_wal_replay_lsn()>='$target'::pg_lsn;" 2>/dev/null || true); [[ "$x" == t ]] && return; sleep 5; done; die "Replica did not replay through $target."; }
progress_sql="SELECT json_build_object('systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),'inRecovery',pg_is_in_recovery(),'firstLiveId',(SELECT id FROM public.vacuum_target ORDER BY id LIMIT 1),'firstLiveIdEven',((SELECT id FROM public.vacuum_target ORDER BY id LIMIT 1)%2=0),'adjacentOddIdAbsent',NOT EXISTS(SELECT 1 FROM public.vacuum_target WHERE id=(SELECT id+1 FROM public.vacuum_target ORDER BY id LIMIT 1)),'payloadBytes',(SELECT octet_length(payload) FROM public.vacuum_target ORDER BY id LIMIT 1),'indexName',to_regclass('public.vacuum_target_id_idx')::text,'heapBytes',pg_relation_size('public.vacuum_target'),'totalBytes',pg_total_relation_size('public.vacuum_target'),'liveTuplesEstimate',(SELECT n_live_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),'deadTuplesEstimate',(SELECT n_dead_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),'tableAutovacuumDisabled',(SELECT 'autovacuum_enabled=false'=ANY(reloptions) FROM pg_class WHERE oid='public.vacuum_target'::regclass),'walLevel',current_setting('wal_level'),'fullPageWrites',current_setting('full_page_writes'),'fullPageWritesSource',(SELECT source FROM pg_settings WHERE name='full_page_writes'),'fullPageWritesSourceFile',(SELECT sourcefile FROM pg_settings WHERE name='full_page_writes'),'fullPageWritesPendingRestart',(SELECT pending_restart FROM pg_settings WHERE name='full_page_writes'),'recoveryPrefetch',current_setting('recovery_prefetch'),'recoveryPrefetchSource',(SELECT source FROM pg_settings WHERE name='recovery_prefetch'),'recoveryPrefetchSourceFile',(SELECT sourcefile FROM pg_settings WHERE name='recovery_prefetch'),'recoveryPrefetchPendingRestart',(SELECT pending_restart FROM pg_settings WHERE name='recovery_prefetch'),'walLogHints',current_setting('wal_log_hints'),'dataPageChecksumVersion',(SELECT data_page_checksum_version FROM pg_control_init()),'vacuumCostDelay',current_setting('vacuum_cost_delay'),'sharedBuffers',current_setting('shared_buffers'),'maintenanceIoConcurrency',current_setting('maintenance_io_concurrency'),'checkpointTimeout',current_setting('checkpoint_timeout'),'maxWalSize',current_setting('max_wal_size'),'archiveMode',current_setting('archive_mode'),'trackIoTiming',current_setting('track_io_timing'),'trackWalIoTiming',current_setting('track_wal_io_timing'),'appSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend'))::text;"
cluster_default_guard(){ jq -e '(.metadata.annotations["kubectl.kubernetes.io/last-applied-configuration"]|fromjson|.spec.postgresql.parameters//{}) as $submitted | (.spec.postgresql.parameters//{}) as $live | (($submitted|has("full_page_writes")|not) and ($submitted|has("recovery_prefetch")|not) and (($live.full_page_writes//"on")=="on") and (($live.recovery_prefetch//"try")=="try"))' <<<"$1" >/dev/null; }
catalog_sql="SELECT json_build_object('indexDefinition',pg_get_indexdef('public.vacuum_target_id_idx'::regclass),'attributes',(SELECT json_agg(json_build_object('name',a.attname,'type',format_type(a.atttypid,a.atttypmod),'storage',a.attstorage,'compression',a.attcompression) ORDER BY a.attnum) FROM pg_attribute a WHERE a.attrelid='public.vacuum_target'::regclass AND a.attnum>0 AND NOT a.attisdropped),'payloadPlain',(SELECT attstorage='p' FROM pg_attribute WHERE attrelid='public.vacuum_target'::regclass AND attname='payload'))::text;"

# Ownership and unchanged-source verification. Never adopt the source RG or snapshots.
[[ $(azc account show --query id -o tsv) == "$SUB" ]] || die 'Azure subscription mismatch.'
jq -e --arg t "$TOKEN" '(.tags.reprotoken==$t)' < <(azc group show -n "$RG" -o json) >/dev/null || die 'Owned resource group token mismatch.'
SOURCE_RG=$(jq -er .sourceResourceGroup "$PAIR"); SOURCE_TOKEN=$(jq -er .sourceResourceGroupOwnerToken "$PAIR")
srg=$(azc group show -n "$SOURCE_RG" -o json) || die 'Retained source RG missing.'
jq -e --arg n "${SOURCE_RG,,}" --arg t "$SOURCE_TOKEN" '(.name|ascii_downcase)==$n and .tags.reprotoken==$t and .tags.reproPurpose=="experiment-snapshots"' <<<"$srg" >/dev/null || die 'Retained source RG ownership mismatch.'
DATA_ID=$(jq -er .data.id "$PAIR"); WAL_ID=$(jq -er .wal.id "$PAIR")
for role in data wal; do id=$(jq -er --arg r "$role" '.[$r].id' "$PAIR"); uid=$(jq -er --arg r "$role" '.[$r].uniqueId' "$PAIR"); src=$(jq -er --arg r "$role" '.[$r].sourceDiskId' "$PAIR"); snap=$(azc snapshot show --ids "$id" -o json) || die "Cannot inspect $role source snapshot."; jq -e --arg id "${id,,}" '.id|ascii_downcase==$id' <<<"$snap" >/dev/null || die "$role snapshot ID mismatch."; jq -e --arg uid "${uid,,}" --arg src "${src,,}" --arg rg "${SOURCE_RG,,}" '(.uniqueId|ascii_downcase)==$uid and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and (.location|ascii_downcase)=="westus3" and .incremental==true and .sku.name=="Standard_ZRS" and .diskSizeGB==512 and (.resourceGroup|ascii_downcase)==$rg and (.creationData.sourceResourceId|ascii_downcase)==$src and (.tags.source_volume_id|ascii_downcase)==$src' <<<"$snap" >/dev/null || die "$role source snapshot provenance changed."; locks=$(azc lock list --resource "$id" -o json); jq -e 'any(.[];.name=="preserve-all-snapshots" and .level=="CanNotDelete")' <<<"$locks" >/dev/null || die "$role source snapshot retention lock missing."; done
AKS_OBJ=$(azc aks show -g "$RG" -n "$AKS" -o json); PRINCIPAL=$(jq -er '.identity.principalId|select(type=="string" and length>0)' <<<"$AKS_OBJ")
ROLE_PROOF="$SD/external-source-snapshot-role-assignment.json"
jq -e --arg t "$TOKEN" --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" '.status as $st | ($st=="active" or $st=="removing_pair" or $st=="removed") and .stackOwnerToken==$t and .principalId==$p and (.roleDefinitionId|ascii_downcase)==$role and (.assignments|length)==2 and all(.assignments[];.createdByThisStack and ((.removed//false)==false or (.removed==true and .deleteExitCode==0 and .absenceConfirmed==true and .postRemovalRoleListAbsent==true)))' "$ROLE_PROOF" >/dev/null || die 'Temporary exact-scope source grant ledger is not owned/consistent.'
for role in data wal; do
  id=$(jq -er --arg r "$role" '.[$r].id' "$PAIR"); aid=$(jq -er --arg r "$role" '.assignments[]|select(.kind==$r)|.assignmentId' "$ROLE_PROOF"); actual=$(azc role assignment list --scope "$id" -o json); removed=$(jq -r --arg r "$role" '.assignments[]|select(.kind==$r)|.removed // false' "$ROLE_PROOF")
  if [[ "$removed" == true ]]; then jq -e --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "${id,,}" 'all(.[];.principalId!=$p or (.roleDefinitionId|ascii_downcase|endswith($role)|not) or (.scope|ascii_downcase)!=$scope)' <<<"$actual" >/dev/null || die "$role previously removed source grant unexpectedly reappeared."; else jq -e --arg aid "${aid,,}" --arg p "$PRINCIPAL" --arg scope "${id,,}" --arg role "${ROLE_ID,,}" 'any(.[];.principalId==$p and (.id|ascii_downcase)==$aid and (.scope|ascii_downcase)==$scope and (.roleDefinitionId|ascii_downcase|endswith($role)))' <<<"$actual" >/dev/null || die "Owned temporary $role role assignment does not match Azure."; fi
done

# Rebind exact source snapshot IDs to static CSI objects and live PVC dataSources.
VSD="src-data-$SHORT"; VSW="src-wal-$SHORT"; CD="src-data-content-$SHORT"; CW="src-wal-content-$SHORT"
for pair in "$VSD:$CD:$DATA_ID" "$VSW:$CW:$WAL_ID"; do IFS=: read -r vs cn sid <<<"$pair"; obj=$(k get volumesnapshot "$vs" -n "$NS" -o json); content=$(k get volumesnapshotcontent "$cn" -o json); jq -e --arg t "$TOKEN" --arg cn "$cn" '.metadata.labels["repro.stack.token"]==$t and .status.readyToUse==true and .status.boundVolumeSnapshotContentName==$cn and .spec.source.volumeSnapshotContentName==$cn' <<<"$obj" >/dev/null || die "$vs binding/ownership mismatch."; jq -e --arg t "$TOKEN" --arg sid "${sid,,}" --arg vs "$vs" --arg ns "$NS" '(.metadata.labels["repro.stack.token"]==$t) and .spec.deletionPolicy=="Retain" and (.spec.source.snapshotHandle|ascii_downcase)==$sid and (.status.snapshotHandle|ascii_downcase)==$sid and .spec.volumeSnapshotRef.name==$vs and .spec.volumeSnapshotRef.namespace==$ns' <<<"$content" >/dev/null || die "$cn exact retained content mismatch."; done
cluster=$(k get cluster "$C" -n "$NS" -o json)
manifest="$SD/source-pair-cluster-manifest.yaml"; expected_manifest_hash=$(awk '{print $1}' "$SD/source-pair-cluster-manifest.sha256"); actual_manifest_hash=$(sha256sum "$manifest"|awk '{print $1}')
[[ "$actual_manifest_hash" == "$expected_manifest_hash" ]] || die 'Pinned source-pair Cluster manifest changed.'
jq -e --arg t "$TOKEN" --arg image "$SYSIMAGE" --arg d "$DATA_ID" --arg w "$WAL_ID" '.metadata.labels["repro.stack.token"]==$t and .spec.instances==1 and .spec.imageName==$image and .status.currentPrimary=="hdd-pg-1" and .status.readyInstances==1 and .metadata.annotations["vacuum-only-repro/source-data-snapshot-id"]==$d and .metadata.annotations["vacuum-only-repro/source-wal-snapshot-id"]==$w' <<<"$cluster" >/dev/null || die 'Primary bootstrap cluster identity/image/source annotation mismatch.'
cluster_default_guard "$cluster" || die 'Submitted Cluster spec has a treatment override or live CNPG defaults differ.'
POD=$(k get pod "$C-1" -n "$NS" -o json); POD_UID=$(jq -er .metadata.uid <<<"$POD"); CLUSTER_UID=$(jq -er .metadata.uid <<<"$cluster")
jq -e --arg c "$C" --arg cu "$CLUSTER_UID" --arg image "$SYSIMAGE" '.metadata.labels["cnpg.io/cluster"]==$c and .metadata.labels["cnpg.io/instanceName"]=="hdd-pg-1" and any(.metadata.ownerReferences[]?;.kind=="Cluster" and .name==$c and .uid==$cu and .controller==true) and any(.spec.containers[];.name=="postgres" and .image==$image) and .status.phase=="Running" and any(.status.containerStatuses[]?;.name=="postgres" and .ready)' <<<"$POD" >/dev/null || die 'Primary pod owner/image/readiness identity mismatch.'
pvcs=$(k get pvc -n "$NS" -l "cnpg.io/cluster=$C" -o json)
DREF=$(jq -cer --arg n "$C-1" '.items[]|select(.metadata.name==$n and .status.phase=="Bound")|.spec.dataSource' <<<"$pvcs") || die 'Primary DATA PVC has no bound snapshot source.'
WREF=$(jq -cer --arg n "$C-1-wal" '.items[]|select(.metadata.name==$n and .status.phase=="Bound")|.spec.dataSource' <<<"$pvcs") || die 'Primary WAL PVC has no bound snapshot source.'
jq -e --arg vs "$VSD" '.kind=="VolumeSnapshot" and .name==$vs and .apiGroup=="snapshot.storage.k8s.io"' <<<"$DREF" >/dev/null || die 'Primary DATA PVC does not reference exact source VSS.'
jq -e --arg vs "$VSW" '.kind=="VolumeSnapshot" and .name==$vs and .apiGroup=="snapshot.storage.k8s.io"' <<<"$WREF" >/dev/null || die 'Primary WAL PVC does not reference exact source VSS.'

# Read-only source fixture/default proof before any config change or checkpoint.
PRE=$(sql "$C-1" "$progress_sql"); CAT=$(sql "$C-1" "$catalog_sql")
jq -e --arg s "$(jq -r .systemIdentifier "$PAIR")" --argjson h 24696061952 --argjson t 25040748544 '.systemIdentifier==$s and .inRecovery==false and .firstLiveId==4 and .firstLiveIdEven==true and .adjacentOddIdAbsent==true and .payloadBytes==1536 and .indexName=="vacuum_target_id_idx" and .heapBytes==$h and .totalBytes==$t and .tableAutovacuumDisabled==true and .walLevel=="logical" and .fullPageWrites=="on" and .fullPageWritesPendingRestart==false and .recoveryPrefetch=="try" and .recoveryPrefetchPendingRestart==false and .appSessions==0' <<<"$PRE" >/dev/null || die 'Restored source fixture/default settings did not verify before tuning.'
jq -e '.payloadPlain==true and .indexDefinition=="CREATE INDEX vacuum_target_id_idx ON public.vacuum_target USING btree (id)" and ([.attributes[]|select(.name=="payload" and .type=="bytea" and .storage=="p")]|length)==1' <<<"$CAT" >/dev/null || die 'Restored table catalog/index/payload storage mismatch.'
DATA_CONTENT=$(k get volumesnapshot "$VSD" -n "$NS" -o json | jq -er .status.boundVolumeSnapshotContentName); WAL_CONTENT=$(k get volumesnapshot "$VSW" -n "$NS" -o json | jq -er .status.boundVolumeSnapshotContentName)
jq -n --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg did "$DATA_ID" --arg duid "$(jq -r .data.uniqueId "$PAIR")" --arg dpvc "$(jq -r .data.sourcePvc "$PAIR")" --arg drpvc "$C-1" --arg dns "$NS" --arg dvs "$VSD" --arg dcn "$DATA_CONTENT" --argjson dref "$DREF" --arg wid "$WAL_ID" --arg wuid "$(jq -r .wal.uniqueId "$PAIR")" --arg wpvc "$(jq -r .wal.sourcePvc "$PAIR")" --arg wrpvc "$C-1-wal" --arg wns "$NS" --arg wvs "$VSW" --arg wcn "$WAL_CONTENT" --argjson wref "$WREF" --argjson fixture "$PRE" --argjson catalog "$CAT" --arg poduid "$POD_UID" --argjson grant "$(cat "$ROLE_PROOF")" --argjson cluster "$cluster" '{version:1,verifiedAtUtc:$at,verifiedBeforeTuning:true,bootstrapPodUid:$poduid,bootstrapEvidenceDirectory:".state/bootstrap-capture-20261006T051717Z",sourcePairRestore:{data:{snapshotId:$did,uniqueId:$duid,sourcePvc:$dpvc,restoredPvc:$drpvc,volumeSnapshotRef:{namespace:$dns,name:$dvs,contentName:$dcn},pvcDataSourceSnapshotRef:$dref},wal:{snapshotId:$wid,uniqueId:$wuid,sourcePvc:$wpvc,restoredPvc:$wrpvc,volumeSnapshotRef:{namespace:$wns,name:$wvs,contentName:$wcn},pvcDataSourceSnapshotRef:$wref},fixture:{verifiedBeforeTuning:true,sourceKind:"reused-prepared-paired-snapshot",systemIdentifier:$fixture.systemIdentifier,heapBytes:$fixture.heapBytes,tableBytes:$fixture.totalBytes,sentinels:{firstLiveId:$fixture.firstLiveId,firstLiveIdEven:$fixture.firstLiveIdEven,adjacentOddIdAbsent:$fixture.adjacentOddIdAbsent,payloadBytes:$fixture.payloadBytes,indexName:$fixture.indexName,tableAutovacuumDisabled:$fixture.tableAutovacuumDisabled},catalog:$catalog,primaryGucsBeforeTuning:{fullPageWrites:$fixture.fullPageWrites,recoveryPrefetch:$fixture.recoveryPrefetch,walLogHints:$fixture.walLogHints,dataPageChecksumVersion:$fixture.dataPageChecksumVersion,fullPageWritesPendingRestart:$fixture.fullPageWritesPendingRestart,recoveryPrefetchPendingRestart:$fixture.recoveryPrefetchPendingRestart,appSessions:$fixture.appSessions}}},sourceSnapshotRoleAssignments:$grant,clusterSpecParameters:{submitted:($cluster.metadata.annotations["kubectl.kubernetes.io/last-applied-configuration"]|fromjson|.spec.postgresql.parameters//{}),live:($cluster.spec.postgresql.parameters//{}),interpretation:"CNPG defaulted full_page_writes=on in the live CR; the pinned user manifest has neither treatment override. Effective role GUCs are verified from pg_settings and postgresql.auto.conf."}}' >"$SD/source-pair-restore-proof.json"
printf '%s\n' "$PRE" | jq . >"$SD/source-pair-pre-tuning-primary.json.tmp"; mv -f "$SD/source-pair-pre-tuning-primary.json.tmp" "$SD/source-pair-pre-tuning-primary.json"
printf '%s\n' "$CAT" | jq . >"$SD/source-pair-pre-tuning-catalog.json"; chmod 600 "$SD/source-pair-restore-proof.json" "$SD/source-pair-pre-tuning-primary.json" "$SD/source-pair-pre-tuning-catalog.json"
phase "source_pair_restore_proved_before_tuning"

# Seed snapshots must be incrementally retained in this token-owned RG and tagged.
phase "before_owned_seed_snapshotclass_verification"
SNAPSHOT_RG="rg-vacuum-snapshots-$TOKEN"; SNAPSHOT_RG_ID="/subscriptions/$SUB/resourceGroups/$SNAPSHOT_RG"; BSC="seed-snapshot-$SHORT"; BK="fpw-off-seed-$SHORT"
class_obj=$(k get volumesnapshotclass "$BSC" -o json) || die 'Owned seed VolumeSnapshotClass is missing.'
class_expected=$(jq -e --arg t "$TOKEN" --arg rg "$SNAPSHOT_RG" '.metadata.labels["repro.stack.token"]==$t and .driver=="disk.csi.azure.com" and .deletionPolicy=="Retain" and .parameters.resourceGroup==$rg and .parameters.incremental=="true" and .parameters.tags==("reprotoken="+$t+",reproPurpose=fpw-off-seed-paired-replica")' <<<"$class_obj" >/dev/null 2>&1 && echo yes || echo no)
if [[ "$class_expected" != yes ]]; then
  jq -e --arg t "$TOKEN" --arg rg "$SNAPSHOT_RG" '.metadata.labels["repro.stack.token"]==$t and .driver=="disk.csi.azure.com" and .deletionPolicy=="Retain" and .parameters.resourceGroup==$rg' <<<"$class_obj" >/dev/null || die 'Refusing to replace a non-owned or mis-scoped snapshot class.'
  existing_vss=$(k get volumesnapshots -n "$NS" -o json)
  jq -e --arg b "$BSC" '[.items[]|select(.spec.volumeSnapshotClassName==$b)]|length==0' <<<"$existing_vss" >/dev/null || die 'Refusing to replace the seed class while snapshots use it.'
  if k get backup "$BK" -n "$NS" -o json >/dev/null 2>&1; then die 'Seed Backup already exists; refusing class replacement.'; fi
  k delete volumesnapshotclass "$BSC" --wait=true >/dev/null
  cat >"$SD/seed-snapshotclass.yaml" <<EOF
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata: {name: $BSC, labels: {repro.stack.token: "$TOKEN"}}
driver: disk.csi.azure.com
deletionPolicy: Retain
parameters:
  resourceGroup: $SNAPSHOT_RG
  incremental: "true"
  tags: "reprotoken=$TOKEN,reproPurpose=fpw-off-seed-paired-replica"
EOF
  chmod 600 "$SD/seed-snapshotclass.yaml"; k apply -f "$SD/seed-snapshotclass.yaml" >/dev/null
  class_obj=$(k get volumesnapshotclass "$BSC" -o json)
  jq -e --arg t "$TOKEN" --arg rg "$SNAPSHOT_RG" '.metadata.labels["repro.stack.token"]==$t and .driver=="disk.csi.azure.com" and .deletionPolicy=="Retain" and .parameters.resourceGroup==$rg and .parameters.incremental=="true" and .parameters.tags==("reprotoken="+$t+",reproPurpose=fpw-off-seed-paired-replica")' <<<"$class_obj" >/dev/null || die 'Recreated seed snapshot class failed ownership/tag checks.'
fi
printf '%s\n' "$class_obj" | jq . >"$SD/seed-snapshotclass.json"; chmod 600 "$SD/seed-snapshotclass.json"
phase "owned_seed_snapshotclass_verified"

# Confirm immediately preceding source state; then remove only the two grants this stack created.
phase "before_exact_source_grant_removal"
LOCK_HELPER="$SD/remove-source-snapshot-role-grants-with-lock-restore-v2.sh"
EXPECTED_LOCK_HELPER_SHA="44b6880723afc47abd542cb4718f4c6d0dcb67533e997f5a078cf6e8d64d5f09"
ACTUAL_LOCK_HELPER_SHA=$(sha256sum "$LOCK_HELPER"|awk '{print $1}')
[[ "$ACTUAL_LOCK_HELPER_SHA" == "$EXPECTED_LOCK_HELPER_SHA" ]] || die 'Pinned transactional source-role helper hash mismatch.'
phase "before_transactional_source_role_cleanup"
bash "$LOCK_HELPER"
ROLE_PROOF_REMOVED=$(cat "$ROLE_PROOF")
jq -e '.postRemovalConfirmed==true and .status=="removed" and all(.assignments[];.createdByThisStack and .removed and .postRemovalRoleListAbsent and .absenceConfirmed and .deleteExitCode==0 and .temporaryLockLifecycle.lockRestoreExitCode==0 and .temporaryLockLifecycle.restoredLock.name=="preserve-all-snapshots" and .temporaryLockLifecycle.restoredLock.level=="CanNotDelete" and .temporaryLockLifecycle.restoredLock.notes=="Preserve every snapshot until explicit user deletion authorization." and .temporaryLockLifecycle.snapshotAfter.id==.snapshotId and .temporaryLockLifecycle.snapshotAfter.uniqueId==.temporaryLockLifecycle.uniqueId and .temporaryLockLifecycle.snapshotAfter.provisioningState=="Succeeded" and .temporaryLockLifecycle.snapshotAfter.snapshotAccessState=="Available")' "$ROLE_PROOF" >/dev/null || die 'Source grant/lock lifecycle proof incomplete.'
jq --argjson lifecycle "$ROLE_PROOF_REMOVED" '.sourceSnapshotRoleAssignments.lifecycle=$lifecycle' "$SD/source-pair-restore-proof.json" >"$SD/source-pair-restore-proof.json.tmp"; mv -f "$SD/source-pair-restore-proof.json.tmp" "$SD/source-pair-restore-proof.json"
phase "source_pair_proved_and_source_grants_removed"

# Only now set primary FPW off and force a checkpoint before generating measured WAL.
phase "before_primary_fpw_off_checkpoint"
POD="$C-1"
WAL_BEFORE=$(sql "$POD" "SELECT json_build_object('walRecords',wal_records,'walFpi',wal_fpi,'walBytes',wal_bytes)::text FROM pg_stat_wal;")
auto_alter "$POD" "ALTER SYSTEM SET full_page_writes = 'off';"
fpw=$(sql "$POD" "SELECT json_build_object('value',setting,'source',source,'sourceFile',sourcefile,'pendingRestart',pending_restart)::text FROM pg_settings WHERE name='full_page_writes';")
jq -e '.value=="off" and (.sourceFile|type=="string" and endswith("/postgresql.auto.conf")) and (.pendingRestart|not)' <<<"$fpw" >/dev/null || die 'Primary FPW off did not become effective from local auto.conf.'
sql "$POD" 'CHECKPOINT;' >/dev/null
fpw2=$(sql "$POD" "SELECT json_build_object('value',setting,'sourceFile',sourcefile,'pendingRestart',pending_restart)::text FROM pg_settings WHERE name='full_page_writes';")
jq -e '.value=="off" and (.sourceFile|type=="string" and endswith("/postgresql.auto.conf")) and (.pendingRestart|not)' <<<"$fpw2" >/dev/null || die 'Primary FPW changed across checkpoint.'
WAL_AFTER=$(sql "$POD" "SELECT json_build_object('walRecords',wal_records,'walFpi',wal_fpi,'walBytes',wal_bytes)::text FROM pg_stat_wal;")
jq -n --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson before "$WAL_BEFORE" --argjson after "$WAL_AFTER" --argjson fpw "$fpw2" --argjson h "$PRE" '{capturedAtUtc:$at,primaryFullPageWritesAfterCheckpoint:$fpw,walLogHints:$h.walLogHints,dataPageChecksumVersion:$h.dataPageChecksumVersion,primaryWalCountersBeforeSetting:$before,primaryWalCountersAfterCheckpoint:$after,deltas:{walRecords:($after.walRecords-$before.walRecords),walFpi:($after.walFpi-$before.walFpi),walBytes:($after.walBytes-$before.walBytes)},interpretation:"WAL FPI counts are recorded; with wal_log_hints enabled they are not assumed to be solely ordinary full_page_writes images."}' >"$SD/fpw-off-checkpoint-counters.json"
phase "primary_fpw_off_checkpoint_verified"

# Confirm separate DATA/WAL mounts, archive one post-change WAL segment, then create paired seed.
k exec -n "$NS" -c postgres "$POD" -- sh -eu -c 'd=$(printenv PGDATA); test -L "$d/pg_wal"; test "$(readlink -f "$d/pg_wal")" = /var/lib/postgresql/wal/pg_wal; test "$(stat -Lc %d "$d")" != "$(stat -Lc %d "$d/pg_wal")"'
BEFORE_ARCHIVE=$(sql "$POD" 'SELECT archived_count FROM pg_stat_archiver;'); sql "$POD" 'SELECT pg_switch_wal();' >/dev/null
end=$((SECONDS+900)); archived='{}'; while ((SECONDS<end)); do archived=$(sql "$POD" "SELECT json_build_object('archived',archived_count,'failed',failed_count,'lastWal',last_archived_wal)::text FROM pg_stat_archiver;"); (( $(jq -r .archived <<<"$archived") > BEFORE_ARCHIVE )) && break; sleep 10; done
(( $(jq -r .archived <<<"$archived") > BEFORE_ARCHIVE )) && jq -e '.failed==0 and (.lastWal|length)>0' <<<"$archived" >/dev/null || die 'New WAL did not archive; refusing standby seed.'
SNAPSHOT_RG="rg-vacuum-snapshots-$TOKEN"; SNAPSHOT_RG_ID="/subscriptions/$SUB/resourceGroups/$SNAPSHOT_RG"; BK="fpw-off-seed-$SHORT"
jq -e --arg n "${SNAPSHOT_RG,,}" --arg t "$TOKEN" --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" '(.name|ascii_downcase)==$n and .tags.reprotoken==$t and .tags.reproPurpose=="experiment-snapshots"' < <(azc group show -n "$SNAPSHOT_RG" -o json) >/dev/null || die 'Owned seed snapshot RG does not verify.'
RET_ASSIGN=$(azc role assignment list --scope "$SNAPSHOT_RG_ID" -o json); RET_ASSIGN_ID=$(jq -er --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "${SNAPSHOT_RG_ID,,}" '[.[]|select(.principalId==$p and (.roleDefinitionId|ascii_downcase|endswith($role)) and (.scope|ascii_downcase)==$scope)|.id]|if length==1 then .[0] else error("assignment count") end' <<<"$RET_ASSIGN") || die 'Owned snapshot-RG role assignment is not unique.'
if k get backup "$BK" -n "$NS" -o json >/dev/null 2>&1; then die 'Seed Backup already exists; refusing duplicate mutation.'; fi
cat >"$SD/seed-backup.yaml" <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata: {name: $BK, namespace: $NS, labels: {repro.stack.token: "$TOKEN"}}
spec: {cluster: {name: $C}, method: volumeSnapshot, online: true, onlineConfiguration: {waitForArchive: true, immediateCheckpoint: false}}
EOF
chmod 600 "$SD/seed-backup.yaml"; k apply -f "$SD/seed-backup.yaml" >/dev/null
end=$((SECONDS+3600)); backup='{}'; while ((SECONDS<end)); do backup=$(k get backup "$BK" -n "$NS" -o json); phase=$(jq -r '.status.phase // empty' <<<"$backup"); [[ "$phase" == completed ]] && break; [[ "$phase" != failed && "$phase" != walArchivingFailing ]] || die "CNPG paired seed backup failed: $phase"; sleep 10; done
jq -e '.status.phase=="completed" and ([.status.snapshotBackupStatus.elements[].type]|sort)==["PG_DATA","PG_WAL"]' <<<"$backup" >/dev/null || die 'Seed Backup did not complete with both DATA/WAL snapshots.'
BACKUP_UID=$(jq -er .metadata.uid <<<"$backup")
jq . <<<"$backup" >"$SD/seed-backup.json"; chmod 600 "$SD/seed-backup.json"
phase "paired_seed_backup_completed"
DATA_VS=$(jq -er '.status.snapshotBackupStatus.elements[]|select(.type=="PG_DATA")|.name' <<<"$backup"); WAL_VS=$(jq -er '.status.snapshotBackupStatus.elements[]|select(.type=="PG_WAL")|.name' <<<"$backup")
declare -A SEED_SNAPSHOTS=()
for item in "$DATA_VS:$C-1:data" "$WAL_VS:$C-1-wal:wal"; do
  IFS=: read -r vs pvc role <<<"$item"
  obj=$(k get volumesnapshot "$vs" -n "$NS" -o json)
  jq -e --arg p "$pvc" '.status.readyToUse==true and .spec.source.persistentVolumeClaimName==$p' <<<"$obj" >/dev/null || die "New $role seed snapshot source/ready check failed."
  jq -e --arg vs "$vs" --arg role "$role" 'any(.status.snapshotBackupStatus.elements[]; .name==$vs and .type==(if $role=="data" then "PG_DATA" else "PG_WAL" end))' <<<"$backup" >/dev/null || die "New $role VSS is not listed by the exact completed Backup."
  content=$(jq -er .status.boundVolumeSnapshotContentName <<<"$obj"); cnt=$(k get volumesnapshotcontent "$content" -o json)
  jq -e --arg vs "$vs" --arg ns "$NS" '.spec.volumeSnapshotRef.name==$vs and .spec.volumeSnapshotRef.namespace==$ns and .spec.deletionPolicy=="Retain" and .status.readyToUse==true' <<<"$cnt" >/dev/null || die "New $role VSC binding/retention check failed."
  handle=$(jq -er .status.snapshotHandle <<<"$cnt"); azsnap=$(azc snapshot show --ids "$handle" -o json) || die "Cannot inspect created $role seed snapshot."
  pvc_obj=$(k get pvc "$pvc" -n "$NS" -o json); pv=$(jq -er .spec.volumeName <<<"$pvc_obj"); source_disk=$(k get pv "$pv" -o json|jq -er .spec.csi.volumeHandle)
  jq -e --arg rg "${SNAPSHOT_RG,,}" --arg t "$TOKEN" --arg loc "${LOC,,}" --arg purpose fpw-off-seed-paired-replica --arg src "${source_disk,,}" '(.resourceGroup|ascii_downcase)==$rg and (.location|ascii_downcase)==$loc and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and .incremental==true and .diskSizeGB==512 and .sku.name=="Standard_ZRS" and .tags.reprotoken==$t and .tags.reproPurpose==$purpose and (.uniqueId|type=="string" and length>0) and (.creationData.sourceResourceId|ascii_downcase)==$src and (.tags.source_volume_id|ascii_downcase)==$src' <<<"$azsnap" >/dev/null || die "Created $role Azure snapshot ownership/source/retention metadata mismatch."
  jq --arg id "$(jq -er .id <<<"$azsnap")" --arg uid "$(jq -er .uniqueId <<<"$azsnap")" --arg pvc "$pvc" --arg vs "$vs" --arg content "$content" --arg handle "$handle" --arg sourceDisk "$source_disk" --argjson vss "$obj" --argjson vsc "$cnt" --argjson azure "$azsnap" -n '{snapshotId:$id,uniqueId:$uid,pvc:$pvc,volumeSnapshot:{name:$vs,object:$vss},volumeSnapshotContent:{name:$content,snapshotHandle:$handle,object:$vsc},sourceDiskId:$sourceDisk,azureSnapshot:$azure}' >"$SD/seed-$role-snapshot.json"
  SEED_SNAPSHOTS[$role]="$SD/seed-$role-snapshot.json"
done
jq -n --arg bk "$BK" --arg uid "$BACKUP_UID" --arg data "$(cat "${SEED_SNAPSHOTS[data]}")" --arg wal "$(cat "${SEED_SNAPSHOTS[wal]}")" '{version:1,backupName:$bk,backupUid:$uid,data:($data|fromjson),wal:($wal|fromjson)}' >"$SD/seed-paired-snapshot-proof.json"; chmod 600 "$SD/seed-paired-snapshot-proof.json"

# Launch the exact one-replica CNPG snapshot restore and preserve its full Job/event evidence before TTL.
phase "before_replica_snapshot_restore_patch"
jobs_before=$(k get jobs -n "$NS" -l "cnpg.io/cluster=$C" -o json | jq -c '[.items[].metadata.uid]|sort')
k patch cluster "$C" -n "$NS" --type=merge -p '{"spec":{"instances":2,"postgresql":{"synchronous":{"method":"any","number":1,"dataDurability":"preferred"}}}}' >/dev/null
JOB_UID=''; event=''; end=$((SECONDS+10800));
while ((SECONDS<end)); do
  jobs=$(k get jobs -n "$NS" -l "cnpg.io/cluster=$C" -o json)
  if [[ -z "$JOB_UID" ]]; then
    JOB=$(jq -c --arg i "$C-2" --argjson old "$jobs_before" '[.items[]|select(.metadata.labels["cnpg.io/instanceName"]==$i and .metadata.labels["cnpg.io/jobRole"]=="snapshot-recovery" and ((.metadata.uid as $u|$old|index($u))==null))]|first // empty' <<<"$jobs")
    if [[ -n "$JOB" ]]; then
      jq -e --arg i "$SYSIMAGE" 'any(.spec.template.spec.containers[];.name=="snapshot-recovery" and .image==$i)' <<<"$JOB" >/dev/null || die 'New snapshot-recovery Job uses unexpected pinned image.'
      JOB_UID=$(jq -r .metadata.uid <<<"$JOB")
      jq -n --arg observed "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson job "$JOB" '{observedAtUtc:$observed,uid:$job.metadata.uid,name:$job.metadata.name,namespace:$job.metadata.namespace,job:$job,spec:$job.spec,statusAtFirstObservation:$job.status}' >"$SD/snapshot-recovery-job.json"
      chmod 600 "$SD/snapshot-recovery-job.json"
      if (( $(jq -r '.status.failed // 0' <<<"$JOB") > 0 )); then
        job_name=$(jq -er .metadata.name <<<"$JOB"); k get pods -n "$NS" -l "job-name=$job_name" -o json >"$SD/snapshot-recovery-job-pods.json" || true
        pod_names=$(jq -r '.items[].metadata.name' "$SD/snapshot-recovery-job-pods.json" 2>/dev/null || true)
        for pod_name in $pod_names; do k logs -n "$NS" "$pod_name" --all-containers=true >"$SD/snapshot-recovery-failed-$pod_name.log" 2>&1 || true; done
        die 'Exact snapshot-recovery Job reported failure; full UID-bound evidence and logs preserved.'
      fi
    fi
  else
    current=$(jq -c --arg u "$JOB_UID" '[.items[]|select(.metadata.uid==$u)]|first // empty' <<<"$jobs")
    if [[ -n "$current" ]]; then jq --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson job "$current" '.lastObservedAtUtc=$at|.job=$job|.statusAtLastObservation=$job.status' "$SD/snapshot-recovery-job.json" >"$SD/snapshot-recovery-job.json.tmp"; mv -f "$SD/snapshot-recovery-job.json.tmp" "$SD/snapshot-recovery-job.json"; fi
    events=$(k get events -n "$NS" -o json)
    event=$(jq -c --arg u "$JOB_UID" '[.items[]|select(.involvedObject.uid==$u and .reason=="Completed")]|first // empty' <<<"$events")
    if [[ -n "$event" ]]; then jq --argjson e "$event" --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '.completedEvent=$e|.completedEventCapturedAtUtc=$at' "$SD/snapshot-recovery-job.json" >"$SD/snapshot-recovery-job.json.tmp"; mv -f "$SD/snapshot-recovery-job.json.tmp" "$SD/snapshot-recovery-job.json"; fi
  fi
  cluster=$(k get cluster "$C" -n "$NS" -o json); [[ -n "$JOB_UID" && $(jq -r '.status.readyInstances // 0' <<<"$cluster") == 2 ]] && break
  sleep 3
done
[[ -n "$JOB_UID" ]] || die 'Did not observe exact new snapshot-recovery Job UID.'
cluster=$(wait_cluster 2); [[ $(jq -r .status.currentPrimary <<<"$cluster") == "$C-1" ]] || die 'Primary role changed during replica seed.'
if [[ -z "$event" ]]; then end=$((SECONDS+180)); while ((SECONDS<end)); do event=$(k get events -n "$NS" -o json | jq -c --arg u "$JOB_UID" '[.items[]|select(.involvedObject.uid==$u and .reason=="Completed")]|first // empty'); if [[ -n "$event" ]]; then jq --argjson e "$event" --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '.completedEvent=$e|.completedEventCapturedAtUtc=$at' "$SD/snapshot-recovery-job.json" >"$SD/snapshot-recovery-job.json.tmp"; mv -f "$SD/snapshot-recovery-job.json.tmp" "$SD/snapshot-recovery-job.json"; break; fi; sleep 3; done; fi
[[ -n "$event" ]] || die 'No Completed event for exact snapshot-recovery Job UID.'
jq -e --arg u "$JOB_UID" '.uid==$u and .job.metadata.uid==$u and .completedEvent.involvedObject.uid==$u and .completedEvent.reason=="Completed"' "$SD/snapshot-recovery-job.json" >/dev/null || die 'Persisted seed Job/event UID binding is incomplete.'
pods=$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json); cluster=$(k get cluster "$C" -n "$NS" -o json); cluster_uid=$(jq -er .metadata.uid <<<"$cluster"); jq -e --arg c "$C" --arg cu "$cluster_uid" --arg i "$SYSIMAGE" '[.items[]|select(.status.phase=="Running" and .metadata.labels["cnpg.io/cluster"]==$c and any(.metadata.ownerReferences[]?;.kind=="Cluster" and .uid==$cu and .controller==true) and any(.spec.containers[];.name=="postgres" and .image==$i) and any(.status.containerStatuses[]?;.name=="postgres" and .ready))]|length==2' <<<"$pods" >/dev/null || die 'Expected two cluster-owned running PostgreSQL pods on the pinned seed image.'
pvcs=$(k get pvc -n "$NS" -l "cnpg.io/cluster=$C" -o json); jq -e --arg n "$C-2" --arg d "$DATA_VS" --arg w "$WAL_VS" '[.items[]|select((.metadata.name==$n and .status.phase=="Bound" and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$d) or (.metadata.name==($n+"-wal") and .status.phase=="Bound" and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$w))]|length==2' <<<"$pvcs" >/dev/null || die 'Standby DATA/WAL PVCs do not point to the exact new paired snapshots.'

# Remove archive writer/credentials only after the replica is seeded; hold stable pods and catch up.
phase "before_archive_writer_removal_and_image_rollout"
uids=$(jq -c '[.items[]|select(.status.phase=="Running")|.metadata.uid]|sort' <<<"$pods"); k patch cluster "$C" -n "$NS" --type=json -p='[{"op":"remove","path":"/spec/backup/barmanObjectStore"}]' >/dev/null
end=$((SECONDS+180)); while ((SECONDS<end)); do cluster=$(k get cluster "$C" -n "$NS" -o json); pods=$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json); [[ $(jq -r '.status.readyInstances // 0' <<<"$cluster") == 2 && $(jq -c '[.items[]|select(.status.phase=="Running")|.metadata.uid]|sort' <<<"$pods") == "$uids" ]] && break; sleep 5; done
jq -e '(.spec.backup.barmanObjectStore//null)==null' <<<"$cluster" >/dev/null && [[ $(jq -c '[.items[]|select(.status.phase=="Running")|.metadata.uid]|sort' <<<"$pods") == "$uids" ]] || die 'Archive writer removal restarted pods or was not effective.'
cache(){ local pod="$1" port=$((40000+RANDOM%20000)) pid code=000 i; k port-forward -n "$NS" "pod/$pod" "$port:8010" >"$SD/archive-cache-$pod.log" 2>&1 & pid=$!; for ((i=0;i<20;i++)); do code=$(timeout 5s curl -sS -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/cache/wal-archive" 2>/dev/null || true); [[ "$code" != 000 ]] && break; sleep 1; done; kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; echo "$code"; }
end=$((SECONDS+180)); cp=000; cr=000; while ((SECONDS<end)); do cp=$(cache "$C-1"); cr=$(cache "$C-2"); [[ "$cp" == 404 && "$cr" == 404 ]] && break; [[ "$cp" == 200 || "$cp" == 404 ]] && [[ "$cr" == 200 || "$cr" == 404 ]] || die 'Unexpected archive cache status.'; sleep 5; done
[[ "$cp" == 404 && "$cr" == 404 ]] || die 'Archive cache did not clear before measurement image rollout.'
k delete secret "$SECRET" -n "$NS" --wait=true >/dev/null
k patch cluster "$C" -n "$NS" --type=merge -p "$(jq -cn --arg i "$IMAGE" '{spec:{imageName:$i,primaryUpdateMethod:"restart"}}')" >/dev/null
digest=${IMAGE##*@}; end=$((SECONDS+10800))
while ((SECONDS<end)); do cluster=$(k get cluster "$C" -n "$NS" -o json); pods=$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json); if [[ $(jq -r '.status.readyInstances // 0' <<<"$cluster") == 2 ]] && jq -e --arg i "$IMAGE" --arg d "$digest" '[.items[]|select(.status.phase=="Running")|select(any(.spec.containers[];.name=="postgres" and .image==$i) and any(.status.containerStatuses[]?;.name=="postgres" and .ready and (.imageID|endswith($d))))]|length==2' <<<"$pods" >/dev/null; then break; fi; [[ $(jq -r '.status.currentPrimary // empty' <<<"$cluster") == "$C-1" ]] || die 'Primary changed during restart-only image rollout.'; sleep 10; done
((SECONDS<end)) || die 'Instances did not reach pinned measurement image.'
jq -e --arg i "$IMAGE" --arg c "$C" '.spec.imageName==$i and .spec.primaryUpdateMethod=="restart" and .status.readyInstances==2 and .status.currentPrimary==($c+"-1") and .spec.backup.barmanObjectStore==null' <<<"$cluster" >/dev/null || die 'Measurement image/primary/archive Cluster check failed.'
pods=$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json); cluster_uid=$(jq -er .metadata.uid <<<"$cluster")
jq -e --arg c "$C" --arg cu "$cluster_uid" --arg image "$IMAGE" --arg digest "$digest" '[.items[]|select(.status.phase=="Running" and .metadata.labels["cnpg.io/cluster"]==$c and any(.metadata.ownerReferences[]?;.kind=="Cluster" and .uid==$cu and .controller==true) and any(.spec.containers[];.name=="postgres" and .image==$image) and any(.status.containerStatuses[]?;.name=="postgres" and .ready and (.imageID|endswith($digest))))]|length==2' <<<"$pods" >/dev/null || die 'Both actual PostgreSQL pods do not prove the pinned measurement image digest.'
PRIMARY_UID=$(k get pod "$C-1" -n "$NS" -o json | jq -er .metadata.uid); REPLICA_UID=$(k get pod "$C-2" -n "$NS" -o json | jq -er .metadata.uid)
phase "measurement_image_rollout_verified"
REQUIRED_LSN=$(sql "$C-1" 'SELECT pg_current_wal_flush_lsn();'); wait_replay "$REQUIRED_LSN"
phase "post_image_catchup_verified"
phase "before_final_role_local_guc_reapply_and_checkpoint"
WAL_BEFORE_FINAL_CHECKPOINT=$(sql "$C-1" "SELECT json_build_object('walRecords',wal_records,'walFpi',wal_fpi,'walBytes',wal_bytes)::text FROM pg_stat_wal;")
auto_alter "$C-1" "ALTER SYSTEM SET full_page_writes = 'off';"
auto_alter "$C-2" "ALTER SYSTEM SET full_page_writes = 'on';"; auto_alter "$C-2" "ALTER SYSTEM SET recovery_prefetch = 'off';"
sql "$C-1" 'CHECKPOINT;' >/dev/null
WAL_AFTER_FINAL_CHECKPOINT=$(sql "$C-1" "SELECT json_build_object('walRecords',wal_records,'walFpi',wal_fpi,'walBytes',wal_bytes)::text FROM pg_stat_wal;")
jq -n --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson before "$WAL_BEFORE_FINAL_CHECKPOINT" --argjson after "$WAL_AFTER_FINAL_CHECKPOINT" '{capturedAtUtc:$at,primaryFullPageWrites:"off",before:$before,after:$after,deltas:{walRecords:($after.walRecords-$before.walRecords),walFpi:($after.walFpi-$before.walFpi),walBytes:($after.walBytes-$before.walBytes)},interpretation:"Post-image-rollout checkpoint with primary full_page_writes=off; WAL FPI counts are not attributed solely to ordinary full_page_writes because wal_log_hints is enabled."}' >"$SD/final-fpw-off-checkpoint-counters.json"
for _ in 1 2 3; do sleep 5; cluster=$(k get cluster "$C" -n "$NS" -o json); jq -e '.status.readyInstances==2 and .status.currentPrimary=="hdd-pg-1"' <<<"$cluster" >/dev/null && cluster_default_guard "$cluster" || die 'Cluster role/defaulted treatment metadata changed unexpectedly.'; [[ $(k get pod "$C-1" -n "$NS" -o json|jq -er .metadata.uid) == "$PRIMARY_UID" && $(k get pod "$C-2" -n "$NS" -o json|jq -er .metadata.uid) == "$REPLICA_UID" ]] || die 'Pod restarted during local GUC setup.'; done
PRIMARY=$(sql "$C-1" "$progress_sql"); REPLICA=$(sql "$C-2" "$progress_sql")
jq -e --arg s "$(jq -r .systemIdentifier "$PAIR")" --argjson h 24696061952 --argjson t 25040748544 '.systemIdentifier==$s and .inRecovery==false and .heapBytes==$h and .totalBytes==$t and .tableAutovacuumDisabled==true and .fullPageWrites=="off" and .fullPageWritesPendingRestart==false and (.fullPageWritesSourceFile|endswith("/postgresql.auto.conf")) and .recoveryPrefetch=="try" and .recoveryPrefetchPendingRestart==false and .appSessions==0' <<<"$PRIMARY" >/dev/null || die 'Primary final GUC/fixture evidence mismatch.'
jq -e --arg s "$(jq -r .systemIdentifier "$PAIR")" --argjson h 24696061952 --argjson t 25040748544 '.systemIdentifier==$s and .inRecovery==true and .heapBytes==$h and .totalBytes==$t and .tableAutovacuumDisabled==true and .fullPageWrites=="on" and .fullPageWritesPendingRestart==false and (.fullPageWritesSourceFile|endswith("/postgresql.auto.conf")) and .recoveryPrefetch=="off" and .recoveryPrefetchPendingRestart==false and (.recoveryPrefetchSourceFile|endswith("/postgresql.auto.conf")) and .appSessions==0' <<<"$REPLICA" >/dev/null || die 'Standby final GUC/fixture evidence mismatch.'
FINAL_LSN=$(sql "$C-1" 'SELECT pg_current_wal_flush_lsn();'); wait_replay "$FINAL_LSN"
for claim in "$C-1" "$C-1-wal" "$C-2" "$C-2-wal"; do pvc=$(k get pvc "$claim" -n "$NS" -o json); jq -e '.status.phase=="Bound" and .spec.resources.requests.storage=="512Gi" and .spec.storageClassName=="azure-standard-hdd"' <<<"$pvc" >/dev/null || die "$claim PVC size/class mismatch."; pv=$(jq -er .spec.volumeName <<<"$pvc"); diskid=$(k get pv "$pv" -o json|jq -er .spec.csi.volumeHandle); disk=$(azc disk show --ids "$diskid" -o json) || die "Cannot inspect $claim disk."; jq -e --arg id "${diskid,,}" --arg rg "${NODE_RG,,}" --arg loc "${LOC,,}" '(.id|ascii_downcase)==$id and ((.id|ascii_downcase)|contains("/resourcegroups/"+$rg+"/providers/microsoft.compute/disks/")) and (.location|ascii_downcase)==$loc and .sku.name=="Standard_LRS" and .diskSizeGB==512 and .managedBy!=null' <<<"$disk" >/dev/null || die "$claim disk owner/SKU/size mismatch."; done

# Final preparation evidence is written only once all gates pass.
FINAL="$SD/prepare.json"
jq -n --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg token "$TOKEN" --arg loc "$LOC" --arg sub "$SUB" --arg rg "$RG" --arg aks "$AKS" --arg image "$IMAGE" --arg sysimage "$SYSIMAGE" --arg bk "$BK" --arg dvs "$DATA_VS" --arg wvs "$WAL_VS" --arg job "$JOB_UID" --argjson fixture "$PRE" --argjson seedPair "$(cat "$SD/seed-paired-snapshot-proof.json")" --argjson sourceProof "$(cat "$SD/source-pair-restore-proof.json")" --argjson roleProof "$(cat "$ROLE_PROOF")" --argjson primary "$PRIMARY" --argjson replica "$REPLICA" --arg catchup "$FINAL_LSN" --argjson archive "$archived" --arg snaprg "$SNAPSHOT_RG" --arg snaprgid "$SNAPSHOT_RG_ID" --arg retrole "$RET_ASSIGN_ID" --argjson cluster "$cluster" --argjson preparedLive "$(jq -r .fixture.preparedLiveTuplesEstimate "$PAIR")" --argjson preparedDead "$(jq -r .fixture.preparedDeadTuplesEstimate "$PAIR")" '{version:1,status:"complete",verifiedAtUtc:$at,ownerToken:$token,location:$loc,subscriptionId:$sub,resourceGroup:$rg,clusterName:$aks,cluster:"repro/hdd-pg",instances:2,sourceKind:"reused-prepared-paired-snapshot",fixtureTargetHeapGiB:23,fixtureTargetHeapBytes:24696061952,systemIdentifier:$fixture.systemIdentifier,heapBytes:$fixture.heapBytes,tableBytes:$fixture.totalBytes,preparedLiveTuplesEstimate:$preparedLive,preparedDeadTuplesEstimate:$preparedDead,fixture:{sourceKind:"reused-prepared-paired-snapshot",heapBytes:$fixture.heapBytes,tableBytes:$fixture.totalBytes,liveTuplesEstimate:$preparedLive,deadTuplesEstimate:$preparedDead,observedLiveTuplesEstimate:$fixture.liveTuplesEstimate,observedDeadTuplesEstimate:$fixture.deadTuplesEstimate,sourcePairSentinels:{firstLiveId:$fixture.firstLiveId,firstLiveIdEven:$fixture.firstLiveIdEven,adjacentOddIdAbsent:$fixture.adjacentOddIdAbsent,payloadBytes:$fixture.payloadBytes,indexName:$fixture.indexName,tableAutovacuumDisabled:$fixture.tableAutovacuumDisabled}},sourcePairRestore:$sourceProof.sourcePairRestore,sourceSnapshotRoleAssignments:$roleProof,replicaSeed:{backupName:$bk,backupUid:$seedPair.backupUid,dataVolumeSnapshot:$dvs,walVolumeSnapshot:$wvs,dataSnapshot:$seedPair.data,walSnapshot:$seedPair.wal,recoveryJobUid:$job,recoveryJobEvidence:".state/snapshot-recovery-job.json",systemImage:$sysimage,measurementImage:$image},clusterSpecTreatment:{submittedParameters:($cluster.metadata.annotations["kubectl.kubernetes.io/last-applied-configuration"]|fromjson|.spec.postgresql.parameters//{}),liveParameters:($cluster.spec.postgresql.parameters//{}),interpretation:"CNPG injected/defaulted FPW=on into the live spec; the pinned applied spec has neither FPW nor recovery_prefetch treatment overrides."},finalRoles:{primary:$primary,replica:$replica},requiredReplayLsn:$catchup,archiveEvidence:$archive,snapshotRetention:{resourceGroup:$snaprg,resourceGroupId:$snaprgid,roleAssignmentId:$retrole,roleAssignmentRemoved:false,allSnapshotsPreserved:true},clientCreated:false}' >"$FINAL.tmp"
chmod 600 "$FINAL.tmp"; mv -f "$FINAL.tmp" "$FINAL"
jq -n --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson source "$(cat "$SD/source-pair-restore-proof.json")" --argjson primary "$PRIMARY" --argjson replica "$REPLICA" '{verifiedAtUtc:$at,sourcePairRestore:$source.sourcePairRestore,finalRoles:{primary:$primary,replica:$replica}}' >"$SD/initial-roles.json"; chmod 600 "$SD/initial-roles.json"
PROVISION_PATH="$ROOT/01-provision.sh"
PROVISION_HASH=$(sha256sum "$PROVISION_PATH" | awk '{print $1}')
HELPER_HASH=$(sha256sum "$LOCK_HELPER" | awk '{print $1}')
GRANT_REMOVAL_HELPER_HASH=$(sha256sum "$SD/remove-source-snapshot-role-grants-with-lock-restore.sh" | awk '{print $1}')
SCRIPT_HASH=$(sha256sum "$SCRIPT_PATH" | awk '{print $1}')
FAILED_02_HASH=$(awk '{print $1}' "$SD/failed-prepare-source.sha256")
COMPLETED_EVENT_UID=$(jq -er '.completedEvent.involvedObject.uid' "$SD/snapshot-recovery-job.json")
jq -n \
  --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg provisionHash "$PROVISION_HASH" --arg grantRemovalHelperHash "$GRANT_REMOVAL_HELPER_HASH" --arg helperHash "$HELPER_HASH" --arg scriptHash "$SCRIPT_HASH" \
  --arg failed02Hash "$FAILED_02_HASH" --arg failedContinuationHash "38eab7574954981d3e152b93ae07a5e20d7ce72cd90770af9e2ffb5ff2561321" \
  --arg job "$JOB_UID" --arg jobName "$(jq -er .name "$SD/snapshot-recovery-job.json")" --arg eventUid "$COMPLETED_EVENT_UID" --arg systemId "$(jq -r .systemIdentifier "$PAIR")" \
  '{status:"completed",completedAtUtc:$at,scriptSources:[
    {path:"01-provision.sh",sha256:$provisionHash,role:"successful_stack_provision"},
    {path:".state/remove-source-snapshot-role-grants-with-lock-restore.sh",sha256:$grantRemovalHelperHash,role:"successful_narrow_source_grant_cleanup_with_lock_restoration"},
    {path:".state/remove-source-snapshot-role-grants-with-lock-restore-v2.sh",sha256:$helperHash,role:"successful_idempotent_exact_grant_lock_snapshot_postverification"},
    {path:".state/continue-after-source-pair-restore-v2.sh",sha256:$scriptHash,role:"successful_source_restore_continuation_and_replica_seed"}
  ],stages:[
    {name:"source_pair_restore_and_fixture_proof",status:"completed",exitCode:0,executionReceiptPath:".state/source-pair-restore-proof.json"},
    {name:"source_snapshot_grant_removal_and_lock_restoration",status:"completed",exitCode:0,executionReceiptPath:".state/source-snapshot-lock-role-removal.json"},
    {name:"backup_and_paired_replica_seed",status:"completed",exitCode:0,executionReceiptPath:".state/seed-paired-snapshot-proof.json"},
    {name:"exact_snapshot_recovery_job_completion",status:"completed",exitCode:0,executionReceiptPath:".state/snapshot-recovery-job.json"},
    {name:"final_role_defaults_fixture_and_replay_gate",status:"completed",exitCode:0,executionReceiptPath:".state/prepare.json"}
  ],historicalAttempts:[
    {name:"original_02_prepare",status:"failed_before_tuning_or_vacuum",sha256:$failed02Hash,sourceAvailable:false,executionReceiptPath:".state/prepare-failure.json",explanation:"Primary/WAL PVC source JSON was not passed to jq; restore had succeeded but the script stopped before any tuning or VACUUM."},
    {name:"first_post_restore_continuation",status:"failed_before_tuning_or_vacuum",sha256:$failedContinuationHash,sourceAvailable:false,executionReceiptPath:".state/continuation-launch.json",explanation:"Azure ScopeLocked prevented deletion of the exact DATA snapshot role assignment; no PostgreSQL tuning, seed, or VACUUM occurred."}
  ],finalSourcePairProof:{path:".state/source-pair-restore-proof.json",verifiedBeforeTuning:true,systemIdentifier:$systemId},replicaSeedProof:{path:".state/seed-paired-snapshot-proof.json",recoveryJobUid:$job,completedEventUid:$eventUid},snapshotRecoveryJobEvidence:{name:$jobName,uid:$job,completedEventUid:$eventUid,eventEvidencePath:".state/snapshot-recovery-job.json"},noVacuumLaunchedDuringPreparation:true,allSnapshotsPreserved:true}' \
  >"$SD/prepare-recovery-audit.json"
chmod 600 "$SD/prepare-recovery-audit.json"
echo "Prepared exact retained source pair; pre-tuning restore/catalog proof, primary full_page_writes=off and standby recovery_prefetch=off verified. Replica seed Job UID $JOB_UID; replay caught up through $FINAL_LSN. Evidence: $FINAL"
