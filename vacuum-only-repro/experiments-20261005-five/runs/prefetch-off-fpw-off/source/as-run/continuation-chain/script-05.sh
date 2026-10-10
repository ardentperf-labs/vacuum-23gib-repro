#!/usr/bin/env bash
# Restore exact retained DATA/WAL snapshots, verify fixture before tuning,
# apply primary-only FPW-off, then create a fresh paired replica seed.
set -Eeuo pipefail
umask 077
ROOT="$(cd -- "$(dirname -- "$0")" && pwd)"; SD="$ROOT/.state"; STATE="$SD/state.json"; PAIR="$SD/source-pair.json"
NS=repro; C=hdd-pg; SC=azure-standard-hdd; SECRET=azure-barman-credentials
SYSIMAGE='ghcr.io/cloudnative-pg/postgresql:16.15-202609140815-system-trixie@sha256:a30ea813ae1c82fc94743c4e71766688158b58d8ddf1f4e20ff5d3edb569a215'
IMAGE='ghcr.io/cloudnative-pg/postgresql:16.15-202609281634-standard-trixie@sha256:1dd42563007500b94b943f2a1620f724efedfc9eea2d07a50c6db4582c2431fd'
ROLE_ID='7efff54f-a5b4-42b5-a1c5-5411624893ce'
die(){ echo "ERROR: $*" >&2; exit 1; }
for x in az kubectl jq timeout flock curl; do command -v "$x" >/dev/null || die "Missing $x"; done
jq -e '.data.id and .wal.id and .data.uniqueId and .wal.uniqueId and .systemIdentifier=="7692518806614212629" and .fixture.heapBytes==24696061952 and .sourceResourceGroupAdopted==false' "$PAIR" >/dev/null || die 'Bad source pair metadata.'
jq -e '.version==1 and .status=="ready"' "$STATE" >/dev/null || die 'Run 01-provision.sh first.'
SUB=$(jq -er .subscriptionId "$STATE"); RG=$(jq -er .resourceGroup "$STATE"); AKS=$(jq -er .clusterName "$STATE"); NODE_RG=$(jq -er .nodeResourceGroup "$STATE")
LOC=$(jq -er .location "$STATE"); TOKEN=$(jq -er .ownerToken "$STATE"); SHORT=$(cut -c1-8 <<<"$TOKEN")
[[ "$LOC" == westus3 ]] || die 'Source pair and owned stack must both be West US 3.'
ST="$SD/storage.json"; jq -e --arg t "$TOKEN" --arg s "$SUB" --arg rg "$RG" '.status=="ready" and .ownerToken==$t and .subscriptionId==$s and .resourceGroup==$rg and .publicAccess==null' "$ST" >/dev/null || die 'Storage metadata ownership mismatch.'
ACCOUNT=$(jq -er .accountName "$ST"); AID=$(jq -er .accountId "$ST"); CONTAINER=$(jq -er .containerName "$ST")
[[ "${AID,,}" == "/subscriptions/${SUB,,}/resourcegroups/${RG,,}/providers/microsoft.storage/storageaccounts/${ACCOUNT,,}" ]] || die 'Archive account is outside this stack.'
mkdir -p "$SD"; chmod 700 "$SD"; TMP=$(mktemp -d "$SD/.prepare.XXXXXX"); chmod 700 "$TMP"
exec 9>"$SD/02-prepare.lock"; flock -n 9 || die 'Prepare already active.'
KCFG=$(jq -r '.kubeconfigPath // empty' "$STATE"); [[ -n "$KCFG" && -f "$KCFG" ]] || die 'Isolated kubeconfig missing.'; export KUBECONFIG="$KCFG"
finish(){ rc=$?; trap - EXIT; rm -rf -- "$TMP"; if ((rc)); then echo 'Prepare stopped; all snapshots and Azure resources were preserved. Inspect evidence before retry.' >&2; fi; exit "$rc"; }; trap finish EXIT
k(){ timeout --foreground --kill-after=10s 180s kubectl --kubeconfig "$KCFG" "$@"; }
azc(){ timeout --foreground --kill-after=15s 240s az "$@" --subscription "$SUB"; }
azlong(){ timeout --foreground --kill-after=15s 1800s az "$@" --subscription "$SUB"; }
sql(){ k exec -n "$NS" -c postgres "$1" -- psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d repro -c "$2"; }
save_bootstrap_diagnostics(){
 local out pod
 out="$SD/bootstrap-diagnostics-$(date -u +%Y%m%dT%H%M%SZ)"
 mkdir -m 700 "$out"
 k get cluster "$C" -n "$NS" -o json >"$out/cluster.json" 2>&1 || true
 k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json >"$out/pods.json" 2>&1 || true
 k get pvc -n "$NS" -l "cnpg.io/cluster=$C" -o json >"$out/pvcs.json" 2>&1 || true
 k get jobs -n "$NS" -l "cnpg.io/cluster=$C" -o json >"$out/jobs.json" 2>&1 || true
 k get events -n "$NS" --sort-by=.lastTimestamp >"$out/events.txt" 2>&1 || true
 for pod in $(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json 2>/dev/null | jq -r '.items[].metadata.name' || true); do
  k describe pod "$pod" -n "$NS" >"$out/$pod.describe.txt" 2>&1 || true
  k logs "$pod" -n "$NS" -c postgres --timestamps --since=30m >"$out/$pod.postgres.log" 2>&1 || true
  k logs "$pod" -n "$NS" -c postgres --previous --timestamps --since=30m >"$out/$pod.postgres-previous.log" 2>&1 || true
 done
 k logs deployment/cnpg-controller-manager -n cnpg-system --all-pods=true --prefix --since=30m >"$out/operator.log" 2>&1 || true
 chmod -R go-rwx "$out"
 echo "Bootstrap diagnostic evidence: $out" >&2
}
wait_cluster(){
 local want="$1" end=$((SECONDS+10800)) started=$SECONDS lastLogCheck=$((SECONDS-60)) c p logs
 while ((SECONDS<end)); do
  c=$(k get cluster "$C" -n "$NS" -o json 2>/dev/null || true)
  if [[ -n "$c" ]] && jq -e --argjson n "$want" '.status.readyInstances==$n and .status.phase=="Cluster in healthy state"' <<<"$c" >/dev/null; then
   p=$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json)
   if jq -e --argjson n "$want" '[.items[]|select(.status.phase=="Running" and any(.status.containerStatuses[]?;.name=="postgres" and .ready))]|length==$n' <<<"$p" >/dev/null; then printf '%s' "$c"; return; fi
  fi
  # Fail early on an explicit missing-WAL/bootstrap error, or on a persistent
  # failed primary, instead of hiding the root cause behind a three-hour wait.
  if [[ "$want" == 1 && $((SECONDS-started)) -ge 240 && $((SECONDS-lastLogCheck)) -ge 60 ]]; then
   lastLogCheck=$SECONDS
   p=$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json 2>/dev/null || true)
   logs=$(for name in $(jq -r '.items[].metadata.name' <<<"$p" 2>/dev/null || true); do k logs "$name" -n "$NS" -c postgres --timestamps --since=10m 2>/dev/null || true; k logs "$name" -n "$NS" -c postgres --previous --timestamps --since=10m 2>/dev/null || true; done)
   if grep -Eiq 'requested WAL segment .* (has already been removed|was not found)|could not restore from archive|could not restore file .* from archive|WAL file .* (not found|was not found)|Barman.*(missing|not found)|WAL ends before end of online backup|backup_label.*(missing|not found)|could not open file .*pg_wal|recovery ended before consistent state|could not find a valid checkpoint record' <<<"$logs"; then save_bootstrap_diagnostics; die 'Explicit source-pair bootstrap/WAL recovery error; preserved diagnostics.'; fi
   if jq -e '[.items[]|select(.status.phase=="Failed" or any(.status.containerStatuses[]?.state.waiting?;.reason=="CrashLoopBackOff" or .reason=="CreateContainerConfigError"))]|length>0' <<<"$p" >/dev/null; then save_bootstrap_diagnostics; die 'Primary bootstrap pod failed; preserved diagnostics.'; fi
  fi
  if [[ "$want" == 1 && $((SECONDS-started)) -ge 1800 ]]; then save_bootstrap_diagnostics; die 'Paired snapshot primary bootstrap exceeded 30 minutes; preserved diagnostics.'; fi
  sleep 5
 done
 if [[ "$want" == 1 ]]; then save_bootstrap_diagnostics; fi
 die "Cluster not healthy with $want instances."
}
wait_replay(){ target="$1"; end=$((SECONDS+3600)); while ((SECONDS<end)); do x=$(sql "$C-2" "SELECT pg_last_wal_replay_lsn()>='$target'::pg_lsn;" 2>/dev/null || true); [[ "$x" == t ]] && return; sleep 5; done; die "Replica did not replay through $target."; }
auto_alter(){ pod="$1"; shift; k exec -n "$NS" -c postgres "$pod" -- sh -eu -c 'd=$(printenv PGDATA); [ -n "$d" ] || d=/var/lib/postgresql/data/pgdata; f=$d/postgresql.auto.conf; test -f "$f" && test ! -L "$f"; chmod u+w "$f"; trap '\''chmod 400 "$f"'\'' EXIT; psql -X -v ON_ERROR_STOP=1 -U postgres -d repro -c "$1" >/dev/null; psql -X -v ON_ERROR_STOP=1 -U postgres -d repro -c "SELECT pg_reload_conf()" >/dev/null' sh "$*"; }
progress_sql="SELECT json_build_object('systemIdentifier',(SELECT system_identifier::text FROM pg_control_system()),'inRecovery',pg_is_in_recovery(),'firstLiveId',(SELECT id FROM public.vacuum_target ORDER BY id LIMIT 1),'firstLiveIdEven',((SELECT id FROM public.vacuum_target ORDER BY id LIMIT 1)%2=0),'adjacentOddIdAbsent',NOT EXISTS(SELECT 1 FROM public.vacuum_target WHERE id=(SELECT id+1 FROM public.vacuum_target ORDER BY id LIMIT 1)),'payloadBytes',(SELECT octet_length(payload) FROM public.vacuum_target ORDER BY id LIMIT 1),'indexName',to_regclass('public.vacuum_target_id_idx')::text,'heapBytes',pg_relation_size('public.vacuum_target'),'totalBytes',pg_total_relation_size('public.vacuum_target'),'liveTuplesEstimate',(SELECT n_live_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),'deadTuplesEstimate',(SELECT n_dead_tup FROM pg_stat_all_tables WHERE relid='public.vacuum_target'::regclass),'tableAutovacuumDisabled',(SELECT 'autovacuum_enabled=false'=ANY(reloptions) FROM pg_class WHERE oid='public.vacuum_target'::regclass),'walLevel',current_setting('wal_level'),'fullPageWrites',current_setting('full_page_writes'),'fullPageWritesSource',(SELECT source FROM pg_settings WHERE name='full_page_writes'),'fullPageWritesSourceFile',(SELECT sourcefile FROM pg_settings WHERE name='full_page_writes'),'fullPageWritesPendingRestart',(SELECT pending_restart FROM pg_settings WHERE name='full_page_writes'),'recoveryPrefetch',current_setting('recovery_prefetch'),'recoveryPrefetchSource',(SELECT source FROM pg_settings WHERE name='recovery_prefetch'),'recoveryPrefetchSourceFile',(SELECT sourcefile FROM pg_settings WHERE name='recovery_prefetch'),'recoveryPrefetchPendingRestart',(SELECT pending_restart FROM pg_settings WHERE name='recovery_prefetch'),'vacuumCostDelay',current_setting('vacuum_cost_delay'),'sharedBuffers',current_setting('shared_buffers'),'maintenanceIoConcurrency',current_setting('maintenance_io_concurrency'),'checkpointTimeout',current_setting('checkpoint_timeout'),'maxWalSize',current_setting('max_wal_size'),'archiveMode',current_setting('archive_mode'),'trackIoTiming',current_setting('track_io_timing'),'trackWalIoTiming',current_setting('track_wal_io_timing'),'appSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend'))::text;"
[[ $(azc account show --query id -o tsv) == "$SUB" ]] || die 'Azure subscription mismatch.'
jq -e --arg t "$TOKEN" '.tags.reprotoken==$t' < <(azc group show -n "$RG" -o json) >/dev/null || die 'Stack resource group ownership mismatch.'
jq -e --arg t "$TOKEN" '.tags.reprotoken==$t' < <(azc group show -n "$NODE_RG" -o json) >/dev/null || die 'Node resource group ownership mismatch.'
jq -e --arg t "$TOKEN" '.metadata.labels["repro.stack.token"]==$t' < <(k get namespace "$NS" -o json) >/dev/null || die 'Namespace ownership mismatch.'
jq -e --arg t "$TOKEN" '.metadata.labels["repro.stack.token"]==$t and (.data["connection-string"]|length>0)' < <(k get secret "$SECRET" -n "$NS" -o json) >/dev/null || die 'Archive Secret ownership mismatch.'
jq -e '.provisioner=="disk.csi.azure.com" and .parameters.skuName=="Standard_LRS" and .parameters.cachingMode=="None"' < <(k get storageclass "$SC" -o json) >/dev/null || die 'StorageClass does not match framework.'
for crd in volumesnapshots.snapshot.storage.k8s.io volumesnapshotcontents.snapshot.storage.k8s.io; do k get crd "$crd" >/dev/null || die "Missing $crd"; done

DATA_ID=$(jq -er .data.id "$PAIR"); WAL_ID=$(jq -er .wal.id "$PAIR"); SOURCE_RG=$(jq -er .sourceResourceGroup "$PAIR"); SOURCE_TOKEN=$(jq -er .sourceResourceGroupOwnerToken "$PAIR")
srg=$(azc group show -n "$SOURCE_RG" -o json) || die 'Source snapshot RG missing.'
jq -e --arg n "${SOURCE_RG,,}" --arg t "$SOURCE_TOKEN" '(.name|ascii_downcase)==$n and .tags.reprotoken==$t and .tags.reproPurpose=="experiment-snapshots"' <<<"$srg" >/dev/null || die 'Source snapshot RG ownership mismatch; not adopting it.'
for role in data wal; do id=$(jq -er --arg r "$role" '.[$r].id' "$PAIR"); uid=$(jq -er --arg r "$role" '.[$r].uniqueId' "$PAIR"); src=$(jq -er --arg r "$role" '.[$r].sourceDiskId' "$PAIR"); snap=$(azc snapshot show --ids "$id" -o json) || die "Cannot inspect $role snapshot."; jq -e --arg id "${id,,}" '.id|ascii_downcase==$id' <<<"$snap" >/dev/null || die "$role snapshot ID changed."; jq -e --arg uid "${uid,,}" --arg src "${src,,}" --arg rg "${SOURCE_RG,,}" '(.uniqueId|ascii_downcase)==$uid and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and (.location|ascii_downcase)=="westus3" and .incremental==true and .sku.name=="Standard_ZRS" and .diskSizeGB==512 and (.resourceGroup|ascii_downcase)==$rg and (.creationData.sourceResourceId|ascii_downcase)==$src and (.tags.source_volume_id|ascii_downcase)==$src' <<<"$snap" >/dev/null || die "$role snapshot provenance/state changed."; locks=$(azc lock list --resource "$id" -o json) || die "Cannot inspect $role snapshot locks."; jq -e 'any(.[]; .name=="preserve-all-snapshots" and .level=="CanNotDelete")' <<<"$locks" >/dev/null || die "$role snapshot lacks the required retention lock."; done

# Create narrow, temporary permissions only at the two exact source snapshots.
AKS_OBJ=$(azc aks show -g "$RG" -n "$AKS" -o json) || die 'Cannot inspect isolated AKS.'
PRINCIPAL=$(jq -er '.identity.principalId|select(type=="string" and length>0)' <<<"$AKS_OBJ") || die 'AKS control-plane identity missing.'
ROLE_NAME=$(azc role definition list --name "$ROLE_ID" --query '[0].roleName' -o tsv); [[ "$ROLE_NAME" == 'Disk Snapshot Contributor' ]] || die 'Role ID is not Disk Snapshot Contributor.'
ROLE_PROOF="$SD/external-source-snapshot-role-assignment.json"
jq -n --arg t "$TOKEN" --arg p "$PRINCIPAL" --arg role "$ROLE_ID" --arg d "$DATA_ID" --arg w "$WAL_ID" '{version:1,status:"creating",stackOwnerToken:$t,principalId:$p,principalType:"AKS control-plane managed identity",roleDefinitionId:$role,postRemovalConfirmed:false,assignments:[{kind:"data",snapshotId:$d,scope:$d,assignmentId:null,createdByThisStack:false,createdAtUtc:null,removedAtUtc:null,deleteExitCode:null,postRemovalRoleListAbsent:false},{kind:"wal",snapshotId:$w,scope:$w,assignmentId:null,createdByThisStack:false,createdAtUtc:null,removedAtUtc:null,deleteExitCode:null,postRemovalRoleListAbsent:false}]}' >"$ROLE_PROOF.tmp"; chmod 600 "$ROLE_PROOF.tmp"; mv -f "$ROLE_PROOF.tmp" "$ROLE_PROOF"
for role in data wal; do id=$(jq -er --arg r "$role" '.[$r].id' "$PAIR"); prior=$(azc role assignment list --scope "$id" -o json) || die 'Cannot inspect exact source snapshot RBAC scope.'; if jq -e --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "${id,,}" 'any(.[]; .principalId==$p and (.roleDefinitionId|ascii_downcase|endswith($role)) and (.scope|ascii_downcase)==$scope)' <<<"$prior" >/dev/null; then die "Unexpected pre-existing assignment at $role source snapshot."; fi; azlong role assignment create --assignee-object-id "$PRINCIPAL" --assignee-principal-type ServicePrincipal --role "$ROLE_ID" --scope "$id" -o none || { after=$(azc role assignment list --scope "$id" -o json) || die 'Assignment create state ambiguous; inspect role proof.'; jq -e --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "${id,,}" 'any(.[]; .principalId==$p and (.roleDefinitionId|ascii_downcase|endswith($role)) and (.scope|ascii_downcase)==$scope)' <<<"$after" >/dev/null || die "Exact role assignment did not materialize for $role."; }; after=$(azc role assignment list --scope "$id" -o json) || die 'Cannot verify source snapshot grant.'; aid=$(jq -er --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "${id,,}" '[.[]|select(.principalId==$p and (.roleDefinitionId|ascii_downcase|endswith($role)) and (.scope|ascii_downcase)==$scope)|.id]|if length==1 then .[0] else error("assignment count") end' <<<"$after") || die "Source snapshot grant not unique for $role."; jq --arg kind "$role" --arg id "$aid" --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '.assignments|=map(if .kind==$kind then .assignmentId=$id|.createdByThisStack=true|.createdAtUtc=$at else . end)' "$ROLE_PROOF" >"$ROLE_PROOF.tmp"; mv -f "$ROLE_PROOF.tmp" "$ROLE_PROOF"; done
jq '.status="active"|.activatedAtUtc=(now|todateiso8601)' "$ROLE_PROOF" >"$ROLE_PROOF.tmp"; mv -f "$ROLE_PROOF.tmp" "$ROLE_PROOF"

# Owned retention destination for only this stack's newly-created seed snapshots.
SNAPSHOT_RG="rg-vacuum-snapshots-$TOKEN"; SNAPSHOT_RG_ID="/subscriptions/$SUB/resourceGroups/$SNAPSHOT_RG"
if [[ $(azc group exists -n "$SNAPSHOT_RG" -o tsv) == true ]]; then sr=$(azc group show -n "$SNAPSHOT_RG" -o json); jq -e --arg n "${SNAPSHOT_RG,,}" --arg t "$TOKEN" --arg l "${LOC,,}" '(.name|ascii_downcase)==$n and (.location|ascii_downcase)==$l and .tags.reprotoken==$t and .tags["managed-by"]=="vacuum-only-repro" and .tags.reproPurpose=="experiment-snapshots"' <<<"$sr" >/dev/null || die 'Snapshot retention RG is not owned by this stack.'; else sr=$(azc group create -n "$SNAPSHOT_RG" -l "$LOC" --tags managed-by=vacuum-only-repro reproPurpose=experiment-snapshots "reprotoken=$TOKEN" -o json); jq -e --arg n "${SNAPSHOT_RG,,}" --arg t "$TOKEN" '(.name|ascii_downcase)==$n and .tags.reprotoken==$t' <<<"$sr" >/dev/null || die 'Created retention RG ownership did not verify.'; fi
RET_ASSIGN=$(azc role assignment list --scope "$SNAPSHOT_RG_ID" -o json) || die 'Cannot inspect stack snapshot-RG RBAC.'
if ! jq -e --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "${SNAPSHOT_RG_ID,,}" 'any(.[]; .principalId==$p and (.roleDefinitionId|ascii_downcase|endswith($role)) and (.scope|ascii_downcase)==$scope)' <<<"$RET_ASSIGN" >/dev/null; then azlong role assignment create --assignee-object-id "$PRINCIPAL" --assignee-principal-type ServicePrincipal --role "$ROLE_ID" --scope "$SNAPSHOT_RG_ID" -o none || die 'Cannot grant stack retention snapshot permission.'; RET_ASSIGN=$(azc role assignment list --scope "$SNAPSHOT_RG_ID" -o json) || die 'Cannot verify stack snapshot-RG grant.'; fi
RET_ASSIGN_ID=$(jq -er --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "${SNAPSHOT_RG_ID,,}" '[.[]|select(.principalId==$p and (.roleDefinitionId|ascii_downcase|endswith($role)) and (.scope|ascii_downcase)==$scope)|.id]|if length==1 then .[0] else error("assignment count") end' <<<"$RET_ASSIGN") || die 'Stack snapshot-RG assignment is not unique.'
state_tmp=$(mktemp "$SD/.state.XXXXXX"); jq --arg n "$SNAPSHOT_RG" --arg id "$SNAPSHOT_RG_ID" --arg t "$TOKEN" --arg p "$PRINCIPAL" --arg role "$ROLE_ID" --arg aid "$RET_ASSIGN_ID" '.snapshotRetentionResourceGroup={name:$n,id:$id,ownerToken:$t,purpose:"experiment-snapshots",aksPrincipalId:$p,roleDefinitionId:$role,roleAssignmentId:$aid,roleAssignmentRemoved:false}' "$STATE" >"$state_tmp"; chmod 600 "$state_tmp"; mv -f "$state_tmp" "$STATE"

VSC="src-pair-$SHORT"; VSD="src-data-$SHORT"; VSW="src-wal-$SHORT"; CD="src-data-content-$SHORT"; CW="src-wal-content-$SHORT"; BSC="seed-snapshot-$SHORT"; BK="fpw-off-seed-$SHORT"
for spec in "volumesnapshotclass:$VSC" "volumesnapshotclass:$BSC" "volumesnapshot:$VSD" "volumesnapshot:$VSW" "volumesnapshotcontent:$CD" "volumesnapshotcontent:$CW" "cluster:$C" "backup:$BK"; do kind=$(cut -d: -f1 <<<"$spec"); name=$(cut -d: -f2 <<<"$spec"); if k get "$kind" "$name" -n "$NS" -o json >/dev/null 2>&1; then die "$kind/$name already exists."; fi; done
cat >"$TMP/source-pair.yaml" <<EOF
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata: {name: $VSC, labels: {repro.stack.token: "$TOKEN"}}
driver: disk.csi.azure.com
deletionPolicy: Retain
parameters: {resourceGroup: $SOURCE_RG}
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotContent
metadata: {name: $CD, labels: {repro.stack.token: "$TOKEN"}}
spec: {deletionPolicy: Retain, driver: disk.csi.azure.com, source: {snapshotHandle: "$DATA_ID"}, volumeSnapshotClassName: $VSC, volumeSnapshotRef: {kind: VolumeSnapshot, name: $VSD, namespace: $NS}}
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata: {name: $VSD, namespace: $NS, labels: {repro.stack.token: "$TOKEN"}}
spec: {volumeSnapshotClassName: $VSC, source: {volumeSnapshotContentName: $CD}}
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotContent
metadata: {name: $CW, labels: {repro.stack.token: "$TOKEN"}}
spec: {deletionPolicy: Retain, driver: disk.csi.azure.com, source: {snapshotHandle: "$WAL_ID"}, volumeSnapshotClassName: $VSC, volumeSnapshotRef: {kind: VolumeSnapshot, name: $VSW, namespace: $NS}}
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata: {name: $VSW, namespace: $NS, labels: {repro.stack.token: "$TOKEN"}}
spec: {volumeSnapshotClassName: $VSC, source: {volumeSnapshotContentName: $CW}}
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata: {name: $BSC, labels: {repro.stack.token: "$TOKEN"}}
driver: disk.csi.azure.com
deletionPolicy: Retain
parameters: {resourceGroup: $SNAPSHOT_RG}
EOF
# The CSI driver must keep imported snapshots when their VolumeSnapshot objects are later removed.
# (The source objects themselves are retained by their Azure resource locks.)
k apply -f "$TMP/source-pair.yaml" >/dev/null
end=$((SECONDS+1800)); while ((SECONDS<end)); do d=$(k get volumesnapshot "$VSD" -n "$NS" -o json 2>/dev/null || true); w=$(k get volumesnapshot "$VSW" -n "$NS" -o json 2>/dev/null || true); if [[ -n "$d" && -n "$w" ]] && jq -e '.status.readyToUse==true' <<<"$d" >/dev/null && jq -e '.status.readyToUse==true' <<<"$w" >/dev/null; then break; fi; sleep 10; done; ((SECONDS<end)) || die 'Existing source VolumeSnapshots did not bind/ready.'
for pair in "$VSD:$CD:$DATA_ID" "$VSW:$CW:$WAL_ID"; do IFS=: read -r vs cn sid <<<"$pair"; obj=$(k get volumesnapshot "$vs" -n "$NS" -o json); content=$(k get volumesnapshotcontent "$cn" -o json); jq -e --arg cn "$cn" '.status.readyToUse==true and .spec.source.volumeSnapshotContentName==$cn' <<<"$obj" >/dev/null || die "$vs did not bind to exact content."; jq -e --arg sid "${sid,,}" --arg n "$vs" --arg ns "$NS" '.spec.deletionPolicy=="Retain" and (.spec.source.snapshotHandle|ascii_downcase)==$sid and .spec.volumeSnapshotRef.name==$n and .spec.volumeSnapshotRef.namespace==$ns and (.status.snapshotHandle|ascii_downcase)==$sid' <<<"$content" >/dev/null || die "$cn content handle/ref mismatch."; done
cat >"$TMP/cluster.yaml" <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: $C
  namespace: $NS
  labels: {repro.stack.token: "$TOKEN"}
  annotations:
    vacuum-only-repro/source-data-snapshot-id: "$DATA_ID"
    vacuum-only-repro/source-wal-snapshot-id: "$WAL_ID"
spec:
  instances: 1
  imageName: $SYSIMAGE
  storage: {storageClass: $SC, size: 512Gi}
  walStorage: {storageClass: $SC, size: 512Gi}
  bootstrap:
    recovery:
      database: repro
      owner: app
      volumeSnapshots:
        storage: {name: $VSD, kind: VolumeSnapshot, apiGroup: snapshot.storage.k8s.io}
        walStorage: {name: $VSW, kind: VolumeSnapshot, apiGroup: snapshot.storage.k8s.io}
  affinity: {enablePodAntiAffinity: true, podAntiAffinityType: required, topologyKey: kubernetes.io/hostname}
  resources: {requests: {cpu: "1", memory: 2Gi}, limits: {cpu: "2", memory: 3Gi}}
  postgresql:
    parameters:
      wal_level: logical
      shared_buffers: 256MB
      track_io_timing: "on"
      track_wal_io_timing: "on"
  backup:
    barmanObjectStore:
      destinationPath: https://$ACCOUNT.blob.core.windows.net/$CONTAINER/cnpg-vacuum-only/$TOKEN/
      serverName: $C
      azureCredentials: {connectionString: {name: $SECRET, key: connection-string}}
    volumeSnapshot: {className: $BSC, walClassName: $BSC, online: true, onlineConfiguration: {waitForArchive: true, immediateCheckpoint: false}}
EOF
# Pin file and apply the complete pair bootstrap only once.
cp "$TMP/cluster.yaml" "$SD/source-pair-cluster-manifest.yaml"; chmod 600 "$SD/source-pair-cluster-manifest.yaml"
sha256sum "$SD/source-pair-cluster-manifest.yaml" >"$SD/source-pair-cluster-manifest.sha256"
jq -n --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg did "$DATA_ID" --arg duid "$(jq -r .data.uniqueId "$PAIR")" --arg wid "$WAL_ID" --arg wuid "$(jq -r .wal.uniqueId "$PAIR")" --arg backup "$(jq -r .backupName "$PAIR")" --arg hash "$(awk '{print $1}' "$SD/source-pair-cluster-manifest.sha256")" '{version:1,phase:"paired_snapshot_primary_bootstrap_submitted",submittedAtUtc:$at,sourcePair:{data:{snapshotId:$did,uniqueId:$duid},wal:{snapshotId:$wid,uniqueId:$wuid},oldBackupName:$backup},bootstrapMode:"CNPG recovery from the exact paired static VolumeSnapshots; data and WAL both referenced directly",manifestSha256:$hash,newStackArchiveConfigured:true,oldArchiveDependencyProven:false,archiveIndependenceClaimed:false}' >"$SD/bootstrap-attempt.json"; chmod 600 "$SD/bootstrap-attempt.json"
k apply -f "$TMP/cluster.yaml" >/dev/null
c=$(wait_cluster 1); POD=$(jq -er .status.currentPrimary <<<"$c"); [[ "$POD" == "$C-1" ]] || die "Unexpected primary pod $POD."
pvcs=$(k get pvc -n "$NS" -l "cnpg.io/cluster=$C" -o json)
DREF=$(jq -cer --arg n "$C-1" '.items[]|select(.metadata.name==$n and .status.phase=="Bound")|.spec.dataSource') || die 'Primary DATA PVC has no bound snapshot source.'
WREF=$(jq -cer --arg n "$C-1-wal" '.items[]|select(.metadata.name==$n and .status.phase=="Bound")|.spec.dataSource') || die 'Primary WAL PVC has no bound snapshot source.'
jq -e --arg vs "$VSD" '.kind=="VolumeSnapshot" and .name==$vs and .apiGroup=="snapshot.storage.k8s.io"' <<<"$DREF" >/dev/null || die 'Primary DATA PVC is not sourced from exact retained DATA VolumeSnapshot.'
jq -e --arg vs "$VSW" '.kind=="VolumeSnapshot" and .name==$vs and .apiGroup=="snapshot.storage.k8s.io"' <<<"$WREF" >/dev/null || die 'Primary WAL PVC is not sourced from exact retained WAL VolumeSnapshot.'
# Verify the snapshot restored the unchanged source fixture before any ALTER SYSTEM or checkpoint.
PRE=$(sql "$POD" "$progress_sql"); jq -e --arg s "$(jq -r .systemIdentifier "$PAIR")" --argjson h 24696061952 --argjson t 25040748544 '.systemIdentifier==$s and .inRecovery==false and .firstLiveIdEven==true and .adjacentOddIdAbsent==true and .payloadBytes==1536 and .indexName=="vacuum_target_id_idx" and .heapBytes==$h and .totalBytes==$t and .tableAutovacuumDisabled==true and .walLevel=="logical" and .fullPageWrites=="on" and .fullPageWritesPendingRestart==false and .recoveryPrefetch=="try" and .recoveryPrefetchPendingRestart==false and .appSessions==0' <<<"$PRE" >/dev/null || die 'Source pair restore did not preserve the expected pre-tuning fixture/default settings.'
# Bind the audited snapshot IDs to the actual primary PVC .spec.dataSource and their retained CSI objects.
DATA_CONTENT=$(jq -er .status.boundVolumeSnapshotContentName < <(k get volumesnapshot "$VSD" -n "$NS" -o json)); WAL_CONTENT=$(jq -er .status.boundVolumeSnapshotContentName < <(k get volumesnapshot "$VSW" -n "$NS" -o json))
PAIR_PROOF="$SD/source-pair-restore-proof.json"
jq -n --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg did "$DATA_ID" --arg duid "$(jq -r .data.uniqueId "$PAIR")" --arg dpvc "$(jq -r .data.sourcePvc "$PAIR")" --arg drpvc "$C-1" --arg dns "$NS" --arg dvs "$VSD" --arg dcn "$DATA_CONTENT" --argjson dref "$DREF" --arg wid "$WAL_ID" --arg wuid "$(jq -r .wal.uniqueId "$PAIR")" --arg wpvc "$(jq -r .wal.sourcePvc "$PAIR")" --arg wrpvc "$C-1-wal" --arg wns "$NS" --arg wvs "$VSW" --arg wcn "$WAL_CONTENT" --argjson wref "$WREF" --argjson fixture "$PRE" --arg principal "$PRINCIPAL" --arg dassign "$(jq -r '.assignments[]|select(.kind=="data")|.assignmentId' "$ROLE_PROOF")" --arg wassign "$(jq -r '.assignments[]|select(.kind=="wal")|.assignmentId' "$ROLE_PROOF")" '{version:1,verifiedAtUtc:$at,verifiedBeforeTuning:true,sourcePairRestore:{data:{snapshotId:$did,uniqueId:$duid,sourcePvc:$dpvc,restoredPvc:$drpvc,volumeSnapshotRef:{namespace:$dns,name:$dvs,contentName:$dcn},pvcDataSourceSnapshotRef:$dref},wal:{snapshotId:$wid,uniqueId:$wuid,sourcePvc:$wpvc,restoredPvc:$wrpvc,volumeSnapshotRef:{namespace:$wns,name:$wvs,contentName:$wcn},pvcDataSourceSnapshotRef:$wref},fixture:{verifiedBeforeTuning:true,systemIdentifier:$fixture.systemIdentifier,heapBytes:$fixture.heapBytes,tableBytes:$fixture.totalBytes,liveTuplesEstimate:$fixture.liveTuplesEstimate,deadTuplesEstimate:$fixture.deadTuplesEstimate,sentinels:{firstLiveId:$fixture.firstLiveId,firstLiveIdEven:$fixture.firstLiveIdEven,adjacentOddIdAbsent:$fixture.adjacentOddIdAbsent,payloadBytes:$fixture.payloadBytes,indexName:$fixture.indexName,tableAutovacuumDisabled:$fixture.tableAutovacuumDisabled}},gucsBeforeTuning:{primary:{fullPageWrites:$fixture.fullPageWrites,recoveryPrefetch:$fixture.recoveryPrefetch,fullPageWritesPendingRestart:$fixture.fullPageWritesPendingRestart,recoveryPrefetchPendingRestart:$fixture.recoveryPrefetchPendingRestart}}},sourceSnapshotRoleAssignments:{principalId:$principal,roleDefinitionId:"7efff54f-a5b4-42b5-a1c5-5411624893ce",data:{snapshotId:$did,assignmentId:$dassign,scope:$did,createdByThisStack:true},wal:{snapshotId:$wid,assignmentId:$wassign,scope:$wid,createdByThisStack:true}}}' >"$PAIR_PROOF"
chmod 600 "$PAIR_PROOF"
# Remove only the two exact grants created above; retain both snapshots and their locks.
for role in data wal; do aid=$(jq -er --arg r "$role" '.assignments[]|select(.kind==$r)|.assignmentId' "$ROLE_PROOF"); sid=$(jq -er --arg r "$role" '.assignments[]|select(.kind==$r)|.snapshotId' "$ROLE_PROOF"); at=$(date -u +%Y-%m-%dT%H:%M:%SZ); set +e; azlong role assignment delete --ids "$aid" -o none; rc=$?; set -e; rows=$(azc role assignment list --scope "$sid" -o json) || die "Cannot verify removal of $role snapshot grant."; if jq -e --arg p "$PRINCIPAL" --arg role "${ROLE_ID,,}" --arg scope "${sid,,}" 'any(.[]; .principalId==$p and (.roleDefinitionId|ascii_downcase|endswith($role)) and (.scope|ascii_downcase)==$scope)' <<<"$rows" >/dev/null; then die "$role exact-scope snapshot role assignment remains."; fi; jq --arg kind "$role" --arg at "$at" --argjson rc "$rc" '.assignments|=map(if .kind==$kind then .removed=true|.removedAtUtc=$at|.deleteExitCode=$rc|.postRemovalRoleListAbsent=true else . end)|.postRemovalConfirmed=all(.assignments[];.removed and .postRemovalRoleListAbsent)|.status="removed"|.completedAtUtc=(now|todateiso8601)' "$ROLE_PROOF" >"$ROLE_PROOF.tmp"; mv -f "$ROLE_PROOF.tmp" "$ROLE_PROOF"; done
jq -e '.postRemovalConfirmed==true and all(.assignments[];.createdByThisStack and .removed and .postRemovalRoleListAbsent)' "$ROLE_PROOF" >/dev/null || die 'Source role assignment lifecycle proof incomplete.'

# Only now set primary FPW off; this is a local ALTER SYSTEM override and survives CNPG reconciliation.
auto_alter "$POD" "ALTER SYSTEM SET full_page_writes = 'off';"
fpw=$(sql "$POD" "SELECT json_build_object('value',setting,'source',source,'sourceFile',sourcefile,'pendingRestart',pending_restart)::text FROM pg_settings WHERE name='full_page_writes';")
jq -e '.value=="off" and (.sourceFile|type=="string" and endswith("/postgresql.auto.conf")) and (.pendingRestart|not)' <<<"$fpw" >/dev/null || die 'Primary FPW off did not become effective from its local auto.conf.'
# Force one post-change checkpoint before the paired online Backup and replica seed.
sql "$POD" 'CHECKPOINT;' >/dev/null
fpw2=$(sql "$POD" "SELECT json_build_object('value',setting,'sourceFile',sourcefile,'pendingRestart',pending_restart)::text FROM pg_settings WHERE name='full_page_writes';")
jq -e '.value=="off" and (.sourceFile|type=="string" and endswith("/postgresql.auto.conf")) and (.pendingRestart|not)' <<<"$fpw2" >/dev/null || die 'Primary FPW changed across checkpoint.'

# Verify separate local DATA/WAL devices before creating the standby seed.
k exec -n "$NS" -c postgres "$POD" -- sh -eu -c 'd=$(printenv PGDATA); test -L "$d/pg_wal"; test "$(readlink -f "$d/pg_wal")" = /var/lib/postgresql/wal/pg_wal; test "$(stat -Lc %d "$d")" != "$(stat -Lc %d "$d/pg_wal")"'
BEFORE_ARCHIVE=$(sql "$POD" 'SELECT archived_count FROM pg_stat_archiver;'); sql "$POD" 'SELECT pg_switch_wal();' >/dev/null
end=$((SECONDS+900)); archived='{}'; while ((SECONDS<end)); do archived=$(sql "$POD" "SELECT json_build_object('archived',archived_count,'failed',failed_count,'lastWal',last_archived_wal)::text FROM pg_stat_archiver;"); (( $(jq -r .archived <<<"$archived") > BEFORE_ARCHIVE )) && break; sleep 10; done
(( $(jq -r .archived <<<"$archived") > BEFORE_ARCHIVE )) && jq -e '.failed==0 and (.lastWal|length)>0' <<<"$archived" >/dev/null || die 'New WAL did not archive; refusing standby seed.'
cat >"$TMP/backup.yaml" <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Backup
metadata: {name: $BK, namespace: $NS, labels: {repro.stack.token: "$TOKEN"}}
spec: {cluster: {name: $C}, method: volumeSnapshot, online: true, onlineConfiguration: {waitForArchive: true, immediateCheckpoint: false}}
EOF
k apply -f "$TMP/backup.yaml" >/dev/null
end=$((SECONDS+3600)); while ((SECONDS<end)); do backup=$(k get backup "$BK" -n "$NS" -o json); phase=$(jq -r '.status.phase // empty' <<<"$backup"); [[ "$phase" == completed ]] && break; [[ "$phase" != failed && "$phase" != walArchivingFailing ]] || die "CNPG backup failed: $phase"; sleep 10; done
jq -e '.status.phase=="completed" and ([.status.snapshotBackupStatus.elements[].type]|sort)==["PG_DATA","PG_WAL"]' <<<"$backup" >/dev/null || die 'Backup lacks exact paired DATA/WAL snapshots.'
DATA_VS=$(jq -er '.status.snapshotBackupStatus.elements[]|select(.type=="PG_DATA")|.name' <<<"$backup"); WAL_VS=$(jq -er '.status.snapshotBackupStatus.elements[]|select(.type=="PG_WAL")|.name' <<<"$backup")
for item in "$DATA_VS:$C-1" "$WAL_VS:$C-1-wal"; do IFS=: read -r vs pvc <<<"$item"; obj=$(k get volumesnapshot "$vs" -n "$NS" -o json); jq -e --arg p "$pvc" '.status.readyToUse==true and .spec.source.persistentVolumeClaimName==$p' <<<"$obj" >/dev/null || die 'New paired backup snapshot source/ready check failed.'; content=$(jq -er .status.boundVolumeSnapshotContentName <<<"$obj"); cnt=$(k get volumesnapshotcontent "$content" -o json); handle=$(jq -er .status.snapshotHandle <<<"$cnt"); azsnap=$(azc snapshot show --ids "$handle" -o json) || die 'Cannot inspect new seed snapshot.'; jq -e --arg rg "${SNAPSHOT_RG,,}" --arg t "$TOKEN" --arg loc "${LOC,,}" '(.resourceGroup|ascii_downcase)==$rg and (.location|ascii_downcase)==$loc and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and .tags.reprotoken==$t' <<<"$azsnap" >/dev/null || die 'New seed snapshot is not in stack retention RG or ready.'; done

# Capture preexisting Jobs, ask CNPG to recover the standby from the fresh paired snapshots.
jobs_before=$(k get jobs -n "$NS" -l "cnpg.io/cluster=$C" -o json | jq -c '[.items[].metadata.uid]|sort')
k patch cluster "$C" -n "$NS" --type=merge -p '{"spec":{"instances":2,"postgresql":{"synchronous":{"method":"any","number":1,"dataDurability":"preferred"}}}}' >/dev/null
# Observe and durably capture the new recovery Job UID before waiting for readiness;
# CNPG TTL may remove the Job object by the time both instances are healthy.
JOB_UID=''; end=$((SECONDS+10800)); while ((SECONDS<end)); do
 jobs=$(k get jobs -n "$NS" -l "cnpg.io/cluster=$C" -o json)
 JOB=$(jq -c --arg i "$C-2" --argjson old "$jobs_before" '[.items[]|select(.metadata.labels["cnpg.io/instanceName"]==$i and .metadata.labels["cnpg.io/jobRole"]=="snapshot-recovery" and ((.metadata.uid as $u|$old|index($u))==null))]|first // empty' <<<"$jobs")
 if [[ -n "$JOB" && -z "$JOB_UID" ]]; then
  jq -e --arg i "$SYSIMAGE" 'any(.spec.template.spec.containers[];.name=="snapshot-recovery" and .image==$i)' <<<"$JOB" >/dev/null || die 'Seed recovery job did not use pinned system image.'
  JOB_UID=$(jq -r .metadata.uid <<<"$JOB"); jq -n --arg n "$(jq -r .metadata.name <<<"$JOB")" --arg u "$JOB_UID" --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '{name:$n,uid:$u,observedAtUtc:$at}' >"$SD/snapshot-recovery-job.json"
 fi
 c=$(k get cluster "$C" -n "$NS" -o json); [[ -n "$JOB_UID" && $(jq -r '.status.readyInstances // 0' <<<"$c") == 2 ]] && break; sleep 5
done
[[ -n "$JOB_UID" ]] || die 'Did not observe exact new snapshot-recovery Job UID.'
c=$(wait_cluster 2); [[ $(jq -r .status.currentPrimary <<<"$c") == "$C-1" ]] || die 'Primary role changed during replica seed.'
event=''; end=$((SECONDS+180)); while ((SECONDS<end)); do event=$(k get events -n "$NS" -o json | jq -c --arg u "$JOB_UID" '[.items[]|select(.involvedObject.uid==$u and .reason=="Completed")]|first // empty'); [[ -n "$event" ]] && break; sleep 5; done
[[ -n "$event" ]] || die 'No Completed event for exact snapshot-recovery Job UID.'
pods=$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json); jq -e --arg t "$TOKEN" '[.items[]|select(.status.phase=="Running" and .metadata.labels["repro.stack.token"]==$t and any(.status.containerStatuses[]?;.name=="postgres" and .ready))]|length==2' <<<"$pods" >/dev/null || die 'Expected two owned running PostgreSQL pods.'
pvcs=$(k get pvc -n "$NS" -l "cnpg.io/cluster=$C" -o json)
jq -e --arg n "$C-2" --arg d "$DATA_VS" --arg w "$WAL_VS" '[.items[]|select((.metadata.name==$n and .status.phase=="Bound" and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$d) or (.metadata.name==($n+"-wal") and .status.phase=="Bound" and .spec.dataSource.kind=="VolumeSnapshot" and .spec.dataSource.name==$w))]|length==2' <<<"$pvcs" >/dev/null || die 'Standby DATA/WAL PVCs do not point to fresh paired snapshots.'
# Clear archive writer and secret after the seeded instance has populated its local WAL cache.
uids=$(jq -c '[.items[]|select(.status.phase=="Running")|.metadata.uid]|sort' <<<"$pods"); k patch cluster "$C" -n "$NS" --type=json -p='[{"op":"remove","path":"/spec/backup/barmanObjectStore"}]' >/dev/null
end=$((SECONDS+180)); while ((SECONDS<end)); do c=$(k get cluster "$C" -n "$NS" -o json); p=$(k get pods -n "$NS" -l "cnpg.io/cluster=$C" -o json); [[ $(jq -r '.status.readyInstances // 0' <<<"$c") == 2 && $(jq -c '[.items[]|select(.status.phase=="Running")|.metadata.uid]|sort' <<<"$p") == "$uids" ]] && break; sleep 5; done
[[ $(jq -r '.spec.backup.barmanObjectStore // null' <<<"$c") == null && $(jq -c '[.items[]|select(.status.phase=="Running")|.metadata.uid]|sort' <<<"$p") == "$uids" ]] || die 'Archive writer removal restarted pods.'
k delete secret "$SECRET" -n "$NS" --wait=true >/dev/null
# Wait until the new snapshot-seeded standby is fully caught up before making final config assertions.
REQUIRED_LSN=$(sql "$C-1" 'SELECT pg_current_wal_flush_lsn();'); wait_replay "$REQUIRED_LSN"
PRIMARY_UID=$(k get pod "$C-1" -n "$NS" -o json | jq -er .metadata.uid); REPLICA_UID=$(k get pod "$C-2" -n "$NS" -o json | jq -er .metadata.uid)
auto_alter "$C-2" "ALTER SYSTEM SET full_page_writes = 'on';"
auto_alter "$C-2" "ALTER SYSTEM SET recovery_prefetch = 'off';"
# Let CNPG reconcile, then confirm the overrides are still local and both pods stayed put.
for _ in 1 2 3; do sleep 5; c=$(k get cluster "$C" -n "$NS" -o json); jq -e '((.spec.postgresql.parameters // {})|has("full_page_writes")|not) and ((.spec.postgresql.parameters // {})|has("recovery_prefetch")|not) and .status.readyInstances==2 and .status.currentPrimary=="hdd-pg-1"' <<<"$c" >/dev/null || die 'CNPG Cluster GUCs or role state changed unexpectedly.'; [[ $(k get pod "$C-1" -n "$NS" -o json|jq -er .metadata.uid) == "$PRIMARY_UID" && $(k get pod "$C-2" -n "$NS" -o json|jq -er .metadata.uid) == "$REPLICA_UID" ]] || die 'Pod restarted during role-local GUC setup.'; done
PRIMARY=$(sql "$C-1" "$progress_sql"); REPLICA=$(sql "$C-2" "$progress_sql")
jq -e --arg s "$(jq -r .systemIdentifier "$PAIR")" --argjson h 24696061952 --argjson t 25040748544 '.systemIdentifier==$s and .inRecovery==false and .heapBytes==$h and .totalBytes==$t and .tableAutovacuumDisabled==true and .fullPageWrites=="off" and .fullPageWritesPendingRestart==false and (.fullPageWritesSourceFile|endswith("/postgresql.auto.conf")) and .recoveryPrefetch=="try" and .recoveryPrefetchPendingRestart==false and .appSessions==0' <<<"$PRIMARY" >/dev/null || die 'Final primary GUC/fixture evidence mismatch.'
jq -e --arg s "$(jq -r .systemIdentifier "$PAIR")" --argjson h 24696061952 --argjson t 25040748544 '.systemIdentifier==$s and .inRecovery==true and .heapBytes==$h and .totalBytes==$t and .tableAutovacuumDisabled==true and .fullPageWrites=="on" and .fullPageWritesPendingRestart==false and (.fullPageWritesSourceFile|endswith("/postgresql.auto.conf")) and .recoveryPrefetch=="off" and .recoveryPrefetchPendingRestart==false and (.recoveryPrefetchSourceFile|endswith("/postgresql.auto.conf")) and .appSessions==0' <<<"$REPLICA" >/dev/null || die 'Final replica GUC/fixture evidence mismatch.'
FINAL_LSN=$(sql "$C-1" 'SELECT pg_current_wal_flush_lsn();'); wait_replay "$FINAL_LSN"
# Verify DATA/WAL remain four distinct Azure HDD managed disks on owned AKS nodes.
for claim in "$C-1" "$C-1-wal" "$C-2" "$C-2-wal"; do pvc=$(k get pvc "$claim" -n "$NS" -o json); jq -e '.status.phase=="Bound" and .spec.resources.requests.storage=="512Gi" and .spec.storageClassName=="azure-standard-hdd"' <<<"$pvc" >/dev/null || die "$claim PVC size/class mismatch."; pv=$(jq -er .spec.volumeName <<<"$pvc"); diskid=$(k get pv "$pv" -o json|jq -er .spec.csi.volumeHandle); disk=$(azc disk show --ids "$diskid" -o json) || die "Cannot inspect $claim disk."; jq -e --arg id "${diskid,,}" --arg rg "${NODE_RG,,}" --arg loc "${LOC,,}" '(.id|ascii_downcase)==$id and ((.id|ascii_downcase)|contains("/resourcegroups/"+$rg+"/providers/microsoft.compute/disks/")) and (.location|ascii_downcase)==$loc and .sku.name=="Standard_LRS" and .diskSizeGB==512 and .managedBy!=null' <<<"$disk" >/dev/null || die "$claim disk owner/SKU/size mismatch."; done

# Save final preparation proof only after every preflight gate passes.
FINAL="$SD/prepare.json"
jq -n --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg token "$TOKEN" --arg loc "$LOC" --arg sub "$SUB" --arg rg "$RG" --arg aks "$AKS" --arg image "$IMAGE" --arg sysimage "$SYSIMAGE" --arg bk "$BK" --arg dvs "$DATA_VS" --arg wvs "$WAL_VS" --arg job "$JOB_UID" --argjson fixture "$PRE" --argjson sourceProof "$(cat "$PAIR_PROOF")" --argjson roleProof "$(cat "$ROLE_PROOF")" --argjson primary "$PRIMARY" --argjson replica "$REPLICA" --arg catchup "$FINAL_LSN" --argjson archive "$archived" --arg snaprg "$SNAPSHOT_RG" --arg snaprgid "$SNAPSHOT_RG_ID" --arg retrole "$RET_ASSIGN_ID" --argjson preparedLive "$(jq -r .fixture.preparedLiveTuplesEstimate "$PAIR")" --argjson preparedDead "$(jq -r .fixture.preparedDeadTuplesEstimate "$PAIR")" '{version:1,status:"complete",verifiedAtUtc:$at,ownerToken:$token,location:$loc,subscriptionId:$sub,resourceGroup:$rg,clusterName:$aks,cluster:"repro/hdd-pg",instances:2,sourceKind:"reused-prepared-paired-snapshot",fixtureTargetHeapGiB:23,fixtureTargetHeapBytes:24696061952,systemIdentifier:$fixture.systemIdentifier,heapBytes:$fixture.heapBytes,tableBytes:$fixture.totalBytes,preparedLiveTuplesEstimate:$preparedLive,preparedDeadTuplesEstimate:$preparedDead,fixture:{sourceKind:"reused-prepared-paired-snapshot",heapBytes:$fixture.heapBytes,tableBytes:$fixture.totalBytes,liveTuplesEstimate:$preparedLive,deadTuplesEstimate:$preparedDead,observedLiveTuplesEstimate:$fixture.liveTuplesEstimate,observedDeadTuplesEstimate:$fixture.deadTuplesEstimate,sourcePairSentinels:{firstLiveId:$fixture.firstLiveId,firstLiveIdEven:$fixture.firstLiveIdEven,adjacentOddIdAbsent:$fixture.adjacentOddIdAbsent,payloadBytes:$fixture.payloadBytes,indexName:$fixture.indexName,tableAutovacuumDisabled:$fixture.tableAutovacuumDisabled}},sourcePairRestore:$sourceProof.sourcePairRestore,sourceSnapshotRoleAssignments:($roleProof + {data:($roleProof.assignments[]|select(.kind=="data")),wal:($roleProof.assignments[]|select(.kind=="wal"))}),replicaSeed:{backupName:$bk,dataVolumeSnapshot:$dvs,walVolumeSnapshot:$wvs,recoveryJobUid:$job,systemImage:$sysimage,measurementImage:$image},finalRoles:{primary:$primary,replica:$replica},requiredReplayLsn:$catchup,archiveEvidence:$archive,snapshotRetention:{resourceGroup:$snaprg,resourceGroupId:$snaprgid,roleAssignmentId:$retrole,roleAssignmentRemoved:false,allSnapshotsPreserved:true},clientCreated:false}' >"$FINAL.tmp"
chmod 600 "$FINAL.tmp"; mv -f "$FINAL.tmp" "$FINAL"
cat >"$SD/initial-roles.json" <<EOF
{"verifiedAtUtc":"$(date -u +%Y-%m-%dT%H:%M:%SZ)","sourcePairRestore":{"data":{"snapshotId":"$DATA_ID","uniqueId":"$(jq -r .data.uniqueId "$PAIR")","sourcePvc":"$C-1","volumeSnapshotRef":{"namespace":"$NS","name":"$VSD","contentName":"$DATA_CONTENT"},"pvcDataSourceSnapshotRef":$DREF},"wal":{"snapshotId":"$WAL_ID","uniqueId":"$(jq -r .wal.uniqueId "$PAIR")","sourcePvc":"$C-1-wal","volumeSnapshotRef":{"namespace":"$NS","name":"$VSW","contentName":"$WAL_CONTENT"},"pvcDataSourceSnapshotRef":$WREF},"fixture":{"verifiedBeforeTuning":true,"systemIdentifier":"$(jq -r .systemIdentifier <<<"$PRE")","heapBytes":$(jq -r .heapBytes <<<"$PRE"),"tableBytes":$(jq -r .totalBytes <<<"$PRE"),"sentinels":{"firstLiveId":$(jq -r .firstLiveId <<<"$PRE"),"firstLiveIdEven":true,"adjacentOddIdAbsent":true,"payloadBytes":1536,"indexName":"vacuum_target_id_idx","tableAutovacuumDisabled":true}},"finalRoles":{"primary":$PRIMARY,"replica":$REPLICA}}
EOF
chmod 600 "$SD/initial-roles.json"
echo "Prepared exact retained DATA/WAL source pair; verified 23-GiB fixture before tuning, primary full_page_writes=off, standby recovery_prefetch=off, and replay through $FINAL_LSN. Evidence: $FINAL"
