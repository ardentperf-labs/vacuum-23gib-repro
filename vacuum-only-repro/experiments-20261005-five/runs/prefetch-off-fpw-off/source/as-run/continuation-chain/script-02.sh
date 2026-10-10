#!/usr/bin/env bash
# Remove only this stack's exact source-snapshot role grants while restoring
# each pre-existing CanNotDelete lock in a finally-style path. No snapshot is
# deleted, moved, retagged, or replaced by this helper.
set -Eeuo pipefail
umask 077
HERE="$(cd -- "$(dirname -- "$0")" && pwd)"
STATE="$HERE/state.json"; PAIR="$HERE/source-pair.json"
ROLE="$HERE/external-source-snapshot-role-assignment.json"
PROOF="$HERE/source-pair-restore-proof.json"; RECEIPT="$HERE/source-snapshot-lock-role-removal.json"
SUB=$(jq -er .subscriptionId "$STATE"); TOKEN=$(jq -er .ownerToken "$STATE")
SOURCE_RG=$(jq -er .sourceResourceGroup "$PAIR"); SOURCE_TOKEN=$(jq -er .sourceResourceGroupOwnerToken "$PAIR"); ROLE_ID='7efff54f-a5b4-42b5-a1c5-5411624893ce'
[[ $(jq -r '.status // empty' "$STATE") == ready ]] || { echo 'stack state not ready' >&2; exit 1; }
jq -e --arg t "$TOKEN" '.verifiedBeforeTuning==true and .sourcePairRestore.fixture.verifiedBeforeTuning==true and .sourcePairRestore.data.snapshotId and .sourcePairRestore.wal.snapshotId' "$PROOF" >/dev/null || { echo 'immutable source-pair restore proof missing' >&2; exit 1; }
azc(){ timeout --foreground --kill-after=15s 240s az "$@" --subscription "$SUB"; }
azlong(){ timeout --foreground --kill-after=15s 1800s az "$@" --subscription "$SUB"; }
LOCK_ARMED=false; ACTIVE_ID=''; ACTIVE_LOCK_NAME=''; ACTIVE_LOCK_NOTES=''; RESTORE_LOG="$HERE/source-lock-trap-restore.jsonl"
restore_active_lock(){
  [[ "$LOCK_ARMED" == true ]] || return 0
  local attempt current rc good=false at
  for attempt in 1 2 3; do
    set +e; current=$(azc lock list --resource "$ACTIVE_ID" -o json 2>/dev/null); rc=$?; set -e
    if [[ "$rc" -eq 0 ]] && jq -e --arg id "${ACTIVE_ID,,}" --arg n "$ACTIVE_LOCK_NAME" --arg notes "$ACTIVE_LOCK_NOTES" 'length==1 and .[0].name==$n and .[0].level=="CanNotDelete" and .[0].notes==$notes and ((.[0].id|ascii_downcase)|startswith($id+"/providers/microsoft.authorization/locks/"))' <<<"$current" >/dev/null; then good=true; break; fi
    set +e; azc lock create --name "$ACTIVE_LOCK_NAME" --lock-type CanNotDelete --resource "$ACTIVE_ID" --notes "$ACTIVE_LOCK_NOTES" -o none >/dev/null 2>&1; rc=$?; set -e
    at=$(date -u +%Y-%m-%dT%H:%M:%SZ); jq -cn --arg id "$ACTIVE_ID" --arg name "$ACTIVE_LOCK_NAME" --arg at "$at" --argjson attempt "$attempt" --argjson rc "$rc" '{snapshotId:$id,lockName:$name,attempt:$attempt,createExitCode:$rc,atUtc:$at}' >>"$RESTORE_LOG"; chmod 600 "$RESTORE_LOG"
  done
  if [[ "$good" == true ]]; then LOCK_ARMED=false; ACTIVE_ID=''; return 0; fi
  echo "FATAL: could not reverify CanNotDelete lock for $ACTIVE_ID; snapshot itself was never deleted." >&2
  return 1
}
trap 'restore_active_lock' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
jq -e --arg t "$SOURCE_TOKEN" --arg n "${SOURCE_RG,,}" '(.name|ascii_downcase)==$n and .tags.reprotoken==$t and .tags.reproPurpose=="experiment-snapshots"' < <(azc group show -n "$SOURCE_RG" -o json) >/dev/null || { echo 'source snapshot resource-group ownership mismatch' >&2; exit 1; }
principal=$(jq -er '.principalId' "$ROLE");
jq -e --arg t "$TOKEN" --arg p "$principal" --arg role "${ROLE_ID,,}" '.status as $s | ($s=="active" or $s=="removing_pair" or $s=="removed") and .stackOwnerToken==$t and .principalId==$p and (.roleDefinitionId|ascii_downcase)==$role and (.assignments|length)==2 and all(.assignments[];.createdByThisStack and ((.removed//false)==false or (.removed==true and .deleteExitCode==0 and .absenceConfirmed==true and .postRemovalRoleListAbsent==true)))' "$ROLE" >/dev/null || { echo 'source assignment ledger is not this stack-owned pair' >&2; exit 1; }
SUB_SCOPE="/subscriptions/$SUB"; RG_SCOPE="/subscriptions/$SUB/resourceGroups/$SOURCE_RG"
all_locks=$(azc lock list -o json) || { echo 'cannot inspect inherited lock inventory' >&2; exit 1; }
if jq -e --arg rg "${RG_SCOPE,,}" --arg sub "${SUB_SCOPE,,}" '[.[]|(.id|ascii_downcase|split("/providers/microsoft.authorization/locks/")[0])]|any(.[];.==$rg or .==$sub)' <<<"$all_locks" >/dev/null; then echo 'inherited RG/subscription lock exists; refusing narrow unlock' >&2; exit 1; fi
if [[ ! -e "$RECEIPT" ]]; then
  jq -n --arg at "2026-10-06T05:45:46Z" --arg sha "38eab7574954981d3e152b93ae07a5e20d7ce72cd90770af9e2ffb5ff2561321" --arg did "$(jq -r .data.id "$PAIR")" --arg duid "$(jq -r .data.uniqueId "$PAIR")" --arg aid "$(jq -r '.assignments[]|select(.kind=="data")|.assignmentId' "$ROLE")" '{version:1,firstScopeLockedFailure:{observedAtUtc:$at,failedContinuationSha256:$sha,failedContinuationSourceAvailable:false,scopeLockedError:"Azure ScopeLocked prevented deleting the exact DATA snapshot-scoped role assignment under its direct CanNotDelete lock.",role:"Disk Snapshot Contributor",snapshotId:$did,uniqueId:$duid,assignmentId:$aid,snapshotAndRoleAssignmentsRemain:true,noPostRestorePostgreSQLMutationOccurred:true},roles:[],allSnapshotsPreserved:true}' >"$RECEIPT"
fi
chmod 600 "$RECEIPT"
for kind in data wal; do
  id=$(jq -er --arg k "$kind" '.[$k].id' "$PAIR"); expected_uid=$(jq -er --arg k "$kind" '.[$k].uniqueId' "$PAIR"); expected_source=$(jq -er --arg k "$kind" '.[$k].sourceDiskId' "$PAIR")
  assignment_id=$(jq -er --arg k "$kind" '.assignments[]|select(.kind==$k)|.assignmentId' "$ROLE")
  snap=$(azc snapshot show --ids "$id" -o json) || { echo "cannot read $kind source snapshot" >&2; exit 1; }
  jq -e --arg id "${id,,}" --arg u "${expected_uid,,}" --arg src "${expected_source,,}" --arg rg "${SOURCE_RG,,}" '(.id|ascii_downcase)==$id and (.uniqueId|ascii_downcase)==$u and (.creationData.sourceResourceId|ascii_downcase)==$src and (.tags.source_volume_id|ascii_downcase)==$src and (.resourceGroup|ascii_downcase)==$rg and .provisioningState=="Succeeded" and .snapshotAccessState=="Available" and .incremental==true and .sku.name=="Standard_ZRS" and .diskSizeGB==512' <<<"$snap" >/dev/null || { echo "$kind source snapshot identity/status mismatch" >&2; exit 1; }
  locks=$(azc lock list --resource "$id" -o json) || { echo "cannot inspect exact $kind lock" >&2; exit 1; }
  jq -e --arg id "${id,,}" 'length==1 and .[0].name=="preserve-all-snapshots" and .[0].level=="CanNotDelete" and .[0].notes=="Preserve every snapshot until explicit user deletion authorization." and ((.[0].id|ascii_downcase)|startswith($id+"/providers/microsoft.authorization/locks/"))' <<<"$locks" >/dev/null || { echo "$kind direct lock inventory differs from the single expected retention lock" >&2; exit 1; }
  expected_lock_id=$(jq -er '.[0].id' <<<"$locks"); lock_name=$(jq -er '.[0].name' <<<"$locks"); lock_notes=$(jq -er '.[0].notes' <<<"$locks")
  rows=$(azc role assignment list --scope "$id" -o json) || { echo "cannot inspect exact $kind role assignment" >&2; exit 1; }
  was_removed=$(jq -r --arg k "$kind" '.assignments[]|select(.kind==$k)|.removed // false' "$ROLE")
  if [[ "$was_removed" == true ]]; then
    jq -e --arg p "$principal" --arg role "${ROLE_ID,,}" --arg scope "${id,,}" 'all(.[]; .principalId!=$p or (.roleDefinitionId|ascii_downcase|endswith($role)|not) or (.scope|ascii_downcase)!=$scope)' <<<"$rows" >/dev/null || { echo "$kind previously removed assignment has reappeared" >&2; exit 1; }
    jq -e --arg id "${id,,}" 'length==1 and .[0].name=="preserve-all-snapshots" and .[0].level=="CanNotDelete" and ((.[0].id|ascii_downcase)|startswith($id+"/providers/microsoft.authorization/locks/"))' <<<"$locks" >/dev/null || { echo "$kind removed assignment lacks its restored snapshot lock" >&2; exit 1; }
    continue
  fi
  jq -e --arg aid "${assignment_id,,}" --arg p "$principal" --arg role "${ROLE_ID,,}" --arg scope "${id,,}" 'any(.[];.principalId==$p and (.id|ascii_downcase)==$aid and (.roleDefinitionId|ascii_downcase|endswith($role)) and (.scope|ascii_downcase)==$scope)' <<<"$rows" >/dev/null || { echo "$kind assignment ID/principal/scope not exactly owned" >&2; exit 1; }
  base=$(jq -cn --arg kind "$kind" --arg id "$id" --arg uid "$expected_uid" --arg aid "$assignment_id" --arg lock "$expected_lock_id" --arg name "$lock_name" --arg notes "$lock_notes" '{kind:$kind,snapshotId:$id,uniqueId:$uid,roleAssignmentId:$aid,originalLock:{id:$lock,name:$name,level:"CanNotDelete",notes:$notes}}')
  # Arm EXIT/INT/TERM restoration before the unlock request in case the next
  # command fails or the process is interrupted at the boundary.
  LOCK_ARMED=true; ACTIVE_ID="$id"; ACTIVE_LOCK_NAME="$lock_name"; ACTIVE_LOCK_NOTES="$lock_notes"
  set +e; azc lock delete --ids "$expected_lock_id" -o none >/dev/null 2>&1; unlock_rc=$?; set -e
  set +e; unlocked_list=$(azc lock list --resource "$id" -o json 2>/dev/null); list_rc=$?; set -e
  assignment_rc=125; assignment_absent=false
  if [[ "$unlock_rc" -eq 0 && "$list_rc" -eq 0 && $(jq 'length' <<<"$unlocked_list") -eq 0 ]]; then
    set +e; azlong role assignment delete --ids "$assignment_id" -o none >/dev/null 2>&1; assignment_rc=$?; set -e
  fi
  # Always attempt to restore the original lock if the exact direct lock is absent.
  set +e; after_unlock=$(azc lock list --resource "$id" -o json 2>/dev/null); after_unlock_rc=$?; set -e
  lock_restore_rc=0; new_lock='{}'
  if [[ "$after_unlock_rc" -ne 0 || $(jq 'length' <<<"${after_unlock:-[]}" 2>/dev/null || echo -1) -eq 0 ]]; then
    set +e; new_lock=$(azc lock create --name "$lock_name" --lock-type CanNotDelete --resource "$id" --notes "$lock_notes" -o json 2>/dev/null); lock_restore_rc=$?; set -e
    printf '%s\n' "${new_lock:-{}}" >"$HERE/recreated-lock-$kind.json"
  fi
  set +e; final_snap=$(azc snapshot show --ids "$id" -o json 2>/dev/null); snap_rc=$?; final_locks=$(azc lock list --resource "$id" -o json 2>/dev/null); locks_rc=$?; final_rows=$(azc role assignment list --scope "$id" -o json 2>/dev/null); rows_rc=$?; set -e
  [[ "$snap_rc" -eq 0 ]] || final_snap='{}'; [[ "$locks_rc" -eq 0 ]] || final_locks='[]'; [[ "$rows_rc" -eq 0 ]] || final_rows='[]'
  jq -e --arg id "${id,,}" --arg u "${expected_uid,,}" --arg src "${expected_source,,}" '(.id|ascii_downcase)==$id and (.uniqueId|ascii_downcase)==$u and (.creationData.sourceResourceId|ascii_downcase)==$src and .provisioningState=="Succeeded" and .snapshotAccessState=="Available"' <<<"$final_snap" >/dev/null || { echo "$kind snapshot identity not unchanged after temporary lock administration" >&2; exit 1; }
  jq -e --arg id "${id,,}" --arg name "$lock_name" --arg notes "$lock_notes" 'length==1 and .[0].name==$name and .[0].level=="CanNotDelete" and .[0].notes==$notes and ((.[0].id|ascii_downcase)|startswith($id+"/providers/microsoft.authorization/locks/"))' <<<"$final_locks" >/dev/null || { echo "$kind snapshot lock restore failed; snapshot preserved but immediate manual attention is required" >&2; exit 1; }
  LOCK_ARMED=false; ACTIVE_ID=''; ACTIVE_LOCK_NAME=''; ACTIVE_LOCK_NOTES=''
  if jq -e --arg p "$principal" --arg role "${ROLE_ID,,}" --arg scope "${id,,}" 'any(.[];.principalId==$p and (.roleDefinitionId|ascii_downcase|endswith($role)) and (.scope|ascii_downcase)==$scope)' <<<"$final_rows" >/dev/null; then assignment_absent=false; else assignment_absent=true; fi
  at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  rec=$(jq -cn --argjson base "$base" --arg at "$at" --argjson unlock "$unlock_rc" --argjson del "$assignment_rc" --argjson create "$lock_restore_rc" --argjson absent "$assignment_absent" --argjson lockAfter "$(jq -c '.[0]' <<<"$final_locks")" --argjson snapshotAfter "$final_snap" '{kind:$base.kind,snapshotId:$base.snapshotId,uniqueId:$base.uniqueId,roleAssignmentId:$base.roleAssignmentId,lockBefore:$base.originalLock,temporaryUnlockExitCode:$unlock,roleAssignmentDeleteExitCode:$del,roleAssignmentAbsent:$absent,lockRestoreExitCode:$create,restoredLock:$lockAfter,snapshotAfter:{id:$snapshotAfter.id,uniqueId:$snapshotAfter.uniqueId,provisioningState:$snapshotAfter.provisioningState,snapshotAccessState:$snapshotAfter.snapshotAccessState},verifiedAtUtc:$at}')
  jq --arg kind "$kind" --argjson rec "$rec" '.roles += [$rec]' "$RECEIPT" >"$RECEIPT.tmp"; mv -f "$RECEIPT.tmp" "$RECEIPT"
  [[ "$unlock_rc" -eq 0 && "$assignment_rc" -eq 0 && "$assignment_absent" == true && "$lock_restore_rc" -eq 0 ]] || { echo "$kind temporary lock/grant sequence incomplete; restored lock is verified and receipt preserved" >&2; exit 1; }
  jq --arg kind "$kind" --arg at "$at" --argjson rc "$assignment_rc" --argjson lifecycle "$rec" '.assignments|=map(if .kind==$kind then .removed=true|.removedAtUtc=$at|.deleteExitCode=$rc|.postRemovalRoleListAbsent=true|.absenceConfirmed=true|.temporaryLockLifecycle=$lifecycle else . end)|.status="removing_pair"|.completedAtUtc=$at' "$ROLE" >"$ROLE.tmp"; mv -f "$ROLE.tmp" "$ROLE"
done
jq -e '(.assignments|length)==2 and all(.assignments[];.createdByThisStack and .removed==true and .deleteExitCode==0 and .absenceConfirmed==true and .postRemovalRoleListAbsent==true) ' "$ROLE" >/dev/null || { echo 'paired role assignment lifecycle proof incomplete' >&2; exit 1; }
jq '.postRemovalConfirmed=true|.status="removed"' "$ROLE" >"$ROLE.tmp"; mv -f "$ROLE.tmp" "$ROLE"
jq --argjson lifecycle "$(cat "$ROLE")" '.sourceSnapshotRoleAssignments.lifecycle=$lifecycle' "$PROOF" >"$PROOF.tmp"; mv -f "$PROOF.tmp" "$PROOF"
echo "Removed only the two exact source grants and restored both original CanNotDelete locks. Receipt: $RECEIPT"
