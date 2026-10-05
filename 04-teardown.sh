#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"; SD="$ROOT/.state"; SF="$SD/state.json"
die(){ printf 'ERROR: %s\n' "$*" >&2; exit 1; }
for x in az jq timeout flock sleep; do command -v "$x" >/dev/null || die "Missing $x"; done
[[ -f "$SF" ]] || die 'No saved ownership state; refusing to guess resources.'
jq -e '(.version==1) and (.subscriptionId|type=="string" and length>0) and (.resourceGroup|type=="string" and length>0) and (.clusterName|type=="string" and length>0) and (.ownerToken|type=="string" and test("^[a-f0-9]{32}$")) and (.nodeResourceGroup|type=="string") and (.requestedNodeResourceGroup|type=="string" and length>0) and (.kubeconfigPath|type=="string" and length>0) and (.networkWatcherBaseline.ids|type=="array")' "$SF" >/dev/null || die 'Invalid ownership state.'
SUB_ENV="${SUB:-}"; SUB="$(jq -r .subscriptionId "$SF")"; RG="$(jq -r .resourceGroup "$SF")"; AKS="$(jq -r .clusterName "$SF")"; NODE_RG="$(jq -r .nodeResourceGroup "$SF")"
REQUESTED="$(jq -r .requestedNodeResourceGroup "$SF")"; TOKEN="$(jq -r .ownerToken "$SF")"; LOC="$(jq -r .location "$SF")"
[[ -z "$SUB_ENV" || "${SUB_ENV,,}" == "${SUB,,}" ]] || die 'SUB differs from saved subscription.'
[[ "$RG" == "rg-vacuum-only-$TOKEN" && "$AKS" == "aks-vacuum-only-${TOKEN:0:12}" && "$REQUESTED" == "rg-vacuum-nodes-$TOKEN" && ( -z "$NODE_RG" || "${NODE_RG,,}" == "${REQUESTED,,}" ) && "$(jq -r .kubeconfigPath "$SF")" == "$SD/kubeconfig" ]] || die 'Resource names or kubeconfig path do not match stack ownership.'
mkdir -p "$SD"; chmod 700 "$SD"; exec 9>"$SD/teardown.lock"; flock -n 9 || die 'Teardown already running.'; exec 7>"$SD/test.lock"; flock -n 7 || die 'Measurement running; let VACUUM finish before teardown.'
exec 8>"$SD/02-prepare.lock"; flock -n 8 || die 'Snapshot preparation is still running.'
azc(){ timeout --foreground --kill-after=15s 240s az "$@" --subscription "$SUB"; }
azlong(){ timeout --foreground --kill-after=15s 1800s az "$@" --subscription "$SUB"; }
active="$(azc account show --query id -o tsv)" || die 'Cannot inspect active subscription.'
[[ "${active,,}" == "${SUB,,}" ]] || die 'Active subscription differs from saved state.'
group_exists(){ local v; v="$(azc group exists -n "$1" -o tsv)" || die "Cannot inspect RG $1."; case "$v" in true)return 0;; false)return 1;; *) die 'Unexpected group-exists response.';; esac; }
assert_snapshotless_rg(){
  local rg="$1" resources
  group_exists "$rg" || return 0
  resources="$(azc resource list -g "$rg" -o json)" || die "Cannot inventory RG $rg before cleanup."
  jq -e 'type=="array"' <<<"$resources" >/dev/null || die "Unexpected resource inventory for RG $rg."
  if jq -e 'any(.[]; ((.type // "" | ascii_downcase) | test("snapshot|restorepoint")))' <<<"$resources" >/dev/null; then
    printf 'Refusing cleanup: snapshot resources remain in %s:\n' "$rg" >&2
    jq -r '.[] | select(((.type // "" | ascii_downcase) | test("snapshot|restorepoint"))) | "  \(.type) \(.id)"' <<<"$resources" >&2
    die 'Preserving every snapshot. Move or retain the owning resource group until snapshot deletion is explicitly authorized.'
  fi
}
KEEPER_ID=''; KEEPER_TOKEN=''; KEEPER_RG=''; KEEPER_MISSING=false; FALLBACK_STATE=none; OWNED_SNAPSHOT="$SD/ownedSnapshot.json"
check_fallback(){
  [[ -f "$OWNED_SNAPSHOT" ]] || return 0
  if [[ "$KEEPER_MISSING" == false ]] && jq -e --arg id "${KEEPER_ID,,}" --arg t "$KEEPER_TOKEN" '(.id|ascii_downcase)==$id and .ownerToken==$t and .createdBy=="vacuum-only-repro/02-prepare.sh"' "$OWNED_SNAPSHOT" >/dev/null; then return 0; fi
  local short srg sid source_disk fallback_rg err snap pending end
  short="${TOKEN:0:8}"; srg="rg-vacuum-source-$short"; sid="/subscriptions/$SUB/resourceGroups/$srg/providers/Microsoft.Compute/snapshots/prepared-$short"
  jq -e --arg t "$TOKEN" --arg s "$SUB" --arg l "${LOC,,}" --arg rg "$srg" --arg id "${sid,,}" \
    '(.version==1) and (.status=="creating" or .status=="retained" or .status=="prepared") and .ownerToken==$t and .subscriptionId==$s and .createdBy=="vacuum-only-repro/02-prepare.sh" and .resourceGroup==$rg and ((.location|ascii_downcase)==$l) and ((.id|ascii_downcase)==$id) and ((.sourceDiskId==null) or (.sourceDiskId|type=="string" and length>0))' "$OWNED_SNAPSHOT" >/dev/null || die 'Fallback snapshot intent does not match this stack.'
  source_disk="$(jq -r '.sourceDiskId // empty' "$OWNED_SNAPSHOT")"
  if [[ -n "$source_disk" ]]; then [[ "${source_disk,,}" == "/subscriptions/${SUB,,}/resourcegroups/${REQUESTED,,}/providers/microsoft.compute/disks/"* ]] || die 'Fallback source disk is outside this stack node RG.'; fi
  if group_exists "$srg"; then
    fallback_rg="$(azc group show -n "$srg" -o json)" || die 'Cannot inspect fallback snapshot RG.'
    jq -e --arg t "$TOKEN" --arg rg "$srg" --arg l "${LOC,,}" '.name==$rg and ((.location|ascii_downcase)==$l) and .tags.reprotoken==$t and .tags["managed-by"]=="vacuum-only-repro" and .tags.reproPurpose=="retained-source-snapshot"' <<<"$fallback_rg" >/dev/null || die 'Fallback snapshot RG ownership did not verify.'
  fi
  err="$(mktemp "$SD/.fallback.XXXXXX")"; chmod 600 "$err"
  if snap="$(azc snapshot show --ids "$sid" -o json 2>"$err")"; then
    jq -e --arg id "${sid,,}" --arg t "$TOKEN" --arg l "${LOC,,}" '((.id|ascii_downcase)==$id) and ((.location|ascii_downcase)==$l) and .tags.reprotoken==$t and .tags.reproPurpose=="retained-primary-pg-snapshot"' <<<"$snap" >/dev/null || die 'Fallback snapshot identity or ownership tags did not verify.'
    end=$((SECONDS+14400))
    while :; do
      pending="$(jq -r '(.provisioningState // "") as $p | ($p=="Creating" or $p=="Updating" or $p=="Accepted" or $p=="Moving" or ($p=="Succeeded" and (((.completionPercent|tonumber? // 100)<100) or ((.snapshotAccessState // "Available")=="Pending"))))' <<<"$snap")"
      [[ "$pending" == true ]] || break
      ((SECONDS<end)) || die 'Fallback snapshot operation is still active; retry teardown after it settles.'
      sleep 15
      if snap="$(azc snapshot show --ids "$sid" -o json 2>"$err")"; then
        jq -e --arg id "${sid,,}" --arg t "$TOKEN" --arg l "${LOC,,}" '((.id|ascii_downcase)==$id) and ((.location|ascii_downcase)==$l) and .tags.reprotoken==$t and .tags.reproPurpose=="retained-primary-pg-snapshot"' <<<"$snap" >/dev/null || die 'Fallback snapshot identity or ownership changed while pending.'
      elif grep -Eiq 'ResourceNotFound|SnapshotNotFound|NotFound|could not be found|404' "$err"; then snap=''; FALLBACK_STATE=missing; break
      else cat "$err" >&2; rm -f "$err"; die 'Cannot check whether fallback snapshot work has settled.'; fi
    done
    if [[ -n "$snap" ]]; then
      if jq -e '(.provisioningState=="Succeeded") and (.diskSizeGB // .diskSizeGb)==512 and .sku.name=="Standard_LRS" and .incremental==false and ((.snapshotAccessState // "Available")=="Available")' <<<"$snap" >/dev/null; then FALLBACK_STATE=ready
      else FALLBACK_STATE=preserved; fi
    fi
  elif grep -Eiq 'ResourceNotFound|SnapshotNotFound|NotFound|could not be found|404' "$err"; then FALLBACK_STATE=missing
  else cat "$err" >&2; rm -f "$err"; die 'Cannot determine whether fallback snapshot work is active.'; fi
  rm -f "$err"
}
verify_keeper(){
  KEEPER_ID=''; KEEPER_TOKEN=''; KEEPER_RG=''; KEEPER_MISSING=false
  if [[ -f "$ROOT/snapshot.json" ]]; then
    jq -e 'type=="object"' "$ROOT/snapshot.json" >/dev/null || die 'snapshot.json is not valid metadata.'
    KEEPER_ID="$(jq -r '.snapshotId // empty' "$ROOT/snapshot.json")"; KEEPER_TOKEN="$(jq -r '.keeperOwnerToken // empty' "$ROOT/snapshot.json")"; KEEPER_RG="$(jq -r '.resourceGroup // empty' "$ROOT/snapshot.json")"
  fi
  if [[ -z "$KEEPER_ID" ]]; then KEEPER_MISSING=true
  else
    [[ -n "$KEEPER_RG" && "${KEEPER_RG,,}" != "${RG,,}" && "${KEEPER_RG,,}" != "${REQUESTED,,}" && "${KEEPER_ID,,}" == "/subscriptions/${SUB,,}/resourcegroups/${KEEPER_RG,,}/providers/microsoft.compute/snapshots/"* ]] || die 'Pinned keeper ID is invalid or overlaps the stack.'
    local err k; err="$(mktemp "$SD/.keeper.XXXXXX")"; chmod 600 "$err"
    if k="$(azc snapshot show --ids "$KEEPER_ID" -o json 2>"$err")"; then
      rm -f "$err"
      [[ -n "$KEEPER_TOKEN" ]] || die 'Keeper exists but its ownership token is absent.'
      jq -e --arg id "${KEEPER_ID,,}" --arg t "$KEEPER_TOKEN" '(.id|ascii_downcase)==$id and .provisioningState=="Succeeded" and (.diskSizeGB // .diskSizeGb)==512 and .sku.name=="Standard_LRS" and .incremental==false and .tags.reprotoken==$t and .tags.reproPurpose=="retained-primary-pg-snapshot"' <<<"$k" >/dev/null || die 'Retained keeper snapshot identity or ownership did not verify.'
    elif grep -Eiq 'ResourceNotFound|SnapshotNotFound|NotFound|could not be found|404' "$err"; then rm -f "$err"; KEEPER_MISSING=true
    else cat "$err" >&2; rm -f "$err"; die 'Cannot determine whether the retained keeper is missing.'; fi
  fi
  check_fallback
}
verify_keeper
show_aks(){ local err out; err="$(mktemp "$SD/.aks.XXXXXX")"; chmod 600 "$err"; if out="$(azc aks show -g "$RG" -n "$AKS" -o json 2>"$err")"; then rm -f "$err"; printf '%s' "$out"; elif grep -Eiq 'ResourceNotFound|NotFound|could not be found' "$err"; then rm -f "$err"; else cat "$err" >&2; rm -f "$err"; die 'Cannot inspect AKS.'; fi; }
wait_aks_gone(){ local end=$((SECONDS+3600)) a; while ((SECONDS<end)); do a="$(show_aks)"; [[ -z "$a" ]] && return 0; jq -e --arg t "$TOKEN" '.tags.reprotoken==$t' <<<"$a" >/dev/null || die 'AKS ownership changed during deletion.'; sleep 15; done; die 'AKS deletion timed out.'; }
wait_rg_gone(){ local g="$1" end=$((SECONDS+3600)); while group_exists "$g"; do ((SECONDS<end)) || die "RG deletion timed out: $g"; sleep 15; done; }
save_node(){ local tmp; tmp="$(mktemp "$SD/.state.XXXXXX")"; jq --arg n "$1" '.nodeResourceGroup=$n' "$SF" >"$tmp"; chmod 600 "$tmp"; mv -f "$tmp" "$SF"; }
WATCHER_CHANGED=false; KEEP_WATCHER=false; NW_OWNED="$(jq -r '.networkWatcher.owned // false' "$SF")"; NW_ID="$(jq -r '.networkWatcher.id // empty' "$SF")"; NW_RG="$(jq -r '.networkWatcher.resourceGroup // empty' "$SF")"; NW_NAME="$(jq -r '.networkWatcher.name // empty' "$SF")"
if [[ "$NW_OWNED" == true ]]; then jq -e --arg t "$TOKEN" --arg l "${LOC,,}" '(.networkWatcher.status=="creating" or .networkWatcher.status=="ready") and .networkWatcher.ownerToken==$t and ((.networkWatcher.location|ascii_downcase)==$l) and .networkWatcher.createdBy=="vacuum-only-repro/01-provision.sh" and (.networkWatcherBaseline.ids|length)==0 and ((.networkWatcher.etag==null) or (.networkWatcher.etag|type=="string" and length>0))' "$SF" >/dev/null || die 'Owned watcher intent or location metadata is invalid.'; fi
watchers_changed(){ local all own; all="$(azc network watcher list -o json)" || die 'Cannot inspect regional Network Watchers.'; own=''; [[ "$NW_OWNED" == true ]] && own="${NW_ID,,}"; jq -n -e --arg loc "${LOC,,}" --argjson base "$(jq -c '.networkWatcherBaseline.ids' "$SF")" --argjson all "$all" --arg own "$own" '[ $all[]|select((.location|ascii_downcase)==$loc)|(.id|ascii_downcase)] as $a | ([ $base[]|ascii_downcase ] + (if ($own!="" and ($a|index($own))!=null) then [$own] else [] end) | unique) as $e | (($a|length)==($e|length)) and all($e[]; . as $id | ($a|index($id))!=null)' >/dev/null; }
jq -e --arg loc "${LOC,,}" '(.networkWatcherBaseline.location|ascii_downcase)==$loc and (.networkWatcherBaseline.ids|type=="array")' "$SF" >/dev/null || die 'Network Watcher baseline does not match stack location.'
watchers_changed || WATCHER_CHANGED=true
# AKS deletion and resource-group deletion can cascade into child snapshots.
# Inventory every group this script might remove before making any Azure change.
assert_snapshotless_rg "$RG"
assert_snapshotless_rg "${NODE_RG:-$REQUESTED}"
if [[ "$NW_OWNED" == true ]]; then assert_snapshotless_rg "$NW_RG"; fi
if [[ -f "$SD/storage.json" ]]; then
  jq -e --arg s "$SUB" --arg r "$RG" --arg t "$TOKEN" --arg a "cnpgwal${TOKEN:0:17}" --arg c "wal-${TOKEN:0:12}" '(.version==1) and (.subscriptionId==$s) and (.resourceGroup==$r) and (.ownerToken==$t) and (.accountName==$a) and (.containerName==$c) and (.status=="creating" or .status=="ready" or .status=="accountReady")' "$SD/storage.json" >/dev/null || die 'WAL storage metadata does not match this stack.'
  if [[ "$(jq -r .status "$SD/storage.json")" == creating ]]; then
    err="$(mktemp "$SD/.storage.XXXXXX")"; chmod 600 "$err"; end=$((SECONDS+2400)); settled=false
    while ((SECONDS<end)); do
      if st="$(azc storage account show -g "$RG" -n "cnpgwal${TOKEN:0:17}" -o json 2>"$err")"; then
        case "$(jq -r '.provisioningState // empty' <<<"$st")" in
          Succeeded) jq -e --arg t "$TOKEN" --arg n "cnpgwal${TOKEN:0:17}" --arg id "/subscriptions/${SUB,,}/resourcegroups/${RG,,}/providers/microsoft.storage/storageaccounts/cnpgwal${TOKEN:0:17}" '(.id|ascii_downcase)==$id and (.name|ascii_downcase)==$n and .tags.reprotoken==$t and .allowBlobPublicAccess==false and .enableHttpsTrafficOnly==true' <<<"$st" >/dev/null || die 'WAL account identity/security did not verify.'; settled=true; break ;;
          Creating|Updating|Accepted|ResolvingDns|ResolvingDNS|Moving|Deleting) ;;
          Failed|Canceled|Cancelled|Deleted) settled=true; break ;;
          *) die 'Unknown WAL account state; preserving resources.' ;;
        esac
      elif grep -Eiq 'ResourceNotFound|NotFound|OperationInProgress|StorageAccountOperationInProgress' "$err"; then :
      else cat "$err" >&2; rm -f "$err"; die 'Cannot inspect WAL account operation.'; fi
      sleep 10
    done
    rm -f "$err"; [[ "$settled" == true ]] || die 'WAL account operation is unresolved; retry teardown.'
  fi
fi
if group_exists "$RG"; then
  r="$(azc group show -n "$RG" -o json)" || die 'Cannot inspect test RG.'; jq -e --arg t "$TOKEN" '.tags.reprotoken==$t' <<<"$r" >/dev/null || die 'Test RG tag mismatch.'
  a="$(show_aks)"
  if [[ -n "$a" ]]; then
    jq -e --arg t "$TOKEN" --arg n "$REQUESTED" '.tags.reprotoken==$t and .nodeResourceGroup==$n' <<<"$a" >/dev/null || die 'AKS owner or Azure-returned node RG differs.'
    NODE_RG="$REQUESTED"; save_node "$NODE_RG"
    if group_exists "$NODE_RG"; then
      n="$(azc group show -n "$NODE_RG" -o json)" || die 'Cannot inspect associated node RG.'; tag="$(jq -r '.tags.reprotoken // empty' <<<"$n")"
      [[ -z "$tag" || "$tag" == "$TOKEN" ]] || die 'Node RG has a different owner.'
      if [[ -z "$tag" ]]; then azc group update -n "$NODE_RG" --set "tags.reprotoken=$TOKEN" -o none || die 'Cannot tag Azure-associated node RG.'; n="$(azc group show -n "$NODE_RG" -o json)"; jq -e --arg t "$TOKEN" '.tags.reprotoken==$t' <<<"$n" >/dev/null || die 'Node RG tag did not verify.'; fi
    fi
    state="$(jq -r '.provisioningState // empty' <<<"$a")"; end=$((SECONDS+5400))
    while [[ "$state" == Creating || "$state" == Updating ]]; do ((SECONDS<end)) || die 'AKS operation timed out.'; sleep 20; a="$(show_aks)"; [[ -n "$a" ]] || break; jq -e --arg t "$TOKEN" --arg n "$REQUESTED" '.tags.reprotoken==$t and .nodeResourceGroup==$n' <<<"$a" >/dev/null || die 'AKS association changed.'; state="$(jq -r '.provisioningState // empty' <<<"$a")"; done
    if [[ -n "$a" ]]; then if [[ "$state" == Deleting ]]; then wait_aks_gone; else [[ "$state" == Succeeded || "$state" == Failed || "$state" == Canceled || "$state" == Cancelled ]] || die "Unknown AKS state '$state'."; assert_snapshotless_rg "$NODE_RG"; azlong aks delete -g "$RG" -n "$AKS" --yes --no-wait -o none || die 'AKS delete failed.'; wait_aks_gone; fi; fi
  fi
fi
if [[ -z "$NODE_RG" ]]; then if group_exists "$REQUESTED"; then die 'Unrecorded node RG exists without Azure AKS association; refusing to guess.'; fi
elif group_exists "$NODE_RG"; then [[ "${NODE_RG,,}" == "${REQUESTED,,}" ]] || die 'Recorded node RG name mismatch.'; n="$(azc group show -n "$NODE_RG" -o json)" || die 'Cannot inspect node RG.'; jq -e --arg t "$TOKEN" '.tags.reprotoken==$t' <<<"$n" >/dev/null || die 'Node RG owner tag mismatch.'; assert_snapshotless_rg "$NODE_RG"; azlong group delete -n "$NODE_RG" --yes --no-wait -o none || die 'Node RG delete failed.'; wait_rg_gone "$NODE_RG"; fi
if group_exists "$RG"; then r="$(azc group show -n "$RG" -o json)" || die 'Cannot recheck test RG.'; jq -e --arg t "$TOKEN" '.tags.reprotoken==$t' <<<"$r" >/dev/null || die 'Test RG owner tag changed.'; assert_snapshotless_rg "$RG"; azlong group delete -n "$RG" --yes --no-wait -o none || die 'Test RG delete failed.'; wait_rg_gone "$RG"; fi
if [[ "$NW_OWNED" == true ]]; then
  if [[ "$WATCHER_CHANGED" == true ]]; then KEEP_WATCHER=true; printf 'Regional watcher set changed; preserving owned watcher for review.\n' >&2
  else
    akslist="$(azc aks list -o json)" || die 'Cannot inspect regional AKS users.'; vnets="$(azc network vnet list -o json)" || die 'Cannot inspect regional VNet users.'; groups="$(azc group list --tag managed-by=vacuum-only-repro -o json)" || die 'Cannot inspect sibling repro stacks.'
    if jq -e --arg loc "${LOC,,}" 'any(.[];(.location|ascii_downcase)==$loc)' <<<"$akslist" >/dev/null || jq -e --arg loc "${LOC,,}" 'any(.[];(.location|ascii_downcase)==$loc)' <<<"$vnets" >/dev/null || jq -e --arg loc "${LOC,,}" --arg rg "${RG,,}" --arg node "${REQUESTED,,}" --arg nw "${NW_RG,,}" 'any(.[];(.location|ascii_downcase)==$loc and ((.name|ascii_downcase)!=$rg) and ((.name|ascii_downcase)!=$node) and ((.name|ascii_downcase)!=$nw))' <<<"$groups" >/dev/null; then
      KEEP_WATCHER=true; printf 'Another regional AKS/VNet/repro stack remains; watcher RG and state are retained for a later 04 retry.\n' >&2
    else
      [[ "${NW_RG,,}" == "rg-vacuum-watcher-${TOKEN,,}" && "$NW_NAME" == "vacuum-watcher-${TOKEN:0:12}" && "${NW_ID,,}" == "/subscriptions/${SUB,,}/resourcegroups/${NW_RG,,}/providers/microsoft.network/networkwatchers/${NW_NAME,,}" && "$(jq -r .networkWatcher.ownerToken "$SF")" == "$TOKEN" ]] || die 'Owned watcher state does not match its token-scoped resource name.'
      if group_exists "$NW_RG"; then
        wrg="$(azc group show -n "$NW_RG" -o json)" || die 'Cannot inspect owned watcher RG.'; jq -e --arg t "$TOKEN" '.tags.reprotoken==$t and .tags.reproPurpose=="owned-network-watcher" and .tags["managed-by"]=="vacuum-only-repro"' <<<"$wrg" >/dev/null || die 'Owned watcher RG tag mismatch.'
        assert_snapshotless_rg "$NW_RG"
        err="$(mktemp "$SD/.watcher.XXXXXX")"; chmod 600 "$err"; watcher_gone=false
        if w="$(azc resource show --ids "$NW_ID" -o json 2>"$err")"; then
          current_etag="$(jq -r '.etag // empty' <<<"$w")"
          jq -e --arg id "${NW_ID,,}" --arg l "${LOC,,}" --arg t "$TOKEN" '(.id|ascii_downcase)==$id and (.type|ascii_downcase)=="microsoft.network/networkwatchers" and (.location|ascii_downcase)==$l and .properties.provisioningState=="Succeeded" and ((.properties.runningOperationIds // [])|type=="array" and length==0) and .tags.reprotoken==$t and .tags.reproPurpose=="owned-vacuum-repro-watcher" and (.etag|type=="string" and length>0)' <<<"$w" >/dev/null || die 'Owned watcher identity, tags, operation state, or ETag did not verify.'
          resources="$(azc resource list -g "$NW_RG" -o json)" || die 'Cannot inspect watcher RG children.'; jq -e --arg id "${NW_ID,,}" '([.[]|select((.id|ascii_downcase)==$id)]|length)==1 and ([.[]|select((.id|ascii_downcase|startswith($id+"/")))]|length)==0' <<<"$resources" >/dev/null || die 'Owned watcher RG contains unknown resources.'
          for child in connectionMonitors flowLogs packetCaptures; do children="$(azc rest --method get --url "https://management.azure.com${NW_ID}/${child}?api-version=2025-09-01" -o json)" || die "Cannot inspect watcher $child."; jq -e '(.value|type=="array") and (.value|length==0) and ((.nextLink//null)==null)' <<<"$children" >/dev/null || die "Watcher has $child children."; done
          azlong rest --method delete --url "https://management.azure.com${NW_ID}?api-version=2025-09-01" --headers "If-Match=$current_etag" -o none || die 'Conditional owned watcher delete failed.'
          end=$((SECONDS+3600)); while ((SECONDS<end)); do if azc resource show --ids "$NW_ID" -o none 2>"$err"; then sleep 15; elif grep -Eiq 'ResourceNotFound|NotFound|could not be found' "$err"; then watcher_gone=true; break; else cat "$err" >&2; die 'Cannot verify owned watcher deletion.'; fi; done
          [[ "$watcher_gone" == true ]] || die 'Owned watcher deletion timed out.'
        elif grep -Eiq 'ResourceNotFound|NotFound|could not be found' "$err"; then watcher_gone=true
        else cat "$err" >&2; die 'Cannot inspect owned watcher.'; fi
        rm -f "$err"
        resources="$(azc resource list -g "$NW_RG" -o json)" || die 'Cannot verify empty watcher RG.'; [[ "$(jq length <<<"$resources")" == 0 ]] || die 'Watcher RG is not empty.'
        azlong group delete -n "$NW_RG" --yes --no-wait -o none || die 'Owned watcher RG delete failed.'; wait_rg_gone "$NW_RG"
      fi
    fi
  fi
fi
verify_keeper
if [[ "$KEEPER_MISSING" == false && -f "$OWNED_SNAPSHOT" ]] && jq -e --arg id "${KEEPER_ID,,}" --arg t "$KEEPER_TOKEN" '(.id|ascii_downcase)==$id and .ownerToken==$t' "$OWNED_SNAPSHOT" >/dev/null; then rm -f -- "$OWNED_SNAPSHOT"; fi
rm -f -- "$SD/kubeconfig" "$SD/storage.json" "$SD/storage-container.json" "$SD/prepare.json" "$SD"/.connection.*
if [[ "$KEEP_WATCHER" == true || "$WATCHER_CHANGED" == true ]]; then die 'Test AKS/storage resources removed; watcher cleanup is deferred and ownership state is retained.'; fi
if [[ -f "$OWNED_SNAPSHOT" ]]; then
  printf 'Owned stack removed; unpromoted fallback snapshot intent and recovery state retained in %s.\n' "$SD"
else
  rm -f -- "$SF"; rmdir "$SD" 2>/dev/null || true
fi
if [[ "$KEEPER_MISSING" == true ]]; then
  case "$FALLBACK_STATE" in
    ready) printf 'Owned stack removed; pinned keeper was missing, verified fallback snapshot preserved.\n' ;;
    preserved) printf 'Owned stack removed; keeper was missing; fallback resources preserved for recovery.\n' ;;
    *) printf 'Owned stack removed; no keeper snapshot was present.\n' ;;
  esac
else
  printf 'Owned AKS, node RG, and test RG removed; retained keeper snapshot verified.\n'
fi
