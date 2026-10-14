#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SD="$ROOT/.state"; SF="$SD/state.json"; KC="$SD/kubeconfig"
SUB="${SUB:-00000000-0000-0000-0000-000000000000}"; LOCATION="${LOCATION:-eastus}"
NODE_VM_SIZE="${NODE_VM_SIZE:-Standard_D2s_v4}"
CNPG="https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/v1.30.1/releases/cnpg-1.30.1.yaml"
die(){ printf 'ERROR: %s\n' "$*" >&2; exit 1; }
azc(){ timeout --foreground --kill-after=15s 240s az "$@" --subscription "$SUB"; }
azlong(){ timeout --foreground --kill-after=15s 1800s az "$@" --subscription "$SUB"; }
for x in az kubectl jq timeout flock od tr ssh-keygen base64; do command -v "$x" >/dev/null || die "Missing $x"; done
[[ "$SUB" =~ ^[0-9a-fA-F-]{36}$ && "$LOCATION" =~ ^[A-Za-z0-9-]+$ && "$NODE_VM_SIZE" =~ ^Standard_[A-Za-z0-9_]+$ ]] || die 'Invalid subscription, location, or VM size.'
mkdir -p "$SD"; chmod 700 "$SD"; exec 9>"$SD/provision.lock"; flock -n 9 || die 'Provision already running.'
[[ ! -e "$SF" && ! -e "$KC" ]] || die 'State or kubeconfig already exists; refusing adoption.'
active="$(azc account show --query id -o tsv)" || die 'Cannot inspect active subscription.'
[[ "${active,,}" == "${SUB,,}" ]] || die 'Active Azure subscription differs from SUB.'
[[ "$NODE_VM_SIZE" == Standard_D2s_v4 ]] || die 'This repeat requires the original Standard_D2s_v4 size.'
if [[ -n "${PREFLIGHT_SKU_FILE:-}" ]]; then sku="$(cat "$PREFLIGHT_SKU_FILE")"; else sku="$(azc vm list-skus -l "$LOCATION" --size "$NODE_VM_SIZE" --all -o json)"; fi
jq -e --arg n "$NODE_VM_SIZE" 'any(.[]; .name==$n and ([.restrictions[]?|select(.type=="Location")]|length)==0 and any(.capabilities[]; .name=="vCPUs" and (.value|tonumber)==2))' <<<"$sku" >/dev/null || die 'The selected D2s_v4 SKU is unavailable for this subscription and region.'
FAMILY="$(jq -er --arg n "$NODE_VM_SIZE" '[.[]|select(.name==$n)|.family]|unique|if length==1 then .[0] else error("Ambiguous SKU family") end' <<<"$sku")"
usage="$(azc vm list-usage -l "$LOCATION" -o json)"
jq -e --arg family "${FAMILY,,}" 'any(.[]; .name.value=="cores" and ((.limit|tonumber)-(.currentValue|tonumber))>=4) and any(.[]; (.name.value|ascii_downcase)==$family and ((.limit|tonumber)-(.currentValue|tonumber))>=4)' <<<"$usage" >/dev/null || die 'This stack needs 4 free regional and selected-family vCPUs.'
printf '%s\n' "$usage" >"$SD/quota-at-provision.json"
printf '%s\n' "$sku" >"$SD/sku-at-provision.json"
exec 8>"$ROOT/../shared-watcher.lock"; flock 8
WATCHERS="$(azc network watcher list -o json)" || die 'Cannot capture regional Network Watcher baseline.'
BASELINE="$(jq -cn --arg loc "$LOCATION" --argjson all "$WATCHERS" '{location:$loc,ids:[$all[]|select((.location|ascii_downcase)==($loc|ascii_downcase))|.id]}')"
TOKEN="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"; [[ "$TOKEN" =~ ^[a-f0-9]{32}$ ]] || die 'Cannot generate owner token.'
RG="rg-vacuum-only-$TOKEN"; AKS="aks-vacuum-only-${TOKEN:0:12}"; REQUESTED_NODE_RG="rg-vacuum-nodes-$TOKEN"
SA="cnpgwal${TOKEN:0:17}"; CONTAINER="wal-${TOKEN:0:12}"; STORAGE_PENDING=false; SECRET_FILE=''
NW_OWNED=false; NW_RG="rg-vacuum-watcher-$TOKEN"; NW_NAME="vacuum-watcher-${TOKEN:0:12}"; NW_ID="/subscriptions/$SUB/resourceGroups/$NW_RG/providers/Microsoft.Network/networkWatchers/$NW_NAME"; NW_STATE=null
if [[ "$(jq '.ids|length' <<<"$BASELINE")" == 0 ]]; then
  NW_OWNED=true
  NW_STATE="$(jq -cn --arg id "$NW_ID" --arg rg "$NW_RG" --arg name "$NW_NAME" --arg loc "$LOCATION" --arg t "$TOKEN" '{owned:true,status:"creating",id:$id,resourceGroup:$rg,name:$name,location:$loc,ownerToken:$t,etag:null,createdBy:"vacuum-only-repro/01-provision.sh"}')"
fi
state(){ local status="$1" node="${2:-}" tmp; tmp="$(mktemp "$SD/.state.XXXXXX")"; jq -n --arg sub "$SUB" --arg rg "$RG" --arg aks "$AKS" --arg node "$node" --arg requested "$REQUESTED_NODE_RG" --arg loc "$LOCATION" --arg token "$TOKEN" --arg kc "$KC" --arg status "$status" --arg size "$NODE_VM_SIZE" --arg family "$FAMILY" --argjson baseline "$BASELINE" --argjson watcher "$NW_STATE" '{version:1,subscriptionId:$sub,resourceGroup:$rg,clusterName:$aks,nodeResourceGroup:$node,requestedNodeResourceGroup:$requested,location:$loc,ownerToken:$token,kubeconfigPath:$kc,status:$status,nodeVmSize:$size,nodeVmFamily:$family,networkWatcherBaseline:$baseline} + (if $watcher==null then {} else {networkWatcher:$watcher} end)' >"$tmp"; chmod 600 "$tmp"; mv -f "$tmp" "$SF"; }
storage(){ local status="$1" id="${2:-}" tmp; tmp="$(mktemp "$SD/.storage.XXXXXX")"; jq -n --arg sub "$SUB" --arg rg "$RG" --arg loc "$LOCATION" --arg token "$TOKEN" --arg account "$SA" --arg id "$id" --arg container "$CONTAINER" --arg status "$status" '{version:1,status:$status,subscriptionId:$sub,resourceGroup:$rg,location:$loc,ownerToken:$token,accountName:$account,accountId:(if $id=="" then null else $id end),containerName:$container,publicAccess:null,secret:{namespace:"repro",name:"azure-barman-credentials",key:"connection-string"}}' >"$tmp"; chmod 600 "$tmp"; mv -f "$tmp" "$SD/storage.json"; }
group_exists(){ local b; b="$(azc group exists -n "$1" -o tsv)" || die "Cannot inspect resource group $1."; case "$b" in true)return 0;; false)return 1;; *) die "Unexpected group-exists response.";; esac; }
SSH=''
on_exit(){ local rc=$?; trap - EXIT; [[ -z "$SSH" ]] || rm -rf -- "$SSH"; [[ -z "$SECRET_FILE" ]] || rm -f -- "$SECRET_FILE"; if ((rc)) && [[ "$STORAGE_PENDING" == true ]]; then printf 'Storage creation is unresolved; preserving state for safe retry: %s\n' "$SF" >&2; elif ((rc)) && [[ -f "$SF" ]]; then printf 'Provision failed; attempting owner-verified teardown.\n' >&2; flock -u 9; "$ROOT/04-teardown.sh" || printf 'Cleanup incomplete; retry 04-teardown.sh.\n' >&2; fi; exit "$rc"; }
trap on_exit EXIT
if group_exists "$RG"; then die 'Generated test RG already exists.'; fi
if group_exists "$REQUESTED_NODE_RG"; then die 'Generated node RG already exists.'; fi
if [[ "$NW_OWNED" == true ]] && group_exists "$NW_RG"; then die 'Generated watcher RG already exists.'; fi
state provisioning
rgj="$(azc group create -n "$RG" -l "$LOCATION" --tags managed-by=vacuum-only-repro "reprotoken=$TOKEN" -o json)" || die 'Cannot create test RG.'
jq -e --arg t "$TOKEN" '.tags.reprotoken==$t' <<<"$rgj" >/dev/null || die 'Test RG ownership tag did not verify.'
if [[ "$NW_OWNED" == true ]]; then
  nwrg="$(azc group create -n "$NW_RG" -l "$LOCATION" --tags managed-by=vacuum-only-repro reproPurpose=owned-network-watcher "reprotoken=$TOKEN" -o json)" || die 'Cannot create owned watcher RG.'
  jq -e --arg t "$TOKEN" '.tags.reprotoken==$t' <<<"$nwrg" >/dev/null || die 'Watcher RG ownership tag did not verify.'
  body="$(jq -cn --arg l "$LOCATION" --arg t "$TOKEN" '{location:$l,properties:{},tags:{reprotoken:$t,reproPurpose:"owned-vacuum-repro-watcher"}}')"
  azlong rest --method put --url "https://management.azure.com${NW_ID}?api-version=2025-09-01" --body "$body" -o none || die 'Owned Network Watcher create failed.'
  end=$((SECONDS+1200)); watcher_ready=false; err="$(mktemp "$SD/.watcher.XXXXXX")"; chmod 600 "$err"
  while ((SECONDS<end)); do
    if w="$(azc resource show --ids "$NW_ID" -o json 2>"$err")"; then
      if [[ "$(jq -r '.properties.provisioningState // empty' <<<"$w")" == Succeeded ]]; then
        jq -e --arg id "${NW_ID,,}" --arg loc "${LOCATION,,}" --arg t "$TOKEN" '(.id|ascii_downcase)==$id and (.type|ascii_downcase)=="microsoft.network/networkwatchers" and (.location|ascii_downcase)==$loc and .tags.reprotoken==$t' <<<"$w" >/dev/null || die 'Created Network Watcher identity did not verify.'
        etag="$(jq -r '.etag // empty' <<<"$w")"; [[ -n "$etag" ]] || die 'Created Network Watcher has no ETag.'
        NW_STATE="$(jq --arg e "$etag" '.status="ready"|.etag=$e' <<<"$NW_STATE")"; state provisioning; watcher_ready=true; break
      fi
    elif grep -Eiq 'ResourceNotFound|NotFound|OperationInProgress' "$err"; then :
    else cat "$err" >&2; die 'Cannot inspect owned Network Watcher.'; fi
    sleep 10
  done
  rm -f "$err"; [[ "$watcher_ready" == true ]] || die 'Owned Network Watcher readiness timed out.'
fi
flock -u 8
SSH="$(mktemp -d "${TMPDIR:-/tmp}/vacuum-ssh.XXXXXX")"
ssh-keygen -q -t rsa -b 4096 -N '' -C vacuum-only-repro -f "$SSH/id_rsa"
printf 'Submitting isolated two-node AKS (%s) in %s.\n' "$NODE_VM_SIZE" "$LOCATION"
azlong aks create -g "$RG" -n "$AKS" -l "$LOCATION" --node-resource-group "$REQUESTED_NODE_RG" --node-count 2 --node-vm-size "$NODE_VM_SIZE" --kubernetes-version 1.35 --load-balancer-sku standard --tier free --enable-managed-identity --ssh-key-value "$SSH/id_rsa.pub" --tags managed-by=vacuum-only-repro "reprotoken=$TOKEN" --no-wait -o none || die 'AKS create submission failed.'
end=$((SECONDS+5400)); ready=false; err="$(mktemp "$SD/.aks-error.XXXXXX")"; chmod 600 "$err"
while ((SECONDS<end)); do
  if a="$(azc aks show -g "$RG" -n "$AKS" -o json 2>"$err")"; then
    jq -e --arg t "$TOKEN" --arg node "$REQUESTED_NODE_RG" '.tags.reprotoken==$t and .nodeResourceGroup==$node' <<<"$a" >/dev/null || die 'AKS owner tag or Azure-returned node RG differs.'
    n="$(jq -r .nodeResourceGroup <<<"$a")"; state "$(jq -r .provisioningState <<<"$a")" "$n"
    case "$(jq -r .provisioningState <<<"$a")" in Succeeded) ready=true; break;; Creating|Updating) ;; Failed|Canceled|Cancelled) die 'AKS provisioning failed.';; *) die 'AKS returned an unknown provisioning state.';; esac
  elif grep -Eiq 'ResourceNotFound|NotFound|could not be found' "$err"; then :
  else cat "$err" >&2; die 'Cannot inspect AKS provisioning.'; fi
  sleep 20
done
rm -f "$err"; [[ "$ready" == true ]] || die 'AKS provisioning timed out.'
nodej="$(azc group show -n "$REQUESTED_NODE_RG" -o json)" || die 'Azure-associated node RG is missing.'
tag="$(jq -r '.tags.reprotoken // empty' <<<"$nodej")"
[[ -z "$tag" || "$tag" == "$TOKEN" ]] || die 'Node RG already has a different owner tag.'
if [[ -z "$tag" ]]; then azc group update -n "$REQUESTED_NODE_RG" --set "tags.reprotoken=$TOKEN" -o none || die 'Cannot tag the AKS-associated node RG.'; nodej="$(azc group show -n "$REQUESTED_NODE_RG" -o json)"; jq -e --arg t "$TOKEN" '.tags.reprotoken==$t' <<<"$nodej" >/dev/null || die 'Node RG owner tag did not verify.'; fi
state infrastructure-ready "$REQUESTED_NODE_RG"
WATCHERS_AFTER="$(azc network watcher list -o json)" || die 'Cannot check regional Network Watchers after AKS creation.'
jq -n -e --arg loc "${LOCATION,,}" --argjson before "$(jq -c .ids <<<"$BASELINE")" --argjson after "$WATCHERS_AFTER" --arg owned "${NW_ID,,}" --argjson nw "$NW_OWNED" '[ $after[]|select((.location|ascii_downcase)==$loc)|(.id|ascii_downcase)] as $a | ([ $before[]|ascii_downcase ] + (if $nw then [$owned] else [] end) | unique) as $e | (($a|length)==($e|length)) and all($e[]; . as $id | ($a|index($id))!=null)' >/dev/null || die 'Regional Network Watcher set changed; teardown will preserve its ownership evidence.'
if [[ "$NW_OWNED" == true ]]; then
  w="$(azc resource show --ids "$NW_ID" -o json)" || die 'Cannot recheck the owned Network Watcher after AKS creation.'
  jq -e --arg id "${NW_ID,,}" --arg loc "${LOCATION,,}" --arg t "$TOKEN" '(.id|ascii_downcase)==$id and (.type|ascii_downcase)=="microsoft.network/networkwatchers" and (.location|ascii_downcase)==$loc and .properties.provisioningState=="Succeeded" and .tags.reprotoken==$t and .tags.reproPurpose=="owned-vacuum-repro-watcher" and (.etag|type=="string" and length>0)' <<<"$w" >/dev/null || die 'Owned Network Watcher identity changed after AKS creation.'
  NW_STATE="$(jq --arg e "$(jq -r .etag <<<"$w")" '.etag=$e' <<<"$NW_STATE")"; state infrastructure-ready "$REQUESTED_NODE_RG"
fi
azc aks get-credentials -g "$RG" -n "$AKS" --file "$KC" --overwrite-existing -o none || die 'Cannot write isolated kubeconfig.'
chmod 600 "$KC"
export KUBECONFIG="$KC"
timeout --foreground --kill-after=10s 120s kubectl create namespace repro --dry-run=client -o yaml | timeout --foreground --kill-after=10s 120s kubectl create -f - >/dev/null
timeout --foreground --kill-after=10s 120s kubectl label namespace repro "repro.stack.token=$TOKEN" --overwrite=false >/dev/null
printf 'Installing CNPG 1.30.1 and the Standard HDD StorageClass.\n'
timeout --foreground --kill-after=15s 900s kubectl apply --server-side -f "$CNPG" >/dev/null || die 'Cannot install CNPG 1.30.1.'
timeout --foreground --kill-after=15s 660s kubectl -n cnpg-system rollout status deployment/cnpg-controller-manager --timeout=10m >/dev/null || die 'CNPG operator did not become ready.'
cat <<'YAML' | timeout --foreground --kill-after=10s 120s kubectl apply -f - >/dev/null
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: azure-standard-hdd
  labels:
    app.kubernetes.io/part-of: vacuum-only-repro
provisioner: disk.csi.azure.com
parameters:
  skuName: Standard_LRS
  cachingMode: None
  kind: Managed
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: WaitForFirstConsumer
YAML
sc="$(timeout --foreground --kill-after=10s 120s kubectl get sc azure-standard-hdd -o json)" || die 'Cannot inspect HDD StorageClass.'
jq -e '.provisioner=="disk.csi.azure.com" and .parameters.skuName=="Standard_LRS" and .parameters.cachingMode=="None" and .parameters.kind=="Managed" and .reclaimPolicy=="Delete" and .allowVolumeExpansion==true and .volumeBindingMode=="WaitForFirstConsumer"' <<<"$sc" >/dev/null || die 'HDD StorageClass verification failed.'
image="$(timeout --foreground --kill-after=10s 120s kubectl get deployment cnpg-controller-manager -n cnpg-system -o json | jq -r '[.spec.template.spec.containers[].image]|join(" ")')"
[[ "$image" == *1.30.1* ]] || die "CNPG image verification failed: $image"
state ready "$REQUESTED_NODE_RG"
printf 'Provisioned AKS; state: %s\n' "$SF"
