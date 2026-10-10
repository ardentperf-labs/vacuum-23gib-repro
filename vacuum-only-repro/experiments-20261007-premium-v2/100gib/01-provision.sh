#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SD="$ROOT/.state"; SF="$SD/state.json"; KC="$SD/kubeconfig"
SUB="${SUB:-00000000-0000-0000-0000-000000000000}"; LOCATION="${LOCATION:-eastus}"
NODE_VM_SIZE="${NODE_VM_SIZE:-Standard_D2s_v4}"
AZURE_ZONE="${AZURE_ZONE:-1}"
CNPG="https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/v1.30.1/releases/cnpg-1.30.1.yaml"
die(){ printf 'ERROR: %s\n' "$*" >&2; exit 1; }
azc(){ timeout --foreground --kill-after=15s 240s az "$@" --subscription "$SUB"; }
azlong(){ timeout --foreground --kill-after=15s 1800s az "$@" --subscription "$SUB"; }
for x in az kubectl jq timeout flock od tr ssh-keygen base64; do command -v "$x" >/dev/null || die "Missing $x"; done
[[ "$SUB" =~ ^[0-9a-fA-F-]{36}$ && "$LOCATION" =~ ^[A-Za-z0-9-]+$ && "$NODE_VM_SIZE" =~ ^Standard_[A-Za-z0-9_]+$ && "$AZURE_ZONE" =~ ^[1-3]$ ]] || die 'Invalid subscription, location, VM size, or AZURE_ZONE (use 1, 2, or 3).'
mkdir -p "$SD"; chmod 700 "$SD"; exec 9>"$SD/provision.lock"; flock -n 9 || die 'Provision already running.'
[[ ! -e "$SF" && ! -e "$KC" ]] || die 'State or kubeconfig already exists; refusing adoption.'
active="$(azc account show --query id -o tsv)" || die 'Cannot inspect active subscription.'
[[ "${active,,}" == "${SUB,,}" ]] || die 'Active Azure subscription differs from SUB.'
WATCHERS="$(azc network watcher list -o json)" || die 'Cannot capture regional Network Watcher baseline.'
BASELINE="$(jq -cn --arg loc "$LOCATION" --argjson all "$WATCHERS" '{location:$loc,ids:[$all[]|select((.location|ascii_downcase)==($loc|ascii_downcase))|.id]}')"
TOKEN="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"; [[ "$TOKEN" =~ ^[a-f0-9]{32}$ ]] || die 'Cannot generate owner token.'
RG="rg-vacuum-only-$TOKEN"; AKS="aks-vacuum-only-${TOKEN:0:12}"; REQUESTED_NODE_RG="rg-vacuum-nodes-$TOKEN"
SNAP_RG="rg-vacuum-pv2-snap-$TOKEN"; SNAP_RG_ID="/subscriptions/$SUB/resourceGroups/$SNAP_RG"
SA="cnpgwal${TOKEN:0:17}"; CONTAINER="wal-${TOKEN:0:12}"; STORAGE_PENDING=false; SECRET_FILE=''
NW_OWNED=false; NW_RG="rg-vacuum-watcher-$TOKEN"; NW_NAME="vacuum-watcher-${TOKEN:0:12}"; NW_ID="/subscriptions/$SUB/resourceGroups/$NW_RG/providers/Microsoft.Network/networkWatchers/$NW_NAME"; NW_STATE=null
if [[ "$(jq '.ids|length' <<<"$BASELINE")" == 0 ]]; then
  NW_OWNED=true
  NW_STATE="$(jq -cn --arg id "$NW_ID" --arg rg "$NW_RG" --arg name "$NW_NAME" --arg loc "$LOCATION" --arg t "$TOKEN" '{owned:true,status:"creating",id:$id,resourceGroup:$rg,name:$name,location:$loc,ownerToken:$t,etag:null,createdBy:"vacuum-only-repro/01-provision.sh"}')"
fi
state(){ local status="$1" node="${2:-}" tmp; tmp="$(mktemp "$SD/.state.XXXXXX")"; jq -n --arg sub "$SUB" --arg rg "$RG" --arg aks "$AKS" --arg node "$node" --arg requested "$REQUESTED_NODE_RG" --arg snaprg "$SNAP_RG" --arg snaprgid "$SNAP_RG_ID" --arg zone "$AZURE_ZONE" --arg loc "$LOCATION" --arg token "$TOKEN" --arg kc "$KC" --arg status "$status" --argjson baseline "$BASELINE" --argjson watcher "$NW_STATE" '{version:1,subscriptionId:$sub,resourceGroup:$rg,clusterName:$aks,nodeResourceGroup:$node,requestedNodeResourceGroup:$requested,snapshotResourceGroup:$snaprg,snapshotResourceGroupId:$snaprgid,availabilityZone:$zone,location:$loc,ownerToken:$token,kubeconfigPath:$kc,status:$status,networkWatcherBaseline:$baseline} + (if $watcher==null then {} else {networkWatcher:$watcher} end)' >"$tmp"; chmod 600 "$tmp"; mv -f "$tmp" "$SF"; }
storage(){ local status="$1" id="${2:-}" tmp; tmp="$(mktemp "$SD/.storage.XXXXXX")"; jq -n --arg sub "$SUB" --arg rg "$RG" --arg loc "$LOCATION" --arg token "$TOKEN" --arg account "$SA" --arg id "$id" --arg container "$CONTAINER" --arg status "$status" '{version:1,status:$status,subscriptionId:$sub,resourceGroup:$rg,location:$loc,ownerToken:$token,accountName:$account,accountId:(if $id=="" then null else $id end),containerName:$container,publicAccess:null,secret:{namespace:"repro",name:"azure-barman-credentials",key:"connection-string"}}' >"$tmp"; chmod 600 "$tmp"; mv -f "$tmp" "$SD/storage.json"; }
group_exists(){ local b; b="$(azc group exists -n "$1" -o tsv)" || die "Cannot inspect resource group $1."; case "$b" in true)return 0;; false)return 1;; *) die "Unexpected group-exists response.";; esac; }
SSH=''
on_exit(){ local rc=$?; trap - EXIT; [[ -z "$SSH" ]] || rm -rf -- "$SSH"; [[ -z "$SECRET_FILE" ]] || rm -f -- "$SECRET_FILE"; if ((rc)) && [[ "$STORAGE_PENDING" == true ]]; then printf 'Storage creation is unresolved; preserving state for safe retry: %s\n' "$SF" >&2; elif ((rc)) && [[ -f "$SF" ]]; then printf 'Provision failed; attempting owner-verified teardown.\n' >&2; "$ROOT/04-teardown.sh" || printf 'Cleanup incomplete; retry 04-teardown.sh.\n' >&2; fi; exit "$rc"; }
trap on_exit EXIT
if group_exists "$RG"; then die 'Generated test RG already exists.'; fi
if group_exists "$REQUESTED_NODE_RG"; then die 'Generated node RG already exists.'; fi
if group_exists "$SNAP_RG"; then die 'Generated retained snapshot RG already exists.'; fi
if [[ "$NW_OWNED" == true ]] && group_exists "$NW_RG"; then die 'Generated watcher RG already exists.'; fi
disk_skus="$(azc vm list-skus --location "$LOCATION" --resource-type disks --all -o json)" || die 'Cannot preflight PremiumV2_LRS regional availability.'
jq -e --arg loc "${LOCATION,,}" --arg zone "$AZURE_ZONE" 'any(.[] | select(.name=="PremiumV2_LRS") | (.locationInfo // .locationsInfo // [])[]? | select((.location // "" | ascii_downcase)==$loc) | (.zones // [])[]?; tostring==$zone)' <<<"$disk_skus" >/dev/null || die "PremiumV2_LRS is not listed in $LOCATION zone $AZURE_ZONE by Azure SKU preflight."
state provisioning
rgj="$(azc group create -n "$RG" -l "$LOCATION" --tags managed-by=vacuum-only-repro "reprotoken=$TOKEN" -o json)" || die 'Cannot create test RG.'
jq -e --arg t "$TOKEN" '.tags.reprotoken==$t' <<<"$rgj" >/dev/null || die 'Test RG ownership tag did not verify.'
snaprgj="$(azc group create -n "$SNAP_RG" -l "$LOCATION" --tags managed-by=vacuum-only-repro reproPurpose=premiumv2-retained-snapshots "reprotoken=$TOKEN" -o json)" || die 'Cannot create the separate retained snapshot RG.'
jq -e --arg t "$TOKEN" --arg n "$SNAP_RG" --arg l "${LOCATION,,}" '.name==$n and (.location|ascii_downcase)==$l and .tags.reprotoken==$t and .tags.reproPurpose=="premiumv2-retained-snapshots"' <<<"$snaprgj" >/dev/null || die 'Retained snapshot RG ownership did not verify.'
azc lock create --name preserve-all-snapshots --lock-type CanNotDelete --resource-group "$SNAP_RG" --notes 'Owned Premium SSD v2 reproduction snapshots are retained until explicit user authorization.' -o none || die 'Cannot install retained snapshot RG CanNotDelete lock.'
snaplock="$(azc lock list --resource-group "$SNAP_RG" -o json)" || die 'Cannot verify retained snapshot RG lock.'
jq -e --arg rgid "${SNAP_RG_ID,,}" 'any(.[]; .level=="CanNotDelete" and (((.scope // ((.id|ascii_downcase|split("/providers/microsoft.authorization/locks/"))[0]))|ascii_downcase)==$rgid))' <<<"$snaplock" >/dev/null || die 'Retained snapshot RG CanNotDelete lock did not verify.'
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
provider="$(azc provider show --namespace Microsoft.Storage --query registrationState -o tsv)" || die 'Cannot inspect Microsoft.Storage provider.'
if [[ "$provider" == NotRegistered ]]; then azlong provider register --namespace Microsoft.Storage -o none || die 'Storage provider registration failed.'; fi
if [[ "$provider" != Registered ]]; then
  end=$((SECONDS+1800)); until [[ "$provider" == Registered ]] || ((SECONDS>=end)); do
    sleep 10; provider="$(azc provider show --namespace Microsoft.Storage --query registrationState -o tsv)" || die 'Cannot poll Storage provider registration.'
    [[ "$provider" == Registering || "$provider" == Registered ]] || die "Storage provider stopped in state $provider."
  done
  [[ "$provider" == Registered ]] || die 'Storage provider registration timed out.'
fi
storage creating; STORAGE_PENDING=true
azlong storage account create -g "$RG" -n "$SA" -l "$LOCATION" --kind StorageV2 --sku Standard_LRS --https-only true --allow-blob-public-access false --min-tls-version TLS1_2 --tags "reprotoken=$TOKEN" reproStack=vacuum-only-repro -o none || printf 'Storage create returned; validating the generated account.\n' >&2
end=$((SECONDS+2400)); ready=false; err="$(mktemp "$SD/.storage-error.XXXXXX")"; chmod 600 "$err"
while ((SECONDS<end)); do
  if account="$(azc storage account show -g "$RG" -n "$SA" -o json 2>"$err")"; then
    case "$(jq -r '.provisioningState // empty' <<<"$account")" in
      Succeeded)
        jq -e --arg sub "${SUB,,}" --arg rg "${RG,,}" --arg loc "${LOCATION,,}" --arg t "$TOKEN" --arg n "$SA" --arg id "/subscriptions/${SUB,,}/resourcegroups/${RG,,}/providers/microsoft.storage/storageaccounts/${SA,,}" '(.id|ascii_downcase)==$id and (.name|ascii_downcase)==($n|ascii_downcase) and (.resourceGroup|ascii_downcase)==$rg and (.location|ascii_downcase)==$loc and .kind=="StorageV2" and .sku.name=="Standard_LRS" and .allowBlobPublicAccess==false and .enableHttpsTrafficOnly==true and .minimumTlsVersion=="TLS1_2" and .tags.reprotoken==$t' <<<"$account" >/dev/null || die 'Storage account ID, ownership, or privacy did not verify.'
        ACCOUNT_ID="$(jq -r .id <<<"$account")"; storage ready "$ACCOUNT_ID"; ready=true; STORAGE_PENDING=false; break ;;
      Creating|Updating|Accepted|ResolvingDns|ResolvingDNS|Moving|Deleting) ;;
      Failed|Canceled|Cancelled|Deleted) STORAGE_PENDING=false; die 'Storage account creation ended unsuccessfully.' ;;
      *) die 'Storage account returned an unknown state; preserving cleanup metadata.' ;;
    esac
  elif grep -Eiq 'ResourceNotFound|NotFound|OperationInProgress|StorageAccountOperationInProgress' "$err"; then :
  else cat "$err" >&2; die 'Cannot verify storage account.'; fi
  sleep 10
done
rm -f "$err"; [[ "$ready" == true ]] || die 'Storage account readiness timed out.'
SECRET_FILE="$(mktemp "$SD/.connection.XXXXXX")"; chmod 600 "$SECRET_FILE"
azc storage account show-connection-string -g "$RG" -n "$SA" --query connectionString -o tsv >"$SECRET_FILE" || die 'Cannot retrieve archive credentials.'
tr -d '\r\n' <"$SECRET_FILE" >"$SECRET_FILE.clean"; mv -f "$SECRET_FILE.clean" "$SECRET_FILE"; [[ -s "$SECRET_FILE" ]] || die 'Archive credentials are empty.'
(
  AZURE_STORAGE_CONNECTION_STRING="$(<"$SECRET_FILE")"; export AZURE_STORAGE_CONNECTION_STRING
  azc storage container create -n "$CONTAINER" --public-access off --metadata "reprotoken=$TOKEN" reprostack=vacuum-only-repro --fail-on-exist -o none
  c="$(azc storage container show -n "$CONTAINER" -o json)"
  jq -e --arg t "$TOKEN" '(.metadata.reprotoken==$t) and (.publicAccess==null or .publicAccess=="off")' <<<"$c" >/dev/null || die 'WAL container ownership or privacy did not verify.'
  printf '%s' "$c" >"$SD/storage-container.json"
)
SSH="$(mktemp -d "${TMPDIR:-/tmp}/vacuum-ssh.XXXXXX")"
ssh-keygen -q -t rsa -b 4096 -N '' -C vacuum-only-repro -f "$SSH/id_rsa"
printf 'Submitting isolated two-node AKS (%s) in %s zone %s.\n' "$NODE_VM_SIZE" "$LOCATION" "$AZURE_ZONE"
azlong aks create -g "$RG" -n "$AKS" -l "$LOCATION" --node-resource-group "$REQUESTED_NODE_RG" --node-count 2 --node-vm-size "$NODE_VM_SIZE" --zones "$AZURE_ZONE" --kubernetes-version 1.35 --load-balancer-sku standard --tier free --enable-managed-identity --ssh-key-value "$SSH/id_rsa.pub" --tags managed-by=vacuum-only-repro "reprotoken=$TOKEN" --no-wait -o none || die 'AKS create submission failed.'
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
state ready "$REQUESTED_NODE_RG"
principal="$(jq -r '.identity.principalId // empty' <<<"$a")"
[[ "$principal" =~ ^[0-9a-fA-F-]{36}$ ]] || die 'AKS control-plane identity principal ID is missing.'
assignment="$(azlong role assignment create --assignee-object-id "$principal" --assignee-principal-type ServicePrincipal --role 'Disk Snapshot Contributor' --scope "$SNAP_RG_ID" -o json)" || die 'Cannot grant the AKS control-plane identity scoped access to the retained snapshot RG.'
jq -e --arg p "${principal,,}" --arg s "${SNAP_RG_ID,,}" --arg role '7efff54f-a5b4-42b5-a1c5-5411624893ce' '(.principalId|ascii_downcase)==$p and (.scope|ascii_downcase)==$s and ((.roleDefinitionId|ascii_downcase)|endswith($role))' <<<"$assignment" >/dev/null || die 'Snapshot role assignment identity/scope did not verify.'
tmp="$(mktemp "$SD/.state.XXXXXX")"; jq --arg id "$(jq -r .id <<<"$assignment")" --arg principal "$principal" '.snapshotRoleAssignment={id:$id,principalId:$principal,role:"Disk Snapshot Contributor",scope:.snapshotResourceGroupId}' "$SF" >"$tmp"; chmod 600 "$tmp"; mv -f "$tmp" "$SF"
WATCHERS_AFTER="$(azc network watcher list -o json)" || die 'Cannot check regional Network Watchers after AKS creation.'
jq -n -e --arg loc "${LOCATION,,}" --argjson before "$(jq -c .ids <<<"$BASELINE")" --argjson after "$WATCHERS_AFTER" --arg owned "${NW_ID,,}" --argjson nw "$NW_OWNED" '[ $after[]|select((.location|ascii_downcase)==$loc)|(.id|ascii_downcase)] as $a | ([ $before[]|ascii_downcase ] + (if $nw then [$owned] else [] end) | unique) as $e | (($a|length)==($e|length)) and all($e[]; . as $id | ($a|index($id))!=null)' >/dev/null || die 'Regional Network Watcher set changed; teardown will preserve its ownership evidence.'
if [[ "$NW_OWNED" == true ]]; then
  w="$(azc resource show --ids "$NW_ID" -o json)" || die 'Cannot recheck the owned Network Watcher after AKS creation.'
  jq -e --arg id "${NW_ID,,}" --arg loc "${LOCATION,,}" --arg t "$TOKEN" '(.id|ascii_downcase)==$id and (.type|ascii_downcase)=="microsoft.network/networkwatchers" and (.location|ascii_downcase)==$loc and .properties.provisioningState=="Succeeded" and .tags.reprotoken==$t and .tags.reproPurpose=="owned-vacuum-repro-watcher" and (.etag|type=="string" and length>0)' <<<"$w" >/dev/null || die 'Owned Network Watcher identity changed after AKS creation.'
  NW_STATE="$(jq --arg e "$(jq -r .etag <<<"$w")" '.etag=$e' <<<"$NW_STATE")"; state ready "$REQUESTED_NODE_RG"
fi
azc aks get-credentials -g "$RG" -n "$AKS" --file "$KC" --overwrite-existing -o none || die 'Cannot write isolated kubeconfig.'
chmod 600 "$KC"
export KUBECONFIG="$KC"
timeout --foreground --kill-after=10s 120s kubectl create namespace repro --dry-run=client -o yaml | timeout --foreground --kill-after=10s 120s kubectl create -f - >/dev/null
timeout --foreground --kill-after=10s 120s kubectl label namespace repro "repro.stack.token=$TOKEN" --overwrite=false >/dev/null
timeout --foreground --kill-after=10s 120s kubectl -n repro create secret generic azure-barman-credentials --from-file="connection-string=$SECRET_FILE" --dry-run=client -o yaml | timeout --foreground --kill-after=10s 120s kubectl create -f - >/dev/null
timeout --foreground --kill-after=10s 120s kubectl -n repro label secret azure-barman-credentials "repro.stack.token=$TOKEN" --overwrite=false >/dev/null
secret_json="$(timeout --foreground --kill-after=10s 120s kubectl -n repro get secret azure-barman-credentials -o json)"
jq -e --arg t "$TOKEN" '.metadata.labels["repro.stack.token"]==$t and .type=="Opaque" and (.data|keys)==["connection-string"]' <<<"$secret_json" >/dev/null || die 'Archive Secret ownership or key did not verify.'
[[ "$(jq -r '.data["connection-string"]' <<<"$secret_json")" == "$(base64 -w0 "$SECRET_FILE")" ]] || die 'Archive Secret content did not verify.'
rm -f "$SECRET_FILE"; SECRET_FILE=''
printf 'Installing CNPG 1.30.1 and the Premium SSD v2 StorageClass.\n'
timeout --foreground --kill-after=15s 900s kubectl apply --server-side -f "$CNPG" >/dev/null || die 'Cannot install CNPG 1.30.1.'
timeout --foreground --kill-after=15s 660s kubectl -n cnpg-system rollout status deployment/cnpg-controller-manager --timeout=10m >/dev/null || die 'CNPG operator did not become ready.'
cat <<'YAML' | timeout --foreground --kill-after=10s 120s kubectl apply -f - >/dev/null
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: azure-premiumv2
  labels:
    app.kubernetes.io/part-of: vacuum-only-repro
provisioner: disk.csi.azure.com
parameters:
  skuName: PremiumV2_LRS
  cachingMode: None
  kind: Managed
  DiskIOPSReadWrite: "3000"
  DiskMBpsReadWrite: "125"
  LogicalSectorSize: "512"
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: WaitForFirstConsumer
YAML
sc="$(timeout --foreground --kill-after=10s 120s kubectl get sc azure-premiumv2 -o json)" || die 'Cannot inspect Premium SSD v2 StorageClass.'
jq -e '.provisioner=="disk.csi.azure.com" and .parameters.skuName=="PremiumV2_LRS" and .parameters.cachingMode=="None" and .parameters.kind=="Managed" and .parameters.DiskIOPSReadWrite=="3000" and .parameters.DiskMBpsReadWrite=="125" and .parameters.LogicalSectorSize=="512" and .reclaimPolicy=="Delete" and .allowVolumeExpansion==true and .volumeBindingMode=="WaitForFirstConsumer"' <<<"$sc" >/dev/null || die 'Premium SSD v2 StorageClass verification failed.'
image="$(timeout --foreground --kill-after=10s 120s kubectl get deployment cnpg-controller-manager -n cnpg-system -o json | jq -r '[.spec.template.spec.containers[].image]|join(" ")')"
[[ "$image" == *1.30.1* ]] || die "CNPG image verification failed: $image"
printf 'Provisioned AKS, private WAL archive, and retained-snapshot RG with CanNotDelete protection; state: %s\n' "$SF"
