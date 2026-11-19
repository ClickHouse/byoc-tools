#!/usr/bin/env bash
#
# ClickHouse BYOC kill switch for Azure.
#
# Temporarily disconnects ClickHouse from a standard BYOC deployment in this Azure
# subscription. Your ClickHouse services keep serving queries; ClickHouse can no longer
# act in your subscription or your AKS cluster until you run `reconnect`.
#
#   plan        (default) read-only: list ClickHouse access paths and what would change
#   disconnect  cut every path (asks for confirmation unless --yes)
#   reconnect   restore everything this script changed (asks unless --yes)
#   status      read-only: show OPEN / CLOSED for every path
#
# Requires: bash 4.4+, az CLI, jq, kubectl, curl. Run as a subscription Owner (role
# assignments, managed identities, AKS). Disabling the ClickHouse service principal
# also needs Cloud Application Administrator in Entra ID, and disabling legacy
# ClickHouse guest users needs User Administrator; without them those steps are
# reported as not done. Both are tenant-wide (they affect every subscription in the
# tenant); --subscription-only leaves them alone and changes only this subscription.
#
# Original settings are saved under --state-dir. Keep that directory until you have
# reconnected: `reconnect` restores from it. Every command is safe to re-run. Exit code
# 2 means some path could not be closed (or restored); the output says which.

if ((BASH_VERSINFO[0] * 100 + BASH_VERSINFO[1] < 404)); then
  echo "ERROR: this script needs bash 4.4 or newer (on macOS: brew install bash)" >&2
  exit 1
fi
set -Eeuo pipefail
shopt -s inherit_errexit

readonly VERSION="1.2.0"
# ClickHouse's multi-tenant BYOC provisioner app (application/client ID).
readonly DEFAULT_PROVISIONER_APP_ID="a3a33a9e-cea2-47e6-a160-fd2add78f73e"
# ClickHouse's Entra tenant (issuer of its management clusters' workload identities)
# and its management subscription (owner of the AKS management private endpoints).
readonly DEFAULT_CH_TENANT_IDS="3d7995a7-a475-466f-ab08-ed4bcfdca0f8"
readonly DEFAULT_CH_SUBSCRIPTIONS="cda9b703-c833-4e38-bc8e-ef01915ab5cd"

COMMAND="plan"
SUBSCRIPTION=""
CLUSTERS=()
PROVISIONER_APP_ID="$DEFAULT_PROVISIONER_APP_ID"
SP_OBJECT_ID=""
CH_TENANT_IDS="$DEFAULT_CH_TENANT_IDS"
CH_SUBSCRIPTIONS="$DEFAULT_CH_SUBSCRIPTIONS"
ADMIN_CIDRS=()
SKIP_KUBERNETES=false
SUBSCRIPTION_ONLY=false
ASSUME_YES=false
STATE_DIR=""
WORK_DIR=""
KUBECONFIG_FILE=""
INCOMPLETE=()

usage() {
  cat <<'EOF'
Usage: byoc-kill-switch-azure.sh <plan|disconnect|reconnect|status> [options]

Options:
  --subscription ID           Subscription of the BYOC infrastructure (default: current az account)
  --cluster NAME              AKS cluster (default: every clickhouse-cloud-* cluster in the subscription)
  --admin-cidr CIDR           CIDR kept on the AKS API server so you can run kubectl
                              (repeatable; default: this machine's public IP)
  --skip-kubernetes           Do not touch objects inside the cluster (leaves Tailscale running)
  --subscription-only         Change only this subscription. Leaves the two tenant-wide steps
                              alone: the ClickHouse BYOC app keeps sign-in to your tenant, and
                              ClickHouse Engineer guest users are not disabled (or re-enabled).
                              The app's role assignments in this subscription are still removed.
                              Use it when other subscriptions in your tenant must stay connected.
  --provisioner-app-id ID     ClickHouse BYOC app ID (default: production)
  --provisioner-object-id ID  Object ID of its service principal in your tenant, if you
                              cannot read Entra ID
  --clickhouse-tenant-ids IDS Space-separated ClickHouse tenant IDs (default: production)
  --clickhouse-subscriptions IDS
                              Space-separated ClickHouse management subscriptions (default: production)
  --state-dir DIR             Where original settings are saved
                              (default: ./clickhouse-byoc-kill-switch-azure-<subscription>)
  --yes                       Do not ask for confirmation
  -h, --help                  Show this help
EOF
}

# ---------------------------------------------------------------------------
# Output and helpers
# ---------------------------------------------------------------------------

log()  { printf '%s\n' "$*" >&2; }
info() { printf '  %s\n' "$*" >&2; }
warn() { printf 'WARNING: %s\n' "$*" >&2; }
die()  { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
step() { printf '\n== %s\n' "$*" >&2; }

incomplete() {
  warn "$*"
  INCOMPLETE+=("$*")
}

on_err() {
  [[ $BASH_SUBSHELL -eq 0 ]] || return 0
  printf '\nERROR: command failed at line %s. Nothing is rolled back.\n' "$1" >&2
  printf 'Fix the cause and run the same command again; finished steps are skipped.\n' >&2
}
trap 'on_err $LINENO' ERR
trap '[[ -n "$WORK_DIR" ]] && rm -rf "$WORK_DIR"' EXIT

applying() { [[ "$COMMAND" == "disconnect" || "$COMMAND" == "reconnect" ]]; }

record() {
  applying && [[ -d "$STATE_DIR" ]] || return 0
  printf '%s %s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$COMMAND" "$*" >>"$STATE_DIR/actions.log"
}

act() {
  local desc="$1"
  shift
  if ! applying; then
    info "[would] $desc"
    return 0
  fi
  info "$desc"
  local rc=0
  "$@" || rc=$?
  [[ $rc -eq 0 ]] || return $rc
  record "$desc"
}

# Entra ID steps: a failure (usually a missing directory role) is reported, not fatal.
act_entra() {
  local desc="$1"
  shift
  if act "$desc" "$@"; then return 0; fi
  incomplete "could not $desc (needs an Entra ID administrator role)"
}

need() { command -v "$1" >/dev/null 2>&1 || die "'$1' is required but not installed"; }

confirm() {
  [[ "$ASSUME_YES" == true ]] && return 0
  local answer
  printf '\nType "%s" to continue: ' "$COMMAND" >&2
  read -r answer
  [[ "$answer" == "$COMMAND" ]] || die "aborted"
}

safe_name() { printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '_'; }

save_once() {
  applying || return 0
  [[ -f "$1" ]] && return 0
  mkdir -p "$(dirname "$1")"
  printf '%s\n' "$2" >"$1"
}

# save_merge FILE JSON_ARRAY: add new entries to a saved array (originals win).
save_merge() {
  applying || return 0
  [[ "$2" != "[]" ]] || return 0
  if [[ -f "$1" ]]; then
    jq -c --slurpfile old "$1" '$old[0] + . | unique_by(.key)' <<<"$2" >"$1.tmp"
    mv "$1.tmp" "$1"
  else
    save_once "$1" "$2"
  fi
}

finish() {
  if [[ ${#INCOMPLETE[@]} -eq 0 ]]; then
    return 0
  fi
  step "INCOMPLETE"
  local m
  for m in "${INCOMPLETE[@]}"; do info "- $m"; done
  if applying; then
    info "Fix the above and run '$COMMAND' again."
    exit 2
  fi
  info "These paths would stay open; see the warnings above."
}

# ARM commands in the target subscription, JSON output.
azs() { az "$@" --subscription "$SUBSCRIPTION" --only-show-errors -o json; }

lower() { tr '[:upper:]' '[:lower:]' <<<"$1"; }

# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------

parse_args() {
  if [[ $# -gt 0 && "$1" != -* ]]; then
    COMMAND="$1"
    shift
  fi
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --subscription|--cluster|--admin-cidr|--provisioner-app-id|--provisioner-object-id|--clickhouse-tenant-ids|--clickhouse-subscriptions|--state-dir)
        [[ $# -ge 2 && -n "$2" && "$2" != -* ]] || die "$1 needs a value" ;;
    esac
    case "$1" in
      --subscription) SUBSCRIPTION="$2"; shift 2 ;;
      --cluster) CLUSTERS+=("$2"); shift 2 ;;
      --admin-cidr) ADMIN_CIDRS+=("$2"); shift 2 ;;
      --skip-kubernetes) SKIP_KUBERNETES=true; shift ;;
      --subscription-only) SUBSCRIPTION_ONLY=true; shift ;;
      --provisioner-app-id) PROVISIONER_APP_ID="$2"; shift 2 ;;
      --provisioner-object-id) SP_OBJECT_ID="$2"; shift 2 ;;
      --clickhouse-tenant-ids) CH_TENANT_IDS="$2"; shift 2 ;;
      --clickhouse-subscriptions) CH_SUBSCRIPTIONS="$2"; shift 2 ;;
      --state-dir) STATE_DIR="${2%/}"; shift 2 ;;
      --yes) ASSUME_YES=true; shift ;;
      -h|--help) usage; exit 0 ;;
      *) usage; die "unknown argument: $1" ;;
    esac
  done
  case "$COMMAND" in plan|disconnect|reconnect|status) ;; *) usage; die "unknown command: $COMMAND" ;; esac
  local c
  for c in "${CLUSTERS[@]}"; do
    [[ "$c" == clickhouse-cloud-* ]] || die "--cluster $c is not a ClickHouse BYOC cluster (clickhouse-cloud-*)"
  done
}

# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

CLUSTER_LIST=""  # "resourceGroup name" per line

discover() {
  local acct all c
  if [[ -n "$SUBSCRIPTION" ]]; then
    acct="$(az account show --subscription "$SUBSCRIPTION" -o json --only-show-errors)"
  else
    acct="$(az account show -o json --only-show-errors)"
  fi
  SUBSCRIPTION="$(jq -r .id <<<"$acct")"
  info "subscription: $SUBSCRIPTION ($(jq -r .name <<<"$acct")), running as $(jq -r .user.name <<<"$acct")"
  [[ -n "$STATE_DIR" ]] || STATE_DIR="./clickhouse-byoc-kill-switch-azure-${SUBSCRIPTION}"

  if [[ -z "$SP_OBJECT_ID" ]]; then
    SP_OBJECT_ID="$(az ad sp show --id "$PROVISIONER_APP_ID" --query id -o tsv --only-show-errors 2>/dev/null || true)"
  fi

  all="$(azs aks list --query '[].{n:name,g:resourceGroup}')"
  for c in "${CLUSTERS[@]}"; do
    jq -e --arg c "$c" 'any(.[]; .n == $c)' <<<"$all" >/dev/null || die "cluster $c not found in subscription $SUBSCRIPTION"
  done
  CLUSTER_LIST="$(jq -r --argjson want "$(printf '%s\n' "${CLUSTERS[@]}" | jq -R . | jq -s 'map(select(. != ""))')" \
    '.[] | select(if ($want | length) > 0 then (.n as $n | $want | index($n)) else (.n | startswith("clickhouse-cloud-")) end)
     | .g + " " + .n' <<<"$all")"
}

resolve_admin_cidrs() {
  [[ ${#ADMIN_CIDRS[@]} -gt 0 ]] && return 0
  need curl
  local ip
  ip="$(curl -fsS --max-time 10 https://api.ipify.org | tr -d '[:space:]')" ||
    die "could not detect this machine's public IP; pass --admin-cidr"
  [[ "$ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "unexpected public IP '$ip'; pass --admin-cidr"
  ADMIN_CIDRS=("$ip/32")
}

# ---------------------------------------------------------------------------
# Role assignments
# ---------------------------------------------------------------------------

# Role assignments of a principal: {in: [...in this subscription], out: [...inherited]}.
role_assignments_of() {
  azs role assignment list --assignee-object-id "$1" --all |
    jq -c --arg sub "/subscriptions/$(lower "$SUBSCRIPTION")" '
      [.[] | {key: .id, id, principalId, principalType, roleDefinitionId, roleDefinitionName, scope, condition, conditionVersion, description}]
      | {in: [.[] | select((.scope | ascii_downcase) as $s | $s == $sub or ($s | startswith($sub + "/")))],
         out: [.[] | select((.scope | ascii_downcase) as $s | ($s == $sub or ($s | startswith($sub + "/"))) | not)]}'
}

INHERITED_ROLES=0  # role assignments above this subscription found by the last remove_role_assignments

remove_role_assignments() {
  local label="$1" principal="$2" file="$3" ras ra list
  ras="$(role_assignments_of "$principal")"
  INHERITED_ROLES="$(jq '.out | length' <<<"$ras")"
  if [[ "$INHERITED_ROLES" -gt 0 ]]; then
    incomplete "$label also has roles assigned above this subscription, which a subscription Owner cannot remove: $(jq -r '[.out[] | .roleDefinitionName + " on " + .scope] | join("; ")' <<<"$ras")"
  fi
  if [[ "$(jq '.in | length' <<<"$ras")" -eq 0 ]]; then
    info "$label: no role assignments in this subscription"
    return 0
  fi
  save_merge "$file" "$(jq -c .in <<<"$ras")"
  list="$(jq -c '.in[]' <<<"$ras")"
  while read -r ra <&3; do
    [[ -n "$ra" ]] || continue
    act "$label: remove role '$(jq -r .roleDefinitionName <<<"$ra")' on $(jq -r .scope <<<"$ra") (saved)" \
      azs role assignment delete --ids "$(jq -r .id <<<"$ra")" >/dev/null
  done 3<<<"$list"
}

restore_role_assignments() {
  local label="$1" file="$2" ra args list
  [[ -f "$file" ]] || return 0
  list="$(jq -c '.[]' "$file")"
  while read -r ra <&3; do
    [[ -n "$ra" ]] || continue
    if [[ "$(azs role assignment list --assignee-object-id "$(jq -r .principalId <<<"$ra")" --role "$(jq -r .roleDefinitionId <<<"$ra")" \
        --scope "$(jq -r .scope <<<"$ra")" | jq length)" -gt 0 ]]; then
      info "$label: role '$(jq -r .roleDefinitionName <<<"$ra")' on $(jq -r .scope <<<"$ra") already assigned"
      continue
    fi
    # Same name (GUID) as the original: ClickHouse's Crossplane and the onboarding
    # Terraform track role assignments by ID, and cannot adopt one with a new name.
    args=(role assignment create --name "$(jq -r '.id | split("/") | last' <<<"$ra")"
      --assignee-object-id "$(jq -r .principalId <<<"$ra")"
      --assignee-principal-type "$(jq -r .principalType <<<"$ra")"
      --role "$(jq -r .roleDefinitionId <<<"$ra")" --scope "$(jq -r .scope <<<"$ra")")
    if [[ -n "$(jq -r '.condition // empty' <<<"$ra")" ]]; then
      args+=(--condition "$(jq -r .condition <<<"$ra")" --condition-version "$(jq -r '.conditionVersion // "2.0"' <<<"$ra")")
    fi
    if [[ -n "$(jq -r '.description // empty' <<<"$ra")" ]]; then
      args+=(--description "$(jq -r .description <<<"$ra")")
    fi
    act "$label: restore role '$(jq -r .roleDefinitionName <<<"$ra")' on $(jq -r .scope <<<"$ra")" azs "${args[@]}" >/dev/null
  done 3<<<"$list"
}

# ---------------------------------------------------------------------------
# Provisioner service principal ("ClickHouse BYOC")
# ---------------------------------------------------------------------------

sp_enabled() { [[ "$(az ad sp show --id "$PROVISIONER_APP_ID" --query accountEnabled -o tsv --only-show-errors)" == "true" ]]; }

provisioner_disconnect() {
  step "ClickHouse BYOC service principal (app $PROVISIONER_APP_ID)"
  if [[ -z "$SP_OBJECT_ID" ]]; then
    if applying; then
      die "cannot read the ClickHouse BYOC service principal in Entra ID; pass --provisioner-object-id (Enterprise applications > ClickHouse BYOC > Object ID)"
    fi
    warn "cannot read the ClickHouse BYOC service principal; disconnect needs --provisioner-object-id"
    return 0
  fi
  # Role assignments first: Azure re-evaluates them on every request (within about 10
  # minutes), so they also cut tokens ClickHouse already holds. Disabling sign-in only
  # stops new tokens.
  remove_role_assignments "service principal" "$SP_OBJECT_ID" "$STATE_DIR/provisioner/role-assignments.json"
  if [[ "$SUBSCRIPTION_ONLY" == true ]]; then
    if [[ "$INHERITED_ROLES" -gt 0 ]]; then
      info "sign-in: skipped (--subscription-only): the ClickHouse app can still sign in to your tenant, and keeps the inherited roles reported above"
    else
      info "sign-in: skipped (--subscription-only): the ClickHouse app can still sign in to your tenant but $(if applying; then echo has; else echo "will have"; fi) no role in this subscription"
    fi
    return 0
  fi
  local enabled
  enabled="$(az ad sp show --id "$PROVISIONER_APP_ID" --query accountEnabled -o tsv --only-show-errors 2>/dev/null || echo unknown)"
  case "$enabled" in
    false)
      info "service principal sign-in already disabled"
      return 0
      ;;
    true) ;;
    *)
      incomplete "could not read the ClickHouse BYOC service principal's sign-in state; disable it in Entra ID by hand (Enterprise applications > ClickHouse BYOC > Properties > Enabled for users to sign in: No)"
      return 0
      ;;
  esac
  save_once "$STATE_DIR/provisioner/disabled-by-kill-switch" "$PROVISIONER_APP_ID"
  act_entra "disable sign-in for the ClickHouse BYOC service principal" \
    az ad sp update --id "$PROVISIONER_APP_ID" --set accountEnabled=false --only-show-errors
}

provisioner_reconnect() {
  step "ClickHouse BYOC service principal"
  restore_role_assignments "service principal" "$STATE_DIR/provisioner/role-assignments.json"
  if [[ -f "$STATE_DIR/provisioner/disabled-by-kill-switch" ]] && ! sp_enabled; then
    if [[ "$SUBSCRIPTION_ONLY" == true ]]; then
      incomplete "an earlier disconnect disabled the ClickHouse BYOC service principal's sign-in; --subscription-only leaves it disabled. Run reconnect without --subscription-only to enable it."
      return 0
    fi
    act_entra "enable sign-in for the ClickHouse BYOC service principal" \
      az ad sp update --id "$PROVISIONER_APP_ID" --set accountEnabled=true --only-show-errors
  elif [[ "$SUBSCRIPTION_ONLY" == true ]]; then
    info "sign-in: skipped (--subscription-only): left as it is"
  fi
}

provisioner_status() {
  step "ClickHouse BYOC service principal"
  if [[ -z "$SP_OBJECT_ID" ]]; then
    info "not readable (pass --provisioner-object-id)"
    return 0
  fi
  local n signin=unknown
  n="$(role_assignments_of "$SP_OBJECT_ID" | jq '.in + .out | length')"
  if sp_enabled 2>/dev/null; then signin=OPEN; elif az ad sp show --id "$PROVISIONER_APP_ID" -o none --only-show-errors 2>/dev/null; then signin=CLOSED; fi
  if [[ "$SUBSCRIPTION_ONLY" != true ]]; then
    info "sign-in: $signin   role assignments: $(if [[ "$n" == 0 ]]; then echo CLOSED; else echo "OPEN ($n)"; fi)"
    return 0
  fi
  info "role assignments: $(if [[ "$n" == 0 ]]; then echo CLOSED; else echo "OPEN ($n)"; fi)"
  case "$signin" in
    OPEN)
      info "sign-in: skipped (--subscription-only): the ClickHouse app can still sign in to your tenant $(if [[ "$n" == 0 ]]; then echo "but has no role in this subscription"; else echo "and still has the roles above"; fi)" ;;
    CLOSED) info "sign-in: CLOSED (disabled earlier; --subscription-only leaves it as it is)" ;;
    *) info "sign-in: skipped (--subscription-only): not readable" ;;
  esac
}

# ---------------------------------------------------------------------------
# Managed identities in the infrastructure resource group
# ---------------------------------------------------------------------------

# Federated credentials of every identity in the resource group whose issuer is not this
# cluster's own OIDC issuer: {ch: [ClickHouse management issuers], other: [anything else]}.
foreign_fics() {
  local rg="$1" issuer="$2" idents ident out="[]" fics
  idents="$(azs identity list -g "$rg" | jq -r '.[].name')"
  while read -r ident <&3; do
    [[ -n "$ident" ]] || continue
    fics="$(azs identity federated-credential list --identity-name "$ident" -g "$rg")"
    out="$(jq -c --argjson add "$(jq -c --arg id "$ident" --arg rg "$rg" --arg iss "${issuer%/}" \
      '[.[] | select((.issuer | rtrimstr("/")) != $iss)
        | {key: ($rg + "/" + $id + "/" + .name), rg: $rg, identity: $id, name, issuer, subject, audiences}]' <<<"$fics")" '. + $add' <<<"$out")"
  done 3<<<"$idents"
  jq -c --arg tenants "$(lower "$CH_TENANT_IDS")" '
    ($tenants | split(" ") | map(select(. != ""))) as $t
    | {ch: [.[] | select((.issuer | ascii_downcase) as $i | any($t[]; . as $x | $i | contains($x)))],
       other: [.[] | select((.issuer | ascii_downcase) as $i | any($t[]; . as $x | $i | contains($x)) | not)]}' <<<"$out"
}

identities_disconnect() {
  local rg="$1" cluster="$2" issuer="$3" dir="$4" fics f list ident pid
  [[ -n "$issuer" ]] || die "$cluster has no OIDC issuer; cannot tell ClickHouse federated credentials apart"
  fics="$(foreign_fics "$rg" "$issuer")"
  if [[ "$(jq '.other | length' <<<"$fics")" -gt 0 ]]; then
    warn "$cluster: federated credentials with other external issuers (not ClickHouse; review them): $(jq -r '[.other[] | .identity + "/" + .name + " (" + .issuer + ")"] | join(", ")' <<<"$fics")"
  fi
  save_merge "$dir/federated-credentials.json" "$(jq -c .ch <<<"$fics")"
  list="$(jq -c '.ch[]' <<<"$fics")"
  while read -r f <&3; do
    [[ -n "$f" ]] || continue
    act "$cluster: delete ClickHouse federated credential $(jq -r '.identity + "/" + .name' <<<"$f") (saved)" \
      azs identity federated-credential delete --identity-name "$(jq -r .identity <<<"$f")" -g "$rg" -n "$(jq -r .name <<<"$f")" --yes
  done 3<<<"$list"
  [[ "$(jq '.ch | length' <<<"$fics")" -gt 0 ]] || info "$cluster: no ClickHouse federated credentials left"
  # The data-plane-mgmt identity: also remove its role assignments, because a
  # managed-identity token stays valid for up to 24 hours after it is issued.
  list="$(azs identity list -g "$rg" | jq -r '.[] | select(.name | endswith("-data-plane-mgmt")) | .name + " " + .principalId')"
  while read -r ident pid <&3; do
    [[ -n "$ident" ]] || continue
    remove_role_assignments "$ident" "$pid" "$dir/$(safe_name "$ident").role-assignments.json"
  done 3<<<"$list"
}

identities_reconnect() {
  local rg="$1" cluster="$2" dir="$3" f file list auds
  if [[ -f "$dir/federated-credentials.json" ]]; then
    list="$(jq -c '.[]' "$dir/federated-credentials.json")"
    while read -r f <&3; do
      [[ -n "$f" ]] || continue
      if azs identity federated-credential show --identity-name "$(jq -r .identity <<<"$f")" -g "$rg" -n "$(jq -r .name <<<"$f")" >/dev/null 2>&1; then
        info "$cluster: federated credential $(jq -r '.identity + "/" + .name' <<<"$f") already exists"
        continue
      fi
      mapfile -t auds < <(jq -r '.audiences[]' <<<"$f")
      act "$cluster: recreate federated credential $(jq -r '.identity + "/" + .name' <<<"$f")" \
        azs identity federated-credential create --identity-name "$(jq -r .identity <<<"$f")" -g "$rg" -n "$(jq -r .name <<<"$f")" \
        --issuer "$(jq -r .issuer <<<"$f")" --subject "$(jq -r .subject <<<"$f")" --audiences "${auds[@]}" >/dev/null
    done 3<<<"$list"
  fi
  shopt -s nullglob
  for file in "$dir"/*.role-assignments.json; do
    restore_role_assignments "$(basename "$file" .role-assignments.json)" "$file"
  done
  shopt -u nullglob
}

# ---------------------------------------------------------------------------
# AKS API server network access
# ---------------------------------------------------------------------------

is_private() { jq -e '.apiServerAccessProfile.enablePrivateCluster == true' >/dev/null <<<"$1"; }

# Public IPs the cluster's own nodes leave through; they must stay allowed.
cluster_egress_cidrs() {
  local desc="$1" rg="$2" ids pid g nats addr
  ids="$(jq -r '[.networkProfile.loadBalancerProfile.effectiveOutboundIPs[]?.id, .networkProfile.natGatewayProfile.effectiveOutboundIPs[]?.id] | .[]' <<<"$desc")"
  for g in "$rg" "$(jq -r .nodeResourceGroup <<<"$desc")"; do
    nats="$(azs network nat gateway list -g "$g")"
    ids+=$'\n'"$(jq -r '.[] | (.publicIpAddresses[]?.id), (.publicIpPrefixes[]?.id)' <<<"$nats")"
  done
  ids="$(sort -u <<<"$ids")"
  while read -r pid <&3; do
    [[ -n "$pid" ]] || continue
    if [[ "$(lower "$pid")" == */publicipprefixes/* ]]; then
      azs network public-ip prefix show --ids "$pid" | jq -r '.ipPrefix // empty'
    else
      addr="$(azs network public-ip show --ids "$pid" | jq -r '.ipAddress // empty')"
      if [[ "$addr" == *:* ]]; then echo "$addr/128"; elif [[ -n "$addr" ]]; then echo "$addr/32"; fi
    fi
  done 3<<<"$ids"
}

network_disconnect() {
  local rg="$1" cluster="$2" desc="$3" dir="$4" cur want egress
  if is_private "$desc"; then
    pls_disconnect "$rg" "$cluster" "$dir"
    return 0
  fi
  egress="$(cluster_egress_cidrs "$desc" "$rg" | sort -u)"
  [[ -n "$egress" ]] || warn "$cluster: found no node egress IPs; nodes on clusters without API server VNet integration may lose the API"
  cur="$(jq -c '.apiServerAccessProfile.authorizedIpRanges // [] | sort' <<<"$desc")"
  want="$( { printf '%s\n' "${ADMIN_CIDRS[@]}"; printf '%s\n' "$egress"; } | jq -R . | jq -cs 'map(select(. != "")) | unique | sort')"
  if [[ "$cur" == "$want" ]]; then
    info "$cluster: API server already limited to $want"
    return 0
  fi
  act "$cluster: limit the AKS API server to $want (your CIDRs + node egress; was $cur)" \
    azs aks update -g "$rg" -n "$cluster" --api-server-authorized-ip-ranges "$(jq -r 'join(",")' <<<"$want")" >/dev/null
}

# Private clusters: ClickHouse reaches the API server through a Private Link service in
# your resource group. Rejecting ClickHouse's connection closes it. A rejected connection
# cannot be approved again: after reconnect, ClickHouse creates a new private endpoint.
pls_disconnect() {
  local rg="$1" cluster="$2" dir="$3" names pls obj orig file subs conns c
  names="$(azs network private-link-service list -g "$rg" | jq -r '.[] | select(.name | endswith("-apiserver-pls")) | .name')"
  while read -r pls <&3; do
    [[ -n "$pls" ]] || continue
    obj="$(azs network private-link-service show -g "$rg" -n "$pls")"
    file="$dir/pls-$(safe_name "$pls").json"
    save_once "$file" "$obj"
    orig="$obj"
    if [[ -f "$file" ]]; then orig="$(cat "$file")"; fi
    # ClickHouse's subscriptions: the ones this service auto-approved (saved original),
    # plus ClickHouse's known management subscriptions. Subscriptions that are only in
    # the visibility list may be yours (a hub), so they are reported, not rejected.
    subs="$(jq -c --arg known "$(lower "$CH_SUBSCRIPTIONS")" --arg own "$(lower "$SUBSCRIPTION")" \
      '[(.autoApproval.subscriptions // [])[], ($known | split(" "))[]]
       | map(ascii_downcase | select(. != "" and . != $own and . != "*")) | unique' <<<"$orig")"
    local others
    others="$(jq -r --argjson subs "$subs" --arg own "$(lower "$SUBSCRIPTION")" '[.privateEndpointConnections[]?
      | select(.privateLinkServiceConnectionState.status != "Rejected")
      | ((.privateEndpoint.id // "") | ascii_downcase | capture("^/subscriptions/(?<s>[^/]+)/").s // "") as $s
      | select($s != $own and ($subs | index($s) | not)) | .name + " (subscription " + $s + ")"] | join(", ")' <<<"$obj")"
    if [[ -n "$others" ]]; then
      warn "$cluster: $pls also has connections from other subscriptions, left alone; reject them by hand if they are ClickHouse's: $others"
    fi
    if [[ "$(jq -c '[(.autoApproval.subscriptions // [])[], (.visibility.subscriptions // [])[]] | length' <<<"$obj")" != 0 ]]; then
      act "$cluster: remove auto-approval and visibility from Private Link service $pls (saved)" \
        azs resource update --ids "$(jq -r .id <<<"$obj")" --set 'properties.autoApproval.subscriptions=[]' 'properties.visibility.subscriptions=[]' >/dev/null
    fi
    conns="$(jq -c --argjson subs "$subs" '.privateEndpointConnections[]?
      | select(.privateLinkServiceConnectionState.status != "Rejected")
      | select(((.privateEndpoint.id // "") | ascii_downcase | capture("^/subscriptions/(?<s>[^/]+)/").s // "") as $s | $subs | index($s))' <<<"$obj")"
    while read -r c <&4; do
      [[ -n "$c" ]] || continue
      act "$cluster: reject ClickHouse private endpoint connection $(jq -r .name <<<"$c") on $pls (one-way)" \
        azs network private-link-service connection update -g "$rg" --service-name "$pls" -n "$(jq -r .name <<<"$c")" \
        --connection-status Rejected --description "ClickHouse kill switch" >/dev/null
    done 4<<<"$conns"
  done 3<<<"$names"
}

network_reconnect() {
  local rg="$1" cluster="$2" dir="$3" orig cur f
  [[ -f "$dir/cluster.json" ]] || return 0
  if is_private "$(cat "$dir/cluster.json")"; then
    shopt -s nullglob
    for f in "$dir"/pls-*.json; do
      local id want now
      id="$(jq -r .id "$f")"
      want="$(jq -c '{a: (.autoApproval.subscriptions // [] | sort), v: (.visibility.subscriptions // [] | sort)}' "$f")"
      now="$(azs resource show --ids "$id" | jq -c '{a: (.properties.autoApproval.subscriptions // [] | sort), v: (.properties.visibility.subscriptions // [] | sort)}')"
      if [[ "$want" == "$now" ]]; then
        info "$cluster: $(jq -r .name "$f") auto-approval and visibility already restored"
      else
        act "$cluster: restore auto-approval and visibility on $(jq -r .name "$f")" \
          azs resource update --ids "$id" --set "properties.autoApproval.subscriptions=$(jq -c .a <<<"$want")" \
          "properties.visibility.subscriptions=$(jq -c .v <<<"$want")" >/dev/null
      fi
    done
    shopt -u nullglob
    warn "$cluster: the rejected ClickHouse private endpoint connection stays rejected. Ask ClickHouse"
    warn "support to recreate its management private endpoint; until then ClickHouse cannot reach the cluster."
    return 0
  fi
  orig="$(jq -c '.apiServerAccessProfile.authorizedIpRanges // [] | sort' "$dir/cluster.json")"
  cur="$(azs aks show -g "$rg" -n "$cluster" | jq -c '.apiServerAccessProfile.authorizedIpRanges // [] | sort')"
  if [[ "$cur" == "$orig" ]]; then
    info "$cluster: API server authorized ranges already match the saved settings"
    return 0
  fi
  act "$cluster: restore AKS API server authorized ranges $orig" \
    azs aks update -g "$rg" -n "$cluster" --api-server-authorized-ip-ranges "$(jq -r 'join(",")' <<<"$orig")" >/dev/null
}

KUBE_FAILURE=""

kubeconfig_for() {
  local rg="$1" cluster="$2" tries="$3" i
  KUBECONFIG_FILE="$WORK_DIR/$(safe_name "$cluster").kubeconfig"
  KUBE_FAILURE="no cluster-admin kubectl access from this machine (run from a host allowed by --admin-cidr; private clusters: from inside the VNet)"
  if ! azs aks get-credentials -g "$rg" -n "$cluster" --admin --file "$KUBECONFIG_FILE" --overwrite-existing >/dev/null 2>&1; then
    # Local accounts disabled: use your Entra ID identity through kubelogin.
    if ! command -v kubelogin >/dev/null 2>&1; then
      KUBE_FAILURE="the cluster has local accounts disabled and kubelogin is not installed (az aks install-cli)"
      return 1
    fi
    azs aks get-credentials -g "$rg" -n "$cluster" --file "$KUBECONFIG_FILE" --overwrite-existing >/dev/null 2>&1 || return 1
    kubelogin convert-kubeconfig -l azurecli --kubeconfig "$KUBECONFIG_FILE" >/dev/null 2>&1 || return 1
  fi
  chmod 600 "$KUBECONFIG_FILE"
  for ((i = 1; i <= tries; i++)); do
    if kubectl --kubeconfig "$KUBECONFIG_FILE" --request-timeout=15s auth can-i '*' '*' --all-namespaces >/dev/null 2>&1; then
      return 0
    fi
    if [[ $i -lt $tries ]]; then sleep 10; fi
  done
  return 1
}

# ---------------------------------------------------------------------------
# Kubernetes (the same code is in the AWS, GCP and Azure scripts)
# ---------------------------------------------------------------------------

# ClusterRoleBindings ClickHouse creates in every BYOC cluster.
readonly CH_CRB_NAMES='-argocd-cluster-admin$|^clickhouse-byoc-eng-|^byoc-eng-admin-clusterrolebinding$|^edit-regulated-clusterrolebinding$|^eks-readonly-group-role-binding$'
# In-cluster users that are ClickHouse identities (cloud specific; on Azure ClickHouse's
# cluster access goes through the identities and bindings handled elsewhere).
CH_K8S_USER_RE='^$'

k() { kubectl --kubeconfig "$KUBECONFIG_FILE" --request-timeout=60s "$@"; }

# selected: bindings to delete (ClickHouse names, or every subject is a ClickHouse identity)
# mixed:    bindings that also grant someone else; left alone and reported
classify_crbs() {
  k get clusterrolebindings -o json | jq -c --arg re "$CH_K8S_USER_RE" --arg names "$CH_CRB_NAMES" '
    def ch_subject: (.kind == "User" and (.name | test($re)))
      or (.kind == "ServiceAccount" and .namespace == "kube-system" and (.name | test("^clickhouse-eng-|-argocd$")));
    [.items[] | . as $b | {b: $b,
       named: ($b.metadata.name | test($names)),
       all_ch: ((($b.subjects // []) | length) > 0 and (($b.subjects // []) | all(ch_subject))),
       any_ch: (($b.subjects // []) | any(ch_subject))}]
    | {selected: [.[] | select(.named or .all_ch) | .b],
       mixed: [.[] | select((.named or .all_ch | not) and .any_ch) | .b.metadata.name]}'
}

k8s_disconnect() {
  local dir="$1" label="$2" n list crbs workloads
  # ClickHouse's in-cluster credentials first: while ArgoCD's ClusterRoleBinding exists,
  # ArgoCD self-heal scales the tailscale-nginx Deployments straight back up (their
  # replica count is in the chart).
  # ArgoCD's non-expiring ServiceAccount token: deleting the Secret revokes it.
  list="$(k -n kube-system get secrets -o json |
    jq -r '.items[] | select(.type == "kubernetes.io/service-account-token" and (.metadata.name | test("-argocd-secret$"))) | .metadata.name')"
  while read -r n <&3; do
    [[ -n "$n" ]] || continue
    act "$label: delete ArgoCD token Secret kube-system/$n" k -n kube-system delete secret "$n"
  done 3<<<"$list"
  # Engineer ServiceAccounts: deleting them revokes every token minted for them.
  list="$(k -n kube-system get serviceaccounts -o json | jq -r '.items[].metadata.name | select(startswith("clickhouse-eng-"))')"
  while read -r n <&3; do
    [[ -n "$n" ]] || continue
    act "$label: delete engineer ServiceAccount kube-system/$n" k -n kube-system delete serviceaccount "$n"
  done 3<<<"$list"
  # ClusterRoleBindings that grant ClickHouse identities access (saved for reconnect).
  crbs="$(classify_crbs)"
  if applying; then
    if [[ -f "$dir/k8s/clusterrolebindings.json" ]]; then
      jq -c --slurpfile old "$dir/k8s/clusterrolebindings.json" '$old[0] + .selected | unique_by(.metadata.name)' <<<"$crbs" >"$dir/k8s/crbs.tmp"
      mv "$dir/k8s/crbs.tmp" "$dir/k8s/clusterrolebindings.json"
    else
      save_once "$dir/k8s/clusterrolebindings.json" "$(jq -c .selected <<<"$crbs")"
    fi
  fi
  list="$(jq -r '.selected[].metadata.name' <<<"$crbs")"
  while read -r n <&3; do
    [[ -n "$n" ]] || continue
    act "$label: delete ClusterRoleBinding $n (saved)" k delete clusterrolebinding "$n"
  done 3<<<"$list"
  n="$(jq -r '.mixed | join(", ")' <<<"$crbs")"
  if [[ -n "$n" ]]; then
    warn "$label: these ClusterRoleBindings grant ClickHouse identities and others; review them by hand: $n"
  fi
  # Tailscale is ClickHouse's network path into the cluster (Kubernetes API, metrics,
  # SQL escalations). Scale the operator down first so it does not recreate proxies.
  # Never scale anything else: the ClickHouse operator and webhooks must keep running.
  if ! k get namespace tailscale >/dev/null 2>&1; then
    info "$label: no tailscale namespace"
    return 0
  fi
  workloads="$(k -n tailscale get deployments,statefulsets -o json | jq -c '[.items[] | {kind, name: .metadata.name, replicas: .spec.replicas}]')"
  save_once "$dir/k8s/tailscale-replicas.json" "$workloads"
  list="$(jq -r 'sort_by(.kind != "Deployment") | .[] | select(.replicas != 0) | (.kind | ascii_downcase) + "/" + .name' <<<"$workloads")"
  while read -r n <&3; do
    [[ -n "$n" ]] || continue
    act "$label: scale tailscale $n to 0" k -n tailscale scale "$n" --replicas=0
  done 3<<<"$list"
  applying || return 0
  # Something else (a controller ClickHouse still reaches) may have scaled them back up.
  list="$(k -n tailscale get deployments,statefulsets -o json |
    jq -r '.items[] | select(.spec.replicas != 0) | (.kind | ascii_downcase) + "/" + .metadata.name + " (" + (.spec.replicas | tostring) + ")"')"
  if [[ -n "$list" ]]; then
    incomplete "$label: tailscale workloads were scaled back up: $(tr '\n' ' ' <<<"$list")- run disconnect again"
  fi
}

k8s_reconnect() {
  local dir="$1" label="$2" obj n list cur before=${#INCOMPLETE[@]}
  if [[ -f "$dir/k8s/clusterrolebindings.json" ]]; then
    list="$(jq -c '.[]' "$dir/k8s/clusterrolebindings.json")"
    while read -r obj <&3; do
      [[ -n "$obj" ]] || continue
      n="$(jq -r .metadata.name <<<"$obj")"
      if k get clusterrolebinding "$n" >/dev/null 2>&1; then
        info "$label: ClusterRoleBinding $n already exists"
        continue
      fi
      act "$label: recreate ClusterRoleBinding $n" k create -f - <<<"$(jq '{apiVersion: "rbac.authorization.k8s.io/v1",
          kind: "ClusterRoleBinding", roleRef, subjects,
          metadata: {name: .metadata.name, labels: .metadata.labels,
                     annotations: ((.metadata.annotations // {}) | del(."kubectl.kubernetes.io/last-applied-configuration"))}}
        | del(.. | nulls)' <<<"$obj")" >/dev/null
    done 3<<<"$list"
  fi
  if [[ -f "$dir/k8s/tailscale-replicas.json" ]]; then
    list="$(jq -c '.[] | select(.replicas != null and .replicas != 0) | {ref: ((.kind | ascii_downcase) + "/" + .name), replicas}' "$dir/k8s/tailscale-replicas.json")"
    while read -r obj <&3; do
      [[ -n "$obj" ]] || continue
      n="$(jq -r .ref <<<"$obj")"
      if ! cur="$(k -n tailscale get "$n" -o jsonpath='{.spec.replicas}' 2>/dev/null)"; then
        incomplete "$label: could not read tailscale $n; scale it back to $(jq -r .replicas <<<"$obj") by hand, or re-run"
        continue
      fi
      if [[ "$cur" != 0 ]]; then
        info "$label: tailscale $n is at ${cur} replicas; leaving it"
        continue
      fi
      act "$label: scale tailscale $n back to $(jq -r .replicas <<<"$obj")" \
        k -n tailscale scale "$n" --replicas="$(jq -r .replicas <<<"$obj")"
    done 3<<<"$list"
  fi
  info "$label: ClickHouse recreates the ArgoCD token and engineer ServiceAccounts after it reconnects"
  if applying && [[ ${#INCOMPLETE[@]} -eq $before ]]; then
    mkdir -p "$dir/k8s"
    : >"$dir/k8s/restored"
  fi
}

k8s_status() {
  local label="$1" ts argo eng crbs state=CLOSED
  ts="$(k -n tailscale get pods --field-selector=status.phase=Running -o name 2>/dev/null | wc -l | tr -d ' ')"
  argo="$(k -n kube-system get secrets -o json | jq '[.items[] | select(.type == "kubernetes.io/service-account-token" and (.metadata.name | test("-argocd-secret$")))] | length')"
  eng="$(k -n kube-system get serviceaccounts -o json | jq '[.items[].metadata.name | select(startswith("clickhouse-eng-"))] | length')"
  crbs="$(classify_crbs | jq '.selected | length')"
  if [[ "$ts$argo$eng$crbs" != "0000" ]]; then state=OPEN; fi
  info "in-cluster: $state (running tailscale pods=$ts, ArgoCD tokens=$argo, engineer SAs=$eng, ClickHouse bindings=$crbs)"
}

# ---------------------------------------------------------------------------
# Legacy ClickHouse engineer guest accounts (older infrastructures)
# ---------------------------------------------------------------------------

# Prints a JSON array, or fails if Entra ID cannot be read.
ch_guests() {
  az rest --method get --url "https://graph.microsoft.com/v1.0/users" --only-show-errors -o json \
    --url-parameters "\$filter=startswith(displayName,'ClickHouse Engineer') and userType eq 'Guest'" \
    "\$select=id,displayName,mail,userPrincipalName,accountEnabled" |
    jq -c '[.value[] | select(((.mail // "") | ascii_downcase | endswith("@clickhouse.com"))
              or ((.userPrincipalName // "") | ascii_downcase | contains("_clickhouse.com#ext#")))
            | {key: .id, id, displayName, accountEnabled}]'
}

guest_enabled() {
  [[ "$(az rest --method get --url "https://graph.microsoft.com/v1.0/users/$1" --url-parameters "\$select=accountEnabled" \
    --only-show-errors -o json | jq -r .accountEnabled)" == true ]]
}

guests_disconnect() {
  step "Legacy ClickHouse engineer guest accounts"
  if [[ "$SUBSCRIPTION_ONLY" == true ]]; then
    info "skipped (--subscription-only): guest users belong to your whole tenant; ClickHouse Engineer guests are left as they are"
    return 0
  fi
  local guests g list
  if ! guests="$(ch_guests 2>/dev/null)"; then
    if applying; then
      incomplete "could not read Entra ID guest users; check for 'ClickHouse Engineer (...)' guests by hand"
    else
      warn "could not read Entra ID guest users"
    fi
    return 0
  fi
  guests="$(jq -c '[.[] | select(.accountEnabled == true)]' <<<"$guests")"
  if [[ "$guests" == "[]" ]]; then
    info "none enabled"
    return 0
  fi
  save_merge "$STATE_DIR/guests.json" "$guests"
  list="$(jq -c '.[]' <<<"$guests")"
  while read -r g <&3; do
    [[ -n "$g" ]] || continue
    act_entra "disable guest $(jq -r .displayName <<<"$g")" \
      az ad user update --id "$(jq -r .id <<<"$g")" --account-enabled false --only-show-errors
  done 3<<<"$list"
}

guests_reconnect() {
  [[ -f "$STATE_DIR/guests.json" ]] || return 0
  step "Legacy ClickHouse engineer guest accounts"
  if [[ "$SUBSCRIPTION_ONLY" == true ]]; then
    incomplete "an earlier disconnect disabled ClickHouse Engineer guest users; --subscription-only leaves them disabled. Run reconnect without --subscription-only to re-enable them."
    return 0
  fi
  local g list
  list="$(jq -c '.[]' "$STATE_DIR/guests.json")"
  while read -r g <&3; do
    [[ -n "$g" ]] || continue
    if guest_enabled "$(jq -r .id <<<"$g")" 2>/dev/null; then
      info "guest $(jq -r .displayName <<<"$g") already enabled"
      continue
    fi
    act_entra "re-enable guest $(jq -r .displayName <<<"$g")" \
      az ad user update --id "$(jq -r .id <<<"$g")" --account-enabled true --only-show-errors
  done 3<<<"$list"
}

guests_status() {
  step "Legacy ClickHouse engineer guest accounts"
  if [[ "$SUBSCRIPTION_ONLY" == true ]]; then
    info "skipped (--subscription-only): guest users belong to your whole tenant and are left as they are"
    return 0
  fi
  local guests n
  if ! guests="$(ch_guests 2>/dev/null)"; then
    info "not readable (needs read access to Entra ID users)"
    return 0
  fi
  n="$(jq '[.[] | select(.accountEnabled == true)] | length' <<<"$guests")"
  info "enabled guests: $(if [[ "$n" == 0 ]]; then echo CLOSED; else echo "OPEN ($n)"; fi)"
}

# ---------------------------------------------------------------------------
# Per cluster
# ---------------------------------------------------------------------------

cluster_dir() { printf '%s/aks/%s/%s' "$STATE_DIR" "$1" "$2"; }

cluster_disconnect() {
  local rg="$1" cluster="$2" dir desc tries=1
  dir="$(cluster_dir "$rg" "$cluster")"
  step "AKS $cluster ($rg)"
  desc="$(azs aks show -g "$rg" -n "$cluster")"
  save_once "$dir/cluster.json" "$desc"
  identities_disconnect "$rg" "$cluster" "$(jq -r '.oidcIssuerProfile.issuerUrl // empty' <<<"$desc")" "$dir"
  network_disconnect "$rg" "$cluster" "$desc" "$dir"
  info "$cluster: ClickHouse may hold an AKS admin client certificate; the network restriction above blocks it"
  if [[ "$SKIP_KUBERNETES" == true ]]; then
    incomplete "$cluster: --skip-kubernetes: Tailscale and the in-cluster ClickHouse credentials stay open"
    return 0
  fi
  if applying; then tries=6; fi
  if ! kubeconfig_for "$rg" "$cluster" "$tries"; then
    if applying; then
      incomplete "$cluster: $KUBE_FAILURE, so Tailscale stays open. Fix that and run disconnect again."
    else
      incomplete "$cluster: $KUBE_FAILURE; disconnect would leave Tailscale and the in-cluster ClickHouse credentials open"
    fi
    return 0
  fi
  k8s_disconnect "$dir" "$cluster"
}

cluster_reconnect() {
  local rg="$1" cluster="$2" dir
  dir="$(cluster_dir "$rg" "$cluster")"
  step "AKS $cluster ($rg)"
  if [[ ! -d "$dir" ]]; then
    warn "$cluster: no saved state in $dir; skipping"
    return 0
  fi
  # In-cluster objects first: restoring the network removes your kubectl access.
  if [[ -f "$dir/k8s/restored" ]]; then
    info "$cluster: in-cluster objects already restored"
  elif [[ "$SKIP_KUBERNETES" != true ]] && kubeconfig_for "$rg" "$cluster" 3; then
    k8s_reconnect "$dir" "$cluster"
  elif [[ -d "$dir/k8s" ]]; then
    incomplete "$cluster: Tailscale and ClickHouse's ClusterRoleBindings not restored ($KUBE_FAILURE, or --skip-kubernetes). Run reconnect again from the host you disconnected from."
  fi
  # Network before identities, so ClickHouse never sees your temporary settings.
  network_reconnect "$rg" "$cluster" "$dir"
  identities_reconnect "$rg" "$cluster" "$dir"
}

cluster_status() {
  local rg="$1" cluster="$2" desc fics n list ident pid
  step "AKS $cluster ($rg)"
  desc="$(azs aks show -g "$rg" -n "$cluster")"
  if is_private "$desc"; then
    info "API server: private"
    azs network private-link-service list -g "$rg" |
      jq -r --arg known "$(lower "$CH_SUBSCRIPTIONS")" --arg own "$(lower "$SUBSCRIPTION")" '.[] | select(.name | endswith("-apiserver-pls"))
        | .name as $p | ([(.autoApproval.subscriptions // [])[] | ascii_downcase] + ($known | split(" "))) as $ch
        | .privateEndpointConnections[]?
        | ((.privateEndpoint.id // "") | ascii_downcase | capture("^/subscriptions/(?<s>[^/]+)/").s // "") as $s
        | select($s != $own)
        | "  " + $p + " connection " + .name + " from " + (if ($ch | index($s)) then "ClickHouse" else "subscription " + $s + " (review)" end) + ": "
          + (if .privateLinkServiceConnectionState.status == "Rejected" then "CLOSED (rejected)" else "OPEN (" + .privateLinkServiceConnectionState.status + ")" end)' >&2
  else
    info "API server authorized ranges: $(jq -c '.apiServerAccessProfile.authorizedIpRanges // ["(any address: OPEN)"]' <<<"$desc")"
  fi
  fics="$(foreign_fics "$rg" "$(jq -r '.oidcIssuerProfile.issuerUrl // empty' <<<"$desc")")"
  n="$(jq '.ch | length' <<<"$fics")"
  info "ClickHouse federated credentials: $(if [[ "$n" == 0 ]]; then echo CLOSED; else echo "OPEN ($(jq -r '[.ch[] | .identity + "/" + .name] | join(", ")' <<<"$fics"))"; fi)"
  list="$(azs identity list -g "$rg" | jq -r '.[] | select(.name | endswith("-data-plane-mgmt")) | .name + " " + .principalId')"
  while read -r ident pid <&3; do
    [[ -n "$ident" ]] || continue
    n="$(role_assignments_of "$pid" | jq '.in | length')"
    info "$ident role assignments: $(if [[ "$n" == 0 ]]; then echo CLOSED; else echo "OPEN ($n)"; fi)"
  done 3<<<"$list"
  if [[ "$SKIP_KUBERNETES" != true ]] && kubeconfig_for "$rg" "$cluster" 1; then
    k8s_status "$cluster"
  else
    info "in-cluster: not checked ($KUBE_FAILURE)"
  fi
}

for_each_cluster() {
  local fn="$1" rg cluster
  [[ -n "$CLUSTER_LIST" ]] || warn "no clickhouse-cloud-* AKS cluster in subscription $SUBSCRIPTION"
  while read -r rg cluster <&3; do
    [[ -n "$cluster" ]] || continue
    "$fn" "$rg" "$cluster"
  done 3<<<"$CLUSTER_LIST"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
  parse_args "$@"
  need az
  need jq
  [[ "$SKIP_KUBERNETES" == true ]] || need kubectl
  WORK_DIR="$(mktemp -d)"
  log "ClickHouse BYOC kill switch for Azure v$VERSION: $COMMAND"
  discover
  info "state dir: $STATE_DIR"
  info "clusters:  $(cut -d' ' -f2 <<<"$CLUSTER_LIST" | tr '\n' ' ')"
  local scope_flag=""
  if [[ "$SUBSCRIPTION_ONLY" == true ]]; then
    scope_flag=" --subscription-only"
    info "scope:     this subscription only (--subscription-only): ClickHouse app sign-in and guest users are left as they are"
  fi

  case "$COMMAND" in
    status)
      provisioner_status
      for_each_cluster cluster_status
      guests_status
      ;;
    plan|disconnect)
      resolve_admin_cidrs
      info "admin CIDRs: ${ADMIN_CIDRS[*]}"
      if [[ "$COMMAND" == "disconnect" ]]; then
        log ""
        log "This blocks ClickHouse from your subscription and AKS clusters. Your services keep"
        log "serving; console operations, backups and upgrades stop until you reconnect."
        log "Do not run the ClickHouse onboarding Terraform while disconnected: it re-grants access."
        confirm
        if [[ -f "$STATE_DIR/reconnected-at" ]]; then
          mv "$STATE_DIR" "$STATE_DIR.reconnected-$(cat "$STATE_DIR/reconnected-at")"
          info "archived the state of the previous disconnect"
        fi
        mkdir -p "$STATE_DIR"
        chmod 700 "$STATE_DIR"
        save_once "$STATE_DIR/disconnected-at" "$(date -u +%s)"
      fi
      # Identities first: they stop ClickHouse from reverting the steps that follow.
      provisioner_disconnect
      for_each_cluster cluster_disconnect
      guests_disconnect
      finish
      step "Done"
      if [[ "$COMMAND" == "plan" ]]; then
        info "Nothing was changed. Run 'disconnect' to apply."
      else
        info "Disconnected. Keep $STATE_DIR: reconnect needs it."
        info "Check with: $0 status --subscription $SUBSCRIPTION$scope_flag"
      fi
      if [[ -n "$scope_flag" ]]; then
        info "Left as they are (--subscription-only): the ClickHouse app's sign-in to your tenant, and ClickHouse Engineer guest users."
      fi
      ;;
    reconnect)
      [[ -d "$STATE_DIR" ]] || die "no saved state in $STATE_DIR (pass --state-dir)"
      if [[ -f "$STATE_DIR/reconnected-at" ]]; then
        info "Already reconnected at $(cat "$STATE_DIR/reconnected-at"); nothing to do."
        exit 0
      fi
      if [[ -f "$STATE_DIR/disconnected-at" ]] && (($(date -u +%s) - $(cat "$STATE_DIR/disconnected-at") < 86400)); then
        warn "You disconnected less than 24 hours ago. Managed-identity tokens ClickHouse obtained"
        warn "before the disconnect regain their permissions when the role assignments come back."
        warn "If you suspect those tokens were stolen, wait until 24 hours have passed."
      fi
      confirm
      for_each_cluster cluster_reconnect
      guests_reconnect
      provisioner_reconnect
      finish
      date -u +%Y%m%dT%H%M%SZ >"$STATE_DIR/reconnected-at"
      step "Done"
      info "Reconnected. Tell ClickHouse support; ClickHouse catches up within about 30 minutes."
      ;;
  esac
}

main "$@"
