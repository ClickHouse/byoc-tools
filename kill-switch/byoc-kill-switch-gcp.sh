#!/usr/bin/env bash
#
# ClickHouse BYOC kill switch for GCP.
#
# Temporarily disconnects ClickHouse from a standard BYOC deployment in this GCP
# project. Your ClickHouse services keep serving queries; ClickHouse can no longer
# act in your project or your GKE cluster until you run `reconnect`.
#
#   plan        (default) read-only: list ClickHouse access paths and what would change
#   disconnect  cut every path (asks for confirmation unless --yes)
#   reconnect   restore everything this script changed (asks unless --yes)
#   status      read-only: show OPEN / CLOSED for every path
#
# Requires: bash 4.4+, gcloud (with gke-gcloud-auth-plugin), jq, kubectl. Run as a
# project owner: the script disables service accounts, edits the project IAM policy and
# updates the GKE cluster. kubectl connects through the GKE DNS endpoint, so it works
# from any machine (Cloud Shell included).
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
# ClickHouse's management projects (their service accounts run dpm, ArgoCD and
# Crossplane) and ClickHouse's BYOC project (eng-admin and runtime identities).
readonly CH_MGMT_PROJECTS_RE='dataplane-(production|staging|development)'
readonly CH_BYOC_PROJECTS_RE='byoc-(prod-9300|staging-c668|dev-8a23)'
# Project IAM members to remove: ClickHouse management service accounts and ClickHouse
# engineers. Runtime identities in the BYOC project are kept, and so are the bindings of
# the account running this script (see SELF_MEMBERS).
readonly CH_MEMBER_RE="^serviceAccount:[^@]+@${CH_MGMT_PROJECTS_RE}\\.iam\\.gserviceaccount\\.com\$|^user:[^@]+@clickhouse\\.com\$"
# jq: is this IAM member (.) one to remove? Needs --arg re "$CH_MEMBER_RE" --argjson self "$SELF_MEMBERS".
# shellcheck disable=SC2016  # a jq program, not shell
readonly JQ_CH_MEMBER='def self_member: . as $m | test($re) and any($self[]; . == $m);
  def ch_member: test($re) and (self_member | not);'
# Service accounts in your project that ClickHouse impersonates.
readonly CH_SA_RE='^clickhouse-management@|^[a-z0-9-]+-dp-mgmt@'

COMMAND="plan"
PROJECT=""
CLUSTERS=()
ADMIN_CIDRS=()
SKIP_KUBERNETES=false
ASSUME_YES=false
CONFIRMED=false
RESTORE_USER_BINDINGS=false
# The active gcloud account, as project IAM members. Never removed: removing them could
# lock this script out of the project halfway through.
ACCOUNT=""
SELF_MEMBERS="[]"
STATE_DIR=""
WORK_DIR=""
KUBECONFIG_FILE=""
INCOMPLETE=()

usage() {
  cat <<'EOF'
Usage: byoc-kill-switch-gcp.sh <plan|disconnect|reconnect|status> --project PROJECT [options]

Options:
  --project PROJECT     GCP project of the BYOC infrastructure
  --cluster NAME        GKE cluster (default: every clickhouse-cloud-* cluster in the project)
  --admin-cidr CIDR     Extra CIDR to keep allowed on the GKE public IP endpoint (repeatable;
                        default: none; kubectl in this script uses the DNS endpoint)
  --skip-kubernetes     Do not touch objects inside the cluster (leaves Tailscale running)
  --state-dir DIR       Where original settings are saved
                        (default: ./clickhouse-byoc-kill-switch-gcp-<project>)
  --restore-user-bindings
                        reconnect: also add back the user:*@clickhouse.com bindings that
                        disconnect removed (default: they stay removed). Works after an
                        earlier reconnect too.
  --yes                 Do not ask for confirmation
  -h, --help            Show this help

disconnect never removes the project IAM bindings of the account running the script.
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

need() { command -v "$1" >/dev/null 2>&1 || die "'$1' is required but not installed"; }

confirm() {
  [[ "$ASSUME_YES" == true || "$CONFIRMED" == true ]] && return 0
  local answer
  printf '\nType "%s" to continue: ' "$COMMAND" >&2
  read -r answer
  [[ "$answer" == "$COMMAND" ]] || die "aborted"
  CONFIRMED=true
}

safe_name() { printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '_'; }

save_once() {
  applying || return 0
  [[ -f "$1" ]] && return 0
  mkdir -p "$(dirname "$1")"
  printf '%s\n' "$2" >"$1"
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

g() { gcloud --project "$PROJECT" --quiet "$@"; }

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
      --project|--cluster|--admin-cidr|--state-dir)
        [[ $# -ge 2 && -n "$2" && "$2" != -* ]] || die "$1 needs a value" ;;
    esac
    case "$1" in
      --project) PROJECT="$2"; shift 2 ;;
      --cluster) CLUSTERS+=("$2"); shift 2 ;;
      --admin-cidr) ADMIN_CIDRS+=("$2"); shift 2 ;;
      --skip-kubernetes) SKIP_KUBERNETES=true; shift ;;
      --restore-user-bindings) RESTORE_USER_BINDINGS=true; shift ;;
      --state-dir) STATE_DIR="${2%/}"; shift 2 ;;
      --yes) ASSUME_YES=true; shift ;;
      -h|--help) usage; exit 0 ;;
      *) usage; die "unknown argument: $1" ;;
    esac
  done
  case "$COMMAND" in plan|disconnect|reconnect|status) ;; *) usage; die "unknown command: $COMMAND" ;; esac
  [[ -n "$PROJECT" ]] || die "--project is required"
  [[ -n "$STATE_DIR" ]] || STATE_DIR="./clickhouse-byoc-kill-switch-gcp-${PROJECT}"
  local c
  for c in "${CLUSTERS[@]}"; do
    [[ "$c" == clickhouse-cloud-* ]] || die "--cluster $c is not a ClickHouse BYOC cluster (clickhouse-cloud-*)"
  done
}

# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

CH_SAS=()
CLUSTER_LIST=""  # "name location" per line

discover() {
  local sas all c
  sas="$(g iam service-accounts list --format=json)"
  # clickhouse-management first: it can re-grant everything else.
  mapfile -t CH_SAS < <(jq -r --arg re "$CH_SA_RE" '[.[].email | select(test($re))] | sort_by(startswith("clickhouse-management@") | not) | .[]' <<<"$sas")
  all="$(g container clusters list --format=json)"
  for c in "${CLUSTERS[@]}"; do
    jq -e --arg c "$c" 'any(.[]; .name == $c)' <<<"$all" >/dev/null || die "cluster $c not found in project $PROJECT"
  done
  CLUSTER_LIST="$(jq -r --argjson want "$(printf '%s\n' "${CLUSTERS[@]}" | jq -R . | jq -s 'map(select(. != ""))')" \
    '.[] | select(if ($want | length) > 0 then (.name as $n | $want | index($n)) else (.name | startswith("clickhouse-cloud-")) end)
     | .name + " " + .location' <<<"$all")"
}

# ---------------------------------------------------------------------------
# Service accounts
# ---------------------------------------------------------------------------

sa_disabled() { g iam service-accounts describe "$1" --format=json | jq -e '.disabled == true' >/dev/null; }

sa_disconnect() {
  step "Service accounts ClickHouse impersonates (${#CH_SAS[@]})"
  [[ ${#CH_SAS[@]} -gt 0 ]] || warn "found neither clickhouse-management@ nor *-dp-mgmt@ in $PROJECT"
  local sa
  # Disabling rejects the access tokens ClickHouse already holds, not only new ones.
  for sa in "${CH_SAS[@]}"; do
    if sa_disabled "$sa"; then
      info "$sa: already disabled"
      continue
    fi
    save_once "$STATE_DIR/service-accounts/$(safe_name "$sa")" "$sa"
    act "$sa: disable" g iam service-accounts disable "$sa"
  done
}

sa_reconnect() {
  step "Service accounts"
  local f sa saved=() ordered=()
  shopt -s nullglob
  for f in "$STATE_DIR"/service-accounts/*; do saved+=("$(cat "$f")"); done
  shopt -u nullglob
  # Reverse of disconnect: the -dp-mgmt account first, clickhouse-management last.
  for sa in "${saved[@]}"; do
    if [[ "$sa" != clickhouse-management@* ]]; then ordered+=("$sa"); fi
  done
  for sa in "${saved[@]}"; do
    if [[ "$sa" == clickhouse-management@* ]]; then ordered+=("$sa"); fi
  done
  for sa in "${ordered[@]}"; do
    if sa_disabled "$sa"; then
      act "$sa: enable" g iam service-accounts enable "$sa"
    else
      info "$sa: already enabled"
    fi
  done
}

sa_status() {
  step "Service accounts"
  local sa
  for sa in "${CH_SAS[@]}"; do
    if sa_disabled "$sa"; then info "$sa: CLOSED (disabled)"; else info "$sa: OPEN"; fi
  done
}

# ---------------------------------------------------------------------------
# Project IAM policy
# ---------------------------------------------------------------------------

# Bindings (role, condition, ClickHouse members only) that grant ClickHouse identities,
# leaving out the account running this script.
ch_bindings() {
  jq -c --arg re "$CH_MEMBER_RE" --argjson self "$SELF_MEMBERS" "$JQ_CH_MEMBER"'
    [.bindings[]? | {role, condition, members: [.members[] | select(ch_member)]}
     | select(.members | length > 0) | del(.condition | nulls)]'
}

# "role: member" for each binding of the running account that would otherwise be removed.
self_bindings() {
  jq -r --arg re "$CH_MEMBER_RE" --argjson self "$SELF_MEMBERS" "$JQ_CH_MEMBER"'
    [.bindings[]? | .role as $r | .members[] | select(self_member) | $r + ": " + .] | join(", ")'
}

iam_disconnect() {
  step "Project IAM: remove ClickHouse management principals"
  local policy removed new_policy kept users
  policy="$(g projects get-iam-policy "$PROJECT" --format=json)"
  kept="$(self_bindings <<<"$policy")"
  [[ -z "$kept" ]] || info "kept, because they are your own account's and removing them could lock you out: $kept"
  removed="$(ch_bindings <<<"$policy")"
  if [[ "$removed" == "[]" ]]; then
    info "no ClickHouse management principals in the project IAM policy"
    return 0
  fi
  jq -r '.[] | "  " + .role + ": " + (.members | join(", "))' <<<"$removed" >&2
  users="$(jq -r '[.[].members[] | select(startswith("user:"))] | unique | length' <<<"$removed")"
  if [[ "$users" -gt 0 ]]; then
    info "reconnect does not add the $users user: members back unless you pass --restore-user-bindings"
  fi
  save_once "$STATE_DIR/project-policy.original.json" "$policy"
  if applying; then
    if [[ -f "$STATE_DIR/project-bindings.json" ]]; then
      jq -c --slurpfile old "$STATE_DIR/project-bindings.json" '$old[0] + . | unique' <<<"$removed" >"$STATE_DIR/bindings.tmp"
      mv "$STATE_DIR/bindings.tmp" "$STATE_DIR/project-bindings.json"
    else
      save_once "$STATE_DIR/project-bindings.json" "$removed"
    fi
  fi
  new_policy="$(jq -c --arg re "$CH_MEMBER_RE" --argjson self "$SELF_MEMBERS" "$JQ_CH_MEMBER"'
    .bindings = [.bindings[] | .members = [.members[] | select(ch_member | not)] | select(.members | length > 0)]' <<<"$policy")"
  printf '%s\n' "$new_policy" >"$WORK_DIR/policy.json"
  act "remove these bindings from the project IAM policy (saved)" \
    g projects set-iam-policy "$PROJECT" "$WORK_DIR/policy.json" --format=none
}

# iam_restore DESC BINDINGS SOFT: add BINDINGS (JSON, as saved) to the project IAM policy.
# SOFT=true: a refused update is reported (exit code 2) instead of stopping the script.
iam_restore() {
  local desc="$1" restore="$2" soft="$3" policy
  policy="$(g projects get-iam-policy "$PROJECT" --format=json)"
  jq -c --argjson add "$restore" '
    reduce $add[] as $b (.;
      if any(.bindings[]?; .role == $b.role and (.condition // null) == ($b.condition // null)) then
        .bindings = [.bindings[] | if .role == $b.role and (.condition // null) == ($b.condition // null)
                                   then .members = (.members + $b.members | unique) else . end]
      else .bindings += [$b] end)
    | if any(.bindings[]; .condition != null) then .version = 3 else . end' <<<"$policy" >"$WORK_DIR/policy.json"
  if jq -e --argjson p "$policy" '(.bindings | map(.members |= sort) | sort_by(.role)) == ($p.bindings | map(.members |= sort) | sort_by(.role))' \
    "$WORK_DIR/policy.json" >/dev/null; then
    info "project IAM policy already contains $desc"
    return 0
  fi
  # Asks only when called after an earlier reconnect (--restore-user-bindings).
  confirm
  if [[ "$soft" != true ]]; then
    act "add $desc back to the project IAM policy" g projects set-iam-policy "$PROJECT" "$WORK_DIR/policy.json" --format=none
  elif ! act "add $desc back to the project IAM policy" g projects set-iam-policy "$PROJECT" "$WORK_DIR/policy.json" --format=none; then
    incomplete "the project refused $desc (an organization policy such as domain-restricted sharing, or roles/owner for a user outside your organization). Add them by hand from $STATE_DIR/project-bindings.json, or re-run reconnect."
  fi
}

iam_reconnect() {
  step "Project IAM: restore ClickHouse management principals"
  if [[ ! -f "$STATE_DIR/project-bindings.json" ]]; then
    info "nothing was removed"
    return 0
  fi
  local users
  users="$(jq -r '[.[].members[] | select(startswith("user:"))] | unique | join(", ")' "$STATE_DIR/project-bindings.json")"
  # ClickHouse's service accounts first, on their own: if the project refuses the engineer
  # bindings, ClickHouse still gets its access back.
  iam_restore "the saved ClickHouse service account bindings" \
    "$(jq -c '[.[] | .members = [.members[] | select(startswith("user:") | not)] | select(.members | length > 0)]' "$STATE_DIR/project-bindings.json")" false
  [[ -n "$users" ]] || return 0
  if [[ "$RESTORE_USER_BINDINGS" != true ]]; then
    # By default ClickHouse engineer user bindings stay removed: ClickHouse no longer uses them.
    info "not restored (stale engineer grants ClickHouse no longer uses; pass --restore-user-bindings to add them back): $users"
    return 0
  fi
  info "restoring the ClickHouse engineer bindings too (--restore-user-bindings): $users"
  iam_restore "the saved ClickHouse engineer (user:) bindings" "$(jq -c . "$STATE_DIR/project-bindings.json")" true
}

iam_status() {
  step "Project IAM"
  local policy b kept
  policy="$(g projects get-iam-policy "$PROJECT" --format=json)"
  b="$(ch_bindings <<<"$policy")"
  if [[ "$b" == "[]" ]]; then
    info "ClickHouse management principals: CLOSED (none)"
  else
    info "ClickHouse management principals: OPEN"
    jq -r '.[] | "    " + .role + ": " + (.members | join(", "))' <<<"$b" >&2
  fi
  kept="$(self_bindings <<<"$policy")"
  [[ -z "$kept" ]] || info "not counted (your own account; disconnect keeps it): $kept"
}

# ---------------------------------------------------------------------------
# GKE control plane
# ---------------------------------------------------------------------------

describe_cluster() { g container clusters describe "$1" --location "$2" --format=json; }

# jq's // treats false as missing, so compare explicitly.
has_public_ip_endpoint() {
  jq -e '(.privateClusterConfig.enablePrivateEndpoint != true)
         and (.controlPlaneEndpointsConfig.ipEndpointsConfig.enabled != false)
         and (.controlPlaneEndpointsConfig.ipEndpointsConfig.enablePublicEndpoint != false)' >/dev/null <<<"$1"
}

dns_endpoint_public() {
  jq -e '.controlPlaneEndpointsConfig.dnsEndpointConfig.allowExternalTraffic == true' >/dev/null <<<"$1"
}

authorized_networks_of() {
  jq -c '{enabled: (.masterAuthorizedNetworksConfig.enabled == true),
          cidrs: ([.masterAuthorizedNetworksConfig.cidrBlocks[]?.cidrBlock] | sort),
          google: (.masterAuthorizedNetworksConfig.gcpPublicCidrsAccessEnabled == true)}' <<<"$1"
}

endpoint_disconnect() {
  local cluster="$1" location="$2" desc="$3" dir="$4" cur want
  if ! has_public_ip_endpoint "$desc"; then
    info "$cluster: no public IP endpoint; ClickHouse reaches the cluster only through IAM (closed above) and Tailscale"
    return 0
  fi
  if [[ "$(jq -r '.privateClusterConfig.enablePrivateNodes // false' <<<"$desc")" != true ]]; then
    warn "$cluster: nodes are not private; Google Cloud public IPs may still reach the IP endpoint"
  fi
  cur="$(authorized_networks_of "$desc")"
  # Keep your --admin-cidr ranges and any private (RFC 1918) ranges already allowed;
  # ClickHouse reaches the endpoint only from public addresses.
  want="$( {
      printf '%s\n' "${ADMIN_CIDRS[@]}"
      jq -r '.cidrs[] | select(test("^(10\\.|192\\.168\\.|172\\.(1[6-9]|2[0-9]|3[01])\\.)"))' <<<"$cur"
    } | jq -R . | jq -cs 'map(select(. != "")) | unique | sort | {enabled: true, cidrs: ., google: false}')"
  # Ranges you kept on purpose, so status does not count them as ClickHouse's.
  save_once "$dir/kept-cidrs.json" "$(jq -c .cidrs <<<"$want")"
  if [[ "$(jq -c '{enabled, cidrs}' <<<"$cur")" == "$(jq -c '{enabled, cidrs}' <<<"$want")" ]]; then
    info "$cluster: authorized networks already exclude ClickHouse"
  else
    local args=(container clusters update "$cluster" --location "$location" --enable-master-authorized-networks)
    if [[ "$(jq '.cidrs | length' <<<"$want")" -gt 0 ]]; then
      args+=("--master-authorized-networks=$(jq -r '.cidrs | join(",")' <<<"$want")")
    fi
    act "$cluster: allow only $(jq -c .cidrs <<<"$want") on the public IP endpoint (was $(jq -c .cidrs <<<"$cur"))" g "${args[@]}"
  fi
  if [[ "$(jq -r .google <<<"$cur")" == true ]]; then
    act "$cluster: stop Google Cloud public IPs reaching the public IP endpoint" \
      g container clusters update "$cluster" --location "$location" --no-enable-google-cloud-access
  fi
}

endpoint_reconnect() {
  local cluster="$1" location="$2" dir="$3" orig cur
  [[ -f "$dir/cluster.json" ]] || return 0
  has_public_ip_endpoint "$(cat "$dir/cluster.json")" || return 0
  orig="$(authorized_networks_of "$(cat "$dir/cluster.json")")"
  cur="$(authorized_networks_of "$(describe_cluster "$cluster" "$location")")"
  if [[ "$cur" == "$orig" ]]; then
    info "$cluster: authorized networks already match the saved settings"
    return 0
  fi
  if [[ "$(jq -r .enabled <<<"$orig")" == true ]]; then
    if [[ "$(jq -c '{enabled, cidrs}' <<<"$cur")" != "$(jq -c '{enabled, cidrs}' <<<"$orig")" ]]; then
      local args=(container clusters update "$cluster" --location "$location" --enable-master-authorized-networks)
      if [[ "$(jq -r '.cidrs | length' <<<"$orig")" -gt 0 ]]; then
        args+=("--master-authorized-networks=$(jq -r '.cidrs | join(",")' <<<"$orig")")
      fi
      act "$cluster: restore authorized networks $(jq -c .cidrs <<<"$orig")" g "${args[@]}"
    fi
    if [[ "$(jq -r .google <<<"$orig")" == true && "$(jq -r .google <<<"$cur")" != true ]]; then
      act "$cluster: restore Google Cloud public IP access" \
        g container clusters update "$cluster" --location "$location" --enable-google-cloud-access
    fi
  else
    act "$cluster: restore authorized networks to disabled" \
      g container clusters update "$cluster" --location "$location" --no-enable-master-authorized-networks
  fi
}

# kubeconfig_for CLUSTER LOCATION TRIES USE_DNS
kubeconfig_for() {
  local cluster="$1" location="$2" tries="$3" use_dns="$4" i args=()
  KUBECONFIG_FILE="$WORK_DIR/$(safe_name "$cluster").kubeconfig"
  if [[ "$use_dns" == true ]]; then args+=(--dns-endpoint); fi
  KUBECONFIG="$KUBECONFIG_FILE" g container clusters get-credentials "$cluster" --location "$location" "${args[@]}" >/dev/null 2>&1 || return 1
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
# In-cluster users that are ClickHouse identities: ClickHouse management and BYOC
# project service accounts, the dp-mgmt service account, and ClickHouse engineers.
CH_K8S_USER_RE="(@(${CH_MGMT_PROJECTS_RE}|${CH_BYOC_PROJECTS_RE})\\.iam\\.gserviceaccount\\.com|-dp-mgmt@[a-z0-9-]+\\.iam\\.gserviceaccount\\.com|@clickhouse\\.com)\$"

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
# Per cluster
# ---------------------------------------------------------------------------

cluster_dir() { printf '%s/gke/%s/%s' "$STATE_DIR" "$2" "$1"; }

# Before changing anything: can this machine reach the cluster for the in-cluster steps?
preflight_cluster() {
  local cluster="$1" location="$2" desc
  [[ "$SKIP_KUBERNETES" != true ]] || return 0
  desc="$(describe_cluster "$cluster" "$location")"
  dns_endpoint_public "$desc" && return 0
  if ! has_public_ip_endpoint "$desc"; then
    info "$cluster: no external endpoint for kubectl; run this script from inside the VPC (or use --skip-kubernetes)"
  elif [[ ${#ADMIN_CIDRS[@]} -eq 0 ]]; then
    die "$cluster: its DNS endpoint does not accept external traffic, so kubectl needs the public IP endpoint. Pass --admin-cidr with this machine's address, or --skip-kubernetes."
  fi
}

cluster_disconnect() {
  local cluster="$1" location="$2" dir desc tries=1 use_dns=true
  dir="$(cluster_dir "$cluster" "$location")"
  step "GKE $cluster ($location)"
  desc="$(describe_cluster "$cluster" "$location")"
  save_once "$dir/cluster.json" "$desc"
  dns_endpoint_public "$desc" || use_dns=false
  endpoint_disconnect "$cluster" "$location" "$desc" "$dir"
  if [[ "$SKIP_KUBERNETES" == true ]]; then
    incomplete "$cluster: --skip-kubernetes: Tailscale and the in-cluster ClickHouse credentials stay open"
    return 0
  fi
  if applying; then tries=6; fi
  if ! kubeconfig_for "$cluster" "$location" "$tries" "$use_dns"; then
    incomplete "$cluster: no cluster-admin kubectl access from here, so Tailscale stays open. You need container.clusters.connect and cluster-admin (for example roles/container.admin)."
    return 0
  fi
  k8s_disconnect "$dir" "$cluster"
}

cluster_reconnect() {
  local cluster="$1" location="$2" dir use_dns=true
  dir="$(cluster_dir "$cluster" "$location")"
  step "GKE $cluster ($location)"
  if [[ ! -d "$dir" ]]; then
    warn "$cluster: no saved state in $dir; skipping"
    return 0
  fi
  dns_endpoint_public "$(cat "$dir/cluster.json")" || use_dns=false
  if [[ -f "$dir/k8s/restored" ]]; then
    info "$cluster: in-cluster objects already restored"
  elif [[ "$SKIP_KUBERNETES" != true ]] && kubeconfig_for "$cluster" "$location" 3 "$use_dns"; then
    k8s_reconnect "$dir" "$cluster"
  elif [[ -d "$dir/k8s" ]]; then
    incomplete "$cluster: Tailscale and ClickHouse's ClusterRoleBindings not restored (no kubectl access from here, or --skip-kubernetes). Run reconnect again from a machine with cluster access."
  fi
  endpoint_reconnect "$cluster" "$location" "$dir"
}

cluster_status() {
  local cluster="$1" location="$2" desc dir use_dns=true
  dir="$(cluster_dir "$cluster" "$location")"
  step "GKE $cluster ($location)"
  desc="$(describe_cluster "$cluster" "$location")"
  dns_endpoint_public "$desc" || use_dns=false
  if has_public_ip_endpoint "$desc"; then
    local cur
    cur="$(authorized_networks_of "$desc")"
    if [[ -f "$dir/cluster.json" ]]; then
      local open kept="[]"
      if [[ -f "$dir/kept-cidrs.json" ]]; then kept="$(cat "$dir/kept-cidrs.json")"; fi
      open="$(jq -rn --argjson c "$cur" --argjson o "$(authorized_networks_of "$(cat "$dir/cluster.json")")" --argjson k "$kept" '
        if ($c.enabled | not) then "OPEN (authorized networks off)"
        elif $c.google then "OPEN (Google Cloud public IPs allowed)"
        else [$o.cidrs[] | select(test("^(10\\.|192\\.168\\.|172\\.(1[6-9]|2[0-9]|3[01])\\.)") | not)
              | select(. as $x | $k | index($x) | not) | select(. as $x | $c.cidrs | index($x))] | length
          | if . == 0 then "CLOSED" else "OPEN (\(.) ClickHouse CIDRs allowed)" end end')"
      info "public IP endpoint: $open, allows $(jq -c .cidrs <<<"$cur")"
    else
      info "public IP endpoint: authorized networks $cur (no saved state to compare)"
    fi
  else
    info "public IP endpoint: CLOSED (off)"
  fi
  if [[ "$SKIP_KUBERNETES" != true ]] && kubeconfig_for "$cluster" "$location" 1 "$use_dns"; then
    k8s_status "$cluster"
  else
    info "in-cluster: not checked (no kubectl access from here)"
  fi
}

for_each_cluster() {
  local fn="$1" cluster location
  [[ -n "$CLUSTER_LIST" ]] || warn "no clickhouse-cloud-* GKE cluster in $PROJECT"
  while read -r cluster location <&3; do
    [[ -n "$cluster" ]] || continue
    "$fn" "$cluster" "$location"
  done 3<<<"$CLUSTER_LIST"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
  parse_args "$@"
  need gcloud
  need jq
  if [[ "$SKIP_KUBERNETES" != true ]]; then
    need kubectl
    need gke-gcloud-auth-plugin
  fi
  WORK_DIR="$(mktemp -d)"
  ACCOUNT="$(gcloud config get-value account 2>/dev/null || true)"
  local imp
  imp="$(gcloud config get-value auth/impersonate_service_account 2>/dev/null || true)"
  SELF_MEMBERS="$(jq -cn --arg a "$ACCOUNT" --arg i "$imp" \
    '[$a, $i | ascii_downcase | select(. != "") | "user:" + ., "serviceAccount:" + .] | unique')"
  discover

  log "ClickHouse BYOC kill switch for GCP v$VERSION: $COMMAND"
  info "project:   $PROJECT (running as ${ACCOUNT:-unknown account}${imp:+, impersonating $imp})"
  [[ -n "$ACCOUNT$imp" ]] || warn "could not read the active gcloud account; disconnect cannot tell which bindings are your own"
  info "state dir: $STATE_DIR"
  info "service accounts ClickHouse impersonates: ${CH_SAS[*]:-none found}"
  info "clusters:  $(cut -d' ' -f1 <<<"$CLUSTER_LIST" | tr '\n' ' ')"

  case "$COMMAND" in
    status)
      sa_status
      iam_status
      for_each_cluster cluster_status
      ;;
    plan|disconnect)
      for_each_cluster preflight_cluster
      if [[ "$COMMAND" == "disconnect" ]]; then
        log ""
        log "This blocks ClickHouse from your project and GKE clusters. Your services keep"
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
      sa_disconnect
      iam_disconnect
      for_each_cluster cluster_disconnect
      finish
      step "Done"
      if [[ "$COMMAND" == "plan" ]]; then
        info "Nothing was changed. Run 'disconnect' to apply."
      else
        info "Disconnected. Keep $STATE_DIR: reconnect needs it."
        info "Check with: $0 status --project $PROJECT"
      fi
      ;;
    reconnect)
      [[ -d "$STATE_DIR" ]] || die "no saved state in $STATE_DIR (pass --state-dir)"
      if [[ -f "$STATE_DIR/reconnected-at" ]]; then
        if [[ "$RESTORE_USER_BINDINGS" != true ]]; then
          info "Already reconnected at $(cat "$STATE_DIR/reconnected-at"); nothing to do."
          exit 0
        fi
        info "Already reconnected at $(cat "$STATE_DIR/reconnected-at"); restoring only the saved user: bindings."
        iam_reconnect
        finish
        step "Done"
        exit 0
      fi
      if [[ -f "$STATE_DIR/disconnected-at" ]] && (($(date -u +%s) - $(cat "$STATE_DIR/disconnected-at") < 3600)); then
        warn "You disconnected less than 1 hour ago. Re-enabling a service account makes its"
        warn "unexpired access tokens valid again. If you suspect those tokens were stolen, wait"
        warn "until 1 hour has passed (longer if your organization extends token lifetimes)."
      fi
      confirm
      # Reverse order: cluster, then project IAM, then the service accounts.
      for_each_cluster cluster_reconnect
      iam_reconnect
      sa_reconnect
      finish
      date -u +%Y%m%dT%H%M%SZ >"$STATE_DIR/reconnected-at"
      step "Done"
      info "Reconnected. Tell ClickHouse support; ClickHouse catches up within about 30 minutes."
      ;;
  esac
}

main "$@"
