#!/usr/bin/env bash
#
# ClickHouse BYOC kill switch for AWS.
#
# Temporarily disconnects ClickHouse from a standard BYOC deployment in this AWS
# account. Your ClickHouse services keep serving queries; ClickHouse can no longer
# act in your account or your EKS cluster until you run `reconnect`.
#
#   plan        (default) read-only: list ClickHouse access paths and what would change
#   disconnect  cut every path (asks for confirmation unless --yes)
#   reconnect   restore everything this script changed (asks unless --yes)
#   status      read-only: show OPEN / CLOSED for every path
#
# Requires: bash 4.4+, aws CLI v2, jq, kubectl, curl. Run as an account administrator:
# the script edits IAM roles, updates the EKS cluster and creates EKS access entries.
#
# Original policies and settings are saved under --state-dir. Keep that directory
# until you have reconnected: `reconnect` restores from it. Every command is safe to
# re-run; steps that are already done are skipped. Exit code 2 means some path could
# not be closed (or restored); the output says which, and re-running finishes the job.

if ((BASH_VERSINFO[0] * 100 + BASH_VERSINFO[1] < 404)); then
  echo "ERROR: this script needs bash 4.4 or newer (on macOS: brew install bash)" >&2
  exit 1
fi
set -Eeuo pipefail
shopt -s inherit_errexit
export AWS_PAGER=""

readonly VERSION="1.2.0"
readonly KS_ID="ClickHouseKillSwitch"
readonly DEFAULT_MGMT_ROLE="ClickHouseManagementRole"
# ClickHouse accounts that the BYOC management roles in your account trust.
readonly DEFAULT_CH_ACCOUNTS="426924874929 381492293576"
readonly EKS_ADMIN_POLICY="arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"

COMMAND="plan"
REGIONS=()
CLUSTERS=()
MGMT_ROLE="$DEFAULT_MGMT_ROLE"
CH_ACCOUNTS="$DEFAULT_CH_ACCOUNTS"
ADMIN_CIDRS=()
ADMIN_PRINCIPAL=""
DISABLE_PUBLIC_ENDPOINT=false
ALLOW_AUTH_MODE_CHANGE=false
SKIP_KUBERNETES=false
KEEP_ADMIN_ACCESS=false
ASSUME_YES=false
STATE_DIR=""
WORK_DIR=""
KUBECONFIG_FILE=""
INCOMPLETE=()

usage() {
  cat <<'EOF'
Usage: byoc-kill-switch-aws.sh <plan|disconnect|reconnect|status> --region REGION [options]

Options:
  --region REGION            AWS region of the BYOC infrastructure (repeat for several regions)
  --cluster NAME             EKS cluster (default: every clickhouse-cloud-* cluster in the region)
  --management-role NAME     Cross-account management role (default: ClickHouseManagementRole)
  --admin-cidr CIDR          CIDR kept on the EKS public endpoint so you can run kubectl
                             (repeatable; default: this machine's public IP /32)
  --disable-public-endpoint  Turn the EKS public endpoint off instead (run from inside the VPC)
  --admin-principal ARN      IAM role or user that gets EKS cluster-admin for kubectl
                             (default: the role you are running as)
  --allow-auth-mode-change   On a CONFIG_MAP-only cluster, switch to API_AND_CONFIG_MAP so you
                             can grant yourself kubectl access (an AWS change that cannot be undone)
  --skip-kubernetes          Do not touch objects inside the cluster (leaves Tailscale running)
  --keep-admin-access        On reconnect, keep the EKS access entry created for you
  --clickhouse-accounts IDS  Space-separated ClickHouse account IDs (default: production)
  --state-dir DIR            Where original settings are saved
                             (default: ./clickhouse-byoc-kill-switch-aws-<account>)
  --yes                      Do not ask for confirmation
  -h, --help                 Show this help
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

# A path that stays open (disconnect) or a step that could not be restored (reconnect).
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

# act "description" cmd args... : print only in plan mode, otherwise run and log.
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
  [[ "$ASSUME_YES" == true ]] && return 0
  local answer
  printf '\nType "%s" to continue: ' "$COMMAND" >&2
  read -r answer
  [[ "$answer" == "$COMMAND" ]] || die "aborted"
}

safe_name() { printf '%s' "$1" | tr -c 'A-Za-z0-9._-' '_'; }

# save_once FILE CONTENT: write once; never overwrite what disconnect saved first.
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
      --region|--cluster|--management-role|--admin-cidr|--admin-principal|--clickhouse-accounts|--state-dir)
        [[ $# -ge 2 && -n "$2" && "$2" != -* ]] || die "$1 needs a value" ;;
    esac
    case "$1" in
      --region) REGIONS+=("$2"); shift 2 ;;
      --cluster) CLUSTERS+=("$2"); shift 2 ;;
      --management-role) MGMT_ROLE="$2"; shift 2 ;;
      --admin-cidr) ADMIN_CIDRS+=("$2"); shift 2 ;;
      --admin-principal) ADMIN_PRINCIPAL="$2"; shift 2 ;;
      --disable-public-endpoint) DISABLE_PUBLIC_ENDPOINT=true; shift ;;
      --allow-auth-mode-change) ALLOW_AUTH_MODE_CHANGE=true; shift ;;
      --skip-kubernetes) SKIP_KUBERNETES=true; shift ;;
      --keep-admin-access) KEEP_ADMIN_ACCESS=true; shift ;;
      --clickhouse-accounts) CH_ACCOUNTS="$2"; shift 2 ;;
      --state-dir) STATE_DIR="${2%/}"; shift 2 ;;
      --yes) ASSUME_YES=true; shift ;;
      -h|--help) usage; exit 0 ;;
      *) usage; die "unknown argument: $1" ;;
    esac
  done
  case "$COMMAND" in plan|disconnect|reconnect|status) ;; *) usage; die "unknown command: $COMMAND" ;; esac
  [[ ${#REGIONS[@]} -gt 0 ]] || die "--region is required"
  if [[ ${#CLUSTERS[@]} -gt 0 && ${#REGIONS[@]} -gt 1 ]]; then
    die "--cluster can only be used with a single --region"
  fi
  local c
  for c in "${CLUSTERS[@]}"; do
    [[ "$c" == clickhouse-cloud-* ]] || die "--cluster $c is not a ClickHouse BYOC cluster (clickhouse-cloud-*)"
  done
}

# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

ACCOUNT_ID=""
CALLER_ARN=""
CH_ROLES=()     # roles ClickHouse assumes: cut
DATA_ROLES=()   # data-path roles that also trust ClickHouse (ClickPipes etc.): left alone
SECOND_HOP=()   # roles assumed from a ClickHouse role (TDE delegate): left alone
declare -A CH_ROLE_ARN=()  # role name -> ARN (with path) of every role in CH_ROLES

discover_identity() {
  local ident
  ident="$(aws sts get-caller-identity --output json)"
  ACCOUNT_ID="$(jq -r .Account <<<"$ident")"
  CALLER_ARN="$(jq -r .Arn <<<"$ident")"
  [[ -n "$STATE_DIR" ]] || STATE_DIR="./clickhouse-byoc-kill-switch-aws-${ACCOUNT_ID}"
}

# Roles in this account that ClickHouse assumes: the management role, plus every role
# whose trust policy allows a ClickHouse account. Roles trusted through an OIDC provider
# (the pod identities that keep your services running) and per-service CH-S3-* data
# roles are never cut. Neither are roles assumed from a ClickHouse role (the TDE key
# delegate): denying them would make ClickHouse treat the TDE key as unavailable.
discover_roles() {
  local ids_re roles classified
  ids_re="$(tr -s ' ' '|' <<<"$CH_ACCOUNTS" | sed 's/^|//; s/|$//')"
  roles="$(aws iam list-roles --output json --query 'Roles[].{n:RoleName,a:Arn,t:AssumeRolePolicyDocument}')"
  classified="$(jq -c --arg mgmt "$MGMT_ROLE" --arg ids "$ids_re" '
    def stmts: (.Statement // []) | if type == "array" then . else [.] end;
    def allow_aws: [stmts[] | select(.Effect == "Allow") | (.Principal.AWS? // empty)
                    | if type == "array" then .[] else . end];
    def federated: [stmts[] | (.Principal.Federated? // empty)] | length > 0;
    def trusts_ch: allow_aws | any(test("^(arn:aws[a-z-]*:iam::)?(" + $ids + ")(:|$)"));
    [.[] | .t |= (if type == "string" then fromjson else . end)] as $r
    | [$r[] | select(.n == $mgmt or ((.t | trusts_ch) and ((.t | federated) | not) and ((.n | startswith("CH-S3-")) | not)))] as $sel
    | ([$sel[].n]) as $names
    | ([$sel[].a]) as $arns
    | {selected: ($names | unique),
       data: [$r[] | select((.t | trusts_ch) and (.n as $n | $names | index($n) | not)) | .n] | unique,
       second: [$r[] | select((.t | allow_aws | any(. as $p | $arns | index($p))) and (.n as $n | $names | index($n) | not)) | .n] | unique,
       arns: [$sel[] | [.n, .a]]}' <<<"$roles")"
  mapfile -t CH_ROLES < <(jq -r '.selected[]' <<<"$classified")
  local n a
  while IFS=$'\t' read -r n a; do
    if [[ -n "$n" ]]; then CH_ROLE_ARN["$n"]="$a"; fi
  done < <(jq -r '.arns[] | @tsv' <<<"$classified")
  mapfile -t DATA_ROLES < <(jq -r '.data[]' <<<"$classified")
  mapfile -t SECOND_HOP < <(jq -r '.second[]' <<<"$classified")
}

clusters_in_region() {
  local region="$1" all c
  all="$(aws eks list-clusters --region "$region" --output json)"
  if [[ ${#CLUSTERS[@]} -gt 0 ]]; then
    for c in "${CLUSTERS[@]}"; do
      jq -e --arg c "$c" '.clusters | index($c)' <<<"$all" >/dev/null || die "cluster $c not found in $region"
      printf '%s\n' "$c"
    done
    return 0
  fi
  jq -r '.clusters[] | select(startswith("clickhouse-cloud-"))' <<<"$all"
}

# Is this IAM principal a ClickHouse identity?
is_ch_principal() {
  local arn="$1" name acct r
  acct="$(cut -d: -f5 <<<"$arn")"
  for r in $CH_ACCOUNTS; do [[ "$acct" == "$r" ]] && return 0; done
  name="${arn##*/}"
  for r in "${CH_ROLES[@]}"; do [[ "$name" == "$r" ]] && return 0; done
  return 1
}

resolve_admin_principal() {
  if [[ -z "$ADMIN_PRINCIPAL" ]]; then
    if [[ "$CALLER_ARN" == *":assumed-role/"* ]]; then
      ADMIN_PRINCIPAL="$(aws iam get-role --role-name "$(cut -d/ -f2 <<<"$CALLER_ARN")" --query Role.Arn --output text)"
    elif [[ "$CALLER_ARN" == *":user/"* ]]; then
      ADMIN_PRINCIPAL="$CALLER_ARN"
    else
      die "cannot use $CALLER_ARN for kubectl access; pass --admin-principal <role ARN>"
    fi
  fi
  if is_ch_principal "$ADMIN_PRINCIPAL"; then
    die "$ADMIN_PRINCIPAL is a ClickHouse role; pass --admin-principal with one of your own"
  fi
}

resolve_admin_cidrs() {
  [[ ${#ADMIN_CIDRS[@]} -gt 0 || "$DISABLE_PUBLIC_ENDPOINT" == true ]] && return 0
  need curl
  local ip
  ip="$(curl -fsS --max-time 10 https://checkip.amazonaws.com | tr -d '[:space:]')" ||
    die "could not detect this machine's public IP; pass --admin-cidr"
  [[ "$ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "unexpected public IP '$ip'; pass --admin-cidr"
  ADMIN_CIDRS=("$ip/32")
}

# ---------------------------------------------------------------------------
# IAM
# ---------------------------------------------------------------------------

deny_all_policy() {
  jq -cn --arg sid "$KS_ID" '{Version: "2012-10-17", Statement: [{Sid: $sid, Effect: "Deny", Action: "*", Resource: "*"}]}'
}

has_deny_policy() {
  local out
  if out="$(aws iam get-role-policy --role-name "$1" --policy-name "$KS_ID" --output json 2>&1)"; then
    return 0
  fi
  [[ "$out" == *NoSuchEntity* ]] && return 1
  die "cannot read the inline policies of $1: $out"
}

trust_of() {
  aws iam get-role --role-name "$1" --output json |
    jq -c '.Role.AssumeRolePolicyDocument | if type == "string" then fromjson else . end'
}

trust_has_deny() {
  jq -e --arg sid "$KS_ID" '(.Statement | if type == "array" then . else [.] end) | any(.Sid == $sid)' >/dev/null
}

iam_disconnect() {
  step "IAM: block every ClickHouse role (${#CH_ROLES[@]})"
  local role trust
  # 1. A deny-all inline policy on every role, back to back. An explicit Deny is
  #    evaluated on every request, so it also stops the sessions ClickHouse already
  #    holds, and the roles can no longer edit their own policies.
  for role in "${CH_ROLES[@]}"; do
    if has_deny_policy "$role"; then
      info "$role: deny-all policy already attached"
    else
      save_once "$STATE_DIR/iam/$(safe_name "$role").deny" "$role"
      act "$role: attach inline deny-all policy $KS_ID" \
        aws iam put-role-policy --role-name "$role" --policy-name "$KS_ID" --policy-document "$(deny_all_policy)"
    fi
  done
  # 2. Deny sts:AssumeRole in the trust policy, so no new sessions start. Sessions that
  #    already exist expire within 1 hour. Until then they can still mint EKS tokens
  #    (sts:GetCallerIdentity cannot be denied). The EKS steps below stop new connections
  #    to the API server and give the roles mapped in aws-auth empty access entries.
  for role in "${CH_ROLES[@]}"; do
    trust="$(trust_of "$role")"
    if trust_has_deny <<<"$trust"; then
      info "$role: trust policy already denies AssumeRole"
      continue
    fi
    save_once "$STATE_DIR/iam/$(safe_name "$role").json" "$(jq -cn --arg r "$role" --argjson t "$trust" '{role: $r, trust: $t}')"
    if ! act "$role: add Deny sts:AssumeRole to the trust policy (original saved)" \
      aws iam update-assume-role-policy --role-name "$role" --policy-document "$(
        jq -c --arg sid "$KS_ID" '.Statement = ((.Statement | if type == "array" then . else [.] end)
          + [{Sid: $sid, Effect: "Deny", Principal: {AWS: "*"},
              Action: ["sts:AssumeRole", "sts:TagSession", "sts:SetSourceIdentity"]}])' <<<"$trust")"; then
      warn "$role: could not add the trust-policy Deny; the deny-all policy still blocks every API call, and the EKS steps below close cluster access"
    fi
  done
  if [[ ${#SECOND_HOP[@]} -gt 0 ]]; then
    info "left in place: ${SECOND_HOP[*]} (assumed from a ClickHouse role; sessions chained before the disconnect expire within 1 hour)"
  fi
  if [[ ${#DATA_ROLES[@]} -gt 0 ]]; then
    info "left in place: ${DATA_ROLES[*]} (data-path roles that also trust ClickHouse, for example ClickPipes)"
  fi
}

iam_reconnect() {
  step "IAM: restore ClickHouse roles"
  local file role roles=()
  shopt -s nullglob
  for file in "$STATE_DIR"/iam/*.json; do
    role="$(jq -r .role "$file")"
    roles+=("$role")
    if trust_of "$role" | trust_has_deny; then
      act "$role: restore the original trust policy" \
        aws iam update-assume-role-policy --role-name "$role" --policy-document "$(jq -c .trust "$file")"
    else
      info "$role: trust policy has no kill-switch statement; leaving it as is"
    fi
  done
  for file in "$STATE_DIR"/iam/*.deny; do roles+=("$(cat "$file")"); done
  shopt -u nullglob
  roles+=("${CH_ROLES[@]}")
  local list
  list="$(printf '%s\n' "${roles[@]}" | sort -u)"
  while read -r role <&3; do
    [[ -n "$role" ]] || continue
    if has_deny_policy "$role"; then
      act "$role: remove inline deny-all policy $KS_ID" \
        aws iam delete-role-policy --role-name "$role" --policy-name "$KS_ID"
    fi
  done 3<<<"$list"
}

iam_status() {
  step "IAM"
  local role p t
  for role in "${CH_ROLES[@]}"; do
    p="OPEN"
    t="OPEN"
    if has_deny_policy "$role"; then p="CLOSED"; fi
    if trust_of "$role" | trust_has_deny; then t="CLOSED"; fi
    info "$(printf '%-60s api-calls: %-6s  new-sessions: %s' "$role" "$p" "$t")"
  done
  if [[ ${#SECOND_HOP[@]} -gt 0 ]]; then info "not changed by design: ${SECOND_HOP[*]}"; fi
  if [[ ${#DATA_ROLES[@]} -gt 0 ]]; then info "not changed by design (data path): ${DATA_ROLES[*]}"; fi
}

# ---------------------------------------------------------------------------
# EKS
# ---------------------------------------------------------------------------

describe_cluster() { aws eks describe-cluster --region "$1" --name "$2" --output json; }

auth_mode() { jq -r '.cluster.accessConfig.authenticationMode // "CONFIG_MAP"' <<<"$1"; }

# Runs through act, where errexit does not apply, so every step checks its own result.
eks_update() {
  local region="$1" cluster="$2" id status i
  shift 2
  id="$(aws eks update-cluster-config --region "$region" --name "$cluster" "$@" --query update.id --output text)" || return 1
  [[ -n "$id" && "$id" != None ]] || die "EKS did not return an update id for $cluster"
  info "waiting for EKS update $id (usually 1-15 minutes)"
  for ((i = 0; i < 160; i++)); do
    status="$(aws eks describe-update --region "$region" --name "$cluster" --update-id "$id" --query update.status --output text)" || return 1
    case "$status" in
      Successful) return 0 ;;
      Failed|Cancelled) die "EKS update $id on $cluster ended $status" ;;
    esac
    sleep 15
  done
  die "timed out waiting for EKS update $id on $cluster"
}

# Can we give ourselves kubectl access? Not on a CONFIG_MAP-only cluster unless allowed
# to switch it to API_AND_CONFIG_MAP.
kubectl_possible() { [[ "$(auth_mode "$1")" != "CONFIG_MAP" || "$ALLOW_AUTH_MODE_CHANGE" == true ]]; }

has_access_entry() {
  aws eks describe-access-entry --region "$1" --cluster-name "$2" --principal-arn "$3" --output json >/dev/null 2>&1
}

has_admin_policy() {
  aws eks list-associated-access-policies --region "$1" --cluster-name "$2" --principal-arn "$3" --output json 2>/dev/null |
    jq -e --arg p "$EKS_ADMIN_POLICY" 'any(.associatedAccessPolicies[]; .policyArn == $p and .accessScope.type == "cluster")' >/dev/null
}

ensure_admin_access() {
  local region="$1" cluster="$2" desc="$3" dir="$4" had_entry=false
  if [[ "$(auth_mode "$desc")" == "CONFIG_MAP" ]]; then
    act "$cluster: switch authentication mode to API_AND_CONFIG_MAP (cannot be undone)" \
      eks_update "$region" "$cluster" --access-config authenticationMode=API_AND_CONFIG_MAP
  fi
  if has_access_entry "$region" "$cluster" "$ADMIN_PRINCIPAL"; then
    had_entry=true
    info "$cluster: $ADMIN_PRINCIPAL already has an access entry"
  else
    act "$cluster: create EKS access entry for $ADMIN_PRINCIPAL" \
      aws eks create-access-entry --region "$region" --cluster-name "$cluster" --principal-arn "$ADMIN_PRINCIPAL" --output json >/dev/null
    save_once "$dir/admin-access-entry" "$ADMIN_PRINCIPAL"
  fi
  if [[ "$had_entry" == true ]] && has_admin_policy "$region" "$cluster" "$ADMIN_PRINCIPAL"; then
    info "$cluster: $ADMIN_PRINCIPAL already has AmazonEKSClusterAdminPolicy"
    return 0
  fi
  # Your own entry existed without cluster-admin: remember to take it away again.
  if [[ "$had_entry" == true ]]; then save_once "$dir/admin-access-policy" "$ADMIN_PRINCIPAL"; fi
  act "$cluster: associate AmazonEKSClusterAdminPolicy (cluster scope) with $ADMIN_PRINCIPAL" \
    aws eks associate-access-policy --region "$region" --cluster-name "$cluster" --principal-arn "$ADMIN_PRINCIPAL" \
    --policy-arn "$EKS_ADMIN_POLICY" --access-scope type=cluster --output json >/dev/null
}

vpc_config() {
  jq -c '.cluster.resourcesVpcConfig | {public: .endpointPublicAccess, private: .endpointPrivateAccess, cidrs: (.publicAccessCidrs // [] | sort)}' <<<"$1"
}

endpoint_disconnect() {
  local region="$1" cluster="$2" desc="$3" cur want target
  cur="$(vpc_config "$desc")"
  if [[ "$(jq -r .public <<<"$cur")" != true ]]; then
    info "$cluster: public endpoint is already off"
    return 0
  fi
  # Private access stays (or becomes) on, so nodes and pods keep reaching the API server.
  if [[ "$DISABLE_PUBLIC_ENDPOINT" == true ]]; then
    act "$cluster: turn the EKS public endpoint off (private access on)" \
      eks_update "$region" "$cluster" --resources-vpc-config '{"endpointPublicAccess":false,"endpointPrivateAccess":true}'
    return 0
  fi
  want="$(printf '%s\n' "${ADMIN_CIDRS[@]}" | jq -R . | jq -cs 'sort')"
  target="$(jq -cn --argjson c "$want" '{public: true, private: true, cidrs: $c}')"
  if [[ "$cur" == "$target" ]]; then
    info "$cluster: public endpoint already limited to $want"
    return 0
  fi
  act "$cluster: limit the EKS public endpoint to $want, private access on (was $(jq -c .cidrs <<<"$cur"))" \
    eks_update "$region" "$cluster" --resources-vpc-config "$(jq -cn --argjson c "$want" '{endpointPublicAccess: true, endpointPrivateAccess: true, publicAccessCidrs: $c}')"
}

endpoint_reconnect() {
  local region="$1" cluster="$2" dir="$3" orig cur
  [[ -f "$dir/cluster.json" ]] || return 0
  orig="$(vpc_config "$(cat "$dir/cluster.json")")"
  cur="$(vpc_config "$(describe_cluster "$region" "$cluster")")"
  if [[ "$cur" == "$orig" ]]; then
    info "$cluster: endpoint access already matches the saved settings"
    return 0
  fi
  local cfg
  if [[ "$(jq -r .public <<<"$orig")" == true ]]; then
    cfg="$(jq -c '{endpointPublicAccess: true, endpointPrivateAccess: .private, publicAccessCidrs: .cidrs}' <<<"$orig")"
  else
    cfg="$(jq -c '{endpointPublicAccess: false, endpointPrivateAccess: .private}' <<<"$orig")"
  fi
  act "$cluster: restore EKS endpoint access $cfg" eks_update "$region" "$cluster" --resources-vpc-config "$cfg"
}

ch_access_entries() {
  local region="$1" cluster="$2" list arn
  list="$(aws eks list-access-entries --region "$region" --cluster-name "$cluster" --output json | jq -r '.accessEntries[]')" || return 1
  while read -r arn <&3; do
    [[ -n "$arn" ]] || continue
    if is_ch_principal "$arn" && ! is_shadow_entry "$region" "$cluster" "$arn"; then printf '%s\n' "$arn"; fi
  done 3<<<"$list"
}

access_entries_disconnect() {
  local region="$1" cluster="$2" desc="$3" dir="$4" list arn entry policies
  [[ "$(auth_mode "$desc")" != "CONFIG_MAP" ]] || return 0
  list="$(ch_access_entries "$region" "$cluster")"
  while read -r arn <&3; do
    [[ -n "$arn" ]] || continue
    if applying; then
      entry="$(aws eks describe-access-entry --region "$region" --cluster-name "$cluster" --principal-arn "$arn" --output json)"
      policies="$(aws eks list-associated-access-policies --region "$region" --cluster-name "$cluster" --principal-arn "$arn" --output json)"
      save_once "$dir/access-entries/$(safe_name "$arn").json" \
        "$(jq -cn --argjson e "$entry" --argjson p "$policies" '{entry: $e.accessEntry, policies: $p.associatedAccessPolicies}')"
    fi
    act "$cluster: delete EKS access entry of ClickHouse principal $arn (saved)" \
      aws eks delete-access-entry --region "$region" --cluster-name "$cluster" --principal-arn "$arn"
    # Without its entry the role's aws-auth mapping (if any) applies: replace it at once.
    if [[ "${CH_ROLE_ARN[${arn##*/}]:-}" == "$arn" ]]; then
      shadow_entry "$region" "$cluster" "$dir" "$arn"
    fi
  done 3<<<"$list"
}

access_entries_reconnect() {
  local region="$1" cluster="$2" dir="$3" f arn type username p groups policies
  # First the empty entries, so the saved entries of the same roles can be recreated.
  shadow_reconnect "$region" "$cluster" "$dir"
  shopt -s nullglob
  for f in "$dir"/access-entries/*.json; do
    arn="$(jq -r .entry.principalArn "$f")"
    if has_access_entry "$region" "$cluster" "$arn"; then
      info "$cluster: access entry for $arn already exists"
    else
      type="$(jq -r '.entry.type // "STANDARD"' "$f")"
      username="$(jq -r '.entry.username // empty' "$f")"
      mapfile -t groups < <(jq -r '.entry.kubernetesGroups // [] | .[]' "$f")
      local args=(--region "$region" --cluster-name "$cluster" --principal-arn "$arn" --type "$type" --output json)
      if [[ -n "$username" ]]; then args+=(--username "$username"); fi
      if [[ ${#groups[@]} -gt 0 ]]; then args+=(--kubernetes-groups "${groups[@]}"); fi
      if [[ "$(jq '.entry.tags // {} | length' "$f")" -gt 0 ]]; then args+=(--tags "$(jq -c .entry.tags "$f")"); fi
      act "$cluster: recreate EKS access entry for $arn" aws eks create-access-entry "${args[@]}" >/dev/null
    fi
    local current="[]"
    if has_access_entry "$region" "$cluster" "$arn"; then
      current="$(aws eks list-associated-access-policies --region "$region" --cluster-name "$cluster" --principal-arn "$arn" --output json |
        jq -c '[.associatedAccessPolicies[] | {policyArn, accessScope}]')"
    fi
    policies="$(jq -c --argjson cur "$current" '.policies // [] | .[] | {policyArn, accessScope}
      | select(. as $p | $cur | any(.policyArn == $p.policyArn and .accessScope == $p.accessScope) | not)' "$f")"
    while read -r p <&3; do
      [[ -n "$p" ]] || continue
      act "$cluster: associate $(jq -r .policyArn <<<"$p") with $arn" \
        aws eks associate-access-policy --region "$region" --cluster-name "$cluster" --principal-arn "$arn" \
        --policy-arn "$(jq -r .policyArn <<<"$p")" --access-scope "$(jq -c .accessScope <<<"$p")" --output json >/dev/null
    done 3<<<"$policies"
  done
  shopt -u nullglob
  if [[ "$KEEP_ADMIN_ACCESS" == true ]]; then return 0; fi
  if [[ -f "$dir/admin-access-entry" ]]; then
    arn="$(cat "$dir/admin-access-entry")"
    if has_access_entry "$region" "$cluster" "$arn"; then
      act "$cluster: remove the access entry created for $arn during disconnect" \
        aws eks delete-access-entry --region "$region" --cluster-name "$cluster" --principal-arn "$arn"
    fi
    if applying; then rm -f "$dir/admin-access-entry"; fi
  fi
  if [[ -f "$dir/admin-access-policy" ]]; then
    arn="$(cat "$dir/admin-access-policy")"
    if has_admin_policy "$region" "$cluster" "$arn"; then
      act "$cluster: remove the AmazonEKSClusterAdminPolicy added to $arn during disconnect" \
        aws eks disassociate-access-policy --region "$region" --cluster-name "$cluster" --principal-arn "$arn" \
        --policy-arn "$EKS_ADMIN_POLICY" --output json >/dev/null
    fi
    if applying; then rm -f "$dir/admin-access-policy"; fi
  fi
}

# ClickHouse roles mapped in the aws-auth ConfigMap (data-plane-mgmt and the ArgoCD role,
# as system:masters) keep their rights on API connections that are already open, for as
# long as their sessions can sign EKS tokens: the endpoint step only stops new
# connections. EKS uses a principal's access entry instead of its aws-auth mapping, so an
# empty entry (no access policies, no groups) takes those rights away. Each one is tagged
# and recorded under shadow-entries/; reconnect deletes only those. aws-auth itself is
# never edited.
readonly SHADOW_TAG="aws-auth-shadow"

is_shadow_entry() {
  aws eks describe-access-entry --region "$1" --cluster-name "$2" --principal-arn "$3" --output json 2>/dev/null |
    jq -e --arg k "$KS_ID" --arg v "$SHADOW_TAG" '.accessEntry.tags[$k] == $v' >/dev/null
}

# Prints "local<TAB>ARN" for every ClickHouse role of this account mapped in aws-auth (ARN
# with its path: aws-auth lists roles without it), and "remote<TAB>ARN" for roles of
# ClickHouse accounts. Fails if the ConfigMap cannot be read; prints nothing if it does
# not exist.
aws_auth_ch_roles() {
  local cm arns a acct r
  cm="$(k -n kube-system get configmap aws-auth --ignore-not-found -o json)" || return 1
  [[ -n "$cm" ]] || return 0
  arns="$(jq -r '.data.mapRoles // ""' <<<"$cm" |
    grep -oE "rolearn[\"']?[[:space:]]*:[[:space:]]*[\"']?arn:[^\"'[:space:],}]+" |
    grep -oE "arn:[^\"'[:space:],}]+" | sort -u || true)"
  while read -r a <&3; do
    [[ -n "$a" ]] || continue
    acct="$(cut -d: -f5 <<<"$a")"
    if [[ "$acct" == "$ACCOUNT_ID" && -n "${CH_ROLE_ARN[${a##*/}]:-}" ]]; then
      printf 'local\t%s\n' "${CH_ROLE_ARN[${a##*/}]}"
      continue
    fi
    for r in $CH_ACCOUNTS; do
      if [[ "$acct" == "$r" ]]; then printf 'remote\t%s\n' "$a"; fi
    done
  done 3<<<"$arns"
}

shadow_disconnect() {
  local region="$1" cluster="$2" dir="$3" list kind arn remote
  if ! list="$(aws_auth_ch_roles)"; then
    if applying; then
      incomplete "$cluster: could not read the aws-auth ConfigMap, so the ClickHouse roles mapped there keep their access on API connections that are already open"
    else
      warn "$cluster: could not read the aws-auth ConfigMap"
    fi
    return 0
  fi
  remote="$(awk -F'\t' '$1 == "remote" {printf "%s ", $2}' <<<"$list")"
  if [[ -n "$remote" ]]; then
    info "$cluster: aws-auth also maps roles of ClickHouse accounts (${remote% }); the endpoint step closes those"
  fi
  while IFS=$'\t' read -r kind arn <&3; do
    [[ "$kind" == local ]] || continue
    if has_access_entry "$region" "$cluster" "$arn"; then
      if is_shadow_entry "$region" "$cluster" "$arn"; then
        info "$cluster: $arn already has an empty access entry"
      elif applying; then
        info "$cluster: $arn has an access entry, so its aws-auth mapping does not apply; leaving it"
      fi
      # (plan: a ClickHouse entry that the access-entry step above replaces)
      continue
    fi
    shadow_entry "$region" "$cluster" "$dir" "$arn"
  done 3<<<"$list"
}

shadow_entry() {
  local region="$1" cluster="$2" dir="$3" arn="$4"
  save_once "$dir/shadow-entries/$(safe_name "$arn")" "$arn"
  act "$cluster: give $arn an empty access entry, so its aws-auth mapping no longer applies" \
    aws eks create-access-entry --region "$region" --cluster-name "$cluster" --principal-arn "$arn" \
    --type STANDARD --tags "$KS_ID=$SHADOW_TAG" --output json >/dev/null
}

shadow_reconnect() {
  local region="$1" cluster="$2" dir="$3" f arn
  shopt -s nullglob
  for f in "$dir"/shadow-entries/*; do
    arn="$(cat "$f")"
    if is_shadow_entry "$region" "$cluster" "$arn"; then
      act "$cluster: remove the empty access entry of $arn" \
        aws eks delete-access-entry --region "$region" --cluster-name "$cluster" --principal-arn "$arn"
    elif has_access_entry "$region" "$cluster" "$arn"; then
      info "$cluster: the access entry of $arn was not created by this script; leaving it"
    fi
    if applying; then rm -f "$f"; fi
  done
  shopt -u nullglob
}

shadow_status() {
  local region="$1" cluster="$2" desc="$3" kube="$4" list kind arn n=0 open=()
  if [[ "$kube" != true ]]; then
    info "ClickHouse roles in aws-auth: not checked (no kubectl access from here)"
    return 0
  fi
  if ! list="$(aws_auth_ch_roles 2>/dev/null)"; then
    info "ClickHouse roles in aws-auth: UNKNOWN (could not read the aws-auth ConfigMap)"
    return 0
  fi
  while IFS=$'\t' read -r kind arn <&3; do
    [[ "$kind" == local ]] || continue
    n=$((n + 1))
    if [[ "$(auth_mode "$desc")" == "CONFIG_MAP" ]] || ! is_shadow_entry "$region" "$cluster" "$arn"; then
      open+=("${arn##*/}")
    fi
  done 3<<<"$list"
  if [[ $n -eq 0 ]]; then
    info "ClickHouse roles in aws-auth: none"
  elif [[ ${#open[@]} -eq 0 ]]; then
    info "ClickHouse roles in aws-auth: CLOSED ($n with an empty access entry)"
  else
    info "ClickHouse roles in aws-auth: OPEN (${open[*]})"
  fi
}

# VPC Lattice (private management path, only on clusters without a public endpoint):
# the cluster security group admits ClickHouse's resource gateway (*-lattice-rg).
# Revoking that rule closes the path. ClickHouse recreates the rule itself after
# reconnect, so this script does not re-add it (a hand-made copy would conflict).
lattice_rules() {
  local region="$1" desc="$2" gateways vpc rg_sgs sgs
  gateways="$(aws vpc-lattice list-resource-gateways --region "$region" --output json)" || return 1
  vpc="$(jq -r .cluster.resourcesVpcConfig.vpcId <<<"$desc")"
  rg_sgs="$(jq -c --arg vpc "$vpc" '[.items[] | select(.vpcIdentifier == $vpc and (.name | endswith("-lattice-rg"))) | (.securityGroupIds // [])[]] | unique' <<<"$gateways")"
  # No ClickHouse resource gateway in the VPC: there is no Lattice path at all.
  if [[ "$rg_sgs" == "[]" ]]; then
    echo "none"
    return 0
  fi
  sgs="$(jq -r '[.cluster.resourcesVpcConfig.clusterSecurityGroupId] + (.cluster.resourcesVpcConfig.securityGroupIds // []) | map(select(. != null)) | unique | join(",")' <<<"$desc")"
  aws ec2 describe-security-group-rules --region "$region" --filters "Name=group-id,Values=$sgs" --output json |
    jq -c --argjson rg "$rg_sgs" '[.SecurityGroupRules[] | select(.IsEgress == false and ((.ReferencedGroupInfo.GroupId // "") as $g | $rg | index($g)))]'
}

lattice_disconnect() {
  local region="$1" cluster="$2" desc="$3" dir="$4" rules r
  if ! rules="$(lattice_rules "$region" "$desc" 2>/dev/null)"; then
    if [[ "$(jq -r .cluster.resourcesVpcConfig.endpointPublicAccess <<<"$desc")" != true ]]; then
      incomplete "$cluster: could not check the VPC Lattice path (permissions, or an aws CLI without vpc-lattice); it may be open"
    else
      warn "$cluster: could not check the VPC Lattice path (permissions, or an aws CLI without vpc-lattice); skipped"
    fi
    return 0
  fi
  if [[ "$rules" == none || "$rules" == "[]" ]]; then
    info "$cluster: no ClickHouse VPC Lattice path"
    return 0
  fi
  save_once "$dir/lattice-sg-rules.json" "$rules"
  local list
  list="$(jq -c '.[]' <<<"$rules")"
  while read -r r <&3; do
    [[ -n "$r" ]] || continue
    act "$cluster: revoke Lattice ingress rule $(jq -r .SecurityGroupRuleId <<<"$r") on $(jq -r .GroupId <<<"$r")" \
      aws ec2 revoke-security-group-ingress --region "$region" --group-id "$(jq -r .GroupId <<<"$r")" \
      --security-group-rule-ids "$(jq -r .SecurityGroupRuleId <<<"$r")" --output json >/dev/null
  done 3<<<"$list"
}

# kubeconfig_for REGION CLUSTER TRIES: write a kubeconfig and check cluster-admin access.
kubeconfig_for() {
  local region="$1" cluster="$2" tries="$3" i
  KUBECONFIG_FILE="$WORK_DIR/$(safe_name "$cluster").kubeconfig"
  aws eks update-kubeconfig --region "$region" --name "$cluster" --kubeconfig "$KUBECONFIG_FILE" --alias "$cluster" >/dev/null || return 1
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
# In-cluster users that are ClickHouse identities (cloud specific; on AWS ClickHouse
# roles map to aws-auth groups, handled by the IAM and EKS steps).
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
# Per cluster
# ---------------------------------------------------------------------------

cluster_dir() { printf '%s/eks/%s/%s' "$STATE_DIR" "$1" "$2"; }

cluster_disconnect() {
  local region="$1" cluster="$2" dir desc k8s_ok=true tries=1
  dir="$(cluster_dir "$region" "$cluster")"
  step "EKS $cluster ($region)"
  desc="$(describe_cluster "$region" "$cluster")"
  save_once "$dir/cluster.json" "$desc"
  if [[ "$SKIP_KUBERNETES" != true ]]; then
    if kubectl_possible "$desc"; then
      ensure_admin_access "$region" "$cluster" "$desc" "$dir"
      if applying; then desc="$(describe_cluster "$region" "$cluster")"; fi
    else
      incomplete "$cluster: CONFIG_MAP authentication, so you cannot grant yourself kubectl access; re-run with --allow-auth-mode-change. Tailscale stays open, and so do the ClickHouse roles mapped in aws-auth on connections that are already open."
      k8s_ok=false
    fi
  fi
  endpoint_disconnect "$region" "$cluster" "$desc"
  access_entries_disconnect "$region" "$cluster" "$desc" "$dir"
  lattice_disconnect "$region" "$cluster" "$desc" "$dir"
  if [[ "$SKIP_KUBERNETES" == true ]]; then
    incomplete "$cluster: --skip-kubernetes: Tailscale, the in-cluster ClickHouse credentials and the ClickHouse roles mapped in aws-auth stay open"
    return 0
  fi
  [[ "$k8s_ok" == true ]] || return 0
  if applying; then tries=12; fi
  if ! kubeconfig_for "$region" "$cluster" "$tries"; then
    if applying; then
      incomplete "$cluster: no cluster-admin kubectl access from this machine, so Tailscale and the ClickHouse roles mapped in aws-auth stay open. Run disconnect again from a host allowed by --admin-cidr (or inside the VPC)."
    else
      info "[would] give the ClickHouse roles mapped in aws-auth empty access entries; scale Tailscale to 0; delete the ArgoCD token, engineer ServiceAccounts and ClickHouse ClusterRoleBindings (needs the access entry above)"
    fi
    return 0
  fi
  shadow_disconnect "$region" "$cluster" "$dir"
  k8s_disconnect "$dir" "$cluster"
}

cluster_reconnect() {
  local region="$1" cluster="$2" dir
  dir="$(cluster_dir "$region" "$cluster")"
  step "EKS $cluster ($region)"
  if [[ ! -d "$dir" ]]; then
    warn "$cluster: no saved state in $dir; skipping"
    return 0
  fi
  # In-cluster objects first: restoring the endpoint removes your kubectl access.
  if [[ -f "$dir/k8s/restored" ]]; then
    info "$cluster: in-cluster objects already restored"
  elif [[ "$SKIP_KUBERNETES" != true ]] && kubeconfig_for "$region" "$cluster" 3; then
    k8s_reconnect "$dir" "$cluster"
  elif [[ -d "$dir/k8s" ]]; then
    incomplete "$cluster: Tailscale and ClickHouse's ClusterRoleBindings not restored (no kubectl access from here, or --skip-kubernetes). On clusters that ClickHouse reaches only through Tailscale this keeps ClickHouse out. Run reconnect again from the host you disconnected from."
  fi
  access_entries_reconnect "$region" "$cluster" "$dir"
  endpoint_reconnect "$region" "$cluster" "$dir"
  if [[ -f "$dir/lattice-sg-rules.json" ]]; then
    info "$cluster: ClickHouse recreates the Lattice security-group rule after reconnect; do not re-add it by hand"
  fi
}

cluster_status() {
  local region="$1" cluster="$2" dir desc n rules kube=false
  dir="$(cluster_dir "$region" "$cluster")"
  step "EKS $cluster ($region)"
  desc="$(describe_cluster "$region" "$cluster")"
  if [[ "$(jq -r .cluster.resourcesVpcConfig.endpointPublicAccess <<<"$desc")" != true ]]; then
    info "public endpoint: CLOSED (off)"
  elif [[ -f "$dir/cluster.json" ]]; then
    n="$(jq -n --slurpfile o "$dir/cluster.json" --argjson c "$desc" '
      [($o[0].cluster.resourcesVpcConfig.publicAccessCidrs // [])[] | select(. as $x | ($c.cluster.resourcesVpcConfig.publicAccessCidrs // []) | index($x))] | length')"
    info "public endpoint: $(if [[ "$n" == 0 ]]; then echo CLOSED; else echo "OPEN ($n ClickHouse CIDRs allowed)"; fi), allows $(jq -c '.cluster.resourcesVpcConfig.publicAccessCidrs' <<<"$desc")"
  else
    info "public endpoint: on, allows $(jq -c '.cluster.resourcesVpcConfig.publicAccessCidrs' <<<"$desc") (no saved state to compare)"
  fi
  if [[ "$(auth_mode "$desc")" != "CONFIG_MAP" ]]; then
    if n="$(ch_access_entries "$region" "$cluster" 2>/dev/null)"; then
      n="$(grep -c . <<<"$n" || true)"
      info "ClickHouse access entries: $(if [[ "$n" == 0 ]]; then echo CLOSED; else echo "OPEN ($n)"; fi)"
    else
      info "ClickHouse access entries: UNKNOWN (could not list them)"
    fi
  fi
  if ! rules="$(lattice_rules "$region" "$desc" 2>/dev/null)"; then
    info "VPC Lattice: UNKNOWN (could not check)"
  elif [[ "$rules" == none ]]; then
    info "VPC Lattice: none (no ClickHouse resource gateway in this VPC)"
  else
    n="$(jq length <<<"$rules")"
    info "VPC Lattice: $(if [[ "$n" == 0 ]]; then echo CLOSED; else echo "OPEN ($n rules)"; fi)"
  fi
  if [[ "$SKIP_KUBERNETES" != true ]] && kubeconfig_for "$region" "$cluster" 1; then kube=true; fi
  shadow_status "$region" "$cluster" "$desc" "$kube"
  if [[ "$kube" == true ]]; then
    k8s_status "$cluster"
  else
    info "in-cluster: not checked (no kubectl access from here)"
  fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

for_each_cluster() {
  local fn="$1" region cluster list
  for region in "${REGIONS[@]}"; do
    list="$(clusters_in_region "$region")"
    [[ -n "$list" ]] || warn "no clickhouse-cloud-* EKS cluster in $region"
    while read -r cluster <&3; do
      [[ -n "$cluster" ]] || continue
      "$fn" "$region" "$cluster"
    done 3<<<"$list"
  done
}

main() {
  parse_args "$@"
  need aws
  need jq
  [[ "$SKIP_KUBERNETES" == true ]] || need kubectl
  WORK_DIR="$(mktemp -d)"
  discover_identity
  discover_roles

  log "ClickHouse BYOC kill switch for AWS v$VERSION: $COMMAND"
  info "account:   $ACCOUNT_ID (running as $CALLER_ARN)"
  info "regions:   ${REGIONS[*]}"
  info "state dir: $STATE_DIR"
  info "ClickHouse roles in this account: ${CH_ROLES[*]:-none found}"
  [[ ${#CH_ROLES[@]} -gt 0 ]] || warn "no ClickHouse roles found; check --management-role and --clickhouse-accounts"

  case "$COMMAND" in
    status)
      iam_status
      for_each_cluster cluster_status
      ;;
    plan|disconnect)
      [[ "$SKIP_KUBERNETES" == true ]] || resolve_admin_principal
      resolve_admin_cidrs
      [[ "$SKIP_KUBERNETES" == true ]] || info "kubectl as: $ADMIN_PRINCIPAL"
      if [[ "$DISABLE_PUBLIC_ENDPOINT" == true ]]; then
        info "EKS public endpoint: will be turned off"
      else
        info "EKS public endpoint: will allow only ${ADMIN_CIDRS[*]}"
      fi
      if [[ "$COMMAND" == "disconnect" ]]; then
        log ""
        log "This blocks ClickHouse from your account and EKS clusters. Your services keep"
        log "serving; console operations, backups and upgrades stop until you reconnect."
        log "Do not run the ClickHouse onboarding Terraform or CloudFormation while disconnected."
        confirm
        if [[ -f "$STATE_DIR/reconnected-at" ]]; then
          mv "$STATE_DIR" "$STATE_DIR.reconnected-$(cat "$STATE_DIR/reconnected-at")"
          info "archived the state of the previous disconnect"
        fi
        mkdir -p "$STATE_DIR"
        chmod 700 "$STATE_DIR"
        save_once "$STATE_DIR/disconnected-at" "$(date -u +%s)"
      fi
      # Check the cluster list (and any --cluster) before the first change.
      for_each_cluster true
      # IAM first: it stops ClickHouse from reverting the steps that follow.
      iam_disconnect
      for_each_cluster cluster_disconnect
      finish
      step "Done"
      if [[ "$COMMAND" == "plan" ]]; then
        info "Nothing was changed. Run 'disconnect' to apply."
      else
        info "Disconnected. Keep $STATE_DIR: reconnect needs it."
        info "EKS now refuses ClickHouse's requests, also on connections it already had open."
        info "Watches ClickHouse opened before the disconnect keep streaming (read-only) until"
        info "the API server ends them, 30-60 minutes after they started."
        info "Check with: $0 status --region ${REGIONS[*]}"
      fi
      ;;
    reconnect)
      [[ -d "$STATE_DIR" ]] || die "no saved state in $STATE_DIR (pass --state-dir)"
      if [[ -f "$STATE_DIR/reconnected-at" ]]; then
        info "Already reconnected at $(cat "$STATE_DIR/reconnected-at"); nothing to do."
        exit 0
      fi
      if [[ -f "$STATE_DIR/disconnected-at" ]] && (($(date -u +%s) - $(cat "$STATE_DIR/disconnected-at") < 3600)); then
        warn "You disconnected less than 1 hour ago. Removing the deny policy re-enables ClickHouse"
        warn "sessions issued before the disconnect that have not expired yet. If you suspect those"
        warn "credentials were stolen, wait until 1 hour has passed."
      fi
      confirm
      # Cluster first (your kubectl access goes away with the endpoint restore), IAM last.
      for_each_cluster cluster_reconnect
      iam_reconnect
      finish
      date -u +%Y%m%dT%H%M%SZ >"$STATE_DIR/reconnected-at"
      step "Done"
      info "Reconnected. Tell ClickHouse support; ClickHouse catches up within about 30 minutes."
      ;;
  esac
}

main "$@"
