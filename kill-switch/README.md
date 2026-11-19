# BYOC kill switch

Disconnect ClickHouse from your cloud account, for example during a security incident, and reconnect when you are ready. Your services keep serving queries while you are disconnected. ClickHouse can no longer change or inspect anything in your account, so console operations, backups, upgrades and support stop until you reconnect.

Disconnecting is a temporary state. Plan to reconnect within 7 days.

| Cloud | Script | Requirements |
|---|---|---|
| AWS | [`byoc-kill-switch-aws.sh`](byoc-kill-switch-aws.sh) | aws CLI v2, jq, kubectl, curl; account administrator |
| GCP | [`byoc-kill-switch-gcp.sh`](byoc-kill-switch-gcp.sh) | gcloud with gke-gcloud-auth-plugin, jq, kubectl; project owner. Works in Cloud Shell |
| Azure | [`byoc-kill-switch-azure.sh`](byoc-kill-switch-azure.sh) | az, jq, kubectl, curl; subscription Owner. Two tenant-wide steps also need Entra ID roles: Cloud Application Administrator to disable the ClickHouse app's sign-in, and User Administrator to disable legacy ClickHouse guest users. `--subscription-only` skips both |

All three need bash 4.4 or newer. On macOS, install it with `brew install bash`.

## Procedure

1. **Tell ClickHouse support** which infrastructure you are disconnecting and why. In an emergency, disconnect first and tell support afterwards.
2. **Optional: set IP access lists** on your services now if you also want to block SQL from the ClickHouse console. They cannot be changed while you are disconnected.
3. **Review the plan.** It changes nothing:

   ```bash
   ./byoc-kill-switch-aws.sh plan --region us-east-1
   ./byoc-kill-switch-gcp.sh plan --project my-project
   ./byoc-kill-switch-azure.sh plan --subscription 00000000-0000-0000-0000-000000000000
   ```

4. **Disconnect.** The script asks you to type `disconnect`, and usually takes a few minutes.

   ```bash
   ./byoc-kill-switch-aws.sh disconnect --region us-east-1
   ```

   On AWS and Azure the script keeps the cluster API reachable from this machine's public IP, so that it can finish the steps inside the cluster. Pass `--admin-cidr` to use another range. For an Azure private cluster, run the script from a machine inside the cluster's virtual network.

5. **Check** that every path shows CLOSED:

   ```bash
   ./byoc-kill-switch-aws.sh status --region us-east-1
   ```

6. **Keep the state directory** (`./clickhouse-byoc-kill-switch-<cloud>-<id>`) until you reconnect. It holds your original settings, and `reconnect` needs it.
7. **Do not run the ClickHouse onboarding Terraform or CloudFormation** while disconnected. It grants ClickHouse access again.
8. **Reconnect** from the same directory, then tell ClickHouse support. ClickHouse usually catches up within 10 minutes, and backups resume on schedule. Retry any console operation that failed while you were disconnected.

   ```bash
   ./byoc-kill-switch-aws.sh reconnect --region us-east-1
   ```

If a step fails, nothing is rolled back. Fix the cause the output names, usually a missing permission or no kubectl access from this machine, and run the same command again.

## Commands

| Command | What it does |
|---|---|
| `plan` (default) | Read-only. Lists every ClickHouse access path the script finds and what `disconnect` would change. |
| `disconnect` | Closes every path, after saving the original settings to the state directory. |
| `status` | Read-only. Shows each path as OPEN or CLOSED. |
| `reconnect` | Restores what `disconnect` changed, from the state directory. |

- **Re-running is safe.** Finished steps are skipped. After a successful reconnect, the next `disconnect` archives the old state directory and starts a new one.
- **Exit code 2** means at least one path is still open, or a step could not be restored. The `INCOMPLETE` list at the end of the output says which and why.
- **Scope.** The identity steps cover the whole AWS account, GCP project or Azure subscription, so every BYOC infrastructure in it is disconnected. `--cluster` limits only the steps on the Kubernetes cluster.
- **Options.** Run a script with `--help` for all of them.

## What keeps working and what stops

| Keeps working | Stops until you reconnect |
|---|---|
| Queries and inserts on existing services | Every console or API change to a service: create, delete, scale, start or stop, settings, passwords, IP access lists, version upgrades |
| Storage access: services use identities inside your cluster, not ClickHouse's | Scheduled backups. Existing backups are kept |
| Idle and wake-up | Restores |
| Node autoscaling | Infrastructure changes |
| TLS certificate renewal and service DNS, unless you also block outbound traffic | ClickHouse support, including engineer access to the cluster |
| Outbound logs, metrics and usage metering | ClickHouse monitoring and alerting |

If you also block outbound traffic, TLS certificates can expire after about 11 days, and new nodes cannot pull images.

## What the scripts change

Identities are cut first, so that ClickHouse automation cannot undo the later steps. The steps inside the cluster come last, because they use your own kubectl access.

### AWS

| ClickHouse access path | What `disconnect` does |
|---|---|
| `ClickHouseManagementRole` and every other role in the account trusted by a ClickHouse account, such as `<name>-<region>-data-plane-mgmt` and `<name>-<region>-argocd` | Adds an inline policy, `ClickHouseKillSwitch`, that denies every action. An explicit deny also applies to sessions that already exist. Adds a statement to the trust policy that denies `sts:AssumeRole`, so no new sessions start. |
| EKS public endpoint, which allows ClickHouse's IP addresses | Allows only `--admin-cidr` (default: this machine's IP), or turns the public endpoint off with `--disable-public-endpoint`. This stops new connections to the API server, but not connections that are already open; the next two rows close those. |
| EKS access entries of ClickHouse roles | Deleted, and replaced at once by an empty entry (below). Reconnect recreates the originals. |
| ClickHouse roles mapped in the aws-auth ConfigMap | Each gets an empty EKS access entry: no access policies and no groups, tagged `ClickHouseKillSwitch=aws-auth-shadow`. EKS uses the access entry instead of the aws-auth mapping, so the role loses its Kubernetes permissions, also on connections that are already open. aws-auth itself is not edited. Reconnect deletes only these tagged entries. |
| VPC Lattice, on clusters without a public endpoint | Revokes the security-group rule that admits ClickHouse's resource gateway. ClickHouse recreates it after you reconnect. |

To run kubectl, the script gives the identity you run it as an EKS access entry with `AmazonEKSClusterAdminPolicy`, and removes it again on reconnect. A cluster that still uses `CONFIG_MAP` authentication needs `--allow-auth-mode-change`, which switches it to `API_AND_CONFIG_MAP`. AWS does not allow switching back. Without it, the ClickHouse mappings in aws-auth stay usable on open connections, and the script reports them (exit code 2).

### GCP

| ClickHouse access path | What `disconnect` does |
|---|---|
| `clickhouse-management@<project>` and `<name>-dp-mgmt@<project>` | Disabled. Google then also rejects the access tokens already issued for them. |
| Project IAM bindings of ClickHouse service accounts, such as `<region>-gke-mgmt@dataplane-production` and `<region>-byoc-argocd@dataplane-production` | Removed. Reconnect adds them back. |
| Project IAM bindings of `@clickhouse.com` users | Removed. Reconnect adds them back only with `--restore-user-bindings`. The account you run the script as never loses its own bindings. |
| GKE public endpoint, which allows ClickHouse's IP addresses | Authorized networks keep only `--admin-cidr` (none by default) and any private (RFC 1918) ranges already in the list. Access from Google Cloud public IPs is turned off. The script reaches the cluster through its DNS endpoint, so it works from anywhere. |

### Azure

| ClickHouse access path | What `disconnect` does |
|---|---|
| The "ClickHouse BYOC" app (service principal) | Its role assignments in the subscription are removed. Azure checks role assignments on every request, so this also stops tokens ClickHouse already holds, within about 10 minutes. Assignments inherited from a management group are reported, because a subscription Owner cannot remove them. Then the app's sign-in is disabled. That last step affects your whole tenant; `--subscription-only` skips it. |
| The `<name>-data-plane-mgmt` and `<name>-tde-delegate` managed identities | Every federated credential issued by ClickHouse's Entra ID tenant is deleted. Credentials from other issuers are reported and left alone. The role assignments of `-data-plane-mgmt` are removed too. |
| AKS API server, public clusters | Authorized IP ranges are set to `--admin-cidr` plus the cluster's own outbound IPs. This also blocks the AKS client certificate ClickHouse holds, which disabling an identity does not revoke. |
| AKS API server, private clusters | Every private endpoint connection from a ClickHouse subscription is rejected, and auto-approval and visibility are cleared. Your own private endpoints are left alone. A rejected connection cannot be approved again, so after you reconnect, ClickHouse support restores its private connection. |
| Legacy "ClickHouse Engineer" guest users | Disabled. This affects your whole tenant; `--subscription-only` skips it. |

Reconnect restores role assignments under their original IDs and recreates the deleted federated credentials.

### Inside the cluster (all clouds)

- **ClickHouse credentials.** The ArgoCD ServiceAccount token (`kube-system/<name>-argocd-secret`) and the `kube-system/clickhouse-eng-*` ServiceAccounts are deleted where they exist. ClusterRoleBindings that grant only ClickHouse identities are deleted. A binding that also grants someone else is reported and left alone.
- **Tailscale.** Every Deployment and StatefulSet in namespace `tailscale` is scaled to 0. Tailscale is ClickHouse's network path into the cluster. The script checks again afterwards and reports anything that scaled back up; run `disconnect` again in that case.

Reconnect restores the ClusterRoleBindings and Tailscale replica counts. Where the ArgoCD token and engineer ServiceAccounts existed, ClickHouse recreates them.

### Left in place on purpose

- **The identities your services run as**, for example `CH-S3-*` and the node, autoscaler, load-balancer and storage roles on AWS. Blocking them would stop your services.
- **The TDE key and its delegate's access to the key.** Removing them makes ClickHouse treat the key as revoked and stop services.
- **The ClickHouse operator, autoscaler and configuration webhook.** ClickHouse pods cannot be recreated without them.
- **AWS: the aws-auth ConfigMap.** A mistake there can stop nodes from joining the cluster. The ClickHouse mappings in it are overridden with empty access entries instead.

## Limits

- **Connections opened before you disconnect.** Closing the API endpoint stops new connections, not open ones.
  - On AWS and GCP, the identity steps also refuse ClickHouse's requests on open connections. A watch that was already running keeps receiving updates, read-only, until the Kubernetes API server ends it, 30 to 60 minutes after it started.
  - On Azure, a connection opened with ClickHouse's AKS client certificate can keep working until it closes. `az aks rotate-certs` ends it (see below).
- **Resources ClickHouse owns**, such as service DNS zones and container images, stay under ClickHouse's control.
- **Outbound traffic** keeps flowing. Blocking it is your decision; see the trade-offs above.
- **The query path.** Clients, including the ClickHouse console SQL editor, connect to your service endpoints as before. Set IP access lists before you disconnect to block them.
- **Credentials ClickHouse may already have copied.** If you suspect theft rather than wanting a pause:
  - Review your cloud audit logs for roles, keys, federated credentials, RBAC bindings or webhooks created recently. The kill switch does not remove them.
  - On Azure, `az aks rotate-certs` revokes stored AKS client certificates. It recreates every node and causes downtime.
  - Wait before reconnecting. Reconnecting brings back the permissions of credentials issued before the cut that have not expired yet: 1 hour on AWS and GCP, 24 hours for Azure managed identities. The scripts warn when you reconnect sooner.
