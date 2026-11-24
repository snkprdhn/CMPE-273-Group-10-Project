# AKS professor review

For agent handoffs, see the [AKS topology and deployment runbook](AGENT_RUNBOOK.md)
for current-target checks, scoped rollouts, hosted verification, and rollback.

This seven-day review uses the local developer profile's one Kafka broker,
synthetic overload-to-recovery Job, and deterministic MOCK Investigator.
The review overlay adds managed SSD volumes, Cilium policy, nine-day Kafka and
Investigator evidence retention, and a Caddy HTTPS entry point. Only the UI
and the named Keycloak realm's browser routes go through that entry point.
The professor has the existing Operator role required by the Investigator.
PostgreSQL holds realm and action state. Caddy's certificate state, Flink
checkpoints and Investigator SQLite evidence have dedicated PVCs.

## Azure preflight

Use the existing Azure for Students subscription with the spending limit on.
Confirm a policy-allowed region, an eligible Linux amd64 AKS system worker,
family and regional vCPU quota, AKS and public IP quota, and a regional price
estimate within $75. Include 168 hours of review plus setup and teardown. The
November 23 preflight accepted the exact one-node Denmark East configuration.
Microsoft lists the worker's regional price as unavailable. The owner explicitly
accepted the unverified DS3_v2 comparison estimate on November 24 and authorized
provisioning. See the
[implementation status](../../docs/implementation/IMPLEMENTATION_STATUS.md)
for the executed checks and the pricing gate.

Use the pinned project tools and Docker Desktop. Build candidates locally with
separate Docker tags. Registry publishing checks the build IDs and platform
before pushing, then deploys by digest.

```sh
umask 077
DOCKER_DEFAULT_PLATFORM=linux/amd64 .venv/bin/python -m tools.delivery_images \
  --repository smart-grid-aks-review --inventory .tools/aks-review/images.json
.venv/bin/kubectl kustomize infra/aks
```

## Provisioning and handoff

Azure allocated the one-worker Free-tier cluster in Denmark East on November 24.
ARM provider preflight accepted one worker and rejected two because they need
eight regional vCPUs. The separate API allowlist follows the current workstation
address. The old Canada Central VM, its disks and networking have been deleted
after private backups. Basic ACR remains a dependency of the AKS deployment.

1. Use a dedicated resource group, AKS Free, one approved worker, Azure CNI
   Overlay with Cilium, an API server authorized IP range for the workstation,
   and SSH disabled. Use a Basic registry with the admin account disabled.
   Grant the cluster kubelet pull access with `az aks update --attach-acr`.
2. Create a single static Standard IPv4 ingress address named `review-ingress`
   in the AKS-managed node resource group. Assign a unique Azure DNS label.
   Read `dnsSettings.fqdn` from the IP resource. The overlay attaches to that IP
   and Caddy obtains a public certificate for its `cloudapp.azure.com` hostname.
3. Use the separate kubeconfig. Give its context the review name when exporting
   credentials. Keep the file in the private, ignored review directory.

```sh
umask 077
az aks get-credentials --resource-group "$review_group" --name "$review_cluster" \
  --file .tools/aks-review/kubeconfig --context smart-grid-aks-review
chmod 600 .tools/aks-review/kubeconfig
.venv/bin/python -m tools.aks_review publish --registry "$review_registry"
for stage in install deploy verify; do
  .venv/bin/python -m tools.aks_review "$stage" --registry "$review_registry" --host "$review_host" || break
done
```

The verification stage runs the seeded scenario and checks allowed and denied
traffic, the live REST/SSE projection and professor sign-in, the exact
completed action, and grounded MOCK Investigator output. It then replaces the
Flink TaskManager to verify checkpoint recovery, restarts the review workloads,
and checks the action and both previous and new investigation evidence over
trusted HTTPS. It captures PVC and registry pull evidence. Use a fresh review
cluster for a new demonstration run. Keep the private output in
`.tools/aks-review/`.

For a successful handoff, start the seven-day clock after all live gates pass.
Resolve the end time by adding 168 hours in UTC and converting to Pacific Time.
Open **Alerts & actions**, select the supplied Action ID, then select
**Investigate action** and **Run investigation**. The local credential file is
mode 0600. On macOS, the account owner can copy the professor password to the
clipboard without printing it and clear the clipboard after securely sharing it.

```sh
.venv/bin/python -c 'import json; print(json.load(open(".tools/aks-review/local.json"))["professor"], end="")' | pbcopy
```

## Availability, credit and cleanup

```sh
curl -fsS --max-time 15 "https://$review_host/healthz"
curl -fsS --max-time 15 "https://$review_host/readyz"
.venv/bin/kubectl --kubeconfig .tools/aks-review/kubeconfig \
  --context smart-grid-aks-review -n platform get pods,pvc,svc
open 'https://portal.azure.com/#view/Microsoft_Azure_Education/EducationMenuBlade/~/overview'
```

After the review ends, get the account owner's confirmation before deleting
anything. The worker, managed OS and data disks, registry, public IPs, load
balancer rules, and billable traffic or disk operations use credits while active.
Stopping AKS can stop worker compute charges, while disks, registry, IPs and
configured load balancer rules can continue charging. After approved teardown,
verify both the dedicated group and the AKS-managed node group are absent.

The accepted comparison estimate is $66.23–$73.14 for 192 hours of AKS resources,
including 168 review hours and 24 hours for setup and teardown. Add a $1 allowance
for the deleted VM deployment and earlier registry time for a total estimate of
$67.23–$74.14. The regional worker price remains unverified. Assumptions include
Basic registry storage within 10 GB, 20 GB of load-balancer processing, five
million disk operations, free-allowance internet egress, and prompt approved
teardown. The spending limit is tied to remaining student credit, rather than
the $75 project ceiling. Check the Education page for posted credit consumption.
