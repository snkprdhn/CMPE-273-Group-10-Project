# Professor review on an Azure VM

The Canada Central VM uses the existing one-broker kind developer profile.
The hosted overlay shares the review identity and evidence retention settings
with the AKS overlay. Calico enforces the container network boundaries.
PostgreSQL, Kafka, Flink checkpoints and Investigator audit state use local
Kubernetes volumes on the 64 GiB Azure data disk mounted at
`/srv/grid-review`. The VM is a single machine and its local volumes require
that machine and disk.

The dedicated resource group contains a `Standard_B4ps_v2` Ubuntu ARM64 VM,
two Standard SSDs, one static public IP, an NSG, and a Basic registry. SSH
is restricted to the approved workstation address. The Kubernetes API and
UI transport bind to VM loopback. Caddy serves only the dashboard and the
permitted browser sign-in routes over the public HTTPS hostname. Its
certificate data is also on the data disk. The proxy service and kind node
restart automatically with the VM. The ARM64 images were prepared locally.
The VM downloads them by digest from ACR and loads them into its private kind
node. The short-lived ACR sign-in can expire because the node keeps the images.

## Review

Sign in with the dedicated professor account and open **Alerts & actions**.
Select the completed action ID from the handoff and open **Investigate action**
to run the deterministic mock and view its cited command evidence. The
account owner can copy the password from the private, ignored credential file
on the workstation without displaying it:

```sh
.venv/bin/python -c 'import json; print(json.load(open(".tools/vm-review/local.json"))["professor"], end="")' | pbcopy
```

## Availability and cost

```sh
curl -fsS --max-time 15 https://smart-grid-ev-review-9e9.canadacentral.cloudapp.azure.com/readyz
open 'https://portal.azure.com/#view/Microsoft_Azure_Education/EducationMenuBlade/~/overview'
```

The plan estimates $40.07 for 192 hours, comprising setup and teardown
allowance and 168 hours of review starting after the live checks pass.
The compute SKU is burstable. Once its CPU credits run low, performance
depends on the base CPU entitlement. Cloud workload checks and the exact
handoff state are tracked in
[implementation status](../../docs/implementation/IMPLEMENTATION_STATUS.md)
and the private review evidence under `.tools/vm-review/`.

After the seven-day window, ask the owner before deleting resources. The VM,
managed disks, public IP, registry and metered traffic or disk operations can
consume credits until teardown. VM deallocation can stop compute charges
while the disks, IP and registry continue to bill.
