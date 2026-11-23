# ADR-034 — Local kind deployment

- Status: Implemented for the local deployment milestone. See the
  [verification status](../implementation/IMPLEMENTATION_STATUS.md) for executed gates.
- Owners: Member_1 simulation/Registry/observability, Member_2 Kafka/PyFlink,
  Member_3 control/PostgreSQL/CDC, Member_4 projection/API/UI/identity/Copilot.

The two local profiles use separate Docker Desktop kind clusters, Calico 3.31
with IPv4 VXLAN, the selected Helm releases for Strimzi, Flink Operator and
PostgreSQL, and project-owned Kustomize overlays. Disable kindnet for this
deployment. Keep the platform default-deny with explicit DNS, service and API
flows. Test actual positive and negative connections from different identities
and namespaces.

Use internal Services and an operator-authenticated loopback port-forward for
the NiceGUI and public realm routes. The kind issuer is exactly
`http://127.0.0.1:18086/realms/smart-grid`. Fetch keys and redeem tokens over the
internal Keycloak Service while verifying the public issuer. The independently
runnable identity profile retains its tested issuer on port 18080. The
local UI proxy allows only the named realm's login/authorization endpoints and
Keycloak static assets. Cluster administration and the application query API
stay internal during normal use.

Flink Operator runs the existing bounded PyFlink job in standalone Kubernetes
application mode on a single node with a shared checkpoint PVC. This profile
keeps the v1 fixed topology and single aggregation task. PostgreSQL uses separate
logical databases and roles. The fenced gateway alone shares a transaction
boundary with the typed Action authorization function, as in ADR-029. Connect
uses the selected Debezium plugin with the scoped publication and a Secret-backed
password reference. A pre-created Lease gives the Action worker a narrower Role
than the host acceptance harness needs.

Run the accepted deterministic scenario in a one-shot Member_1 Job against the
kind workloads. Verify the durable gateway effect, committed Flink windows,
outbox delivery, replay-equivalent projections, matching REST/SSE and real
Keycloak/Chromium dashboard login. Review adds broker/process redundancy on the
same node. Prometheus/Grafana/trace packaging, Copilot and live operator mutation
adapters remain subsequent work. See the [runbook](../../infra/kind/README.md).
