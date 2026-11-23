# Operations API — Member_4

Implemented standalone backend: [member4](../../member4/README.md).

Real owned PostgreSQL projections and Kafka consumers provide REST snapshots,
filters/pagination, safe errors and durable cross-replica SSE replay/reset.
Policy/scenario operations use explicit contract mocks; identity uses configured
local mock bearer tokens. No Registry/Control table reads or control decisions.

See [ADR-030](../../docs/decisions/ADR-030-member4-projections-api.md) and
[verification](../../docs/implementation/IMPLEMENTATION_STATUS.md).
NiceGUI, Keycloak, read-only Tool Gateway, Copilot and full kind packaging remain
later milestones.
