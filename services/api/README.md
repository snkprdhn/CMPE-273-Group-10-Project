# Control API

**Status: design only; implementation not started.**

Planned real-time projection service and FastAPI boundary for queries, configuration, health, scenario control, and Server-Sent Events. It consumes load, alert, action, and result events into the owned `projection` store and manages versioned SSE fan-out, cursor replay, stale-state detection, and snapshot recovery. Configuration/scenario operations call typed internal boundaries and never write another service's tables. It also exposes the internal read-only Tool Gateway used by the Copilot Service; these routes accept typed allowlisted queries and never arbitrary SQL, PromQL, shell, Kafka, or Kubernetes operations. Recorded events, mock identity, and mock model/tool results allow independent testing. See the [API contract](../../docs/contracts/API_CONTRACTS.md), [agent design](../../docs/design/AGENTIC_COPILOT.md), and [Member_4's work plan](../../docs/team/04_MEMBER_4_REALTIME_UI.md).
