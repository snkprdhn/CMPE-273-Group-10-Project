# Control API

**Status: design only; implementation not started.**

Planned real-time projection service and FastAPI boundary for queries, configuration, health, scenario control, and Server-Sent Events. It consumes domain events into query views and manages versioned SSE fan-out, cursor replay, stale-state detection, and snapshot recovery. It also exposes the internal read-only Tool Gateway used by the Copilot Service; these routes accept typed allowlisted queries and never arbitrary SQL, PromQL, shell, Kafka, or Kubernetes operations. The browser will not access Kafka or the database directly. See the [API contract](../../docs/contracts/API_CONTRACTS.md), [agent design](../../docs/design/AGENTIC_COPILOT.md), and [Member_4's work plan](../../docs/team/04_MEMBER_4_REALTIME_UI.md).
