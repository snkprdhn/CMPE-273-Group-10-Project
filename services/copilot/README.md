# Grid Incident Investigator

**Status: design only; implementation has not started.**

Planned home of the provider-neutral, read-only agent orchestration service. It accepts authenticated investigation requests, calls allowlisted tools through the internal Tool Gateway, validates evidence-backed structured answers, persists a bounded audit record, and streams progress to the operator dashboard. It has no command, policy-write, raw SQL, Kafka, shell, or Kubernetes tool.

See the [agent design](../../docs/design/AGENTIC_COPILOT.md), [API contract](../../docs/contracts/API_CONTRACTS.md), and [security design](../../docs/design/SECURITY_AND_PRIVACY.md).
