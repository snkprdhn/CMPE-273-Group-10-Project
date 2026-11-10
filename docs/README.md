# Project Documentation

This directory contains the design package for the Smart Grid / EV Fleet Energy Balancing Platform. The root README is the executive overview; the files below are the detailed sources of truth.

## Design

- [System design](design/SYSTEM_DESIGN.md) — scope, requirements, architecture, component responsibilities, and deployment view
- [Data flow](design/DATA_FLOW.md) — topics, partitioning, windows, control logic, and processing semantics
- [Data model](design/DATA_MODEL.md) — conceptual entities, relationships, state machines, and retention
- [Failure handling](design/FAILURE_HANDLING.md) — failure modes, retries, recovery, and degraded behavior
- [UI design](design/UI_DESIGN.md) — information architecture, screens, wireframes, visual language, and live-update behavior
- [Security and privacy](design/SECURITY_AND_PRIVACY.md) — trust boundaries, threats, secrets, and safety constraints
- [Kubernetes deployment](design/KUBERNETES_DEPLOYMENT.md) — target runtime, discovery, probes, scaling, and failure demonstrations
- [Communication and resilience](design/COMMUNICATION_AND_RESILIENCE.md) — REST/gRPC/Kafka/SSE choices, deadlines, retries, breakers, and bulkheads
- [Transactions and sagas](design/TRANSACTIONS_AND_SAGAS.md) — outbox/inbox and recoverable control workflow
- [Coordination and fencing](design/COORDINATION_AND_FENCING.md) — leader leases, stale workers, and ordering scopes
- [Replication and consistency](design/REPLICATION_AND_CONSISTENCY.md) — Kafka replication, database concurrency, and operation-specific guarantees
- [Observability and SLOs](design/OBSERVABILITY_AND_SLOS.md) — OpenTelemetry, signals, SLIs, SLOs, and diagnostic evidence
- [Grid Incident Investigator](design/AGENTIC_COPILOT.md) — required bounded agent, read-only tools, provider abstraction, guardrails, and evaluation
- [Project scope](design/PROJECT_SCOPE.md) — required, stretch, excluded, and delivery-gate definitions
- [Open-source reuse](design/OPEN_SOURCE_STACK.md) — selected projects, build-versus-reuse boundary, integration ownership, and resource profiles

## Contracts

- [Event contracts](contracts/EVENT_CONTRACTS.md) — canonical Kafka topics and event envelopes
- [HTTP API contracts](contracts/API_CONTRACTS.md) — planned operator and UI API
- [gRPC contracts](contracts/GRPC_CONTRACTS.md) — typed internal device registry and deadline behavior
- [Agent tool contracts](contracts/AGENT_TOOL_CONTRACTS.md) — typed read-only tools, evidence envelope, errors, output validation, and versioning

Contract changes require agreement from the producer, consumer, and integration owner. Once implementation begins, machine-readable schemas under `schemas/` will become the executable representation of these documents.

## Team and delivery

- [Team overview](team/TEAM_OVERVIEW.md)
- [Member_1 — distributed ingestion and simulation](team/01_MEMBER_1_INGESTION.md)
- [Member_2 — messaging and stream processing](team/02_MEMBER_2_STREAMING.md)
- [Member_3 — distributed control and persistence](team/03_MEMBER_3_CONTROL.md)
- [Member_4 — real-time projection, UI, and integration](team/04_MEMBER_4_REALTIME_UI.md)
- [Acceptance plan](demo/ACCEPTANCE_PLAN.md)
- [Decision register](decisions/README.md)
- [Course alignment](course/CURRICULUM_ALIGNMENT.md)
- [Threat model](security/THREAT_MODEL.md)
- [Failure-injection matrix](testing/FAILURE_INJECTION_MATRIX.md)
- [Load-test plan](testing/LOAD_TEST_PLAN.md)
- [Agent evaluation plan](testing/AGENT_EVALUATION_PLAN.md)
- [Incident diagnosis runbook](runbooks/INCIDENT_DIAGNOSIS.md)

## Documentation rules

1. Describe planned behavior as planned until it has been implemented and verified.
2. Keep event field names and API paths canonical in the contract documents rather than copying competing definitions into component READMEs.
3. Record significant architectural changes as ADRs in `docs/decisions/`.
4. Add setup commands only after another team member has reproduced them.
5. Update diagrams and acceptance criteria in the same pull request as any design change.
