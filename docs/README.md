# Project Documentation

This directory contains the design package for the Smart Grid / EV Fleet Energy Balancing Platform. The root README is the executive overview; the files below are the detailed sources of truth.

## Implementation

- [Implementation status](implementation/IMPLEMENTATION_STATUS.md) — completed milestone, remaining gates, and next step
- [Accepted version matrix](implementation/VERSION_MATRIX.md) — exact pins and upstream compatibility evidence
- [Local prerequisites](implementation/LOCAL_PREREQUISITES.md) — machine inventory, manual installation, verification, and resource profiles
- [Preparation verification](implementation/PREPARATION_VERIFICATION.md) — exact diagnostic commands and observed results

- [Master implementation guide](implementation/MASTER_IMPLEMENTATION_GUIDE.md) — prerequisites, decisions, twelve-week build order, independent workflows, local kind integration (cloud deferred), CI/CD, testing, rollback, evidence, and final acceptance

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
- [Project scope](design/PROJECT_SCOPE.md) — required, independently deliverable, excluded, and delivery-gate definitions
- [Open-source reuse](design/OPEN_SOURCE_STACK.md) — selected projects, build-versus-reuse boundary, integration ownership, and resource profiles
- [Dependency minimization](design/DEPENDENCY_MINIMIZATION.md) — contract-first boundaries, logical data ownership, platform ownership, and integration cadence

## Contracts

- [Event contracts](contracts/EVENT_CONTRACTS.md) — canonical Kafka topics and event envelopes
- [Stream semantics v1](contracts/STREAM_SEMANTICS_V1.md) — reporting, identity, validation, windows, completeness, revisions, capacity, and late-data rules
- [HTTP API contracts](contracts/API_CONTRACTS.md) — executable operator/UI routing and shared schemas
- [gRPC contracts](contracts/GRPC_CONTRACTS.md) — typed internal device registry and deadline behavior
- [Agent tool contracts](contracts/AGENT_TOOL_CONTRACTS.md) — typed read-only tools, evidence envelope, errors, output validation, and versioning

Contract changes require agreement from the producer, consumer, and integration owner. The [v1 schema bundle](../schemas/README.md) is the executable representation of these documents; `make contracts` validates it and [ADR-025](decisions/ADR-025-executable-contract-freeze.md) resolves exact defaults.

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
- [Independent component testing](testing/INDEPENDENT_COMPONENT_TESTING.md)
- [Incident diagnosis runbook](runbooks/INCIDENT_DIAGNOSIS.md)

## Documentation rules

1. Describe planned behavior as planned until it has been implemented and verified.
2. Keep event field names and API paths canonical in the contract documents rather than copying competing definitions into component READMEs.
3. Record significant architectural changes as ADRs in `docs/decisions/`.
4. Setup instructions may be documented before execution when clearly labeled unverified. Record exact commands, environment, and results when reproduced; a clean-checkout rerun on the implementation computer is sufficient.
5. Update diagrams and acceptance criteria in the same pull request as any design change.
