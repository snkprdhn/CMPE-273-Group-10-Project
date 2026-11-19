# Project Scope and Delivery Boundaries

This file is the scope-control source of truth for a four-person, twelve-week project using AI-assisted development. AI may accelerate scaffolding, tests, manifests, and documentation, but it does not remove integration, correctness, resource, and live-demo risk.

## Required scope

- Kubernetes as the final runtime using `kind` or `minikube`
- One-broker Kafka developer profile and three-broker final-review profile
- Strimzi-managed Kafka and Flink Kubernetes Operator rather than custom stateful controllers
- Kafka topics, partitions, consumer groups, replay, DLQ, and schema versions
- Flink event-time windows, watermarks, late corrections, checkpoints, and recovery
- EV, smart-meter, and solar simulators with deterministic fault scenarios
- Small gRPC Device Registry with deadlines and typed contracts
- Replicated Action Service with Kubernetes Lease and fencing epoch
- Threshold/hysteresis policy, charger selection, and duplicate-safe commands
- Debezium/Kafka Connect transactional outbox publication, idempotent inbox, and one simplified control saga
- One Helm-managed PostgreSQL primary with persistent storage and service-owned logical schemas/roles
- FastAPI REST/SSE service and focused React operator dashboard
- Local OIDC roles, Kubernetes RBAC/Secrets/NetworkPolicies, and negative tests
- Prometheus/Grafana, OpenTelemetry tracing, and structured Kubernetes logs
- Versioned JSON Schemas, golden fixtures, and CI compatibility checks without a runtime schema-registry service
- Load tests, SLO evidence, and focused failure demonstrations
- One read-only Grid Incident Investigator with seven typed tools, evidence validation, Kubernetes deployment, deterministic mock mode, and fixed evaluations

## Independent delivery requirement

- Each member has a local profile using versioned inputs, mocks, and open-source test dependencies.
- Each member owns their workload's Kubernetes resources, security policy, instrumentation, dashboard panel, failure test, and runbook entry.
- PostgreSQL logical stores and roles are owned by one service boundary; no cross-service table reads are allowed.
- Contract CI and golden fixtures run before pairwise integration.
- The final overlay contains only workloads that already pass their standalone and pairwise gates.

## Explicitly excluded

- Multi-region or multi-cluster deployment
- Service mesh and Consul deployment
- Custom Raft/Paxos implementation
- Production automatic database failover
- Full internal mTLS/certificate-rotation system
- Autonomous grid-control agent
- Multi-agent swarm, agent write tools, vector database, fine-tuning pipeline, and LLM decisions in the control path
- Real chargers, meters, utility networks, or customer data
- Billing, market pricing, customer-account management, and payments
- ML demand forecasting
- Mobile application
- Regulatory or production-utility compliance claims
- Custom operators, brokers, schema registries, identity providers, telemetry collectors, and load/chaos frameworks
- Runtime schema registry, PostgreSQL operator, Loki, KEDA, database replicas/failover, anomaly-detection service, and multi-provider model comparison

## Delivery gates

| Gate | Latest target | Required result |
|---|---|---|
| Contracts frozen | End of Week 2 | Events, gRPC, REST/SSE, agent tools/evidence, database ownership, fixtures, units, and Kubernetes conventions agreed |
| Standalone slices | End of Week 4 | All four member profiles pass without another member's running service |
| Pairwise/Kubernetes baseline | End of Week 6 | Pairwise gates pass; verified workloads run with discovery, probes, persistence, and basic UI |
| Reliability complete | End of Week 8 | Checkpoint recovery, idempotency, leader fencing, outbox/inbox, and pod recovery pass |
| Operations complete | End of Week 10 | OTel, dashboards, auth, load tests, failure matrix, read-only agent tools, and mock-provider evaluations produce evidence |
| Feature freeze | Start of Week 11 | Only fixes, measurements, documentation, and rehearsal remain |

If a gate slips by more than one week, the team reduces UI polish or nonessential test variations before cutting correctness, Kubernetes, observability, security, or the closed-loop business demonstration.

The external-model adapter is integrated only after the distributed core and observability tools are stable; the tool contracts, mock provider, and evaluation fixtures can be completed earlier. The [open-source reuse plan](OPEN_SOURCE_STACK.md) is authoritative for build-versus-buy decisions. Adding another platform component requires an ADR showing that an existing selected tool cannot meet the need and that the operational cost fits the review profile.
