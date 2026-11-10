# Project Scope and Delivery Tiers

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
- One CloudNativePG-managed PostgreSQL primary with persistent storage and time-indexed history tables
- FastAPI REST/SSE service and focused React operator dashboard
- Local OIDC roles, Kubernetes RBAC/Secrets/NetworkPolicies, and negative tests
- Prometheus/Grafana, OpenTelemetry tracing, and structured Kubernetes logs
- Apicurio-backed Kafka schema governance and compatible evolution checks
- Load tests, SLO evidence, and focused failure demonstrations
- One read-only Grid Incident Investigator with seven typed tools, evidence validation, Kubernetes deployment, deterministic mock mode, and fixed evaluations

## Stretch scope

- PostgreSQL read replica and stale-read experiment
- KEDA scaling from Kafka lag
- Loki log aggregation
- Transparent anomaly detection against a rolling baseline
- Comparison of a second external model provider using the same evaluation cases
- Exportable agent-assisted incident summary after the investigation workflow is stable

Stretch work starts only after the full required telemetry-to-command loop runs in Kubernetes, automated tests pass, and the final failure scenarios are stable.

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

## Delivery gates

| Gate | Latest target | Required result |
|---|---|---|
| Contracts frozen | End of Week 2 | Events, gRPC, REST/SSE, agent tools/evidence, units, ownership, and Kubernetes approach agreed |
| Distributed core | End of Week 4 | Simulator → Kafka → Flink → Action → Kafka → simulator works |
| Kubernetes baseline | End of Week 6 | Required services run with discovery, probes, persistence, and basic UI |
| Reliability complete | End of Week 8 | Checkpoint recovery, idempotency, leader fencing, outbox/inbox, and pod recovery pass |
| Operations complete | End of Week 10 | OTel, dashboards, auth, load tests, failure matrix, read-only agent tools, and mock-provider evaluations produce evidence |
| Feature freeze | Start of Week 11 | Only fixes, measurements, documentation, and rehearsal remain |

If a gate slips by more than one week, stretch work stops and the team reduces UI polish or nonessential test variations before cutting correctness, Kubernetes, or observability.

The external-model adapter is integrated only after the distributed core and observability tools are stable; the tool contracts, mock provider, and evaluation fixtures can be completed earlier. The [open-source reuse plan](OPEN_SOURCE_STACK.md) is authoritative for build-versus-buy decisions. Adding another platform component requires an ADR showing that an existing selected tool cannot meet the need and that the operational cost fits the review profile.
