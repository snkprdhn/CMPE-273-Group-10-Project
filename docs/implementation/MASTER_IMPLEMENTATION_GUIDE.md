# Master Implementation Guide

**Status:** Repository preparation and executable contract milestones complete; an empty local kind cluster and locked preparation venv are verified; application services, application dependency locks, and platform deployment remain unimplemented/unverified. See [implementation status](IMPLEMENTATION_STATUS.md), [accepted versions](VERSION_MATRIX.md), and [manual prerequisites](LOCAL_PREREQUISITES.md).

## 1. Purpose

This is the end-to-end delivery guide for the Smart Grid / EV Fleet Energy Balancing Platform. It preserves Member_1 through Member_4 ownership while implementation happens on one computer. It covers prerequisites, contract freeze, Python application development, local kind packaging, testing, security, observability, failure experiments, release evidence, and final demonstration. Cloud deployment and external model calls are deferred.

This guide coordinates the work; it does not replace the detailed design and contract documents. When a conflict exists, the canonical contract or accepted architecture decision is authoritative.

## 2. Delivery principles

1. Build the deterministic grid-control path before the optional external-model adapter.
2. Develop against versioned contracts, fixtures, and mocks rather than another member's unfinished service.
3. Require every component to pass a standalone gate before it enters shared integration.
4. Use Kubernetes for integration and final demonstration, while keeping local component tests lightweight.
5. Reuse open-source infrastructure and spend team effort on grid-specific behavior and distributed-systems evidence.
6. Keep the LLM-based investigator read-only and outside the grid-control path.
7. Treat documentation, tests, Kubernetes resources, security, and telemetry as part of each component—not as final-week work.
8. Mark targets as proposed until a repeatable test produces measured evidence.

## 3. Sources of truth

| Concern | Authoritative document |
|---|---|
| Scope and exclusions | [Project scope](../design/PROJECT_SCOPE.md) |
| Architecture and responsibilities | [System design](../design/SYSTEM_DESIGN.md) |
| Runtime data movement | [Data flow](../design/DATA_FLOW.md) |
| Kafka event names and fields | [Event contracts](../contracts/EVENT_CONTRACTS.md) |
| Windows, ordering, lateness, and quality | [Stream semantics v1](../contracts/STREAM_SEMANTICS_V1.md) |
| REST and SSE behavior | [API contracts](../contracts/API_CONTRACTS.md) |
| Device Registry RPC | [gRPC contracts](../contracts/GRPC_CONTRACTS.md) |
| Agent tools and evidence | [Agent tool contracts](../contracts/AGENT_TOOL_CONTRACTS.md) |
| Work ownership | [Team overview](../team/TEAM_OVERVIEW.md) |
| Independent development | [Dependency minimization](../design/DEPENDENCY_MINIMIZATION.md) |
| Kubernetes behavior | [Kubernetes deployment design](../design/KUBERNETES_DEPLOYMENT.md) |
| Open-source choices | [Open-source reuse](../design/OPEN_SOURCE_STACK.md) |
| Final evidence | [Acceptance plan](../demo/ACCEPTANCE_PLAN.md) |

## 4. Target environments

[ADR-006](../decisions/ADR-006-local-kind.md) makes local kind the required integration and demonstration target. All project application/test code is Python; JVM-based infrastructure stays inside containers. Host Java/Maven and Node.js/npm are unnecessary.

| Environment | Purpose | Required characteristics |
|---|---|---|
| Standalone lightweight development | One owned slice with fixtures/mocks | Only minimum real dependencies; independently runnable |
| Local kind integration/review | Closed loop, replica/recovery/security evidence | Pinned images/contracts; three-broker review exercises; staged to fit 16 GiB host |
| Cloud | Deferred | No resources, accounts, cloud registry, budget, or credentials required |

Use local `platform`, `observability`, and `integration` namespaces as appropriate; member namespace labels remain available for isolation without requiring four concurrent copies. Scope topics/groups, logical stores, and fixtures by environment/run ID. Build locally and load application images into kind; no registry is needed.

## 5. Prerequisites and downloads

### 5.1 Developer machine

The inspected machine is Apple M5/ARM64 with 10 CPU cores and 16 GiB RAM (268 GiB disk was free at the initial inventory). Use the [resource budgets](LOCAL_PREREQUISITES.md#resource-profiles): 4 CPUs/6 GiB Docker Desktop development profile; 8 CPUs/10 GiB larger review profile, one at a time. These are planning budgets, not measured deployment capacity.

### 5.2 Required tools and downloads

The authoritative [version matrix](VERSION_MATRIX.md) and [prerequisites guide](LOCAL_PREREQUISITES.md) contain installed results, exact pins, manual installation commands, and verification steps. Git, Docker Desktop/CLI, project-venv Python, kind, matching kubectl/bundled Kustomize, and Helm cover the workflow. Project Python 3.12.9 is installed in `.venv`; use venvs for all Python work. The default host Python 3.14 is unsuitable for the selected PyFlink environment.

Docker Desktop is now running; kind/Helm/matching kubectl and Python 3.12.9 are installed project-locally. Activate `.venv` for all Python and selected CLI work. The latest verified gates are in implementation status. Java/Node were inventoried but are not host prerequisites. Use Python gRPC tooling, Locust, and Playwright Python instead of grpcurl/k6/authored JavaScript.

### 5.3 Accounts and data

No cloud or external model account is required. Use deterministic model mocks, synthetic devices/data, and local disposable credentials. Keep credentials out of Git, images, browser state, logs, and evidence.

## 6. Decisions and milestone boundaries

Preparation decisions are accepted in [ADR-002](../decisions/ADR-002-python-stream-processing.md), [ADR-004](../decisions/ADR-004-python-dashboard.md), [ADR-006](../decisions/ADR-006-local-kind.md), and [ADR-024](../decisions/ADR-024-pinned-local-baseline.md). They resolve PyFlink, Python UI/test tooling, local deployment, version pins, partition/replication defaults, and resource profiles.

The **contract/schema milestone is complete** under [ADR-025](../decisions/ADR-025-executable-contract-freeze.md): canonical JSON Schema/Protobuf/OpenAPI, versioned examples, deterministic golden cases, compatibility checks and contract CI are implemented. Action defaults and local identity names/roles are fixed there. Run `make contracts`; see [schemas](../../schemas/README.md). The next milestone is repository foundation/component packaging, including native ARM64 PyFlink build/import/checkpoint evidence before an aggregation slice is called runnable. Repository licensing remains an owner/public-release decision. Cloud and external-provider choices remain deferred.

No application service scaffolding is part of preparation.

## 7. Planned repository additions

The implementation should converge on this structure. Names may change through an ADR, but ownership boundaries must remain.

```text
.
├── Makefile or task runner        # Small, documented entry points only
├── .env.example                   # Safe placeholders; no secrets
├── .github/workflows/             # Contract, component, image, and deployment CI
├── config/                        # Non-secret defaults and scenario definitions
├── schemas/                       # JSON Schema, Protobuf, and OpenAPI sources
├── simulators/                    # Member_1 implementation
├── services/
│   ├── registry/                  # Member_1
│   ├── aggregation/               # Member_2
│   ├── action/                    # Member_3
│   ├── api/                       # Member_4
│   └── copilot/                   # Member_4
├── dashboard/                     # Member_4
├── infra/
│   ├── kind/                      # Local cluster configuration
│   ├── compose/                   # Optional lightweight dependencies
│   ├── charts/                    # Pinned values for third-party charts
│   ├── base/                      # Project-owned Kustomize bases
│   ├── overlays/
│   │   ├── local/
│   │   ├── member-1/
│   │   ├── member-2/
│   │   ├── member-3/
│   │   ├── member-4/
│   │   └── integration/
│   └── review/                    # Local larger-profile evidence/configuration
└── tests/
    ├── contracts/
    ├── component/
    ├── integration/
    ├── e2e/
    ├── load/
    ├── failure/
    └── evidence/
```

## 8. Configuration and secret rules

### 8.1 Configuration hierarchy

Use the following precedence, from lowest to highest:

1. Version-controlled safe application defaults
2. Environment-specific ConfigMaps or Helm values
3. Kubernetes Secrets for credentials
4. Test-specific overrides supplied by the runner

Every setting must have an owner, type, default, allowed range, and secret/non-secret classification. Required settings should fail fast at startup with a useful error; secrets must never be printed.

### 8.2 Identifiers and isolation

Every environment defines an immutable environment ID such as `local-m2`, `dev-m3`, or `integration`. Use it in:

- Kafka topic prefixes and consumer-group names where infrastructure is shared
- PostgreSQL database/schema names and roles
- scenario and fixture identifiers
- object/checkpoint paths
- evidence directories and dashboard filters

### 8.3 Secret handling

- Commit `.env.example`, never `.env`.
- Use safe local placeholder credentials only for disposable local profiles.
- Keep shared credentials in Kubernetes Secrets created by CI or the environment administrator.
- Local implementation needs no cloud credentials; do not add AWS keys or external model keys.
- Rotate a credential immediately if it appears in Git history, logs, screenshots, or chat.
- Keep the model key available only to the Copilot pod and exclude it from all core-service environments.

## 9. Collaboration and delivery workflow

### 9.1 Branch and pull-request flow

```text
Issue with owner and acceptance criterion
    -> focused feature branch
    -> local standalone tests
    -> contract/component CI
    -> review by at least one member
    -> producer and consumer review for contract changes
    -> merge to main
    -> immutable image build
    -> integration namespace deployment
    -> integration evidence
```

No pull request may require another member's unpublished branch. Interface defects must first become a failing shared fixture or contract test.

### 9.2 Image identity

Every deployed image must be immutable and traceable to:

- Git commit SHA
- component name
- build timestamp in metadata, not in reproducible application output
- dependency/SBOM artifact
- CI run

Do not deploy `latest`. Promote the same tested image digest from a member namespace to `integration`; do not rebuild it during promotion.

### 9.3 Shared-environment ownership

| Area | Primary owner | Required reviewers |
|---|---|---|
| Local kind lifecycle and base namespaces | Shared, coordinated by Member_4 | Affected component owners |
| Kafka/Strimzi and Flink Operator | Member_2 | Producers/consumers affected by change |
| PostgreSQL and Kafka Connect/Debezium | Member_3 | Logical-store owner affected by change |
| OTel, Prometheus, Grafana, and Tempo installation | Member_1 | Member_4 for product SLO views |
| Gateway, Keycloak, and final integration overlay | Member_4 | All component owners |
| Component Deployment, Service, policy, telemetry, and runbook | Component owner | One teammate |

The environment administrator maintains the cluster but does not implement or debug everyone else's workloads.

## 10. Twelve-week implementation sequence

### Phase 0 — Freeze the delivery baseline (Weeks 1–2)

**All members**

1. Accept or revise every blocking ADR.
2. Pin the tool, chart, operator, image, and language versions.
3. Freeze topic names, keys, event envelopes, units, timestamps, and compatibility rules.
4. Freeze Protobuf, OpenAPI, SSE, agent-tool, and evidence contracts.
5. Create valid and invalid fixtures with expected outcomes.
6. Agree on environment IDs, ports, health endpoints, log fields, trace headers, and metric naming.
7. Use the accepted local resource profiles; cloud remains deferred.

**Exit gate:** Contract validation can run without application code, all open blocking choices have ADRs, and each member can explain their standalone input and expected output.

### Phase 1 — Build the repository foundation (Weeks 1–2)

1. Add language dependency manifests and locked dependency files per component.
2. Add lint, format, unit-test, contract-test, and security-scan CI jobs.
3. Generate code from Protobuf/OpenAPI only through repeatable scripts.
4. Create `.env.example` and a typed configuration reference.
5. Add container build files with non-root users, health checks where appropriate, pinned bases, and minimal runtime layers.
6. Add the local `kind` configuration and empty Kustomize bases/overlays.
7. Add an evidence directory convention without committing large generated results.

**Exit gate:** A documentation-only or skeleton pull request can run all foundation checks, and every container skeleton builds without application secrets.

### Phase 2 — Implement four standalone slices in parallel (Weeks 2–4)

#### Member_1: edge, Registry, and reliability tooling

1. Implement deterministic transformer, meter, solar, and EV charger scenario models.
2. Publish schema-valid telemetry using the required partition key and stable event identity.
3. Implement bounded buffering, retry/backoff, delivery callbacks, and observable backpressure.
4. Implement charger command consumption with expiry, command-ID deduplication, fencing-epoch rejection, and result events.
5. Implement the gRPC Registry contract, deadlines, validation, and owned persistence.
6. Create the scenario runner and reusable Locust/Toxiproxy orchestration interface.
7. Package owned workloads and add health, metrics, traces, logs, policies, and fault tests.

#### Member_2: Kafka and stateful stream processing

1. Implement Strimzi Kafka developer and review profiles plus versioned topics/users.
2. Implement schema-compatibility CI and reject invalid telemetry to the DLQ.
3. Implement event-time aggregation, watermarking, completeness states, revisions, and late-event audit behavior.
4. Use keyed state per transformer and preserve deterministic IDs and revision ordering.
5. Configure checkpoints, restart behavior, state restoration, and bounded replay.
6. Publish transformer-load events without writing another service's database.
7. Package the job/operator resources and add metrics, traces/log context, dashboards, policies, and failure tests.

#### Member_3: control, coordination, and persistence

1. Implement the threshold/hysteresis/cooldown policy and charger-selection logic as deterministic domain functions.
2. Implement the Action state machine with stable plan, command, and causation identities.
3. Implement inbox deduplication, optimistic versions, PostgreSQL transactions, and transactional outbox rows.
4. Configure Debezium/Kafka Connect outbox publication and verify safe duplicate delivery.
5. Implement Kubernetes Lease leadership and a durable fencing epoch; reject stale work.
6. Implement acknowledgement, timeout, retry, partial outcome, safe hold, and restore behavior.
7. Package owned resources and add migrations, health, metrics, traces, logs, policies, backup/export, and fault tests.

#### Member_4: projections, API, UI, identity, and investigator

1. Implement replay-safe projection consumers for load, alert, action, command, and result events.
2. Implement REST snapshots, pagination/error shapes, SSE event IDs, cursor recovery, and explicit stale/partial states.
3. Implement the focused operator UI with normal, warning, critical, recovering, stale, loading, empty, forbidden, and error states.
4. Configure Keycloak roles and enforce authorization in the API, not only in the browser.
5. Implement the typed read-only Tool Gateway against owned projections and bounded observability adapters.
6. Implement the Copilot with deterministic mock responses first, evidence validation, budgets, audit records, and provider isolation.
7. Package owned workloads and add health, metrics, traces, logs, policies, dashboards, and failure tests.

**Exit gate:** Every member passes the standalone profile in [Independent Component Testing](../testing/INDEPENDENT_COMPONENT_TESTING.md) without another member's running service.

### Phase 3 — Complete pairwise integration (Weeks 4–6)

Run the gates independently so one failed boundary does not block all integration:

1. Member_1 to Member_2: telemetry fixtures and live events produce the same load windows.
2. Member_2 to Member_3: critical, revised, stale, and recovery loads produce allowed state transitions only.
3. Member_3 to Member_1: duplicate, expired, delayed, and stale-epoch commands produce the expected device result.
4. Members_2/3 to Member_4: recorded and live events converge to identical projections and SSE updates.
5. Member_4 agent boundary: incident fixtures resolve to valid evidence without write access or core-service calls.

For each gate, preserve the contract versions, input seed, commands, expected result, actual result, trace ID, and owner sign-off.

**Exit gate:** All four pairwise reports pass, and any discovered interface defect has a regression fixture on both sides.

### Phase 4 — Assemble full local Kubernetes (Weeks 5–7)

Install in dependency order:

1. Create the pinned multi-node `kind` cluster and local storage/ingress prerequisites.
2. Create namespaces, quotas, service accounts, RBAC, and default-deny NetworkPolicies.
3. Install Strimzi, Flink Kubernetes Operator, and Prometheus Operator/chart; deploy the OTel Collector directly by Helm (ADR-024).
4. Install PostgreSQL, Keycloak, Prometheus, Grafana, Tempo, and the OTel Collector.
5. Create Kafka, topics/users, Kafka Connect, databases/roles, and Keycloak realm/client configuration.
6. Apply project bases for Registry, simulators, aggregation, Action, projection/API, dashboard, and Copilot.
7. Load deterministic fixtures and run the normal-state smoke test.
8. Run the overload-to-throttle-to-recovery scenario.
9. Verify probes, DNS, persistence, security boundaries, dashboards, logs, traces, and cleanup.

Document commands as planned until executed; a clean-checkout rerun on this computer records reproducibility without inventing a second operator. The intended eventual command surface should be small: prerequisites check, cluster create, platform install, application deploy, smoke test, evidence capture, and cluster delete.

**Exit gate:** A new teammate can reproduce the complete local system from a clean checkout using the verified runbook.

### Phase 5 — Cloud deployment deferred

No AWS/EKS resources, accounts, registry, or credentials are required or authorized. Use the local kind integration/review profiles. Reopening cloud deployment requires a separately approved scope change.

### Phase 6 — Reliability, security, and operations (Weeks 7–10)

1. Complete idempotency, replay, fencing, checkpoint, outbox/inbox, and restore tests.
2. Enforce pod security settings, least-privilege RBAC, NetworkPolicies, secret isolation, and negative authorization tests.
3. Add bounded-cardinality domain metrics and trace context across Kafka, gRPC, REST, SSE, and agent tools.
4. Create the required Grafana views and SLO/error-budget calculations.
5. Document backup/export and recovery for authoritative Registry and Control state.
6. Run pod, broker, dependency-latency, database-outage, leader-failover, provider-outage, and UI-reconnect experiments.
7. Confirm the core workflow remains healthy when the external model is unavailable.

**Exit gate:** The failure matrix, threat-model checks, SLO evidence, and incident packet contain reproducible results rather than design claims.

### Phase 7 — Performance, acceptance, and freeze (Weeks 9–12)

1. Establish a stable baseline on a named machine/cluster configuration.
2. Increase simulated devices and event rate gradually; identify the first bottleneck with two signals.
3. Tune only after preserving the original evidence and hypothesis.
4. Run the complete acceptance sequence and record deviations.
5. Freeze features at the start of Week 11.
6. Rebuild from a clean checkout and deploy the exact release candidate.
7. Produce the contribution record, known limitations, diagrams, screenshots, evidence index, and rehearsal script.
8. Rehearse locally using deterministic model mocks and cached infrastructure images.

**Exit gate:** The final release digest, manifests, seed, reports, dashboards, traces, and demo sequence are reproducible and owned by the team.

## 11. Local development plan

### 11.1 Per-member loop

Each member follows the same daily loop:

1. Pull the accepted contract bundle.
2. Start only the owned component and its minimum local dependencies.
3. Feed versioned fixtures or use an owned fake dependency.
4. Run unit, contract, and component tests.
5. Inspect expected durable state, output events, metrics, traces, and logs.
6. Run the owned failure case.
7. Stop dependencies and remove disposable test state.
8. Attach the relevant evidence to the pull request.

### 11.2 Full local cluster verification order

For a clean deployment, verify each layer before installing the next:

| Layer | Verification before proceeding |
|---|---|
| Cluster | Nodes ready, storage works, DNS resolves, ingress/gateway prerequisite ready |
| Operators | Controllers ready and CRDs established |
| Data platform | Kafka quorum/profile healthy, PostgreSQL ready, persistent volumes bound |
| Observability and identity | OTel, Prometheus, Grafana, Tempo, and Keycloak ready |
| Contracts/configuration | Topics, roles, databases, realm, policies, and secrets present |
| Applications | Probes pass and no unexpected restart loop |
| Business flow | Seeded telemetry produces load, action, command, result, projection, and UI update |
| Resilience | Selected restart/replay/failover behavior matches the acceptance rule |

Never continue installing later layers to hide an unhealthy earlier dependency.

## 12. Local integration rules

1. Keep independent Member_1 through Member_4 slices and versioned fixture boundaries.
2. Use explicit `kind-smart-grid-*` contexts and environment/run IDs.
3. Run one VM/profile at a time and stage review exercises within the memory budget.
4. Preserve service-owned schemas/roles, RBAC, and enforced NetworkPolicy tests.
5. Record commit, image digest, commands, seeds, configuration, and results for each integration gate.
6. Export evidence/backups before deleting disposable clusters or volumes.

## 13. Testing strategy and execution order

Testing progresses from cheapest and most isolated to most expensive and integrated.

| Level | Runs where | Blocks | Primary evidence |
|---|---|---|---|
| Static/lint/type | Local and every PR | Merge | Clean tool output |
| Unit | Local and every PR | Merge | Domain and edge-case assertions |
| Contract | Local and every PR | Merge | Schema/API compatibility and golden fixtures |
| Component | Standalone local profile | Merge for owned slice | Real dependency plus fake boundaries |
| Pairwise | Local or member namespaces | Integration promotion | Producer/consumer evidence report |
| End-to-end | Local kind `integration` | Release candidate | Closed-loop business outcome |
| Security | Component and integration | Release candidate | Positive/negative authorization and isolation |
| Failure | Local kind scheduled runs | Final acceptance | Recovery time, correctness, and evidence packet |
| Load | Stable integration environment | Final performance claims | Raw result, machine/cluster profile, percentiles |
| Agent evaluation | Deterministic mock; injected provider failures | Agent release | Tool choice, citations, denial, cost, and failure isolation |

### 13.1 Required smoke flow

1. Register deterministic simulated devices and transformer assignment.
2. Publish normal meter, solar, and EV telemetry.
3. Observe one complete transformer-load window.
4. Increase EV demand above the critical threshold.
5. Observe exactly one logical action plan and duplicate-safe commands.
6. Observe device results and reduced EV demand.
7. Observe recovery and safe restore.
8. Confirm the projection/API/UI show the same identifiers and state.
9. Confirm metrics, trace IDs, and correlated logs explain the flow.
10. Run the read-only investigation using the deterministic provider and validate its evidence IDs.

### 13.2 Test data rules

- Use deterministic seeds and synthetic identifiers.
- Keep UTC timestamps and units consistent with contracts.
- Isolate each run by environment and run ID.
- Define expected events, durable state, metrics, and error/DLQ behavior.
- Never treat missing telemetry as zero load or automatic recovery.
- Preserve failed input as a minimal regression fixture.

## 14. CI/CD plan

### 14.1 Pull-request pipeline

1. Detect affected components and contracts.
2. Run formatting, linting, type checks, secret scan, and dependency checks.
3. Validate JSON Schema examples, Protobuf compatibility/generation, OpenAPI, and agent schemas.
4. Run affected unit and component tests.
5. Build the component image without publishing a release tag.
6. Scan the image and generate an SBOM.
7. Render Helm/Kustomize resources and run policy/schema validation.

### 14.2 Main-branch pipeline

1. Repeat required PR checks.
2. Build and sign/tag immutable images by commit SHA.
3. Load locally built immutable images into kind; registry publishing is deferred.
4. Deploy the reviewed digests to the protected `integration` namespace.
5. Run smoke and pairwise regression tests.
6. Roll back the deployment if health or smoke checks fail.
7. Store compact test/evidence artifacts with retention appropriate for the course.

### 14.3 Scheduled pipeline

- Full end-to-end suite
- Failure-injection subset
- Dependency and container scan
- Load baseline at controlled times
- Deterministic mock-agent evaluation and injected provider failures
- Backup/export and restore verification

CI must not expose secrets to untrusted pull requests. Run local deployment gates deliberately on this computer; no cloud runner or paid model calls are required.

## 15. Kubernetes implementation checklist per workload

Every component owner supplies and verifies:

- Deployment, Job, or operator custom resource appropriate to the workload
- Internal Service and only the minimum external route
- ConfigMap/Secret references with documented configuration
- Startup, readiness, and liveness behavior
- CPU/memory requests and limits
- Graceful shutdown and termination grace period
- ServiceAccount and least-privilege RBAC
- Default-deny-compatible NetworkPolicy
- Pod security context, non-root user, and read-only filesystem where practical
- PodDisruptionBudget only where replica behavior justifies it
- Persistent storage and backup rules for authoritative state
- Prometheus discovery, OTel configuration, and structured logs
- Dashboard panel, alert/SLO signal, runbook entry, and fault test
- Kustomize base and local/member/integration patches without duplicated full manifests

## 16. Observability and evidence plan

Use consistent correlation fields across events, spans, and logs:

- `event_id`
- `correlation_id`
- `causation_id`
- `transformer_id`
- `device_id` when bounded and appropriate
- `action_id`
- `command_id`
- `scenario_run_id`
- `trace_id`
- `environment_id`

Every major experiment must preserve:

1. Hypothesis and expected result
2. Commit and image digest
3. Environment and resource profile
4. Scenario seed and time window
5. One relevant metric view
6. One distributed trace or bounded trace summary
7. Correlated structured logs
8. Business impact
9. Recovery time and correctness result
10. Limitation or alternative explanation

## 17. Backup, restore, and rollback

### 17.1 Authoritative versus rebuildable state

| State | Treatment |
|---|---|
| Registry assignments/capabilities | Backup/export or deterministic re-registration procedure |
| Control policy, action, inbox/outbox, command, and fencing state | Authoritative; backup/export and tested recovery required |
| Projection data | Rebuildable from retained Kafka events; document replay procedure |
| Copilot audit | Retain only bounded course evidence; not part of control recovery |
| Flink state | Recover through configured checkpoints/savepoints and retained sources |
| Kafka test data | Retain long enough for replay/evidence; not a permanent archive |

### 17.2 Rollback rules

- Stateless application rollback uses the previously tested image digest.
- Schema changes require backward-compatible expand/migrate/contract steps; never assume an image rollback can reverse destructive data changes.
- Flink upgrades require an approved savepoint/checkpoint and state-compatibility plan.
- Kafka contract breaking changes use a new major topic/schema version and a migration period.
- Platform chart/operator changes require pinned previous values and a tested rollback/recovery procedure.
- Reconcile local kind resources from declared configuration; record recovery commands.

## 18. Local resource and lifecycle controls

Keep one Docker Desktop resource profile active. Monitor memory pressure, CPU, images, PVCs, checkpoints, and evidence disk use. Stop idle components between exercises. Preserve authoritative state/evidence before deleting kind clusters; PVCs on kind nodes disappear with those nodes. No cloud resources or external model calls are authorized.

## 19. Troubleshooting order

Diagnose from the lowest failing layer upward:

1. Confirm Git commit, image digest, environment ID, Kubernetes context, namespace, and local Docker context.
2. Check node readiness, resource pressure, storage, and DNS.
3. Check operator/controller health and custom-resource status.
4. Check Kafka, PostgreSQL, Keycloak, and observability dependencies.
5. Check workload startup/readiness and recent Kubernetes events.
6. Check NetworkPolicies, RBAC, Service endpoints, and credentials.
7. Check Kafka keys, consumer groups, lag, schema version, and DLQ/late topics.
8. Check trace propagation, correlated logs, and bounded metrics.
9. Reproduce with the smallest contract fixture in the standalone profile.
10. Add the reproduction as a regression test before merging the fix.

Do not respond to an uncertain command effect by blindly retrying. Query durable command/result state and apply the documented idempotency and reconciliation rules.

## 20. Scope reduction order

If delivery falls behind, reduce work in this order:

1. Additional UI polish beyond required accessible states
2. Additional dashboards beyond the acceptance views
3. Extra failure variations after each required category has evidence
4. External model-provider integration, while retaining the deterministic mock and typed agent evaluation
5. Extra concurrent review workloads; stage exercises on local kind while preserving required evidence

Do not cut the closed-loop business scenario, Kafka/Flink semantics, duplicate-safe control, leader fencing, checkpoint recovery, Kubernetes runtime, core observability, contract tests, or security boundaries.

## 21. Definition of ready for implementation

Application service implementation begins only when (contract authoring is complete; remaining foundation gates still apply):

- Blocking ADRs and the version matrix are accepted.
- Contract schemas and golden examples agree with the prose contracts.
- Each member has a standalone input, expected output, owned directories, and test boundary.
- The local resource profiles are selected; cloud stays deferred.
- CI can validate contracts and repository hygiene.
- No real credentials or customer data are present.

## 22. Definition of done

The project is complete when a clean checkout can reproduce a Kubernetes deployment in which:

1. Deterministic devices publish valid telemetry under measured load.
2. Kafka and Flink calculate correct event-time transformer loads under delay, duplicate delivery, restart, and replay.
3. A critical load produces one explainable, fenced, duplicate-safe throttle workflow.
4. Devices acknowledge commands and measured demand falls before safe recovery and restore.
5. Projections, REST/SSE, and the UI show consistent current and historical state.
6. Kubernetes discovery, readiness, reconciliation, replicas, persistence, access control, and failure behavior are demonstrated.
7. Metrics, traces, and correlated logs explain one injected incident and support the proposed SLO evidence.
8. The read-only investigator cites valid evidence, denies write attempts, labels uncertainty, and can fail without affecting grid control.
9. Each member demonstrates an owned distributed component, independent tests, Kubernetes resources, instrumentation, and failure result.
10. The release, reports, known limitations, cleanup procedure, and final-demo sequence are documented and reproducible.

## 23. Command verification policy

Setup commands may be documented before execution when explicitly labeled unverified. Do not describe scripts/manifests or service commands as working before they exist. For each verified command, record exact text, machine/prerequisite versions, expected/actual results, and limitations. A clean-checkout rerun on this computer supplies local reproducibility; Member_1 through Member_4 labels remain ownership boundaries and do not imply four operators.

This policy prevents the final runbook from accumulating plausible-looking commands that have never worked.

