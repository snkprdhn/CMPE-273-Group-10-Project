# Master Implementation Guide

**Status:** Implementation plan only. Application code, deployment manifests, automation, and executable project commands have not been created or verified.

## 1. Purpose

This is the end-to-end delivery guide for the Smart Grid / EV Fleet Energy Balancing Platform. It turns the approved design into an ordered implementation plan for four members working in parallel. It covers prerequisites, contract freeze, local development, Kubernetes packaging, a shared AWS EKS integration environment, testing, security, observability, failure experiments, release evidence, and final demonstration.

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

The project uses three environments. All three run the same contracts and application images; only scale, dependencies, and exposure differ.

| Environment | Purpose | Users | Required characteristics |
|---|---|---|---|
| Standalone local profile | Daily implementation and component tests | One member | Owned service, fixtures/mocks, and minimum real dependencies |
| Full local Kubernetes | Reproducible offline integration and grading fallback | Whole team | `kind`, all core services, bounded scale, no cloud dependency |
| Shared AWS EKS | Continuous team integration, realistic collaboration, and optional final demo | Whole team and CI | Namespace isolation, shared platform, controlled access, budgets, and repeatable teardown |

The shared EKS cluster is recommended for integration but must never become a prerequisite for unit or component work. The full local Kubernetes profile remains the reproducible fallback if AWS, campus networking, credentials, or quotas are unavailable.

### 4.1 Shared-cluster namespace model

```text
smart-grid EKS cluster
├── platform                 # Operators and shared platform services
├── observability            # OTel, Prometheus, Grafana, Tempo
├── member-1-dev             # Member_1 integration sandbox
├── member-2-dev             # Member_2 integration sandbox
├── member-3-dev             # Member_3 integration sandbox
├── member-4-dev             # Member_4 integration sandbox
└── integration              # Protected full-system release candidate
```

Member namespaces are disposable sandboxes. The `integration` namespace is deployed from the protected `main` branch through CI and must not be edited manually except during a documented incident exercise. Shared Kafka or PostgreSQL installations must use environment-specific topic prefixes, consumer groups, databases/schemas, roles, and fixture IDs so one sandbox cannot corrupt another.

## 5. Prerequisites and downloads

### 5.1 Developer machine

Recommended starting capacity for the full local profile:

- 8 CPU cores or equivalent
- 16 GB RAM, with at least 8 GB available to the container runtime
- 40 GB free disk space for images, volumes, checkpoints, and test evidence
- Reliable broadband for the first image and dependency download

A lower-capacity machine can run a member's standalone profile. The three-broker review profile may require the shared EKS cluster or a higher-capacity laptop. Actual resource requests and limits must be measured and recorded before feature freeze.

### 5.2 Required tools

Exact versions are a blocking architecture decision. Before implementation, the team must create one reviewed version matrix and pin every item; `latest` tags are prohibited.

| Tool | Why it is needed | Installation source | Preflight check |
|---|---|---|---|
| Git | Source control and collaboration | [Git downloads](https://git-scm.com/downloads) | `git --version` |
| Docker Desktop, Docker Engine, or compatible runtime | Builds images and runs Testcontainers/`kind` | [Docker documentation](https://docs.docker.com/get-docker/) | `docker version` |
| `kubectl` | Kubernetes administration | [Kubernetes tool installation](https://kubernetes.io/docs/tasks/tools/) | `kubectl version --client` |
| `kind` | Reproducible local Kubernetes | [kind quick start](https://kind.sigs.k8s.io/docs/user/quick-start/) | `kind version` |
| Helm | Installs pinned third-party charts | [Helm installation](https://helm.sh/docs/intro/install/) | `helm version` |
| Kustomize support | Builds project-owned overlays | Pin standalone Kustomize or the supported `kubectl kustomize` version | `kubectl kustomize --help` |
| Python 3.12 | Simulators and planned Python services | [Python downloads](https://www.python.org/downloads/) | `python3 --version` |
| Java/JDK | Flink tooling/runtime if required by the selected implementation | Selected JDK vendor after ADR approval | `java -version` |
| Node.js LTS and package manager | React/TypeScript dashboard | [Node.js downloads](https://nodejs.org/en/download) | `node --version` |
| `jq` | Scripted JSON inspection | Operating-system package manager | `jq --version` |
| `grpcurl` | Registry contract and diagnostic calls | [grpcurl releases](https://github.com/fullstorydev/grpcurl/releases) | `grpcurl -version` |
| `k6` | API/SSE load tests | [k6 installation](https://grafana.com/docs/k6/latest/set-up/install-k6/) | `k6 version` |

### 5.3 Shared AWS environment tools

Only members administering or diagnosing EKS require these locally; CI uses a short-lived federated identity rather than stored access keys.

| Tool/account | Purpose | Verification |
|---|---|---|
| AWS account with student credits or an agreed budget | Hosts the shared cluster | Billing owner confirms budget and region |
| AWS CLI v2 | Identity, EKS, registry, and diagnostics | `aws --version` and `aws sts get-caller-identity` |
| `eksctl` | Declarative EKS lifecycle | `eksctl version` |
| AWS IAM identity for each member | Auditable individual access | EKS access entry exists for the principal |
| GitHub repository secrets or OIDC trust | CI image publishing and deployment | Test workflow assumes role without a long-lived key |

The team must not share one AWS access key. Use individual IAM identities and EKS access entries. Human access, CI access, and billing administration are separate responsibilities.

### 5.4 External accounts

- GitHub accounts for all members and branch protection on `main`
- One model-provider account only if the external Copilot adapter is enabled
- No real utility, customer, vehicle, or location data
- No model-provider credential in browser code, Git, container images, fixtures, screenshots, or logs

## 6. Decisions that must close before coding

The following decisions block reproducible implementation:

1. PyFlink or Java for the aggregation job.
2. Exact compatible versions of Kubernetes, `kind`, Strimzi, Kafka, Flink Operator, Flink, Debezium, PostgreSQL, Keycloak, OpenTelemetry, Prometheus, Grafana, and Tempo.
3. Kafka partition count, replication factor, retention, and developer/review resource limits.
4. JSON Schema compatibility rule and validation tooling.
5. Default action thresholds, hysteresis, cooldown, recovery, and command timeout.
6. Keycloak realm, clients, roles, seeded users, and local redirect URIs.
7. Container registry: GitHub Container Registry or Amazon ECR.
8. EKS region, cluster lifecycle owner, cost ceiling, node shape, and shutdown policy.
9. External model provider, model identifier, per-investigation budget, and credential owner.
10. Repository license, dependency attribution, image scanning, and software bill-of-materials process.

Record each accepted choice as an ADR under `docs/decisions/`. The version matrix and deployment overlays must reference the ADRs rather than silently choosing different versions.

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
│   └── eks/                       # Declarative EKS cluster/access configuration
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
- Prefer GitHub-to-AWS OIDC for CI; do not store long-lived AWS keys in repository secrets.
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
| EKS cluster, access, budget, and base namespaces | One rotating environment administrator | One other member |
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
7. Agree on the local machine profile and EKS budget/region.

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
6. Create the scenario runner and reusable k6/Toxiproxy orchestration interface.
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
3. Install operators: Strimzi, Flink Kubernetes Operator, OpenTelemetry Operator, and Prometheus Operator/chart.
4. Install PostgreSQL, Keycloak, Prometheus, Grafana, Tempo, and the OTel Collector.
5. Create Kafka, topics/users, Kafka Connect, databases/roles, and Keycloak realm/client configuration.
6. Apply project bases for Registry, simulators, aggregation, Action, projection/API, dashboard, and Copilot.
7. Load deterministic fixtures and run the normal-state smoke test.
8. Run the overload-to-throttle-to-recovery scenario.
9. Verify probes, DNS, persistence, security boundaries, dashboards, logs, traces, and cleanup.

The exact commands must be added to `infra/README.md` only after a second member reproduces them. The intended eventual command surface should be small: prerequisites check, cluster create, platform install, application deploy, smoke test, evidence capture, and cluster delete.

**Exit gate:** A new teammate can reproduce the complete local system from a clean checkout using the verified runbook.

### Phase 5 — Create the shared EKS integration environment (Weeks 5–7)

This phase may start once the local Kubernetes base is healthy. Do not debug first-time application failures in EKS.

1. Confirm AWS budget, region, quota, lifecycle owner, tags, and teardown date.
2. Create a reviewed declarative `eksctl` cluster configuration rather than relying on CLI defaults.
3. Create the managed node group sized for the integration profile and persistent workloads.
4. Configure individual EKS access entries and namespace-scoped Kubernetes RBAC.
5. Configure CI federation, container registry access, image scanning, and immutable tags/digests.
6. Create `platform`, `observability`, member sandbox, and protected `integration` namespaces.
7. Apply quotas, limit ranges, default-deny policies, storage classes, and restricted external exposure.
8. Install the same pinned operators and charts used locally with EKS-specific values only where required.
9. Deploy each member's already-passing slice to its sandbox.
10. Promote immutable image digests to `integration` after pairwise gates pass.
11. Run smoke, authorization, persistence, observability, and failure tests.
12. Record actual resource use and adjust the node plan without weakening required replicas or correctness.

Planned lifecycle operations after the reviewed configuration exists:

```text
Create/update cluster from infra/eks/cluster.yaml
Create access entries from reviewed IAM principal mappings
Update kubeconfig and verify the expected AWS identity/context
Install pinned platform releases
Deploy Kustomize integration overlay by immutable image digest
Capture evidence and backup/export required state
Delete the cluster through the same declared lifecycle tool when no longer needed
Verify CloudFormation, load balancers, volumes, snapshots, and registry retention after teardown
```

Do not copy unreviewed example commands into production accounts. Follow the current [Amazon EKS `eksctl` guide](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-eksctl.html) and [EKS access-entry documentation](https://docs.aws.amazon.com/eks/latest/userguide/access-entries.html) when the cluster configuration is implemented.

**Exit gate:** All members can inspect their namespace, CI can deploy `integration`, unauthorized cross-namespace operations fail, and the local fallback still passes.

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
8. Rehearse with the external model disabled and with the shared AWS cluster unavailable.

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

## 12. Shared EKS collaboration rules

1. Every member retains a working local profile.
2. Member sandboxes may deploy branch images; `integration` accepts only reviewed main-branch digests.
3. Use explicit Kubernetes contexts and show the current context before any deployment or cleanup operation.
4. Namespace-scoped permissions are the default; cluster-admin is limited to the environment administrator and backup administrator.
5. Platform changes require a maintenance notice in the team channel and a rollback plan.
6. Shared topics and databases require environment prefixes and least-privilege credentials.
7. Generated load tests run in scheduled windows so they do not invalidate another member's results.
8. Store evidence with environment, commit, image digest, start/end time, scenario seed, and configuration.
9. Set an AWS budget alert before cluster creation and review costs at least weekly.
10. Tear down unused load balancers, volumes, snapshots, and the cluster after the course unless retention is explicitly approved.

## 13. Testing strategy and execution order

Testing progresses from cheapest and most isolated to most expensive and integrated.

| Level | Runs where | Blocks | Primary evidence |
|---|---|---|---|
| Static/lint/type | Local and every PR | Merge | Clean tool output |
| Unit | Local and every PR | Merge | Domain and edge-case assertions |
| Contract | Local and every PR | Merge | Schema/API compatibility and golden fixtures |
| Component | Standalone local profile | Merge for owned slice | Real dependency plus fake boundaries |
| Pairwise | Local or member namespaces | Integration promotion | Producer/consumer evidence report |
| End-to-end | Local full cluster and EKS `integration` | Release candidate | Closed-loop business outcome |
| Security | Component and integration | Release candidate | Positive/negative authorization and isolation |
| Failure | Local/EKS scheduled runs | Final acceptance | Recovery time, correctness, and evidence packet |
| Load | Stable integration environment | Final performance claims | Raw result, machine/cluster profile, percentiles |
| Agent evaluation | Mock on every PR; external provider scheduled | Agent release | Tool choice, citations, denial, cost, and failure isolation |

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
3. Publish images to the chosen registry.
4. Deploy the reviewed digests to the protected `integration` namespace.
5. Run smoke and pairwise regression tests.
6. Roll back the deployment if health or smoke checks fail.
7. Store compact test/evidence artifacts with retention appropriate for the course.

### 14.3 Scheduled pipeline

- Full end-to-end suite
- Failure-injection subset
- Dependency and container scan
- Load baseline at controlled times
- Agent external-provider evaluation within the agreed budget
- Backup/export and restore verification

CI must not expose secrets to untrusted pull requests or allow a forked workflow to deploy to EKS.

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
- Failed EKS changes must not be repaired through undocumented console edits; reconcile the declared configuration.

## 18. AWS cost and lifecycle controls

Before creating EKS:

1. Confirm credits, billing owner, cost ceiling, and teardown date.
2. Create budget notifications to at least two members.
3. Tag resources with project, course, environment, owner, and expiration date.
4. Choose the smallest measured node group that supports the integration profile.
5. Avoid unnecessary public load balancers, NAT traffic, snapshots, and duplicate clusters.
6. Use one shared cluster—not one cluster per member.
7. Scale simulators down outside scheduled tests.
8. Review volumes, load balancers, snapshots, elastic IPs, and container retention weekly.

The final cleanup is complete only when the cluster lifecycle tool reports deletion and the team verifies that dependent AWS resources are no longer billing. Preserve documentation and compact evidence outside the cluster before teardown.

## 19. Troubleshooting order

Diagnose from the lowest failing layer upward:

1. Confirm Git commit, image digest, environment ID, Kubernetes context, namespace, and AWS identity.
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
5. Shared EKS as a final-demo dependency, while retaining full local Kubernetes

Do not cut the closed-loop business scenario, Kafka/Flink semantics, duplicate-safe control, leader fencing, checkpoint recovery, Kubernetes runtime, core observability, contract tests, or security boundaries.

## 21. Definition of ready for implementation

Implementation begins only when:

- Blocking ADRs and the version matrix are accepted.
- Contract schemas and golden examples agree with the prose contracts.
- Each member has a standalone input, expected output, owned directories, and test boundary.
- The team agrees on local resource expectations and the shared EKS budget decision.
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

Project-specific commands are intentionally not listed as executable commands in this version because their scripts, manifests, and pinned versions do not yet exist. As implementation progresses:

1. The component owner adds the smallest useful command to the component README or shared task runner.
2. A different member runs it from a clean checkout.
3. The reviewer records the tested operating system, prerequisite versions, and result.
4. Only then is the command copied into this master guide as verified.

This policy prevents the final runbook from accumulating plausible-looking commands that have never worked.

