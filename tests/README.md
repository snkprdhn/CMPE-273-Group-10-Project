# Tests

Every component has an isolated test environment (`.tools/<component>-venv`, or `.venv` for aggregation) and runs without the others. Database and Kafka tests skip with an explicit reason unless their DSN is set; `make infra-up` starts the disposable local PostgreSQL and Kafka they use.

| Area | Command | What it covers |
|---|---|---|
| Contracts | `make contracts` (add `BASE_REF=HEAD` to compare against Git) | Schemas, OpenAPI, Protobuf, valid and invalid fixtures, golden stream, control and agent cases, compatibility mutations. See [schemas](../schemas/README.md). |
| Devices | `make devices-test`; `make devices-live` with `DEVICES_TEST_DSN` and `DEVICES_TEST_KAFKA` | Simulator, fleet model, fenced edge gateway, Registry. See [devices](../devices/README.md). |
| Aggregation | `make aggregation-test`; `make aggregation-live` (real Kafka and Flink TaskManager kill) | Windows, lateness, duplicates, keyed bounded state, checkpoint recovery. See [aggregation](../aggregation/README.md). |
| Action | `ACTION_TEST_DSN=... make action-test` | Policy, saga, inbox and outbox, fencing, Lease, Debezium delivery. See [action](../action/README.md). |
| Projection and API | `PROJECTION_TEST_DSN=... PROJECTION_TEST_KAFKA=... make projection-test` | Journal, views, REST, SSE, OIDC, review. See [projection](../projection/README.md). |
| Identity | `make identity-test` | Keycloak realm, tokens, browser sign-in. See [identity](../identity/README.md). |
| Dashboard | `make dashboard-test` | NiceGUI pages in Chromium (Playwright), control room, client. See [dashboard](../dashboard/README.md). |
| Investigator | `make copilot-test` | Bounded read-only tools, citations, authorization. |
| Observability | `make observability-test` | Telemetry, dashboards, Flink tracing, evidence packet. |
| Tooling | `.venv/bin/python -m pytest -q tests/test_*.py` | Kind rendering, delivery, load tools, documentation links. |

Live and cluster acceptance (kind) is `make local-acceptance`. The hosted AKS checks are `tools.demo_check` and the runbook in [infra/aks](../infra/aks/AGENT_RUNBOOK.md). Reference replay of the contract fixtures is an offline specification. It does not establish Kafka recovery, persistent fencing or workload performance, which the live suites measure.
