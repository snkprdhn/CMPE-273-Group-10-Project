CONTRACT_PYTHON ?= .tools/contracts-venv/bin/python

.PHONY: contracts bindings
contracts:
	$(CONTRACT_PYTHON) tools/contracts.py $(if $(BASE_REF),--base-ref $(BASE_REF))

bindings:
	$(CONTRACT_PYTHON) tools/contracts.py --generate

# Lightweight local dependencies only; never touches the kind cluster.
.PHONY: infra-up infra-health infra-down infra-reset infra-smoke infra-verify infra-faults-up infra-faults-down infra-fault-smoke
infra-up infra-health infra-down infra-smoke infra-verify infra-faults-up infra-faults-down infra-fault-smoke:
	.venv/bin/python tools/local_infra.py $(patsubst infra-%,%,$@)

infra-reset:
	.venv/bin/python tools/local_infra.py reset --confirm "$(CONFIRM)"

# Member_1 standalone runtime; independent of Member_2/3/4 applications.
MEMBER1_PYTHON ?= .tools/member1-venv/bin/python
MEMBER1_COMPOSE = docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/member1.yaml
.PHONY: member1-test member1-live member1-build member1-up member1-down
member1-test: bindings
	$(MEMBER1_PYTHON) -m pytest -q tests/member1/test_edge.py tests/member1/test_operations.py

member1-live: bindings
	MEMBER1_TEST_DSN=postgresql://registry:registry_disposable@127.0.0.1:15432/local_dev_registry MEMBER1_TEST_KAFKA=127.0.0.1:19092 $(MEMBER1_PYTHON) -m pytest -q tests/member1

member1-build: bindings
	$(MEMBER1_COMPOSE) build registry simulator

member1-up: bindings
	$(MEMBER1_COMPOSE) up -d --build --wait registry simulator

member1-down:
	$(MEMBER1_COMPOSE) stop registry simulator

# Member_2 standalone aggregation. No simulator, Registry or PostgreSQL required.
MEMBER2_COMPOSE = docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/member2.yaml
.PHONY: member2-test member2-build member2-live member2-down
member2-test:
	.venv/bin/python -m pytest -q tests/member2/test_aggregation.py

member2-build:
	$(MEMBER2_COMPOSE) build jobmanager

member2-live:
	.venv/bin/python tools/member2_live.py

member2-down:
	$(MEMBER2_COMPOSE) stop taskmanager jobmanager

# Member_3 Action domain; real PostgreSQL with fixtures and a fake Registry only.
MEMBER3_PYTHON ?= .tools/member3-venv/bin/python
.PHONY: member3-test
member3-test:
	$(MEMBER3_PYTHON) -m pytest -q tests/member3

# Member_4 operations backend; owned PostgreSQL plus contract mocks only.
MEMBER4_PYTHON ?= .tools/member4-venv/bin/python
.PHONY: member4-test
member4-test:
	$(MEMBER4_PYTHON) -m pytest -q tests/member4

# Member_4 Python operator console; fixture API is separate and disposable.
DASHBOARD_PYTHON ?= .tools/dashboard-venv/bin/python
DASHBOARD_TEST_PYTHON ?= .tools/dashboard-test-venv/bin/python
.PHONY: dashboard dashboard-fixtures dashboard-demo dashboard-test
dashboard:
	$(DASHBOARD_PYTHON) -m dashboard

dashboard-fixtures:
	$(DASHBOARD_PYTHON) -m dashboard.fixture_api

dashboard-demo:
	$(DASHBOARD_PYTHON) -m dashboard --fixture

dashboard-test:
	PLAYWRIGHT_BROWSERS_PATH=.tools/playwright $(DASHBOARD_TEST_PYTHON) -m pytest -q tests/dashboard

.PHONY: identity-bootstrap identity-up identity-test
identity-bootstrap:
	.tools/member4-venv/bin/python -m identity.bootstrap

identity-up: identity-bootstrap
	docker compose -f infra/compose/compose.yaml -f infra/identity/compose.yaml up -d --build --no-deps keycloak

identity-test:
	.tools/member4-venv/bin/python -m pytest -q tests/identity/test_auth.py tests/identity/test_browser.py

.PHONY: identity-api-test identity-live-test
identity-api-test:
	MEMBER4_TEST_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection .tools/member4-venv/bin/python -m pytest -q tests/member4/test_api.py tests/member4/test_oidc.py tests/member4/test_review.py tests/member4/test_projection.py tests/member4/test_dashboard_boundary.py

identity-live-test:
	IDENTITY_LIVE=1 PLAYWRIGHT_BROWSERS_PATH=.tools/playwright .tools/dashboard-test-venv/bin/python -m pytest -q tests/identity/test_live.py

# Dedicated local Docker Desktop kind profile. Use PROFILE=review after dev measurement.
PROFILE ?= dev
.PHONY: kind-create kind-install kind-deploy kind-smoke kind-policy kind-inspect kind-measure kind-open-ui kind-stop
kind-create kind-install kind-deploy kind-smoke kind-policy kind-inspect kind-measure kind-open-ui kind-stop:
	.venv/bin/python -m tools.kind_local $(patsubst kind-%,%,$@) --profile "$(PROFILE)" $(if $(KIND_NAME),--name "$(KIND_NAME)")

# Member_1 local observability; explicit kind context, no cloud dependencies.
OBSERVABILITY_CONTEXT ?= kind-smart-grid-kind-dev
OBSERVABILITY_KUBECONFIG ?= .tools/kind-dev.kubeconfig
.PHONY: observability-render observability-install observability-inspect observability-test
observability-render observability-install observability-inspect:
	.venv/bin/python -m tools.observability $(patsubst observability-%,%,$@) --context "$(OBSERVABILITY_CONTEXT)" --kubeconfig "$(OBSERVABILITY_KUBECONFIG)"

observability-test:
	.tools/observability-venv/bin/python -m pytest -q tests/observability

# Member_4 independent deterministic read-only investigator.
COPILOT_PYTHON ?= .tools/copilot-venv/bin/python
.PHONY: copilot copilot-test
copilot:
	$(COPILOT_PYTHON) -m copilot
copilot-test:
	$(COPILOT_PYTHON) -m pytest -q tests/copilot
