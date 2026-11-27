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

# Devices standalone runtime; independent of the other applications.
DEVICES_PYTHON ?= .tools/devices-venv/bin/python
DEVICES_COMPOSE = docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/devices.yaml
.PHONY: devices-test devices-live devices-build devices-up devices-down
devices-test: bindings
	$(DEVICES_PYTHON) -m pytest -q tests/devices/test_edge.py tests/devices/test_operations.py tests/devices/test_fleet.py

devices-live: bindings
	DEVICES_TEST_DSN=postgresql://registry:registry_disposable@127.0.0.1:15432/local_dev_registry DEVICES_TEST_KAFKA=127.0.0.1:19092 $(DEVICES_PYTHON) -m pytest -q tests/devices

devices-build: bindings
	$(DEVICES_COMPOSE) build registry simulator

devices-up: bindings
	$(DEVICES_COMPOSE) up -d --build --wait registry simulator

devices-down:
	$(DEVICES_COMPOSE) stop registry simulator

# Standalone aggregation. No simulator, Registry or PostgreSQL required.
AGGREGATION_COMPOSE = docker compose -p smart-grid-local-dev -f infra/compose/compose.yaml -f infra/compose/aggregation.yaml
.PHONY: aggregation-test aggregation-build aggregation-live aggregation-down
aggregation-test:
	.venv/bin/python -m pytest -q tests/aggregation/test_aggregation.py tests/aggregation/test_keyed.py

aggregation-build:
	$(AGGREGATION_COMPOSE) build jobmanager

aggregation-live:
	.venv/bin/python tools/aggregation_live.py

aggregation-down:
	$(AGGREGATION_COMPOSE) stop taskmanager jobmanager

# Action domain; real PostgreSQL with fixtures and a fake Registry only.
ACTION_PYTHON ?= .tools/action-venv/bin/python
.PHONY: action-test
action-test:
	$(ACTION_PYTHON) -m pytest -q tests/action

# Projection/operations backend; owned PostgreSQL plus contract mocks only.
PROJECTION_PYTHON ?= .tools/projection-venv/bin/python
.PHONY: projection-test
projection-test:
	$(PROJECTION_PYTHON) -m pytest -q tests/projection

# Python operator console; fixture API is separate and disposable.
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
	.tools/projection-venv/bin/python -m identity.bootstrap

identity-up: identity-bootstrap
	docker compose -f infra/compose/compose.yaml -f infra/identity/compose.yaml up -d --build --no-deps keycloak

identity-test:
	.tools/projection-venv/bin/python -m pytest -q tests/identity/test_auth.py tests/identity/test_browser.py tests/identity/test_demo.py

.PHONY: identity-api-test identity-live-test
identity-api-test:
	PROJECTION_TEST_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection .tools/projection-venv/bin/python -m pytest -q tests/projection/test_api.py tests/projection/test_oidc.py tests/projection/test_review.py tests/projection/test_projection.py tests/projection/test_dashboard_boundary.py

identity-live-test:
	IDENTITY_LIVE=1 PLAYWRIGHT_BROWSERS_PATH=.tools/playwright .tools/dashboard-test-venv/bin/python -m pytest -q tests/identity/test_live.py

# Dedicated local Docker Desktop kind profile. Use PROFILE=review after dev measurement.
PROFILE ?= dev
.PHONY: kind-create kind-install kind-deploy kind-smoke kind-policy kind-inspect kind-measure kind-open-ui kind-stop
kind-create kind-install kind-deploy kind-smoke kind-policy kind-inspect kind-measure kind-open-ui kind-stop:
	.venv/bin/python -m tools.kind_local $(patsubst kind-%,%,$@) --profile "$(PROFILE)" $(if $(KIND_NAME),--name "$(KIND_NAME)")

.PHONY: kind-reset
kind-reset:
	.venv/bin/python -m tools.kind_local reset --profile "$(PROFILE)" --name "$(KIND_NAME)" --confirm "$(CONFIRM)"

# Local observability; explicit kind context, no cloud dependencies.
OBSERVABILITY_CONTEXT ?= kind-smart-grid-kind-dev
OBSERVABILITY_KUBECONFIG ?= .tools/kind-dev.kubeconfig
.PHONY: observability-render observability-install observability-inspect observability-test
observability-render observability-install observability-inspect:
	.venv/bin/python -m tools.observability $(patsubst observability-%,%,$@) --context "$(OBSERVABILITY_CONTEXT)" --kubeconfig "$(OBSERVABILITY_KUBECONFIG)"

observability-test:
	.tools/observability-venv/bin/python -m pytest -q tests/observability

# Independent deterministic read-only investigator.
COPILOT_PYTHON ?= .tools/copilot-venv/bin/python
.PHONY: copilot copilot-test
copilot:
	$(COPILOT_PYTHON) -m copilot
copilot-test:
	$(COPILOT_PYTHON) -m pytest -q tests/copilot

.PHONY: delivery-images delivery-render delivery-scan local-acceptance
delivery-images:
	.venv/bin/python -m tools.delivery_images

delivery-render:
	$(CONTRACT_PYTHON) -m tools.delivery_checks render

delivery-scan:
	.venv/bin/python -m tools.delivery_checks scan $(if $(IMAGE_INVENTORY),--inventory "$(IMAGE_INVENTORY)")

local-acceptance:
	.venv/bin/python -m tools.delivery --profile "$(PROFILE)" --name "$(KIND_NAME)"
