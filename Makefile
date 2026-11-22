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
