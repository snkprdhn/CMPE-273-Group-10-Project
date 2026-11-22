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
