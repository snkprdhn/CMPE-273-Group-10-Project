# Grid Incident Investigator — Member_4

The deterministic read-only implementation lives in `copilot/`. It uses the
seven frozen contracts, real owned projection reads, bounded observability
adapters, Operator authorization, budgets, citations, safe audit and UI evidence.
See [ADR-036](../../docs/decisions/ADR-036-grid-incident-investigator.md).

From the repository root, prepare an independent environment with existing pins:

```sh
.venv/bin/python -m venv .tools/copilot-venv
PIP_CACHE_DIR=.tools/pip-cache .tools/copilot-venv/bin/python -m pip install --only-binary=:all: --require-hashes -r copilot/requirements.txt
make copilot-test
```

With the existing [identity](../../identity/README.md) and
[projection/API](../../member4/README.md) running, start `make copilot` in another
terminal. It binds `127.0.0.1:18087`, reads the projection API on port 18084 and
validates the existing local Keycloak tokens. Start the existing dashboard on
18086, log in as Operator, select a transformer, and choose **Investigate**.
The drawer displays progress, AI-generated MOCK output, unknowns and expandable
citation evidence. Viewer requests fail authorization. **Close investigator**
closes the drawer; it does not cancel the server job. The API also exposes cancel
and reconnectable investigation SSE. Provider failure never changes API readiness.

Seeded real-projection acceptance (disposable PostgreSQL must already be up):

```sh
MEMBER4_TEST_DSN=postgresql://projection:projection_disposable@127.0.0.1:15432/local_dev_projection .tools/member4-venv/bin/python -m pytest -q tests/member4/test_investigator.py
```

This creates/drops an isolated schema, loads recorded events, executes real tools
and the mock provider, checks actual command evidence and citations, and verifies
projection readiness. Old recorded windows correctly produce PARTIAL, not a fresh
physical diagnosis. Standalone tests need no database/model provider.

Runtime configuration: `COPILOT_PROJECTION_URL`, optional loopback
`COPILOT_PROMETHEUS_URL` and `COPILOT_TEMPO_URL`, `COPILOT_AUDIT_PATH`, and the UI's
`COPILOT_URL`. URLs are deployment configuration, never model arguments. Audit
retention is 24 hours on access; default file is `.tools/copilot/audit.sqlite`.
Inspect it with the venv's standard-library `sqlite3` (tables `investigations`,
`denials`, `events`). It contains safe hashes/scope/results, no bearer or raw prompt.
Do not publish local evidence. Delete this independent file only after stopping
the service if an explicit investigation-history reset is wanted.

`make kind-deploy` now builds/loads the separate pinned local Copilot image and
includes its Deployment, Service, dedicated PVC, probes and NetworkPolicies.
No database/Kafka/Kubernetes credential or external-provider Secret is supplied.
The kind manifests have been rendered and the container built; live rollout and
full control-loop/provider-outage acceptance are **not yet verified**.

The adapter returns explicit gaps where raw backends cannot satisfy required
v1 fields, including full per-component health, true checkpoint age and correlation
search without an authorized trace ID. See ADR-036 for exact limits. External
providers are disabled and unimplemented; no application API entitlement is
inferred from a Codex subscription. Do not use the Investigator to select chargers,
change policy, calculate authoritative load, or issue controls.
