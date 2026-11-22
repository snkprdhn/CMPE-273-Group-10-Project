# Implementation status

Rechecked **2026-11-21 19:58 PST (America/Los_Angeles)** after the user-authorized Docker Desktop/virtual-environment setup. One computer implements all workstreams; Member_1 through Member_4 ownership is preserved.

## Completed milestones

| Milestone | Verified result | Commands/evidence |
|---|---|---|
| Repository preparation and decisions | Python application/containerized infrastructure boundary, accepted matrix, ADRs, prerequisites, root AGENTS.md; no application scaffold | [ADRs](../decisions/README.md), [matrix](VERSION_MATRIX.md), documentation checks |
| Local prerequisites and empty development environment | Docker Desktop reused/started/configured, built-in Kubernetes disabled; isolated Python 3.12.9 venv and locked preparation packages; verified native tools; empty local kind 1.34.0 cluster | Exact commands/results/digests in [PREPARATION_VERIFICATION.md](PREPARATION_VERIFICATION.md); commands below |

## Current verified checklist

- [x] Existing Git 2.54.0 and Docker Desktop 4.99.0 verified. Engine 29.9.3 / CLI 29.8.0 API 1.56 communicate on `desktop-linux`, Linux ARM64.
- [x] Desktop development settings: 4 CPUs, 6144 MiB configured RAM; engine reports 6,210,510,848 usable guest bytes. Built-in Kubernetes **disabled**. Colima stays stopped.
- [x] Managed CPython **3.12.9** downloaded to ignored `.tools/python`; project `.venv` has isolated prefix and ARM64 interpreter. No global Python packages installed.
- [x] kind **0.30.0**, kubectl **1.34.0**, Helm **3.19.0** installed into `.venv/bin` from official checksum-verified artifacts; activation selects these versions. Bundled Kustomize **5.7.1** verified.
- [x] pytest **8.3.5**, jsonschema **4.23.0**, and eight transitive distributions installed only in `.venv`. [requirements-dev.txt](../../requirements-dev.txt) pins all ten versions/wheel hashes; hash-checked download and `pip check` pass.
- [x] Network-isolated pinned Python 3.12.9 ARM64 Docker diagnostic container runs and is automatically removed. Locally tagged `smart-grid/prerequisite-smoke:3.12.9` loads into kind successfully; node image-store inspection confirms it is present.
- [x] Empty **smart-grid-dev** kind cluster created from pinned 1.34.0 image. `kind-smart-grid-dev` API `/readyz` returns `ok`; one control-plane node Ready; all nine system pods Running with zero restarts at verification.
- [x] Default `standard` StorageClass/local-path provisioner registered. This does **not** verify PVC read/write or recovery.
- [x] Ten local documentation/environment tests pass; `git diff --check` and venv `pip check` pass. `.venv` and `.tools` are ignored.
- [ ] PVC provision/read/write/recovery; workload DNS and enforced NetworkPolicy allow-deny tests. Current kind cluster uses default networking.
- [ ] Infrastructure chart/operator installs, project overlays, application/component environments, PyFlink native ARM64 source build/import/checkpoint tests, and closed-loop behavior.
- [ ] Larger 8-CPU/10-GiB review-profile resource fit and all performance claims.

## Latest prerequisite recheck

All 15 read-only tool/runtime/capacity probes exited 0. The cached, network-isolated Python diagnostic container and the existing kind image-store check also passed. Installed native payloads match the previously checksum-verified releases; all ten preparation distributions match the lock, reside inside the venv, and have matching cached wheel hashes (**14 offline integrity checks passed**). System site-packages are disabled. Ten tests, `pip check`, and `git diff --check` pass. No version change or repair was needed in the tested prerequisite layer.

Current capacity snapshot: **258 GiB available disk**, **32% macOS free-memory metric**, 16 GiB total RAM. These observations are not review-profile or performance evidence. Application dependency metadata was not re-fetched, and no pending application/infrastructure gate was marked complete. [Exact rerun commands/results](PREPARATION_VERIFICATION.md#prerequisite-recheck--2026-11-21-1958-pst-latest).

## Current milestone

**Requested local environment setup is complete at the prerequisite/empty-cluster level.** Docker Desktop and `smart-grid-dev` are left running for local inspection. No application services, operator stacks, cloud resources, or paid API calls were created. The user subsequently authorized committing and pushing the verified preparation changes; publication state is recorded by Git history and the remote branch. Existing global tools, Colima profile, Docker data, and earlier uncommitted repository work were preserved.

Use a virtual environment for all Python work. Start each terminal with:

```sh
source .venv/bin/activate
export PIP_REQUIRE_VIRTUALENV=true
export PIP_CACHE_DIR="$PWD/.tools/pip-cache"
python -m pip check
python -m pytest -q tests/test_preparation_docs.py tests/test_preparation_environment.py
kubectl --context kind-smart-grid-dev get nodes
```

Docker/Kubernetes commands may need approved host access from Codex's sandbox; a socket-permission denial is distinct from a runtime failure. Full activation/recreation instructions are in [LOCAL_PREREQUISITES.md](LOCAL_PREREQUISITES.md).

## Decisions and resolved contradictions

- [ADR-006](../decisions/ADR-006-local-kind.md): **Docker Desktop supersedes Colima** by explicit user choice. kind remains the project target, Desktop Kubernetes stays off, cloud is deferred, and deterministic model mocks are required.
- [ADR-024](../decisions/ADR-024-pinned-local-baseline.md): always use venvs; native tool binaries are colocated in `.venv/bin` because they are not pip packages. uv 0.9.11 was used only in an ephemeral bootstrap venv to download exact Python without system installation. `.python-version` and hashed preparation requirements record the baseline.
- [ADR-002](../decisions/ADR-002-python-stream-processing.md): PyFlink 2.2.1/Python 3.12.9, no authored Java or host JDK; native Linux ARM64 source build remains a future packaging gate.
- [ADR-004](../decisions/ADR-004-python-dashboard.md): NiceGUI 3.19.0 replaces React/TypeScript. Compatible selected metadata pins: FastAPI 0.165.0, Starlette 1.10.0, Pydantic 2.14.4/core 2.50.5. Those application libraries are **not installed or fully resolved** here. API SSE remains canonical; UI bridges to NiceGUI's built-in browser transport.
- Member_1 through Member_4 remain ownership boundaries. Preserve independent component venvs/tests/stores when application implementation starts.
- Stream semantics remain five-second windows, ten-second watermarks, and thirty-second inclusive correction limits. The conflicting old eight-second latency target was withdrawn pending measurement. Large-scale targets and larger review settings remain unmeasured.
- OTel Collector uses its direct Helm chart; no OTel Operator/cert-manager/database operator added. Future security profile requires Calico with default CNI disabled.

## Known limitations

The empty cluster is not an application implementation or a full infrastructure deployment. Default storage registration is not persistence/recovery proof. No application schemas were implemented, no model/provider integration exists, and no full-stack load/resource/security claims are verified. The PyFlink Linux ARM64 wheel is absent upstream; source build is selected but untested. NiceGUI sessions require sticky routing/reconnect tests before multi-replica UI claims. Repository license remains an owner decision before public release.

Docker Desktop disk-image capacity was preserved; 40/80-GiB figures are project disk-use budgets, not a disk resize. Resource review uses one Desktop VM with changed limits, not concurrent Colima/Desktop VMs. Existing settings were backed up in ignored `.tools` before applying targeted resource/Kubernetes changes.

## Next recommended milestone

**Contract freeze and executable schemas/fixtures**, runnable inside a Python venv without application services: JSON Schema/events including DLQ/late cases, topology fixtures, Protobuf, OpenAPI/SSE, and agent tool/evidence contracts with valid/invalid golden cases and compatibility tests. Resolve detailed Action/auth defaults there. Keep PyFlink image/source-build proof before calling the aggregation slice runnable. This does not complete the master guide's entire Phase 0.

Ownership: Member_1 device/topology/gRPC fixtures; Member_2 telemetry/load/stream semantics; Member_3 command/control/saga fixtures; Member_4 API/SSE/auth/UI/agent fixtures.
