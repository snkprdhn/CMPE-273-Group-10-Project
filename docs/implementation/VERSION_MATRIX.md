# Accepted version and compatibility matrix

Accepted: **2026-11-21**, under [ADR-002](../decisions/ADR-002-python-stream-processing.md), [ADR-004](../decisions/ADR-004-python-dashboard.md), [ADR-006](../decisions/ADR-006-local-kind.md), and [ADR-024](../decisions/ADR-024-pinned-local-baseline.md). These are reproducibility pins, not “latest” or production-security claims. Locally rechecked **2026-11-21 19:58 PST** after the user-authorized Docker Desktop/venv setup. The installed tool versions and running kind cluster still match; no pin changes were needed. Application metadata evidence below is from the earlier review and is not a fresh full-stack verification. Host observations and exact gate status are in [verification](PREPARATION_VERIFICATION.md); manual setup commands are in [prerequisites](LOCAL_PREREQUISITES.md).

## Host tools

Docker Desktop setup completed on **2026-11-21**; the tool/runtime checks passed again at **19:58 PST**. Activate `.venv` before every Python/tool command. Native tools reside in `.venv/bin`; no global binaries or global Python packages were replaced.

| Tool | Accepted/installed version | Verified evidence |
|---|---|---|
| Git | 2.54.0 Apple Git-157 | Existing executable runs |
| Docker Desktop | 4.99.0 (245822), existing app | Started; built-in Kubernetes reports disabled |
| Docker Engine / CLI | Engine 29.9.3; CLI 29.8.0; Buildx 0.38.1 | Client/server API 1.56; Linux ARM64; diagnostic container runs |
| Runtime resources | Desktop 4 CPUs, 6144 MiB configured | `docker info`: 4 CPUs / 6,210,510,848 usable guest bytes |
| Python | CPython 3.12.9, macOS ARM64 | Managed interpreter in `.tools/python`; isolated `.venv` prefix and exact patch verified |
| kind | 0.30.0, `.venv/bin/kind` | Installed payload rechecked against the verified release artifact; existing `smart-grid-dev` cluster healthy |
| Kubernetes | 1.34.0 | Project kind API `/readyz` returns `ok`; node Ready; system pods Running |
| kubectl | 1.34.0, `.venv/bin/kubectl` | Installed payload rechecked against the verified release artifact; client and project API calls pass |
| Kustomize | 5.7.1, bundled with selected kubectl | Client reports exact version; full project overlay rendering deferred |
| Helm | 3.19.0, `.venv/bin/helm` | Installed payload rechecked against the verified archive; executable reports v3.19.0+g3d8990f |
| Python preparation tools | pytest 8.3.5, jsonschema 4.23.0 | Installed versions/venv locations/cached wheel hashes rechecked; imports/tests/pip check pass; ten-distribution [hashed lock](../../requirements-dev.txt) validated against downloaded wheels |
| Bootstrap installer | uv 0.9.11 in temporary bootstrap venv only | Downloaded exact managed Python; not a runtime dependency |
| Colima | 0.10.3, existing but unselected | Remains stopped; no profile removed or changed |
| jq | 1.7.1 Apple build | Optional existing executable verified |
| Host Java/Maven/Node/npm/grpcurl/k6 | Not prerequisites | Infrastructure JVMs remain container-only; Python tools replace authored JavaScript tests |
| AWS CLI/eksctl | Deferred | No installation/account required |

Existing global Python 3.14.7 and kubectl 1.37.2 (Docker-bundled alternative 1.36.1) are preserved and are not selected for project commands. Built-in Docker Desktop Kubernetes is disabled even though its metadata names a different Kubernetes version; only the CLI-managed kind cluster is the project target. Exact checks and archive digests are in [setup evidence](PREPARATION_VERIFICATION.md).

Pin kind's multi-platform node image to:

```text
kindest/node:v1.34.0@sha256:7416a61b42b1662ca6ca89f02028ac133a309a2a30ba309614e8ec94d976dc5a
```

The [kind 0.30.0 release](https://github.com/kubernetes-sigs/kind/releases/tag/v0.30.0) publishes this digest and supports ARM64/amd64 nodes. [Helm's version-skew table](https://helm.sh/docs/v3/topics/version_skew/) pairs 3.19.x with Kubernetes 1.31–1.34. Kubernetes permits [kubectl within one minor of the API server](https://kubernetes.io/releases/version-skew-policy/#kubectl), hence the installed 1.37 client is unsuitable for this baseline. The selected client reports darwin/arm64 and bundled Kustomize 5.7.1. The existing global clients are retained; activation selects the compatible local binary.

## Containerized infrastructure

| Component/owner | Exact selected version/artifact | Compatibility constraint and official evidence |
|---|---|---|
| Policy CNI / shared infrastructure | Calico 3.31.0, upstream manifest; IPv4 VXLAN/iptables | [Calico 3.31 requirements](https://raw.githubusercontent.com/tigera/docs/master/calico_versioned_docs/version-3.31/_includes/partials/_system-requirements.mdx) list ARM64 and Kubernetes 1.32–1.35; default kind CNI disabled for security profile; integration/allow-deny tests unverified |
| Strimzi / Member_2 | Helm chart/operator 0.45.2 | [Upstream matrix](https://strimzi.io/downloads/) lists Kafka 3.9.2 and Kubernetes through 1.35 for this release |
| Kafka and Connect / Member_2, Member_3 | `quay.io/strimzi/kafka:0.45.2-kafka-3.9.2` | KRaft; Connect shares the selected Kafka distribution; no ZooKeeper |
| Flink Operator / Member_2 | Chart/image 1.15.0 | [Apache downloads](https://flink.apache.org/downloads/) lists Flink 2.2.x support; webhook disabled locally |
| Flink / Member_2 | `flink:2.2.1-scala_2.12-java17` | [Upstream image recipe](https://raw.githubusercontent.com/apache/flink-docker/master/2.2/scala_2.12-java17-ubuntu/Dockerfile); derive a Python-enabled ARM64 image at implementation time |
| PyFlink / Member_2 | `apache-flink==2.2.1` | [Python 3.9–3.12 supported](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/python/installation/); Linux ARM64 source wheel build required, [published files](https://pypi.org/project/apache-flink/2.2.1/#files) |
| Flink Kafka connector / Member_2 | `org.apache.flink:flink-sql-connector-kafka:5.0.0-2.2` | Bundled upstream JAR for Python jobs; [matching connector line](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/datastream/kafka/); includes runtime dependencies, no custom Java build |
| PostgreSQL / Member_3 | `postgres:17.4-bookworm` | [Debezium 3.0 tested versions](https://debezium.io/releases/3.0/) include PostgreSQL 17, Kafka Connect >=3.1, and Java >=17 connectors |
| PostgreSQL packaging / Member_3 | `oci://registry-1.docker.io/cloudpirates/postgres`, chart 0.22.0, common 2.2.1 | [Chart source metadata](https://raw.githubusercontent.com/CloudPirates-io/helm-charts/main/charts/postgres/Chart.yaml); explicitly override default PostgreSQL 18 image to selected 17.4; helpers account for pre-18 data paths |
| CDC / Member_3 | Debezium PostgreSQL connector plugin `3.0.8.Final` | Add upstream plugin archive and Outbox Event Router to selected Strimzi Connect image; PostgreSQL pgoutput/logical WAL; [release compatibility](https://debezium.io/releases/3.0/) |
| Keycloak / Member_4 | `quay.io/keycloak/keycloak:26.1.4` | [Official release](https://www.keycloak.org/2025/03/keycloak-2614-released); own database/role; verify OIDC and PostgreSQL startup during identity milestone |
| OTel Collector / Member_1 | Chart 0.117.3; `otel/opentelemetry-collector-contrib:0.120.0` | [Chart metadata](https://raw.githubusercontent.com/open-telemetry/opentelemetry-helm-charts/opentelemetry-collector-0.117.3/charts/opentelemetry-collector/Chart.yaml); explicit repository override; direct Deployment, no OTel Operator |
| Metrics / Member_1 | kube-prometheus-stack chart 69.8.2; Prometheus Operator 0.80.1; Prometheus 3.2.1 | [Chart](https://raw.githubusercontent.com/prometheus-community/helm-charts/kube-prometheus-stack-69.8.2/charts/kube-prometheus-stack/Chart.yaml) accepts Kubernetes >=1.19; [values](https://raw.githubusercontent.com/prometheus-community/helm-charts/kube-prometheus-stack-69.8.2/charts/kube-prometheus-stack/values.yaml) pin Prometheus; disable unused exporters/Alertmanager in dev |
| Grafana / Member_1 | `grafana/grafana:11.6.0` via explicit stack chart image override | Avoid depending on a floating Grafana subchart image; chart archive's packaged dependencies retained and checksummed at packaging gate |
| Traces / Member_1 | Tempo chart 1.18.2; `grafana/tempo:2.7.1` | [Chart metadata](https://raw.githubusercontent.com/grafana/helm-charts/tempo-1.18.2/charts/tempo/Chart.yaml); single binary, local PVC, no cloud object store |
| Fault proxy / Member_1 | `ghcr.io/shopify/toxiproxy:2.12.0` | [Upstream project](https://github.com/Shopify/toxiproxy); on-demand test dependency |
| Python service base / all | `python:3.12.9-slim-bookworm` | Same Python minor as PyFlink; pin distribution separately inside derived Flink image |

## Python application/testing baseline

Install application libraries only when the owning implementation milestone begins. These are selected direct application pins; **no application environment resolution or transitive lock is claimed yet**. The preparation pytest/jsonschema environment is installed and fully pinned in requirements-dev.txt. Keep PyFlink in its own image/environment and browser/load tools in test environments.

| Concern | Selected pins | Owner |
|---|---|---|
| API and validation | FastAPI 0.165.0, Starlette 1.10.0, Uvicorn 0.35.0, Pydantic 2.14.4 (core 2.50.5), HTTPX 0.28.1 | Member_4; reuse where needed |
| Dashboard | NiceGUI 3.19.0, built-in ECharts | Member_4 |
| Kafka client | confluent-kafka 2.8.2 | Member_1/Member_3/Member_4 |
| Database | psycopg[binary] 3.2.9 | Member_1/Member_3/Member_4 |
| gRPC generation/runtime | grpcio 1.85.3, grpcio-tools 1.71.0 (ADR-025 runtime amendment) | Member_1; independently generated clients |
| Schemas | jsonschema 4.23.0; JSON Schema Draft 2020-12 | All; executable fixtures and compatibility checks verified under ADR-025 |
| Correctness tests | pytest 8.3.5; Testcontainers Python 4.10.0 | All |
| Browser tests | Playwright Python 1.51.0, pytest-playwright 0.7.0 | Member_4 |
| Load tests | Locust 2.33.2 | Member_1 |

The NiceGUI [recheck amendment](../decisions/ADR-004-python-dashboard.md#prerequisite-recheck-amendment--2026-11-21) replaces the original vulnerable 2.24.2 pin and updates FastAPI/Starlette/Pydantic together. Nine selected direct metadata constraints passed; full resolution/imports remain unverified. NiceGUI's [release requirements](https://raw.githubusercontent.com/zauberzeug/nicegui/v3.19.0/pyproject.toml) allow this Python/FastAPI family. Retain its packaged frontend assets. [Playwright Python](https://playwright.dev/python/docs/intro) requires its downloaded browser/packaged driver, not a host Node.js install. Locust replaces authored JavaScript k6 scenarios. Security scans and exact transitive dependency locking belong to the component/foundation gates, before these versions are deployed.

## Acceptance boundary

**Verified:** Docker Desktop startup and development CPU/RAM configuration; built-in Kubernetes disabled; a network-isolated pinned ARM64 Python 3.12.9 container and local image loading into kind; checksum-verified native CLI tools; project Python venv/pip consistency and ten local tests; empty kind 1.34.0 cluster with Ready node, healthy API/system pods, and registered default local-path StorageClass. Six core image platform manifests were also inspected remotely in the earlier recheck.

**Not verified:** actual PVC provisioning/read/write/recovery, workload DNS/NetworkPolicy allow-deny behavior, review-profile resource fit, infrastructure Helm releases/project overlays, application dependency locks/imports/startup, PyFlink source wheel build/checkpoint recovery, and full-stack behavior. StorageClass presence does not prove storage recovery; default kind networking does not prove policy enforcement. Only preparation Python dependencies are installed/locked. Each future chart/JAR/image/sidecar still needs pinned integrity metadata and its owning runtime gate.

Environment setup scaffolded no application service, created no cloud resource, and made no paid API call. Commit/push requires explicit user authorization, which was provided after verification. Docker Desktop was reused, not reinstalled. Colima remains an unselected alternative.

## Executable contract tooling — accepted 2026-11-21

[ADR-025](../decisions/ADR-025-executable-contract-freeze.md) adds an independent `.tools/contracts-venv` using the existing CPython **3.12.9**. The preparation venv and all application/infrastructure pins above are preserved. No system software was installed for this milestone.

| Direct tool / selected compiler runtime | Pin | Verification |
|---|---|---|
| JSON Schema | jsonschema 4.23.0 | Draft 2020-12 schemas, formats and domain checks pass |
| Tests | pytest 8.3.5 | Contract suite passes locally on macOS ARM64 |
| Protobuf compiler/binding generator | grpcio-tools 1.71.0 | Compiles all four RPCs and six messages; generated Python messages parse/binary-round-trip fixtures |
| Compiler runtime | grpcio 1.85.3; protobuf 5.29.6 | Satisfy installed compiler metadata; `pip check` and generation pass; isolated from PyFlink |
| OpenAPI | openapi-spec-validator 0.7.2 | OpenAPI 3.1.1 document and local canonical references validate offline |

[requirements-contracts.txt](../../requirements-contracts.txt) pins all 27 distributions and wheel hashes. Hash-checked downloads succeeded for macOS ARM64 and Linux x86_64 CPython 3.12; a clean Mac venv installed offline from those wheels and passed the suite. Linux wheel resolution is verified; hosted GitHub CI execution remains pending. No host `protoc`, JDK, Maven, Node.js or npm is needed. Wire standards are JSON Schema Draft 2020-12, Protobuf proto3, and OpenAPI 3.1.1. Official sources: [gRPC compiler release metadata](https://pypi.org/pypi/grpcio-tools/1.71.0/json), [Protobuf](https://protobuf.dev/programming-guides/proto3/), [OpenAPI](https://spec.openapis.org/oas/v3.1.1.html).

Read-only inventory during the contract milestone confirmed Apple M5/ARM64, 10 CPU cores, 16 GiB RAM, 258 GiB available disk, and 36% macOS free-memory snapshot. Git 2.54.0, project Python 3.12.9, kind 0.30.0, kubectl 1.34.0/Kustomize 5.7.1, Helm 3.19.0, Docker Desktop 4.99.0/Engine 29.9.3/CLI 29.8.0, and the Kubernetes 1.34.0 Ready node match the accepted baseline. Docker still has four CPUs/6,210,510,848 usable guest bytes; built-in Kubernetes is disabled. Colima 0.10.3 is stopped. Global Python is 3.14.7, Node.js is absent, and `/usr/bin/java` is only a launcher with no installed Java runtime. None was changed. The [official Flink 2.2 installation guide](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/python/installation/) still specifies Python 3.9–3.12; the [Helm table](https://helm.sh/docs/v3/topics/version_skew/) still pairs 3.19.x with Kubernetes 1.31–1.34. Those compatibility checks do not prove the future ARM64 PyFlink image or full stack runnable.

## Lightweight Compose artifact lock — 2026-11-21

The local development milestone uses the same selected Kafka/PostgreSQL releases as
the planned kind stack. Pulled multi-platform image digests (ARM64 selected locally):

| Image | Manifest digest |
|---|---|
| `quay.io/strimzi/kafka:0.45.2-kafka-3.9.2` | `sha256:5d96fe7c68db643f4916146176a7587aaf9f91c32fa9da7300f013e3e79298d8` |
| `postgres:17.4-bookworm` | `sha256:304ab813518754228f9f792f79d6da36359b82d8ecf418096c636725f8c930ad` |
| `ghcr.io/shopify/toxiproxy:2.12.0` | `sha256:9378ed52a28bc50edc1350f936f518f31fa95f0d15917d6eb40b8e376d1a214e` |

Docker Compose **5.7.0** is bundled with the existing Docker Desktop. No new host or
Python dependency is needed: the runner uses stdlib and the pinned images' Kafka/psql
clients. See [runbook](../../infra/compose/README.md) and [status](IMPLEMENTATION_STATUS.md)
for executed runtime evidence and remaining limitations. This does not deploy the
planned Kubernetes stack or establish its compatibility through runtime tests.

The Compose profile derives `smart-grid-local-dev/kafka:3.9.2`,
`smart-grid-local-dev/postgres:17.4` and `smart-grid-local-dev/toxiproxy:2.12.0` locally
from those exact bases using [one multi-target Dockerfile](../../infra/compose/Dockerfile).
It copies only repository configuration and prepares Kafka's non-root directories.
No image is pushed; `up --build` uses the build cache and reflects configuration edits.
Runtime host bind mounts are omitted after diagnostics found a Docker Desktop
FSEvents watcher deadlock during VolumeShare.Add; named data volumes are retained.


## Member_1 standalone application locks — 2026-11-22

[ADR-026](../decisions/ADR-026-member1-standalone-state.md) resolves the old runtime
row in favor of ADR-025's grpcio **1.85.3**, grpcio-tools **1.71.0**, protobuf
**5.29.6**. Kafka **2.8.2**, psycopg/binary **3.2.9**, jsonschema **4.23.0**, and
pytest **8.3.5** retain their selected pins. Separate [Registry](../../member1/requirements-registry.txt)
and [simulator](../../member1/requirements-simulator.txt) hash locks/venvs isolate
runtime dependencies; the [combined test lock](../../member1/requirements.txt)
is Member_1-only. macOS ARM64 clean offline installs and Linux ARM64 image
build/start pass. Linux x86_64 wheel resolution/hash verification is separate
from hosted CI execution. These application results supersede only Member_1's
previously unimplemented dependency/startup claims; other application gates
remain unverified. Exact commands/results are in implementation status.

## Member_2 standalone runtime — verified 2026-11-22

The selected Python **3.12.9**, Flink/PyFlink **2.2.1**, connector **5.0.0-2.2**,
jsonschema **4.23.0**, pytest **8.3.5** and confluent-kafka **2.8.2** are unchanged.
[Member_2's independent lock](../../member2/requirements.txt) pins all 56 runtime/
test distributions with published artifact hashes. The pure-test subset in
`member2/requirements-test.txt` uses the same pins with portable artifact hashes. PyFlink is compiled natively
from its hash-verified sdist; Beam 2.61.0, grpcio 1.65.5 and protobuf 5.29.6 stay
isolated from Registry/contract environments. Offline built-wheel installation,
`pip check`, ARM64 import, golden tests and real checkpoint restore pass.

Immutable Flink base: `flink:2.2.1-scala_2.12-java17@sha256:f0aa6be1ea6979e0c0c29d02bb1a41eee0bc1a350b86a39056351db165f9f3ff`.
Connector JAR SHA-256: `5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616`.
The Python base reuses Member_1's accepted immutable Python 3.12.9 base.
The image retains resolved versions and compiled wheel SHA-256s in
`/opt/member2-resolved.txt` and `/opt/member2-wheel-sha256.txt`. Build-only apt/
isolated Python build tools are not a bit-reproducible toolchain lock. Linux
amd64 execution, image security scans/SBOM, kind/operator deployment, replicated
Kafka and full-session HA remain unverified. Earlier statements that PyFlink
ARM64 build/import/checkpoint execution is unverified are superseded only for
this standalone profile. See [ADR-027](../decisions/ADR-027-member2-standalone-aggregation.md).

## Member_3 standalone Action domain — verified 2026-11-22

The [Member_3 lock](../../member3/requirements.txt) reuses accepted Python 3.12.9,
psycopg/binary 3.2.9, jsonschema 4.23.0 and pytest 8.3.5 pins/hashes in an independent
`.tools/member3-venv`. All 12 runtime/test distributions installed offline from
existing Mac ARM64 wheels with hash checking; `pip check` and 42 real PostgreSQL
tests pass. No gRPC/Kafka/PyFlink/model dependency is introduced for this domain
slice. PostgreSQL reuses the existing pinned 17.4 local container. Linux runtime,
CDC, distributed leadership and kind packaging remain separate gates under
[ADR-028](../decisions/ADR-028-member3-action-domain.md).

## Outbox/fencing runtime — 2026-11-22

[ADR-029](../decisions/ADR-029-outbox-and-fencing.md) uses the already selected
Strimzi Kafka/Connect **0.45.2 / 3.9.2** immutable base above and PostgreSQL **17.4**.
The upstream Debezium PostgreSQL **3.0.8.Final** plugin tarball has SHA-256
`600806875b810abb96b3774f789d9ed33dbe4ac687d3ed9454bc0caa76b7279c`;
[Dockerfile](../../infra/member3/Dockerfile.connect) verifies it before extraction.
The independent Member_3 lock adds only the previously selected **confluent-kafka
2.8.2** artifact hashes. The Member_1 simulator lock adds the existing **psycopg /
psycopg-binary 3.2.9** pins for the optional PostgreSQL effect profile. No Kubernetes
SDK or new version selection is introduced; the local runner invokes the pinned
kubectl. Real local CDC and kind Lease failure evidence is in implementation
status. Full kind workload/security/operations packaging remains unverified.
