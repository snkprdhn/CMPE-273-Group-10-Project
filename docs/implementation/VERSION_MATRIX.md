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
| gRPC generation/runtime | grpcio 1.71.0, grpcio-tools 1.71.0 | Member_1; independently generated clients |
| Schemas | jsonschema 4.23.0; JSON Schema Draft 2020-12 | All; executable compatibility fixtures next milestone |
| Correctness tests | pytest 8.3.5; Testcontainers Python 4.10.0 | All |
| Browser tests | Playwright Python 1.51.0, pytest-playwright 0.7.0 | Member_4 |
| Load tests | Locust 2.33.2 | Member_1 |

The NiceGUI [recheck amendment](../decisions/ADR-004-python-dashboard.md#prerequisite-recheck-amendment--2026-11-21) replaces the original vulnerable 2.24.2 pin and updates FastAPI/Starlette/Pydantic together. Nine selected direct metadata constraints passed; full resolution/imports remain unverified. NiceGUI's [release requirements](https://raw.githubusercontent.com/zauberzeug/nicegui/v3.19.0/pyproject.toml) allow this Python/FastAPI family. Retain its packaged frontend assets. [Playwright Python](https://playwright.dev/python/docs/intro) requires its downloaded browser/packaged driver, not a host Node.js install. Locust replaces authored JavaScript k6 scenarios. Security scans and exact transitive dependency locking belong to the component/foundation gates, before these versions are deployed.

## Acceptance boundary

**Verified:** Docker Desktop startup and development CPU/RAM configuration; built-in Kubernetes disabled; a network-isolated pinned ARM64 Python 3.12.9 container and local image loading into kind; checksum-verified native CLI tools; project Python venv/pip consistency and ten local tests; empty kind 1.34.0 cluster with Ready node, healthy API/system pods, and registered default local-path StorageClass. Six core image platform manifests were also inspected remotely in the earlier recheck.

**Not verified:** actual PVC provisioning/read/write/recovery, workload DNS/NetworkPolicy allow-deny behavior, review-profile resource fit, infrastructure Helm releases/project overlays, application dependency locks/imports/startup, PyFlink source wheel build/checkpoint recovery, and full-stack behavior. StorageClass presence does not prove storage recovery; default kind networking does not prove policy enforcement. Only preparation Python dependencies are installed/locked. Each future chart/JAR/image/sidecar still needs pinned integrity metadata and its owning runtime gate.

Environment setup scaffolded no application service, created no cloud resource, and made no paid API call. Commit/push requires explicit user authorization, which was provided after verification. Docker Desktop was reused, not reinstalled. Colima remains an unselected alternative.
