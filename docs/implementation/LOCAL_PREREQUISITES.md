# Local prerequisites and environment

Rechecked **2026-11-21 19:58 PST (America/Los_Angeles)**. The selected runtime is **Docker Desktop**, with **kind** supplying the project Kubernetes cluster. All application/test code stays Python; infrastructure JVMs remain inside containers. See the [version matrix](VERSION_MATRIX.md), [execution evidence](PREPARATION_VERIFICATION.md), and [status](IMPLEMENTATION_STATUS.md).

The latest recheck passed all installed-tool and current-cluster probes. Disk availability is now **258 GiB** and the macOS free-memory snapshot is **32%**; workload capacity remains unmeasured. No reinstall or configuration change was needed. See the newest section of the evidence report for exact results; older snapshots there are historical.

## Verified installed environment

The inspected computer is Apple M5/ARM64, 10 CPU cores, 16 GiB RAM. Docker Desktop **4.99.0** was already installed and has been started/configured. Its Engine **29.9.3** answers on `desktop-linux` as Linux ARM64. The development allocation is **4 CPUs / 6144 MiB configured RAM**; Docker reports 6,210,510,848 bytes usable guest memory after VM overhead. Built-in Kubernetes reports **disabled**. Existing Colima remains stopped and is not used.

| Prerequisite | Selected installation | Verified result |
|---|---|---|
| Git | Existing Apple Git 2.54.0 | Executes |
| Docker Desktop | Existing 4.99.0; Engine 29.9.3; Docker CLI 29.8.0 | Engine answers; ARM64 Python diagnostic container executes |
| Python | Project-managed CPython 3.12.9 in `.tools/python/`; `.venv` interpreter | Correct version/ARM64 and isolated prefix verified |
| kind | `.venv/bin/kind` 0.30.0 | Official checksum and executable verified |
| kubectl | `.venv/bin/kubectl` 1.34.0 | Official checksum; bundled Kustomize 5.7.1 verified |
| Helm | `.venv/bin/helm` 3.19.0 | Official archive checksum and executable verified |
| Python preparation tools | pytest 8.3.5, jsonschema 4.23.0 and transitive dependencies | Imports, `pip check`, valid/invalid schema tests; hashed wheel downloads verified |
| jq | Existing Apple 1.7.1 | Optional convenience; no reinstall |

Host Java, Maven, Node.js/npm, grpcurl, k6, AWS CLI, eksctl, and standalone Kustomize are not prerequisites. PyFlink and application dependencies are installed only into their owning component/container environments during implementation. No application services were scaffolded by this setup.

## Daily activation and verification

Always activate the project venv, or use its Python/binaries by full relative path. Native kind/kubectl/Helm are not Python packages; putting their binaries in the venv's `bin` directory keeps PATH selection local without replacing Homebrew/system tools.

```sh
cd /Users/sonit/Documents/Code/CMPE-273/CMPE-273-Group-10-Project
source .venv/bin/activate
export PIP_REQUIRE_VIRTUALENV=true
export PIP_CACHE_DIR="$PWD/.tools/pip-cache"
command -v python pip kind kubectl helm
python --version
kind version
kubectl version --client
helm version --short
python -m pip check
python -m pytest -q tests/test_preparation_docs.py tests/test_preparation_environment.py
```

Without activation use `.venv/bin/python -m pytest ...`. The existing global Python 3.14 and Homebrew kubectl 1.37 are preserved; they are not selected for project work. The environment tests intentionally require macOS ARM64/Python 3.12.9 and the selected local tools.

## Recreating the environment

The following reproduces the performed setup on this Mac after a deliberate venv removal. Do not delete a populated venv just to rerun these instructions. No system Python or global pip install is needed. A temporary bootstrap venv holds **uv 0.9.11** solely to obtain CPython 3.12.9; uv is not an application dependency or replacement package manager. [uv's Python documentation](https://docs.astral.sh/uv/guides/install-python/) describes managed interpreter installation.

```sh
python3 -m venv /tmp/smart-grid-bootstrap-venv
/tmp/smart-grid-bootstrap-venv/bin/python -m pip install uv==0.9.11
/tmp/smart-grid-bootstrap-venv/bin/uv python install 3.12.9 --install-dir .tools/python --no-bin --cache-dir .tools/cache
.tools/python/cpython-3.12.9-macos-aarch64-none/bin/python3.12 -m venv .venv
.venv/bin/python -m pip install --require-hashes -r requirements-dev.txt
```

[requirements-dev.txt](../../requirements-dev.txt) pins all ten installed preparation distributions and their wheel hashes for **CPython 3.12/macOS ARM64**. Its native wheel hash is platform-specific. Different platforms require a separately verified lock, not dropping hash validation. `.venv` and `.tools` are ignored; dependency caches, interpreter binaries, Docker settings backups, and downloaded archives must not enter Git. Future components use their own isolated venvs/locks rather than adding their entire stack to this preparation environment.

Download native tools from the pinned official artifacts and check hashes **before** installing. These blocks show the equivalent repeatable installation into the activated venv; the performed installer used the same URLs and SHA-256 checks. [kind release](https://github.com/kubernetes-sigs/kind/releases/tag/v0.30.0), [kubectl installation](https://kubernetes.io/docs/tasks/tools/install-kubectl-macos/), [Helm installation](https://helm.sh/docs/v3/intro/install/).

```sh
source .venv/bin/activate
mkdir -p .tools/downloads
cd .tools/downloads
curl -fLO https://github.com/kubernetes-sigs/kind/releases/download/v0.30.0/kind-darwin-arm64
curl -fLO https://github.com/kubernetes-sigs/kind/releases/download/v0.30.0/kind-darwin-arm64.sha256sum
shasum -a 256 -c kind-darwin-arm64.sha256sum
install -m 0755 kind-darwin-arm64 "$VIRTUAL_ENV/bin/kind"
curl -fL -o kubectl https://dl.k8s.io/release/v1.34.0/bin/darwin/arm64/kubectl
curl -fL -o kubectl.sha256 https://dl.k8s.io/release/v1.34.0/bin/darwin/arm64/kubectl.sha256
printf '%s  kubectl\n' "$(cat kubectl.sha256)" | shasum -a 256 -c -
install -m 0755 kubectl "$VIRTUAL_ENV/bin/kubectl"
curl -fLO https://get.helm.sh/helm-v3.19.0-darwin-arm64.tar.gz
curl -fLO https://get.helm.sh/helm-v3.19.0-darwin-arm64.tar.gz.sha256sum
shasum -a 256 -c helm-v3.19.0-darwin-arm64.tar.gz.sha256sum
tar -xzf helm-v3.19.0-darwin-arm64.tar.gz
install -m 0755 darwin-arm64/helm "$VIRTUAL_ENV/bin/helm"
cd ../..
```

## Docker Desktop and kind

Docker Desktop supplies the engine/VM. Its built-in Kubernetes must stay disabled even when its status mentions a `kind` provisioner: that is separate from the repository's CLI-managed **smart-grid-dev / Kubernetes 1.34.0** cluster. Docker Desktop is free for education/personal use under its [license terms](https://docs.docker.com/subscription-billing/desktop-license/). No cloud account or paid model API is needed.

```sh
source .venv/bin/activate
unset DOCKER_HOST DOCKER_CONTEXT
unset TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE
docker desktop start --detach --timeout 30
docker context use desktop-linux
docker version
docker info --format '{{.ServerVersion}} {{.OSType}}/{{.Architecture}} CPUs={{.NCPU}} Memory={{.MemTotal}}'
docker desktop kubernetes status
kind get clusters
```

Docker access may require this app's approved host access; a sandbox `permission denied` on the socket is distinct from an engine health failure. Commands succeeded with approved host access during setup.

The empty development kind cluster was created and verified using this exact pin: API ready, one Kubernetes 1.34.0 node Ready, system pods Running, default StorageClass registered. Actual PVC I/O/recovery and network-policy tests are still pending. Run creation only if `kind get clusters` does not already list `smart-grid-dev`; do not replace existing clusters/data to rerun setup.

```sh
export KIND_EXPERIMENTAL_PROVIDER=docker
kind create cluster --name smart-grid-dev --image kindest/node:v1.34.0@sha256:7416a61b42b1662ca6ca89f02028ac133a309a2a30ba309614e8ec94d976dc5a --wait 120s
kubectl --context kind-smart-grid-dev get --raw /readyz
kubectl --context kind-smart-grid-dev get nodes -o wide
kubectl --context kind-smart-grid-dev get storageclass
```

Local image loading was also verified with the diagnostic image: `kind load docker-image smart-grid/prerequisite-smoke:3.12.9 --name smart-grid-dev`. No registry upload was used.

Use explicit contexts for all later cluster operations. If Testcontainers needs explicit socket discovery in a component venv, derive `DOCKER_HOST` from `docker context inspect desktop-linux --format '{{.Endpoints.docker.Host}}'` and use `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock`; clear overrides before changing runtimes. Testcontainers itself is not installed/verified by preparation.

For a fresh machine without Docker Desktop, install the Apple Silicon build from the [official Mac instructions](https://docs.docker.com/desktop/setup/install/mac-install/) (or `brew install --cask docker-desktop`), then accept any required first-run prompts yourself. This machine already had the app; it was not reinstalled.

## Resource profiles

Use one container VM at a time. Colima stays stopped. Docker Desktop **Settings → Resources** configures CPU/memory; leave **Settings → Kubernetes → Enable Kubernetes** off. Development settings were applied and verified; the larger review settings remain a planned profile. Existing Docker disk-image capacity was preserved rather than shrunk; 40/80 GiB below are project usage budgets, not a claim that the disk-image limit was changed.

| Budget | Lightweight development | Larger review |
|---|---|---|
| Docker Desktop VM | 4 CPUs, 6 GiB RAM, 40 GiB project disk-use budget | 8 CPUs, 10 GiB RAM, 80 GiB project disk-use budget |
| kind topology | 1 control-plane node | 1 control-plane + 2 workers when topology evidence is needed |
| Workload memory request ceiling | 4 GiB total; leave 2 GiB for VM/Kubernetes/cache | 7.5 GiB total; leave 2.5 GiB for VM/Kubernetes/cache |
| Kafka | 1 combined KRaft broker/controller; 512 MiB heap, initial 768 MiB container limit | 3 combined KRaft brokers/controllers, each 512 MiB heap/768 MiB initial limit |
| Topics | 3 partitions, RF=1, minISR=1 | 3 partitions, RF=3, minISR=2; `acks=all` |
| Flink | On demand: 1 JobManager + 1 TaskManager, parallelism 1 | 1 JobManager + 2 TaskManagers, parallelism 2 when testing recovery/scaling |
| Application | Owned component only; fixtures and deterministic model/tool mocks | Two Action/API replicas for failover/routing; Registry as exercised; one UI instance initially |
| Data/operations | PostgreSQL or Kafka only when needed; observability endpoints tested locally | One PostgreSQL primary, one Connect worker, Collector/Prometheus/Grafana/Tempo as needed per exercise |
| Scenario starting size | 1 transformer, 10 devices, 1 Hz | 3 transformers, 100 devices, 1 Hz; scale only after measurement |
| Data budgets | 24h Kafka retention, 5 GiB Kafka PVC, 5 GiB PostgreSQL, 2 GiB checkpoints | 24h Kafka retention, 5 GiB per broker, 5 GiB PostgreSQL, 5 GiB checkpoints, 5 GiB observability |

These are **initial planning limits**, not tested resource guarantees. Flink memory must include Python workers, JVM overhead, network buffers, and managed memory; start JobManager at 768 MiB and each TaskManager at 1 GiB, then measure. Requests/limits must be set for every future workload. The entire review stack is not promised to fit concurrently. Run (1) broker/stream/control recovery, (2) identity/UI/replica tests, and (3) observability/agent evaluations in stages, retaining signals needed to prove each exercise. The closed-loop baseline must still run end to end at bounded scale. Never use RF=1 to claim broker-failure tolerance or drop fencing/checkpoints to meet a budget. Record actual memory, swap/OOM, CPU, and disk results before accepting any review measurement.


The larger review profile requires changing Desktop settings to 8 CPUs/10 GiB and re-verifying `docker info`; no second concurrent VM is needed. No larger-profile fit or performance result is claimed. Stop Docker Desktop through its UI or `docker desktop stop` when finished; this also stops its local kind nodes. Removing `smart-grid-dev` with `kind delete cluster --name smart-grid-dev` destroys its node-local PVC data, so export needed state/evidence first. Cleanup commands were not run during setup.

## Deferred implementation gates

This environment does not establish application behavior. PyFlink's native Linux ARM64 source build/import/checkpoint test remains pending; no host JDK is needed. Application/component venvs, full dependency locks, infrastructure charts/operators, policy-capable Calico networking, volume read/write/recovery, and the closed-loop system belong to later milestones. Default kind networking is not NetworkPolicy enforcement evidence.

## Contract milestone tooling

The latest read-only inventory confirms the same hardware/tool versions and running kind baseline; 258 GiB disk was available and the free-memory snapshot was 36%. Node.js is absent; Java's system launcher reports no installed runtime. They remain unnecessary. No system software/runtime configuration was changed during contract implementation.

Install only the separate locked contract tooling with the existing project interpreter:

```sh
.venv/bin/python -m venv .tools/contracts-venv
.tools/contracts-venv/bin/python -m pip install --cache-dir .tools/pip-cache --only-binary=:all: --require-hashes -r requirements-contracts.txt
.tools/contracts-venv/bin/python -m pip check
make contracts
make bindings
make contracts BASE_REF=HEAD
```

These commands have been executed locally. A clean second ignored venv also installed this lock offline from hash-verified wheels and passed. `make` is the existing macOS/standard-runner entry point; if unavailable, run `.tools/contracts-venv/bin/python tools/contracts.py` directly. Bindings/descriptors go to `.tools/generated`; two capability definitions in the bundle are generated from the canonical Protobuf. No global pip, host compiler/JDK, or application framework is needed. See [the executable bundle](../../schemas/README.md). Standard Ubuntu CI uses the same Python version and lock; Mac-only preparation checks are separate. The existing preparation lock remains platform-specific, while the contract lock includes the published wheel hashes needed by both selected validation platforms.
