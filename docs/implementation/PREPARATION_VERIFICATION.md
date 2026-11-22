# Preparation verification

## Prerequisite recheck — 2026-11-21 19:58 PST (latest)

Re-ran the existing environment checks using project `.venv` tools and approved host access for Docker/Kubernetes. No installations, version changes, configuration changes, cluster recreation, application deployment, cloud resource, commit, or push occurred. The only runtime mutation was a temporary, network-isolated diagnostic container, automatically removed. No image was pulled (`--pull=never`). Earlier setup/recheck sections below are historical.

### Current executable and local verification results

```sh
source .venv/bin/activate
export PIP_REQUIRE_VIRTUALENV=true
export PIP_CACHE_DIR="$PWD/.tools/pip-cache"
command -v python pip kind kubectl helm
python --version
kind version
kubectl version --client --output=json
helm version --short
python -m pip check
python -m pytest -q tests/test_preparation_docs.py tests/test_preparation_environment.py
git diff --check
git check-ignore .venv/bin/python .tools/downloads/kind-darwin-arm64
```

All commands exited **0**. Activation selects project `.venv/bin` for all five named executables. Verified Python **3.12.9**, kind **0.30.0**, kubectl **1.34.0**, Kustomize **5.7.1**, Helm **3.19.0+g3d8990f**. `pip check`: **No broken requirements found**. Tests: **10 passed**. Git ignores the interpreter and downloaded tools. No native-tool or Python dependency drift was found.

### Current Docker and kind results

Each command had a 20-second process timeout; Kubernetes API requests also used a 10-second request timeout. Every row exited **0**; no timeout occurred. Snapshot captured at `2026-11-21T19:58:52.586787-08:00`.

| Exact command | Observed result |
|---|---|
| `git --version` | git version 2.54.0 (Apple Git-157) |
| `docker desktop version` | Docker Desktop CLI plugin version: 4.99.0 |
| `docker --context desktop-linux version` | Docker Desktop 4.99.0; CLI 29.8.0; Engine 29.9.3; client/server API 1.56; Linux ARM64 server. |
| `docker --context desktop-linux info --format '{{.ServerVersion}} {{.OSType}}/{{.Architecture}} CPUs={{.NCPU}} Memory={{.MemTotal}}'` | 29.9.3 linux/aarch64 CPUs=4 Memory=6210510848 |
| `docker desktop kubernetes status` | State disabled; Kubernetes stopped. Its 1.36.1 metadata is not the project kind cluster. |
| `docker context show` | desktop-linux |
| `colima list` | Existing default profile remains Stopped; no Colima runtime was started. |
| `.venv/bin/kind get clusters` | smart-grid-dev |
| `.venv/bin/kubectl --context kind-smart-grid-dev --request-timeout=10s get --raw /readyz` | ok |
| `.venv/bin/kubectl --context kind-smart-grid-dev --request-timeout=10s get nodes -o wide` | smart-grid-dev-control-plane: Ready, Kubernetes v1.34.0. |
| `.venv/bin/kubectl --context kind-smart-grid-dev --request-timeout=10s get pods -A` | All nine system pods: 1/1 Ready, Running, zero restarts. |
| `.venv/bin/kubectl --context kind-smart-grid-dev --request-timeout=10s get storageclass` | standard (default), rancher.io/local-path, WaitForFirstConsumer; registration only. |
| `docker --context desktop-linux ps --format '{{.Names}}'` | smart-grid-dev-control-plane |
| `df -h .` | 258 GiB available on the 460 GiB filesystem; usage increased since initial setup inventory. |
| `memory_pressure -Q` | 16 GiB total RAM; macOS system-wide free-memory metric 32% (snapshot, not workload capacity). |

### Current isolated-container and node image-store checks

```sh
docker --context desktop-linux run --rm --pull=never --network none --platform linux/arm64 python:3.12.9-slim-bookworm@sha256:40011d5cf225eb43b650ec158d4a59ca3110e6566ddfd3f506824db6153761cf python -c 'import platform,sys; assert sys.version_info[:3] == (3,12,9); assert platform.machine() == "aarch64"; print("Python 3.12.9 / aarch64: runtime smoke passed")'
```

Exit **0**:

```text
Python 3.12.9 / aarch64: runtime smoke passed
```

```sh
docker --context desktop-linux exec smart-grid-dev-control-plane crictl inspecti docker.io/smart-grid/prerequisite-smoke:3.12.9
```

Exit **0**:

```text
{"id": "sha256:916619b289581c9a4f2941745a09b2934cee4bd354049355b4d4d53485d67573", "repoTags": ["docker.io/smart-grid/prerequisite-smoke:3.12.9"], "size": "44972843"}
```

The existing kind image was inspected, not loaded again. The diagnostic container verifies local Python execution only; no application behavior, DNS, PVC, or NetworkPolicy test is implied.

### Offline installed-artifact and lock audit

A `.venv/bin/python` audit executed **14 integrity checks**, all passing:

- PASS: venv prefix isolated and system site-packages disabled.
- PASS: kind installed binary matches checksum-verified release artifact.
- PASS: kubectl installed binary matches checksum-verified release artifact.
- PASS: helm installed binary matches checksum-verified release artifact.
- PASS: attrs==26.1.0 installed inside venv; cached wheel hash matches lock.
- PASS: iniconfig==2.3.1 installed inside venv; cached wheel hash matches lock.
- PASS: jsonschema==4.23.0 installed inside venv; cached wheel hash matches lock.
- PASS: jsonschema-specifications==2025.9.1 installed inside venv; cached wheel hash matches lock.
- PASS: packaging==26.3 installed inside venv; cached wheel hash matches lock.
- PASS: pluggy==1.7.0 installed inside venv; cached wheel hash matches lock.
- PASS: pytest==8.3.5 installed inside venv; cached wheel hash matches lock.
- PASS: referencing==0.37.0 installed inside venv; cached wheel hash matches lock.
- PASS: rpds-py==2026.9.1 installed inside venv; cached wheel hash matches lock.
- PASS: typing_extensions==4.16.0 installed inside venv; cached wheel hash matches lock.

The audit used `hashlib.sha256` on cached official kind/kubectl artifacts and the Helm archive, compared installed native payloads byte-for-byte (Helm extracted in memory), and used `packaging.requirements.Requirement` with `importlib.metadata` for every requirements-dev.txt pin. Each distribution's location was verified to be under `sys.prefix`; each cached wheel SHA-256 matched its lock entry. `pyvenv.cfg` explicitly has `include-system-site-packages = false`. This checks current files against previously verified release artifacts, without contacting upstream or rescanning vulnerabilities. It does not claim application-package resolution or security certification.

### Corrections and remaining gates

- Clarified the newest evidence versus earlier historical setup snapshots, and refreshed disk/free-memory observations.
- All currently tested prerequisite pins remain compatible with the running development cluster; no pin update was justified.
- Only prerequisite/empty-cluster and preparation-package checks are complete. PVC I/O/recovery, workload DNS/NetworkPolicy enforcement, chart/operator deployment, application environments, PyFlink ARM64 build/checkpoints, review-profile fit, and performance/security claims remain pending.
- Upstream application dependency compatibility evidence was not re-fetched during this local recheck. Previously inspected application pins remain selected, not installed or fully verified.

---

## Docker Desktop and venv setup — 2026-11-21 (historical setup)

This records the verified initial Desktop/venv setup and supersedes the older missing/stopped prerequisite observations below; use the latest recheck above for current state. The user explicitly authorized prerequisite installation and local environment setup, selected Docker Desktop, required venv use wherever possible, and prohibited application scaffolding/cloud resources/commits/pushes. All earlier uncommitted changes were preserved.

### Installation and configuration performed

- Read installed `/Applications/Docker.app/Contents/Info.plist`: Docker Desktop **4.99.0** already existed, so no Desktop installation was needed. Started with `docker desktop start --detach --timeout 30` (exit 0).
- `python3 -m venv /tmp/smart-grid-bootstrap-venv` and `/tmp/smart-grid-bootstrap-venv/bin/python -m pip install --disable-pip-version-check uv==0.9.11` exited 0. This isolated temporary installer did not modify global Python packages.
- `/tmp/smart-grid-bootstrap-venv/bin/uv python install 3.12.9 --install-dir .tools/python --no-bin --cache-dir .tools/cache` exited 0 and downloaded macOS ARM64 CPython 3.12.9.
- `.tools/python/cpython-3.12.9-macos-aarch64-none/bin/python3.12 -m venv .venv` exited 0. Python checks assert `sys.prefix != sys.base_prefix`, patch `(3,12,9)`, and `platform.machine() == 'arm64'`.
- `.venv/bin/python -m pip install --disable-pip-version-check pytest==8.3.5 jsonschema==4.23.0` exited 0. Ten distributions installed into `.venv`, none globally.
- `python3 /tmp/install_grid_native_tools.py` exited 0: downloaded official native artifacts, compared SHA-256 with upstream checksum files, and installed binaries in `.venv/bin`. The equivalent URLs/commands are preserved in [prerequisites](LOCAL_PREREQUISITES.md); verified archive hashes follow.

| Tool | Verified SHA-256 of official downloaded binary/archive |
|---|---|
| kind | `ceaf40df1d1551c481fb50e3deb5c3deecad5fd599df5469626b70ddf52a1518` |
| kubectl | `d491f4c47c34856188d38e87a27866bd94a66a57b8db3093a82ae43baf3bb20d` |
| helm | `31513e1193da4eb4ae042eb5f98ef9aca7890cfa136f4707c8d4f70e2115bef6` |

The native tools are not Python packages; venv-local executable placement makes activation select them. Existing Homebrew/system tools were not overwritten. `.venv` and `.tools` are ignored by Git. `.python-version` records 3.12.9. A checked-in preparation requirements file records installed versions and selected wheel hashes.

- Resolver metadata: `.venv/bin/python -m pip install --disable-pip-version-check --dry-run --ignore-installed --only-binary=:all: --report .tools/preparation-pip-report.json pytest==8.3.5 jsonschema==4.23.0` exited 0. `requirements-dev.txt` was generated from the ten report entries (exact versions/archive SHA-256).
- Hash verification: `.venv/bin/python -m pip download --disable-pip-version-check --require-hashes --no-deps -r requirements-dev.txt --dest .tools/wheelhouse` exited 0 for all ten locked wheels. This lock targets CPython 3.12/macOS ARM64; application environment locks are still deferred.
- Docker settings: first verified no running user containers with `docker --context desktop-linux ps --format '{{.ID}} {{.Names}}'` (empty, exit 0). Stopped Desktop with `docker desktop stop` (exit 0); backed up settings to ignored `.tools/docker-settings-before-project-setup.json`; set only `Cpus=4`, `MemoryMiB=6144`, and `KubernetesEnabled=false` in settings-store.json, preserving other settings. Restarted with `docker desktop start --detach --timeout 30` (exit 0). Engine verification confirms applied CPU/memory values. Existing stopped containers/volumes were not deleted. Disk-image capacity was not resized.
- Docker CLI initially hit sandbox socket/log permissions; approved host-access reruns succeeded. These permission failures are not counted as runtime successes.
- GUI inspection was interrupted; no settings success is inferred from it. Settings/configuration and API results below supply the actual evidence.

### Actual container and cluster smoke

```sh
docker --context desktop-linux run --rm --network none --platform linux/arm64 python:3.12.9-slim-bookworm@sha256:40011d5cf225eb43b650ec158d4a59ca3110e6566ddfd3f506824db6153761cf python -c 'import sys,platform; assert sys.version_info[:3] == (3,12,9); assert platform.machine() == "aarch64"; print(sys.version); print(platform.machine()); print("container runtime smoke passed")'
```

Exit 0: image pulled at the pinned ARM64 manifest digest; Python **3.12.9**, **aarch64**, `container runtime smoke passed`. The diagnostic container had no network and was automatically removed; its image remains cached.

```sh
source .venv/bin/activate
unset DOCKER_HOST DOCKER_CONTEXT
export KIND_EXPERIMENTAL_PROVIDER=docker
kind create cluster --name smart-grid-dev --image kindest/node:v1.34.0@sha256:7416a61b42b1662ca6ca89f02028ac133a309a2a30ba309614e8ec94d976dc5a --wait 120s
```

Exit 0: cluster created; reported ready after 17 seconds once the node image was available. Kubeconfig was merged with context `kind-smart-grid-dev`; existing contexts were preserved. No application services or project operator resources were deployed. The kind node, CNI, CoreDNS, and local-path provisioner are the empty-cluster prerequisites supplied by kind itself.

### Final runtime checks (all exit 0)

**`docker --context desktop-linux version`**

```text
Client: Docker Engine - Community
 Version:           29.8.0
 API version:       1.56
 Go version:        go1.27.1
 Git commit:        88096ef005
 Built:             Thu Sep  3 21:47:32 2026
 OS/Arch:           darwin/arm64
 Context:           desktop-linux

Server: Docker Desktop 4.99.0 (245822)
 Engine:
  Version:          29.9.3
  API version:      1.56 (minimum version 1.40)
  Go version:       go1.26.10
  Git commit:       70efc0e
  Built:            Tue Nov  3 21:41:25 2026
  OS/Arch:          linux/arm64
  Experimental:     false
 containerd:
  Version:          v2.4.2
  GitCommit:        b7a25261e029d68978bbf290df0a62fc819b3c51
 runc:
  Version:          1.5.2
  GitCommit:        v1.5.2-0-g29dd3dc2
 docker-init:
  Version:          0.19.0
  GitCommit:        de40ad0
```

**`docker --context desktop-linux info --format '{{.ServerVersion}} {{.OSType}}/{{.Architecture}} CPUs={{.NCPU}} Memory={{.MemTotal}}'`**

```text
29.9.3 linux/aarch64 CPUs=4 Memory=6210510848
```

**`docker desktop kubernetes status`**

```text
Field               Value
ID:                 35aedc70
State:              disabled
Mode:               kind
Node Count:         1
Version:            1.36.1
Progress Message:   Kubernetes is stopped
Error:              None
```

**`.venv/bin/kind get clusters`**

```text
smart-grid-dev
```

**`.venv/bin/kubectl --context kind-smart-grid-dev get --raw /readyz`**

```text
ok
```

**`.venv/bin/kubectl --context kind-smart-grid-dev get nodes -o wide`**

```text
NAME                           STATUS   ROLES           AGE    VERSION   INTERNAL-IP   EXTERNAL-IP   OS-IMAGE                         KERNEL-VERSION    CONTAINER-RUNTIME
smart-grid-dev-control-plane   Ready    control-plane   2m4s   v1.34.0   172.19.0.2    <none>        Debian GNU/Linux 12 (bookworm)   7.0.14-linuxkit   containerd://2.1.3
```

**`.venv/bin/kubectl --context kind-smart-grid-dev get storageclass`**

```text
NAME                 PROVISIONER             RECLAIMPOLICY   VOLUMEBINDINGMODE      ALLOWVOLUMEEXPANSION   AGE
standard (default)   rancher.io/local-path   Delete          WaitForFirstConsumer   false                  2m2s
```

**`.venv/bin/kubectl --context kind-smart-grid-dev get pods -A`**

```text
NAMESPACE            NAME                                                   READY   STATUS    RESTARTS   AGE
kube-system          coredns-66bc5c9577-d9gng                               1/1     Running   0          115s
kube-system          coredns-66bc5c9577-p84tn                               1/1     Running   0          115s
kube-system          etcd-smart-grid-dev-control-plane                      1/1     Running   0          2m4s
kube-system          kindnet-xwsh6                                          1/1     Running   0          116s
kube-system          kube-apiserver-smart-grid-dev-control-plane            1/1     Running   0          2m3s
kube-system          kube-controller-manager-smart-grid-dev-control-plane   1/1     Running   0          2m3s
kube-system          kube-proxy-z8fk2                                       1/1     Running   0          116s
kube-system          kube-scheduler-smart-grid-dev-control-plane            1/1     Running   0          2m3s
local-path-storage   local-path-provisioner-7b8c8ddbd6-qq6ql                1/1     Running   0          115s
```

**`docker --context desktop-linux ps --format '{{.Names}}'`**

```text
smart-grid-dev-control-plane
```

Desktop's disabled built-in Kubernetes metadata mentions version 1.36.1 and a kind provisioner; that is not the running project cluster. The CLI-managed `smart-grid-dev` node is v1.34.0. The default StorageClass is registered, but no PVC read/write/recovery test was executed. No policy-capable CNI or allow/deny test was installed/executed.

### Local image loading into kind

```sh
source .venv/bin/activate
unset DOCKER_HOST DOCKER_CONTEXT
docker --context desktop-linux image tag python:3.12.9-slim-bookworm@sha256:40011d5cf225eb43b650ec158d4a59ca3110e6566ddfd3f506824db6153761cf smart-grid/prerequisite-smoke:3.12.9
kind load docker-image smart-grid/prerequisite-smoke:3.12.9 --name smart-grid-dev
docker --context desktop-linux exec smart-grid-dev-control-plane crictl images smart-grid/prerequisite-smoke
```

The load completed successfully. Node image-store output includes `docker.io/smart-grid/prerequisite-smoke`, tag `3.12.9`, image ID prefix `916619b289581`, size 45 MB. This verifies the local Docker-to-kind image path on the installed versions without a registry upload, application build, or application deployment. The diagnostic image/tag remains cached in the local engine/node.

### Final local checks

```sh
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
git diff --check
git check-ignore .venv/bin/python .venv/bin/kind .tools/python/cpython-3.12.9-macos-aarch64-none/bin/python3.12
```

All exit 0. Activation selects project `.venv/bin` for Python/pip/kind/kubectl/Helm. Versions: Python **3.12.9**, kind **0.30.0**, kubectl **1.34.0**, bundled Kustomize **5.7.1**, Helm **3.19.0**. `pip check`: **No broken requirements found**. Tests: **10 passed**. Native binaries were also checked with `file`: each reports Mach-O 64-bit ARM64. `git check-ignore` confirms no interpreter/native binaries are included in the change set. Colima remains stopped.

### Scope and remaining gates

Docker Desktop and the empty kind cluster are left running. No application scaffolding, cloud resources, paid APIs, commits, or pushes occurred. Preparation libraries are installed/locked; application libraries are not. Full Helm/operator deployment, application tests, actual persistent-volume I/O/recovery, enforced network isolation, larger resource-profile fit, and PyFlink ARM64 source build/checkpoint verification remain open. Prior read-only inventories below are historical and must not override this setup result.

---

## Pre-setup prerequisite recheck — 2026-11-21, approximately 19:19 PST (historical)

This section supersedes the initial tool-readiness snapshot below. Existing uncommitted preparation changes were preserved. No software was installed, runtime started, cluster created, or Docker/Kubernetes configuration changed. Missing commands and unsuccessful runtime probes are **not complete checks**. All exact command results below are from this rerun; discovery in the documented user-local install paths also found no selected binaries.

| Readiness gate | Verified observation | Complete? |
|---|---|---|
| Git | 2.54.0 Apple Git-157 executes | Yes: installed executable only |
| Host architecture/capacity | ARM64; 16 GiB total RAM; 35% macOS free-memory metric; 268 GiB free disk | Yes: observations, not workload fit |
| Docker CLI / Buildx | Docker 29.8.0; Buildx 0.38.1 execute | Yes: clients only |
| Docker daemon | Both version/info fail at the missing desktop-linux socket | No |
| Colima runtime | 0.10.3 installed; existing default profile stopped, aarch64, 2 CPU/4 GiB/20 GiB, docker+k3s | No: VM not running or sized for selected profile |
| Selected Python | Host default 3.14.7 executes; 3.12.9 absent; pyenv absent | No: selected interpreter/container not run |
| kind | Version and cluster-list commands exit 127 | No |
| Selected kubectl | Installed 1.37.2; desired 1.34.0 absent | No: selected client missing |
| Existing Kustomize | kubectl reports 5.8.1; help executes | Yes: existing bundle only; selected bundle/render pending |
| Helm | Version command exits 127 | No |
| jq | Apple 1.7.1 executes | Yes: optional tool |
| Kubernetes deployment | Current kubeconfig context is `colima`; kind unavailable and daemon stopped | No: no API server/readiness/storage probe performed |

### Exact rerun commands

Each command was executed with a 20-second timeout; no command timed out. Colima's version command exited 0 but also emitted a macOS system_profiler Activation Lock check timeout warning. That warning does not establish a running runtime.

**`git --version` — exit 0**

```text
git version 2.54.0 (Apple Git-157)
```

**`uname -m` — exit 0**

```text
arm64
```

**`sw_vers` — exit 0**

```text
ProductName:		macOS
ProductVersion:		27.0.1
BuildVersion:		26A434
```

**`df -h .` — exit 0**

```text
Filesystem      Size    Used   Avail Capacity iused ifree %iused  Mounted on
/dev/disk3s5   460Gi   154Gi   268Gi    37%    1.6M  2.8G    0%   /System/Volumes/Data
```

**`memory_pressure -Q` — exit 0**

```text
The system has 17179869184 (1048576 pages with a page size of 16384).
System-wide memory free percentage: 35%
```

**`command -v git docker colima python3 python3.12 kind kubectl helm jq node npm java javac mvn pyenv` — exit 1**

```text
/usr/bin/git
/opt/homebrew/bin/docker
/opt/homebrew/bin/colima
/opt/homebrew/bin/python3
/opt/homebrew/bin/kubectl
/usr/bin/jq
/usr/bin/java
/usr/bin/javac
/opt/homebrew/bin/mvn
```

**`whence -a python3 python3.12 kind kubectl helm docker colima` — exit 0**

```text
/opt/homebrew/bin/python3
/usr/bin/python3
/opt/homebrew/bin/python3
/opt/homebrew/bin/kubectl
/opt/homebrew/bin/kubectl
/Users/sonit/.docker/bin/kubectl
/opt/homebrew/bin/docker
/opt/homebrew/bin/docker
/Users/sonit/.docker/bin/docker
/opt/homebrew/bin/colima
/opt/homebrew/bin/colima
```

**`docker version` — exit 1**

```text
Client: Docker Engine - Community
 Version:           29.8.0
 API version:       1.56
 Go version:        go1.27.1
 Git commit:        88096ef005
 Built:             Thu Sep  3 21:47:32 2026
 OS/Arch:           darwin/arm64
 Context:           desktop-linux
failed to connect to the docker API at unix:///Users/sonit/.docker/run/docker.sock; check if the path is correct and if the daemon is running: dial unix /Users/sonit/.docker/run/docker.sock: connect: no such file or directory
```

**`docker info --format '{{.ServerVersion}} {{.OSType}}/{{.Architecture}} CPUs={{.NCPU}} Memory={{.MemTotal}}'` — exit 1**

```text
/ CPUs=0 Memory=0
failed to connect to the docker API at unix:///Users/sonit/.docker/run/docker.sock; check if the path is correct and if the daemon is running: dial unix /Users/sonit/.docker/run/docker.sock: connect: no such file or directory
```

**`docker context ls` — exit 0**

```text
NAME              DESCRIPTION                               DOCKER ENDPOINT                                   ERROR
colima            colima                                    unix:///Users/sonit/.colima/default/docker.sock
default           Current DOCKER_HOST based configuration   unix:///var/run/docker.sock
desktop-linux *   Docker Desktop                            unix:///Users/sonit/.docker/run/docker.sock
```

**`colima version` — exit 0**

```text
colima version 0.10.3
git commit: 00f6c297e92a82c04a4ab507db0a61435650d7e8
2026-11-21 19:19:43.434 system_profiler[84125:17574579] Timed out waiting for the Activation Lock Capable check
```

**`colima list` — exit 0**

```text
PROFILE    STATUS     ARCH       CPUS    MEMORY    DISK     RUNTIME       ADDRESS
default    Stopped    aarch64    2       4GiB      20GiB    docker+k3s
```

**`colima status` — exit 1**

```text
time="2026-11-21T19:19:33-08:00" level=fatal msg="colima is not running"
```

**`python3 --version` — exit 0**

```text
Python 3.14.7
```

**`python3.12 --version` — exit 127**

```text
zsh:1: command not found: python3.12
```

**`kind version` — exit 127**

```text
zsh:1: command not found: kind
```

**`kind get clusters` — exit 127**

```text
zsh:1: command not found: kind
```

**`kubectl version --client --output=json` — exit 0**

```text
{
  "clientVersion": {
    "major": "1",
    "minor": "37",
    "gitVersion": "v1.37.2",
    "gitCommit": "c50e5395f1ae2d4a2c05f28fdcc38c0916cc1a32",
    "gitTreeState": "clean",
    "buildDate": "2026-10-27T12:01:04Z",
    "goVersion": "go1.26.10",
    "compiler": "gc",
    "platform": "darwin/arm64"
  },
  "kustomizeVersion": "v5.8.1"
}
```

**`kubectl kustomize --help` — exit 0**

```text
Build a set of KRM resources using a 'kustomization.yaml' file. The DIR argument must be a path to a directory containing 'kustomization.yaml', or a git repository URL with a path suffix specifying same with respect to the repository root. If DIR is omitted, '.' is assumed.
(help body omitted; exit status retained)
```

**`kubectl config current-context` — exit 0**

```text
colima
```

**`helm version --short` — exit 127**

```text
zsh:1: command not found: helm
```

**`jq --version` — exit 0**

```text
jq-1.7.1-apple
```

**`node --version` — exit 127**

```text
zsh:1: command not found: node
```

**`npm --version` — exit 127**

```text
zsh:1: command not found: npm
```

**`java -version` — exit 1**

```text
The operation couldn’t be completed. Unable to locate a Java Runtime.
Please visit http://www.java.com for information on installing Java.
```

**`javac -version` — exit 1**

```text
The operation couldn’t be completed. Unable to locate a Java Runtime.
Please visit http://www.java.com for information on installing Java.
```

**`mvn --version` — exit 0**

```text
Apache Maven 3.10.1 (3afd8084b5629f80b97750906a719879ed4cd283)
Maven home: /opt/homebrew/Cellar/maven/3.10.1/libexec
Java version: 27.0.1.1, vendor: Homebrew, runtime: /opt/homebrew/Cellar/openjdk/27.0.1.1/libexec/openjdk.jdk/Contents/Home
Default locale: en_US, platform encoding: UTF-8, time zone: America/Los_Angeles
OS name: "mac os x", version: "27.0.1", arch: "aarch64", family: "mac"
```

**`pyenv versions --bare` — exit 127**

```text
zsh:1: command not found: pyenv
```

**`for candidate in "$HOME/.local/bin/kind" "$HOME/.local/bin/kubectl" "$HOME/.local/bin/helm" "$HOME/.pyenv/versions/3.12.9/bin/python3"; do if test -x "$candidate"; then ls -l "$candidate"; file "$candidate"; fi; done` — exit 0**

```text
(no output)
```

Additional commands: `docker buildx version` exited 0 (`v0.38.1`); `docker manifest inspect --help` and `colima start --help` exited 0. `python3 -m pip --version` exited 0 (pip 26.2.1); the existing pip-vendored `packaging` module was used for metadata comparisons, without installing it. Java/javac fail through the shell, Node/npm remain absent, and Maven 3.10.1 executes using its internal Java 27; none is a host prerequisite.

Additional alternate-path inventory: `"$HOME/.docker/bin/kubectl" version --client --output=json` exited 0 and reported kubectl **1.36.1**, Kustomize **5.8.1**, darwin/arm64. This alternative is still two minor releases ahead of the selected Kubernetes 1.34, so it does not close the matching-client gate. `brew list --versions python@3.12 kind helm kubernetes-cli colima docker` listed `colima 0.10.3`, `docker 29.8.0`, and `kubernetes-cli 1.37.2`; no selected Python/kind/Helm formula was listed.

### Registry metadata verification

Initial sandboxed registry requests failed DNS resolution. Repeating the same **read-only** requests with approved network access returned exit 0 for all six images. No image layers were pulled. These inspect commands work without a Docker daemon, so the earlier implication that manifest inspection required a daemon was incorrect. Only registry availability/platform metadata is complete; startup, health, and behavior are unverified.

| Exact command | Verified Linux ARM64 manifest digest |
|---|---|
| `docker buildx imagetools inspect --raw kindest/node:v1.34.0@sha256:7416a61b42b1662ca6ca89f02028ac133a309a2a30ba309614e8ec94d976dc5a` | `sha256:3229b411042d457a44ea8807e90520e4b7da8700a8617ed20f0c4cb799bebe2b` |
| `docker buildx imagetools inspect --raw flink:2.2.1-scala_2.12-java17` | `sha256:b499dd3fad113d67960786dde24ddc75bc60adea94d514ef3a609e6a84818e66` |
| `docker buildx imagetools inspect --raw quay.io/strimzi/kafka:0.45.2-kafka-3.9.2` | `sha256:6f3fbd752db17a47115beab6dad1dd98d8b5c901ecfb41a9ffa9464abfd080d3` |
| `docker buildx imagetools inspect --raw postgres:17.4-bookworm` | `sha256:51a22dd7011534315b3c3f641e4a27204e90ce543674c945072176147824834e` |
| `docker buildx imagetools inspect --raw quay.io/keycloak/keycloak:26.1.4` | `sha256:78909e12f706c6584c4c2eeca9c4964a2ae58903f166fb942236f6fbb1ef8a96` |
| `docker buildx imagetools inspect --raw python:3.12.9-slim-bookworm` | `sha256:40011d5cf225eb43b650ec158d4a59ca3110e6566ddfd3f506824db6153761cf` |

### Python dependency metadata and corrected pins

Read-only JSON requests to the official PyPI endpoints succeeded for `nicegui/3.19.0`, `nicegui/2.24.2`, `fastapi/0.116.1`, `pydantic/2.11.7`, and `apache-flink/2.2.1`; the unversioned FastAPI/Pydantic/Starlette endpoints reported **0.165.0 / 2.14.4 / 1.10.0** respectively. Full URL pattern: `https://pypi.org/pypi/<name>/<version>/json` (omit the version segment for the discovery requests). Pins now use the exact discovered versions; future verification should request those versioned URLs. No wheels or dependencies were installed.

NiceGUI **3.19.0** replaces 2.24.2 following upstream [GHSA-m47w-xwfc-82pf](https://github.com/zauberzeug/nicegui/security/advisories/GHSA-m47w-xwfc-82pf) (fixed in 3.19.0) and [GHSA-jfrm-rx66-g536](https://github.com/zauberzeug/nicegui/security/advisories/GHSA-jfrm-rx66-g536) (fixed in 3.12.0). This is a targeted correction, not a comprehensive vulnerability scan.

The patched UI package requires Starlette >=1.3.1 and pydantic-core >=2.35.0. The old FastAPI 0.116.1 requires Starlette <0.48, and Pydantic 2.11.7 pins core 2.33.2. Therefore merely upgrading NiceGUI would create conflicts. The shared API/UI baseline is now NiceGUI **3.19.0**, FastAPI **0.165.0**, Starlette **1.10.0**, Pydantic **2.14.4** (core **2.50.5**).

The following nine comparisons were executed using `pip._vendor.packaging.requirements.Requirement` / `SpecifierSet` against downloaded metadata, evaluating Python 3.12.9 / Linux aarch64 / no optional extras:

- PASS: nicegui supports Python 3.12.9.
- PASS: nicegui requires fastapi!=0.123.5,>=0.109.1; selected 0.165.0.
- PASS: nicegui requires starlette>=1.3.1; selected 1.10.0.
- PASS: fastapi supports Python 3.12.9.
- PASS: fastapi requires starlette>=1.6.0; selected 1.10.0.
- PASS: fastapi requires pydantic>=2.9.0; selected 2.14.4.
- PASS: pydantic supports Python 3.12.9.
- PASS: starlette supports Python 3.12.9.
- PASS: Pydantic core 2.50.5 satisfies NiceGUI pydantic-core>=2.35.0.

These complete only the listed version-constraint checks. Full transitive resolution, wheel availability for every dependency, installation, imports, browser/SSE behavior, and security scanning are pending. The selected PyFlink release file listing still has no Linux ARM64 wheel; native source build remains unverified.

### Documentation corrections and checks

- Explicitly disable Colima's optional k3s in both proposed named profiles. The existing default profile already contains k3s and is not the selected kind profile.
- Clear `DOCKER_HOST`/`DOCKER_CONTEXT` before switching Docker contexts. A previously exported endpoint overrides `docker context use`; clear Testcontainers overrides when moving to the review profile too.
- Distinguish client version/help checks, selected-version readiness, remote manifest availability, and actual runtime smoke tests. Default Docker-info template zeros on connection failure are not machine capacity measurements.
- Preserve the original snapshot below as history. Selected tools/runtime are not marked complete merely because installation instructions exist.

Verification command: `python3 -m unittest discover -s tests -p 'test_preparation_docs.py' -v` — **5 tests passed** after the changes. `git diff --check` — **exit 0**. `git status --short` and `git diff --stat` were inspected; only documentation and the pre-existing documentation test are in the worktree, with no service scaffolding, installation, commit, or push.

## Original preparation snapshot (historical)

Executed on 2026-11-21, America/Los_Angeles, in this repository. These are actual read-only diagnostics. Commands documenting installation/startup in LOCAL_PREREQUISITES.md were **not executed**. Initial git status was clean; the repository contained Markdown designs/component placeholders, no executable schemas, services, manifests, or test harness. No existing AGENTS.md was found in the repository or its inspected parent tree.

## Machine capacity

The sandbox initially denied sysctl reads; rerunning the read-only hardware query with approved access succeeded:

```sh
sysctl -n hw.memsize
sysctl -n hw.ncpu
sysctl -n machdep.cpu.brand_string
sw_vers
```

Results respectively: `17179869184` bytes (16 GiB), `10`, `Apple M5`, macOS `27.0.1` build `26A434`. `uname -m` returned `arm64`; `df -h .` reported 268 GiB available on a 460 GiB filesystem. These are total memory/disk observations, not available workload RAM measurements.

Additional read-only commands `vm_stat` and `memory_pressure -Q` exited 0. The snapshot had 7,810 free pages of 16,384 bytes (about 122 MiB immediately free); macOS reported a system-wide free-memory percentage of 33%. These are different OS accounting views, not a promise that a new 6/10 GiB VM can run alongside the current apps. Close memory-heavy apps and recheck before starting a profile. `colima start --help` and `colima status --help` exited 0 and confirmed named profile syntax and the documented resource flags; help commands did not start Colima.

## Exact tool commands and results

Each command below was executed separately through a subprocess with its exit status retained. Expected missing/stopped tools are failures and are not counted as passing prerequisite checks.

### `git status --short`

Exit code: **0**.

```text
(no output)
```

### `git --version`

Exit code: **0**.

```text
git version 2.54.0 (Apple Git-157)
```

### `uname -m`

Exit code: **0**.

```text
arm64
```

### `sw_vers`

Exit code: **0**.

```text
ProductName:		macOS
ProductVersion:		27.0.1
BuildVersion:		26A434
```

### `df -h .`

Exit code: **0**.

```text
Filesystem      Size    Used   Avail Capacity iused ifree %iused  Mounted on
/dev/disk3s5   460Gi   154Gi   268Gi    37%    1.6M  2.8G    0%   /System/Volumes/Data
```

### `docker version`

Exit code: **1**.

```text
Client: Docker Engine - Community
 Version:           29.8.0
 API version:       1.56
 Go version:        go1.27.1
 Git commit:        88096ef005
 Built:             Thu Sep  3 21:47:32 2026
 OS/Arch:           darwin/arm64
 Context:           desktop-linux
failed to connect to the docker API at unix:///Users/sonit/.docker/run/docker.sock; check if the path is correct and if the daemon is running: dial unix /Users/sonit/.docker/run/docker.sock: connect: no such file or directory
```

### `docker context ls`

Exit code: **0**.

```text
NAME              DESCRIPTION                               DOCKER ENDPOINT                                   ERROR
colima            colima                                    unix:///Users/sonit/.colima/default/docker.sock
default           Current DOCKER_HOST based configuration   unix:///var/run/docker.sock
desktop-linux *   Docker Desktop                            unix:///Users/sonit/.docker/run/docker.sock
```

### `colima version`

Exit code: **0**.

```text
colima version 0.10.3
git commit: 00f6c297e92a82c04a4ab507db0a61435650d7e8
```

### `colima status`

Exit code: **1**.

```text
time="2026-11-21T19:09:01-08:00" level=fatal msg="colima is not running"
```

### `python3 --version`

Exit code: **0**.

```text
Python 3.14.7
```

### `python3.12 --version`

Exit code: **127**.

```text
zsh:1: command not found: python3.12
```

### `node --version`

Exit code: **127**.

```text
zsh:1: command not found: node
```

### `npm --version`

Exit code: **127**.

```text
zsh:1: command not found: npm
```

### `java -version`

Exit code: **1**.

```text
The operation couldn’t be completed. Unable to locate a Java Runtime.
Please visit http://www.java.com for information on installing Java.
```

### `javac -version`

Exit code: **1**.

```text
The operation couldn’t be completed. Unable to locate a Java Runtime.
Please visit http://www.java.com for information on installing Java.
```

### `/opt/homebrew/opt/openjdk@17/bin/java -version`

Exit code: **0**.

```text
openjdk version "17.0.21.1" 2026-11-17
OpenJDK Runtime Environment Homebrew (build 17.0.21.1+0)
OpenJDK 64-Bit Server VM Homebrew (build 17.0.21.1+0, mixed mode, sharing)
```

### `kubectl version --client`

Exit code: **0**.

```text
Client Version: v1.37.2
Kustomize Version: v5.8.1
```

### `kind version`

Exit code: **127**.

```text
zsh:1: command not found: kind
```

### `helm version --short`

Exit code: **127**.

```text
zsh:1: command not found: helm
```

### `jq --version`

Exit code: **0**.

```text
jq-1.7.1-apple
```

### `grpcurl -version`

Exit code: **127**.

```text
zsh:1: command not found: grpcurl
```

### `k6 version`

Exit code: **127**.

```text
zsh:1: command not found: k6
```

### `brew --version`

Exit code: **0**.

```text
Homebrew 7.0.15
```

## Additional inspected tools and repository evidence

Initial inventory also executed `mvn --version`: Maven 3.10.1 was installed and selected Homebrew Java 27.0.1.1 internally. `/opt/homebrew/opt/openjdk@17/bin/javac -version` returned 17.0.21.1; `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home mvn --version` selected Java 17.0.21.1. These are inventory findings only; host JDK/Maven setup is unnecessary for the accepted stack. `command -v` found no podman, Node/npm, kind, Helm, grpcurl, or k6.

Repository inspection commands included `git status --short`, `git log -1 --oneline`, `rg --files`, `find .. -name AGENTS.md -print`, and targeted `cat`/`sed` reads of the master guide, decision register, scope, open-source stack, design, team, testing, and contract documents. The initial HEAD was `f7f3d51` (`Enhance documentation with master implementation guide and clarify Kubernetes deployment strategy`). No existing implementation/executable contract overrode the design during this milestone.

## Documentation verification

```sh
python3 -m unittest discover -s tests -p 'test_preparation_docs.py' -v
git diff --check
git diff --stat
git status --short
```

**Executed results:** 5 tests passed (including broken-link and invalid-shell failure cases); `git diff --check` exited 0. `git diff --stat` and `git status --short` exited 0 and showed only this milestone’s documentation and documentation-test changes. No service, dependency manifest, deployment manifest, or machine configuration was added. Tests cover repository relative-link targets, working/missing link cases, shell-instruction syntax without execution, and rejection of invalid shell syntax. They make no network requests and do not install or start anything. No application tests exist yet because application scaffolding is outside this milestone.

## Upstream evidence and limits

The [version matrix](VERSION_MATRIX.md) links official project compatibility tables, release documentation, chart metadata, and PyPI package artifacts consulted during preparation. Browsing was read-only. A compatibility range is documentary evidence, not proof of an image build/deployment on this host. PyFlink Linux ARM64 source build, pip resolution, container/chart integrity/architecture inspection, security scans, kind, storage, and full-stack resource fit were not executed. No cloud resource, paid API, model call, installation, commit, or push occurred.
