# ADR-006 — Local kind is the deployment target

- Status: Accepted, 2026-11-21
- Authority: User instruction: one computer, local infrastructure, free tools, no cloud resources

## Context and decision

The previous guide recommended shared AWS EKS and treated local Kubernetes as a fallback. That contradicts the approved delivery environment. **Local kind is required** for integration and demonstration. Cloud deployment, cloud registries, AWS accounts/CLI/eksctl, paid API calls, and an external model provider are deferred. No cloud credentials are prerequisites. Deterministic model mocks are the default for development, evaluation, and review.

Use **Docker Desktop 4.99.0** on Apple Silicon, with a native ARM64 engine and CLI-managed kind nodes. The user explicitly selected Desktop in the environment-setup follow-up; this supersedes the initial Colima choice. Keep Docker Desktop’s built-in Kubernetes disabled. Installed Colima remains stopped and unmodified. Desktop is free for this educational project under its [license terms](https://docs.docker.com/subscription-billing/desktop-license/). Build images locally and use `kind load docker-image`; no container registry is required. Pin kind, its node image digest, a matching kubectl minor, and Helm in the [matrix](../implementation/VERSION_MATRIX.md).

The machine has 10 CPU cores and 16 GiB RAM; the initial inventory recorded 268 GiB free disk. Adopt the [prerequisites guide's resource profiles](../implementation/LOCAL_PREREQUISITES.md#resource-profiles): a 4-CPU/6-GiB development VM and an 8-CPU/10-GiB review VM, used separately. These are planning ceilings, not measured capacity claims. Review exercises may run in stages on the same computer; retain three Kafka brokers for broker-failure evidence and two Action/API replicas for the corresponding exercises. A kind cluster on one laptop cannot demonstrate physical host availability.

Kafka uses KRaft combined broker/controller nodes: one in development, three in review; no ZooKeeper. Start with three partitions per domain topic, RF=1/minISR=1 in development and RF=3/minISR=2 in review, producers `acks=all`. Internal Connect/transaction/offset topics must use profile-appropriate replication too. Keep 24-hour local event retention and 1-hour DLQ/late audit inspection minimum; use 24 hours for both initially. Topic keys and semantics remain canonical contracts; never change partition count during an ordered scenario. Retention/scale can be lowered only with explicit evidence requirements preserved.

## Alternatives and consequences

EKS adds cost, credentials, remote access, and lifecycle work; deferred. Minikube duplicates the selected local target; excluded from required setup. Compose may run minimal component dependencies later, but is unnecessary in this preparation milestone. The final full-stack resource fit, image builds, volume recovery, and deployment remain future verification gates.

## Environment setup amendment — 2026-11-21

The user authorized installing missing prerequisites and starting/configuring the local environment. Docker Desktop was already installed; Engine 29.9.3 and CLI 29.8.0 communicate successfully. Set development resources to 4 CPUs/6144 MiB; the engine reports 6,210,510,848 usable guest bytes after VM overhead. The larger 8-CPU/10-GiB review profile remains proposed. Disk-image capacity and unrelated Docker data/settings were preserved; 40/80-GiB figures are usage budgets, not resized disks.

Colima is a valid alternative but no longer the selected runtime. Docker Desktop offers the requested GUI and requires fewer project-specific VM lifecycle commands. kind remains mandatory; enabling Desktop's separately managed Kubernetes would create a competing cluster/version. All prerequisite Python tooling uses `.venv`; no global Python packages, JDK, or Node.js were installed. See current environment evidence (private evidence).
