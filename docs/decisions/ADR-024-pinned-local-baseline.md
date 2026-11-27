# ADR-024 — Pinned local implementation baseline

- Status: Accepted, 2026-11-21
- Authority: User authorization to resolve routine preparation decisions

## Decision

Accept [VERSION_MATRIX.md](../implementation/VERSION_MATRIX.md) as the single version source for this baseline. It distinguishes installed tools, selected versions, upstream compatibility evidence, and execution evidence. Exact application/transitive locks, chart archives, checksums, and image digests beyond the kind node are produced and tested when their owning component is implemented. Do not infer a tested deployment from an accepted version choice. The original documentation-only pass installed nothing. The subsequent user-authorized environment setup installs preparation tools in `.venv` and records their exact lock/checksums.

Use separate component virtual environments. In particular, PyFlink's Beam/Protobuf/native dependencies must not be merged with Registry code generation or NiceGUI dependencies into one root environment. Use standard `venv`/pip and hashed component locks; no shared application framework or package manager is needed yet.

Use Strimzi and the Flink Kubernetes Operator as designed. Disable the Flink admission webhook in the local baseline (`webhook.create=false`), using CRD schema validation and offline checks until runtime packaging tests exist. The [upstream quick start](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.15/docs/try-flink-kubernetes-operator/quick-start/) supports disabling that webhook. This avoids adding cert-manager solely for the local cluster; it does not disable the API server's CRD schema validation.

For the security-capable kind profile, select Calico **3.31.0** with IPv4 VXLAN/iptables networking and `disableDefaultCNI: true`; default kind networking is only an empty-cluster/development smoke profile and cannot establish NetworkPolicy isolation. Reuse the [pinned upstream Calico manifest](https://raw.githubusercontent.com/projectcalico/calico/v3.31.0/manifests/calico.yaml), with reviewed VXLAN settings, and require allow/deny tests at the infrastructure gate. No Calico installation or Kubernetes/Docker Desktop integration was executed here.

Install the OTel Collector directly using its upstream Helm chart; omit the OTel Operator and auto-instrumentation. Application owners explicitly instrument Python services. This resolves the open-source plan's otherwise unnecessary second telemetry lifecycle controller and cert-manager dependency. Keep Prometheus Operator via kube-prometheus-stack, Grafana, and single-binary Tempo.

Use the free CloudPirates PostgreSQL Helm chart with the official PostgreSQL image, one persistent primary, and explicit PostgreSQL 17 image override. Disable replication and metrics sidecars unless required by a measured test; CDC uses `wal_level=logical`, replication slots, and pgoutput. Do not rely on a paid image catalog or introduce a database operator. Action owns deployment; service owners retain their isolated roles/schemas. Keycloak uses its own database/role in that primary.

## Alternatives and consequences

An unpinned “latest” stack would change beneath implementation. Installing all infrastructure for every edit would consume the laptop's memory. Instead, provision components as milestones need them and retain independently testable slices. Older pinned releases are a reproducibility baseline, not a claim of current security support; scan/update affected pins before deployment acceptance and rerun compatibility gates. A failed upstream artifact or ARM build gate must be reported and resolved with an updated ADR/matrix, never a silent runtime substitution.

Repository licensing remains an owner decision; no license is invented here. Public release/licensing and SBOM policy are tracked for the foundation milestone and do not block contract authoring. External providers and cloud decisions have no active selection requirement.

## Virtual environment amendment — 2026-11-21

Always use venvs for Python development/testing. `.python-version` selects 3.12.9; the managed interpreter resides in ignored `.tools/python`, and `.venv` is the preparation/contract-tools environment. An ephemeral `/tmp/smart-grid-bootstrap-venv` with uv 0.9.11 fetched that exact interpreter without changing system Python; uv is not a project runtime dependency. The [hashed preparation lock](../../requirements-dev.txt) contains pytest/jsonschema and their installed transitive dependencies for CPython 3.12/macOS ARM64. Application libraries remain independently scoped to future component environments.

Native kind, kubectl, and Helm cannot be installed as Python packages; install official SHA-256-verified binaries into `.venv/bin` so activation selects them without replacing global tools. Recreating `.venv` requires restoring both Python packages and those binaries. Docker Desktop is a host application and runs outside the venv by necessity. This follows the user's “virtual env wherever possible” instruction.
