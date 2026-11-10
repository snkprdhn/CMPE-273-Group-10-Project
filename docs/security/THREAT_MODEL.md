# Threat Model

## Protected assets

Control authority, transformer policy, action/saga state, command integrity, service credentials, audit history, and operator identity are protected assets. All project data is synthetic, but unauthorized commands remain a correctness and safety risk within the simulation.

## Threats and planned controls

| Threat | Control and verification |
|---|---|
| Forged telemetry | Cluster-internal broker access, producer identity/configuration, schema/device validation; Kafka ACLs are stretch hardening |
| Unauthorized command publisher | NetworkPolicy, dedicated service identity/configuration, and schema validation; topic ACLs are stretch hardening |
| Stale leader command | Fencing epoch and device-side rejection |
| Replay/duplicate | Event IDs, inbox, command idempotency ledger |
| UI privilege escalation | OIDC roles, server authorization, negative tests |
| East-west lateral access | ServiceAccounts and NetworkPolicies |
| Secret disclosure | Secret storage, redaction, scanning, no secrets in images/logs |
| Resource exhaustion | Payload/rate limits, bulkheads, bounded queues, quotas |
| Prompt/tool injection | Read-only allowlist, typed tools, authorization, human approval |
| Agent data exfiltration | Synthetic/redacted field allowlist, restricted egress, provider credential isolation, bounded audit data |
| Unsupported agent diagnosis | Evidence-ID validation, freshness metadata, observations/hypotheses separation, operator verification |
| Model-provider failure or abuse | Deadline, rate/concurrency limit, breaker, deterministic mock mode, and no control-path dependency |
| Misleading stale state | Freshness/version metadata and explicit stale UI |

## Required security tests

Viewer cannot start a scenario or investigation; Operator cannot change administrator policy; simulator cannot publish commands; Action Service cannot publish raw telemetry; invalid/expired identity is rejected; stale epoch and expired command are rejected; blocked network paths remain unreachable; and secrets do not appear in logs or traces. Agent tests also attempt a write tool, arbitrary PromQL/SQL, prompt injection in retrieved text, cross-entity access outside the caller's scope, oversized time windows, and provider-secret disclosure; all must be rejected or redacted.
