# Incident Diagnosis Runbook

1. State the user-visible symptom and affected transformer/API.
2. Freeze an absolute time window and timezone; compare an equal baseline window.
3. Optionally open a scoped Grid Incident Investigator session to gather candidate evidence; do not treat its conclusion as authoritative.
4. Check relevant SLI, error budget, freshness, Kafka lag, checkpoint age, and ready replicas.
5. Open a representative trace and identify the slow or failing boundary.
6. Follow trace/correlation IDs into structured logs and action/event history.
7. Verify each agent observation against its linked evidence and record omissions or unsupported claims.
8. Record one observation, one hypothesis, one alternative, and one missing signal.
9. Choose a bounded mitigation with rollback criteria; do not blindly retry uncertain effects.
10. Verify recovery using business outcome, technical metrics, and a fresh trace.
11. Preserve incident evidence and document the instrumentation gap.

The runbook distinguishes missing data from zero, correlation from causation, a timeout from a failed write, and platform restart from application-level recovery. The investigator can accelerate evidence gathering, but it cannot choose or execute the mitigation.


## Automated local exercise

Follow the [observability runbook](../../infra/observability/README.md#seeded-incident-packet)
for `tools.observability incident`. It requires an actual SQL timeout/HTTP 503,
queries live Prometheus and Tempo, matches JSON logs by trace ID, records the
operator snapshot impact, and measures both injection-to-recovery and
mitigation-to-recovery. Missing/mismatched evidence fails the run. The injected
lock affects only a fresh run-owned schema; it is released with rollback and
has an eight-second idle transaction safety timeout. The prior five baseline
reads and three sub-second recovery reads are preserved, with the synthetic
fixtures and mixed runtime labeled. This is a controlled database-lock exercise,
not a broker-loss/leader-handover or sustained-capacity test.
