# Final demo and evidence index

Run the [README sequence](../../README.md#reproduce-the-local-demo) with a new name such as `smart-grid-kind-dev-demo01`. Keep build/acceptance output and the named directory in `.tools`. The Investigator uses `MOCK` with local Keycloak users.

1. **Member_1** shows the cluster inspection, the seeded simulator, workload readiness, and Calico ingress/egress checks. Open the [historical incident packet runbook](../../infra/observability/README.md) for one correlated SQL timeout, error metric, trace and log.
2. **Member_2** shows Kafka partitions, a committed critical load, and the Flink completed → restored → completed checkpoint IDs. Explain the three-broker review evidence and single-node limits.
3. **Member_3** opens the committed action in the dashboard. Match the action and command IDs with the report. Show the measured EV drop, two logical effects despite replay, and safe return to 7.2 kW. Show the authoritative PostgreSQL restore comparison and describe the fence.
4. **Member_4** shows the same action in the browser, REST and SSE. Open the completed action and select **Investigate action** as Operator, use “What happened during this incident?”, and inspect citations and unknowns. Show the Viewer and unauthorized write denial in the acceptance evidence.
5. Review the [design audit](DESIGN_AUDIT.md), [load results](../testing/LOAD_TEST_REPORT.md) and this run's gate statuses. Stop the cluster to keep state, or export needed evidence and explicitly reset it as in the README.

## Evidence index

| Evidence | Path or source |
|---|---|
| Suite, source identity, gates, and mock mode | Named kind directory `acceptance.json` and `source-hashes.json` |
| Builds and running images | `images.json` and `runtime-images.json` |
| Simulator, events, duplicate effects, replay, REST/SSE, smoke status | `simulator.log`, `evidence.json`, `report.json` |
| Browser proof and Investigator | `browser.png`, `investigator.png`, `investigator.json` |
| TaskManager recovery and resource observations | `flink-recovery.json`, `resources.jsonl` |
| SQL archives, checksums and scratch comparison | `authoritative-backup/` including `backup.json` and `restore.json` |
| Component and pairwise reports | [Implementation status](../implementation/IMPLEMENTATION_STATUS.md) and [live integration](../implementation/LOCAL_INTEGRATION.md) |
| Incident metric, trace, logs and SLI | [Incident diagnosis runbook](../runbooks/INCIDENT_DIAGNOSIS.md) |
| Render output, dependency/image scans and SBOMs | `.tools/delivery-checks/` or the delivery workflow artifact when hosted checks run |

Evidence is private when it contains local data, credentials, or PostgreSQL archives. Share curated screenshots and status summaries. Check `acceptance.json` for missing or failed stages before presenting results.
