# Local recovery

Use the exact checkout, kind name, and kubeconfig that created the source. Member_1 owns Registry, Member_2 owns Flink/Kafka, Member_3 owns PostgreSQL/control/CDC, and Member_4 owns identity and projections.

## PostgreSQL export and restore drill

The acceptance runner exports four PostgreSQL databases after the scenario. Registry contains assignments and capabilities. Control contains the saga, inbox/outbox, fencing epochs, and the simulated gateway's effects and result outbox. Keycloak contains the local realm and users. Projections can be rebuilt, but Member_4's operator annotations and mutation audit also live in that database and need backup.

To take a separate export and verify it in scratch databases, choose a new run ID and use the deployed cluster name.

```sh
.venv/bin/python -m tools.delivery_backup export --name "$KIND_NAME" --run export01
.venv/bin/python -m tools.delivery_backup restore-drill --name "$KIND_NAME" --run export01
```

Each private custom-format archive is transactionally consistent within its database. For a coordinated offline export, quiesce writes first, including simulations, Action, Connect, projection consumption, and Keycloak activity, and keep PostgreSQL running. The files and SHA-256 manifest are in `.tools/delivery-backups/$KIND_NAME-export01/`. Acceptance archives are in its `authoritative-backup/` subdirectory. Protect these alongside the matching local credentials if retaining copies.

The restore drill validates all checksums before connecting, creates empty databases with restricted connections, restores schema, table data, ownership and object grants in a transaction, compares every COPY row and sequence value against the archive, and drops the scratch databases. The existing PostgreSQL cluster supplies the owner roles. A passing drill tests the logical dumps and restores. A cross-cluster cutover also needs independently bootstrapped roles and database grants, matching schema and credentials, and an explicit WAL/CDC/Kafka recovery plan. Logical backups omit replication slots, Kafka offsets, PVCs, and the standalone SQLite edge. A reset discards kind volumes, including Kafka and Flink state. Keep writers isolated during recovery and reconcile command IDs and fencing epochs before allowing control to resume.

## Projection views and source replay

The acceptance workload rebuilds a scratch copy of the journal and compares its current views with the live views. To rebuild existing views in place during local maintenance, use the explicit context from the demo checkout.

```sh
grid_kubeconfig=".tools/${KIND_NAME#smart-grid-}.kubeconfig"
.venv/bin/kubectl --kubeconfig "$grid_kubeconfig" --context "kind-$KIND_NAME" -n platform exec deploy/projection -- /opt/venv/bin/python -m member4 rebuild
```

The rebuild takes the store lock, keeps the journal, annotations, and durable source positions, and rotates the SSE generation. Active clients must fetch a fresh snapshot before reconnecting. If the projection database is lost, restore the dump to an isolated store, or create and migrate an isolated store, seed Registry/topology context, and consume with the original source binding from the beginning. Check earliest retained offsets first. The consumer refuses retention gaps. A Kafka-only replay loses operator annotations. See [Kafka source and rebuild semantics](../../member4/README.md#kafka-and-rebuild).

## Flink recovery

The acceptance suite uses the deployed job's REST checkpoint status to confirm a completed checkpoint, deletes only its TaskManager, and waits for the same job ID to become RUNNING with a restored checkpoint and a newer completion. Inspect `flink-recovery.json`. The checkpoint PVC is node-local and Kafka retention is 24 hours.

```sh
.venv/bin/kubectl --kubeconfig "$grid_kubeconfig" --context "kind-$KIND_NAME" -n platform get flinkdeployment/aggregation,pvc/checkpoints
.venv/bin/kubectl --kubeconfig "$grid_kubeconfig" --context "kind-$KIND_NAME" -n platform port-forward --address 127.0.0.1 svc/aggregation-rest 18081:8081
```

In another terminal, open [Flink REST/UI](http://127.0.0.1:18081) and inspect the job's checkpoints and failure history. Preserve the PVC and Kafka source when attempting a TaskManager retry. For an intentional application change use an operator-coordinated savepoint and explicitly compatible restore settings. The shipped deployment uses `upgradeMode: stateless`. Recover from checkpoints through the task restart procedure above. For the standalone Compose job, [Member_2's service runbook](../../services/aggregation/README.md) describes the selected savepoint/checkpoint exercise. A lost kind node requires independent copies of the savepoint and retained Kafka source, plus an explicit restore/replay plan.

To repeat the TaskManager fault on a disposable dev cluster, first confirm a
completed checkpoint in the Flink UI. List the TaskManager and set
`taskmanager_pod` to its exact name, then delete that pod. Verify that the same
job ID reports a restored checkpoint and a later completed checkpoint.

```sh
.venv/bin/kubectl --kubeconfig "$grid_kubeconfig" --context "kind-$KIND_NAME" -n platform get pods -l component=taskmanager,grid-app=aggregation
.venv/bin/kubectl --kubeconfig "$grid_kubeconfig" --context "kind-$KIND_NAME" -n platform delete "pod/$taskmanager_pod" --timeout=90s
```
