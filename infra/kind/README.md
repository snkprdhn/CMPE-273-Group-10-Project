# Local kind deployment

The Smart Grid / EV Fleet Energy Balancing Platform runs on Docker Desktop with
its built-in Kubernetes disabled. Each profile gets a dedicated kind cluster and
kubeconfig. The original host/Compose profiles remain available for independent
component development.

## Run the lightweight profile

Follow the [pinned prerequisites](../../docs/implementation/LOCAL_PREREQUISITES.md)
for Docker Desktop, the project preparation venv, kind, kubectl, Helm, and the
independent contract venv. Stage this profile separately from other local stacks
when working with the 6 GiB Docker VM budget. The smoke test additionally uses the existing dashboard
test venv and Chromium. From a prepared checkout, install that environment if
needed:

```sh
.venv/bin/python -m venv .tools/dashboard-test-venv
.tools/dashboard-test-venv/bin/python -m pip install --require-hashes -r dashboard/requirements-test.txt
PLAYWRIGHT_BROWSERS_PATH=.tools/playwright .tools/dashboard-test-venv/bin/python -m playwright install chromium
```

```sh
make kind-create
make kind-install
make kind-deploy
make kind-smoke
make kind-measure
make kind-inspect
```

`kind-create` installs Calico with the default kind CNI disabled. `kind-install`
installs the scoped Strimzi and Flink operators from checksum-pinned Helm archives.
`kind-deploy` generates Registry bindings, builds and loads the owned images,
creates per-role Secrets, installs PostgreSQL via Helm, migrates databases, and
applies the Kustomize profile in dependency order. It waits for the broker, v1
topics, Connect and its connector, the Flink job, and the application rollouts.
The first PyFlink build can take several minutes.

Run `make kind-open-ui` and open [the dashboard](http://127.0.0.1:18086).
Use `viewer`, `operator`, or `administrator` and the matching generated password in
`.tools/kind-dev/local.json`. Keep that file private. The browser uses the same
loopback origin for NiceGUI and the allowlisted Keycloak login endpoints. The
server uses Kubernetes DNS for API calls, token exchange, and JWKS. The issuer,
audience, PKCE, state, and nonce checks remain in effect. Kafka, PostgreSQL, gRPC,
Connect, Flink REST, and Keycloak administration have cluster-internal Services.
The UI reads live projections. Scenario start/stop and policy writes return 503
until live adapters are implemented. The smoke opens a temporary loopback API port-forward for the REST/SSE assertions
and closes both forwards after the browser test. Ctrl+C closes the interactive UI
forward.

`make kind-stop` cordons this profile's node and terminates the application
pods before Kafka and PostgreSQL. It then stops the Docker node and keeps its
local storage. If the Kubernetes API is unavailable, it attempts a bounded Docker
node shutdown. `make kind-create` resumes and uncordons the node. The one-shot simulator Job uses a fresh
scenario store. Re-running smoke on a completed Job rechecks its retained
evidence and repeats the live network, authorization and browser checks. Start
with a new dedicated cluster for a new scenario execution. To deliberately
discard the development cluster and its PVCs, use `.venv/bin/kind delete cluster
--name smart-grid-kind-dev` after keeping any desired evidence. Git does not
track kubeconfigs, passwords, build logs or reports.

## Inspect evidence

```sh
.venv/bin/kubectl --kubeconfig .tools/kind-dev.kubeconfig -n platform get pods,svc,pvc,netpol
.venv/bin/kubectl --kubeconfig .tools/kind-dev.kubeconfig -n platform get kafka,kafkaconnector,flinkdeployment,lease
.venv/bin/kubectl --kubeconfig .tools/kind-dev.kubeconfig -n platform logs deploy/action
.venv/bin/kubectl --kubeconfig .tools/kind-dev.kubeconfig -n platform logs job/simulator
```

`.tools/kind-dev/report.json` records gate results, committed loads, commands,
acknowledgements, final charger state, REST/SSE equality and browser acceptance.
`simulator.log` retains the in-cluster test output and `browser.png` shows the
observed action. `make kind-policy` repeats the isolated Calico ingress and egress checks after the probe image and base policies are in place. The probes exercise the same PostgreSQL selectors with disposable TCP listeners, independent of database restarts.

`resources.jsonl` holds kubelet working sets in MiB and CPU in
millicores, including samples taken during smoke. CPU is sampled utilization,
where 1000m is one CPU. Memory is observed working set rather than capacity,
resource requests or a long-term peak. Compare complete smoke samples before
estimating space for additional replicas.

## Three-broker review profile

Measure the lightweight profile first and stop it before starting review. The
planning budget for Docker Desktop is 8 CPUs and 10 GiB for review. Other stacks
running on the same Docker VM share this budget.

```sh
make kind-measure
make kind-stop
make PROFILE=review kind-create kind-install kind-deploy kind-smoke kind-measure kind-inspect
make PROFILE=review kind-stop
```

Each domain topic has three partitions and 24-hour retention. Review has three combined KRaft controllers/brokers with persistent volumes,
topic and Connect/internal-state replication factor 3, and Kafka minimum ISR 2.
It adds an Action standby, a second Registry and API replica, and a second Flink
TaskManager slot. Aggregation uses the existing fixed single-task semantics.
Keycloak, PostgreSQL, Connect, the projection consumer and UI remain single
instances. Both profiles use one kind node so Flink can share the local
checkpoint PVC. Neither profile establishes physical host-failure tolerance.
Use the [implementation status](../../docs/implementation/IMPLEMENTATION_STATUS.md)
for executed capacity and failure evidence.

## Owners and health

| Owner | Workload | Health signal |
|---|---|---|
| Member_1 | Registry | gRPC service with startup/liveness HTTP and a database-backed readiness check |
| Member_1 | Scenario simulator | Bounded Job, committed Kafka evidence, process exit and durable edge effect counts |
| Member_2 | Strimzi and Kafka | Operator probes and Kafka/Topic Ready conditions, broker probes |
| Member_2 | Flink operator and PyFlink | Operator startup/liveness, JobManager REST, TaskManager RPC, RUNNING job and completed checkpoints |
| Member_3 | PostgreSQL and schema bootstrap | StatefulSet PostgreSQL probes, PVC status, bootstrap Job completion |
| Member_3 | Connect/Debezium | Connect probes, KafkaConnector Ready, RUNNING task and delivered outbox |
| Member_3 | Action | Lease resource, durable epoch and fresh worker progress marker |
| Member_4 | Projection | Assigned Kafka consumer and fresh progress marker after polling/committing |
| Member_4 | API and UI | HTTP liveness and dependency readiness, authenticated REST/SSE and UI smoke |
| Member_4 | Keycloak | Management-interface startup/readiness/liveness on port 9000 |
| Member_4 with Member_1 | Local cluster and policy evidence | Calico rollout, node Ready, CoreDNS, live allow/deny probes |

The Action service account can read and update only the pre-created Action
Lease. Worker probes prefer local progress for liveness and a shorter window for
readiness. HTTP apps use explicit probes and bounded termination. Platform
pod ingress and egress default to deny. DNS, dependency flows and operator
access are explicitly permitted. The smoke requires successful connections and
network timeouts for disallowed routes. This disposable loopback profile uses
HTTP and a policy-isolated plaintext Kafka listener. Application-level Kafka
ACL/TLS and production ingress are separate security work.


A separate source checkout can use `KIND_NAME=smart-grid-kind-dev-clean` for
development. The checkout keeps its own ignored credentials and kubeconfig.
Build inputs must be regular files inside the Docker build context. In
particular, keep the verified Debezium archive in `.tools/downloads` rather
than linking that directory outside the checkout.

Deployment waits for the Debezium connector and its assigned task to report
RUNNING. An operator Ready condition alone can precede task assignment. The
full smoke also requires committed outbox commands and applied gateway effects.


Both profiles passed the closed-loop acceptance recorded in
[implementation status](../../docs/implementation/IMPLEMENTATION_STATUS.md).
The measured development node used 4680–4857 MiB and the review node used
6956–7296 MiB during their six acceptance samples. Run profiles separately.
Docker Desktop can restart existing kind nodes after its VM is restarted.
Inspect `docker ps` and stop competing nodes before starting the next profile.
