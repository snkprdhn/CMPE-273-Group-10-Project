#!/usr/bin/env bash
# AKS benchmark setup: add DS2_v2 Flink node, generate mock identity secret,
# deploy updated overlay, and verify cluster health.
#
# Run from the repository root after setting up the AKS kubeconfig:
#   export REVIEW_DIR=.tools/aks-review
#   bash infra/aks/benchmark-setup.sh
#
# Prerequisites: az CLI logged in, kubectl configured, REVIEW_DIR exists.
# AGENTS.md: no commits or pushes from this script. No Azure resources beyond
# the one DS2_v2 node pool authorised in the benchmark plan.
set -euo pipefail
umask 077

REVIEW_GROUP="${REVIEW_GROUP:-rg-smart-grid-professor-review}"
REVIEW_CLUSTER="${REVIEW_CLUSTER:-smart-grid-aks-review}"
REVIEW_ACR="${REVIEW_ACR:-smartgridevreview9e9}"
REVIEW_REGISTRY="${REVIEW_REGISTRY:-${REVIEW_ACR}.azurecr.io}"
REVIEW_DIR="${REVIEW_DIR:-.tools/aks-review}"
REVIEW_HOST="${REVIEW_HOST:-smart-grid-ev-review-9e9.denmarkeast.cloudapp.azure.com}"

k() { .venv/bin/kubectl --kubeconfig "$REVIEW_DIR/kubeconfig" --context "$REVIEW_CLUSTER" -n platform "$@"; }
ko() { .venv/bin/kubectl --kubeconfig "$REVIEW_DIR/kubeconfig" --context "$REVIEW_CLUSTER" "$@"; }

echo "=== Step 0: Pre-flight checks ==="
az account show --query '{name:name,state:state}' -o json
review_sub=$(az account show --query id -o tsv)
az rest --method get \
  --url "https://management.azure.com/subscriptions/${review_sub}?api-version=2022-12-01" \
  --query '{spendingLimit:subscriptionPolicies.spendingLimit}' -o json
curl -fsS --max-time 15 "https://${REVIEW_HOST}/readyz"
echo ""
echo "Spending limit must be On and readyz must return 200 before continuing."
echo "Press Enter to continue or Ctrl+C to abort."
read -r

echo ""
echo "=== Step 1: Verify DSv2 quota before adding node ==="
az vm list-usage --location denmarkeast \
  --query "[?contains(name.value,'DSv2')].{name:name.value,current:currentValue,limit:limit}" \
  -o table
# Expect current=4, limit>=6 to have room for a DS2_v2 (2 vCPU).
echo "DS2_v2 needs 2 vCPUs. Confirm quota above shows room before continuing."
read -r

echo ""
echo "=== Step 2: Add DS2_v2 Flink node pool ==="
az aks nodepool add \
  --cluster-name "$REVIEW_CLUSTER" \
  --resource-group "$REVIEW_GROUP" \
  --name flinkpool \
  --node-count 1 \
  --node-vm-size Standard_DS2_v2 \
  --mode User \
  --labels grid-role=flink \
  --no-wait
echo "Waiting for flinkpool to be provisioned..."
az aks nodepool wait \
  --cluster-name "$REVIEW_CLUSTER" \
  --resource-group "$REVIEW_GROUP" \
  --name flinkpool \
  --created \
  --timeout 600

echo ""
echo "=== Step 3: Get new node name and taint it ==="
ko get nodes -L kubernetes.io/arch,grid-role
FLINK_NODE=$(ko get nodes -l grid-role=flink -o jsonpath='{.items[0].metadata.name}')
echo "Tainting node: $FLINK_NODE"
ko taint node "$FLINK_NODE" grid-role=flink:NoSchedule

echo ""
echo "=== Step 4: Generate mock identity token ==="
mkdir -p "$REVIEW_DIR"
TOKEN=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
IDENTITIES="{\"${TOKEN}\":{\"actor\":\"demo-user\",\"role\":\"Viewer\"}}"
echo "TOKEN=$TOKEN" > "$REVIEW_DIR/mock-identity.env"
echo "IDENTITIES=$IDENTITIES" >> "$REVIEW_DIR/mock-identity.env"
chmod 600 "$REVIEW_DIR/mock-identity.env"
echo "Mock identity written to $REVIEW_DIR/mock-identity.env (keep private)"

echo ""
echo "=== Step 5: Create mock-identity-runtime Secret in cluster ==="
k create secret generic mock-identity-runtime \
  --from-literal="MEMBER4_MOCK_IDENTITIES=${IDENTITIES}" \
  --from-literal="DASHBOARD_API_TOKEN=${TOKEN}" \
  --dry-run=client -o yaml | \
  k apply -f -

echo ""
echo "=== Step 6: Scale down Keycloak and Tempo ==="
k scale deployment keycloak --replicas=0
# Tempo may be in observability namespace.
.venv/bin/kubectl --kubeconfig "$REVIEW_DIR/kubeconfig" --context "$REVIEW_CLUSTER" \
  -n observability scale deployment tempo --replicas=0 2>/dev/null || \
  echo "Tempo not found in observability ns — checking platform ns..."
k scale deployment tempo --replicas=0 2>/dev/null || echo "Tempo not in platform ns either — skipping"

echo ""
echo "=== Step 7: Patch API and UI deployments for mock auth ==="
# API: add MEMBER4_AUTH_MODE=mock and MEMBER4_MOCK_IDENTITIES from secret.
k patch deployment api --type=json -p='[
  {"op":"add","path":"/spec/template/spec/containers/0/env/-",
   "value":{"name":"MEMBER4_AUTH_MODE","value":"mock"}},
  {"op":"add","path":"/spec/template/spec/containers/0/env/-",
   "value":{"name":"MEMBER4_MOCK_IDENTITIES","valueFrom":{"secretKeyRef":{"name":"mock-identity-runtime","key":"MEMBER4_MOCK_IDENTITIES"}}}}
]'

# UI: add MEMBER4_AUTH_MODE=mock, DASHBOARD_API_TOKEN, Prometheus/Flink URLs.
k patch deployment ui --type=json -p='[
  {"op":"add","path":"/spec/template/spec/containers/0/env/-",
   "value":{"name":"MEMBER4_AUTH_MODE","value":"mock"}},
  {"op":"add","path":"/spec/template/spec/containers/0/env/-",
   "value":{"name":"DASHBOARD_API_TOKEN","valueFrom":{"secretKeyRef":{"name":"mock-identity-runtime","key":"DASHBOARD_API_TOKEN"}}}},
  {"op":"add","path":"/spec/template/spec/containers/0/env/-",
   "value":{"name":"PROMETHEUS_URL","value":"http://prometheus-stack-kube-prom-prometheus.observability:9090"}},
  {"op":"add","path":"/spec/template/spec/containers/0/env/-",
   "value":{"name":"FLINK_REST_URL","value":"http://aggregation-rest:8081"}},
  {"op":"add","path":"/spec/template/spec/containers/0/env/-",
   "value":{"name":"GRAFANA_URL","value":"http://prometheus-stack-grafana.observability:80"}}
]'

echo ""
echo "=== Step 8: Apply benchmark network policies ==="
ko apply -f infra/aks/benchmark-policies.yaml

echo ""
echo "=== Step 9: Patch Flink deployment (parallelism 3, RocksDB, node affinity) ==="
# The FlinkDeployment must be patched via kubectl apply with the full spec diff,
# or via the Flink operator. We use a targeted merge patch here.
k patch flinkdeployment aggregation --type=merge -p='{
  "spec": {
    "job": {
      "parallelism": 3,
      "upgradeMode": "stateless",
      "args": [
        "-py", "/app/member2/job.py",
        "--run", "aksbenchmark01",
        "--bootstrap", "grid-kafka-bootstrap:9092",
        "--prefix", "integration-kind.",
        "--topology", "/app/member2/integration-topology.json",
        "--checkpoint", "file:///checkpoints/integration-kind",
        "--checkpoint-ms", "5000",
        "--max-events", "1100000",
        "--state-backend", "rocksdb"
      ]
    },
    "flinkConfiguration": {
      "taskmanager.numberOfTaskSlots": "3",
      "state.backend.type": "rocksdb",
      "state.backend.incremental": "true",
      "state.backend.rocksdb.memory.managed": "true",
      "execution.checkpointing.interval": "5000"
    },
    "taskManager": {
      "replicas": 1,
      "resource": {"cpu": 1.5, "memory": "4096m"}
    },
    "jobManager": {
      "resource": {"cpu": 0.3, "memory": "512m"}
    },
    "podTemplate": {
      "spec": {
        "tolerations": [{"key":"grid-role","operator":"Equal","value":"flink","effect":"NoSchedule"}],
        "affinity": {
          "nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": {
              "nodeSelectorTerms": [{
                "matchExpressions": [{"key":"grid-role","operator":"In","values":["flink"]}]
              }]
            }
          }
        }
      }
    }
  }
}'

echo ""
echo "=== Step 10: Wait for Flink to reach STABLE ==="
for i in $(seq 1 60); do
  state=$(k get flinkdeployment aggregation -o jsonpath='{.status.lifecycleState}' 2>/dev/null || echo "UNKNOWN")
  echo "  [$i/60] Flink lifecycle state: $state"
  if [ "$state" = "STABLE" ]; then
    echo "Flink STABLE."
    break
  fi
  sleep 10
done

echo ""
echo "=== Step 11: Roll out new UI image ==="
echo "Build the UI image first (make dashboard-test must pass), then run:"
echo ""
echo "  rev=\"benchmark-v1\""
echo "  candidate=\"\$REVIEW_REGISTRY/smart-grid-kind/ui:\$rev\""
echo "  docker build --platform linux/amd64 --provenance=false \\"
echo "    -f infra/kind/Dockerfile --target ui -t \"\$candidate\" ."
echo "  az acr login -n \$REVIEW_ACR"
echo "  docker push \"\$candidate\" 2>&1 | tee \"\$REVIEW_DIR/\${rev}-push.log\""
echo "  digest=\$(sed -nE 's/.*digest: (sha256:[a-f0-9]{64}).*/\\1/p' \"\$REVIEW_DIR/\${rev}-push.log\" | tail -n 1)"
echo "  k get deployment ui -o jsonpath='{.spec.template.spec.containers[?(@.name==\"ui\")].image}' > \"\$REVIEW_DIR/\${rev}-previous-image\""
echo "  k set image deployment/ui \"ui=\$REVIEW_REGISTRY/smart-grid-kind/ui@\$digest\""
echo "  k rollout status deployment/ui --timeout=240s"
echo ""

echo "=== Step 12: Final health check ==="
sleep 10
curl -fsS --max-time 15 "https://${REVIEW_HOST}/readyz" && echo " readyz OK"
ko get nodes -L kubernetes.io/arch,grid-role
k get deploy,flinkdeployment,kafka,kafkaconnect
k get pods -o wide | grep -E "NAME|aggregation"
echo ""
echo "Verify Flink pods are on the DS2_v2 (grid-role=flink) node."
echo "Verify Keycloak shows 0/0 replicas."
echo "Verify mock auth: curl -H 'Authorization: Bearer TOKEN' https://HOST/api/v1/overview"
echo "TOKEN is in $REVIEW_DIR/mock-identity.env"
