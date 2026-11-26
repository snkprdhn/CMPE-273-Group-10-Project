#!/usr/bin/env bash
# AKS 1M end-to-end benchmark runner.
#
# Produces 1,000,000 Kafka telemetry events into the production pipeline
# (integration-kind. prefix) via a local port-forward to the AKS Kafka broker,
# then monitors Flink committed offsets and consumer lag until zero lag is
# observed or 360 seconds elapse (60s grace beyond the 300s target).
#
# Run from the repository root:
#   export REVIEW_DIR=.tools/aks-review
#   bash infra/aks/benchmark-run.sh
#
# Evidence is written to $REVIEW_DIR/million-benchmark/.
# AGENTS.md: no commits or pushes. Spending limit must be On before running.
set -euo pipefail
umask 077

REVIEW_DIR="${REVIEW_DIR:-.tools/aks-review}"
REVIEW_CLUSTER="${REVIEW_CLUSTER:-smart-grid-aks-review}"
EVIDENCE_DIR="$REVIEW_DIR/million-benchmark"
mkdir -p "$EVIDENCE_DIR"

k() { .venv/bin/kubectl --kubeconfig "$REVIEW_DIR/kubeconfig" --context "$REVIEW_CLUSTER" -n platform "$@"; }

echo "=== Benchmark pre-flight ==="
echo "Verifying Flink STABLE and Debezium RUNNING..."
flink_state=$(k get flinkdeployment aggregation -o jsonpath='{.status.lifecycleState}')
echo "Flink lifecycle: $flink_state"
if [ "$flink_state" != "STABLE" ]; then
  echo "ERROR: Flink is not STABLE. Abort."
  exit 1
fi

connector_status=$(k exec grid-connect-connect-0 -- \
  curl -fsS --max-time 5 http://127.0.0.1:8083/connectors/integration-kind/status 2>/dev/null \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'], d['tasks'][0]['state'])")
echo "Debezium connector + task: $connector_status"
if [ "$connector_status" != "RUNNING RUNNING" ]; then
  echo "ERROR: Debezium not RUNNING. Abort."
  exit 1
fi

echo ""
echo "=== Starting Kafka port-forward ==="
# Kill any existing port-forward on 29092.
pkill -f "port-forward.*29092" 2>/dev/null || true
sleep 1
.venv/bin/kubectl --kubeconfig "$REVIEW_DIR/kubeconfig" --context "$REVIEW_CLUSTER" \
  -n platform port-forward svc/grid-kafka-bootstrap 29092:9092 \
  > "$EVIDENCE_DIR/port-forward.log" 2>&1 &
PF_PID=$!
echo "Port-forward PID: $PF_PID"
sleep 3

# Verify port-forward is alive.
if ! kill -0 $PF_PID 2>/dev/null; then
  echo "ERROR: port-forward failed. See $EVIDENCE_DIR/port-forward.log"
  exit 1
fi
echo "Port-forward ready on 127.0.0.1:29092"

cleanup() {
  echo ""
  echo "Cleaning up port-forward..."
  kill $PF_PID 2>/dev/null || true
}
trap cleanup EXIT

echo ""
echo "=== Recording pre-benchmark cluster state ==="
k get pods -o wide > "$EVIDENCE_DIR/pre-benchmark-pods.txt"
k get nodes -L kubernetes.io/arch,grid-role > "$EVIDENCE_DIR/pre-benchmark-nodes.txt"
k get flinkdeployment aggregation -o json > "$EVIDENCE_DIR/pre-benchmark-flink.json"

echo ""
echo "=== Starting 1M event producer ==="
START_TIME=$(date -u +%s)
START_LABEL=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
echo "Benchmark start: $START_LABEL" | tee "$EVIDENCE_DIR/timing.txt"

# Run producer in background; capture output.
.tools/member2-venv/bin/python -m tests.member2.scale aksbenchmark01 \
  --rounds 400 \
  --rate 2500 \
  --parallelism 3 \
  --bootstrap 127.0.0.1:29092 \
  --prefix integration-kind. \
  --max-events 1100000 \
  > "$EVIDENCE_DIR/producer.log" 2>&1 &
PRODUCER_PID=$!
echo "Producer PID: $PRODUCER_PID"

echo ""
echo "=== Monitoring consumer lag (every 10s, up to 360s) ==="
echo "timestamp,elapsed_s,p0_lag,p1_lag,p2_lag,total_lag,committed_total" \
  > "$EVIDENCE_DIR/aksbenchmark01-progress.csv"

ZERO_LAG_TIME=""
for i in $(seq 1 36); do
  sleep 10
  ELAPSED=$(( $(date -u +%s) - START_TIME ))

  # Read committed offsets from Kafka consumer group via port-forward.
  LAG_JSON=$(python3 - <<'PYEOF' 2>/dev/null || echo "{}"
import json, sys
try:
    from confluent_kafka.admin import AdminClient
    from confluent_kafka import TopicPartition
    admin = AdminClient({'bootstrap.servers': '127.0.0.1:29092'})
    topic = 'integration-kind.grid.telemetry.v1'
    group = 'integration-kind.m2-aksbenchmark01-aggregation-v1'
    offsets = admin.list_consumer_group_offsets([__import__('confluent_kafka').ConsumerGroupTopicPartitions(group)])
    result = offsets[group].result()
    parts = {p.partition: p.offset for p in result.topic_partitions}
    # Get high watermarks.
    consumer = __import__('confluent_kafka').Consumer({'bootstrap.servers':'127.0.0.1:29092','group.id':'_tmp_wm'})
    wm = {}
    for part in sorted(parts):
        lo, hi = consumer.get_watermark_offsets(TopicPartition(topic, part), timeout=3)
        wm[part] = hi
    consumer.close()
    lag = {p: max(0, wm.get(p, 0) - parts.get(p, 0)) for p in wm}
    committed = sum(parts.values())
    result_data = {'lag': lag, 'committed': committed}
    print(json.dumps(result_data))
except Exception as e:
    print(json.dumps({'error': str(e)}))
PYEOF
)

  P0=$(echo "$LAG_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('lag',{}).get('0','-'))" 2>/dev/null || echo "-")
  P1=$(echo "$LAG_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('lag',{}).get('1','-'))" 2>/dev/null || echo "-")
  P2=$(echo "$LAG_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('lag',{}).get('2','-'))" 2>/dev/null || echo "-")
  TOTAL_LAG=$(echo "$LAG_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); lag=d.get('lag',{}); print(sum(lag.values()) if lag else '-')" 2>/dev/null || echo "-")
  COMMITTED=$(echo "$LAG_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('committed','-'))" 2>/dev/null || echo "-")
  NOW=$(date -u '+%Y-%m-%dT%H:%M:%SZ')

  echo "$NOW,${ELAPSED},${P0},${P1},${P2},${TOTAL_LAG},${COMMITTED}" \
    >> "$EVIDENCE_DIR/aksbenchmark01-progress.csv"
  echo "[$NOW] elapsed=${ELAPSED}s lag=[p0=${P0} p1=${P1} p2=${P2}] total=${TOTAL_LAG} committed=${COMMITTED}"

  if [ "$TOTAL_LAG" = "0" ] && [ "$COMMITTED" != "-" ]; then
    ZERO_LAG_TIME=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
    echo "ZERO LAG at ${ZERO_LAG_TIME} (${ELAPSED}s elapsed)" | tee -a "$EVIDENCE_DIR/timing.txt"
    if [ "$ELAPSED" -le 300 ]; then
      echo "✓ BENCHMARK PASSED: zero lag in ${ELAPSED}s (target ≤300s)" | tee -a "$EVIDENCE_DIR/timing.txt"
    else
      echo "✗ BENCHMARK: zero lag at ${ELAPSED}s (target was ≤300s)" | tee -a "$EVIDENCE_DIR/timing.txt"
    fi
    break
  fi
done

if [ -z "$ZERO_LAG_TIME" ]; then
  ELAPSED=$(( $(date -u +%s) - START_TIME ))
  echo "TIMEOUT: zero lag not observed within 360s (elapsed=${ELAPSED}s)" | tee -a "$EVIDENCE_DIR/timing.txt"
fi

# Wait for producer to finish.
wait $PRODUCER_PID || true
echo ""
echo "Producer exit code: $?"

echo ""
echo "=== Capturing final state ==="
k get pods -o wide > "$EVIDENCE_DIR/post-benchmark-pods.txt"
k get flinkdeployment aggregation -o json > "$EVIDENCE_DIR/post-benchmark-flink.json"

# Final consumer group offsets.
python3 - > "$EVIDENCE_DIR/final-offsets.json" 2>/dev/null <<'PYEOF' || echo "{}" > "$EVIDENCE_DIR/final-offsets.json"
import json
try:
    from confluent_kafka.admin import AdminClient
    admin = AdminClient({'bootstrap.servers': '127.0.0.1:29092'})
    group = 'integration-kind.m2-aksbenchmark01-aggregation-v1'
    from confluent_kafka import ConsumerGroupTopicPartitions
    offsets = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])
    result = offsets[group].result()
    parts = {p.partition: p.offset for p in result.topic_partitions}
    print(json.dumps({'group': group, 'offsets': parts, 'total': sum(parts.values())}))
except Exception as e:
    print(json.dumps({'error': str(e)}))
PYEOF

echo ""
echo "=== Evidence written to $EVIDENCE_DIR ==="
ls -lh "$EVIDENCE_DIR/"
echo ""
cat "$EVIDENCE_DIR/timing.txt"
echo ""
echo "Next: capture dashboard screenshot, Grafana consumer-lag panel screenshot,"
echo "then update docs/implementation/IMPLEMENTATION_STATUS.md with the results."
