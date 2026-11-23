"""Member_1 technical views, Member_4 grid and proposed SLO views."""
import json

VIEWS = {
 'grid-load': ('Transformer load and demand reduction', [
    ('Observed load kW', 'grid_business_kw{signal="net_load_kw"}'),
    ('EV demand kW', 'grid_business_kw{signal="ev_demand_kw"}'),
    ('Target / expected / confirmed reduction kW', 'grid_business_kw{signal=~".*reduction_kw"}')]),
 'grid-streaming': ('Kafka lag and Flink health', [
    ('Committed consumer lag', 'sum by (consumergroup) (kafka_consumergroup_lag)'),
    ('Semantic watermark delay ms (missing is unknown)', 'flink_taskmanager_job_task_operator_watermark_delay_ms'),
    ('Completed checkpoints', 'flink_jobmanager_job_numberOfCompletedCheckpoints'),
    ('Failed checkpoints', 'flink_jobmanager_job_numberOfFailedCheckpoints'),
    ('Checkpoint duration ms', 'flink_jobmanager_job_lastCheckpointDuration'),
    ('Flink backpressure ms/s', 'flink_taskmanager_job_task_backPressuredTimeMsPerSecond')]),
 'grid-control': ('Action outcomes and leadership', [
    ('Observed domain outcomes (projection changes)', 'sum by (kind,outcome) (increase(grid_domain_outcomes_total[5m]))'),
    ('Last reported leadership (5m expiry after silence)', 'grid_leader'),
    ('Control operation errors', 'sum by (boundary) (rate(grid_operations_total{boundary=~"control.*",outcome="error"}[5m]))'),
    ('Database boundary p99 seconds', 'histogram_quantile(0.99,sum by (le,boundary) (rate(grid_boundary_duration_seconds_bucket{boundary=~"db.*"}[5m])))')]),
 'grid-api': ('API and SSE health', [
    ('Eligible query requests / errors', 'sum by (outcome) (rate(grid_api_requests_total{operation="query"}[5m]))'),
    ('Query response-start p99 seconds', 'histogram_quantile(0.99,sum by (le) (rate(grid_api_duration_seconds_bucket{operation="query"}[5m])))'),
    ('Active SSE clients', 'grid_sse_clients'),
    ('SSE cursor resets', 'increase(grid_sse_resets_total[5m])'),
    ('SSE reconnects', 'increase(grid_sse_reconnects_total[5m])'),
    ('SSE sends / errors', 'sum by (outcome) (rate(grid_operations_total{boundary="sse.send"}[5m]))')]),
 'grid-slos': ('Resource saturation and proposed SLOs', [
    ('Container working set bytes', 'sum by (namespace,pod) (container_memory_working_set_bytes{namespace=~"platform|observability",container!=""})'),
    ('CPU seconds / second', 'sum by (namespace,pod) (rate(container_cpu_usage_seconds_total{namespace=~"platform|observability",container!=""}[5m]))'),
    ('Memory / declared container limits percent', '100 * sum by (namespace,pod) (container_memory_working_set_bytes{namespace=~"platform|observability",container!="",image!=""}) / sum by (namespace,pod) (kube_pod_container_resource_limits{namespace=~"platform|observability",resource="memory"})'),
    ('CPU throttled periods fraction', 'sum by (namespace,pod) (rate(container_cpu_cfs_throttled_periods_total{namespace=~"platform|observability"}[5m])) / sum by (namespace,pod) (rate(container_cpu_cfs_periods_total{namespace=~"platform|observability"}[5m]))'),
    ('Restarts', 'sum by (namespace,pod) (kube_pod_container_status_restarts_total{namespace=~"platform|observability"})'),
    ('Proposed query good fraction (success AND <=1s), target .995', 'sum(increase(grid_api_duration_seconds_bucket{operation="query",outcome="ok",le="1"}[5m])) / sum(increase(grid_api_requests_total{operation="query"}[5m]))'),
    ('Collector refused spans', 'otelcol_receiver_refused_spans_total')])}


def dashboards():
    for uid, (title, panels) in VIEWS.items():
        yield uid, {'uid': uid, 'title': title, 'schemaVersion': 40, 'version': 1,
            'tags': ['smart-grid', 'Member_1', 'Member_4'], 'timezone': 'utc',
            'time': {'from': 'now-15m', 'to': 'now'}, 'refresh': '5s',
            'description': 'Local observed signals. No data is unknown, never healthy. SLOs are proposed targets, not acceptance claims. Prometheus retains 24h.',
            'panels': [{'id': n+1, 'title': label, 'type': 'timeseries',
                'datasource': {'type': 'prometheus', 'uid': 'prometheus'},
                'gridPos': {'x': n%2*12, 'y': n//2*9, 'w': 12, 'h': 9},
                'targets': [{'refId': 'A', 'expr': query, 'legendFormat': '{{service}} {{boundary}} {{signal}} {{outcome}} {{pod}}'}],
                'fieldConfig': {'defaults': {'noValue': 'unknown'}, 'overrides': []},
                'options': {'legend': {'displayMode': 'table'}}} for n, (label, query) in enumerate(panels)]}


def configmaps():
    return {'apiVersion': 'v1', 'kind': 'List', 'items': [
        {'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': uid,
            'namespace': 'observability', 'labels': {'grafana_dashboard': '1', 'grid-owner': 'Member_1'}},
         'data': {uid+'.json': json.dumps(body)}} for uid, body in dashboards()]}


if __name__ == '__main__':
    print(json.dumps(configmaps()))
