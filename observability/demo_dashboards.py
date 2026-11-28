"""Grafana dashboards for the continuous demo (observability and grid views).

Generated as ConfigMaps for the Grafana sidecar. Every expression uses a metric that exists on the
running cluster; a panel with no data shows "no data" rather than a healthy-looking zero.
"""
import json

DS = {'type': 'prometheus', 'uid': 'prometheus'}
NET = 'max by (transformer) (grid_business_kw{signal="net_load_kw"})'
CAP = 'max by (transformer) (grid_business_kw{signal="capacity_kw"})'
UTIL = f'100 * {NET} / {CAP}'
TELEMETRY = 'topic=~".*grid.telemetry.v1"'
LOADS = 'topic=~".*grid.transformer-load.v1"'
GROUPS = 'consumergroup=~"agg-fleet.*|integration-kind[.]action[.]control[.]v1|integration-kind[.]projection(-telemetry)?"'
SERVICE_LAG = (f'kafka_consumergroup_lag{{{GROUPS}}}'
               f' unless (kafka_consumergroup_lag{{consumergroup="integration-kind.projection",{TELEMETRY}}}'
               ' and on (topic, partition) kafka_consumergroup_lag{consumergroup="integration-kind.projection-telemetry"})'
               ' unless kafka_consumergroup_lag{consumergroup="integration-kind.projection",topic=~".*grid[.]telemetry[.]dlq[.]v1"}')
THRESHOLDS_UTIL = [{'color': 'green', 'value': None}, {'color': 'orange', 'value': 80}, {'color': 'red', 'value': 95}]


class Board:
    def __init__(self, uid, title, description, tags=()):
        self.uid, self.title, self.description, self.tags = uid, title, description, list(tags)
        self.panels, self.x, self.y, self.row_height = [], 0, 0, 0

    def place(self, width, height):
        if self.x + width > 24:
            self.x, self.y, self.row_height = 0, self.y + self.row_height, 0
        position = {'x': self.x, 'y': self.y, 'w': width, 'h': height}
        self.x += width
        self.row_height = max(self.row_height, height)
        return position

    def row(self, title):
        self.x, self.y, self.row_height = 0, self.y + self.row_height, 0
        self.panels.append({'id': len(self.panels) + 1, 'type': 'row', 'title': title, 'collapsed': False,
                            'gridPos': {'x': 0, 'y': self.y, 'w': 24, 'h': 1}, 'panels': []})
        self.y += 1

    def add(self, kind, title, targets, width=8, height=8, unit=None, thresholds=None, options=None, custom=None,
            description=None, minimum=None, maximum=None, overrides=None, decimals=None):
        defaults = {'noValue': 'no data', 'color': {'mode': 'thresholds' if thresholds else 'palette-classic'}}
        if unit:
            defaults['unit'] = unit
        if thresholds:
            defaults['thresholds'] = {'mode': 'absolute', 'steps': thresholds}
        if minimum is not None:
            defaults['min'] = minimum
        if maximum is not None:
            defaults['max'] = maximum
        if decimals is not None:
            defaults['decimals'] = decimals
        if custom:
            defaults['custom'] = custom
        panel = {'id': len(self.panels) + 1, 'type': kind, 'title': title, 'datasource': DS, 'gridPos': self.place(width, height),
                 'targets': [{'refId': chr(65 + i), 'datasource': DS, 'expr': expr, 'legendFormat': legend,
                              'instant': kind in ('gauge', 'bargauge', 'table')} for i, (expr, legend) in enumerate(targets)],
                 'fieldConfig': {'defaults': defaults, 'overrides': overrides or []}, 'options': options or {}}
        if description:
            panel['description'] = description
        self.panels.append(panel)

    def stat(self, title, expr, unit=None, thresholds=None, width=4, height=4, description=None, spark=True, decimals=None):
        self.add('stat', title, [(expr, '')], width, height, unit, thresholds, description=description, decimals=decimals,
                 options={'colorMode': 'value', 'graphMode': 'area' if spark else 'none', 'textMode': 'value',
                          'reduceOptions': {'calcs': ['lastNotNull'], 'fields': '', 'values': False}})

    def series(self, title, targets, width=12, height=8, unit=None, thresholds=None, stack=False, minimum=0, description=None, fill=15):
        custom = {'lineWidth': 2, 'fillOpacity': fill, 'showPoints': 'never', 'spanNulls': True}
        if stack:
            custom['stacking'] = {'mode': 'normal', 'group': 'A'}
        if thresholds:
            custom['thresholdsStyle'] = {'mode': 'line'}
        self.add('timeseries', title, targets, width, height, unit, thresholds, custom=custom, minimum=minimum, description=description,
                 options={'legend': {'displayMode': 'table', 'placement': 'bottom', 'calcs': ['lastNotNull', 'max']}, 'tooltip': {'mode': 'multi'}})

    def bars(self, title, expr, legend='{{transformer}}', width=12, height=10, unit='percent', thresholds=None, maximum=None):
        self.add('bargauge', title, [(expr, legend)], width, height, unit, thresholds or THRESHOLDS_UTIL, minimum=0, maximum=maximum,
                 options={'displayMode': 'gradient', 'orientation': 'horizontal', 'showUnfilled': True, 'valueMode': 'color',
                          'reduceOptions': {'calcs': ['lastNotNull'], 'values': False}})

    def gauge(self, title, expr, unit='percent', width=4, height=6, thresholds=None, maximum=100):
        self.add('gauge', title, [(expr, '')], width, height, unit, thresholds or THRESHOLDS_UTIL, minimum=0, maximum=maximum,
                 options={'reduceOptions': {'calcs': ['lastNotNull']}, 'showThresholdMarkers': True})

    def render(self, window='now-30m'):
        return {'uid': self.uid, 'title': self.title, 'description': self.description, 'schemaVersion': 40, 'version': 1,
                'tags': ['smart-grid'] + self.tags, 'timezone': 'utc', 'refresh': '5s', 'time': {'from': window, 'to': 'now'},
                'graphTooltip': 1, 'editable': False, 'panels': self.panels,
                'templating': {'list': []},
                'links': [{'title': 'Live control room', 'type': 'link', 'url': '/live', 'icon': 'bolt', 'targetBlank': False},
                          {'title': 'All grid dashboards', 'type': 'dashboards', 'tags': ['smart-grid'], 'asDropdown': True}]}


HERO = 'transformer=~"tx-00[1-5]"'
HERO_UTIL = ('100 * max by (transformer) (grid_business_kw{signal="net_load_kw",' + HERO + '}) / '
             'max by (transformer) (grid_business_kw{signal="capacity_kw",' + HERO + '})')
OK = [{'color': 'green', 'value': None}, {'color': 'orange', 'value': 50}, {'color': 'red', 'value': 80}]
LAG_STEPS = [{'color': 'green', 'value': None}, {'color': 'orange', 'value': 500}, {'color': 'red', 'value': 5000}]


def control_room():
    b = Board('grid-control-room', 'Grid control room', 'Fleet demand, per-transformer utilization and automatic actions.', ['grid'])
    b.stat('Fleet net demand', f'sum({NET})', 'kwatt', width=4, decimals=2)
    b.stat('Fleet capacity', f'sum({CAP})', 'kwatt', width=4, decimals=2)
    b.stat('Fleet utilization', f'100 * sum({NET}) / sum({CAP})', 'percent', THRESHOLDS_UTIL, width=4, decimals=0)
    b.stat('Over 80% load', f'count(({UTIL}) >= 80) or vector(0)', None,
           [{'color': 'green', 'value': None}, {'color': 'orange', 'value': 3}, {'color': 'red', 'value': 8}], width=4)
    b.stat('Over 95% load', f'count(({UTIL}) >= 95) or vector(0)', None, [{'color': 'green', 'value': None}, {'color': 'red', 'value': 1}], width=4)
    b.stat('Active incidents', 'sum(devices_fleet_overload_active) or vector(0)', None,
           [{'color': 'green', 'value': None}, {'color': 'orange', 'value': 1}], width=4)
    b.series('Fleet demand by source', [
        ('sum(max by (transformer) (grid_business_kw{signal="meter_demand_kw"}))', 'Households'),
        ('sum(max by (transformer) (grid_business_kw{signal="ev_demand_kw"}))', 'EV charging'),
        ('-sum(max by (transformer) (grid_business_kw{signal="solar_generation_kw"}))', 'Solar generation')],
        width=12, height=9, unit='kwatt', stack=True, minimum=None, fill=45)
    b.series('Net load against capacity', [(f'sum({NET})', 'Net load'), (f'sum({CAP})', 'Capacity')], width=12, height=9, unit='kwatt')
    b.bars('Utilization by transformer, now', UTIL, width=12, height=14, maximum=120)
    b.series('Managed transformers: utilization', [(HERO_UTIL, '{{transformer}}')], width=12, height=14, unit='percent',
             thresholds=THRESHOLDS_UTIL, minimum=0, description='tx-001 to tx-005 take part in the automatic control loop.')
    b.row('Automatic actions')
    b.series('Demand reduced by throttling', [('max by (transformer) (grid_business_kw{signal="confirmed_reduction_kw"})', '{{transformer}}')],
             width=12, unit='kwatt', description='Reduction confirmed by the chargers, per managed transformer.')
    b.series('Action and command outcomes, 5 minute increase', [('sum by (kind, outcome) (increase(grid_domain_outcomes_total[5m]))', '{{kind}} {{outcome}}')],
             width=12, unit='short')
    return b


def pipeline():
    b = Board('grid-pipeline', 'Streaming pipeline', 'Devices to Kafka to Flink windows to control, with correctness counters.', ['pipeline'])
    b.stat('Devices publishing', 'max(devices_fleet_devices)', None, width=4)
    b.stat('Events / sec', f'sum(rate(kafka_topic_partition_current_offset{{{TELEMETRY}}}[30s]))', 'ops', width=4, decimals=0)
    b.stat('Windows / sec', f'sum(rate(kafka_topic_partition_current_offset{{{LOADS}}}[30s]))', 'ops', width=4, decimals=1)
    b.stat('Output freshness', 'time() - max(flink_taskmanager_job_task_operator_latest_output_ms) / 1000', 's',
           [{'color': 'green', 'value': None}, {'color': 'orange', 'value': 15}, {'color': 'red', 'value': 30}], width=4, decimals=1)
    b.stat('Consumer lag, Flink', f'sum(kafka_consumergroup_lag{{{TELEMETRY},consumergroup=~"agg-fleet.*"}})', None, LAG_STEPS, width=4, decimals=0)
    b.stat('Failed checkpoints', 'max(flink_jobmanager_job_numberOfFailedCheckpoints)', None,
           [{'color': 'green', 'value': None}, {'color': 'orange', 'value': 3}, {'color': 'red', 'value': 10}], width=4, decimals=0, spark=False)
    b.series('Throughput', [(f'sum(rate(devices_fleet_events_total[30s]))', 'devices publish'),
                            (f'sum(rate(kafka_topic_partition_current_offset{{{TELEMETRY}}}[30s]))', 'kafka ingest'),
                            (f'sum(rate(kafka_topic_partition_current_offset{{{LOADS}}}[30s])) * 10', 'load windows x10')],
             width=12, unit='ops')
    b.series('Consumer lag by service', [(f'sum by (consumergroup) ({SERVICE_LAG})', '{{consumergroup}}')], width=12, unit='short')
    b.row('Flink')
    b.series('Task busy and backpressure, % of time', [
        ('max(flink_taskmanager_job_task_busyTimeMsPerSecond) / 10', 'busy'),
        ('max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond) / 10', 'backpressured')], width=8, unit='percent')
    b.series('Checkpoint duration', [('max(flink_jobmanager_job_lastCheckpointDuration)', 'last checkpoint')], width=8, unit='ms')
    b.series('Checkpoint state size', [('max(flink_jobmanager_job_lastCheckpointSize)', 'size')], width=8, unit='bytes',
             description='Window-scoped deduplication keeps this flat however long the run lasts.')
    b.row('Correctness: out-of-order, duplicate and late data')
    b.series('Duplicates dropped, per minute', [('sum(increase(flink_taskmanager_job_task_operator_duplicates[1m]))', 'duplicates')], width=6)
    b.series('Late events accepted (windows corrected)', [('sum(increase(flink_taskmanager_job_task_operator_corrections[1m]))', 'window corrections')], width=6)
    b.series('Too late, audited', [('sum(increase(flink_taskmanager_job_task_operator_very_late[1m]))', 'late topic')], width=6)
    b.series('Rejected (DLQ)', [('sum(increase(flink_taskmanager_job_task_operator_dlq[1m]))', 'dead letter')], width=6)
    b.series('Injected faults, per minute', [
        ('sum(increase(devices_fleet_late_injected_total[1m]))', 'delayed'),
        ('sum(increase(devices_fleet_duplicates_injected_total[1m]))', 'duplicated'),
        ('sum(increase(devices_fleet_dropped_total[1m]))', 'dropped'),
        ('sum(increase(devices_fleet_very_late_injected_total[1m]))', 'very late')], width=12,
        description='What the simulator did to the stream; compare with the correctness panels.')
    b.series('Device coverage of the latest windows', [
        ('sum(max by (transformer) (grid_window_devices{kind="good"}))', 'good'),
        ('sum(max by (transformer) (grid_window_devices{kind="estimated"}))', 'estimated'),
        ('sum(max by (transformer) (grid_window_devices{kind="missing"}))', 'missing')], width=12, stack=True, fill=40)
    return b


def kafka():
    b = Board('grid-kafka', 'Kafka', 'Topics, partitions, retention and broker health (one broker on the student cluster).', ['kafka'])
    b.stat('Brokers', 'max(kafka_brokers)', None, [{'color': 'red', 'value': None}, {'color': 'green', 'value': 1}], width=4, spark=False)
    b.stat('Online partitions', 'count(kafka_topic_partition_leader >= 0) or vector(0)', None, width=4, spark=False)
    b.stat('Under-replicated', 'sum(kafka_topic_partition_under_replicated_partition) or vector(0)', None,
           [{'color': 'green', 'value': None}, {'color': 'red', 'value': 1}], width=4, spark=False,
           description='Zero on one broker by definition. Replication across three brokers was verified on the local cluster.')
    b.stat('Telemetry retained', f'sum(kafka_topic_partition_current_offset{{{TELEMETRY}}} - kafka_topic_partition_oldest_offset{{{TELEMETRY}}})', 'short', width=4, decimals=0)
    b.stat('Retention window',
           f'sum(kafka_topic_partition_current_offset{{{TELEMETRY}}} - kafka_topic_partition_oldest_offset{{{TELEMETRY}}}) / sum(rate(kafka_topic_partition_current_offset{{{TELEMETRY}}}[5m])) / 60',
           'm', width=4, decimals=0, description='Old records are deleted by time and by size, so the volume never fills.')
    b.stat('Kafka volume used', '100 * kubelet_volume_stats_used_bytes{persistentvolumeclaim="data-0-grid-brokers-0"} / kubelet_volume_stats_capacity_bytes{persistentvolumeclaim="data-0-grid-brokers-0"}',
           'percent', [{'color': 'green', 'value': None}, {'color': 'orange', 'value': 70}, {'color': 'red', 'value': 85}], width=4, decimals=0)
    b.series('Messages per second by topic', [('sum by (topic) (rate(kafka_topic_partition_current_offset[1m]))', '{{topic}}')], width=12, unit='ops')
    b.series('Telemetry per partition', [(f'sum by (partition) (rate(kafka_topic_partition_current_offset{{{TELEMETRY}}}[1m]))', 'partition {{partition}}')], width=12, unit='ops',
             description='Records are keyed by transformer, so each transformer stays in one partition and keeps its order.')
    b.series('Records retained by topic', [('sum by (topic) (kafka_topic_partition_current_offset - kafka_topic_partition_oldest_offset)', '{{topic}}')], width=12, unit='short')
    b.series('Oldest offset moves forward (rolling deletion)', [(f'sum(kafka_topic_partition_oldest_offset{{{TELEMETRY}}})', 'oldest telemetry offset')], width=12, unit='short', minimum=None)
    return b


def platform():
    b = Board('grid-platform', 'Platform and SLO', 'Pod CPU and memory, restarts, volumes and API health on the AKS cluster.', ['platform'])
    pods = 'namespace="platform",container!="",container!="POD"'
    b.series('CPU by pod, cores', [(f'sum by (pod) (rate(container_cpu_usage_seconds_total{{{pods}}}[1m]))', '{{pod}}')], width=12, height=9, unit='short')
    b.series('Memory by pod', [(f'sum by (pod) (container_memory_working_set_bytes{{{pods}}})', '{{pod}}')], width=12, height=9, unit='bytes')
    b.series('CPU throttled, fraction of periods', [
        (f'sum by (pod) (rate(container_cpu_cfs_throttled_periods_total{{{pods}}}[1m])) / sum by (pod) (rate(container_cpu_cfs_periods_total{{{pods}}}[1m]))', '{{pod}}')],
        width=12, unit='percentunit', description='A pod near 1 is limited by its CPU limit, not by demand.')
    b.series('Container restarts', [('sum by (pod) (kube_pod_container_status_restarts_total{namespace="platform"})', '{{pod}}')], width=12, unit='short')
    b.series('Volume used, percent', [('100 * kubelet_volume_stats_used_bytes{namespace="platform"} / kubelet_volume_stats_capacity_bytes{namespace="platform"}', '{{persistentvolumeclaim}}')],
             width=12, unit='percent')
    b.series('API requests per second', [('sum by (operation) (rate(grid_api_requests_total[1m]))', '{{operation}}')], width=12, unit='ops')
    b.series('API response start, p99', [('histogram_quantile(0.99, sum by (le) (rate(grid_api_duration_seconds_bucket{operation="query"}[5m])))', 'query p99')], width=12, unit='s')
    b.series('Database round trip, p99', [('histogram_quantile(0.99, sum by (le, boundary) (rate(grid_boundary_duration_seconds_bucket{boundary=~"db.*"}[5m])))', '{{boundary}}')], width=12, unit='s')
    return b


def storage():
    b = Board('grid-storage', 'Rolling retention and storage', 'Data is kept for a rolling window and old records are removed before any volume fills.', ['storage'])
    pvc = 'kubelet_volume_stats_used_bytes{namespace=~"platform|observability"}'
    b.bars('Volume used, percent', '100 * kubelet_volume_stats_used_bytes{namespace=~"platform|observability"} / kubelet_volume_stats_capacity_bytes{namespace=~"platform|observability"}',
           legend='{{persistentvolumeclaim}}', width=12, height=10, thresholds=[{'color': 'green', 'value': None}, {'color': 'orange', 'value': 70}, {'color': 'red', 'value': 85}], maximum=100)
    b.series('Volume used, bytes', [(pvc, '{{persistentvolumeclaim}}')], width=12, height=10, unit='bytes',
             description='Sawtooth lines are the rolling window: deletion keeps each volume below its cap.')
    b.series('Kafka retention window, minutes', [(
        f'sum(kafka_topic_partition_current_offset{{{TELEMETRY}}} - kafka_topic_partition_oldest_offset{{{TELEMETRY}}}) / sum(rate(kafka_topic_partition_current_offset{{{TELEMETRY}}}[5m])) / 60', 'telemetry')],
        width=12, unit='m')
    b.series('Kafka log size per topic, records', [('sum by (topic) (kafka_topic_partition_current_offset - kafka_topic_partition_oldest_offset)', '{{topic}}')], width=12, unit='short')
    return b


BOARDS = [control_room, pipeline, kafka, platform, storage]


def dashboards():
    for make in BOARDS:
        board = make()
        yield board.uid, board.render()


def configmaps():
    return {'apiVersion': 'v1', 'kind': 'List', 'items': [
        {'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'demo-' + uid, 'namespace': 'observability',
            'labels': {'grafana_dashboard': '1', }}, 'data': {uid + '.json': json.dumps(body)}}
        for uid, body in dashboards()]}


if __name__ == '__main__':
    print(json.dumps(configmaps()))
