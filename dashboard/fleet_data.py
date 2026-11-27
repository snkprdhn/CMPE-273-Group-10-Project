"""Shared live data for the control room: one poller per process, any number of browser tabs.

Sources, all read-only except the fleet control calls:
  Prometheus  per-transformer load, device coverage, pipeline rates, lag, storage
  REST API    actions and alerts of the managed transformers
  Fleet       fault-injection and incident controls (token held server-side)

History is kept in memory for three hours at five-second resolution and backfilled from
Prometheus at start, so a restarted pod does not show an empty chart.
"""
import asyncio
from collections import deque
import os
import time

import httpx

STEP = 5                      # seconds between samples
KEEP = 3 * 3600 // STEP       # three hours of samples
BACKFILL_SECONDS = 3600
BACKFILL_STEP = 10

TX = {
    'net': 'max by (transformer) (grid_business_kw{signal="net_load_kw"})',
    'ev': 'max by (transformer) (grid_business_kw{signal="ev_demand_kw"})',
    'meter': 'max by (transformer) (grid_business_kw{signal="meter_demand_kw"})',
    'solar': 'max by (transformer) (grid_business_kw{signal="solar_generation_kw"})',
    'capacity': 'max by (transformer) (grid_business_kw{signal="capacity_kw"})',
    'good': 'max by (transformer) (grid_window_devices{kind="good"})',
    'estimated': 'max by (transformer) (grid_window_devices{kind="estimated"})',
    'missing': 'max by (transformer) (grid_window_devices{kind="missing"})',
    'late': 'max by (transformer) (grid_window_devices{kind="late"})',
    'incident': 'max by (transformer) (devices_fleet_overload_active)',
}
SCALARS = {
    'producer_rate': 'sum(rate(devices_fleet_events_total[30s]))',
    'kafka_in': 'sum(rate(kafka_topic_partition_current_offset{topic=~".*grid.telemetry.v1"}[30s]))',
    'kafka_out': 'sum(rate(kafka_topic_partition_current_offset{topic=~".*grid.transformer-load.v1"}[30s]))',
    'flink_busy': 'max(flink_taskmanager_job_task_busyTimeMsPerSecond) / 10',
    'flink_backpressure': 'max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond) / 10',
    'checkpoint_ms': 'max(flink_jobmanager_job_lastCheckpointDuration)',
    'checkpoint_failed': 'max(flink_jobmanager_job_numberOfFailedCheckpoints)',
    'checkpoint_done': 'max(flink_jobmanager_job_numberOfCompletedCheckpoints)',
    'output_age_ms': 'time() * 1000 - max(flink_taskmanager_job_task_operator_latest_output_ms)',
    'duplicates': 'sum(increase(flink_taskmanager_job_task_operator_duplicates[5m]))',
    'late': 'sum(increase(flink_taskmanager_job_task_operator_very_late[5m]))',
    'accepted_late': 'max(flink_taskmanager_job_task_operator_accepted_late_count)',
    'dlq': 'sum(increase(flink_taskmanager_job_task_operator_dlq[5m]))',
    'corrections': 'sum(increase(flink_taskmanager_job_task_operator_corrections[5m]))',
    'devices': 'max(devices_fleet_devices)',
    'controllable': 'max(devices_fleet_controllable_chargers)',
    'injected_late': 'sum(increase(devices_fleet_late_injected_total[5m]))',
    'injected_duplicates': 'sum(increase(devices_fleet_duplicates_injected_total[5m]))',
    'injected_dropped': 'sum(increase(devices_fleet_dropped_total[5m]))',
    'commands': 'sum(increase(devices_fleet_commands_applied_total[10m]))',
    'telemetry_retained': 'sum(kafka_topic_partition_current_offset{topic=~".*grid.telemetry.v1"} - kafka_topic_partition_oldest_offset{topic=~".*grid.telemetry.v1"})',
    'load_retained': 'sum(kafka_topic_partition_current_offset{topic=~".*grid.transformer-load.v1"} - kafka_topic_partition_oldest_offset{topic=~".*grid.transformer-load.v1"})',
}
LAG = ('sum by (consumergroup) (kafka_consumergroup_lag{topic=~".*grid.(telemetry|transformer-load).v1",'
       'consumergroup=~"agg-fleet.*|integration-kind.action.control.v1|integration-kind.projection"})')
VOLUMES = '100 * max by (persistentvolumeclaim) (kubelet_volume_stats_used_bytes{namespace="platform"} / kubelet_volume_stats_capacity_bytes{namespace="platform"})'


def number(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if value == value and abs(value) != float('inf') else None


def by_label(result, label='transformer'):
    out = {}
    for row in result:
        value = number(row['value'][1])
        if value is not None:
            out[row['metric'].get(label, '')] = value
    return out


def scalar(result):
    for row in result:
        value = number(row['value'][1])
        if value is not None:
            return value
    return None


def band(utilization, warning=80, critical=95):
    if utilization is None:
        return 'unknown'
    return 'critical' if utilization >= critical else 'warning' if utilization >= warning else 'normal'


class FleetData:
    def __init__(self, prometheus, api=None, token=None, fleet=None, fleet_token=None, clock=time.time):
        self.prometheus, self.api, self.token = prometheus.rstrip('/'), api, token
        self.fleet, self.fleet_token, self.clock = fleet, fleet_token, clock
        self.http = httpx.AsyncClient(timeout=httpx.Timeout(4.0), trust_env=False)
        self.tx = {}
        self.scalars = {}
        self.lag = {}
        self.volumes = {}
        self.actions, self.alerts = [], []
        self.fleet_history = deque(maxlen=KEEP)            # (t, net, ev, meter, solar, capacity)
        self.util_history = {}                              # transformer -> deque[(t, utilization, net, capacity)]
        self.pipeline_history = deque(maxlen=KEEP)         # (t, producer, kafka_in, kafka_out, busy, lag_total)
        self.updated = 0.0
        self.errors = {}
        self.last_injection = 0.0
        self.stopped = False

    async def query(self, promql):
        try:
            response = await self.http.get(self.prometheus + '/api/v1/query', params={'query': promql})
            response.raise_for_status()
            body = response.json()
            if body.get('status') == 'success':
                return body['data']['result']
        except (httpx.HTTPError, ValueError, KeyError):
            pass
        return None

    async def query_range(self, promql, start, end, step):
        try:
            response = await self.http.get(self.prometheus + '/api/v1/query_range',
                                           params=dict(query=promql, start=start, end=end, step=step))
            response.raise_for_status()
            body = response.json()
            if body.get('status') == 'success':
                return body['data']['result']
        except (httpx.HTTPError, ValueError, KeyError):
            pass
        return []

    async def backfill(self):
        end = int(self.clock())
        start = end - BACKFILL_SECONDS
        expressions = {name: f'sum({expr})' for name, expr in
                       (('net', TX['net']), ('ev', TX['ev']), ('meter', TX['meter']), ('solar', TX['solar']), ('capacity', TX['capacity']))}
        columns = {}
        for name, expr in expressions.items():
            rows = await self.query_range(expr, start, end, BACKFILL_STEP)
            columns[name] = {int(float(t)): number(v) for t, v in rows[0]['values']} if rows else {}
        latest = columns['capacity'][max(columns['capacity'])] if columns['capacity'] else None
        for t in sorted(columns['net']):
            if latest and (columns['capacity'].get(t) or 0) < latest * .8:
                continue   # samples from before the whole fleet reported
            self.fleet_history.append((t, *(columns[n].get(t) for n in ('net', 'ev', 'meter', 'solar', 'capacity'))))
        rates = {}
        for name, expr in (('producer', SCALARS['producer_rate']), ('in', SCALARS['kafka_in']), ('out', SCALARS['kafka_out']), ('busy', SCALARS['flink_busy'])):
            rows = await self.query_range(expr, start, end, BACKFILL_STEP)
            rates[name] = {int(float(t)): number(v) for t, v in rows[0]['values']} if rows else {}
        for t in sorted(rates['in']):
            self.pipeline_history.append((t, rates['producer'].get(t), rates['in'].get(t), rates['out'].get(t), rates['busy'].get(t), None))
        net = await self.query_range(TX['net'], start, end, BACKFILL_STEP)
        cap = await self.query_range(TX['capacity'], start, end, BACKFILL_STEP)
        capacity = {r['metric'].get('transformer'): {int(float(t)): number(v) for t, v in r['values']} for r in cap}
        for row in net:
            name = row['metric'].get('transformer')
            history = self.util_history.setdefault(name, deque(maxlen=KEEP))
            for t, v in row['values']:
                t, v = int(float(t)), number(v)
                c = capacity.get(name, {}).get(t)
                if v is not None and c:
                    history.append((t, v / c * 100, v, c))

    async def poll(self):
        names = list(TX)
        results = await asyncio.gather(*[self.query(TX[n]) for n in names],
                                       *[self.query(q) for q in SCALARS.values()], self.query(LAG), self.query(VOLUMES))
        tx_rows, scalar_rows, lag_rows, volume_rows = results[:len(names)], results[len(names):-2], results[-2], results[-1]
        if all(r is None for r in results):
            self.errors['prometheus'] = 'unreachable'
            return
        self.errors.pop('prometheus', None)
        now = self.clock()
        columns = {n: by_label(r or []) for n, r in zip(names, tx_rows)}
        ids = sorted(set(columns['net']) | set(columns['capacity']))
        fresh = {}
        for name in ids:
            net, capacity = columns['net'].get(name), columns['capacity'].get(name)
            utilization = net / capacity * 100 if net is not None and capacity else None
            fresh[name] = dict(
                net=net, capacity=capacity, utilization=utilization, ev=columns['ev'].get(name, 0.0),
                meter=columns['meter'].get(name, 0.0), solar=columns['solar'].get(name, 0.0),
                good=columns['good'].get(name), estimated=columns['estimated'].get(name, 0), missing=columns['missing'].get(name, 0),
                late=columns['late'].get(name, 0), incident=bool(columns['incident'].get(name, 0)), managed=name in columns['incident'], band=band(utilization))
            if utilization is not None:
                history = self.util_history.setdefault(name, deque(maxlen=KEEP))
                if not history or now - history[-1][0] >= STEP - .5:
                    history.append((now, utilization, net, capacity))
        if fresh:
            self.tx = fresh
            totals = [sum(v[k] or 0 for v in fresh.values()) for k in ('net', 'ev', 'meter', 'solar', 'capacity')]
            if not self.fleet_history or now - self.fleet_history[-1][0] >= STEP - .5:
                self.fleet_history.append((now, *totals))
        self.scalars = {n: scalar(r or []) for n, r in zip(SCALARS, scalar_rows)}
        self.lag = by_label(lag_rows or [], 'consumergroup')
        self.volumes = by_label(volume_rows or [], 'persistentvolumeclaim')
        if not self.pipeline_history or now - self.pipeline_history[-1][0] >= STEP - .5:
            s = self.scalars
            self.pipeline_history.append((now, s.get('producer_rate'), s.get('kafka_in'), s.get('kafka_out'),
                                          s.get('flink_busy'), sum(self.lag.values()) if self.lag else None))
        self.updated = now

    async def poll_api(self):
        if not self.api:
            return
        headers = {'Authorization': f'Bearer {self.token}'} if self.token else {}
        try:
            actions = await self.http.get(self.api + '/actions', params={'limit': 200}, headers=headers)
            alerts = await self.http.get(self.api + '/alerts', params={'limit': 200}, headers=headers)
            actions.raise_for_status()
            alerts.raise_for_status()
            self.actions = sorted(actions.json().get('items', []), key=lambda a: a['transition_time'], reverse=True)
            self.alerts = alerts.json().get('items', [])
            self.errors.pop('api', None)
        except (httpx.HTTPError, ValueError, KeyError):
            self.errors['api'] = 'unreachable'

    async def run(self):
        await self.backfill()
        ticks = 0
        while not self.stopped:
            try:
                await self.poll()
                if ticks % 2 == 0:
                    await self.poll_api()
            except Exception as exc:   # one bad poll must not end the loop
                self.errors['poll'] = type(exc).__name__
            ticks += 1
            await asyncio.sleep(2.5)

    async def control(self, path, body):
        """Fleet control call. Rate limited here so a public page cannot flood the simulator."""
        if not self.fleet or not self.fleet_token:
            raise RuntimeError('fleet control is not configured')
        if self.clock() - self.last_injection < 8:
            raise RuntimeError('wait a few seconds between controls')
        self.last_injection = self.clock()
        response = await self.http.post(self.fleet + path, json=body, headers={'Authorization': 'Bearer ' + self.fleet_token})
        response.raise_for_status()
        return response.json()

    def age(self):
        return None if not self.updated else max(0.0, self.clock() - self.updated)

    def summary(self):
        rows = list(self.tx.values())
        counts = {b: sum(1 for r in rows if r['band'] == b) for b in ('normal', 'warning', 'critical')}
        totals = {k: sum(r[k] or 0 for r in rows) for k in ('net', 'ev', 'meter', 'solar', 'capacity')}
        utilization = totals['net'] / totals['capacity'] * 100 if totals['capacity'] else None
        covered = sum((r['good'] or 0) + (r['estimated'] or 0) for r in rows)
        expected = covered + sum(r['missing'] or 0 for r in rows)
        return dict(counts=counts, totals=totals, utilization=utilization, coverage=covered / expected * 100 if expected else None,
                    count=len(rows), incidents=sum(1 for r in rows if r['incident']))

    async def close(self):
        self.stopped = True
        await self.http.aclose()


def from_environment():
    return FleetData(os.getenv('PROMETHEUS_URL', 'http://monitoring-prometheus.observability:9090'),
                     os.getenv('FLEET_API_URL', 'http://api:18084/api/v1'), os.getenv('DASHBOARD_API_TOKEN'),
                     os.getenv('FLEET_CONTROL_URL', 'http://fleet:8003'), os.getenv('FLEET_CONTROL_TOKEN'))
