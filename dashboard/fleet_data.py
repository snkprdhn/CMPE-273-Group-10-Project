"""Shared live data for the control room: one poller per process, any number of browser tabs.

Sources, all read-only except the fleet control calls:
  Prometheus  per-transformer load, device coverage, pipeline rates, lag, storage
  REST API    actions, load history and charger details for managed transformers
  Fleet       fault-injection and incident controls (token held server-side)

History is kept in memory for three hours at five-second resolution and backfilled from
Prometheus at start, so a restarted pod does not show an empty chart.
"""
import asyncio
from collections import deque
from datetime import datetime, timezone
import os
import time

import httpx

from dashboard.client import instant, stamp

STEP = 5                      # seconds between samples
KEEP = 3 * 3600 // STEP       # three hours of samples
BACKFILL_SECONDS = 3600
BACKFILL_STEP = 10

COUNTED = ('ACKS_PENDING', 'REDUCTION_CONFIRMED', 'RECOVERY_MONITORING')   # states that hold a confirmed reduction
ACTIVE_STATES = ('PLAN_CREATED', 'COMMANDS_DISPATCHED', 'ACKS_PENDING', 'REDUCTION_CONFIRMED', 'RECOVERY_MONITORING', 'RESTORE_DISPATCHED')
HELD_STATES = ('PARTIALLY_APPLIED', 'TIMED_OUT', 'NEEDS_OPERATOR_REVIEW')   # safe hold: no further commands until an operator acts
ROLLING = 60                  # seconds in the peak-avoided rolling mean
HOUR = 3600
SEGMENT_KEEP = 3 * 3600
LOADS_SECONDS = 600           # history requested from /transformers/{id}/loads
LOADS_PAGES = 3
LOADS_FRESH = 60              # the loads API is trusted only when its newest window ended this recently
HISTORY_SECONDS = 30 * 60     # span of the Prometheus fallback series
LOADS_MIN_GAP = 1.0           # seconds between loads fetches of one transformer
SELECTED_TTL = 60             # a transformer stays "watched" this long after a tab last showed it
DEMAND_CHARGE = 25.0          # illustrative $ per kW-month
DEVICES_TTL = 30              # the charger list stays "watched" this long after a tab last showed the Devices view
DEVICES_EVERY = 10            # seconds between charger list fetches while it is watched
THROTTLING = ('COMMANDS_DISPATCHED', 'ACKS_PENDING', 'REDUCTION_CONFIRMED', 'RECOVERY_MONITORING')   # a throttle is in force
TRIGGER_KEEP = 4000           # trigger-window utilizations remembered for the decision log
TRIGGER_LOOKUP = 12           # newest actions whose trigger window is looked up
TRIGGER_TRIES = 3             # polls spent looking for one action's trigger window

# The projection service re-exports the same business gauges and keeps them for about 140 s, so a plain `max by`
# would read its stale copy instead of the live Flink value. Both selectors below drop the projection series.
LIVE = 'exported_job!~"projection.*"'


def business(signal):
    return f'max by (transformer) (grid_business_kw{{signal="{signal}",{LIVE}}})'


def window_devices(kind):
    return f'max by (transformer) (grid_window_devices{{kind="{kind}",{LIVE}}})'


TX = {
    'net': business('net_load_kw'),
    'ev': business('ev_demand_kw'),
    'meter': business('meter_demand_kw'),
    'solar': business('solar_generation_kw'),
    'capacity': business('capacity_kw'),
    'good': window_devices('good'),
    'estimated': window_devices('estimated'),
    'missing': window_devices('missing'),
    'late': window_devices('late'),
    'incident': 'max by (transformer) (devices_fleet_overload_active)',
    'unmanaged': 'max by (transformer) (devices_fleet_unmanaged_kw)',
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
    'accepted_late': 'sum(increase(flink_taskmanager_job_task_operator_accepted_late_count[5m]))',
    'watermark_delay_ms': 'max(flink_taskmanager_job_task_operator_watermark_delay_ms)',
    'dlq': 'sum(increase(flink_taskmanager_job_task_operator_dlq[5m]))',
    'corrections': 'sum(increase(flink_taskmanager_job_task_operator_corrections[5m]))',
    'telemetry_total': 'sum(kafka_topic_partition_current_offset{topic=~".*grid.telemetry.v1"})',
    'devices': 'max(devices_fleet_devices)',
    'controllable': 'max(devices_fleet_controllable_chargers)',
    'injected_late': 'sum(increase(devices_fleet_late_injected_total[5m]))',
    'injected_duplicates': 'sum(increase(devices_fleet_duplicates_injected_total[5m]))',
    'injected_dropped': 'sum(increase(devices_fleet_dropped_total[5m]))',
    'commands': 'sum(increase(devices_fleet_commands_applied_total[10m]))',
    'telemetry_retained': 'sum(kafka_topic_partition_current_offset{topic=~".*grid.telemetry.v1"} - kafka_topic_partition_oldest_offset{topic=~".*grid.telemetry.v1"})',
    'load_retained': 'sum(kafka_topic_partition_current_offset{topic=~".*grid.transformer-load.v1"} - kafka_topic_partition_oldest_offset{topic=~".*grid.transformer-load.v1"})',
}
LAG = ('sum by (consumergroup) (kafka_consumergroup_lag{'
       'consumergroup=~"agg-fleet.*|integration-kind[.]action[.]control[.]v1|integration-kind[.]projection(-telemetry)?"}'
       ' unless (kafka_consumergroup_lag{consumergroup="integration-kind.projection",topic=~".*grid[.]telemetry[.]v1"}'
       ' and on (topic, partition) kafka_consumergroup_lag{consumergroup="integration-kind.projection-telemetry"})'
       ' unless kafka_consumergroup_lag{consumergroup="integration-kind.projection",topic=~".*grid[.]telemetry[.]dlq[.]v1"})')
PARTITION_RATE = 'sum by (partition) (rate(kafka_topic_partition_current_offset{topic=~".*grid.telemetry.v1"}[1m]))'
PARTITION_LAG = 'sum by (partition) (kafka_consumergroup_lag{consumergroup=~"agg-fleet.*",topic=~".*grid.telemetry.v1"})'
SUBTASK_RATE = 'sum by (subtask_index) (rate(flink_taskmanager_job_task_operator_input[1m]))'
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


def epoch(value):
    """Seconds since the Unix epoch for an ISO-8601 instant, or None when it is invalid or has no zone."""
    try:
        moment = instant(value)
    except (AttributeError, TypeError, ValueError):
        return None
    return None if moment.tzinfo is None else moment.timestamp()


def overlap(start, end, low, high):
    return max(0.0, min(end, high) - max(start, low))


def band(utilization, warning=80, critical=95):
    if utilization is None:
        return 'unknown'
    return 'critical' if utilization >= critical else 'warning' if utilization >= warning else 'normal'


def charger_row(view):
    """One DeviceView of /devices as a flat dict, or None when it is malformed."""
    try:
        device, telemetry = view['device'], view.get('latest_telemetry') or {}
        key, tx = device['device_id'], device['transformer_id']
        history = [c for c in view.get('command_history') or [] if isinstance(c, dict)]
        if not isinstance(key, str) or not isinstance(tx, str):
            return None
        priority = device.get('priority')
        last = max(history, key=lambda c: epoch(c.get('result_time')) or 0, default=None)
        return dict(
            id=key, tx=tx, controllable=device.get('controllable') is True,
            priority=priority if isinstance(priority, int) and not isinstance(priority, bool) else None,
            minimum=number(device.get('minimum_rate_kw')), maximum=number(device.get('maximum_rate_kw')),
            departure=device.get('departure_time') or telemetry.get('departure_time'),
            rate=number(telemetry.get('current_rate_kw')), power=number(telemetry.get('power_kw')), quality=telemetry.get('quality'),
            freshness=view.get('freshness') if isinstance(view.get('freshness'), str) else 'UNKNOWN',
            last=None if last is None else dict(outcome=last.get('outcome'), action_id=last.get('action_id'), applied=number(last.get('applied_rate_kw')),
                                                previous=number(last.get('previous_rate_kw')), at=last.get('result_time')))
    except (KeyError, TypeError, AttributeError):
        return None


SETTLED = ('REDUCTION_CONFIRMED', 'RECOVERY_MONITORING')   # every selected charger has been commanded and its result is in


def throttling_actions(actions, states=THROTTLING):
    """transformer -> ids of the actions whose throttle is in force right now (`states` narrows what counts)."""
    held = {}
    for a in actions:
        if a.get('status') in states and isinstance(a.get('transformer_id'), str):
            held.setdefault(a['transformer_id'], set()).add(a.get('action_id'))
    return held


def charger_state(charger, throttling, settled=None):
    """Throttled, Spared or Normal.

    Throttled: the transformer's throttle is in force and this charger's newest command belongs to it and lowered its rate.
    Spared: the throttle has settled (`settled`: reduction confirmed or recovery monitoring, so every selected charger has been
    commanded), the charger reports usable telemetry (GOOD or ESTIMATED), is charging and controllable, and it got no command from it.
    Before that, an uncommanded charger may simply not have its result projected yet, so it stays Normal.
    """
    ids = throttling.get(charger['tx'])
    if not ids:
        return 'Normal'
    last = charger['last']
    commanded = bool(last) and last['action_id'] in ids
    if commanded:
        applied, previous = last['applied'], last['previous']
        lowered = applied is not None and ((previous is not None and applied < previous) or
                                           (last['outcome'] == 'ALREADY_APPLIED' and charger['maximum'] is not None and applied < charger['maximum']))
        return 'Throttled' if last['outcome'] in ('APPLIED', 'ALREADY_APPLIED') and lowered else 'Normal'
    online = charger['quality'] in ('GOOD', 'ESTIMATED') and charger['freshness'] not in ('STALE', 'UNKNOWN')
    charging = (charger['power'] or 0) > 0 and (charger['rate'] or 0) > 0
    return 'Spared' if charger['tx'] in (settled if settled is not None else throttling) and online and charging and charger['controllable'] else 'Normal'


class FleetData:
    def __init__(self, prometheus, api=None, token=None, fleet=None, fleet_token=None, clock=time.time):
        self.prometheus, self.api, self.token = prometheus.rstrip('/'), api, token
        self.fleet, self.fleet_token, self.clock = fleet, fleet_token, clock
        self.http = httpx.AsyncClient(timeout=httpx.Timeout(4.0), trust_env=False)
        self.tx = {}
        self.scalars = {}
        self.lag = {}
        self.volumes = {}
        self.partition_rate = {}
        self.partition_lag = {}
        self.subtask_rate = {}
        self.actions = []
        self.actions_freshness = None                       # `freshness` of the last /actions page: STALE when the projection is behind
        self.api_good_at = None                             # time of the last /actions poll that worked
        self.segments = {}                                  # transformer -> [[start, end|None, kw]] confirmed throttle timeline
        self._open = {}                                     # action_id -> its open segment
        self._seen_active = {}                              # action_id -> last time it was seen holding a reduction
        self.loads = {}                                     # transformer -> dict(fetched, rows) shared by every tab
        self.selected = {}                                  # transformer -> last time a tab showed it
        self.triggers = {}                                  # (window_id, revision) -> utilization % at that window
        self._tries = {}                                    # action_id -> trigger-window lookups so far
        self.chargers = []                                  # the managed EV chargers (one page of /devices), newest poll
        self.devices_wanted = -1e9                          # last time a tab showed the Devices view
        self._devices_at = -1e9
        self._inflight, self._last_fetch = {}, {}
        self.fleet_history = deque(maxlen=KEEP)            # (t, net, ev, meter, solar, capacity)
        self.util_history = {}                              # transformer -> deque[(t, utilization, net, capacity)]
        self.unmanaged_history = {}                         # transformer -> deque[(t, offered net kW)]
        self.tx_history = {}                                # transformer -> deque[(t, net, ev, meter, solar, capacity)]
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
        await self.poll_api()          # seeds the throttle timeline with actions that are active now
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
        per = {}
        for key in ('net', 'ev', 'meter', 'solar', 'capacity', 'unmanaged'):
            rows = await self.query_range(TX[key], start, end, BACKFILL_STEP)
            per[key] = {r['metric'].get('transformer'): {int(float(t)): number(v) for t, v in r['values']} for r in rows}
        for name, net in per['net'].items():
            utilization = self.util_history.setdefault(name, deque(maxlen=KEEP))
            detail = self.tx_history.setdefault(name, deque(maxlen=KEEP))
            capacity = per['capacity'].get(name, {})
            for t in sorted(net):
                v, c = net[t], capacity.get(t)
                if v is None:
                    continue
                if c:
                    utilization.append((t, v / c * 100, v, c))
                offered = per['unmanaged'].get(name, {}).get(t)
                if offered is not None:
                    self.unmanaged_history.setdefault(name, deque(maxlen=KEEP)).append((t, offered))
                detail.append((t, v, *(per[k].get(name, {}).get(t) or 0.0 for k in ('ev', 'meter', 'solar')), c))

    async def poll(self):
        names = list(TX)
        results = await asyncio.gather(*[self.query(TX[n]) for n in names],
                                       *[self.query(q) for q in SCALARS.values()], self.query(LAG), self.query(VOLUMES),
                                       self.query(PARTITION_RATE), self.query(PARTITION_LAG), self.query(SUBTASK_RATE))
        tx_rows, scalar_rows = results[:len(names)], results[len(names):-5]
        lag_rows, volume_rows, part_rate_rows, part_lag_rows, sub_rate_rows = results[-5:]
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
                    offered = columns['unmanaged'].get(name)
                    if offered is not None:
                        self.unmanaged_history.setdefault(name, deque(maxlen=KEEP)).append((now, offered))
            if net is not None:
                detail = self.tx_history.setdefault(name, deque(maxlen=KEEP))
                if not detail or now - detail[-1][0] >= STEP - .5:
                    detail.append((now, net, fresh[name]['ev'], fresh[name]['meter'], fresh[name]['solar'], capacity))
        if fresh:
            self.tx = fresh
            totals = [sum(v[k] or 0 for v in fresh.values()) for k in ('net', 'ev', 'meter', 'solar', 'capacity')]
            if not self.fleet_history or now - self.fleet_history[-1][0] >= STEP - .5:
                self.fleet_history.append((now, *totals))
        self.scalars = {n: scalar(r or []) for n, r in zip(SCALARS, scalar_rows)}
        self.lag = by_label(lag_rows or [], 'consumergroup')
        self.volumes = by_label(volume_rows or [], 'persistentvolumeclaim')
        self.partition_rate = by_label(part_rate_rows or [], 'partition')
        self.partition_lag = by_label(part_lag_rows or [], 'partition')
        self.subtask_rate = by_label(sub_rate_rows or [], 'subtask_index')
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
            response = await self.http.get(self.api + '/actions', params={'limit': 200}, headers=headers)
            response.raise_for_status()
            page = response.json()
            self.actions = sorted(page.get('items', []), key=lambda a: a['transition_time'], reverse=True)
            self.actions_freshness = page.get('freshness')
            self.errors.pop('api', None)
            self.track(self.actions)
            self.api_good_at = self.clock()
        except (httpx.HTTPError, ValueError, KeyError):
            self.errors['api'] = 'unreachable'

    def track(self, actions, now=None):
        """Fold one /actions poll into the per-transformer throttle timeline of (start, end, confirmed kW).

        Only ACKS_PENDING (partial confirmed kW), REDUCTION_CONFIRMED and RECOVERY_MONITORING count. A segment ends at
        the RESTORE_DISPATCHED time; when the poll skipped to COMPLETED or a held state it ends at the last time the
        action was seen holding a reduction. Held states never count.
        """
        now = self.clock() if now is None else now
        present = set()
        for a in actions:
            try:
                key, tx, status = a['action_id'], a['transformer_id'], a['status']
                at = epoch(a['transition_time'])
                kw = number(a.get('confirmed_reduction_kw'))
            except (KeyError, TypeError, AttributeError):
                continue
            if at is None or not isinstance(key, str) or not isinstance(tx, str):
                continue
            present.add(key)
            seg = self._open.get(key)
            if status in COUNTED:
                kw = max(kw or 0.0, 0.0)
                self._seen_active[key] = now
                if seg and seg[2] == kw:
                    continue
                if seg:
                    seg[1] = max(at, seg[0])
                    del self._open[key]
                if kw > 0:
                    seg = [at, None, kw]
                    self.segments.setdefault(tx, []).append(seg)
                    self._open[key] = seg
            elif seg:
                seg[1] = max(at if status == 'RESTORE_DISPATCHED' else self._seen_active.get(key, seg[0]), seg[0])
                del self._open[key]
        for key in [k for k in self._open if k not in present]:
            seg = self._open.pop(key)
            seg[1] = max(self._seen_active.get(key, seg[0]), seg[0])
        for key in [k for k in self._seen_active if k not in present and k not in self._open]:
            del self._seen_active[key]
        horizon = now - SEGMENT_KEEP
        for tx in list(self.segments):
            self.segments[tx] = [g for g in self.segments[tx] if g[1] is None or g[1] >= horizon]
            if not self.segments[tx]:
                del self.segments[tx]

    def kpis(self, now=None):
        """Business numbers from the confirmed throttle timeline only.

        While the actions API is failing the clock stops at the last good poll, so open segments do not keep growing.
        """
        now = self.clock() if now is None else now
        if 'api' in self.errors and self.api_good_at is not None:
            now = min(now, self.api_good_at)
        low = now - HOUR
        shed = peak = kwh = 0.0
        for segments in self.segments.values():
            shed += sum(kw for s, e, kw in segments if e is None and s <= now)
            spans = [(s, now if e is None else min(e, now), kw) for s, e, kw in segments if s < now]
            if not spans:
                continue
            kwh += sum(kw * overlap(s, e, low, now) for s, e, kw in spans) / 3600
            candidates = {low, now}
            for s, e, _ in spans:
                candidates.update((s, e, s + ROLLING, e + ROLLING))
            peak += max(sum(kw * overlap(s, e, t - ROLLING, t) for s, e, kw in spans) / ROLLING
                        for t in candidates if low <= t <= now)
        return dict(shed_kw=shed, peak_avoided_kw=peak, kwh_shifted=kwh, dollars=peak * DEMAND_CHARGE)

    def loadable(self, tx):
        """False for a transformer the API does not manage: /loads answers 404 there, so it is never requested.
        A transformer that Prometheus has not reported yet is given the benefit of the doubt."""
        known = self.tx.get(tx)
        return bool(tx) and (known is None or bool(known.get('managed')))

    def select(self, tx):
        """Called by every active tab on each tick so the transformer it shows keeps being polled."""
        if self.loadable(tx):
            self.selected[tx] = self.clock()

    async def fetch_loads(self, tx):
        """Load history of one transformer. One request in flight per transformer, at least 1 s between fetches."""
        if not self.api or not self.loadable(tx):
            return self.loads.get(tx)
        running = self._inflight.get(tx)
        if running:
            return await asyncio.shield(running)
        if self.clock() - self._last_fetch.get(tx, -1e9) < LOADS_MIN_GAP:
            return self.loads.get(tx)
        self._last_fetch[tx] = self.clock()
        task = self._inflight[tx] = asyncio.ensure_future(self._load(tx))
        task.add_done_callback(lambda _: self._inflight.pop(tx, None))
        return await asyncio.shield(task)

    async def _load(self, tx):
        now = self.clock()
        headers = {'Authorization': f'Bearer {self.token}'} if self.token else {}
        params = dict(start=stamp(datetime.fromtimestamp(now - LOADS_SECONDS, timezone.utc)),
                      end=stamp(datetime.fromtimestamp(now + STEP, timezone.utc)), limit=200)
        rows = {}
        try:
            for _ in range(LOADS_PAGES):
                response = await self.http.get(f'{self.api}/transformers/{tx}/loads', params=params, headers=headers)
                response.raise_for_status()
                page = response.json()
                for item in page.get('items', []):
                    row = self._load_row(item)
                    if row:
                        self._remember(row)
                    if row and (row['start'] not in rows or row['revision'] >= rows[row['start']]['revision']):
                        rows[row['start']] = row
                if not page.get('next_cursor'):
                    break
                params = dict(params, cursor=page['next_cursor'])
        except (httpx.HTTPError, ValueError, KeyError, TypeError, AttributeError):
            self.errors['loads'] = 'unreachable'
            return self.loads.get(tx)
        self.errors.pop('loads', None)
        self.loads[tx] = dict(fetched=now, rows=[rows[k] for k in sorted(rows)])
        return self.loads[tx]

    def _remember(self, row):
        """Keep the utilization of every published revision: an action names the exact window and revision that fired it."""
        if row['window_id'] is None or row['utilization'] is None:
            return
        self.triggers[(row['window_id'], row['revision'])] = row['utilization']
        while len(self.triggers) > TRIGGER_KEEP:
            del self.triggers[next(iter(self.triggers))]

    def trigger_pct(self, action):
        """Utilization % of the window that triggered `action`, or None when that window is not cached."""
        try:
            return self.triggers.get((action['trigger_window_id'], action.get('trigger_revision', 0)))
        except (KeyError, TypeError, AttributeError):
            return None

    @staticmethod
    def _load_row(item):
        try:
            start, end = epoch(item['window_start']), epoch(item['window_end'])
            net, revision = number(item['net_load_kw']), item.get('revision', 0)
        except (KeyError, TypeError):
            return None
        if start is None or end is None or net is None or end <= start or not isinstance(revision, int):
            return None
        capacity = number(item.get('capacity_kw'))
        utilization = number(item.get('utilization_pct'))
        if utilization is None and capacity:
            utilization = net / capacity * 100
        window_id = item.get('window_id')
        return dict(start=start, end=end, net=net, capacity=capacity, revision=revision, utilization=utilization,
                    window_id=window_id if isinstance(window_id, str) else None)

    def shadow_series(self, tx):
        """(window_end, managed kW, shadow kW, capacity) per window. The shadow adds the confirmed reduction only to
        windows that lie entirely inside a throttle segment."""
        entry = self.loads.get(tx)
        if not entry:
            return []
        segments = self.segments.get(tx, [])
        out = []
        for row in entry['rows']:
            added = sum(kw for s, e, kw in segments if row['start'] >= s and (e is None or row['end'] <= e))
            out.append((row['end'], row['net'], row['net'] + added, row['capacity']))
        return out

    def history(self, tx):
        """Rows for the managed-vs-shadow chart and where they came from.

        (time, managed kW, shadow kW or None, capacity). Source is 'loads' (the API, when its newest window is recent),
        'prometheus' (the API has nothing recent: the projection can lag, and it keeps only some windows) or
        'monitor-only' (not managed: no shadow line, no API).
        """
        if not self.loadable(tx):
            return [(t, net, None, cap) for t, _, net, cap in self.util_history.get(tx, ())], 'monitor-only'
        offered = dict(self.unmanaged_history.get(tx, ()))
        horizon = self.clock() - HISTORY_SECONDS
        if offered and self.clock() - max(offered) <= LOADS_FRESH:
            return [(t, net, offered.get(t), cap) for t, _, net, cap in self.util_history.get(tx, ())
                    if t >= horizon], 'offered'
        entry = self.loads.get(tx)
        if entry and entry['rows'] and self.clock() - entry['rows'][-1]['end'] <= LOADS_FRESH:
            return self.shadow_series(tx), 'loads'
        segments = self.segments.get(tx, [])
        rows = []
        for t, _, net, cap in self.util_history.get(tx, ()):
            if t >= horizon:
                added = sum(kw for s, e, kw in segments if t - STEP >= s and (e is None or t <= e))
                rows.append((t, net, net + added, cap))
        return rows, 'prometheus'

    async def poll_loads(self):
        watched = [tx for tx, seen in self.selected.items() if self.clock() - seen < SELECTED_TTL]
        for tx in [t for t in self.selected if t not in watched]:
            del self.selected[tx]
        lookups = []
        for a in self.actions[:TRIGGER_LOOKUP]:
            key, tx = a.get('action_id'), a.get('transformer_id')
            if self.trigger_pct(a) is None and self._tries.get(key, 0) < TRIGGER_TRIES and self.loadable(tx) and tx not in watched + lookups:
                lookups.append(tx)
            if self.trigger_pct(a) is None and isinstance(key, str):
                self._tries[key] = self._tries.get(key, 0) + 1
        await asyncio.gather(*[self.fetch_loads(tx) for tx in watched + lookups])

    def watch_devices(self):
        """Called by every tab that shows the Devices view on each tick; the charger list is only fetched while it is renewed."""
        self.devices_wanted = self.clock()

    async def poll_devices(self, force=False):
        """One page of GET /devices?device_type=EV_CHARGER&limit=200. A single page, so the API writes no cursor rows."""
        if not self.api:
            return
        now = self.clock()
        if not force and now - self.devices_wanted > DEVICES_TTL:
            return
        if now - self._devices_at < (LOADS_MIN_GAP if force else DEVICES_EVERY):
            return
        self._devices_at = now
        headers = {'Authorization': f'Bearer {self.token}'} if self.token else {}
        try:
            response = await self.http.get(self.api + '/devices', params=dict(device_type='EV_CHARGER', limit=200), headers=headers)
            response.raise_for_status()
            items = response.json().get('items', [])
            if not isinstance(items, list):
                raise ValueError('items is not a list')
            self.chargers = sorted((c for c in map(charger_row, items) if c), key=lambda c: c['id'])
            self.errors.pop('devices', None)
        except (httpx.HTTPError, ValueError, KeyError, TypeError, AttributeError):
            self.errors['devices'] = 'unreachable'

    async def run(self):
        await self.backfill()
        ticks = 0
        while not self.stopped:
            try:
                await self.poll()
                if ticks % 2 == 0:
                    await self.poll_api()
                    await self.poll_loads()
                    await self.poll_devices()
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
