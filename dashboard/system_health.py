"""Live system health tab: Kafka throughput + Flink metrics via Prometheus HTTP API and Flink REST.

Dashboard component. No Keycloak dependency; reads only from Prometheus and
the Flink REST service. Both sources are optional: the tab renders with partial data if
either source is unreachable, so a Flink restart during the demo does not blank the page.

Prometheus queries:
  - Kafka messages in/s:   rate of topic partition offset growth on the telemetry topic
  - Consumer lag total:    sum of lag across all partitions for the aggregation group
  - Per-partition lag:     per-partition breakdown for the lag chart

Flink REST queries (direct, not via Prometheus — real-time subtask counters):
  - numRecordsInPerSecond / numRecordsOutPerSecond per running job
  - lastCheckpointDuration / lastCheckpointSize
"""
import asyncio
import os
from collections import deque
from datetime import datetime, timezone

import httpx
from nicegui import ui

# ── Prometheus PromQL expressions ───────────────────────────────────────────────

_KAFKA_IN_RATE = (
    'sum(rate(kafka_topic_partition_current_offset'
    '{topic=~"integration-kind\\.grid\\.telemetry.*"}[30s]))'
)
_CONSUMER_LAG_TOTAL = (
    'sum(kafka_consumergroup_lag'
    '{consumergroup=~".*aggregation.*"})'
)
_CONSUMER_LAG_BY_PARTITION = (
    'sum by (partition) (kafka_consumergroup_lag'
    '{consumergroup=~".*aggregation.*"})'
)
_FLINK_HEAP = 'flink_jobmanager_Status_JVM_Memory_Heap_Used'

# ── History buffers (60 samples × 3 s = 3-minute rolling window) ────────────────

_HISTORY_LEN = 60


def _empty_history():
    return deque(maxlen=_HISTORY_LEN)


# ── Prometheus helper ────────────────────────────────────────────────────────────

async def _query_prometheus(session: httpx.AsyncClient, url: str, promql: str) -> list:
    """Return the ``result`` list from a Prometheus instant-query response, or [] on error."""
    try:
        r = await session.get(f'{url}/api/v1/query', params={'query': promql}, timeout=3.0)
        r.raise_for_status()
        data = r.json()
        if data.get('status') == 'success':
            return data.get('data', {}).get('result', [])
    except (httpx.HTTPError, ValueError, KeyError):
        pass
    return []


def _scalar(results: list, default=None):
    """Extract a single float from a Prometheus instant-query result list."""
    try:
        return float(results[0]['value'][1])
    except (IndexError, KeyError, TypeError, ValueError):
        return default


# ── Flink REST helper ────────────────────────────────────────────────────────────

async def _query_flink(session: httpx.AsyncClient, url: str) -> dict:
    """Return a dict of Flink job metrics, or an empty dict on error."""
    try:
        jobs_r = await session.get(f'{url}/jobs', timeout=3.0)
        jobs_r.raise_for_status()
        jobs = jobs_r.json().get('jobs', [])
        running = [j['id'] for j in jobs if j.get('status') == 'RUNNING']
        if not running:
            return {}
        jid = running[0]
        metrics_r = await session.get(
            f'{url}/jobs/{jid}/metrics',
            params={'get': 'numRecordsInPerSecond,numRecordsOutPerSecond,'
                           'lastCheckpointDuration,lastCheckpointSize'},
            timeout=3.0,
        )
        metrics_r.raise_for_status()
        return {m['id']: m.get('sum', m.get('value')) for m in metrics_r.json()}
    except (httpx.HTTPError, ValueError, KeyError):
        return {}


# ── Formatting helpers ───────────────────────────────────────────────────────────

def _fmt_rate(value) -> str:
    if value is None:
        return '—'
    return f'{float(value):,.0f} /s'


def _fmt_lag(value) -> str:
    if value is None:
        return '—'
    v = float(value)
    return f'{v:,.0f}' if v < 1_000_000 else f'{v/1_000_000:.2f} M'


def _fmt_ms(value) -> str:
    if value is None:
        return '—'
    return f'{float(value):,.0f} ms'


def _fmt_mb(value) -> str:
    if value is None:
        return '—'
    return f'{float(value)/1_048_576:,.1f} MiB'


# ── Main render function ─────────────────────────────────────────────────────────

def render_system_health(prometheus_url: str | None = None,
                          flink_url: str | None = None,
                          grafana_url: str | None = None) -> None:
    """Render the system health tab inside the current NiceGUI page context."""

    prom_url = prometheus_url or os.getenv(
        'PROMETHEUS_URL',
        'http://prometheus-stack-kube-prom-prometheus.observability:9090',
    )
    fl_url = flink_url or os.getenv('FLINK_REST_URL', 'http://aggregation-rest:8081')
    gf_url = grafana_url or os.getenv('GRAFANA_URL', 'http://localhost:3000')

    # Shared HTTP session for all polls (reused across timer ticks).
    session = httpx.AsyncClient(timeout=4.0, trust_env=False)

    # Rolling history for charts.
    hist_records_in: deque[float] = _empty_history()
    hist_records_out: deque[float] = _empty_history()
    hist_lag: deque[dict] = _empty_history()   # list of {partition: lag} per tick
    hist_times: deque[str] = _empty_history()  # HH:MM:SS labels

    # ── KPI card widgets ─────────────────────────────────────────────────────────
    with ui.column().classes('console gap-6').props('id=system-health-main'):

        with ui.row().classes('w-full justify-between items-start'):
            with ui.column().classes('gap-1'):
                ui.label('SYSTEM HEALTH').classes('eyebrow')
                ui.label('Kafka throughput · Flink aggregation · Consumer lag').classes('muted')
            ui.link('Open Grafana →', gf_url, new_tab=True).classes('text-sm')

        # Four KPI cards
        with ui.element('div').classes('kpis'):
            with ui.card().classes('gap-2'):
                ui.label('Records In/s').classes('eyebrow')
                records_in_label = ui.label('—').classes('metric').props(
                    'aria-label="Flink records ingested per second"')
                ui.label('Flink ingest rate').classes('muted text-sm')

            with ui.card().classes('gap-2'):
                ui.label('Records Out/s').classes('eyebrow')
                records_out_label = ui.label('—').classes('metric').props(
                    'aria-label="Flink records emitted per second"')
                ui.label('Flink emit rate').classes('muted text-sm')

            with ui.card().classes('gap-2'):
                ui.label('Consumer Lag').classes('eyebrow')
                lag_label = ui.label('—').classes('metric').props(
                    'aria-label="Total Kafka consumer lag across all partitions"')
                ui.label('All partitions · aggregation group').classes('muted text-sm')

            with ui.card().classes('gap-2'):
                ui.label('Last Checkpoint').classes('eyebrow')
                checkpoint_label = ui.label('—').classes('metric').props(
                    'aria-label="Duration of the most recent Flink checkpoint"')
                checkpoint_size_label = ui.label('—').classes('muted text-sm')

        # ── Records in/out chart ──────────────────────────────────────────────────
        with ui.card().classes('w-full gap-3'):
            ui.label('Flink throughput — records/s').classes('eyebrow')
            throughput_chart = ui.echart({
                'animation': False,
                'aria': {'enabled': True, 'label': {
                    'description': 'Flink records per second over time; '
                                   'records-in in blue, records-out in green'}},
                'tooltip': {'trigger': 'axis'},
                'legend': {'data': ['Records In/s', 'Records Out/s']},
                'grid': {'left': 60, 'right': 20, 'top': 30, 'bottom': 45},
                'xAxis': {'type': 'category', 'data': [], 'name': 'UTC'},
                'yAxis': {'type': 'value', 'name': 'records/s', 'min': 0},
                'series': [
                    {'name': 'Records In/s', 'type': 'line', 'smooth': False,
                     'data': [], 'showSymbol': False,
                     'lineStyle': {'color': '#2563a6', 'width': 2},
                     'areaStyle': {'color': '#e4eef9'}},
                    {'name': 'Records Out/s', 'type': 'line', 'smooth': False,
                     'data': [], 'showSymbol': False,
                     'lineStyle': {'color': '#166344', 'width': 2}},
                ],
            }).classes('w-full h-56').props(
                'role=img aria-label="Flink throughput chart"')

        # ── Consumer lag chart ────────────────────────────────────────────────────
        with ui.card().classes('w-full gap-3'):
            ui.label('Kafka consumer lag — per partition').classes('eyebrow')
            lag_chart = ui.echart({
                'animation': False,
                'aria': {'enabled': True, 'label': {
                    'description': 'Kafka consumer lag per partition over time'}},
                'tooltip': {'trigger': 'axis'},
                'legend': {'data': []},
                'grid': {'left': 60, 'right': 20, 'top': 30, 'bottom': 45},
                'xAxis': {'type': 'category', 'data': [], 'name': 'UTC'},
                'yAxis': {'type': 'value', 'name': 'messages', 'min': 0},
                'series': [],
            }).classes('w-full h-56').props(
                'role=img aria-label="Consumer lag chart"')

        last_error = ui.label('').classes('muted text-sm').props('aria-live=polite')

    # ── Async poll and update ─────────────────────────────────────────────────────

    async def _refresh():
        now_label = datetime.now(timezone.utc).strftime('%H:%M:%S')

        # Query both sources concurrently.
        prom_in, prom_lag_total, prom_lag_parts, prom_heap, flink_metrics = \
            await asyncio.gather(
                _query_prometheus(session, prom_url, _KAFKA_IN_RATE),
                _query_prometheus(session, prom_url, _CONSUMER_LAG_TOTAL),
                _query_prometheus(session, prom_url, _CONSUMER_LAG_BY_PARTITION),
                _query_prometheus(session, prom_url, _FLINK_HEAP),
                _query_flink(session, fl_url),
                return_exceptions=True,
            )

        # Guard against unexpected exceptions returned by gather.
        def _safe(val, default):
            return default if isinstance(val, Exception) else val

        prom_in = _safe(prom_in, [])
        prom_lag_total = _safe(prom_lag_total, [])
        prom_lag_parts = _safe(prom_lag_parts, [])
        flink_metrics = _safe(flink_metrics, {})

        # Prefer Flink REST records/s (more real-time than Prometheus scrape lag).
        f_in = flink_metrics.get('numRecordsInPerSecond')
        f_out = flink_metrics.get('numRecordsOutPerSecond')
        f_ckpt_ms = flink_metrics.get('lastCheckpointDuration')
        f_ckpt_sz = flink_metrics.get('lastCheckpointSize')

        # Fall back to Prometheus for records-in if Flink REST is unavailable.
        kafka_in = _scalar(prom_in) if f_in is None else None
        lag_total = _scalar(prom_lag_total)

        # Update KPI labels.
        records_in_label.set_text(_fmt_rate(f_in if f_in is not None else kafka_in))
        records_out_label.set_text(_fmt_rate(f_out))
        lag_label.set_text(_fmt_lag(lag_total))
        checkpoint_label.set_text(_fmt_ms(f_ckpt_ms))
        checkpoint_size_label.set_text(_fmt_mb(f_ckpt_sz) if f_ckpt_sz is not None else 'checkpoint size')

        # Update history buffers.
        hist_times.append(now_label)
        hist_records_in.append(float(f_in) if f_in is not None else
                               (float(kafka_in) if kafka_in is not None else 0.0))
        hist_records_out.append(float(f_out) if f_out is not None else 0.0)

        # Lag per partition.
        partition_lag: dict[str, float] = {}
        for r in prom_lag_parts if isinstance(prom_lag_parts, list) else []:
            try:
                part = r['metric'].get('partition', '?')
                partition_lag[f'p{part}'] = float(r['value'][1])
            except (KeyError, TypeError, ValueError):
                pass
        hist_lag.append(partition_lag)

        times_list = list(hist_times)

        # Push throughput chart update.
        throughput_chart.options['xAxis']['data'] = times_list
        throughput_chart.options['series'][0]['data'] = list(hist_records_in)
        throughput_chart.options['series'][1]['data'] = list(hist_records_out)
        throughput_chart.update()

        # Push lag chart update — rebuild series if partition set changed.
        all_partitions = sorted({p for tick in hist_lag for p in tick})
        lag_chart.options['xAxis']['data'] = times_list
        lag_chart.options['legend']['data'] = all_partitions
        lag_chart.options['series'] = [
            {'name': p, 'type': 'line', 'smooth': False, 'showSymbol': False,
             'data': [tick.get(p, 0) for tick in hist_lag]}
            for p in all_partitions
        ]
        lag_chart.update()

        # Clear any stale error message.
        last_error.set_text('')

    async def _safe_refresh():
        try:
            await _refresh()
        except Exception as exc:  # noqa: BLE001
            last_error.set_text(f'Metrics unavailable: {exc}')

    # Drive polls every 3 seconds; stop timer when the page is closed.
    timer = ui.timer(3.0, _safe_refresh)

    async def _cleanup():
        timer.cancel()
        await session.aclose()

    ui.context.client.on_disconnect(_cleanup)
