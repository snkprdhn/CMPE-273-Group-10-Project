"""Continuous synthetic fleet: many transformers, thousands of readings per minute, forever.

The pure model (topology, demand curves, overload schedule) has no I/O and is tested
directly. The runner publishes telemetry on the real Kafka topics. A few "hero" transformers
also have registered controllable chargers whose rate follows the real Action commands, so the
closed loop (overload, throttle, measured reduction, restore) repeats without an operator.

Run: python -m devices.fleet   (configuration comes from FLEET_* environment variables)
"""
from collections import deque
import hashlib
import json
import math
import os
import random
import signal
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import NAMESPACE_URL, uuid5

from devices.common import METRICS, event, instant, log, monitor, stamp, utc, validate

WINDOW = 5
# Per device mix of a transformer: 55% meters, 35% chargers, 10% solar.
SHARES = (('SMART_METER', 'meter', .55), ('EV_CHARGER', 'ev', .35), ('SOLAR', 'solar', .10))
METER_KW, EV_BASE_KW, SOLAR_PEAK_KW = 1.0, 3.0, 1.5
OVERLOAD_SHARE = 1.0    # fraction of a hero's chargers that draw full rate during its overload
EV_MIN, EV_MAX = 1.0, 7.2
CYCLE_SECONDS, OVERLOAD_SECONDS, OVERLOAD_OFFSET = 240, 90, 60
HERO_UTILIZATION = .54
HERO_WAVE, WAVE = .03, .12   # heroes stay steady between incidents so every overload is clean


def frac(*parts):
    """Deterministic value in [0, 1) from names; stable across restarts and processes."""
    digest = hashlib.blake2b('/'.join(map(str, parts)).encode(), digest_size=8).digest()
    return int.from_bytes(digest, 'big') / 2**64


def counts(per):
    if per < 4:
        raise ValueError('at least four devices per transformer')
    meters = round(per * SHARES[0][2])
    evs = round(per * SHARES[1][2])
    return {'SMART_METER': meters, 'EV_CHARGER': evs, 'SOLAR': per - meters - evs}


def transformer_id(index):
    return f'tx-{index:03}'


def fleet(transformers=25, per=40, heroes=5):
    """Topology, device capabilities and the hero transformer IDs. Indexes start at 1."""
    if not 1 <= transformers <= 200 or not 1 <= heroes <= transformers:
        raise ValueError('transformers 1..200 and 1 <= heroes <= transformers')
    mix = counts(per)
    prefix = {kind: name for kind, name, _ in SHARES}
    topology, capabilities = {'topology_version': f'fleet-{transformers}x{per}', 'transformers': []}, []
    for index in range(1, transformers + 1):
        tx = transformer_id(index)
        devices = []
        for kind, count in mix.items():
            for number in range(1, count + 1):
                device = f'{prefix[kind]}-{index:03}-{number:02}'
                devices.append({'device_id': device, 'device_type': kind})
                ev = kind == 'EV_CHARGER'
                capabilities.append(dict(device_id=device, transformer_id=tx, device_type=kind,
                    controllable=ev and index <= heroes, minimum_rate_kw=EV_MIN if ev else 0,
                    maximum_rate_kw=EV_MAX if ev else 100, priority=1 + int(frac(device) * 3),
                    capability_version=1, updated_at='1970-01-01T00:00:00Z'))
        nominal = (mix['SMART_METER'] * METER_KW + mix['EV_CHARGER'] * EV_BASE_KW
                   - mix['SOLAR'] * SOLAR_PEAK_KW * .6)
        utilization = HERO_UTILIZATION if index <= heroes else .42 + .36 * ((index * 7) % 10) / 9
        topology['transformers'].append(dict(transformer_id=tx, capacity_kw=round(nominal / utilization, 1), devices=devices))
    return topology, capabilities, [transformer_id(i) for i in range(1, heroes + 1)]


def overload_active(hero_index, t, cycle=CYCLE_SECONDS, length=OVERLOAD_SECONDS):
    """Hero overloads are staggered so some transformer is usually in an incident."""
    shift = hero_index * cycle / 5
    return OVERLOAD_OFFSET <= (t + shift) % cycle < OVERLOAD_OFFSET + length


def sun(t):
    return min(1.0, max(.1, .55 + .35 * math.sin(2 * math.pi * t / 1800 + .5)))


def offered_kw(device, kind, index, hero, t, window, cycle=CYCLE_SECONDS, length=OVERLOAD_SECONDS, forced=False):
    """Power the device would draw without any command. Solar is negative."""
    wave = 1 + (HERO_WAVE if hero else WAVE) * math.sin(2 * math.pi * t / 600 + index)
    noise = (frac(device, window) - .5) * .16
    if kind == 'SMART_METER':
        return round(max(.05, METER_KW * wave * (.8 + .4 * frac(device)) * (1 + noise)), 3)
    if kind == 'SOLAR':
        return round(-SOLAR_PEAK_KW * sun(t) * (.85 + .3 * frac(device)) * (1 + noise / 2), 3)
    if hero and (forced or overload_active(index - 1, t, cycle, length)) and frac(device, 'overload') < OVERLOAD_SHARE:
        return EV_MAX   # a full-rate evening charge; also keeps the restore target equal to the maximum
    return round(min(EV_MAX, max(EV_MIN, (2.0 + 2.0 * frac(device)) * wave * (1 + noise))), 3)


class Knobs:
    """Fault injection fractions. A timed burst overrides the baseline until it expires."""
    LIMITS = {'late': (0, .5), 'duplicate': (0, .5), 'drop': (0, .1), 'very_late': (0, .2)}
    BURSTS = {'out_of_order': {'late': .25}, 'duplicates': {'duplicate': .30}, 'very_late': {'very_late': .10}}

    def __init__(self, late=.01, duplicate=.01, drop=.003, very_late=.001, clock=time.monotonic):
        self.base = dict(late=late, duplicate=duplicate, drop=drop, very_late=very_late)
        self.clock = clock
        self.bursts = {}
        self.forced = {}
        self.heroes = ()
        self.lock = threading.Lock()

    def force_overload(self, transformer, seconds):
        """Operator-triggered incident on a managed transformer, for a bounded time."""
        if transformer not in self.heroes or isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or not 30 <= seconds <= 180:
            raise ValueError('transformer must be a managed one and seconds 30..180')
        with self.lock:
            self.forced[transformer] = self.clock() + seconds

    def is_forced(self, transformer):
        with self.lock:
            return self.forced.get(transformer, 0) > self.clock()

    def set(self, values):
        with self.lock:
            for name, value in values.items():
                low, high = self.LIMITS[name]
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not low <= value <= high:
                    raise ValueError(f'{name} must be a number in [{low}, {high}]')
                self.base[name] = float(value)

    def inject(self, kind, seconds):
        if kind not in self.BURSTS or isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or not 5 <= seconds <= 300:
            raise ValueError('kind is out_of_order, duplicates or very_late and seconds is 5..300')
        with self.lock:
            self.bursts[kind] = self.clock() + seconds

    def current(self):
        with self.lock:
            now = self.clock()
            values = dict(self.base)
            for kind, until in list(self.bursts.items()):
                if until <= now:
                    del self.bursts[kind]
                    continue
                for name, value in self.BURSTS[kind].items():
                    values[name] = max(values[name], value)
            return values

    def status(self):
        now = self.clock()
        with self.lock:
            return dict(self.base, active={k: round(v - now, 1) for k, v in self.bursts.items() if v > now},
                    overloads={k: round(v - now, 1) for k, v in self.forced.items() if v > now})


class FleetEdge:
    """Many fenced charger states behind the single-device interface the Kafka handler expects."""
    def __init__(self, states):
        self.states = states
        self.first = next(iter(states.values()))

    def apply(self, command, key, now, headers=None):
        if key not in self.states:
            raise ValueError('WRONG_SHARD')
        return self.states[key].apply(command, key, now, headers)

    def pending(self):
        return self.first.pending()

    def acknowledge(self, identity):
        self.first.acknowledge(identity)


class Control(BaseHTTPRequestHandler):
    knobs = None
    token = None

    def reply(self, code, body):
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def authorized(self):
        return bool(self.token) and self.headers.get('Authorization') == 'Bearer ' + self.token

    def do_GET(self):
        if self.path == '/status' and self.authorized():
            self.reply(200, self.knobs.status())
        else:
            self.reply(404 if self.authorized() else 401, {'error': 'unavailable'})

    def do_POST(self):
        if not self.authorized():
            return self.reply(401, {'error': 'unauthorized'})
        try:
            length = int(self.headers.get('Content-Length', '0'))
            if not 0 < length <= 1024:
                raise ValueError('body required, at most 1 KiB')
            body = json.loads(self.rfile.read(length))
            if self.path == '/inject':
                self.knobs.inject(body['kind'], body['seconds'])
            elif self.path == '/knobs':
                self.knobs.set(body)
            elif self.path == '/overload':
                self.knobs.force_overload(body['transformer'], body['seconds'])
            else:
                return self.reply(404, {'error': 'unknown path'})
            self.reply(200, self.knobs.status())
        except (ValueError, KeyError, TypeError) as exc:
            self.reply(400, {'error': str(exc)[:200]})

    def log_message(self, *args):
        pass


def serve_control(knobs, token, port, host='0.0.0.0'):
    handler = type('Handler', (Control,), dict(knobs=knobs, token=token))
    server = ThreadingHTTPServer((host, port), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def reading(device, kind, tx, boot, window, at, ingested, run, power, extra=None):
    payload = dict(device_id=device, transformer_id=tx, device_type=kind, boot_id=boot,
                   power_kw=power, quality='GOOD', sequence_number=window)
    if extra:
        payload.update(extra)
    value = event('telemetry.reading', payload, at, f'{run}/{boot}/{window}/{device}', run)
    value['ingested_at'] = stamp(ingested)
    return value


def apply_commands(messages, states, edge, publisher, receiver):
    """Apply a batch of charger commands, publish every result once, then commit the offsets.

    Effects and their result events are durable in PostgreSQL before anything is published or
    committed, so a crash in between replays safely (the command ID makes the effect idempotent).
    One flush and one offset commit per batch keeps a 14-charger plan well inside the ack timeout.
    """
    from confluent_kafka import TopicPartition
    last = {}
    for message in messages:
        if message.error():
            raise RuntimeError('Kafka command consumption failed')
        last[(message.topic(), message.partition())] = message.offset() + 1
        key = message.key().decode()
        if key not in states:
            METRICS['fleet_foreign_commands_skipped_total'] += 1   # an old run's device is not ours to apply
            continue
        raw = message.value()
        if len(raw) > 65536:
            raise ValueError('command exceeds 64KiB')
        edge.apply(json.loads(raw), key, utc(), message.headers())
        METRICS['fleet_commands_applied_total'] += 1
    pending = edge.pending()
    for identity, raw, carried in pending:
        value = json.loads(raw)
        topic = 'grid.command-result.v1' if value['event_type'] == 'command.result' else 'grid.telemetry.v1'
        key = value['payload']['device_id' if topic == 'grid.command-result.v1' else 'transformer_id']
        publisher.send(topic, key, value, json.loads(carried))
    if pending:
        publisher.drain()
        for identity, _, _ in pending:
            edge.acknowledge(identity)
    receiver.commit(offsets=[TopicPartition(t, p, o) for (t, p), o in last.items()], asynchronous=False)


def schedule(topology_devices, window_index, phases):
    """Event times for one window. Every device reports once, inside its own window."""
    start = window_index * WINDOW
    return sorted(((start + phases[d['device_id']], d) for d in topology_devices), key=lambda item: item[0])


def main():
    from confluent_kafka import Producer
    from devices.kafka import consumer, flush_results, Publisher
    from observability import setup
    setup('devices.fleet')
    env = os.environ
    transformers, per, hero_count = int(env.get('FLEET_TRANSFORMERS', 25)), int(env.get('FLEET_DEVICES', 40)), int(env.get('FLEET_HEROES', 5))
    prefix, bootstrap = env.get('FLEET_PREFIX', 'integration-kind.'), env.get('FLEET_BOOTSTRAP', 'grid-kafka-bootstrap:9092')
    run = env.get('FLEET_RUN', 'fleet')
    cycle, length = int(env.get('FLEET_CYCLE_SECONDS', CYCLE_SECONDS)), int(env.get('FLEET_OVERLOAD_SECONDS', OVERLOAD_SECONDS))
    validate('Id', run)
    topology, capabilities, heroes = fleet(transformers, per, hero_count)
    by_tx = {t['transformer_id']: t['devices'] for t in topology['transformers']}
    kinds = {c['device_id']: c['device_type'] for c in capabilities}
    owner = {c['device_id']: c['transformer_id'] for c in capabilities}
    phases = {c['device_id']: .2 + 4.4 * frac(c['device_id'], 'phase') for c in capabilities}
    hero_set = frozenset(heroes)
    boot = f'{run}-{int(time.time())}'
    knobs = Knobs(*(float(env.get(f'FLEET_{n.upper()}', v)) for n, v in
                    (('late', .01), ('duplicate', .01), ('drop', .003), ('very_late', .001))))
    knobs.heroes = tuple(heroes)
    token = env.get('FLEET_CONTROL_TOKEN')
    if token:
        serve_control(knobs, token, int(env.get('FLEET_CONTROL_PORT', 8003)))
    stopped = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stopped.set())

    states, client = {}, None
    hero_devices = [c for c in capabilities if c['controllable']]
    if hero_devices and env.get('EDGE_DSN'):
        import grpc
        import psycopg
        from devices.fencing import FencedEdgeState
        from devices.registry import Client, device_dict, device_message, pb
        client = Client(env.get('REGISTRY_ADDRESS', 'registry:50051'), 'simulator', env['REGISTRY_SIMULATOR_TOKEN'])
        edge_db = psycopg.connect(env['EDGE_DSN'], connect_timeout=2, autocommit=True)
        for wanted in hero_devices:
            wanted = dict(wanted, updated_at=stamp(utc()))
            try:
                actual = device_dict(client.call('GetDevice', pb.GetDeviceRequest(device_id=wanted['device_id'])))
                if any(actual[k] != wanted[k] for k in wanted if k != 'updated_at'):
                    raise RuntimeError('existing Registry capability mismatch; refusing to overwrite')
            except grpc.RpcError as exc:
                if exc.code() != grpc.StatusCode.NOT_FOUND:
                    raise
                actual = device_dict(client.call('RegisterDevice', pb.RegisterDeviceRequest(
                    operation_id=f'{run}-{wanted["device_id"]}', device=device_message(wanted))))
            states[wanted['device_id']] = FencedEdgeState(env['EDGE_DSN'], actual, db=edge_db)

        def beat():
            tick = 0
            while not stopped.wait(8):
                tick += 1
                for device in list(states):
                    try:
                        request = pb.HeartbeatRequest(operation_id=f'{boot}-hb-{tick}-{device}', device_id=device, capability_version=1)
                        request.observed_at.FromDatetime(utc() - timedelta(seconds=1))
                        # Direct stub call: 70 devices every 8 s would otherwise write a log line per call.
                        client.stub.Heartbeat(request, timeout=2, metadata=client.metadata)
                        METRICS['fleet_heartbeats_total'] += 1
                    except Exception:
                        METRICS['fleet_heartbeat_errors_total'] += 1
        threading.Thread(target=beat, daemon=True).start()
    edge = FleetEdge(states) if states else None

    class Quiet(Publisher):
        trace_delivery = False

        def send(self, topic, key, value, headers=None):
            return self._send(topic, key, value, [])   # no span or log line per reading

    publisher = Quiet(bootstrap, prefix, capacity=100000, delivery_ms=30000, **{
        'compression.type': 'lz4', 'linger.ms': 20, 'queue.buffering.max.kbytes': 65536})
    receiver = consumer(bootstrap, prefix, env.get('FLEET_GROUP', prefix + 'devices.edge.v1')) if edge else None
    health = monitor(int(env.get('FLEET_METRICS_PORT', 8002)), publisher.ready, '0.0.0.0')
    METRICS.update(fleet_devices=len(capabilities), fleet_transformers=transformers, fleet_heroes=len(heroes),
                   fleet_controllable_chargers=len(states))
    log('FLEET_STARTED')
    rng = random.Random(f'{boot}')
    ready, delayed = deque(), []       # due readings; (release time, topic, key, value) for injected delays
    generated = int(time.time() // WINDOW)
    try:
        flush = time.monotonic() + 10
        while not stopped.is_set():
            wall = time.time()
            current = int(wall // WINDOW)
            while generated < current + 1 and generated * WINDOW - wall < 1:
                for at_epoch, device in schedule([d for ds in by_tx.values() for d in ds], generated, phases):
                    ready.append((at_epoch, generated, device['device_id']))
                generated += 1
            fractions = knobs.current()
            while ready and ready[0][0] <= wall:
                at_epoch, window, device = ready.popleft()
                tx, kind = owner[device], kinds[device]
                index, hero = int(tx[3:]), tx in hero_set
                at = datetime.fromtimestamp(at_epoch, timezone.utc)
                controlled = device in states
                if controlled:
                    rate = states[device].current()[0]
                    wanted = offered_kw(device, kind, index, hero, at_epoch, window, cycle, length, knobs.is_forced(tx))
                    # A commanded limit is what the charger delivers. At the maximum it follows demand.
                    power = rate if rate < EV_MAX - 1e-9 else wanted
                    extra = dict(current_rate_kw=power, minimum_rate_kw=EV_MIN, priority=1 + int(frac(device) * 3))
                elif kind == 'EV_CHARGER':
                    power = offered_kw(device, kind, index, hero, at_epoch, window, cycle, length, knobs.is_forced(tx))
                    extra = dict(current_rate_kw=power, minimum_rate_kw=EV_MIN, priority=1 + int(frac(device) * 3))
                else:
                    power, extra = offered_kw(device, kind, index, hero, at_epoch, window), None
                value = reading(device, kind, tx, boot, window, at, utc(), run, power, extra)
                if not controlled:
                    roll = rng.random()
                    if roll < fractions['drop']:
                        METRICS['fleet_dropped_total'] += 1
                        continue
                    if roll < fractions['drop'] + fractions['very_late']:
                        delayed.append((wall + rng.uniform(40, 55), tx, value))
                        METRICS['fleet_very_late_injected_total'] += 1
                        continue
                    if roll < fractions['drop'] + fractions['very_late'] + fractions['late']:
                        delayed.append((wall + rng.uniform(7, 20), tx, value))
                        METRICS['fleet_late_injected_total'] += 1
                        continue
                    if rng.random() < fractions['duplicate']:
                        publisher.send('grid.telemetry.v1', tx, value)
                        METRICS['fleet_duplicates_injected_total'] += 1
                publisher.send('grid.telemetry.v1', tx, value)
                METRICS['fleet_events_total'] += 1
            for item in [d for d in delayed if d[0] <= wall]:
                delayed.remove(item)
                value = dict(item[2], ingested_at=stamp(utc()))
                publisher.send('grid.telemetry.v1', item[1], value)
                METRICS['fleet_events_total'] += 1
            for tx in heroes:
                METRICS[f'fleet_overload_active{{transformer="{tx}"}}'] = int(
                    overload_active(int(tx[3:]) - 1, wall, cycle, length) or knobs.is_forced(tx))
            if receiver is not None:
                messages = receiver.consume(num_messages=100, timeout=0)
                if messages:
                    apply_commands(messages, states, edge, publisher, receiver)
            publisher.producer.poll(0)
            if time.monotonic() >= flush:
                flush_results(edge, publisher) if edge else None
                flush = time.monotonic() + 10
            METRICS['fleet_backlog'] = len(ready) + len(delayed)
            stopped.wait(.02)
    finally:
        try:
            publisher.drain()
        finally:
            if receiver is not None:
                receiver.close()
            health.shutdown()


def topology_cli(argv):
    """python -m devices.fleet topology [TRANSFORMERS [DEVICES [HEROES]]]  prints the Flink topology fixture."""
    numbers = [int(a) for a in argv[2:5]]
    topology, _, _ = fleet(*(numbers + [25, 40, 5][len(numbers):]))
    print(json.dumps(topology, indent=1))


if __name__ == '__main__':
    import sys
    if sys.argv[1:2] == ['topology']:
        topology_cli(sys.argv)
    else:
        main()
