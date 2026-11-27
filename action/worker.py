"""Local worker: Lease, epoch, restart reconciliation and durable Kafka inbox."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import threading
import time

from confluent_kafka import Consumer
from jsonschema.exceptions import ValidationError

from observability import setup, boundary, leader, log

from action.control import Conflict, Control
from action.fixtures import FakeRegistry
from action.leadership import Leadership, Lease
from action.registry import Registry


def main():
    setup('action.worker')
    dsn = os.environ['CONTROL_DSN']
    prefix = os.getenv('KAFKA_PREFIX', 'local-delivery.')
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    lease = Lease(namespace=os.getenv('LEASE_NAMESPACE', 'smart-grid-control'),
                  name=os.getenv('LEASE_NAME', 'action'))
    authority = Leadership(dsn, lease)
    consumer = None
    registry = None
    health = Path(os.environ['WORKER_HEALTH_FILE']) if 'WORKER_HEALTH_FILE' in os.environ else None
    # Transformers under automatic control. Empty means all. Readings and loads of other
    # transformers cannot lead to a command, so they are skipped before any decoding or SQL.
    scope = frozenset(t for t in os.getenv('CONTROL_TRANSFORMERS', '').split(',') if t)
    # Unattended deployments (the live demo) release plans held for review after a restart or timeout.
    auto_compensate = os.getenv('ACTION_AUTO_COMPENSATE') == '1'
    try:
        if 'CONTROL_SNAPSHOTS' in os.environ:
            with open(os.environ['CONTROL_SNAPSHOTS']) as source:
                registry = FakeRegistry(json.load(source))
        else:
            registry = Registry(os.getenv('REGISTRY_ADDRESS', '127.0.0.1:15051'),
                                os.environ['REGISTRY_ACTION_TOKEN'], dsn)
        while not stop.is_set():
            if not lease.step():
                leader(False)
                if health is not None:
                    health.touch()
                stop.wait(1)
                continue
            leader(True)
            epoch = authority.activate()
            control = Control(dsn, registry, epoch, authority, publish_alerts=isinstance(registry, Registry))
            control.reconcile(datetime.now(timezone.utc))
            consumer = Consumer({'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP', '127.0.0.1:19092'),
                'group.id': prefix + 'action.control.v1', 'enable.auto.commit': False,
                'enable.auto.offset.store': False, 'auto.offset.reset': 'earliest',
                'isolation.level': 'read_committed', 'allow.auto.create.topics': False})
            topics = [prefix + 'grid.transformer-load.v1', prefix + 'grid.command-result.v1']
            if isinstance(registry, Registry):
                topics.append(prefix + 'grid.telemetry.v1')
            consumer.subscribe(topics)
            renew_at, tick_at = time.monotonic() + getattr(lease, 'duration', 15) / 3, 0
            pending, commit_at = 0, time.monotonic() + .5
            telemetry_seen = rejected = 0
            compensate_at = 0
            while not stop.is_set():
                # Keep transport/SQL leadership checks off the per-reading hot path.
                # Control transactions still verify the live Lease and durable epoch.
                if time.monotonic() >= renew_at:
                    if not lease.step():
                        leader(False)
                        break
                    authority.activate()
                    renew_at = time.monotonic() + getattr(lease, 'duration', 15) / 3
                message = consumer.poll(.2)
                now = datetime.now(timezone.utc)
                if message is not None:
                    if message.error():
                        raise RuntimeError(str(message.error()))
                    if len(message.value()) > 65536:
                        raise ValueError('event exceeds 64KiB')
                    if scope and not message.topic().endswith('grid.command-result.v1') and message.key().decode(errors='replace') not in scope:
                        consumer.store_offsets(message=message)
                        pending += 1
                    elif message.topic().endswith('grid.telemetry.v1'):
                        # Charger observations are advisory input. One malformed or hostile record
                        # must not stop control: count it, log a bounded sample, and move on.
                        # Authoritative load and result records below still fail closed.
                        telemetry_seen += 1
                        try:
                            event, key = json.loads(message.value()), message.key().decode()
                            if telemetry_seen % 64 == 1:
                                with boundary('kafka.process', message.headers(), event):
                                    registry.observe(event, key, now)
                            else:
                                registry.observe(event, key, now)
                        except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, ValidationError):
                            rejected += 1
                            if rejected <= 20 or rejected % 1000 == 0:
                                log('TELEMETRY_REJECTED', error_category='telemetry_validation')
                        consumer.store_offsets(message=message)
                        pending += 1
                    else:
                        # Decode first: malformed records still fail at the wire boundary.
                        event, key = json.loads(message.value()), message.key().decode()
                        with boundary('kafka.process', message.headers(), event):
                            handler = control.load if message.topic().endswith('grid.transformer-load.v1') else control.result
                            try:
                                handler(event, key, now)
                            except Conflict as exc:
                                # A restarted stream can re-emit a window revision with different contents, and a
                                # retried restore can be answered twice. The first accepted record stays authoritative;
                                # the conflicting copy is logged and not applied. Any other conflict (a stale saga
                                # version) still stops the worker.
                                allowed = {'grid.transformer-load.v1': 'EVENT_ID_REUSED', 'grid.command-result.v1': 'CONFLICTING_RESULT'}
                                if allowed.get(message.topic().removeprefix(prefix)) != str(exc):
                                    raise
                                log('RECORD_CONFLICT_SKIPPED', error_category=str(exc).lower())
                            # Effects/inbox are durable before offsets are eligible.
                            # A crash before this bounded batch commits causes safe replay.
                            consumer.store_offsets(message=message)
                            pending += 1
                if pending and (pending >= 100 or time.monotonic() >= commit_at):
                    consumer.commit(asynchronous=False)
                    pending, commit_at = 0, time.monotonic() + .5
                if time.monotonic() >= tick_at:
                    control.tick(now)
                    tick_at = time.monotonic() + .5
                    if auto_compensate and time.monotonic() >= compensate_at:
                        control.compensate(now)
                        compensate_at = time.monotonic() + 5
                if health is not None:
                    health.touch()
            break  # a lost incarnation exits; restart with a fresh identity
    finally:
        leader(False)
        # Fence first; never keep serving while closing group membership.
        try:
            authority.stop()
        finally:
            try:
                if consumer is not None:
                    consumer.close()
            finally:
                if isinstance(registry, Registry):
                    registry.close()


if __name__ == '__main__':
    main()
