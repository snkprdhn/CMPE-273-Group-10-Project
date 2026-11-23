"""Local worker: Lease, epoch, restart reconciliation and durable Kafka inbox."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import threading

from confluent_kafka import Consumer

from observability import setup, boundary, leader

from member3.control import Control
from member3.fixtures import FakeRegistry
from member3.leadership import Leadership, Lease
from member3.registry import Registry


def main():
    setup('member3.action')
    dsn = os.environ['CONTROL_DSN']
    prefix = os.getenv('KAFKA_PREFIX', 'local-m3.')
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    lease = Lease(namespace=os.getenv('LEASE_NAMESPACE', 'smart-grid-control'),
                  name=os.getenv('LEASE_NAME', 'action'))
    authority = Leadership(dsn, lease)
    consumer = None
    registry = None
    health = Path(os.environ['WORKER_HEALTH_FILE']) if 'WORKER_HEALTH_FILE' in os.environ else None
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
                'group.id': prefix + 'member3.control.v1', 'enable.auto.commit': False,
                'enable.auto.offset.store': False, 'auto.offset.reset': 'earliest',
                'isolation.level': 'read_committed', 'allow.auto.create.topics': False})
            topics = [prefix + 'grid.transformer-load.v1', prefix + 'grid.command-result.v1']
            if isinstance(registry, Registry):
                topics.append(prefix + 'grid.telemetry.v1')
            consumer.subscribe(topics)
            while not stop.is_set():
                if not lease.step():
                    leader(False)
                    break
                authority.activate()
                message = consumer.poll(.2)
                now = datetime.now(timezone.utc)
                if message is not None:
                    if message.error():
                        raise RuntimeError(str(message.error()))
                    if len(message.value()) > 65536:
                        raise ValueError('event exceeds 64KiB')
                    # Decode first: malformed records still fail at the wire boundary.
                    event, key = json.loads(message.value()), message.key().decode()
                    with boundary('kafka.process', message.headers(), event):
                        if message.topic().endswith('grid.telemetry.v1'):
                            registry.observe(event, key, now)
                        else:
                            handler = control.load if message.topic().endswith('grid.transformer-load.v1') else control.result
                            handler(event, key, now)
                        consumer.commit(message=message, asynchronous=False)
                control.tick(now)
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
