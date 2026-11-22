"""Local worker: Lease, epoch, restart reconciliation and durable Kafka inbox."""
from datetime import datetime, timezone
import json
import os
import signal
import threading

from confluent_kafka import Consumer

from member3.control import Control
from member3.fixtures import FakeRegistry
from member3.leadership import Leadership, Lease


def main():
    dsn = os.environ['CONTROL_DSN']
    prefix = os.getenv('KAFKA_PREFIX', 'local-m3.')
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    # Explicit fixture adapter: live observations are a separate integration gate.
    registry = FakeRegistry()
    if os.getenv('CONTROL_SNAPSHOTS'):
        with open(os.environ['CONTROL_SNAPSHOTS']) as source:
            registry.values = json.load(source)
    lease = Lease(namespace=os.getenv('LEASE_NAMESPACE', 'smart-grid-control'),
                  name=os.getenv('LEASE_NAME', 'action'))
    authority = Leadership(dsn, lease)
    consumer = None
    try:
        while not stop.is_set():
            if not lease.step():
                stop.wait(1)
                continue
            epoch = authority.activate()
            control = Control(dsn, registry, epoch, authority)
            control.reconcile(datetime.now(timezone.utc))
            consumer = Consumer({'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP', '127.0.0.1:19092'),
                'group.id': prefix + 'member3.control.v1', 'enable.auto.commit': False,
                'enable.auto.offset.store': False, 'auto.offset.reset': 'earliest',
                'isolation.level': 'read_committed', 'allow.auto.create.topics': False})
            consumer.subscribe([prefix + 'grid.transformer-load.v1', prefix + 'grid.command-result.v1'])
            while not stop.is_set():
                if not lease.step():
                    break
                authority.activate()
                message = consumer.poll(.2)
                now = datetime.now(timezone.utc)
                if message is not None:
                    if message.error():
                        raise RuntimeError(str(message.error()))
                    if len(message.value()) > 65536:
                        raise ValueError('event exceeds 64KiB')
                    event, key = json.loads(message.value()), message.key().decode()
                    handler = control.load if message.topic().endswith('grid.transformer-load.v1') else control.result
                    handler(event, key, now)
                    consumer.commit(message=message, asynchronous=False)
                control.tick(now)
            break  # a lost incarnation exits; restart with a fresh identity
    finally:
        # Fence first; never keep serving while closing group membership.
        try:
            authority.stop()
        finally:
            if consumer is not None:
                consumer.close()


if __name__ == '__main__':
    main()
