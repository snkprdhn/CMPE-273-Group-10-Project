"""End-to-end 100k telemetry producer for the live AKS pipeline (multi-transformer).

Self-contained (only needs confluent_kafka) so it can run inside the Flink
JobManager pod, where in-cluster Kafka broker DNS resolves. An external
port-forward does NOT work for produce/consume (Strimzi advertises internal DNS).

Emits schema-valid telemetry.reading events for a 25-transformer topology
(tx-000..tx-024, each with ev-/meter-/solar-) to the EXISTING production topic
integration-kind.grid.telemetry.v1. The 25 keys spread across all 3 Kafka
partitions, so all 3 Flink subtasks drain in parallel. Events flow through the
real Flink -> Action -> Debezium -> projection -> dashboard pipeline. EV draw
cycles normal -> overload -> recovery so Action classifies CRITICAL and (for the
registered tx-001) dispatches throttle/restore.

event_id is uuid5(NAMESPACE_URL, <content-addressed string>), matching
member1.common.event(), so every event is a valid UUID and globally unique via
a fresh boot_id per run.

In-cluster usage:
    kubectl cp tools/e2e_benchmark.py <jm-pod>:/tmp/e2e_benchmark.py
    kubectl exec <jm-pod> -- /opt/venv/bin/python /tmp/e2e_benchmark.py \
        --bootstrap grid-kafka-bootstrap:9092 --target 100000
"""
import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from uuid import NAMESPACE_URL, uuid4, uuid5

from confluent_kafka import Producer

TOPIC = 'integration-kind.grid.telemetry.v1'
TRANSFORMERS = 25
# Per-transformer device set mirrors the member1-v1 shape (one of each type).
DEVICE_KINDS = [('ev', 'EV_CHARGER'), ('meter', 'SMART_METER'), ('solar', 'SOLAR')]


def stamp(value):
    return value.isoformat().replace('+00:00', 'Z')


def gen_events(round_idx, start, boot_id, run_id):
    """Yield one 5-second window's worth of readings for all 25 transformers.

    EV 7.2 kW on overload rounds (crosses the 20 kW cap), 4 kW otherwise.
    Phase cycles every 3 rounds: normal, overload, recovery.
    """
    import random
    rng = random.Random(round_idx)
    phase = round_idx % 3  # 0 normal, 1 overload, 2 recovery
    for t in range(TRANSFORMERS):
        tx = f'tx-{t:03d}'
        for prefix, kind in DEVICE_KINDS:
            device_id = f'{prefix}-{t:03d}'
            kw = {'SMART_METER': (8, 18, 8)[phase], 'SOLAR': -2,
                  'EV_CHARGER': 7.2 if phase == 1 else 4}[kind]
            if kind != 'EV_CHARGER':
                kw = round(kw + rng.uniform(-0.1, 0.1), 6)
            payload = dict(device_id=device_id, transformer_id=tx, device_type=kind,
                           boot_id=boot_id, power_kw=kw, quality='GOOD',
                           sequence_number=round_idx)
            if kind == 'EV_CHARGER':
                payload.update(current_rate_kw=kw, minimum_rate_kw=1, priority=1)
            src = f'{run_id}/{round_idx}/{stamp(start)}/{boot_id}/{tx}/{device_id}'
            eid = str(uuid5(NAMESPACE_URL, src))
            yield tx, dict(event_id=eid, event_type='telemetry.reading', schema_version=1,
                           source='e2e-benchmark', event_time=stamp(start),
                           ingested_at=stamp(start), correlation_id=run_id,
                           causation_id=None, payload=payload)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bootstrap', default='grid-kafka-bootstrap:9092')
    parser.add_argument('--topic', default=TOPIC)
    parser.add_argument('--target', type=int, default=100000)
    parser.add_argument('--rate', type=int, default=4000)
    args = parser.parse_args()

    producer = Producer({
        'bootstrap.servers': args.bootstrap,
        'acks': 'all',
        'enable.idempotence': True,
        'compression.type': 'lz4',
        'linger.ms': 20,
        'batch.num.messages': 10000,
        'partitioner': 'murmur2_random',
    })

    events_per_round = TRANSFORMERS * len(DEVICE_KINDS)  # 75
    rounds = max(3, (args.target + events_per_round - 1) // events_per_round)
    boot = f'e2e-{uuid4()}'
    # Advance event-time 5s per round so each round is its own tumbling window.
    base = (datetime.now(timezone.utc) - timedelta(hours=2)).replace(microsecond=0)

    sent = 0
    failures = []

    def on_delivery(err, msg):
        if err is not None:
            failures.append(str(err))

    print(json.dumps({'phase': 'start', 'target': args.target, 'rounds': rounds,
                      'events_per_round': events_per_round, 'transformers': TRANSFORMERS,
                      'topic': args.topic, 'boot_id': boot}), flush=True)
    begin = time.monotonic()
    for r in range(rounds):
        start = base + timedelta(seconds=5 * r)
        run_id = 'e2e-multi'
        for tx, ev in gen_events(r, start, boot, run_id):
            producer.produce(args.topic, key=tx,
                             value=json.dumps(ev, separators=(',', ':')),
                             on_delivery=on_delivery)
            sent += 1
            if sent % 500 == 0:
                producer.poll(0)
                expected = begin + sent / args.rate
                delay = expected - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
            if sent % 10000 == 0:
                elapsed = time.monotonic() - begin
                print(json.dumps({'phase': 'progress', 'sent': sent,
                                  'elapsed_s': round(elapsed, 1),
                                  'eps': round(sent / elapsed, 1),
                                  'failures': len(failures)}), flush=True)
        if sent >= args.target:
            break

    remaining = producer.flush(60)
    elapsed = time.monotonic() - begin
    print(json.dumps({'phase': 'done', 'sent': sent, 'elapsed_s': round(elapsed, 2),
                      'eps': round(sent / elapsed, 1), 'unflushed': remaining,
                      'rounds_emitted': r + 1,
                      'failures': failures[:5], 'failure_count': len(failures)}), flush=True)
    if failures:
        raise SystemExit(f'{len(failures)} delivery failures')
    if remaining:
        raise SystemExit(f'{remaining} messages not flushed')


if __name__ == '__main__':
    main()
