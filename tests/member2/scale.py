"""Member_1 synthetic fleet and Member_2 live Kafka/PyFlink scale acceptance."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
from statistics import median
import subprocess
from threading import Thread
import time
from uuid import uuid4

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from member2.aggregation import LOAD, DLQ, LATE, stamp, validate
from tests.member2.live import api, wait

DEVICES, TRANSFORMERS, ROUNDS, PARTITIONS = 2500, 25, 40, 3
TOTAL = DEVICES * ROUNDS


def fleet():
    kinds = ['SMART_METER'] * 55 + ['EV_CHARGER'] * 35 + ['SOLAR'] * 10
    return {'topology_version': 'scale-v1', 'transformers': [
        {'transformer_id': f'tx-{i:03}', 'capacity_kw': 400, 'devices': [
            {'device_id': f'device-{i:03}-{j:03}', 'device_type': kind}
            for j, kind in enumerate(kinds)]} for i in range(TRANSFORMERS)]}


def counters(job, subtask=0):
    vertex = next(v for v in api('jobs/' + job)['vertices'] if v['name'].startswith('Member_2'))
    path = f"jobs/{job}/vertices/{vertex['id']}/subtasks/{subtask}/metrics"
    required = ('input', 'duplicates', 'very_late', 'dlq', 'retained_event_ids')
    names = [m['id'] for m in api(path) if m['id'].rsplit('.', 1)[-1] in required]
    if not names:
        return {}
    return {m['id'].rsplit('.', 1)[-1]: int(m['value']) for m in api(path + '?get=' + ','.join(names))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run')
    parser.add_argument('--rate', type=int, default=2500)
    parser.add_argument('--rounds', type=int, default=40)
    parser.add_argument('--timeout', type=int, default=7200)
    parser.add_argument('--bootstrap', default='kafka:9092')
    parser.add_argument('--replication', type=int, choices=(1, 3), default=1)
    parser.add_argument('--allow-recovery', action='store_true')
    parser.add_argument('--parallelism', type=int, default=1)
    args = parser.parse_args()
    if not 1 <= args.parallelism <= 32:
        parser.error('parallelism must be 1..32')
    if not 4 <= args.rounds <= 800 or not 60 <= args.timeout <= 14400:
        parser.error('rounds must be 4..800 and timeout 60..14400')
    rounds, total = args.rounds, DEVICES * args.rounds
    if not re.fullmatch(r'[a-zA-Z0-9_-]{1,48}', args.run) or not 100 <= args.rate <= 10000:
        parser.error('invalid run ID or rate outside 100–10000')
    run = args.run
    topology = fleet()
    validate('Topology', topology)
    directory = Path('/checkpoints') / run
    directory.mkdir()
    fixture = directory / 'topology.json'
    fixture.write_text(json.dumps(topology))
    prefix = f'm2-{run}.'
    input_topic = prefix + 'grid.telemetry.v1'
    config = {'bootstrap.servers': args.bootstrap}
    admin = AdminClient(config)
    for future in admin.create_topics([
        NewTopic(prefix + topic, PARTITIONS, args.replication, config={'min.insync.replicas': '2' if args.replication == 3 else '1'}) for topic in ('grid.telemetry.v1', LOAD, DLQ, LATE)
    ]).values():
        future.result(30)
    subprocess.run(['flink', 'run', '-d', '-py', '/app/member2/job.py', '--run', run,
                    '--topology', str(fixture), '--checkpoint', directory.as_uri(),
                    '--checkpoint-ms', '10000', '--bootstrap', args.bootstrap,
                    '--max-events', str(total), '--state-backend', 'rocksdb',
                    *(['--demo-transformer-parallelism', str(args.parallelism)] if args.parallelism > 1 else [])], check=True, timeout=120)
    job = wait(lambda: next((j['jid'] for j in api('jobs/overview')['jobs']
                             if j['name'] == f'member2-{run}' and j['state'] == 'RUNNING'), None), 'scale job')
    consumer = Consumer({**config, 'group.id': f'm2-{run}-aggregation-v1',
                         'enable.auto.commit': False, 'isolation.level': 'read_committed'})
    partitions = [TopicPartition(input_topic, i) for i in range(PARTITIONS)]
    producer = Producer({**config, 'partitioner': 'murmur2_random', 'acks': 'all',
                         'enable.idempotence': True, 'compression.type': 'lz4'})
    partition_counts, key_partitions, failures = Counter(), defaultdict(set), []
    expected = {}
    base = (datetime.now(timezone.utc) - timedelta(minutes=10)).replace(second=0, microsecond=0)

    def delivered(error, record):
        if error:
            failures.append(str(error))
        else:
            partition_counts[record.partition()] += 1
            key_partitions[record.key().decode()].add(record.partition())

    def publish():
        sent = 0
        first = None
        for cycle in range(rounds):
            start = base + timedelta(seconds=5 * cycle + (10 if cycle == rounds - 1 else 0))
            for tx in topology['transformers']:
                key = tx['transformer_id']
                net = 0
                for j, device in enumerate(tx['devices']):
                    kind = device['device_type']
                    # Interleave device clocks within the window to exercise out-of-order input.
                    at = start + timedelta(seconds=4 - j % 5)
                    power = {'SMART_METER': 2, 'EV_CHARGER': (7 if cycle % 4 < 2 else 3), 'SOLAR': -2}[kind]
                    power += (j % 3) * .25
                    event = dict(event_id=str(uuid4()), event_type='telemetry.reading', schema_version=1,
                                 source='scale-simulator', event_time=stamp(at), ingested_at=stamp(start + timedelta(seconds=4)),
                                 correlation_id=run, causation_id=None, payload=dict(
                                     **device, transformer_id=key, boot_id='scale-boot', quality='GOOD',
                                     sequence_number=cycle, power_kw=power))
                    if first is None:
                        first = event
                    if sent == total - 2:
                        event = first
                    if sent == total - 1:
                        event['ingested_at'] = stamp(start + timedelta(seconds=36))
                    producer.produce(input_topic, key=event['payload']['transformer_id'], value=json.dumps(event), on_delivery=delivered)
                    sent += 1
                    net += power
                    if sent % 100 == 0:
                        delay = begin + sent / args.rate - time.monotonic()
                        if delay > 0:
                            time.sleep(delay)
                        producer.poll(0)
                if cycle < rounds - 1:
                    expected[(tx['transformer_id'], stamp(start))] = net
        if producer.flush(30):
            failures.append('producer flush timed out')
        publish_seconds.append(time.monotonic() - begin)

    def sample():
        high = [consumer.get_watermark_offsets(p, timeout=5)[1] for p in partitions]
        committed = [max(0, p.offset) for p in consumer.committed(partitions, timeout=5)]
        cp = api(f'jobs/{job}/checkpoints')
        latest = cp['latest'].get('completed')
        managers = api('taskmanagers')['taskmanagers']
        memory = {}
        for manager in managers:
            url = f"taskmanagers/{manager['id']}/metrics"
            names = ['Status.JVM.Memory.Heap.Used', 'Status.JVM.Memory.NonHeap.Used']
            memory.update({row['id']: int(row['value']) for row in api(url + '?get=' + ','.join(names))})
        return dict(seconds=time.monotonic()-begin, high=high, committed=committed,
                    lag=[h-c for h,c in zip(high, committed)], checkpoints=cp['counts'],
                    latest=latest, restored=cp['latest'].get('restored'), history=cp['history'], jvm_memory_bytes=memory)

    samples, publish_seconds = [], []
    begin = time.monotonic()
    thread = Thread(target=publish, daemon=True)
    thread.start()
    with (directory / 'samples.jsonl').open('w') as evidence:
        while time.monotonic() - begin < args.timeout:
            try:
                row = sample()
            except Exception as exc:
                print(json.dumps({'sample_error': type(exc).__name__, 'seconds': time.monotonic()-begin}), flush=True)
                time.sleep(3)
                continue
            samples.append(row)
            (directory / 'progress.json').write_text(json.dumps(dict(job=job, **row)))
            print(json.dumps(row), file=evidence, flush=True)
            print(json.dumps({k: row[k] for k in ('seconds', 'committed', 'lag')}), flush=True)
            if not thread.is_alive():
                if failures or not publish_seconds or sum(partition_counts.values()) != total:
                    raise RuntimeError(f'Producer did not deliver all records: {failures[:3]}')
                if sum(row['committed']) == total:
                    break
            time.sleep(3)
        else:
            raise RuntimeError('Flink did not checkpoint all input offsets')
    consumer.close()
    if args.parallelism == 1:
        processed = wait(lambda: values if (values := counters(job)).get('retained_event_ids') == total - 1 else None,
                         'final operator counters', timeout=30)
    else:
        # The retained-ID gauge reports the current key, so it cannot be summed.
        processed = None

        def complete_counters():
            values = Counter()
            for subtask in range(args.parallelism):
                values.update(counters(job, subtask))
            values.pop('retained_event_ids', None)
            return dict(values) if values['input'] == total else None

        if not args.allow_recovery:
            processed = wait(complete_counters, 'final parallel counters', timeout=30)
    if processed is not None:
        assert processed['duplicates'] == 1 and processed['very_late'] == 1 and processed['dlq'] == 0, processed
        if not args.allow_recovery:
            assert processed['input'] == total, processed
    assert not failures and sum(partition_counts.values()) == total, failures
    assert len(partition_counts) == PARTITIONS
    assert len(key_partitions) == TRANSFORMERS and all(len(p) == 1 for p in key_partitions.values())

    output = Consumer({**config, 'group.id': 'verify-' + run, 'enable.auto.commit': False,
                       'isolation.level': 'read_committed', 'enable.partition.eof': True})
    assignment = [TopicPartition(prefix + name, i, 0) for name in (LOAD, DLQ, LATE) for i in range(PARTITIONS)]
    output.assign(assignment)
    latest, ids, eof = {}, set(), set()
    audit = Counter()
    deadline = time.monotonic() + 600
    while len(eof) < len(assignment) and time.monotonic() < deadline:
        msg = output.poll(1)
        if msg is None:
            continue
        if msg.error():
            assert msg.error().code() == KafkaError._PARTITION_EOF, msg.error()
            eof.add((msg.topic(), msg.partition()))
            continue
        event = json.loads(msg.value())
        assert event['event_id'] not in ids
        ids.add(event['event_id'])
        topic = msg.topic().removeprefix(prefix)
        validate({LOAD: 'Load', DLQ: 'Dlq', LATE: 'Late'}[topic], event)
        if topic != LOAD:
            audit[topic] += 1
            continue
        p = event['payload']
        key = p['transformer_id'], p['window_start']
        assert msg.key().decode() == key[0]
        if key not in latest or p['revision'] > latest[key]['revision']:
            latest[key] = p
    output.close()
    assert len(eof) == len(assignment) and audit == {LATE: 1}, audit
    assert latest.keys() == expected.keys(), (len(latest), len(expected))
    for key, net in expected.items():
        assert latest[key]['net_load_kw'] == net and latest[key]['good_device_count'] == 100, (key, latest[key])
    checkpoints = {cp['id']: cp for row in samples for cp in row['history'] if cp['status'] == 'COMPLETED'}
    durations = [cp['end_to_end_duration'] for cp in checkpoints.values()]
    report = dict(job=job, state_backend='rocksdb', max_events=total, rounds=rounds, replication=args.replication, records=total, devices=DEVICES, transformers=TRANSFORMERS, delivered=dict(partition_counts),
                  partition_keys={key: next(iter(value)) for key,value in key_partitions.items()},
                  publish_seconds=publish_seconds[0], delivered_eps=total/publish_seconds[0],
                  partition_delivered_eps={p: count/publish_seconds[0] for p, count in partition_counts.items()},
                  checkpointed_seconds=samples[-1]['seconds'], checkpointed_eps=total/samples[-1]['seconds'],
                  max_sampled_lag=max(sum(row['lag']) for row in samples), final_lag=samples[-1]['lag'],
                  checkpoints=samples[-1]['checkpoints'], checkpoint_ms=dict(median=median(durations), max=max(durations)),
                  max_checkpoint_bytes=max(cp['state_size'] for cp in checkpoints.values()),
                  jvm_peak_bytes=max(sum(row['jvm_memory_bytes'].values()) for row in samples),
                  verified_windows=len(latest), unique_outputs=len(ids), audit=dict(audit), operator=processed)
    (directory / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report), flush=True)


if __name__ == '__main__':
    main()
