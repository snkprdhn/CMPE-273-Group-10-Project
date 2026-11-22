"""Standalone commands. See member1/README.md for the local runbook."""
import argparse
import json
import math
import os
from pathlib import Path
import signal
import threading
import time
from uuid import uuid4

from member1.common import instant, log, monitor, stamp, utc, validate
from member1.simulator import command_fixture, devices, EdgeState, scenario, topology


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['scenario', 'registry', 'simulate', 'command', 'command-fixture', 'register'])
    parser.add_argument('--case', choices=['throttle', 'restore', 'expired', 'stale'], default='throttle')
    parser.add_argument('--live', action='store_true', help='use current UTC for command-fixture')
    parser.add_argument('--seed', type=int, default=273)
    parser.add_argument('--steps', type=int, default=30)
    parser.add_argument('--interval', type=float, default=1)
    parser.add_argument('--start', default='2026-11-22T18:00:00Z')
    parser.add_argument('--run-id', default='member1-001')
    parser.add_argument('--bootstrap', default='127.0.0.1:19092')
    parser.add_argument('--prefix', default='local-dev.')
    parser.add_argument('--group', default='local-dev.member1.edge.v1')
    parser.add_argument('--state', default='data/member1-edge.sqlite')
    parser.add_argument('--registry', default='127.0.0.1:15051')
    parser.add_argument('--bind', default='127.0.0.1')
    parser.add_argument('--metrics-port', type=int)
    parser.add_argument('--fixture', help='command envelope JSON file')
    parser.add_argument('--topology', help='write topology JSON file')
    args = parser.parse_args()
    if args.steps < 3 or not math.isfinite(args.interval) or args.interval <= 0:
        parser.error('--steps must be >=3 and --interval positive and finite')
    validate('Id', args.run_id)
    validate('Timestamp', args.start)
    if args.mode == 'command-fixture':
        print(json.dumps(command_fixture(utc() if args.live else instant(args.start), args.run_id, args.case), sort_keys=True))
        return
    if args.mode == 'scenario':
        start = instant(args.start)
        if args.topology:
            Path(args.topology).write_text(json.dumps(topology(start), indent=2) + '\n')
        for value in scenario(args.seed, start, args.steps, args.run_id, interval=args.interval):
            print(json.dumps(value, sort_keys=True, allow_nan=False))
        return
    if args.mode in ('registry', 'register'):
        from member1.registry import Client, device_message, pb, Registry, serve
        if args.mode == 'register':
            client = Client(args.registry, 'simulator', os.environ['REGISTRY_SIMULATOR_TOKEN'])
            try:
                for d in devices(instant(args.start)):
                    result = client.call('RegisterDevice', pb.RegisterDeviceRequest(
                        operation_id=f"{args.run_id}-{d['device_id']}", device=device_message(d)))
                    print(result)
            finally:
                client.channel.close()
            return
        registry = Registry(os.environ['REGISTRY_DSN'], {
            'simulator': os.environ['REGISTRY_SIMULATOR_TOKEN'],
            'action': os.environ['REGISTRY_ACTION_TOKEN'], 'api': os.environ['REGISTRY_API_TOKEN']},
            os.environ['REGISTRY_CURSOR_KEY'])
        registry.migrate()
        server, _ = serve(registry, args.registry)
        health = monitor(args.metrics_port or 8001, registry.ready, args.bind)
        stopped = threading.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: stopped.set())
        log('REGISTRY_STARTED')
        stopped.wait()
        server.stop(2).wait()
        health.shutdown()
        return
    from member1.kafka import consumer, flush_results, handle_message, Publisher
    publisher = Publisher(args.bootstrap, args.prefix)
    if args.mode == 'command':
        if not args.fixture:
            parser.error('--fixture is required')
        value = json.loads(Path(args.fixture).read_text())
        publisher.send('grid.charger-command.v1', value['payload']['device_id'], value)
        publisher.drain()
        return
    Path(args.state).parent.mkdir(parents=True, exist_ok=True)
    state = EdgeState(args.state, devices(utc())[0])
    receiver = consumer(args.bootstrap, args.prefix, args.group)
    health = monitor(args.metrics_port or 8002, publisher.ready, args.bind)
    stopped = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stopped.set())
    try:
        flush_results(state, publisher)
        # Live scenarios rebase time and boot, preserving reproducible power/phase
        # choices. Offline `scenario` is the byte-repeatable fixture command.
        start, boot = utc(), str(uuid4())
        samples = iter(scenario(args.seed, start, args.steps, args.run_id,
                                boot_id=boot, interval=args.interval))
        next_tick = time.monotonic()
        finished = False
        while not stopped.is_set():
            if time.monotonic() >= next_tick and not finished:
                for _ in range(3):
                    try:
                        value = next(samples)
                    except StopIteration:
                        finished = True
                        break
                    if value['payload']['device_type'] == 'EV_CHARGER':
                        # Simulated offered demand follows the waveform; an applied
                        # command is a durable upper limit, retained in recovery.
                        rate = min(value['payload']['power_kw'], state.current()[0])
                        value = state.reading(utc(), args.run_id)
                        value['payload']['power_kw'] = rate
                        value['payload']['current_rate_kw'] = rate
                    else:
                        value['event_time'] = value['ingested_at'] = stamp(utc())
                    publisher.send('grid.telemetry.v1', 'tx-001', value)
                publisher.drain()
                next_tick += args.interval
            message = receiver.poll(.1)
            if message is not None:
                handle_message(message, state, publisher, receiver, utc())
            # Remain alive after the waveform, accepting fixture commands until stop.
    finally:
        try:
            publisher.drain()
        finally:
            receiver.close()
            state.db.close()
            health.shutdown()


if __name__ == '__main__':
    main()
