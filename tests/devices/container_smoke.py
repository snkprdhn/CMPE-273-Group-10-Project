"""Test client for the running devices Compose slice; only local resources.

Run with the devices test venv after `make devices-up`. Leaves owned durable
state in place, and recreates only the two devices containers (no data deletion).
"""
import json
import subprocess
from urllib.request import urlopen
from uuid import uuid4

from devices.common import ROOT, utc, validate
from devices.kafka import Publisher
from devices.registry import Client, device_message, pb
from devices.simulator import command_fixture, devices
from tests.devices.test_kafka_live import poll, reader

COMPOSE = ['docker', 'compose', '-p', 'smart-grid-local-dev', '-f',
           str(ROOT / 'infra/compose/compose.yaml'), '-f', str(ROOT / 'infra/compose/devices.yaml')]


def main():
    # The imported test reader is intentionally a contract-boundary fixture client.
    import tests.devices.test_kafka_live as fixtures
    fixtures.BOOTSTRAP = '127.0.0.1:19092'
    run = 'container-' + uuid4().hex
    now = utc()
    client = Client('127.0.0.1:15051', 'simulator', 'simulator_disposable')
    results = reader('local-dev.grid.command-result.v1', 'local-dev.devices.container.' + run)
    telemetry = reader('local-dev.grid.telemetry.v1', 'local-dev.devices.container.telemetry.' + run)
    publisher = Publisher('127.0.0.1:19092')
    try:
        for d in devices(now):
            # Use unique registration IDs to preserve earlier authoritative fixtures.
            d['device_id'] = run + '-' + d['device_id']
            d['transformer_id'] = run + '-tx'
            response = client.call('RegisterDevice', pb.RegisterDeviceRequest(
                operation_id=d['device_id'], device=device_message(d)))
            assert response.device_id == d['device_id']
        heartbeat = pb.HeartbeatRequest(operation_id=run + '-heartbeat', device_id=run + '-ev-001', capability_version=1)
        heartbeat.observed_at.FromDatetime(utc())
        client.call('Heartbeat', heartbeat)
        query = pb.ListEligibleChargersRequest(transformer_id=run + '-tx', policy_version=1)
        query.evaluated_at.FromDatetime(utc())
        assert [d.device_id for d in client.call('ListEligibleChargers', query).devices] == [run + '-ev-001']
        value = command_fixture(utc(), run)
        publisher.send('grid.charger-command.v1', 'ev-001', value)
        publisher.drain()
        message = poll(results, lambda e: e['payload']['command_id'] == run + '-throttle')
        result = json.loads(message.value())
        validate('Result', result, 'ev-001')
        assert result['payload']['outcome'] == 'APPLIED'
        follow = json.loads(poll(telemetry, lambda e: e['causation_id'] == run + '-throttle').value())
        validate('Telemetry', follow, 'tx-001')
        assert follow['payload']['power_kw'] == 3
        subprocess.run(COMPOSE + ['up', '-d', '--force-recreate', '--no-deps', '--wait', 'registry', 'simulator'], check=True)
        assert client.call('GetDevice', pb.GetDeviceRequest(device_id=run + '-ev-001')).device_id == run + '-ev-001'
        # Replay after process/container recreation, with the same logical command.
        publisher.send('grid.charger-command.v1', 'ev-001', value)
        publisher.drain()
        message = poll(results, lambda e: e['payload']['command_id'] == run + '-throttle')
        assert json.loads(message.value())['payload']['outcome'] == 'ALREADY_APPLIED'
        stale = command_fixture(utc(), run, 'stale')
        publisher.send('grid.charger-command.v1', 'ev-001', stale)
        publisher.drain()
        message = poll(results, lambda e: e['payload']['command_id'] == run + '-stale')
        assert json.loads(message.value())['payload']['reason'] == 'STALE_EPOCH'
        for case, expected in [('expired', 'EXPIRED'), ('restore', 'APPLIED')]:
            fixture = command_fixture(utc(), run, case)
            publisher.send('grid.charger-command.v1', 'ev-001', fixture)
            publisher.drain()
            message = poll(results, lambda e: e['payload']['command_id'] == run + '-' + case)
            assert json.loads(message.value())['payload']['outcome'] == expected
        restored = json.loads(poll(telemetry, lambda e: e['causation_id'] == run + '-restore').value())
        assert restored['payload']['boot_id'] != follow['payload']['boot_id']
        assert restored['payload']['power_kw'] == 7.2
        for port in (8001, 8002):
            assert json.load(urlopen(f'http://127.0.0.1:{port}/readyz', timeout=2))['healthy']
            metrics = urlopen(f'http://127.0.0.1:{port}/metrics', timeout=2).read()
            assert b'devices_' in metrics
        print('PASS: container telemetry/results, persisted dedup/fence, Registry persistence and health')
    finally:
        client.channel.close()
        results.close()
        telemetry.close()


if __name__ == '__main__':
    main()
