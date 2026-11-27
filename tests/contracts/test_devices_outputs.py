"""Device outputs also pass the frozen executable semantic validator."""
import json
from pathlib import Path

from devices.common import instant
from devices.simulator import devices, EdgeState, scenario
from tools.contracts import validate


def test_seeded_devices_outputs_against_frozen_contracts(tmp_path):
    now = instant('2026-11-22T18:00:00Z')
    for value in scenario(273, now):
        validate('Telemetry', value, key='tx-001')
    validate('Topology', json.loads(Path('devices/fixtures/topology.json').read_text()))
    edge = EdgeState(tmp_path / 'edge.sqlite', devices(now)[0])
    try:
        for case in json.loads(Path('devices/fixtures/commands.json').read_text()):
            validate('Command', case['value'], key='ev-001')
            output = edge.apply(case['value'], 'ev-001', now)
            validate('Result', output, key='ev-001')
            assert output['payload']['outcome'] == case['expected']
        for _, raw, _ in edge.pending():
            value = json.loads(raw)
            validate('Result' if value['event_type'] == 'command.result' else 'Telemetry', value,
                     key='ev-001' if value['event_type'] == 'command.result' else 'tx-001')
    finally:
        edge.db.close()
