"""Member_3 boundary fixtures satisfy the frozen executable v1 semantics."""
import json
from pathlib import Path

from tools.contracts import validate


def test_member3_input_fixtures_against_frozen_contracts():
    cases = json.loads(Path('member3/fixtures/scenarios.json').read_text())
    for case in cases:
        for step in case['steps']:
            if step['op'] == 'load':
                validate('Load', step['event'], key=step['event']['payload']['transformer_id'])
            elif step['op'] == 'result':
                validate('Result', step['event'], key=step['event']['payload']['device_id'])
            elif step['op'] == 'snapshots':
                for snapshot in step['values']:
                    validate('Device', snapshot['device'])
