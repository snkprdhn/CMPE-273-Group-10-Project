"""Versioned synthetic fixtures and the explicit fake Registry test boundary."""
from copy import deepcopy
import json
from pathlib import Path


class FakeRegistry:
    def __init__(self, snapshots=()):
        self.values = deepcopy(snapshots)
        self.unavailable = False

    def snapshots(self, transformer_id, evaluated_at, policy_version):
        if self.unavailable:
            raise ConnectionError('Registry unavailable')
        return deepcopy(self.values)


def scenarios():
    return json.loads(Path(__file__).with_name('fixtures').joinpath('scenarios.json').read_text())


def replay(control, case, restart=None):
    """Run every operation against real owned persistence, including restart."""
    from action.domain import instant
    from action.control import Control
    for step in case['steps']:
        op = step['op']
        if op == 'snapshots':
            control.registry.values = deepcopy(step['values'])
        elif op == 'load':
            control.load(step['event'], step['event']['payload']['transformer_id'], instant(step['now']))
        elif op == 'result':
            control.result(step['event'], step['event']['payload']['device_id'], instant(step['now']))
        elif op == 'tick':
            control.tick(instant(step['now']))
        elif op == 'restart':
            control = (restart or Control)(control.dsn, control.registry, control.epoch)
            control.migrate()
        else:
            raise ValueError('unknown fixture operation')
    return control.inspect()
