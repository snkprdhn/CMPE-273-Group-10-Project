"""Operational endpoints and CLI fixture behavior."""
import json
import subprocess
import sys
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest

from devices.common import METRICS, monitor


def test_health_liveness_readiness_and_metrics():
    status = [False]
    server = monitor(0, lambda: status[0])
    url = f'http://127.0.0.1:{server.server_port}'
    try:
        assert json.load(urlopen(url + '/healthz')) == {'healthy': True}
        with pytest.raises(HTTPError) as exc:
            urlopen(url + '/readyz')
        assert exc.value.code == 503
        status[0] = True
        assert json.load(urlopen(url + '/readyz')) == {'healthy': True}
        METRICS['test_total'] = 3
        assert b'devices_test_total 3' in urlopen(url + '/metrics').read()
        with pytest.raises(HTTPError) as exc:
            urlopen(url + '/unknown')
        assert exc.value.code == 404
    finally:
        METRICS.pop('test_total', None)
        server.shutdown()
        server.server_close()


def test_scenario_cli_is_byte_repeatable(tmp_path):
    command = [sys.executable, '-m', 'devices', 'scenario', '--seed', '273', '--steps', '3',
               '--topology', str(tmp_path / 'topology.json')]
    first = subprocess.check_output(command)
    assert first == subprocess.check_output(command)
    assert len(first.splitlines()) == 9
    assert json.loads((tmp_path / 'topology.json').read_text())['transformers'][0]['capacity_kw'] == 20
