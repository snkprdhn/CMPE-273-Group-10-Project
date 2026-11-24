import pytest
from tools import load_preflight


def test_stopped_cluster_is_never_a_load_success(monkeypatch):
    def capture(command):
        value = 'exited sha256:abc' if command[1] == 'inspect' else ''
        return dict(command=command, exit_code=0, stdout=value, stderr='')
    monkeypatch.setattr(load_preflight, 'capture', capture)
    report = load_preflight.inventory('smart-grid-kind-dev-load')
    assert report['status'] == 'BLOCKED'
    assert report['load_executed'] is False
    assert report['measurements'] is None
    assert report['blockers']


def test_failed_inventory_is_not_ready(monkeypatch):
    monkeypatch.setattr(load_preflight, 'capture', lambda command: dict(exit_code=1, stdout='', stderr='denied'))
    assert load_preflight.inventory('smart-grid-kind-review')['status'] == 'BLOCKED'
    with pytest.raises(ValueError):
        load_preflight.inventory('unrelated-container')
