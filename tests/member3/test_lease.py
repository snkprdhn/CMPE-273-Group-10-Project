"""Monotonic renewal deadline checks across delayed Kubernetes responses."""
from copy import deepcopy

import pytest

from member3.leadership import Lease


@pytest.mark.parametrize('delay_at', ['get', 'replace'])
def test_renewal_cannot_resurrect_expired_incarnation(monkeypatch, delay_at):
    clock = [9.0]
    monkeypatch.setattr('member3.leadership.time.monotonic', lambda: clock[0])
    lease = Lease()
    lease.deadline = 10
    record = dict(metadata=dict(uid='uid', resourceVersion='1'), spec=dict(
        holderIdentity=lease.holder, renewTime='2026-11-22T00:00:00Z',
        leaseDurationSeconds=15, leaseTransitions=1))
    def request(verb, body=None):
        if verb == delay_at:
            clock[0] = 10.5
        return deepcopy(body or record)
    monkeypatch.setattr(lease, 'request', request)
    assert not lease.step()
    assert lease.deadline == 0
