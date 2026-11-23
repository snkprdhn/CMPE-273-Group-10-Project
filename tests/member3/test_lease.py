"""Monotonic renewal deadline checks across delayed Kubernetes responses."""
from copy import deepcopy
import io
import json
from urllib.error import HTTPError

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


def test_in_cluster_lease_uses_ca_and_rotated_token_and_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setenv('KUBERNETES_SERVICE_HOST', '10.96.0.1')
    monkeypatch.setenv('KUBERNETES_SERVICE_PORT', '443')
    lease = Lease(namespace='platform')
    lease.service_account = tmp_path
    token = tmp_path / 'token'
    token.write_text('first-local-token')
    ssl_context = object()
    seen = []

    def create_context(*, cafile):
        assert cafile == tmp_path / 'ca.crt'
        return ssl_context

    def request(req, *, context, timeout):
        assert req.full_url == 'https://10.96.0.1:443/apis/coordination.k8s.io/v1/namespaces/platform/leases/action'
        assert context is ssl_context
        assert timeout == 3
        seen.append((req.method, req.get_header('Authorization'), req.data))
        if req.method == 'GET':
            return io.BytesIO(b'{"metadata":{"resourceVersion":"42"}}')
        raise HTTPError(req.full_url, 409, 'conflict', None, None)

    monkeypatch.setattr('member3.leadership.ssl.create_default_context', create_context)
    monkeypatch.setattr('member3.leadership.urlopen', request)
    record = lease.request('get')
    token.write_text('rotated-local-token')
    with pytest.raises(RuntimeError, match='HTTP 409'):
        lease.request('replace', record)
    assert seen == [('GET', 'Bearer first-local-token', None),
                    ('PUT', 'Bearer rotated-local-token', json.dumps(record).encode())]
