"""Fast checks for the local deployment wiring and its restrictive defaults."""
import json

import pytest

from tools.kind_local import calico_manifest, connector, runtime_secrets


def test_connector_uses_secret_provider_and_exact_outbox_contract():
    spec = connector()['spec']
    assert spec['class'] == 'io.debezium.connector.postgresql.PostgresConnector'
    config = spec['config']
    assert config['database.password'] == '${env:CDC_PASSWORD}'
    assert config['table.include.list'] == 'integration_kind.action_outbox'
    assert config['publication.autocreate.mode'] == 'disabled'
    assert config['transforms.outbox.route.topic.replacement'] == 'integration-kind.${routedByValue}'


def test_runtime_identities_are_split_and_realm_has_only_local_client():
    passwords = {name: name + '-synthetic' for name in
                 ('local_admin', 'registry', 'control', 'projection', 'keycloak', 'member3_cdc',
                  'member1_gateway', 'rpc_api', 'rpc_action', 'rpc_simulator', 'cursor',
                  'viewer', 'operator', 'administrator')}
    values = runtime_secrets(passwords)
    assert set(values['action-runtime']) == {'CONTROL_DSN', 'REGISTRY_ACTION_TOKEN'}
    assert set(values['projection-runtime']) == {'PROJECTION_DSN'}
    assert set(values['keycloak-runtime']) == {'KC_DB_PASSWORD'}
    realm = json.loads(values['keycloak-realm']['realm.json'])
    assert realm['clients'][0]['redirectUris'] == ['http://127.0.0.1:18086/auth/callback']
    assert realm['clients'][0]['directAccessGrantsEnabled'] is False
    assert 'local_admin-synthetic' not in json.dumps({k: v for k, v in values.items()
                                                       if k not in ('database-bootstrap', 'postgres-admin')})


def test_calico_edit_fails_if_upstream_shape_changes(tmp_path):
    path = tmp_path / 'calico.yaml'
    path.write_text('calico_backend: "bird"\n')
    with pytest.raises(RuntimeError, match='unique patch target'):
        calico_manifest(path)


@pytest.mark.parametrize(('profile', 'replicas', 'minimum_isr'), [('dev', 1, 1), ('review', 3, 2)])
def test_rendered_profiles_use_frozen_topics_and_internal_services(profile, replicas, minimum_isr):
    import subprocess

    from tools.kind_local import BIN, INFRA, ROOT

    yaml = pytest.importorskip('yaml')
    if not (BIN / 'kubectl').exists():
        pytest.skip('Install the pinned preparation kubectl before rendering kind profiles')
    output = subprocess.check_output([BIN / 'kubectl', 'kustomize', INFRA / profile], text=True)
    objects = list(yaml.safe_load_all(output))
    bundle = json.loads((ROOT / 'schemas/v1/bundle.json').read_text())
    assert all(obj['metadata']['labels']['grid-owner'] in ('Member_1', 'Member_2', 'Member_3', 'Member_4')
               for obj in objects)
    topics = {obj['spec']['topicName']: obj['spec'] for obj in objects if obj['kind'] == 'KafkaTopic'}
    expected = {'integration-kind.' + topic for topic in bundle['x-events']}
    assert set(topics) == {*expected, '__debezium-heartbeat.integration_kind'}
    for name, spec in topics.items():
        assert (spec['replicas'], spec['config']['min.insync.replicas']) == (replicas, minimum_isr)
        if name in expected:
            assert spec['partitions'] == 3
            assert spec['config']['retention.ms'] == 86400000
    pool = next(obj['spec'] for obj in objects if obj['kind'] == 'KafkaNodePool')
    assert pool['replicas'] == replicas and set(pool['roles']) == {'controller', 'broker'}
    services = [obj for obj in objects if obj['kind'] == 'Service']
    assert all(obj['spec'].get('type', 'ClusterIP') == 'ClusterIP' for obj in services)


@pytest.mark.parametrize('api_available', [True, False])
def test_kind_stop_terminates_workers_before_data_and_bounds_fallback(monkeypatch, api_available):
    import subprocess

    from tools.kind_local import Cluster

    commands = []
    cluster = Cluster('dev')
    def docker(*args, **kwargs):
        commands.append(args)
        return 'true\n'
    def kube(*args, **kwargs):
        commands.append(args)
        if not api_available:
            raise subprocess.CalledProcessError(1, args)
    monkeypatch.setattr('tools.kind_local.run', docker)
    monkeypatch.setattr(cluster, 'k', kube)
    cluster.stop()
    assert commands[1][1:3] == ('cordon', cluster.node)
    assert commands[-1] == ('docker', 'stop', '--timeout', '60', cluster.node)
    shutdowns = [command for command in commands if 'delete' in command]
    if api_available:
        assert len(shutdowns) == 2
        assert 'action' in shutdowns[0][4] and 'kafka,postgres' in shutdowns[1][4]
    else:
        assert not shutdowns


def test_connector_waits_for_task_assignment_and_rejects_failed_task(monkeypatch):
    from tools.kind_local import Cluster

    cluster = Cluster('dev')
    requested = []
    monkeypatch.setattr('tools.kind_local.time.sleep', lambda _: None)
    monkeypatch.setattr(cluster, 'ready', lambda resource, **_: requested.append(resource))
    for final, should_fail in [('RUNNING', False), ('FAILED', True)]:
        responses = iter([[], [{'state': final}]])
        monkeypatch.setattr(cluster, 'k', lambda *args, **kwargs: json.dumps(
            {'connector': {'state': 'RUNNING'}, 'tasks': next(responses)}))
        if should_fail:
            with pytest.raises(RuntimeError, match='Debezium failed'):
                cluster.ready_connector()
        else:
            cluster.ready_connector()
    assert requested == ['kafkaconnector/integration-kind'] * 2
