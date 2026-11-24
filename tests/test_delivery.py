"""Check delivery gates without Docker, a running cluster, or credentials."""
import json

import pytest

from tools import delivery_backup as backup
from tools.delivery_checks import verify_manifests
from tools.kind_local import Cluster


def test_named_clusters_have_distinct_local_state():
    first = Cluster('dev', 'smart-grid-kind-dev-first')
    second = Cluster('dev', 'smart-grid-kind-dev-second')
    assert first.directory.name == 'kind-dev-first'
    assert first.kubeconfig != second.kubeconfig
    assert Cluster('dev').directory.name == 'kind-dev'
    with pytest.raises(ValueError, match='Cluster name'):
        Cluster('dev', 'smart-grid-kind-dev-../other')


def test_reset_requires_exact_named_cluster_before_docker_access(monkeypatch):
    from tools.kind_local import main

    monkeypatch.setattr('sys.argv', ['kind_local', 'reset', '--name', 'smart-grid-kind-dev-first',
                                    '--confirm', 'smart-grid-kind-dev-second'])
    with pytest.raises(SystemExit, match='2'):
        main()


def test_restore_checks_all_archive_hashes_before_connecting(tmp_path):
    archives = {}
    for name in backup.STORES:
        path = tmp_path / (name + '.dump')
        path.write_bytes(name.encode())
        archives[name] = backup.file_hash(path)
    path.write_bytes(b'changed')
    (tmp_path / 'backup.json').write_text(json.dumps({'sha256': archives}))
    with pytest.raises(ValueError, match='checksum mismatch'):
        backup.restore_drill(None, tmp_path)


def test_restore_comparison_retains_table_names_and_escaped_row_data(monkeypatch):
    base = b'COPY public.data (value) FROM stdin;\n'
    sequence = b"SELECT pg_catalog.setval('id', 5, true);\n"

    def digest(rows, header=base, end=b'\\.\n'):
        def dump(*args, stdout, **kwargs):
            stdout.write(b'\\restrict nonce\n' + header + rows + end + sequence)
        monkeypatch.setattr(backup, 'pg', dump)
        return backup.data_hash(None, 'pg_dump')

    expected = digest(b'one\\ntwo\nthree\n')
    assert expected['rows'] == 2
    assert expected == digest(b'three\none\\ntwo\n')
    assert expected != digest(b'one\\nTWO\nthree\n')
    assert expected != digest(b'one\\ntwo\nthree\n', base.replace(b'data', b'other'))
    with pytest.raises(ValueError, match='Unterminated COPY'):
        digest(b'one\n', end=b'')


def test_manifest_rejects_floating_images_and_duplicate_resources(tmp_path):
    pytest.importorskip('yaml')
    path = tmp_path / 'render.yaml'
    resource = ('apiVersion: v1\nkind: Pod\nmetadata: {name: probe}\n'
                'spec: {containers: [{name: probe, image: example/probe:v1}]}\n')
    path.write_text(resource)
    verify_manifests(path)
    path.write_text(resource.replace(':v1}', ':latest}'))
    with pytest.raises(ValueError, match='Floating image'):
        verify_manifests(path)
    path.write_text(resource.replace(':v1}', '@sha256:abc}'))
    with pytest.raises(ValueError, match='Floating image'):
        verify_manifests(path)
    path.write_text(resource + '---\n' + resource)
    with pytest.raises(ValueError, match='Duplicate resource'):
        verify_manifests(path)
