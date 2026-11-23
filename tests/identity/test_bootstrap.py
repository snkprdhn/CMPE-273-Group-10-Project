"""Opt-in idempotent bootstrap check against the existing local identity database."""
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('IDENTITY_LIVE') != '1', reason='requires local identity database')


def test_bootstrap_preserves_credentials_and_unrelated_files():
    root = Path('.tools/identity')
    before = (root/'local.json').read_bytes()
    with tempfile.NamedTemporaryFile(dir=root, prefix='review-marker-') as marker:
        os.chmod(marker.name, 0o644)
        subprocess.run([sys.executable, '-m', 'identity.bootstrap'], check=True, capture_output=True)
        assert stat.S_IMODE(Path(marker.name).stat().st_mode) == 0o644
    assert (root/'local.json').read_bytes() == before
    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    for name in ('local.json', 'realm.json', 'keycloak.env', 'Dockerfile', '.dockerignore'):
        assert stat.S_IMODE((root/name).stat().st_mode) == 0o600
