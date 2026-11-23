import os
from pathlib import Path
import subprocess
import sys
import time

import httpx
import pytest

API = 'http://127.0.0.1:18185'
UI = 'http://127.0.0.1:18186'


@pytest.fixture(scope='session')
def servers():
    processes = []
    logs = []
    Path('.tools/dashboard-evidence').mkdir(exist_ok=True)
    try:
        for name, command, url in [
            ('fixture', [sys.executable, '-m', 'uvicorn', 'dashboard.fixture_api:create_fixture_app', '--factory', '--host', '127.0.0.1', '--port', '18185'], API+'/api/v1/health'),
            ('ui', [sys.executable, '-m', 'dashboard', '--fixture', '--port', '18186', '--api-url', API+'/api/v1'], UI),
        ]:
            log = open(f'.tools/dashboard-evidence/{name}.log', 'w+')
            logs.append(log)
            proc = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env={**{k: v for k, v in os.environ.items() if k != 'PYTEST_CURRENT_TEST'}, 'COPILOT_URL':'http://127.0.0.1:18187'})
            processes.append(proc)
            for _ in range(100):
                if proc.poll() is not None:
                    log.seek(0)
                    pytest.fail(log.read())
                try:
                    if httpx.get(url, timeout=.3, trust_env=False).status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                time.sleep(.1)
            else:
                pytest.fail(f'{name} did not start')
        yield
    finally:
        for proc in reversed(processes):
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        for log in logs:
            log.close()


@pytest.fixture
def fixture_api(servers):
    with httpx.Client(base_url=API, trust_env=False) as api:
        api.post('/fixture', json={'step': 0, 'fault': '', 'clear_history': True}).raise_for_status()
        yield api
        api.post('/fixture', json={'step': 0, 'fault': '', 'clear_history': True}).raise_for_status()


@pytest.fixture
def copilot_server(servers):
    log=open('.tools/dashboard-evidence/copilot.log','w+')
    process=subprocess.Popen([sys.executable,'-m','uvicorn','tests.copilot.server:app',
        '--host','127.0.0.1','--port','18187'],stdout=log,stderr=subprocess.STDOUT)
    try:
        for _ in range(100):
            if process.poll() is not None:
                log.seek(0)
                pytest.fail(log.read())
            try:
                if httpx.get('http://127.0.0.1:18187/healthz',timeout=.3,trust_env=False).status_code==200:
                    break
            except httpx.HTTPError:
                pass
            time.sleep(.1)
        else:
            pytest.fail('Investigator did not start')
        yield
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill();process.wait()
        log.close()
