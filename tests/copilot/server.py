"""Explicit loopback-only UI acceptance harness, never a production auth mode."""
from pathlib import Path
from identity.auth import Denied
from copilot.tools import Tools
from copilot.service import Service
from copilot.api import create_app

class Identity:
    def authenticate(self,token):
        if token != 'fixture-operator-token':
            raise Denied(401,'Fixture identity required')
        return dict(actor='fixture-operator',role='Operator')

path=Path('.tools/dashboard-evidence/investigator.sqlite')
path.unlink(missing_ok=True)
identity=Identity()
app=create_app(Service(Tools('http://127.0.0.1:18185',identity),str(path)),identity)
