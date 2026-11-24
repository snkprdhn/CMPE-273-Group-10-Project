"""Run with the existing Member_4 isolated lock/environment."""
import os
from pathlib import Path
import uvicorn
from identity.auth import OIDC
from copilot.tools import Tools
from copilot.service import Service
from copilot.api import create_app

from observability import setup
setup('member4.copilot')
path=Path(os.getenv('COPILOT_AUDIT_PATH','.tools/copilot/audit.sqlite'))
path.parent.mkdir(parents=True,exist_ok=True)
identity=OIDC()
service=Service(Tools(os.getenv('COPILOT_PROJECTION_URL','http://127.0.0.1:18084'),identity,
                      os.getenv('COPILOT_PROMETHEUS_URL'),os.getenv('COPILOT_TEMPO_URL')),str(path),
                retention_days=int(os.getenv('COPILOT_AUDIT_RETENTION_DAYS', '1')))
uvicorn.run(create_app(service,identity),host='0.0.0.0' if os.getenv('GRID_LOCAL_KIND')=='1' else '127.0.0.1',port=18087,access_log=False)
