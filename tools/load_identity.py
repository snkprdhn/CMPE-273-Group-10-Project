"""Acquire a real local Viewer token through the existing PKCE acceptance client."""
import json
from pathlib import Path
import sys
import httpx
from playwright.sync_api import sync_playwright
from tools.kind_browser import token

if __name__ == '__main__':
    target = Path(sys.argv[1])
    password = json.loads(Path(sys.argv[2]).read_text())['viewer']
    with httpx.Client(trust_env=False, timeout=5) as http, sync_playwright() as playwright:
        with playwright.chromium.launch() as browser:
            bearer = token(browser, http, password)
    temporary=target.with_suffix('.next')
    with temporary.open('x') as file:
        temporary.chmod(0o600)
        file.write(bearer)
    temporary.replace(target)
