"""Verify the projected Action and acknowledgement in real Chromium."""
import argparse
import json
from pathlib import Path

from playwright.sync_api import expect, sync_playwright


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', required=True)
    parser.add_argument('--action', required=True)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport={'width': 1440, 'height': 1000})
            page.goto(args.url)
            page.get_by_role('button', name='Alerts & actions', exact=True).click(timeout=30000)
            action = page.get_by_role('button', name=args.action, exact=True)
            expect(action).to_be_visible(timeout=30000)
            action.click()
            detail = page.get_by_role('dialog')
            expect(detail.get_by_text('COMPLETED', exact=True)).to_be_visible()
            expect(detail.get_by_text('APPLIED', exact=True).first).to_be_visible()
            page.screenshot(path=args.output.with_suffix('.png'), full_page=True)
            args.output.write_text(json.dumps(dict(matched=True, action_id=args.action, status='COMPLETED')))
        finally:
            browser.close()


if __name__ == '__main__':
    main()
