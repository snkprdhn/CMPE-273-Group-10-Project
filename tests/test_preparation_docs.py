"""Dependency-free preparation checks; no services, installs, or network access."""

from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from urllib.parse import unquote, urlsplit


ROOT = Path(__file__).resolve().parents[1]


def missing_links(root):
    missing = []
    for document in root.rglob("*.md"):
        if any(part.startswith(".") for part in document.relative_to(root).parts):
            continue
        for target in re.findall(r"\[[^\]]*\]\(([^\s)]+)\)", document.read_text()):
            parsed = urlsplit(target.strip("<>"))
            if parsed.scheme or not parsed.path:
                continue
            if not (document.parent / unquote(parsed.path)).exists():
                missing.append((str(document.relative_to(root)), target))
    return missing


class PreparationDocumentationTests(unittest.TestCase):
    def test_repository_relative_links_exist(self):
        self.assertEqual(missing_links(ROOT), [])

    def test_missing_links_are_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "README.md").write_text(
                "[missing](absent.md) [external](https://example.com) [anchor](#here)"
            )
            self.assertEqual(missing_links(root), [("README.md", "absent.md")])

    def test_existing_relative_link_is_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "docs").mkdir()
            (root / "README.md").write_text("[guide](docs/guide.md)")
            (root / "docs/guide.md").write_text("[home](../README.md)")
            self.assertEqual(missing_links(root), [])

    def test_manual_shell_instructions_parse_without_execution(self):
        guide = ROOT / "docs/implementation/LOCAL_PREREQUISITES.md"
        blocks = re.findall(r"```sh\n(.*?)```", guide.read_text(), re.DOTALL)
        self.assertTrue(blocks, "No installation/verification instructions found")
        for number, block in enumerate(blocks, 1):
            with self.subTest(block=number):
                result = subprocess.run(
                    ["/bin/zsh", "-n"], input=block, text=True, capture_output=True
                )
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_shell_syntax_check_rejects_broken_instructions(self):
        result = subprocess.run(
            ["/bin/zsh", "-n"], input="echo 'unclosed", text=True, capture_output=True
        )
        self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
