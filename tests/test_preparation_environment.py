"""Local prerequisite smoke checks; run with .venv/bin/python, no network needed."""

import importlib.metadata
from pathlib import Path
import platform
import subprocess
import sys
import unittest

from jsonschema import Draft202012Validator, ValidationError


class PreparationEnvironmentTests(unittest.TestCase):
    def test_isolated_selected_python(self):
        self.assertNotEqual(sys.prefix, sys.base_prefix, "Run with .venv/bin/python")
        self.assertEqual(sys.version_info[:3], (3, 12, 9))
        self.assertEqual(platform.machine(), "arm64")

    def test_pinned_python_tools_import(self):
        self.assertEqual(importlib.metadata.version("pytest"), "8.3.5")
        self.assertEqual(importlib.metadata.version("jsonschema"), "4.23.0")

    def test_selected_native_tools_execute(self):
        bin_dir = Path(sys.prefix) / "bin"
        for name, args, expected in [
            ("kind", ["version"], "kind v0.30.0"),
            ("kubectl", ["version", "--client"], "Client Version: v1.34.0"),
            ("helm", ["version", "--short"], "v3.19.0"),
        ]:
            with self.subTest(tool=name):
                result = subprocess.run(
                    [str(bin_dir / name), *args],
                    capture_output=True, text=True, timeout=10,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(expected, result.stdout)

    def test_schema_validator_accepts_valid_input(self):
        Draft202012Validator({"type": "integer", "minimum": 0}).validate(1)

    def test_schema_validator_rejects_invalid_input(self):
        with self.assertRaises(ValidationError):
            Draft202012Validator({"type": "integer", "minimum": 0}).validate(-1)


if __name__ == "__main__":
    unittest.main()
