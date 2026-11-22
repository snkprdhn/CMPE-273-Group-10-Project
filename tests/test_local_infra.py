"""Safety/failure tests for the stdlib Compose entry point; real checks: make infra-verify."""

import contextlib
import io
import json
import runpy
import subprocess
import unittest
from unittest.mock import patch

from tools import local_infra as infra


class LocalInfraTests(unittest.TestCase):
    def test_compose_uses_no_host_binds_and_only_loopback_ports(self):
        config = json.loads(infra.compose("config", "--format", "json").stdout)
        for service in config["services"].values():
            for mount in service.get("volumes", []):
                self.assertEqual(mount["type"], "volume", "Host bind mounts trigger the Desktop watcher stall")
            for port in service.get("ports", []):
                self.assertEqual(port["host_ip"], "127.0.0.1")

    def test_reset_requires_exact_project_before_docker(self):
        for arguments in (["reset"], ["reset", "--confirm", "other-project"]):
            with self.subTest(arguments=arguments), patch.object(infra, "compose") as call:
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    infra.main(arguments)
                call.assert_not_called()

    def test_reset_has_fixed_scope_and_no_global_prune(self):
        with patch.object(infra, "compose") as call:
            infra.main(["reset", "--confirm", infra.PROJECT])
        call.assert_called_once_with("down", "--volumes", "--timeout", "30")
        self.assertEqual(infra.COMPOSE[infra.COMPOSE.index("--project-name") + 1],
                         "smart-grid-local-dev")
        self.assertNotIn("prune", infra.COMPOSE)

    def test_health_fails_when_missing_or_unhealthy(self):
        for output in ("[]", '[{"Service":"kafka","Health":"unhealthy"}]'):
            with self.subTest(output=output), patch.object(infra, "compose") as call:
                call.return_value.stdout = output
                with self.assertRaisesRegex(RuntimeError, "missing or unhealthy"):
                    infra.health()

    def test_health_accepts_compose_json_lines(self):
        with patch.object(infra, "compose") as call, patch.object(infra, "kafka"), \
                patch.object(infra, "sql") as sql:
            call.return_value.stdout = (
                '{"Service":"kafka","Health":"healthy"}\n'
                '{"Service":"postgres","Health":"healthy"}\n')
            sql.return_value.stdout = "1\n"
            infra.health()

    def test_up_rebuilds_configuration_images_before_health(self):
        with patch.object(infra, "compose") as call, \
                patch.object(infra, "create_topic"), patch.object(infra, "health") as health:
            infra.up()
        call.assert_called_once_with(
            "up", "--build", "-d", "--wait", "--wait-timeout", "150", "kafka", "postgres")
        health.assert_called_once_with()

    def test_consume_rejects_wrong_record(self):
        with patch.object(infra, "kafka") as call:
            call.return_value.stdout = "smoke-key\twrong\n"
            with self.assertRaisesRegex(RuntimeError, "mismatch"):
                infra.consume("local-dev.smoke.test", "expected")

    def test_subprocess_failure_is_propagated(self):
        with patch.object(infra.subprocess, "run", side_effect=subprocess.CalledProcessError(1, [])):
            with self.assertRaises(subprocess.CalledProcessError):
                infra.compose("ps")

    def test_fault_proxy_is_restored_on_failed_outage_assertion(self):
        with patch.object(infra, "health"), patch.object(infra, "proxy_request") as proxy, \
                patch.object(infra, "sql") as sql:
            sql.return_value.stdout = "1\n"
            sql.return_value.returncode = 0
            with self.assertRaisesRegex(RuntimeError, "Disabled proxy"):
                infra.fault_smoke()
            self.assertEqual(proxy.call_args.args, ("/proxies/postgres", {"enabled": True}))

    def test_timeout_bytes_report_cleanly_without_command_credentials(self):
        error = subprocess.TimeoutExpired(["credential-must-not-print"], 1, stderr=b"engine stalled")
        output = io.StringIO()
        with patch("sys.argv", ["local_infra.py", "health"]), \
                patch("subprocess.run", side_effect=error), \
                contextlib.redirect_stderr(output), self.assertRaises(SystemExit) as caught:
            runpy.run_path(str(infra.ROOT / "tools/local_infra.py"), run_name="__main__")
        self.assertEqual(caught.exception.code, 1)
        self.assertIn("engine stalled", output.getvalue())
        self.assertNotIn("credential-must-not-print", output.getvalue())

    def test_domain_topics_come_from_contract_bundle(self):
        self.assertEqual(len(infra.topics()), 8)
        self.assertIn("local-dev.grid.telemetry.v1", infra.topics())
        self.assertTrue(all(topic.startswith("local-dev.grid.") for topic in infra.topics()))


if __name__ == "__main__":
    unittest.main()
