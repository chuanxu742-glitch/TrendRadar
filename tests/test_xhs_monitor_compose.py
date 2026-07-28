from __future__ import annotations

import unittest
from pathlib import Path

import yaml


class XiaohongshuMonitorComposeTests(unittest.TestCase):
    def test_xhs_collector_is_isolated_from_main_service(self) -> None:
        compose_path = Path(__file__).resolve().parents[1] / "docker" / "docker-compose.yml"
        compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
        services = compose["services"]

        self.assertIn("xhs-monitor", services)
        xhs_environment = services["xhs-monitor"]["environment"]
        main_environment = services["trendradar"]["environment"]
        official_environment = services["official-monitor"]["environment"]

        self.assertFalse(any(item.startswith("XHS_COOKIE=") for item in xhs_environment))
        self.assertIn("xhs-login", services)
        self.assertEqual(
            services["xhs-login"]["entrypoint"],
            ["python", "-m", "xhs_monitor.login"],
        )
        self.assertIn("../config:/app/config", services["xhs-login"]["volumes"])
        self.assertIn("XHS_ENABLED=false", main_environment)
        self.assertFalse(any(item.startswith("XHS_COOKIE=") for item in main_environment))
        self.assertIn(
            "MONITOR_XHS_SUMMARY_URL=http://xhs-monitor:8091/api/v1/summary",
            official_environment,
        )
        official_volumes = services["official-monitor"]["volumes"]
        self.assertFalse(any("output/news" in item for item in official_volumes))


if __name__ == "__main__":
    unittest.main()
