from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from monitor import healthcheck, official_monitor


class DashboardContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dashboard = (Path(__file__).parents[1] / "dashboard.html").read_text(encoding="utf-8")

    def test_dashboard_exposes_functional_health_metrics(self) -> None:
        for marker in (
            'id="healthStatus"',
            'id="functionalHealth"',
            'id="dueBacklog"',
            'id="baselineCoverage"',
            'id="comparableCoverage"',
            'id="capacityRatio"',
            'id="primaryFreshness"',
            'id="deferredSources"',
            'id="pendingSources"',
            'id="siteInventoryTotal"',
            'id="siteInventoryFetched"',
            'id="siteInventoryCoverage"',
            'id="siteInventoryClasses"',
            'id="siteInventorySamples"',
            'id="siteInventorySkipped"',
            '$("deferredSources").textContent=number(data.summary.deferred)',
            '$("siteInventoryCoverage").textContent=percent(u.fetch_coverage)',
            "renderHealth(data.health)",
        ):
            self.assertIn(marker, self.dashboard)

    def test_dashboard_uses_precise_operational_terms(self) -> None:
        for copy in (
            "验证有效抓取",
            "正式复核任务",
            "已暂停来源",
            "容量延后",
            "来源覆盖与政策知识",
            "去重知识来源网址",
            "全站稳定网址",
            "低相关轮换抽检",
            "未读取及跳过原因",
        ):
            self.assertIn(copy, self.dashboard)
        for misleading_copy in ("智能抓取成功", "等待人工处理", "现行政策知识库", "去重监控来源"):
            self.assertNotIn(misleading_copy, self.dashboard)

    def test_knowledge_current_policy_link_requires_absolute_http_url(self) -> None:
        self.assertIn(
            'if(typeof value!=="string"||!value.trim())return""',
            self.dashboard,
        )
        self.assertIn('const u=new URL(value.trim())', self.dashboard)
        self.assertNotIn("new URL(value,location.href)", self.dashboard)
        self.assertIn(
            'else{article.append(node("div","knowledge-meta","尚无已确认现行页"))}',
            self.dashboard,
        )
        for fake_link in ('href="#"', "href='#'", 'href=""', "href=''"):
            self.assertNotIn(fake_link, self.dashboard)

    def test_dashboard_supports_safe_single_and_batch_source_intake(self) -> None:
        for marker in (
            'id="sourceButton"',
            'id="sourceDialog"',
            'id="sourceInput"',
            'id="sourceCheck"',
            'id="sourceAi"',
            'id="sourceImport"',
            'id="sourceUndo"',
            'id="sourcePreviewRows"',
            'id="manualSourceList"',
            'postJson("/api/v1/sources/preview"',
            'postJson("/api/v1/sources"',
            "/api/v1/source-intake/batches/",
            "每批最多 200 个",
            "AI 只整理名称，不会改写或编造网址",
        ):
            self.assertIn(marker, self.dashboard)
        for unsafe_sink in (
            ".innerHTML",
            ".outerHTML",
            "insertAdjacentHTML",
            "document.write",
        ):
            self.assertNotIn(unsafe_sink, self.dashboard)

    @staticmethod
    def primary_state(status: str, now: str) -> dict[str, object]:
        return {
            "status": status,
            "checked_at": now,
            "last_ok_at": now,
            "snapshot_path": "snapshots/primary/current",
            "snapshot_version_count": 2,
        }

    @staticmethod
    def write_evidence_heartbeat(root: Path, now: str) -> None:
        (root / "evidence-agent-status.json").write_text(
            json.dumps({
                "status": "ok",
                "evidence_status": "ok",
                "knowledge_status": "ok",
                "last_run_at": now,
                "knowledge_last_run_at": now,
            }),
            encoding="utf-8",
        )

    def test_all_enabled_monitored_primary_failures_are_unhealthy(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            official_monitor, "STATE_DIR", Path(temporary)
        ):
            self.write_evidence_heartbeat(Path(temporary), now)
            health = official_monitor.functional_health_summary(
                [
                    {
                        "id": "primary",
                        "monitor_role": "current-primary",
                        "_db_lifecycle_state": "active",
                    }
                ],
                {
                    "primary": self.primary_state("error", now),
                },
                0,
            )
        self.assertEqual(health["status"], "unhealthy")
        self.assertIn("primary_source_current_failures", health["reasons"])
        self.assertIn("all_current_primary_sources_failed", health["reasons"])
        self.assertEqual(health["current_primary_monitored_sources"], 1)

    def test_partial_enabled_monitored_primary_failure_remains_degraded(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        sources = [
            {"id": source_id, "monitor_role": "current-primary", "_db_lifecycle_state": "active"}
            for source_id in ("failed", "working")
        ]
        state = {
            "failed": self.primary_state("error", now),
            "working": self.primary_state("ok", now),
        }
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            official_monitor, "STATE_DIR", Path(temporary)
        ):
            self.write_evidence_heartbeat(Path(temporary), now)
            health = official_monitor.functional_health_summary(sources, state, 0)

        self.assertEqual(health["status"], "degraded")
        self.assertEqual(health["current_primary_monitored_sources"], 2)
        self.assertEqual(health["primary_source_current_failures"], 1)
        self.assertNotIn("all_current_primary_sources_failed", health["reasons"])

    def test_unverified_primary_failure_is_not_reported_as_current_failure(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        source = {
            "id": "unverified",
            "monitor_role": "current-primary",
            "_db_lifecycle_state": "active",
        }
        record = self.primary_state("error", now)
        record.pop("last_ok_at")
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            official_monitor, "STATE_DIR", Path(temporary)
        ):
            self.write_evidence_heartbeat(Path(temporary), now)
            health = official_monitor.functional_health_summary(
                [source], {"unverified": record}, 0
            )

        self.assertNotEqual(health["status"], "unhealthy")
        self.assertEqual(health["primary_source_current_failures"], 0)
        self.assertEqual(health["primary_source_unverified_failures"], 1)
        self.assertNotIn("primary_source_current_failures", health["reasons"])
        self.assertNotIn("all_current_primary_sources_failed", health["reasons"])

    def test_no_primary_source_does_not_trigger_total_primary_failure(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        sources = [
            {"id": "secondary", "monitor_role": "trusted-secondary", "_db_lifecycle_state": "active"}
        ]
        state = {"secondary": self.primary_state("ok", now)}
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            official_monitor, "STATE_DIR", Path(temporary)
        ):
            self.write_evidence_heartbeat(Path(temporary), now)
            health = official_monitor.functional_health_summary(sources, state, 0)

        self.assertNotEqual(health["status"], "unhealthy")
        self.assertEqual(health["current_primary_monitored_sources"], 0)
        self.assertNotIn("all_current_primary_sources_failed", health["reasons"])

    def test_baseline_pending_disabled_and_retired_primary_sources_are_excluded(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        sources = [
            {
                "id": "baseline",
                "monitor_role": "current-primary",
                "_db_lifecycle_state": "baseline_ready",
            },
            {
                "id": "pending",
                "monitor_role": "current-primary",
                "_db_lifecycle_state": "validating",
            },
            {
                "id": "disabled",
                "monitor_role": "current-primary",
                "_db_lifecycle_state": "active",
                "enabled": False,
            },
            {
                "id": "retired",
                "monitor_role": "current-primary",
                "_db_lifecycle_state": "retired",
            },
            {
                "id": "quarantined",
                "monitor_role": "current-primary",
                "_db_lifecycle_state": "quarantined",
            },
        ]
        state = {source["id"]: self.primary_state("error", now) for source in sources}
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            official_monitor, "STATE_DIR", Path(temporary)
        ):
            self.write_evidence_heartbeat(Path(temporary), now)
            health = official_monitor.functional_health_summary(sources, state, 0)

        self.assertNotEqual(health["status"], "unhealthy")
        self.assertEqual(health["current_primary_monitored_sources"], 0)
        self.assertEqual(health["primary_source_current_failures"], 0)
        self.assertEqual(health["active_sources"], 2)
        self.assertEqual(health["inventory_total_sources"], 5)
        self.assertEqual(health["freshness"]["current-primary"]["sources"], 2)
        self.assertNotIn("all_current_primary_sources_failed", health["reasons"])

    def test_health_coverage_uses_durable_distinct_snapshot_versions(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        sources = [
            {
                "id": "baseline",
                "monitor_role": "current-primary",
                "_db_lifecycle_state": "active",
            },
            {
                "id": "changed",
                "monitor_role": "current-primary",
                "_db_lifecycle_state": "validating",
            },
            {
                "id": "unchanged",
                "monitor_role": "current-primary",
                "_db_lifecycle_state": "baseline_ready",
            },
            {
                "id": "disabled",
                "monitor_role": "current-primary",
                "_db_lifecycle_state": "active",
                "enabled": False,
            },
            {
                "id": "retired",
                "monitor_role": "current-primary",
                "_db_lifecycle_state": "retired",
            },
        ]
        state = {
            source["id"]: self.primary_state("ok", now)
            for source in sources
        }
        durable = {
            "baseline": {"complete_snapshots": 1, "content_versions": 1},
            "changed": {"complete_snapshots": 2, "content_versions": 2},
            "unchanged": {"complete_snapshots": 2, "content_versions": 1},
            "disabled": {"complete_snapshots": 5, "content_versions": 5},
            "retired": {"complete_snapshots": 5, "content_versions": 5},
        }
        with (
            tempfile.TemporaryDirectory() as temporary,
            mock.patch.object(official_monitor, "STATE_DIR", Path(temporary)),
            mock.patch.object(
                official_monitor,
                "_durable_snapshot_coverage",
                return_value=durable,
            ) as coverage,
        ):
            self.write_evidence_heartbeat(Path(temporary), now)
            health = official_monitor.functional_health_summary(sources, state, 0)

        coverage.assert_called_once_with({"baseline", "changed", "unchanged"})
        self.assertEqual(health["inventory_total_sources"], 5)
        self.assertEqual(health["active_sources"], 3)
        self.assertEqual(health["baseline_sources"], 3)
        self.assertEqual(health["baseline_coverage"], 1.0)
        self.assertEqual(health["comparable_sources"], 1)
        self.assertEqual(health["comparable_coverage"], 0.3333)
        self.assertEqual(health["required_checks_per_day"], 12.0)

    def test_health_coverage_falls_back_to_state_without_creating_database(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        sources = [
            {"id": "one", "monitor_role": "current-primary", "lifecycle_state": "active"},
            {"id": "two", "monitor_role": "trusted-secondary", "lifecycle_state": "active"},
            {"id": "retired", "monitor_role": "reference", "lifecycle_state": "retired"},
        ]
        state = {
            "one": self.primary_state("ok", now),
            "two": {
                **self.primary_state("ok", now),
                "snapshot_version_count": 1,
                "previous_snapshot_path": None,
            },
            "retired": self.primary_state("ok", now),
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database_path = root / "absent-monitor.db"
            self.write_evidence_heartbeat(root, now)
            official_monitor.close_monitor_store()
            with (
                mock.patch.object(official_monitor, "STATE_DIR", root),
                mock.patch.dict(
                    os.environ,
                    {"MONITOR_DATABASE": str(database_path)},
                    clear=False,
                ),
            ):
                health = official_monitor.functional_health_summary(sources, state, 0)
            official_monitor.close_monitor_store()

        self.assertFalse(database_path.exists())
        self.assertEqual(health["inventory_total_sources"], 3)
        self.assertEqual(health["active_sources"], 2)
        self.assertEqual(health["baseline_sources"], 2)
        self.assertEqual(health["comparable_sources"], 1)

    def test_knowledge_loop_error_is_functionally_unhealthy(self) -> None:
        now = datetime.now(timezone.utc)
        source = {
            "id": "primary",
            "monitor_role": "current-primary",
            "_db_lifecycle_state": "active",
        }
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            official_monitor, "STATE_DIR", Path(temporary)
        ):
            (Path(temporary) / "evidence-agent-status.json").write_text(
                json.dumps({
                    "status": "error",
                    "evidence_status": "ok",
                    "knowledge_status": "error",
                    "knowledge_agent_errors": 1,
                    "last_run_at": now.isoformat(),
                    "knowledge_last_run_at": now.isoformat(),
                }),
                encoding="utf-8",
            )
            health = official_monitor.functional_health_summary(
                [source], {"primary": self.primary_state("ok", now.isoformat())}, 0
            )

        self.assertEqual(health["status"], "unhealthy")
        self.assertIn("knowledge_agent_error", health["reasons"])
        self.assertNotIn("evidence_agent_error", health["reasons"])

    def test_single_knowledge_failure_and_aged_backlog_are_only_degraded(self) -> None:
        now = datetime.now(timezone.utc)
        oldest = now - timedelta(
            seconds=official_monitor.KNOWLEDGE_PENDING_WARN_SECONDS + 1
        )
        source = {
            "id": "primary",
            "monitor_role": "current-primary",
            "_db_lifecycle_state": "active",
        }
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            official_monitor, "STATE_DIR", Path(temporary)
        ):
            (Path(temporary) / "evidence-agent-status.json").write_text(
                json.dumps({
                    "status": "degraded",
                    "evidence_status": "ok",
                    "knowledge_status": "degraded",
                    "knowledge_review_required": 1,
                    "knowledge_pending": 1,
                    "knowledge_oldest_pending_at": oldest.isoformat(),
                    "last_run_at": now.isoformat(),
                    "knowledge_last_run_at": now.isoformat(),
                }),
                encoding="utf-8",
            )
            health = official_monitor.functional_health_summary(
                [source], {"primary": self.primary_state("ok", now.isoformat())}, 0
            )

        self.assertEqual(health["status"], "degraded")
        self.assertIn("knowledge_update_review_required", health["reasons"])
        self.assertIn("knowledge_update_backlog_overdue", health["reasons"])
        self.assertEqual(health["knowledge_agent"]["pending"], 1)

    def test_stale_knowledge_heartbeat_is_functionally_unhealthy(self) -> None:
        now = datetime.now(timezone.utc)
        stale = now - timedelta(
            seconds=official_monitor.KNOWLEDGE_AGENT_MAX_STALE_SECONDS + 1
        )
        source = {
            "id": "primary",
            "monitor_role": "current-primary",
            "_db_lifecycle_state": "active",
        }
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            official_monitor, "STATE_DIR", Path(temporary)
        ):
            (Path(temporary) / "evidence-agent-status.json").write_text(
                json.dumps({
                    "status": "ok",
                    "evidence_status": "ok",
                    "knowledge_status": "ok",
                    "last_run_at": now.isoformat(),
                    "knowledge_last_run_at": stale.isoformat(),
                }),
                encoding="utf-8",
            )
            health = official_monitor.functional_health_summary(
                [source], {"primary": self.primary_state("ok", now.isoformat())}, 0
            )

        self.assertEqual(health["status"], "unhealthy")
        self.assertIn("knowledge_agent_heartbeat_stale", health["reasons"])


class HealthcheckTests(unittest.TestCase):
    @staticmethod
    def response(status: int = 200) -> mock.MagicMock:
        response = mock.MagicMock()
        response.__enter__.return_value.status = status
        return response

    @staticmethod
    def write_state(root: Path, health_status: str | dict[str, object], *, stale: bool = False) -> None:
        timestamp = datetime.now(timezone.utc)
        if stale:
            timestamp -= timedelta(seconds=healthcheck.MAX_STALE_SECONDS + 10)
        (root / "scan-progress.json").write_text(
            json.dumps({"last_progress_at": timestamp.isoformat()}),
            encoding="utf-8",
        )
        functional_health = (
            health_status if isinstance(health_status, dict) else {"status": health_status}
        )
        (root / "status.json").write_text(
            json.dumps(
                {
                    "generated_at": timestamp.isoformat(),
                    "functional_health": functional_health,
                }
            ),
            encoding="utf-8",
        )

    def run_check(self, root: Path) -> int:
        with (
            mock.patch.object(healthcheck, "STATE_DIR", root),
            mock.patch.object(healthcheck.urllib.request, "urlopen", return_value=self.response()),
        ):
            return healthcheck.main()

    def test_healthy_and_degraded_remain_container_healthy(self) -> None:
        for status in ("healthy", "degraded"):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                self.write_state(root, status)
                self.assertEqual(self.run_check(root), 0)

    def test_unhealthy_functional_status_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.write_state(root, "unhealthy")
            self.assertEqual(self.run_check(root), 1)

    def test_dynamic_primary_availability_status_drives_healthcheck(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        sources = [
            {"id": source_id, "monitor_role": "current-primary", "_db_lifecycle_state": "active"}
            for source_id in ("first", "second")
        ]
        for states, expected_status, expected_exit in (
            (("error", "ok"), "degraded", 0),
            (("error", "error"), "unhealthy", 1),
        ):
            with self.subTest(states=states), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                DashboardContractTests.write_evidence_heartbeat(root, now)
                with mock.patch.object(official_monitor, "STATE_DIR", root):
                    health = official_monitor.functional_health_summary(
                        sources,
                        {
                            source["id"]: DashboardContractTests.primary_state(status, now)
                            for source, status in zip(sources, states, strict=True)
                        },
                        0,
                    )
                self.assertEqual(health["status"], expected_status)
                self.write_state(root, health)
                self.assertEqual(self.run_check(root), expected_exit)

    def test_stale_heartbeat_still_fails_when_functionally_degraded(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.write_state(root, "degraded", stale=True)
            self.assertEqual(self.run_check(root), 1)

    def test_http_liveness_failure_still_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.write_state(root, "healthy")
            with (
                mock.patch.object(healthcheck, "STATE_DIR", root),
                mock.patch.object(healthcheck.urllib.request, "urlopen", side_effect=OSError("down")),
            ):
                self.assertEqual(healthcheck.main(), 1)


if __name__ == "__main__":
    unittest.main()
