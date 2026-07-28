from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest import mock

from monitor import official_monitor
from monitor.monitor_store import (
    KnowledgeUpdateProposal,
    MonitorStore,
    PolicyChangeRevision,
    ReviewTask,
    SourceEndpoint,
)


NOW = "2026-07-22T01:00:00+00:00"


class QuietMonitorRequestHandler(official_monitor.MonitorRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        del format, args


class MonitorHttpContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.dashboard_path = self.root / "dashboard.html"
        self.dashboard_path.write_text("<!doctype html><title>monitor</title>", encoding="utf-8")
        self.authoritative_feed = (
            b'<?xml version="1.0" encoding="utf-8"?>'
            b"<rss><channel><snapshot_complete>true</snapshot_complete>"
            b"<snapshot_count>0</snapshot_count><item /></channel></rss>"
        )
        (self.root / "feed.xml").write_bytes(self.authoritative_feed)
        (self.root / "ops-feed.xml").write_bytes(b"<rss><channel /></rss>")
        (self.root / "status.json").write_text(
            json.dumps({"functional_health": {"status": "unhealthy"}}),
            encoding="utf-8",
        )
        self.state_patch = mock.patch.object(official_monitor, "STATE_DIR", self.root)
        self.journal_patch = mock.patch.object(
            official_monitor,
            "STATE_JOURNAL_PATH",
            self.root / "state-journal.jsonl",
        )
        self.dashboard_patch = mock.patch.object(
            official_monitor, "DASHBOARD_PATH", self.dashboard_path
        )
        self.store = MonitorStore(self.root / "monitor.db")
        self.store_patch = mock.patch.object(
            official_monitor, "monitor_store", return_value=self.store
        )
        self.state_patch.start()
        self.journal_patch.start()
        self.dashboard_patch.start()
        self.store_patch.start()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), QuietMonitorRequestHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.store_patch.stop()
        self.dashboard_patch.stop()
        self.journal_patch.stop()
        self.state_patch.stop()
        self.store.close()
        self.temporary.cleanup()

    def fetch(self, path: str) -> tuple[int, bytes]:
        try:
            with urllib.request.urlopen(f"{self.base_url}{path}", timeout=5) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read()

    def fetch_json(self, path: str) -> dict[str, object]:
        status, body = self.fetch(path)
        self.assertEqual(status, 200)
        payload = json.loads(body)
        self.assertIsInstance(payload, dict)
        return payload

    def post_json(self, path: str, payload: dict[str, object]) -> tuple[int, dict[str, object]]:
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def add_sources(self, count: int) -> None:
        for index in range(count):
            self.store.upsert_source(
                SourceEndpoint(
                    id=f"source-{index}",
                    canonical_url=f"https://example.test/pets/{index}",
                    role="current-primary",
                    lifecycle_state="active",
                )
            )

    def add_policy_revisions(self, count: int) -> list[PolicyChangeRevision]:
        return [
            self.store.append_policy_change_revision(
                PolicyChangeRevision(
                    id=f"revision-{index}",
                    change_id=f"change-{index}",
                    fact_key=f"fact-{index}",
                    status="confirmed",
                    occurred_at=NOW,
                    headline=f"Policy {index}",
                )
            )
            for index in range(count)
        ]

    def test_unhealthy_authoritative_feed_returns_503_with_existing_snapshot_body(self) -> None:
        status, body = self.fetch("/feed.xml")

        self.assertEqual(status, 503)
        self.assertEqual(body, self.authoritative_feed)
        self.assertIn(b"<snapshot_complete>true</snapshot_complete>", body)

    def test_unhealthy_monitor_keeps_diagnostic_surfaces_readable(self) -> None:
        for path in ("/ops-feed.xml", "/api/v1/knowledge-current", "/"):
            with self.subTest(path=path):
                status, _ = self.fetch(path)
                self.assertEqual(status, 200)

    def test_healthy_authoritative_feed_remains_readable(self) -> None:
        (self.root / "status.json").write_text(
            json.dumps({"functional_health": {"status": "healthy"}}),
            encoding="utf-8",
        )

        status, body = self.fetch("/feed.xml")

        self.assertEqual(status, 200)
        self.assertEqual(body, self.authoritative_feed)

    def test_social_intelligence_endpoint_returns_boss_safe_payload(self) -> None:
        expected = {
            "status": "unavailable",
            "status_label": "今日数据暂未更新",
            "recent_count": 0,
            "items": [],
        }
        with mock.patch.object(
            official_monitor,
            "social_intelligence_payload",
            return_value=expected,
        ) as payload:
            response = self.fetch_json("/api/v1/social-intelligence?limit=20")

        self.assertEqual(response, expected)
        payload.assert_called_once_with(limit=20)

    def test_xhs_settings_and_login_are_proxied_without_credentials(self) -> None:
        expected_settings = {
            "keywords": [
                {
                    "id": "pet-cabin",
                    "query": "宠物进客舱",
                    "name": "小红书·宠物进客舱",
                    "enabled": True,
                }
            ],
            "interval_minutes": 30,
            "credential_saved": False,
        }
        with (
            mock.patch.object(
                official_monitor,
                "xiaohongshu_settings_payload",
                return_value=expected_settings,
            ),
            mock.patch.object(
                official_monitor,
                "xiaohongshu_login_status_payload",
                return_value={
                    "status": "idle",
                    "status_label": "尚未开始登录",
                    "qr_image": "",
                    "expires_at": "",
                },
            ),
        ):
            settings = self.fetch_json("/api/v1/xhs/settings")
            login = self.fetch_json("/api/v1/xhs/login/status")

        self.assertEqual(settings, expected_settings)
        self.assertEqual(login["status"], "idle")
        self.assertNotIn("cookie", str(settings).lower())
        self.assertNotIn("cookie", str(login).lower())

    def test_xhs_keyword_save_and_login_start_require_local_json_write(self) -> None:
        saved = {
            "keywords": [{"id": "pet", "query": "宠物", "name": "小红书·宠物"}],
            "credential_saved": False,
            "interval_minutes": 30,
        }
        started = {
            "status": "waiting_scan",
            "status_label": "请使用小红书 App 扫码确认",
            "qr_image": "data:image/svg+xml;base64,PHN2Zy8+",
            "expires_at": "123",
        }
        with (
            mock.patch.object(
                official_monitor,
                "update_xiaohongshu_settings",
                return_value=saved,
            ) as update,
            mock.patch.object(
                official_monitor,
                "start_xiaohongshu_login",
                return_value=started,
            ) as start,
        ):
            settings_status, settings = self.post_json(
                "/api/v1/xhs/settings",
                {"keywords": [{"query": "宠物"}]},
            )
            login_status, login = self.post_json(
                "/api/v1/xhs/login/start",
                {},
            )

        self.assertEqual(settings_status, 200)
        self.assertEqual(login_status, 200)
        self.assertEqual(settings, saved)
        self.assertEqual(login, started)
        update.assert_called_once_with([{"query": "宠物"}])
        start.assert_called_once_with()

    def test_sources_count_is_filtered_total_not_page_size(self) -> None:
        self.add_sources(3)

        first = self.fetch_json(
            "/api/v1/sources?state=active&role=current-primary&limit=2"
        )
        second = self.fetch_json(
            "/api/v1/sources?state=active&role=current-primary&limit=2&offset=2"
        )

        self.assertEqual(first["count"], 3)
        self.assertEqual(first["page_count"], 2)
        self.assertEqual(first["next_offset"], 2)
        self.assertIs(first["has_more"], True)
        self.assertEqual(second["count"], 3)
        self.assertEqual(second["page_count"], 1)
        self.assertIsNone(second["next_offset"])
        self.assertIs(second["has_more"], False)

    def test_source_preview_extracts_batch_without_mutating_sources(self) -> None:
        status, payload = self.post_json(
            "/api/v1/sources/preview",
            {
                "input": (
                    "CDC https://www.cdc.gov/importation/dogs/\n"
                    "AVS nparks.gov.sg/avs/pets"
                ),
                "use_ai": False,
            },
        )

        self.assertEqual(status, 200)
        self.assertEqual(payload["valid_count"], 2)
        self.assertEqual(self.store.count_sources(), 0)

    def test_batch_source_import_is_persisted_and_listed_as_manual(self) -> None:
        status, payload = self.post_json(
            "/api/v1/sources",
            {
                "items": [
                    {
                        "url": "https://example.gov/pet-import",
                        "name": "示例宠物入境政策",
                    },
                    {"url": "https://example.gov/pet-export"},
                ]
            },
        )
        manual = self.fetch_json("/api/v1/manual-sources")

        self.assertEqual(status, 201)
        self.assertEqual(payload["added"], 2)
        self.assertEqual(manual["count"], 2)
        self.assertTrue(all(
            item["role"] == "candidate" for item in manual["items"]
        ))
        self.assertTrue(all(
            item["metadata"]["source_origin"] == "manual"
            for item in manual["items"]
        ))

    def test_manual_source_batch_can_be_undone_idempotently(self) -> None:
        _, created = self.post_json(
            "/api/v1/sources",
            {"items": [{"url": "https://example.gov/pets"}]},
        )
        path = (
            "/api/v1/source-intake/batches/"
            f"{created['batch_id']}/actions"
        )

        status, first = self.post_json(path, {"action": "undo"})
        _, second = self.post_json(path, {"action": "undo"})
        source = self.store.get_source(created["items"][0]["source_id"])

        self.assertEqual(status, 200)
        self.assertEqual(first["retired"], 1)
        self.assertEqual(second["retired"], 0)
        self.assertEqual(source.lifecycle_state, "retired")
        self.assertFalse(source.enabled)

    def test_private_source_import_is_rejected(self) -> None:
        status, payload = self.post_json(
            "/api/v1/sources",
            {"input": "http://127.0.0.1/pets"},
        )

        self.assertEqual(status, 400)
        self.assertIn("no URL", payload["error"])
        self.assertEqual(self.store.count_sources(), 0)

    def test_source_import_rejects_more_than_batch_limit(self) -> None:
        status, payload = self.post_json(
            "/api/v1/sources",
            {
                "items": [
                    {"url": f"https://example{index}.gov/pets"}
                    for index in range(201)
                ]
            },
        )

        self.assertEqual(status, 400)
        self.assertIn("exceeds 200", payload["error"])
        self.assertEqual(self.store.count_sources(), 0)

    def test_cross_origin_source_change_is_rejected(self) -> None:
        request = urllib.request.Request(
            f"{self.base_url}/api/v1/sources",
            data=json.dumps({
                "items": [{"url": "https://example.gov/pets"}]
            }).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Origin": "https://attacker.example",
            },
            method="POST",
        )

        with self.assertRaises(urllib.error.HTTPError) as captured:
            urllib.request.urlopen(request, timeout=5)

        self.assertEqual(captured.exception.code, 400)
        self.assertEqual(self.store.count_sources(), 0)

    def test_site_url_inventory_supports_filters_pagination_and_summary(self) -> None:
        records = {
            "https://air.test/pets": {
                "url": "https://air.test/pets",
                "origin": "https://air.test/",
                "stable": True,
                "relevance": "high",
                "fetch_status": "fetched",
                "last_fetched_at": NOW,
            },
            "https://air.test/about": {
                "url": "https://air.test/about",
                "origin": "https://air.test/",
                "stable": True,
                "relevance": "low",
                "fetch_status": "unread",
                "last_fetched_at": "",
            },
            "https://other.test/pets": {
                "url": "https://other.test/pets",
                "origin": "https://other.test/",
                "stable": True,
                "relevance": "high",
                "fetch_status": "unread",
                "last_fetched_at": "",
            },
        }
        (self.root / "site-url-inventory.json").write_text(
            json.dumps({"schema_version": 1, "urls": records}),
            encoding="utf-8",
        )

        payload = self.fetch_json(
            "/api/v1/site-url-inventory"
            "?origin=https%3A%2F%2Fair.test%2F&limit=1"
        )

        self.assertEqual(payload["count"], 2)
        self.assertEqual(payload["page_count"], 1)
        self.assertEqual(payload["next_offset"], 1)
        self.assertEqual(payload["summary"]["stable_urls"], 3)
        self.assertEqual(payload["summary"]["fetched_urls"], 1)

        filtered = self.fetch_json(
            "/api/v1/site-url-inventory?relevance=high&status=unread"
        )
        self.assertEqual(filtered["count"], 1)
        self.assertEqual(filtered["items"][0]["url"], "https://other.test/pets")

        status, body = self.fetch(
            "/api/v1/site-url-inventory?relevance=urgent"
        )
        self.assertEqual(status, 400)
        self.assertIn(b"relevance must be", body)

    def test_review_task_count_is_filtered_total_not_page_size(self) -> None:
        self.add_sources(5)
        for index in range(4):
            self.store.open_review_task(
                ReviewTask(
                    id=f"review-{index}",
                    task_type="source_recovery",
                    source_id=f"source-{index}",
                    reason="source failed",
                    created_at=NOW,
                    due_at=NOW,
                    retry_after=NOW,
                    resume_action="revalidate_source",
                )
            )
        self.store.open_review_task(
            ReviewTask(
                id="review-resolved",
                task_type="source_recovery",
                source_id="source-4",
                reason="already resolved",
                created_at=NOW,
                due_at=NOW,
                retry_after=NOW,
                resume_action="revalidate_source",
                status="resolved",
            )
        )

        payload = self.fetch_json("/api/v1/review-tasks?status=open&limit=2")

        self.assertEqual(payload["count"], 4)
        self.assertEqual(payload["page_count"], 2)
        self.assertEqual(payload["next_offset"], 2)
        self.assertIs(payload["has_more"], True)

    def test_source_recovery_retry_and_resume_actions_schedule_revalidation(self) -> None:
        self.store.upsert_source(
            SourceEndpoint(
                id="source-retry",
                canonical_url="https://example.test/pets/retry",
                role="current-primary",
                lifecycle_state="quarantined",
                enabled=False,
            )
        )
        self.store.open_review_task(
            ReviewTask(
                id="review-retry",
                task_type="source_recovery",
                source_id="source-retry",
                reason="authentication required",
                created_at=NOW,
                due_at=NOW,
                retry_after=NOW,
                resume_action="revalidate_source",
            )
        )

        retry_status, retry_task = self.post_json(
            "/api/v1/review-tasks/review-retry/actions",
            {"action": "retry", "actor": "operator"},
        )
        endpoint = self.store.get_source("source-retry")
        self.assertEqual(retry_status, 200)
        self.assertEqual(retry_task["resume_action"], "revalidate_source")
        self.assertTrue(retry_task["retry_after"])
        self.assertEqual(endpoint.lifecycle_state, "validating")
        self.assertTrue(endpoint.enabled)

        resume_status, resumed_task = self.post_json(
            "/api/v1/review-tasks/review-retry/actions",
            {"action": "resume", "actor": "operator"},
        )
        self.assertEqual(resume_status, 200)
        self.assertEqual(resumed_task["status"], "in_progress")
        self.assertEqual(resumed_task["owner"], "operator")

    def test_knowledge_update_count_is_filtered_total_not_page_size(self) -> None:
        revisions = self.add_policy_revisions(3)
        for index, revision in enumerate(revisions):
            self.store.create_knowledge_update_proposal(
                KnowledgeUpdateProposal(
                    id=f"proposal-{index}",
                    policy_change_revision_id=revision.id,
                    target_ref=f"knowledge/{index}.json",
                    patch_path=f"patches/{index}.json",
                    patch_sha256=str(index) * 64,
                    proposed_at=NOW,
                )
            )

        payload = self.fetch_json(
            "/api/v1/knowledge-updates?status=proposed&limit=2"
        )

        self.assertEqual(payload["count"], 3)
        self.assertEqual(payload["page_count"], 2)
        self.assertEqual(payload["next_offset"], 2)
        self.assertIs(payload["has_more"], True)

    def test_policy_change_count_and_cursor_describe_full_filtered_result(self) -> None:
        self.add_policy_revisions(3)

        first = self.fetch_json("/api/v1/policy-changes?view=effective&limit=2")
        second = self.fetch_json(
            f"/api/v1/policy-changes?view=effective&limit=2&after={first['next_cursor']}"
        )

        self.assertEqual(first["count"], 3)
        self.assertEqual(first["remaining_count"], 3)
        self.assertEqual(first["page_count"], 2)
        self.assertIs(first["has_more"], True)
        self.assertEqual(second["count"], 3)
        self.assertEqual(second["remaining_count"], 1)
        self.assertEqual(second["page_count"], 1)
        self.assertIs(second["has_more"], False)
        self.assertEqual(second["next_cursor"], second["current_cursor"])

    def test_policy_change_digest_forwards_country_and_date_filters(self) -> None:
        expected = {
            "counts": {"changes": 1},
            "country_groups": [{"entity_key": "united-states", "changes": []}],
            "airline_groups": [],
            "other_groups": [],
            "text": "【政策变动汇总】",
            "markdown": "# 政策变动汇总",
        }
        with mock.patch.object(
            official_monitor,
            "policy_change_digest_payload",
            return_value=expected,
        ) as digest:
            payload = self.fetch_json(
                "/api/v1/policy-change-digest"
                "?from=2026-04-01&to=2026-04-30&kind=country&limit=20"
            )

        self.assertEqual(payload, expected)
        digest.assert_called_once_with(
            start_date="2026-04-01",
            end_date="2026-04-30",
            entity_kind="country",
            period="",
            limit=20,
        )

    def test_policy_change_digest_supports_period_and_markdown_export(self) -> None:
        expected = {
            "counts": {"changes": 0},
            "text": "【政策变动汇总】",
            "markdown": "# 政策变动汇总",
        }
        with mock.patch.object(
            official_monitor,
            "policy_change_digest_payload",
            return_value=expected,
        ) as digest:
            status, body = self.fetch(
                "/api/v1/policy-change-digest?period=weekly&format=markdown"
            )

        self.assertEqual(status, 200)
        self.assertEqual(body.decode("utf-8"), "# 政策变动汇总")
        digest.assert_called_once_with(
            start_date="",
            end_date="",
            entity_kind="",
            period="weekly",
            limit=10000,
        )

    def test_policy_change_digest_rejects_unknown_export_format(self) -> None:
        status, body = self.fetch(
            "/api/v1/policy-change-digest?format=html"
        )

        self.assertEqual(status, 400)
        self.assertIn("format must be", json.loads(body)["error"])

    def test_current_knowledge_count_is_total_and_items_are_limited(self) -> None:
        folder = self.root / "knowledge-current"
        folder.mkdir()
        for index in range(3):
            (folder / f"{index}.json").write_text(
                json.dumps(
                    {
                        "target_ref": f"knowledge/{index}.json",
                        "active_proposal_id": f"proposal-{index}",
                    }
                ),
                encoding="utf-8",
            )

        payload = self.fetch_json("/api/v1/knowledge-current?limit=2")

        self.assertEqual(payload["count"], 3)
        self.assertEqual(payload["page_count"], 2)
        self.assertEqual(payload["next_offset"], 2)
        self.assertIs(payload["has_more"], True)


if __name__ == "__main__":
    unittest.main()
