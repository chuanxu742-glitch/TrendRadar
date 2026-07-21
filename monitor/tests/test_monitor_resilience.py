from __future__ import annotations

import json
import os
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from monitor import official_monitor
from monitor.scrapling_fetch import BrowserFetchError, FetchResult, ScraplingAdaptiveFetcher


class FakeProcess:
    def __init__(self, command: list[str], timeout: bool = False, return_code: int = 0) -> None:
        self.command = command
        self.timeout = timeout
        self.return_code = return_code
        self.pid = 12345

    def wait(self, timeout: int | None = None) -> int:
        if self.timeout:
            raise subprocess.TimeoutExpired(self.command, timeout)
        output = Path(self.command[self.command.index("--output") + 1])
        (output / "content.bin").write_bytes(b"<html>pet policy</html>")
        (output / "result.json").write_text(
            json.dumps({"status_code": 200, "url": "https://example.test/pets", "headers": {}}),
            encoding="utf-8",
        )
        return self.return_code

    def kill(self) -> None:
        pass


class BrowserIsolationTests(unittest.TestCase):
    def test_browser_success_reads_worker_artifacts(self) -> None:
        fetcher = ScraplingAdaptiveFetcher(browser_hard_timeout=20)
        with (
            mock.patch(
                "monitor.scrapling_fetch.subprocess.Popen",
                side_effect=lambda command, **_: FakeProcess(command),
            ),
            mock.patch.object(fetcher, "_kill_process_tree"),
        ):
            result = fetcher._browser_fetch("dynamic", "https://example.test/pets", 15, "shell")
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.content, b"<html>pet policy</html>")
        self.assertEqual(result.mode, "dynamic")

    def test_browser_timeout_kills_process_group_and_records_metric(self) -> None:
        fetcher = ScraplingAdaptiveFetcher(browser_hard_timeout=20)
        fake = FakeProcess([], timeout=True)
        with (
            mock.patch("monitor.scrapling_fetch.subprocess.Popen", return_value=fake),
            mock.patch.object(fetcher, "_kill_process_tree") as kill_tree,
        ):
            with self.assertRaisesRegex(BrowserFetchError, "hard timeout"):
                fetcher._browser_fetch("dynamic", "https://example.test/hang", 15, "shell")
        kill_tree.assert_called_once_with(fake)
        self.assertEqual(fetcher.browser_timeouts, 1)

    def test_browser_worker_accepts_no_action_by_default(self) -> None:
        fetcher = ScraplingAdaptiveFetcher(browser_hard_timeout=20)
        with (
            mock.patch(
                "monitor.scrapling_fetch.subprocess.Popen",
                side_effect=lambda command, **_: FakeProcess(command),
            ) as popen,
            mock.patch.object(fetcher, "_kill_process_tree"),
        ):
            fetcher._browser_fetch("dynamic", "https://example.test/pets", 15, "shell")
        self.assertNotIn("--action", popen.call_args.args[0])

    def test_stealth_worker_enables_cloudflare_only_when_requested(self) -> None:
        fetcher = ScraplingAdaptiveFetcher(browser_hard_timeout=75)
        with (
            mock.patch(
                "monitor.scrapling_fetch.subprocess.Popen",
                side_effect=lambda command, **_: FakeProcess(command),
            ) as popen,
            mock.patch.object(fetcher, "_kill_process_tree"),
        ):
            fetcher._browser_fetch(
                "stealth", "https://example.test/pets", 15, "waf", solve_cloudflare=True
            )
        command = popen.call_args.args[0]
        self.assertIn("--solve-cloudflare", command)
        self.assertEqual(command[command.index("--timeout-ms") + 1], "60000")


class ScrapingAgentIntegrationTests(unittest.TestCase):
    @staticmethod
    def page(status: int, body: bytes, content_type: str = "text/html") -> mock.Mock:
        return mock.Mock(
            status=status,
            url="https://air.test/travel/pets",
            headers={"Content-Type": content_type},
            body=body,
        )

    def test_javascript_shell_escalates_to_dynamic(self) -> None:
        fetcher = ScraplingAdaptiveFetcher(dynamic_limit=1, stealth_limit=1)
        rendered = FetchResult(
            200, "https://air.test/travel/pets", {"Content-Type": "text/html"},
            (b"pet transport policy " * 30), "dynamic",
        )
        with (
            mock.patch("monitor.scrapling_fetch.Fetcher.get", return_value=self.page(200, b"<script>app()</script>")),
            mock.patch.object(fetcher, "_browser_fetch", return_value=rendered) as browser_fetch,
        ):
            result = fetcher.fetch(
                "https://air.test/travel/pets", expect_topic=True, topic_terms=("pet",)
            )
        self.assertEqual(result.mode, "dynamic")
        browser_fetch.assert_called_once_with(
            "dynamic", "https://air.test/travel/pets", 15, "agent:dynamic"
        )

    def test_waf_escalates_to_stealth(self) -> None:
        fetcher = ScraplingAdaptiveFetcher(dynamic_limit=1, stealth_limit=1)
        rendered = FetchResult(
            200, "https://air.test/travel/pets", {"Content-Type": "text/html"},
            (b"pet transport policy " * 30), "stealth",
        )
        with (
            mock.patch(
                "monitor.scrapling_fetch.Fetcher.get",
                return_value=self.page(403, b"<html>access denied</html>"),
            ),
            mock.patch.object(fetcher, "_browser_fetch", return_value=rendered) as browser_fetch,
        ):
            result = fetcher.fetch("https://air.test/travel/pets")
        self.assertEqual(result.mode, "stealth")
        browser_fetch.assert_called_once_with(
            "stealth", "https://air.test/travel/pets", 15, "agent:stealth", True
        )

    def test_cloudflare_turnstile_escalates_instead_of_entering_manual_queue(self) -> None:
        fetcher = ScraplingAdaptiveFetcher(dynamic_limit=1, stealth_limit=1)
        rendered = FetchResult(
            200, "https://air.test/travel/pets", {"Content-Type": "text/html"},
            (b"pet transport policy " * 30), "stealth",
        )
        challenge = (
            b'<html><title>Just a moment</title><div class="cf-turnstile">'
            b'Verify you are human</div></html>'
        )
        with (
            mock.patch(
                "monitor.scrapling_fetch.Fetcher.get",
                return_value=self.page(403, challenge),
            ),
            mock.patch.object(fetcher, "_browser_fetch", return_value=rendered) as browser_fetch,
        ):
            result = fetcher.fetch("https://air.test/travel/pets")
        self.assertEqual(result.mode, "stealth")
        browser_fetch.assert_called_once_with(
            "stealth", "https://air.test/travel/pets", 15, "agent:stealth", True
        )
        self.assertEqual(fetcher.cloudflare_attempts, 1)
        self.assertEqual(fetcher.cloudflare_fetch_successes, 1)

    def test_generic_captcha_still_requires_authorized_handler(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state_dir = Path(temporary)
            fetcher = ScraplingAdaptiveFetcher(agent_state_dir=state_dir)
            with (
                mock.patch(
                    "monitor.scrapling_fetch.Fetcher.get",
                    return_value=self.page(200, b"<html>hCaptcha verification</html>"),
                ),
                mock.patch.object(fetcher, "_browser_fetch") as browser_fetch,
            ):
                result = fetcher.fetch("https://air.test/travel/pets")
            queue = json.loads((state_dir / "manual-queue.json").read_text(encoding="utf-8"))
        browser_fetch.assert_not_called()
        self.assertEqual(result.status_code, 200)
        self.assertEqual(queue[0]["attempts"][0]["failure_kind"], "human_verification")

    def test_authentication_checkpoint_is_persisted_without_retry(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state_dir = Path(temporary)
            fetcher = ScraplingAdaptiveFetcher(agent_state_dir=state_dir)
            with (
                mock.patch(
                    "monitor.scrapling_fetch.Fetcher.get",
                    return_value=self.page(401, b"<html>authentication required</html>"),
                ),
                mock.patch.object(fetcher, "_browser_fetch") as browser_fetch,
            ):
                result = fetcher.fetch("https://air.test/travel/pets")
            queue = json.loads((state_dir / "manual-queue.json").read_text(encoding="utf-8"))
        browser_fetch.assert_not_called()
        self.assertEqual(result.status_code, 401)
        self.assertEqual(queue[0]["attempts"][0]["failure_kind"], "authentication_required")

    def test_exhausted_dynamic_budget_falls_back_to_stealth(self) -> None:
        fetcher = ScraplingAdaptiveFetcher(dynamic_limit=0, stealth_limit=1)
        rendered = FetchResult(
            200, "https://air.test/travel/pets", {"Content-Type": "text/html"},
            (b"pet transport policy " * 30), "stealth",
        )
        with (
            mock.patch("monitor.scrapling_fetch.Fetcher.get", return_value=self.page(200, b"<script>app()</script>")),
            mock.patch.object(fetcher, "_browser_fetch", return_value=rendered) as browser_fetch,
        ):
            result = fetcher.fetch(
                "https://air.test/travel/pets", expect_topic=True, topic_terms=("pet",)
            )
        self.assertEqual(result.mode, "stealth")
        browser_fetch.assert_called_once_with(
            "stealth", "https://air.test/travel/pets", 15, "agent:stealth", True
        )

    def test_long_documentation_mentions_do_not_trigger_waf_or_captcha(self) -> None:
        fetcher = ScraplingAdaptiveFetcher(dynamic_limit=1, stealth_limit=1)
        body = (
            "<html><body>Public API documentation about Cloudflare, Turnstile and JavaScript. "
            + ("Pet transport policy reference text. " * 200)
            + "</body></html>"
        ).encode()
        with (
            mock.patch(
                "monitor.scrapling_fetch.Fetcher.get", return_value=self.page(200, body)
            ),
            mock.patch.object(fetcher, "_browser_fetch") as browser_fetch,
        ):
            result = fetcher.fetch("https://air.test/travel/pets")
        self.assertEqual(result.mode, "static")
        browser_fetch.assert_not_called()

    def test_incomplete_static_content_retries_with_dynamic(self) -> None:
        fetcher = ScraplingAdaptiveFetcher(dynamic_limit=1, stealth_limit=1)
        rendered = FetchResult(
            200, "https://air.test/travel/pets", {"Content-Type": "text/html"},
            (b"<html><main>pet transport policy details " + b"rule " * 300 + b"</main></html>"),
            "dynamic",
        )
        with (
            mock.patch(
                "monitor.scrapling_fetch.Fetcher.get",
                return_value=self.page(200, b"<html><body>pet policy summary</body></html>"),
            ),
            mock.patch.object(fetcher, "_browser_fetch", return_value=rendered),
        ):
            result = fetcher.fetch(
                "https://air.test/travel/pets", expect_topic=True, topic_terms=("pet",),
                minimum_visible_chars=1000,
            )
        self.assertEqual(result.mode, "dynamic")

    def test_pdf_does_not_require_html_topic_extraction(self) -> None:
        fetcher = ScraplingAdaptiveFetcher(dynamic_limit=1, stealth_limit=1)
        pdf = b"%PDF-1.7\n" + b"x" * 1000
        with (
            mock.patch(
                "monitor.scrapling_fetch.Fetcher.get",
                return_value=self.page(200, pdf, "application/pdf"),
            ),
            mock.patch.object(fetcher, "_browser_fetch") as browser_fetch,
        ):
            result = fetcher.fetch(
                "https://air.test/travel/pets.pdf", expect_topic=True,
                topic_terms=("pet",), minimum_visible_chars=80,
            )
        self.assertEqual(result.mode, "static")
        browser_fetch.assert_not_called()


class DashboardDataTests(unittest.TestCase):
    def test_failure_categories_are_actionable(self) -> None:
        self.assertEqual(official_monitor.failure_category("404 Client Error", 404), "页面不存在")
        self.assertEqual(official_monitor.failure_category("SSL certificate verify failed"), "证书错误")
        self.assertEqual(official_monitor.failure_category("dynamic browser hard timeout after 75s"), "浏览器抓取")
        self.assertEqual(official_monitor.failure_category("required topic terms not found"), "内容校验")
        self.assertEqual(official_monitor.failure_category("HTTP 500 Server Error"), "服务端错误")
        self.assertEqual(official_monitor.failure_category("curl: (6) Could not resolve host"), "域名解析")

    def test_dashboard_url_rewrites_container_local_links(self) -> None:
        self.assertEqual(
            official_monitor.dashboard_url("http://official-monitor:8090/inventory.json?full=1"),
            "/inventory.json?full=1",
        )
        self.assertEqual(
            official_monitor.dashboard_url("https://example.test/policy"),
            "https://example.test/policy",
        )

    def test_dashboard_payload_includes_journaled_failures(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state_dir = Path(temporary)
            (state_dir / "state.json").write_text(
                json.dumps({"ok-source": {"status": "ok", "url": "https://ok.test", "snapshot_path": "snap"}}),
                encoding="utf-8",
            )
            (state_dir / "state-journal.jsonl").write_text(
                json.dumps({
                    "source_id": "bad-source",
                    "record": {
                        "status": "error",
                        "name": "Broken policy",
                        "url": "https://bad.test/policy",
                        "error": "404 Client Error",
                        "status_code": 404,
                        "checked_at": "2026-07-20T12:00:00+08:00",
                    },
                }) + "\n",
                encoding="utf-8",
            )
            (state_dir / "status.json").write_text("{}", encoding="utf-8")
            (state_dir / "source_registry.json").write_text(
                json.dumps({"entities_with_trusted_sources": 0, "entities": {}}), encoding="utf-8"
            )
            (state_dir / "events.json").write_text("[]", encoding="utf-8")
            with (
                mock.patch.object(official_monitor, "STATE_DIR", state_dir),
                mock.patch.object(official_monitor, "STATE_JOURNAL_PATH", state_dir / "state-journal.jsonl"),
                mock.patch.object(official_monitor, "SCAN_PROGRESS_PATH", state_dir / "scan-progress.json"),
                mock.patch.object(official_monitor, "POLICY_SUMMARIES_PATH", state_dir / "policy-summaries.json"),
            ):
                payload = official_monitor.dashboard_payload()

        self.assertEqual(payload["summary"]["total"], 2)
        self.assertEqual(payload["summary"]["error"], 1)
        self.assertEqual(payload["failures"][0]["category"], "页面不存在")

    def test_dashboard_payload_prioritizes_policy_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state_dir = Path(temporary)
            (state_dir / "state.json").write_text(json.dumps({
                "policy-source": {
                    "status": "ok",
                    "name": "Example airline",
                    "url": "https://example.test/pets",
                    "category": "airline",
                    "knowledge_base_refs": ["airlines/example.md"],
                }
            }), encoding="utf-8")
            (state_dir / "status.json").write_text("{}", encoding="utf-8")
            (state_dir / "source_registry.json").write_text(json.dumps({"entities": {}}), encoding="utf-8")
            (state_dir / "events.json").write_text(json.dumps([
                {
                    "guid": "candidate:new-source",
                    "url": "https://candidate.test",
                    "detected_at": "2026-07-20T12:00:00+08:00",
                    "summary": "candidate",
                },
                {
                    "guid": "content:policy-source:new-hash",
                    "url": "https://example.test/pets",
                    "detected_at": "2026-07-20T12:00:00+08:00",
                    "summary": "变更片段：-old rule | +new rule",
                },
            ]), encoding="utf-8")
            (state_dir / "policy-summaries.json").write_text(json.dumps({
                "content:policy-source:new-hash": {
                    "headline": "宠物入境证明要求调整",
                    "summary": "官方页面明确修改了宠物入境证明要求。",
                    "impact": "影响入境材料准备。",
                    "action": "更新客户材料清单。",
                    "importance": "high",
                    "policy_change": True,
                    "change_kind": "健康证明",
                }
            }), encoding="utf-8")
            with (
                mock.patch.object(official_monitor, "STATE_DIR", state_dir),
                mock.patch.object(official_monitor, "STATE_JOURNAL_PATH", state_dir / "state-journal.jsonl"),
                mock.patch.object(official_monitor, "SCAN_PROGRESS_PATH", state_dir / "scan-progress.json"),
                mock.patch.object(official_monitor, "POLICY_SUMMARIES_PATH", state_dir / "policy-summaries.json"),
            ):
                payload = official_monitor.dashboard_payload()

        self.assertEqual(payload["changes"]["total"], 1)
        self.assertEqual(payload["changes"]["content"], 1)
        self.assertEqual(len(payload["events"]), 1)
        self.assertEqual(payload["events"][0]["summary"], "变更片段：-old rule | +new rule")
        self.assertEqual(payload["events"][0]["knowledge_base_refs"], ["airlines/example.md"])
        self.assertEqual(payload["events"][0]["business"]["headline"], "宠物入境证明要求调整")
        self.assertEqual(payload["changes"]["verified_total"], 1)
        self.assertEqual(payload["changes"]["verified_country"], 0)

    def test_dashboard_payload_exposes_agent_summary_without_secrets(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state_dir = Path(temporary)
            (state_dir / "state.json").write_text("{}", encoding="utf-8")
            (state_dir / "status.json").write_text("{}", encoding="utf-8")
            (state_dir / "source_registry.json").write_text('{"entities": {}}', encoding="utf-8")
            (state_dir / "events.json").write_text("[]", encoding="utf-8")
            agent_dir = state_dir / "scraping-agent"
            agent_dir.mkdir()
            (agent_dir / "status.json").write_text(json.dumps({
                "attempts": 7, "status_counts": {"success": 4, "blocked": 1},
                "failure_counts": {"waf": 2},
            }), encoding="utf-8")
            (agent_dir / "site-profiles.json").write_text(json.dumps({
                "air.test/pets": {"active_strategy": "dynamic"},
                "gov.test/import": {"candidate": {"strategy": "stealth"}},
            }), encoding="utf-8")
            (agent_dir / "manual-queue.json").write_text(json.dumps([{
                "site_key": "air.test/pets", "reason": "human verification", "updated_at": "now",
            }]), encoding="utf-8")
            with (
                mock.patch.object(official_monitor, "STATE_DIR", state_dir),
                mock.patch.object(official_monitor, "STATE_JOURNAL_PATH", state_dir / "state-journal.jsonl"),
                mock.patch.object(official_monitor, "SCAN_PROGRESS_PATH", state_dir / "scan-progress.json"),
                mock.patch.object(official_monitor, "POLICY_SUMMARIES_PATH", state_dir / "policy-summaries.json"),
            ):
                payload = official_monitor.dashboard_payload()
        self.assertEqual(payload["agent"]["attempts"], 7)
        self.assertEqual(payload["agent"]["learned_profiles"], 2)
        self.assertEqual(payload["agent"]["candidate_profiles"], 1)
        self.assertEqual(payload["agent"]["manual_queue"], 1)
        serialized = json.dumps(payload["agent"])
        self.assertNotIn("api_key", serialized)
        self.assertNotIn("secret", serialized)

    def test_parse_ai_summary_response_accepts_json_fence(self) -> None:
        parsed = official_monitor.parse_ai_summary_response(
            '```json\n[{"id":"content:one:hash","headline":"政策变化"}]\n```'
        )
        self.assertEqual(parsed[0]["headline"], "政策变化")

    def test_policy_summaries_process_multiple_batches_concurrently(self) -> None:
        events = [
            {
                "guid": f"content:source-{index}:hash", "title": f"Change {index}",
                "summary": "policy diff", "url": f"https://air.test/{index}",
                "policy_evidence": {"quality_gate": True, "changed_fields": ["fee"]},
            }
            for index in range(45)
        ]
        calls: list[int] = []

        def summarize(batch: list[dict], _: str) -> dict[str, dict]:
            calls.append(len(batch))
            return {
                event["guid"]: {
                    "headline": "非政策变化", "summary": "页面结构调整", "impact": "无",
                    "action": "无需处理", "importance": "low", "policy_change": False,
                    "change_kind": "非政策页面变化", "generated_at": official_monitor.now_iso(),
                }
                for event in batch
            }

        with tempfile.TemporaryDirectory() as temporary:
            with (
                mock.patch.object(official_monitor, "POLICY_SUMMARIES_PATH", Path(temporary) / "summaries.json"),
                mock.patch.object(official_monitor, "AI_SUMMARY_BATCH_SIZE", 20),
                mock.patch.object(official_monitor, "AI_SUMMARY_CONCURRENCY", 4),
                mock.patch.object(official_monitor, "summarize_policy_batch", side_effect=summarize),
                mock.patch.dict(os.environ, {"AI_API_KEY": "test-key", "MONITOR_AI_SUMMARY_ENABLED": "true"}),
            ):
                completed = official_monitor.generate_policy_summaries(events)
                saved = json.loads(official_monitor.POLICY_SUMMARIES_PATH.read_text(encoding="utf-8"))
        self.assertEqual(completed, 45)
        self.assertEqual(sorted(calls), [5, 20, 20])
        self.assertEqual(len(saved), 45)

    def test_policy_summary_skips_content_without_field_evidence(self) -> None:
        events = [{
            "guid": "content:source:noise", "title": "Layout change",
            "summary": "navigation changed", "url": "https://air.test/pets",
            "policy_evidence": {"quality_gate": False},
        }]
        with tempfile.TemporaryDirectory() as temporary:
            with (
                mock.patch.object(official_monitor, "POLICY_SUMMARIES_PATH", Path(temporary) / "summaries.json"),
                mock.patch.object(official_monitor, "summarize_policy_batch") as summarize,
                mock.patch.dict(os.environ, {"AI_API_KEY": "test-key", "MONITOR_AI_SUMMARY_ENABLED": "true"}),
            ):
                completed = official_monitor.generate_policy_summaries(events)
        self.assertEqual(completed, 0)
        summarize.assert_not_called()

    def test_policy_summary_batch_has_process_hard_timeout(self) -> None:
        event = {
            "guid": "content:source-one:hash", "title": "Change",
            "summary": "policy diff", "url": "https://air.test/pets",
        }
        with (
            mock.patch.object(official_monitor, "AI_SUMMARY_HARD_TIMEOUT", 12),
            mock.patch.object(
                official_monitor.subprocess,
                "run",
                side_effect=subprocess.TimeoutExpired(["python", "worker.py"], 12),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "hard timeout after 12s"):
                official_monitor.summarize_policy_batch([event], "test-key")

    def test_business_brief_excludes_operations_failure_details(self) -> None:
        with mock.patch.object(official_monitor, "dashboard_payload", return_value={
            "generated_at": "now", "summary": {}, "changes": {}, "progress": {}, "discovery": {}, "events": [],
            "failures": [{"error": "private technical detail"}], "error_categories": [], "entities": {},
        }):
            payload = official_monitor.business_brief_payload()
        self.assertNotIn("failures", payload)
        self.assertEqual(payload["events"], [])

    def test_event_entity_classifies_migrations_and_knowledge_refs(self) -> None:
        self.assertEqual(
            official_monitor.event_entity({"guid": "migration:airline:example-air:hash"}, {}),
            ("airline", "example-air"),
        )
        state = {"source-one": {"category": "country-policy", "knowledge_base_refs": ["countries/sg.md"]}}
        self.assertEqual(
            official_monitor.event_entity({"guid": "content:source-one:hash"}, state),
            ("country", ""),
        )

    def test_site_discovery_recognizes_multilingual_policy_urls(self) -> None:
        self.assertTrue(official_monitor.discovery_signal("https://air.test/fr/voyager-avec-un-animal"))
        self.assertTrue(official_monitor.discovery_signal("https://air.test/ja/ペット/transport"))
        self.assertTrue(official_monitor.discovery_signal("https://air.test/support", "Traveling with pets"))
        self.assertFalse(official_monitor.discovery_signal("https://air.test/summer-sale"))
        self.assertFalse(official_monitor.usable_candidate_url("https://air.test/images/blind-dog.svg"))
        self.assertTrue(official_monitor.usable_candidate_url("https://air.test/forms/pet-import.pdf"))
        self.assertFalse(official_monitor.trusted_discovery_source({
            "url": "https://api.whatsapp.com/send/123",
            "entity_ids": ["airline:air-test"], "evidence_hints": ["official-context"],
        }))
        self.assertFalse(official_monitor.trusted_discovery_source({
            "url": "https://assets-us-01.kc-usercontent.com/file.pdf",
            "entity_ids": ["airline:air-test"], "evidence_hints": ["official-context"],
        }))

    def test_parse_sitemap_supports_indexes_and_url_sets(self) -> None:
        pages, nested = official_monitor.parse_sitemap(
            b'<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><sitemap><loc>/pages.xml</loc></sitemap></sitemapindex>',
            "https://air.test/sitemap.xml",
        )
        self.assertEqual(pages, [])
        self.assertEqual(nested, ["https://air.test/pages.xml"])
        pages, nested = official_monitor.parse_sitemap(
            b'<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc>https://air.test/travel/pets</loc></url></urlset>',
            "https://air.test/pages.xml",
        )
        self.assertEqual(pages, ["https://air.test/travel/pets"])
        self.assertEqual(nested, [])

    def test_site_discovery_is_scheduled_once_per_domain(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state_path = Path(temporary) / "site-discovery.json"
            sources = [
                {
                    "id": "one", "url": "https://www.air.test/pets",
                    "entity_ids": ["airline:air-test"], "evidence_hints": ["official-context"],
                },
                {
                    "id": "two", "url": "https://www.air.test/baggage",
                    "entity_ids": ["airline:air-test"], "evidence_hints": ["official-context"],
                },
            ]
            result = {
                "origin": "https://www.air.test/", "checked_at": official_monitor.now_iso(),
                "sitemaps_checked": 1, "pages_checked": 1, "new_policy_urls": 2, "errors": 0,
            }
            with (
                mock.patch.object(official_monitor, "SITE_DISCOVERY_STATE_PATH", state_path),
                mock.patch.object(official_monitor, "SITE_DISCOVERY_SITES_PER_CYCLE", 2),
                mock.patch.object(official_monitor, "discover_site", return_value=result) as discover,
                mock.patch.object(official_monitor, "save_json"),
            ):
                summary = official_monitor.run_site_discovery(mock.Mock(), sources, {}, [])
            discover.assert_called_once()
            self.assertEqual(summary["eligible_sites"], 1)
            self.assertEqual(summary["new_policy_urls"], 2)

    def test_site_discovery_processes_sites_concurrently_and_checkpoints_each_result(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state_path = Path(temporary) / "site-discovery.json"
            sources = [
                {
                    "id": f"source-{index}", "url": f"https://air-{index}.test/pets",
                    "entity_ids": [f"airline:air-{index}"], "evidence_hints": ["official-context"],
                }
                for index in range(4)
            ]
            active = 0
            peak = 0
            guard = threading.Lock()

            def discover(_fetcher, source, discovered, events, _fallback_lock):
                nonlocal active, peak
                with guard:
                    active += 1
                    peak = max(peak, active)
                time.sleep(0.02)
                with guard:
                    active -= 1
                return {
                    "origin": source["url"], "checked_at": official_monitor.now_iso(),
                    "engine": "Katana", "sitemaps_checked": 0, "pages_checked": 1,
                    "new_policy_urls": 0, "errors": 0,
                }

            with (
                mock.patch.object(official_monitor, "SITE_DISCOVERY_STATE_PATH", state_path),
                mock.patch.object(official_monitor, "SITE_DISCOVERY_SITES_PER_CYCLE", 4),
                mock.patch.object(official_monitor, "SITE_DISCOVERY_CONCURRENCY", 4),
                mock.patch.object(official_monitor, "discover_site", side_effect=discover),
                mock.patch.object(official_monitor, "save_json") as save,
            ):
                summary = official_monitor.run_site_discovery(mock.Mock(), sources, {}, [])
            self.assertGreaterEqual(peak, 2)
            self.assertEqual(summary["sites_checked"], 4)
            self.assertEqual(summary["concurrency"], 4)
            self.assertGreaterEqual(save.call_count, 12)

    def test_irrelevant_discovered_page_does_not_emit_policy_content_event(self) -> None:
        response = mock.Mock(
            status_code=200,
            url="https://air.test/summer-sale",
            headers={"Content-Type": "text/html"},
            content=(
                b"<html><title>Summer sale</title><body>Book affordable international flights today. "
                b"Explore destinations, seasonal offers, rewards and airport services.</body></html>"
            ),
            mode="static",
            escalation_reason="",
        )
        response.raise_for_status.return_value = None
        fetcher = mock.Mock()
        fetcher.fetch.return_value = response
        source = {
            "id": "candidate-sale", "url": response.url, "category": "discovered-current-candidate",
            "entity_ids": ["airline:air-test"], "evidence_hints": ["official-context"],
        }
        previous = {"status": "ok", "sha256": "old", "content_sample": "Old sale"}
        events: list[dict] = []
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            official_monitor, "STATE_DIR", Path(temporary)
        ):
            record = official_monitor.scan_source(fetcher, source, previous, events, {})
        self.assertFalse(record["validation"]["topic_relevant"])
        self.assertEqual(events, [])

    def test_katana_adapter_normalizes_scope_and_filters_assets(self) -> None:
        completed = subprocess.CompletedProcess(
            args=[], returncode=0,
            stdout=(
                "https://air.test/\n"
                "https://air.test/travel/pets?utm_source=test\n"
                "https://air.test/images/dog.svg\n"
                "https://outside.test/pets\n"
            ),
            stderr="",
        )
        with mock.patch.object(official_monitor.subprocess, "run", return_value=completed) as run:
            result = official_monitor.katana_discover_urls("https://air.test/")
        self.assertTrue(result["ok"])
        self.assertEqual(result["urls"], ["https://air.test/", "https://air.test/travel/pets"])
        command = run.call_args.args[0]
        self.assertIn("-kf", command)
        self.assertEqual(command[command.index("-kf") + 1], "all")
        self.assertIn("-jc", command)
        self.assertIn("-mdp", command)

    def test_katana_is_primary_and_registers_only_policy_urls(self) -> None:
        source = {
            "id": "air-test", "url": "https://air.test/known-policy",
            "entity_ids": ["airline:air-test"], "evidence_hints": ["official-context"],
        }
        discovered: dict[str, dict] = {}
        events: list[dict] = []
        with (
            mock.patch.object(official_monitor, "KATANA_ENABLED", True),
            mock.patch.object(official_monitor, "katana_discover_urls", return_value={
                "ok": True,
                "urls": ["https://air.test/travel/pets", "https://air.test/summer-sale"],
                "duration_ms": 12,
            }),
            mock.patch.object(official_monitor, "discover_site_fallback") as fallback,
        ):
            result = official_monitor.discover_site(mock.Mock(), source, discovered, events)
        fallback.assert_not_called()
        self.assertEqual(result["engine"], "Katana")
        self.assertEqual(result["new_policy_urls"], 1)
        self.assertIn("https://air.test/travel/pets", discovered)
        self.assertNotIn("https://air.test/summer-sale", discovered)

    def test_katana_timeout_retains_partial_urls(self) -> None:
        def timeout_after_writing(command: list[str], **_: object) -> None:
            output_path = Path(command[command.index("-o") + 1])
            output_path.write_text(
                "https://air.test/\nhttps://air.test/travel/pets\n", encoding="utf-8"
            )
            raise subprocess.TimeoutExpired(cmd=command, timeout=45)

        with mock.patch.object(official_monitor.subprocess, "run", side_effect=timeout_after_writing):
            result = official_monitor.katana_discover_urls("https://air.test/")
        self.assertTrue(result["ok"])
        self.assertTrue(result["timed_out"])
        self.assertIn("https://air.test/travel/pets", result["urls"])

    def test_katana_without_policy_candidate_uses_builtin_supplement(self) -> None:
        source = {
            "id": "air-test", "url": "https://air.test/known-policy",
            "entity_ids": ["airline:air-test"], "evidence_hints": ["official-context"],
        }
        fallback_result = {
            "origin": "https://air.test/", "checked_at": official_monitor.now_iso(),
            "engine": "内置发现器", "sitemaps_checked": 2, "pages_checked": 1,
            "new_policy_urls": 1, "errors": 0,
        }
        with (
            mock.patch.object(official_monitor, "KATANA_ENABLED", True),
            mock.patch.object(official_monitor, "katana_discover_urls", return_value={
                "ok": True, "urls": ["https://air.test/"], "duration_ms": 10, "timed_out": False,
            }),
            mock.patch.object(
                official_monitor, "discover_site_fallback", return_value=fallback_result
            ) as fallback,
        ):
            result = official_monitor.discover_site(mock.Mock(), source, {}, [])
        fallback.assert_called_once()
        self.assertEqual(result["engine"], "Katana + 内置补充")
        self.assertEqual(result["new_policy_urls"], 1)

    def test_discovery_probe_is_static_only_and_opens_domain_circuit_on_403(self) -> None:
        fetcher = mock.Mock()
        fetcher.fetch_static.return_value = FetchResult(
            status_code=403, url="https://air.test/robots.txt", headers={},
            content=b"blocked", mode="static-discovery", escalation_reason="",
        )
        source = {
            "id": "air", "url": "https://air.test/pets",
            "entity_ids": ["airline:air"], "evidence_hints": ["official-context"],
        }
        result = official_monitor.discover_site_fallback(fetcher, source, {}, [])
        self.assertTrue(result["blocked"])
        self.assertGreater(result["circuit_open_until"], time.time())
        self.assertEqual(fetcher.fetch_static.call_count, 1)
        fetcher.fetch.assert_not_called()

    def test_language_variants_are_merged_into_one_monitored_policy_family(self) -> None:
        source = {
            "id": "air", "url": "https://air.test/",
            "entity_ids": ["airline:air"], "evidence_hints": ["official-context"],
        }
        discovered: dict[str, dict] = {}
        events: list[dict] = []
        first = official_monitor.register_discovered_candidate(
            "https://air.test/en-us/travel/pets", source, "test", discovered, events
        )
        second = official_monitor.register_discovered_candidate(
            "https://air.test/ja-jp/travel/pets", source, "test", discovered, events
        )
        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(sum(item.get("monitor_enabled", True) for item in discovered.values()), 1)
        self.assertEqual(len(events), 1)

    def test_policy_field_diff_requires_factual_rule_evidence(self) -> None:
        noise = official_monitor.policy_field_diff(
            "Pet policy navigation", "Pet policy information"
        )
        factual = official_monitor.policy_field_diff(
            "Pet fee is USD 50", "Pet fee is USD 75"
        )
        self.assertFalse(noise["quality_gate"])
        self.assertTrue(factual["quality_gate"])
        self.assertIn("fee", factual["changed_fields"])

    def test_adaptive_scan_concurrency_reduces_workers_for_restricted_history(self) -> None:
        state = {
            f"source-{index}": {
                "checked_at": f"2026-07-21T11:{index:02d}:00+08:00",
                "status": "error", "status_code": 403, "error": "HTTP 403",
            }
            for index in range(20)
        }
        with mock.patch.object(official_monitor, "SCAN_CONCURRENCY", 8):
            concurrency, reason = official_monitor.adaptive_scan_concurrency(state)
        self.assertEqual(concurrency, 4)
        self.assertIn("受限率", reason)


class CheckpointResumeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.temporary.name)
        self.patches = [
            mock.patch.object(official_monitor, "STATE_DIR", self.state_dir),
            mock.patch.object(official_monitor, "STATE_JOURNAL_PATH", self.state_dir / "state-journal.jsonl"),
            mock.patch.object(official_monitor, "SCAN_PROGRESS_PATH", self.state_dir / "scan-progress.json"),
            mock.patch.object(official_monitor, "BATCH_SIZE", 3),
            mock.patch.object(official_monitor, "CHECKPOINT_EVERY", 1),
            mock.patch.object(official_monitor, "run_site_discovery", return_value={
                "enabled": True, "eligible_sites": 0, "sites_checked": 0, "new_policy_urls": 0,
            }),
            mock.patch.object(official_monitor, "build_source_registry", return_value={
                "generated_at": "now",
                "entity_count": 0,
                "entities_with_current": 0,
                "entities_with_trusted_sources": 0,
                "entities": {},
            }),
        ]
        for patcher in self.patches:
            patcher.start()

    def tearDown(self) -> None:
        for patcher in reversed(self.patches):
            patcher.stop()
        self.temporary.cleanup()

    def test_scan_isolates_worker_failure_and_completes_batch(self) -> None:
        sources = [
            {"id": f"source-{index}", "url": f"https://example.test/{index}"}
            for index in range(3)
        ]
        inventory = {"entities": [], "entity_count": 0, "sources": sources}
        with mock.patch.object(official_monitor, "load_sources", return_value=(sources, inventory, {})):
            first_calls: list[str] = []

            def fail_second(fetcher, source, previous, events, discovered):
                first_calls.append(source["id"])
                if source["id"] == "source-1":
                    raise RuntimeError("simulated process crash")
                return {"status": "ok", "url": source["url"], "snapshot_path": "snapshot"}

            with mock.patch.object(official_monitor, "scan_source", side_effect=fail_second):
                status = official_monitor.scan()

        self.assertEqual(sorted(first_calls), ["source-0", "source-1", "source-2"])
        self.assertEqual(status["cycle"]["pending"], 0)
        self.assertEqual(status["sources_error"], 1)
        self.assertNotIn("sources", status)
        persisted_status = json.loads((self.state_dir / "status.json").read_text(encoding="utf-8"))
        self.assertNotIn("sources", persisted_status)
        self.assertFalse((self.state_dir / "scan-progress.json").exists())
        self.assertFalse((self.state_dir / "state-journal.jsonl").exists())

    def test_scan_runs_different_hosts_concurrently_but_serializes_same_host(self) -> None:
        sources = [
            {"id": "same-1", "url": "https://same.test/one"},
            {"id": "same-2", "url": "https://same.test/two"},
            {"id": "other-1", "url": "https://other.test/one"},
            {"id": "third-1", "url": "https://third.test/one"},
        ]
        inventory = {"entities": [], "entity_count": 0, "sources": sources}
        active_total = 0
        peak_total = 0
        active_by_host: dict[str, int] = {}
        peak_by_host: dict[str, int] = {}
        fetcher_ids: set[int] = set()
        dynamic_semaphore_ids: set[int] = set()
        stealth_semaphore_ids: set[int] = set()
        guard = threading.Lock()

        def measured_scan(fetcher, source, previous, events, discovered):
            nonlocal active_total, peak_total
            host = source["url"].split("/")[2]
            with guard:
                fetcher_ids.add(id(fetcher))
                dynamic_semaphore_ids.add(id(fetcher.dynamic_semaphore))
                stealth_semaphore_ids.add(id(fetcher.stealth_semaphore))
                active_total += 1
                peak_total = max(peak_total, active_total)
                active_by_host[host] = active_by_host.get(host, 0) + 1
                peak_by_host[host] = max(peak_by_host.get(host, 0), active_by_host[host])
            time.sleep(0.04)
            with guard:
                active_total -= 1
                active_by_host[host] -= 1
            return {"status": "ok", "url": source["url"], "snapshot_path": "snapshot"}

        with (
            mock.patch.object(official_monitor, "BATCH_SIZE", 4),
            mock.patch.object(official_monitor, "SCAN_CONCURRENCY", 4),
            mock.patch.object(official_monitor, "load_sources", return_value=(sources, inventory, {})),
            mock.patch.object(official_monitor, "scan_source", side_effect=measured_scan),
        ):
            official_monitor.scan()

        self.assertGreaterEqual(peak_total, 2)
        self.assertEqual(peak_by_host["same.test"], 1)
        self.assertEqual(len(fetcher_ids), 4)
        self.assertEqual(len(dynamic_semaphore_ids), 1)
        self.assertEqual(len(stealth_semaphore_ids), 1)

    def test_resume_skips_sources_removed_after_checkpoint(self) -> None:
        current_sources = [
            {"id": "source-0", "url": "https://example.test/0"},
            {"id": "source-2", "url": "https://example.test/2"},
        ]
        inventory = {"entities": [], "entity_count": 0, "sources": current_sources}
        (self.state_dir / "scan-progress.json").write_text(
            json.dumps({
                "phase": "scanning",
                "batch_source_ids": ["source-0", "removed-source", "source-2"],
                "batch_size": 3,
                "next_index": 1,
                "next_cursor": 0,
                "started_at": "2026-07-20T12:00:00+08:00",
            }),
            encoding="utf-8",
        )
        (self.state_dir / "state-journal.jsonl").write_text(
            json.dumps({
                "source_id": "source-0",
                "record": {"status": "ok", "url": "https://example.test/0", "snapshot_path": "snapshot"},
            }) + "\n",
            encoding="utf-8",
        )
        calls: list[str] = []

        def succeed(fetcher, source, previous, events, discovered):
            calls.append(source["id"])
            return {"status": "ok", "url": source["url"], "snapshot_path": "snapshot"}

        with (
            mock.patch.object(official_monitor, "load_sources", return_value=(current_sources, inventory, {})),
            mock.patch.object(official_monitor, "scan_source", side_effect=succeed),
        ):
            status = official_monitor.scan()

        self.assertEqual(calls, ["source-2"])
        self.assertEqual(status["cycle"]["pending"], 0)


class AdaptiveSchedulingTests(unittest.TestCase):
    def test_never_seen_and_high_value_sources_are_selected_first(self) -> None:
        now = 2_000_000_000.0
        sources = [
            {"id": "reference", "url": "https://reference.test", "category": "country-index"},
            {"id": "policy", "url": "https://policy.test", "category": "airline-policy"},
            {
                "id": "critical", "url": "https://critical.test", "category": "airline-policy",
                "evidence_hints": ["official-context"],
            },
            {"id": "new", "url": "https://new.test", "category": "country-index"},
        ]
        state = {
            "reference": {"status": "ok", "checked_at": "2020-01-01T00:00:00+00:00"},
            "policy": {"status": "ok", "checked_at": "2025-01-01T00:00:00+00:00"},
            "critical": {"status": "ok", "checked_at": "2025-01-01T00:00:00+00:00"},
        }
        batch, due_count, tiers = official_monitor.select_scan_batch(sources, state, 3, now)
        self.assertEqual([item["id"] for item in batch], ["new", "critical", "policy"])
        self.assertEqual(due_count, 4)
        self.assertEqual(tiers["核心政策"], 1)

    def test_failure_retry_uses_exponential_backoff(self) -> None:
        source = {"id": "failed", "url": "https://failed.test", "category": "airline-policy"}
        previous = {
            "status": "error", "checked_at": "2026-01-01T00:00:00+00:00", "consecutive_failures": 3,
        }
        checked = official_monitor.parse_checked_at(previous["checked_at"])
        with (
            mock.patch.object(official_monitor, "FAILURE_BACKOFF_BASE", 100),
            mock.patch.object(official_monitor, "FAILURE_BACKOFF_MAX", 1000),
        ):
            due_at, tier = official_monitor.source_due_at(source, previous)
        self.assertEqual(due_at, checked + 400)
        self.assertEqual(tier, "失败重试")


if __name__ == "__main__":
    unittest.main()
