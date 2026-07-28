from __future__ import annotations

import json
import hashlib
import os
import subprocess
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from unittest import mock

from monitor import official_monitor
from monitor.monitor_store import (
    ChangeCandidate,
    ContentSnapshot,
    EvidenceBundle,
    SourceEndpoint,
    stable_id,
)
from monitor.scrapling_fetch import BrowserFetchError, FetchResult, ScraplingAdaptiveFetcher


@contextmanager
def temporary_monitor_directory():
    with tempfile.TemporaryDirectory() as temporary:
        try:
            yield temporary
        finally:
            official_monitor.close_monitor_store()


def seed_verified_policy_lineage(
    root: Path,
    *,
    source_id: str,
    url: str,
    guid: str,
    fact_key: str,
    old_rule: str,
    new_rule: str,
    source_count: int = 1,
    spans: tuple[dict[str, object], ...] = (),
) -> dict[str, object]:
    store = official_monitor.monitor_store()
    store.upsert_source(
        SourceEndpoint(
            id=source_id,
            canonical_url=url,
            display_name=source_id,
            role="current-primary",
            lifecycle_state="active",
            enabled=True,
            metadata={"knowledge_base_refs": ["airlines/example.md"]},
        )
    )
    snapshots: list[ContentSnapshot] = []
    for side, text, captured_at in (
        ("old", old_rule, "2026-07-20T10:00:00+00:00"),
        ("new", new_rule, "2026-07-20T11:00:00+00:00"),
    ):
        relative = Path("snapshots") / source_id / side / "content.md"
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        snapshots.append(
            store.record_snapshot(
                ContentSnapshot(
                    id=stable_id("snapshot", source_id, side, guid),
                    source_id=source_id,
                    captured_at=captured_at,
                    content_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                    normalized_path=relative.as_posix(),
                    complete=True,
                )
            )
        )
    candidate = store.upsert_change_candidate(
        ChangeCandidate(
            id=stable_id("candidate", guid),
            source_id=source_id,
            detected_at="2026-07-20T12:00:00+00:00",
            state="confirmed",
            old_snapshot_id=snapshots[0].id,
            new_snapshot_id=snapshots[1].id,
            fact_key=fact_key,
        )
    )
    artifact = {
        "candidate_id": candidate.id,
        "source_id": source_id,
        "status": "verified",
        "old_snapshot_id": snapshots[0].id,
        "new_snapshot_id": snapshots[1].id,
        "old_rule": [old_rule],
        "new_rule": [new_rule],
    }
    relative_evidence = Path("evidence") / f"{candidate.id}.json"
    evidence_path = root / relative_evidence
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    official_monitor.save_json(evidence_path, artifact)
    bundle = store.record_evidence_bundle(
        EvidenceBundle(
            id=stable_id("evidence", candidate.id, guid),
            candidate_id=candidate.id,
            status="verified",
            rule_version=str(official_monitor.POLICY_EVIDENCE_RULE_VERSION),
            evidence_path=relative_evidence.as_posix(),
            evidence_sha256=hashlib.sha256(evidence_path.read_bytes()).hexdigest(),
            old_snapshot_id=snapshots[0].id,
            new_snapshot_id=snapshots[1].id,
            source_count=source_count,
            spans=spans,
            structured_facts={
                "old_rule": [old_rule],
                "new_rule": [new_rule],
                "changed_fields": ["policy"],
            },
            verified_at="2026-07-20T12:00:00+00:00",
        )
    )
    return {
        "change_candidate_id": candidate.id,
        "evidence_bundle_id": bundle.id,
        "policy_fact_key": fact_key,
    }


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
        with temporary_monitor_directory() as temporary:
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
        with temporary_monitor_directory() as temporary:
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

    def test_failure_scope_only_alerts_sources_with_a_successful_baseline(self) -> None:
        self.assertEqual(official_monitor.failure_scope({"last_ok_at": "2026-07-20T12:00:00+08:00"}), "current")
        self.assertEqual(official_monitor.failure_scope({"checked_at": "2026-07-20T12:00:00+08:00"}), "unverified")

    def test_migrate_failure_record_backfills_lifecycle_fields(self) -> None:
        record = official_monitor.migrate_failure_record({"status": "error", "error": "HTTP 404"})
        self.assertEqual(record["failure_category"], "页面不存在")
        self.assertEqual(record["consecutive_failures"], 1)

    def test_reference_and_terminal_unverified_sources_are_retired(self) -> None:
        reference = {
            "id": "kb-reference", "url": "https://example.test/reference",
            "categories": ["country-fast-lookup"], "evidence_hints": [],
        }
        terminal = {
            "id": "kb-dead", "url": "https://example.test/dead",
            "categories": ["airline-policy"], "evidence_hints": ["official-context"],
        }
        state = {"kb-dead": {"status": "error", "error": "HTTP 404", "consecutive_failures": 1}}
        active, retired = official_monitor.partition_monitor_sources([reference, terminal], state)
        self.assertEqual(active, [])
        self.assertEqual({item["reason"] for item in retired}, {"reference-only", "terminal-unverified:页面不存在"})

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
        with temporary_monitor_directory() as temporary:
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
        self.assertEqual(payload["summary"]["current_error"], 0)
        self.assertEqual(payload["summary"]["unverified_error"], 1)
        self.assertEqual(payload["failures"][0]["category"], "页面不存在")
        self.assertEqual(payload["unverified_failures"][0]["id"], "bad-source")

    def test_dashboard_payload_prioritizes_policy_changes(self) -> None:
        with temporary_monitor_directory() as temporary:
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
                    "review_status": "verified",
                    "evidence_rule_version": official_monitor.POLICY_EVIDENCE_RULE_VERSION,
                }
            }), encoding="utf-8")
            with (
                mock.patch.object(official_monitor, "STATE_DIR", state_dir),
                mock.patch.object(official_monitor, "STATE_JOURNAL_PATH", state_dir / "state-journal.jsonl"),
                mock.patch.object(official_monitor, "SCAN_PROGRESS_PATH", state_dir / "scan-progress.json"),
                mock.patch.object(official_monitor, "POLICY_SUMMARIES_PATH", state_dir / "policy-summaries.json"),
            ):
                state = json.loads(
                    (state_dir / "state.json").read_text(encoding="utf-8")
                )
                state["policy-source"].update(
                    seed_verified_policy_lineage(
                        state_dir,
                        source_id="policy-source",
                        url="https://example.test/pets",
                        guid="content:policy-source:new-hash",
                        fact_key="fact:policy-source:entry-proof",
                        old_rule="Old entry certificate rule.",
                        new_rule="New entry certificate rule.",
                    )
                )
                official_monitor.save_json(state_dir / "state.json", state)
                official_monitor.refresh_policy_change_outputs(
                    json.loads((state_dir / "events.json").read_text(encoding="utf-8")),
                    json.loads((state_dir / "policy-summaries.json").read_text(encoding="utf-8")),
                )
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
        with temporary_monitor_directory() as temporary:
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
        self.assertEqual(payload["agent"]["manual_queue"], 0)
        self.assertEqual(payload["agent"]["blocked_records"], 1)
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
                "policy_evidence": {
                    "quality_gate": True, "status": "verified",
                    "rule_version": official_monitor.POLICY_EVIDENCE_RULE_VERSION,
                    "changed_fields": ["fee"],
                },
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

        with temporary_monitor_directory() as temporary:
            with (
                mock.patch.object(official_monitor, "STATE_DIR", Path(temporary)),
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
        with temporary_monitor_directory() as temporary:
            with (
                mock.patch.object(official_monitor, "STATE_DIR", Path(temporary)),
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
        with temporary_monitor_directory() as temporary:
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
        with temporary_monitor_directory() as temporary:
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
        with temporary_monitor_directory() as temporary, mock.patch.object(
            official_monitor, "STATE_DIR", Path(temporary)
        ):
            record = official_monitor.scan_source(fetcher, source, previous, events, {})
        self.assertFalse(record["validation"]["topic_relevant"])
        self.assertEqual(events, [])

    def test_scan_does_not_emit_policy_event_when_candidate_sentence_existed_before(self) -> None:
        old_text = (
            "Travelling to the EU\nPets must have a microchip and rabies vaccination.\n"
            "These requirements also apply to assistance dogs.\nWeather: hazy sunshine"
        )
        response = mock.Mock(
            status_code=200,
            url="https://gov.test/pet-movements",
            headers={"Content-Type": "text/html"},
            content=(
                b"<html><title>Pet Movements</title><body><h2>Travelling to the EU</h2>"
                b"<p>Pets must have a microchip and rabies vaccination.</p>"
                b"<p>These requirements also apply to assistance dogs.</p>"
                b"<p>Weather: mainly sunny</p></body></html>"
            ),
            mode="static",
            escalation_reason="",
        )
        response.raise_for_status.return_value = None
        fetcher = mock.Mock()
        fetcher.fetch.return_value = response
        source = {
            "id": "country-test", "url": response.url, "category": "country-policy",
            "entity_ids": ["country:test"], "evidence_hints": ["official-context"],
            "min_content_bytes": 80,
        }
        events: list[dict] = []
        with temporary_monitor_directory() as temporary, mock.patch.object(
            official_monitor, "STATE_DIR", Path(temporary)
        ):
            snapshot = Path(temporary) / "snapshots" / "country-test" / "old"
            snapshot.mkdir(parents=True)
            (snapshot / "content.md").write_text(old_text, encoding="utf-8")
            previous = {
                "status": "ok", "sha256": "old", "snapshot_path": "snapshots/country-test/old",
                "content_sample": old_text,
            }
            record = official_monitor.scan_source(fetcher, source, previous, events, {})

        self.assertEqual(events, [])
        self.assertEqual(record["policy_evidence_agent"]["status"], "no_change")

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

    def test_nested_help_center_locale_is_removed_from_policy_family(self) -> None:
        left = official_monitor.policy_url_family_key(
            "https://help.ryanair.test/hc/en-fi/articles/123-pet-policy"
        )
        right = official_monitor.policy_url_family_key(
            "https://help.ryanair.test/hc/ja-jp/articles/123-pet-policy"
        )
        self.assertEqual(left, right)

    def test_inventory_language_variants_are_collapsed_before_monitoring(self) -> None:
        sources = [
            {
                "id": "fi", "url": "https://help.ryanair.test/fi-fi/travel/pets",
                "entity_ids": ["airline:ryanair"], "knowledge_base_refs": ["fi.md"],
            },
            {
                "id": "en", "url": "https://help.ryanair.test/en-us/travel/pets",
                "entity_ids": ["airline:ryanair"], "knowledge_base_refs": ["en.md"],
            },
        ]
        collapsed = official_monitor.collapse_source_families(sources)
        self.assertEqual(len(collapsed), 1)
        self.assertEqual(collapsed[0]["id"], "en")
        self.assertEqual(collapsed[0]["knowledge_base_refs"], ["en.md", "fi.md"])
        self.assertEqual(collapsed[0]["url_aliases"], ["https://help.ryanair.test/fi-fi/travel/pets"])

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

    def test_policy_evidence_agent_rejects_line_already_present_in_old_snapshot(self) -> None:
        sentence = "These requirements also apply to assistance dogs."
        result = official_monitor.PolicyEvidenceAgent().review(
            f"Travelling to the EU\nA microchip is required.\n{sentence}",
            f"Travelling to the EU\nA microchip is required.\n{sentence}\nSunny weather",
            {"quality_gate": True, "changed_fields": ["required"], "removed": [], "added": [sentence]},
        )
        self.assertEqual(result["status"], "no_change")
        self.assertFalse(result["quality_gate"])

    def test_policy_evidence_agent_adds_context_for_verified_rule_change(self) -> None:
        result = official_monitor.PolicyEvidenceAgent().review(
            "Pet fees\nThe pet fee must be USD 50.\nBook before departure.",
            "Pet fees\nThe pet fee must be USD 75.\nBook before departure.",
            {
                "quality_gate": True, "changed_fields": ["fee"],
                "removed": ["The pet fee must be USD 50."],
                "added": ["The pet fee must be USD 75."],
            },
        )
        self.assertEqual(result["status"], "verified")
        self.assertTrue(result["quality_gate"])
        self.assertIn("Pet fees", result["old_context"])
        self.assertIn("Book before departure", result["new_context"])

    def test_policy_evidence_agent_rejects_real_form_and_observed_price_noise(self) -> None:
        cases = {
            "zatca-form": (
                "For inquiries, add a comment.",
                "For inquiries, add a comment.\nRequired Field\nThis field is required",
            ),
            "monde-observed-price": (
                "Fri 02 Oct - Thu 08 Oct / Observed price 2026 / KY\n"
                "Flight Glasgow › Málaga\n$1126",
                "Fri 11 Sep - Wed 30 Sep / Observed price 2026 / KY\n"
                "Flight Birmingham › Paris\n$602",
            ),
        }
        for label, (previous, current) in cases.items():
            with self.subTest(label=label):
                candidate = official_monitor.policy_field_diff(
                    official_monitor.extract_policy_fields(previous),
                    official_monitor.extract_policy_fields(current),
                )
                result = official_monitor.PolicyEvidenceAgent().review(
                    previous,
                    current,
                    candidate,
                )
                self.assertEqual(result["status"], "insufficient_evidence")
                self.assertFalse(result["quality_gate"])
                self.assertEqual(
                    result["reason"],
                    "changed_fact_lacks_pet_policy_context",
                )

    def test_policy_evidence_agent_rejects_ui_noise_even_near_pet_content(self) -> None:
        cases = {
            "generic-form": (
                "Pet travel information\nFeedback",
                "Pet travel information\nFeedback\nRequired Field\nThis field is required",
                "generic_form_validation_noise",
            ),
            "flight-search-price": (
                "Pet travel offers\nFlight deals round trip economy class\n"
                "Fri 02 Oct - Thu 08 Oct / Observed price 2026 / KY",
                "Pet travel offers\nFlight deals round trip economy class\n"
                "Fri 11 Sep - Wed 30 Sep / Observed price 2026 / KY",
                "flight_search_price_noise",
            ),
            "ordinary-flight-fare": (
                "Pet travel offers\nFlight deals round trip economy class\nPrice: USD 100",
                "Pet travel offers\nFlight deals round trip economy class\nPrice: USD 120",
                "flight_search_price_noise",
            ),
        }
        for label, (previous, current, reason) in cases.items():
            with self.subTest(label=label):
                candidate = official_monitor.policy_field_diff(
                    official_monitor.extract_policy_fields(previous),
                    official_monitor.extract_policy_fields(current),
                )
                result = official_monitor.PolicyEvidenceAgent().review(
                    previous,
                    current,
                    candidate,
                )
                self.assertEqual(result["status"], "insufficient_evidence")
                self.assertFalse(result["quality_gate"])
                self.assertEqual(result["reason"], reason)

    def test_policy_evidence_agent_normalizes_html_entities_before_comparison(self) -> None:
        result = official_monitor.PolicyEvidenceAgent().review(
            "Pet carrier\nMaximum 8 kg (pet incl. soft travel carrier).",
            "Pet carrier\nMaximum 8&nbsp;kg (pet incl. soft travel carrier).",
            {
                "quality_gate": True,
                "changed_fields": ["carrier"],
                "removed": ["Maximum 8 kg (pet incl. soft travel carrier)."],
                "added": ["Maximum 8&nbsp;kg (pet incl. soft travel carrier)."],
            },
        )
        self.assertEqual(result["status"], "no_change")
        self.assertFalse(result["quality_gate"])

    def test_policy_evidence_agent_rejects_promotional_content_near_pet_page(self) -> None:
        result = official_monitor.PolicyEvidenceAgent().review(
            "Pet airline policy\nCelebrate 20 Years of BringFido! $20 off a $250+ hotel booking.",
            "Pet airline policy",
            {
                "quality_gate": True,
                "changed_fields": ["booking"],
                "removed": ["Celebrate 20 Years of BringFido! $20 off a $250+ hotel booking."],
                "added": [],
            },
        )
        self.assertEqual(result["reason"], "promotional_content_noise")
        self.assertFalse(result["quality_gate"])

    def test_policy_candidate_precheck_requires_authoritative_source_and_policy_intent(self) -> None:
        candidate = official_monitor.ChangeCandidate(
            id="candidate:test", source_id="source:test", detected_at="2026-07-22T00:00:00+00:00",
            state="gathering_evidence", headline="页面页脚与登录文案调整",
        )
        source = official_monitor.SourceEndpoint(
            id="source:test", canonical_url="https://www.bringfido.com/travel/airline_policies/test/",
            role="trusted-secondary", lifecycle_state="active", enabled=True,
        )
        self.assertEqual(
            official_monitor.policy_candidate_precheck(candidate, source),
            "candidate_headline_identifies_non_policy_page_change",
        )
        candidate = replace(candidate, headline="宠物费用从 50 美元调整为 75 美元")
        self.assertEqual(
            official_monitor.policy_candidate_precheck(candidate, source),
            "third_party_source_requires_corroboration",
        )
        source = replace(source, role="candidate", canonical_url="https://airline.test/pets")
        self.assertEqual(
            official_monitor.policy_candidate_precheck(candidate, source),
            "source_not_yet_authoritative_for_policy_change",
        )

    def test_policy_evidence_agent_accepts_real_pet_policy_changes(self) -> None:
        cases = {
            "pet-fee": (
                "Pet transportation\nThe fee must be USD 50 per flight.",
                "Pet transportation\nThe fee must be USD 75 per flight.",
            ),
            "carrier": (
                "Cats may leave the carrier during a delay.",
                "Cats must remain in the carrier during the entire journey.",
            ),
            "assistance-dog": (
                "Assistance dogs are accepted in the cabin.",
                "Assistance dogs must present a valid veterinary certificate.",
            ),
        }
        for label, (previous, current) in cases.items():
            with self.subTest(label=label):
                candidate = official_monitor.policy_field_diff(
                    official_monitor.extract_policy_fields(previous),
                    official_monitor.extract_policy_fields(current),
                )
                result = official_monitor.PolicyEvidenceAgent().review(
                    previous,
                    current,
                    candidate,
                )
                self.assertEqual(result["status"], "verified")
                self.assertTrue(result["quality_gate"])

    def test_legacy_summary_is_replayed_from_snapshots_before_counting(self) -> None:
        guid = "content:air-test:bbbbbbbbbbbbffffffffffffffffffffffffffffffffffffffffffff"
        summaries = {guid: {"policy_change": True, "headline": "费用调整"}}
        with temporary_monitor_directory() as temporary, mock.patch.object(
            official_monitor, "STATE_DIR", Path(temporary)
        ):
            root = Path(temporary) / "snapshots" / "air-test"
            old = root / "20260720T100000+0800-aaaaaaaaaaaa"
            new = root / "20260721T100000+0800-bbbbbbbbbbbb"
            old.mkdir(parents=True)
            new.mkdir(parents=True)
            (old / "content.md").write_text("Pet fees\nThe pet fee must be USD 50.", encoding="utf-8")
            (new / "content.md").write_text("Pet fees\nThe pet fee must be USD 75.", encoding="utf-8")
            changed = official_monitor.revalidate_policy_summaries(summaries)

        self.assertTrue(changed)
        self.assertTrue(summaries[guid]["policy_change"])
        self.assertEqual(summaries[guid]["review_status"], "verified")

    def test_legacy_summary_without_preceding_snapshot_is_not_confirmed(self) -> None:
        guid = "content:air-test:bbbbbbbbbbbbffffffffffffffffffffffffffffffffffffffffffff"
        summaries = {guid: {"policy_change": True, "headline": "费用调整"}}
        with temporary_monitor_directory() as temporary, mock.patch.object(
            official_monitor, "STATE_DIR", Path(temporary)
        ):
            current = (
                Path(temporary) / "snapshots" / "air-test" /
                "20260721T100000+0800-bbbbbbbbbbbb"
            )
            current.mkdir(parents=True)
            (current / "content.md").write_text("The pet fee must be USD 75.", encoding="utf-8")
            official_monitor.revalidate_policy_summaries(summaries)

        self.assertFalse(summaries[guid]["policy_change"])
        self.assertEqual(summaries[guid]["review_status"], "not_confirmed")
        self.assertEqual(summaries[guid]["evidence_reason"], "missing_preceding_snapshot")

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

    def test_one_time_repair_can_explicitly_override_adaptive_concurrency(self) -> None:
        with (
            mock.patch.dict(os.environ, {"MONITOR_FORCE_SCAN_CONCURRENCY": "true"}),
            mock.patch.object(official_monitor, "SCAN_CONCURRENCY", 16),
        ):
            concurrency, reason = official_monitor.adaptive_scan_concurrency({})
        self.assertEqual(concurrency, 16)
        self.assertEqual(reason, "一次性修复显式覆盖")


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
        official_monitor.close_monitor_store()
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
            "reference": {"status": "ok", "checked_at": "2020-01-01T00:00:00+00:00", "snapshot_path": "ref"},
            "policy": {"status": "ok", "checked_at": "2025-01-01T00:00:00+00:00", "snapshot_path": "policy"},
            "critical": {"status": "ok", "checked_at": "2025-01-01T00:00:00+00:00", "snapshot_path": "critical"},
        }
        batch, due_count, tiers = official_monitor.select_scan_batch(sources, state, 3, now)
        self.assertEqual([item["id"] for item in batch], ["new", "critical", "policy"])
        self.assertEqual(due_count, 4)
        self.assertEqual(tiers["政策来源"], 2)

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


class IntelligenceFeedTests(unittest.TestCase):
    def test_policy_change_ledger_survives_event_eviction_and_deduplicates_sources(self) -> None:
        summaries = {
            "content:source-one:same-hash": {
                "headline": "航司强化宠物入箱", "summary": "全程不得离开运输箱",
                "impact": "承运规则变化", "action": "更新告知", "importance": "high",
                "policy_change": True, "change_kind": "承运规则", "generated_at": "2026-07-20T10:00:00+08:00",
                "review_status": "verified", "evidence_rule_version": official_monitor.POLICY_EVIDENCE_RULE_VERSION,
            },
            "content:source-two:same-hash": {
                "headline": "航司强化宠物在箱要求", "summary": "全程不得离开运输箱",
                "impact": "承运规则变化", "action": "更新告知", "importance": "high",
                "policy_change": True, "change_kind": "承运规则", "generated_at": "2026-07-20T11:00:00+08:00",
                "review_status": "verified", "evidence_rule_version": official_monitor.POLICY_EVIDENCE_RULE_VERSION,
            },
        }
        state = {
            "source-one": {"name": "示例航司", "url": "https://air.test/pets", "category": "airline-policy", "knowledge_base_refs": ["airlines/test.md"]},
            "source-two": {"name": "示例航司", "url": "https://air.test/pets-2", "category": "airline-policy", "knowledge_base_refs": ["airlines/test.md"]},
        }
        with temporary_monitor_directory() as temporary, mock.patch.object(
            official_monitor, "STATE_DIR", Path(temporary)
        ):
            root = Path(temporary)
            state["source-one"].update(
                seed_verified_policy_lineage(
                    root,
                    source_id="source-one",
                    url="https://air.test/pets",
                    guid="content:source-one:same-hash",
                    fact_key="fact:shared-carrier-rule",
                    old_rule="Pets may leave the carrier.",
                    new_rule="Pets must remain in the carrier.",
                )
            )
            state["source-two"].update(
                seed_verified_policy_lineage(
                    root,
                    source_id="source-two",
                    url="https://air.test/pets-2",
                    guid="content:source-two:same-hash",
                    fact_key="fact:shared-carrier-rule",
                    old_rule="Pets may leave the carrier.",
                    new_rule="Pets must remain in the carrier.",
                )
            )
            ledger = official_monitor.sync_policy_change_ledger([], summaries, state)
            persisted = json.loads((Path(temporary) / "policy-changes.json").read_text(encoding="utf-8"))

        self.assertEqual(len(ledger), 1)
        self.assertEqual(len(persisted[0]["source_guids"]), 2)
        self.assertEqual(persisted[0]["entity_kind"], "airline")
        self.assertEqual(ledger[0]["source_id"], "source-two")

    def test_policy_change_ledger_keeps_valid_source_when_later_duplicate_has_no_evidence(self) -> None:
        summaries = {
            "content:source-one:same-hash": {
                "headline": "有效来源",
                "summary": "全程不得离开运输箱",
                "impact": "承运规则变化",
                "action": "更新告知",
                "importance": "high",
                "policy_change": True,
                "change_kind": "承运规则",
                "generated_at": "2026-07-20T10:00:00+08:00",
                "review_status": "verified",
                "evidence_rule_version": official_monitor.POLICY_EVIDENCE_RULE_VERSION,
            },
            "content:source-two:same-hash": {
                "headline": "较晚但无证据来源",
                "summary": "全程不得离开运输箱",
                "impact": "承运规则变化",
                "action": "更新告知",
                "importance": "high",
                "policy_change": True,
                "change_kind": "承运规则",
                "generated_at": "2026-07-20T11:00:00+08:00",
                "review_status": "verified",
                "evidence_rule_version": official_monitor.POLICY_EVIDENCE_RULE_VERSION,
            },
        }
        state = {
            "source-one": {
                "name": "示例航司",
                "url": "https://air.test/pets",
                "category": "airline-policy",
                "knowledge_base_refs": ["airlines/test.md"],
            },
            "source-two": {
                "name": "示例航司",
                "url": "https://air.test/pets-2",
                "category": "airline-policy",
                "knowledge_base_refs": ["airlines/test.md"],
                "policy_fact_key": "fact:shared-carrier-rule",
            },
        }
        with temporary_monitor_directory() as temporary, mock.patch.object(
            official_monitor, "STATE_DIR", Path(temporary)
        ):
            state["source-one"].update(
                seed_verified_policy_lineage(
                    Path(temporary),
                    source_id="source-one",
                    url="https://air.test/pets",
                    guid="content:source-one:same-hash",
                    fact_key="fact:shared-carrier-rule",
                    old_rule="Pets may leave the carrier.",
                    new_rule="Pets must remain in the carrier.",
                )
            )
            ledger = official_monitor.sync_policy_change_ledger([], summaries, state)

        self.assertEqual(len(ledger), 1)
        self.assertEqual(ledger[0]["source_id"], "source-one")
        self.assertEqual(len(ledger[0]["source_guids"]), 2)

    def test_policy_change_ledger_prefers_stronger_evidence_before_newer_time(self) -> None:
        summaries = {
            "content:source-one:same-hash": {
                "headline": "较强证据",
                "summary": "全程不得离开运输箱",
                "impact": "承运规则变化",
                "action": "更新告知",
                "importance": "high",
                "policy_change": True,
                "change_kind": "承运规则",
                "generated_at": "2026-07-20T10:00:00+08:00",
                "review_status": "verified",
                "evidence_rule_version": official_monitor.POLICY_EVIDENCE_RULE_VERSION,
            },
            "content:source-two:same-hash": {
                "headline": "较新证据",
                "summary": "全程不得离开运输箱",
                "impact": "承运规则变化",
                "action": "更新告知",
                "importance": "high",
                "policy_change": True,
                "change_kind": "承运规则",
                "generated_at": "2026-07-20T11:00:00+08:00",
                "review_status": "verified",
                "evidence_rule_version": official_monitor.POLICY_EVIDENCE_RULE_VERSION,
            },
        }
        state = {
            source_id: {
                "name": "示例航司",
                "url": f"https://air.test/{source_id}",
                "category": "airline-policy",
                "knowledge_base_refs": ["airlines/test.md"],
                "policy_fact_key": "fact:shared-carrier-rule",
            }
            for source_id in ("source-one", "source-two")
        }
        with temporary_monitor_directory() as temporary, mock.patch.object(
            official_monitor, "STATE_DIR", Path(temporary)
        ):
            root = Path(temporary)
            stronger_lineage = seed_verified_policy_lineage(
                root,
                source_id="source-one",
                url=state["source-one"]["url"],
                guid="content:source-one:same-hash",
                fact_key="fact:shared-carrier-rule",
                old_rule="Pets may leave the carrier.",
                new_rule="Pets must remain in the carrier.",
                source_count=2,
                spans=({"text": "primary"}, {"text": "secondary"}),
            )
            state["source-two"].update(
                seed_verified_policy_lineage(
                    root,
                    source_id="source-two",
                    url=state["source-two"]["url"],
                    guid="content:source-two:same-hash",
                    fact_key="fact:shared-carrier-rule",
                    old_rule="Pets may leave the carrier.",
                    new_rule="Pets must remain in the carrier.",
                )
            )
            initial = official_monitor.sync_policy_change_ledger([], summaries, state)
            state["source-one"].update(stronger_lineage)
            ledger = official_monitor.sync_policy_change_ledger([], summaries, state)

        self.assertEqual(initial[0]["source_id"], "source-two")
        self.assertEqual(len(ledger), 1)
        self.assertEqual(ledger[0]["source_id"], "source-one")
        self.assertEqual(len(ledger[0]["source_guids"]), 2)
        self.assertNotEqual(initial[0]["revision_id"], ledger[0]["revision_id"])

    def test_business_brief_includes_searchable_knowledge_entities(self) -> None:
        with temporary_monitor_directory() as temporary:
            root = Path(temporary)
            (root / "inventory.json").write_text(json.dumps({
                "generated_at": "2026-07-22T08:00:00+08:00",
                "files_scanned": 2,
                "url_references": 3,
                "unique_sources": 2,
                "entities": [{
                    "id": "airline:test-air",
                    "kind": "airline",
                    "name": "test-air",
                    "knowledge_base_refs": ["airlines/test-air.md"],
                }],
            }), encoding="utf-8")
            (root / "source_registry.json").write_text(json.dumps({
                "entities": {"airline:test-air": {
                    "id": "airline:test-air",
                    "kind": "airline",
                    "name": "test-air",
                    "current": {"url": "https://example.test/pets", "validated_at": "2026-07-22"},
                    "trusted_current_sources": [{"url": "https://example.test/pets"}],
                    "candidates": [{"url": "https://example.test/pets"}],
                }},
            }), encoding="utf-8")
            with (
                mock.patch.object(official_monitor, "STATE_DIR", root),
                mock.patch.object(official_monitor, "dashboard_payload", return_value={
                    "generated_at": "", "summary": {}, "changes": {}, "progress": {},
                    "discovery": {}, "agent": {}, "events": [],
                }),
            ):
                brief = official_monitor.business_brief_payload()

        self.assertEqual(brief["knowledge"]["entity_count"], 1)
        self.assertEqual(brief["knowledge"]["coverage_counts"]["current"], 1)
        self.assertEqual(brief["knowledge"]["entities"][0]["knowledge_base_refs"], ["airlines/test-air.md"])

    def test_business_feed_excludes_monitor_operations(self) -> None:
        events = [
            {
                "guid": "candidate:one", "title": "[发现现行页面候选] 航司",
                "url": "https://example.test/candidate", "detected_at": "2026-07-21T10:00:00+00:00",
                "summary": "内部候选",
            },
            {
                "guid": "content:one:hash", "title": "[数据源内容变化] 航司",
                "url": "https://example.test/policy", "detected_at": "2026-07-21T11:00:00+00:00",
                "summary": "费用发生变化",
                "policy_evidence": {"quality_gate": True},
            },
        ]
        with temporary_monitor_directory() as temporary:
            root = Path(temporary)
            summaries_path = root / "policy-summaries.json"
            summaries_path.write_text(json.dumps({
                "content:one:hash": {
                    "headline": "费用发生变化", "summary": "费用从100调整为150",
                    "impact": "报价变化", "action": "更新报价", "importance": "high",
                    "policy_change": True, "change_kind": "费用", "generated_at": "2026-07-21T11:00:00+00:00",
                    "review_status": "verified", "evidence_rule_version": official_monitor.POLICY_EVIDENCE_RULE_VERSION,
                }
            }), encoding="utf-8")
            with (
                mock.patch.object(official_monitor, "STATE_DIR", root),
                mock.patch.object(official_monitor, "POLICY_SUMMARIES_PATH", summaries_path),
            ):
                lineage = seed_verified_policy_lineage(
                    root,
                    source_id="one",
                    url="https://example.test/policy",
                    guid="content:one:hash",
                    fact_key="fact:fee-change",
                    old_rule="Fee is 100.",
                    new_rule="Fee is 150.",
                )
                official_monitor.save_json(
                    root / "state.json",
                    {
                        "one": {
                            "name": "航司",
                            "url": "https://example.test/policy",
                            "category": "airline-policy",
                            "knowledge_base_refs": ["airlines/example.md"],
                            **lineage,
                        }
                    },
                )
                official_monitor.write_feed(events)
                business_feed = (root / "feed.xml").read_text(encoding="utf-8")
                ops_feed = (root / "ops-feed.xml").read_text(encoding="utf-8")

        self.assertIn("费用发生变化", business_feed)
        self.assertNotIn("发现现行页面候选", business_feed)
        self.assertIn("发现现行页面候选", ops_feed)

    def test_policy_change_summary_contains_decision_fields(self) -> None:
        summary = official_monitor.policy_change_summary(
            {"category": "airline-policy"},
            {
                "changed_fields": ["fee", "weight"],
                "removed": ["Old fee is 100 USD"],
                "added": ["New fee is 150 USD"],
            },
            ["airlines/example.md"],
        )
        self.assertIn("旧规则", summary)
        self.assertIn("新规则", summary)
        self.assertIn("建议行动", summary)


if __name__ == "__main__":
    unittest.main()
