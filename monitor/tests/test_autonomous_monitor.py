from __future__ import annotations

import hashlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from xml.etree import ElementTree as ET

from monitor import official_monitor
from monitor.monitor_store import (
    CHANGE_EVIDENCE_OWNER,
    KNOWLEDGE_OPERATIONS_OWNER,
    SOURCE_RECOVERY_OWNER,
    ChangeCandidate,
    ContentSnapshot,
    EvidenceBundle,
    KnowledgeUpdateProposal,
    PolicyChangeRevision,
    ReviewTask,
    SourceEndpoint,
    stable_id,
)
from monitor.scrapling_fetch import BrowserFetchError


NOW = "2026-07-22T08:00:00+00:00"


class AutonomousMonitorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.patchers = [
            mock.patch.object(official_monitor, "STATE_DIR", self.root),
            mock.patch.object(
                official_monitor,
                "STATE_JOURNAL_PATH",
                self.root / "state-journal.jsonl",
            ),
            mock.patch.object(
                official_monitor,
                "POLICY_SUMMARIES_PATH",
                self.root / "policy-summaries.json",
            ),
            mock.patch.dict(
                os.environ,
                {"MONITOR_DATABASE": str(self.root / "monitor.db")},
            ),
        ]
        for patcher in self.patchers:
            patcher.start()
        official_monitor.close_monitor_store()
        self.store = official_monitor.monitor_store()

    def tearDown(self) -> None:
        official_monitor.close_monitor_store()
        for patcher in reversed(self.patchers):
            patcher.stop()
        self.temporary.cleanup()

    def add_source(self, *, state: str = "active") -> SourceEndpoint:
        return self.store.upsert_source(
            SourceEndpoint(
                id="source-one",
                canonical_url="https://air.test/pets",
                display_name="示例航司",
                applies_to_entity_ids=("airline:test-air",),
                role="current-primary",
                lifecycle_state=state,
                enabled=state not in {"quarantined", "retired"},
                metadata={
                    "category": "airline-policy",
                    "knowledge_base_refs": ["airlines/test-air.md"],
                },
            )
        )

    def add_snapshot(self, snapshot_id: str, text: str, digest: str) -> ContentSnapshot:
        relative = Path("snapshots") / snapshot_id / "content.md"
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        return self.store.record_snapshot(
            ContentSnapshot(
                id=snapshot_id,
                source_id="source-one",
                captured_at=NOW,
                content_sha256=digest,
                normalized_path=relative.as_posix(),
                complete=True,
            )
        )

    def add_evidence(
        self,
        candidate_id: str,
        old_snapshot_id: str,
        new_snapshot_id: str,
        *,
        old_rule: list[str] | None = None,
        new_rule: list[str] | None = None,
        status: str = "verified",
    ) -> EvidenceBundle:
        old_rule = [] if old_rule is None else old_rule
        new_rule = (
            ["Pets must remain in the carrier."]
            if new_rule is None
            else new_rule
        )
        payload = {
            "candidate_id": candidate_id,
            "source_id": "source-one",
            "status": status,
            "old_snapshot_id": old_snapshot_id,
            "new_snapshot_id": new_snapshot_id,
            "old_rule": old_rule,
            "new_rule": new_rule,
        }
        relative = Path("evidence") / f"{candidate_id}-seed.json"
        (self.root / relative).parent.mkdir(parents=True, exist_ok=True)
        official_monitor.save_json(self.root / relative, payload)
        return self.store.record_evidence_bundle(
            EvidenceBundle(
                id=f"evidence-{candidate_id}",
                candidate_id=candidate_id,
                status=status,
                rule_version=str(official_monitor.POLICY_EVIDENCE_RULE_VERSION),
                evidence_path=relative.as_posix(),
                evidence_sha256=hashlib.sha256((self.root / relative).read_bytes()).hexdigest(),
                old_snapshot_id=old_snapshot_id,
                new_snapshot_id=new_snapshot_id,
                structured_facts={
                    "old_rule": old_rule,
                    "new_rule": new_rule,
                    "changed_fields": ["carrier"],
                },
                verified_at=NOW if status == "verified" else None,
            )
        )

    def add_verified_lineage(
        self,
        label: str,
        *,
        fact_key: str | None = None,
        old_rule: list[str] | None = None,
        new_rule: list[str] | None = None,
    ) -> tuple[ChangeCandidate, EvidenceBundle]:
        old_rule = [] if old_rule is None else old_rule
        new_rule = [f"规则 {label}"] if new_rule is None else new_rule
        old_text = "\n".join(old_rule) or f"旧规则 {label}"
        new_text = "\n".join(new_rule) or f"规则 {label} 已删除"
        old_snapshot = self.add_snapshot(
            f"snapshot-{label}-old", old_text, "ignored"
        )
        new_snapshot = self.add_snapshot(
            f"snapshot-{label}-new", new_text, "ignored"
        )
        candidate = self.store.upsert_change_candidate(
            ChangeCandidate(
                id=f"candidate-{label}",
                source_id="source-one",
                detected_at=NOW,
                state="confirmed",
                old_snapshot_id=old_snapshot.id,
                new_snapshot_id=new_snapshot.id,
                fact_key=fact_key or f"fact-{label}",
            )
        )
        evidence = self.add_evidence(
            candidate.id,
            old_snapshot.id,
            new_snapshot.id,
            old_rule=old_rule,
            new_rule=new_rule,
        )
        return candidate, evidence

    def add_knowledge_proposal(
        self,
        label: str,
        *,
        revision_status: str = "confirmed",
        write_patch: bool = True,
        empty_new_rule: bool = False,
        operation: str = "upsert",
        summary: str | None = None,
        target_ref: str | None = None,
    ) -> tuple[PolicyChangeRevision, KnowledgeUpdateProposal]:
        summary = summary or f"规则 {label}"
        target_ref = target_ref or f"airlines/{label}.md"
        candidate, evidence = self.add_verified_lineage(
            f"knowledge-{label}",
            fact_key=f"fact-{label}",
            new_rule=[f"规则 {label}"],
        )
        revision = self.store.append_policy_change_revision(
            PolicyChangeRevision(
                id=f"revision-{label}",
                change_id=f"change-{label}",
                fact_key=f"fact-{label}",
                source_id="source-one",
                candidate_id=candidate.id,
                evidence_bundle_id=evidence.id,
                status=revision_status,
                occurred_at=NOW,
                headline=f"规则 {label}",
                summary=summary,
            )
        )
        relative = Path("knowledge-updates") / f"{label}.json"
        path = self.root / relative
        if write_patch:
            path.parent.mkdir(parents=True, exist_ok=True)
            official_monitor.save_json(
                path,
                {
                    "change_id": revision.change_id,
                    "revision_id": revision.id,
                    "target_ref": target_ref,
                    "old_rule": [],
                    "new_rule": [] if empty_new_rule else [f"规则 {label}"],
                    "operation": operation,
                    "evidence_bundle_id": evidence.id,
                },
            )
            patch_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
        else:
            patch_sha256 = hashlib.sha256(b"missing patch").hexdigest()
        proposal = self.store.create_knowledge_update_proposal(
            KnowledgeUpdateProposal(
                id=f"proposal-{label}",
                policy_change_revision_id=revision.id,
                target_ref=target_ref,
                patch_path=relative.as_posix(),
                patch_sha256=patch_sha256,
                proposed_at=NOW,
                metadata={"evidence_bundle_id": evidence.id},
            )
        )
        return revision, proposal

    def test_browser_capacity_deferral_is_audited_without_source_failure_or_review(self) -> None:
        self.add_source(state="active")
        source = {
            "id": "source-one",
            "name": "示例航司",
            "url": "https://air.test/pets",
            "category": "airline-policy",
            "knowledge_base_refs": ["airlines/test-air.md"],
        }
        previous = {
            "status": "ok",
            "checked_at": "2026-07-22T07:00:00+00:00",
            "last_ok_at": "2026-07-22T07:00:00+00:00",
            "consecutive_failures": 0,
            "name": "示例航司",
            "url": source["url"],
        }
        events: list[dict[str, object]] = []
        fetcher = mock.Mock()
        fetcher.fetch.side_effect = BrowserFetchError(
            "dynamic browser budget exhausted",
            failure_kind="budget",
        )

        record = official_monitor.scan_source(fetcher, source, previous, events)
        persisted = official_monitor.persist_check_result(source, previous, record)
        official_monitor.save_json(self.root / "state.json", {"source-one": persisted})
        dashboard = official_monitor.dashboard_payload()

        endpoint = self.store.get_source("source-one")
        check = self.store.connection.execute(
            "SELECT status, error_category, source_lifecycle_after "
            "FROM check_runs WHERE id=?",
            (persisted["check_run_id"],),
        ).fetchone()
        self.assertEqual(record["status"], "deferred")
        self.assertEqual(record["consecutive_failures"], 0)
        self.assertEqual(events, [])
        self.assertEqual(tuple(check), ("deferred", "capacity_budget", "active"))
        self.assertEqual(endpoint.lifecycle_state, "active")
        self.assertEqual(endpoint.consecutive_failures, 0)
        self.assertEqual(self.store.list_review_tasks(source_id="source-one"), [])
        self.assertEqual(self.store.list_outbox(topic="monitor.sources", limit=20), [])
        self.assertEqual(dashboard["summary"]["current_error"], 0)
        self.assertEqual(dashboard["summary"]["error"], 0)
        self.assertEqual(dashboard["summary"]["deferred"], 1)
        self.assertEqual(dashboard["failures"], [])

    def test_blocked_fetch_kinds_create_actionable_source_recovery_tasks(self) -> None:
        blocked_cases = (
            ("human_verification", "authorized_human_verification"),
            ("cloudflare_challenge", "authorized_human_verification"),
            ("authentication_required", "authorized_authentication"),
            ("authentication_checkpoint", "authorized_authentication"),
            ("access_forbidden", "review_access_policy"),
            ("waf", "review_access_policy"),
            ("waf_blocked", "review_access_policy"),
        )
        for index, (failure_kind, required_action) in enumerate(blocked_cases):
            source_id = f"source-blocked-{index}"
            url = f"https://air-{index}.test/pets"
            self.store.upsert_source(
                SourceEndpoint(
                    id=source_id,
                    canonical_url=url,
                    role="current-primary",
                    lifecycle_state="active",
                )
            )
            source = {
                "id": source_id,
                "name": f"Blocked {index}",
                "url": url,
                "category": "airline-policy",
            }
            record = {
                "status": "error",
                "status_code": 403,
                "checked_at": NOW,
                "error": f"blocked by {failure_kind}",
                "failure_category": "访问受限",
                "agent_failure_kind": failure_kind,
                "validation": {"valid": False},
                "consecutive_failures": 1,
            }

            with self.subTest(failure_kind=failure_kind):
                persisted = official_monitor.persist_check_result(source, {}, record)
                endpoint = self.store.get_source(source_id)
                check = self.store.connection.execute(
                    "SELECT status FROM check_runs WHERE id=?",
                    (persisted["check_run_id"],),
                ).fetchone()
                tasks = self.store.list_review_tasks(
                    source_id=source_id,
                    task_type="source_recovery",
                )
                self.assertEqual(check["status"], "blocked")
                self.assertEqual(endpoint.lifecycle_state, "quarantined")
                self.assertFalse(endpoint.enabled)
                self.assertEqual(len(tasks), 1)
                self.assertEqual(tasks[0].owner, SOURCE_RECOVERY_OWNER)
                self.assertEqual(tasks[0].source_id, source_id)
                self.assertTrue(tasks[0].retry_after)
                self.assertEqual(tasks[0].resume_action, "revalidate_source")
                self.assertEqual(tasks[0].metadata["required_action"], required_action)
                self.assertEqual(tasks[0].metadata["agent_failure_kind"], failure_kind)

    def test_retryable_fetch_error_does_not_create_source_recovery_task(self) -> None:
        self.add_source(state="active")
        source = {
            "id": "source-one",
            "name": "示例航司",
            "url": "https://air.test/pets",
            "category": "airline-policy",
        }
        persisted = official_monitor.persist_check_result(
            source,
            {},
            {
                "status": "error",
                "status_code": 503,
                "checked_at": NOW,
                "error": "HTTP 503 server error",
                "failure_category": "服务端错误",
                "agent_failure_kind": "server_error",
                "validation": {"valid": False},
                "consecutive_failures": 1,
            },
        )

        self.assertEqual(self.store.get_source("source-one").lifecycle_state, "degraded")
        self.assertEqual(self.store.list_review_tasks(source_id="source-one"), [])
        check = self.store.connection.execute(
            "SELECT status FROM check_runs WHERE id=?", (persisted["check_run_id"],)
        ).fetchone()
        self.assertEqual(check["status"], "error")

    def test_deterministic_summary_names_old_and_new_rules(self) -> None:
        summary = official_monitor.deterministic_policy_summary({
            "guid": "content:source-one:new",
            "title": "[数据源内容变化] 示例航司",
            "url": "https://air.test/pets",
            "policy_evidence": {
                "rule_version": official_monitor.POLICY_EVIDENCE_RULE_VERSION,
                "status": "verified",
                "quality_gate": True,
                "changed_fields": ["carrier"],
                "removed": ["Pets may leave the carrier during a connection."],
                "added": ["Pets must remain in the carrier during the entire journey."],
            },
        })
        self.assertIsNotNone(summary)
        self.assertTrue(summary["policy_change"])
        self.assertIn("原规则：", summary["summary"])
        self.assertIn("新规则：", summary["summary"])
        self.assertFalse(official_monitor.ambiguous_business_summary(summary["summary"]))

    def test_authoritative_feed_is_complete_and_not_event_limited(self) -> None:
        events = [
            {
                "guid": f"change-{index}",
                "change_id": f"change-{index}",
                "revision": 1,
                "status": "confirmed",
                "title": f"Policy {index}",
                "url": f"https://air.test/pets/{index}",
                "detected_at": NOW,
                "summary": f"Rule {index}",
            }
            for index in range(2)
        ]
        path = self.root / "feed.xml"
        with mock.patch.object(official_monitor, "EVENT_LIMIT", 1):
            official_monitor._write_feed_file(
                events,
                path,
                title="Policy changes",
                description="Complete snapshot",
                complete_snapshot=True,
            )
        channel = ET.fromstring(path.read_bytes()).find("channel")
        self.assertIsNotNone(channel)
        self.assertEqual(channel.findtext("snapshot_complete"), "true")
        self.assertEqual(channel.findtext("snapshot_count"), "2")
        self.assertEqual(len(channel.findall("item")), 2)
        self.assertFalse(path.with_suffix(".xml.tmp").exists())

    def test_empty_summary_snapshot_does_not_delete_effective_change(self) -> None:
        self.add_source()
        candidate, evidence = self.add_verified_lineage(
            "ledger",
            fact_key="fact-ledger",
            new_rule=["宠物全程必须留在运输箱内。"],
        )
        new_snapshot = self.store.get_snapshot(candidate.new_snapshot_id or "")
        guid = f"content:source-one:{new_snapshot.content_sha256}"
        summaries = {
            guid: {
                "headline": "示例航司新增全程入箱要求",
                "summary": "新规则：宠物全程必须留在运输箱内。",
                "impact": "影响客舱运输安排。",
                "action": "值机前确认运输箱。",
                "importance": "high",
                "policy_change": True,
                "change_kind": "承运规则",
                "review_status": "verified",
                "evidence_rule_version": official_monitor.POLICY_EVIDENCE_RULE_VERSION,
                "generated_at": NOW,
            }
        }
        events = [{
            "guid": guid,
            "title": "[数据源内容变化] 示例航司",
            "url": "https://air.test/pets",
            "detected_at": NOW,
            "summary": "old -> new",
            "policy_evidence": {
                "status": "verified",
                "quality_gate": True,
                "changed_fields": ["carrier"],
                "removed": [],
                "added": ["宠物全程必须留在运输箱内。"],
            },
        }]
        state = {
            "source-one": {
                "name": "示例航司",
                "url": "https://air.test/pets",
                "category": "airline-policy",
                "knowledge_base_refs": ["airlines/test-air.md"],
                "change_candidate_id": candidate.id,
                "evidence_bundle_id": evidence.id,
                "policy_fact_key": candidate.fact_key,
            }
        }
        first = official_monitor.sync_policy_change_ledger(events, summaries, state)
        second = official_monitor.sync_policy_change_ledger([], {}, state)
        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 1)
        self.assertEqual(len(self.store.list_effective_policy_changes()), 1)

    def test_policy_change_digest_uses_only_valid_durable_evidence(self) -> None:
        self.add_source()
        candidate, evidence = self.add_verified_lineage(
            "digest",
            fact_key="fact-digest",
            old_rule=["旧入境规则"],
            new_rule=["新入境规则"],
        )
        self.store.append_policy_change_revision(
            PolicyChangeRevision(
                id="revision-digest",
                change_id="change-digest",
                fact_key="fact-digest",
                source_id="source-one",
                candidate_id=candidate.id,
                evidence_bundle_id=evidence.id,
                status="confirmed",
                occurred_at="2026-04-16T08:30:00+08:00",
                headline="高风险国家列表更新",
                summary="原规则：旧入境规则。新规则：新入境规则。",
                impact="影响入境材料核对。",
                recommended_action="按新规则复核材料。",
                metadata={
                    "entity_kind": "country",
                    "entity_key": "united-states",
                    "announcement_date": "2026-04-15",
                    "announcement_date_source": "https://example.test/us-policy",
                    "importance": "high",
                    "change_kind": "入境检疫",
                    "url": "https://example.test/us-policy",
                },
            )
        )

        digest = official_monitor.policy_change_digest_payload()

        self.assertEqual(digest["counts"]["changes"], 1)
        self.assertEqual(digest["counts"]["excluded_invalid_evidence"], 0)
        group = digest["country_groups"][0]
        self.assertEqual(group["label"], "美国 (United States)")
        self.assertEqual(group["changes"][0]["old_rules"], ["旧入境规则"])
        self.assertIn("高风险国家列表更新", digest["text"])

    def test_stale_or_missing_evidence_is_queued_when_snapshots_are_recoverable(self) -> None:
        self.add_source()
        old = self.add_snapshot("stale-old", "Pets are allowed in cabin.", "ignored")
        new = self.add_snapshot(
            "stale-new",
            "Pets must remain in the carrier.",
            "ignored",
        )
        stale = self.store.upsert_change_candidate(
            ChangeCandidate(
                id="candidate-stale",
                source_id="source-one",
                detected_at=NOW,
                state="rejected",
                old_snapshot_id=old.id,
                new_snapshot_id=new.id,
                fact_key="fact-stale",
            )
        )
        missing = self.store.upsert_change_candidate(
            ChangeCandidate(
                id="candidate-missing-bundle",
                source_id="source-one",
                detected_at=NOW,
                state="rejected",
                old_snapshot_id=old.id,
                new_snapshot_id=new.id,
                fact_key="fact-missing",
            )
        )
        artifact = self.root / "evidence" / "stale.json"
        artifact.parent.mkdir(parents=True, exist_ok=True)
        official_monitor.save_json(artifact, {"rule_version": 2})
        self.store.record_evidence_bundle(
            EvidenceBundle(
                id="evidence-stale",
                candidate_id=stale.id,
                status="insufficient",
                rule_version="2",
                evidence_path="evidence/stale.json",
                evidence_sha256=hashlib.sha256(artifact.read_bytes()).hexdigest(),
                old_snapshot_id=old.id,
                new_snapshot_id=new.id,
            )
        )

        result = official_monitor.queue_stale_evidence_reprocessing(self.store)

        self.assertEqual(result["queued"], 2)
        self.assertEqual(result["no_bundle"], 1)
        self.assertEqual(self.store.get_change_candidate(stale.id).state, "gathering_evidence")
        self.assertEqual(
            self.store.get_change_candidate(missing.id).state,
            "gathering_evidence",
        )

    def test_recovery_intent_survives_old_json_error_and_gets_priority(self) -> None:
        self.add_source(state="quarantined")
        old_state = {
            "source-one": {
                "status": "error",
                "checked_at": "2026-07-22T07:00:00+00:00",
                "error": "human verification",
                "failure_category": "访问受限",
                "consecutive_failures": 3,
            }
        }
        official_monitor.save_json(self.root / "state.json", old_state)
        official_monitor.schedule_source_revalidation(
            "source-one", actor="operator", reason="session restored"
        )
        active, excluded = official_monitor.sync_sources_with_store(
            [{
                "id": "source-one",
                "url": "https://air.test/pets",
                "monitor_role": "current-primary",
            }],
            official_monitor.load_state_with_journal(self.root / "state.json"),
        )
        state = official_monitor.load_state_with_journal(self.root / "state.json")
        selected, _, tiers = official_monitor.select_scan_batch(active, state, 1)
        self.assertEqual(excluded, [])
        self.assertEqual([item["id"] for item in selected], ["source-one"])
        self.assertEqual(tiers["queue:recovery"], 1)
        self.assertEqual(self.store.get_source("source-one").lifecycle_state, "validating")

    def test_duplicate_check_run_does_not_advance_json_lifecycle(self) -> None:
        self.add_source(state="validating")
        snapshot_dir = self.root / "snapshots" / "source-one" / "fixed"
        snapshot_dir.mkdir(parents=True)
        (snapshot_dir / "content.md").write_text("Pet carrier must weigh 8 kg.", encoding="utf-8")
        record = {
            "status": "ok",
            "status_code": 200,
            "checked_at": NOW,
            "sha256": "digest-one",
            "snapshot_path": "snapshots/source-one/fixed",
            "content_type": "text/html",
            "content_bytes": 30,
            "validation": {"valid": True},
            "content_changed": False,
        }
        first = official_monitor.persist_check_result(
            {"id": "source-one", "url": "https://air.test/pets"}, {}, dict(record)
        )
        second = official_monitor.persist_check_result(
            {"id": "source-one", "url": "https://air.test/pets"}, {}, dict(record)
        )
        self.assertTrue(first["check_run_inserted"])
        self.assertFalse(second["check_run_inserted"])
        self.assertEqual(first["lifecycle_state"], "baseline_ready")
        self.assertEqual(second["lifecycle_state"], "baseline_ready")

    def test_evidence_agent_publishes_revision_and_proposal(self) -> None:
        self.add_source()
        self.add_snapshot(
            "snapshot-old",
            "Pets must remain in the carrier during flight.",
            "old-digest",
        )
        self.add_snapshot(
            "snapshot-new",
            "Pets must remain in the carrier during flight.\n"
            "Effective from April 1, 2026, the pet carrier must weigh no more "
            "than 8 kg to ensure safe transport.",
            "new-digest",
        )
        candidate = self.store.upsert_change_candidate(
            ChangeCandidate(
                id="candidate-one",
                source_id="source-one",
                detected_at=NOW,
                state="gathering_evidence",
                old_snapshot_id="snapshot-old",
                new_snapshot_id="snapshot-new",
                payload={
                    "url": "https://air.test/pets",
                    "knowledge_base_refs": ["airlines/test-air.md"],
                },
            )
        )
        seed = self.add_evidence(candidate.id, "snapshot-old", "snapshot-new")
        result = official_monitor.evidence_agent_once()
        self.assertEqual(result["confirmed"], 1)
        published_candidate = self.store.get_change_candidate(candidate.id)
        self.assertEqual(published_candidate.state, "confirmed")
        self.assertTrue(published_candidate.payload["published_guid"])
        latest = self.store.list_evidence_bundles(candidate_id=candidate.id, limit=1)[0]
        self.assertNotEqual(latest.id, seed.id)
        self.assertEqual(
            hashlib.sha256((self.root / latest.evidence_path).read_bytes()).hexdigest(),
            latest.evidence_sha256,
        )
        self.assertEqual(latest.structured_facts["effective_date"], "2026-04-01")
        self.assertEqual(
            latest.structured_facts["official_reason_status"],
            "sourced",
        )
        self.assertEqual(len(self.store.list_effective_policy_changes()), 1)
        self.assertEqual(result["knowledge_applied"], 1)
        self.assertEqual(
            len(self.store.list_knowledge_update_proposals(statuses=("applied",))),
            1,
        )
        summaries = official_monitor.load_json(self.root / "policy-summaries.json", {})
        self.assertTrue(next(iter(summaries.values()))["policy_change"])
        self.assertEqual(next(iter(summaries.values()))["effective_date"], "2026-04-01")

    def test_retried_candidate_evidence_artifacts_remain_immutable(self) -> None:
        self.add_source()
        old = self.add_snapshot("snapshot-old", "Pets allowed in cabin.", "old-digest")
        new = self.add_snapshot("snapshot-new", "Pets must remain in a carrier.", "new-digest")
        source = {
            "id": "source-one",
            "url": "https://air.test/pets",
            "entity_ids": ["airline:test-air"],
        }
        previous = {
            "snapshot_id": old.id,
            "snapshot_path": old.normalized_path.rsplit("/", 1)[0],
            "sha256": old.content_sha256,
        }
        base_record = {
            "content_changed": True,
            "checked_at": NOW,
            "sha256": new.content_sha256,
            "page_title": "Pet policy",
            "policy_evidence_agent": {
                "status": "verified",
                "quality_gate": True,
                "rule_version": 1,
                "changed_fields": ["carrier"],
                "removed": ["Pets allowed in cabin."],
                "added": ["Pets must remain in a carrier."],
            },
        }
        official_monitor.persist_change_candidate(source, previous, dict(base_record), new.id)
        official_monitor.persist_change_candidate(
            source,
            previous,
            {**base_record, "checked_at": "2026-07-22T09:00:00+00:00"},
            new.id,
        )
        bundles = self.store.list_evidence_bundles(
            candidate_id=official_monitor.stable_id(
                "candidate", "source-one", new.content_sha256
            ),
            limit=10,
        )
        self.assertEqual(len(bundles), 2)
        self.assertEqual(len({bundle.evidence_path for bundle in bundles}), 2)
        for bundle in bundles:
            self.assertEqual(
                hashlib.sha256((self.root / bundle.evidence_path).read_bytes()).hexdigest(),
                bundle.evidence_sha256,
            )

    def test_evidence_agent_failure_opens_real_review_task(self) -> None:
        self.add_source()
        candidate = self.store.upsert_change_candidate(
            ChangeCandidate(
                id="candidate-broken",
                source_id="source-one",
                detected_at=NOW,
                state="gathering_evidence",
            )
        )
        result = official_monitor.evidence_agent_once()
        tasks = self.store.list_review_tasks(
            change_candidate_id=candidate.id,
            task_type="change_evidence",
        )
        self.assertEqual(result["review_required"], 1)
        self.assertEqual(self.store.get_change_candidate(candidate.id).state, "review_required")
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].owner, CHANGE_EVIDENCE_OWNER)
        self.assertTrue(tasks[0].due_at)
        self.assertEqual(tasks[0].resume_action, "enrich_evidence")

    def test_evidence_agent_automatically_rejects_deterministic_noise(self) -> None:
        self.add_source()
        old = self.add_snapshot(
            "snapshot-form-old",
            "Pet travel\nRequired Field",
            "ignored",
        )
        new = self.add_snapshot(
            "snapshot-form-new",
            "Pet travel\nThis field is required.",
            "ignored",
        )
        candidate = self.store.upsert_change_candidate(
            ChangeCandidate(
                id="candidate-form-noise",
                source_id="source-one",
                detected_at=NOW,
                state="gathering_evidence",
                old_snapshot_id=old.id,
                new_snapshot_id=new.id,
                fact_key="fact-form-noise",
            )
        )
        existing_task = official_monitor.ensure_candidate_review_task(
            candidate,
            reason="generic_form_validation_noise",
        )

        result = official_monitor.evidence_agent_once()

        rejected = self.store.get_change_candidate(candidate.id)
        task = self.store.get_review_task(existing_task.id)
        evidence = self.store.list_evidence_bundles(candidate_id=candidate.id, limit=1)[0]
        self.assertEqual(result["rejected"], 1)
        self.assertEqual(result["review_required"], 0)
        self.assertEqual(rejected.state, "rejected")
        self.assertEqual(rejected.resolution_reason, "generic_form_validation_noise")
        self.assertEqual(rejected.payload["evidence_agent_decision"], "automatic_rejection")
        self.assertEqual(evidence.status, "insufficient")
        self.assertEqual(task.status, "cancelled")
        self.assertEqual(
            self.store.list_review_tasks(
                change_candidate_id=candidate.id,
                task_type="change_evidence",
                statuses=("open", "assigned", "in_progress"),
            ),
            [],
        )

    def test_evidence_agent_fails_closed_on_tampered_snapshot(self) -> None:
        self.add_source()
        old = self.add_snapshot(
            "snapshot-tampered-old",
            "Pets may travel in cabin.",
            "ignored",
        )
        new = self.add_snapshot(
            "snapshot-tampered-new",
            "Pets must remain in the carrier.",
            "ignored",
        )
        candidate = self.store.upsert_change_candidate(
            ChangeCandidate(
                id="candidate-tampered-snapshot",
                source_id="source-one",
                detected_at=NOW,
                state="gathering_evidence",
                old_snapshot_id=old.id,
                new_snapshot_id=new.id,
                fact_key="fact-tampered-snapshot",
            )
        )
        (self.root / (new.normalized_path or "")).write_text(
            "tampered after capture",
            encoding="utf-8",
        )

        result = official_monitor.evidence_agent_once()

        self.assertEqual(result["confirmed"], 0)
        self.assertEqual(result["review_required"], 1)
        self.assertEqual(
            self.store.get_change_candidate(candidate.id).state,
            "review_required",
        )
        self.assertEqual(self.store.list_effective_policy_changes(), [])
        self.assertEqual(
            self.store.list_knowledge_update_proposals(statuses=("proposed", "approved", "applied")),
            [],
        )

    def test_unreadable_snapshot_is_reported_as_invalid_evidence(self) -> None:
        self.add_source()
        candidate, evidence = self.add_verified_lineage("unreadable-snapshot")
        snapshot = self.store.get_snapshot(candidate.old_snapshot_id or "")
        target = (self.root / (snapshot.normalized_path or "")).resolve()
        original_read_bytes = Path.read_bytes

        def guarded_read_bytes(path: Path) -> bytes:
            if path == target:
                raise PermissionError("snapshot denied")
            return original_read_bytes(path)

        with mock.patch.object(Path, "read_bytes", guarded_read_bytes):
            valid, reason = official_monitor.validate_candidate_evidence_chain(
                self.store,
                candidate_id=candidate.id,
                evidence_bundle_id=evidence.id,
                source_id=candidate.source_id,
                fact_key=candidate.fact_key,
            )

        self.assertFalse(valid)
        self.assertIn("snapshot normalized artifact is unreadable", reason)

    def test_bootstrap_counts_unreadable_legacy_snapshot_without_exiting(self) -> None:
        self.add_source()
        self.store.set_metadata("legacy_import_completed", {"completed_at": NOW})
        snapshot_dir = self.root / "snapshots" / "source-one" / "legacy-version"
        snapshot_dir.mkdir(parents=True)
        content_path = snapshot_dir / "content.md"
        content_path.write_text("legacy rule", encoding="utf-8")
        official_monitor.save_json(
            snapshot_dir / "metadata.json",
            {
                "source_id": "source-one",
                "status": "ok",
                "checked_at": NOW,
                "sha256": hashlib.sha256(b"legacy rule").hexdigest(),
                "validation": {"valid": True},
            },
        )
        original_read_bytes = Path.read_bytes

        def guarded_read_bytes(path: Path) -> bytes:
            if path == content_path:
                raise PermissionError("legacy snapshot denied")
            return original_read_bytes(path)

        with mock.patch.object(Path, "read_bytes", guarded_read_bytes):
            status = official_monitor.bootstrap_monitor_store()

        self.assertEqual(status["legacy_snapshot_recovery"]["invalid"], 1)
        self.assertEqual(status["legacy_snapshot_recovery"]["unreadable"], 1)

    def test_bootstrap_reports_legacy_import_io_error_and_retries_next_start(self) -> None:
        with mock.patch.object(
            self.store,
            "import_legacy_directory",
            side_effect=PermissionError("legacy state denied"),
        ):
            status = official_monitor.bootstrap_monitor_store()

        self.assertIn("legacy state denied", status["legacy_import_error"])
        self.assertFalse(self.store.get_metadata("legacy_import_completed", False))

    def test_bootstrap_retracts_and_rolls_back_old_evidence_rule_version(self) -> None:
        self.add_source()
        with mock.patch.object(official_monitor, "POLICY_EVIDENCE_RULE_VERSION", 1):
            revision, proposal = self.add_knowledge_proposal("legacy-rule-version")
            official_monitor.apply_knowledge_proposal(
                proposal.id,
                actor="legacy-agent",
                reason="seed prior rule version",
            )
        self.store.set_metadata("legacy_import_completed", {"completed_at": NOW})

        status = official_monitor.bootstrap_monitor_store()

        self.assertEqual(official_monitor.POLICY_EVIDENCE_RULE_VERSION, 3)
        self.assertEqual(status["evidence_chain_repair"]["retracted"], 1)
        self.assertGreaterEqual(status["knowledge_operation_recovery"]["recovered"], 1)
        self.assertEqual(self.store.list_effective_policy_changes(), [])
        self.assertEqual(
            self.store.get_change_candidate(revision.candidate_id or "").state,
            "gathering_evidence",
        )
        self.assertEqual(
            self.store.get_knowledge_update_proposal(proposal.id).status,
            "rolled_back",
        )
        tasks = self.store.list_review_tasks(task_type="change_evidence")
        self.assertEqual(tasks, [])

        recheck = official_monitor.evidence_agent_once()

        self.assertEqual(recheck["processed"], 1)
        self.assertEqual(recheck["rejected"], 1)
        self.assertEqual(recheck["review_required"], 0)
        self.assertEqual(
            self.store.get_change_candidate(revision.candidate_id or "").state,
            "rejected",
        )
        latest = self.store.list_evidence_bundles(
            candidate_id=revision.candidate_id,
            limit=1,
        )[0]
        self.assertEqual(latest.rule_version, "3")
        self.assertEqual(latest.status, "insufficient")

    def test_knowledge_agent_automatically_applies_confirmed_proposal(self) -> None:
        self.add_source()
        _, proposal = self.add_knowledge_proposal("auto")

        result = official_monitor.knowledge_update_agent_once()

        self.assertEqual(result["knowledge_applied"], 1)
        self.assertEqual(
            self.store.get_knowledge_update_proposal(proposal.id).status,
            "applied",
        )
        materialized = official_monitor.materialized_knowledge_inventory()
        self.assertEqual(materialized[0]["active_proposal_id"], proposal.id)
        self.assertEqual(materialized[0]["current_rule"], ["规则 auto"])

    def test_knowledge_agent_fails_closed_on_tampered_evidence(self) -> None:
        self.add_source()
        revision, proposal = self.add_knowledge_proposal("tampered-evidence")
        bundle = self.store.get_evidence_bundle(revision.evidence_bundle_id or "")
        (self.root / bundle.evidence_path).write_text("{}", encoding="utf-8")

        result = official_monitor.knowledge_update_agent_once()

        self.assertEqual(result["knowledge_applied"], 0)
        self.assertEqual(result["knowledge_review_required"], 1)
        self.assertEqual(
            self.store.get_knowledge_update_proposal(proposal.id).status,
            "rejected",
        )
        self.assertEqual(official_monitor.materialized_knowledge_inventory(), [])
        tasks = self.store.list_review_tasks(task_type="knowledge_update_application")
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].owner, KNOWLEDGE_OPERATIONS_OWNER)
        self.assertIn("evidence artifact hash mismatch", tasks[0].metadata["error"])

    def test_unreadable_evidence_is_retracted_during_bootstrap(self) -> None:
        self.add_source()
        revision, _ = self.add_knowledge_proposal("unreadable-evidence")
        bundle = self.store.get_evidence_bundle(revision.evidence_bundle_id or "")
        target = (self.root / bundle.evidence_path).resolve()
        self.store.set_metadata("legacy_import_completed", {"completed_at": NOW})
        original_read_bytes = Path.read_bytes

        def guarded_read_bytes(path: Path) -> bytes:
            if path == target:
                raise PermissionError("evidence denied")
            return original_read_bytes(path)

        with mock.patch.object(Path, "read_bytes", guarded_read_bytes):
            status = official_monitor.bootstrap_monitor_store()

        self.assertEqual(status["evidence_chain_repair"]["retracted"], 1)
        self.assertEqual(
            self.store.list_effective_policy_changes(),
            [],
        )
        self.assertEqual(
            self.store.get_change_candidate(revision.candidate_id or "").state,
            "gathering_evidence",
        )
        tasks = self.store.list_review_tasks(task_type="change_evidence")
        self.assertEqual(tasks, [])

    def test_unreadable_knowledge_patch_is_rolled_back_during_bootstrap(self) -> None:
        self.add_source()
        _, proposal = self.add_knowledge_proposal("unreadable-patch")
        official_monitor.apply_knowledge_proposal(
            proposal.id,
            actor="test",
            reason="seed applied state",
        )
        target = (self.root / proposal.patch_path).resolve()
        self.store.set_metadata("legacy_import_completed", {"completed_at": NOW})
        original_read_bytes = Path.read_bytes

        def guarded_read_bytes(path: Path) -> bytes:
            if path == target:
                raise PermissionError("patch denied")
            return original_read_bytes(path)

        with mock.patch.object(Path, "read_bytes", guarded_read_bytes):
            status = official_monitor.bootstrap_monitor_store()

        self.assertGreaterEqual(status["knowledge_operation_recovery"]["recovered"], 1)
        self.assertEqual(
            self.store.get_knowledge_update_proposal(proposal.id).status,
            "rolled_back",
        )
        materialized = official_monitor.materialized_knowledge_inventory()
        self.assertEqual(len(materialized), 1)
        self.assertIsNone(materialized[0]["active_proposal_id"])
        self.assertEqual(materialized[0]["current_rule"], [])

    def test_knowledge_agent_repeat_run_is_idempotent(self) -> None:
        self.add_source()
        _, proposal = self.add_knowledge_proposal("repeat")
        first = official_monitor.knowledge_update_agent_once()
        target_path = self.root / "knowledge-current" / (
            f"{official_monitor.stable_id('knowledge-target', proposal.target_ref)}.json"
        )
        first_payload = target_path.read_bytes()

        second = official_monitor.knowledge_update_agent_once()

        self.assertEqual(first["knowledge_applied"], 1)
        self.assertEqual(second["knowledge_applied"], 0)
        self.assertEqual(second["knowledge_checked"], 0)
        self.assertEqual(target_path.read_bytes(), first_payload)
        self.assertEqual(
            self.store.get_knowledge_update_proposal(proposal.id).status,
            "applied",
        )

    def test_knowledge_agent_skips_non_confirmed_revision(self) -> None:
        self.add_source()
        _, proposal = self.add_knowledge_proposal(
            "draft",
            revision_status="draft",
        )

        result = official_monitor.knowledge_update_agent_once()

        self.assertEqual(result["knowledge_applied"], 0)
        self.assertEqual(result["knowledge_skipped"], 1)
        self.assertEqual(
            self.store.get_knowledge_update_proposal(proposal.id).status,
            "proposed",
        )
        self.assertEqual(official_monitor.materialized_knowledge_inventory(), [])

    def test_knowledge_agent_materialization_failure_opens_review_task(self) -> None:
        self.add_source()
        revision, proposal = self.add_knowledge_proposal(
            "missing",
            write_patch=False,
        )

        result = official_monitor.knowledge_update_agent_once()
        tasks = self.store.list_review_tasks(
            task_type="knowledge_update_application",
        )
        original_due_at = tasks[0].due_at
        repeated = official_monitor.knowledge_update_agent_once()
        repeated_tasks = self.store.list_review_tasks(
            task_type="knowledge_update_application",
        )

        self.assertEqual(result["knowledge_applied"], 0)
        self.assertEqual(result["knowledge_review_required"], 1)
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].status, "open")
        self.assertEqual(tasks[0].owner, KNOWLEDGE_OPERATIONS_OWNER)
        self.assertIn("automatic knowledge application failed", tasks[0].reason)
        self.assertEqual(
            tasks[0].resume_action,
            "retry_knowledge_update_application",
        )
        self.assertEqual(tasks[0].metadata["proposal_id"], proposal.id)
        self.assertEqual(
            tasks[0].metadata["policy_change_revision_id"],
            revision.id,
        )
        self.assertEqual(tasks[0].metadata["target_ref"], proposal.target_ref)
        self.assertTrue(tasks[0].metadata["error"])
        self.assertEqual(repeated["knowledge_review_required"], 0)
        self.assertEqual(len(repeated_tasks), 1)
        self.assertEqual(repeated_tasks[0].due_at, original_due_at)

    def test_knowledge_agent_exception_does_not_block_evidence_cycle(self) -> None:
        self.add_source()
        candidate = self.store.upsert_change_candidate(
            ChangeCandidate(
                id="candidate-agent-isolation",
                source_id="source-one",
                detected_at=NOW,
                state="gathering_evidence",
            )
        )

        with mock.patch.object(
            official_monitor,
            "knowledge_update_agent_once",
            side_effect=RuntimeError("knowledge loop unavailable"),
        ):
            result = official_monitor.evidence_agent_once()

        self.assertEqual(result["processed"], 1)
        self.assertEqual(result["review_required"], 1)
        self.assertEqual(result["knowledge_agent_errors"], 1)
        self.assertEqual(
            self.store.get_change_candidate(candidate.id).state,
            "review_required",
        )

    def test_worker_status_is_error_when_knowledge_loop_fails(self) -> None:
        with mock.patch.object(
            official_monitor,
            "evidence_agent_once",
            return_value={
                "processed": 0,
                "confirmed": 0,
                "review_required": 0,
                "knowledge_agent_errors": 1,
            },
        ):
            payload = official_monitor.evidence_agent_worker_once()

        stored = official_monitor.load_json(
            self.root / "evidence-agent-status.json", {}
        )
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["evidence_status"], "ok")
        self.assertEqual(payload["knowledge_status"], "error")
        self.assertEqual(stored["status"], "error")
        self.assertEqual(stored["knowledge_agent_errors"], 1)

    def test_single_knowledge_failure_is_degraded_and_reports_backlog(self) -> None:
        self.add_source()
        _, proposal = self.add_knowledge_proposal("pending-health")

        payload = official_monitor.evidence_agent_status_payload(
            {
                "processed": 0,
                "confirmed": 0,
                "review_required": 0,
                "knowledge_checked": 1,
                "knowledge_applied": 0,
                "knowledge_review_required": 1,
            },
            completed_at=NOW,
        )

        self.assertEqual(payload["status"], "degraded")
        self.assertEqual(payload["knowledge_status"], "degraded")
        self.assertEqual(payload["knowledge_pending"], 1)
        self.assertEqual(payload["knowledge_oldest_pending_at"], proposal.proposed_at)

    def test_new_knowledge_proposal_falls_back_to_confirmed_summary(self) -> None:
        self.add_source()
        summary = "宠物须全程留在运输箱内；运输箱与宠物合计不得超过 8kg。"
        candidate, evidence = self.add_verified_lineage(
            "summary-fallback",
            fact_key="fact-summary-fallback",
            new_rule=official_monitor.confirmed_summary_rules(summary),
        )
        revision = self.store.append_policy_change_revision(
            PolicyChangeRevision(
                id="revision-summary-fallback",
                change_id="change-summary-fallback",
                fact_key="fact-summary-fallback",
                source_id="source-one",
                candidate_id=candidate.id,
                evidence_bundle_id=evidence.id,
                status="confirmed",
                occurred_at=NOW,
                headline="新增运输限制",
                summary=summary,
            )
        )
        proposal = official_monitor.ensure_knowledge_update_proposal(
            self.store,
            revision_id=revision.id,
            change_id=revision.change_id,
            item={
                "entity_kind": "airline",
                "entity_key": "test-air",
                "knowledge_base_refs": ["airlines/fallback.md"],
                "_evidence": {"removed": [], "added": []},
            },
            business={"summary": summary},
            evidence_bundle_id=evidence.id,
        )
        patch = official_monitor.load_json(self.root / proposal.patch_path, {})

        self.assertEqual(
            patch["new_rule"],
            official_monitor.confirmed_summary_rules(summary),
        )
        self.assertEqual(patch["rule_origin"], "confirmed_revision_summary")
        self.assertEqual(patch["rule_source_revision_id"], revision.id)
        official_monitor.knowledge_update_agent_once()
        current = official_monitor.materialized_knowledge_inventory()[0]
        self.assertEqual(current["current_rule"], patch["new_rule"])

    def test_bootstrap_reconcile_repairs_active_applied_empty_rule(self) -> None:
        self.add_source()
        summary = "宠物必须全程留在运输箱内。"
        _, proposal = self.add_knowledge_proposal(
            "legacy-empty",
            empty_new_rule=True,
            summary=summary,
        )
        official_monitor.apply_knowledge_proposal(proposal.id, actor="operator")
        target_path = self.root / "knowledge-current" / (
            f"{official_monitor.stable_id('knowledge-target', proposal.target_ref)}.json"
        )
        legacy_payload = official_monitor.load_json(target_path, {})
        legacy_payload["current_rule"] = []
        legacy_payload.pop("rule_origin", None)
        official_monitor.save_json(target_path, legacy_payload)

        recovered = official_monitor.reconcile_knowledge_operations(self.store)
        repaired = official_monitor.load_json(target_path, {})

        self.assertGreaterEqual(recovered["recovered"], 1)
        self.assertEqual(repaired["active_proposal_id"], proposal.id)
        self.assertEqual(
            repaired["current_rule"],
            official_monitor.confirmed_summary_rules(summary),
        )
        self.assertEqual(repaired["rule_origin"], "confirmed_revision_summary")

    def test_empty_active_rule_repair_does_not_promote_history(self) -> None:
        self.add_source()
        target_ref = "airlines/shared-history.md"
        _, proposal_a = self.add_knowledge_proposal(
            "history-a",
            target_ref=target_ref,
        )
        summary_b = "当前有效规则 B。"
        _, proposal_b = self.add_knowledge_proposal(
            "history-b",
            empty_new_rule=True,
            summary=summary_b,
            target_ref=target_ref,
        )
        official_monitor.apply_knowledge_proposal(proposal_a.id, actor="operator")
        official_monitor.apply_knowledge_proposal(proposal_b.id, actor="operator")
        target_path = self.root / "knowledge-current" / (
            f"{official_monitor.stable_id('knowledge-target', target_ref)}.json"
        )
        payload = official_monitor.load_json(target_path, {})
        payload["current_rule"] = []
        official_monitor.save_json(target_path, payload)

        official_monitor.reconcile_knowledge_operations(self.store)
        repaired = official_monitor.load_json(target_path, {})

        self.assertEqual(repaired["active_proposal_id"], proposal_b.id)
        self.assertEqual(
            repaired["current_rule"],
            official_monitor.confirmed_summary_rules(summary_b),
        )
        self.assertEqual(
            [item["active_proposal_id"] for item in repaired["history"]],
            [proposal_a.id],
        )

    def test_retracted_and_delete_operations_do_not_use_summary_fallback(self) -> None:
        self.add_source()
        retracted_summary = "该摘要不得在撤销后恢复为当前规则。"
        revision, proposal = self.add_knowledge_proposal(
            "retracted-empty",
            empty_new_rule=True,
            summary=retracted_summary,
        )
        official_monitor.apply_knowledge_proposal(proposal.id, actor="operator")
        target_path = self.root / "knowledge-current" / (
            f"{official_monitor.stable_id('knowledge-target', proposal.target_ref)}.json"
        )
        payload = official_monitor.load_json(target_path, {})
        payload["current_rule"] = []
        official_monitor.save_json(target_path, payload)
        self.store.append_policy_change_revision(
            PolicyChangeRevision(
                id="revision-summary-retracted",
                change_id=revision.change_id,
                fact_key=revision.fact_key,
                source_id="source-one",
                status="retracted",
                occurred_at="2026-07-22T12:00:00+00:00",
                headline="撤销空规则",
                summary="撤销。",
            )
        )

        official_monitor.reconcile_knowledge_operations(self.store)
        rolled_back = official_monitor.load_json(target_path, {})
        self.assertEqual(
            self.store.get_knowledge_update_proposal(proposal.id).status,
            "rolled_back",
        )
        self.assertIsNone(rolled_back["active_proposal_id"])
        self.assertNotIn(retracted_summary, rolled_back.get("current_rule", []))

        delete_summary = "删除操作摘要不得成为当前规则。"
        _, delete_proposal = self.add_knowledge_proposal(
            "delete-empty",
            empty_new_rule=True,
            operation="delete",
            summary=delete_summary,
        )
        official_monitor.apply_knowledge_proposal(delete_proposal.id, actor="operator")
        delete_path = self.root / "knowledge-current" / (
            f"{official_monitor.stable_id('knowledge-target', delete_proposal.target_ref)}.json"
        )
        official_monitor.reconcile_knowledge_operations(self.store)
        deleted = official_monitor.load_json(delete_path, {})
        self.assertEqual(deleted["current_rule"], [])
        self.assertNotEqual(deleted.get("rule_origin"), "confirmed_revision_summary")

    def test_pending_knowledge_application_is_reconciled_and_rollback_is_real(self) -> None:
        self.add_source()
        candidate, evidence = self.add_verified_lineage(
            "pending-one",
            fact_key="fact-one",
            new_rule=["运输箱和宠物合计不得超过 8kg"],
        )
        revision = self.store.append_policy_change_revision(
            PolicyChangeRevision(
                id="revision-one",
                change_id="change-one",
                fact_key="fact-one",
                source_id="source-one",
                candidate_id=candidate.id,
                evidence_bundle_id=evidence.id,
                status="confirmed",
                occurred_at=NOW,
                headline="示例航司新增 8kg 限制",
                summary="新规则：运输箱和宠物合计不得超过 8kg。",
            )
        )
        patch_payload = {
            "change_id": revision.change_id,
            "revision_id": revision.id,
            "target_ref": "airlines/test-air.md",
            "old_rule": [],
            "new_rule": ["运输箱和宠物合计不得超过 8kg"],
            "evidence_bundle_id": evidence.id,
        }
        patch_path = self.root / "knowledge-updates" / "revision-one.json"
        patch_path.parent.mkdir(parents=True, exist_ok=True)
        official_monitor.save_json(patch_path, patch_payload)
        proposal = self.store.create_knowledge_update_proposal(
            KnowledgeUpdateProposal(
                id="proposal-one",
                policy_change_revision_id=revision.id,
                target_ref="airlines/test-air.md",
                patch_path="knowledge-updates/revision-one.json",
                patch_sha256=hashlib.sha256(patch_path.read_bytes()).hexdigest(),
                proposed_at=NOW,
                metadata={"evidence_bundle_id": evidence.id},
            )
        )
        proposal = self.store.transition_knowledge_update_proposal(
            proposal.id, "approved", owner="operator"
        )
        official_monitor.record_knowledge_operation(
            proposal, action="apply", status="pending", actor="operator"
        )
        official_monitor.materialize_knowledge_update(proposal)
        recovery = official_monitor.reconcile_knowledge_operations(self.store)
        self.assertEqual(recovery["recovered"], 1)
        applied = self.store.get_knowledge_update_proposal(proposal.id)
        self.assertEqual(applied.status, "applied")
        self.assertEqual(
            official_monitor.materialized_knowledge_inventory()[0]["current_rule"],
            ["运输箱和宠物合计不得超过 8kg"],
        )
        official_monitor.rollback_materialized_knowledge(applied)
        rolled_back = self.store.transition_knowledge_update_proposal(
            applied.id, "rolled_back", owner="operator", reason="test rollback"
        )
        self.assertEqual(rolled_back.status, "rolled_back")
        self.assertIsNone(
            official_monitor.materialized_knowledge_inventory()[0]["active_revision_id"]
        )

    def test_duplicate_apply_is_idempotent_and_retraction_recovery_rolls_back(self) -> None:
        self.add_source()
        candidate, evidence = self.add_verified_lineage(
            "idempotent",
            fact_key="fact-idempotent",
            new_rule=["运输箱不得超过 8 千克"],
        )
        revision = self.store.append_policy_change_revision(
            PolicyChangeRevision(
                id="revision-idempotent",
                change_id="change-idempotent",
                fact_key="fact-idempotent",
                source_id="source-one",
                candidate_id=candidate.id,
                evidence_bundle_id=evidence.id,
                status="confirmed",
                occurred_at=NOW,
                headline="新增运输箱限制",
                summary="新规则：运输箱不得超过 8 千克。",
            )
        )
        patch_path = self.root / "knowledge-updates" / "idempotent.json"
        patch_path.parent.mkdir(parents=True, exist_ok=True)
        official_monitor.save_json(patch_path, {
            "change_id": revision.change_id,
            "revision_id": revision.id,
            "target_ref": "airlines/test-air.md",
            "old_rule": [],
            "new_rule": ["运输箱不得超过 8 千克"],
            "evidence_bundle_id": evidence.id,
        })
        proposal = self.store.create_knowledge_update_proposal(
            KnowledgeUpdateProposal(
                id="proposal-idempotent",
                policy_change_revision_id=revision.id,
                target_ref="airlines/test-air.md",
                patch_path="knowledge-updates/idempotent.json",
                patch_sha256=hashlib.sha256(patch_path.read_bytes()).hexdigest(),
                proposed_at=NOW,
                metadata={"evidence_bundle_id": evidence.id},
            )
        )
        first, _ = official_monitor.apply_knowledge_proposal(proposal.id, actor="operator")
        second, _ = official_monitor.apply_knowledge_proposal(proposal.id, actor="operator")
        self.assertEqual((first.status, second.status), ("applied", "applied"))
        current = official_monitor.load_json(
            self.root / "knowledge-current" / f"{official_monitor.stable_id('knowledge-target', proposal.target_ref)}.json",
            {},
        )
        self.assertEqual(current["history"], [])

        self.store.append_policy_change_revision(
            PolicyChangeRevision(
                id="revision-retracted",
                change_id=revision.change_id,
                fact_key=revision.fact_key,
                source_id="source-one",
                status="retracted",
                occurred_at="2026-07-22T10:00:00+00:00",
                headline="撤销误报",
                summary="证据不足，撤销该变化。",
            )
        )
        recovered = official_monitor.reconcile_knowledge_operations(self.store)
        self.assertGreaterEqual(recovered["recovered"], 1)
        self.assertEqual(
            self.store.get_knowledge_update_proposal(proposal.id).status,
            "rolled_back",
        )
        self.assertIsNone(
            official_monitor.materialized_knowledge_inventory()[0]["active_revision_id"]
        )

    def test_retracted_history_entry_is_pruned_and_cannot_be_restored(self) -> None:
        self.add_source()

        def apply_update(
            label: str,
            change_id: str,
            old_rule: list[str],
            new_rule: list[str],
        ) -> tuple[PolicyChangeRevision, KnowledgeUpdateProposal]:
            candidate, evidence = self.add_verified_lineage(
                f"history-{label}",
                fact_key=f"fact-{label}",
                old_rule=old_rule,
                new_rule=new_rule,
            )
            revision = self.store.append_policy_change_revision(
                PolicyChangeRevision(
                    id=f"revision-{label}",
                    change_id=change_id,
                    fact_key=f"fact-{label}",
                    source_id="source-one",
                    candidate_id=candidate.id,
                    evidence_bundle_id=evidence.id,
                    status="confirmed",
                    occurred_at=NOW,
                    headline=f"规则 {label}",
                    summary=f"规则 {label}",
                )
            )
            relative = Path("knowledge-updates") / f"{label}.json"
            patch_path = self.root / relative
            patch_path.parent.mkdir(parents=True, exist_ok=True)
            official_monitor.save_json(
                patch_path,
                {
                    "change_id": revision.change_id,
                    "revision_id": revision.id,
                    "target_ref": "airlines/test-air.md",
                    "old_rule": old_rule,
                    "new_rule": new_rule,
                    "evidence_bundle_id": evidence.id,
                },
            )
            proposal = self.store.create_knowledge_update_proposal(
                KnowledgeUpdateProposal(
                    id=f"proposal-{label}",
                    policy_change_revision_id=revision.id,
                    target_ref="airlines/test-air.md",
                    patch_path=relative.as_posix(),
                    patch_sha256=hashlib.sha256(patch_path.read_bytes()).hexdigest(),
                    proposed_at=NOW,
                    metadata={"evidence_bundle_id": evidence.id},
                )
            )
            official_monitor.apply_knowledge_proposal(proposal.id, actor="operator")
            return revision, proposal

        revision_a, proposal_a = apply_update("a", "change-a", [], ["规则 A"])
        _, proposal_b = apply_update("b", "change-b", ["规则 A"], ["规则 B"])
        official_monitor.apply_knowledge_proposal(proposal_a.id, actor="operator")
        still_current = official_monitor.materialized_knowledge_inventory()[0]
        self.assertEqual(still_current["active_proposal_id"], proposal_b.id)
        self.assertEqual(still_current["current_rule"], ["规则 B"])
        self.store.append_policy_change_revision(
            PolicyChangeRevision(
                id="revision-a-retracted",
                change_id=revision_a.change_id,
                fact_key=revision_a.fact_key,
                source_id="source-one",
                status="retracted",
                occurred_at="2026-07-22T10:00:00+00:00",
                headline="撤销规则 A",
                summary="规则 A 的证据已失效。",
            )
        )

        recovered = official_monitor.reconcile_knowledge_operations(self.store)
        self.assertGreaterEqual(recovered["recovered"], 1)
        self.assertEqual(
            self.store.get_knowledge_update_proposal(proposal_a.id).status,
            "rolled_back",
        )
        current = official_monitor.materialized_knowledge_inventory()[0]
        self.assertEqual(current["active_proposal_id"], proposal_b.id)
        self.assertEqual(current["current_rule"], ["规则 B"])

        official_monitor.rollback_knowledge_proposal(
            proposal_b.id,
            actor="operator",
            reason="撤销较新的规则 B",
        )
        restored = official_monitor.materialized_knowledge_inventory()[0]
        self.assertIsNone(restored["active_proposal_id"])
        self.assertNotEqual(restored["current_rule"], ["规则 A"])

        revision_c, proposal_c = apply_update("c", "change-c", [], ["规则 C"])
        _, proposal_d = apply_update("d", "change-d", ["规则 C"], ["规则 D"])
        self.store.append_policy_change_revision(
            PolicyChangeRevision(
                id="revision-c-retracted",
                change_id=revision_c.change_id,
                fact_key=revision_c.fact_key,
                source_id="source-one",
                status="retracted",
                occurred_at="2026-07-22T11:00:00+00:00",
                headline="撤销规则 C",
                summary="规则 C 的证据已失效。",
            )
        )
        official_monitor.rollback_knowledge_proposal(
            proposal_d.id,
            actor="operator",
            reason="在恢复前直接撤销较新的规则 D",
        )
        direct_rollback = official_monitor.materialized_knowledge_inventory()[0]
        self.assertEqual(
            self.store.get_knowledge_update_proposal(proposal_c.id).status,
            "rolled_back",
        )
        self.assertIsNone(direct_rollback["active_proposal_id"])
        self.assertNotEqual(direct_rollback["current_rule"], ["规则 C"])

    def test_invalid_inventory_preserves_db_sources_until_authoritative_recovery(self) -> None:
        self.add_source()
        self.store.upsert_source(
            SourceEndpoint(
                id="source-ghost",
                canonical_url="https://ghost.test/pets",
                display_name="Ghost",
                lifecycle_state="active",
                enabled=True,
            )
        )
        inventory_path = self.root / "knowledge_sources.json"
        config_path = self.root / "sources.yaml"
        inventory_path.write_text("{broken-json", encoding="utf-8")
        config_path.write_text("sources: []\n", encoding="utf-8")

        with (
            mock.patch.object(official_monitor, "INVENTORY_PATH", inventory_path),
            mock.patch.object(official_monitor, "CONFIG_PATH", config_path),
        ):
            active, invalid_inventory, _ = official_monitor.load_sources()
            self.assertEqual(
                {item["id"] for item in active},
                {"source-one", "source-ghost"},
            )
            self.assertFalse(invalid_inventory["inventory_health"]["authoritative"])
            self.assertEqual(self.store.get_source("source-one").lifecycle_state, "active")
            self.assertEqual(self.store.get_source("source-ghost").lifecycle_state, "active")
            discovery_sources, _, _ = official_monitor.load_sources(sync_store=False)
            self.assertEqual(
                {item["id"] for item in discovery_sources},
                {"source-one", "source-ghost"},
            )

            official_monitor.save_json(
                inventory_path,
                {
                    "schema_version": 1,
                    "generation_status": "complete",
                    "generated_at": NOW,
                    "unique_sources": 1,
                    "sources": [
                        {
                            "id": "source-one",
                            "name": "示例航司",
                            "url": "https://air.test/pets",
                            "categories": ["airline-policy"],
                            "knowledge_base_refs": ["airlines/test-air.md"],
                        }
                    ],
                },
            )
            active, recovered_inventory, _ = official_monitor.load_sources()

        self.assertTrue(recovered_inventory["inventory_health"]["authoritative"])
        self.assertEqual([item["id"] for item in active], ["source-one"])
        ghost = self.store.get_source("source-ghost")
        self.assertEqual(ghost.lifecycle_state, "retired")
        self.assertFalse(ghost.enabled)

    def test_bootstrap_rolls_back_legacy_pollution_and_rebuilds_from_snapshots(self) -> None:
        self.add_source()
        self.store.set_metadata("legacy_import_completed", {"completed_at": NOW})

        def write_legacy_snapshot(label: str, captured_at: str, text: str) -> str:
            folder = self.root / "snapshots" / "source-one" / label
            folder.mkdir(parents=True)
            content_path = folder / "content.md"
            content_path.write_text(text, encoding="utf-8")
            digest = hashlib.sha256(content_path.read_bytes()).hexdigest()
            official_monitor.save_json(
                folder / "metadata.json",
                {
                    "name": "示例航司",
                    "url": "https://air.test/pets",
                    "source_id": "source-one",
                    "checked_at": captured_at,
                    "status": "ok",
                    "status_code": 200,
                    "content_type": "text/html; charset=utf-8",
                    "sha256": digest,
                    "validation": {"valid": True},
                    "text_truncated": False,
                },
            )
            return digest

        write_legacy_snapshot(
            "20260722T060000+0000",
            "2026-07-22T06:00:00+00:00",
            "Pets may travel in cabin.",
        )
        new_digest = write_legacy_snapshot(
            "20260722T070000+0000",
            "2026-07-22T07:00:00+00:00",
            "Pets must remain in the carrier during flight.\n"
            "The pet carrier must weigh no more than 8 kg.",
        )
        guid = f"content:source-one:{new_digest}"
        candidate_id = stable_id("candidate", guid)
        self.store.upsert_change_candidate(
            ChangeCandidate(
                id=candidate_id,
                source_id="source-one",
                detected_at=NOW,
                state="rejected",
                fact_key=guid,
                resolution_reason="legacy summary retained for audit",
                payload={"legacy_summary_audit_only": True},
            )
        )
        revision = self.store.append_policy_change_revision(
            PolicyChangeRevision(
                id="revision-legacy-pollution",
                change_id=stable_id("change", "fact-legacy-recovery"),
                fact_key="fact-legacy-recovery",
                source_id="source-one",
                status="confirmed",
                occurred_at=NOW,
                headline="新增全程入箱与 8kg 限制",
                summary="宠物须全程留在运输箱内，运输箱不得超过 8kg。",
                metadata={"source_guids": [guid]},
            )
        )
        relative_patch = Path("knowledge-updates") / "legacy-pollution.json"
        patch_path = self.root / relative_patch
        patch_path.parent.mkdir(parents=True)
        official_monitor.save_json(
            patch_path,
            {
                "change_id": revision.change_id,
                "revision_id": revision.id,
                "target_ref": "airlines/test-air.md",
                "old_rule": [],
                "new_rule": ["污染的旧规则"],
            },
        )
        legacy_proposal = self.store.create_knowledge_update_proposal(
            KnowledgeUpdateProposal(
                id="proposal-legacy-pollution",
                policy_change_revision_id=revision.id,
                target_ref="airlines/test-air.md",
                patch_path=relative_patch.as_posix(),
                patch_sha256=hashlib.sha256(patch_path.read_bytes()).hexdigest(),
                proposed_at=NOW,
            )
        )
        legacy_proposal = self.store.transition_knowledge_update_proposal(
            legacy_proposal.id,
            "approved",
            owner="legacy-import",
        )
        self.store.transition_knowledge_update_proposal(
            legacy_proposal.id,
            "applied",
            owner="legacy-import",
        )
        current_path = self.root / "knowledge-current" / (
            f"{stable_id('knowledge-target', legacy_proposal.target_ref)}.json"
        )
        current_path.parent.mkdir(parents=True)
        official_monitor.save_json(
            current_path,
            {
                "target_ref": legacy_proposal.target_ref,
                "active_proposal_id": legacy_proposal.id,
                "active_revision_id": revision.id,
                "current_rule": ["污染的旧规则"],
                "previous_rule": [],
                "history": [],
            },
        )
        official_monitor.save_json(
            self.root / "state.json",
            {
                "source-one": {
                    "name": "示例航司",
                    "url": "https://air.test/pets",
                    "category": "airline-policy",
                    "knowledge_base_refs": ["airlines/test-air.md"],
                }
            },
        )
        official_monitor.save_json(
            official_monitor.POLICY_SUMMARIES_PATH,
            {
                guid: {
                    "headline": "新增全程入箱与 8kg 限制",
                    "summary": "宠物须全程留在运输箱内，运输箱不得超过 8kg。",
                    "impact": "影响客舱携宠旅客。",
                    "action": "出行前确认运输箱。",
                    "policy_change": True,
                    "review_status": "verified",
                    "evidence_rule_version": official_monitor.POLICY_EVIDENCE_RULE_VERSION,
                    "generated_at": NOW,
                }
            },
        )

        bootstrap = official_monitor.bootstrap_monitor_store()

        recovered_candidate = self.store.get_change_candidate(candidate_id)
        self.assertEqual(
            bootstrap["legacy_snapshot_recovery"]["recovered"]
            + bootstrap["legacy_snapshot_recovery"]["existing"],
            2,
            bootstrap["legacy_snapshot_recovery"],
        )
        self.assertEqual(bootstrap["legacy_candidate_recovery"]["recovered"], 1)
        self.assertEqual(bootstrap["evidence_chain_repair"]["retracted"], 1)
        self.assertEqual(recovered_candidate.state, "gathering_evidence")
        self.assertTrue(recovered_candidate.old_snapshot_id)
        self.assertTrue(recovered_candidate.new_snapshot_id)
        self.assertEqual(
            self.store.get_knowledge_update_proposal(legacy_proposal.id).status,
            "rolled_back",
        )
        self.assertIsNone(
            official_monitor.load_json(current_path, {}).get("active_proposal_id")
        )
        self.assertEqual(self.store.list_effective_policy_changes(), [])
        self.assertEqual(
            self.store.list_review_tasks(
                change_candidate_id=candidate_id,
                task_type="change_evidence",
            ),
            [],
        )

        cycle = official_monitor.evidence_agent_once()

        self.assertEqual(
            cycle["confirmed"],
            1,
            (cycle, self.store.get_change_candidate(candidate_id)),
        )
        self.assertEqual(cycle["knowledge_applied"], 1)
        self.assertEqual(self.store.get_change_candidate(candidate_id).state, "confirmed")
        effective = self.store.list_effective_policy_changes()
        self.assertEqual(len(effective), 1)
        rebuilt_revision = self.store.get_policy_change_revision(
            effective[0]["revision_id"]
        )
        self.assertEqual(rebuilt_revision.change_id, revision.change_id)
        self.assertEqual(rebuilt_revision.candidate_id, candidate_id)
        self.assertTrue(rebuilt_revision.evidence_bundle_id)
        rebuilt_bundle = self.store.get_evidence_bundle(
            rebuilt_revision.evidence_bundle_id or ""
        )
        artifact = official_monitor.load_json(
            self.root / rebuilt_bundle.evidence_path,
            {},
        )
        self.assertIn("old", artifact["snapshots"])
        self.assertIn("new", artifact["snapshots"])
        self.assertEqual(
            len(self.store.list_knowledge_update_proposals(statuses=("applied",))),
            1,
        )

    def test_bootstrap_is_one_time_and_source_registry_retires_ghosts(self) -> None:
        official_monitor.save_json(self.root / "state.json", {
            "legacy-source": {
                "name": "Legacy",
                "url": "https://legacy.test/pets",
                "status": "error",
                "checked_at": NOW,
            }
        })
        first = official_monitor.bootstrap_monitor_store()
        self.assertEqual(first["legacy_import"]["sources"], 1)
        self.store.transition_source(
            "legacy-source", "quarantined", reason="manual-required:test", force=True,
        )
        guid = "content:legacy-source:legacy-digest"
        candidate_id = stable_id("candidate", guid)
        self.store.upsert_change_candidate(
            ChangeCandidate(
                id=candidate_id,
                source_id="legacy-source",
                detected_at=NOW,
                state="confirmed",
                fact_key=guid,
            )
        )
        review = self.store.open_review_task(
            ReviewTask(
                id="review-bootstrap-legacy-summary",
                task_type="change_evidence",
                source_id="legacy-source",
                change_candidate_id=candidate_id,
                reason="candidate is missing comparable snapshots",
                created_at=NOW,
                due_at="2026-07-23T08:00:00+00:00",
            )
        )
        official_monitor.save_json(
            official_monitor.POLICY_SUMMARIES_PATH,
            {guid: {"policy_change": True, "review_status": "verified"}},
        )
        second = official_monitor.bootstrap_monitor_store()
        self.assertTrue(second["legacy_import_skipped"])
        self.assertEqual(second["legacy_summary_repair"]["rejected_candidates"], 1)
        self.assertEqual(self.store.get_change_candidate(candidate_id).state, "rejected")
        self.assertEqual(self.store.get_review_task(review.id).status, "cancelled")
        self.assertEqual(self.store.get_source("legacy-source").lifecycle_state, "quarantined")

        self.add_source()
        active, excluded = official_monitor.sync_sources_with_store(
            [{
                "id": "source-one",
                "url": "https://air.test/pets",
                "monitor_role": "current-primary",
            }],
            {},
            retire_absent=True,
        )
        self.assertEqual([item["id"] for item in active], ["source-one"])
        self.assertIn("legacy-source", {item["id"] for item in excluded})
        retired = self.store.get_source("legacy-source")
        self.assertEqual(retired.lifecycle_state, "retired")
        self.assertFalse(retired.enabled)


if __name__ == "__main__":
    unittest.main()
