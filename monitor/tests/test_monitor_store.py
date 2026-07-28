from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from monitor.monitor_store import (
    CHANGE_EVIDENCE_OWNER,
    KNOWLEDGE_OPERATIONS_OWNER,
    SOURCE_RECOVERY_OWNER,
    ChangeCandidate,
    CheckRun,
    ContentSnapshot,
    EvidenceBundle,
    KnowledgeUpdateProposal,
    MonitorStore,
    PolicyChangeRevision,
    ReviewTask,
    SourceEndpoint,
    canonicalize_url,
    stable_id,
    stable_source_id,
)


NOW = "2026-07-22T01:00:00+00:00"
LATER = "2026-07-22T07:00:00+00:00"


class MonitorStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temporary.name) / "monitor.sqlite3"
        self.store = MonitorStore(self.database_path)

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()

    def add_source(
        self,
        source_id: str = "source-one",
        state: str = "validating",
        metadata: dict[str, object] | None = None,
    ) -> SourceEndpoint:
        return self.store.upsert_source(
            SourceEndpoint(
                id=source_id,
                canonical_url="HTTPS://Example.COM:443/pets#details",
                display_name="Example Pets",
                owner_organization_id="organization:example",
                applies_to_entity_ids=("airline:example",),
                role="current-primary",
                lifecycle_state=state,
                next_due_at=NOW,
                metadata={
                    "knowledge_base_refs": ["airlines/example.md"],
                    **(metadata or {}),
                },
            )
        )

    def add_confirmed_revision(self, revision_id: str = "revision-one") -> PolicyChangeRevision:
        return self.store.append_policy_change_revision(
            PolicyChangeRevision(
                id=revision_id,
                change_id="change-one",
                fact_key="pet:cabin:weight-limit",
                source_id="source-one",
                status="confirmed",
                occurred_at=NOW,
                headline="客舱宠物重量限制调整",
                summary="重量限制调整为 8 千克。",
                impact="影响客舱携宠旅客。",
                recommended_action="订票前重新称重。",
            )
        )

    def test_schema_is_idempotent_and_file_database_uses_wal(self) -> None:
        self.store.initialize_schema()
        self.store.initialize_schema()
        tables = {
            row[0]
            for row in self.store.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        self.assertEqual(self.store.journal_mode(), "wal")
        self.assertTrue(
            {
                "source_endpoints",
                "check_runs",
                "content_snapshots",
                "change_candidates",
                "evidence_bundles",
                "policy_change_revisions",
                "review_tasks",
                "knowledge_update_proposals",
                "outbox_events",
            }.issubset(tables)
        )
        self.assertEqual(
            self.store.connection.execute("PRAGMA user_version").fetchone()[0], 3
        )

    def test_list_sources_uses_batched_queries(self) -> None:
        for index in range(3):
            self.add_source(f"source-{index}")
        statements: list[str] = []
        self.store.connection.set_trace_callback(statements.append)

        sources = self.store.list_sources(limit=100)

        self.store.connection.set_trace_callback(None)
        source_queries = [
            statement
            for statement in statements
            if statement.lstrip().upper().startswith("SELECT")
            and "FROM source_endpoints" in statement
        ]
        entity_queries = [
            statement
            for statement in statements
            if statement.lstrip().upper().startswith("SELECT")
            and "FROM source_entity_links" in statement
        ]
        self.assertEqual(len(sources), 3)
        self.assertEqual(len(source_queries), 1)
        self.assertEqual(len(entity_queries), 1)

    def test_v2_check_run_migration_preserves_rows_and_accepts_deferred_status(self) -> None:
        self.add_source(state="active")
        self.store.record_check_run(
            CheckRun(
                id="existing-v2-check",
                source_id="source-one",
                started_at=NOW,
                finished_at=NOW,
                status="success",
                source_lifecycle_after="active",
            )
        )
        self.store.close()

        connection = sqlite3.connect(self.database_path)
        connection.execute("PRAGMA foreign_keys = OFF")
        row = connection.execute(
            "SELECT * FROM check_runs WHERE id='existing-v2-check'"
        ).fetchone()
        columns = [item[1] for item in connection.execute("PRAGMA table_info(check_runs)")]
        connection.execute("DROP INDEX idx_check_source_time")
        connection.execute("DROP TABLE check_runs")
        connection.execute(
            """
            CREATE TABLE check_runs (
                id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL REFERENCES source_endpoints(id) ON DELETE RESTRICT,
                started_at TEXT NOT NULL,
                finished_at TEXT NOT NULL,
                status TEXT NOT NULL CHECK (
                    status IN ('success','not_modified','error','blocked','terminal')
                ),
                http_status INTEGER,
                fetch_strategy TEXT,
                error_category TEXT,
                error_detail TEXT,
                snapshot_id TEXT REFERENCES content_snapshots(id) ON DELETE RESTRICT,
                agent_run_id TEXT,
                next_due_at TEXT,
                source_lifecycle_after TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            )
            """
        )
        placeholders = ",".join("?" for _ in columns)
        connection.execute(
            f"INSERT INTO check_runs ({','.join(columns)}) VALUES ({placeholders})",
            row,
        )
        connection.execute("DELETE FROM schema_migrations WHERE version=3")
        connection.execute("PRAGMA user_version = 2")
        connection.commit()
        connection.close()

        self.store = MonitorStore(self.database_path)
        preserved = self.store.connection.execute(
            "SELECT status, source_id FROM check_runs WHERE id='existing-v2-check'"
        ).fetchone()
        inserted = self.store.record_check_run(
            CheckRun(
                id="new-v3-deferred-check",
                source_id="source-one",
                started_at=LATER,
                finished_at=LATER,
                status="deferred",
                error_category="capacity_budget",
                error_detail="dynamic browser budget exhausted",
                source_lifecycle_after="active",
            )
        )

        self.assertEqual(tuple(preserved), ("success", "source-one"))
        self.assertTrue(inserted)
        self.assertEqual(
            self.store.connection.execute("PRAGMA user_version").fetchone()[0], 3
        )
        self.assertEqual(self.store.connection.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_deferred_check_preserves_failure_count_and_lifecycle_without_outbox(self) -> None:
        self.add_source(
            state="active",
            metadata={
                "revalidation_requested_at": NOW,
                "revalidation_actor": "operator",
                "revalidation_reason": "explicit retry",
            },
        )

        inserted = self.store.record_check_run(
            CheckRun(
                id="capacity-deferred",
                source_id="source-one",
                started_at=NOW,
                finished_at=LATER,
                status="deferred",
                error_category="capacity_budget",
                error_detail="stealth browser budget exhausted",
                next_due_at="2026-07-22T08:00:00+00:00",
                # Defensive contract: a capacity deferral must not be allowed to
                # degrade a source even if an older caller supplied that state.
                source_lifecycle_after="degraded",
                metadata={"agent_failure_kind": "budget"},
            )
        )

        source = self.store.get_source("source-one")
        self.assertTrue(inserted)
        self.assertEqual(source.lifecycle_state, "active")
        self.assertEqual(source.consecutive_failures, 0)
        self.assertEqual(source.metadata["last_deferred_check"]["check_run_id"], "capacity-deferred")
        self.assertEqual(source.metadata["revalidation_requested_at"], NOW)
        self.assertEqual(source.metadata["revalidation_actor"], "operator")
        self.assertEqual(self.store.list_outbox(topic="monitor.sources", limit=20), [])

    def test_missing_legacy_budget_check_is_synthesized_as_auditable_deferral(self) -> None:
        self.add_source(state="degraded")
        report = self.store.reclassify_browser_budget_deferrals({
            "source-one": {
                "status": "error",
                "error": "stealth browser budget exhausted",
                "agent_failure_kind": "budget",
                "checked_at": LATER,
                "last_ok_at": NOW,
                "check_run_id": "missing-legacy-check",
                "check_run_inserted": True,
                "consecutive_failures": 4,
                "lifecycle_state": "degraded",
            }
        })

        run = self.store.connection.execute(
            "SELECT status, error_category, source_lifecycle_after, metadata_json "
            "FROM check_runs WHERE id='missing-legacy-check'"
        ).fetchone()
        source = self.store.get_source("source-one")
        metadata = json.loads(run["metadata_json"])

        self.assertEqual(report["synthesized_deferred_check_runs"], 1)
        self.assertEqual(
            (run["status"], run["error_category"], run["source_lifecycle_after"]),
            ("deferred", "capacity_budget", "active"),
        )
        self.assertEqual(metadata["reclassified_from"], "legacy_state_error")
        self.assertEqual(metadata["legacy_consecutive_failures"], 4)
        self.assertEqual(source.lifecycle_state, "active")
        self.assertEqual(source.consecutive_failures, 0)

    def test_legacy_budget_error_and_review_are_reclassified_and_cancelled(self) -> None:
        self.add_source(state="active")
        self.store.record_check_run(
            CheckRun(
                id="healthy-before-budget",
                source_id="source-one",
                started_at=NOW,
                finished_at=NOW,
                status="success",
                source_lifecycle_after="active",
            )
        )
        self.store.record_check_run(
            CheckRun(
                id="legacy-budget-error",
                source_id="source-one",
                started_at=LATER,
                finished_at=LATER,
                status="error",
                error_category="other",
                error_detail="dynamic browser budget exhausted",
                source_lifecycle_after="degraded",
                metadata={"agent_failure_kind": "budget"},
            )
        )
        self.store.open_review_task(
            ReviewTask(
                id="legacy-budget-review",
                task_type="source_recovery",
                source_id="source-one",
                reason=(
                    "no untried strategy remains after budget: "
                    "dynamic browser budget exhausted"
                ),
                created_at=LATER,
                due_at="2026-07-23T07:00:00+00:00",
                retry_after="2026-07-23T07:00:00+00:00",
                resume_action="revalidate_source",
                metadata={"agent_failure_kind": "budget"},
            )
        )

        report = self.store.reclassify_browser_budget_deferrals()

        source = self.store.get_source("source-one")
        repaired_run = self.store.connection.execute(
            "SELECT status, error_category, source_lifecycle_after "
            "FROM check_runs WHERE id='legacy-budget-error'"
        ).fetchone()
        review = self.store.get_review_task("legacy-budget-review")
        lifecycle_events = [
            event
            for event in self.store.list_outbox(topic="monitor.sources", limit=20)
            if event.event_type == "source.lifecycle_changed"
        ]

        self.assertEqual(report["reclassified_check_runs"], 1)
        self.assertEqual(report["cancelled_budget_reviews"], 1)
        self.assertEqual(tuple(repaired_run), ("deferred", "capacity_budget", "active"))
        self.assertEqual(source.lifecycle_state, "active")
        self.assertEqual(source.consecutive_failures, 0)
        self.assertEqual(review.status, "cancelled")
        self.assertEqual(review.resume_action, "automatic_retry_after_capacity_reset")
        self.assertEqual(lifecycle_events, [])
        self.assertEqual(
            self.store.list_review_actions("legacy-budget-review")[0]["actor"],
            "budget-deferral-migration",
        )

    def test_stable_ids_use_canonical_url(self) -> None:
        expected = "https://example.com/pets?a=1"
        self.assertEqual(
            canonicalize_url("HTTPS://Example.COM:443/pets?a=1#section"), expected
        )
        self.assertEqual(
            stable_source_id("HTTPS://Example.COM:443/pets?a=1#section"),
            stable_source_id(expected),
        )

    def test_outer_transaction_rolls_back_nested_store_operations(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "abort"):
            with self.store.transaction():
                self.add_source()
                raise RuntimeError("abort")
        with self.assertRaises(KeyError):
            self.store.get_source("source-one")

    def test_check_run_atomically_updates_source_and_keeps_snapshot_body_out(self) -> None:
        self.add_source()
        snapshot = ContentSnapshot(
            id="snapshot-one",
            source_id="source-one",
            captured_at=NOW,
            content_sha256="a" * 64,
            raw_path="snapshots/source-one/raw.html",
            normalized_path="snapshots/source-one/normalized.txt",
            mime_type="text/html",
            content_bytes=1234,
            complete=True,
            extractor_version="html-v2",
            metadata={
                "language": "zh-CN",
                "content_sample": "THIS-MUST-NOT-BE-IN-SQLITE",
                "html": "<html>secret</html>",
            },
        )
        run = CheckRun(
            id="check-one",
            source_id="source-one",
            started_at=NOW,
            finished_at="2026-07-22T01:00:05+00:00",
            status="success",
            http_status=200,
            fetch_strategy="static",
            snapshot_id=snapshot.id,
            next_due_at=LATER,
            source_lifecycle_after="baseline_ready",
        )
        self.assertTrue(self.store.record_check_run(run, snapshot))
        self.assertFalse(self.store.record_check_run(run, snapshot))

        source = self.store.get_source("source-one")
        stored_snapshot = self.store.get_snapshot("snapshot-one")
        self.assertEqual(source.lifecycle_state, "baseline_ready")
        self.assertEqual(source.last_good_snapshot_id, "snapshot-one")
        self.assertEqual(source.next_due_at, "2026-07-22T07:00:00.000000+00:00")
        self.assertEqual(source.consecutive_failures, 0)
        self.assertEqual(stored_snapshot.metadata, {"language": "zh-CN"})
        columns = {
            row[1]
            for row in self.store.connection.execute("PRAGMA table_info(content_snapshots)")
        }
        self.assertFalse({"body", "text", "html", "content"} & columns)
        self.store.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.assertNotIn(b"THIS-MUST-NOT-BE-IN-SQLITE", self.database_path.read_bytes())

    def test_complete_snapshot_coverage_groups_sources_and_distinct_versions(self) -> None:
        self.add_source()
        self.store.upsert_source(
            SourceEndpoint(
                id="source-two",
                canonical_url="https://example.org/pets",
                display_name="Example Two",
                role="trusted-secondary",
                lifecycle_state="active",
            )
        )
        snapshots = (
            ("one-a", "source-one", "a" * 64, True),
            ("one-a-repeat", "source-one", "a" * 64, True),
            ("one-incomplete", "source-one", "b" * 64, False),
            ("two-a", "source-two", "a" * 64, True),
            ("two-b", "source-two", "b" * 64, True),
        )
        for snapshot_id, source_id, digest, complete in snapshots:
            self.store.record_snapshot(
                ContentSnapshot(
                    id=snapshot_id,
                    source_id=source_id,
                    captured_at=NOW,
                    content_sha256=digest,
                    normalized_path=f"snapshots/{source_id}/{snapshot_id}.md",
                    complete=complete,
                )
            )

        self.assertEqual(
            self.store.complete_snapshot_coverage({"source-one", "source-two"}),
            {
                "source-one": {"complete_snapshots": 2, "content_versions": 1},
                "source-two": {"complete_snapshots": 2, "content_versions": 2},
            },
        )
        self.assertEqual(
            self.store.complete_snapshot_coverage({"source-two"}),
            {"source-two": {"complete_snapshots": 2, "content_versions": 2}},
        )
        self.assertEqual(self.store.complete_snapshot_coverage(set()), {})

    def test_source_lifecycle_prevents_unforced_retired_reentry(self) -> None:
        self.add_source(state="active")
        retired = self.store.transition_source(
            "source-one", "retired", reason="terminal 404"
        )
        self.assertFalse(retired.enabled)
        self.assertIsNone(retired.next_due_at)
        self.assertEqual(retired.retirement_reason, "terminal 404")
        with self.assertRaisesRegex(ValueError, "invalid source transition"):
            self.store.transition_source("source-one", "validating")
        with self.assertRaisesRegex(ValueError, "cannot be reactivated"):
            self.store.transition_source(
                "source-one", "validating", reason="operator restore", force=True
            )
        restored = self.store.reactivate_source(
            "source-one", actor="operator", reason="official URL restored"
        )
        self.assertTrue(restored.enabled)
        self.assertIsNone(restored.retirement_reason)
        events = self.store.list_outbox(topic="monitor.sources", limit=20)
        self.assertEqual(events[-1].event_type, "source.reactivated")

    def test_candidate_evidence_and_policy_revision_feed(self) -> None:
        self.add_source(state="active")
        candidate = self.store.upsert_change_candidate(
            ChangeCandidate(
                id="candidate-one",
                source_id="source-one",
                detected_at=NOW,
                state="gathering_evidence",
                fact_key="pet:cabin:weight-limit",
                headline="疑似重量限制变化",
                confidence=0.7,
            )
        )
        bundle = EvidenceBundle(
            id="evidence-one",
            candidate_id=candidate.id,
            status="verified",
            rule_version="evidence-v2",
            evidence_path="evidence/candidate-one.json",
            evidence_sha256="b" * 64,
            source_count=2,
            spans=({"snapshot_id": "external", "start": 10, "end": 20},),
            structured_facts={"limit_kg": 8},
            created_at=NOW,
            verified_at=NOW,
        )
        self.store.record_evidence_bundle(bundle)
        self.assertEqual(
            self.store.get_evidence_bundle("evidence-one").structured_facts,
            {"limit_kg": 8},
        )
        first = self.store.append_policy_change_revision(
            PolicyChangeRevision(
                id="revision-one",
                change_id="change-one",
                fact_key="pet:cabin:weight-limit",
                source_id="source-one",
                candidate_id="candidate-one",
                evidence_bundle_id="evidence-one",
                status="confirmed",
                occurred_at=NOW,
                headline="重量限制变更",
                summary="限制为 8 千克。",
            )
        )
        first_cursor = self.store.get_change_cursor()
        effective = self.store.list_effective_policy_changes()
        self.assertEqual(first.status, "confirmed")
        self.assertEqual([item["revision_id"] for item in effective], ["revision-one"])
        self.assertEqual(effective[0]["cursor"], first_cursor)

        second = self.store.append_policy_change_revision(
            PolicyChangeRevision(
                id="revision-two",
                change_id="change-one",
                fact_key="pet:cabin:weight-limit",
                source_id="source-one",
                status="confirmed",
                occurred_at=LATER,
                headline="重量限制变更",
                summary="确认运输箱计入 8 千克。",
            )
        )
        self.assertEqual(second.status, "confirmed")
        self.assertEqual(
            self.store.get_policy_change_revision("revision-one").status, "superseded"
        )
        self.assertEqual(
            [item["revision_id"] for item in self.store.list_effective_policy_changes()],
            ["revision-two"],
        )
        cursor_before_retry = self.store.get_change_cursor()
        self.store.append_policy_change_revision(second)
        self.assertEqual(self.store.get_change_cursor(), cursor_before_retry)

        self.store.append_policy_change_revision(
            PolicyChangeRevision(
                id="revision-three",
                change_id="change-one",
                fact_key="pet:cabin:weight-limit",
                source_id="source-one",
                status="retracted",
                occurred_at="2026-07-22T08:00:00+00:00",
                reason="官方证据不足",
            )
        )
        self.assertEqual(self.store.list_effective_policy_changes(), [])
        feed = self.store.read_change_feed(after_cursor=cursor_before_retry)
        self.assertEqual(len(feed), 1)
        self.assertEqual(feed[0].payload["operation"], "delete")
        self.assertGreater(feed[0].cursor or 0, cursor_before_retry)

    def test_evidence_bundle_id_cannot_be_overwritten(self) -> None:
        self.add_source(state="active")
        candidate = self.store.upsert_change_candidate(
            ChangeCandidate(
                id="candidate-immutable",
                source_id="source-one",
                detected_at=NOW,
                state="gathering_evidence",
            )
        )
        bundle = EvidenceBundle(
            id="evidence-immutable",
            candidate_id=candidate.id,
            status="verified",
            rule_version="1",
            evidence_path="evidence/original.json",
            evidence_sha256="a" * 64,
            structured_facts={"new_rule": ["规则 A"]},
            created_at=NOW,
            verified_at=NOW,
        )
        self.store.record_evidence_bundle(bundle)
        self.store.record_evidence_bundle(bundle)

        with self.assertRaisesRegex(ValueError, "immutable evidence bundle collision"):
            self.store.record_evidence_bundle(
                EvidenceBundle(
                    id=bundle.id,
                    candidate_id=candidate.id,
                    status="verified",
                    rule_version="1",
                    evidence_path="evidence/replaced.json",
                    evidence_sha256="b" * 64,
                    structured_facts={"new_rule": ["规则 B"]},
                    created_at=NOW,
                    verified_at=NOW,
                )
            )
        self.assertEqual(
            self.store.get_evidence_bundle(bundle.id).evidence_path,
            "evidence/original.json",
        )

    def test_review_task_has_owner_sla_actions_and_resume_control(self) -> None:
        self.add_source(state="degraded")
        task = self.store.open_review_task(
            ReviewTask(
                id="review-one",
                task_type="source_recovery",
                source_id="source-one",
                reason="连续出现人机验证",
                priority=90,
                due_at=LATER,
                retry_after=LATER,
                resume_action="revalidate_source",
                created_at=NOW,
            )
        )
        self.assertEqual(task.status, "open")
        self.assertEqual(task.owner, SOURCE_RECOVERY_OWNER)
        with self.assertRaisesRegex(ValueError, "requires an owner"):
            self.store.transition_review_task(
                task.id, "in_progress", actor="operator@example.com"
            )
        assigned = self.store.transition_review_task(
            task.id,
            "assigned",
            actor="supervisor",
            owner="operator@example.com",
            action_payload={"ticket": "OPS-101"},
        )
        self.assertEqual(assigned.owner, "operator@example.com")
        resolved = self.store.transition_review_task(
            task.id,
            "resolved",
            actor="operator@example.com",
            resolution="登录态已恢复并连续验证两次",
            resume_action="resume_source:source-one",
        )
        self.assertEqual(resolved.status, "resolved")
        self.assertEqual(resolved.resume_action, "resume_source:source-one")
        actions = self.store.list_review_actions(task.id)
        self.assertEqual(len(actions), 2)
        self.assertEqual(actions[0]["payload"]["ticket"], "OPS-101")

    def test_source_recovery_task_requires_source_retry_and_resume_controls(self) -> None:
        self.add_source(state="degraded")
        base = {
            "id": "review-contract",
            "task_type": "source_recovery",
            "source_id": "source-one",
            "reason": "source requires recovery",
            "created_at": NOW,
            "due_at": LATER,
            "retry_after": LATER,
            "resume_action": "revalidate_source",
        }
        for field_name, message in (
            ("source_id", "requires a source_id"),
            ("retry_after", "requires a retry_after"),
            ("resume_action", "requires a resume_action"),
        ):
            with self.subTest(field=field_name), self.assertRaisesRegex(ValueError, message):
                self.store.open_review_task(ReviewTask(**{**base, field_name: None}))

        task = self.store.open_review_task(ReviewTask(**base))
        self.assertEqual(task.source_id, "source-one")
        self.assertEqual(task.owner, SOURCE_RECOVERY_OWNER)
        self.assertEqual(task.retry_after, task.due_at)
        self.assertEqual(task.resume_action, "revalidate_source")

    def test_review_contract_reconciliation_archives_legacy_and_repairs_recovery(self) -> None:
        self.add_source(state="degraded")
        agent_candidate = self.store.upsert_change_candidate(
            ChangeCandidate(
                id="candidate-agent-backlog",
                source_id="source-one",
                detected_at=NOW,
                state="gathering_evidence",
            )
        )
        formal_candidate = self.store.upsert_change_candidate(
            ChangeCandidate(
                id="candidate-formal-review",
                source_id="source-one",
                detected_at=NOW,
                state="review_required",
            )
        )
        legacy = self.store.open_review_task(
            ReviewTask(
                id="legacy-agent-record",
                task_type="legacy_agent_blocked",
                reason="domain-level blocked record",
                created_at=NOW,
                due_at=LATER,
                metadata={"site_key": "example.com/pets"},
            )
        )
        recovery = self.store.open_review_task(
            ReviewTask(
                id="source-recovery-record",
                task_type="source_recovery",
                source_id="source-one",
                reason="authentication required",
                created_at=NOW,
                due_at=LATER,
                retry_after=LATER,
                resume_action="revalidate_source",
            )
        )
        with self.store.transaction() as connection:
            connection.execute(
                """
                UPDATE review_tasks
                SET owner=NULL, due_at=NULL, retry_after=NULL, resume_action=NULL
                WHERE id=?
                """,
                (recovery.id,),
            )
            for values in (
                (
                    "legacy-evidence-agent-review",
                    agent_candidate.id,
                    "complete legacy snapshots recovered; evidence agent verification pending",
                    LATER,
                    "enrich_evidence",
                    json.dumps({"legacy_summary_guid": "legacy-guid"}),
                ),
                (
                    "formal-change-evidence-review",
                    formal_candidate.id,
                    "evidence remains insufficient after autonomous enrichment",
                    None,
                    None,
                    "{}",
                ),
                (
                    "deterministic-evidence-rejection",
                    agent_candidate.id,
                    "flight_search_price_noise",
                    LATER,
                    "enrich_evidence",
                    "{}",
                ),
            ):
                connection.execute(
                    """
                    INSERT INTO review_tasks(
                        id, task_type, source_id, change_candidate_id, status,
                        owner, priority, reason, due_at, retry_after, resolution,
                        resume_action, metadata_json, created_at, updated_at, resolved_at
                    ) VALUES (?, 'change_evidence', 'source-one', ?, 'open', NULL,
                              90, ?, ?, NULL, NULL, ?, ?, ?, ?, NULL)
                    """,
                    (*values, NOW, NOW),
                )
            connection.execute(
                """
                INSERT INTO review_tasks(
                    id, task_type, status, owner, priority, reason, due_at,
                    retry_after, resolution, resume_action, metadata_json,
                    created_at, updated_at, resolved_at
                ) VALUES (
                    'knowledge-application-review', 'knowledge_update_application',
                    'open', NULL, 95, 'automatic knowledge application failed',
                    NULL, NULL, NULL, NULL, '{}', ?, ?, NULL
                )
                """,
                (NOW, NOW),
            )

        first = self.store.reconcile_review_task_contracts()
        second = self.store.reconcile_review_task_contracts()

        archived = self.store.get_review_task(legacy.id)
        repaired = self.store.get_review_task(recovery.id)
        agent_review = self.store.get_review_task("legacy-evidence-agent-review")
        deterministic_review = self.store.get_review_task(
            "deterministic-evidence-rejection"
        )
        formal_review = self.store.get_review_task("formal-change-evidence-review")
        knowledge_review = self.store.get_review_task("knowledge-application-review")
        self.assertEqual(first["archived_legacy_agent_records"], 1)
        self.assertEqual(first["cancelled_legacy_evidence_agent_reviews"], 2)
        self.assertEqual(first["backfilled_source_recovery_controls"], 1)
        self.assertEqual(first["backfilled_change_evidence_contracts"], 1)
        self.assertEqual(first["backfilled_knowledge_review_contracts"], 1)
        self.assertEqual(second["archived_legacy_agent_records"], 0)
        self.assertEqual(second["cancelled_legacy_evidence_agent_reviews"], 0)
        self.assertEqual(second["backfilled_source_recovery_controls"], 0)
        self.assertEqual(second["backfilled_change_evidence_contracts"], 0)
        self.assertEqual(second["backfilled_knowledge_review_contracts"], 0)
        self.assertEqual(archived.status, "cancelled")
        self.assertTrue(archived.metadata["audit_only"])
        self.assertEqual(agent_review.status, "cancelled")
        self.assertTrue(agent_review.metadata["audit_only"])
        self.assertTrue(agent_review.metadata["agent_managed"])
        self.assertEqual(deterministic_review.status, "cancelled")
        self.assertTrue(deterministic_review.metadata["audit_only"])
        self.assertTrue(deterministic_review.metadata["agent_managed"])
        self.assertEqual(
            self.store.get_change_candidate(agent_candidate.id).state,
            "gathering_evidence",
        )
        self.assertEqual(
            self.store.get_change_candidate(formal_candidate.id).state,
            "review_required",
        )
        self.assertEqual(repaired.owner, SOURCE_RECOVERY_OWNER)
        self.assertEqual(repaired.retry_after, repaired.due_at)
        self.assertEqual(repaired.resume_action, "revalidate_source")
        self.assertEqual(formal_review.owner, CHANGE_EVIDENCE_OWNER)
        self.assertTrue(formal_review.due_at)
        self.assertEqual(formal_review.resume_action, "enrich_evidence")
        self.assertEqual(knowledge_review.owner, KNOWLEDGE_OPERATIONS_OWNER)
        self.assertTrue(knowledge_review.due_at)
        self.assertEqual(
            knowledge_review.resume_action,
            "retry_knowledge_update_application",
        )
        self.assertEqual(
            {task.id for task in self.store.list_review_tasks()},
            {recovery.id, formal_review.id, knowledge_review.id},
        )
        self.assertEqual(len(self.store.list_review_actions(legacy.id)), 1)
        self.assertEqual(len(self.store.list_review_actions(agent_review.id)), 1)
        self.assertEqual(len(self.store.list_review_actions(deterministic_review.id)), 1)
        self.assertEqual(len(self.store.list_review_actions(recovery.id)), 1)
        self.assertEqual(len(self.store.list_review_actions(formal_review.id)), 1)
        self.assertEqual(len(self.store.list_review_actions(knowledge_review.id)), 1)
        summary = self.store.operational_summary()["review_tasks"]
        self.assertEqual(summary["active"], 3)
        self.assertEqual(summary["total"], 3)
        self.assertEqual(summary["legacy_audit"], 3)

    def test_knowledge_update_proposal_is_versioned_and_reversible(self) -> None:
        self.add_source(state="active")
        self.add_confirmed_revision()
        proposal = self.store.create_knowledge_update_proposal(
            KnowledgeUpdateProposal(
                id="proposal-one",
                policy_change_revision_id="revision-one",
                target_ref="airlines/example.md",
                patch_path="knowledge-patches/proposal-one.diff",
                patch_sha256="c" * 64,
                proposed_at=NOW,
                summary="更新客舱重量限制",
            )
        )
        self.assertEqual(proposal.status, "proposed")
        self.store.transition_knowledge_update_proposal(
            proposal.id, "approved", owner="reviewer@example.com", reason="证据完整"
        )
        self.store.transition_knowledge_update_proposal(
            proposal.id, "applied", owner="publisher"
        )
        rolled_back = self.store.transition_knowledge_update_proposal(
            proposal.id, "rolled_back", owner="publisher", reason="上游撤销"
        )
        self.assertEqual(rolled_back.status, "rolled_back")
        self.assertEqual(rolled_back.patch_path, "knowledge-patches/proposal-one.diff")

    def test_legacy_json_import_is_repeatable_and_does_not_store_snapshot_body(self) -> None:
        inventory = {
            "sources": {
                "source-one": {
                    "id": "source-one",
                    "name": "Example Pets",
                    "url": "https://example.com/pets",
                    "entity_ids": ["airline:example"],
                    "knowledge_base_refs": ["airlines/example.md"],
                }
            }
        }
        registry = {
            "entities": {
                "airline:example": {
                    "id": "airline:example",
                    "current": {
                        "source_id": "source-one",
                        "url": "https://example.com/pets",
                        "snapshot_path": "snapshots/source-one/latest",
                    },
                    "trusted_current_sources": [],
                    "candidates": [],
                }
            }
        }
        state = {
            "source-one": {
                "name": "Example Pets",
                "url": "https://example.com/pets",
                "entity_ids": ["airline:example"],
                "checked_at": NOW,
                "status": "ok",
                "status_code": 200,
                "sha256": "d" * 64,
                "snapshot_path": "snapshots/source-one/latest",
                "content_type": "text/html",
                "content_bytes": 1500,
                "content_sample": "LEGACY-BODY-MUST-NOT-BE-STORED",
            }
        }
        summaries = {
            "content:source-one:digest": {
                "headline": "政策变化",
                "summary": "重量限制变化",
                "policy_change": True,
                "review_status": "verified",
                "evidence_reason": "verified snapshots",
                "generated_at": NOW,
            }
        }
        changes = [
            {
                "guid": "content:source-one:digest",
                "change_key": "fact:example:weight",
                "source_id": "source-one",
                "detected_at": NOW,
                "business": {
                    "headline": "重量限制变化",
                    "summary": "限制更新为 8 千克",
                    "impact": "影响携宠旅客",
                    "action": "出发前称重",
                },
            }
        ]
        events = [
            {
                "guid": "legacy-event-one",
                "title": "旧监控事件",
                "url": "https://example.com/pets",
                "summary": "发现更新",
                "detected_at": NOW,
            }
        ]
        manual_queue = [
            {
                "task_id": "fetch:example.com:one",
                "site_key": "example.com",
                "group_key": "example.com:human_verification",
                "reason": "human verification checkpoint",
                "updated_at": NOW,
                "occurrences": 2,
                "task_ids": ["fetch:example.com:one", "fetch:example.com:two"],
                "attempts": [
                    {
                        "attempt": 1,
                        "strategy": "static",
                        "status": "blocked",
                        "failure_kind": "human_verification",
                    }
                ],
            }
        ]
        first = self.store.import_legacy_documents(
            inventory=inventory,
            source_registry=registry,
            state=state,
            policy_summaries=summaries,
            policy_changes=changes,
            manual_queue=manual_queue,
            events=events,
        )
        counts_before = {
            table: self.store.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "source_endpoints",
                "content_snapshots",
                "change_candidates",
                "policy_change_revisions",
                "review_tasks",
                "outbox_events",
            )
        }
        second = self.store.import_legacy_documents(
            inventory=inventory,
            source_registry=registry,
            state=state,
            policy_summaries=summaries,
            policy_changes=changes,
            manual_queue=manual_queue,
            events=events,
        )
        counts_after = {
            table: self.store.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in counts_before
        }
        self.assertEqual(counts_before, counts_after)
        self.assertEqual(first.candidates, 0)
        self.assertEqual(first.policy_revisions, 1)
        self.assertEqual(first.review_tasks, 1)
        self.assertEqual(second.policy_revisions, 0)
        self.assertEqual(second.review_tasks, 0)
        self.assertEqual(second.outbox_events, 0)
        self.assertEqual(self.store.list_change_candidates(), [])
        source = self.store.get_source("source-one")
        self.assertEqual(source.role, "current-primary")
        self.assertEqual(source.lifecycle_state, "active")
        snapshot_id = stable_id(
            "snapshot", "source-one", "d" * 64, "snapshots/source-one/latest"
        )
        self.assertEqual(source.last_good_snapshot_id, snapshot_id)
        metadata_json = self.store.connection.execute(
            "SELECT metadata_json FROM content_snapshots WHERE id=?", (snapshot_id,)
        ).fetchone()[0]
        self.assertNotIn("content_sample", metadata_json)
        self.store.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.assertNotIn(
            b"LEGACY-BODY-MUST-NOT-BE-STORED", self.database_path.read_bytes()
        )
        self.assertEqual(len(self.store.list_effective_policy_changes()), 1)
        imported_tasks = self.store.list_review_tasks(statuses=("cancelled",))
        self.assertEqual(len(imported_tasks), 1)
        self.assertEqual(imported_tasks[0].status, "cancelled")
        self.assertTrue(imported_tasks[0].metadata["audit_only"])
        self.assertEqual(imported_tasks[0].metadata["occurrences"], 2)

        self.store.transition_source(
            "source-one", "retired", reason="operator retired legacy source"
        )
        self.store.import_legacy_documents(
            inventory=inventory,
            source_registry=registry,
            state=state,
            policy_summaries=summaries,
            policy_changes=changes,
            manual_queue=manual_queue,
            events=events,
        )
        retired = self.store.get_source("source-one")
        self.assertEqual(retired.lifecycle_state, "retired")
        self.assertFalse(retired.enabled)
        self.assertEqual(retired.last_good_snapshot_id, snapshot_id)

    def test_legacy_summary_candidate_repair_keeps_revision_audit_only(self) -> None:
        self.add_source(state="active")
        guid = "content:source-one:legacy-digest"
        candidate_id = stable_id("candidate", guid)
        self.store.upsert_change_candidate(
            ChangeCandidate(
                id=candidate_id,
                source_id="source-one",
                detected_at=NOW,
                state="confirmed",
                fact_key=guid,
                headline="旧摘要变化",
                payload={"summary": "历史摘要"},
            )
        )
        unrelated = self.store.upsert_change_candidate(
            ChangeCandidate(
                id="candidate-runtime",
                source_id="source-one",
                detected_at=LATER,
                state="gathering_evidence",
                fact_key="runtime-fact",
            )
        )
        revision = self.add_confirmed_revision("revision-legacy-audit")
        task = self.store.open_review_task(
            ReviewTask(
                id="review-legacy-summary",
                task_type="change_evidence",
                source_id="source-one",
                change_candidate_id=candidate_id,
                reason="candidate is missing comparable snapshots",
                created_at=NOW,
                due_at=LATER,
            )
        )

        repaired = self.store.reject_legacy_summary_candidates(
            {guid: {"policy_change": True, "review_status": "verified"}}
        )

        self.assertEqual(
            repaired,
            {
                "matched_candidates": 1,
                "rejected_candidates": 1,
                "cancelled_reviews": 1,
            },
        )
        candidate = self.store.get_change_candidate(candidate_id)
        self.assertEqual(candidate.state, "rejected")
        self.assertIn("audit", candidate.resolution_reason or "")
        self.assertTrue(candidate.payload["legacy_summary_audit_only"])
        self.assertEqual(candidate.payload["legacy_summary_guid"], guid)
        self.assertEqual(self.store.get_change_candidate(unrelated.id).state, "gathering_evidence")
        cancelled = self.store.get_review_task(task.id)
        self.assertEqual(cancelled.status, "cancelled")
        self.assertEqual(cancelled.resume_action, "audit_only")
        self.assertEqual(
            self.store.list_review_actions(task.id)[-1]["actor"],
            "legacy-summary-migration",
        )
        self.assertEqual(self.store.get_policy_change_revision(revision.id).status, "confirmed")

        repeated = self.store.reject_legacy_summary_candidates({guid: {}})
        self.assertEqual(repeated["matched_candidates"], 1)
        self.assertEqual(repeated["rejected_candidates"], 0)
        self.assertEqual(repeated["cancelled_reviews"], 0)

    def test_legacy_import_without_marker_cannot_downgrade_runtime_aggregates(self) -> None:
        """Stale JSON must be harmless when a real database predates the marker."""
        self.assertIsNone(self.store.get_metadata("legacy_import_completed"))
        self.add_source(state="active")

        snapshot_path = "snapshots/source-one/legacy"
        snapshot_sha256 = "e" * 64
        snapshot_id = stable_id(
            "snapshot", "source-one", snapshot_sha256, snapshot_path
        )
        self.store.record_snapshot(
            ContentSnapshot(
                id=snapshot_id,
                source_id="source-one",
                captured_at=LATER,
                content_sha256=snapshot_sha256,
                raw_path="snapshots/source-one/runtime.raw.gz",
                normalized_path="snapshots/source-one/runtime.md",
                mime_type="text/html; charset=utf-8",
                content_bytes=4096,
                complete=True,
                extractor_version="runtime-v3",
                metadata={"capture_method": "runtime-agent", "status_code": 200},
            )
        )

        guid = "content:source-one:runtime-candidate"
        candidate_id = stable_id("candidate", guid)
        self.store.upsert_change_candidate(
            ChangeCandidate(
                id=candidate_id,
                source_id="source-one",
                detected_at=LATER,
                state="rejected",
                fact_key=guid,
                headline="运行态已判定为误报",
                confidence=0.99,
                resolution_reason="人工确认页面导航噪声",
                payload={"reviewed_by": "operator", "runtime_revision": 3},
            )
        )

        fact_key = "fact:runtime:pet-policy"
        change_id = stable_id("change", fact_key)
        self.store.append_policy_change_revision(
            PolicyChangeRevision(
                id="runtime-revision",
                change_id=change_id,
                fact_key=fact_key,
                source_id="source-one",
                status="confirmed",
                occurred_at=LATER,
                headline="运行态确认变化",
                summary="该修订来自结构化运行态。",
            )
        )

        group_key = "example.com:human_verification"
        task_id = stable_id("review", "legacy-agent-blocked", group_key)
        self.store.open_review_task(
            ReviewTask(
                id=task_id,
                task_type="source_recovery",
                source_id="source-one",
                owner="operator@example.com",
                reason="运行态复核任务",
                priority=95,
                due_at=LATER,
                retry_after=LATER,
                resume_action="revalidate_source",
                created_at=NOW,
                metadata={"ticket": "OPS-900"},
            )
        )
        self.store.transition_review_task(
            task_id,
            "resolved",
            actor="operator@example.com",
            resolution="已恢复并完成连续验证",
            resume_action="resume_source:source-one",
        )

        legacy_state = {
            "source-one": {
                "name": "过期来源名称",
                "url": "https://example.com/pets",
                "checked_at": NOW,
                "status": "blocked",
                "status_code": 403,
                "sha256": snapshot_sha256,
                "snapshot_path": snapshot_path,
                "raw_path": "snapshots/source-one/legacy.raw.gz",
                "normalized_path": "snapshots/source-one/legacy.md",
                "content_type": "application/octet-stream",
                "content_bytes": 12,
            }
        }
        legacy_summaries = {
            guid: {
                "headline": "过期文件声称这是已确认变化",
                "summary": "过期摘要",
                "policy_change": True,
                "review_status": "verified",
                "evidence_reason": "legacy evidence",
                "generated_at": NOW,
            }
        }
        legacy_changes = [
            {
                "guid": "legacy-runtime-policy",
                "change_key": fact_key,
                "source_id": "source-one",
                "detected_at": NOW,
                "business": {
                    "headline": "过期变化",
                    "summary": "不得覆盖或 supersede 运行态修订",
                },
            }
        ]
        legacy_queue = [
            {
                "task_id": "legacy-task",
                "group_key": group_key,
                "reason": "过期阻断记录",
                "updated_at": NOW,
                "occurrences": 99,
            }
        ]

        legacy_directory = Path(self.temporary.name) / "legacy-json"
        (legacy_directory / "scraping-agent").mkdir(parents=True)
        legacy_documents = {
            "inventory.json": {
                "sources": {
                    "source-one": {
                        "id": "source-one",
                        "name": "过期来源名称",
                        "url": "https://example.com/pets",
                    }
                }
            },
            "state.json": legacy_state,
            "policy-summaries.json": legacy_summaries,
            "policy-changes.json": legacy_changes,
            "scraping-agent/manual-queue.json": legacy_queue,
        }
        for relative_path, document in legacy_documents.items():
            (legacy_directory / relative_path).write_text(
                json.dumps(document, ensure_ascii=False), encoding="utf-8"
            )

        report = self.store.import_legacy_directory(legacy_directory)

        self.assertEqual(report.sources, 0)
        self.assertEqual(report.snapshots, 0)
        self.assertEqual(report.candidates, 0)
        self.assertEqual(report.policy_revisions, 0)
        self.assertEqual(report.review_tasks, 0)

        source = self.store.get_source("source-one")
        self.assertEqual(source.lifecycle_state, "active")
        self.assertTrue(source.enabled)
        self.assertEqual(source.display_name, "Example Pets")
        self.assertEqual(source.role, "current-primary")

        snapshot = self.store.get_snapshot(snapshot_id)
        self.assertTrue(snapshot.complete)
        self.assertEqual(snapshot.raw_path, "snapshots/source-one/runtime.raw.gz")
        self.assertEqual(snapshot.normalized_path, "snapshots/source-one/runtime.md")
        self.assertEqual(snapshot.mime_type, "text/html; charset=utf-8")
        self.assertEqual(snapshot.content_bytes, 4096)
        self.assertEqual(snapshot.extractor_version, "runtime-v3")
        self.assertEqual(snapshot.metadata["capture_method"], "runtime-agent")

        candidate = self.store.get_change_candidate(candidate_id)
        self.assertEqual(candidate.state, "rejected")
        self.assertEqual(candidate.headline, "运行态已判定为误报")
        self.assertEqual(candidate.resolution_reason, "人工确认页面导航噪声")
        self.assertEqual(candidate.payload["runtime_revision"], 3)

        revision = self.store.get_policy_change_revision("runtime-revision")
        self.assertEqual(revision.status, "confirmed")
        with self.assertRaises(KeyError):
            self.store.get_policy_change_revision(
                stable_id("revision", "legacy", fact_key)
            )

        task = self.store.get_review_task(task_id)
        self.assertEqual(task.status, "resolved")
        self.assertEqual(task.owner, "operator@example.com")
        self.assertEqual(task.reason, "运行态复核任务")
        self.assertEqual(task.resolution, "已恢复并完成连续验证")
        self.assertEqual(task.resume_action, "resume_source:source-one")
        self.assertEqual(task.metadata, {"ticket": "OPS-900"})


if __name__ == "__main__":
    unittest.main()
