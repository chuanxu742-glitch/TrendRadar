from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence
from urllib.parse import urlsplit, urlunsplit


SCHEMA_VERSION = 3

SOURCE_ROLES = {
    "current-primary",
    "trusted-secondary",
    "candidate",
    "reference",
    "historical",
}
SOURCE_STATES = {
    "discovered",
    "validating",
    "baseline_ready",
    "active",
    "degraded",
    "recovering",
    "quarantined",
    "retired",
}
CHECK_STATUSES = {"success", "not_modified", "error", "blocked", "terminal", "deferred"}
CHANGE_CANDIDATE_STATES = {
    "suspected",
    "gathering_evidence",
    "confirmed",
    "rejected",
    "review_required",
}
EVIDENCE_STATES = {"pending", "verified", "insufficient", "rejected"}
REVISION_STATES = {"draft", "confirmed", "retracted", "superseded"}
REVIEW_STATES = {"open", "assigned", "in_progress", "resolved", "cancelled"}
PROPOSAL_STATES = {"proposed", "approved", "applied", "rejected", "rolled_back"}
SOURCE_RECOVERY_OWNER = "source-operations"
CHANGE_EVIDENCE_OWNER = "policy-review"
KNOWLEDGE_OPERATIONS_OWNER = "knowledge-operations"
REVIEW_TASK_OWNER_BY_TYPE = {
    "source_recovery": SOURCE_RECOVERY_OWNER,
    "change_evidence": CHANGE_EVIDENCE_OWNER,
    "evidence_chain_recovery": CHANGE_EVIDENCE_OWNER,
    "knowledge_update_application": KNOWLEDGE_OPERATIONS_OWNER,
    "knowledge_operation": KNOWLEDGE_OPERATIONS_OWNER,
    "knowledge_rollback": KNOWLEDGE_OPERATIONS_OWNER,
}
REVIEW_TASK_RESUME_ACTION_BY_TYPE = {
    "source_recovery": "revalidate_source",
    "change_evidence": "enrich_evidence",
    "evidence_chain_recovery": "rebuild_evidence_chain",
    "knowledge_update_application": "retry_knowledge_update_application",
    "knowledge_operation": "reconcile_knowledge_operation",
    "knowledge_rollback": "rollback_knowledge_update",
}
EVIDENCE_AGENT_REJECTION_REASONS = {
    "candidate_already_present_in_both_snapshots",
    "full_snapshot_diff_has_no_factual_policy_rule",
    "changed_fact_lacks_pet_policy_context",
    "generic_form_validation_noise",
    "flight_search_price_noise",
    "promotional_content_noise",
    "candidate_headline_identifies_non_policy_page_change",
    "source_not_yet_authoritative_for_policy_change",
    "third_party_source_requires_corroboration",
}

_ROLE_RANK = {
    "historical": 0,
    "reference": 1,
    "candidate": 2,
    "trusted-secondary": 3,
    "current-primary": 4,
}
_SOURCE_TRANSITIONS = {
    "discovered": {"validating", "quarantined", "retired"},
    "validating": {"baseline_ready", "degraded", "quarantined", "retired"},
    "baseline_ready": {"active", "degraded", "quarantined", "retired"},
    "active": {"degraded", "recovering", "quarantined", "retired"},
    "degraded": {"recovering", "active", "quarantined", "retired"},
    "recovering": {"active", "degraded", "quarantined", "retired"},
    "quarantined": {"validating", "retired"},
    "retired": set(),
}
_REVIEW_TRANSITIONS = {
    "open": {"assigned", "in_progress", "resolved", "cancelled"},
    "assigned": {"open", "in_progress", "resolved", "cancelled"},
    "in_progress": {"open", "resolved", "cancelled"},
    "resolved": {"open"},
    "cancelled": {"open"},
}
_PROPOSAL_TRANSITIONS = {
    "proposed": {"approved", "rejected"},
    "approved": {"applied", "rejected"},
    "applied": {"rolled_back"},
    "rejected": {"proposed"},
    "rolled_back": {"proposed"},
}
_SNAPSHOT_METADATA_KEYS = {
    "canonical_url",
    "capture_method",
    "encoding",
    "etag",
    "language",
    "last_modified",
    "source_url",
    "status_code",
}


def is_browser_budget_exhaustion(failure_kind: Any = "", detail: Any = "") -> bool:
    kind = str(failure_kind or "").strip().lower()
    text = str(detail or "").strip().lower()
    return (
        "browser budget exhausted" in text
        or "browser capacity budget exhausted" in text
        or (
        kind == "budget"
        and (
            "browser" in text
            or "dynamic" in text
            or "stealth" in text
            or "capacity budget" in text
            or not text
        )
        )
    )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def normalize_timestamp(value: str | datetime | None) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")


def canonicalize_url(url: str) -> str:
    value = url.strip()
    if not value:
        raise ValueError("source URL must not be empty")
    parsed = urlsplit(value)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        raise ValueError(f"unsupported source URL: {url!r}")
    host = parsed.hostname.lower()
    port = parsed.port
    if port and not ((parsed.scheme.lower() == "http" and port == 80) or (parsed.scheme.lower() == "https" and port == 443)):
        host = f"{host}:{port}"
    if parsed.username:
        auth = parsed.username
        if parsed.password:
            auth += f":{parsed.password}"
        host = f"{auth}@{host}"
    path = parsed.path or "/"
    return urlunsplit((parsed.scheme.lower(), host, path, parsed.query, ""))


def stable_id(kind: str, *parts: Any) -> str:
    material = json.dumps(parts, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]
    return f"{kind}-{digest}"


def stable_source_id(url: str) -> str:
    return stable_id("source", canonicalize_url(url))


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _mapping(value: Mapping[str, Any] | None) -> dict[str, Any]:
    return dict(value or {})


def _snapshot_metadata(value: Mapping[str, Any] | None) -> dict[str, Any]:
    source = _mapping(value)
    return {key: source[key] for key in _SNAPSHOT_METADATA_KEYS if key in source}


@dataclass(frozen=True, slots=True)
class SourceEndpoint:
    id: str
    canonical_url: str
    display_name: str = ""
    owner_organization_id: str | None = None
    applies_to_entity_ids: tuple[str, ...] = ()
    role: str = "candidate"
    lifecycle_state: str = "discovered"
    enabled: bool = True
    next_due_at: str | None = None
    last_checked_at: str | None = None
    last_good_snapshot_id: str | None = None
    consecutive_failures: int = 0
    retirement_reason: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ContentSnapshot:
    id: str
    source_id: str
    captured_at: str
    content_sha256: str
    raw_path: str | None = None
    normalized_path: str | None = None
    mime_type: str | None = None
    content_bytes: int | None = None
    complete: bool = True
    extractor_version: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CheckRun:
    id: str
    source_id: str
    started_at: str
    finished_at: str
    status: str
    http_status: int | None = None
    fetch_strategy: str | None = None
    error_category: str | None = None
    error_detail: str | None = None
    snapshot_id: str | None = None
    agent_run_id: str | None = None
    next_due_at: str | None = None
    source_lifecycle_after: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ChangeCandidate:
    id: str
    source_id: str
    detected_at: str
    state: str = "suspected"
    old_snapshot_id: str | None = None
    new_snapshot_id: str | None = None
    fact_key: str | None = None
    headline: str = ""
    confidence: float | None = None
    resolution_reason: str | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EvidenceBundle:
    id: str
    candidate_id: str
    status: str
    rule_version: str
    evidence_path: str
    evidence_sha256: str
    old_snapshot_id: str | None = None
    new_snapshot_id: str | None = None
    source_count: int = 1
    spans: Sequence[Mapping[str, Any]] = ()
    structured_facts: Mapping[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=utc_now)
    verified_at: str | None = None


@dataclass(frozen=True, slots=True)
class PolicyChangeRevision:
    change_id: str
    fact_key: str
    status: str
    occurred_at: str
    id: str = ""
    revision_no: int = 0
    supersedes_revision_id: str | None = None
    outbox_cursor: int | None = None
    source_id: str | None = None
    candidate_id: str | None = None
    evidence_bundle_id: str | None = None
    headline: str = ""
    summary: str = ""
    impact: str = ""
    recommended_action: str = ""
    reason: str | None = None
    published_at: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ReviewTask:
    id: str
    task_type: str
    reason: str
    created_at: str
    status: str = "open"
    source_id: str | None = None
    change_candidate_id: str | None = None
    owner: str | None = None
    priority: int = 50
    due_at: str | None = None
    retry_after: str | None = None
    resolution: str | None = None
    resume_action: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class KnowledgeUpdateProposal:
    id: str
    policy_change_revision_id: str
    target_ref: str
    patch_path: str
    patch_sha256: str
    proposed_at: str
    status: str = "proposed"
    summary: str = ""
    owner: str | None = None
    decided_at: str | None = None
    applied_at: str | None = None
    decision_reason: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class OutboxEvent:
    id: str
    topic: str
    aggregate_type: str
    aggregate_id: str
    event_type: str
    occurred_at: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    cursor: int | None = None
    published_at: str | None = None
    attempts: int = 0
    last_error: str | None = None


@dataclass(frozen=True, slots=True)
class LegacyImportReport:
    sources: int = 0
    snapshots: int = 0
    candidates: int = 0
    policy_revisions: int = 0
    review_tasks: int = 0
    outbox_events: int = 0


class MonitorStore:
    """Transactional source of truth for the official monitoring workflow."""

    def __init__(self, database_path: str | Path) -> None:
        self.path = Path(database_path)
        if str(database_path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._local = threading.local()
        self.connection = sqlite3.connect(str(database_path), timeout=30, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 30000")
        self.connection.execute("PRAGMA synchronous = NORMAL")
        if str(database_path) != ":memory:":
            self.connection.execute("PRAGMA journal_mode = WAL")
        self.initialize_schema()

    def __enter__(self) -> MonitorStore:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            self.connection.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            depth = getattr(self._local, "transaction_depth", 0)
            self._local.transaction_depth = depth + 1
            if depth == 0:
                self.connection.execute("BEGIN IMMEDIATE")
            try:
                yield self.connection
            except BaseException:
                if depth == 0:
                    self.connection.rollback()
                raise
            else:
                if depth == 0:
                    self.connection.commit()
            finally:
                self._local.transaction_depth = depth

    def initialize_schema(self) -> None:
        with self._lock:
            self.connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS monitor_metadata (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS source_endpoints (
                    id TEXT PRIMARY KEY,
                    canonical_url TEXT NOT NULL,
                    display_name TEXT NOT NULL DEFAULT '',
                    owner_organization_id TEXT,
                    role TEXT NOT NULL CHECK (role IN ('current-primary','trusted-secondary','candidate','reference','historical')),
                    lifecycle_state TEXT NOT NULL CHECK (lifecycle_state IN ('discovered','validating','baseline_ready','active','degraded','recovering','quarantined','retired')),
                    enabled INTEGER NOT NULL CHECK (enabled IN (0,1)),
                    next_due_at TEXT,
                    last_checked_at TEXT,
                    last_good_snapshot_id TEXT,
                    consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
                    retirement_reason TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(last_good_snapshot_id) REFERENCES content_snapshots(id)
                        DEFERRABLE INITIALLY DEFERRED
                );
                CREATE INDEX IF NOT EXISTS idx_source_due ON source_endpoints(enabled, next_due_at);
                CREATE INDEX IF NOT EXISTS idx_source_url ON source_endpoints(canonical_url);
                CREATE INDEX IF NOT EXISTS idx_source_lifecycle ON source_endpoints(lifecycle_state, role);

                CREATE TABLE IF NOT EXISTS source_entity_links (
                    source_id TEXT NOT NULL REFERENCES source_endpoints(id) ON DELETE CASCADE,
                    entity_id TEXT NOT NULL,
                    relation TEXT NOT NULL DEFAULT 'applies_to',
                    PRIMARY KEY (source_id, entity_id, relation)
                );

                CREATE TABLE IF NOT EXISTS content_snapshots (
                    id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL REFERENCES source_endpoints(id) ON DELETE RESTRICT,
                    captured_at TEXT NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    raw_path TEXT,
                    normalized_path TEXT,
                    mime_type TEXT,
                    content_bytes INTEGER,
                    complete INTEGER NOT NULL CHECK (complete IN (0,1)),
                    extractor_version TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    UNIQUE(source_id, content_sha256, raw_path, normalized_path)
                );
                CREATE INDEX IF NOT EXISTS idx_snapshot_source_time ON content_snapshots(source_id, captured_at DESC);

                CREATE TABLE IF NOT EXISTS check_runs (
                    id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL REFERENCES source_endpoints(id) ON DELETE RESTRICT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('success','not_modified','error','blocked','terminal','deferred')),
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
                );
                CREATE INDEX IF NOT EXISTS idx_check_source_time ON check_runs(source_id, finished_at DESC);

                CREATE TABLE IF NOT EXISTS change_candidates (
                    id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL REFERENCES source_endpoints(id) ON DELETE RESTRICT,
                    old_snapshot_id TEXT REFERENCES content_snapshots(id) ON DELETE RESTRICT,
                    new_snapshot_id TEXT REFERENCES content_snapshots(id) ON DELETE RESTRICT,
                    fact_key TEXT,
                    detected_at TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('suspected','gathering_evidence','confirmed','rejected','review_required')),
                    headline TEXT NOT NULL DEFAULT '',
                    confidence REAL,
                    resolution_reason TEXT,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_candidate_state_time ON change_candidates(state, detected_at);

                CREATE TABLE IF NOT EXISTS evidence_bundles (
                    id TEXT PRIMARY KEY,
                    candidate_id TEXT NOT NULL REFERENCES change_candidates(id) ON DELETE RESTRICT,
                    status TEXT NOT NULL CHECK (status IN ('pending','verified','insufficient','rejected')),
                    rule_version TEXT NOT NULL,
                    evidence_path TEXT NOT NULL,
                    evidence_sha256 TEXT NOT NULL,
                    old_snapshot_id TEXT REFERENCES content_snapshots(id) ON DELETE RESTRICT,
                    new_snapshot_id TEXT REFERENCES content_snapshots(id) ON DELETE RESTRICT,
                    source_count INTEGER NOT NULL DEFAULT 1 CHECK (source_count >= 1),
                    spans_json TEXT NOT NULL DEFAULT '[]',
                    structured_facts_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    verified_at TEXT
                );

                CREATE TABLE IF NOT EXISTS policy_changes (
                    id TEXT PRIMARY KEY,
                    fact_key TEXT NOT NULL UNIQUE,
                    source_id TEXT REFERENCES source_endpoints(id) ON DELETE RESTRICT,
                    candidate_id TEXT REFERENCES change_candidates(id) ON DELETE RESTRICT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS policy_change_revisions (
                    id TEXT PRIMARY KEY,
                    change_id TEXT NOT NULL REFERENCES policy_changes(id) ON DELETE RESTRICT,
                    revision_no INTEGER NOT NULL CHECK (revision_no > 0),
                    status TEXT NOT NULL CHECK (status IN ('draft','confirmed','retracted','superseded')),
                    evidence_bundle_id TEXT REFERENCES evidence_bundles(id) ON DELETE RESTRICT,
                    headline TEXT NOT NULL DEFAULT '',
                    summary TEXT NOT NULL DEFAULT '',
                    impact TEXT NOT NULL DEFAULT '',
                    recommended_action TEXT NOT NULL DEFAULT '',
                    reason TEXT,
                    supersedes_revision_id TEXT REFERENCES policy_change_revisions(id) ON DELETE RESTRICT,
                    superseded_by_revision_id TEXT REFERENCES policy_change_revisions(id) ON DELETE RESTRICT,
                    occurred_at TEXT NOT NULL,
                    published_at TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    outbox_cursor INTEGER,
                    created_at TEXT NOT NULL,
                    UNIQUE(change_id, revision_no)
                );
                CREATE INDEX IF NOT EXISTS idx_revision_effective ON policy_change_revisions(status, occurred_at DESC);

                CREATE TABLE IF NOT EXISTS review_tasks (
                    id TEXT PRIMARY KEY,
                    task_type TEXT NOT NULL,
                    source_id TEXT REFERENCES source_endpoints(id) ON DELETE RESTRICT,
                    change_candidate_id TEXT REFERENCES change_candidates(id) ON DELETE RESTRICT,
                    status TEXT NOT NULL CHECK (status IN ('open','assigned','in_progress','resolved','cancelled')),
                    owner TEXT,
                    priority INTEGER NOT NULL CHECK (priority BETWEEN 0 AND 100),
                    reason TEXT NOT NULL,
                    due_at TEXT,
                    retry_after TEXT,
                    resolution TEXT,
                    resume_action TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    resolved_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_review_queue ON review_tasks(status, priority DESC, due_at);

                CREATE TABLE IF NOT EXISTS review_task_actions (
                    id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL REFERENCES review_tasks(id) ON DELETE CASCADE,
                    action_type TEXT NOT NULL,
                    actor TEXT,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_review_actions ON review_task_actions(task_id, created_at);

                CREATE TABLE IF NOT EXISTS knowledge_update_proposals (
                    id TEXT PRIMARY KEY,
                    policy_change_revision_id TEXT NOT NULL REFERENCES policy_change_revisions(id) ON DELETE RESTRICT,
                    target_ref TEXT NOT NULL,
                    patch_path TEXT NOT NULL,
                    patch_sha256 TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('proposed','approved','applied','rejected','rolled_back')),
                    summary TEXT NOT NULL DEFAULT '',
                    owner TEXT,
                    proposed_at TEXT NOT NULL,
                    decided_at TEXT,
                    applied_at TEXT,
                    decision_reason TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_knowledge_proposal_state ON knowledge_update_proposals(status, proposed_at);

                CREATE TABLE IF NOT EXISTS outbox_events (
                    cursor INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT NOT NULL UNIQUE,
                    topic TEXT NOT NULL,
                    aggregate_type TEXT NOT NULL,
                    aggregate_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    occurred_at TEXT NOT NULL,
                    published_at TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
                    last_error TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox_events(published_at, cursor);
                CREATE INDEX IF NOT EXISTS idx_outbox_topic_cursor ON outbox_events(topic, cursor);
                """
            )
            self._migrate_check_runs_for_deferred_status()
            self.connection.execute(
                "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                (SCHEMA_VERSION, utc_now()),
            )
            self.connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            self.connection.commit()

    def _migrate_check_runs_for_deferred_status(self) -> None:
        row = self.connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='check_runs'"
        ).fetchone()
        if row is None or "'deferred'" in str(row[0] or ""):
            return

        columns = (
            "id, source_id, started_at, finished_at, status, http_status, "
            "fetch_strategy, error_category, error_detail, snapshot_id, "
            "agent_run_id, next_due_at, source_lifecycle_after, metadata_json, created_at"
        )
        self.connection.commit()
        self.connection.execute("PRAGMA foreign_keys = OFF")
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            self.connection.execute(
                """
                CREATE TABLE check_runs_v3 (
                    id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL REFERENCES source_endpoints(id) ON DELETE RESTRICT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (
                        status IN ('success','not_modified','error','blocked','terminal','deferred')
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
            self.connection.execute(
                f"INSERT INTO check_runs_v3 ({columns}) SELECT {columns} FROM check_runs"
            )
            self.connection.execute("DROP TABLE check_runs")
            self.connection.execute("ALTER TABLE check_runs_v3 RENAME TO check_runs")
            self.connection.execute(
                "CREATE INDEX idx_check_source_time ON check_runs(source_id, finished_at DESC)"
            )
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise
        finally:
            self.connection.execute("PRAGMA foreign_keys = ON")
        violations = self.connection.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise RuntimeError(f"check_runs migration introduced foreign key violations: {violations}")

    def journal_mode(self) -> str:
        with self._lock:
            row = self.connection.execute("PRAGMA journal_mode").fetchone()
        return str(row[0]).lower()

    def get_metadata(self, key: str, default: Any = None) -> Any:
        with self._lock:
            row = self.connection.execute(
                "SELECT value_json FROM monitor_metadata WHERE key=?",
                (key,),
            ).fetchone()
        if row is None:
            return default
        try:
            return json.loads(row[0])
        except json.JSONDecodeError:
            return default

    def set_metadata(self, key: str, value: Any) -> None:
        with self.transaction() as connection:
            connection.execute("""
                INSERT INTO monitor_metadata(key, value_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json=excluded.value_json,
                    updated_at=excluded.updated_at
            """, (key, _json(value), utc_now()))

    def upsert_source(self, endpoint: SourceEndpoint, *, allow_reactivate: bool = False) -> SourceEndpoint:
        if endpoint.role not in SOURCE_ROLES:
            raise ValueError(f"invalid source role: {endpoint.role}")
        if endpoint.lifecycle_state not in SOURCE_STATES:
            raise ValueError(f"invalid source lifecycle state: {endpoint.lifecycle_state}")
        if endpoint.consecutive_failures < 0:
            raise ValueError("consecutive_failures must be non-negative")
        if endpoint.lifecycle_state in {"quarantined", "retired"} and endpoint.enabled:
            raise ValueError(f"{endpoint.lifecycle_state} source must be disabled")
        canonical_url = canonicalize_url(endpoint.canonical_url)
        now = utc_now()
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT lifecycle_state, retirement_reason FROM source_endpoints WHERE id=?",
                (endpoint.id,),
            ).fetchone()
            if (
                existing
                and existing["lifecycle_state"] == "retired"
                and endpoint.lifecycle_state != "retired"
                and not allow_reactivate
            ):
                endpoint = replace(
                    endpoint,
                    lifecycle_state="retired",
                    enabled=False,
                    retirement_reason=existing["retirement_reason"],
                    next_due_at=None,
                )
            connection.execute(
                """
                INSERT INTO source_endpoints(
                    id, canonical_url, display_name, owner_organization_id, role,
                    lifecycle_state, enabled, next_due_at, last_checked_at,
                    last_good_snapshot_id, consecutive_failures, retirement_reason,
                    metadata_json, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    canonical_url=excluded.canonical_url,
                    display_name=excluded.display_name,
                    owner_organization_id=excluded.owner_organization_id,
                    role=excluded.role,
                    lifecycle_state=excluded.lifecycle_state,
                    enabled=excluded.enabled,
                    next_due_at=excluded.next_due_at,
                    last_checked_at=excluded.last_checked_at,
                    last_good_snapshot_id=excluded.last_good_snapshot_id,
                    consecutive_failures=excluded.consecutive_failures,
                    retirement_reason=excluded.retirement_reason,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                (
                    endpoint.id,
                    canonical_url,
                    endpoint.display_name,
                    endpoint.owner_organization_id,
                    endpoint.role,
                    endpoint.lifecycle_state,
                    int(endpoint.enabled),
                    normalize_timestamp(endpoint.next_due_at),
                    normalize_timestamp(endpoint.last_checked_at),
                    endpoint.last_good_snapshot_id,
                    endpoint.consecutive_failures,
                    endpoint.retirement_reason,
                    _json(endpoint.metadata),
                    now,
                    now,
                ),
            )
            connection.execute(
                "DELETE FROM source_entity_links WHERE source_id=? AND relation='applies_to'",
                (endpoint.id,),
            )
            connection.executemany(
                "INSERT OR IGNORE INTO source_entity_links(source_id, entity_id, relation) VALUES (?,?,'applies_to')",
                [(endpoint.id, entity_id) for entity_id in sorted(set(endpoint.applies_to_entity_ids))],
            )
        return self.get_source(endpoint.id)

    @staticmethod
    def _source_from_row(
        row: Mapping[str, Any],
        entity_ids: Sequence[str] = (),
    ) -> SourceEndpoint:
        return SourceEndpoint(
            id=row["id"],
            canonical_url=row["canonical_url"],
            display_name=row["display_name"],
            owner_organization_id=row["owner_organization_id"],
            applies_to_entity_ids=tuple(entity_ids),
            role=row["role"],
            lifecycle_state=row["lifecycle_state"],
            enabled=bool(row["enabled"]),
            next_due_at=row["next_due_at"],
            last_checked_at=row["last_checked_at"],
            last_good_snapshot_id=row["last_good_snapshot_id"],
            consecutive_failures=row["consecutive_failures"],
            retirement_reason=row["retirement_reason"],
            metadata=json.loads(row["metadata_json"]),
        )

    def _sources_from_rows(self, rows: Sequence[Mapping[str, Any]]) -> list[SourceEndpoint]:
        if not rows:
            return []
        source_ids = [str(row["id"]) for row in rows]
        entity_ids: dict[str, list[str]] = {source_id: [] for source_id in source_ids}
        with self._lock:
            for index in range(0, len(source_ids), 500):
                chunk = source_ids[index:index + 500]
                links = self.connection.execute(
                    f"""
                    SELECT source_id, entity_id
                    FROM source_entity_links
                    WHERE relation='applies_to'
                      AND source_id IN ({','.join('?' for _ in chunk)})
                    ORDER BY source_id, entity_id
                    """,
                    chunk,
                ).fetchall()
                for link in links:
                    entity_ids[str(link["source_id"])].append(str(link["entity_id"]))
        return [
            self._source_from_row(row, entity_ids[str(row["id"])])
            for row in rows
        ]

    def get_source(self, source_id: str) -> SourceEndpoint:
        with self._lock:
            row = self.connection.execute(
                "SELECT * FROM source_endpoints WHERE id=?",
                (source_id,),
            ).fetchone()
            if row is None:
                raise KeyError(source_id)
            entity_rows = self.connection.execute(
                "SELECT entity_id FROM source_entity_links "
                "WHERE source_id=? AND relation='applies_to' ORDER BY entity_id",
                (source_id,),
            ).fetchall()
        return self._source_from_row(row, [str(item[0]) for item in entity_rows])

    def list_sources(
        self,
        *,
        states: Sequence[str] = (),
        roles: Sequence[str] = (),
        enabled: bool | None = None,
        limit: int = 10000,
        offset: int = 0,
    ) -> list[SourceEndpoint]:
        invalid_states = set(states) - SOURCE_STATES
        invalid_roles = set(roles) - SOURCE_ROLES
        if invalid_states:
            raise ValueError(f"invalid source states: {sorted(invalid_states)}")
        if invalid_roles:
            raise ValueError(f"invalid source roles: {sorted(invalid_roles)}")
        clauses: list[str] = []
        values: list[Any] = []
        if states:
            clauses.append(f"lifecycle_state IN ({','.join('?' for _ in states)})")
            values.extend(states)
        if roles:
            clauses.append(f"role IN ({','.join('?' for _ in roles)})")
            values.extend(roles)
        if enabled is not None:
            clauses.append("enabled=?")
            values.append(int(enabled))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        values.extend((max(int(limit), 0), max(int(offset), 0)))
        with self._lock:
            rows = self.connection.execute(
                f"""
                SELECT * FROM source_endpoints {where}
                ORDER BY role, lifecycle_state, id LIMIT ? OFFSET ?
                """,
                values,
            ).fetchall()
        return self._sources_from_rows(rows)

    def count_sources(
        self,
        *,
        states: Sequence[str] = (),
        roles: Sequence[str] = (),
        enabled: bool | None = None,
    ) -> int:
        invalid_states = set(states) - SOURCE_STATES
        invalid_roles = set(roles) - SOURCE_ROLES
        if invalid_states:
            raise ValueError(f"invalid source states: {sorted(invalid_states)}")
        if invalid_roles:
            raise ValueError(f"invalid source roles: {sorted(invalid_roles)}")
        clauses: list[str] = []
        values: list[Any] = []
        if states:
            clauses.append(f"lifecycle_state IN ({','.join('?' for _ in states)})")
            values.extend(states)
        if roles:
            clauses.append(f"role IN ({','.join('?' for _ in roles)})")
            values.extend(roles)
        if enabled is not None:
            clauses.append("enabled=?")
            values.append(int(enabled))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            row = self.connection.execute(
                f"SELECT COUNT(*) FROM source_endpoints {where}", values
            ).fetchone()
        return int(row[0])

    def list_due_sources(self, at: str | datetime | None = None, limit: int = 100) -> list[SourceEndpoint]:
        due_at = normalize_timestamp(at) or utc_now()
        with self._lock:
            rows = self.connection.execute(
                """
                SELECT * FROM source_endpoints
                WHERE enabled=1 AND lifecycle_state != 'retired'
                  AND (next_due_at IS NULL OR next_due_at <= ?)
                ORDER BY CASE role
                    WHEN 'current-primary' THEN 0 WHEN 'trusted-secondary' THEN 1
                    WHEN 'candidate' THEN 2 WHEN 'reference' THEN 3 ELSE 4 END,
                    COALESCE(next_due_at, ''), id
                LIMIT ?
                """,
                (due_at, limit),
            ).fetchall()
        return self._sources_from_rows(rows)

    def transition_source(
        self,
        source_id: str,
        new_state: str,
        *,
        reason: str | None = None,
        next_due_at: str | datetime | None = None,
        force: bool = False,
    ) -> SourceEndpoint:
        if new_state not in SOURCE_STATES:
            raise ValueError(f"invalid source lifecycle state: {new_state}")
        with self.transaction() as connection:
            current = self.get_source(source_id)
            if new_state != current.lifecycle_state and not force:
                if new_state not in _SOURCE_TRANSITIONS[current.lifecycle_state]:
                    raise ValueError(f"invalid source transition: {current.lifecycle_state} -> {new_state}")
            if new_state == "retired" and not reason:
                raise ValueError("retirement requires a reason")
            if current.lifecycle_state == "retired" and new_state != "retired":
                raise ValueError("retired source cannot be reactivated by a lifecycle transition")
            enabled = 0 if new_state in {"quarantined", "retired"} else 1
            due = None if new_state in {"quarantined", "retired"} else normalize_timestamp(next_due_at)
            metadata = dict(current.metadata)
            if reason:
                metadata["lifecycle_reason"] = reason
            elif new_state == "active":
                metadata.pop("lifecycle_reason", None)
            connection.execute(
                """
                UPDATE source_endpoints
                SET lifecycle_state=?, enabled=?, retirement_reason=?, next_due_at=?,
                    metadata_json=?, updated_at=?
                WHERE id=?
                """,
                (
                    new_state, enabled, reason if new_state == "retired" else None, due,
                    _json(metadata), utc_now(), source_id,
                ),
            )
            self._enqueue_outbox(
                OutboxEvent(
                    id=stable_id("event", "source.lifecycle", source_id, current.lifecycle_state, new_state, reason),
                    topic="monitor.sources",
                    aggregate_type="SourceEndpoint",
                    aggregate_id=source_id,
                    event_type="source.lifecycle_changed",
                    occurred_at=utc_now(),
                    payload={"from": current.lifecycle_state, "to": new_state, "reason": reason},
                )
            )
        return self.get_source(source_id)

    def request_source_revalidation(
        self,
        source_id: str,
        *,
        actor: str,
        reason: str,
        next_due_at: str | datetime | None = None,
    ) -> SourceEndpoint:
        if not actor.strip() or not reason.strip():
            raise ValueError("source revalidation requires actor and reason")
        with self.transaction() as connection:
            current = self.get_source(source_id)
            if current.lifecycle_state == "retired":
                raise ValueError("retired source requires explicit reactivation")
            target = {
                "quarantined": "validating",
                "degraded": "recovering",
            }.get(current.lifecycle_state, current.lifecycle_state)
            if target != current.lifecycle_state and target not in _SOURCE_TRANSITIONS[current.lifecycle_state]:
                raise ValueError(f"invalid source transition: {current.lifecycle_state} -> {target}")
            now = utc_now()
            metadata = {
                **dict(current.metadata),
                "revalidation_requested_at": now,
                "revalidation_actor": actor,
                "revalidation_reason": reason,
            }
            connection.execute(
                """
                UPDATE source_endpoints
                SET lifecycle_state=?, enabled=1, next_due_at=?, metadata_json=?, updated_at=?
                WHERE id=?
                """,
                (
                    target, normalize_timestamp(next_due_at) or now,
                    _json(metadata), now, source_id,
                ),
            )
            self._enqueue_outbox(
                OutboxEvent(
                    id=stable_id("event", "source.revalidation_requested", source_id, actor, now),
                    topic="monitor.sources",
                    aggregate_type="SourceEndpoint",
                    aggregate_id=source_id,
                    event_type="source.revalidation_requested",
                    occurred_at=now,
                    payload={
                        "from": current.lifecycle_state,
                        "to": target,
                        "actor": actor,
                        "reason": reason,
                    },
                )
            )
        return self.get_source(source_id)

    def reactivate_source(
        self,
        source_id: str,
        *,
        actor: str,
        reason: str,
        next_due_at: str | datetime | None = None,
    ) -> SourceEndpoint:
        """Explicitly restore a retired source with an auditable operator decision."""

        if not actor.strip() or not reason.strip():
            raise ValueError("source reactivation requires actor and reason")
        with self.transaction() as connection:
            current = self.get_source(source_id)
            if current.lifecycle_state != "retired":
                raise ValueError("only a retired source can be explicitly reactivated")
            now = utc_now()
            metadata = dict(current.metadata)
            metadata.pop("lifecycle_reason", None)
            metadata.update({
                "revalidation_requested_at": now,
                "revalidation_actor": actor,
                "revalidation_reason": reason,
            })
            connection.execute(
                """
                UPDATE source_endpoints
                SET lifecycle_state='validating', enabled=1, retirement_reason=NULL,
                    next_due_at=?, consecutive_failures=0, metadata_json=?, updated_at=?
                WHERE id=?
                """,
                (normalize_timestamp(next_due_at) or now, _json(metadata), now, source_id),
            )
            self._enqueue_outbox(
                OutboxEvent(
                    id=stable_id("event", "source.reactivated", source_id, actor, reason, now),
                    topic="monitor.sources",
                    aggregate_type="SourceEndpoint",
                    aggregate_id=source_id,
                    event_type="source.reactivated",
                    occurred_at=now,
                    payload={"from": "retired", "to": "validating", "actor": actor, "reason": reason},
                )
            )
        return self.get_source(source_id)

    def record_snapshot(self, snapshot: ContentSnapshot) -> ContentSnapshot:
        if not snapshot.content_sha256:
            raise ValueError("snapshot content_sha256 must not be empty")
        if not snapshot.raw_path and not snapshot.normalized_path:
            raise ValueError("snapshot must reference a raw or normalized path")
        metadata = _snapshot_metadata(snapshot.metadata)
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO content_snapshots(
                    id, source_id, captured_at, content_sha256, raw_path, normalized_path,
                    mime_type, content_bytes, complete, extractor_version, metadata_json, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    raw_path=excluded.raw_path,
                    normalized_path=excluded.normalized_path,
                    mime_type=excluded.mime_type,
                    content_bytes=excluded.content_bytes,
                    complete=excluded.complete,
                    extractor_version=excluded.extractor_version,
                    metadata_json=excluded.metadata_json
                """,
                (
                    snapshot.id,
                    snapshot.source_id,
                    normalize_timestamp(snapshot.captured_at),
                    snapshot.content_sha256,
                    snapshot.raw_path,
                    snapshot.normalized_path,
                    snapshot.mime_type,
                    snapshot.content_bytes,
                    int(snapshot.complete),
                    snapshot.extractor_version,
                    _json(metadata),
                    utc_now(),
                ),
            )
        return self.get_snapshot(snapshot.id)

    def get_snapshot(self, snapshot_id: str) -> ContentSnapshot:
        with self._lock:
            row = self.connection.execute("SELECT * FROM content_snapshots WHERE id=?", (snapshot_id,)).fetchone()
        if row is None:
            raise KeyError(snapshot_id)
        return ContentSnapshot(
            id=row["id"], source_id=row["source_id"], captured_at=row["captured_at"],
            content_sha256=row["content_sha256"], raw_path=row["raw_path"],
            normalized_path=row["normalized_path"], mime_type=row["mime_type"],
            content_bytes=row["content_bytes"], complete=bool(row["complete"]),
            extractor_version=row["extractor_version"], metadata=json.loads(row["metadata_json"]),
        )

    def list_snapshots(
        self,
        *,
        source_id: str | None = None,
        complete_only: bool = False,
        limit: int = 100,
    ) -> list[ContentSnapshot]:
        clauses: list[str] = []
        values: list[Any] = []
        if source_id is not None:
            clauses.append("source_id=?")
            values.append(source_id)
        if complete_only:
            clauses.append("complete=1")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        values.append(max(int(limit), 0))
        with self._lock:
            rows = self.connection.execute(
                f"SELECT id FROM content_snapshots {where} "
                "ORDER BY captured_at, id LIMIT ?",
                values,
            ).fetchall()
        return [self.get_snapshot(str(row["id"])) for row in rows]

    def complete_snapshot_coverage(
        self,
        source_ids: set[str] | None = None,
    ) -> dict[str, dict[str, int]]:
        """Return durable complete-snapshot and content-version counts per source.

        Sources are aggregated in bounded batches so health reporting does not issue
        one query per source or exceed SQLite's bind-variable limit. Repeated captures
        of identical content count as snapshots but not as comparable versions.
        """

        requested = {str(source_id) for source_id in source_ids or set() if source_id}
        if source_ids is not None and not requested:
            return {}
        rows: list[sqlite3.Row] = []
        batches: list[tuple[str, ...] | None]
        if source_ids is None:
            batches = [None]
        else:
            ordered = tuple(sorted(requested))
            batches = [ordered[index:index + 900] for index in range(0, len(ordered), 900)]
        with self._lock:
            for batch in batches:
                source_filter = ""
                values: tuple[str, ...] = ()
                if batch is not None:
                    placeholders = ",".join("?" for _ in batch)
                    source_filter = f" AND source_id IN ({placeholders})"
                    values = batch
                rows.extend(
                    self.connection.execute(
                        """
                        SELECT source_id,
                               COUNT(*) AS complete_snapshots,
                               COUNT(DISTINCT content_sha256) AS content_versions
                        FROM content_snapshots
                        WHERE complete=1
                        """
                        f"{source_filter} GROUP BY source_id",
                        values,
                    ).fetchall()
                )
        return {
            str(row["source_id"]): {
                "complete_snapshots": int(row["complete_snapshots"] or 0),
                "content_versions": int(row["content_versions"] or 0),
            }
            for row in rows
        }

    def record_check_run(self, run: CheckRun, snapshot: ContentSnapshot | None = None) -> bool:
        if run.status not in CHECK_STATUSES:
            raise ValueError(f"invalid check status: {run.status}")
        if run.source_lifecycle_after and run.source_lifecycle_after not in SOURCE_STATES:
            raise ValueError(f"invalid source lifecycle state: {run.source_lifecycle_after}")
        if snapshot and (snapshot.source_id != run.source_id or (run.snapshot_id and run.snapshot_id != snapshot.id)):
            raise ValueError("check run and snapshot must belong to the same source")
        with self.transaction() as connection:
            if snapshot:
                self.record_snapshot(snapshot)
            source = self.get_source(run.source_id)
            snapshot_id = run.snapshot_id or (snapshot.id if snapshot else None)
            recorded_lifecycle = (
                source.lifecycle_state
                if run.status == "deferred"
                else run.source_lifecycle_after
            )
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO check_runs(
                    id, source_id, started_at, finished_at, status, http_status,
                    fetch_strategy, error_category, error_detail, snapshot_id,
                    agent_run_id, next_due_at, source_lifecycle_after, metadata_json, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    run.id, run.source_id, normalize_timestamp(run.started_at), normalize_timestamp(run.finished_at),
                    run.status, run.http_status, run.fetch_strategy, run.error_category, run.error_detail,
                    snapshot_id, run.agent_run_id, normalize_timestamp(run.next_due_at),
                    recorded_lifecycle, _json(run.metadata), utc_now(),
                ),
            )
            inserted = cursor.rowcount == 1
            if not inserted:
                return False
            if run.status in {"success", "not_modified"}:
                failures = 0
            elif run.status == "deferred":
                failures = source.consecutive_failures
            else:
                failures = source.consecutive_failures + 1
            last_good = source.last_good_snapshot_id
            if snapshot and snapshot.complete and run.status == "success":
                last_good = snapshot.id
            lifecycle = recorded_lifecycle or source.lifecycle_state
            if lifecycle != source.lifecycle_state and lifecycle not in _SOURCE_TRANSITIONS[source.lifecycle_state]:
                raise ValueError(
                    f"invalid source transition: {source.lifecycle_state} -> {lifecycle}"
                )
            retirement_reason = source.retirement_reason
            if lifecycle == "retired":
                retirement_reason = run.error_detail or run.error_category
                if not retirement_reason:
                    raise ValueError("retirement check run requires an error detail or category")
            elif lifecycle != source.lifecycle_state:
                retirement_reason = None
            enabled = 0 if lifecycle in {"quarantined", "retired"} else 1
            source_metadata = dict(source.metadata)
            if run.status != "deferred":
                source_metadata.pop("revalidation_requested_at", None)
                source_metadata.pop("revalidation_actor", None)
                source_metadata.pop("revalidation_reason", None)
            if run.status in {"success", "not_modified"}:
                source_metadata.pop("lifecycle_reason", None)
            elif run.status == "deferred":
                source_metadata["last_deferred_check"] = {
                    "check_run_id": run.id,
                    "reason": run.error_detail or run.error_category or "capacity budget exhausted",
                    "deferred_at": normalize_timestamp(run.finished_at),
                }
            else:
                source_metadata["lifecycle_reason"] = run.error_detail or run.error_category or run.status
            connection.execute(
                """
                UPDATE source_endpoints
                SET last_checked_at=?, last_good_snapshot_id=?, consecutive_failures=?,
                    next_due_at=?, lifecycle_state=?, enabled=?, retirement_reason=?,
                    metadata_json=?, updated_at=?
                WHERE id=?
                """,
                (
                    normalize_timestamp(run.finished_at), last_good, failures,
                    normalize_timestamp(run.next_due_at), lifecycle, enabled,
                    retirement_reason, _json(source_metadata), utc_now(), run.source_id,
                ),
            )
            if lifecycle != source.lifecycle_state:
                self._enqueue_outbox(
                    OutboxEvent(
                        id=stable_id(
                            "event", "source.lifecycle", run.id,
                            source.lifecycle_state, lifecycle,
                        ),
                        topic="monitor.sources",
                        aggregate_type="SourceEndpoint",
                        aggregate_id=run.source_id,
                        event_type="source.lifecycle_changed",
                        occurred_at=run.finished_at,
                        payload={
                            "from": source.lifecycle_state,
                            "to": lifecycle,
                            "check_run_id": run.id,
                            "reason": retirement_reason,
                        },
                    )
                )
        return True

    def reclassify_browser_budget_deferrals(
        self,
        legacy_records: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict[str, int]:
        """Repair capacity deferrals that older versions persisted as source failures."""

        repaired_at = utc_now()
        legacy_records = legacy_records or {}
        affected_sources = {
            str(source_id)
            for source_id, record in legacy_records.items()
            if is_browser_budget_exhaustion(
                record.get("agent_failure_kind"),
                record.get("error") or record.get("deferred_reason"),
            )
        }
        repaired_run_ids: set[str] = set()
        synthesized_run_ids: set[str] = set()
        removed_events = 0
        cancelled_reviews = 0

        with self.transaction() as connection:
            rows = connection.execute(
                """
                SELECT id, source_id, error_detail, metadata_json
                FROM check_runs WHERE status='error'
                """
            ).fetchall()
            for row in rows:
                metadata = json.loads(row["metadata_json"] or "{}")
                if not is_browser_budget_exhaustion(
                    metadata.get("agent_failure_kind"), row["error_detail"]
                ):
                    continue
                metadata.update({
                    "agent_failure_kind": "budget",
                    "deferred_reason": "browser_capacity_budget_exhausted",
                    "reclassified_from": "error",
                    "reclassified_at": repaired_at,
                })
                connection.execute(
                    """
                    UPDATE check_runs
                    SET status='deferred', error_category='capacity_budget', metadata_json=?
                    WHERE id=?
                    """,
                    (_json(metadata), row["id"]),
                )
                repaired_run_ids.add(str(row["id"]))
                affected_sources.add(str(row["source_id"]))

            for source_id, record in legacy_records.items():
                source_row = connection.execute(
                    "SELECT lifecycle_state FROM source_endpoints WHERE id=?", (source_id,)
                ).fetchone()
                if source_row is None:
                    continue
                requested_id = str(record.get("check_run_id") or "").strip()
                check_id = requested_id or stable_id(
                    "check", "legacy-budget-deferred", source_id,
                    record.get("checked_at"), record.get("error"),
                )
                existing_run = connection.execute(
                    "SELECT source_id FROM check_runs WHERE id=?", (check_id,)
                ).fetchone()
                if existing_run is not None:
                    if str(existing_run["source_id"]) == source_id:
                        continue
                    check_id = stable_id(
                        "check", "legacy-budget-deferred", source_id,
                        record.get("checked_at"), record.get("error"), requested_id,
                    )
                    if connection.execute(
                        "SELECT 1 FROM check_runs WHERE id=?", (check_id,)
                    ).fetchone() is not None:
                        continue
                try:
                    checked_at = normalize_timestamp(record.get("checked_at")) or repaired_at
                except (TypeError, ValueError):
                    checked_at = repaired_at
                status_value = record.get("status_code")
                try:
                    http_status = int(status_value) if status_value is not None and status_value != "" else None
                except (TypeError, ValueError):
                    http_status = None
                detail = str(record.get("error") or "browser capacity budget exhausted")
                connection.execute(
                    """
                    INSERT INTO check_runs(
                        id, source_id, started_at, finished_at, status, http_status,
                        fetch_strategy, error_category, error_detail, snapshot_id,
                        agent_run_id, next_due_at, source_lifecycle_after,
                        metadata_json, created_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        check_id, source_id, checked_at, checked_at, "deferred", http_status,
                        str(record.get("fetch_mode") or "") or None,
                        "capacity_budget", detail, None,
                        str(record.get("agent_run_id") or "") or None,
                        repaired_at, str(source_row["lifecycle_state"]),
                        _json({
                            "agent_failure_kind": "budget",
                            "deferred_reason": "browser_capacity_budget_exhausted",
                            "reclassified_from": "legacy_state_error",
                            "reclassified_at": repaired_at,
                            "legacy_check_run_id": requested_id or None,
                            "legacy_check_run_inserted": record.get("check_run_inserted"),
                            "legacy_consecutive_failures": record.get("consecutive_failures"),
                            "legacy_lifecycle_state": record.get("lifecycle_state"),
                        }),
                        repaired_at,
                    ),
                )
                repaired_run_ids.add(check_id)
                synthesized_run_ids.add(check_id)

            for source_id in sorted(affected_sources):
                source = connection.execute(
                    "SELECT * FROM source_endpoints WHERE id=?", (source_id,)
                ).fetchone()
                if source is None:
                    continue
                runs = connection.execute(
                    """
                    SELECT id, status, source_lifecycle_after, error_category, error_detail
                    FROM check_runs WHERE source_id=?
                    ORDER BY finished_at DESC, created_at DESC, id DESC
                    """,
                    (source_id,),
                ).fetchall()
                latest_non_deferred = next(
                    (run for run in runs if run["status"] != "deferred"), None
                )
                failures = 0
                for run in runs:
                    if run["status"] == "deferred":
                        continue
                    if run["status"] in {"success", "not_modified"}:
                        break
                    failures += 1

                lifecycle = str(source["lifecycle_state"])
                if lifecycle == "degraded":
                    if latest_non_deferred is None:
                        legacy = legacy_records.get(source_id, {})
                        has_baseline = bool(
                            source["last_good_snapshot_id"]
                            or legacy.get("last_ok_at")
                            or legacy.get("snapshot_path")
                        )
                        lifecycle = (
                            "active"
                            if has_baseline
                            else "discovered" if source["role"] == "candidate" else "validating"
                        )
                    elif latest_non_deferred["status"] in {"success", "not_modified"}:
                        previous_lifecycle = str(
                            latest_non_deferred["source_lifecycle_after"] or ""
                        )
                        lifecycle = (
                            previous_lifecycle
                            if previous_lifecycle in SOURCE_STATES
                            and previous_lifecycle not in {"degraded", "quarantined", "retired"}
                            else "active" if source["last_good_snapshot_id"] else "validating"
                        )

                source_metadata = json.loads(source["metadata_json"] or "{}")
                current_reason = str(source_metadata.get("lifecycle_reason") or "")
                if is_browser_budget_exhaustion("budget", current_reason):
                    source_metadata.pop("lifecycle_reason", None)
                if failures and latest_non_deferred is not None:
                    source_metadata["lifecycle_reason"] = (
                        latest_non_deferred["error_detail"]
                        or latest_non_deferred["error_category"]
                        or latest_non_deferred["status"]
                    )
                source_metadata["budget_deferral_repair"] = {
                    "repaired_at": repaired_at,
                    "reclassified_check_runs": sum(
                        run["id"] in repaired_run_ids for run in runs
                    ),
                }
                latest_is_deferred = bool(runs and runs[0]["status"] == "deferred")
                if not runs and source_id in legacy_records:
                    latest_is_deferred = True
                next_due_at = repaired_at if latest_is_deferred and source["enabled"] else source["next_due_at"]
                connection.execute(
                    """
                    UPDATE source_endpoints
                    SET lifecycle_state=?, consecutive_failures=?, next_due_at=?,
                        metadata_json=?, updated_at=? WHERE id=?
                    """,
                    (
                        lifecycle, failures, next_due_at,
                        _json(source_metadata), repaired_at, source_id,
                    ),
                )
                source_repaired_run_ids = [
                    str(run["id"]) for run in runs if str(run["id"]) in repaired_run_ids
                ]
                if source_repaired_run_ids:
                    connection.executemany(
                        "UPDATE check_runs SET source_lifecycle_after=? WHERE id=? AND source_id=?",
                        [
                            (lifecycle, run_id, source_id)
                            for run_id in source_repaired_run_ids
                        ],
                    )

            lifecycle_events = connection.execute(
                """
                SELECT cursor, payload_json FROM outbox_events
                WHERE topic='monitor.sources' AND event_type='source.lifecycle_changed'
                """
            ).fetchall()
            for event in lifecycle_events:
                payload = json.loads(event["payload_json"] or "{}")
                if str(payload.get("check_run_id") or "") not in repaired_run_ids:
                    continue
                connection.execute(
                    "DELETE FROM outbox_events WHERE cursor=?", (event["cursor"],)
                )
                removed_events += 1

            false_unavailable_guids = {
                f"unavailable:{source_id}:"
                f"{hashlib.sha256(str(record.get('error') or '').encode()).hexdigest()[:12]}"
                for source_id, record in legacy_records.items()
            }
            legacy_events = connection.execute(
                "SELECT cursor, payload_json FROM outbox_events WHERE topic='legacy.monitor.events'"
            ).fetchall()
            for event in legacy_events:
                payload = json.loads(event["payload_json"] or "{}")
                if str(payload.get("guid") or "") not in false_unavailable_guids:
                    continue
                connection.execute(
                    "DELETE FROM outbox_events WHERE cursor=?", (event["cursor"],)
                )
                removed_events += 1

            active_reviews = connection.execute(
                """
                SELECT * FROM review_tasks
                WHERE status IN ('open','assigned','in_progress')
                """
            ).fetchall()
            for task in active_reviews:
                details = f"{task['reason']} {task['metadata_json']}"
                if not is_browser_budget_exhaustion("budget", details):
                    continue
                connection.execute(
                    """
                    UPDATE review_tasks
                    SET status='cancelled', resolution=?, resume_action=?,
                        updated_at=?, resolved_at=? WHERE id=?
                    """,
                    (
                        "capacity budget exhaustion is automatically deferred, not manually reviewed",
                        "automatic_retry_after_capacity_reset",
                        repaired_at,
                        repaired_at,
                        task["id"],
                    ),
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO review_task_actions(
                        id, task_id, action_type, actor, payload_json, created_at
                    ) VALUES (?,?,?,?,?,?)
                    """,
                    (
                        stable_id("review-action", "budget-deferral-repair", task["id"]),
                        task["id"],
                        f"status:{task['status']}->cancelled",
                        "budget-deferral-migration",
                        _json({
                            "resolution": "capacity budget exhaustion is not an actionable source failure",
                            "resume_action": "automatic_retry_after_capacity_reset",
                        }),
                        repaired_at,
                    ),
                )
                cancelled_reviews += 1

        return {
            "reclassified_check_runs": len(repaired_run_ids),
            "synthesized_deferred_check_runs": len(synthesized_run_ids),
            "repaired_sources": len(affected_sources),
            "removed_false_lifecycle_events": removed_events,
            "cancelled_budget_reviews": cancelled_reviews,
        }

    def reject_legacy_summary_candidates(
        self,
        policy_summaries: Mapping[str, Any] | None,
    ) -> dict[str, int]:
        """Keep legacy summaries as audit data without scheduling evidence work."""

        legacy_candidate_ids = {
            stable_id("candidate", str(guid)): str(guid)
            for guid, summary in _mapping(policy_summaries).items()
            if isinstance(summary, Mapping) and str(guid).startswith("content:")
        }
        if not legacy_candidate_ids:
            return {
                "matched_candidates": 0,
                "rejected_candidates": 0,
                "cancelled_reviews": 0,
            }

        repaired_at = utc_now()
        reason = "legacy policy summary retained for audit without comparable snapshots"
        matched_candidates = 0
        rejected_candidates = 0
        cancelled_reviews = 0
        with self.transaction() as connection:
            candidates = connection.execute(
                """
                SELECT id, state, payload_json FROM change_candidates
                WHERE old_snapshot_id IS NULL AND new_snapshot_id IS NULL
                """
            ).fetchall()
            matched_ids: list[str] = []
            for candidate in candidates:
                candidate_id = str(candidate["id"])
                guid = legacy_candidate_ids.get(candidate_id)
                if guid is None:
                    continue
                matched_candidates += 1
                matched_ids.append(candidate_id)
                if candidate["state"] == "rejected":
                    continue
                payload = json.loads(candidate["payload_json"] or "{}")
                payload.update({
                    "legacy_summary_guid": guid,
                    "legacy_summary_audit_only": True,
                    "legacy_summary_reclassified_at": repaired_at,
                })
                connection.execute(
                    """
                    UPDATE change_candidates
                    SET state='rejected', resolution_reason=?, payload_json=?, updated_at=?
                    WHERE id=?
                    """,
                    (reason, _json(payload), repaired_at, candidate_id),
                )
                rejected_candidates += 1

            for candidate_id in matched_ids:
                tasks = connection.execute(
                    """
                    SELECT id, status FROM review_tasks
                    WHERE change_candidate_id=? AND task_type='change_evidence'
                      AND status IN ('open','assigned','in_progress')
                    """,
                    (candidate_id,),
                ).fetchall()
                for task in tasks:
                    connection.execute(
                        """
                        UPDATE review_tasks
                        SET status='cancelled', resolution=?, resume_action='audit_only',
                            retry_after=NULL, updated_at=?, resolved_at=?
                        WHERE id=?
                        """,
                        (reason, repaired_at, repaired_at, task["id"]),
                    )
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO review_task_actions(
                            id, task_id, action_type, actor, payload_json, created_at
                        ) VALUES (?,?,?,?,?,?)
                        """,
                        (
                            stable_id(
                                "review-action", "legacy-summary-audit-only", task["id"]
                            ),
                            task["id"],
                            f"status:{task['status']}->cancelled",
                            "legacy-summary-migration",
                            _json({"resolution": reason, "resume_action": "audit_only"}),
                            repaired_at,
                        ),
                    )
                    cancelled_reviews += 1

        return {
            "matched_candidates": matched_candidates,
            "rejected_candidates": rejected_candidates,
            "cancelled_reviews": cancelled_reviews,
        }

    def upsert_change_candidate(self, candidate: ChangeCandidate) -> ChangeCandidate:
        if candidate.state not in CHANGE_CANDIDATE_STATES:
            raise ValueError(f"invalid change candidate state: {candidate.state}")
        if candidate.confidence is not None and not 0 <= candidate.confidence <= 1:
            raise ValueError("candidate confidence must be between 0 and 1")
        now = utc_now()
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO change_candidates(
                    id, source_id, old_snapshot_id, new_snapshot_id, fact_key,
                    detected_at, state, headline, confidence, resolution_reason,
                    payload_json, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    old_snapshot_id=excluded.old_snapshot_id,
                    new_snapshot_id=excluded.new_snapshot_id,
                    fact_key=excluded.fact_key,
                    state=excluded.state,
                    headline=excluded.headline,
                    confidence=excluded.confidence,
                    resolution_reason=excluded.resolution_reason,
                    payload_json=excluded.payload_json,
                    updated_at=excluded.updated_at
                """,
                (
                    candidate.id, candidate.source_id, candidate.old_snapshot_id, candidate.new_snapshot_id,
                    candidate.fact_key, normalize_timestamp(candidate.detected_at), candidate.state,
                    candidate.headline, candidate.confidence, candidate.resolution_reason,
                    _json(candidate.payload), now, now,
                ),
            )
        return self.get_change_candidate(candidate.id)

    def get_change_candidate(self, candidate_id: str) -> ChangeCandidate:
        with self._lock:
            row = self.connection.execute("SELECT * FROM change_candidates WHERE id=?", (candidate_id,)).fetchone()
        if row is None:
            raise KeyError(candidate_id)
        return ChangeCandidate(
            id=row["id"], source_id=row["source_id"], old_snapshot_id=row["old_snapshot_id"],
            new_snapshot_id=row["new_snapshot_id"], fact_key=row["fact_key"],
            detected_at=row["detected_at"], state=row["state"], headline=row["headline"],
            confidence=row["confidence"], resolution_reason=row["resolution_reason"],
            payload=json.loads(row["payload_json"]),
        )

    def list_change_candidates(
        self,
        *,
        states: Sequence[str] = (),
        limit: int = 100,
    ) -> list[ChangeCandidate]:
        invalid = set(states) - CHANGE_CANDIDATE_STATES
        if invalid:
            raise ValueError(f"invalid change candidate states: {sorted(invalid)}")
        values: list[Any] = []
        where = ""
        if states:
            where = f"WHERE state IN ({','.join('?' for _ in states)})"
            values.extend(states)
        values.append(max(int(limit), 0))
        with self._lock:
            rows = self.connection.execute(
                f"SELECT id FROM change_candidates {where} ORDER BY detected_at LIMIT ?",
                values,
            ).fetchall()
        return [self.get_change_candidate(row["id"]) for row in rows]

    def record_evidence_bundle(self, bundle: EvidenceBundle) -> EvidenceBundle:
        if bundle.status not in EVIDENCE_STATES:
            raise ValueError(f"invalid evidence status: {bundle.status}")
        if not bundle.evidence_path or not bundle.evidence_sha256:
            raise ValueError("evidence bundle must reference a path and hash")
        try:
            existing = self.get_evidence_bundle(bundle.id)
        except KeyError:
            existing = None
        if existing is not None:
            expected = replace(
                bundle,
                created_at=normalize_timestamp(bundle.created_at) or "",
                verified_at=normalize_timestamp(bundle.verified_at),
                spans=tuple(bundle.spans),
                structured_facts=dict(bundle.structured_facts),
            )
            if existing != expected:
                raise ValueError(f"immutable evidence bundle collision: {bundle.id}")
            return existing
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO evidence_bundles(
                    id, candidate_id, status, rule_version, evidence_path, evidence_sha256,
                    old_snapshot_id, new_snapshot_id, source_count, spans_json,
                    structured_facts_json, created_at, verified_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    bundle.id, bundle.candidate_id, bundle.status, bundle.rule_version,
                    bundle.evidence_path, bundle.evidence_sha256, bundle.old_snapshot_id,
                    bundle.new_snapshot_id, bundle.source_count, _json(bundle.spans),
                    _json(bundle.structured_facts), normalize_timestamp(bundle.created_at),
                    normalize_timestamp(bundle.verified_at),
                ),
            )
        return self.get_evidence_bundle(bundle.id)

    def get_evidence_bundle(self, bundle_id: str) -> EvidenceBundle:
        with self._lock:
            row = self.connection.execute(
                "SELECT * FROM evidence_bundles WHERE id=?", (bundle_id,)
            ).fetchone()
        if row is None:
            raise KeyError(bundle_id)
        return EvidenceBundle(
            id=row["id"], candidate_id=row["candidate_id"], status=row["status"],
            rule_version=row["rule_version"], evidence_path=row["evidence_path"],
            evidence_sha256=row["evidence_sha256"], old_snapshot_id=row["old_snapshot_id"],
            new_snapshot_id=row["new_snapshot_id"], source_count=row["source_count"],
            spans=tuple(json.loads(row["spans_json"])),
            structured_facts=json.loads(row["structured_facts_json"]),
            created_at=row["created_at"], verified_at=row["verified_at"],
        )

    def list_evidence_bundles(
        self,
        *,
        candidate_id: str | None = None,
        limit: int = 100,
    ) -> list[EvidenceBundle]:
        values: list[Any] = []
        where = ""
        if candidate_id is not None:
            where = "WHERE candidate_id=?"
            values.append(candidate_id)
        values.append(max(int(limit), 0))
        with self._lock:
            rows = self.connection.execute(
                f"SELECT id FROM evidence_bundles {where} ORDER BY created_at DESC LIMIT ?",
                values,
            ).fetchall()
        return [self.get_evidence_bundle(row["id"]) for row in rows]

    def append_policy_change_revision(
        self,
        revision: PolicyChangeRevision,
        *,
        idempotency_key: str | None = None,
    ) -> PolicyChangeRevision:
        if revision.status not in REVISION_STATES:
            raise ValueError(f"invalid policy revision status: {revision.status}")
        revision_id = revision.id or stable_id(
            "revision",
            revision.change_id,
            idempotency_key or revision.status,
            revision.evidence_bundle_id,
            revision.summary,
            revision.occurred_at,
        )
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT id FROM policy_change_revisions WHERE id=?", (revision_id,)
            ).fetchone()
            if existing:
                return self.get_policy_change_revision(revision_id)
            now = utc_now()
            connection.execute(
                """
                INSERT INTO policy_changes(id, fact_key, source_id, candidate_id, created_at, updated_at)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    fact_key=excluded.fact_key,
                    source_id=COALESCE(excluded.source_id, policy_changes.source_id),
                    candidate_id=COALESCE(excluded.candidate_id, policy_changes.candidate_id),
                    updated_at=excluded.updated_at
                """,
                (revision.change_id, revision.fact_key, revision.source_id, revision.candidate_id, now, now),
            )
            previous = connection.execute(
                """
                SELECT id FROM policy_change_revisions
                WHERE change_id=? AND status='confirmed'
                ORDER BY revision_no DESC LIMIT 1
                """,
                (revision.change_id,),
            ).fetchone()
            revision_no = connection.execute(
                "SELECT COALESCE(MAX(revision_no), 0) + 1 FROM policy_change_revisions WHERE change_id=?",
                (revision.change_id,),
            ).fetchone()[0]
            supersedes = previous["id"] if previous and revision.status in {"confirmed", "retracted", "superseded"} else None
            connection.execute(
                """
                INSERT INTO policy_change_revisions(
                    id, change_id, revision_no, status, evidence_bundle_id, headline,
                    summary, impact, recommended_action, reason, supersedes_revision_id,
                    occurred_at, published_at, metadata_json, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    revision_id, revision.change_id, revision_no, revision.status,
                    revision.evidence_bundle_id, revision.headline, revision.summary,
                    revision.impact, revision.recommended_action, revision.reason,
                    supersedes, normalize_timestamp(revision.occurred_at),
                    normalize_timestamp(revision.published_at), _json(revision.metadata), now,
                ),
            )
            if supersedes:
                connection.execute(
                    "UPDATE policy_change_revisions SET status='superseded', superseded_by_revision_id=? WHERE id=?",
                    (revision_id, supersedes),
                )
                connection.execute(
                    """
                    UPDATE knowledge_update_proposals
                    SET status='rejected', decision_reason=?, decided_at=?, updated_at=?
                    WHERE policy_change_revision_id=? AND status IN ('proposed','approved')
                    """,
                    (f"superseded by policy revision {revision_id}", now, now, supersedes),
                )
            if revision.status in {"confirmed", "retracted", "superseded"}:
                operation = "upsert" if revision.status == "confirmed" else "delete"
                event_id = stable_id("event", "policy.change", revision_id)
                cursor = self._enqueue_outbox(
                    OutboxEvent(
                        id=event_id,
                        topic="policy.changes",
                        aggregate_type="PolicyChange",
                        aggregate_id=revision.change_id,
                        event_type=f"policy_change.{revision.status}",
                        occurred_at=revision.occurred_at,
                        payload={
                            "operation": operation,
                            "change_id": revision.change_id,
                            "revision_id": revision_id,
                            "revision": revision_no,
                            "status": revision.status,
                            "supersedes": supersedes,
                            "fact_key": revision.fact_key,
                            "headline": revision.headline,
                            "summary": revision.summary,
                            "impact": revision.impact,
                            "recommended_action": revision.recommended_action,
                            "source_id": revision.source_id,
                        },
                    )
                )
                connection.execute(
                    "UPDATE policy_change_revisions SET outbox_cursor=? WHERE id=?",
                    (cursor, revision_id),
                )
        return self.get_policy_change_revision(revision_id)

    def get_policy_change_revision(self, revision_id: str) -> PolicyChangeRevision:
        with self._lock:
            row = self.connection.execute(
                """
                SELECT r.*, c.fact_key, c.source_id, c.candidate_id
                FROM policy_change_revisions r JOIN policy_changes c ON c.id=r.change_id
                WHERE r.id=?
                """,
                (revision_id,),
            ).fetchone()
        if row is None:
            raise KeyError(revision_id)
        return PolicyChangeRevision(
            id=row["id"], change_id=row["change_id"], fact_key=row["fact_key"],
            revision_no=row["revision_no"],
            supersedes_revision_id=row["supersedes_revision_id"],
            outbox_cursor=row["outbox_cursor"],
            source_id=row["source_id"], candidate_id=row["candidate_id"],
            evidence_bundle_id=row["evidence_bundle_id"], status=row["status"],
            headline=row["headline"], summary=row["summary"], impact=row["impact"],
            recommended_action=row["recommended_action"], reason=row["reason"],
            occurred_at=row["occurred_at"], published_at=row["published_at"],
            metadata=json.loads(row["metadata_json"]),
        )

    def list_effective_policy_changes(self, *, after_cursor: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.connection.execute(
                """
                SELECT r.*, c.fact_key, c.source_id, c.candidate_id
                FROM policy_change_revisions r JOIN policy_changes c ON c.id=r.change_id
                WHERE r.status='confirmed' AND COALESCE(r.outbox_cursor, 0) > ?
                ORDER BY r.outbox_cursor LIMIT ?
                """,
                (after_cursor, limit),
            ).fetchall()
        return [
            {
                "cursor": row["outbox_cursor"],
                "change_id": row["change_id"],
                "revision_id": row["id"],
                "revision": row["revision_no"],
                "status": row["status"],
                "supersedes": row["supersedes_revision_id"],
                "fact_key": row["fact_key"],
                "source_id": row["source_id"],
                "candidate_id": row["candidate_id"],
                "evidence_bundle_id": row["evidence_bundle_id"],
                "headline": row["headline"],
                "summary": row["summary"],
                "impact": row["impact"],
                "recommended_action": row["recommended_action"],
                "occurred_at": row["occurred_at"],
                "published_at": row["published_at"],
                "metadata": json.loads(row["metadata_json"]),
            }
            for row in rows
        ]

    def count_effective_policy_changes(self, *, after_cursor: int = 0) -> int:
        with self._lock:
            row = self.connection.execute(
                """
                SELECT COUNT(*) FROM policy_change_revisions
                WHERE status='confirmed' AND COALESCE(outbox_cursor, 0) > ?
                """,
                (max(int(after_cursor), 0),),
            ).fetchone()
        return int(row[0])

    def get_change_cursor(self) -> int:
        with self._lock:
            return int(self.connection.execute(
                "SELECT COALESCE(MAX(cursor), 0) FROM outbox_events WHERE topic='policy.changes'"
            ).fetchone()[0])

    def read_change_feed(self, *, after_cursor: int = 0, limit: int = 100) -> list[OutboxEvent]:
        return self.list_outbox(after_cursor=after_cursor, limit=limit, topic="policy.changes")

    def count_change_feed(self, *, after_cursor: int = 0) -> int:
        return self.count_outbox(after_cursor=after_cursor, topic="policy.changes")

    def open_review_task(self, task: ReviewTask) -> ReviewTask:
        if task.status not in REVIEW_STATES:
            raise ValueError(f"invalid review status: {task.status}")
        if not 0 <= task.priority <= 100:
            raise ValueError("review priority must be between 0 and 100")
        if not task.reason.strip():
            raise ValueError("review task requires a reason")
        if not task.due_at:
            raise ValueError("review task requires an SLA due_at")
        effective_owner = task.owner or REVIEW_TASK_OWNER_BY_TYPE.get(task.task_type)
        effective_resume_action = task.resume_action
        if task.task_type != "source_recovery":
            effective_resume_action = (
                effective_resume_action
                or REVIEW_TASK_RESUME_ACTION_BY_TYPE.get(task.task_type)
            )
        if task.task_type == "source_recovery":
            if not task.source_id:
                raise ValueError("source recovery task requires a source_id")
            if not task.retry_after:
                raise ValueError("source recovery task requires a retry_after")
            if not effective_resume_action:
                raise ValueError("source recovery task requires a resume_action")
        if (
            task.status in {"open", "assigned", "in_progress"}
            and task.task_type != "legacy_agent_blocked"
            and not effective_owner
        ):
            raise ValueError(f"{task.status} review task requires an owner")
        if (
            task.status in {"open", "assigned", "in_progress"}
            and task.task_type != "legacy_agent_blocked"
            and not effective_resume_action
        ):
            raise ValueError(f"{task.status} review task requires a resume_action")
        now = utc_now()
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO review_tasks(
                    id, task_type, source_id, change_candidate_id, status, owner,
                    priority, reason, due_at, retry_after, resolution, resume_action,
                    metadata_json, created_at, updated_at, resolved_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    status=CASE
                        WHEN review_tasks.status IN ('resolved','cancelled') THEN 'open'
                        ELSE review_tasks.status
                    END,
                    owner=COALESCE(NULLIF(review_tasks.owner, ''), excluded.owner),
                    priority=MAX(review_tasks.priority, excluded.priority),
                    reason=excluded.reason,
                    due_at=excluded.due_at,
                    retry_after=excluded.retry_after,
                    resume_action=excluded.resume_action,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at,
                    resolved_at=CASE
                        WHEN review_tasks.status IN ('resolved','cancelled') THEN NULL
                        ELSE review_tasks.resolved_at
                    END
                """,
                (
                    task.id, task.task_type, task.source_id, task.change_candidate_id,
                    task.status, effective_owner, task.priority, task.reason,
                    normalize_timestamp(task.due_at), normalize_timestamp(task.retry_after),
                    task.resolution, effective_resume_action, _json(task.metadata),
                    normalize_timestamp(task.created_at), now,
                    now if task.status in {"resolved", "cancelled"} else None,
                ),
            )
        return self.get_review_task(task.id)

    def get_review_task(self, task_id: str) -> ReviewTask:
        with self._lock:
            row = self.connection.execute("SELECT * FROM review_tasks WHERE id=?", (task_id,)).fetchone()
        if row is None:
            raise KeyError(task_id)
        return ReviewTask(
            id=row["id"], task_type=row["task_type"], source_id=row["source_id"],
            change_candidate_id=row["change_candidate_id"], status=row["status"],
            owner=row["owner"], priority=row["priority"], reason=row["reason"],
            due_at=row["due_at"], retry_after=row["retry_after"], resolution=row["resolution"],
            resume_action=row["resume_action"], metadata=json.loads(row["metadata_json"]),
            created_at=row["created_at"],
        )

    def transition_review_task(
        self,
        task_id: str,
        new_status: str,
        *,
        actor: str,
        owner: str | None = None,
        resolution: str | None = None,
        resume_action: str | None = None,
        retry_after: str | datetime | None = None,
        action_payload: Mapping[str, Any] | None = None,
    ) -> ReviewTask:
        if new_status not in REVIEW_STATES:
            raise ValueError(f"invalid review status: {new_status}")
        with self.transaction() as connection:
            current = self.get_review_task(task_id)
            if new_status != current.status and new_status not in _REVIEW_TRANSITIONS[current.status]:
                raise ValueError(f"invalid review transition: {current.status} -> {new_status}")
            if new_status in {"assigned", "in_progress"} and not owner:
                raise ValueError(f"{new_status} review task requires an owner supplied by the operator")
            effective_owner = owner if owner is not None else current.owner
            if new_status == "resolved" and not resolution:
                raise ValueError("resolved review task requires a resolution")
            now = utc_now()
            effective_resume_action = (
                resume_action if resume_action is not None else current.resume_action
            )
            effective_retry_after = (
                normalize_timestamp(retry_after)
                if retry_after is not None
                else current.retry_after
            )
            connection.execute(
                """
                UPDATE review_tasks SET status=?, owner=?, resolution=?, resume_action=?,
                    retry_after=?, updated_at=?, resolved_at=? WHERE id=?
                """,
                (
                    new_status, effective_owner, resolution, effective_resume_action,
                    effective_retry_after, now,
                    now if new_status in {"resolved", "cancelled"} else None, task_id,
                ),
            )
            action_id = stable_id("review-action", task_id, current.status, new_status, actor, now)
            connection.execute(
                """
                INSERT INTO review_task_actions(id, task_id, action_type, actor, payload_json, created_at)
                VALUES (?,?,?,?,?,?)
                """,
                (
                    action_id, task_id, f"status:{current.status}->{new_status}", actor,
                    _json({
                        "resolution": resolution,
                        "resume_action": effective_resume_action,
                        "retry_after": effective_retry_after,
                        **_mapping(action_payload),
                    }),
                    now,
                ),
            )
        return self.get_review_task(task_id)

    def reconcile_review_task_contracts(self) -> dict[str, int]:
        """Archive Agent backlog rows and repair formal review contracts.

        ``manual-queue.json`` entries are domain-level Agent audit records. They
        cannot be operated as source recovery tasks because they have no stable
        ``SourceEndpoint`` binding. Older database versions imported those records
        as active reviews, so keep them queryable but remove them from the active
        workflow. Recoverable legacy evidence belongs to the evidence Agent queue,
        not to human review. Runtime source recovery and true evidence-review tasks
        get deterministic owners, SLA deadlines and resume controls.
        """

        repaired_at = utc_now()
        archived_legacy = 0
        cancelled_agent_evidence = 0
        cancelled_invalid_recovery = 0
        backfilled_recovery = 0
        backfilled_change_evidence = 0
        backfilled_evidence_chain = 0
        backfilled_knowledge = 0

        def default_due_at(row: sqlite3.Row, *, hours: int) -> str:
            existing = normalize_timestamp(row["due_at"])
            if existing:
                return existing
            created_at = normalize_timestamp(row["created_at"]) or repaired_at
            created = datetime.fromisoformat(created_at)
            return (created + timedelta(hours=hours)).isoformat(timespec="microseconds")

        with self.transaction() as connection:
            legacy_rows = connection.execute(
                """
                SELECT * FROM review_tasks
                WHERE task_type='legacy_agent_blocked'
                  AND status IN ('open','assigned','in_progress')
                """
            ).fetchall()
            for row in legacy_rows:
                metadata = json.loads(row["metadata_json"] or "{}")
                metadata.update({
                    "audit_only": True,
                    "archived_reason": "missing source-bound recovery control",
                })
                resolution = (
                    "legacy AgentStateStore blocked record retained for audit; "
                    "no source-bound recovery control"
                )
                connection.execute(
                    """
                    UPDATE review_tasks
                    SET status='cancelled', resolution=?, metadata_json=?,
                        updated_at=?, resolved_at=?
                    WHERE id=?
                    """,
                    (resolution, _json(metadata), repaired_at, repaired_at, row["id"]),
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO review_task_actions(
                        id, task_id, action_type, actor, payload_json, created_at
                    ) VALUES (?,?,?,?,?,?)
                    """,
                    (
                        stable_id("review-action", "legacy-agent-audit", row["id"]),
                        row["id"],
                        f"status:{row['status']}->cancelled",
                        "legacy-agent-migration",
                        _json({"resolution": resolution, "audit_only": True}),
                        repaired_at,
                    ),
                )
                archived_legacy += 1

            agent_evidence_rows = connection.execute(
                """
                SELECT review_tasks.*, change_candidates.state AS candidate_state
                FROM review_tasks
                LEFT JOIN change_candidates
                  ON change_candidates.id=review_tasks.change_candidate_id
                WHERE review_tasks.task_type='change_evidence'
                  AND review_tasks.status IN ('open','assigned','in_progress')
                """
            ).fetchall()
            for row in agent_evidence_rows:
                metadata = json.loads(row["metadata_json"] or "{}")
                deterministic_rejection = str(row["reason"] or "") in EVIDENCE_AGENT_REJECTION_REASONS
                recoverable_backlog = bool(
                    metadata.get("recoverable_from_snapshots")
                    and row["candidate_state"] == "gathering_evidence"
                )
                legacy_backlog = (
                    row["reason"]
                    == "complete legacy snapshots recovered; evidence agent verification pending"
                )
                if not (deterministic_rejection or recoverable_backlog or legacy_backlog):
                    continue
                metadata.update({
                    "audit_only": True,
                    "agent_managed": True,
                    "archived_reason": (
                        "evidence Agent determined this is not a policy change"
                        if deterministic_rejection
                        else "recoverable evidence remains in Agent backlog"
                    ),
                })
                resolution = (
                    "evidence Agent rejected a deterministic non-change; no human review required"
                    if deterministic_rejection
                    else (
                        "recoverable evidence remains in gathering_evidence for "
                        "the evidence Agent; no human review required"
                    )
                )
                connection.execute(
                    """
                    UPDATE review_tasks
                    SET status='cancelled', resolution=?, metadata_json=?,
                        updated_at=?, resolved_at=?
                    WHERE id=?
                    """,
                    (resolution, _json(metadata), repaired_at, repaired_at, row["id"]),
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO review_task_actions(
                        id, task_id, action_type, actor, payload_json, created_at
                    ) VALUES (?,?,?,?,?,?)
                    """,
                    (
                        stable_id("review-action", "evidence-agent-managed", row["id"]),
                        row["id"],
                        f"status:{row['status']}->cancelled",
                        "review-contract-migration",
                        _json({
                            "resolution": resolution,
                            "audit_only": True,
                            "candidate_state_preserved": True,
                        }),
                        repaired_at,
                    ),
                )
                cancelled_agent_evidence += 1

            invalid_rows = connection.execute(
                """
                SELECT * FROM review_tasks
                WHERE task_type='source_recovery'
                  AND status IN ('open','assigned','in_progress')
                  AND (source_id IS NULL OR TRIM(source_id)='')
                """
            ).fetchall()
            for row in invalid_rows:
                metadata = json.loads(row["metadata_json"] or "{}")
                metadata.update({
                    "audit_only": True,
                    "archived_reason": "source recovery task has no source_id",
                })
                resolution = "invalid source recovery record retained for audit"
                connection.execute(
                    """
                    UPDATE review_tasks
                    SET status='cancelled', resolution=?, metadata_json=?,
                        updated_at=?, resolved_at=?
                    WHERE id=?
                    """,
                    (resolution, _json(metadata), repaired_at, repaired_at, row["id"]),
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO review_task_actions(
                        id, task_id, action_type, actor, payload_json, created_at
                    ) VALUES (?,?,?,?,?,?)
                    """,
                    (
                        stable_id("review-action", "invalid-source-recovery", row["id"]),
                        row["id"],
                        f"status:{row['status']}->cancelled",
                        "review-contract-migration",
                        _json({"resolution": resolution, "audit_only": True}),
                        repaired_at,
                    ),
                )
                cancelled_invalid_recovery += 1

            recovery_rows = connection.execute(
                """
                SELECT * FROM review_tasks
                WHERE task_type='source_recovery'
                  AND status IN ('open','assigned','in_progress')
                  AND source_id IS NOT NULL AND TRIM(source_id)!=''
                  AND (
                    owner IS NULL OR TRIM(owner)='' OR
                    due_at IS NULL OR TRIM(due_at)='' OR
                    retry_after IS NULL OR TRIM(retry_after)='' OR
                    resume_action IS NULL OR TRIM(resume_action)=''
                  )
                """
            ).fetchall()
            for row in recovery_rows:
                due_at = default_due_at(row, hours=24)
                retry_after = (
                    normalize_timestamp(row["retry_after"])
                    or due_at
                    or normalize_timestamp(row["created_at"])
                    or repaired_at
                )
                resume_action = str(row["resume_action"] or "").strip() or "revalidate_source"
                owner = str(row["owner"] or "").strip() or SOURCE_RECOVERY_OWNER
                connection.execute(
                    """
                    UPDATE review_tasks
                    SET owner=?, due_at=?, retry_after=?, resume_action=?, updated_at=?
                    WHERE id=?
                    """,
                    (owner, due_at, retry_after, resume_action, repaired_at, row["id"]),
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO review_task_actions(
                        id, task_id, action_type, actor, payload_json, created_at
                    ) VALUES (?,?,?,?,?,?)
                    """,
                    (
                        stable_id("review-action", "source-recovery-contract", row["id"]),
                        row["id"],
                        "recovery_controls_backfilled",
                        "review-contract-migration",
                        _json({
                            "owner": owner,
                            "due_at": due_at,
                            "retry_after": retry_after,
                            "resume_action": resume_action,
                        }),
                        repaired_at,
                    ),
                )
                backfilled_recovery += 1

            change_evidence_rows = connection.execute(
                """
                SELECT * FROM review_tasks
                WHERE task_type='change_evidence'
                  AND status IN ('open','assigned','in_progress')
                  AND (
                    owner IS NULL OR TRIM(owner)='' OR
                    due_at IS NULL OR TRIM(due_at)='' OR
                    resume_action IS NULL OR TRIM(resume_action)=''
                  )
                """
            ).fetchall()
            for row in change_evidence_rows:
                due_at = default_due_at(row, hours=8)
                owner = str(row["owner"] or "").strip() or CHANGE_EVIDENCE_OWNER
                resume_action = str(row["resume_action"] or "").strip() or "enrich_evidence"
                connection.execute(
                    """
                    UPDATE review_tasks
                    SET owner=?, due_at=?, resume_action=?, updated_at=?
                    WHERE id=?
                    """,
                    (owner, due_at, resume_action, repaired_at, row["id"]),
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO review_task_actions(
                        id, task_id, action_type, actor, payload_json, created_at
                    ) VALUES (?,?,?,?,?,?)
                    """,
                    (
                        stable_id("review-action", "change-evidence-contract", row["id"]),
                        row["id"],
                        "review_contract_backfilled",
                        "review-contract-migration",
                        _json({
                            "owner": owner,
                            "due_at": due_at,
                            "resume_action": resume_action,
                        }),
                        repaired_at,
                    ),
                )
                backfilled_change_evidence += 1

            other_formal_rows = connection.execute(
                """
                SELECT * FROM review_tasks
                WHERE task_type IN (
                    'evidence_chain_recovery',
                    'knowledge_update_application',
                    'knowledge_operation',
                    'knowledge_rollback'
                )
                  AND status IN ('open','assigned','in_progress')
                  AND (
                    owner IS NULL OR TRIM(owner)='' OR
                    due_at IS NULL OR TRIM(due_at)='' OR
                    resume_action IS NULL OR TRIM(resume_action)=''
                  )
                """
            ).fetchall()
            for row in other_formal_rows:
                task_type = str(row["task_type"])
                owner = (
                    str(row["owner"] or "").strip()
                    or REVIEW_TASK_OWNER_BY_TYPE[task_type]
                )
                resume_action = (
                    str(row["resume_action"] or "").strip()
                    or REVIEW_TASK_RESUME_ACTION_BY_TYPE[task_type]
                )
                due_at = default_due_at(
                    row,
                    hours=8 if task_type == "evidence_chain_recovery" else 4,
                )
                connection.execute(
                    """
                    UPDATE review_tasks
                    SET owner=?, due_at=?, resume_action=?, updated_at=?
                    WHERE id=?
                    """,
                    (owner, due_at, resume_action, repaired_at, row["id"]),
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO review_task_actions(
                        id, task_id, action_type, actor, payload_json, created_at
                    ) VALUES (?,?,?,?,?,?)
                    """,
                    (
                        stable_id("review-action", "formal-review-contract", row["id"]),
                        row["id"],
                        "review_contract_backfilled",
                        "review-contract-migration",
                        _json({
                            "owner": owner,
                            "due_at": due_at,
                            "resume_action": resume_action,
                        }),
                        repaired_at,
                    ),
                )
                if task_type == "evidence_chain_recovery":
                    backfilled_evidence_chain += 1
                else:
                    backfilled_knowledge += 1

        return {
            "archived_legacy_agent_records": archived_legacy,
            "cancelled_legacy_evidence_agent_reviews": cancelled_agent_evidence,
            "cancelled_invalid_source_recovery": cancelled_invalid_recovery,
            "backfilled_source_recovery_controls": backfilled_recovery,
            "backfilled_change_evidence_contracts": backfilled_change_evidence,
            "backfilled_evidence_chain_contracts": backfilled_evidence_chain,
            "backfilled_knowledge_review_contracts": backfilled_knowledge,
        }

    def list_review_actions(self, task_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.connection.execute(
                "SELECT * FROM review_task_actions WHERE task_id=? ORDER BY rowid", (task_id,)
            ).fetchall()
        return [
            {
                "id": row["id"], "task_id": row["task_id"], "action_type": row["action_type"],
                "actor": row["actor"], "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def list_review_tasks(
        self,
        *,
        statuses: Sequence[str] = ("open", "assigned", "in_progress"),
        owner: str | None = None,
        source_id: str | None = None,
        change_candidate_id: str | None = None,
        task_type: str | None = None,
        due_before: str | datetime | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ReviewTask]:
        invalid = set(statuses) - REVIEW_STATES
        if invalid:
            raise ValueError(f"invalid review statuses: {sorted(invalid)}")
        if not statuses:
            return []
        placeholders = ",".join("?" for _ in statuses)
        clauses = [f"status IN ({placeholders})"]
        values: list[Any] = list(statuses)
        if owner is not None:
            clauses.append("owner=?")
            values.append(owner)
        if source_id is not None:
            clauses.append("source_id=?")
            values.append(source_id)
        if change_candidate_id is not None:
            clauses.append("change_candidate_id=?")
            values.append(change_candidate_id)
        if task_type is not None:
            clauses.append("task_type=?")
            values.append(task_type)
        if due_before is not None:
            clauses.append("due_at<=?")
            values.append(normalize_timestamp(due_before))
        values.extend((max(int(limit), 0), max(int(offset), 0)))
        with self._lock:
            rows = self.connection.execute(
                f"""
                SELECT id FROM review_tasks WHERE {' AND '.join(clauses)}
                ORDER BY priority DESC, due_at, created_at LIMIT ? OFFSET ?
                """,
                values,
            ).fetchall()
            return [self.get_review_task(row["id"]) for row in rows]

    def count_review_tasks(
        self,
        *,
        statuses: Sequence[str] = ("open", "assigned", "in_progress"),
        owner: str | None = None,
        source_id: str | None = None,
        change_candidate_id: str | None = None,
        task_type: str | None = None,
        due_before: str | datetime | None = None,
    ) -> int:
        invalid = set(statuses) - REVIEW_STATES
        if invalid:
            raise ValueError(f"invalid review statuses: {sorted(invalid)}")
        if not statuses:
            return 0
        placeholders = ",".join("?" for _ in statuses)
        clauses = [f"status IN ({placeholders})"]
        values: list[Any] = list(statuses)
        if owner is not None:
            clauses.append("owner=?")
            values.append(owner)
        if source_id is not None:
            clauses.append("source_id=?")
            values.append(source_id)
        if change_candidate_id is not None:
            clauses.append("change_candidate_id=?")
            values.append(change_candidate_id)
        if task_type is not None:
            clauses.append("task_type=?")
            values.append(task_type)
        if due_before is not None:
            clauses.append("due_at<=?")
            values.append(normalize_timestamp(due_before))
        with self._lock:
            row = self.connection.execute(
                f"SELECT COUNT(*) FROM review_tasks WHERE {' AND '.join(clauses)}",
                values,
            ).fetchone()
        return int(row[0])

    def create_knowledge_update_proposal(self, proposal: KnowledgeUpdateProposal) -> KnowledgeUpdateProposal:
        if proposal.status not in PROPOSAL_STATES:
            raise ValueError(f"invalid knowledge proposal status: {proposal.status}")
        if not proposal.patch_path or not proposal.patch_sha256:
            raise ValueError("knowledge proposal must reference a patch path and hash")
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO knowledge_update_proposals(
                    id, policy_change_revision_id, target_ref, patch_path, patch_sha256,
                    status, summary, owner, proposed_at, decided_at, applied_at,
                    decision_reason, metadata_json, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    proposal.id, proposal.policy_change_revision_id, proposal.target_ref,
                    proposal.patch_path, proposal.patch_sha256, proposal.status,
                    proposal.summary, proposal.owner, normalize_timestamp(proposal.proposed_at),
                    normalize_timestamp(proposal.decided_at), normalize_timestamp(proposal.applied_at),
                    proposal.decision_reason, _json(proposal.metadata), utc_now(),
                ),
            )
        return self.get_knowledge_update_proposal(proposal.id)

    def get_knowledge_update_proposal(self, proposal_id: str) -> KnowledgeUpdateProposal:
        with self._lock:
            row = self.connection.execute(
                "SELECT * FROM knowledge_update_proposals WHERE id=?", (proposal_id,)
            ).fetchone()
        if row is None:
            raise KeyError(proposal_id)
        return KnowledgeUpdateProposal(
            id=row["id"], policy_change_revision_id=row["policy_change_revision_id"],
            target_ref=row["target_ref"], patch_path=row["patch_path"],
            patch_sha256=row["patch_sha256"], status=row["status"], summary=row["summary"],
            owner=row["owner"], proposed_at=row["proposed_at"], decided_at=row["decided_at"],
            applied_at=row["applied_at"], decision_reason=row["decision_reason"],
            metadata=json.loads(row["metadata_json"]),
        )

    def list_knowledge_update_proposals(
        self,
        *,
        statuses: Sequence[str] = ("proposed", "approved"),
        policy_change_revision_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[KnowledgeUpdateProposal]:
        invalid = set(statuses) - PROPOSAL_STATES
        if invalid:
            raise ValueError(f"invalid knowledge proposal statuses: {sorted(invalid)}")
        if not statuses:
            return []
        placeholders = ",".join("?" for _ in statuses)
        clauses = [f"status IN ({placeholders})"]
        values: list[Any] = list(statuses)
        if policy_change_revision_id is not None:
            clauses.append("policy_change_revision_id=?")
            values.append(policy_change_revision_id)
        values.extend((max(int(limit), 0), max(int(offset), 0)))
        with self._lock:
            rows = self.connection.execute(
                f"""
                SELECT id FROM knowledge_update_proposals
                WHERE {' AND '.join(clauses)}
                ORDER BY proposed_at DESC LIMIT ? OFFSET ?
                """,
                values,
            ).fetchall()
        return [self.get_knowledge_update_proposal(row["id"]) for row in rows]

    def count_knowledge_update_proposals(
        self,
        *,
        statuses: Sequence[str] = ("proposed", "approved"),
        policy_change_revision_id: str | None = None,
    ) -> int:
        invalid = set(statuses) - PROPOSAL_STATES
        if invalid:
            raise ValueError(f"invalid knowledge proposal statuses: {sorted(invalid)}")
        if not statuses:
            return 0
        placeholders = ",".join("?" for _ in statuses)
        clauses = [f"status IN ({placeholders})"]
        values: list[Any] = list(statuses)
        if policy_change_revision_id is not None:
            clauses.append("policy_change_revision_id=?")
            values.append(policy_change_revision_id)
        with self._lock:
            row = self.connection.execute(
                f"""
                SELECT COUNT(*) FROM knowledge_update_proposals
                WHERE {' AND '.join(clauses)}
                """,
                values,
            ).fetchone()
        return int(row[0])

    def knowledge_update_backlog_summary(self) -> dict[str, Any]:
        """Return pending knowledge work without loading proposal bodies."""

        with self._lock:
            row = self.connection.execute(
                """
                SELECT COUNT(*) AS pending, MIN(proposed_at) AS oldest_pending_at
                FROM knowledge_update_proposals
                WHERE status IN ('proposed','approved')
                """
            ).fetchone()
        return {
            "pending": int(row["pending"]),
            "oldest_pending_at": row["oldest_pending_at"],
        }

    def operational_summary(self) -> dict[str, Any]:
        """Return low-cost counts used by the operator dashboard and health API."""

        def grouped(connection: sqlite3.Connection, table: str, column: str) -> dict[str, int]:
            rows = connection.execute(
                f"SELECT {column}, COUNT(*) AS total FROM {table} GROUP BY {column}"
            ).fetchall()
            return {str(row[0]): int(row[1]) for row in rows}

        with self._lock:
            source_states = grouped(self.connection, "source_endpoints", "lifecycle_state")
            source_roles = grouped(self.connection, "source_endpoints", "role")
            proposal_states = grouped(self.connection, "knowledge_update_proposals", "status")
            candidate_states = grouped(self.connection, "change_candidates", "state")
            revision_states = grouped(self.connection, "policy_change_revisions", "status")
            audit_expression = (
                "COALESCE(json_extract(CASE WHEN json_valid(metadata_json) "
                "THEN metadata_json ELSE '{}' END, '$.audit_only'), 0)"
            )
            formal_review_clause = (
                "task_type!='legacy_agent_blocked' AND "
                f"{audit_expression} != 1 AND ("
                "status NOT IN ('open','assigned','in_progress') OR "
                "owner IS NOT NULL AND TRIM(owner)!='' AND "
                "due_at IS NOT NULL AND TRIM(due_at)!='' AND "
                "resume_action IS NOT NULL AND TRIM(resume_action)!='' AND ("
                "task_type!='source_recovery' OR "
                "(retry_after IS NOT NULL AND TRIM(retry_after)!='')"
                "))"
            )
            review_state_rows = self.connection.execute(
                f"""
                SELECT status, COUNT(*) AS total FROM review_tasks
                WHERE {formal_review_clause}
                GROUP BY status
                """
            ).fetchall()
            review_states = {
                str(row["status"]): int(row["total"])
                for row in review_state_rows
            }
            overdue_reviews = int(self.connection.execute(
                f"""
                SELECT COUNT(*) FROM review_tasks
                WHERE {formal_review_clause}
                  AND status IN ('open','assigned','in_progress') AND due_at < ?
                """,
                (utc_now(),),
            ).fetchone()[0])
            active_reviews = int(self.connection.execute(
                f"""
                SELECT COUNT(*) FROM review_tasks
                WHERE {formal_review_clause}
                  AND status IN ('open','assigned','in_progress')
                """
            ).fetchone()[0])
            legacy_audit_reviews = int(self.connection.execute(
                f"""
                SELECT COUNT(*) FROM review_tasks
                WHERE task_type='legacy_agent_blocked' OR {audit_expression} = 1
                """
            ).fetchone()[0])
        active_proposal_states = {"proposed", "approved"}
        return {
            "sources": {
                "total": sum(source_states.values()),
                "by_state": source_states,
                "pending": sum(
                    source_states.get(state, 0)
                    for state in ("discovered", "validating", "recovering")
                ),
                "by_role": source_roles,
                "paused": source_states.get("quarantined", 0),
                "retired": source_states.get("retired", 0),
            },
            "review_tasks": {
                "total": sum(review_states.values()),
                "active": active_reviews,
                "overdue": overdue_reviews,
                "legacy_audit": legacy_audit_reviews,
                "by_status": review_states,
            },
            "knowledge_updates": {
                "total": sum(proposal_states.values()),
                "pending": sum(proposal_states.get(state, 0) for state in active_proposal_states),
                "by_status": proposal_states,
            },
            "change_candidates": {
                "total": sum(candidate_states.values()),
                "by_status": candidate_states,
            },
            "policy_revisions": {
                "total": sum(revision_states.values()),
                "by_status": revision_states,
            },
        }

    def transition_knowledge_update_proposal(
        self,
        proposal_id: str,
        new_status: str,
        *,
        owner: str,
        reason: str | None = None,
    ) -> KnowledgeUpdateProposal:
        if new_status not in PROPOSAL_STATES:
            raise ValueError(f"invalid knowledge proposal status: {new_status}")
        with self.transaction() as connection:
            current = self.get_knowledge_update_proposal(proposal_id)
            if new_status != current.status and new_status not in _PROPOSAL_TRANSITIONS[current.status]:
                raise ValueError(f"invalid proposal transition: {current.status} -> {new_status}")
            now = utc_now()
            cursor = connection.execute(
                """
                UPDATE knowledge_update_proposals
                SET status=?, owner=?, decision_reason=?, decided_at=?,
                    applied_at=?, updated_at=? WHERE id=? AND status=?
                """,
                (
                    new_status, owner, reason,
                    now if new_status in {"approved", "rejected"} else current.decided_at,
                    now if new_status == "applied" else current.applied_at,
                    now, proposal_id, current.status,
                ),
            )
            if cursor.rowcount != 1:
                raise ValueError("knowledge proposal changed concurrently")
        return self.get_knowledge_update_proposal(proposal_id)

    def enqueue_outbox(self, event: OutboxEvent) -> int:
        with self.transaction():
            return self._enqueue_outbox(event)

    def _enqueue_outbox(self, event: OutboxEvent) -> int:
        self.connection.execute(
            """
            INSERT OR IGNORE INTO outbox_events(
                id, topic, aggregate_type, aggregate_id, event_type,
                payload_json, occurred_at, published_at, attempts, last_error
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                event.id, event.topic, event.aggregate_type, event.aggregate_id,
                event.event_type, _json(event.payload), normalize_timestamp(event.occurred_at),
                normalize_timestamp(event.published_at), event.attempts, event.last_error,
            ),
        )
        row = self.connection.execute("SELECT cursor FROM outbox_events WHERE id=?", (event.id,)).fetchone()
        return int(row[0])

    def list_outbox(
        self,
        *,
        after_cursor: int = 0,
        limit: int = 100,
        topic: str | None = None,
        unpublished_only: bool = False,
    ) -> list[OutboxEvent]:
        clauses = ["cursor > ?"]
        values: list[Any] = [after_cursor]
        if topic:
            clauses.append("topic = ?")
            values.append(topic)
        if unpublished_only:
            clauses.append("published_at IS NULL")
        values.append(limit)
        with self._lock:
            rows = self.connection.execute(
                f"SELECT * FROM outbox_events WHERE {' AND '.join(clauses)} ORDER BY cursor LIMIT ?",
                values,
            ).fetchall()
        return [
            OutboxEvent(
                id=row["id"], cursor=row["cursor"], topic=row["topic"],
                aggregate_type=row["aggregate_type"], aggregate_id=row["aggregate_id"],
                event_type=row["event_type"], payload=json.loads(row["payload_json"]),
                occurred_at=row["occurred_at"], published_at=row["published_at"],
                attempts=row["attempts"], last_error=row["last_error"],
            )
            for row in rows
        ]

    def count_outbox(
        self,
        *,
        after_cursor: int = 0,
        topic: str | None = None,
        unpublished_only: bool = False,
    ) -> int:
        clauses = ["cursor > ?"]
        values: list[Any] = [max(int(after_cursor), 0)]
        if topic:
            clauses.append("topic = ?")
            values.append(topic)
        if unpublished_only:
            clauses.append("published_at IS NULL")
        with self._lock:
            row = self.connection.execute(
                f"SELECT COUNT(*) FROM outbox_events WHERE {' AND '.join(clauses)}",
                values,
            ).fetchone()
        return int(row[0])

    def mark_outbox_published(self, event_id: str, published_at: str | datetime | None = None) -> None:
        with self.transaction() as connection:
            connection.execute(
                "UPDATE outbox_events SET published_at=?, attempts=attempts+1, last_error=NULL WHERE id=?",
                (normalize_timestamp(published_at) or utc_now(), event_id),
            )

    def mark_outbox_failed(self, event_id: str, error: str) -> None:
        with self.transaction() as connection:
            connection.execute(
                "UPDATE outbox_events SET attempts=attempts+1, last_error=? WHERE id=?",
                (error, event_id),
            )

    def import_legacy_directory(self, directory: str | Path) -> LegacyImportReport:
        base = Path(directory)

        def load(name: str, default: Any) -> Any:
            path = base / name
            return json.loads(path.read_text(encoding="utf-8")) if path.exists() else default

        legacy_state = load("state.json", {})
        if isinstance(legacy_state, Mapping):
            enriched_state = {}
            for source_id, value in legacy_state.items():
                item = dict(value) if isinstance(value, Mapping) else value
                if isinstance(item, dict) and item.get("snapshot_path"):
                    snapshot_dir = base / str(item["snapshot_path"])
                    normalized = snapshot_dir / "content.md"
                    if normalized.is_file():
                        item["normalized_path"] = normalized.relative_to(base).as_posix()
                    raw_candidates = sorted(snapshot_dir.glob("raw.*.gz"))
                    if raw_candidates:
                        item["raw_path"] = raw_candidates[0].relative_to(base).as_posix()
                enriched_state[str(source_id)] = item
            legacy_state = enriched_state

        return self.import_legacy_documents(
            inventory=load("inventory.json", {}),
            discovered_sources=load("discovered_sources.json", {}),
            source_registry=load("source_registry.json", {}),
            state=legacy_state,
            policy_summaries=load("policy-summaries.json", {}),
            policy_changes=load("policy-changes.json", []),
            manual_queue=load("scraping-agent/manual-queue.json", []),
            events=load("events.json", []),
        )

    def import_legacy_documents(
        self,
        *,
        inventory: Mapping[str, Any] | None = None,
        discovered_sources: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None = None,
        source_registry: Mapping[str, Any] | None = None,
        state: Mapping[str, Any] | None = None,
        policy_summaries: Mapping[str, Any] | None = None,
        policy_changes: Sequence[Mapping[str, Any]] | None = None,
        manual_queue: Sequence[Mapping[str, Any]] | None = None,
        events: Sequence[Mapping[str, Any]] | None = None,
    ) -> LegacyImportReport:
        source_count = snapshot_count = candidate_count = revision_count = review_count = event_count = 0
        source_docs: dict[str, dict[str, Any]] = {}
        inventory = _mapping(inventory)
        state = _mapping(state)
        registry = _mapping(source_registry)

        raw_sources = inventory.get("sources", {})
        iterable = raw_sources.values() if isinstance(raw_sources, Mapping) else raw_sources
        for item in iterable or ():
            if isinstance(item, Mapping) and item.get("url"):
                source_id = str(item.get("id") or stable_source_id(str(item["url"])))
                source_docs[source_id] = {**dict(item), "id": source_id, "role": "candidate"}

        discovered_values = (
            discovered_sources.values()
            if isinstance(discovered_sources, Mapping)
            else discovered_sources or ()
        )
        for item in discovered_values:
            if not isinstance(item, Mapping) or not item.get("url"):
                continue
            source_id = str(item.get("id") or stable_source_id(str(item["url"])))
            previous = source_docs.get(source_id, {})
            source_docs[source_id] = {
                **previous,
                **dict(item),
                "id": source_id,
                "role": previous.get("role", "candidate"),
            }

        registry_entities = registry.get("entities", {})
        entity_iterable = registry_entities.values() if isinstance(registry_entities, Mapping) else registry_entities
        for entity in entity_iterable or ():
            if not isinstance(entity, Mapping):
                continue
            entity_id = str(entity.get("id") or "")
            for role, key in (
                ("candidate", "candidates"),
                ("trusted-secondary", "trusted_current_sources"),
                ("current-primary", "current"),
            ):
                values = entity.get(key) or []
                if isinstance(values, Mapping):
                    values = [values]
                for item in values:
                    if not isinstance(item, Mapping) or not item.get("url"):
                        continue
                    source_id = str(item.get("source_id") or item.get("id") or stable_source_id(str(item["url"])))
                    previous = source_docs.get(source_id, {})
                    entities = set(previous.get("entity_ids") or ())
                    if entity_id:
                        entities.add(entity_id)
                    source_docs[source_id] = {
                        **previous,
                        **dict(item),
                        "id": source_id,
                        "entity_ids": sorted(entities),
                        "role": role if _ROLE_RANK[role] >= _ROLE_RANK.get(previous.get("role", "historical"), 0) else previous["role"],
                    }

        for source_id, item in state.items():
            if not isinstance(item, Mapping) or not item.get("url"):
                continue
            previous = source_docs.get(str(source_id), {})
            source_docs[str(source_id)] = {**previous, **dict(item), "id": str(source_id)}

        with self.transaction():
            for source_id, item in source_docs.items():
                status = str(item.get("status") or "")
                lifecycle = "active" if status == "ok" else "degraded" if status in {"error", "blocked"} else "discovered"
                if item.get("retired") or item.get("retirement_reason"):
                    lifecycle = "retired"
                role = str(item.get("role") or "candidate")
                existing = self.connection.execute("SELECT * FROM source_endpoints WHERE id=?", (source_id,)).fetchone()
                if existing:
                    if _ROLE_RANK[existing["role"]] > _ROLE_RANK.get(role, 0):
                        role = existing["role"]
                    if existing["lifecycle_state"] == "retired":
                        lifecycle = "retired"
                    elif not status and not item.get("retired") and not item.get("retirement_reason"):
                        lifecycle = existing["lifecycle_state"]
                sha256 = str(item.get("sha256") or "")
                snapshot_path = str(item.get("snapshot_path") or "")
                snapshot_id = None
                if sha256 and snapshot_path:
                    snapshot_id = stable_id("snapshot", source_id, sha256, snapshot_path)
                last_good_snapshot_id = snapshot_id or (existing["last_good_snapshot_id"] if existing else None)
                retirement_reason = item.get("retirement_reason")
                if lifecycle == "retired" and not retirement_reason and existing:
                    retirement_reason = existing["retirement_reason"]
                if existing is None:
                    self.upsert_source(
                        SourceEndpoint(
                            id=source_id,
                            canonical_url=str(item["url"]),
                            display_name=str(item.get("name") or source_id),
                            applies_to_entity_ids=tuple(item.get("entity_ids") or ()),
                            role=role if role in SOURCE_ROLES else "candidate",
                            lifecycle_state=lifecycle,
                            enabled=lifecycle != "retired",
                            last_checked_at=item.get("checked_at"),
                            last_good_snapshot_id=last_good_snapshot_id,
                            consecutive_failures=int(item.get("consecutive_failures", 0) or 0),
                            retirement_reason=retirement_reason,
                            metadata={
                                "category": item.get("category"),
                                "categories": item.get("categories") or (),
                                "knowledge_base_refs": item.get("knowledge_base_refs") or (),
                                "legacy_status": status,
                            },
                        )
                    )
                    source_count += 1
                if snapshot_id:
                    snapshot = ContentSnapshot(
                        id=snapshot_id,
                        source_id=source_id,
                        captured_at=str(item.get("checked_at") or utc_now()),
                        content_sha256=sha256,
                        raw_path=str(item.get("raw_path") or snapshot_path),
                        normalized_path=(
                            str(item.get("normalized_path"))
                            if item.get("normalized_path")
                            else f"{snapshot_path.rstrip('/')}/content.md"
                        ),
                        mime_type=item.get("content_type"),
                        content_bytes=item.get("content_bytes"),
                        complete=status == "ok",
                        metadata={
                            "canonical_url": item.get("canonical_url"),
                            "etag": item.get("etag"),
                            "last_modified": item.get("last_modified"),
                            "status_code": item.get("status_code"),
                        },
                    )
                    snapshot_metadata = _snapshot_metadata(snapshot.metadata)
                    cursor = self.connection.execute(
                        """
                        INSERT OR IGNORE INTO content_snapshots(
                            id, source_id, captured_at, content_sha256, raw_path,
                            normalized_path, mime_type, content_bytes, complete,
                            extractor_version, metadata_json, created_at
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            snapshot.id, snapshot.source_id,
                            normalize_timestamp(snapshot.captured_at),
                            snapshot.content_sha256, snapshot.raw_path,
                            snapshot.normalized_path, snapshot.mime_type,
                            snapshot.content_bytes, int(snapshot.complete),
                            snapshot.extractor_version, _json(snapshot_metadata), utc_now(),
                        ),
                    )
                    snapshot_count += int(cursor.rowcount == 1)

            for item in policy_changes or ():
                if not isinstance(item, Mapping):
                    continue
                business = item.get("business") if isinstance(item.get("business"), Mapping) else {}
                source_id = str(item.get("source_id") or "") or None
                if source_id and source_id not in source_docs:
                    source_id = None
                fact_key = str(item.get("change_key") or item.get("guid") or stable_id("legacy-change", item))
                change_id = stable_id("change", fact_key)
                revision_id = stable_id("revision", "legacy", fact_key)
                existing_revision = self.connection.execute(
                    "SELECT 1 FROM policy_change_revisions WHERE id=?", (revision_id,)
                ).fetchone()
                existing_change = self.connection.execute(
                    "SELECT 1 FROM policy_changes WHERE id=? OR fact_key=?",
                    (change_id, fact_key),
                ).fetchone()
                if existing_revision is not None or existing_change is not None:
                    continue
                self.append_policy_change_revision(
                    PolicyChangeRevision(
                        id=revision_id,
                        change_id=change_id,
                        fact_key=fact_key,
                        source_id=source_id,
                        status="confirmed",
                        occurred_at=str(item.get("detected_at") or utc_now()),
                        headline=str(business.get("headline") or item.get("title") or ""),
                        summary=str(business.get("summary") or item.get("summary") or ""),
                        impact=str(business.get("impact") or ""),
                        recommended_action=str(business.get("action") or ""),
                        metadata={
                            "legacy_guid": item.get("guid"),
                            "entity_kind": item.get("entity_kind"),
                            "entity_key": item.get("entity_key"),
                            "knowledge_base_refs": item.get("knowledge_base_refs") or (),
                        },
                    )
                )
                revision_count += 1

            for item in manual_queue or ():
                if not isinstance(item, Mapping):
                    continue
                budget_details = " ".join(
                    [
                        str(item.get("reason") or ""),
                        str(item.get("failure_kind") or ""),
                        str(item.get("last_failure_kind") or ""),
                        *[
                            f"{attempt.get('failure_kind', '')} {attempt.get('detail', '')}"
                            for attempt in item.get("attempts") or ()
                            if isinstance(attempt, Mapping)
                        ],
                    ]
                )
                if is_browser_budget_exhaustion(
                    item.get("last_failure_kind") or item.get("failure_kind"),
                    budget_details,
                ):
                    continue
                group_key = str(item.get("group_key") or item.get("task_id") or stable_id("legacy-review", item))
                task_id = stable_id("review", "legacy-agent-blocked", group_key)
                existed = self.connection.execute(
                    "SELECT 1 FROM review_tasks WHERE id=?", (task_id,)
                ).fetchone()
                updated_at = normalize_timestamp(item.get("updated_at")) or utc_now()
                due_at = (datetime.fromisoformat(updated_at) + timedelta(hours=24)).isoformat(timespec="microseconds")
                if existed is None:
                    self.open_review_task(
                        ReviewTask(
                            id=task_id,
                            task_type="legacy_agent_blocked",
                            reason=str(item.get("reason") or "legacy blocked fetch requires review"),
                            created_at=updated_at,
                            due_at=due_at,
                            status="cancelled",
                            priority=80 if "human_verification" in group_key else 60,
                            resolution=(
                                "legacy AgentStateStore blocked record retained for audit; "
                                "no source-bound recovery control"
                            ),
                            metadata={
                                "audit_only": True,
                                "group_key": group_key,
                                "site_key": item.get("site_key"),
                                "task_ids": item.get("task_ids") or ([item.get("task_id")] if item.get("task_id") else []),
                                "occurrences": item.get("occurrences") or 1,
                            },
                        )
                    )
                    review_count += 1
                for attempt in item.get("attempts") or ():
                    if not isinstance(attempt, Mapping):
                        continue
                    action_id = stable_id(
                        "review-action", "legacy", task_id, attempt.get("attempt"),
                        attempt.get("strategy"), attempt.get("status"), attempt.get("failure_kind"),
                    )
                    self.connection.execute(
                        """
                        INSERT OR IGNORE INTO review_task_actions(
                            id, task_id, action_type, actor, payload_json, created_at
                        ) VALUES (?,?,?,?,?,?)
                        """,
                        (
                            action_id, task_id, "legacy_fetch_attempt", "scraping-agent",
                            _json(dict(attempt)), updated_at,
                        ),
                    )

            for item in events or ():
                if not isinstance(item, Mapping):
                    continue
                guid = str(item.get("guid") or stable_id("legacy-event", item))
                event_id = stable_id("event", "legacy", guid)
                existed = self.connection.execute("SELECT 1 FROM outbox_events WHERE id=?", (event_id,)).fetchone()
                self._enqueue_outbox(
                    OutboxEvent(
                        id=event_id,
                        topic="legacy.monitor.events",
                        aggregate_type="LegacyMonitorEvent",
                        aggregate_id=guid,
                        event_type="legacy.event_imported",
                        occurred_at=str(item.get("detected_at") or utc_now()),
                        payload={
                            "guid": guid,
                            "title": item.get("title"),
                            "url": item.get("url"),
                            "summary": item.get("summary"),
                        },
                    )
                )
                event_count += int(existed is None)

        return LegacyImportReport(
            sources=source_count,
            snapshots=snapshot_count,
            candidates=candidate_count,
            policy_revisions=revision_count,
            review_tasks=review_count,
            outbox_events=event_count,
        )


__all__ = [
    "ChangeCandidate",
    "CheckRun",
    "ContentSnapshot",
    "EvidenceBundle",
    "KnowledgeUpdateProposal",
    "LegacyImportReport",
    "MonitorStore",
    "OutboxEvent",
    "PolicyChangeRevision",
    "ReviewTask",
    "SOURCE_RECOVERY_OWNER",
    "CHANGE_EVIDENCE_OWNER",
    "KNOWLEDGE_OPERATIONS_OWNER",
    "SourceEndpoint",
    "canonicalize_url",
    "is_browser_budget_exhaustion",
    "stable_id",
    "stable_source_id",
]
