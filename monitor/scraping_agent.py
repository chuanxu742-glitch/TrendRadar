from __future__ import annotations

import json
import os
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Literal


StepStatus = Literal["success", "not_modified", "retry", "terminal", "blocked", "deferred"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def _safe_detail(value: str) -> str:
    value = re.sub(r"sk-[A-Za-z0-9_-]+", "sk-[REDACTED]", value)
    return re.sub(r"\s+", " ", value).strip()[:300]


def _is_browser_budget_entry(item: dict[str, Any]) -> bool:
    attempts = item.get("attempts", [])
    details = [
        str(item.get("reason", "")),
        str(item.get("failure_kind", "")),
        str(item.get("last_failure_kind", "")),
    ]
    if isinstance(attempts, list):
        for attempt in attempts:
            if isinstance(attempt, dict):
                details.extend((
                    str(attempt.get("strategy", "")),
                    str(attempt.get("failure_kind", "")),
                    str(attempt.get("detail", "")),
                ))
    combined = " ".join(details).lower()
    budget_kind = any(
        detail.strip().lower() == "budget"
        for detail in details
    )
    return (
        "browser budget exhausted" in combined
        or "browser capacity budget exhausted" in combined
        or (
            budget_kind
            and any(term in combined for term in ("browser", "dynamic", "stealth"))
        )
        or (
            "after budget" in combined
            and ("dynamic" in combined or "stealth" in combined)
        )
    )


def _manual_group_key(item: dict[str, Any]) -> str:
    host = str(item.get("site_key", "")).split("/", 1)[0].lower()
    attempts = item.get("attempts", [])
    failure_kind = str(item.get("failure_kind", ""))
    if isinstance(attempts, list) and attempts:
        failure_kind = failure_kind or str(attempts[-1].get("failure_kind", ""))
    if not failure_kind:
        reason = str(item.get("reason", "")).lower()
        failure_kind = next(
            (kind for kind in (
                "human_verification", "authentication_required", "access_forbidden",
                "rate_limited", "server_error", "waf", "budget", "javascript_shell",
                "incomplete_content", "network", "timeout",
            ) if kind in reason),
            "other",
        )
    return f"{host}:{failure_kind}"


def _compact_manual_entries(queue: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compacted: dict[str, dict[str, Any]] = {}
    for raw in queue:
        entry = dict(raw)
        key = _manual_group_key(entry)
        existing = compacted.get(key)
        if existing is None:
            entry["group_key"] = key
            entry["task_ids"] = list(dict.fromkeys(entry.get("task_ids", [entry.get("task_id", "")])))
            entry["occurrences"] = max(int(entry.get("occurrences", 1)), 1)
            compacted[key] = entry
            continue
        task_ids = list(dict.fromkeys(
            existing.get("task_ids", []) + entry.get("task_ids", [entry.get("task_id", "")])
        ))[-50:]
        newer = entry if str(entry.get("updated_at", "")) >= str(existing.get("updated_at", "")) else existing
        compacted[key] = {
            **newer,
            "group_key": key,
            "task_ids": task_ids,
            "task_id": task_ids[-1] if task_ids else newer.get("task_id", ""),
            "occurrences": int(existing.get("occurrences", 1)) + int(entry.get("occurrences", 1)),
        }
    return sorted(compacted.values(), key=lambda value: str(value.get("updated_at", "")))[-500:]


def _synchronized(method: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(method)
    def wrapped(self: "AgentStateStore", *args: Any, **kwargs: Any) -> Any:
        with AgentStateStore._cache_lock:
            return method(self, *args, **kwargs)
    return wrapped


@dataclass(frozen=True)
class LoopLimits:
    max_attempts: int = 3
    max_duration_seconds: int = 180
    max_same_failure: int = 2


@dataclass
class StepResult:
    status: StepStatus
    strategy: str
    output: Any = None
    failure_kind: str = ""
    detail: str = ""
    suggested_strategies: tuple[str, ...] = ()
    metrics: dict[str, Any] = field(default_factory=dict)
    retry_same_strategy: bool = False


@dataclass
class LoopResult:
    status: StepStatus
    strategy: str
    output: Any
    attempts: list[dict[str, Any]]
    stop_reason: str = ""
    last_error: str = ""


class AgentStateStore:
    """Persist only decisions and metrics; never persist page content or credentials."""

    _cache_lock = threading.RLock()
    _status_cache: dict[str, dict[str, Any]] = {}

    def __init__(self, directory: Path | None) -> None:
        self.directory = directory
        self.profiles_path = directory / "site-profiles.json" if directory else None
        self.runs_path = directory / "runs.jsonl" if directory else None
        self.manual_path = directory / "manual-queue.json" if directory else None
        self.deferred_path = directory / "deferred-queue.json" if directory else None
        self.status_path = directory / "status.json" if directory else None

    @staticmethod
    def _load(path: Path | None, default: Any) -> Any:
        if path is None:
            return default
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return default

    @staticmethod
    def _save(path: Path | None, value: Any) -> None:
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(path)

    @_synchronized
    def preferred_strategy(self, site_key: str, allowed: tuple[str, ...]) -> str:
        profiles = self._load(self.profiles_path, {})
        strategy = str(profiles.get(site_key, {}).get("preferred_strategy", ""))
        return strategy if strategy in allowed else ""

    @_synchronized
    def promote(self, site_key: str, strategy: str, metrics: dict[str, Any] | None = None) -> None:
        if self.profiles_path is None:
            return
        profiles = self._load(self.profiles_path, {})
        current = profiles.get(site_key, {})
        if not current and strategy == "static":
            return
        active = str(current.get("active_strategy") or current.get("preferred_strategy") or "")
        now = _now_iso()
        metrics = metrics or {}
        if active == strategy:
            current["active_successes"] = int(current.get("active_successes", 0)) + 1
            current["active_failures"] = 0
            current["confidence"] = min(float(current.get("confidence", 0.7)) + 0.05, 1.0)
        else:
            candidate = current.get("candidate", {})
            successes = int(candidate.get("successes", 0)) + 1 if candidate.get("strategy") == strategy else 1
            candidate = {
                "strategy": strategy,
                "successes": successes,
                "first_seen_at": candidate.get("first_seen_at", now) if candidate.get("strategy") == strategy else now,
                "last_verified_at": now,
                "template_fingerprint": metrics.get("template_fingerprint", ""),
                "completeness_score": metrics.get("completeness_score"),
            }
            current["candidate"] = candidate
            if successes >= 2:
                history = list(current.get("history", []))
                if active:
                    history.append(
                        {
                            "strategy": active,
                            "retired_at": now,
                            "version": int(current.get("version", 1)),
                        }
                    )
                active = strategy
                current.update(
                    {
                        "active_strategy": active,
                        "preferred_strategy": active,
                        "version": int(current.get("version", 0)) + 1,
                        "active_successes": successes,
                        "active_failures": 0,
                        "confidence": 0.8,
                        "history": history[-10:],
                    }
                )
                current.pop("candidate", None)
        current.update(
            {
                "verified_at": now,
                "success_count": int(current.get("success_count", 0)) + 1,
                "template_fingerprint": metrics.get("template_fingerprint", current.get("template_fingerprint", "")),
                "last_completeness_score": metrics.get("completeness_score"),
            }
        )
        profiles[site_key] = current
        self._save(self.profiles_path, profiles)

    @_synchronized
    def record_failure(self, site_key: str, strategy: str, failure_kind: str) -> None:
        if self.profiles_path is None:
            return
        profiles = self._load(self.profiles_path, {})
        current = profiles.get(site_key)
        if not isinstance(current, dict):
            return
        active = str(current.get("active_strategy") or current.get("preferred_strategy") or "")
        if strategy == active:
            failures = int(current.get("active_failures", 0)) + 1
            current["active_failures"] = failures
            current["confidence"] = max(float(current.get("confidence", 0.8)) - 0.2, 0.0)
            if failures >= 2:
                history = list(current.get("history", []))
                previous = history.pop() if history else {}
                fallback = str(previous.get("strategy") or "static")
                current.update(
                    {
                        "active_strategy": fallback,
                        "preferred_strategy": fallback,
                        "active_failures": 0,
                        "status": "rolled_back",
                        "rolled_back_at": _now_iso(),
                        "rollback_reason": failure_kind,
                        "history": history,
                    }
                )
        candidate = current.get("candidate", {})
        if isinstance(candidate, dict) and candidate.get("strategy") == strategy:
            candidate["failures"] = int(candidate.get("failures", 0)) + 1
            if candidate["failures"] >= 2:
                current.pop("candidate", None)
        profiles[site_key] = current
        self._save(self.profiles_path, profiles)

    @_synchronized
    def append_attempt(self, task_id: str, site_key: str, attempt: dict[str, Any]) -> None:
        if self.runs_path is None:
            return
        significant = attempt.get("status") != "success" or attempt.get("strategy") != "static"
        record = {
            "recorded_at": _now_iso(),
            "task_id": task_id,
            "site_key": site_key,
            **attempt,
        }
        if significant:
            self.runs_path.parent.mkdir(parents=True, exist_ok=True)
            with self.runs_path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(record, ensure_ascii=False) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
        cache_key = str(self.status_path.resolve()) if self.status_path else ""
        with self._cache_lock:
            status = self._status_cache.get(cache_key)
            if status is None:
                status = self._load(self.status_path, {})
                self._status_cache[cache_key] = status
            status["updated_at"] = _now_iso()
            status["attempts"] = int(status.get("attempts", 0)) + 1
            key = str(attempt.get("status", "unknown"))
            counts = status.setdefault("status_counts", {})
            counts[key] = int(counts.get(key, 0)) + 1
            failures = status.setdefault("failure_counts", {})
            failure_kind = str(attempt.get("failure_kind", ""))
            if failure_kind:
                failures[failure_kind] = int(failures.get(failure_kind, 0)) + 1
            recent = list(status.get("recent", []))
            recent.append({"task_id": task_id, "site_key": site_key, **attempt})
            status["recent"] = recent[-30:]
            if significant or status["attempts"] % 25 == 0:
                self._save(self.status_path, status)

    @_synchronized
    def resolve_manual(self, task_id: str) -> None:
        if self.manual_path is None:
            return
        queue = self._load(self.manual_path, [])
        changed = False
        filtered = []
        for entry in queue:
            task_ids = list(entry.get("task_ids", []))
            if entry.get("task_id") == task_id or task_id in task_ids:
                changed = True
                task_ids = [value for value in task_ids if value != task_id]
                if task_ids:
                    entry = {**entry, "task_ids": task_ids, "task_id": task_ids[-1]}
                    filtered.append(entry)
                continue
            filtered.append(entry)
        if changed:
            self._save(self.manual_path, filtered)

    @_synchronized
    def compact_manual_queue(self) -> int:
        if self.manual_path is None:
            return 0
        queue = self._load(self.manual_path, [])
        queue = queue if isinstance(queue, list) else []
        deferred = [dict(item) for item in queue if isinstance(item, dict) and _is_browser_budget_entry(item)]
        if deferred:
            archived = self._load(self.deferred_path, [])
            archived = archived if isinstance(archived, list) else []
            repaired_at = _now_iso()
            archived.extend({
                **item,
                "state": "deferred",
                "deferred_reason": "browser_capacity_budget_exhausted",
                "reclassified_at": repaired_at,
            } for item in deferred)
            self._save(self.deferred_path, archived[-2000:])
        compacted = _compact_manual_entries([
            item for item in queue
            if isinstance(item, dict) and not _is_browser_budget_entry(item)
        ])
        self._save(self.manual_path, compacted)
        return len(compacted)

    @_synchronized
    def enqueue_manual(self, task_id: str, site_key: str, reason: str, attempts: list[dict[str, Any]]) -> None:
        if self.manual_path is None:
            return
        if _is_browser_budget_entry({"reason": reason, "attempts": attempts}):
            return
        queue = self._load(self.manual_path, [])
        last_attempt = attempts[-1] if attempts else {}
        actionable_attempt = next(
            (
                attempt for attempt in reversed(attempts)
                if str(attempt.get("failure_kind", "")) not in {"", "budget"}
            ),
            last_attempt,
        )
        metrics = actionable_attempt.get("metrics", {}) if isinstance(actionable_attempt, dict) else {}
        failure_kind = (
            str(actionable_attempt.get("failure_kind", ""))
            if isinstance(actionable_attempt, dict)
            else ""
        )
        last_failure_kind = (
            str(last_attempt.get("failure_kind", "")) if isinstance(last_attempt, dict) else ""
        )
        required_action = {
            "human_verification": "authorized_human_verification",
            "cloudflare_challenge": "authorized_human_verification",
            "authentication_required": "authorized_authentication",
            "access_forbidden": "review_access_policy",
        }.get(failure_kind, "review_and_resume")
        item = {
            "task_id": task_id,
            "task_ids": [task_id],
            "site_key": site_key,
            "reason": _safe_detail(reason),
            "state": "paused",
            "failure_kind": failure_kind,
            "last_failure_kind": last_failure_kind,
            "status_code": metrics.get("status_code") if isinstance(metrics, dict) else None,
            "required_action": required_action,
            "updated_at": _now_iso(),
            "attempts": attempts[-5:],
            "occurrences": 1,
        }
        queue = [entry for entry in queue if entry.get("task_id") != task_id]
        queue.append(item)
        self._save(self.manual_path, _compact_manual_entries(queue))


class AgentLoop:
    def __init__(self, store: AgentStateStore, limits: LoopLimits | None = None) -> None:
        self.store = store
        self.limits = limits or LoopLimits()

    @staticmethod
    def _choose_next(
        outcome: StepResult, allowed: tuple[str, ...], attempted: set[str]
    ) -> str:
        if outcome.retry_same_strategy and outcome.strategy in allowed:
            return outcome.strategy
        candidates = (*outcome.suggested_strategies, *allowed)
        return next((strategy for strategy in candidates if strategy in allowed and strategy not in attempted), "")

    @staticmethod
    def _invalid_success_reason(outcome: StepResult) -> str:
        """Reject transport results that were incorrectly labelled as successful.

        Generic AgentLoop users may omit HTTP metrics. Fetching users that provide
        them must prove a usable 2xx response before learning a strategy or clearing
        a paused task.
        """
        status_code = outcome.metrics.get("status_code")
        if status_code is not None:
            try:
                code = int(status_code)
            except (TypeError, ValueError):
                return "invalid HTTP status metric"
            if not 200 <= code < 300:
                return f"HTTP {code} cannot be an agent success"
        if outcome.metrics.get("complete") is False:
            return "incomplete content cannot be an agent success"
        if outcome.metrics.get("topic_matched") is False:
            return "topic validation failure cannot be an agent success"
        return ""

    def run(
        self,
        task_id: str,
        site_key: str,
        strategies: tuple[str, ...],
        execute: Callable[[str, int], StepResult],
    ) -> LoopResult:
        if not strategies:
            raise ValueError("agent loop requires at least one strategy")
        preferred = self.store.preferred_strategy(site_key, strategies)
        strategy = preferred or strategies[0]
        started = time.monotonic()
        attempted: set[str] = set()
        attempts: list[dict[str, Any]] = []
        failure_counts: dict[str, int] = {}
        last_output: Any = None
        last_error = ""

        for number in range(1, self.limits.max_attempts + 1):
            if time.monotonic() - started >= self.limits.max_duration_seconds:
                if attempts and attempts[-1].get("failure_kind") == "budget":
                    return LoopResult(
                        "deferred", strategy, last_output, attempts,
                        "browser capacity budget exhausted", last_error,
                    )
                reason = "agent time budget exhausted"
                self.store.enqueue_manual(task_id, site_key, reason, attempts)
                return LoopResult("blocked", strategy, last_output, attempts, reason, last_error)
            attempted.add(strategy)
            try:
                outcome = execute(strategy, number)
            except Exception as exc:
                outcome = StepResult(
                    status="retry",
                    strategy=strategy,
                    failure_kind=type(exc).__name__.lower(),
                    detail=str(exc),
                )
            if outcome.status == "success":
                invalid_success = self._invalid_success_reason(outcome)
                if invalid_success:
                    outcome = StepResult(
                        status="terminal",
                        strategy=outcome.strategy,
                        output=outcome.output,
                        failure_kind="invalid_success",
                        detail=invalid_success,
                        metrics=outcome.metrics,
                    )
            last_output = outcome.output if outcome.output is not None else last_output
            last_error = _safe_detail(outcome.detail)
            attempt = {
                "attempt": number,
                "strategy": strategy,
                "status": outcome.status,
                "failure_kind": outcome.failure_kind,
                "detail": last_error,
                "metrics": outcome.metrics,
                "duration_ms": int((time.monotonic() - started) * 1000),
            }
            attempts.append(attempt)
            self.store.append_attempt(task_id, site_key, attempt)

            if outcome.status == "success":
                self.store.promote(site_key, strategy, outcome.metrics)
                self.store.resolve_manual(task_id)
                return LoopResult("success", strategy, last_output, attempts)
            if outcome.status == "not_modified":
                return LoopResult("not_modified", strategy, last_output, attempts)
            if outcome.failure_kind != "budget":
                self.store.record_failure(site_key, strategy, outcome.failure_kind)
            if outcome.status == "terminal":
                return LoopResult("terminal", strategy, last_output, attempts, last_error, last_error)
            if outcome.status == "blocked":
                self.store.enqueue_manual(task_id, site_key, last_error or "manual action required", attempts)
                return LoopResult("blocked", strategy, last_output, attempts, last_error, last_error)

            signature = f"{strategy}:{outcome.failure_kind}:{last_error}"
            failure_counts[signature] = failure_counts.get(signature, 0) + 1
            if failure_counts[signature] >= self.limits.max_same_failure:
                reason = f"same failure repeated {failure_counts[signature]} times"
                if outcome.failure_kind == "budget":
                    return LoopResult(
                        "deferred", strategy, last_output, attempts,
                        "browser capacity budget exhausted", last_error,
                    )
                self.store.enqueue_manual(task_id, site_key, reason, attempts)
                return LoopResult("blocked", strategy, last_output, attempts, reason, last_error)

            strategy = self._choose_next(outcome, strategies, attempted)
            if not strategy:
                reason = (
                    f"no untried strategy remains after {outcome.failure_kind}: {last_error}"
                ).rstrip(": ")
                if outcome.failure_kind == "budget":
                    return LoopResult(
                        "deferred", attempts[-1]["strategy"], last_output, attempts,
                        "browser capacity budget exhausted", last_error,
                    )
                self.store.enqueue_manual(task_id, site_key, reason, attempts)
                return LoopResult("blocked", attempts[-1]["strategy"], last_output, attempts, reason, last_error)

        reason = f"agent attempt budget exhausted; last error: {last_error}".rstrip(": ")
        if attempts and attempts[-1].get("failure_kind") == "budget":
            return LoopResult(
                "deferred", strategy, last_output, attempts,
                "browser capacity budget exhausted", last_error,
            )
        self.store.enqueue_manual(task_id, site_key, reason, attempts)
        return LoopResult("blocked", strategy, last_output, attempts, reason, last_error)
