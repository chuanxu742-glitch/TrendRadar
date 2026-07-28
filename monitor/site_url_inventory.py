from __future__ import annotations

import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping
from urllib.parse import parse_qsl, unquote, urlsplit


SCHEMA_VERSION = 1

_BLOCKED_EXTENSIONS = (
    ".7z", ".avi", ".avif", ".bmp", ".css", ".csv", ".eot", ".exe", ".gif", ".gz",
    ".ico", ".jpeg", ".jpg", ".js", ".map", ".mov", ".mp3", ".mp4", ".ogg", ".png",
    ".rar", ".svg", ".tar", ".tgz", ".ttf", ".webm", ".webp", ".woff", ".woff2", ".zip",
)
_VOLATILE_PATH_MARKERS = (
    "/account", "/admin", "/auth", "/cart", "/checkout", "/login", "/logout",
    "/register", "/search", "/sign-in", "/signin", "/sign-up", "/signup",
    "/wp-admin",
)
_VOLATILE_QUERY_KEYS = {
    "_", "callback", "cursor", "filter", "page", "pageindex", "pagenumber",
    "preview", "q", "query", "search", "session", "sessionid", "sort", "token",
}
_POLICY_CONTEXT_TERMS = (
    "policy", "policies", "rule", "rules", "regulation", "requirement", "requirements",
    "fee", "fees", "certificate", "vaccination", "quarantine", "permit", "baggage",
    "cargo", "special service", "travel information", "政策", "规定", "规则", "要求",
    "费用", "证书", "疫苗", "检疫", "许可证", "行李", "货运", "特殊服务", "旅行信息",
)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_timestamp(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def stable_url_decision(url: str) -> tuple[bool, str]:
    """Classify whether a URL represents a stable, fetchable content page."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return False, "invalid_url"
    if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
        return False, "unsupported_scheme"
    path = unquote(parts.path or "/").lower()
    if path.endswith(_BLOCKED_EXTENSIONS):
        return False, "static_asset"
    if any(marker in path for marker in _VOLATILE_PATH_MARKERS):
        return False, "interactive_or_search_page"
    if len(url) > 1000:
        return False, "url_too_long"
    query_items = parse_qsl(parts.query, keep_blank_values=True)
    if len(query_items) > 8:
        return False, "volatile_query"
    if any(key.lower() in _VOLATILE_QUERY_KEYS for key, _ in query_items):
        return False, "volatile_query"
    return True, ""


def _matched_terms(text: str, terms: Iterable[str]) -> list[str]:
    lowered = unquote(text).lower()
    matched = []
    for term in terms:
        normalized = term.lower()
        if not normalized:
            continue
        if normalized.isascii() and any(character.isalnum() for character in normalized):
            present = bool(re.search(
                rf"(?<![a-z0-9]){re.escape(normalized)}(?![a-z0-9])",
                lowered,
            ))
        else:
            present = normalized in lowered
        if present:
            matched.append(term)
    return list(dict.fromkeys(matched))[:20]


def classify_url_relevance(
    url: str,
    *,
    title: str = "",
    anchor: str = "",
    parent_context: str = "",
    direct_terms: Iterable[str] = (),
    hub_terms: Iterable[str] = (),
) -> dict[str, Any]:
    """Score URL-level evidence before downloading the target page."""
    url_text = f"{urlsplit(url).path} {urlsplit(url).query}"
    context_text = f"{title} {anchor} {parent_context}".strip()
    direct_url = _matched_terms(url_text, direct_terms)
    direct_context = _matched_terms(context_text, direct_terms)
    hub_url = _matched_terms(url_text, hub_terms)
    hub_context = _matched_terms(context_text, hub_terms)
    policy_context = _matched_terms(f"{url_text} {context_text}", _POLICY_CONTEXT_TERMS)

    score = 0
    if direct_url:
        score = max(score, 90)
    if direct_context:
        score = max(score, 80)
    if hub_url:
        score = max(score, 45)
    if hub_context:
        score = max(score, 40)
    if policy_context:
        score = max(score, 35)
    if urlsplit(url).path.rstrip("/") == "":
        score = max(score, 10)
    score = min(
        score
        + min(len(direct_url) + len(direct_context), 4) * 2
        + min(len(policy_context), 3),
        100,
    )
    relevance = "high" if score >= 70 else "medium" if score >= 30 else "low"
    return {
        "relevance": relevance,
        "relevance_score": score,
        "matched_terms": list(dict.fromkeys(
            direct_url + direct_context + hub_url + hub_context + policy_context
        ))[:20],
    }


def merge_site_url_record(previous: Mapping[str, Any], incoming: Mapping[str, Any]) -> dict[str, Any]:
    merged = {**previous, **incoming}
    for key in (
        "source_ids", "entity_ids", "discovery_methods", "parent_urls", "anchor_samples",
        "matched_terms",
    ):
        merged[key] = list(dict.fromkeys(
            [str(item) for item in previous.get(key, []) if str(item)]
            + [str(item) for item in incoming.get(key, []) if str(item)]
        ))[:20]
    if previous.get("first_seen_at"):
        merged["first_seen_at"] = previous["first_seen_at"]
    for key in (
        "last_fetched_at", "last_scheduled_at", "next_sample_at", "schedule_reason",
        "last_http_status", "last_fetch_mode",
    ):
        if previous.get(key) and not incoming.get(key):
            merged[key] = previous[key]
    if previous.get("last_fetched_at") and not incoming.get("last_fetched_at"):
        for key in ("fetch_status", "content_relevant", "last_skip_reason"):
            if key in previous:
                merged[key] = previous[key]
    elif previous.get("last_skip_reason") and not incoming.get("last_skip_reason"):
        merged["last_skip_reason"] = previous["last_skip_reason"]
    merged["fetch_count"] = max(
        int(previous.get("fetch_count", 0) or 0),
        int(incoming.get("fetch_count", 0) or 0),
    )
    previous_score = int(previous.get("relevance_score", 0) or 0)
    incoming_score = int(incoming.get("relevance_score", 0) or 0)
    if previous_score > incoming_score:
        for key in ("relevance", "relevance_score", "fetch_policy"):
            merged[key] = previous.get(key)
    return merged


def register_site_url(
    inventory: dict[str, dict[str, Any]],
    url: str,
    *,
    origin: str,
    source_id: str,
    entity_ids: Iterable[str],
    discovery_method: str,
    parent_url: str = "",
    anchor: str = "",
    title: str = "",
    parent_context: str = "",
    direct_terms: Iterable[str] = (),
    hub_terms: Iterable[str] = (),
    observed_at: str = "",
) -> dict[str, Any]:
    observed = observed_at or now_iso()
    stable, skip_reason = stable_url_decision(url)
    classification = classify_url_relevance(
        url,
        title=title,
        anchor=anchor,
        parent_context=parent_context,
        direct_terms=direct_terms,
        hub_terms=hub_terms,
    )
    record = {
        "schema_version": SCHEMA_VERSION,
        "url": url,
        "origin": origin,
        "host": (urlsplit(url).hostname or "").lower(),
        "stable": stable,
        "source_ids": [source_id] if source_id else [],
        "entity_ids": [str(item) for item in entity_ids if str(item)],
        "first_seen_at": observed,
        "last_seen_at": observed,
        "discovery_methods": [discovery_method] if discovery_method else [],
        "parent_urls": [parent_url] if parent_url else [],
        "anchor_samples": [anchor[:300]] if anchor else [],
        **classification,
        "fetch_policy": (
            "full" if classification["relevance"] == "high"
            else "scheduled" if classification["relevance"] == "medium"
            else "sample"
        ),
        "fetch_status": "unread" if stable else "skipped",
        "fetch_count": 0,
        "last_fetched_at": "",
        "last_scheduled_at": "",
        "last_skip_reason": skip_reason,
    }
    merged = merge_site_url_record(inventory.get(url, {}), record)
    inventory[url] = merged
    return merged


def mark_scheduled(record: Mapping[str, Any], reason: str, scheduled_at: str = "") -> dict[str, Any]:
    updated = dict(record)
    updated["last_scheduled_at"] = scheduled_at or now_iso()
    updated["schedule_reason"] = reason
    if updated.get("fetch_status") == "unread":
        updated["fetch_status"] = "scheduled"
    return updated


def mark_fetch_result(
    record: Mapping[str, Any],
    result: Mapping[str, Any],
    *,
    sampled_again_after_seconds: int,
) -> dict[str, Any]:
    updated = dict(record)
    checked_at = str(result.get("checked_at") or now_iso())
    status = str(result.get("status") or "error")
    updated["last_fetched_at"] = checked_at
    updated["fetch_count"] = int(updated.get("fetch_count", 0) or 0) + 1
    updated["last_http_status"] = result.get("status_code")
    updated["last_fetch_mode"] = result.get("fetch_mode", "")
    if status == "ok":
        relevant = bool(result.get("validation", {}).get("topic_relevant"))
        updated["fetch_status"] = "fetched"
        updated["content_relevant"] = relevant
        updated["last_skip_reason"] = "" if relevant else "content_not_pet_policy"
    else:
        updated["fetch_status"] = "unread"
        updated["content_relevant"] = None
        updated["last_skip_reason"] = str(
            result.get("deferred_reason") or result.get("error") or status
        )[:300]
    if updated.get("relevance") == "low":
        timestamp = parse_timestamp(checked_at) or datetime.now(timezone.utc)
        updated["next_sample_at"] = (
            timestamp + timedelta(seconds=max(sampled_again_after_seconds, 1))
        ).isoformat()
    return updated


def select_due_records(
    inventory: Mapping[str, Mapping[str, Any]],
    *,
    origin: str,
    relevance: str,
    limit: int,
    minimum_interval_seconds: int,
    active_urls: Iterable[str] = (),
    current_time: datetime | None = None,
) -> list[str]:
    if limit <= 0:
        return []
    current = current_time or datetime.now(timezone.utc)
    active = set(active_urls)
    candidates: list[tuple[float, str]] = []
    for url, record in inventory.items():
        if (
            record.get("origin") != origin
            or record.get("relevance") != relevance
            or not record.get("stable", True)
            or url in active
        ):
            continue
        next_sample = parse_timestamp(record.get("next_sample_at"))
        if next_sample and next_sample > current:
            continue
        scheduled = parse_timestamp(record.get("last_scheduled_at"))
        if scheduled and (current - scheduled).total_seconds() < minimum_interval_seconds:
            continue
        fetched = parse_timestamp(record.get("last_fetched_at"))
        candidates.append((fetched.timestamp() if fetched else 0.0, url))
    candidates.sort(key=lambda item: (item[0], item[1]))
    return [url for _, url in candidates[:limit]]


def skip_reason_category(value: Any) -> str:
    reason = str(value or "").strip()
    lowered = reason.lower()
    if not reason:
        return ""
    if "browser budget exhausted" in lowered:
        return "browser_budget_exhausted"
    if "browser failed" in lowered:
        return "browser_fetch_failed"
    if lowered.startswith("http "):
        return "http_error"
    if "javascript shell" in lowered:
        return "javascript_content_incomplete"
    if "access denied" in lowered or "访问受限" in reason:
        return "access_restricted"
    return reason if len(reason) <= 80 else reason[:77] + "..."


def inventory_summary(inventory: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    stable_records = [record for record in inventory.values() if record.get("stable", True)]
    relevance = Counter(str(record.get("relevance") or "low") for record in stable_records)
    fetched = sum(record.get("fetch_status") == "fetched" for record in stable_records)
    sampled = sum(
        record.get("relevance") == "low" and record.get("fetch_status") == "fetched"
        for record in stable_records
    )
    skipped = Counter(
        skip_reason_category(record.get("last_skip_reason"))
        for record in inventory.values()
        if record.get("last_skip_reason")
    )
    total = len(stable_records)
    return {
        "schema_version": SCHEMA_VERSION,
        "total_urls": len(inventory),
        "stable_urls": total,
        "high_relevance": relevance["high"],
        "medium_relevance": relevance["medium"],
        "low_relevance": relevance["low"],
        "fetched_urls": fetched,
        "unread_urls": max(total - fetched, 0),
        "fetch_coverage": round(fetched / max(total, 1), 4),
        "low_relevance_sampled": sampled,
        "skipped_urls": sum(not record.get("stable", True) for record in inventory.values()),
        "skip_reasons": dict(skipped.most_common(12)),
        "origins": len({
            str(record.get("origin")) for record in inventory.values() if record.get("origin")
        }),
        "updated_at": now_iso(),
    }
