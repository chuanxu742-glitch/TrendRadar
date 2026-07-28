# coding=utf-8
"""Build evidence-backed policy change digests for business-facing surfaces."""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Mapping, Sequence

try:
    from .entity_names import entity_names as resolve_entity_names
except ImportError:
    from entity_names import entity_names as resolve_entity_names


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SUMMARY_RULE_RE = re.compile(
    r"(?:^|[。；\n])\s*(原规则|旧规则|新规则|新增规则|删除规则)\s*[：:]\s*"
    r"(.*?)(?=(?:[。；\n]\s*(?:原规则|旧规则|新规则|新增规则|删除规则)\s*[：:])|$)"
)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _rules(value: Any) -> list[str]:
    if isinstance(value, str):
        values = re.split(r"[\r\n]+|；", value)
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        values = value
    else:
        values = ()
    return list(
        dict.fromkeys(
            re.sub(r"\s+", " ", str(item)).strip(" ；。")
            for item in values
            if str(item).strip(" ；。")
        )
    )


def _summary_rules(summary: Any) -> tuple[list[str], list[str]]:
    old_rules: list[str] = []
    new_rules: list[str] = []
    for label, value in _SUMMARY_RULE_RE.findall(str(summary or "")):
        if label in {"原规则", "旧规则"}:
            old_rules.extend(_rules(value))
        elif label in {"新规则", "新增规则"}:
            new_rules.extend(_rules(value))
    return list(dict.fromkeys(old_rules)), list(dict.fromkeys(new_rules))


def _iso_date(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    candidate = text[:10]
    if not _DATE_RE.fullmatch(candidate):
        return ""
    try:
        return date.fromisoformat(candidate).isoformat()
    except ValueError:
        return ""


def policy_digest_period(
    period: str,
    *,
    today: date | None = None,
) -> tuple[str, str]:
    """Resolve a local calendar period into inclusive ISO dates."""

    normalized = str(period or "").strip().lower()
    if normalized not in {"", "all", "daily", "weekly", "monthly"}:
        raise ValueError("period must be daily, weekly, monthly, all, or empty")
    if normalized in {"", "all"}:
        return "", ""
    current = today or date.today()
    if normalized == "daily":
        start = current
    elif normalized == "weekly":
        start = current - timedelta(days=current.weekday())
    else:
        start = current.replace(day=1)
    return start.isoformat(), current.isoformat()


def _date_fields(metadata: Mapping[str, Any], occurred_at: Any) -> dict[str, str]:
    announcement_date_source = str(metadata.get("announcement_date_source") or "").strip()
    effective_date_source = str(metadata.get("effective_date_source") or "").strip()
    announcement_date = (
        _iso_date(metadata.get("announcement_date") or metadata.get("announced_date"))
        if announcement_date_source
        else ""
    )
    effective_date = (
        _iso_date(metadata.get("effective_date"))
        if effective_date_source
        else ""
    )
    detected_date = _iso_date(occurred_at)
    if effective_date:
        change_date, date_kind = effective_date, "effective"
    elif announcement_date:
        change_date, date_kind = announcement_date, "announcement"
    else:
        change_date, date_kind = detected_date, "detected"
    return {
        "announcement_date": announcement_date,
        "announcement_date_source": announcement_date_source if announcement_date else "",
        "effective_date": effective_date,
        "effective_date_source": effective_date_source if effective_date else "",
        "detected_date": detected_date,
        "change_date": change_date,
        "date_kind": date_kind,
    }


def _official_reason(metadata: Mapping[str, Any]) -> tuple[str, str]:
    value = str(metadata.get("official_reason") or "").strip()
    status = str(metadata.get("official_reason_status") or "").strip().lower()
    source = str(metadata.get("official_reason_source") or "").strip()
    if value and status == "sourced" and source:
        return value, "sourced"
    return "", "not_stated"


def build_policy_change_digest(
    changes: Sequence[Mapping[str, Any]],
    *,
    evidence_facts: Mapping[str, Mapping[str, Any]] | None = None,
    entity_names: Mapping[str, Mapping[str, Any]] | None = None,
    start_date: str = "",
    end_date: str = "",
    entity_kind: str = "",
    generated_at: str = "",
) -> dict[str, Any]:
    """Group confirmed policy revisions without inventing dates or policy reasons."""

    facts_by_bundle = evidence_facts or {}
    configured_names = entity_names or {}
    start = _iso_date(start_date)
    end = _iso_date(end_date)
    if entity_kind and entity_kind not in {"country", "airline", "other"}:
        raise ValueError("entity_kind must be country, airline, other, or empty")
    if start and end and start > end:
        raise ValueError("start_date must not be after end_date")

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for raw in changes:
        item = _mapping(raw)
        if str(item.get("status") or "confirmed").lower() != "confirmed":
            continue
        metadata = _mapping(item.get("metadata"))
        kind = str(metadata.get("entity_kind") or item.get("entity_kind") or "other").strip()
        kind = kind if kind in {"country", "airline"} else "other"
        if entity_kind and kind != entity_kind:
            continue
        key = str(metadata.get("entity_key") or item.get("entity_key") or "").strip()
        key = key or str(item.get("source_id") or item.get("change_id") or "unclassified")
        bundle_id = str(item.get("evidence_bundle_id") or "").strip()
        facts = _mapping(facts_by_bundle.get(bundle_id))
        old_rules = _rules(facts.get("old_rule") or metadata.get("old_rule"))
        new_rules = _rules(facts.get("new_rule") or metadata.get("new_rule"))
        if not old_rules and not new_rules:
            old_rules, new_rules = _summary_rules(item.get("summary"))
        dates = _date_fields(metadata, item.get("occurred_at") or item.get("detected_at"))
        change_date = dates["change_date"]
        if start and (not change_date or change_date < start):
            continue
        if end and (not change_date or change_date > end):
            continue
        reason, reason_status = _official_reason(metadata)
        name_zh, name_en, label = resolve_entity_names(
            kind, key, metadata, configured_names
        )
        importance = str(metadata.get("importance") or item.get("importance") or "medium")
        grouped[(kind, key)].append(
            {
                "change_id": str(item.get("change_id") or ""),
                "revision_id": str(item.get("revision_id") or item.get("id") or ""),
                "revision": int(item.get("revision") or item.get("revision_no") or 1),
                "headline": str(item.get("headline") or "政策条款发生变化").strip(),
                **dates,
                "old_rules": old_rules,
                "new_rules": new_rules,
                "official_reason": reason,
                "official_reason_status": reason_status,
                "change_kind": str(metadata.get("change_kind") or "其他政策"),
                "importance": importance if importance in {"high", "medium", "low"} else "medium",
                "impact": str(item.get("impact") or "").strip(),
                "recommended_action": str(item.get("recommended_action") or "").strip(),
                "source_url": str(metadata.get("url") or item.get("url") or "").strip(),
                "source_name": str(metadata.get("subject") or item.get("subject") or "").strip(),
                "evidence_bundle_id": bundle_id,
                "entity_kind": kind,
                "entity_key": key,
                "entity_name_zh": name_zh,
                "entity_name_en": name_en,
                "entity_label": label,
            }
        )

    sections = {"country": [], "airline": [], "other": []}
    for (kind, key), values in grouped.items():
        values.sort(
            key=lambda value: (
                value.get("change_date", ""),
                value.get("headline", ""),
                value.get("revision", 0),
            ),
            reverse=True,
        )
        first = values[0]
        sections[kind].append(
            {
                "entity_kind": kind,
                "entity_key": key,
                "name_zh": first["entity_name_zh"],
                "name_en": first["entity_name_en"],
                "label": first["entity_label"],
                "change_count": len(values),
                "high_priority_count": sum(value["importance"] == "high" for value in values),
                "changes": values,
            }
        )
    for groups in sections.values():
        groups.sort(key=lambda value: (value["label"].casefold(), value["entity_key"]))

    all_changes = [
        change
        for kind in ("country", "airline", "other")
        for group in sections[kind]
        for change in group["changes"]
    ]
    known_dates = sorted(change["change_date"] for change in all_changes if change["change_date"])
    period_start = start or (known_dates[0] if known_dates else "")
    period_end = end or (known_dates[-1] if known_dates else "")
    return {
        "generated_at": generated_at or datetime.now().astimezone().isoformat(),
        "period": {"start": period_start, "end": period_end},
        "counts": {
            "changes": len(all_changes),
            "countries": len(sections["country"]),
            "airlines": len(sections["airline"]),
            "other_entities": len(sections["other"]),
            "high_priority": sum(change["importance"] == "high" for change in all_changes),
        },
        "country_groups": sections["country"],
        "airline_groups": sections["airline"],
        "other_groups": sections["other"],
    }


def render_policy_change_digest_text(digest: Mapping[str, Any]) -> str:
    """Render a reusable Chinese digest from the structured aggregation."""

    period = _mapping(digest.get("period"))
    start, end = str(period.get("start") or ""), str(period.get("end") or "")
    period_label = f" {start} 至 {end}" if start and end else f" {start or end}" if start or end else ""
    lines = [f"【政策变动汇总】{period_label}".rstrip()]
    groups = [
        *list(digest.get("country_groups") or ()),
        *list(digest.get("airline_groups") or ()),
        *list(digest.get("other_groups") or ()),
    ]
    section_numbers = "一二三四五六七八九十"
    for group_index, raw_group in enumerate(groups, start=1):
        group = _mapping(raw_group)
        numeral = section_numbers[group_index - 1] if group_index <= len(section_numbers) else str(group_index)
        lines.extend(["", f"{numeral}、{group.get('label') or '待归类'}", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"])
        for change_index, raw_change in enumerate(group.get("changes") or (), start=1):
            change = _mapping(raw_change)
            importance = " ★重大变化" if change.get("importance") == "high" else ""
            date_value = str(change.get("change_date") or "")
            date_kind = {
                "effective": "生效",
                "announcement": "公告",
                "detected": "发现",
            }.get(str(change.get("date_kind") or ""), "日期")
            date_label = f"（{date_kind}：{date_value}）" if date_value else ""
            old_text = "；".join(_rules(change.get("old_rules"))) or "此前规则未形成可验证基线"
            new_text = "；".join(_rules(change.get("new_rules"))) or "该条款已删除，未发现替代规则"
            reason = (
                str(change.get("official_reason") or "")
                if change.get("official_reason_status") == "sourced"
                else "官网未说明"
            )
            lines.extend(
                [
                    f"{change_index}. {change.get('headline') or '政策条款发生变化'}{date_label}{importance}",
                    f"- 原内容: {old_text}",
                    f"- 新内容: {new_text}",
                    f"- 生效时间: {change.get('effective_date') or '官网未明确说明'}",
                    f"- 官方原因: {reason}",
                ]
            )
            if change.get("impact"):
                lines.append(f"- 业务影响: {change['impact']}")
            if change.get("recommended_action"):
                lines.append(f"- 建议行动: {change['recommended_action']}")
            if change.get("source_url"):
                lines.append(f"- 官方来源: {change['source_url']}")
    if not groups:
        lines.extend(["", "当前范围内没有通过证据链校验的有效政策变化。"])
    return "\n".join(lines)


def render_policy_change_digest_markdown(digest: Mapping[str, Any]) -> str:
    """Render a copy/export friendly Markdown digest."""

    period = _mapping(digest.get("period"))
    start, end = str(period.get("start") or ""), str(period.get("end") or "")
    period_label = f"（{start} 至 {end}）" if start and end else f"（{start or end}）" if start or end else ""
    lines = [f"# 政策变动汇总{period_label}"]
    groups = [
        *list(digest.get("country_groups") or ()),
        *list(digest.get("airline_groups") or ()),
        *list(digest.get("other_groups") or ()),
    ]
    for raw_group in groups:
        group = _mapping(raw_group)
        lines.extend(["", f"## {group.get('label') or '待归类'}"])
        for raw_change in group.get("changes") or ():
            change = _mapping(raw_change)
            importance = "（重大变化）" if change.get("importance") == "high" else ""
            old_text = "；".join(_rules(change.get("old_rules"))) or "此前规则未形成可验证基线"
            new_text = "；".join(_rules(change.get("new_rules"))) or "该条款已删除，未发现替代规则"
            reason = (
                str(change.get("official_reason") or "")
                if change.get("official_reason_status") == "sourced"
                else "官网未说明"
            )
            lines.extend(
                [
                    "",
                    f"### {change.get('headline') or '政策条款发生变化'}{importance}",
                    f"- 原内容：{old_text}",
                    f"- 新内容：{new_text}",
                    f"- 公告时间：{change.get('announcement_date') or '官网未明确说明'}",
                    f"- 生效时间：{change.get('effective_date') or '官网未明确说明'}",
                    f"- 官方原因：{reason}",
                ]
            )
            if change.get("source_url"):
                lines.append(f"- 官方来源：{change['source_url']}")
    if not groups:
        lines.extend(["", "当前范围内没有通过证据链校验的有效政策变化。"])
    return "\n".join(lines)
