# coding=utf-8
"""Extract policy metadata only when the official snapshot states it explicitly."""

from __future__ import annotations

import re
from datetime import date
from typing import Any, Iterable


_MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
_MONTH_PATTERN = "|".join(_MONTHS)
_DATE_PATTERNS = (
    re.compile(r"\b(20\d{2})[-/.](0?[1-9]|1[0-2])[-/.](0?[1-9]|[12]\d|3[01])\b"),
    re.compile(r"(20\d{2})年\s*(0?[1-9]|1[0-2])月\s*(0?[1-9]|[12]\d|3[01])日"),
    re.compile(
        rf"\b({_MONTH_PATTERN})\s+([0-3]?\d)(?:st|nd|rd|th)?[,]?\s+(20\d{{2}})\b",
        re.IGNORECASE,
    ),
    re.compile(
        rf"\b([0-3]?\d)(?:st|nd|rd|th)?\s+({_MONTH_PATTERN})[,]?\s+(20\d{{2}})\b",
        re.IGNORECASE,
    ),
)
_EFFECTIVE_MARKERS = re.compile(
    r"\b(?:effective|takes?\s+effect|comes?\s+into\s+(?:effect|force)|"
    r"starting|beginning|with\s+effect\s+from|from)\b|"
    r"(?:生效|实施|施行|执行|自.{0,32}?起|从.{0,32}?起)",
    re.IGNORECASE,
)
_ANNOUNCEMENT_MARKERS = re.compile(
    r"\b(?:published|posted|announced|updated|issued|last\s+updated)\b|"
    r"(?:发布|公布|公告|宣布|更新于|发布日期)",
    re.IGNORECASE,
)
_REASON_MARKERS = re.compile(
    r"\b(?:because|due\s+to|in\s+order\s+to|to\s+ensure|so\s+that|"
    r"in\s+response\s+to|based\s+on)\b|(?:由于|因为|为确保|为了|以确保|因应|依据)",
    re.IGNORECASE,
)
_SENTENCE_SPLIT = re.compile(r"(?<=[。！？；])|(?<=[.!?;])\s+|\r?\n+")


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _sentences(values: Iterable[Any]) -> list[str]:
    result: list[str] = []
    for value in values:
        for sentence in _SENTENCE_SPLIT.split(str(value or "")):
            cleaned = _clean(sentence).strip(" -•\t")
            if cleaned and cleaned not in result:
                result.append(cleaned)
    return result


def _normalized_date(match: re.Match[str]) -> str:
    groups = match.groups()
    if groups[0].casefold() in _MONTHS:
        year, month, day = int(groups[2]), _MONTHS[groups[0].casefold()], int(groups[1])
    elif groups[1].casefold() in _MONTHS:
        year, month, day = int(groups[2]), _MONTHS[groups[1].casefold()], int(groups[0])
    else:
        year, month, day = map(int, groups[:3])
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return ""


def _dates(sentence: str) -> list[str]:
    values: list[str] = []
    for pattern in _DATE_PATTERNS:
        for match in pattern.finditer(sentence):
            normalized = _normalized_date(match)
            if normalized and normalized not in values:
                values.append(normalized)
    return values


def _source_reference(source_url: str, sentence: str) -> str:
    excerpt = _clean(sentence)[:500]
    return f"{source_url} | {excerpt}" if source_url else excerpt


def extract_sourced_policy_metadata(
    current_text: str,
    *,
    added: Iterable[Any] = (),
    new_context: str = "",
    source_url: str = "",
) -> dict[str, str]:
    """Return dates and reasons with an exact official-snapshot excerpt.

    The function does not infer missing values. A date must occur in the same
    sentence as a semantic marker, and a reason is copied from a sentence that
    explicitly contains a causal marker.
    """

    focused = _sentences([*list(added), new_context])
    if not focused:
        focused = _sentences([current_text])
    result = {
        "announcement_date": "",
        "announcement_date_source": "",
        "effective_date": "",
        "effective_date_source": "",
        "official_reason": "",
        "official_reason_status": "not_stated",
        "official_reason_source": "",
    }
    for sentence in focused:
        values = _dates(sentence)
        if values and not result["effective_date"] and _EFFECTIVE_MARKERS.search(sentence):
            result["effective_date"] = values[0]
            result["effective_date_source"] = _source_reference(source_url, sentence)
        if values and not result["announcement_date"] and _ANNOUNCEMENT_MARKERS.search(sentence):
            result["announcement_date"] = values[0]
            result["announcement_date_source"] = _source_reference(source_url, sentence)
        if not result["official_reason"] and _REASON_MARKERS.search(sentence):
            result["official_reason"] = sentence[:500]
            result["official_reason_status"] = "sourced"
            result["official_reason_source"] = _source_reference(source_url, sentence)
    return result
