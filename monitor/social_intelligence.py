from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen


VALID_STATUSES = {"available", "partial", "unavailable", "not_configured"}
STATUS_LABELS = {
    "available": "今日已更新",
    "partial": "部分关键词已更新",
    "unavailable": "今日数据暂未更新",
    "not_configured": "尚未接入数据",
}


def unavailable_payload() -> dict[str, Any]:
    return {
        "status": "unavailable",
        "status_label": "今日数据暂未更新",
        "source_count": 0,
        "successful_sources": 0,
        "failed_sources": 0,
        "updated_at": "",
        "today_count": 0,
        "recent_count": 0,
        "items": [],
    }


def _safe_note_url(value: Any) -> str:
    candidate = str(value or "").strip()
    if not candidate:
        return ""
    try:
        parsed = urlsplit(candidate)
    except ValueError:
        return ""
    hostname = (parsed.hostname or "").lower()
    if parsed.scheme not in {"http", "https"}:
        return ""
    if hostname != "xiaohongshu.com" and not hostname.endswith(".xiaohongshu.com"):
        return ""
    return candidate


def _integer(value: Any) -> int:
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _summary_url(url: str, limit: int) -> str:
    parsed = urlsplit(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["limit"] = str(min(max(int(limit), 1), 500))
    return urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment)
    )


def _service_url(summary_url: str, path: str) -> str:
    parsed = urlsplit(summary_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("小红书服务地址无效")
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def request_xiaohongshu_service(
    summary_url: str,
    path: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout: float = 5.0,
) -> dict[str, Any]:
    content = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        content = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(
        _service_url(summary_url, path),
        data=content,
        headers=headers,
        method=method,
    )
    try:
        with urlopen(request, timeout=max(float(timeout), 0.1)) as response:
            result = json.loads(response.read(2 * 1024 * 1024).decode("utf-8"))
    except HTTPError as exc:
        try:
            error_payload = json.loads(exc.read(64 * 1024).decode("utf-8"))
            message = str(error_payload.get("error") or "")
        except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
            message = ""
        raise ValueError(message[:200] or "小红书配置请求失败") from None
    except (URLError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("小红书配置服务暂时不可用") from None
    if not isinstance(result, dict):
        raise ValueError("小红书配置服务返回异常")
    return result


def _normalize_payload(payload: Any, *, limit: int) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("summary response must be an object")
    status = str(payload.get("status") or "")
    if status not in VALID_STATUSES:
        raise ValueError("summary response has invalid status")
    raw_items = payload.get("items")
    if not isinstance(raw_items, list):
        raise ValueError("summary response items must be a list")

    items: list[dict[str, Any]] = []
    for raw in raw_items[:limit]:
        if not isinstance(raw, dict):
            continue
        title = str(raw.get("title") or "").strip()
        if not title:
            continue
        items.append(
            {
                "source_id": str(raw.get("source_id") or ""),
                "source_name": str(raw.get("source_name") or "小红书"),
                "title": title,
                "rank": _integer(raw.get("rank")),
                "url": _safe_note_url(raw.get("url")),
                "first_seen_at": str(raw.get("first_seen_at") or ""),
                "last_seen_at": str(raw.get("last_seen_at") or ""),
            }
        )

    return {
        "status": status,
        "status_label": STATUS_LABELS[status],
        "source_count": _integer(payload.get("source_count")),
        "successful_sources": _integer(payload.get("successful_sources")),
        "failed_sources": _integer(payload.get("failed_sources")),
        "updated_at": str(payload.get("updated_at") or ""),
        "today_count": _integer(payload.get("today_count")),
        "recent_count": len(items),
        "items": items,
    }


def fetch_xiaohongshu_intelligence(
    summary_url: str,
    *,
    limit: int = 100,
    timeout: float = 2.0,
) -> dict[str, Any]:
    """Read the independent collector's boss-safe summary API."""

    requested_limit = min(max(int(limit), 1), 500)
    try:
        request = Request(
            _summary_url(summary_url, requested_limit),
            headers={"Accept": "application/json"},
        )
        with urlopen(request, timeout=max(float(timeout), 0.1)) as response:
            if response.status != 200:
                return unavailable_payload()
            payload = json.loads(response.read(2 * 1024 * 1024).decode("utf-8"))
        return _normalize_payload(payload, limit=requested_limit)
    except (HTTPError, URLError, OSError, ValueError, json.JSONDecodeError):
        return unavailable_payload()
