from __future__ import annotations

import ipaddress
import json
import re
from typing import Any, Iterable, Mapping
from urllib.parse import urlsplit, urlunsplit

try:
    from .site_url_inventory import stable_url_decision
except ImportError:
    from site_url_inventory import stable_url_decision


MAX_INPUT_CHARS = 64 * 1024
MAX_BATCH_URLS = 200
MAX_NAME_CHARS = 160

_URL_CANDIDATE_RE = re.compile(
    r"""(?ix)
    (?:
        [a-z][a-z0-9+.-]*://[^\s<>"'`]+
        |
        www\.[^\s<>"'`]+
        |
        (?<![@\w])
        (?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+
        [a-z]{2,63}
        (?::\d{1,5})?
        (?:/[^\s<>"'`]*)?
    )
    """,
)
_EDGE_PUNCTUATION = " \t\r\n<>()[]{}\"'`，。；：！？、,;:!?|"
_BLOCKED_HOSTS = {"localhost", "localhost.localdomain"}
_BLOCKED_HOST_SUFFIXES = (".localhost", ".local", ".internal", ".home", ".lan")


def _reason(code: str, detail: str = "") -> dict[str, str]:
    return {"code": code, "detail": detail}


def normalize_submitted_url(value: Any) -> tuple[str, dict[str, str] | None]:
    raw = str(value or "").strip().strip(_EDGE_PUNCTUATION)
    while raw and raw[-1] in ")]}":
        raw = raw[:-1].rstrip()
    if not raw:
        return "", _reason("empty_url")
    if "://" not in raw:
        raw = f"https://{raw}"
    try:
        parsed = urlsplit(raw)
        port = parsed.port
    except ValueError as exc:
        return "", _reason("invalid_url", str(exc))
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        return "", _reason("unsupported_scheme", scheme)
    if not parsed.hostname:
        return "", _reason("missing_hostname")
    if parsed.username or parsed.password:
        return "", _reason("credentials_not_allowed")
    try:
        host = parsed.hostname.encode("idna").decode("ascii").lower().rstrip(".")
    except UnicodeError:
        return "", _reason("invalid_hostname")
    if not host:
        return "", _reason("invalid_hostname")
    if port and not 1 <= port <= 65535:
        return "", _reason("invalid_port")
    normalized_host = host
    if port and not (
        (scheme == "http" and port == 80)
        or (scheme == "https" and port == 443)
    ):
        normalized_host = f"{host}:{port}"
    normalized = urlunsplit((
        scheme,
        normalized_host,
        parsed.path or "/",
        parsed.query,
        "",
    ))
    stable, reason = stable_url_decision(normalized)
    if not stable:
        return normalized, _reason(reason)
    security_reason = public_url_security_reason(normalized)
    if security_reason:
        return normalized, _reason(security_reason)
    return normalized, None


def public_url_security_reason(url: str) -> str:
    try:
        host = (urlsplit(url).hostname or "").lower().rstrip(".")
    except ValueError:
        return "invalid_url"
    if host in _BLOCKED_HOSTS or host.endswith(_BLOCKED_HOST_SUFFIXES):
        return "private_host_not_allowed"
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        if "." not in host:
            return "public_hostname_required"
        return ""
    return "" if address.is_global else "private_address_not_allowed"


def extract_url_candidates(text: Any) -> list[str]:
    value = str(text or "")[:MAX_INPUT_CHARS]
    value = re.sub(r"[，；、|]+", " ", value)
    value = re.sub(
        r",(?=(?:https?://|www\.|[a-z0-9-]+\.[a-z]{2,}))",
        " ",
        value,
        flags=re.IGNORECASE,
    )
    return [
        match.group(0).strip().strip(_EDGE_PUNCTUATION)
        for match in _URL_CANDIDATE_RE.finditer(value)
        if match.group(0).strip().strip(_EDGE_PUNCTUATION)
    ]


def prepare_source_candidates(
    text: Any,
    *,
    max_urls: int = MAX_BATCH_URLS,
) -> dict[str, Any]:
    value = str(text or "")[:MAX_INPUT_CHARS]
    raw_candidates = extract_url_candidates(value)
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    limit = max(min(int(max_urls), MAX_BATCH_URLS), 1)
    for raw in raw_candidates[:limit]:
        normalized, error = normalize_submitted_url(raw)
        item: dict[str, Any] = {
            "input": raw[:1000],
            "url": normalized,
            "name": "",
            "name_origin": "",
            "status": "valid" if not error else "invalid",
            "reason": error or {},
        }
        if normalized and normalized in seen:
            item["status"] = "duplicate_in_batch"
            item["reason"] = _reason("duplicate_in_batch")
        elif normalized:
            seen.add(normalized)
        items.append(item)
    return {
        "items": items,
        "input_chars": len(value),
        "candidate_count": len(raw_candidates),
        "truncated": len(raw_candidates) > limit or len(str(text or "")) > MAX_INPUT_CHARS,
        "max_urls": limit,
    }


def _evidence_key(url: str) -> str:
    parsed = urlsplit(url)
    value = f"{parsed.netloc}{parsed.path}"
    if parsed.query:
        value += f"?{parsed.query}"
    return value.rstrip("/").lower()


def _input_evidence_text(value: str) -> str:
    return re.sub(r"\s+", "", value).lower()


def merge_ai_suggestions(
    original_input: str,
    deterministic_items: Iterable[Mapping[str, Any]],
    ai_items: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    merged = [dict(item) for item in deterministic_items]
    by_url = {
        str(item.get("url")): item
        for item in merged
        if item.get("url")
    }
    by_evidence_key = {
        _evidence_key(url): item
        for url, item in by_url.items()
    }
    evidence = _input_evidence_text(original_input)
    for suggestion in ai_items:
        normalized, error = normalize_submitted_url(suggestion.get("url"))
        if error or not normalized:
            continue
        key = _input_evidence_text(_evidence_key(normalized))
        if not key or key not in evidence:
            continue
        item = by_url.get(normalized) or by_evidence_key.get(_evidence_key(normalized))
        if item is None:
            continue
        name = re.sub(r"\s+", " ", str(suggestion.get("name") or "")).strip()
        if name:
            item["name"] = name[:MAX_NAME_CHARS]
            item["name_origin"] = "ai"
    return merged


def parse_ai_source_response(response: str) -> list[dict[str, str]]:
    value = str(response or "").strip()
    if value.startswith("```"):
        value = re.sub(r"^```(?:json)?\s*", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\s*```$", "", value)
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        start = value.find("{")
        end = value.rfind("}")
        if start < 0 or end <= start:
            return []
        try:
            payload = json.loads(value[start:end + 1])
        except json.JSONDecodeError:
            return []
    raw_items = payload.get("items", []) if isinstance(payload, dict) else payload
    if not isinstance(raw_items, list):
        return []
    return [
        {
            "url": str(item.get("url") or ""),
            "name": str(item.get("name") or ""),
        }
        for item in raw_items
        if isinstance(item, dict) and item.get("url")
    ][:MAX_BATCH_URLS]
