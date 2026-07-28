from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Iterable


MAX_KEYWORDS = 50
MAX_QUERY_LENGTH = 100
MAX_NAME_LENGTH = 120
_VALID_ID = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")


def normalize_keywords(raw_keywords: Iterable[Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen_queries: set[str] = set()
    seen_ids: set[str] = set()
    for raw in raw_keywords:
        if not isinstance(raw, dict):
            raise ValueError("每个关键词必须是对象")
        query = str(raw.get("query") or "").strip()
        if not query:
            raise ValueError("关键词不能为空")
        if len(query) > MAX_QUERY_LENGTH:
            raise ValueError(f"关键词不能超过 {MAX_QUERY_LENGTH} 个字符")
        query_key = query.casefold()
        if query_key in seen_queries:
            continue
        seen_queries.add(query_key)

        keyword_id = str(raw.get("id") or "").strip().lower()
        if not _VALID_ID.fullmatch(keyword_id):
            keyword_id = (
                "custom-"
                + hashlib.sha256(query.encode("utf-8")).hexdigest()[:12]
            )
        if keyword_id in seen_ids:
            keyword_id = (
                "custom-"
                + hashlib.sha256(f"{query}\0{len(normalized)}".encode("utf-8")).hexdigest()[:12]
            )
        seen_ids.add(keyword_id)

        name = str(raw.get("name") or f"小红书·{query}").strip()
        if len(name) > MAX_NAME_LENGTH:
            raise ValueError(f"显示名称不能超过 {MAX_NAME_LENGTH} 个字符")
        normalized.append(
            {
                "id": keyword_id,
                "query": query,
                "name": name or f"小红书·{query}",
                "enabled": bool(raw.get("enabled", True)),
            }
        )
        if len(normalized) > MAX_KEYWORDS:
            raise ValueError(f"最多配置 {MAX_KEYWORDS} 个关键词")
    return normalized


def load_managed_keywords(path: Path) -> list[dict[str, Any]] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict) or not isinstance(payload.get("keywords"), list):
        return None
    try:
        return normalize_keywords(payload["keywords"])
    except ValueError:
        return None


def save_managed_keywords(
    path: Path,
    raw_keywords: Iterable[Any],
) -> list[dict[str, Any]]:
    normalized = normalize_keywords(raw_keywords)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    temporary_path.write_text(
        json.dumps(
            {"version": 1, "keywords": normalized},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    temporary_path.replace(path)
    return normalized
