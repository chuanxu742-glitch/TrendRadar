from __future__ import annotations

import re
import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


XHS_SOURCE_PATTERN = "xhs-%"
MAX_DATABASE_FILES = 31
_NOTE_ID_SUFFIX = re.compile(r"\s+\[[0-9A-Za-z_-]{8}\]\s*$")
_DATE_STEM = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _database_paths(news_dir: Path) -> list[Path]:
    if not news_dir.is_dir():
        return []
    paths = [
        path
        for path in news_dir.glob("*.db")
        if path.is_file() and _DATE_STEM.fullmatch(path.stem)
    ]
    return sorted(paths, key=lambda path: path.stem, reverse=True)[:MAX_DATABASE_FILES]


def _connect_read_only(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"{path.resolve().as_uri()}?mode=ro",
        uri=True,
        timeout=1,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _observed_at(day: str, crawl_time: str, created_at: str = "") -> str:
    normalized = str(crawl_time or "").strip().replace("-", ":")
    if re.fullmatch(r"\d{2}:\d{2}", normalized):
        return f"{day}T{normalized}:00+08:00"
    created = str(created_at or "").strip()
    if created:
        try:
            return datetime.fromisoformat(created).isoformat()
        except ValueError:
            pass
    return f"{day}T00:00:00+08:00"


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


def _source_states(connection: sqlite3.Connection, day: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT p.id, p.name, css.status, cr.crawl_time, cr.created_at
        FROM crawl_source_status css
        JOIN crawl_records cr ON cr.id=css.crawl_record_id
        JOIN platforms p ON p.id=css.platform_id
        WHERE p.id LIKE ?
          AND css.crawl_record_id=(
              SELECT MAX(css_latest.crawl_record_id)
              FROM crawl_source_status css_latest
              WHERE css_latest.platform_id=css.platform_id
          )
        ORDER BY p.id
        """,
        (XHS_SOURCE_PATTERN,),
    ).fetchall()
    return [
        {
            "source_id": str(row["id"]),
            "name": str(row["name"] or row["id"]),
            "status": str(row["status"]),
            "updated_at": _observed_at(
                day,
                str(row["crawl_time"] or ""),
                str(row["created_at"] or ""),
            ),
        }
        for row in rows
    ]


def _items(
    connection: sqlite3.Connection,
    day: str,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT n.platform_id, p.name AS source_name, n.title, n.rank, n.url,
               n.first_crawl_time, n.last_crawl_time, n.updated_at
        FROM news_items n
        JOIN platforms p ON p.id=n.platform_id
        WHERE n.platform_id LIKE ?
        ORDER BY n.last_crawl_time DESC, n.rank ASC, n.id DESC
        LIMIT ?
        """,
        (XHS_SOURCE_PATTERN, max(limit, 1)),
    ).fetchall()
    return [
        {
            "source_id": str(row["platform_id"]),
            "source_name": str(row["source_name"] or row["platform_id"]),
            "title": _NOTE_ID_SUFFIX.sub("", str(row["title"] or "")).strip(),
            "rank": int(row["rank"] or 0),
            "url": _safe_note_url(row["url"]),
            "first_seen_at": _observed_at(
                day,
                str(row["first_crawl_time"] or ""),
                str(row["updated_at"] or ""),
            ),
            "last_seen_at": _observed_at(
                day,
                str(row["last_crawl_time"] or ""),
                str(row["updated_at"] or ""),
            ),
        }
        for row in rows
        if str(row["title"] or "").strip()
    ]


def load_xiaohongshu_intelligence(
    news_dir: Path,
    *,
    limit: int = 100,
) -> dict[str, Any]:
    """Read boss-facing Xiaohongshu signals without exposing session details."""

    requested_limit = min(max(int(limit), 1), 500)
    sources: list[dict[str, Any]] = []
    collected: list[dict[str, Any]] = []
    seen: set[str] = set()
    latest_database_day = ""

    for path in _database_paths(news_dir):
        day = path.stem
        try:
            with closing(_connect_read_only(path)) as connection:
                current_sources = _source_states(connection, day)
                if current_sources and not sources:
                    sources = current_sources
                    latest_database_day = day
                remaining = requested_limit - len(collected)
                if remaining <= 0:
                    continue
                for item in _items(connection, day, limit=remaining * 2):
                    identity = (
                        f"{item['source_id']}|{item['url']}"
                        if item["url"]
                        else f"{item['source_id']}|{item['title']}"
                    )
                    if identity in seen:
                        continue
                    seen.add(identity)
                    collected.append(item)
                    if len(collected) >= requested_limit:
                        break
        except (OSError, sqlite3.Error):
            continue

    successful = sum(item["status"] == "success" for item in sources)
    failed = sum(item["status"] == "failed" for item in sources)
    if not sources:
        status = "not_configured"
        status_label = "尚未接入数据"
    elif successful and failed:
        status = "partial"
        status_label = "部分关键词已更新"
    elif successful:
        status = "available"
        status_label = "今日已更新"
    else:
        status = "unavailable"
        status_label = "今日数据暂未更新"

    today = datetime.now().astimezone().date().isoformat()
    today_count = sum(
        str(item.get("last_seen_at") or "").startswith(today)
        for item in collected
    )
    updated_at = max(
        (str(item.get("updated_at") or "") for item in sources),
        default="",
    )
    return {
        "status": status,
        "status_label": status_label,
        "source_count": len(sources),
        "successful_sources": successful,
        "failed_sources": failed,
        "latest_database_day": latest_database_day,
        "updated_at": updated_at,
        "today_count": today_count,
        "recent_count": len(collected),
        "items": collected,
    }
