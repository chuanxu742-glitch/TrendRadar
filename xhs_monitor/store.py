from __future__ import annotations

import hashlib
import re
import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlsplit


_NOTE_ID_SUFFIX = re.compile(r"\s+\[[0-9A-Za-z_-]{8}\]\s*$")


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def safe_note_url(value: Any) -> str:
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


def _note_key(source_id: str, title: str, url: str) -> str:
    identity = url or title
    return hashlib.sha256(f"{source_id}\0{identity}".encode("utf-8")).hexdigest()


class XiaohongshuStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    def _initialize(self) -> None:
        with closing(self.connect()) as connection:
            connection.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS keywords (
                    source_id TEXT PRIMARY KEY,
                    query TEXT NOT NULL,
                    name TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    last_status TEXT NOT NULL DEFAULT 'pending',
                    last_checked_at TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    source_count INTEGER NOT NULL,
                    successful_sources INTEGER NOT NULL,
                    failed_sources INTEGER NOT NULL,
                    item_count INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS notes (
                    note_key TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    rank INTEGER NOT NULL DEFAULT 0,
                    url TEXT NOT NULL DEFAULT '',
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    FOREIGN KEY(source_id) REFERENCES keywords(source_id)
                );
                CREATE INDEX IF NOT EXISTS idx_notes_last_seen
                    ON notes(last_seen_at DESC);
                CREATE INDEX IF NOT EXISTS idx_notes_source
                    ON notes(source_id, last_seen_at DESC);
                """
            )
            connection.commit()

    def sync_keywords(self, keywords: Iterable[dict[str, Any]]) -> list[dict[str, str]]:
        normalized: list[dict[str, str]] = []
        seen: set[str] = set()
        for raw in keywords:
            if not isinstance(raw, dict) or not raw.get("enabled", True):
                continue
            keyword_id = str(raw.get("id") or "").strip()
            query = str(raw.get("query") or "").strip()
            if not keyword_id or not query:
                continue
            source_id = keyword_id if keyword_id.startswith("xhs-") else f"xhs-{keyword_id}"
            if source_id in seen:
                continue
            seen.add(source_id)
            normalized.append(
                {
                    "source_id": source_id,
                    "query": query,
                    "name": str(raw.get("name") or f"小红书·{query}").strip(),
                }
            )

        timestamp = now_iso()
        with closing(self.connect()) as connection:
            connection.execute("UPDATE keywords SET enabled=0, updated_at=?", (timestamp,))
            for item in normalized:
                connection.execute(
                    """
                    INSERT INTO keywords(
                        source_id, query, name, enabled, updated_at
                    ) VALUES(?, ?, ?, 1, ?)
                    ON CONFLICT(source_id) DO UPDATE SET
                        query=excluded.query,
                        name=excluded.name,
                        enabled=1,
                        updated_at=excluded.updated_at
                    """,
                    (
                        item["source_id"],
                        item["query"],
                        item["name"],
                        timestamp,
                    ),
                )
            connection.commit()
        return normalized

    def record_run(
        self,
        *,
        started_at: str,
        keywords: Iterable[dict[str, str]],
        results: dict[str, dict[str, dict[str, Any]]],
        failed_ids: Iterable[str],
    ) -> dict[str, int | str]:
        configured = list(keywords)
        configured_ids = {item["source_id"] for item in configured}
        failed = {str(item) for item in failed_ids if str(item) in configured_ids}
        successful = configured_ids.intersection(results)
        failed.update(configured_ids - successful)
        finished_at = now_iso()
        item_count = 0

        with closing(self.connect()) as connection:
            for item in configured:
                status = "success" if item["source_id"] in successful else "failed"
                connection.execute(
                    """
                    UPDATE keywords
                    SET last_status=?, last_checked_at=?, updated_at=?
                    WHERE source_id=?
                    """,
                    (status, finished_at, finished_at, item["source_id"]),
                )

            for source_id in successful:
                for raw_title, raw_item in (results.get(source_id) or {}).items():
                    if not isinstance(raw_item, dict):
                        continue
                    title = _NOTE_ID_SUFFIX.sub("", str(raw_title or "")).strip()
                    if not title:
                        continue
                    url = safe_note_url(raw_item.get("url") or raw_item.get("mobileUrl"))
                    ranks = raw_item.get("ranks") or []
                    try:
                        rank = int(ranks[-1]) if ranks else 0
                    except (TypeError, ValueError):
                        rank = 0
                    key = _note_key(source_id, title, url)
                    connection.execute(
                        """
                        INSERT INTO notes(
                            note_key, source_id, title, rank, url,
                            first_seen_at, last_seen_at
                        ) VALUES(?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(note_key) DO UPDATE SET
                            title=excluded.title,
                            rank=excluded.rank,
                            url=excluded.url,
                            last_seen_at=excluded.last_seen_at
                        """,
                        (key, source_id, title, rank, url, finished_at, finished_at),
                    )
                    item_count += 1

            source_count = len(configured_ids)
            successful_count = len(successful)
            failed_count = max(source_count - successful_count, len(failed))
            if not source_count:
                run_status = "not_configured"
            elif successful_count == source_count:
                run_status = "available"
            elif successful_count:
                run_status = "partial"
            else:
                run_status = "unavailable"
            connection.execute(
                """
                INSERT INTO runs(
                    started_at, finished_at, status, source_count,
                    successful_sources, failed_sources, item_count
                ) VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    started_at,
                    finished_at,
                    run_status,
                    source_count,
                    successful_count,
                    failed_count,
                    item_count,
                ),
            )
            connection.commit()
        return {
            "status": run_status,
            "source_count": source_count,
            "successful_sources": successful_count,
            "failed_sources": failed_count,
            "item_count": item_count,
        }

    def summary(self, *, limit: int = 100) -> dict[str, Any]:
        requested_limit = min(max(int(limit), 1), 500)
        today = datetime.now().astimezone().date().isoformat()
        with closing(self.connect()) as connection:
            source_rows = connection.execute(
                """
                SELECT source_id, name, last_status, last_checked_at
                FROM keywords
                WHERE enabled=1
                ORDER BY source_id
                """
            ).fetchall()
            run = connection.execute(
                """
                SELECT finished_at, status, source_count, successful_sources,
                       failed_sources
                FROM runs
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            note_rows = connection.execute(
                """
                SELECT n.source_id, k.name AS source_name, n.title, n.rank,
                       n.url, n.first_seen_at, n.last_seen_at
                FROM notes n
                JOIN keywords k ON k.source_id=n.source_id
                WHERE k.enabled=1
                ORDER BY n.last_seen_at DESC, n.rank ASC, n.note_key
                LIMIT ?
                """,
                (requested_limit,),
            ).fetchall()

        source_count = len(source_rows)
        updated_at = str(run["finished_at"] or "") if run else ""
        is_today = updated_at.startswith(today)
        successful_sources = int(run["successful_sources"] or 0) if run and is_today else 0
        failed_sources = int(run["failed_sources"] or 0) if run and is_today else source_count
        if not source_count:
            status = "not_configured"
            status_label = "尚未接入数据"
        elif not run or not is_today:
            status = "unavailable"
            status_label = "今日数据暂未更新"
        else:
            status = str(run["status"])
            status_label = {
                "available": "今日已更新",
                "partial": "部分关键词已更新",
                "unavailable": "今日数据暂未更新",
            }.get(status, "今日数据暂未更新")

        items = [
            {
                "source_id": str(row["source_id"]),
                "source_name": str(row["source_name"]),
                "title": str(row["title"]),
                "rank": int(row["rank"] or 0),
                "url": safe_note_url(row["url"]),
                "first_seen_at": str(row["first_seen_at"]),
                "last_seen_at": str(row["last_seen_at"]),
            }
            for row in note_rows
        ]
        today_count = sum(item["last_seen_at"].startswith(today) for item in items)
        return {
            "status": status,
            "status_label": status_label,
            "source_count": source_count,
            "successful_sources": successful_sources,
            "failed_sources": failed_sources,
            "updated_at": updated_at,
            "today_count": today_count,
            "recent_count": len(items),
            "items": items,
        }
