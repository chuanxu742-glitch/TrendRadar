from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime
from pathlib import Path

from monitor.social_intelligence import load_xiaohongshu_intelligence


SCHEMA = """
CREATE TABLE platforms (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    updated_at TEXT
);
CREATE TABLE crawl_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    crawl_time TEXT NOT NULL UNIQUE,
    total_items INTEGER DEFAULT 0,
    created_at TEXT
);
CREATE TABLE crawl_source_status (
    crawl_record_id INTEGER NOT NULL,
    platform_id TEXT NOT NULL,
    status TEXT NOT NULL
);
CREATE TABLE news_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    platform_id TEXT NOT NULL,
    rank INTEGER NOT NULL,
    url TEXT DEFAULT '',
    mobile_url TEXT DEFAULT '',
    first_crawl_time TEXT NOT NULL,
    last_crawl_time TEXT NOT NULL,
    crawl_count INTEGER DEFAULT 1,
    created_at TEXT,
    updated_at TEXT
);
"""


class XiaohongshuIntelligenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def database(self, day: str) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.root / f"{day}.db",
            isolation_level=None,
        )
        connection.executescript(SCHEMA)
        return connection

    def seed_source(
        self,
        connection: sqlite3.Connection,
        *,
        status: str,
        source_id: str = "xhs-pet-cabin-refused",
        name: str = "小红书·宠物进客舱被拒载",
    ) -> None:
        connection.execute(
            "INSERT INTO platforms(id, name, updated_at) VALUES(?, ?, ?)",
            (source_id, name, "2026-07-28 17:32:03"),
        )
        cursor = connection.execute(
            """
            INSERT INTO crawl_records(crawl_time, total_items, created_at)
            VALUES('17-32', 1, '2026-07-28 17:32:03')
            """
        )
        connection.execute(
            """
            INSERT INTO crawl_source_status(crawl_record_id, platform_id, status)
            VALUES(?, ?, ?)
            """,
            (cursor.lastrowid, source_id, status),
        )

    def test_missing_news_directory_is_not_configured(self) -> None:
        payload = load_xiaohongshu_intelligence(self.root / "missing")

        self.assertEqual(payload["status"], "not_configured")
        self.assertEqual(payload["items"], [])
        self.assertEqual(payload["source_count"], 0)

    def test_failed_sources_are_business_safe_unavailable_state(self) -> None:
        with closing(self.database("2026-07-28")) as connection:
            self.seed_source(connection, status="failed")

        payload = load_xiaohongshu_intelligence(self.root)

        self.assertEqual(payload["status"], "unavailable")
        self.assertEqual(payload["status_label"], "今日数据暂未更新")
        self.assertEqual(payload["failed_sources"], 1)
        self.assertNotIn("cookie", str(payload).lower())
        self.assertNotIn("error", payload)

    def test_reads_only_xiaohongshu_items_and_sanitizes_output(self) -> None:
        today = datetime.now().astimezone().date().isoformat()
        with closing(self.database(today)) as connection:
            self.seed_source(connection, status="success")
            connection.execute(
                """
                INSERT INTO news_items(
                    title, platform_id, rank, url, first_crawl_time,
                    last_crawl_time, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "某航司拒绝宠物进入客舱 [note1234]",
                    "xhs-pet-cabin-refused",
                    1,
                    "https://www.xiaohongshu.com/explore/note1234?xsec_token=safe",
                    "16-00",
                    "17-32",
                    f"{today} 17:32:03",
                    f"{today} 17:32:03",
                ),
            )
            connection.execute(
                """
                INSERT INTO platforms(id, name, updated_at)
                VALUES('other', '其他平台', ?)
                """,
                (f"{today} 17:32:03",),
            )
            connection.execute(
                """
                INSERT INTO news_items(
                    title, platform_id, rank, url, first_crawl_time,
                    last_crawl_time, created_at, updated_at
                ) VALUES('无关内容', 'other', 1, 'https://example.com', '17-00',
                         '17-00', ?, ?)
                """,
                (f"{today} 17:32:03", f"{today} 17:32:03"),
            )

        payload = load_xiaohongshu_intelligence(self.root)

        self.assertEqual(payload["status"], "available")
        self.assertEqual(payload["recent_count"], 1)
        self.assertEqual(payload["today_count"], 1)
        self.assertEqual(payload["items"][0]["title"], "某航司拒绝宠物进入客舱")
        self.assertIn("xiaohongshu.com/explore/", payload["items"][0]["url"])

    def test_non_xiaohongshu_item_url_is_not_exposed(self) -> None:
        with closing(self.database("2026-07-28")) as connection:
            self.seed_source(connection, status="success")
            connection.execute(
                """
                INSERT INTO news_items(
                    title, platform_id, rank, url, first_crawl_time,
                    last_crawl_time, created_at, updated_at
                ) VALUES('风险链接 [badurl12]', 'xhs-pet-cabin-refused', 1,
                         'https://example.com/collect', '17-00', '17-32',
                         '2026-07-28 17:32:03', '2026-07-28 17:32:03')
                """
            )

        payload = load_xiaohongshu_intelligence(self.root)

        self.assertEqual(payload["items"][0]["url"], "")


if __name__ == "__main__":
    unittest.main()
