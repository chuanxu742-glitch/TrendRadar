import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import requests

from trendradar.crawler.rss.fetcher import RSSFeedConfig, RSSFetcher
from trendradar.crawler.rss.parser import RSSParser
from trendradar.__main__ import NewsAnalyzer
from trendradar.storage.base import RSSData, RSSItem
from trendradar.storage.local import LocalStorageBackend


OFFICIAL_FEED_ID = "official-source-changes"
TEST_DATE = "2026-07-22"


def change_item(
    change_id: str,
    title: str,
    *,
    revision: int = 1,
    status: str = "confirmed",
    supersedes: str = "",
) -> RSSItem:
    return RSSItem(
        title=title,
        feed_id=OFFICIAL_FEED_ID,
        feed_name="官网变化",
        url=f"https://example.test/{change_id}",
        guid=f"guid:{change_id}",
        published_at="2026-07-22T08:00:00+08:00",
        summary=f"{title}摘要",
        change_id=change_id,
        revision=revision,
        status=status,
        supersedes=supersedes,
        is_active=status == "confirmed",
    )


def rss_data(crawl_time: str, items: dict, failed_ids=None) -> RSSData:
    failures = failed_ids or []
    return RSSData(
        date=TEST_DATE,
        crawl_time=crawl_time,
        items=items,
        id_to_name={OFFICIAL_FEED_ID: "官网变化"},
        failed_ids=failures,
        authoritative_complete_ids=(
            [OFFICIAL_FEED_ID]
            if OFFICIAL_FEED_ID in items and OFFICIAL_FEED_ID not in failures
            else []
        ),
    )


class RSSParserEncodingTests(unittest.TestCase):
    def test_namespaced_change_metadata_is_parsed_from_utf8_bytes(self) -> None:
        xml = """<?xml version="1.0" encoding="utf-8"?>
        <rss version="2.0" xmlns:tr="urn:trendradar:policy-change"><channel>
          <title>官网变化</title><item>
            <title>保加利亚航空：宠物须留在运输箱内</title>
            <link>https://example.test/bulgaria</link><guid>legacy-guid</guid>
            <tr:change_id>change:bg</tr:change_id><tr:revision>2</tr:revision>
            <tr:status>confirmed</tr:status><tr:supersedes>change:old</tr:supersedes>
          </item>
        </channel></rss>""".encode("utf-8")

        item = RSSParser().parse(xml)[0]

        self.assertEqual(item.title, "保加利亚航空：宠物须留在运输箱内")
        self.assertEqual(item.change_id, "change:bg")
        self.assertEqual(item.revision, 2)
        self.assertEqual(item.status, "confirmed")
        self.assertEqual(item.supersedes, "change:old")

    def test_fetcher_uses_response_bytes_and_legacy_guid_defaults(self) -> None:
        fingerprint = [["change:legacy", 1, "confirmed", ""]]
        digest = hashlib.sha256(
            json.dumps(fingerprint, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        xml = f"""<?xml version="1.0" encoding="utf-8"?>
        <rss version="2.0"><channel><title>官网变化</title>
          <snapshot_complete>true</snapshot_complete><snapshot_count>1</snapshot_count>
          <snapshot_digest>{digest}</snapshot_digest><item>
          <title>保加利亚航空政策变化</title>
          <link>https://example.test/bulgaria</link><guid>change:legacy</guid>
        </item></channel></rss>""".encode("utf-8")
        response = requests.Response()
        response.status_code = 200
        response._content = xml
        response.encoding = "ISO-8859-1"  # 模拟缺少 charset 时 requests 的错误猜测

        feed = RSSFeedConfig(
            id=OFFICIAL_FEED_ID,
            name="官网变化",
            url="https://example.test/feed.xml",
        )
        fetcher = RSSFetcher([feed], request_interval=0)
        with mock.patch.object(fetcher.session, "get", return_value=response):
            items, error = fetcher.fetch_feed(feed)

        self.assertIsNone(error)
        self.assertEqual(items[0].title, "保加利亚航空政策变化")
        self.assertEqual(items[0].change_id, "change:legacy")
        self.assertEqual(items[0].revision, 1)
        self.assertEqual(items[0].status, "confirmed")

    def test_fetcher_rejects_unverified_authoritative_snapshot(self) -> None:
        response = requests.Response()
        response.status_code = 200
        response._content = b"<rss><channel><title>partial</title><item><title>x</title></item></channel></rss>"
        feed = RSSFeedConfig(
            id=OFFICIAL_FEED_ID,
            name="官网变化",
            url="https://example.test/feed.xml",
        )
        fetcher = RSSFetcher([feed], request_interval=0)
        with mock.patch.object(fetcher.session, "get", return_value=response):
            items, error = fetcher.fetch_feed(feed)

        self.assertEqual(items, [])
        self.assertIn("未声明完整", error)


class RSSFetcherResilienceTests(unittest.TestCase):
    @staticmethod
    def _simple_feed_response(title: str) -> requests.Response:
        response = requests.Response()
        response.status_code = 200
        response._content = f"""<?xml version="1.0" encoding="utf-8"?>
        <rss version="2.0"><channel><title>Test Feed</title>
          <item><title>{title}</title><link>https://example.test/{title}</link>
          <guid>guid:{title}</guid></item>
        </channel></rss>""".encode("utf-8")
        return response

    def test_fetch_feed_retries_transient_timeout_then_succeeds(self) -> None:
        feed = RSSFeedConfig(id="feed-a", name="源 A", url="https://example.test/a.xml")
        fetcher = RSSFetcher([feed], request_interval=0)
        with mock.patch.object(
            fetcher.session, "get",
            side_effect=[requests.Timeout("t"), self._simple_feed_response("ok")],
        ) as mock_get, mock.patch("trendradar.crawler.rss.fetcher.time.sleep") as mock_sleep:
            items, error = fetcher.fetch_feed(feed)

        self.assertIsNone(error)
        self.assertEqual(len(items), 1)
        self.assertEqual(mock_get.call_count, 2)
        mock_sleep.assert_called_once()

    def test_fetch_feed_gives_up_after_max_retries(self) -> None:
        feed = RSSFeedConfig(id="feed-b", name="源 B", url="https://example.test/b.xml")
        fetcher = RSSFetcher([feed], request_interval=0)
        with mock.patch.object(
            fetcher.session, "get", side_effect=requests.Timeout("t"),
        ) as mock_get, mock.patch("trendradar.crawler.rss.fetcher.time.sleep"):
            items, error = fetcher.fetch_feed(feed)

        self.assertEqual(items, [])
        self.assertIn("请求超时", error)
        self.assertIn("已重试", error)
        self.assertEqual(mock_get.call_count, 3)  # 首次 + 2 次重试

    def test_fetch_feed_does_not_retry_permanent_404(self) -> None:
        feed = RSSFeedConfig(id="feed-c", name="源 C", url="https://example.test/c.xml")
        fetcher = RSSFetcher([feed], request_interval=0)

        response_404 = requests.Response()
        response_404.status_code = 404

        with mock.patch.object(
            fetcher.session, "get", return_value=response_404,
        ) as mock_get, mock.patch("trendradar.crawler.rss.fetcher.time.sleep") as mock_sleep:
            items, error = fetcher.fetch_feed(feed)

        self.assertEqual(items, [])
        self.assertIsNotNone(error)
        self.assertEqual(mock_get.call_count, 1)  # 永久性错误不重试
        mock_sleep.assert_not_called()

    def test_fetch_all_runs_concurrently_and_aggregates_every_feed(self) -> None:
        feeds = [
            RSSFeedConfig(id=f"feed-{i}", name=f"源 {i}", url=f"https://example.test/{i}.xml")
            for i in range(5)
        ]
        fetcher = RSSFetcher(feeds, request_interval=0)

        def fake_get(url, timeout):
            index = url.rsplit("/", 1)[-1].split(".")[0]
            return self._simple_feed_response(f"item-{index}")

        with mock.patch.object(fetcher.session, "get", side_effect=fake_get):
            rss_data = fetcher.fetch_all()

        self.assertEqual(len(rss_data.items), 5)
        self.assertEqual(rss_data.failed_ids, [])
        for i in range(5):
            self.assertIn(f"feed-{i}", rss_data.items)


class RSSAuthoritativeSyncTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.storage = LocalStorageBackend(
            data_dir=self.temporary.name,
            enable_txt=False,
            enable_html=False,
        )

    def tearDown(self) -> None:
        self.storage.cleanup()
        self.temporary.cleanup()

    def _active_rows(self):
        conn = self.storage._get_connection(TEST_DATE, db_type="rss")
        return conn.execute(
            "SELECT * FROM rss_items WHERE is_active = 1 ORDER BY change_id"
        ).fetchall()

    def test_existing_rss_database_is_migrated_before_change_indexes(self) -> None:
        db_dir = Path(self.temporary.name) / "rss"
        db_dir.mkdir(parents=True, exist_ok=True)
        db_path = db_dir / f"{TEST_DATE}.db"
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE rss_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                feed_id TEXT NOT NULL,
                url TEXT NOT NULL,
                published_at TEXT,
                summary TEXT,
                author TEXT,
                first_crawl_time TEXT NOT NULL,
                last_crawl_time TEXT NOT NULL,
                crawl_count INTEGER DEFAULT 1,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE official_change_checkpoint (
                change_id TEXT PRIMARY KEY,
                revision INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT '',
                reported_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            INSERT INTO official_change_checkpoint
            (change_id, revision, status, reported_at, updated_at)
            VALUES ('change:legacy', 1, 'confirmed', 'old', 'old')
        """)
        conn.commit()
        conn.close()

        migrated = self.storage._get_connection(TEST_DATE, db_type="rss")
        columns = {
            row[1] for row in migrated.execute("PRAGMA table_info(rss_items)").fetchall()
        }
        indexes = {
            row[1] for row in migrated.execute("PRAGMA index_list(rss_items)").fetchall()
        }
        self.assertTrue({
            "guid", "change_id", "revision", "status", "is_active", "ai_sync_pending"
        } <= columns)
        self.assertIn("idx_rss_change_feed", indexes)
        checkpoint_columns = {
            row[1]
            for row in migrated.execute(
                "PRAGMA table_info(official_change_checkpoint)"
            ).fetchall()
        }
        self.assertTrue({"delivery_state", "delivered_at"} <= checkpoint_columns)
        self.assertEqual(
            migrated.execute("""
                SELECT delivery_state FROM official_change_checkpoint
                WHERE change_id='change:legacy'
            """).fetchone()[0],
            "delivered",
        )

    def _install_ai_results(self) -> dict[str, int]:
        rss_conn = self.storage._get_connection(TEST_DATE, db_type="rss")
        ids = {
            row["change_id"]: int(row["id"])
            for row in rss_conn.execute(
                "SELECT id, change_id FROM rss_items WHERE feed_id = ?",
                (OFFICIAL_FEED_ID,),
            ).fetchall()
        }
        news_conn = self.storage._get_connection(TEST_DATE)
        news_conn.execute("""
            INSERT INTO ai_filter_tags
            (tag, description, priority, status, version, prompt_hash, interests_file, created_at)
            VALUES ('政策变化', '', 1, 'active', 1, 'hash', 'ai_interests.txt', 'now')
        """)
        tag_id = news_conn.execute("SELECT id FROM ai_filter_tags").fetchone()[0]
        for rss_id in ids.values():
            news_conn.execute("""
                INSERT INTO ai_filter_results
                (news_item_id, source_type, tag_id, relevance_score, status, created_at)
                VALUES (?, 'rss', ?, 1.0, 'active', 'now')
            """, (rss_id, tag_id))
            news_conn.execute("""
                INSERT INTO ai_filter_analyzed_news
                (news_item_id, source_type, interests_file, prompt_hash, matched, created_at)
                VALUES (?, 'rss', 'ai_interests.txt', 'hash', 1, 'now')
            """, (rss_id,))
        news_conn.commit()
        return ids

    def test_latest_valid_set_retracts_missing_and_invalidates_ai(self) -> None:
        initial = rss_data("10:00", {
            OFFICIAL_FEED_ID: [
                change_item("change:bg", "保加利亚旧摘要"),
                change_item("change:hainan", "海南旧误报"),
            ]
        })
        self.assertTrue(self.storage.save_rss_data(initial))
        ids = self._install_ai_results()

        revised_bg = change_item("change:bg", "保加利亚新摘要", revision=2)
        current = rss_data("11:00", {OFFICIAL_FEED_ID: [revised_bg]})
        self.assertTrue(self.storage.save_rss_data(current))

        rss_conn = self.storage._get_connection(TEST_DATE, db_type="rss")
        bg = rss_conn.execute(
            "SELECT * FROM rss_items WHERE change_id = 'change:bg'"
        ).fetchone()
        hainan = rss_conn.execute(
            "SELECT * FROM rss_items WHERE change_id = 'change:hainan'"
        ).fetchone()
        self.assertEqual((bg["revision"], bg["status"], bg["is_active"]), (2, "confirmed", 1))
        self.assertEqual((hainan["status"], hainan["is_active"]), ("retracted", 0))

        visible = self.storage.get_rss_data(TEST_DATE)
        self.assertEqual(
            [item.change_id for item in visible.items[OFFICIAL_FEED_ID]],
            ["change:bg"],
        )

        news_conn = self.storage._get_connection(TEST_DATE)
        active_ai = news_conn.execute(
            "SELECT COUNT(*) FROM ai_filter_results WHERE status = 'active'"
        ).fetchone()[0]
        analyzed = news_conn.execute(
            "SELECT COUNT(*) FROM ai_filter_analyzed_news"
        ).fetchone()[0]
        self.assertEqual(active_ai, 0)
        self.assertEqual(analyzed, 0)

        changed = self.storage.detect_new_rss_items(current)
        self.assertEqual([item.change_id for item in changed[OFFICIAL_FEED_ID]], ["change:bg"])
        self.assertTrue(
            self.storage.acknowledge_official_changes([("change:bg", 2)])
        )

        unchanged = rss_data("12:00", {OFFICIAL_FEED_ID: [revised_bg]})
        self.assertTrue(self.storage.save_rss_data(unchanged))
        self.assertEqual(self.storage.detect_new_rss_items(unchanged), {})
        self.assertEqual(ids["change:hainan"], int(hainan["id"]))

    def test_ai_invalidation_is_retried_after_partial_failure(self) -> None:
        first = rss_data("10:00", {
            OFFICIAL_FEED_ID: [change_item("change:bg", "旧摘要")]
        })
        self.assertTrue(self.storage.save_rss_data(first))
        self._install_ai_results()

        revised = rss_data("11:00", {
            OFFICIAL_FEED_ID: [change_item("change:bg", "新摘要", revision=2)]
        })
        with mock.patch.object(
            self.storage,
            "_invalidate_rss_ai_state",
            side_effect=RuntimeError("simulated news db failure"),
        ):
            self.assertFalse(self.storage.save_rss_data(revised))

        rss_conn = self.storage._get_connection(TEST_DATE, db_type="rss")
        self.assertEqual(
            rss_conn.execute(
                "SELECT ai_sync_pending FROM rss_items WHERE change_id='change:bg'"
            ).fetchone()[0],
            1,
        )

        retry = rss_data("12:00", {
            OFFICIAL_FEED_ID: [change_item("change:bg", "新摘要", revision=2)]
        })
        self.assertTrue(self.storage.save_rss_data(retry))
        self.assertEqual(
            rss_conn.execute(
                "SELECT ai_sync_pending FROM rss_items WHERE change_id='change:bg'"
            ).fetchone()[0],
            0,
        )
        news_conn = self.storage._get_connection(TEST_DATE)
        self.assertEqual(
            news_conn.execute(
                "SELECT COUNT(*) FROM ai_filter_results WHERE status='active'"
            ).fetchone()[0],
            0,
        )

    def test_failed_authoritative_fetch_does_not_retract(self) -> None:
        initial = rss_data("10:00", {
            OFFICIAL_FEED_ID: [change_item("change:bg", "保加利亚")]
        })
        self.assertTrue(self.storage.save_rss_data(initial))

        failed = rss_data("11:00", {}, failed_ids=[OFFICIAL_FEED_ID])
        self.assertTrue(self.storage.save_rss_data(failed))

        self.assertEqual(len(self._active_rows()), 1)
        self.assertEqual(self._active_rows()[0]["status"], "confirmed")

    def test_unverified_authoritative_snapshot_does_not_retract(self) -> None:
        initial = rss_data("10:00", {
            OFFICIAL_FEED_ID: [change_item("change:bg", "保加利亚")]
        })
        self.assertTrue(self.storage.save_rss_data(initial))

        incomplete = RSSData(
            date=TEST_DATE,
            crawl_time="11:00",
            items={OFFICIAL_FEED_ID: []},
            id_to_name={OFFICIAL_FEED_ID: "官网变化"},
        )
        self.assertTrue(self.storage.save_rss_data(incomplete))

        self.assertEqual(len(self._active_rows()), 1)

    def test_official_change_revision_checkpoint_is_cross_day(self) -> None:
        first_item = change_item("change:bg", "保加利亚", revision=1)
        first = RSSData(
            date="2026-07-22",
            crawl_time="10:00",
            items={OFFICIAL_FEED_ID: [first_item]},
            id_to_name={OFFICIAL_FEED_ID: "官网变化"},
            authoritative_complete_ids=[OFFICIAL_FEED_ID],
        )
        self.assertTrue(self.storage.save_rss_data(first))
        self.assertEqual(
            [item.change_id for item in self.storage.detect_new_rss_items(first)[OFFICIAL_FEED_ID]],
            ["change:bg"],
        )
        self.assertTrue(
            self.storage.acknowledge_official_changes([("change:bg", 1)])
        )

        next_day = RSSData(
            date="2026-07-23",
            crawl_time="10:00",
            items={OFFICIAL_FEED_ID: [change_item("change:bg", "保加利亚", revision=1)]},
            id_to_name={OFFICIAL_FEED_ID: "官网变化"},
            authoritative_complete_ids=[OFFICIAL_FEED_ID],
        )
        self.assertTrue(self.storage.save_rss_data(next_day))
        self.assertEqual(self.storage.detect_new_rss_items(next_day), {})

        revised = RSSData(
            date="2026-07-23",
            crawl_time="11:00",
            items={OFFICIAL_FEED_ID: [change_item("change:bg", "新规则", revision=2)]},
            id_to_name={OFFICIAL_FEED_ID: "官网变化"},
            authoritative_complete_ids=[OFFICIAL_FEED_ID],
        )
        self.assertTrue(self.storage.save_rss_data(revised))
        self.assertEqual(
            [item.change_id for item in self.storage.detect_new_rss_items(revised)[OFFICIAL_FEED_ID]],
            ["change:bg"],
        )

    def test_pending_official_change_replays_after_restart_until_ack(self) -> None:
        first = RSSData(
            date="2026-07-22",
            crawl_time="10:00",
            items={
                OFFICIAL_FEED_ID: [
                    change_item("change:retry", "待交付规则", revision=1)
                ]
            },
            id_to_name={OFFICIAL_FEED_ID: "官网变化"},
            authoritative_complete_ids=[OFFICIAL_FEED_ID],
        )
        self.assertTrue(self.storage.save_rss_data(first))
        detected = self.storage.detect_new_rss_items(first)
        self.assertEqual(
            [item.change_id for item in detected[OFFICIAL_FEED_ID]],
            ["change:retry"],
        )
        # 同一进程重复读取仍返回相同待交付批次。
        self.assertEqual(
            [
                item.change_id
                for item in self.storage.detect_new_rss_items(first)[OFFICIAL_FEED_ID]
            ],
            ["change:retry"],
        )

        checkpoint = self.storage._get_connection(
            "__official-change-checkpoint__",
            db_type="rss",
        )
        self.assertEqual(
            checkpoint.execute("""
                SELECT delivery_state FROM official_change_checkpoint
                WHERE change_id='change:retry'
            """).fetchone()[0],
            "pending",
        )

        # 模拟检测完成、报告生成前崩溃。
        self.storage.cleanup()
        self.storage = LocalStorageBackend(
            data_dir=self.temporary.name,
            enable_txt=False,
            enable_html=False,
        )
        retry = RSSData(
            date="2026-07-23",
            crawl_time="10:00",
            items={
                OFFICIAL_FEED_ID: [
                    change_item("change:retry", "待交付规则", revision=1)
                ]
            },
            id_to_name={OFFICIAL_FEED_ID: "官网变化"},
            authoritative_complete_ids=[OFFICIAL_FEED_ID],
        )
        self.assertTrue(self.storage.save_rss_data(retry))
        replayed = self.storage.detect_new_rss_items(retry)
        self.assertEqual(
            [item.change_id for item in replayed[OFFICIAL_FEED_ID]],
            ["change:retry"],
        )
        self.assertTrue(
            self.storage.acknowledge_official_changes([("change:retry", 1)])
        )

        delivered = RSSData(
            date="2026-07-24",
            crawl_time="10:00",
            items={
                OFFICIAL_FEED_ID: [
                    change_item("change:retry", "待交付规则", revision=1)
                ]
            },
            id_to_name={OFFICIAL_FEED_ID: "官网变化"},
            authoritative_complete_ids=[OFFICIAL_FEED_ID],
        )
        self.assertTrue(self.storage.save_rss_data(delivered))
        self.assertEqual(self.storage.detect_new_rss_items(delivered), {})

    def test_failed_delivery_ack_restores_pending_checkpoint(self) -> None:
        current = rss_data(
            "10:00",
            {
                OFFICIAL_FEED_ID: [
                    change_item("change:ack-retry", "确认失败后重试")
                ]
            },
        )
        self.assertTrue(self.storage.save_rss_data(current))
        self.assertIn(
            OFFICIAL_FEED_ID,
            self.storage.detect_new_rss_items(current),
        )

        with mock.patch.object(
            self.storage,
            "_persist_official_change_checkpoint",
            return_value=False,
            create=True,
        ):
            self.assertFalse(
                self.storage.acknowledge_official_changes(
                    [("change:ack-retry", 1)]
                )
            )

        checkpoint = self.storage._get_connection(
            "__official-change-checkpoint__",
            db_type="rss",
        )
        self.assertEqual(
            tuple(checkpoint.execute("""
                SELECT delivery_state, delivered_at
                FROM official_change_checkpoint
                WHERE change_id='change:ack-retry'
            """).fetchone()),
            ("pending", None),
        )
        replayed = self.storage.detect_new_rss_items(current)
        self.assertEqual(
            [item.change_id for item in replayed[OFFICIAL_FEED_ID]],
            ["change:ack-retry"],
        )

    def test_ambiguous_delivery_write_is_confirmed_by_remote_readback(self) -> None:
        current = rss_data(
            "10:00",
            {
                OFFICIAL_FEED_ID: [
                    change_item("change:ambiguous", "响应丢失但远端已写入")
                ]
            },
        )
        self.assertTrue(self.storage.save_rss_data(current))
        self.assertIn(
            OFFICIAL_FEED_ID,
            self.storage.detect_new_rss_items(current),
        )
        remote_copy = Path(self.temporary.name) / "remote-checkpoint.db"

        def ambiguous_persist() -> bool:
            source = self.storage._get_connection(
                "__official-change-checkpoint__",
                db_type="rss",
            )
            destination = sqlite3.connect(remote_copy)
            try:
                source.backup(destination)
            finally:
                destination.close()
            return False

        def verify_remote(
            change_revisions: list[tuple[str, int]],
            expected_state: str,
        ) -> bool:
            remote = sqlite3.connect(remote_copy)
            try:
                rows = remote.execute("""
                    SELECT change_id, revision, delivery_state
                    FROM official_change_checkpoint
                """).fetchall()
            finally:
                remote.close()
            states = {
                str(change_id): (int(revision), str(delivery_state))
                for change_id, revision, delivery_state in rows
            }
            return all(
                states.get(change_id) == (revision, expected_state)
                for change_id, revision in change_revisions
            )

        with (
            mock.patch.object(
                self.storage,
                "_persist_official_change_checkpoint",
                side_effect=ambiguous_persist,
                create=True,
            ),
            mock.patch.object(
                self.storage,
                "_verify_official_change_checkpoint_state",
                side_effect=verify_remote,
                create=True,
            ),
        ):
            self.assertTrue(
                self.storage.acknowledge_official_changes(
                    [("change:ambiguous", 1)]
                )
            )

        checkpoint = self.storage._get_connection(
            "__official-change-checkpoint__",
            db_type="rss",
        )
        self.assertEqual(
            checkpoint.execute("""
                SELECT delivery_state FROM official_change_checkpoint
                WHERE change_id='change:ambiguous'
            """).fetchone()[0],
            "delivered",
        )

    def test_supersedes_marks_previous_change_inactive(self) -> None:
        old = change_item("change:old", "旧规则")
        self.assertTrue(self.storage.save_rss_data(
            rss_data("10:00", {OFFICIAL_FEED_ID: [old]})
        ))
        new = change_item("change:new", "新规则", supersedes="change:old")
        self.assertTrue(self.storage.save_rss_data(
            rss_data("11:00", {OFFICIAL_FEED_ID: [new]})
        ))

        conn = self.storage._get_connection(TEST_DATE, db_type="rss")
        old_row = conn.execute(
            "SELECT status, is_active FROM rss_items WHERE change_id = 'change:old'"
        ).fetchone()
        self.assertEqual((old_row["status"], old_row["is_active"]), ("superseded", 0))

    def test_generic_rss_missing_item_is_not_authoritatively_retracted(self) -> None:
        generic = RSSItem(
            title="普通新闻",
            feed_id="generic",
            feed_name="普通源",
            url="https://example.test/news",
            guid="generic-guid",
        )
        first = RSSData(
            date=TEST_DATE,
            crawl_time="10:00",
            items={"generic": [generic]},
            id_to_name={"generic": "普通源"},
        )
        empty_success = RSSData(
            date=TEST_DATE,
            crawl_time="11:00",
            items={"generic": []},
            id_to_name={"generic": "普通源"},
        )

        self.assertTrue(self.storage.save_rss_data(first))
        self.assertTrue(self.storage.save_rss_data(empty_success))

        conn = self.storage._get_connection(TEST_DATE, db_type="rss")
        row = conn.execute("SELECT status, is_active FROM rss_items").fetchone()
        self.assertEqual((row["status"], row["is_active"]), ("", 1))


class OfficialChangeDeliveryBoundaryTests(unittest.TestCase):
    @staticmethod
    def _official_group() -> dict:
        return {
            "word": "官网政策变化",
            "count": 1,
            "is_official_change_group": True,
            "titles": [{
                "title": "[政策变化] 交付测试",
                "source_id": OFFICIAL_FEED_ID,
                "change_id": "change:delivery",
                "revision": 1,
                "status": "confirmed",
            }],
        }

    def _analyzer(
        self,
        html_file: str | None,
        *,
        include_official_change: bool = True,
    ) -> NewsAnalyzer:
        analyzer = NewsAnalyzer.__new__(NewsAnalyzer)
        analyzer.ctx = mock.MagicMock()
        analyzer.ctx.platform_ids = []
        analyzer.ctx.filter_method = "keyword"
        analyzer.ctx.detect_new_titles.return_value = {}
        analyzer.ctx.format_time.return_value = "10-00"
        analyzer.ctx.load_frequency_words.return_value = ([], [], [])
        analyzer.ctx.create_scheduler.return_value.resolve.return_value = SimpleNamespace(
            report_mode="incremental",
            frequency_file=None,
            filter_method="keyword",
            interests_file=None,
            collect=True,
        )
        analyzer.report_mode = "incremental"
        analyzer.frequency_file = None
        analyzer.filter_method = "keyword"
        analyzer.interests_file = None
        analyzer.is_docker_container = False
        analyzer._pending_official_changes = {("change:delivery", 1)}
        analyzer.storage_manager = mock.MagicMock()
        analyzer.storage_manager.acknowledge_official_changes.return_value = True
        analyzer._prepare_current_title_info = mock.MagicMock(return_value={})
        analyzer._prepare_standalone_data = mock.MagicMock(return_value=None)
        rss_items = [self._official_group()] if include_official_change else None
        analyzer._run_analysis_pipeline = mock.MagicMock(
            return_value=([], html_file, None, rss_items, None, None)
        )
        analyzer._send_notification_if_needed = mock.MagicMock(return_value=False)
        analyzer._should_open_browser = mock.MagicMock(return_value=False)
        return analyzer

    def test_successful_html_report_acknowledges_pending_changes(self) -> None:
        analyzer = self._analyzer("output/report.html")
        analyzer._execute_mode_strategy(
            {"should_send_notification": False, "report_type": "增量分析"},
            {},
            {},
            [],
        )
        analyzer.storage_manager.acknowledge_official_changes.assert_called_once_with(
            [("change:delivery", 1)]
        )
        self.assertEqual(analyzer._pending_official_changes, set())

    def test_failed_report_keeps_changes_pending(self) -> None:
        analyzer = self._analyzer(None)
        analyzer._execute_mode_strategy(
            {"should_send_notification": False, "report_type": "增量分析"},
            {},
            {},
            [],
        )
        analyzer.storage_manager.acknowledge_official_changes.assert_not_called()
        self.assertEqual(
            analyzer._pending_official_changes,
            {("change:delivery", 1)},
        )

    def test_unrelated_html_does_not_acknowledge_hidden_change(self) -> None:
        analyzer = self._analyzer(
            "output/report.html",
            include_official_change=False,
        )
        analyzer._execute_mode_strategy(
            {"should_send_notification": False, "report_type": "增量分析"},
            {},
            {},
            [],
        )
        analyzer.storage_manager.acknowledge_official_changes.assert_not_called()
        self.assertEqual(
            analyzer._pending_official_changes,
            {("change:delivery", 1)},
        )

    def test_official_change_bypasses_keyword_filter_and_is_acknowledged(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            storage = LocalStorageBackend(
                data_dir=temporary,
                enable_txt=False,
                enable_html=False,
            )
            try:
                item = change_item(
                    "change:keyword-bypass",
                    "[政策变化] 不含业务关键词的规则",
                )
                current = rss_data("10:00", {OFFICIAL_FEED_ID: [item]})
                self.assertTrue(storage.save_rss_data(current))

                analyzer = NewsAnalyzer.__new__(NewsAnalyzer)
                analyzer.ctx = SimpleNamespace(
                    timezone="Asia/Shanghai",
                    rss_config={
                        "FRESHNESS_FILTER": {
                            "ENABLED": True,
                            "MAX_AGE_DAYS": 30,
                        }
                    },
                    rss_feeds=[{
                        "id": OFFICIAL_FEED_ID,
                        "max_age_days": 30,
                    }],
                    config={
                        "DISPLAY": {"REGIONS": {"RSS": True}},
                        "MAX_NEWS_PER_KEYWORD": 0,
                        "SORT_BY_POSITION_FIRST": False,
                        "TIMEZONE": "Asia/Shanghai",
                        "DEBUG": False,
                    },
                    load_frequency_words=lambda _: ([{
                        "required": [],
                        "normal": ["绝不匹配"],
                        "group_key": "无匹配",
                    }], [], []),
                )
                analyzer.storage_manager = storage
                analyzer.report_mode = "incremental"
                analyzer.frequency_file = None
                analyzer.rank_threshold = 50
                analyzer._pending_official_changes = set()
                analyzer._rss_total_count = 0

                rss_stats, _, _, _ = analyzer._process_rss_data_by_mode(current)
                self.assertEqual(rss_stats[0]["word"], "官网政策变化")
                self.assertEqual(
                    rss_stats[0]["titles"][0]["change_id"],
                    "change:keyword-bypass",
                )
                delivered = analyzer._official_change_revisions_in_report(rss_stats)
                self.assertTrue(
                    analyzer._acknowledge_pending_official_changes(delivered)
                )
                checkpoint = storage._get_connection(
                    "__official-change-checkpoint__",
                    db_type="rss",
                )
                self.assertEqual(
                    checkpoint.execute("""
                        SELECT delivery_state FROM official_change_checkpoint
                        WHERE change_id='change:keyword-bypass'
                    """).fetchone()[0],
                    "delivered",
                )
            finally:
                storage.cleanup()

    def test_ai_filter_cannot_remove_official_change_group(self) -> None:
        analyzer = NewsAnalyzer.__new__(NewsAnalyzer)
        analyzer.ctx = mock.MagicMock()
        analyzer.ctx.config = {
            "AI_ANALYSIS": {"ENABLED": False},
            "AI_TRANSLATION": {"ENABLED": False},
            "STORAGE": {"FORMATS": {"HTML": False}},
        }
        analyzer.ctx.display_mode = "keyword"
        analyzer.ctx.run_ai_filter.return_value = SimpleNamespace(
            success=True,
            total_matched=0,
            tags=[],
        )
        analyzer.ctx.convert_ai_filter_to_report_data.return_value = ([], [], [])
        analyzer.filter_method = "ai"
        analyzer.interests_file = None
        analyzer.rank_threshold = 50
        analyzer._rss_total_count = 1
        analyzer._rss_source_total = 1
        analyzer._rss_source_failed = 0

        result = analyzer._run_analysis_pipeline(
            {},
            "incremental",
            {},
            {},
            [],
            [],
            {},
            rss_items=[self._official_group()],
        )

        self.assertEqual(result[3][0]["word"], "官网政策变化")

    def test_delivered_change_does_not_reenter_current_or_daily_rss_groups(self) -> None:
        for report_mode in ("current", "daily"):
            with self.subTest(report_mode=report_mode), tempfile.TemporaryDirectory() as temporary:
                storage = LocalStorageBackend(
                    data_dir=temporary,
                    enable_txt=False,
                    enable_html=False,
                )
                try:
                    analyzer = NewsAnalyzer.__new__(NewsAnalyzer)
                    analyzer.ctx = SimpleNamespace(
                        timezone="Asia/Shanghai",
                        rss_config={
                            "FRESHNESS_FILTER": {
                                "ENABLED": True,
                                "MAX_AGE_DAYS": 30,
                            }
                        },
                        rss_feeds=[{
                            "id": OFFICIAL_FEED_ID,
                            "max_age_days": 30,
                        }],
                        config={
                            "DISPLAY": {"REGIONS": {"RSS": True}},
                            "MAX_NEWS_PER_KEYWORD": 0,
                            "SORT_BY_POSITION_FIRST": False,
                            "TIMEZONE": "Asia/Shanghai",
                            "DEBUG": False,
                        },
                        load_frequency_words=lambda _: ([], [], []),
                    )
                    analyzer.storage_manager = storage
                    analyzer.report_mode = report_mode
                    analyzer.frequency_file = None
                    analyzer.rank_threshold = 50
                    analyzer._pending_official_changes = set()
                    analyzer._rss_total_count = 0

                    first = rss_data("10:00", {
                        OFFICIAL_FEED_ID: [
                            change_item(
                                "change:no-replay",
                                "[政策变化] 仅投递一次",
                            )
                        ]
                    })
                    self.assertTrue(storage.save_rss_data(first))
                    first_stats, _, _, _ = analyzer._process_rss_data_by_mode(first)
                    self.assertEqual(first_stats[0]["word"], "官网政策变化")
                    delivered = analyzer._official_change_revisions_in_report(first_stats)
                    self.assertTrue(
                        analyzer._acknowledge_pending_official_changes(delivered)
                    )

                    second = rss_data("11:00", {
                        OFFICIAL_FEED_ID: [
                            change_item(
                                "change:no-replay",
                                "[政策变化] 仅投递一次",
                            )
                        ]
                    })
                    self.assertTrue(storage.save_rss_data(second))
                    second_stats, second_new_stats, raw_items, _ = (
                        analyzer._process_rss_data_by_mode(second)
                    )

                    self.assertIsNone(second_stats)
                    self.assertIsNone(second_new_stats)
                    self.assertFalse(raw_items)
                    self.assertEqual(analyzer._pending_official_changes, set())
                finally:
                    storage.cleanup()

    def test_rss_region_disabled_keeps_official_change_pending(self) -> None:
        analyzer = NewsAnalyzer.__new__(NewsAnalyzer)
        analyzer.ctx = SimpleNamespace(
            timezone="Asia/Shanghai",
            rss_config={"FRESHNESS_FILTER": {"ENABLED": True}},
            rss_feeds=[],
            config={
                "DISPLAY": {"REGIONS": {"RSS": False}},
                "TIMEZONE": "Asia/Shanghai",
                "DEBUG": False,
            },
            load_frequency_words=lambda _: ([], [], []),
        )
        analyzer.storage_manager = mock.MagicMock()
        analyzer.storage_manager.detect_new_rss_items.return_value = {
            OFFICIAL_FEED_ID: [
                change_item("change:rss-off", "[政策变化] RSS 关闭")
            ]
        }
        analyzer.report_mode = "incremental"
        analyzer.frequency_file = None
        analyzer.rank_threshold = 50
        analyzer._pending_official_changes = set()

        rss_stats, _, _, _ = analyzer._process_rss_data_by_mode(
            rss_data("10:00", {})
        )

        self.assertIsNone(rss_stats)
        delivered = analyzer._official_change_revisions_in_report(rss_stats)
        analyzer._acknowledge_pending_official_changes(delivered)
        analyzer.storage_manager.acknowledge_official_changes.assert_not_called()
        self.assertEqual(
            analyzer._pending_official_changes,
            {("change:rss-off", 1)},
        )


if __name__ == "__main__":
    unittest.main()
