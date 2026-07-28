from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from monitor import official_monitor
from monitor.site_url_inventory import (
    classify_url_relevance,
    inventory_summary,
    mark_fetch_result,
    mark_scheduled,
    register_site_url,
    select_due_records,
    stable_url_decision,
)


DIRECT_TERMS = ("pet", "pets", "dog", "cat", "宠物", "犬", "猫")
HUB_TERMS = ("travel-info", "baggage", "special-services", "行李")


class StableUrlInventoryTests(unittest.TestCase):
    def test_stable_url_filter_records_executable_skip_reasons(self) -> None:
        self.assertEqual(
            stable_url_decision("https://air.test/assets/logo.svg"),
            (False, "static_asset"),
        )
        self.assertEqual(
            stable_url_decision("https://air.test/search?q=pet"),
            (False, "interactive_or_search_page"),
        )
        self.assertEqual(
            stable_url_decision("https://air.test/content?id=8372"),
            (True, ""),
        )

    def test_opaque_url_becomes_high_relevance_from_anchor_context(self) -> None:
        classified = classify_url_relevance(
            "https://air.test/content?id=8372",
            anchor="Traveling with pets",
            direct_terms=DIRECT_TERMS,
            hub_terms=HUB_TERMS,
        )
        self.assertEqual(classified["relevance"], "high")
        self.assertGreaterEqual(classified["relevance_score"], 80)

    def test_short_topic_terms_do_not_match_inside_unrelated_words(self) -> None:
        classified = classify_url_relevance(
            "https://air.test/vacation-locations",
            direct_terms=DIRECT_TERMS,
            hub_terms=HUB_TERMS,
        )
        self.assertEqual(classified["relevance"], "low")

    def test_every_observed_url_is_registered_with_context(self) -> None:
        inventory: dict[str, dict] = {}
        record = register_site_url(
            inventory,
            "https://air.test/content?id=8372",
            origin="https://air.test/",
            source_id="air-test",
            entity_ids=["airline:air-test"],
            discovery_method="sitemap",
            parent_url="https://air.test/sitemap.xml",
            anchor="Pet transport requirements",
            direct_terms=DIRECT_TERMS,
            hub_terms=HUB_TERMS,
            observed_at="2026-07-28T00:00:00+00:00",
        )
        self.assertIn(record["url"], inventory)
        self.assertEqual(record["relevance"], "high")
        self.assertEqual(record["fetch_status"], "unread")
        self.assertEqual(record["parent_urls"], ["https://air.test/sitemap.xml"])

    def test_low_relevance_sampling_rotates_oldest_unread_first(self) -> None:
        now = datetime(2026, 7, 28, tzinfo=timezone.utc)
        inventory = {
            "https://air.test/a": {
                "origin": "https://air.test/",
                "stable": True,
                "relevance": "low",
                "last_fetched_at": "",
                "last_scheduled_at": "",
            },
            "https://air.test/b": {
                "origin": "https://air.test/",
                "stable": True,
                "relevance": "low",
                "last_fetched_at": (now - timedelta(days=60)).isoformat(),
                "last_scheduled_at": (now - timedelta(days=60)).isoformat(),
            },
            "https://air.test/c": {
                "origin": "https://air.test/",
                "stable": True,
                "relevance": "low",
                "last_fetched_at": (now - timedelta(days=1)).isoformat(),
                "last_scheduled_at": (now - timedelta(days=1)).isoformat(),
            },
        }
        selected = select_due_records(
            inventory,
            origin="https://air.test/",
            relevance="low",
            limit=2,
            minimum_interval_seconds=30 * 86400,
            current_time=now,
        )
        self.assertEqual(selected, ["https://air.test/a", "https://air.test/b"])

    def test_fetch_result_updates_coverage_and_next_sample(self) -> None:
        record = {
            "url": "https://air.test/about",
            "origin": "https://air.test/",
            "stable": True,
            "relevance": "low",
            "fetch_status": "scheduled",
            "fetch_count": 0,
            "last_fetched_at": "",
        }
        updated = mark_fetch_result(
            mark_scheduled(record, "low-sample", "2026-07-28T00:00:00+00:00"),
            {
                "status": "ok",
                "status_code": 200,
                "checked_at": "2026-07-28T00:01:00+00:00",
                "validation": {"topic_relevant": False},
            },
            sampled_again_after_seconds=30 * 86400,
        )
        summary = inventory_summary({updated["url"]: updated})
        self.assertEqual(updated["last_skip_reason"], "content_not_pet_policy")
        self.assertEqual(updated["fetch_count"], 1)
        self.assertEqual(summary["fetch_coverage"], 1.0)
        self.assertEqual(summary["low_relevance_sampled"], 1)

    def test_reobserving_url_does_not_erase_fetch_history(self) -> None:
        inventory: dict[str, dict] = {}
        url = "https://air.test/about"
        registered = register_site_url(
            inventory,
            url,
            origin="https://air.test/",
            source_id="air-test",
            entity_ids=["airline:air-test"],
            discovery_method="sitemap",
            direct_terms=DIRECT_TERMS,
            hub_terms=HUB_TERMS,
            observed_at="2026-07-01T00:00:00+00:00",
        )
        inventory[url] = mark_fetch_result(
            registered,
            {
                "status": "ok",
                "checked_at": "2026-07-02T00:00:00+00:00",
                "validation": {"topic_relevant": False},
            },
            sampled_again_after_seconds=30 * 86400,
        )

        observed_again = register_site_url(
            inventory,
            url,
            origin="https://air.test/",
            source_id="air-test",
            entity_ids=["airline:air-test"],
            discovery_method="crawl-link",
            direct_terms=DIRECT_TERMS,
            hub_terms=HUB_TERMS,
            observed_at="2026-07-28T00:00:00+00:00",
        )

        self.assertEqual(observed_again["fetch_status"], "fetched")
        self.assertEqual(
            observed_again["last_fetched_at"], "2026-07-02T00:00:00+00:00"
        )
        self.assertEqual(
            observed_again["last_skip_reason"], "content_not_pet_policy"
        )

    def test_failed_fetch_is_not_counted_as_read_coverage(self) -> None:
        record = {
            "url": "https://air.test/blocked",
            "origin": "https://air.test/",
            "stable": True,
            "relevance": "high",
            "fetch_status": "unread",
            "fetch_count": 0,
        }
        failed = mark_fetch_result(
            record,
            {
                "status": "error",
                "checked_at": "2026-07-28T00:00:00+00:00",
                "error": "access denied",
            },
            sampled_again_after_seconds=30 * 86400,
        )
        summary = inventory_summary({failed["url"]: failed})
        self.assertEqual(summary["fetched_urls"], 0)
        self.assertEqual(summary["unread_urls"], 1)
        self.assertEqual(summary["fetch_coverage"], 0.0)
        self.assertEqual(summary["skip_reasons"], {"access_restricted": 1})


class SiteUrlInventoryPersistenceTests(unittest.TestCase):
    def test_legacy_json_migrates_to_indexed_sqlite_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            url = "https://air.test/pets"
            (root / "site-url-inventory.json").write_text(
                json.dumps({
                    "schema_version": 1,
                    "urls": {
                        url: {
                            "url": url,
                            "origin": "https://air.test/",
                            "stable": True,
                            "relevance": "high",
                            "fetch_status": "fetched",
                            "last_fetched_at": "2026-07-01T00:00:00+00:00",
                        }
                    },
                }),
                encoding="utf-8",
            )
            with mock.patch.object(official_monitor, "STATE_DIR", root):
                persisted, summary = official_monitor.persist_site_url_updates({
                    url: {
                        "url": url,
                        "origin": "https://air.test/",
                        "stable": True,
                        "relevance": "high",
                        "fetch_status": "unread",
                        "last_fetched_at": "",
                        "last_seen_at": "2026-07-28T00:00:00+00:00",
                    }
                })
                page, count = official_monitor.query_site_url_inventory(
                    origin="https://air.test/",
                    relevance="high",
                )

            self.assertTrue((root / "site-url-inventory.db").exists())
            self.assertEqual(count, 1)
            self.assertEqual(page[0]["fetch_status"], "fetched")
            self.assertEqual(persisted[url]["last_seen_at"], "2026-07-28T00:00:00+00:00")
            self.assertEqual(summary["stable_urls"], 1)

    def test_existing_sqlite_schema_backfills_skip_reasons(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = root / "site-url-inventory.db"
            url = "https://air.test/blocked"
            record = {
                "url": url,
                "origin": "https://air.test/",
                "stable": True,
                "relevance": "high",
                "fetch_status": "error",
                "last_skip_reason": "access denied",
            }
            connection = sqlite3.connect(database)
            connection.execute(
                """
                CREATE TABLE site_urls (
                    url TEXT PRIMARY KEY,
                    origin TEXT NOT NULL,
                    relevance TEXT NOT NULL,
                    stable INTEGER NOT NULL,
                    fetch_status TEXT NOT NULL,
                    last_seen_at TEXT,
                    data_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "INSERT INTO site_urls VALUES (?,?,?,?,?,?,?)",
                (
                    url,
                    record["origin"],
                    record["relevance"],
                    1,
                    record["fetch_status"],
                    "",
                    json.dumps(record),
                ),
            )
            connection.commit()
            connection.close()

            with mock.patch.object(official_monitor, "STATE_DIR", root):
                _, summary = official_monitor.persist_site_url_updates({})
                inspection = sqlite3.connect(database)
                try:
                    columns = {
                        row[1]
                        for row in inspection.execute(
                            "PRAGMA table_info(site_urls)"
                        )
                    }
                finally:
                    inspection.close()

            self.assertIn("last_skip_reason", columns)
            self.assertEqual(summary["fetched_urls"], 0)
            self.assertEqual(summary["unread_urls"], 1)
            self.assertEqual(summary["skip_reasons"], {"access_restricted": 1})


if __name__ == "__main__":
    unittest.main()
