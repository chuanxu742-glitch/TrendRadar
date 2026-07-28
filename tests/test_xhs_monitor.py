from __future__ import annotations

import json
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from urllib.request import urlopen

from xhs_monitor.service import Settings, build_handler, collect_once
from xhs_monitor.store import XiaohongshuStore
from http.server import ThreadingHTTPServer


KEYWORDS = [
    {
        "id": "pet-cabin-refused",
        "query": "宠物进客舱被拒载",
        "name": "小红书·宠物进客舱被拒载",
    },
    {
        "id": "service-dog-cabin-refused",
        "query": "服务犬进客舱被拒载",
        "name": "小红书·服务犬进客舱被拒载",
    },
]


class FakeFetcher:
    def fetch_all(self):
        return (
            {
                "xhs-pet-cabin-refused": {
                    "宠物进客舱被拒载案例 [note1234]": {
                        "ranks": [1],
                        "url": "https://www.xiaohongshu.com/explore/note1234",
                    },
                    "非法跳转 [badurl12]": {
                        "ranks": [2],
                        "url": "https://example.com/collect",
                    },
                }
            },
            {},
            ["xhs-service-dog-cabin-refused"],
        )


class XiaohongshuMonitorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = XiaohongshuStore(self.root / "xhs-monitor.db")
        self.settings = Settings(
            enabled=True,
            port=8091,
            interval_seconds=1800,
            immediate_run=True,
            database_path=self.root / "xhs-monitor.db",
            keywords=KEYWORDS,
            fetcher_config={"COOKIE": "not-exposed", "KEYWORDS": KEYWORDS},
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_partial_collection_is_persisted_and_summarized(self) -> None:
        result = collect_once(
            self.settings,
            self.store,
            fetcher_factory=lambda _config: FakeFetcher(),
        )
        summary = self.store.summary()

        self.assertEqual(result["status"], "partial")
        self.assertEqual(summary["status"], "partial")
        self.assertEqual(summary["source_count"], 2)
        self.assertEqual(summary["successful_sources"], 1)
        self.assertEqual(summary["failed_sources"], 1)
        self.assertEqual(summary["recent_count"], 2)
        self.assertEqual(summary["items"][0]["title"], "宠物进客舱被拒载案例")
        self.assertEqual(summary["items"][1]["url"], "")
        self.assertNotIn("COOKIE", str(summary))
        self.assertNotIn("not-exposed", str(summary))

    def test_repeated_collection_deduplicates_notes(self) -> None:
        for _ in range(2):
            collect_once(
                self.settings,
                self.store,
                fetcher_factory=lambda _config: FakeFetcher(),
            )

        self.assertEqual(self.store.summary()["recent_count"], 2)

    def test_collector_failure_does_not_expose_exception(self) -> None:
        def failed_factory(_config):
            raise RuntimeError("cookie=secret")

        result = collect_once(
            self.settings,
            self.store,
            fetcher_factory=failed_factory,
        )
        summary = self.store.summary()

        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(summary["status_label"], "今日数据暂未更新")
        self.assertNotIn("secret", str(summary))
        self.assertNotIn("error", summary)

    def test_disabled_collection_records_safe_unavailable_state(self) -> None:
        result = collect_once(
            replace(self.settings, enabled=False),
            self.store,
            fetcher_factory=lambda _config: self.fail("fetcher must not run"),
        )

        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(self.store.summary()["status"], "unavailable")

    def test_summary_http_api_and_health_are_available(self) -> None:
        collect_once(
            self.settings,
            self.store,
            fetcher_factory=lambda _config: FakeFetcher(),
        )
        server = ThreadingHTTPServer(("127.0.0.1", 0), build_handler(self.store))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base_url = f"http://127.0.0.1:{server.server_port}"
            with urlopen(f"{base_url}/health/live", timeout=2) as response:
                self.assertEqual(json.load(response), {"status": "ok"})
            with urlopen(f"{base_url}/api/v1/summary?limit=1", timeout=2) as response:
                summary = json.load(response)
            self.assertEqual(summary["status"], "partial")
            self.assertEqual(len(summary["items"]), 1)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
