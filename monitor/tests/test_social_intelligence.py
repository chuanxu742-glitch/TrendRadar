from __future__ import annotations

import io
import json
import unittest
from unittest.mock import patch

from monitor.social_intelligence import fetch_xiaohongshu_intelligence


class FakeResponse:
    status = 200

    def __init__(self, payload: object) -> None:
        self.buffer = io.BytesIO(
            json.dumps(payload, ensure_ascii=False).encode("utf-8")
        )

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        return self.buffer.read(size)


class XiaohongshuIntelligenceClientTests(unittest.TestCase):
    def test_reads_independent_summary_and_sanitizes_items(self) -> None:
        payload = {
            "status": "available",
            "status_label": "ignored upstream label",
            "source_count": 2,
            "successful_sources": 2,
            "failed_sources": 0,
            "updated_at": "2026-07-28T18:00:00+08:00",
            "today_count": 1,
            "recent_count": 99,
            "items": [
                {
                    "source_id": "xhs-pet",
                    "source_name": "小红书·宠物进客舱",
                    "title": "某航司拒绝宠物进入客舱",
                    "rank": 1,
                    "url": "https://www.xiaohongshu.com/explore/note1234",
                    "first_seen_at": "2026-07-28T18:00:00+08:00",
                    "last_seen_at": "2026-07-28T18:00:00+08:00",
                    "author": "客户 A",
                    "summary": "客户在机场办理时被告知宠物无法进入客舱。",
                    "business_value": "需核实航司现场执行条件。",
                    "content_excerpt": "办理当天，工作人员说明当前机型不接受宠物。",
                },
                {
                    "source_id": "xhs-pet",
                    "source_name": "小红书·宠物进客舱",
                    "title": "不安全链接会被清除",
                    "rank": 2,
                    "url": "https://example.com/collect",
                    "first_seen_at": "",
                    "last_seen_at": "",
                },
            ],
        }
        with patch(
            "monitor.social_intelligence.urlopen",
            return_value=FakeResponse(payload),
        ) as mocked:
            result = fetch_xiaohongshu_intelligence(
                "http://xhs-monitor:8091/api/v1/summary",
                limit=20,
            )

        self.assertEqual(result["status"], "available")
        self.assertEqual(result["status_label"], "今日已更新")
        self.assertEqual(result["recent_count"], 2)
        self.assertEqual(result["items"][1]["url"], "")
        self.assertEqual(
            result["items"][0]["summary"],
            "客户在机场办理时被告知宠物无法进入客舱。",
        )
        self.assertEqual(result["items"][0]["author"], "客户 A")
        request = mocked.call_args.args[0]
        self.assertIn("limit=20", request.full_url)

    def test_network_failure_is_business_safe_unavailable_state(self) -> None:
        with patch(
            "monitor.social_intelligence.urlopen",
            side_effect=OSError("connection refused; cookie=secret"),
        ):
            result = fetch_xiaohongshu_intelligence(
                "http://xhs-monitor:8091/api/v1/summary"
            )

        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["status_label"], "今日数据暂未更新")
        self.assertEqual(result["items"], [])
        self.assertNotIn("cookie", str(result).lower())
        self.assertNotIn("error", result)

    def test_invalid_response_is_not_forwarded_to_dashboard(self) -> None:
        with patch(
            "monitor.social_intelligence.urlopen",
            return_value=FakeResponse(
                {
                    "status": "degraded",
                    "items": [],
                    "internal_error": "login expired",
                }
            ),
        ):
            result = fetch_xiaohongshu_intelligence(
                "http://xhs-monitor:8091/api/v1/summary"
            )

        self.assertEqual(result["status"], "unavailable")
        self.assertNotIn("internal_error", result)


if __name__ == "__main__":
    unittest.main()
