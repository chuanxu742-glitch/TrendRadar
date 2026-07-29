import unittest
from unittest.mock import Mock

from trendradar.crawler.xiaohongshu import (
    XiaohongshuFetcher,
    XiaohongshuRiskControlError,
    XiaohongshuSessionError,
    _ensure_spider_xhs_import_path,
    spider_xhs_runtime,
)


def make_config(**overrides):
    config = {
        "COOKIE": "a1=a1-value; web_session=session-value",
        "KEYWORDS": [{"id": "ai", "query": "人工智能", "name": "小红书·人工智能"}],
        "LIMIT_PER_KEYWORD": 20,
        "SORT": "latest",
        "NOTE_TYPE": "all",
        "NOTE_TIME": "day",
        "INTERVAL_MIN_SECONDS": 0,
        "INTERVAL_MAX_SECONDS": 0,
    }
    config.update(overrides)
    return config


class XiaohongshuFetcherTests(unittest.TestCase):
    def test_search_delegates_to_spider_xhs_and_normalizes_results(self):
        api = Mock()
        api.search_note.return_value = (
            True,
            "成功",
            {
                "data": {
                    "items": [
                        {
                            "id": "note-12345678",
                            "xsec_token": "token/value",
                            "note_card": {"display_title": "AI 新进展"},
                        }
                    ]
                }
            },
        )
        fetcher = XiaohongshuFetcher(
            make_config(), api_client=api, sleep_func=lambda _: None
        )

        results, names, failed = fetcher.fetch_all()

        self.assertEqual(names, {"xhs-ai": "小红书·人工智能"})
        self.assertEqual(failed, [])
        self.assertIn("AI 新进展 [note-123]", results["xhs-ai"])
        self.assertIn(
            "xsec_token=token%2Fvalue",
            results["xhs-ai"]["AI 新进展 [note-123]"]["url"],
        )
        api.search_note.assert_called_once_with(
            query="人工智能",
            cookies_str="a1=a1-value; web_session=session-value",
            page=1,
            sort_type_choice=1,
            note_type=0,
            note_time=1,
            proxies=None,
        )

    def test_duplicate_note_ids_are_removed(self):
        items = [
            {"id": "same-note", "note_card": {"display_title": "标题一"}},
            {"id": "same-note", "note_card": {"display_title": "标题二"}},
        ]
        normalized = XiaohongshuFetcher._normalize_items(items)
        self.assertEqual(len(normalized), 1)

    def test_note_detail_is_read_and_normalized(self):
        api = Mock()
        api.get_note_info.return_value = (
            True,
            "成功",
            {
                "data": {
                    "items": [
                        {
                            "note_card": {
                                "title": "宠物客舱运输经历",
                                "desc": "旅客记录了宠物进入客舱时的材料和现场要求。",
                                "ip_location": "上海",
                                "user": {"nickname": "测试用户"},
                                "interact_info": {
                                    "liked_count": "12",
                                    "collected_count": "3",
                                    "comment_count": "4",
                                },
                            }
                        }
                    ]
                }
            },
        )
        fetcher = XiaohongshuFetcher(make_config(), api_client=api)

        detail = fetcher.fetch_detail(
            "https://www.xiaohongshu.com/explore/note1234"
            "?xsec_token=token&xsec_source=pc_search"
        )

        self.assertEqual(detail["title"], "宠物客舱运输经历")
        self.assertIn("现场要求", detail["content"])
        self.assertEqual(detail["author"], "测试用户")
        self.assertEqual(detail["liked_count"], "12")
        self.assertEqual(detail["detail_status"], "success")
        api.get_note_info.assert_called_once()

    def test_note_detail_rejects_non_xiaohongshu_url(self):
        fetcher = XiaohongshuFetcher(make_config(), api_client=Mock())

        with self.assertRaises(Exception):
            fetcher.fetch_detail("https://example.com/collect")

    def test_risk_response_opens_circuit_breaker(self):
        api = Mock()
        api.search_note.return_value = (False, "访问频繁，请完成验证", None)
        fetcher = XiaohongshuFetcher(make_config(), api_client=api)

        with self.assertRaises(XiaohongshuRiskControlError):
            fetcher.search("人工智能")

    def test_expired_session_stops_remaining_keywords(self):
        api = Mock()
        api.search_note.return_value = (False, "登录已过期", None)
        fetcher = XiaohongshuFetcher(
            make_config(
                KEYWORDS=[
                    {"id": "ai", "query": "人工智能"},
                    {"id": "robot", "query": "机器人"},
                ]
            ),
            api_client=api,
        )

        results, names, failed = fetcher.fetch_all()

        self.assertEqual(results, {})
        self.assertEqual(set(names), {"xhs-ai"})
        self.assertEqual(failed, ["xhs-ai", "xhs-robot"])
        self.assertEqual(api.search_note.call_count, 1)
        with self.assertRaises(XiaohongshuSessionError):
            fetcher.search("人工智能")

    def test_missing_cookie_marks_all_keywords_failed_without_request(self):
        api = Mock()
        config = make_config(
            COOKIE="",
            KEYWORDS=[
                {"id": "ai", "query": "人工智能"},
                {"id": "robot", "query": "机器人"},
            ],
        )
        fetcher = XiaohongshuFetcher(config, api_client=api)

        results, names, failed = fetcher.fetch_all()

        self.assertEqual(results, {})
        self.assertEqual(set(names), {"xhs-ai", "xhs-robot"})
        self.assertEqual(failed, ["xhs-ai", "xhs-robot"])
        api.search_note.assert_not_called()

    def test_pinned_spider_xhs_javascript_signing_runtime(self):
        _ensure_spider_xhs_import_path()
        from xhs_utils.xhs_util import generate_request_params, generate_x_rap_param

        api = "/api/sns/web/v1/search/notes"
        payload = {"keyword": "离线签名测试", "page": 1}
        with spider_xhs_runtime():
            headers, cookies, data = generate_request_params(
                "a1=19e81d396e30q1nrjbnryxl7zx5rp4gjwk309pbbo50000331494",
                api,
                payload,
                "POST",
            )
            rap = generate_x_rap_param(api, data)

        self.assertTrue(headers["x-s"].startswith("XYS_"))
        self.assertTrue(headers["x-s-common"])
        self.assertTrue(headers["x-t"].isdigit())
        self.assertEqual(cookies["a1"][:4], "19e8")
        self.assertIn("离线签名测试", data)
        self.assertTrue(rap)


if __name__ == "__main__":
    unittest.main()
