# coding=utf-8
"""
splitter.py 特征测试（characterization tests）

这些测试锁定 split_content_into_batches 当前的真实输出（含已知缺陷），
目的是在后续重构（比如把按渠道的 if/elif 分支收敛成配置表）时提供回归保护。
测试内容来自对当前代码实际运行结果的采样，不是凭空设计的"期望值"。
"""

import unittest
from datetime import datetime

from trendradar.notification.splitter import split_content_into_batches


FIXED_TIME = datetime(2026, 1, 1, 12, 0, 0)


def _fixed_time() -> datetime:
    return FIXED_TIME


def make_title(title, source, url, rank, is_new=False, mobile_url="", count=1, time_display="08:00"):
    return {
        "title": title,
        "source_name": source,
        "url": url,
        "mobile_url": mobile_url,
        "rank": rank,
        "ranks": [rank] if rank else [],
        "rank_threshold": 10,
        "is_new": is_new,
        "count": count,
        "time_display": time_display,
    }


def build_report_data():
    return {
        "stats": [
            {"word": "AI", "count": 12, "titles": [
                make_title("标题一", "来源A", "https://a.test/1", 1, is_new=True),
                make_title("标题二", "来源B", "https://b.test/2", 5),
            ]},
            {"word": "芯片", "count": 3, "titles": [
                make_title("标题三", "来源C", "", 8),
            ]},
        ],
        "new_titles": [
            {"source_name": "来源A", "titles": [
                make_title("标题一", "来源A", "https://a.test/1", 1, is_new=True),
            ]},
        ],
        "failed_ids": ["some-failed-platform"],
        "total_new_count": 1,
    }


class SplitterGoldenOutputTests(unittest.TestCase):
    """对 7 个渠道的默认（单批次）输出做精确比对，采样自当前代码的真实运行结果。"""

    def _batches(self, format_type):
        return split_content_into_batches(
            build_report_data(), format_type, mode="daily", get_time_func=_fixed_time,
        )

    def test_feishu_golden_output(self):
        batches = self._batches("feishu")
        self.assertEqual(len(batches), 1)
        expected = (
            "**总新闻：** 3 条（新增 1 + 0）\n"
            "**热榜：** 3/3\n"
            "\n"
            "**类型：** 热点分析报告\n"
            "**时间：** 2026-01-01 12:00:00\n"
            "**最热话题：** AI(12) | 芯片(3)\n"
            "\n---\n\n"
            "📊 **热点词汇统计** (共 3 条)\n\n"
            "🔥 <font color='grey'>[1/2]</font> **AI** : <font color='red'>12</font> 条\n\n"
            "  1. <font color='grey'>&#91;来源A&#93;</font> 🆕 [标题一](https://a.test/1)"
            " <font color='red'>**[1]**</font> <font color='grey'>- 08:00</font>\n"
            "\n"
            "  2. <font color='grey'>&#91;来源B&#93;</font> [标题二](https://b.test/2)"
            " <font color='red'>**[5]**</font> <font color='grey'>- 08:00</font>\n"
            "\n---\n\n"
            "📌 <font color='grey'>[2/2]</font> **芯片** : 3 条\n\n"
            "  1. <font color='grey'>&#91;来源C&#93;</font> 标题三"
            " <font color='red'>**[8]**</font> <font color='grey'>- 08:00</font>\n"
            "\n---\n\n"
            "🆕 **本次新增热点新闻** (共 1 条)\n\n"
            "**来源A** (1 条):\n\n"
            "  1. [标题一](https://a.test/1) <font color='red'>**[1]**</font>"
            " <font color='grey'>- 08:00</font>\n"
            "\n\n---\n\n"
            "⚠️ **数据获取失败的平台：**\n\n"
            "  • <font color='red'>some-failed-platform</font>\n"
            "\n\n"
            "<font color='grey'>更新时间：2026-01-01 12:00:00</font>"
        )
        self.assertEqual(batches[0], expected)

    def test_dingtalk_golden_output(self):
        batches = self._batches("dingtalk")
        self.assertEqual(len(batches), 1)
        self.assertIn("**总新闻：** 3 条（新增 1 + 0）", batches[0])
        self.assertIn("🔥 [1/2] **AI** : **12** 条", batches[0])
        self.assertIn("  1. [来源A] 🆕 [标题一](https://a.test/1) **[1]** - 08:00", batches[0])
        self.assertIn("⚠️ **数据获取失败的平台：**", batches[0])
        self.assertIn("  • **some-failed-platform**", batches[0])
        self.assertTrue(batches[0].endswith("> 更新时间：2026-01-01 12:00:00"))

    def test_wework_golden_output(self):
        batches = self._batches("wework")
        self.assertEqual(len(batches), 1)
        self.assertIn("🔥 [1/2] **AI** : **12** 条", batches[0])
        # wework/bark 用四个换行做分隔，而不是 --- 分割线
        self.assertIn("\n\n\n\n📌 [2/2] **芯片**", batches[0])
        self.assertIn("  • some-failed-platform", batches[0])
        self.assertTrue(batches[0].endswith("> 更新时间：2026-01-01 12:00:00"))

    def test_telegram_golden_output(self):
        batches = self._batches("telegram")
        self.assertEqual(len(batches), 1)
        # telegram 不用粗体标记
        self.assertIn("总新闻： 3 条（新增 1 + 0）", batches[0])
        self.assertIn("🔥 [1/2] AI : 12 条", batches[0])
        self.assertIn('  1. [来源A] 🆕 <a href="https://a.test/1">标题一</a> <b>[1]</b> <code>- 08:00</code>', batches[0])
        self.assertTrue(batches[0].endswith("更新时间：2026-01-01 12:00:00"))

    def test_ntfy_golden_output(self):
        batches = self._batches("ntfy")
        self.assertEqual(len(batches), 1)
        self.assertIn("  1. [来源A] 🆕 [标题一](https://a.test/1) **[1]** `- 08:00`", batches[0])

    def test_ntfy_new_titles_missing_rich_formatting_known_bug(self):
        """已知缺陷：process_new_titles_section 里 ntfy 没有专属分支，
        导致"本次新增热点新闻"区块的标题回退成纯文本，丢失链接/排名/时间。
        这条测试锁定现状，重构时如果连带修复了，需要显式更新/移除本测试并告知用户。
        """
        batches = self._batches("ntfy")
        self.assertIn("**来源A** (1 条):\n\n  1. 标题一\n", batches[0])

    def test_bark_new_titles_second_item_missing_rich_formatting_known_bug(self):
        """已知缺陷：process_new_titles_section 处理"剩余新增新闻"（第 2 条起）
        的分支只匹配 format_type == "wework"，没有覆盖 "bark"（第一条新闻的
        分支反而是用 in ("wework", "bark") 覆盖了两者）。导致 bark 推送里
        新增区块第 2 条起的标题回退成纯文本。同样只锁定现状，不在重构时顺带修。
        """
        report_data = build_report_data()
        report_data["new_titles"][0]["titles"].append(
            make_title("标题四", "来源A", "https://a.test/4", 2)
        )
        report_data["total_new_count"] = 2
        batches = split_content_into_batches(
            report_data, "bark", mode="daily", get_time_func=_fixed_time,
        )
        content = batches[0]
        # 第一条（index 1）走了 wework/bark 分支，保留了链接
        self.assertIn("[标题一](https://a.test/1)", content)
        # 第二条（index 2）掉进了纯文本回退
        self.assertIn("  2. 标题四\n", content)

    def test_bark_golden_output(self):
        batches = self._batches("bark")
        self.assertEqual(len(batches), 1)
        self.assertIn("🔥 [1/2] **AI** : **12** 条", batches[0])
        self.assertIn("  • some-failed-platform", batches[0])

    def test_slack_golden_output(self):
        batches = self._batches("slack")
        self.assertEqual(len(batches), 1)
        # slack 用单个 * 做粗体
        self.assertIn("*总新闻：* 3 条（新增 1 + 0）", batches[0])
        self.assertIn("🔥 [1/2] *AI* : *12* 条", batches[0])
        self.assertIn("  1. [来源A] 🆕 <https://a.test/1|标题一> *[1]* `- 08:00`", batches[0])
        self.assertTrue(batches[0].endswith("_更新时间：2026-01-01 12:00:00_"))


class SplitterOverflowTests(unittest.TestCase):
    """max_bytes 很小时应产生多个批次，且不丢失任何标题内容。"""

    def test_small_max_bytes_splits_into_multiple_batches_without_losing_titles(self):
        report_data = build_report_data()
        batches = split_content_into_batches(
            report_data, "feishu", mode="daily", get_time_func=_fixed_time, max_bytes=400,
        )
        self.assertGreater(len(batches), 1)
        joined = "".join(batches)
        for title in ("标题一", "标题二", "标题三"):
            self.assertIn(title, joined)
        # 每个批次都不应超过 max_bytes（允许 header/footer 计算误差，这里做宽松上界检查）
        for batch in batches:
            self.assertLessEqual(len(batch.encode("utf-8")), 400 + 200)

    def test_atomicity_word_header_and_first_title_stay_together(self):
        """词组标题第一次出现时，必须和该词组第一条新闻在同一批次里
        （之后如果该词组因超限续到下一批，会重复带上词组标题作为上下文，
        这属于既有设计，不在本测试的断言范围内）。
        """
        report_data = build_report_data()
        batches = split_content_into_batches(
            report_data, "feishu", mode="daily", get_time_func=_fixed_time, max_bytes=600,
        )
        first_batch_with_ai_header = next(b for b in batches if "**AI**" in b)
        self.assertIn("标题一", first_batch_with_ai_header)


class SplitterRssSectionTests(unittest.TestCase):
    def test_rss_stats_and_new_items_render_with_separators(self):
        report_data = {
            "stats": [], "new_titles": [], "failed_ids": [], "total_new_count": 0,
        }
        rss_items = [
            {"word": "国际", "count": 2, "titles": [
                make_title("RSS标题一", "Feed甲", "https://feed.test/1", 0),
                make_title("RSS标题二", "Feed甲", "", 0),
            ]},
        ]
        rss_new_items = [
            {"word": "国际", "count": 1, "titles": [
                make_title("RSS标题一", "Feed甲", "https://feed.test/1", 0),
            ]},
        ]
        batches = split_content_into_batches(
            report_data, "feishu", mode="daily", get_time_func=_fixed_time,
            rss_items=rss_items, rss_new_items=rss_new_items,
        )
        content = batches[0]
        self.assertIn("📰 **RSS 订阅统计** (共 2 条)", content)
        self.assertIn("RSS标题一", content)
        self.assertIn("RSS标题二", content)
        self.assertIn("🆕 **RSS 本次新增** (共 1 条)", content)


class SplitterAiSectionTests(unittest.TestCase):
    def test_long_ai_content_is_split_by_line_without_losing_lines(self):
        report_data = {
            "stats": [], "new_titles": [], "failed_ids": [], "total_new_count": 0,
        }
        ai_lines = [f"AI 分析要点 {i}：这是一段较长的分析内容用于撑大字节数" for i in range(40)]
        ai_content = "\n".join(ai_lines)
        batches = split_content_into_batches(
            report_data, "feishu", mode="daily", get_time_func=_fixed_time,
            ai_content=ai_content, max_bytes=600,
        )
        self.assertGreater(len(batches), 1)
        joined = "".join(batches)
        for line in ai_lines:
            self.assertIn(line, joined)


class SplitterStandaloneSectionTests(unittest.TestCase):
    def test_standalone_platforms_and_rss_feeds_render(self):
        report_data = {
            "stats": [], "new_titles": [], "failed_ids": [], "total_new_count": 0,
        }
        standalone_data = {
            "platforms": [{
                "id": "zhihu", "name": "知乎热榜",
                "items": [{"title": "独立标题一", "url": "https://zhihu.test/1", "rank": 1, "ranks": [1],
                           "first_time": "08:00", "last_time": "09:00", "count": 2}],
            }],
            "rss_feeds": [{
                "id": "hn", "name": "Hacker News",
                "items": [{"title": "独立RSS标题", "url": "https://hn.test/1",
                           "published_at": "2026-01-01T08:00:00", "author": "作者甲"}],
            }],
        }
        batches = split_content_into_batches(
            report_data, "feishu", mode="daily", get_time_func=_fixed_time,
            standalone_data=standalone_data,
        )
        content = batches[0]
        self.assertIn("📋 **独立展示区** (共 2 条)", content)
        self.assertIn("独立标题一", content)
        self.assertIn("独立RSS标题", content)


class SplitterEdgeCaseTests(unittest.TestCase):
    def test_policy_digest_is_delivered_even_without_news(self):
        report_data = {
            "stats": [],
            "new_titles": [],
            "failed_ids": [],
            "total_new_count": 0,
            "policy_change_digest": {
                "counts": {"changes": 1},
                "text": "【政策变动汇总】\n一、美国 (United States)\n- 新内容: 新规则",
            },
        }
        batches = split_content_into_batches(
            report_data,
            "feishu",
            mode="daily",
            get_time_func=_fixed_time,
        )

        self.assertIn("政策变动汇总", "\n".join(batches))
        self.assertNotIn("暂无匹配的热点词汇", "\n".join(batches))

    def test_empty_report_returns_placeholder_message(self):
        report_data = {"stats": [], "new_titles": [], "failed_ids": [], "total_new_count": 0}
        batches = split_content_into_batches(
            report_data, "feishu", mode="daily", get_time_func=_fixed_time,
        )
        self.assertEqual(len(batches), 1)
        self.assertIn("暂无匹配的热点词汇", batches[0])

    def test_incremental_mode_placeholder_message(self):
        report_data = {"stats": [], "new_titles": [], "failed_ids": [], "total_new_count": 0}
        batches = split_content_into_batches(
            report_data, "feishu", mode="incremental", get_time_func=_fixed_time,
        )
        self.assertIn("增量模式下暂无新增匹配的热点词汇", batches[0])

    def test_custom_region_order_changes_section_sequence(self):
        report_data = build_report_data()
        batches_default = split_content_into_batches(
            report_data, "feishu", mode="daily", get_time_func=_fixed_time,
        )
        batches_reordered = split_content_into_batches(
            report_data, "feishu", mode="daily", get_time_func=_fixed_time,
            region_order=["new_items", "hotlist", "rss", "standalone", "ai_analysis"],
        )
        pos_stats_default = batches_default[0].index("📊 **热点词汇统计**")
        pos_new_default = batches_default[0].index("🆕 **本次新增热点新闻**")
        pos_stats_reordered = batches_reordered[0].index("📊 **热点词汇统计**")
        pos_new_reordered = batches_reordered[0].index("🆕 **本次新增热点新闻**")

        self.assertLess(pos_stats_default, pos_new_default)
        self.assertLess(pos_new_reordered, pos_stats_reordered)


if __name__ == "__main__":
    unittest.main()
