import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from trendradar.ai.filter_pipeline import (
    is_official_change_report_candidate,
    is_reportable_rss_item,
)
from trendradar.report.html import (
    _render_policy_digest_html,
    build_rss_focus_items,
    deduplicate_rss_stats,
    load_monitor_knowledge_overview,
    render_html_content,
)


class IntelligenceReportTests(unittest.TestCase):
    def test_policy_digest_html_exposes_sourced_fields_and_exports(self) -> None:
        html = _render_policy_digest_html(
            {
                "counts": {"changes": 1},
                "country_groups": [
                    {
                        "label": "新加坡 (Singapore)",
                        "changes": [
                            {
                                "headline": "进口清关代理要求",
                                "old_rules": ["主人可直接办理"],
                                "new_rules": ["须由批准代理办理"],
                                "effective_date": "2026-04-01",
                                "official_reason": "为确保动物进口正确处理",
                                "official_reason_status": "sourced",
                            }
                        ],
                    }
                ],
                "airline_groups": [],
                "other_groups": [],
            }
        )

        self.assertIn("新加坡 (Singapore)", html)
        self.assertIn("须由批准代理办理", html)
        self.assertIn("format=markdown", html)

    def test_loads_monitor_knowledge_overview(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "inventory.json").write_text(json.dumps({
                "generated_at": "2026-07-22T08:00:00+08:00",
                "files_scanned": 5,
                "unique_sources": 4,
            }), encoding="utf-8")
            (root / "source_registry.json").write_text(json.dumps({
                "entities": {
                    "country:china": {"id": "country:china", "kind": "country", "name": "china", "current": {"url": "https://example.test/china"}},
                    "airline:test-air": {"id": "airline:test-air", "kind": "airline", "name": "test-air", "trusted_current_sources": [{"url": "https://example.test/air"}]},
                }
            }), encoding="utf-8")
            overview = load_monitor_knowledge_overview(root)

        self.assertEqual(overview["entity_count"], 2)
        self.assertEqual(overview["covered"], 2)
        self.assertEqual(overview["kind_counts"], {"country": 1, "airline": 1})

    def test_monitor_operations_are_not_reportable(self) -> None:
        self.assertFalse(is_reportable_rss_item({
            "source_id": "official-source-changes",
            "title": "[发现现行页面候选] 某航空公司",
        }))
        self.assertFalse(is_reportable_rss_item({
            "source_id": "official-source-changes",
            "title": "[数据源内容变化] 某航空公司",
        }))
        self.assertFalse(is_reportable_rss_item({
            "source_id": "official-source-changes",
            "title": "[政策变化] 某航空公司",
        }))
        self.assertTrue(is_official_change_report_candidate({
            "source_id": "official-source-changes",
            "title": "[政策变化] 某航空公司",
        }))
        self.assertFalse(is_official_change_report_candidate({
            "source_id": "official-source-changes",
            "title": "[发现现行页面候选] 某航空公司",
        }))

    def test_focus_items_prioritize_risk_and_deduplicate(self) -> None:
        stats = [
            {"titles": [
                {"title": "普通行业消息", "url": "https://example.test/general", "relevance_score": 0.9},
                {"title": "宠物托运事故导致死亡", "url": "https://example.test/risk", "relevance_score": 0.8},
            ]},
            {"titles": [
                {"title": "同一事故的重复标签", "url": "https://example.test/risk", "relevance_score": 0.8},
            ]},
        ]
        focus = build_rss_focus_items(stats)
        self.assertEqual(focus[0]["label"], "风险预警")
        self.assertEqual(len(focus), 2)

    def test_display_groups_do_not_repeat_the_same_article(self) -> None:
        duplicate_url = "https://example.test/repeated"
        stats = [
            {
                "word": "运输安全事故",
                "count": 1,
                "titles": [{"title": "同一条事故", "url": duplicate_url}],
            },
            {
                "word": "托运投诉维权",
                "count": 1,
                "titles": [{"title": "同一条事故", "url": duplicate_url}],
            },
        ]

        deduplicated = deduplicate_rss_stats(stats)

        self.assertEqual(len(deduplicated), 1)
        self.assertEqual(deduplicated[0]["word"], "运输安全事故")
        self.assertEqual(deduplicated[0]["count"], 1)

    def test_html_shows_focus_and_collapses_full_feed(self) -> None:
        with mock.patch("trendradar.report.html.load_monitor_knowledge_overview", return_value={
            "entity_count": 2,
            "files_scanned": 5,
            "unique_sources": 4,
            "covered": 2,
            "kind_counts": {"country": 1, "airline": 1},
            "featured": [{"kind": "country", "name": "china"}],
            "generated_at": "2026-07-22",
            "dashboard_url": "http://127.0.0.1:8090/",
        }):
            html = render_html_content(
                {"stats": [], "new_titles": [], "failed_ids": [], "total_new_count": 0},
                total_titles=1,
                rss_items=[{
                    "word": "托运事故",
                    "count": 1,
                    "titles": [{
                        "title": "宠物托运事故导致死亡",
                        "url": "https://example.test/risk",
                        "source_name": "行业新闻",
                        "time_display": "07-21 10:00",
                        "matched_keyword": "托运事故",
                    }],
                }],
            )
        self.assertIn("行业情报重点", html)
        self.assertIn("为什么重要", html)
        self.assertIn("建议行动", html)
        self.assertIn("查看全部 1 条情报", html)
        self.assertIn("监控知识库", html)
        self.assertIn("搜索全部现行政策", html)


if __name__ == "__main__":
    unittest.main()
