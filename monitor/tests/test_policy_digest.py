from __future__ import annotations

import unittest

from monitor.policy_digest import (
    build_policy_change_digest,
    policy_digest_period,
    render_policy_change_digest_markdown,
    render_policy_change_digest_text,
)


class PolicyChangeDigestTests(unittest.TestCase):
    def test_groups_verified_facts_and_preserves_date_meaning(self) -> None:
        changes = [
            {
                "change_id": "change-us",
                "revision_id": "revision-us",
                "revision": 2,
                "status": "confirmed",
                "evidence_bundle_id": "evidence-us",
                "headline": "高风险国家列表更新",
                "summary": "原规则：旧名单。新规则：新名单。",
                "impact": "影响高风险国家来源犬只入境安排。",
                "recommended_action": "按新名单复核来源国家。",
                "occurred_at": "2026-04-16T08:30:00+08:00",
                "metadata": {
                    "entity_kind": "country",
                    "entity_key": "united-states",
                    "change_kind": "入境检疫",
                    "importance": "high",
                    "announcement_date": "2026-04-15",
                    "announcement_date_source": "https://example.test/policy",
                    "official_reason": "根据全球狂犬病疫情监测数据调整",
                    "official_reason_status": "sourced",
                    "official_reason_source": "https://example.test/reason",
                    "url": "https://example.test/policy",
                },
            }
        ]
        digest = build_policy_change_digest(
            changes,
            evidence_facts={
                "evidence-us": {
                    "old_rule": ["约117个国家和地区"],
                    "new_rule": ["更新高风险国家名单"],
                }
            },
            generated_at="2026-04-16T09:00:00+08:00",
        )

        self.assertEqual(digest["counts"]["changes"], 1)
        group = digest["country_groups"][0]
        self.assertEqual(group["label"], "美国 (United States)")
        change = group["changes"][0]
        self.assertEqual(change["change_date"], "2026-04-15")
        self.assertEqual(change["date_kind"], "announcement")
        self.assertEqual(change["effective_date"], "")
        self.assertEqual(change["old_rules"], ["约117个国家和地区"])
        self.assertEqual(change["official_reason_status"], "sourced")

        text = render_policy_change_digest_text(digest)
        self.assertIn("一、美国 (United States)", text)
        self.assertIn("- 原内容: 约117个国家和地区", text)
        self.assertIn("- 生效时间: 官网未明确说明", text)
        self.assertIn("- 官方原因: 根据全球狂犬病疫情监测数据调整", text)
        self.assertIn("★重大变化", text)

    def test_does_not_publish_unverified_reason(self) -> None:
        digest = build_policy_change_digest(
            [
                {
                    "change_id": "change-sg",
                    "status": "confirmed",
                    "headline": "进口清关代理要求",
                    "occurred_at": "2026-04-01T00:00:00+08:00",
                    "metadata": {
                        "entity_kind": "country",
                        "entity_key": "singapore",
                        "effective_date": "2026-04-01",
                        "official_reason": "可能为了加强生物安全",
                        "official_reason_status": "inferred",
                    },
                }
            ]
        )

        change = digest["country_groups"][0]["changes"][0]
        self.assertEqual(change["official_reason"], "")
        self.assertEqual(change["official_reason_status"], "not_stated")
        self.assertEqual(change["effective_date"], "")
        self.assertEqual(change["date_kind"], "detected")
        self.assertIn("官方原因: 官网未说明", render_policy_change_digest_text(digest))

    def test_filters_by_kind_and_date_without_using_detection_as_effective_date(self) -> None:
        digest = build_policy_change_digest(
            [
                {
                    "change_id": "country-change",
                    "status": "confirmed",
                    "headline": "国家政策",
                    "occurred_at": "2026-04-10T00:00:00+08:00",
                    "metadata": {
                        "entity_kind": "country",
                        "entity_key": "new-zealand",
                        "effective_date": "2026-03-01",
                        "effective_date_source": "https://example.test/policy",
                    },
                },
                {
                    "change_id": "airline-change",
                    "status": "confirmed",
                    "headline": "航司政策",
                    "occurred_at": "2026-04-10T00:00:00+08:00",
                    "metadata": {
                        "entity_kind": "airline",
                        "entity_key": "example-air",
                    },
                },
            ],
            entity_kind="country",
            start_date="2026-03-01",
            end_date="2026-03-31",
        )

        self.assertEqual(digest["counts"]["changes"], 1)
        change = digest["country_groups"][0]["changes"][0]
        self.assertEqual(change["date_kind"], "effective")
        self.assertEqual(change["change_date"], "2026-03-01")
        self.assertEqual(digest["airline_groups"], [])

    def test_empty_digest_explains_evidence_requirement(self) -> None:
        text = render_policy_change_digest_text(build_policy_change_digest([]))

        self.assertIn("没有通过证据链校验", text)

    def test_calendar_periods_and_markdown_export(self) -> None:
        from datetime import date

        self.assertEqual(
            policy_digest_period("weekly", today=date(2026, 7, 28)),
            ("2026-07-27", "2026-07-28"),
        )
        self.assertEqual(
            policy_digest_period("monthly", today=date(2026, 7, 28)),
            ("2026-07-01", "2026-07-28"),
        )
        markdown = render_policy_change_digest_markdown(
            build_policy_change_digest(
                [],
                start_date="2026-07-28",
                end_date="2026-07-28",
            )
        )
        self.assertIn("# 政策变动汇总", markdown)
        self.assertIn("没有通过证据链校验", markdown)


if __name__ == "__main__":
    unittest.main()
