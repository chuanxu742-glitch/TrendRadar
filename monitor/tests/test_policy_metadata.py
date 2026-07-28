from __future__ import annotations

import unittest

from monitor.policy_metadata import extract_sourced_policy_metadata


class PolicyMetadataTests(unittest.TestCase):
    def test_extracts_only_explicitly_sourced_dates_and_reason(self) -> None:
        text = (
            "Published April 15, 2026. "
            "The new requirement is effective from 2026-06-30. "
            "This change is made to ensure animal imports are handled correctly."
        )
        result = extract_sourced_policy_metadata(
            text,
            added=[text],
            source_url="https://official.example/policy",
        )
        self.assertEqual(result["announcement_date"], "2026-04-15")
        self.assertEqual(result["effective_date"], "2026-06-30")
        self.assertEqual(result["official_reason_status"], "sourced")
        self.assertIn("official.example", result["effective_date_source"])
        self.assertIn("to ensure", result["official_reason"])

    def test_does_not_treat_unlabelled_date_as_effective_or_announced(self) -> None:
        result = extract_sourced_policy_metadata(
            "The certificate contains the date 2026-04-15.",
            added=["The certificate contains the date 2026-04-15."],
        )
        self.assertEqual(result["announcement_date"], "")
        self.assertEqual(result["effective_date"], "")
        self.assertEqual(result["official_reason_status"], "not_stated")

    def test_supports_chinese_source_statements(self) -> None:
        result = extract_sourced_policy_metadata(
            "本要求自2026年4月1日起实施。为确保动物进口正确处理，清关须由批准代理完成。",
            added=[
                "本要求自2026年4月1日起实施。",
                "为确保动物进口正确处理，清关须由批准代理完成。",
            ],
        )
        self.assertEqual(result["effective_date"], "2026-04-01")
        self.assertEqual(result["official_reason_status"], "sourced")
        self.assertIn("为确保", result["official_reason"])


if __name__ == "__main__":
    unittest.main()
