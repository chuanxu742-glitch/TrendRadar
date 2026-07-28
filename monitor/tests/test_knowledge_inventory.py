from __future__ import annotations

import unittest

from monitor.build_knowledge_inventory import clean_url


class KnowledgeInventoryUrlTests(unittest.TestCase):
    def test_search_and_mail_urls_are_not_monitor_sources(self) -> None:
        self.assertIsNone(clean_url("https://www.google.com/search?q=pet+policy"))
        self.assertIsNone(clean_url("https://outlook.office.com/mail/"))

    def test_malformed_url_with_chinese_note_is_rejected(self) -> None:
        self.assertIsNone(clean_url("https://example.test/pets中文备注"))

    def test_official_url_is_retained(self) -> None:
        self.assertEqual(clean_url("https://agency.gov/pets"), "https://agency.gov/pets")

    def test_real_unicode_path_is_retained(self) -> None:
        url = "https://news.example/春秋航空宠物进客舱服务/"
        self.assertEqual(clean_url(url), url)


if __name__ == "__main__":
    unittest.main()
