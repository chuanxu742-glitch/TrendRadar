from __future__ import annotations

import json
import unittest
from pathlib import Path

from monitor.entity_names import country_name_zh, entity_names


class EntityNameTests(unittest.TestCase):
    def test_all_current_inventory_countries_have_chinese_labels(self) -> None:
        path = Path("output/official-monitor/inventory.json")
        if not path.exists():
            self.skipTest("local inventory is not available")
        inventory = json.loads(path.read_text(encoding="utf-8"))
        keys = [
            str(item["id"]).split(":", 1)[1]
            for item in inventory.get("entities", [])
            if item.get("kind") == "country"
        ]
        self.assertEqual([key for key in keys if not country_name_zh(key)], [])

    def test_subdivision_label_retains_country_and_english_region(self) -> None:
        zh, en, label = entity_names(
            "country",
            "united-states-california",
            {},
            {},
        )
        self.assertEqual(zh, "美国·California")
        self.assertEqual(en, "United States California")
        self.assertEqual(label, "美国·California (United States California)")


if __name__ == "__main__":
    unittest.main()
