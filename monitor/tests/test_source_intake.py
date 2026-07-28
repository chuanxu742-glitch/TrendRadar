from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from monitor import official_monitor
from monitor.source_intake import (
    merge_ai_suggestions,
    normalize_submitted_url,
    parse_ai_source_response,
    prepare_source_candidates,
)


class SourceIntakeFormattingTests(unittest.TestCase):
    def test_mixed_single_batch_and_excel_text_is_extracted(self) -> None:
        prepared = prepare_source_candidates(
            """
            美国 CDC\thttps://www.cdc.gov/importation/dogs/
            新加坡 AVS，nparks.gov.sg/avs/pets
            [IATA](https://www.iata.org/en/programs/cargo/live-animals/)，example.gov/pets
            """
        )

        self.assertEqual(prepared["candidate_count"], 4)
        self.assertEqual(
            [item["url"] for item in prepared["items"]],
            [
                "https://www.cdc.gov/importation/dogs/",
                "https://nparks.gov.sg/avs/pets",
                "https://www.iata.org/en/programs/cargo/live-animals/",
                "https://example.gov/pets",
            ],
        )
        self.assertTrue(all(item["status"] == "valid" for item in prepared["items"]))

    def test_normalization_removes_fragment_and_detects_batch_duplicate(self) -> None:
        prepared = prepare_source_candidates(
            "example.gov/pets#rules\nhttps://example.gov/pets"
        )

        self.assertEqual(prepared["items"][0]["url"], "https://example.gov/pets")
        self.assertEqual(prepared["items"][1]["status"], "duplicate_in_batch")

    def test_private_interactive_and_static_urls_are_rejected(self) -> None:
        cases = {
            "http://127.0.0.1/admin": "interactive_or_search_page",
            "http://10.0.0.2/pets": "private_address_not_allowed",
            "https://portal.local/pets": "private_host_not_allowed",
            "https://user:secret@example.gov/pets": "credentials_not_allowed",
            "https://example.gov/search?q=pets": "interactive_or_search_page",
            "https://example.gov/logo.svg": "static_asset",
        }

        for value, reason in cases.items():
            with self.subTest(value=value):
                _, error = normalize_submitted_url(value)
                self.assertEqual(error["code"], reason)

    def test_ai_can_name_exact_urls_but_cannot_invent_urls(self) -> None:
        original = "CDC https://www.cdc.gov/importation/dogs/"
        deterministic = prepare_source_candidates(original)["items"]

        merged = merge_ai_suggestions(
            original,
            deterministic,
            [
                {
                    "url": "https://www.cdc.gov/importation/dogs/",
                    "name": "美国 CDC 犬只入境",
                },
                {
                    "url": "https://invented.example/pets",
                    "name": "编造来源",
                },
            ],
        )

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["name"], "美国 CDC 犬只入境")
        self.assertEqual(merged[0]["name_origin"], "ai")

    def test_fenced_ai_json_is_parsed(self) -> None:
        response = """```json
        {"items":[{"url":"https://example.gov/pets","name":"宠物政策"}]}
        ```"""
        self.assertEqual(
            parse_ai_source_response(response),
            [{"url": "https://example.gov/pets", "name": "宠物政策"}],
        )

    def test_batch_limit_is_enforced(self) -> None:
        prepared = prepare_source_candidates(
            "\n".join(
                f"https://example{index}.gov/pets"
                for index in range(201)
            )
        )

        self.assertEqual(len(prepared["items"]), 200)
        self.assertTrue(prepared["truncated"])


class ManualSourcePersistenceTests(unittest.TestCase):
    def tearDown(self) -> None:
        official_monitor.close_monitor_store()

    def test_import_adds_candidate_inventory_and_survives_authoritative_sync(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = root / "sources.yaml"
            inventory_path = root / "knowledge_sources.json"
            config_path.write_text("sources: []\n", encoding="utf-8")
            inventory_path.write_text(
                json.dumps({
                    "schema_version": 1,
                    "generation_status": "complete",
                    "generated_at": "2026-07-28T00:00:00+00:00",
                    "unique_sources": 0,
                    "sources": [],
                }),
                encoding="utf-8",
            )
            with (
                mock.patch.object(official_monitor, "STATE_DIR", root),
                mock.patch.object(official_monitor, "CONFIG_PATH", config_path),
                mock.patch.object(official_monitor, "INVENTORY_PATH", inventory_path),
            ):
                official_monitor.close_monitor_store()
                try:
                    result = official_monitor.import_manual_sources([
                        {
                            "url": "https://example.gov/pet-import",
                            "name": "示例宠物入境政策",
                        }
                    ])
                    source_id = result["items"][0]["source_id"]
                    endpoint = official_monitor.monitor_store().get_source(source_id)
                    site_record = official_monitor.load_site_url_records([
                        "https://example.gov/pet-import"
                    ])["https://example.gov/pet-import"]
                    sources, _, _ = official_monitor.load_sources()
                finally:
                    official_monitor.close_monitor_store()

            self.assertEqual(result["added"], 1)
            self.assertEqual(endpoint.role, "candidate")
            self.assertEqual(endpoint.lifecycle_state, "discovered")
            self.assertEqual(endpoint.metadata["source_origin"], "manual")
            self.assertEqual(site_record["relevance"], "high")
            self.assertEqual(site_record["fetch_status"], "scheduled")
            self.assertIn(source_id, {source["id"] for source in sources})

    def test_import_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with mock.patch.object(official_monitor, "STATE_DIR", root):
                official_monitor.close_monitor_store()
                try:
                    first = official_monitor.import_manual_sources([
                        {"url": "https://example.gov/pets"}
                    ])
                    second = official_monitor.import_manual_sources([
                        {"url": "https://example.gov/pets"}
                    ])
                finally:
                    official_monitor.close_monitor_store()

            self.assertEqual(first["added"], 1)
            self.assertEqual(second["added"], 0)
            self.assertEqual(second["items"][0]["status"], "existing_source")

    def test_paused_manual_source_is_not_retired_by_authoritative_sync(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = root / "sources.yaml"
            inventory_path = root / "knowledge_sources.json"
            config_path.write_text("sources: []\n", encoding="utf-8")
            inventory_path.write_text(
                json.dumps({
                    "schema_version": 1,
                    "generation_status": "complete",
                    "generated_at": "2026-07-28T00:00:00+00:00",
                    "unique_sources": 0,
                    "sources": [],
                }),
                encoding="utf-8",
            )
            with (
                mock.patch.object(official_monitor, "STATE_DIR", root),
                mock.patch.object(official_monitor, "CONFIG_PATH", config_path),
                mock.patch.object(official_monitor, "INVENTORY_PATH", inventory_path),
            ):
                official_monitor.close_monitor_store()
                try:
                    imported = official_monitor.import_manual_sources([
                        {"url": "https://example.gov/pets"}
                    ])
                    source_id = imported["items"][0]["source_id"]
                    official_monitor.monitor_store().transition_source(
                        source_id,
                        "quarantined",
                        reason="operator pause",
                    )
                    official_monitor.load_sources()
                    endpoint = official_monitor.monitor_store().get_source(source_id)
                finally:
                    official_monitor.close_monitor_store()

            self.assertEqual(endpoint.lifecycle_state, "quarantined")
            self.assertFalse(endpoint.enabled)

    def test_preview_falls_back_when_ai_is_not_configured(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with (
                mock.patch.object(official_monitor, "STATE_DIR", root),
                mock.patch.dict(
                    "os.environ",
                    {"AI_API_KEY": ""},
                    clear=False,
                ),
            ):
                official_monitor.close_monitor_store()
                try:
                    preview = official_monitor.preview_manual_source_input(
                        "https://example.gov/pets",
                        use_ai=True,
                    )
                finally:
                    official_monitor.close_monitor_store()

            self.assertFalse(preview["ai"]["available"])
            self.assertFalse(preview["ai"]["used"])
            self.assertEqual(preview["valid_count"], 1)

    def test_preview_uses_ai_names_but_discards_fabricated_urls(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with (
                mock.patch.object(official_monitor, "STATE_DIR", root),
                mock.patch.dict(os.environ, {"AI_API_KEY": "test-key"}),
                mock.patch.object(
                    official_monitor,
                    "_ai_source_intake_suggestions",
                    return_value=[
                        {
                            "url": "https://example.gov/pets",
                            "name": "示例政府宠物政策",
                        },
                        {
                            "url": "https://fabricated.example/pets",
                            "name": "编造来源",
                        },
                    ],
                ),
            ):
                official_monitor.close_monitor_store()
                try:
                    preview = official_monitor.preview_manual_source_input(
                        "https://example.gov/pets",
                        use_ai=True,
                    )
                finally:
                    official_monitor.close_monitor_store()

            self.assertTrue(preview["ai"]["used"])
            self.assertEqual(len(preview["items"]), 1)
            self.assertEqual(preview["items"][0]["name"], "示例政府宠物政策")


if __name__ == "__main__":
    unittest.main()
