from __future__ import annotations

import unittest

from monitor.production_validation import (
    validate_digest_contract,
    validate_site_inventory_contract,
)


class ProductionValidationTests(unittest.TestCase):
    def test_accepts_empty_evidence_safe_digest(self) -> None:
        failures = validate_digest_contract(
            {
                "counts": {"changes": 0},
                "country_groups": [],
                "airline_groups": [],
                "other_groups": [],
            }
        )
        self.assertEqual(failures, [])

    def test_rejects_unsourced_fields_and_missing_evidence(self) -> None:
        failures = validate_digest_contract(
            {
                "counts": {"changes": 1},
                "country_groups": [
                    {
                        "changes": [
                            {
                                "change_id": "unsafe",
                                "effective_date": "2026-04-01",
                                "official_reason": "inferred",
                                "official_reason_status": "not_stated",
                            }
                        ]
                    }
                ],
                "airline_groups": [],
                "other_groups": [],
            }
        )
        self.assertTrue(any("no evidence bundle" in value for value in failures))
        self.assertTrue(any("unsourced effective date" in value for value in failures))
        self.assertTrue(any("inferred reason" in value for value in failures))

    def test_validates_site_inventory_coverage_contract(self) -> None:
        self.assertEqual(
            validate_site_inventory_contract({
                "summary": {
                    "stable_urls": 10,
                    "fetched_urls": 4,
                    "unread_urls": 6,
                    "fetch_coverage": 0.4,
                },
                "items": [{"url": "https://air.test/pets"}],
                "page_count": 1,
            }),
            [],
        )
        failures = validate_site_inventory_contract({
            "summary": {
                "stable_urls": -1,
                "fetched_urls": 0,
                "unread_urls": 0,
                "fetch_coverage": 2,
            },
            "items": [],
            "page_count": 1,
        })
        self.assertTrue(any("stable_urls" in value for value in failures))
        self.assertTrue(any("coverage" in value for value in failures))
        self.assertTrue(any("page count" in value for value in failures))


if __name__ == "__main__":
    unittest.main()
