from __future__ import annotations

import unittest

from monitor.production_validation import validate_digest_contract


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


if __name__ == "__main__":
    unittest.main()
