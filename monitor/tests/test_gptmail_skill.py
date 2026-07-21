from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from monitor.skills.gptmail_otp.provider import (
    PublicKeyError,
    PublicKeyProvider,
    TIMEZONE,
    extract_public_key,
    refresh_cutoff,
)


class PublicKeyExtractionTests(unittest.TestCase):
    def test_extracts_key_only_from_public_key_button(self) -> None:
        page = """
        <code>PUBLIC_API_KEY</code>
        <button aria-label="显示并复制公共 API Key"><span><code>sk-abcDEF1234567890xyz0</code></span></button>
        """
        self.assertEqual(extract_public_key(page), "sk-abcDEF1234567890xyz0")

    def test_rejects_documentation_placeholder(self) -> None:
        with self.assertRaises(PublicKeyError):
            extract_public_key('<button aria-label="显示并复制公共 API Key"><code>PUBLIC_API_KEY</code></button>')

    def test_refresh_cutoff_uses_previous_day_before_eight(self) -> None:
        before = datetime(2026, 7, 21, 7, 59, tzinfo=TIMEZONE)
        after = datetime(2026, 7, 21, 8, 1, tzinfo=TIMEZONE)
        self.assertEqual(refresh_cutoff(before).day, 20)
        self.assertEqual(refresh_cutoff(after).day, 21)


class PublicKeyProviderTests(unittest.TestCase):
    def test_refresh_validates_and_atomically_persists(self) -> None:
        rendered = '<button aria-label="显示并复制公共 API Key"><code>sk-abcDEF1234567890xyz0</code></button>'
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "gptmail" / "public-key.json"
            provider = PublicKeyProvider(
                state_path=path,
                fetch_rendered=lambda: rendered,
                validate=lambda key: key.endswith("xyz0"),
            )
            key = provider.ensure_fresh(now=datetime(2026, 7, 21, 9, tzinfo=TIMEZONE))
            record = json.loads(path.read_text(encoding="utf-8"))
            status = provider.status()
        self.assertEqual(key, "sk-abcDEF1234567890xyz0")
        self.assertEqual(record["fingerprint"], status["fingerprint"])
        self.assertNotIn("key", status)

    def test_failed_refresh_keeps_last_known_good_key(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "public-key.json"
            path.write_text(json.dumps({
                "key": "sk-oldKey1234567890abcd",
                "fingerprint": "old",
                "refreshed_at": "2026-07-19T09:00:00+08:00",
            }), encoding="utf-8")
            provider = PublicKeyProvider(
                state_path=path,
                fetch_rendered=lambda: "<html>missing</html>",
                validate=lambda _: True,
            )
            key = provider.ensure_fresh(now=datetime(2026, 7, 21, 9, tzinfo=TIMEZONE))
        self.assertEqual(key, "sk-oldKey1234567890abcd")


if __name__ == "__main__":
    unittest.main()
