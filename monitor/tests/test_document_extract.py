from __future__ import annotations

import io
import unittest
import zipfile

from monitor.document_extract import extract_document


class DocumentExtractTests(unittest.TestCase):
    def test_plain_text_is_normalized(self) -> None:
        result = extract_document("  Pet fee 100 EUR\r\n\r\nMust book 48 hours early  ".encode(), "text/plain")
        self.assertTrue(result.complete)
        self.assertEqual(result.text, "Pet fee 100 EUR\nMust book 48 hours early")

    def test_docx_paragraphs_are_extracted_without_external_dependency(self) -> None:
        document = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
            '<w:body><w:p><w:r><w:t>Pets must remain in the carrier.</w:t></w:r></w:p>'
            '<w:p><w:r><w:t>Maximum weight is 8 kg.</w:t></w:r></w:p></w:body></w:document>'
        )
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, "w") as archive:
            archive.writestr("word/document.xml", document)
        result = extract_document(
            payload.getvalue(),
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
        self.assertTrue(result.complete)
        self.assertEqual(result.kind, "docx")
        self.assertIn("Maximum weight is 8 kg.", result.text)

    def test_unknown_binary_is_not_presented_as_policy_text(self) -> None:
        result = extract_document(b"\x00\x01\x02", "application/octet-stream")
        self.assertFalse(result.complete)
        self.assertEqual(result.reason, "unsupported_binary_document")


if __name__ == "__main__":
    unittest.main()
