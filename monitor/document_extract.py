from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any
from xml.etree import ElementTree as ET


_TEXT_TYPES = (
    "application/json",
    "application/ld+json",
    "application/xml",
    "text/",
)
_DOCX_TYPES = (
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
)
_PDF_TYPES = ("application/pdf",)
_WORD_NS = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
_CORE_NS = "{http://schemas.openxmlformats.org/package/2006/metadata/core-properties}"
_DC_NS = "{http://purl.org/dc/elements/1.1/}"


@dataclass(frozen=True)
class ExtractedDocument:
    text: str
    title: str
    kind: str
    parser: str
    complete: bool
    reason: str = ""


def _content_kind(content_type: str, url: str) -> str:
    lowered = content_type.casefold().split(";", 1)[0].strip()
    suffix = PurePosixPath(url.split("?", 1)[0]).suffix.casefold()
    if any(value in lowered for value in _PDF_TYPES) or suffix == ".pdf":
        return "pdf"
    if any(value in lowered for value in _DOCX_TYPES) or suffix == ".docx":
        return "docx"
    if any(lowered.startswith(value) for value in _TEXT_TYPES) or suffix in {
        ".txt", ".csv", ".json", ".xml", ".md",
    }:
        return "text"
    return "binary"


def _normalize_text(value: str, max_chars: int) -> str:
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    lines = []
    for raw in value.splitlines():
        line = re.sub(r"[\t\u00a0 ]+", " ", raw).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)[:max_chars]


def _decode_text(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-16", "gb18030", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def _extract_pdf(content: bytes, max_chars: int) -> ExtractedDocument:
    try:
        from pypdf import PdfReader
    except ImportError:
        return ExtractedDocument("", "", "pdf", "unavailable", False, "pypdf_not_installed")
    try:
        reader = PdfReader(io.BytesIO(content), strict=False)
        parts: list[str] = []
        total = 0
        for page in reader.pages:
            text = page.extract_text() or ""
            if text:
                parts.append(text)
                total += len(text)
            if total >= max_chars:
                break
        normalized = _normalize_text("\n".join(parts), max_chars)
        metadata: Any = reader.metadata or {}
        title = str(getattr(metadata, "title", "") or metadata.get("/Title", "") or "").strip()
        return ExtractedDocument(
            normalized,
            title,
            "pdf",
            "pypdf",
            bool(normalized),
            "" if normalized else "pdf_has_no_extractable_text",
        )
    except Exception as exc:
        return ExtractedDocument("", "", "pdf", "pypdf", False, f"pdf_parse_error:{type(exc).__name__}")


def _docx_title(archive: zipfile.ZipFile) -> str:
    try:
        root = ET.fromstring(archive.read("docProps/core.xml"))
    except (KeyError, ET.ParseError):
        return ""
    title = root.find(f"{_DC_NS}title")
    return (title.text or "").strip() if title is not None else ""


def _extract_docx(content: bytes, max_chars: int) -> ExtractedDocument:
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            root = ET.fromstring(archive.read("word/document.xml"))
            paragraphs: list[str] = []
            for paragraph in root.iter(f"{_WORD_NS}p"):
                values = [node.text or "" for node in paragraph.iter(f"{_WORD_NS}t")]
                line = "".join(values).strip()
                if line:
                    paragraphs.append(line)
            text = _normalize_text("\n".join(paragraphs), max_chars)
            return ExtractedDocument(
                text,
                _docx_title(archive),
                "docx",
                "openxml",
                bool(text),
                "" if text else "docx_has_no_extractable_text",
            )
    except (KeyError, zipfile.BadZipFile, ET.ParseError) as exc:
        return ExtractedDocument("", "", "docx", "openxml", False, f"docx_parse_error:{type(exc).__name__}")


def extract_document(
    content: bytes,
    content_type: str,
    url: str = "",
    *,
    max_chars: int = 2 * 1024 * 1024,
) -> ExtractedDocument:
    """Extract stable visible text from non-HTML policy documents.

    The caller keeps the raw bytes as the immutable evidence. This function only
    produces normalized text suitable for topic validation and clause diffs.
    """

    kind = _content_kind(content_type, url)
    if kind == "pdf":
        return _extract_pdf(content, max_chars)
    if kind == "docx":
        return _extract_docx(content, max_chars)
    if kind == "text":
        text = _normalize_text(_decode_text(content), max_chars)
        return ExtractedDocument(text, "", kind, "text-decoder", bool(text), "" if text else "empty_text")
    return ExtractedDocument("", "", kind, "none", False, "unsupported_binary_document")
