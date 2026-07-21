from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit


URL_RE = re.compile(r"https?://[^\s<>\[\]{}\"']+", re.IGNORECASE)
IGNORED_EXTENSIONS = {
    ".avif", ".bmp", ".css", ".eot", ".gif", ".ico", ".jpeg", ".jpg",
    ".js", ".map", ".mp3", ".mp4", ".ogg", ".png", ".svg", ".ttf",
    ".webm", ".webp", ".woff", ".woff2",
}
IGNORED_HOSTS = {
    "fonts.googleapis.com", "fonts.gstatic.com", "ka-p.fontawesome.com",
    "www.google-analytics.com", "www.googletagmanager.com", "www.w3.org",
}
SOCIAL_HOST_SUFFIXES = (
    "facebook.com", "instagram.com", "linkedin.com", "pinterest.com",
    "tiktok.com", "twitter.com", "x.com", "youtube.com",
)
OFFICIAL_HINT_TERMS = ("official", "government", "authority", "regulator", "官网", "官方", "政府", "现行")
PRIMARY_PAGE_HINT_TERMS = (
    "official page", "official policy", "official site", "current official", "official current",
    "官网", "官方页面", "官方政策", "现行政策", "现行页面",
)
HISTORICAL_HINT_TERMS = ("old", "legacy", "archive", "deprecated", "404", "旧版", "旧路径", "历史")


def clean_url(raw: str) -> str | None:
    value = html.unescape(html.unescape(raw)).strip()
    value = value.rstrip(".,;:!?)]}，。；：！？\\")
    if "..." in value:
        return None
    try:
        parts = urlsplit(value)
    except ValueError:
        return None
    host = (parts.hostname or "").lower().strip(".")
    if parts.scheme.lower() not in {"http", "https"} or not host or "." not in host:
        return None
    if host in IGNORED_HOSTS or any(host == item or host.endswith("." + item) for item in SOCIAL_HOST_SUFFIXES):
        return None
    if Path(parts.path).suffix.lower() in IGNORED_EXTENSIONS:
        return None
    netloc = host
    if parts.port:
        netloc += f":{parts.port}"
    return urlunsplit((parts.scheme.lower(), netloc, parts.path or "/", parts.query, ""))


def extract_urls(text: str) -> set[str]:
    decoded = html.unescape(html.unescape(text))
    urls: set[str] = set()
    for match in URL_RE.findall(decoded):
        cleaned = clean_url(match)
        if cleaned:
            urls.add(cleaned)
    return urls


def context_hints(text: str, url: str) -> list[str]:
    decoded = html.unescape(html.unescape(text))
    position = decoded.lower().find(url.lower())
    if position < 0:
        return []
    line_start = decoded.rfind("\n", 0, position) + 1
    line_end = decoded.find("\n", position)
    if line_end < 0:
        line_end = len(decoded)
    line = decoded[line_start:line_end]
    local_position = position - line_start
    context = line[max(0, local_position - 100) : local_position + len(url) + 100].lower()
    hints: list[str] = []
    if any(term in context for term in OFFICIAL_HINT_TERMS):
        hints.append("official-context")
    if any(term in context for term in PRIMARY_PAGE_HINT_TERMS):
        hints.append("primary-page-context")
    if any(term in context for term in HISTORICAL_HINT_TERMS):
        hints.append("historical-context")
    return hints


def iter_documents(root: Path):
    groups = [
        ("country-policy", root / "procedures" / "countries", "*.md"),
        ("airline-policy", root / "airlines", "*.md"),
        ("country-fast-lookup", root / "fast_lookup", "*.json"),
        ("country-change-evidence", root / "changes", "*.json"),
    ]
    for category, folder, pattern in groups:
        if folder.exists():
            for path in sorted(folder.glob(pattern)):
                yield category, path
    for category, path in [
        ("country-index", root / "procedures" / "index.json"),
        ("airline-directory", root / "airlines" / "airlines_directory.json"),
        ("ipata-members", root / "members.json"),
    ]:
        if path.exists():
            yield category, path


def entity_id_for(category: str, relative: str) -> str | None:
    stem = Path(relative).stem.lower().replace("_", "-")
    if category == "country-policy":
        return f"country:{stem}"
    if category == "airline-policy":
        return f"airline:{stem}"
    if category == "airline-directory":
        return "directory:airlines"
    if category == "ipata-members":
        return "directory:ipata-members"
    return None


def build(root: Path) -> dict:
    records: dict[str, dict] = {}
    files_scanned = 0
    references = 0
    category_counts: Counter[str] = Counter()
    for category, path in iter_documents(root):
        files_scanned += 1
        text = path.read_text(encoding="utf-8", errors="ignore")
        relative = path.relative_to(root).as_posix()
        entity_id = entity_id_for(category, relative)
        for url in extract_urls(text):
            references += 1
            item = records.setdefault(
                url,
                {
                    "id": "kb-" + hashlib.sha256(url.encode("utf-8")).hexdigest()[:20],
                    "name": urlsplit(url).netloc,
                    "url": url,
                    "categories": [],
                    "knowledge_base_refs": [],
                    "entity_ids": [],
                    "evidence_hints": [],
                },
            )
            if category not in item["categories"]:
                item["categories"].append(category)
            if relative not in item["knowledge_base_refs"]:
                item["knowledge_base_refs"].append(relative)
            if entity_id and entity_id not in item["entity_ids"]:
                item["entity_ids"].append(entity_id)
            for hint in context_hints(text, url):
                if hint not in item["evidence_hints"]:
                    item["evidence_hints"].append(hint)

    sources = sorted(
        records.values(),
        key=lambda item: (
            0 if "country-policy" in item["categories"] else 1,
            0 if "airline-policy" in item["categories"] else 1,
            item["url"],
        ),
    )
    for item in sources:
        item["categories"].sort()
        item["knowledge_base_refs"].sort()
        item["entity_ids"].sort()
        item["evidence_hints"].sort()
        for category in item["categories"]:
            category_counts[category] += 1
    entities: dict[str, dict] = {}
    for source in sources:
        for entity_id in source["entity_ids"]:
            entity = entities.setdefault(
                entity_id,
                {
                    "id": entity_id,
                    "kind": entity_id.split(":", 1)[0],
                    "name": entity_id.split(":", 1)[1],
                    "candidate_source_ids": [],
                    "knowledge_base_refs": [],
                },
            )
            entity["candidate_source_ids"].append(source["id"])
            entity["knowledge_base_refs"].extend(source["knowledge_base_refs"])
    for entity in entities.values():
        entity["candidate_source_ids"] = sorted(set(entity["candidate_source_ids"]))
        entity["knowledge_base_refs"] = sorted(set(entity["knowledge_base_refs"]))
    return {
        "generated_at": datetime.now(timezone.utc).astimezone().isoformat(),
        "knowledge_base_root": str(root),
        "files_scanned": files_scanned,
        "url_references": references,
        "unique_sources": len(sources),
        "category_counts": dict(sorted(category_counts.items())),
        "entity_count": len(entities),
        "entities": sorted(entities.values(), key=lambda item: item["id"]),
        "sources": sources,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a monitor inventory from the IPATA knowledge base")
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if not args.root.exists():
        raise SystemExit(f"knowledge base not found: {args.root}")
    payload = build(args.root)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(args.output)
    print(json.dumps({key: payload[key] for key in ("files_scanned", "url_references", "unique_sources", "category_counts")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
