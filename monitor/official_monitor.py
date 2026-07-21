from __future__ import annotations

import difflib
import gzip
import hashlib
import html
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from email.utils import format_datetime, parsedate_to_datetime
from html.parser import HTMLParser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, unquote, urlencode, urljoin, urlsplit, urlunsplit
from xml.etree import ElementTree as ET

import yaml

try:
    from .scrapling_fetch import ScraplingAdaptiveFetcher
    from .scraping_agent import AgentStateStore
except ImportError:
    from scrapling_fetch import ScraplingAdaptiveFetcher
    from scraping_agent import AgentStateStore


CONFIG_PATH = Path(os.getenv("MONITOR_CONFIG", "/app/monitor/sources.yaml"))
STATE_DIR = Path(os.getenv("MONITOR_STATE_DIR", "/app/state"))
INVENTORY_PATH = Path(os.getenv("MONITOR_INVENTORY", "/app/state/knowledge_sources.json"))
PORT = int(os.getenv("MONITOR_PORT", "8090"))
BATCH_SIZE = int(os.getenv("MONITOR_BATCH_SIZE", "75"))
SCAN_CONCURRENCY = min(max(int(os.getenv("MONITOR_SCAN_CONCURRENCY", "8")), 1), 16)
SCAN_DYNAMIC_CONCURRENCY = min(max(int(os.getenv("MONITOR_SCAN_DYNAMIC_CONCURRENCY", "2")), 1), 4)
SCAN_STEALTH_CONCURRENCY = min(max(int(os.getenv("MONITOR_SCAN_STEALTH_CONCURRENCY", "1")), 1), 2)
REQUEST_TIMEOUT = int(os.getenv("MONITOR_REQUEST_TIMEOUT", "15"))
EVENT_LIMIT = int(os.getenv("MONITOR_EVENT_LIMIT", "1000"))
MAX_RAW_SNAPSHOT_BYTES = int(os.getenv("MONITOR_MAX_RAW_SNAPSHOT_BYTES", str(20 * 1024 * 1024)))
MAX_TEXT_SNAPSHOT_CHARS = int(os.getenv("MONITOR_MAX_TEXT_SNAPSHOT_CHARS", str(2 * 1024 * 1024)))
VALIDATION_RULE_VERSION = 2
DYNAMIC_FETCH_LIMIT = int(os.getenv("MONITOR_DYNAMIC_FETCH_LIMIT", "5"))
STEALTH_FETCH_LIMIT = int(os.getenv("MONITOR_STEALTH_FETCH_LIMIT", "2"))
BROWSER_HARD_TIMEOUT = int(os.getenv("MONITOR_BROWSER_HARD_TIMEOUT", "75"))
CLOUDFLARE_SOLVER_ENABLED = os.getenv("MONITOR_CLOUDFLARE_SOLVER_ENABLED", "true").lower() == "true"
CLOUDFLARE_TIMEOUT = int(os.getenv("MONITOR_CLOUDFLARE_TIMEOUT", "60"))
AGENT_MAX_ATTEMPTS = max(int(os.getenv("MONITOR_AGENT_MAX_ATTEMPTS", "3")), 1)
AGENT_MAX_DURATION = max(int(os.getenv("MONITOR_AGENT_MAX_DURATION", "180")), 15)
CHECKPOINT_EVERY = max(int(os.getenv("MONITOR_CHECKPOINT_EVERY", "10")), 1)
FAILURE_RETRY_INTERVAL = max(int(os.getenv("MONITOR_FAILURE_RETRY_INTERVAL", "30")), 5)
CRITICAL_CHECK_INTERVAL = max(int(os.getenv("MONITOR_CRITICAL_CHECK_INTERVAL", "21600")), 900)
POLICY_CHECK_INTERVAL = max(int(os.getenv("MONITOR_POLICY_CHECK_INTERVAL", "86400")), 3600)
REFERENCE_CHECK_INTERVAL = max(int(os.getenv("MONITOR_REFERENCE_CHECK_INTERVAL", "604800")), 3600)
FAILURE_BACKOFF_BASE = max(int(os.getenv("MONITOR_FAILURE_BACKOFF_BASE", "21600")), 900)
FAILURE_BACKOFF_MAX = max(
    int(os.getenv("MONITOR_FAILURE_BACKOFF_MAX", "604800")), FAILURE_BACKOFF_BASE
)
STATE_JOURNAL_PATH = STATE_DIR / "state-journal.jsonl"
SCAN_PROGRESS_PATH = STATE_DIR / "scan-progress.json"
DASHBOARD_PATH = Path(__file__).with_name("dashboard.html")
POLICY_SUMMARIES_PATH = STATE_DIR / "policy-summaries.json"
SITE_DISCOVERY_STATE_PATH = STATE_DIR / "site-discovery.json"
SITE_DISCOVERY_SUMMARY_PATH = STATE_DIR / "site-discovery-summary.json"
AI_SUMMARY_BATCH_SIZE = max(int(os.getenv("MONITOR_AI_SUMMARY_BATCH_SIZE", "20")), 1)
AI_SUMMARY_CONCURRENCY = min(max(int(os.getenv("MONITOR_AI_SUMMARY_CONCURRENCY", "4")), 1), 8)
AI_SUMMARY_INTERVAL = max(int(os.getenv("MONITOR_AI_SUMMARY_INTERVAL", "5")), 5)
AI_SUMMARY_REQUEST_TIMEOUT = max(int(os.getenv("MONITOR_AI_SUMMARY_REQUEST_TIMEOUT", "90")), 15)
AI_SUMMARY_HARD_TIMEOUT = max(
    int(os.getenv("MONITOR_AI_SUMMARY_HARD_TIMEOUT", "120")), AI_SUMMARY_REQUEST_TIMEOUT + 15
)
AI_SUMMARY_RETRIES = max(int(os.getenv("MONITOR_AI_SUMMARY_RETRIES", "0")), 0)
SITE_DISCOVERY_ENABLED = os.getenv("MONITOR_SITE_DISCOVERY_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
SITE_DISCOVERY_SITES_PER_CYCLE = max(int(os.getenv("MONITOR_SITE_DISCOVERY_SITES_PER_CYCLE", "2")), 0)
SITE_DISCOVERY_CONCURRENCY = min(max(int(os.getenv("MONITOR_SITE_DISCOVERY_CONCURRENCY", "1")), 1), 8)
SITE_DISCOVERY_CYCLE_INTERVAL = max(int(os.getenv("MONITOR_SITE_DISCOVERY_CYCLE_INTERVAL", "30")), 5)
SITE_DISCOVERY_CIRCUIT_SECONDS = max(int(os.getenv("MONITOR_SITE_DISCOVERY_CIRCUIT_SECONDS", "21600")), 900)
SITE_DISCOVERY_INTERVAL_SECONDS = max(int(os.getenv("MONITOR_SITE_DISCOVERY_INTERVAL", "86400")), 3600)
SITE_DISCOVERY_MAX_SITEMAPS = max(int(os.getenv("MONITOR_SITE_DISCOVERY_MAX_SITEMAPS", "12")), 1)
SITE_DISCOVERY_MAX_URLS = max(int(os.getenv("MONITOR_SITE_DISCOVERY_MAX_URLS", "150")), 1)
SITE_DISCOVERY_MAX_PAGES = max(int(os.getenv("MONITOR_SITE_DISCOVERY_MAX_PAGES", "6")), 1)
SITE_DISCOVERY_MAX_DEPTH = max(int(os.getenv("MONITOR_SITE_DISCOVERY_MAX_DEPTH", "2")), 0)
KATANA_ENABLED = os.getenv("MONITOR_KATANA_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
KATANA_PATH = os.getenv("MONITOR_KATANA_PATH", "/usr/local/bin/katana")
KATANA_DEPTH = max(int(os.getenv("MONITOR_KATANA_DEPTH", "3")), 3)
KATANA_MAX_PAGES = max(int(os.getenv("MONITOR_KATANA_MAX_PAGES", "150")), 10)
KATANA_CRAWL_DURATION = max(int(os.getenv("MONITOR_KATANA_CRAWL_DURATION", "30")), 15)
KATANA_PROCESS_TIMEOUT = max(int(os.getenv("MONITOR_KATANA_PROCESS_TIMEOUT", "45")), KATANA_CRAWL_DURATION + 15)
STATE_IO_LOCK = threading.RLock()
TOPIC_TERMS = (
    "pet", "pets", "animal", "animals", "dog", "dogs", "cat", "cats", "rabies",
    "veterinary", "quarantine", "live animal", "pet travel", "pet transport", "cargo",
    "baggage", "animaux", "haustier", "mascota", "mascotas", "animali", "animais", "huisdier",
    "животные", "питомец", "ペット", "動物", "반려동물", "สัตว์เลี้ยง", "حيوانات", "evcil hayvan",
    "宠物", "动物", "犬", "猫", "狂犬", "检疫", "托运", "运输", "航空箱",
)
STRONG_TOPIC_TERMS = (
    "pet", "pets", "live animal", "dog", "dogs", "cat", "cats", "rabies", "veterinary",
    "quarantine", "animaux", "haustier", "mascota", "mascotas", "animali", "animais", "huisdier",
    "животные", "питомец", "ペット", "動物", "반려동물", "สัตว์เลี้ยง", "حيوانات", "evcil hayvan",
    "宠物", "动物", "犬", "猫", "狂犬", "检疫", "托运", "航空箱",
)
POLICY_FIELD_TERMS = (
    "fee", "fees", "cost", "price", "charge", "weight", "size", "dimension", "carrier",
    "crate", "kennel", "breed", "prohibited", "not allowed", "accepted", "required", "must",
    "certificate", "vaccination", "vaccine", "rabies", "quarantine", "microchip", "permit",
    "reservation", "booking", "deadline", "hours before", "days before", "temperature", "embargo",
    "费用", "价格", "收费", "重量", "尺寸", "航空箱", "笼", "品种", "禁止", "允许", "必须",
    "证明", "疫苗", "狂犬", "检疫", "芯片", "许可证", "预约", "时限", "提前", "温度", "禁运",
)
LINK_DISCOVERY_TERMS = (
    "pet", "pets", "pet-travel", "pet-transport", "traveling-with-pets", "dog", "dogs", "cat", "cats",
    "live-animal", "live animal", "animal-import", "animal-export", "宠物", "犬", "猫", "托运",
)
MULTILINGUAL_URL_TERMS = LINK_DISCOVERY_TERMS + (
    "animaux", "animal-de-compagnie", "voyager-avec-un-animal", "haustier", "haustiere", "tiertransport",
    "mascota", "mascotas", "viajar-con-mascotas", "animali", "animale", "viaggiare-con-animali",
    "animais", "viajar-com-animais", "huisdier", "huisdieren", "dier", "zwierzę", "zwierzeta",
    "живот", "животные", "питомец", "питомцы", "ペット", "動物", "반려동물", "애완동물",
    "สัตว์เลี้ยง", "حيوان", "حيوانات", "evcil-hayvan", "hayvan", "hewan", "peliharaan",
)
DISCOVERY_HUB_TERMS = (
    "travel-info", "travel-information", "before-you-fly", "prepare", "baggage", "special-assistance",
    "special-services", "help", "faq", "support", "cargo", "conditions-of-carriage", "passenger-info",
    "旅行信息", "行李", "特殊服务", "帮助", "常见问题", "貨物", "手荷物", "서비스", "수하물",
)
DISCOVERY_EXCLUDED_HOSTS = (
    "akamaihd.net", "amazonaws.com", "azureedge.net", "bit.ly", "cloudfront.net", "cloudinary.com",
    "contentful.com", "ctfassets.net", "docs.google.com", "drive.google.com", "fastly.net",
    "facebook.com", "instagram.com", "linkedin.com", "linktr.ee", "t.me", "telegram.me",
    "googleusercontent.com", "kc-usercontent.com", "twitter.com", "whatsapp.com", "wp.com",
    "x.com", "youtu.be", "youtube.com",
)
SOFT_ERROR_MARKERS = (
    "page not found", "404 not found", "access denied", "forbidden", "just a moment",
    "checking your browser", "enable javascript", "service unavailable", "页面不存在",
    "访问被拒绝", "系统维护", "找不到页面",
)
USER_AGENT = os.getenv(
    "MONITOR_USER_AGENT",
    "TrendRadar-KnowledgeSourceMonitor/2.0 (+local industry monitoring)",
)


class TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.skip_depth = 0
        self.title_parts: list[str] = []
        self.in_title = False
        self.canonical_url = ""
        self.metadata: dict[str, str] = {}
        self.links: list[tuple[str, str]] = []
        self.current_link = ""
        self.current_link_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = {key.lower(): value or "" for key, value in attrs}
        if tag == "title":
            self.in_title = True
        if tag == "a" and attributes.get("href"):
            self.current_link = attributes["href"]
            self.current_link_text = []
        if tag == "link" and "canonical" in attributes.get("rel", "").lower():
            self.canonical_url = attributes.get("href", "")
        if tag == "meta":
            key = (attributes.get("property") or attributes.get("name") or "").lower()
            if key and attributes.get("content"):
                self.metadata[key] = attributes["content"].strip()
        if tag == "time" and attributes.get("datetime"):
            self.metadata.setdefault("time:datetime", attributes["datetime"].strip())
        if tag in {"script", "style", "noscript", "svg", "template"}:
            self.skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self.in_title = False
        if tag == "a" and self.current_link:
            self.links.append((self.current_link, " ".join(self.current_link_text).strip()))
            self.current_link = ""
            self.current_link_text = []
        if tag in {"script", "style", "noscript", "svg", "template"} and self.skip_depth:
            self.skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self.skip_depth:
            text = re.sub(r"\s+", " ", data).strip()
            if text:
                self.parts.append(text)
                if self.in_title:
                    self.title_parts.append(text)
                if self.current_link:
                    self.current_link_text.append(text)


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def save_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def merge_events(existing: list[dict[str, Any]], additions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged = list(existing)
    seen = {str(item.get("guid", "")) for item in merged if item.get("guid")}
    for item in additions:
        guid = str(item.get("guid", ""))
        if guid and guid in seen:
            continue
        merged.append(item)
        if guid:
            seen.add(guid)
    return merged[-EVENT_LIMIT:]


def percentile(values: list[int | float], percent: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = min(max(int(round((len(ordered) - 1) * percent)), 0), len(ordered) - 1)
    return int(ordered[index])


def persist_shared_updates(
    discovered_additions: dict[str, dict[str, Any]],
    event_additions: list[dict[str, Any]],
    remove_urls: tuple[str, ...] = (),
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    discovered_path = STATE_DIR / "discovered_sources.json"
    events_path = STATE_DIR / "events.json"
    with STATE_IO_LOCK:
        current_discovered = load_json(discovered_path, {})
        current_events = load_json(events_path, [])
        if not isinstance(current_discovered, dict):
            current_discovered = {}
        if not isinstance(current_events, list):
            current_events = []
        for url in remove_urls:
            current_discovered.pop(url, None)
        current_discovered.update(discovered_additions)
        current_events = merge_events(current_events, event_additions)
        save_json(discovered_path, current_discovered)
        save_json(events_path, current_events)
        return current_discovered, current_events


def load_state_with_journal(path: Path) -> dict[str, Any]:
    state = load_json(path, {})
    if not isinstance(state, dict):
        state = {}
    if not STATE_JOURNAL_PATH.exists():
        return state
    try:
        with STATE_JOURNAL_PATH.open("r", encoding="utf-8") as journal:
            for line in journal:
                try:
                    item = json.loads(line)
                    source_id = item.get("source_id")
                    record = item.get("record")
                    if source_id and isinstance(record, dict):
                        state[source_id] = record
                except (AttributeError, json.JSONDecodeError):
                    continue
    except OSError:
        pass
    return state


def append_state_journal(records: dict[str, dict[str, Any]]) -> None:
    if not records:
        return
    with STATE_JOURNAL_PATH.open("a", encoding="utf-8") as journal:
        for source_id, record in records.items():
            journal.write(json.dumps({"source_id": source_id, "record": record}, ensure_ascii=False) + "\n")
        journal.flush()
        os.fsync(journal.fileno())


def clear_state_journal() -> None:
    try:
        STATE_JOURNAL_PATH.unlink()
    except FileNotFoundError:
        pass


def source_list_signature(sources: list[dict[str, Any]]) -> str:
    return hashlib.sha256("|".join(item["id"] for item in sources).encode()).hexdigest()[:20]


def source_check_interval(source: dict[str, Any]) -> tuple[int, str]:
    """Return the monitoring cadence and tier for one source."""
    hints = set(source.get("evidence_hints", []))
    categories = {
        item.strip().lower()
        for item in str(source.get("category", "")).split(",")
        if item.strip()
    }
    if hints.intersection({"official-context", "primary-page-context"}):
        return CRITICAL_CHECK_INTERVAL, "核心政策"
    if categories.intersection({"airline-policy", "country-policy"}):
        return POLICY_CHECK_INTERVAL, "政策来源"
    return REFERENCE_CHECK_INTERVAL, "参考来源"


def parse_checked_at(value: Any) -> float:
    try:
        parsed = datetime.fromisoformat(str(value))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except (TypeError, ValueError):
        return 0.0


def source_due_at(source: dict[str, Any], previous: dict[str, Any]) -> tuple[float, str]:
    checked_at = parse_checked_at(previous.get("checked_at"))
    interval, tier = source_check_interval(source)
    if not checked_at:
        return 0.0, tier
    if previous.get("status") == "error":
        failures = max(int(previous.get("consecutive_failures", 1) or 1), 1)
        category = str(previous.get("failure_category") or failure_category(
            str(previous.get("error", "")), previous.get("status_code")
        ))
        if category == "页面不存在":
            interval = 7 * 86400
        elif category in {"访问受限", "证书错误", "域名解析"}:
            interval = 24 * 3600
        elif category == "请求限流":
            interval = 6 * 3600
        elif category in {"请求超时", "服务端错误", "浏览器抓取"}:
            interval = min(3600 * (2 ** min(failures - 1, 6)), 24 * 3600)
        elif category == "内容校验":
            interval = 6 * 3600
        else:
            interval = min(FAILURE_BACKOFF_BASE * (2 ** min(failures - 1, 10)), FAILURE_BACKOFF_MAX)
        tier = "失败重试"
    return checked_at + interval, tier


def select_scan_batch(
    sources: list[dict[str, Any]], state: dict[str, Any], batch_size: int, now_timestamp: float | None = None,
) -> tuple[list[dict[str, Any]], int, Counter[str]]:
    """Select due sources, prioritising never-seen and high-value policy pages."""
    if batch_size <= 0 or not sources:
        return [], 0, Counter()
    if len(sources) <= batch_size:
        tiers = Counter(source_check_interval(source)[1] for source in sources)
        return list(sources), len(sources), tiers

    now_timestamp = time.time() if now_timestamp is None else now_timestamp
    due: list[tuple[int, int, int, float, dict[str, Any], str]] = []
    tier_rank = {"核心政策": 0, "政策来源": 1, "失败重试": 2, "参考来源": 3}
    for order, source in enumerate(sources):
        previous = state.get(source["id"], {})
        due_at, tier = source_due_at(source, previous)
        if due_at <= now_timestamp:
            never_seen = 0 if not previous.get("checked_at") else 1
            priority = int(source.get("policy_priority", discovery_priority(source.get("url", ""))) or 0)
            due.append((never_seen, tier_rank.get(tier, 9), -priority, due_at, source, tier))
    due.sort(key=lambda item: (item[0], item[1], item[2], item[3], item[4]["id"]))
    selected = due[:batch_size]
    return [item[4] for item in selected], len(due), Counter(item[5] for item in selected)


def failure_category(error: str, status_code: int | None = None) -> str:
    lowered = error.lower()
    if status_code == 404 or "http 404" in lowered or "404 client error" in lowered:
        return "页面不存在"
    if status_code == 403 or "http 403" in lowered or "403 client error" in lowered:
        return "访问受限"
    if status_code == 429 or "http 429" in lowered or "429 client error" in lowered:
        return "请求限流"
    if "certificate" in lowered or "ssl" in lowered or "tls" in lowered:
        return "证书错误"
    if (
        "name resolution" in lowered
        or "failed to resolve" in lowered
        or "could not resolve host" in lowered
        or "dns" in lowered
    ):
        return "域名解析"
    if "hard timeout" in lowered or "browser failed" in lowered:
        return "浏览器抓取"
    if "timeout" in lowered or "timed out" in lowered:
        return "请求超时"
    if "topic" in lowered or "soft error" in lowered or "content too small" in lowered:
        return "内容校验"
    if (status_code and status_code >= 500) or re.search(r"\b(?:http\s*)?5\d{2}\b", lowered):
        return "服务端错误"
    return "其他错误"


def adaptive_scan_concurrency(state: dict[str, Any]) -> tuple[int, str]:
    recent = sorted(
        (record for record in state.values() if record.get("checked_at")),
        key=lambda record: str(record.get("checked_at", "")),
        reverse=True,
    )[:100]
    if not recent:
        return SCAN_CONCURRENCY, "无历史样本"
    restricted = sum(
        str(record.get("failure_category") or failure_category(
            str(record.get("error", "")), record.get("status_code")
        )) in {"访问受限", "请求限流", "浏览器抓取"}
        for record in recent
    )
    ratio = restricted / len(recent)
    if ratio >= 0.4:
        return min(SCAN_CONCURRENCY, 4), f"近期受限率{ratio:.0%}"
    if ratio >= 0.2:
        return min(SCAN_CONCURRENCY, 6), f"近期受限率{ratio:.0%}"
    return SCAN_CONCURRENCY, f"近期受限率{ratio:.0%}"


def dashboard_url(url: str) -> str:
    """Keep container-local monitor links usable from the host dashboard."""
    parts = urlsplit(url)
    if parts.hostname == "official-monitor":
        path = parts.path or "/"
        return urlunsplit(("", "", path, parts.query, parts.fragment))
    return url


def fallback_business_summary(event_type: str) -> dict[str, str]:
    values = {
        "content": {
            "headline": "官方政策页面正文发生变化",
            "summary": "正在判断本次变化是否涉及实际政策条款。",
            "impact": "可能影响宠物入境材料、运输条件或办理流程。",
            "action": "建议业务人员核对原文证据后再更新客户方案。",
            "importance": "medium",
            "policy_change": None,
            "change_kind": "待判定",
        },
        "migration": {
            "headline": "现行政策页面已经切换",
            "summary": "官方政策入口发生迁移，系统已保留旧页面快照并开始监控新页面。",
            "impact": "旧链接可能不再代表当前有效政策。",
            "action": "后续业务引用应切换到新的官方页面。",
            "importance": "high",
            "policy_change": False,
            "change_kind": "页面变化",
        },
        "unavailable": {
            "headline": "官方政策来源暂时无法访问",
            "summary": "此前正常的官方来源当前访问失败，暂时无法确认页面最新状态。",
            "impact": "相关政策时效性存在待确认风险。",
            "action": "报价或出方案前应人工复核，并避免引用失效链接。",
            "importance": "high",
            "policy_change": False,
            "change_kind": "来源状态",
        },
        "recovered": {
            "headline": "官方政策来源已恢复访问",
            "summary": "此前不可访问的来源已经恢复，系统已重新纳入持续监控。",
            "impact": "可以重新核验该来源的最新政策内容。",
            "action": "建议确认恢复后的页面内容是否同时发生变化。",
            "importance": "low",
            "policy_change": False,
            "change_kind": "来源状态",
        },
    }
    return values.get(event_type, values["content"]).copy()


def parse_ai_summary_response(response: str) -> list[dict[str, Any]]:
    cleaned = response.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", cleaned, flags=re.IGNORECASE)
    start = cleaned.find("[")
    end = cleaned.rfind("]")
    if start < 0 or end <= start:
        raise ValueError("AI response does not contain a JSON array")
    value = json.loads(cleaned[start:end + 1])
    if not isinstance(value, list):
        raise ValueError("AI summary response must be a list")
    return [item for item in value if isinstance(item, dict)]


def summarize_policy_batch(pending: list[dict[str, Any]], api_key: str) -> dict[str, dict[str, Any]]:
    items = [
        {
            "id": event["guid"],
            "type": event["guid"].split(":", 1)[0],
            "title": event.get("title", ""),
            "summary": event.get("summary", ""),
            "url": event.get("url", ""),
            "policy_evidence": event.get("policy_evidence", {}),
        }
        for event in pending
    ]
    prompt = (
        "你是宠物跨境运输政策分析员。请把以下官方页面变化整理成业务员能快速理解的中文简报。"
        "严格输出 JSON 数组，不要输出 Markdown。每项必须包含 id、policy_change、change_kind、headline、summary、impact、action、importance。"
        "policy_change 必须为布尔值。只有宠物入境材料、检疫、健康证明、疫苗与狂犬病要求、费用、禁运限制、"
        "承运方式、航空箱规格、预约时限等实际规则发生变化时才为 true。页面布局、导航、语言菜单、更新时间、"
        "广告、无关新闻、随机标识、链接迁移或无法确认具体条款时必须为 false。"
        "change_kind 从入境检疫、健康证明、疫苗要求、承运规则、费用、禁运限制、航空箱、办理时限、其他政策中选择；非政策变化写非政策页面变化。"
        "headline 不超过24个汉字；summary 不超过80个汉字；impact 和 action 各不超过50个汉字；"
        "importance 只能是 high、medium、low。不得猜测原文没有提供的具体政策条款；信息不足时明确写需人工核对。"
        f"\n输入：{json.dumps(items, ensure_ascii=False)}"
    )
    payload = {
        "config": {
            "MODEL": os.getenv("AI_MODEL", "deepseek/deepseek-v4-flash"),
            "API_KEY": api_key,
            "API_BASE": os.getenv("AI_API_BASE", ""),
            "TEMPERATURE": 0.2,
            "MAX_TOKENS": 3000,
            "TIMEOUT": AI_SUMMARY_REQUEST_TIMEOUT,
            "NUM_RETRIES": AI_SUMMARY_RETRIES,
        },
        "messages": [
            {"role": "system", "content": "只输出符合要求的 JSON 数组。"},
            {"role": "user", "content": prompt},
        ],
    }
    worker = Path(__file__).with_name("policy_summary_batch_worker.py")
    with tempfile.TemporaryDirectory(prefix="policy-summary-") as temporary:
        folder = Path(temporary)
        input_path = folder / "input.json"
        output_path = folder / "output.txt"
        input_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        try:
            process = subprocess.run(
                [sys.executable, str(worker), "--input", str(input_path), "--output", str(output_path)],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=AI_SUMMARY_HARD_TIMEOUT,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(f"AI batch hard timeout after {AI_SUMMARY_HARD_TIMEOUT}s") from exc
        if process.returncode != 0 or not output_path.exists():
            detail = (process.stderr or process.stdout).strip()[-500:]
            raise RuntimeError(f"AI batch worker failed: {detail or process.returncode}")
        response = output_path.read_text(encoding="utf-8", errors="replace")
    valid_ids = {str(event.get("guid", "")) for event in pending}
    results: dict[str, dict[str, Any]] = {}
    for item in parse_ai_summary_response(response):
        event_id = str(item.get("id", ""))
        if not event_id or event_id not in valid_ids:
            continue
        fallback = fallback_business_summary(event_id.split(":", 1)[0])
        results[event_id] = {
            "headline": str(item.get("headline") or fallback["headline"])[:100],
            "summary": str(item.get("summary") or fallback["summary"])[:500],
            "impact": str(item.get("impact") or fallback["impact"])[:300],
            "action": str(item.get("action") or fallback["action"])[:300],
            "importance": item.get("importance") if item.get("importance") in {"high", "medium", "low"} else "medium",
            "policy_change": item.get("policy_change") is True,
            "change_kind": str(item.get("change_kind") or "其他政策")[:50],
            "generated_at": now_iso(),
        }
    return results


def generate_policy_summaries(events: list[dict[str, Any]]) -> int:
    api_key = os.getenv("AI_API_KEY", "")
    if not api_key or os.getenv("MONITOR_AI_SUMMARY_ENABLED", "true").lower() not in {"1", "true", "yes"}:
        return 0
    summaries = load_json(POLICY_SUMMARIES_PATH, {})
    if not isinstance(summaries, dict):
        summaries = {}
    changed = False
    for event in events:
        event_id = str(event.get("guid", ""))
        prefix = event_id.split(":", 1)[0]
        if prefix not in {"migration", "unavailable", "recovered"}:
            continue
        existing = summaries.get(event_id, {}) if isinstance(summaries.get(event_id), dict) else {}
        if existing.get("policy_change") is not False:
            existing.update(fallback_business_summary(prefix))
            existing["generated_at"] = now_iso()
            summaries[event_id] = existing
            changed = True
    capacity = AI_SUMMARY_BATCH_SIZE * AI_SUMMARY_CONCURRENCY
    pending = [
        event for event in reversed(events)
        if event.get("guid", "").split(":", 1)[0] == "content"
        and event.get("policy_evidence", {}).get("quality_gate") is True
        and (
            not isinstance(summaries.get(event.get("guid", "")), dict)
            or "policy_change" not in summaries.get(event.get("guid", ""), {})
        )
    ][:capacity]
    if not pending:
        if changed:
            save_json(POLICY_SUMMARIES_PATH, summaries)
        return 0

    batches = [
        pending[index:index + AI_SUMMARY_BATCH_SIZE]
        for index in range(0, len(pending), AI_SUMMARY_BATCH_SIZE)
    ]
    completed = 0
    with ThreadPoolExecutor(max_workers=min(AI_SUMMARY_CONCURRENCY, len(batches))) as executor:
        futures = {
            executor.submit(summarize_policy_batch, batch, api_key): batch for batch in batches
        }
        for future in as_completed(futures):
            try:
                batch_results = future.result()
                summaries.update(batch_results)
                completed += len(batch_results)
            except Exception as exc:
                print(f"[official-monitor] policy summary batch failed: {exc}", flush=True)
    save_json(POLICY_SUMMARIES_PATH, summaries)
    return completed


def policy_summary_worker() -> None:
    while True:
        processed = 0
        try:
            events = load_json(STATE_DIR / "events.json", [])
            if isinstance(events, list):
                processed = generate_policy_summaries(events)
        except Exception as exc:
            print(f"[official-monitor] policy summary failed: {exc}", flush=True)
            processed = 0
        capacity = AI_SUMMARY_BATCH_SIZE * AI_SUMMARY_CONCURRENCY
        time.sleep(1 if processed >= capacity else AI_SUMMARY_INTERVAL)


def event_label(event: dict[str, Any], state: dict[str, Any]) -> tuple[str, str]:
    guid = event.get("guid", "")
    prefix, _, remainder = guid.partition(":")
    labels = {
        "content": "政策页面内容变化",
        "migration": "现行政策页面切换",
        "candidate": "发现现行页面候选",
        "unavailable": "数据源不可用",
        "recovered": "数据源恢复",
        "inventory": "监控来源清单变化",
    }
    label = labels.get(prefix, "监控事件")
    subject = ""
    if prefix in {"content", "unavailable", "recovered"}:
        source_id = remainder.split(":", 1)[0]
        record = state.get(source_id, {})
        subject = record.get("name") or urlsplit(event.get("url", "")).hostname or source_id
    elif prefix == "migration":
        pieces = remainder.split(":")
        subject = ":".join(pieces[:2]) if len(pieces) > 1 else remainder
    else:
        subject = urlsplit(event.get("url", "")).hostname or ""
    return label, subject


def event_entity(event: dict[str, Any], state: dict[str, Any]) -> tuple[str, str]:
    guid_parts = str(event.get("guid", "")).split(":")
    prefix = guid_parts[0] if guid_parts else ""
    if prefix == "migration" and len(guid_parts) >= 3 and guid_parts[1] in {"country", "airline"}:
        return guid_parts[1], guid_parts[2]
    record: dict[str, Any] = {}
    if prefix in {"content", "unavailable", "recovered"} and len(guid_parts) >= 2:
        record = state.get(guid_parts[1], {})
    evidence = " ".join(
        [
            str(record.get("category", "")),
            " ".join(record.get("knowledge_base_refs", [])),
            str(event.get("summary", "")),
        ]
    ).lower()
    if "airline" in evidence or "airlines/" in evidence:
        return "airline", ""
    if any(marker in evidence for marker in ("country", "countries/", "fast_lookup/", "国家")):
        return "country", ""
    return "other", ""


def dashboard_payload() -> dict[str, Any]:
    state = load_state_with_journal(STATE_DIR / "state.json")
    status = load_json(STATE_DIR / "status.json", {})
    discovery_summary = load_json(SITE_DISCOVERY_SUMMARY_PATH, status.get("site_discovery", {}))
    if isinstance(discovery_summary, dict):
        discovery_summary = dict(discovery_summary)
        if str(discovery_summary.get("engine", "")).startswith("Katana"):
            discovery_summary["engine"] = "站内网址发现器"
    progress = load_json(SCAN_PROGRESS_PATH, {})
    registry = load_json(STATE_DIR / "source_registry.json", {})
    events = load_json(STATE_DIR / "events.json", [])
    policy_summaries = load_json(POLICY_SUMMARIES_PATH, {})
    agent_dir = STATE_DIR / "scraping-agent"
    agent_status = load_json(agent_dir / "status.json", {})
    agent_profiles = load_json(agent_dir / "site-profiles.json", {})
    agent_manual = load_json(agent_dir / "manual-queue.json", [])
    if not isinstance(agent_profiles, dict):
        agent_profiles = {}
    if not isinstance(agent_manual, list):
        agent_manual = []
    if not isinstance(policy_summaries, dict):
        policy_summaries = {}

    failures = []
    error_counts: Counter[str] = Counter()
    for source_id, record in state.items():
        if record.get("status") != "error":
            continue
        error = str(record.get("error", "未知错误"))
        category = failure_category(error, record.get("status_code"))
        error_counts[category] += 1
        failures.append(
            {
                "id": source_id,
                "name": record.get("name") or urlsplit(record.get("url", "")).hostname or source_id,
                "url": record.get("url", ""),
                "category": category,
                "error": error,
                "checked_at": record.get("checked_at", ""),
                "status_code": record.get("status_code"),
                "source_category": record.get("category", ""),
                "knowledge_base_refs": record.get("knowledge_base_refs", [])[:4],
                "fetch_mode": record.get("fetch_mode", "static"),
            }
        )
    failures.sort(key=lambda item: item["checked_at"], reverse=True)

    entity_values = list(registry.get("entities", {}).values())
    entity_summary = {}
    for kind, label in (("country", "国家/地区"), ("airline", "航司")):
        items = [item for item in entity_values if item.get("kind") == kind]
        entity_summary[kind] = {
            "label": label,
            "total": len(items),
            "trusted": sum(bool(item.get("trusted_current_sources")) for item in items),
            "current": sum(bool(item.get("current")) for item in items),
        }

    policy_event_types = {"content", "migration", "unavailable", "recovered"}
    event_counts: Counter[str] = Counter()
    entity_event_counts: Counter[str] = Counter()
    event_items = []
    now = datetime.now().astimezone()
    last_24h = 0
    last_7d = 0
    for event in events:
        prefix = event.get("guid", "").split(":", 1)[0]
        if prefix not in policy_event_types:
            continue
        event_counts[prefix] += 1
        entity_event_counts[event_entity(event, state)[0]] += 1
        try:
            detected = datetime.fromisoformat(str(event.get("detected_at", "")))
            age_seconds = (now - detected.astimezone()).total_seconds()
            if 0 <= age_seconds <= 86400:
                last_24h += 1
            if 0 <= age_seconds <= 7 * 86400:
                last_7d += 1
        except (TypeError, ValueError):
            pass

    for event in reversed(events):
        prefix = event.get("guid", "").split(":", 1)[0]
        if prefix not in policy_event_types:
            continue
        label, subject = event_label(event, state)
        source_id = ""
        record: dict[str, Any] = {}
        if prefix in {"content", "unavailable", "recovered"}:
            source_id = event.get("guid", "").split(":", 2)[1]
            record = state.get(source_id, {})
        entity_kind, entity_key = event_entity(event, state)
        business = fallback_business_summary(prefix)
        ai_business = policy_summaries.get(event.get("guid", ""), {})
        if isinstance(ai_business, dict):
            business.update({key: value for key, value in ai_business.items() if value})
        event_items.append(
            {
                "type": prefix,
                "label": label,
                "subject": subject,
                "url": dashboard_url(str(event.get("url", ""))),
                "detected_at": event.get("detected_at", ""),
                "summary": event.get("summary", ""),
                "source_id": source_id,
                "source_category": record.get("category", ""),
                "knowledge_base_refs": record.get("knowledge_base_refs", [])[:4],
                "business": business,
                "summary_status": (
                    "verified" if isinstance(business.get("policy_change"), bool) else "pending"
                ),
                "entity_kind": entity_kind,
                "entity_key": entity_key,
            }
        )
        if len(event_items) >= 250:
            break

    verified_policy_events = [
        item for item in event_items if item.get("business", {}).get("policy_change") is True
    ]
    verified_entity_counts = Counter(item.get("entity_kind", "other") for item in verified_policy_events)
    verified_kind_counts = Counter(item.get("business", {}).get("change_kind", "其他政策") for item in verified_policy_events)

    batch_size = int(progress.get("batch_size", 0) or 0)
    durable_index = int(progress.get("next_index", 0) or 0)
    completed_count = len(progress.get("completed_source_ids", [])) if progress else 0
    current_index = completed_count or int(progress.get("current_index", durable_index) or durable_index)
    active_workers = int(progress.get("active_workers", 0) or 0)
    status_cycle = status.get("cycle", {})
    return {
        "generated_at": now_iso(),
        "summary": {
            "total": len(state),
            "ok": sum(record.get("status") == "ok" for record in state.values()),
            "error": len(failures),
            "snapshots": sum(bool(record.get("snapshot_path")) for record in state.values()),
            "trusted_entities": registry.get("entities_with_trusted_sources", 0),
        },
        "changes": {
            "total": sum(event_counts.values()),
            "last_24h": last_24h,
            "last_7d": last_7d,
            "content": event_counts["content"],
            "migration": event_counts["migration"],
            "unavailable": event_counts["unavailable"],
            "recovered": event_counts["recovered"],
            "country": entity_event_counts["country"],
            "airline": entity_event_counts["airline"],
            "other": entity_event_counts["other"],
            "verified_total": len(verified_policy_events),
            "verified_country": verified_entity_counts["country"],
            "verified_airline": verified_entity_counts["airline"],
            "verified_other": verified_entity_counts["other"],
            "verified_kinds": [
                {"name": name, "count": count} for name, count in verified_kind_counts.most_common()
            ],
        },
        "progress": {
            "active": bool(progress),
            "mode": "全量扫描" if batch_size > 500 else "日常巡检",
            "batch_size": batch_size,
            "durable_index": durable_index,
            "current_index": current_index,
            "completed": current_index,
            "running": min(active_workers, max(batch_size - current_index, 0)) if progress else 0,
            "concurrency": int(progress.get("scan_concurrency", 0) or 0),
            "configured_concurrency": int(progress.get("configured_scan_concurrency", 0) or 0),
            "adaptive_reason": progress.get("adaptive_reason", ""),
            "duration_p50_ms": int(progress.get("duration_p50_ms", status.get("fetcher", {}).get("duration_p50_ms", 0)) or 0),
            "duration_p95_ms": int(progress.get("duration_p95_ms", status.get("fetcher", {}).get("duration_p95_ms", 0)) or 0),
            "throughput_per_minute": float(progress.get("throughput_per_minute", status.get("fetcher", {}).get("throughput_per_minute", 0)) or 0),
            "percent": round((current_index / batch_size * 100), 2) if batch_size else 0,
            "current_source_id": progress.get("current_source_id", ""),
            "current_url": progress.get("current_url", ""),
            "started_at": progress.get("started_at", ""),
            "last_progress_at": progress.get("last_progress_at", status.get("generated_at", "")),
            "last_checkpoint_at": progress.get("last_checkpoint_at", ""),
            "checked": status_cycle.get("checked", len(state)),
            "pending": status_cycle.get("pending", 0),
        },
        "discovery": discovery_summary or {
            "enabled": SITE_DISCOVERY_ENABLED,
            "eligible_sites": 0,
            "sites_due": 0,
            "sites_checked": 0,
            "new_policy_urls": 0,
        },
        "agent": {
            "attempts": int(agent_status.get("attempts", 0)),
            "successes": int(agent_status.get("status_counts", {}).get("success", 0)),
            "blocked": int(agent_status.get("status_counts", {}).get("blocked", 0)),
            "learned_profiles": sum(
                bool(item.get("candidate"))
                or str(item.get("active_strategy") or item.get("preferred_strategy") or "static") != "static"
                for item in agent_profiles.values()
            ),
            "candidate_profiles": sum(bool(item.get("candidate")) for item in agent_profiles.values()),
            "rolled_back_profiles": sum(item.get("status") == "rolled_back" for item in agent_profiles.values()),
            "manual_queue": len(agent_manual),
            "failure_counts": agent_status.get("failure_counts", {}),
            "recent_manual": [
                {
                    "site_key": item.get("site_key", ""),
                    "reason": item.get("reason", ""),
                    "updated_at": item.get("updated_at", ""),
                    "occurrences": int(item.get("occurrences", 1) or 1),
                }
                for item in agent_manual[-10:]
            ],
            "updated_at": agent_status.get("updated_at", ""),
        },
        "entities": entity_summary,
        "error_categories": [
            {"name": name, "count": count} for name, count in error_counts.most_common()
        ],
        "failures": failures,
        "events": event_items,
        "event_counts": dict(event_counts),
    }


def business_brief_payload() -> dict[str, Any]:
    payload = dashboard_payload()
    return {
        key: payload.get(key, {} if key == "agent" else [])
        for key in ("generated_at", "summary", "changes", "progress", "discovery", "agent", "events")
    }


def normalize_html(content: bytes, keywords: list[str] | None = None) -> tuple[str, str]:
    parser = TextExtractor()
    parser.feed(content.decode("utf-8", errors="replace"))
    parts = parser.parts
    title = " ".join(parser.title_parts)[:160] or (parts[0][:160] if parts else "")
    if keywords:
        lowered = [keyword.lower() for keyword in keywords]
        relevant = [part for part in parts if any(keyword in part.lower() for keyword in lowered)]
        if relevant:
            parts = relevant
    normalized = "\n".join(dict.fromkeys(parts))
    normalized = re.sub(r"[ \t]+", " ", normalized).strip()
    return normalized, title


def parse_html_facts(content: bytes, base_url: str) -> dict[str, Any]:
    parser = TextExtractor()
    parser.feed(content.decode("utf-8", errors="replace"))
    canonical = urljoin(base_url, html.unescape(parser.canonical_url)) if parser.canonical_url else ""
    dates = []
    for key in (
        "article:modified_time", "article:published_time", "date", "datepublished",
        "datemodified", "last-modified", "time:datetime",
    ):
        if parser.metadata.get(key):
            dates.append({"kind": key, "value": parser.metadata[key]})
    raw_text = content.decode("utf-8", errors="ignore")
    for key in ("dateModified", "datePublished"):
        match = re.search(rf'"{key}"\s*:\s*"([^"]+)"', raw_text, re.IGNORECASE)
        if match:
            dates.append({"kind": key.lower(), "value": match.group(1)})
    return {
        "canonical_url": canonical,
        "metadata": parser.metadata,
        "dates": dates[:10],
        "links": parser.links,
    }


def same_site(left: str, right: str) -> bool:
    left_host = (urlsplit(left).hostname or "").lower()
    right_host = (urlsplit(right).hostname or "").lower()
    return bool(left_host and right_host) and (
        left_host == right_host or left_host.endswith("." + right_host) or right_host.endswith("." + left_host)
    )


def acceptable_canonical(candidate: str, fetched_url: str) -> bool:
    if not same_site(candidate, fetched_url):
        return False
    candidate_path = urlsplit(candidate).path.rstrip("/")
    fetched_path = urlsplit(fetched_url).path.rstrip("/")
    return not (not candidate_path and fetched_path)


def normalize_candidate_url(value: str, base_url: str) -> str | None:
    try:
        absolute = urljoin(base_url, html.unescape(value)).strip()
        parts = urlsplit(absolute)
    except ValueError:
        return None
    if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
        return None
    scheme = parts.scheme.lower()
    host = parts.hostname.lower()
    port = parts.port
    netloc = host if not port or (scheme == "https" and port == 443) or (scheme == "http" and port == 80) else f"{host}:{port}"
    query = urlencode(
        [(key, val) for key, val in parse_qsl(parts.query, keep_blank_values=True) if key.lower() not in {"iid", "cid", "campaign"} and not key.lower().startswith("utm_")]
    )
    return urlunsplit((scheme, netloc, parts.path or "/", query, ""))


def policy_url_family_key(url: str) -> str:
    parts = urlsplit(url)
    segments = [segment for segment in parts.path.split("/") if segment]
    if segments and re.fullmatch(r"[a-zA-Z]{2}(?:[-_][a-zA-Z]{2})?", segments[0]):
        segments = segments[1:]
    query = urlencode([
        (key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if key.lower() not in {"lang", "language", "locale", "preflang"}
    ])
    path = "/" + "/".join(segments) if segments else "/"
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path.rstrip("/") or "/", query, ""))


def discovery_priority(url: str) -> int:
    decoded = unquote(f"{urlsplit(url).path} {urlsplit(url).query}").lower()
    strong = sum(contains_term(decoded, term) for term in STRONG_TOPIC_TERMS)
    supporting = sum(contains_term(decoded, term) for term in ("baggage", "cargo", "travel", "entry", "import", "export"))
    return min(strong * 20 + supporting * 5, 100)


def usable_candidate_url(url: str) -> bool:
    path = urlsplit(url).path.lower()
    blocked_extensions = (
        ".avif", ".bmp", ".css", ".eot", ".gif", ".ico", ".jpeg", ".jpg", ".js", ".json",
        ".map", ".mp3", ".mp4", ".ogg", ".png", ".svg", ".ttf", ".webm", ".webp", ".woff", ".woff2",
    )
    return not path.endswith(blocked_extensions) and not any(
        marker in path for marker in ("pagenotfound", "page-not-found", "not-found", "/404", "/error")
    )


def soft_error_reason(title: str, normalized: str) -> str:
    title_text = title.lower()
    beginning = normalized[:1200].lower()
    for marker in SOFT_ERROR_MARKERS:
        if marker in title_text or (len(normalized) < 2500 and marker in beginning):
            return f"soft error marker detected: {marker}"
    return ""


def topic_relevance(normalized: str) -> tuple[bool, list[str]]:
    lowered = normalized.lower()
    matched = [term for term in TOPIC_TERMS if contains_term(lowered, term)]
    return any(contains_term(lowered, term) for term in STRONG_TOPIC_TERMS), matched[:20]


def contains_term(text: str, term: str) -> bool:
    lowered = text.lower()
    if term.isascii():
        return re.search(rf"(?<![a-z]){re.escape(term)}(?![a-z])", lowered) is not None
    return term in lowered


def fingerprint(content: bytes, content_type: str, keywords: list[str] | None = None) -> tuple[str, str, int, str]:
    if "html" in content_type.lower() or "xml" in content_type.lower():
        normalized, title = normalize_html(content, keywords)
        payload = normalized.encode("utf-8")
        sample = normalized[:6000]
    else:
        payload = content
        title = ""
        sample = f"binary content; bytes={len(content)}"
    return hashlib.sha256(payload).hexdigest(), title, len(payload), sample


def describe_diff(before: str, after: str) -> str:
    if not before or not after or before.startswith("binary content") or after.startswith("binary content"):
        return "正文或文件内容哈希发生变化。"
    lines = list(
        difflib.unified_diff(
            before.splitlines(), after.splitlines(), fromfile="before", tofile="after", lineterm="", n=1
        )
    )
    selected = [line for line in lines if line.startswith(("+", "-")) and not line.startswith(("+++", "---"))]
    return "变更片段：" + " | ".join(selected[:8])[:1600] if selected else "页面结构或正文发生变化。"


def extract_policy_fields(normalized: str) -> str:
    lines = []
    for raw in normalized.splitlines():
        line = re.sub(r"\s+", " ", raw).strip()
        if len(line) < 8 or not any(contains_term(line, term) for term in POLICY_FIELD_TERMS):
            continue
        lines.append(line[:1000])
    return "\n".join(dict.fromkeys(lines))[:12000]


def policy_field_diff(before: str, after: str) -> dict[str, Any]:
    before_lines = set(filter(None, before.splitlines()))
    after_lines = set(filter(None, after.splitlines()))
    removed = sorted(before_lines - after_lines)[:12]
    added = sorted(after_lines - before_lines)[:12]
    changed = removed + added
    factual_pattern = re.compile(
        r"(?:\d|[$€£¥]|kg\b|lb\b|cm\b|inch|hour|day|must|shall|required|prohibit|not allowed|"
        r"禁止|不得|必须|需要|费用|重量|尺寸|小时|天)", re.IGNORECASE
    )
    fields = [term for term in POLICY_FIELD_TERMS if any(contains_term(line, term) for line in changed)]
    return {
        "quality_gate": bool(changed and any(factual_pattern.search(line) for line in changed)),
        "changed_fields": list(dict.fromkeys(fields))[:12],
        "removed": removed,
        "added": added,
    }


def add_event(events: list[dict[str, Any]], event: dict[str, Any]) -> None:
    if not any(existing.get("guid") == event["guid"] for existing in events):
        events.append(event)


def snapshot_extension(content_type: str) -> str:
    lowered = content_type.lower()
    if "html" in lowered:
        return "html"
    if "pdf" in lowered:
        return "pdf"
    if "json" in lowered:
        return "json"
    if "xml" in lowered:
        return "xml"
    return "bin"


def save_snapshot(
    source: dict[str, Any], record: dict[str, Any], content: bytes, normalized: str, previous: dict[str, Any]
) -> str:
    timestamp = datetime.fromisoformat(record["checked_at"]).strftime("%Y%m%dT%H%M%S%z")
    digest = record["sha256"][:12]
    relative = Path("snapshots") / source["id"] / f"{timestamp}-{digest}"
    folder = STATE_DIR / relative
    folder.mkdir(parents=True, exist_ok=True)
    raw_saved = len(content) <= MAX_RAW_SNAPSHOT_BYTES
    raw_name = ""
    if raw_saved:
        raw_name = f"raw.{snapshot_extension(record.get('content_type', ''))}.gz"
        with gzip.open(folder / raw_name, "wb", compresslevel=6) as output:
            output.write(content)
    text_truncated = len(normalized) > MAX_TEXT_SNAPSHOT_CHARS
    (folder / "content.md").write_text(normalized[:MAX_TEXT_SNAPSHOT_CHARS], encoding="utf-8")
    previous_text = previous.get("content_sample", "")
    if previous.get("snapshot_path"):
        previous_content = STATE_DIR / previous["snapshot_path"] / "content.md"
        if previous_content.exists():
            previous_text = previous_content.read_text(encoding="utf-8", errors="ignore")
    if previous_text:
        (folder / "diff.md").write_text(
            describe_diff(previous_text, normalized), encoding="utf-8"
        )
    metadata = {
        key: value for key, value in record.items() if key not in {"content_sample"}
    }
    metadata.update(
        {
            "source_id": source["id"], "snapshot_path": relative.as_posix(), "raw_file": raw_name,
            "raw_saved": raw_saved, "raw_bytes": len(content), "text_truncated": text_truncated,
        }
    )
    save_json(folder / "metadata.json", metadata)
    current_dir = STATE_DIR / "current"
    current_dir.mkdir(parents=True, exist_ok=True)
    save_json(
        current_dir / f"{source['id']}.json",
        {
            "source_id": source["id"], "url": source["url"], "canonical_url": record.get("canonical_url") or record.get("final_url"),
            "snapshot_path": relative.as_posix(), "sha256": record["sha256"], "validated_at": record["checked_at"],
            "validation": record.get("validation", {}), "policy_dates": record.get("policy_dates", []),
        },
    )
    return relative.as_posix()


def discovered_source(url: str, source: dict[str, Any], reason: str) -> dict[str, Any]:
    source_host = (urlsplit(source["url"]).hostname or "").lower()
    hints: list[str] = []
    if "primary-page-context" in source.get("evidence_hints", []):
        hints.extend(["official-context", "primary-page-context"])
    elif "official-context" in source.get("evidence_hints", []):
        hints.append("official-context")
    elif any(token in source_host for token in (".gov", ".gouv", ".gob", "government", "europa.eu")):
        hints.append("official-context")
    return {
        "id": "discovered-" + hashlib.sha256(url.encode("utf-8")).hexdigest()[:20],
        "name": urlsplit(url).netloc,
        "url": url,
        "categories": ["discovered-current-candidate"],
        "category": "discovered-current-candidate",
        "knowledge_base_refs": source_refs(source),
        "entity_ids": source.get("entity_ids", []),
        "evidence_hints": hints,
        "discovered_from": source["url"],
        "discovery_reason": reason,
        "discovered_at": now_iso(),
        "url_family_key": policy_url_family_key(url),
        "policy_priority": discovery_priority(url),
        "monitor_enabled": True,
    }


def discover_page_candidates(
    source: dict[str, Any], final_url: str, facts: dict[str, Any], normalized: str,
    discovered: dict[str, dict[str, Any]], events: list[dict[str, Any]],
) -> None:
    if not any(entity.startswith(("airline:", "country:")) for entity in source.get("entity_ids", [])):
        return
    origin_host = (urlsplit(source["url"]).hostname or "").lower()
    trusted_origin = "primary-page-context" in source.get("evidence_hints", []) or any(
        token in origin_host for token in (".gov", ".gouv", ".gob", "government", "europa.eu")
    )
    if not trusted_origin:
        return
    relevant, _ = topic_relevance(normalized)
    candidates: list[tuple[str, str]] = []
    original = normalize_candidate_url(source["url"], source["url"])
    redirected = normalize_candidate_url(final_url, source["url"])
    if redirected and original and redirected != original:
        candidates.append((redirected, "http-redirect"))
    canonical = normalize_candidate_url(facts.get("canonical_url", ""), final_url) if facts.get("canonical_url") else None
    if canonical and original and canonical != original and acceptable_canonical(canonical, final_url):
        candidates.append((canonical, "html-canonical"))
    if (
        relevant
        and "discovered-current-candidate" not in source.get("category", "")
        and any(hint in source.get("evidence_hints", []) for hint in ("official-context", "primary-page-context"))
    ):
        for href, anchor in facts.get("links", []):
            candidate = normalize_candidate_url(href, final_url)
            if not candidate or candidate == original or not same_site(candidate, final_url):
                continue
            signal = f"{urlsplit(candidate).path} {anchor}".lower()
            if any(contains_term(signal, term) for term in LINK_DISCOVERY_TERMS):
                candidates.append((candidate, "same-site-relevant-link"))
            if len(candidates) >= 12:
                break
    for candidate, reason in dict.fromkeys(candidates):
        register_discovered_candidate(candidate, source, reason, discovered, events)


def discovery_signal(url: str, anchor: str = "") -> bool:
    """Return whether a URL or anchor looks like a pet transport policy page."""
    decoded = unquote(f"{urlsplit(url).path} {urlsplit(url).query} {anchor}").lower()
    return any(contains_term(decoded, term) for term in MULTILINGUAL_URL_TERMS)


def discovery_hub_signal(url: str, anchor: str = "") -> bool:
    decoded = unquote(f"{urlsplit(url).path} {anchor}").lower()
    return any(contains_term(decoded, term) for term in DISCOVERY_HUB_TERMS)


def parse_sitemap(content: bytes, base_url: str) -> tuple[list[str], list[str]]:
    """Return (page URLs, nested sitemap URLs) from a sitemap document."""
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return [], []
    root_kind = root.tag.rsplit("}", 1)[-1].lower()
    locations = []
    for element in root.iter():
        if element.tag.rsplit("}", 1)[-1].lower() != "loc" or not element.text:
            continue
        candidate = normalize_candidate_url(element.text.strip(), base_url)
        if candidate:
            locations.append(candidate)
    unique = list(dict.fromkeys(locations))
    return ([], unique) if root_kind == "sitemapindex" else (unique, [])


def register_discovered_candidate(
    url: str,
    source: dict[str, Any],
    reason: str,
    discovered: dict[str, dict[str, Any]],
    events: list[dict[str, Any]],
) -> bool:
    normalized = normalize_candidate_url(url, source["url"])
    if not normalized or normalized in discovered or not usable_candidate_url(normalized):
        return False
    family_key = policy_url_family_key(normalized)
    source_entities = set(source.get("entity_ids", []))
    for existing_url, existing in discovered.items():
        existing_family = existing.get("url_family_key") or policy_url_family_key(existing_url)
        if existing_family != family_key:
            continue
        if source_entities and not source_entities.intersection(existing.get("entity_ids", [])):
            continue
        alias = discovered_source(normalized, source, reason)
        alias.update({"monitor_enabled": False, "merged_into": existing_url})
        discovered[normalized] = alias
        return False
    item = discovered_source(normalized, source, reason)
    discovered[normalized] = item
    add_event(
        events,
        {
            "guid": f"candidate:{item['id']}",
            "title": f"[发现现行页面候选] {item['name']}",
            "url": normalized,
            "detected_at": item["discovered_at"],
            "summary": f"发现方式：{reason}。原来源：{source['url']}。候选页面将在后续批次验证，不会直接覆盖当前版本。",
        },
    )
    return True


def trusted_discovery_source(source: dict[str, Any]) -> bool:
    entities = source.get("entity_ids", [])
    if not any(entity.startswith(("airline:", "country:")) for entity in entities):
        return False
    hints = source.get("evidence_hints", [])
    host = (urlsplit(source.get("url", "")).hostname or "").lower()
    if any(host == blocked or host.endswith("." + blocked) for blocked in DISCOVERY_EXCLUDED_HOSTS):
        return False
    return (
        any(hint in hints for hint in ("official-context", "primary-page-context"))
        or any(token in host for token in (".gov", ".gouv", ".gob", "government", "europa.eu"))
    )


def fetch_discovery_document(fetcher: ScraplingAdaptiveFetcher, url: str) -> tuple[Any | None, str]:
    try:
        response = fetcher.fetch_static(url, timeout=REQUEST_TIMEOUT)
    except Exception as exc:
        return None, failure_category(str(exc))
    if response.status_code in {401, 403, 429}:
        return None, "访问受限" if response.status_code != 429 else "请求限流"
    if response.status_code == 404:
        return None, "页面不存在"
    if response.status_code >= 400:
        return None, failure_category(f"HTTP {response.status_code}", response.status_code)
    return response, ""


def discover_site_fallback(
    fetcher: ScraplingAdaptiveFetcher,
    source: dict[str, Any],
    discovered: dict[str, dict[str, Any]],
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    """Discover policy URLs across one trusted site with strict crawl budgets."""
    started = time.monotonic()
    parts = urlsplit(source["url"])
    origin = urlunsplit((parts.scheme or "https", parts.netloc, "/", "", ""))
    found = 0
    sitemap_urls_checked = 0
    pages_checked = 0
    sitemap_queue = [urljoin(origin, "sitemap.xml"), urljoin(origin, "sitemap_index.xml")]
    error_categories: Counter[str] = Counter()

    def finish(blocked: bool = False) -> dict[str, Any]:
        return {
            "origin": origin,
            "checked_at": now_iso(),
            "engine": "内置发现器",
            "sitemaps_checked": sitemap_urls_checked,
            "pages_checked": pages_checked,
            "new_policy_urls": found,
            "errors": sum(error_categories.values()),
            "error_categories": dict(error_categories),
            "blocked": blocked,
            "circuit_open_until": time.time() + SITE_DISCOVERY_CIRCUIT_SECONDS if blocked else 0,
            "duration_ms": round((time.monotonic() - started) * 1000),
        }

    robots, robots_error = fetch_discovery_document(fetcher, urljoin(origin, "robots.txt"))
    if robots is not None:
        robots_text = robots.content.decode("utf-8", errors="ignore")
        for match in re.finditer(r"(?im)^\s*sitemap\s*:\s*(\S+)", robots_text):
            candidate = normalize_candidate_url(match.group(1), origin)
            if candidate and same_site(candidate, origin):
                sitemap_queue.insert(0, candidate)
    elif robots_error not in {"", "页面不存在"}:
        error_categories[robots_error] += 1
        if robots_error in {"访问受限", "请求限流"}:
            return finish(blocked=True)

    seen_sitemaps: set[str] = set()
    for sitemap_url in sitemap_queue:
        if sitemap_url in seen_sitemaps or sitemap_urls_checked >= SITE_DISCOVERY_MAX_SITEMAPS:
            continue
        seen_sitemaps.add(sitemap_url)
        response, sitemap_error = fetch_discovery_document(fetcher, sitemap_url)
        sitemap_urls_checked += 1
        if response is None:
            if sitemap_error not in {"", "页面不存在"}:
                error_categories[sitemap_error] += 1
            if sitemap_error in {"访问受限", "请求限流"}:
                return finish(blocked=True)
            continue
        page_urls, nested_sitemaps = parse_sitemap(response.content, response.url)
        for nested in nested_sitemaps:
            if same_site(nested, origin) and nested not in seen_sitemaps:
                sitemap_queue.append(nested)
        for candidate in page_urls:
            if found >= SITE_DISCOVERY_MAX_URLS:
                break
            if same_site(candidate, origin) and discovery_signal(candidate):
                found += int(register_discovered_candidate(
                    candidate, source, "site-sitemap-policy-url", discovered, events
                ))

    crawl_queue: list[tuple[str, int]] = [(origin, 0)]
    visited_pages: set[str] = set()
    while crawl_queue and pages_checked < SITE_DISCOVERY_MAX_PAGES and found < SITE_DISCOVERY_MAX_URLS:
        current_url, depth = crawl_queue.pop(0)
        if current_url in visited_pages:
            continue
        visited_pages.add(current_url)
        response, page_error = fetch_discovery_document(fetcher, current_url)
        pages_checked += 1
        if response is None:
            if page_error:
                error_categories[page_error] += 1
            if page_error in {"访问受限", "请求限流"}:
                return finish(blocked=True)
            continue
        content_type = response.headers.get("Content-Type", "").lower()
        if "html" not in content_type and b"<html" not in response.content[:2000].lower():
            continue
        facts = parse_html_facts(response.content, response.url)
        for href, anchor in facts.get("links", []):
            candidate = normalize_candidate_url(href, response.url)
            if not candidate or not same_site(candidate, origin) or not usable_candidate_url(candidate):
                continue
            if discovery_signal(candidate, anchor):
                found += int(register_discovered_candidate(
                    candidate, source, "site-crawl-policy-link", discovered, events
                ))
            elif depth < SITE_DISCOVERY_MAX_DEPTH and discovery_hub_signal(candidate, anchor):
                crawl_queue.append((candidate, depth + 1))

    return finish()


def katana_discover_urls(origin: str) -> dict[str, Any]:
    started = time.monotonic()
    timed_out = False
    with tempfile.TemporaryDirectory(prefix="katana-discovery-") as temporary:
        output_path = Path(temporary) / "urls.txt"
        command = [
            KATANA_PATH,
            "-u", origin,
            "-silent",
            "-nc",
            "-d", str(KATANA_DEPTH),
            "-s", "breadth-first",
            "-jc",
            "-kf", "all",
            "-iqp",
            "-fs", "rdn",
            "-mdp", str(KATANA_MAX_PAGES),
            "-ct", f"{KATANA_CRAWL_DURATION}s",
            "-timeout", str(REQUEST_TIMEOUT),
            "-retry", "1",
            "-H", f"User-Agent: {USER_AGENT}",
            "-ot", "{{url}}",
            "-o", str(output_path),
        ]
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=KATANA_PROCESS_TIMEOUT,
                check=False,
            )
        except FileNotFoundError:
            return {"ok": False, "urls": [], "error": f"Katana executable not found: {KATANA_PATH}"}
        except subprocess.TimeoutExpired as exc:
            timed_out = True
            stdout = exc.stdout or ""
            stderr = exc.stderr or ""
            if isinstance(stdout, bytes):
                stdout = stdout.decode("utf-8", errors="replace")
            if isinstance(stderr, bytes):
                stderr = stderr.decode("utf-8", errors="replace")
            result = subprocess.CompletedProcess(command, 124, stdout=stdout, stderr=stderr)

        output = result.stdout
        if output_path.exists():
            output += "\n" + output_path.read_text(encoding="utf-8", errors="replace")
        urls = []
        for line in output.splitlines():
            candidate = normalize_candidate_url(line.strip(), origin)
            if candidate and same_site(candidate, origin) and usable_candidate_url(candidate):
                urls.append(candidate)
    urls = list(dict.fromkeys(urls))[:KATANA_MAX_PAGES]
    return {
        "ok": bool(urls),
        "urls": urls,
        "returncode": result.returncode,
        "timed_out": timed_out,
        "duration_ms": round((time.monotonic() - started) * 1000),
        "error": (
            f"Katana reached {KATANA_PROCESS_TIMEOUT}s hard timeout; partial URLs retained"
            if timed_out else result.stderr.strip()[-500:] if result.returncode else ""
        ),
    }


def discover_site(
    fetcher: ScraplingAdaptiveFetcher,
    source: dict[str, Any],
    discovered: dict[str, dict[str, Any]],
    events: list[dict[str, Any]],
    fallback_lock: threading.Lock | None = None,
) -> dict[str, Any]:
    def run_fallback() -> dict[str, Any]:
        if fallback_lock is None:
            return discover_site_fallback(fetcher, source, discovered, events)
        with fallback_lock:
            return discover_site_fallback(fetcher, source, discovered, events)

    parts = urlsplit(source["url"])
    origin = urlunsplit((parts.scheme or "https", parts.netloc, "/", "", ""))
    if KATANA_ENABLED:
        katana = katana_discover_urls(origin)
        if katana.get("ok"):
            found = 0
            for candidate in katana["urls"]:
                if found >= SITE_DISCOVERY_MAX_URLS:
                    break
                if discovery_signal(candidate):
                    found += int(register_discovered_candidate(
                        candidate, source, "katana-site-crawl", discovered, events
                    ))
            if found:
                return {
                    "origin": origin,
                    "checked_at": now_iso(),
                    "engine": "Katana",
                    "urls_seen": len(katana["urls"]),
                    "sitemaps_checked": 0,
                    "pages_checked": len(katana["urls"]),
                    "new_policy_urls": found,
                    "duration_ms": katana.get("duration_ms", 0),
                    "partial": bool(katana.get("timed_out")),
                    "errors": int(bool(katana.get("timed_out"))),
                }
            fallback = run_fallback()
            fallback.update({
                "engine": "Katana + 内置补充",
                "katana_urls_seen": len(katana["urls"]),
                "katana_duration_ms": katana.get("duration_ms", 0),
                "katana_partial": bool(katana.get("timed_out")),
            })
            return fallback

    fallback = run_fallback()
    fallback["fallback_reason"] = "Katana已关闭" if not KATANA_ENABLED else str(katana.get("error") or "Katana未返回URL")
    return fallback


def run_site_discovery(
    fetcher: ScraplingAdaptiveFetcher,
    sources: list[dict[str, Any]],
    discovered: dict[str, dict[str, Any]],
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    state = load_json(SITE_DISCOVERY_STATE_PATH, {})
    if not isinstance(state, dict):
        state = {}
    if not SITE_DISCOVERY_ENABLED or SITE_DISCOVERY_SITES_PER_CYCLE <= 0:
        summary = {
            "enabled": SITE_DISCOVERY_ENABLED, "eligible_sites": 0, "sites_due": 0,
            "sites_checked": 0, "new_policy_urls": 0, "engine": "Katana" if KATANA_ENABLED else "内置发现器",
        }
        save_json(SITE_DISCOVERY_SUMMARY_PATH, summary)
        return summary

    by_origin: dict[str, dict[str, Any]] = {}
    for source in sources:
        if not trusted_discovery_source(source):
            continue
        parts = urlsplit(source["url"])
        origin = urlunsplit((parts.scheme or "https", parts.netloc, "/", "", ""))
        by_origin.setdefault(origin, source)

    now_timestamp = time.time()
    due: list[tuple[int, float, str, dict[str, Any]]] = []
    circuit_open_sites = 0
    for origin, source in by_origin.items():
        if float(state.get(origin, {}).get("circuit_open_until", 0) or 0) > now_timestamp:
            circuit_open_sites += 1
            continue
        last_checked = str(state.get(origin, {}).get("checked_at", ""))
        try:
            checked_timestamp = datetime.fromisoformat(last_checked).timestamp()
        except (TypeError, ValueError):
            checked_timestamp = 0.0
        if now_timestamp - checked_timestamp >= SITE_DISCOVERY_INTERVAL_SECONDS:
            entity_ids = source.get("entity_ids", [])
            entity_priority = 0 if any(entity.startswith("airline:") for entity in entity_ids) else 1
            due.append((entity_priority, checked_timestamp, origin, source))
    due.sort(key=lambda item: (item[0], item[1], item[2]))

    selected = due[:SITE_DISCOVERY_SITES_PER_CYCLE]
    results = []
    discovered_snapshot = dict(discovered)
    fallback_lock = threading.Lock()

    def discover_one(origin: str, source: dict[str, Any]) -> tuple[str, dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
        local_discovered = dict(discovered_snapshot)
        local_events: list[dict[str, Any]] = []
        result = discover_site(fetcher, source, local_discovered, local_events, fallback_lock)
        additions = {
            key: value for key, value in local_discovered.items()
            if key not in discovered_snapshot
        }
        return origin, result, additions, local_events

    previous_summary = load_json(SITE_DISCOVERY_SUMMARY_PATH, {})
    previous_checked = max(int(previous_summary.get("sites_checked", 0) or 0), 1)
    previous_failed_ratio = float(previous_summary.get("sites_failed", 0) or 0) / previous_checked
    adaptive_limit = 2 if previous_failed_ratio >= 0.5 else 3 if previous_failed_ratio >= 0.25 else SITE_DISCOVERY_CONCURRENCY
    workers = min(SITE_DISCOVERY_CONCURRENCY, adaptive_limit, len(selected))
    with ThreadPoolExecutor(max_workers=max(workers, 1)) as executor:
        futures = {
            executor.submit(discover_one, origin, source): origin
            for _, _, origin, source in selected
        }
        for future in as_completed(futures):
            origin = futures[future]
            try:
                _, result, additions, new_events = future.result()
            except Exception as exc:
                result = {
                    "origin": origin, "checked_at": now_iso(), "engine": "Katana",
                    "sitemaps_checked": 0, "pages_checked": 0, "new_policy_urls": 0,
                    "errors": 1, "error": str(exc)[:300],
                }
                additions, new_events = {}, []
            current_discovered, current_events = persist_shared_updates(additions, new_events)
            discovered.clear()
            discovered.update(current_discovered)
            events[:] = current_events
            state[origin] = result
            results.append(result)
            save_json(SITE_DISCOVERY_STATE_PATH, state)
    summary = {
        "enabled": True,
        "engine": "Katana" if any(str(item.get("engine", "")).startswith("Katana") for item in results) else "内置发现器",
        "eligible_sites": len(by_origin),
        "sites_due": max(len(due) - len(results), 0),
        "sites_checked": len(results),
        "concurrency": workers,
        "configured_concurrency": SITE_DISCOVERY_CONCURRENCY,
        "adaptive_limited": workers < min(SITE_DISCOVERY_CONCURRENCY, len(selected)),
        "new_policy_urls": sum(item["new_policy_urls"] for item in results),
        "sitemaps_checked": sum(item["sitemaps_checked"] for item in results),
        "pages_checked": sum(item["pages_checked"] for item in results),
        "errors": sum(item["errors"] for item in results),
        "sites_failed": sum(bool(item.get("errors")) for item in results),
        "circuit_open_sites": circuit_open_sites + sum(bool(item.get("blocked")) for item in results),
        "error_categories": dict(sum((Counter(item.get("error_categories", {})) for item in results), Counter())),
        "duration_p50_ms": percentile([int(item.get("duration_ms", 0) or 0) for item in results], 0.5),
        "duration_p95_ms": percentile([int(item.get("duration_ms", 0) or 0) for item in results], 0.95),
    }
    save_json(SITE_DISCOVERY_SUMMARY_PATH, summary)
    return summary


def date_sort_key(values: list[dict[str, str]]) -> str:
    candidates = []
    for item in values:
        value = item.get("value", "")
        match = re.search(r"(20\d{2})[-/.](\d{1,2})[-/.](\d{1,2})", value)
        if match:
            candidates.append(f"{int(match.group(1)):04d}-{int(match.group(2)):02d}-{int(match.group(3)):02d}")
            continue
        try:
            candidates.append(parsedate_to_datetime(value).date().isoformat())
        except (TypeError, ValueError, OverflowError):
            pass
    return max(candidates, default="")


def candidate_score(record: dict[str, Any], entity_id: str = "") -> int:
    if (
        record.get("status") != "ok"
        or not record.get("validation", {}).get("valid")
        or record.get("validation", {}).get("rule_version") != VALIDATION_RULE_VERSION
    ):
        return -1000
    hints = record.get("evidence_hints", [])
    if entity_id.startswith("airline:") and "primary-page-context" not in hints:
        return -500
    score = 0
    if record.get("validation", {}).get("topic_relevant"):
        score += 40
    if "official-context" in hints:
        score += 35
    if "historical-context" in hints:
        score -= 70
    if any(category in record.get("category", "") for category in ("country-policy", "airline-policy")):
        score += 10
    host = (urlsplit(record.get("canonical_url") or record.get("final_url") or record["url"]).hostname or "").lower()
    if any(token in host for token in (".gov", ".gouv", ".gob", "government", "europa.eu")):
        score += 25
    if record.get("canonical_url") and record.get("canonical_url") != record.get("url"):
        score += 5
    if date_sort_key(record.get("policy_dates", [])):
        score += 5
    return score


def build_source_registry(
    inventory: dict[str, Any], state: dict[str, Any], previous_registry: dict[str, Any], events: list[dict[str, Any]]
) -> dict[str, Any]:
    entity_defs = {item["id"]: item for item in inventory.get("entities", [])}
    inventory_sources = {item["id"]: item for item in inventory.get("sources", [])}
    by_entity: dict[str, list[dict[str, Any]]] = {key: [] for key in entity_defs}
    for source_id, record in state.items():
        definition = inventory_sources.get(source_id, {})
        enriched = {
            **record,
            "category": ", ".join(definition.get("categories", [])) or record.get("category", ""),
            "evidence_hints": definition.get("evidence_hints", record.get("evidence_hints", [])),
            "entity_ids": definition.get("entity_ids", record.get("entity_ids", [])),
            "source_id": source_id,
        }
        for entity_id in record.get("entity_ids", []):
            by_entity.setdefault(entity_id, []).append(enriched)
    previous_entities = previous_registry.get("entities", {})
    entities: dict[str, Any] = {}
    for entity_id, candidates in by_entity.items():
        ranked = sorted(
            candidates,
            key=lambda item: (candidate_score(item, entity_id), date_sort_key(item.get("policy_dates", [])), item.get("checked_at", "")),
            reverse=True,
        )
        trusted = [item for item in ranked if candidate_score(item, entity_id) >= 70]
        selected = trusted[0] if entity_id.startswith("airline:") and trusted else None
        current = None
        if selected:
            selected_url = normalize_candidate_url(
                selected.get("canonical_url") or selected.get("final_url") or selected["url"], selected["url"]
            ) or selected["url"]
            current = {
                "source_id": selected["source_id"], "url": selected["url"],
                "canonical_url": selected_url,
                "snapshot_path": selected.get("snapshot_path", ""), "score": candidate_score(selected, entity_id),
                "policy_date": date_sort_key(selected.get("policy_dates", [])),
                "confidence": "high" if candidate_score(selected, entity_id) >= 80 else "medium",
                "validated_at": selected.get("checked_at"),
            }
        entities[entity_id] = {
            "id": entity_id, "kind": entity_defs.get(entity_id, {}).get("kind", entity_id.split(":", 1)[0]),
            "name": entity_defs.get(entity_id, {}).get("name", entity_id.split(":", 1)[-1]),
            "current": current,
            "trusted_current_sources": [
                {
                    "source_id": item["source_id"], "url": normalize_candidate_url(
                        item.get("canonical_url") or item.get("final_url") or item["url"], item["url"]
                    ) or item["url"],
                    "score": candidate_score(item, entity_id), "policy_date": date_sort_key(item.get("policy_dates", [])),
                    "snapshot_path": item.get("snapshot_path", ""),
                }
                for item in trusted[:10]
            ],
            "candidates": [
                {
                    "source_id": item["source_id"], "url": item["url"], "status": item.get("status"),
                    "score": candidate_score(item, entity_id), "policy_date": date_sort_key(item.get("policy_dates", [])),
                    "error": item.get("error", ""),
                }
                for item in ranked
            ],
        }
        old_current = previous_entities.get(entity_id, {}).get("current")
        if old_current and current and old_current.get("canonical_url") != current.get("canonical_url"):
            signature = hashlib.sha256((old_current.get("canonical_url", "") + current["canonical_url"]).encode()).hexdigest()[:16]
            add_event(
                events,
                {
                    "guid": f"migration:{entity_id}:{signature}", "title": f"[现行页面切换] {entity_id}",
                    "url": current["canonical_url"], "detected_at": now_iso(),
                    "summary": f"旧页面：{old_current.get('canonical_url')}。新页面：{current['canonical_url']}。可信度：{current['confidence']}。旧快照继续保留。",
                },
            )
    return {
        "generated_at": now_iso(), "entity_count": len(entities),
        "entities_with_current": sum(1 for item in entities.values() if item["current"]),
        "entities_with_trusted_sources": sum(1 for item in entities.values() if item["trusted_current_sources"]),
        "entities": entities,
    }


def load_sources() -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, dict[str, Any]]]:
    inventory = load_json(INVENTORY_PATH, {})
    discovered = inventory.get("sources", []) if isinstance(inventory, dict) else []
    discovered_candidates = load_json(STATE_DIR / "discovered_sources.json", {})
    if not isinstance(discovered_candidates, dict):
        discovered_candidates = {}
    else:
        discovered_candidates = {
            url: item for url, item in discovered_candidates.items() if usable_candidate_url(url)
        }
    config = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    manual = [item for item in config.get("sources", []) if item.get("enabled", True)]
    by_url: dict[str, dict[str, Any]] = {}
    for item in discovered:
        source = dict(item)
        source["category"] = ", ".join(source.get("categories", [])) or "knowledge-base"
        by_url[source["url"]] = source
    for url, item in discovered_candidates.items():
        if usable_candidate_url(url) and item.get("monitor_enabled", True):
            by_url[url] = dict(item)
    for item in manual:
        existing = by_url.get(item["url"], {})
        refs = list(dict.fromkeys(existing.get("knowledge_base_refs", []) + ([item["knowledge_base_ref"]] if item.get("knowledge_base_ref") else [])))
        source = {**existing, **item, "knowledge_base_refs": refs}
        source.setdefault("id", "manual-" + hashlib.sha256(source["url"].encode()).hexdigest()[:20])
        by_url[source["url"]] = source
    sources = sorted(by_url.values(), key=lambda item: item["id"])
    return sources, inventory, discovered_candidates


def write_feed(events: list[dict[str, Any]]) -> None:
    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")
    ET.SubElement(channel, "title").text = "宠物托运知识库数据源变更"
    ET.SubElement(channel, "link").text = f"http://official-monitor:{PORT}/status.json"
    ET.SubElement(channel, "description").text = "知识库引用的国家政策、航司政策和行业数据源内容及可用性变化"
    ET.SubElement(channel, "language").text = "zh-cn"
    for event in reversed(events[-EVENT_LIMIT:]):
        item = ET.SubElement(channel, "item")
        ET.SubElement(item, "title").text = event["title"]
        ET.SubElement(item, "link").text = event["url"]
        ET.SubElement(item, "guid", isPermaLink="false").text = event["guid"]
        ET.SubElement(item, "pubDate").text = format_datetime(datetime.fromisoformat(event["detected_at"]))
        ET.SubElement(item, "description").text = event["summary"]
    ET.indent(rss, space="  ")
    ET.ElementTree(rss).write(STATE_DIR / "feed.xml", encoding="utf-8", xml_declaration=True)


def source_refs(source: dict[str, Any]) -> list[str]:
    refs = source.get("knowledge_base_refs", [])
    if source.get("knowledge_base_ref"):
        refs = refs + [source["knowledge_base_ref"]]
    return list(dict.fromkeys(refs))


def scan_source(
    fetcher: ScraplingAdaptiveFetcher, source: dict[str, Any], previous: dict[str, Any],
    events: list[dict[str, Any]], discovered: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    checked_at = now_iso()
    refs = source_refs(source)
    record: dict[str, Any] = {
        "name": source.get("name") or urlsplit(source["url"]).netloc,
        "url": source["url"],
        "category": source.get("category", "knowledge-base"),
        "knowledge_base_refs": refs,
        "entity_ids": source.get("entity_ids", []),
        "evidence_hints": source.get("evidence_hints", []),
        "checked_at": checked_at,
    }
    headers: dict[str, str] = {}
    if previous.get("etag"):
        headers["If-None-Match"] = previous["etag"]
    if previous.get("last_modified"):
        headers["If-Modified-Since"] = previous["last_modified"]
    response = None
    try:
        expect_topic = bool(source.get("required_terms")) or (
            bool(source.get("entity_ids"))
            and any(hint in source.get("evidence_hints", []) for hint in ("official-context", "primary-page-context"))
        )
        source_categories = {
            item.strip().lower()
            for item in str(source.get("category", "")).split(",")
            if item.strip()
        }
        strict_policy_page = (
            "primary-page-context" in source.get("evidence_hints", [])
            or bool(source_categories.intersection({"airline-policy", "country-policy"}))
        )
        minimum_expected = int(
            source.get("min_content_bytes", 300 if strict_policy_page else 80)
        )
        response = fetcher.fetch(
            source["url"],
            headers=headers,
            timeout=int(source.get("timeout", REQUEST_TIMEOUT)),
            expect_topic=expect_topic,
            topic_terms=STRONG_TOPIC_TERMS,
            minimum_visible_chars=minimum_expected,
        )
        if response.status_code == 304 and previous.get("sha256"):
            return {
                **previous, "checked_at": checked_at, "last_ok_at": checked_at,
                "consecutive_failures": 0, "status": "ok", "status_code": 304,
            }
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "")
        digest, page_title, content_length, sample = fingerprint(response.content, content_type, source.get("keywords"))
        is_html = "html" in content_type.lower() or "xml" in content_type.lower()
        full_text, _ = normalize_html(response.content) if is_html else (sample, "")
        policy_fields = extract_policy_fields(full_text) if is_html else ""
        previous_policy_fields = str(previous.get("policy_fields", ""))
        if not previous_policy_fields and is_html:
            previous_policy_fields = extract_policy_fields(str(previous.get("content_sample", "")))
        field_diff = policy_field_diff(previous_policy_fields, policy_fields) if is_html else {
            "quality_gate": False, "changed_fields": [], "removed": [], "added": [],
        }
        facts = parse_html_facts(response.content, response.url) if is_html else {"canonical_url": "", "dates": [], "links": []}
        minimum = minimum_expected
        if content_length < minimum:
            raise ValueError(f"content too small after normalization: {content_length} bytes")
        reason = soft_error_reason(page_title, full_text) if is_html else ""
        if reason:
            raise ValueError(reason)
        required_terms = [term.lower() for term in source.get("required_terms", [])]
        if required_terms and is_html:
            if not any(term in full_text.lower() for term in required_terms):
                raise ValueError("required topic terms not found; possible soft error page")
        relevant, matched_terms = topic_relevance(full_text)
        resolved_url = normalize_candidate_url(response.url, response.url) or response.url
        canonical_url = normalize_candidate_url(facts.get("canonical_url", ""), response.url) if facts.get("canonical_url") else ""
        if canonical_url and not acceptable_canonical(canonical_url, response.url):
            canonical_url = ""
        policy_dates = list(facts.get("dates", []))
        if response.headers.get("Last-Modified"):
            policy_dates.append({"kind": "http:last-modified", "value": response.headers["Last-Modified"]})
        record.update(
            {
                "status": "ok", "status_code": response.status_code, "final_url": response.url,
                "canonical_url": canonical_url or resolved_url,
                "fetch_mode": response.mode, "escalation_reason": response.escalation_reason,
                "content_type": content_type, "content_bytes": content_length, "sha256": digest,
                "page_title": page_title, "content_sample": sample,
                "policy_fields": policy_fields,
                "policy_fingerprint": hashlib.sha256(policy_fields.encode("utf-8")).hexdigest() if policy_fields else "",
                "policy_dates": policy_dates, "validation": {
                    "valid": True, "rule_version": VALIDATION_RULE_VERSION, "soft_error": False,
                    "topic_relevant": relevant, "matched_terms": matched_terms,
                },
                "etag": response.headers.get("ETag", ""), "last_modified": response.headers.get("Last-Modified", ""),
                "last_ok_at": checked_at, "consecutive_failures": 0,
            }
        )
        if not previous.get("sha256") or previous.get("sha256") != digest or not previous.get("snapshot_path"):
            record["snapshot_path"] = save_snapshot(source, record, response.content, full_text, previous)
        else:
            record["snapshot_path"] = previous.get("snapshot_path", "")
        if discovered is not None and is_html:
            discover_page_candidates(source, response.url, facts, full_text, discovered, events)
        content_changed = (
            previous.get("status") == "ok"
            and previous.get("sha256")
            and previous["sha256"] != digest
        )
        has_policy_baseline = "policy_fingerprint" in previous
        policy_change_candidate = (
            field_diff["quality_gate"] and has_policy_baseline
            if is_html else content_changed
        )
        if relevant and content_changed and policy_change_candidate:
            ref_text = "、".join(refs[:4]) or "未标注"
            field_names = "、".join(field_diff.get("changed_fields", [])[:6]) or "政策条款"
            add_event(
                events,
                {
                    "guid": f"content:{source['id']}:{digest}", "title": f"[数据源内容变化] {record['name']}",
                    "url": response.url, "detected_at": checked_at,
                    "summary": f"检测到政策字段变化：{field_names}。分类：{record['category']}。知识库位置：{ref_text}。",
                    "policy_evidence": field_diff,
                },
            )
        if previous.get("status") == "error":
            add_event(
                events,
                {
                    "guid": f"recovered:{source['id']}:{digest}", "title": f"[数据源恢复] {record['name']}",
                    "url": response.url, "detected_at": checked_at,
                    "summary": f"此前不可访问的数据源已恢复。知识库位置：{'、'.join(refs[:4]) or '未标注'}。",
                },
            )
    except Exception as exc:
        status_code = getattr(response, "status_code", None) if response is not None else None
        record.update({
            "status": "error", "error": str(exc)[:500], "validation": {"valid": False},
            "status_code": status_code,
            "failure_category": failure_category(str(exc), status_code),
            "consecutive_failures": max(int(previous.get("consecutive_failures", 0) or 0), 0) + 1,
            "last_ok_at": previous.get("last_ok_at", previous.get("checked_at", "") if previous.get("status") == "ok" else ""),
            "last_good_snapshot_path": previous.get("snapshot_path", previous.get("last_good_snapshot_path", "")),
        })
        if response is not None:
            record.update({"fetch_mode": response.mode, "escalation_reason": response.escalation_reason})
        if previous.get("status") == "ok":
            error_key = hashlib.sha256(str(exc).encode()).hexdigest()[:12]
            add_event(
                events,
                {
                    "guid": f"unavailable:{source['id']}:{error_key}", "title": f"[数据源不可用] {record['name']}",
                    "url": source["url"], "detected_at": checked_at,
                    "summary": f"数据源从正常变为不可访问：{record['error']}。知识库位置：{'、'.join(refs[:4]) or '未标注'}。",
                },
            )
    return record


def scan() -> dict[str, Any]:
    sources, inventory, discovered = load_sources()
    state_path = STATE_DIR / "state.json"
    events_path = STATE_DIR / "events.json"
    meta_path = STATE_DIR / "monitor_meta.json"
    registry_path = STATE_DIR / "source_registry.json"
    state = load_state_with_journal(state_path)
    events = load_json(events_path, [])
    previous_registry = load_json(registry_path, {})
    meta = load_json(meta_path, {"cursor": 0, "known_source_ids": []})
    current_ids = [source["id"] for source in sources]
    source_by_id = {source["id"]: source for source in sources}
    old_ids = set(meta.get("known_source_ids", []))
    if old_ids:
        added = set(current_ids) - old_ids
        removed = old_ids - set(current_ids)
        if added or removed:
            signature = hashlib.sha256(("|".join(sorted(added)) + "#" + "|".join(sorted(removed))).encode()).hexdigest()[:16]
            add_event(
                events,
                {
                    "guid": f"inventory:{signature}", "title": "[知识库数据源清单变化]",
                    "url": f"http://official-monitor:{PORT}/inventory.json", "detected_at": now_iso(),
                    "summary": f"知识库新增 {len(added)} 个数据源，移除 {len(removed)} 个数据源。",
                },
            )
    saved_progress = load_json(SCAN_PROGRESS_PATH, {})
    saved_batch_ids = saved_progress.get("batch_source_ids", []) if isinstance(saved_progress, dict) else []
    resume = bool(
        saved_progress.get("phase") == "scanning"
        and saved_batch_ids
        and int(saved_progress.get("next_index", 0)) <= len(saved_batch_ids)
    )
    if resume:
        batch_ids = saved_batch_ids
        start_index = max(int(saved_progress.get("next_index", 0)), 0)
        next_cursor = int(saved_progress.get("next_cursor", 0))
        progress = dict(saved_progress)
        missing_sources = sum(source_id not in source_by_id for source_id in batch_ids)
        print(
            f"[official-monitor] resuming batch at {start_index}/{len(batch_ids)} "
            f"missing_sources={missing_sources}",
            flush=True,
        )
    else:
        batch, due_count, selected_tiers = select_scan_batch(sources, state, BATCH_SIZE)
        next_cursor = 0
        batch_ids = [item["id"] for item in batch]
        start_index = 0
        progress = {
            "phase": "scanning",
            "source_signature": source_list_signature(sources),
            "batch_source_ids": batch_ids,
            "batch_size": len(batch_ids),
            "next_index": 0,
            "next_cursor": next_cursor,
            "started_at": now_iso(),
            "last_progress_at": now_iso(),
            "restart_count": 0,
            "due_count": due_count,
            "selected_tiers": dict(selected_tiers),
            "completed_source_ids": [],
        }
        save_json(SCAN_PROGRESS_PATH, progress)

    completed_ids = set(progress.get("completed_source_ids", []))
    if not completed_ids and start_index:
        completed_ids.update(batch_ids[:start_index])
    dynamic_semaphore = threading.Semaphore(SCAN_DYNAMIC_CONCURRENCY)
    stealth_semaphore = threading.Semaphore(SCAN_STEALTH_CONCURRENCY)
    host_locks = {
        (urlsplit(source_by_id[source_id]["url"]).hostname or source_id): threading.Semaphore(1)
        for source_id in batch_ids
        if source_id in source_by_id
    }
    worker_fetchers: list[ScraplingAdaptiveFetcher] = []
    worker_fetchers_lock = threading.Lock()
    discovered_snapshot = dict(discovered)
    effective_scan_concurrency, adaptive_reason = adaptive_scan_concurrency(state)
    scan_started_monotonic = time.monotonic()
    fetch_durations: list[int] = []

    def page_fetcher() -> ScraplingAdaptiveFetcher:
        current = ScraplingAdaptiveFetcher(
            1,
            1,
            browser_hard_timeout=BROWSER_HARD_TIMEOUT,
            cloudflare_solver_enabled=CLOUDFLARE_SOLVER_ENABLED,
            cloudflare_timeout=CLOUDFLARE_TIMEOUT,
            agent_state_dir=STATE_DIR / "scraping-agent",
            agent_max_attempts=AGENT_MAX_ATTEMPTS,
            agent_max_duration=AGENT_MAX_DURATION,
            dynamic_semaphore=dynamic_semaphore,
            stealth_semaphore=stealth_semaphore,
        )
        with worker_fetchers_lock:
            worker_fetchers.append(current)
        return current

    def scan_one(position: int, source: dict[str, Any]) -> tuple[int, dict[str, Any], dict[str, Any], list[dict[str, Any]], tuple[str, ...], int]:
        started = time.monotonic()
        local_events: list[dict[str, Any]] = []
        local_discovered = dict(discovered_snapshot)
        host = urlsplit(source["url"]).hostname or source["id"]
        with host_locks[host]:
            record = scan_source(
                page_fetcher(), source, state.get(source["id"], {}),
                local_events, local_discovered,
            )
        additions = {
            key: value for key, value in local_discovered.items()
            if key not in discovered_snapshot
        }
        remove_urls: tuple[str, ...] = ()
        if (
            source.get("category") == "discovered-current-candidate"
            and record.get("status") == "ok"
            and not record.get("validation", {}).get("topic_relevant")
        ):
            remove_urls = (source["url"],)
        duration_ms = round((time.monotonic() - started) * 1000)
        record["fetch_duration_ms"] = duration_ms
        return position, record, additions, local_events, remove_urls, duration_ms

    pending = [
        (position, source_by_id[source_id])
        for position, source_id in enumerate(batch_ids)
        if source_id not in completed_ids and source_id in source_by_id
    ]
    missing = [source_id for source_id in batch_ids if source_id not in completed_ids and source_id not in source_by_id]
    completed_ids.update(missing)
    progress.update({
        "completed_source_ids": list(completed_ids),
        "active_workers": min(effective_scan_concurrency, len(pending)),
        "scan_concurrency": effective_scan_concurrency,
        "configured_scan_concurrency": SCAN_CONCURRENCY,
        "adaptive_reason": adaptive_reason,
        "last_activity_at": now_iso(),
    })
    save_json(SCAN_PROGRESS_PATH, progress)

    with ThreadPoolExecutor(max_workers=max(min(effective_scan_concurrency, len(pending)), 1)) as executor:
        futures = {
            executor.submit(scan_one, position, source): (position, source)
            for position, source in pending
        }
        for future in as_completed(futures):
            position, source = futures[future]
            try:
                _, record, additions, new_events, remove_urls, duration_ms = future.result()
            except Exception as exc:
                record = {
                    "name": source.get("name") or urlsplit(source["url"]).netloc,
                    "url": source["url"], "status": "error", "error": str(exc)[:500],
                    "checked_at": now_iso(), "validation": {"valid": False},
                    "consecutive_failures": int(state.get(source["id"], {}).get("consecutive_failures", 0)) + 1,
                }
                additions, new_events, remove_urls, duration_ms = {}, [], (), 0
            if duration_ms:
                fetch_durations.append(duration_ms)
            state[source["id"]] = record
            append_state_journal({source["id"]: record})
            discovered, events = persist_shared_updates(additions, new_events, remove_urls)
            completed_ids.add(source["id"])
            next_index = 0
            while next_index < len(batch_ids) and batch_ids[next_index] in completed_ids:
                next_index += 1
            completed = len(completed_ids)
            progress.update({
                "current_index": completed,
                "current_source_id": source["id"],
                "current_url": source["url"],
                "completed_source_ids": list(completed_ids),
                "next_index": next_index,
                "last_activity_at": now_iso(),
                "last_progress_at": now_iso(),
                "last_checkpoint_at": now_iso(),
                "duration_p50_ms": percentile(fetch_durations, 0.5),
                "duration_p95_ms": percentile(fetch_durations, 0.95),
                "throughput_per_minute": round(
                    len(fetch_durations) / max(time.monotonic() - scan_started_monotonic, 0.001) * 60, 1
                ),
            })
            save_json(SCAN_PROGRESS_PATH, progress)
            if completed % CHECKPOINT_EVERY == 0 or completed == len(batch_ids):
                print(
                    f"[official-monitor] concurrent batch progress {completed}/{len(batch_ids)} "
                    f"workers={progress['active_workers']} source={source['id']}",
                    flush=True,
                )

    aggregate_fetcher_stats: Counter[str] = Counter()
    for item in worker_fetchers:
        aggregate_fetcher_stats.update(item.stats())
    fetcher_metrics: dict[str, Any] = {**dict(aggregate_fetcher_stats),
        "worker_instances": len(worker_fetchers),
        "scan_concurrency": effective_scan_concurrency,
        "configured_scan_concurrency": SCAN_CONCURRENCY,
        "dynamic_concurrency": SCAN_DYNAMIC_CONCURRENCY,
        "stealth_concurrency": SCAN_STEALTH_CONCURRENCY,
        "duration_p50_ms": percentile(fetch_durations, 0.5),
        "duration_p95_ms": percentile(fetch_durations, 0.95),
        "throughput_per_minute": round(
            len(fetch_durations) / max(time.monotonic() - scan_started_monotonic, 0.001) * 60, 1
        ),
        "adaptive_reason": adaptive_reason,
    }
    site_discovery = load_json(SITE_DISCOVERY_SUMMARY_PATH, {})
    active = set(current_ids)
    state = {key: value for key, value in state.items() if key in active}
    registry = build_source_registry(inventory, state, previous_registry, events)
    meta = {
        "cursor": next_cursor, "known_source_ids": current_ids, "last_batch_started_at": now_iso(),
        "last_batch_completed_at": now_iso(), "last_batch_size": len(batch_ids), "inventory_generated_at": inventory.get("generated_at"),
    }
    status = {
        "generated_at": now_iso(), "inventory": {
            "files_scanned": inventory.get("files_scanned", 0), "url_references": inventory.get("url_references", 0),
            "unique_sources": len(sources), "knowledge_base_entities": inventory.get("entity_count", 0),
            "discovered_candidates": len(discovered), "category_counts": inventory.get("category_counts", {}),
        },
        "cycle": {
            "batch_size": len(batch_ids), "next_cursor": next_cursor, "checked": len(state),
            "pending": max(len(sources) - len(state), 0),
            "due": int(progress.get("due_count", len(batch_ids)) or 0),
            "selected_tiers": progress.get("selected_tiers", {}),
        },
        "sources_ok": sum(1 for item in state.values() if item.get("status") == "ok"),
        "sources_error": sum(1 for item in state.values() if item.get("status") == "error"),
        "snapshots_saved": sum(1 for item in state.values() if item.get("snapshot_path")),
        "entities_with_current": registry["entities_with_current"],
        "entities_with_trusted_sources": registry["entities_with_trusted_sources"],
        "fetcher": {"engine": "scrapling", **fetcher_metrics},
        "discovery_fetcher": {"engine": "scrapling", "independent_worker": True},
        "site_discovery": site_discovery,
    }
    save_json(state_path, state)
    discovered, events = persist_shared_updates({}, events)
    save_json(meta_path, meta)
    save_json(registry_path, registry)
    save_json(STATE_DIR / "status.json", status)
    save_json(STATE_DIR / "inventory.json", inventory)
    write_feed(events)
    clear_state_journal()
    try:
        SCAN_PROGRESS_PATH.unlink()
    except FileNotFoundError:
        pass
    return status


class MonitorRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, directory=str(STATE_DIR), **kwargs)

    def send_bytes(self, content: bytes, content_type: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(content)

    def do_GET(self) -> None:  # noqa: N802
        path = urlsplit(self.path).path
        if path in {"/", "/dashboard", "/dashboard.html"}:
            try:
                self.send_bytes(DASHBOARD_PATH.read_bytes(), "text/html; charset=utf-8")
            except OSError as exc:
                self.send_error(500, f"dashboard unavailable: {exc}")
            return
        if path == "/api/overview.json":
            try:
                content = json.dumps(dashboard_payload(), ensure_ascii=False).encode("utf-8")
                self.send_bytes(content, "application/json; charset=utf-8")
            except Exception as exc:
                self.send_error(500, f"overview unavailable: {exc}")
            return
        if path == "/api/brief.json":
            try:
                content = json.dumps(business_brief_payload(), ensure_ascii=False).encode("utf-8")
                self.send_bytes(content, "application/json; charset=utf-8")
            except Exception as exc:
                self.send_error(500, f"brief unavailable: {exc}")
            return
        super().do_GET()


def serve() -> None:
    ThreadingHTTPServer(("0.0.0.0", PORT), MonitorRequestHandler).serve_forever()


def site_discovery_worker() -> None:
    while True:
        started = time.monotonic()
        try:
            sources, _, discovered = load_sources()
            events = load_json(STATE_DIR / "events.json", [])
            fetcher = ScraplingAdaptiveFetcher(
                DYNAMIC_FETCH_LIMIT,
                STEALTH_FETCH_LIMIT,
                browser_hard_timeout=BROWSER_HARD_TIMEOUT,
                cloudflare_solver_enabled=CLOUDFLARE_SOLVER_ENABLED,
                cloudflare_timeout=CLOUDFLARE_TIMEOUT,
                agent_state_dir=STATE_DIR / "scraping-agent",
                agent_max_attempts=AGENT_MAX_ATTEMPTS,
                agent_max_duration=AGENT_MAX_DURATION,
            )
            summary = run_site_discovery(fetcher, sources, discovered, events)
            print(
                "[official-monitor] independent discovery complete; "
                f"checked={summary.get('sites_checked', 0)} "
                f"due={summary.get('sites_due', 0)} "
                f"new_urls={summary.get('new_policy_urls', 0)}",
                flush=True,
            )
            retry_interval = SITE_DISCOVERY_CYCLE_INTERVAL
        except Exception as exc:
            print(f"[official-monitor] independent discovery failed: {exc}", flush=True)
            retry_interval = FAILURE_RETRY_INTERVAL
        elapsed = time.monotonic() - started
        time.sleep(max(int(retry_interval - elapsed), 0))


def main() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    AgentStateStore(STATE_DIR / "scraping-agent").compact_manual_queue()
    threading.Thread(target=serve, daemon=True).start()
    threading.Thread(target=policy_summary_worker, daemon=True).start()
    threading.Thread(target=site_discovery_worker, daemon=True).start()
    while True:
        cycle_started = time.monotonic()
        try:
            status = scan()
            target_interval = int(os.getenv("MONITOR_INTERVAL", "900"))
            elapsed = time.monotonic() - cycle_started
            interval = max(int(target_interval - elapsed), 0)
            print(
                "[official-monitor] scan complete; "
                f"checked={status['cycle']['checked']} pending={status['cycle']['pending']} "
                f"next batch in {interval}s",
                flush=True,
            )
        except Exception as exc:
            print(f"[official-monitor] scan failed: {exc}", flush=True)
            interval = FAILURE_RETRY_INTERVAL
            print(f"[official-monitor] retrying failed scan in {interval}s", flush=True)
        time.sleep(interval)


if __name__ == "__main__":
    main()
