# coding=utf-8
"""
RSS 抓取器

负责从配置的 RSS 源抓取数据并转换为标准格式
"""

import hashlib
import json
import random
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

import requests

from .parser import RSSParser
from trendradar.storage.base import RSSItem, RSSData
from trendradar.utils.time import get_configured_time, DEFAULT_TIMEZONE


OFFICIAL_CHANGE_FEED_ID = "official-source-changes"
CHANGE_STATUSES = {"confirmed", "retracted", "superseded"}

# 并发抓取的最大线程数（独立于源数量，避免同时打开过多连接）
MAX_CONCURRENT_FETCHES = 8
# 超时/限流/5xx 等临时性错误的最大重试次数（不含首次尝试）
MAX_TRANSIENT_RETRIES = 2
_TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}


@dataclass
class RSSFeedConfig:
    """RSS 源配置"""
    id: str                     # 源 ID
    name: str                   # 显示名称
    url: str                    # RSS URL
    max_items: int = 0          # 最大条目数（0=不限制）
    enabled: bool = True        # 是否启用
    max_age_days: Optional[int] = None  # 文章最大年龄（天），覆盖全局设置；None=使用全局，0=禁用过滤


class RSSFetcher:
    """RSS 抓取器"""

    def __init__(
        self,
        feeds: List[RSSFeedConfig],
        request_interval: int = 2000,
        timeout: int = 15,
        use_proxy: bool = False,
        proxy_url: str = "",
        timezone: str = DEFAULT_TIMEZONE,
        freshness_enabled: bool = True,
        default_max_age_days: int = 3,
    ):
        """
        初始化抓取器

        Args:
            feeds: RSS 源配置列表
            request_interval: 请求间隔（毫秒）
            timeout: 请求超时（秒）
            use_proxy: 是否使用代理
            proxy_url: 代理 URL
            timezone: 时区配置（如 'Asia/Shanghai'）
            freshness_enabled: 是否启用新鲜度过滤
            default_max_age_days: 默认最大文章年龄（天）
        """
        self.feeds = [f for f in feeds if f.enabled]
        self.request_interval = request_interval
        self.timeout = timeout
        self.use_proxy = use_proxy
        self.proxy_url = proxy_url
        self.timezone = timezone
        self.freshness_enabled = freshness_enabled
        self.default_max_age_days = default_max_age_days

        self.parser = RSSParser()
        self.session = self._create_session()
        self._verified_authoritative_ids: set[str] = set()

    @staticmethod
    def _validate_authoritative_snapshot(content: bytes, feed_url: str) -> None:
        """Reject partial/legacy snapshots before absence can retract stored changes."""

        try:
            root = ET.fromstring(content)
        except ET.ParseError as exc:
            raise ValueError(f"权威变化快照 XML 不完整 ({feed_url}): {exc}") from exc

        def local_name(element: ET.Element) -> str:
            return str(element.tag).rsplit("}", 1)[-1]

        channel = next((item for item in root.iter() if local_name(item) == "channel"), None)
        if channel is None:
            raise ValueError(f"权威变化快照缺少 channel ({feed_url})")

        fields = {
            local_name(child): str(child.text or "").strip()
            for child in channel
            if local_name(child) != "item"
        }
        if fields.get("snapshot_complete", "").lower() != "true":
            raise ValueError(f"权威变化快照未声明完整 ({feed_url})")

        item_nodes = [child for child in channel if local_name(child) == "item"]
        try:
            declared_count = int(fields.get("snapshot_count", ""))
        except ValueError as exc:
            raise ValueError(f"权威变化快照条目数无效 ({feed_url})") from exc
        if declared_count != len(item_nodes):
            raise ValueError(
                f"权威变化快照条目数不一致 ({feed_url}): {declared_count} != {len(item_nodes)}"
            )

        fingerprint = []
        for item in item_nodes:
            values = {local_name(child): str(child.text or "").strip() for child in item}
            fingerprint.append([
                values.get("change_id") or values.get("guid") or values.get("link", ""),
                int(values.get("revision") or 1),
                (values.get("status") or "confirmed").lower(),
                values.get("supersedes", ""),
            ])
        digest = hashlib.sha256(
            json.dumps(fingerprint, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        if fields.get("snapshot_digest") != digest:
            raise ValueError(f"权威变化快照摘要校验失败 ({feed_url})")

    def _create_session(self) -> requests.Session:
        """创建请求会话"""
        session = requests.Session()
        session.headers.update({
            "User-Agent": "TrendRadar/2.0 RSS Reader (https://github.com/trendradar)",
            "Accept": "application/feed+json, application/json, application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        })

        if self.use_proxy and self.proxy_url:
            session.proxies = {
                "http": self.proxy_url,
                "https": self.proxy_url,
            }

        return session

    @staticmethod
    def _is_transient_http_error(exc: requests.HTTPError) -> bool:
        status = exc.response.status_code if exc.response is not None else None
        return status in _TRANSIENT_STATUS_CODES

    @staticmethod
    def _backoff_sleep(attempt: int) -> None:
        """指数退避：第 0 次重试等 ~1s，第 1 次 ~2s，带少量随机抖动"""
        time.sleep((2 ** attempt) + random.uniform(0, 0.5))

    def fetch_feed(self, feed: RSSFeedConfig) -> Tuple[List[RSSItem], Optional[str]]:
        """
        抓取单个 RSS 源，对超时/限流/5xx 等临时性错误做指数退避重试

        Args:
            feed: RSS 源配置

        Returns:
            (条目列表, 错误信息) 元组
        """
        attempt = 0
        while True:
            try:
                return self._fetch_feed_once(feed)
            except requests.Timeout:
                if attempt < MAX_TRANSIENT_RETRIES:
                    self._backoff_sleep(attempt)
                    attempt += 1
                    continue
                error = f"请求超时 ({self.timeout}s)，已重试 {attempt} 次"
                print(f"[RSS] {feed.name}: {error}")
                return [], error
            except requests.HTTPError as e:
                if self._is_transient_http_error(e) and attempt < MAX_TRANSIENT_RETRIES:
                    self._backoff_sleep(attempt)
                    attempt += 1
                    continue
                error = f"请求失败: {e}"
                print(f"[RSS] {feed.name}: {error}")
                return [], error
            except requests.RequestException as e:
                error = f"请求失败: {e}"
                print(f"[RSS] {feed.name}: {error}")
                return [], error
            except ValueError as e:
                error = f"解析失败: {e}"
                print(f"[RSS] {feed.name}: {error}")
                return [], error
            except Exception as e:
                error = f"未知错误: {e}"
                print(f"[RSS] {feed.name}: {error}")
                return [], error

    def _fetch_feed_once(self, feed: RSSFeedConfig) -> Tuple[List[RSSItem], Optional[str]]:
        """单次抓取尝试，异常直接向上抛出，由 fetch_feed 统一分类处理并决定是否重试"""
        response = self.session.get(feed.url, timeout=self.timeout)
        response.raise_for_status()

        if feed.id == OFFICIAL_CHANGE_FEED_ID:
            self._validate_authoritative_snapshot(response.content, feed.url)
            self._verified_authoritative_ids.add(feed.id)

        # 必须传原始字节，让 XML 声明/BOM 决定编码；response.text 在缺少
        # charset 的 UTF-8 feed 上会被 requests 误判为 ISO-8859-1。
        parsed_items = self.parser.parse(response.content, feed.url)

        # 限制条目数量（0=不限制）
        # 官方变化源是权威有效集合，不能在同步前截断，否则会误撤销旧条目。
        if feed.max_items > 0 and feed.id != OFFICIAL_CHANGE_FEED_ID:
            parsed_items = parsed_items[:feed.max_items]

        # 转换为 RSSItem（使用配置的时区）
        now = get_configured_time(self.timezone)
        crawl_time = now.strftime("%H:%M")
        items = []

        for parsed in parsed_items:
            change_id = str(parsed.change_id or "").strip()
            revision = int(parsed.revision or 0)
            status = str(parsed.status or "").strip().lower()
            supersedes = str(parsed.supersedes or "").strip()

            if feed.id == OFFICIAL_CHANGE_FEED_ID:
                change_id = change_id or str(parsed.guid or "").strip() or parsed.url
                revision = revision if revision > 0 else 1
                status = status if status in CHANGE_STATUSES else "confirmed"

            item = RSSItem(
                title=parsed.title,
                feed_id=feed.id,
                feed_name=feed.name,
                url=parsed.url,
                guid=parsed.guid or "",
                published_at=parsed.published_at or "",
                summary=parsed.summary or "",
                author=parsed.author or "",
                crawl_time=crawl_time,
                first_time=crawl_time,
                last_time=crawl_time,
                count=1,
                change_id=change_id,
                revision=revision,
                status=status,
                supersedes=supersedes,
                is_active=status not in {"retracted", "superseded"},
            )
            items.append(item)

        # 注意：新鲜度过滤已移至推送阶段（_convert_rss_items_to_list）
        # 这样所有文章都会存入数据库，但旧文章不会推送
        print(f"[RSS] {feed.name}: 获取 {len(items)} 条")
        return items, None

    def fetch_all(self) -> RSSData:
        """
        抓取所有 RSS 源

        Returns:
            RSSData 对象
        """
        all_items: Dict[str, List[RSSItem]] = {}
        id_to_name: Dict[str, str] = {}
        failed_ids: List[str] = []
        self._verified_authoritative_ids.clear()

        # 使用配置的时区
        now = get_configured_time(self.timezone)
        crawl_time = now.strftime("%H:%M")
        crawl_date = now.strftime("%Y-%m-%d")

        print(f"[RSS] 开始抓取 {len(self.feeds)} 个 RSS 源...")

        # 并发抓取：各源的等待响应时间互不阻塞，仅限制同时在途的请求数；
        # 提交间仍保留原有的请求间隔（带随机波动），错开各请求的发起时间。
        max_workers = max(1, min(MAX_CONCURRENT_FETCHES, len(self.feeds)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_feed = {}
            for i, feed in enumerate(self.feeds):
                if i > 0 and self.request_interval > 0:
                    interval = self.request_interval / 1000
                    jitter = random.uniform(-0.2, 0.2) * interval
                    time.sleep(max(0.0, interval + jitter))
                future_to_feed[executor.submit(self.fetch_feed, feed)] = feed

            for future in as_completed(future_to_feed):
                feed = future_to_feed[future]
                items, error = future.result()

                id_to_name[feed.id] = feed.name

                if error:
                    failed_ids.append(feed.id)
                else:
                    all_items[feed.id] = items

        total_items = sum(len(items) for items in all_items.values())
        print(f"[RSS] 抓取完成: {len(all_items)} 个源成功, {len(failed_ids)} 个失败, 共 {total_items} 条")

        return RSSData(
            date=crawl_date,
            crawl_time=crawl_time,
            items=all_items,
            id_to_name=id_to_name,
            failed_ids=failed_ids,
            authoritative_complete_ids=sorted(self._verified_authoritative_ids),
        )

    @classmethod
    def from_config(cls, config: Dict) -> "RSSFetcher":
        """
        从配置字典创建抓取器

        Args:
            config: 配置字典，格式如下：
                {
                    "enabled": true,
                    "request_interval": 2000,
                    "freshness_filter": {
                        "enabled": true,
                        "max_age_days": 3
                    },
                    "feeds": [
                        {"id": "hacker-news", "name": "Hacker News", "url": "...", "max_age_days": 1}
                    ]
                }

        Returns:
            RSSFetcher 实例
        """
        # 读取新鲜度过滤配置
        freshness_config = config.get("freshness_filter", {})
        freshness_enabled = freshness_config.get("enabled", True)  # 默认启用
        default_max_age_days = freshness_config.get("max_age_days", 3)  # 默认3天

        feeds = []
        for feed_config in config.get("feeds", []):
            # 读取并验证单个 feed 的 max_age_days（可选）
            max_age_days_raw = feed_config.get("max_age_days")
            max_age_days = None
            if max_age_days_raw is not None:
                try:
                    max_age_days = int(max_age_days_raw)
                    if max_age_days < 0:
                        feed_id = feed_config.get("id", "unknown")
                        print(f"[警告] RSS feed '{feed_id}' 的 max_age_days 为负数，将使用全局默认值")
                        max_age_days = None
                except (ValueError, TypeError):
                    feed_id = feed_config.get("id", "unknown")
                    print(f"[警告] RSS feed '{feed_id}' 的 max_age_days 格式错误：{max_age_days_raw}")
                    max_age_days = None

            feed = RSSFeedConfig(
                id=feed_config.get("id", ""),
                name=feed_config.get("name", ""),
                url=feed_config.get("url", ""),
                max_items=feed_config.get("max_items", 0),  # 0=不限制
                enabled=feed_config.get("enabled", True),
                max_age_days=max_age_days,  # None=使用全局，0=禁用，>0=覆盖
            )
            if feed.id and feed.url:
                feeds.append(feed)

        return cls(
            feeds=feeds,
            request_interval=config.get("request_interval", 2000),
            timeout=config.get("timeout", 15),
            use_proxy=config.get("use_proxy", False),
            proxy_url=config.get("proxy_url", ""),
            timezone=config.get("timezone", DEFAULT_TIMEZONE),
            freshness_enabled=freshness_enabled,
            default_max_age_days=default_max_age_days,
        )
