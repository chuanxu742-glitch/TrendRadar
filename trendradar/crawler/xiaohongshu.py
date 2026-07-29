# coding=utf-8
"""小红书固定关键词搜索数据源，直接复用 Spider_XHS PC API。"""

from __future__ import annotations

import random
import os
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple
from urllib.parse import quote, urlsplit


SPIDER_XHS_ROOT = (
    Path(__file__).resolve().parents[1] / "vendor" / "spider_xhs"
)
RISK_MESSAGE_MARKERS = (
    "验证",
    "验证码",
    "访问频繁",
    "风控",
    "captcha",
    "verify",
    "http 461",
    "http 471",
)
SESSION_MESSAGE_MARKERS = ("登录已过期", "无登录信息", "session expired")
_SPIDER_XHS_RUNTIME_LOCK = threading.RLock()


class XiaohongshuError(RuntimeError):
    """小红书抓取失败。"""


class XiaohongshuRiskControlError(XiaohongshuError):
    """服务端返回风控信号；调用方应停止本轮抓取。"""


class XiaohongshuSessionError(XiaohongshuError):
    """登录会话无效；必须重新登录后再运行。"""


@dataclass(frozen=True)
class XiaohongshuKeyword:
    """一个固定关键词数据源。"""

    source_id: str
    query: str
    name: str


def _ensure_spider_xhs_import_path() -> None:
    """让未修改的 Spider_XHS 绝对导入可从其固定快照加载。"""
    root = str(SPIDER_XHS_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)


@contextmanager
def spider_xhs_runtime() -> Iterator[None]:
    """保持上游以仓库根目录解析 static JS，再恢复 TrendRadar 工作目录。"""
    with _SPIDER_XHS_RUNTIME_LOCK:
        previous_cwd = Path.cwd()
        os.chdir(SPIDER_XHS_ROOT)
        try:
            yield
        finally:
            os.chdir(previous_cwd)


def load_spider_xhs_pc_api() -> Any:
    """延迟加载上游 XHS_Apis，避免未启用时初始化 Node.js 签名环境。"""
    _ensure_spider_xhs_import_path()
    try:
        from apis.xhs_pc_apis import XHS_Apis
    except ImportError as exc:  # pragma: no cover - dependency error path
        raise XiaohongshuError(
            "Spider_XHS 运行依赖缺失，请执行 `uv sync` 和 `npm install`"
        ) from exc
    return XHS_Apis()


def load_spider_xhs_login_api() -> Any:
    """延迟加载上游二维码/手机验证码登录模块。"""
    _ensure_spider_xhs_import_path()
    try:
        from apis.xhs_pc_login_apis import XHSLoginApi
    except ImportError as exc:  # pragma: no cover - dependency error path
        raise XiaohongshuError(
            "Spider_XHS 登录依赖缺失，请执行 `uv sync` 和 `npm install`"
        ) from exc
    return XHSLoginApi()


class XiaohongshuFetcher:
    """按配置串行调用 Spider_XHS 的 PC 关键词搜索接口。"""

    SORT_TYPES = {
        "general": 0,
        "latest": 1,
        "popular": 2,
        "comments": 3,
        "collects": 4,
    }
    NOTE_TYPES = {"all": 0, "video": 1, "image": 2}
    NOTE_TIMES = {"all": 0, "day": 1, "week": 2, "half_year": 3}

    def __init__(
        self,
        config: Dict[str, Any],
        *,
        api_client: Any = None,
        sleep_func=time.sleep,
    ) -> None:
        self.config = config
        self.cookie = str(config.get("COOKIE", "")).strip()
        self.limit = min(20, max(1, int(config.get("LIMIT_PER_KEYWORD", 20))))
        self.interval_min = max(0.0, float(config.get("INTERVAL_MIN_SECONDS", 15)))
        self.interval_max = max(
            self.interval_min, float(config.get("INTERVAL_MAX_SECONDS", 30))
        )
        self.sort = str(config.get("SORT", "latest"))
        self.note_type = str(config.get("NOTE_TYPE", "all"))
        self.note_time = str(config.get("NOTE_TIME", "day"))
        self.proxy_url = str(config.get("PROXY_URL", "")).strip()
        self.api = api_client if api_client is not None else load_spider_xhs_pc_api()
        self._sleep = sleep_func

    @classmethod
    def from_config(cls, config: Dict[str, Any], **kwargs: Any) -> "XiaohongshuFetcher":
        return cls(config, **kwargs)

    def fetch_all(self) -> Tuple[Dict, Dict, List]:
        """抓取全部固定关键词；遇到风控或会话失效立即停止。"""
        keywords = self._load_keywords()
        results: Dict[str, Dict] = {}
        id_to_name: Dict[str, str] = {}
        failed_ids: List[str] = []

        if not self.cookie:
            failed_ids.extend(keyword.source_id for keyword in keywords)
            print("[小红书] 未配置有效登录文件，跳过固定关键词抓取")
            return results, {k.source_id: k.name for k in keywords}, failed_ids

        for index, keyword in enumerate(keywords):
            id_to_name[keyword.source_id] = keyword.name
            try:
                items = self.search(keyword.query)
                results[keyword.source_id] = self._normalize_items(items)
                print(f"[小红书] {keyword.name}: 获取 {len(results[keyword.source_id])} 条")
            except (XiaohongshuRiskControlError, XiaohongshuSessionError) as exc:
                failed_ids.extend(item.source_id for item in keywords[index:])
                print(f"[小红书] 停止本轮抓取: {exc}")
                break
            except Exception as exc:
                failed_ids.append(keyword.source_id)
                print(f"[小红书] {keyword.name} 抓取失败: {exc}")

            if index < len(keywords) - 1:
                self._sleep(random.uniform(self.interval_min, self.interval_max))

        return results, id_to_name, failed_ids

    def search(self, query: str) -> List[Dict[str, Any]]:
        """调用 Spider_XHS.search_note，只抓取指定关键词第一页。"""
        proxies = None
        if self.proxy_url:
            proxies = {"http": self.proxy_url, "https": self.proxy_url}

        with spider_xhs_runtime():
            success, message, response = self.api.search_note(
                query=query,
                cookies_str=self.cookie,
                page=1,
                sort_type_choice=self.SORT_TYPES.get(self.sort, 1),
                note_type=self.NOTE_TYPES.get(self.note_type, 0),
                note_time=self.NOTE_TIMES.get(self.note_time, 1),
                proxies=proxies,
            )
        message = str(message or "")
        if not success:
            lowered = message.lower()
            if any(marker in lowered for marker in SESSION_MESSAGE_MARKERS):
                raise XiaohongshuSessionError(message or "小红书登录会话失效")
            if any(marker in lowered for marker in RISK_MESSAGE_MARKERS):
                raise XiaohongshuRiskControlError(message or "小红书请求被风控")
            raise XiaohongshuError(message or "Spider_XHS 搜索请求失败")

        if not isinstance(response, dict):
            raise XiaohongshuError("Spider_XHS 搜索响应为空")
        items = response.get("data", {}).get("items", [])
        if not isinstance(items, list):
            raise XiaohongshuError("Spider_XHS 搜索响应缺少 data.items")
        return items[: self.limit]

    def fetch_detail(self, url: str) -> Dict[str, Any]:
        """读取一条搜索结果的正文和公开业务字段。"""

        candidate = str(url or "").strip()
        try:
            parsed = urlsplit(candidate)
        except ValueError as exc:
            raise XiaohongshuError("小红书笔记地址无效") from exc
        hostname = (parsed.hostname or "").lower()
        if (
            parsed.scheme not in {"http", "https"}
            or (
                hostname != "xiaohongshu.com"
                and not hostname.endswith(".xiaohongshu.com")
            )
        ):
            raise XiaohongshuError("小红书笔记地址无效")

        proxies = None
        if self.proxy_url:
            proxies = {"http": self.proxy_url, "https": self.proxy_url}
        with spider_xhs_runtime():
            success, message, response = self.api.get_note_info(
                candidate,
                self.cookie,
                proxies=proxies,
            )
        message = str(message or "")
        if not success:
            lowered = message.lower()
            if any(marker in lowered for marker in SESSION_MESSAGE_MARKERS):
                raise XiaohongshuSessionError(message or "小红书登录会话失效")
            if any(marker in lowered for marker in RISK_MESSAGE_MARKERS):
                raise XiaohongshuRiskControlError(message or "小红书请求被风控")
            raise XiaohongshuError(message or "小红书笔记详情读取失败")

        if not isinstance(response, dict):
            raise XiaohongshuError("小红书笔记详情响应为空")
        items = response.get("data", {}).get("items", [])
        if not isinstance(items, list) or not items or not isinstance(items[0], dict):
            raise XiaohongshuError("小红书笔记详情缺少 data.items")
        note_card = items[0].get("note_card") or {}
        if not isinstance(note_card, dict):
            raise XiaohongshuError("小红书笔记详情缺少 note_card")
        user = note_card.get("user") or {}
        interact = note_card.get("interact_info") or {}
        if not isinstance(user, dict):
            user = {}
        if not isinstance(interact, dict):
            interact = {}
        return {
            "title": str(
                note_card.get("title") or note_card.get("display_title") or ""
            ).strip()[:300],
            "content": str(note_card.get("desc") or "").strip()[:8000],
            "author": str(
                user.get("nickname") or user.get("nick_name") or ""
            ).strip()[:100],
            "ip_location": str(note_card.get("ip_location") or "").strip()[:100],
            "liked_count": str(
                interact.get("liked_count") or interact.get("like_count") or ""
            ).strip()[:30],
            "collected_count": str(
                interact.get("collected_count")
                or interact.get("collect_count")
                or ""
            ).strip()[:30],
            "comment_count": str(interact.get("comment_count") or "").strip()[:30],
            "detail_status": "success",
        }

    def _load_keywords(self) -> List[XiaohongshuKeyword]:
        keywords: List[XiaohongshuKeyword] = []
        seen_ids = set()
        for raw in self.config.get("KEYWORDS", []):
            if not isinstance(raw, dict) or not raw.get("enabled", True):
                continue
            keyword_id = str(raw.get("id", "")).strip()
            query = str(raw.get("query", "")).strip()
            if not keyword_id or not query or keyword_id in seen_ids:
                continue
            seen_ids.add(keyword_id)
            keywords.append(
                XiaohongshuKeyword(
                    source_id=f"xhs-{keyword_id}",
                    query=query,
                    name=str(raw.get("name") or f"小红书·{query}"),
                )
            )
        return keywords

    @staticmethod
    def _normalize_items(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        normalized: Dict[str, Dict[str, Any]] = {}
        seen_note_ids = set()
        for rank, item in enumerate(items, 1):
            if not isinstance(item, dict):
                continue
            note_card = item.get("note_card") or {}
            note_id = str(
                item.get("id")
                or item.get("note_id")
                or note_card.get("note_id")
                or ""
            ).strip()
            if not note_id or note_id in seen_note_ids:
                continue
            seen_note_ids.add(note_id)

            title = str(
                note_card.get("display_title") or note_card.get("title") or ""
            ).strip()
            if not title:
                title = f"小红书笔记 {note_id[:8]}"
            storage_title = f"{title} [{note_id[:8]}]"
            xsec_token = str(
                item.get("xsec_token") or note_card.get("xsec_token") or ""
            ).strip()
            url = f"https://www.xiaohongshu.com/explore/{quote(note_id, safe='')}"
            if xsec_token:
                url += (
                    f"?xsec_token={quote(xsec_token, safe='')}"
                    "&xsec_source=pc_search"
                )

            normalized[storage_title] = {
                "ranks": [rank],
                "url": url,
                "mobileUrl": url,
            }
        return normalized
