from __future__ import annotations

import hashlib
import json
import os
import random
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qsl, urlsplit

import yaml

from xhs_monitor.fetcher import (
    XiaohongshuFetcher,
    XiaohongshuRiskControlError,
    XiaohongshuSessionError,
)
from xhs_monitor.intelligence import analyze_notes
from xhs_monitor.settings import (
    load_managed_keywords,
    save_managed_keywords,
)
from xhs_monitor.store import XiaohongshuStore, now_iso
from xhs_monitor.web_login import LoginSessionManager


@dataclass(frozen=True)
class Settings:
    enabled: bool
    port: int
    interval_seconds: int
    immediate_run: bool
    database_path: Path
    keywords: list[dict[str, Any]]
    fetcher_config: dict[str, Any]
    settings_path: Path = Path("/app/state/settings.json")
    cookie_file: Path = Path("/app/state/xhs_cookie.txt")


def _enabled(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _cookie_path(config: dict[str, Any], config_path: Path) -> Path:
    configured_file = str(
        os.getenv("XHS_COOKIE_FILE", "") or config.get("cookie_file") or ""
    ).strip()
    path = Path(configured_file or "/app/state/xhs_cookie.txt")
    if not path.is_absolute():
        path = (config_path.parent.parent / path).resolve()
    return path


def _read_cookie(cookie_path: Path) -> str:
    try:
        cookie = cookie_path.read_text(encoding="utf-8").strip()
        if cookie:
            return cookie
    except OSError:
        pass
    return str(os.getenv("XHS_COOKIE", "")).strip()


def load_settings(path: Path | None = None) -> Settings:
    config_path = path or Path(
        os.getenv("XHS_MONITOR_CONFIG", "/app/config/xhs-monitor.yaml")
    )
    with config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    config = payload.get("xiaohongshu") or {}
    if not isinstance(config, dict):
        raise ValueError("xiaohongshu config must be a mapping")
    intervals = config.get("request_interval_seconds") or {}
    if not isinstance(intervals, dict):
        intervals = {}
    keywords = config.get("keywords") or []
    if not isinstance(keywords, list):
        raise ValueError("xiaohongshu.keywords must be a list")
    settings_path = Path(
        os.getenv("XHS_MONITOR_SETTINGS", "/app/state/settings.json")
    )
    managed_keywords = load_managed_keywords(settings_path)
    if managed_keywords is not None:
        keywords = managed_keywords
    cookie_file = _cookie_path(config, config_path)

    return Settings(
        enabled=_enabled(
            os.getenv("XHS_ENABLED"),
            default=_enabled(config.get("enabled"), True),
        ),
        port=max(int(os.getenv("XHS_MONITOR_PORT", "8091")), 1),
        interval_seconds=max(int(os.getenv("XHS_MONITOR_INTERVAL", "1800")), 60),
        immediate_run=_enabled(os.getenv("XHS_IMMEDIATE_RUN"), True),
        database_path=Path(
            os.getenv("XHS_MONITOR_DATABASE", "/app/state/xhs-monitor.db")
        ),
        keywords=keywords,
        fetcher_config={
            "COOKIE": _read_cookie(cookie_file),
            "LIMIT_PER_KEYWORD": config.get("limit_per_keyword", 20),
            "INTERVAL_MIN_SECONDS": intervals.get("min", 15),
            "INTERVAL_MAX_SECONDS": intervals.get("max", 30),
            "SORT": config.get("sort", "latest"),
            "NOTE_TYPE": config.get("note_type", "all"),
            "NOTE_TIME": config.get("note_time", "day"),
            "PROXY_URL": str(os.getenv("XHS_PROXY_URL", "")).strip(),
            "DETAIL_LIMIT_PER_KEYWORD": config.get("detail_limit_per_keyword", 5),
            "DETAIL_INTERVAL_MIN_SECONDS": intervals.get("detail_min", 3),
            "DETAIL_INTERVAL_MAX_SECONDS": intervals.get("detail_max", 6),
            "KEYWORDS": keywords,
        },
        settings_path=settings_path,
        cookie_file=cookie_file,
    )


def collect_once(
    settings: Settings,
    store: XiaohongshuStore,
    *,
    fetcher_factory: Callable[[dict[str, Any]], Any] = XiaohongshuFetcher.from_config,
    analyzer: Callable[[list[dict[str, Any]]], dict[str, dict[str, Any]]] = analyze_notes,
    sleep_func: Callable[[float], None] = time.sleep,
) -> dict[str, int | str]:
    started_at = now_iso()
    keywords = store.sync_keywords(settings.keywords)
    configured_ids = [item["source_id"] for item in keywords]
    if not settings.enabled:
        return store.record_run(
            started_at=started_at,
            keywords=keywords,
            results={},
            failed_ids=configured_ids,
        )
    fetcher = None
    try:
        fetcher = fetcher_factory(settings.fetcher_config)
        results, _names, failed_ids = fetcher.fetch_all()
    except XiaohongshuRiskControlError as exc:
        # 风控信号：单独输出可辨识提示，行为保持不变（全部标记失败）
        print(
            "[xhs-monitor] collection blocked by risk control: "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )
        results = {}
        failed_ids = configured_ids
    except XiaohongshuSessionError as exc:
        # 会话失效：需要重新扫码登录
        print(
            "[xhs-monitor] collection failed, session invalid (re-login required): "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )
        results = {}
        failed_ids = configured_ids
    except Exception as exc:
        # 其他异常：输出 repr 以便区分网络、解析等问题
        print(f"[xhs-monitor] collection failed: {exc!r}", flush=True)
        results = {}
        failed_ids = configured_ids
    result = store.record_run(
        started_at=started_at,
        keywords=keywords,
        results=results,
        failed_ids=failed_ids,
    )
    if fetcher is None:
        return result

    try:
        detail_limit = min(
            max(int(settings.fetcher_config.get("DETAIL_LIMIT_PER_KEYWORD", 5)), 1),
            20,
        )
    except (TypeError, ValueError):
        detail_limit = 5
    candidates = store.detail_candidates(limit_per_source=detail_limit)
    reanalysis_candidates = store.analysis_candidates(limit=10)

    try:
        interval_min = max(
            float(settings.fetcher_config.get("DETAIL_INTERVAL_MIN_SECONDS", 0)),
            0.0,
        )
        interval_max = max(
            float(settings.fetcher_config.get("DETAIL_INTERVAL_MAX_SECONDS", 0)),
            interval_min,
        )
    except (TypeError, ValueError):
        interval_min, interval_max = 0.0, 0.0

    detailed: list[dict[str, Any]] = []
    fetch_detail = getattr(fetcher, "fetch_detail", None)
    for index, candidate in enumerate(candidates):
        try:
            if callable(fetch_detail):
                detail = fetch_detail(candidate["url"])
            else:
                detail = {
                    "title": candidate["title"],
                    "content": candidate["title"],
                    "detail_status": "success",
                }
            detailed.append({**candidate, **detail})
        except (XiaohongshuRiskControlError, XiaohongshuSessionError) as exc:
            # 风控或会话失效：中止本轮详情抓取
            reason = (
                "risk control"
                if isinstance(exc, XiaohongshuRiskControlError)
                else "session invalid"
            )
            print(
                f"[xhs-monitor] detail fetch aborted ({reason}): "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )
            store.update_note_intelligence(
                candidate["note_key"],
                detail={"detail_status": "failed"},
            )
            break
        except Exception as exc:
            print(
                "[xhs-monitor] detail fetch failed for "
                f"{candidate['note_key']}: {exc!r}",
                flush=True,
            )
            store.update_note_intelligence(
                candidate["note_key"],
                detail={"detail_status": "failed"},
            )
        if index < len(candidates) - 1 and interval_max > 0:
            sleep_func(random.uniform(interval_min, interval_max))

    analyses = analyzer(detailed) if detailed else {}
    for item in detailed:
        store.update_note_intelligence(
            item["note_key"],
            detail=item,
            analysis=analyses.get(item["note_key"]),
        )
    result["enriched_items"] = len(detailed)
    result["relevant_items"] = sum(
        1
        for item in detailed
        if analyses.get(item["note_key"], {}).get("relevant") is True
    )
    reanalyses = analyzer(reanalysis_candidates) if reanalysis_candidates else {}
    for item in reanalysis_candidates:
        store.update_note_intelligence(
            item["note_key"],
            detail=item,
            analysis=reanalyses.get(item["note_key"]),
        )
    result["reanalyzed_items"] = len(reanalysis_candidates)
    return result


def build_handler(
    store: XiaohongshuStore,
    *,
    settings_loader: Callable[[], Settings] = load_settings,
    login_manager: LoginSessionManager | None = None,
) -> type[BaseHTTPRequestHandler]:
    class RequestHandler(BaseHTTPRequestHandler):
        server_version = "TrendRadarXhsMonitor/1.0"

        def send_json(self, payload: Any, status: int = 200) -> None:
            content = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(content)

        def require_local_json_write(self) -> None:
            # 与 monitor/http_api.py 的 _require_local_json_write 语义一致：
            # 写接口必须是 JSON 请求；带 Origin 时仅允许本机来源或与 Host 一致；
            # 无 Origin 的请求放行（official-monitor 容器代理调用不带 Origin）。
            content_type = str(self.headers.get("Content-Type") or "").lower()
            if not content_type.startswith("application/json"):
                raise ValueError("Content-Type must be application/json")
            origin = str(self.headers.get("Origin") or "").strip()
            if not origin:
                return
            origin_host = (urlsplit(origin).hostname or "").lower()
            request_host = str(self.headers.get("Host") or "").split(":", 1)[0].lower()
            if origin_host not in {"localhost", "127.0.0.1", "::1", request_host}:
                raise ValueError("cross-origin source changes are not allowed")

        def read_json_body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length", "0") or 0)
            if length < 0 or length > 64 * 1024:
                raise ValueError("请求内容过大")
            payload = json.loads(self.rfile.read(length) or b"{}")
            if not isinstance(payload, dict):
                raise ValueError("请求内容必须是对象")
            return payload

        def settings_payload(self) -> dict[str, Any]:
            settings = settings_loader()
            return {
                "keywords": [
                    {
                        "id": str(item.get("id") or ""),
                        "query": str(item.get("query") or ""),
                        "name": str(item.get("name") or ""),
                        "enabled": bool(item.get("enabled", True)),
                    }
                    for item in settings.keywords
                    if isinstance(item, dict)
                ],
                "interval_minutes": max(settings.interval_seconds // 60, 1),
                "credential_saved": bool(
                    str(settings.fetcher_config.get("COOKIE") or "")
                ),
            }

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlsplit(self.path)
            if parsed.path in {"/health/live", "/health/ready"}:
                self.send_json({"status": "ok"})
                return
            if parsed.path == "/api/v1/summary":
                try:
                    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
                    limit = min(max(int(query.get("limit", "100") or 100), 1), 500)
                    self.send_json(store.summary(limit=limit))
                except (TypeError, ValueError) as exc:
                    self.send_json({"error": str(exc)}, 400)
                return
            if parsed.path == "/api/v1/settings":
                self.send_json(self.settings_payload())
                return
            if parsed.path == "/api/v1/login/status":
                if login_manager is None:
                    self.send_json(
                        {
                            "status": "unavailable",
                            "status_label": "登录服务暂时不可用",
                            "qr_image": "",
                            "expires_at": "",
                        },
                        503,
                    )
                else:
                    self.send_json(login_manager.status())
                return
            self.send_json({"error": "not found"}, 404)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlsplit(self.path)
            try:
                if parsed.path == "/api/v1/settings":
                    self.require_local_json_write()
                    payload = self.read_json_body()
                    keywords = payload.get("keywords")
                    if not isinstance(keywords, list):
                        raise ValueError("keywords 必须是列表")
                    settings = settings_loader()
                    normalized = save_managed_keywords(
                        settings.settings_path,
                        keywords,
                    )
                    store.sync_keywords(normalized)
                    self.send_json(self.settings_payload())
                    return
                if parsed.path == "/api/v1/login/start":
                    self.require_local_json_write()
                    self.read_json_body()
                    if login_manager is None:
                        self.send_json(
                            {"error": "登录服务暂时不可用"},
                            503,
                        )
                    else:
                        self.send_json(login_manager.start())
                    return
                self.send_json({"error": "not found"}, 404)
            except (json.JSONDecodeError, TypeError, ValueError) as exc:
                self.send_json({"error": str(exc)}, 400)

        def log_message(self, _format: str, *_args: Any) -> None:
            return

    return RequestHandler


def _runtime_signature(settings: Settings) -> str:
    signature_payload = {
        "cookie": str(settings.fetcher_config.get("COOKIE") or ""),
        "keywords": settings.keywords,
        "enabled": settings.enabled,
        "interval_seconds": settings.interval_seconds,
    }
    return hashlib.sha256(
        json.dumps(
            signature_payload,
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def collection_worker(
    settings: Settings,
    store: XiaohongshuStore,
    *,
    settings_loader: Callable[[], Settings] = load_settings,
    collect_func: Callable[[Settings, XiaohongshuStore], dict[str, int | str]] = collect_once,
    monotonic_func: Callable[[], float] = time.monotonic,
    sleep_func: Callable[[float], None] = time.sleep,
) -> None:
    current_settings = settings
    last_signature = _runtime_signature(settings)
    next_run = (
        monotonic_func()
        if settings.immediate_run
        else monotonic_func() + settings.interval_seconds
    )
    while True:
        refreshed_settings = settings_loader()
        refreshed_signature = _runtime_signature(refreshed_settings)
        runtime_changed = refreshed_signature != last_signature
        if runtime_changed:
            current_settings = refreshed_settings
            last_signature = refreshed_signature
            next_run = monotonic_func()
        if monotonic_func() >= next_run:
            result = collect_func(current_settings, store)
            print(
                "[xhs-monitor] collection complete; "
                f"status={result['status']} sources={result['source_count']} "
                f"items={result['item_count']} "
                f"enriched={result.get('enriched_items', 0)} "
                f"relevant={result.get('relevant_items', 0)} "
                f"reanalyzed={result.get('reanalyzed_items', 0)}",
                flush=True,
            )
            next_run = monotonic_func() + current_settings.interval_seconds
        sleep_func(min(max(next_run - monotonic_func(), 1), 10))


def main() -> None:
    settings = load_settings()
    store = XiaohongshuStore(settings.database_path)
    store.sync_keywords(settings.keywords)
    login_manager = LoginSessionManager(settings.cookie_file)
    threading.Thread(
        target=collection_worker,
        args=(settings, store),
        daemon=True,
    ).start()
    # 默认仅监听本机；容器部署由 compose 显式设置 XHS_BIND_HOST=0.0.0.0
    bind_host = str(os.getenv("XHS_BIND_HOST", "")).strip() or "127.0.0.1"
    server = ThreadingHTTPServer(
        (bind_host, settings.port),
        build_handler(store, login_manager=login_manager),
    )
    print(
        f"[xhs-monitor] summary API listening on {bind_host}:{settings.port}",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
