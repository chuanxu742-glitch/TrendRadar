from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qsl, urlsplit

import yaml

from xhs_monitor.fetcher import XiaohongshuFetcher
from xhs_monitor.store import XiaohongshuStore, now_iso


@dataclass(frozen=True)
class Settings:
    enabled: bool
    port: int
    interval_seconds: int
    immediate_run: bool
    database_path: Path
    keywords: list[dict[str, Any]]
    fetcher_config: dict[str, Any]


def _enabled(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _read_cookie(config: dict[str, Any], config_path: Path) -> str:
    configured_file = str(
        os.getenv("XHS_COOKIE_FILE", "") or config.get("cookie_file") or ""
    ).strip()
    if configured_file:
        path = Path(configured_file)
        if not path.is_absolute():
            path = (config_path.parent.parent / path).resolve()
        try:
            cookie = path.read_text(encoding="utf-8").strip()
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
            "COOKIE": _read_cookie(config, config_path),
            "LIMIT_PER_KEYWORD": config.get("limit_per_keyword", 20),
            "INTERVAL_MIN_SECONDS": intervals.get("min", 15),
            "INTERVAL_MAX_SECONDS": intervals.get("max", 30),
            "SORT": config.get("sort", "latest"),
            "NOTE_TYPE": config.get("note_type", "all"),
            "NOTE_TIME": config.get("note_time", "day"),
            "PROXY_URL": str(os.getenv("XHS_PROXY_URL", "")).strip(),
            "KEYWORDS": keywords,
        },
    )


def collect_once(
    settings: Settings,
    store: XiaohongshuStore,
    *,
    fetcher_factory: Callable[[dict[str, Any]], Any] = XiaohongshuFetcher.from_config,
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
    try:
        fetcher = fetcher_factory(settings.fetcher_config)
        results, _names, failed_ids = fetcher.fetch_all()
    except Exception:
        results = {}
        failed_ids = configured_ids
    return store.record_run(
        started_at=started_at,
        keywords=keywords,
        results=results,
        failed_ids=failed_ids,
    )


def build_handler(store: XiaohongshuStore) -> type[BaseHTTPRequestHandler]:
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
            self.send_json({"error": "not found"}, 404)

        def log_message(self, _format: str, *_args: Any) -> None:
            return

    return RequestHandler


def _credential_signature(settings: Settings) -> str:
    cookie = str(settings.fetcher_config.get("COOKIE") or "")
    return hashlib.sha256(cookie.encode("utf-8")).hexdigest()


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
    last_signature = _credential_signature(settings)
    next_run = (
        monotonic_func()
        if settings.immediate_run
        else monotonic_func() + settings.interval_seconds
    )
    while True:
        refreshed_settings = settings_loader()
        refreshed_signature = _credential_signature(refreshed_settings)
        credential_changed = refreshed_signature != last_signature
        if credential_changed:
            current_settings = refreshed_settings
            last_signature = refreshed_signature
            next_run = monotonic_func()
        if monotonic_func() >= next_run:
            result = collect_func(current_settings, store)
            print(
                "[xhs-monitor] collection complete; "
                f"status={result['status']} sources={result['source_count']} "
                f"items={result['item_count']}",
                flush=True,
            )
            next_run = monotonic_func() + current_settings.interval_seconds
        sleep_func(min(max(next_run - monotonic_func(), 1), 10))


def main() -> None:
    settings = load_settings()
    store = XiaohongshuStore(settings.database_path)
    store.sync_keywords(settings.keywords)
    threading.Thread(
        target=collection_worker,
        args=(settings, store),
        daemon=True,
    ).start()
    server = ThreadingHTTPServer(("0.0.0.0", settings.port), build_handler(store))
    print(f"[xhs-monitor] summary API listening on {settings.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
