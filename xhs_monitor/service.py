from __future__ import annotations

import json
import importlib.util
import os
import sys
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qsl, urlsplit

import yaml

from xhs_monitor.store import XiaohongshuStore, now_iso


def _load_fetcher_class() -> type:
    """Load the focused fetcher without importing the full TrendRadar application."""

    module_name = "_trendradar_xiaohongshu_fetcher"
    module_path = (
        Path(__file__).resolve().parents[1]
        / "trendradar"
        / "crawler"
        / "xiaohongshu.py"
    )
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load Xiaohongshu fetcher from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module.XiaohongshuFetcher


XiaohongshuFetcher = _load_fetcher_class()


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
    cookie = str(os.getenv("XHS_COOKIE", "")).strip()
    if cookie:
        return cookie
    configured_file = str(
        os.getenv("XHS_COOKIE_FILE", "") or config.get("cookie_file") or ""
    ).strip()
    if not configured_file:
        return ""
    path = Path(configured_file)
    if not path.is_absolute():
        path = (config_path.parent.parent / path).resolve()
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


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


def collection_worker(settings: Settings, store: XiaohongshuStore) -> None:
    if not settings.immediate_run:
        time.sleep(settings.interval_seconds)
    while True:
        started = time.monotonic()
        result = collect_once(settings, store)
        print(
            "[xhs-monitor] collection complete; "
            f"status={result['status']} sources={result['source_count']} "
            f"items={result['item_count']}",
            flush=True,
        )
        elapsed = time.monotonic() - started
        time.sleep(max(settings.interval_seconds - elapsed, 0))


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
