from __future__ import annotations

import json
import os
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


STATE_DIR = Path(os.getenv("MONITOR_STATE_DIR", "/app/state"))
PORT = int(os.getenv("MONITOR_PORT", "8090"))
MAX_STALE_SECONDS = int(os.getenv("MONITOR_HEALTH_MAX_STALE_SECONDS", "1800"))


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def functional_status(payload: dict) -> str:
    health = payload.get("functional_health", {})
    if isinstance(health, str):
        return health
    if isinstance(health, dict):
        return str(health.get("status", ""))
    return ""


def main() -> int:
    try:
        for path in (
            "/health/live",
            "/health/ready",
            "/api/v1/policy-change-digest?period=daily",
            "/api/v1/site-url-inventory?limit=1",
        ):
            with urllib.request.urlopen(f"http://127.0.0.1:{PORT}{path}", timeout=5) as response:
                if response.status != 200:
                    return 1
    except Exception:
        return 1

    progress_path = STATE_DIR / "scan-progress.json"
    status_path = STATE_DIR / "status.json"
    heartbeat_path = progress_path if progress_path.exists() else status_path
    if not heartbeat_path.exists():
        return 1
    try:
        heartbeat = read_json(heartbeat_path)
        timestamp = heartbeat.get("last_progress_at") or heartbeat.get("generated_at")
        age = (datetime.now(timezone.utc) - parse_time(timestamp)).total_seconds()
        if age > MAX_STALE_SECONDS:
            return 1
        if status_path.exists() and functional_status(read_json(status_path)) == "unhealthy":
            return 1
        return 0
    except Exception:
        return 1


if __name__ == "__main__":
    sys.exit(main())
