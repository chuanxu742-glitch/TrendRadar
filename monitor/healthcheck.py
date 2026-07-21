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


def main() -> int:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{PORT}/", timeout=5) as response:
            if response.status != 200:
                return 1
    except Exception:
        return 1

    progress_path = STATE_DIR / "scan-progress.json"
    status_path = STATE_DIR / "status.json"
    target = progress_path if progress_path.exists() else status_path
    if not target.exists():
        return 1
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
        timestamp = payload.get("last_progress_at") or payload.get("generated_at")
        age = (datetime.now(timezone.utc) - parse_time(timestamp)).total_seconds()
        return 0 if age <= MAX_STALE_SECONDS else 1
    except Exception:
        return 1


if __name__ == "__main__":
    sys.exit(main())
