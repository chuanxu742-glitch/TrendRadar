"""Read official-monitor digest artifacts without coupling to the monitor runtime."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional


def load_monitor_policy_digest(
    period: str = "daily",
    *,
    state_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    normalized = str(period or "daily").strip().lower()
    if normalized not in {"daily", "weekly", "monthly", "latest"}:
        normalized = "daily"
    root = state_dir or Path(os.getenv("MONITOR_STATE_DIR", "output/official-monitor"))
    path = root / "policy-digests" / f"{normalized}.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    return value if isinstance(value, dict) else {}
