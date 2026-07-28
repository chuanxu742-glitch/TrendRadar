from __future__ import annotations

import argparse
import json
from pathlib import Path

try:
    from . import official_monitor
except ImportError:
    import official_monitor


def repair(state_path: Path, force_restricted_retry: bool) -> dict[str, int]:
    state = official_monitor.load_state_with_journal(state_path)
    migrated = 0
    forced = 0
    repaired: dict[str, dict] = {}
    for source_id, raw in state.items():
        record = official_monitor.migrate_failure_record(raw)
        if record != raw:
            migrated += 1
        if (
            force_restricted_retry
            and record.get("status") == "error"
            and official_monitor.failure_scope(record) == "unverified"
            and record.get("failure_category") in {"访问受限", "请求限流"}
        ):
            record["checked_at"] = ""
            record["lifecycle_retry_forced"] = True
            forced += 1
        repaired[source_id] = record
    backup = state_path.with_name(state_path.stem + ".before-lifecycle-repair.json")
    if not backup.exists():
        official_monitor.save_json(backup, state)
    official_monitor.save_json(state_path, repaired)
    return {"records": len(repaired), "migrated": migrated, "forced_retries": forced}


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate source lifecycle state and schedule restricted retries")
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--force-restricted-retry", action="store_true")
    args = parser.parse_args()
    result = repair(args.state, args.force_restricted_retry)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
