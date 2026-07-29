from __future__ import annotations

import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from monitor import official_monitor
from monitor.monitor_store import (
    ChangeCandidate,
    ContentSnapshot,
    MonitorStore,
    SourceEndpoint,
)


def snapshot_stamp(age_days: int) -> str:
    moment = datetime.now(timezone.utc) - timedelta(days=age_days)
    return moment.strftime("%Y%m%dT%H%M%S%z")


class SnapshotRetentionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.database_path = self.root / "monitor.db"
        self.store = MonitorStore(self.database_path)
        self.patches = [
            mock.patch.object(official_monitor, "STATE_DIR", self.root),
            mock.patch.object(
                official_monitor,
                "STATE_JOURNAL_PATH",
                self.root / "state-journal.jsonl",
            ),
            mock.patch.object(official_monitor, "monitor_store", return_value=self.store),
            mock.patch.dict("os.environ", {"MONITOR_DATABASE": str(self.database_path)}),
        ]
        for patch in self.patches:
            patch.start()

    def tearDown(self) -> None:
        for patch in reversed(self.patches):
            patch.stop()
        self.store.close()
        self.temporary.cleanup()

    def make_snapshot_dir(self, source_id: str, age_days: int, digest: str) -> Path:
        folder = (
            self.root / "snapshots" / source_id / f"{snapshot_stamp(age_days)}-{digest}"
        )
        folder.mkdir(parents=True)
        (folder / "content.md").write_text("policy text", encoding="utf-8")
        return folder

    def run_retention(self, retention_days: int | None = None) -> dict[str, int]:
        with redirect_stdout(io.StringIO()):
            return official_monitor.enforce_snapshot_retention(retention_days)

    def test_retention_keeps_referenced_and_recent_snapshots(self) -> None:
        self.store.upsert_source(
            SourceEndpoint(
                id="s1",
                canonical_url="https://example.gov/pets",
                role="current-primary",
                lifecycle_state="active",
            )
        )
        state_ref = self.make_snapshot_dir("s1", 100, "aaaaaaaaaaaa")
        evidence_ref = self.make_snapshot_dir("s1", 90, "bbbbbbbbbbbb")
        stale = self.make_snapshot_dir("s1", 80, "cccccccccccc")
        fresh = self.make_snapshot_dir("s1", 5, "dddddddddddd")
        latest = self.make_snapshot_dir("s1", 1, "eeeeeeeeeeee")
        # state 记录引用 100 天前的快照
        (self.root / "state.json").write_text(
            json.dumps(
                {"s1": {"snapshot_path": state_ref.relative_to(self.root).as_posix()}}
            ),
            encoding="utf-8",
        )
        # 数据库证据链引用 90 天前的快照
        self.store.record_snapshot(
            ContentSnapshot(
                id="snap-evidence",
                source_id="s1",
                captured_at="2026-04-30T00:00:00+00:00",
                content_sha256="0" * 64,
                normalized_path=(
                    evidence_ref / "content.md"
                ).relative_to(self.root).as_posix(),
            )
        )
        self.store.upsert_change_candidate(
            ChangeCandidate(
                id="candidate-1",
                source_id="s1",
                detected_at="2026-05-01T00:00:00+00:00",
                state="confirmed",
                old_snapshot_id="snap-evidence",
            )
        )

        summary = self.run_retention()

        self.assertFalse(stale.exists())
        self.assertTrue(state_ref.exists())
        self.assertTrue(evidence_ref.exists())
        self.assertTrue(fresh.exists())
        self.assertTrue(latest.exists())
        self.assertEqual(summary["removed_dirs"], 1)
        self.assertEqual(summary["kept_referenced"], 2)
        self.assertGreater(summary["freed_bytes"], 0)

    def test_latest_two_snapshots_survive_even_when_stale(self) -> None:
        oldest = self.make_snapshot_dir("s2", 120, "aaaaaaaaaaaa")
        middle = self.make_snapshot_dir("s2", 110, "bbbbbbbbbbbb")
        newest = self.make_snapshot_dir("s2", 100, "cccccccccccc")

        summary = self.run_retention()

        self.assertFalse(oldest.exists())
        self.assertTrue(middle.exists())
        self.assertTrue(newest.exists())
        self.assertEqual(summary["removed_dirs"], 1)

    def test_recent_snapshots_survive_custom_retention_window(self) -> None:
        old = self.make_snapshot_dir("s2", 10, "aaaaaaaaaaaa")
        for index, digest in enumerate(("bbbbbbbbbbbb", "cccccccccccc")):
            self.make_snapshot_dir("s2", 2 - index, digest)

        summary = self.run_retention(retention_days=7)

        self.assertFalse(old.exists())
        self.assertEqual(summary["removed_dirs"], 1)

    def test_empty_source_directories_are_cleaned_up(self) -> None:
        empty_dir = self.root / "snapshots" / "s3"
        empty_dir.mkdir(parents=True)

        self.run_retention()

        self.assertFalse(empty_dir.exists())

    def test_database_failure_aborts_cleanup_without_deleting(self) -> None:
        stale = self.make_snapshot_dir("s4", 120, "aaaaaaaaaaaa")
        self.make_snapshot_dir("s4", 110, "bbbbbbbbbbbb")
        self.make_snapshot_dir("s4", 100, "cccccccccccc")

        with mock.patch.object(
            official_monitor,
            "monitor_store",
            side_effect=sqlite3.OperationalError("database is locked"),
        ):
            with self.assertRaises(sqlite3.OperationalError):
                self.run_retention()

        self.assertTrue(stale.exists())


if __name__ == "__main__":
    unittest.main()
