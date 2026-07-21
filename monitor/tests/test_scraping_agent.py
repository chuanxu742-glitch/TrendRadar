from __future__ import annotations

import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from monitor.scraping_agent import AgentLoop, AgentStateStore, LoopLimits, StepResult


class AgentLoopTests(unittest.TestCase):
    def test_state_store_preserves_concurrent_attempts_and_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)

            def run(index: int) -> None:
                loop = AgentLoop(AgentStateStore(directory))
                loop.run(
                    f"concurrent:{index}", f"site-{index}.test/page",
                    ("dynamic",),
                    lambda strategy, _: StepResult("success", strategy),
                )

            with ThreadPoolExecutor(max_workers=8) as executor:
                list(executor.map(run, range(50)))
            status = json.loads((directory / "status.json").read_text(encoding="utf-8"))
            profiles = json.loads((directory / "site-profiles.json").read_text(encoding="utf-8"))

        self.assertEqual(status["attempts"], 50)
        self.assertEqual(len(profiles), 50)

    def test_plain_static_success_does_not_create_adapter_profile(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            loop = AgentLoop(AgentStateStore(directory))
            for index in range(25):
                loop.run(
                    f"static:{index}", f"site-{index}.test/page", ("static", "dynamic", "stealth"),
                    lambda strategy, _: StepResult("success", strategy),
                )
            status = json.loads((directory / "status.json").read_text(encoding="utf-8"))
            profiles_exist = (directory / "site-profiles.json").exists()
            runs_exist = (directory / "runs.jsonl").exists()
        self.assertFalse(profiles_exist)
        self.assertFalse(runs_exist)
        self.assertEqual(status["attempts"], 25)

    def test_candidate_requires_two_successes_before_promotion(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = AgentStateStore(Path(temporary))
            loop = AgentLoop(store, LoopLimits(max_attempts=3))
            called: list[str] = []

            def execute(strategy: str, _: int) -> StepResult:
                called.append(strategy)
                if strategy == "static":
                    return StepResult(
                        "retry", strategy, failure_kind="javascript_shell",
                        suggested_strategies=("dynamic",),
                    )
                return StepResult("success", strategy, output="complete page")

            result = loop.run("page:one", "air.test", ("static", "dynamic", "stealth"), execute)
            first_profiles = json.loads((Path(temporary) / "site-profiles.json").read_text(encoding="utf-8"))
            second = loop.run("page:one", "air.test", ("static", "dynamic", "stealth"), execute)
            profiles = json.loads((Path(temporary) / "site-profiles.json").read_text(encoding="utf-8"))

        self.assertEqual(result.status, "success")
        self.assertEqual(second.status, "success")
        self.assertEqual(called, ["static", "dynamic", "static", "dynamic"])
        self.assertEqual(first_profiles["air.test"]["candidate"]["strategy"], "dynamic")
        self.assertEqual(profiles["air.test"]["preferred_strategy"], "dynamic")

    def test_active_strategy_rolls_back_after_two_failures(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "site-profiles.json").write_text(
                json.dumps({
                    "air.test": {
                        "active_strategy": "dynamic", "preferred_strategy": "dynamic",
                        "version": 2, "confidence": 0.8,
                        "history": [{"strategy": "static", "version": 1}],
                    }
                }), encoding="utf-8"
            )
            loop = AgentLoop(AgentStateStore(directory), LoopLimits(max_attempts=1))
            for task_id in ("failure:one", "failure:two"):
                loop.run(
                    task_id, "air.test", ("static", "dynamic", "stealth"),
                    lambda strategy, _: StepResult(
                        "terminal", strategy, failure_kind="content_regression", detail="validation failed"
                    ),
                )
            profile = json.loads((directory / "site-profiles.json").read_text(encoding="utf-8"))["air.test"]
        self.assertEqual(profile["preferred_strategy"], "static")
        self.assertEqual(profile["status"], "rolled_back")

    def test_learned_strategy_is_used_first(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "site-profiles.json").write_text(
                json.dumps({"air.test": {"preferred_strategy": "stealth"}}), encoding="utf-8"
            )
            called: list[str] = []
            loop = AgentLoop(AgentStateStore(directory))
            result = loop.run(
                "page:two", "air.test", ("static", "dynamic", "stealth"),
                lambda strategy, _: called.append(strategy) or StepResult("success", strategy),
            )
        self.assertEqual(result.status, "success")
        self.assertEqual(called, ["stealth"])

    def test_exhausted_strategies_enter_manual_queue(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            loop = AgentLoop(AgentStateStore(directory), LoopLimits(max_attempts=2))
            result = loop.run(
                "page:three", "air.test", ("static", "dynamic"),
                lambda strategy, _: StepResult(
                    "retry", strategy, failure_kind="timeout", detail="request timed out"
                ),
            )
            queue = json.loads((directory / "manual-queue.json").read_text(encoding="utf-8"))
        self.assertEqual(result.status, "blocked")
        self.assertEqual(len(queue), 1)
        self.assertEqual(len(queue[0]["attempts"]), 2)

    def test_manual_queue_deduplicates_same_domain_and_failure_family(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for index, path in enumerate(("robots.txt", "sitemap.xml")):
                loop = AgentLoop(AgentStateStore(directory), LoopLimits(max_attempts=1))
                loop.run(
                    f"page:{index}", f"air.test/{path}", ("static",),
                    lambda strategy, _: StepResult(
                        "blocked", strategy, failure_kind="human_verification",
                        detail="human verification checkpoint requires an authorized handler",
                    ),
                )
            queue = json.loads((directory / "manual-queue.json").read_text(encoding="utf-8"))
        self.assertEqual(len(queue), 1)
        self.assertEqual(queue[0]["occurrences"], 2)
        self.assertEqual(len(queue[0]["task_ids"]), 2)

    def test_journal_redacts_key_like_values(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            loop = AgentLoop(AgentStateStore(directory), LoopLimits(max_attempts=1))
            loop.run(
                "page:four", "air.test", ("static",),
                lambda strategy, _: StepResult(
                    "terminal", strategy, failure_kind="auth", detail="bad sk-secretValue123"
                ),
            )
            journal = (directory / "runs.jsonl").read_text(encoding="utf-8")
        self.assertNotIn("secretValue123", journal)
        self.assertIn("[REDACTED]", journal)


if __name__ == "__main__":
    unittest.main()
