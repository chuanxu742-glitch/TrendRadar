from __future__ import annotations

import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

from monitor.scraping_agent import AgentLoop, AgentStateStore, LoopLimits, StepResult
from monitor.scrapling_fetch import (
    BrowserFetchBudget,
    BrowserFetchError,
    FetchResult,
    ScraplingAdaptiveFetcher,
)


class FetchAgentIntegrityTests(unittest.TestCase):
    @staticmethod
    def page(status: int, body: bytes, content_type: str = "text/html") -> mock.Mock:
        return mock.Mock(
            status=status,
            url="https://air.test/travel/pets",
            headers={"Content-Type": content_type},
            body=body,
        )

    @staticmethod
    def complete(mode: str = "static") -> FetchResult:
        return FetchResult(
            200,
            "https://air.test/travel/pets",
            {"Content-Type": "text/html"},
            b"<html><main>" + b"pet transport policy rule " * 40 + b"</main></html>",
            mode,
        )

    def test_404_is_terminal_and_cannot_clear_existing_pause(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state_dir = Path(temporary)
            fetcher = ScraplingAdaptiveFetcher(agent_state_dir=state_dir)
            responses = [
                self.page(401, b"<html>authentication required</html>"),
                self.page(404, b"<html>not found</html>"),
            ]
            with (
                mock.patch("monitor.scrapling_fetch.Fetcher.get", side_effect=responses),
                mock.patch.object(fetcher, "_browser_fetch") as browser_fetch,
            ):
                self.assertEqual(fetcher.fetch("https://air.test/travel/pets").status_code, 401)
                self.assertEqual(fetcher.fetch("https://air.test/travel/pets").status_code, 404)

            queue = json.loads((state_dir / "manual-queue.json").read_text(encoding="utf-8"))
            status = json.loads((state_dir / "status.json").read_text(encoding="utf-8"))

        browser_fetch.assert_not_called()
        self.assertEqual(len(queue), 1)
        self.assertEqual(status["status_counts"]["blocked"], 1)
        self.assertEqual(status["status_counts"]["terminal"], 1)
        self.assertNotIn("success", status["status_counts"])

    def test_304_is_not_success_and_does_not_clear_pause(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state_dir = Path(temporary)
            fetcher = ScraplingAdaptiveFetcher(agent_state_dir=state_dir)
            with mock.patch(
                "monitor.scrapling_fetch.Fetcher.get",
                side_effect=[
                    self.page(401, b"<html>authentication required</html>"),
                    self.page(304, b""),
                ],
            ):
                fetcher.fetch("https://air.test/travel/pets")
                response = fetcher.fetch("https://air.test/travel/pets")
            queue = json.loads((state_dir / "manual-queue.json").read_text(encoding="utf-8"))
            status = json.loads((state_dir / "status.json").read_text(encoding="utf-8"))

        self.assertEqual(response.status_code, 304)
        self.assertEqual(len(queue), 1)
        self.assertEqual(status["status_counts"]["not_modified"], 1)
        self.assertNotIn("success", status["status_counts"])

    def test_429_retries_same_transport_within_agent_budget(self) -> None:
        fetcher = ScraplingAdaptiveFetcher(agent_max_attempts=3)
        with (
            mock.patch(
                "monitor.scrapling_fetch.Fetcher.get",
                side_effect=[self.page(429, b"rate limited"), self.page(200, self.complete().content)],
            ) as get,
            mock.patch.object(fetcher, "_browser_fetch") as browser_fetch,
        ):
            result = fetcher.fetch("https://air.test/travel/pets")

        self.assertEqual(result.mode, "static")
        self.assertEqual(get.call_count, 2)
        browser_fetch.assert_not_called()
        self.assertEqual(fetcher.agent_successes, 1)

    def test_repeated_5xx_stops_after_bounded_retries(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state_dir = Path(temporary)
            fetcher = ScraplingAdaptiveFetcher(agent_state_dir=state_dir, agent_max_attempts=5)
            with (
                mock.patch(
                    "monitor.scrapling_fetch.Fetcher.get",
                    side_effect=[self.page(500, b"server error"), self.page(500, b"server error")],
                ) as get,
                mock.patch.object(fetcher, "_browser_fetch") as browser_fetch,
            ):
                result = fetcher.fetch("https://air.test/travel/pets")
            queue = json.loads((state_dir / "manual-queue.json").read_text(encoding="utf-8"))

        self.assertEqual(result.status_code, 500)
        self.assertEqual(get.call_count, 2)
        browser_fetch.assert_not_called()
        self.assertEqual(queue[0]["state"], "paused")
        self.assertEqual(queue[0]["failure_kind"], "server_error")
        self.assertEqual(queue[0]["status_code"], 500)

    def test_401_and_captcha_create_structured_pauses(self) -> None:
        for status, body, failure_kind, required_action in (
            (401, b"<html>authentication required</html>", "authentication_required", "authorized_authentication"),
            (200, b"<html>hCaptcha verification</html>", "human_verification", "authorized_human_verification"),
        ):
            with self.subTest(failure_kind=failure_kind), tempfile.TemporaryDirectory() as temporary:
                state_dir = Path(temporary)
                fetcher = ScraplingAdaptiveFetcher(agent_state_dir=state_dir)
                with mock.patch(
                    "monitor.scrapling_fetch.Fetcher.get", return_value=self.page(status, body)
                ):
                    if status == 200:
                        result = fetcher.fetch("https://air.test/travel/pets")
                        with self.assertRaises(BrowserFetchError) as raised:
                            result.raise_for_status()
                        self.assertEqual(raised.exception.failure_kind, failure_kind)
                    else:
                        self.assertEqual(
                            fetcher.fetch("https://air.test/travel/pets").status_code, status
                        )
                queue = json.loads((state_dir / "manual-queue.json").read_text(encoding="utf-8"))

            self.assertEqual(queue[0]["state"], "paused")
            self.assertEqual(queue[0]["failure_kind"], failure_kind)
            self.assertEqual(queue[0]["status_code"], status)
            self.assertEqual(queue[0]["required_action"], required_action)

    def test_403_remaining_after_stealth_is_structured_pause(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state_dir = Path(temporary)
            fetcher = ScraplingAdaptiveFetcher(
                stealth_limit=1, agent_state_dir=state_dir
            )
            denied = FetchResult(
                403, "https://air.test/travel/pets", {"Content-Type": "text/html"},
                b"<html>access denied</html>", "stealth",
            )
            with (
                mock.patch(
                    "monitor.scrapling_fetch.Fetcher.get",
                    return_value=self.page(403, b"<html>access denied</html>"),
                ),
                mock.patch.object(fetcher, "_browser_fetch", return_value=denied),
            ):
                result = fetcher.fetch("https://air.test/travel/pets")
            queue = json.loads((state_dir / "manual-queue.json").read_text(encoding="utf-8"))

        self.assertEqual(result.status_code, 403)
        self.assertEqual(queue[0]["failure_kind"], "access_forbidden")
        self.assertEqual(queue[0]["required_action"], "review_access_policy")

    def test_2xx_missing_expected_topic_never_becomes_success(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state_dir = Path(temporary)
            fetcher = ScraplingAdaptiveFetcher(
                dynamic_limit=1, stealth_limit=1, agent_state_dir=state_dir
            )
            unrelated = FetchResult(
                200, "https://air.test/travel/pets", {"Content-Type": "text/html"},
                b"<html><main>" + b"unrelated corporate information " * 80 + b"</main></html>",
                "dynamic",
            )
            with (
                mock.patch(
                    "monitor.scrapling_fetch.Fetcher.get",
                    return_value=self.page(200, unrelated.content),
                ),
                mock.patch.object(fetcher, "_browser_fetch", return_value=unrelated),
            ):
                result = fetcher.fetch(
                    "https://air.test/travel/pets",
                    expect_topic=True,
                    topic_terms=("pet",),
                )
                with self.assertRaises(BrowserFetchError) as raised:
                    result.raise_for_status()
            status = json.loads((state_dir / "status.json").read_text(encoding="utf-8"))
            profile_exists = (state_dir / "site-profiles.json").exists()

        self.assertIn(raised.exception.failure_kind, {"incomplete_content", "budget"})
        self.assertNotIn("success", status["status_counts"])
        self.assertFalse(profile_exists)

    def test_global_browser_budget_is_shared_across_fetchers(self) -> None:
        budget = BrowserFetchBudget(dynamic_limit=1, stealth_limit=0)
        first = ScraplingAdaptiveFetcher(browser_budget=budget)
        second = ScraplingAdaptiveFetcher(browser_budget=budget)
        shell = self.page(200, b"<script>app()</script>")
        with (
            mock.patch("monitor.scrapling_fetch.Fetcher.get", return_value=shell),
            mock.patch.object(first, "_browser_fetch", return_value=self.complete("dynamic")) as first_browser,
            mock.patch.object(second, "_browser_fetch") as second_browser,
        ):
            self.assertEqual(first.fetch("https://air.test/travel/pets").mode, "dynamic")
            invalid = second.fetch("https://air.test/travel/pets")
            with self.assertRaises(BrowserFetchError):
                invalid.raise_for_status()

        first_browser.assert_called_once()
        second_browser.assert_not_called()
        self.assertEqual(budget.snapshot()["dynamic_used"], 1)

    def test_browser_budget_consumption_is_thread_safe(self) -> None:
        budget = BrowserFetchBudget(dynamic_limit=3, stealth_limit=0)
        with ThreadPoolExecutor(max_workers=20) as executor:
            accepted = list(executor.map(lambda _: budget.try_consume("dynamic"), range(20)))
        self.assertEqual(sum(accepted), 3)
        self.assertEqual(budget.snapshot()["dynamic_used"], 3)


class AgentLoopSuccessGateTests(unittest.TestCase):
    def test_browser_budget_exhaustion_is_deferred_without_manual_pause_or_profile_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            store = AgentStateStore(directory)
            loop = AgentLoop(store, LoopLimits(max_attempts=1))

            result = loop.run(
                "budget-task",
                "air.test/pets",
                ("dynamic",),
                lambda strategy, _: StepResult(
                    "retry",
                    strategy,
                    failure_kind="budget",
                    detail="dynamic browser budget exhausted",
                ),
            )

            manual_queue = (
                json.loads((directory / "manual-queue.json").read_text(encoding="utf-8"))
                if (directory / "manual-queue.json").exists()
                else []
            )
            profile_exists = (directory / "site-profiles.json").exists()

        self.assertEqual(result.status, "deferred")
        self.assertEqual(result.stop_reason, "browser capacity budget exhausted")
        self.assertEqual(manual_queue, [])
        self.assertFalse(profile_exists)

    def test_compaction_archives_legacy_budget_pauses_and_keeps_actionable_work(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            directory.mkdir(parents=True, exist_ok=True)
            (directory / "manual-queue.json").write_text(
                json.dumps(
                    [
                        {
                            "task_id": "budget-task",
                            "site_key": "air.test/pets",
                            "state": "paused",
                            "reason": (
                                "no untried strategy remains after budget: "
                                "dynamic browser budget exhausted"
                            ),
                            "attempts": [
                                {
                                    "strategy": "dynamic",
                                    "failure_kind": "budget",
                                    "detail": "dynamic browser budget exhausted",
                                }
                            ],
                        },
                        {
                            "task_id": "auth-task",
                            "site_key": "secure.test/pets",
                            "state": "paused",
                            "reason": "authentication required",
                            "failure_kind": "authentication_required",
                            "attempts": [],
                        },
                    ]
                ),
                encoding="utf-8",
            )
            store = AgentStateStore(directory)

            remaining = store.compact_manual_queue()
            remaining_again = store.compact_manual_queue()
            manual_queue = json.loads(
                (directory / "manual-queue.json").read_text(encoding="utf-8")
            )
            deferred_queue = json.loads(
                (directory / "deferred-queue.json").read_text(encoding="utf-8")
            )

        self.assertEqual((remaining, remaining_again), (1, 1))
        self.assertEqual([item["task_id"] for item in manual_queue], ["auth-task"])
        self.assertEqual([item["task_id"] for item in deferred_queue], ["budget-task"])
        self.assertEqual(deferred_queue[0]["state"], "deferred")
        self.assertEqual(
            deferred_queue[0]["deferred_reason"],
            "browser_capacity_budget_exhausted",
        )

    def test_non_2xx_success_label_cannot_learn_or_clear_pause(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            store = AgentStateStore(directory)
            loop = AgentLoop(store, LoopLimits(max_attempts=1))
            loop.run(
                "same-task", "air.test/pets", ("dynamic",),
                lambda strategy, _: StepResult(
                    "blocked", strategy, failure_kind="authentication_required",
                    detail="authentication required", metrics={"status_code": 401},
                ),
            )
            result = loop.run(
                "same-task", "air.test/pets", ("dynamic",),
                lambda strategy, _: StepResult(
                    "success", strategy, metrics={
                        "status_code": 404, "complete": True, "topic_matched": True,
                    },
                ),
            )
            queue = json.loads((directory / "manual-queue.json").read_text(encoding="utf-8"))

        self.assertEqual(result.status, "terminal")
        self.assertEqual(len(queue), 1)
        self.assertFalse((directory / "site-profiles.json").exists())


if __name__ == "__main__":
    unittest.main()
