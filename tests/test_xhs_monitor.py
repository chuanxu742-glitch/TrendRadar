from __future__ import annotations

import json
import io
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stdout
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from xhs_monitor.login import qrcode_login
from xhs_monitor.service import (
    Settings,
    build_handler,
    collect_once,
    collection_worker,
    load_settings,
)
from xhs_monitor.store import XiaohongshuStore
from xhs_monitor.settings import load_managed_keywords
from xhs_monitor.web_login import LoginSessionManager
from http.server import ThreadingHTTPServer


KEYWORDS = [
    {
        "id": "pet-cabin-refused",
        "query": "宠物进客舱被拒载",
        "name": "小红书·宠物进客舱被拒载",
    },
    {
        "id": "service-dog-cabin-refused",
        "query": "服务犬进客舱被拒载",
        "name": "小红书·服务犬进客舱被拒载",
    },
]


class FakeFetcher:
    def fetch_all(self):
        return (
            {
                "xhs-pet-cabin-refused": {
                    "宠物进客舱被拒载案例 [note1234]": {
                        "ranks": [1],
                        "url": "https://www.xiaohongshu.com/explore/note1234",
                    },
                    "非法跳转 [badurl12]": {
                        "ranks": [2],
                        "url": "https://example.com/collect",
                    },
                }
            },
            {},
            ["xhs-service-dog-cabin-refused"],
        )


class FakeLoginApi:
    def generate_init_cookies(self):
        return {"initial": "cookie"}

    def generate_qrcode(self, cookies):
        return True, "", {
            "cookies": cookies,
            "qr_url": "https://example.invalid/qr",
            "qr_id": "qr-id",
            "code": "code",
        }

    def show_qrcode_terminal(self, _url):
        print("[test-qr]")

    def check_qrcode_status(self, _qr_id, _code, cookies):
        return True, "登录成功", cookies

    def get_user_info(self, cookies):
        return True, {"nickname": "测试账号"}, cookies

    def cookies_to_str(self, _cookies):
        return "private-session-value"


class BlockingLoginApi(FakeLoginApi):
    def __init__(self) -> None:
        self.release = threading.Event()

    def check_qrcode_status(self, _qr_id, _code, cookies):
        self.release.wait(timeout=2)
        return True, "登录成功", cookies


class FlakyLoginApi(FakeLoginApi):
    """前 failures 次轮询抛异常，之后返回登录成功。"""

    def __init__(self, failures: int) -> None:
        self.remaining_failures = failures
        self.calls = 0

    def check_qrcode_status(self, _qr_id, _code, cookies):
        self.calls += 1
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            raise RuntimeError("transient poll error")
        return True, "登录成功", cookies


class XiaohongshuMonitorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = XiaohongshuStore(self.root / "xhs-monitor.db")
        self.settings = Settings(
            enabled=True,
            port=8091,
            interval_seconds=1800,
            immediate_run=True,
            database_path=self.root / "xhs-monitor.db",
            keywords=KEYWORDS,
            fetcher_config={"COOKIE": "not-exposed", "KEYWORDS": KEYWORDS},
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_partial_collection_is_persisted_and_summarized(self) -> None:
        result = collect_once(
            self.settings,
            self.store,
            fetcher_factory=lambda _config: FakeFetcher(),
        )
        summary = self.store.summary()

        self.assertEqual(result["status"], "partial")
        self.assertEqual(summary["status"], "partial")
        self.assertEqual(summary["source_count"], 2)
        self.assertEqual(summary["successful_sources"], 1)
        self.assertEqual(summary["failed_sources"], 1)
        self.assertEqual(summary["candidate_count"], 2)
        self.assertEqual(summary["pending_detail_count"], 1)
        self.assertEqual(summary["recent_count"], 1)
        self.assertEqual(summary["items"][0]["title"], "宠物进客舱被拒载案例")
        self.assertTrue(summary["items"][0]["summary"])
        self.assertEqual(summary["items"][0]["summary_origin"], "deterministic")
        self.assertNotIn("COOKIE", str(summary))
        self.assertNotIn("not-exposed", str(summary))

    def test_repeated_collection_deduplicates_notes(self) -> None:
        for _ in range(2):
            collect_once(
                self.settings,
                self.store,
                fetcher_factory=lambda _config: FakeFetcher(),
            )

        summary = self.store.summary()
        self.assertEqual(summary["candidate_count"], 2)
        self.assertEqual(summary["recent_count"], 1)

    def test_ai_analysis_filters_irrelevant_results_and_exposes_summary(self) -> None:
        def analyzer(items):
            return {
                item["note_key"]: {
                    "relevant": item["title"].startswith("宠物"),
                    "topic": "拒载",
                    "summary": "旅客反馈宠物办理客舱运输时遭到拒载。",
                    "business_value": "需要核对航司客舱承运执行口径。",
                    "relevance_reason": "涉及宠物客舱拒载",
                    "summary_origin": "ai",
                }
                for item in items
            }

        result = collect_once(
            self.settings,
            self.store,
            fetcher_factory=lambda _config: FakeFetcher(),
            analyzer=analyzer,
        )
        summary = self.store.summary()

        self.assertEqual(result["enriched_items"], 1)
        self.assertEqual(result["relevant_items"], 1)
        self.assertEqual(summary["recent_count"], 1)
        self.assertEqual(
            summary["items"][0]["summary"],
            "旅客反馈宠物办理客舱运输时遭到拒载。",
        )
        self.assertEqual(
            summary["items"][0]["business_value"],
            "需要核对航司客舱承运执行口径。",
        )

    def test_collector_failure_does_not_expose_exception(self) -> None:
        def failed_factory(_config):
            raise RuntimeError("cookie=secret")

        result = collect_once(
            self.settings,
            self.store,
            fetcher_factory=failed_factory,
        )
        summary = self.store.summary()

        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(summary["status_label"], "今日数据暂未更新")
        self.assertNotIn("secret", str(summary))
        self.assertNotIn("error", summary)

    def test_disabled_collection_records_safe_unavailable_state(self) -> None:
        result = collect_once(
            replace(self.settings, enabled=False),
            self.store,
            fetcher_factory=lambda _config: self.fail("fetcher must not run"),
        )

        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(self.store.summary()["status"], "unavailable")

    def test_summary_http_api_and_health_are_available(self) -> None:
        collect_once(
            self.settings,
            self.store,
            fetcher_factory=lambda _config: FakeFetcher(),
        )
        server = ThreadingHTTPServer(("127.0.0.1", 0), build_handler(self.store))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base_url = f"http://127.0.0.1:{server.server_port}"
            with urlopen(f"{base_url}/health/live", timeout=2) as response:
                self.assertEqual(json.load(response), {"status": "ok"})
            with urlopen(f"{base_url}/api/v1/summary?limit=1", timeout=2) as response:
                summary = json.load(response)
            self.assertEqual(summary["status"], "partial")
            self.assertEqual(len(summary["items"]), 1)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_standalone_login_writes_cookie_without_printing_it(self) -> None:
        cookie_path = self.root / "config" / "xhs_cookie.txt"
        output = io.StringIO()

        with redirect_stdout(output):
            success = qrcode_login(
                cookie_path,
                api_loader=lambda: FakeLoginApi(),
            )

        self.assertTrue(success)
        self.assertEqual(cookie_path.read_text(encoding="utf-8"), "private-session-value")
        self.assertNotIn("private-session-value", output.getvalue())

    def test_cookie_file_takes_priority_over_stale_environment_cookie(self) -> None:
        config_directory = self.root / "config"
        config_directory.mkdir()
        config_path = config_directory / "xhs-monitor.yaml"
        config_path.write_text(
            """
xiaohongshu:
  enabled: true
  cookie_file: config/xhs_cookie.txt
  keywords: []
""".strip(),
            encoding="utf-8",
        )
        (config_directory / "xhs_cookie.txt").write_text(
            "fresh-file-session",
            encoding="utf-8",
        )

        with patch.dict(
            "os.environ",
            {
                "XHS_COOKIE": "stale-environment-session",
                "XHS_COOKIE_FILE": "",
            },
            clear=False,
        ):
            settings = load_settings(config_path)

        self.assertEqual(settings.fetcher_config["COOKIE"], "fresh-file-session")

    def test_worker_collects_immediately_when_login_state_changes(self) -> None:
        waiting_settings = replace(
            self.settings,
            immediate_run=False,
            fetcher_config={**self.settings.fetcher_config, "COOKIE": ""},
        )
        logged_in_settings = replace(
            waiting_settings,
            fetcher_config={
                **waiting_settings.fetcher_config,
                "COOKIE": "new-session",
            },
        )
        loaded_settings = iter([waiting_settings, logged_in_settings])
        collected: list[Settings] = []
        sleep_calls = 0

        def collect_func(settings, _store):
            collected.append(settings)
            return {
                "status": "available",
                "source_count": 2,
                "item_count": 1,
            }

        def sleep_func(_seconds):
            nonlocal sleep_calls
            sleep_calls += 1
            if sleep_calls >= 2:
                raise StopIteration

        with self.assertRaises(StopIteration):
            collection_worker(
                waiting_settings,
                self.store,
                settings_loader=lambda: next(loaded_settings),
                collect_func=collect_func,
                monotonic_func=lambda: 0,
                sleep_func=sleep_func,
            )

        self.assertEqual(len(collected), 1)
        self.assertEqual(collected[0].fetcher_config["COOKIE"], "new-session")

    def test_settings_and_web_login_api_are_safe_and_persistent(self) -> None:
        settings_path = self.root / "settings.json"
        cookie_path = self.root / "xhs_cookie.txt"
        base_settings = replace(
            self.settings,
            settings_path=settings_path,
            cookie_file=cookie_path,
            fetcher_config={
                **self.settings.fetcher_config,
                "COOKIE": "",
            },
        )

        def settings_loader():
            managed = load_managed_keywords(settings_path)
            keywords = managed if managed is not None else base_settings.keywords
            cookie = cookie_path.read_text(encoding="utf-8") if cookie_path.exists() else ""
            return replace(
                base_settings,
                keywords=keywords,
                fetcher_config={
                    **base_settings.fetcher_config,
                    "COOKIE": cookie,
                    "KEYWORDS": keywords,
                },
            )

        login_api = BlockingLoginApi()
        login_manager = LoginSessionManager(
            cookie_path,
            api_loader=lambda: login_api,
            poll_seconds=0.1,
        )
        server = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            build_handler(
                self.store,
                settings_loader=settings_loader,
                login_manager=login_manager,
            ),
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base_url = f"http://127.0.0.1:{server.server_port}"
        try:
            settings_request = Request(
                f"{base_url}/api/v1/settings",
                data=json.dumps(
                    {
                        "keywords": [
                            {"query": "宠物进客舱"},
                            {"query": "宠物进客舱"},
                            {"query": "服务犬"},
                        ]
                    },
                    ensure_ascii=False,
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(settings_request, timeout=2) as response:
                settings = json.load(response)

            self.assertEqual(
                [item["query"] for item in settings["keywords"]],
                ["宠物进客舱", "服务犬"],
            )
            self.assertFalse(settings["credential_saved"])
            self.assertEqual(
                [item["query"] for item in load_managed_keywords(settings_path) or []],
                ["宠物进客舱", "服务犬"],
            )

            login_request = Request(
                f"{base_url}/api/v1/login/start",
                data=b"{}",
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(login_request, timeout=2) as response:
                login = json.load(response)

            self.assertIn(login["status"], {"waiting_scan", "waiting_confirm"})
            self.assertTrue(login["qr_image"].startswith("data:image/svg+xml;base64,"))
            self.assertNotIn("private-session-value", str(login))

            login_api.release.set()
            for _ in range(30):
                if login_manager.status()["status"] == "success":
                    break
                time.sleep(0.05)
            self.assertEqual(login_manager.status()["status"], "success")
            self.assertEqual(
                cookie_path.read_text(encoding="utf-8"),
                "private-session-value",
            )
        finally:
            login_api.release.set()
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_post_endpoints_enforce_local_json_write_guard(self) -> None:
        base_settings = replace(
            self.settings,
            settings_path=self.root / "settings.json",
        )
        server = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            build_handler(self.store, settings_loader=lambda: base_settings),
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base_url = f"http://127.0.0.1:{server.server_port}"
        body = json.dumps(
            {"keywords": [{"query": "宠物进客舱"}]},
            ensure_ascii=False,
        ).encode("utf-8")

        def post(path: str, headers: dict[str, str], data: bytes = body):
            request = Request(
                f"{base_url}{path}",
                data=data,
                headers=headers,
                method="POST",
            )
            return urlopen(request, timeout=2)

        try:
            # 无 Origin 的 JSON 请求放行（official-monitor 代理调用路径）
            with post(
                "/api/v1/settings",
                {"Content-Type": "application/json"},
            ) as response:
                self.assertEqual(response.status, 200)
            # 本机来源 Origin 放行
            with post(
                "/api/v1/settings",
                {
                    "Content-Type": "application/json",
                    "Origin": f"http://127.0.0.1:{server.server_port}",
                },
            ) as response:
                self.assertEqual(response.status, 200)
            # 跨域 Origin 拒绝
            with self.assertRaises(HTTPError) as cross_origin:
                post(
                    "/api/v1/settings",
                    {
                        "Content-Type": "application/json",
                        "Origin": "https://evil.example",
                    },
                )
            self.assertEqual(cross_origin.exception.code, 400)
            cross_origin.exception.close()
            # 非 JSON Content-Type 拒绝
            with self.assertRaises(HTTPError) as bad_type:
                post("/api/v1/settings", {"Content-Type": "text/plain"})
            self.assertEqual(bad_type.exception.code, 400)
            bad_type.exception.close()
            # login/start 同样受防护
            with self.assertRaises(HTTPError) as login_denied:
                post(
                    "/api/v1/login/start",
                    {
                        "Content-Type": "application/json",
                        "Origin": "https://evil.example",
                    },
                    data=b"{}",
                )
            self.assertEqual(login_denied.exception.code, 400)
            login_denied.exception.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def _wait_for_login_status(
        self,
        manager: LoginSessionManager,
        expected: str,
        timeout: float = 5,
    ) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = manager.status()["status"]
            if status == expected:
                return status
            time.sleep(0.05)
        return manager.status()["status"]

    def test_web_login_poll_tolerates_transient_failures(self) -> None:
        cookie_path = self.root / "xhs_cookie.txt"
        login_api = FlakyLoginApi(failures=2)
        manager = LoginSessionManager(
            cookie_path,
            api_loader=lambda: login_api,
            poll_seconds=0.1,
        )

        manager.start()

        # 连续 2 次瞬时异常后成功，登录仍应完成
        self.assertEqual(
            self._wait_for_login_status(manager, "success"),
            "success",
        )
        self.assertEqual(login_api.calls, 3)
        self.assertEqual(
            cookie_path.read_text(encoding="utf-8"),
            "private-session-value",
        )

    def test_web_login_poll_fails_after_three_consecutive_errors(self) -> None:
        cookie_path = self.root / "xhs_cookie.txt"
        login_api = FlakyLoginApi(failures=100)
        manager = LoginSessionManager(
            cookie_path,
            api_loader=lambda: login_api,
            poll_seconds=0.1,
        )

        manager.start()

        # 连续 3 次失败才判定登录失败
        self.assertEqual(
            self._wait_for_login_status(manager, "failed"),
            "failed",
        )
        self.assertEqual(login_api.calls, 3)
        self.assertFalse(cookie_path.exists())


if __name__ == "__main__":
    unittest.main()
