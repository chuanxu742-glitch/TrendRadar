from __future__ import annotations

import re
import json
import hashlib
import os
import signal
import subprocess
import sys
import tempfile
import threading
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from requests.structures import CaseInsensitiveDict
from scrapling.fetchers import Fetcher

try:
    from .scraping_agent import AgentLoop, AgentStateStore, LoopLimits, StepResult
except ImportError:
    from scraping_agent import AgentLoop, AgentStateStore, LoopLimits, StepResult


WAF_MARKERS = (
    "just a moment", "checking your browser", "cloudflare", "cf-chl-", "access denied",
)
CLOUDFLARE_CHALLENGE_MARKERS = (
    "just a moment",
    "checking your browser",
    "cf-chl-",
    "cf-turnstile",
    "challenges.cloudflare.com",
)
SHELL_MARKERS = (
    "enable javascript", "please enable javascript", "javascript is required", "请启用javascript", "请启用 javascript",
)
CAPTCHA_MARKERS = (
    "captcha", "recaptcha", "hcaptcha", "turnstile", "verify you are human", "人机验证",
)
AUTH_MARKERS = (
    "sign in to continue", "login required", "authentication required", "登录后继续", "需要登录",
)


class _VisibleText(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.skip = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"script", "style", "noscript", "svg", "template"}:
            self.skip += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style", "noscript", "svg", "template"} and self.skip:
            self.skip -= 1

    def handle_data(self, data: str) -> None:
        if not self.skip:
            value = re.sub(r"\s+", " ", data).strip()
            if value:
                self.parts.append(value)


def visible_text(content: bytes) -> str:
    parser = _VisibleText()
    try:
        parser.feed(content.decode("utf-8", errors="replace"))
    except Exception:
        return ""
    return " ".join(parser.parts)


def contains_expected_topic(text: str, terms: tuple[str, ...]) -> bool:
    lowered = text.lower()
    for term in terms:
        if term.isascii():
            if re.search(rf"(?<![a-z]){re.escape(term.lower())}(?![a-z])", lowered):
                return True
        elif term in text:
            return True
    return False


def template_fingerprint(content: bytes) -> str:
    text = content[:500_000].decode("utf-8", errors="ignore").lower()
    tags = re.findall(r"<\s*([a-z][a-z0-9:-]*)\b", text)
    skeleton = "|".join(tags[:1500])
    return hashlib.sha256(skeleton.encode("utf-8")).hexdigest()[:16] if skeleton else ""


def completeness_metrics(
    result: "FetchResult",
    preview: str,
    expect_topic: bool,
    topic_terms: tuple[str, ...],
    minimum_visible_chars: int,
) -> dict[str, Any]:
    content_type = result.headers.get("Content-Type", "").lower()
    prefix = result.content[:512].lstrip().lower()
    is_html = (
        "html" in content_type
        or "xml" in content_type
        or prefix.startswith((b"<!doctype html", b"<html", b"<?xml"))
    )
    known_binary = any(
        marker in content_type
        for marker in ("pdf", "octet-stream", "zip", "msword", "officedocument")
    ) or b"\x00" in prefix
    is_textual = (
        is_html
        or content_type.startswith("text/")
        or "json" in content_type
        or (not content_type and not known_binary)
    )
    effective_expect_topic = expect_topic and is_textual
    topic_matched = True
    if effective_expect_topic:
        topic_matched = bool(topic_terms) and contains_expected_topic(preview, topic_terms)
    visible_size = len(preview) if is_html else len(result.content)
    minimum = max(minimum_visible_chars, 1)
    score = 20 if 200 <= result.status_code < 300 else 0
    reasons: list[str] = []
    if visible_size >= minimum:
        score += 20
    else:
        reasons.append(f"visible content below minimum: {visible_size}/{minimum}")
    if visible_size >= max(minimum * 4, 500):
        score += 10
    if effective_expect_topic:
        if topic_matched:
            score += 25
        else:
            reasons.append(
                "expected policy topic missing"
                if topic_terms
                else "topic validation requested without configured terms"
            )
    else:
        score += 15
    lowered = result.content[:500_000].decode("utf-8", errors="ignore").lower()
    if is_html:
        if re.search(r"<(?:main|article|body)\b", lowered):
            score += 5
        if any(marker in lowered for marker in ("</html>", "</main>", "</article>")):
            score += 5
    else:
        score += 10
    complete = score >= 60 and visible_size >= minimum and topic_matched
    return {
        "complete": complete,
        "completeness_score": min(score, 100),
        "visible_chars": visible_size,
        "minimum_visible_chars": minimum,
        "topic_matched": topic_matched,
        "template_fingerprint": template_fingerprint(result.content) if is_html else "",
        "completeness_reasons": reasons,
    }


@dataclass
class FetchResult:
    status_code: int
    url: str
    headers: CaseInsensitiveDict
    content: bytes
    mode: str
    escalation_reason: str = ""
    validation_error: str = ""
    failure_kind: str = ""

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code} for {self.url}")
        if self.validation_error:
            raise BrowserFetchError(
                self.validation_error,
                failure_kind=self.failure_kind,
                status_code=self.status_code,
            )


class BrowserFetchError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        failure_kind: str = "",
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.failure_kind = failure_kind
        self.status_code = status_code


class BrowserFetchBudget:
    """Thread-safe attempt budget that can be shared by many fetcher instances.

    The limits are total browser launches for the lifetime of this object, not
    concurrency limits. Call ``reset`` at an explicit scheduling-cycle boundary.
    Existing callers that do not provide a budget keep per-fetcher limits.
    """

    def __init__(self, dynamic_limit: int = 5, stealth_limit: int = 2) -> None:
        self.dynamic_limit = max(int(dynamic_limit), 0)
        self.stealth_limit = max(int(stealth_limit), 0)
        self._dynamic_used = 0
        self._stealth_used = 0
        self._lock = threading.Lock()

    def try_consume(self, mode: str) -> bool:
        if mode not in {"dynamic", "stealth"}:
            raise ValueError(f"unsupported browser budget mode: {mode}")
        used_name = f"_{mode}_used"
        limit = self.dynamic_limit if mode == "dynamic" else self.stealth_limit
        with self._lock:
            used = int(getattr(self, used_name))
            if used >= limit:
                return False
            setattr(self, used_name, used + 1)
            return True

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "dynamic_used": self._dynamic_used,
                "dynamic_limit": self.dynamic_limit,
                "stealth_used": self._stealth_used,
                "stealth_limit": self.stealth_limit,
            }

    def reset(self) -> None:
        with self._lock:
            self._dynamic_used = 0
            self._stealth_used = 0


def _result(page: Any, mode: str, reason: str = "") -> FetchResult:
    headers = CaseInsensitiveDict(dict(getattr(page, "headers", {}) or {}))
    if mode == "static":
        body = getattr(page, "body", b"")
        content = body if isinstance(body, bytes) else str(body).encode("utf-8", errors="replace")
    else:
        rendered = getattr(page, "html_content", "")
        if rendered:
            content = str(rendered).encode("utf-8", errors="replace")
            headers["Content-Type"] = "text/html; charset=utf-8"
        else:
            body = getattr(page, "body", b"")
            content = body if isinstance(body, bytes) else str(body).encode("utf-8", errors="replace")
    return FetchResult(
        status_code=int(getattr(page, "status", 0) or 0),
        url=str(getattr(page, "url", "")),
        headers=headers,
        content=content,
        mode=mode,
        escalation_reason=reason,
    )


class ScraplingAdaptiveFetcher:
    def __init__(
        self,
        dynamic_limit: int = 5,
        stealth_limit: int = 2,
        browser_hard_timeout: int = 75,
        cloudflare_solver_enabled: bool = True,
        cloudflare_timeout: int = 60,
        agent_state_dir: Path | None = None,
        agent_max_attempts: int = 3,
        agent_max_duration: int = 180,
        dynamic_semaphore: threading.Semaphore | None = None,
        stealth_semaphore: threading.Semaphore | None = None,
        browser_budget: BrowserFetchBudget | None = None,
    ) -> None:
        self.browser_budget = browser_budget or BrowserFetchBudget(dynamic_limit, stealth_limit)
        self.dynamic_limit = self.browser_budget.dynamic_limit
        self.stealth_limit = self.browser_budget.stealth_limit
        self.browser_hard_timeout = max(browser_hard_timeout, 15)
        self.cloudflare_solver_enabled = cloudflare_solver_enabled
        self.cloudflare_timeout = max(cloudflare_timeout, 60)
        self.dynamic_used = 0
        self.stealth_used = 0
        self.browser_timeouts = 0
        self.browser_failures = 0
        self.cloudflare_attempts = 0
        self.cloudflare_fetch_successes = 0
        self.cloudflare_failures = 0
        self.agent_runs = 0
        self.agent_successes = 0
        self.agent_blocked = 0
        self.dynamic_semaphore = dynamic_semaphore
        self.stealth_semaphore = stealth_semaphore
        self.agent_loop = AgentLoop(
            AgentStateStore(agent_state_dir),
            LoopLimits(
                max_attempts=max(agent_max_attempts, 1),
                max_duration_seconds=max(agent_max_duration, 15),
                max_same_failure=2,
            ),
        )

    @staticmethod
    def _log_tail(path: Path, limit: int = 2000) -> str:
        try:
            value = path.read_text(encoding="utf-8", errors="replace")
            return value[-limit:].strip()
        except OSError:
            return ""

    @staticmethod
    def _kill_process_tree(process: subprocess.Popen[Any]) -> None:
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGKILL)
            else:
                process.kill()
        except ProcessLookupError:
            pass
        finally:
            try:
                process.wait(timeout=5)
            except (subprocess.TimeoutExpired, OSError):
                pass

    def _browser_fetch(
        self,
        mode: str,
        url: str,
        timeout: int,
        reason: str,
        solve_cloudflare: bool = False,
    ) -> FetchResult:
        worker = Path(__file__).with_name("browser_fetch_worker.py")
        browser_timeout_seconds = (
            max(timeout, self.cloudflare_timeout)
            if solve_cloudflare
            else max(timeout, 30)
        )
        browser_timeout_ms = browser_timeout_seconds * 1000
        with tempfile.TemporaryDirectory(prefix="policy-browser-") as temporary:
            output = Path(temporary)
            log_path = output / "worker.log"
            command = [
                sys.executable,
                str(worker),
                "--mode",
                mode,
                "--url",
                url,
                "--timeout-ms",
                str(browser_timeout_ms),
                "--output",
                str(output),
            ]
            if solve_cloudflare:
                command.append("--solve-cloudflare")
            with log_path.open("wb") as log:
                process = subprocess.Popen(
                    command,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=(os.name == "posix"),
                )
                try:
                    hard_timeout = max(
                        self.browser_hard_timeout,
                        browser_timeout_seconds + 15 if solve_cloudflare else 0,
                    )
                    return_code = process.wait(timeout=hard_timeout)
                except subprocess.TimeoutExpired as exc:
                    self.browser_timeouts += 1
                    self._kill_process_tree(process)
                    raise BrowserFetchError(
                        f"{mode} browser hard timeout after {hard_timeout}s for {url}"
                    ) from exc
                # Patchright/Chromium can leave descendants behind even when the
                # worker exits successfully. The worker has its own process group,
                # so cleaning that group cannot affect the monitor process.
                self._kill_process_tree(process)

            if return_code != 0:
                self.browser_failures += 1
                detail = self._log_tail(output / "error.txt") or self._log_tail(log_path)
                raise BrowserFetchError(
                    f"{mode} browser failed for {url}: {detail or f'exit code {return_code}'}"
                )

            try:
                metadata = json.loads((output / "result.json").read_text(encoding="utf-8"))
                content = (output / "content.bin").read_bytes()
            except (OSError, json.JSONDecodeError) as exc:
                self.browser_failures += 1
                raise BrowserFetchError(f"{mode} browser returned incomplete output for {url}: {exc}") from exc

            return FetchResult(
                status_code=int(metadata.get("status_code", 0)),
                url=str(metadata.get("url") or url),
                headers=CaseInsensitiveDict(metadata.get("headers") or {}),
                content=content,
                mode=mode,
                escalation_reason=reason,
            )

    @staticmethod
    def fetch_static(url: str, headers: dict[str, str] | None = None, timeout: int = 15) -> FetchResult:
        """Transport-only fetch for robots, sitemaps and discovery probes.

        This path deliberately bypasses the agent loop, browser escalation and
        manual queue because discovery infrastructure URLs are not policy pages.
        """
        page = Fetcher.get(
            url,
            impersonate="chrome",
            timeout=timeout,
            retries=0,
            headers=headers or None,
        )
        return _result(page, "static-discovery")

    def fetch(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        timeout: int = 15,
        expect_topic: bool = False,
        topic_terms: tuple[str, ...] = (),
        minimum_visible_chars: int = 80,
    ) -> FetchResult:
        parts = urlsplit(url)
        first_segment = next((item for item in parts.path.split("/") if item), "_")
        site_key = f"{parts.netloc.lower()}/{first_segment.lower()}"
        task_id = f"fetch:{site_key}:{hashlib.sha256(url.encode()).hexdigest()[:12]}"

        def execute(strategy: str, _: int) -> StepResult:
            used_cloudflare_solver = False
            try:
                if strategy == "static":
                    page = Fetcher.get(
                        url,
                        impersonate="chrome",
                        timeout=timeout,
                        retries=0,
                        headers=headers or None,
                    )
                    result = _result(page, "static")
                elif strategy == "dynamic":
                    if not self.browser_budget.try_consume("dynamic"):
                        return StepResult(
                            "retry", strategy, failure_kind="budget",
                            detail="dynamic browser budget exhausted",
                            suggested_strategies=("static", "stealth"),
                        )
                    self.dynamic_used += 1
                    if self.dynamic_semaphore is None:
                        result = self._browser_fetch("dynamic", url, timeout, "agent:dynamic")
                    else:
                        with self.dynamic_semaphore:
                            result = self._browser_fetch("dynamic", url, timeout, "agent:dynamic")
                elif strategy == "stealth":
                    if not self.browser_budget.try_consume("stealth"):
                        return StepResult(
                            "retry", strategy, failure_kind="budget",
                            detail="stealth browser budget exhausted",
                            suggested_strategies=("static", "dynamic"),
                        )
                    self.stealth_used += 1
                    solve_cloudflare = self.cloudflare_solver_enabled
                    used_cloudflare_solver = solve_cloudflare
                    if solve_cloudflare:
                        self.cloudflare_attempts += 1
                    try:
                        if self.stealth_semaphore is None:
                            result = self._browser_fetch(
                                "stealth", url, timeout, "agent:stealth", solve_cloudflare
                            )
                        else:
                            with self.stealth_semaphore:
                                result = self._browser_fetch(
                                    "stealth", url, timeout, "agent:stealth", solve_cloudflare
                                )
                    except Exception:
                        if solve_cloudflare:
                            self.cloudflare_failures += 1
                        raise
                else:
                    return StepResult("terminal", strategy, failure_kind="strategy", detail="unknown strategy")
            except Exception as exc:
                lowered_error = str(exc).lower()
                terminal = any(
                    marker in lowered_error
                    for marker in ("certificate", "ssl", "tls", "resolve", "dns", "name resolution")
                )
                suggestions = ("dynamic", "stealth") if strategy == "static" else ("stealth",)
                return StepResult(
                    "terminal" if terminal else "retry",
                    strategy,
                    failure_kind="network" if terminal else "fetch_error",
                    detail=str(exc),
                    suggested_strategies=suggestions,
                )

            if result.status_code == 304:
                return StepResult("not_modified", strategy, output=result, metrics={"status_code": 304})
            preview = visible_text(result.content)
            lowered = result.content[:200_000].decode("utf-8", errors="ignore").lower()
            waf = (
                result.status_code == 403
                or "cf-chl-" in lowered
                or (
                    len(preview) < 3000
                    and any(marker in lowered for marker in WAF_MARKERS)
                )
            )
            captcha = len(preview) < 3000 and any(
                marker in lowered for marker in CAPTCHA_MARKERS
            )
            server = result.headers.get("Server", "").lower()
            cloudflare_challenge = len(preview) < 3000 and (
                any(marker in lowered for marker in CLOUDFLARE_CHALLENGE_MARKERS)
                or ("cloudflare" in server and result.status_code in {403, 429, 503})
            )
            auth_required = result.status_code == 401 or (
                len(preview) < 1000 and any(marker in preview.lower() for marker in AUTH_MARKERS)
            )
            shell = (
                result.status_code == 200
                and "html" in result.headers.get("Content-Type", "").lower()
                and (
                    len(preview) < 250
                    or (
                        len(preview) < 2000
                        and any(marker in preview.lower() for marker in SHELL_MARKERS)
                    )
                    or (expect_topic and not contains_expected_topic(preview, topic_terms) and "<script" in lowered)
                )
            )
            metrics = {
                "status_code": result.status_code,
                "content_bytes": len(result.content),
                "visible_chars": len(preview),
            }
            if result.status_code == 404:
                return StepResult(
                    "terminal", strategy, output=result, failure_kind="http_not_found",
                    detail="HTTP 404 page not found", metrics=metrics,
                )
            if result.status_code == 410:
                return StepResult(
                    "terminal", strategy, output=result, failure_kind="http_gone",
                    detail="HTTP 410 resource gone", metrics=metrics,
                )
            if captcha:
                if cloudflare_challenge and strategy != "stealth":
                    return StepResult(
                        "retry", strategy, output=result,
                        failure_kind="cloudflare_challenge",
                        detail="Cloudflare challenge detected",
                        suggested_strategies=("stealth",), metrics=metrics,
                    )
                return StepResult(
                    "blocked", strategy, output=result, failure_kind="human_verification",
                    detail="human verification checkpoint requires an authorized handler", metrics=metrics,
                )
            if auth_required:
                return StepResult(
                    "blocked", strategy, output=result, failure_kind="authentication_required",
                    detail="authentication checkpoint requires an authorized handler", metrics=metrics,
                )
            if result.status_code == 403 and strategy == "stealth":
                return StepResult(
                    "blocked", strategy, output=result, failure_kind="access_forbidden",
                    detail="HTTP 403 remained after the authorized recovery strategy",
                    metrics=metrics,
                )
            if result.status_code == 429:
                return StepResult(
                    "retry", strategy, output=result, failure_kind="rate_limited",
                    detail="HTTP 429 rate limited", retry_same_strategy=True, metrics=metrics,
                )
            if 500 <= result.status_code < 600 and not waf:
                return StepResult(
                    "retry", strategy, output=result, failure_kind="server_error",
                    detail=f"HTTP {result.status_code} server error",
                    retry_same_strategy=True, metrics=metrics,
                )
            if waf:
                if strategy == "stealth":
                    return StepResult(
                        "blocked", strategy, output=result, failure_kind="waf",
                        detail="WAF remained after the authorized recovery strategy",
                        metrics=metrics,
                    )
                return StepResult(
                    "retry", strategy, output=result,
                    failure_kind="access_forbidden" if result.status_code == 403 else "waf",
                    detail="WAF or HTTP 403 response", suggested_strategies=("stealth", "dynamic"),
                    metrics=metrics,
                )
            if not 200 <= result.status_code < 300:
                return StepResult(
                    "terminal", strategy, output=result, failure_kind="http_status",
                    detail=f"HTTP {result.status_code} is not a usable response", metrics=metrics,
                )
            if shell:
                return StepResult(
                    "retry", strategy, output=result, failure_kind="javascript_shell",
                    detail="JavaScript shell or expected topic missing",
                    suggested_strategies=("dynamic", "stealth"), metrics=metrics,
                )
            completeness = completeness_metrics(
                result, preview, expect_topic, topic_terms, minimum_visible_chars
            )
            metrics.update(completeness)
            if not completeness["complete"]:
                return StepResult(
                    "retry", strategy, output=result, failure_kind="incomplete_content",
                    detail="; ".join(completeness["completeness_reasons"]) or "content completeness score too low",
                    suggested_strategies=("dynamic", "stealth"), metrics=metrics,
                )
            if used_cloudflare_solver:
                self.cloudflare_fetch_successes += 1
            return StepResult("success", strategy, output=result, metrics=metrics)

        self.agent_runs += 1
        loop_result = self.agent_loop.run(
            task_id, site_key, ("static", "dynamic", "stealth"), execute
        )
        if loop_result.status == "success":
            self.agent_successes += 1
        elif loop_result.status == "blocked":
            self.agent_blocked += 1
        if isinstance(loop_result.output, FetchResult):
            if loop_result.status not in {"success", "not_modified"}:
                loop_result.output.escalation_reason = loop_result.stop_reason or loop_result.last_error
                last_attempt = loop_result.attempts[-1] if loop_result.attempts else {}
                loop_result.output.validation_error = (
                    loop_result.last_error or loop_result.stop_reason or "agent content validation failed"
                )
                loop_result.output.failure_kind = str(last_attempt.get("failure_kind", ""))
            return loop_result.output
        raise BrowserFetchError(
            loop_result.last_error or loop_result.stop_reason or "agent fetch failed",
            failure_kind=str(loop_result.attempts[-1].get("failure_kind", "")) if loop_result.attempts else "",
        )

    def stats(self) -> dict[str, int]:
        return {
            "dynamic_used": self.dynamic_used,
            "dynamic_limit": self.dynamic_limit,
            "stealth_used": self.stealth_used,
            "stealth_limit": self.stealth_limit,
            "browser_timeouts": self.browser_timeouts,
            "browser_failures": self.browser_failures,
            "cloudflare_attempts": self.cloudflare_attempts,
            "cloudflare_fetch_successes": self.cloudflare_fetch_successes,
            "cloudflare_failures": self.cloudflare_failures,
            "agent_runs": self.agent_runs,
            "agent_successes": self.agent_successes,
            "agent_blocked": self.agent_blocked,
        }
