from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable
from zoneinfo import ZoneInfo

import requests


PAGE_URL = "https://mail.chatgpt.org.uk/zh/api/"
API_BASE_URL = "https://mail.chatgpt.org.uk"
TIMEZONE = ZoneInfo("Asia/Shanghai")
KEY_PATTERN = re.compile(r"^(?:sk-[A-Za-z0-9_-]{8,}|gpt-test)$")


class PublicKeyError(RuntimeError):
    pass


class _PublicKeyHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.button_depth = 0
        self.code_depth = 0
        self.parts: list[str] = []

    @staticmethod
    def _is_public_key_button(attrs: list[tuple[str, str | None]]) -> bool:
        values = {key.lower(): value or "" for key, value in attrs}
        label = values.get("aria-label", "").lower()
        return "api key" in label and ("公共" in label or "public" in label)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "button" and self._is_public_key_button(attrs):
            self.button_depth = 1
            return
        if self.button_depth:
            self.button_depth += 1
            if tag == "code":
                self.code_depth = self.button_depth

    def handle_endtag(self, tag: str) -> None:
        if not self.button_depth:
            return
        if tag == "code" and self.code_depth == self.button_depth:
            self.code_depth = 0
        self.button_depth -= 1

    def handle_data(self, data: str) -> None:
        if self.code_depth:
            self.parts.append(data)


def extract_public_key(rendered_html: str) -> str:
    parser = _PublicKeyHTMLParser()
    parser.feed(rendered_html)
    candidates = [html.unescape(part).strip() for part in parser.parts if part.strip()]
    for candidate in candidates:
        if KEY_PATTERN.fullmatch(candidate):
            return candidate
    raise PublicKeyError("rendered page did not contain a valid public API key")


def key_fingerprint(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]


def refresh_cutoff(now: datetime) -> datetime:
    local = now.astimezone(TIMEZONE)
    cutoff = local.replace(hour=8, minute=0, second=0, microsecond=0)
    return cutoff if local >= cutoff else cutoff - timedelta(days=1)


class PublicKeyProvider:
    def __init__(
        self,
        state_path: Path | None = None,
        page_url: str = PAGE_URL,
        api_base_url: str = API_BASE_URL,
        browser_timeout: int = 75,
        request_timeout: int = 20,
        fetch_rendered: Callable[[], str] | None = None,
        validate: Callable[[str], bool] | None = None,
    ) -> None:
        state_dir = Path(os.getenv("MONITOR_STATE_DIR", "/app/state"))
        self.state_path = state_path or state_dir / "gptmail" / "public-key.json"
        self.page_url = page_url
        self.api_base_url = api_base_url.rstrip("/")
        self.browser_timeout = max(browser_timeout, 30)
        self.request_timeout = request_timeout
        self._fetch_rendered_override = fetch_rendered
        self._validate_override = validate

    def _load(self) -> dict:
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            if KEY_PATTERN.fullmatch(str(data.get("key", ""))):
                return data
        except (OSError, json.JSONDecodeError):
            pass
        return {}

    def _save(self, key: str, now: datetime) -> dict:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "key": key,
            "fingerprint": key_fingerprint(key),
            "source": self.page_url,
            "refreshed_at": now.astimezone(TIMEZONE).isoformat(),
        }
        temporary = self.state_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")
        try:
            os.chmod(temporary, 0o600)
        except OSError:
            pass
        temporary.replace(self.state_path)
        return record

    def _fetch_rendered(self) -> str:
        if self._fetch_rendered_override:
            return self._fetch_rendered_override()
        worker = Path(__file__).resolve().parents[2] / "browser_fetch_worker.py"
        with tempfile.TemporaryDirectory(prefix="gptmail-key-") as temporary:
            output = Path(temporary)
            command = [
                sys.executable,
                str(worker),
                "--mode",
                "dynamic",
                "--url",
                self.page_url,
                "--timeout-ms",
                str(self.browser_timeout * 1000),
                "--output",
                str(output),
                "--action",
                "reveal-gptmail-public-key",
            ]
            try:
                completed = subprocess.run(
                    command,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=self.browser_timeout + 15,
                    check=False,
                )
            except subprocess.TimeoutExpired as exc:
                raise PublicKeyError("public key page rendering timed out") from exc
            if completed.returncode != 0:
                raise PublicKeyError("public key page rendering failed")
            try:
                return (output / "content.bin").read_text(encoding="utf-8")
            except OSError as exc:
                raise PublicKeyError("rendered public key page was not returned") from exc

    def _validate(self, key: str) -> bool:
        if self._validate_override:
            return self._validate_override(key)
        try:
            response = requests.get(
                f"{self.api_base_url}/api/stats",
                headers={"X-API-Key": key},
                timeout=self.request_timeout,
            )
            payload = response.json()
        except (requests.RequestException, ValueError):
            return False
        return response.status_code == 200 and payload.get("success") is True

    def refresh(self, now: datetime | None = None) -> dict:
        current_time = now or datetime.now(TIMEZONE)
        key = extract_public_key(self._fetch_rendered())
        if not self._validate(key):
            raise PublicKeyError("public API key validation failed")
        return self._save(key, current_time)

    def ensure_fresh(self, force: bool = False, now: datetime | None = None) -> str:
        current_time = now or datetime.now(TIMEZONE)
        record = self._load()
        if record and not force:
            try:
                refreshed = datetime.fromisoformat(record["refreshed_at"])
                if refreshed >= refresh_cutoff(current_time):
                    return record["key"]
            except (KeyError, TypeError, ValueError):
                pass
        try:
            return self.refresh(current_time)["key"]
        except PublicKeyError:
            if record:
                return record["key"]
            raise

    def status(self) -> dict:
        record = self._load()
        return {
            "available": bool(record),
            "fingerprint": record.get("fingerprint", ""),
            "refreshed_at": record.get("refreshed_at", ""),
            "source": self.page_url,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="GPTMail public key refresher")
    parser.add_argument("mode", choices=("once", "watch"), nargs="?", default="once")
    parser.add_argument("--interval", type=int, default=900)
    args = parser.parse_args()
    provider = PublicKeyProvider()
    while True:
        try:
            provider.ensure_fresh()
            print(json.dumps(provider.status(), ensure_ascii=False), flush=True)
        except PublicKeyError as exc:
            print(f"[gptmail-key] refresh failed: {exc}", file=sys.stderr, flush=True)
            if args.mode == "once":
                return 1
        if args.mode == "once":
            return 0
        time.sleep(max(args.interval, 60))


if __name__ == "__main__":
    raise SystemExit(main())
