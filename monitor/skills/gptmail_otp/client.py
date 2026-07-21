from __future__ import annotations

import re
import time
from typing import Any

import requests

from .provider import API_BASE_URL, PublicKeyProvider


CODE_PATTERN = re.compile(r"(?<!\d)(\d{4,8})(?!\d)")


class GPTMailAPIError(RuntimeError):
    pass


class GPTMailClient:
    def __init__(
        self,
        provider: PublicKeyProvider | None = None,
        base_url: str = API_BASE_URL,
        timeout: int = 20,
        allow_destructive: bool = False,
    ) -> None:
        self.provider = provider or PublicKeyProvider(api_base_url=base_url)
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.allow_destructive = allow_destructive

    def _request(self, method: str, path: str, retry_auth: bool = True, **kwargs: Any) -> dict:
        key = self.provider.ensure_fresh()
        headers = dict(kwargs.pop("headers", {}))
        headers["X-API-Key"] = key
        try:
            response = requests.request(
                method, f"{self.base_url}{path}", headers=headers, timeout=self.timeout, **kwargs
            )
        except requests.RequestException as exc:
            raise GPTMailAPIError(f"GPTMail request failed: {type(exc).__name__}") from exc
        if response.status_code in {401, 403} and retry_auth:
            self.provider.ensure_fresh(force=True)
            return self._request(method, path, retry_auth=False, **kwargs)
        try:
            payload = response.json()
        except ValueError as exc:
            raise GPTMailAPIError(f"GPTMail returned HTTP {response.status_code} without JSON") from exc
        if response.status_code >= 400 or payload.get("success") is not True:
            error = str(payload.get("error") or f"HTTP {response.status_code}")[:300]
            raise GPTMailAPIError(error)
        return payload

    def generate_email(self, prefix: str | None = None, domain: str | None = None) -> str:
        body = {key: value for key, value in {"prefix": prefix, "domain": domain}.items() if value}
        payload = self._request("POST", "/api/generate-email", json=body)
        return str(payload["data"]["email"])

    def list_emails(self, email: str) -> list[dict]:
        payload = self._request("GET", "/api/emails", params={"email": email})
        data = payload.get("data", [])
        if isinstance(data, dict):
            data = data.get("emails", [])
        return list(data or [])

    def read_email(self, message_id: str) -> dict:
        return dict(self._request("GET", f"/api/email/{message_id}").get("data") or {})

    def wait_for_code(
        self,
        email: str,
        timeout: int = 180,
        poll_interval: int = 5,
        pattern: re.Pattern[str] = CODE_PATTERN,
    ) -> str:
        deadline = time.monotonic() + timeout
        seen: set[str] = set()
        while time.monotonic() < deadline:
            for item in self.list_emails(email):
                message_id = str(item.get("id", ""))
                if not message_id or message_id in seen:
                    continue
                seen.add(message_id)
                detail = self.read_email(message_id)
                text = "\n".join(
                    str(detail.get(field, "")) for field in ("subject", "text", "html")
                )
                match = pattern.search(text)
                if match:
                    return match.group(1) if match.lastindex else match.group(0)
            time.sleep(max(poll_interval, 1))
        raise TimeoutError("email verification code was not received before timeout")

    def delete_email(self, message_id: str) -> None:
        if not self.allow_destructive:
            raise PermissionError("destructive GPTMail operations are disabled")
        self._request("DELETE", f"/api/email/{message_id}")
