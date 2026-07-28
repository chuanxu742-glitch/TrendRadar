"""Client for the official-monitor structured HTTP API."""

from __future__ import annotations

import json
import os
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class OfficialMonitorAPIError(RuntimeError):
    """Raised when the official monitor cannot return structured data."""


class OfficialMonitorService:
    """Read-only access to official source, change and workflow records."""

    def __init__(self, base_url: str | None = None, timeout: float | None = None):
        self.base_url = (
            base_url
            or os.getenv("OFFICIAL_MONITOR_URL")
            or "http://127.0.0.1:8090"
        ).rstrip("/")
        self.timeout = timeout or float(os.getenv("OFFICIAL_MONITOR_TIMEOUT", "10"))

    @staticmethod
    def _csv(values: Iterable[str] | None) -> str | None:
        if values is None:
            return None
        normalized = [str(value).strip() for value in values if str(value).strip()]
        return ",".join(normalized) or None

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        query = urlencode(
            {
                key: value
                for key, value in params.items()
                if value is not None and value != ""
            }
        )
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"
        request = Request(url, headers={"Accept": "application/json"})

        try:
            with urlopen(request, timeout=self.timeout) as response:  # noqa: S310
                payload = response.read()
        except HTTPError as exc:
            detail = exc.read(512).decode("utf-8", errors="replace")
            raise OfficialMonitorAPIError(
                f"official monitor returned HTTP {exc.code}: {detail}"
            ) from exc
        except (OSError, URLError) as exc:
            raise OfficialMonitorAPIError(
                f"official monitor is unavailable at {self.base_url}: {exc}"
            ) from exc

        try:
            result = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise OfficialMonitorAPIError(
                "official monitor returned invalid JSON"
            ) from exc
        if not isinstance(result, dict):
            raise OfficialMonitorAPIError(
                "official monitor returned a non-object JSON response"
            )
        return result

    def get_sources(
        self,
        *,
        states: Iterable[str] | None = None,
        roles: Iterable[str] | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> dict[str, Any]:
        return self._get(
            "/api/v1/sources",
            {
                "state": self._csv(states),
                "role": self._csv(roles),
                "limit": limit,
                "offset": max(offset, 0) or None,
            },
        )

    def get_functional_health(self) -> dict[str, Any]:
        return self._get("/health/functional", {})

    def get_policy_changes(
        self,
        *,
        view: str = "effective",
        after_cursor: int = 0,
        limit: int = 100,
    ) -> dict[str, Any]:
        normalized_view = view.strip().lower()
        if normalized_view not in {"effective", "events"}:
            raise ValueError("view must be 'effective' or 'events'")
        return self._get(
            "/api/v1/policy-changes",
            {
                "view": "effective" if normalized_view == "effective" else None,
                "after": max(after_cursor, 0),
                "limit": limit,
            },
        )

    def get_policy_change_digest(
        self,
        *,
        start_date: str = "",
        end_date: str = "",
        entity_kind: str = "",
        period: str = "",
        limit: int = 10000,
    ) -> dict[str, Any]:
        normalized_kind = entity_kind.strip().lower()
        if normalized_kind not in {"", "country", "airline", "other"}:
            raise ValueError("entity_kind must be country, airline, other, or empty")
        normalized_period = period.strip().lower()
        if normalized_period not in {"", "all", "daily", "weekly", "monthly"}:
            raise ValueError("period must be daily, weekly, monthly, all, or empty")
        if normalized_period not in {"", "all"} and (start_date or end_date):
            raise ValueError("period cannot be combined with start_date or end_date")
        return self._get(
            "/api/v1/policy-change-digest",
            {
                "from": start_date.strip(),
                "to": end_date.strip(),
                "kind": normalized_kind,
                "period": normalized_period,
                "limit": min(max(limit, 1), 10000),
            },
        )

    def get_review_tasks(
        self,
        *,
        statuses: Iterable[str] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        return self._get(
            "/api/v1/review-tasks",
            {
                "status": self._csv(statuses),
                "limit": limit,
                "offset": max(offset, 0) or None,
            },
        )

    def get_knowledge_updates(
        self,
        *,
        statuses: Iterable[str] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        return self._get(
            "/api/v1/knowledge-updates",
            {
                "status": self._csv(statuses),
                "limit": limit,
                "offset": max(offset, 0) or None,
            },
        )

    def get_current_knowledge(self, *, limit: int = 500, offset: int = 0) -> dict[str, Any]:
        """Return the materialized current-rule overlay after approved updates."""

        return self._get(
            "/api/v1/knowledge-current",
            {
                "limit": limit if limit != 500 else None,
                "offset": max(offset, 0) or None,
            },
        )
