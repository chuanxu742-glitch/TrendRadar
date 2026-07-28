"""Synthetic release gate for the official policy digest."""

from __future__ import annotations

import argparse
import json
import math
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return math.inf
    ordered = sorted(values)
    index = max(0, min(math.ceil(percentile * len(ordered)) - 1, len(ordered) - 1))
    return ordered[index]


def _get_json(url: str, timeout: float) -> tuple[int, dict[str, Any], float]:
    started = time.perf_counter()
    request = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=timeout) as response:  # noqa: S310
            status = response.status
            payload = response.read()
    except HTTPError as exc:
        status = exc.code
        payload = exc.read()
    elapsed_ms = (time.perf_counter() - started) * 1000
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        value = {}
    return status, value if isinstance(value, dict) else {}, elapsed_ms


def validate_digest_contract(digest: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    counts = digest.get("counts", {})
    groups = [
        *(digest.get("country_groups") or []),
        *(digest.get("airline_groups") or []),
        *(digest.get("other_groups") or []),
    ]
    changes = [
        change
        for group in groups
        if isinstance(group, dict)
        for change in group.get("changes", [])
        if isinstance(change, dict)
    ]
    if int(counts.get("changes", -1)) != len(changes):
        failures.append("digest count does not match grouped changes")
    for change in changes:
        if not change.get("evidence_bundle_id"):
            failures.append(f"{change.get('change_id', 'unknown')} has no evidence bundle")
        if change.get("effective_date") and not change.get("effective_date_source"):
            failures.append(f"{change.get('change_id', 'unknown')} has an unsourced effective date")
        if change.get("announcement_date") and not change.get("announcement_date_source"):
            failures.append(f"{change.get('change_id', 'unknown')} has an unsourced announcement date")
        if change.get("official_reason_status") == "sourced":
            if not change.get("official_reason"):
                failures.append(f"{change.get('change_id', 'unknown')} has an empty sourced reason")
        elif change.get("official_reason"):
            failures.append(f"{change.get('change_id', 'unknown')} exposes an inferred reason")
    return failures


def run_validation(
    base_url: str,
    *,
    samples: int = 5,
    timeout: float = 5.0,
    max_p95_ms: float = 2000.0,
) -> dict[str, Any]:
    base = base_url.rstrip("/")
    failures: list[str] = []
    checks: list[dict[str, Any]] = []
    latencies: list[float] = []
    endpoints = (
        ("/health/live", {"ok"}),
        ("/health/ready", {"ready"}),
        ("/health/functional", {"healthy", "degraded"}),
    )
    for path, allowed in endpoints:
        try:
            status, payload, latency = _get_json(base + path, timeout)
        except (OSError, URLError) as exc:
            failures.append(f"{path} unavailable: {exc}")
            continue
        checks.append({"path": path, "status_code": status, "latency_ms": round(latency, 2)})
        latencies.append(latency)
        if status != 200 or str(payload.get("status")) not in allowed:
            failures.append(f"{path} failed with HTTP {status} and status={payload.get('status')}")
    digest: dict[str, Any] = {}
    for _ in range(max(samples, 1)):
        try:
            status, payload, latency = _get_json(
                base + "/api/v1/policy-change-digest?period=daily",
                timeout,
            )
        except (OSError, URLError) as exc:
            failures.append(f"policy digest unavailable: {exc}")
            break
        latencies.append(latency)
        digest = payload
        if status != 200:
            failures.append(f"policy digest failed with HTTP {status}")
            break
    failures.extend(validate_digest_contract(digest))
    p95_ms = _percentile(latencies, 0.95)
    if p95_ms > max_p95_ms:
        failures.append(f"p95 latency {p95_ms:.2f}ms exceeds {max_p95_ms:.2f}ms")
    return {
        "status": "passed" if not failures else "failed",
        "generated_at": datetime.now().astimezone().isoformat(),
        "base_url": base,
        "checks": checks,
        "samples": max(samples, 1),
        "p95_latency_ms": round(p95_ms, 2) if math.isfinite(p95_ms) else None,
        "digest_changes": int(digest.get("counts", {}).get("changes", 0) or 0),
        "failures": failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate a deployed official policy digest")
    parser.add_argument("--url", default="http://127.0.0.1:8090")
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--max-p95-ms", type=float, default=2000.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = run_validation(
        args.url,
        samples=args.samples,
        timeout=args.timeout,
        max_p95_ms=args.max_p95_ms,
    )
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
