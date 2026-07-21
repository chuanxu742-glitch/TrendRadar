from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from scrapling.fetchers import DynamicFetcher, StealthyFetcher


def _reveal_gptmail_public_key(page: Any) -> None:
    # The Astro island hydrates on idle; the SSR button exists before its click
    # handler is attached, so wait briefly before the first interaction.
    page.wait_for_timeout(1500)
    button = page.get_by_role(
        "button", name="显示并复制公共 API Key", exact=True
    )
    button.click(force=True)
    page.wait_for_timeout(500)


def _headers(page: Any) -> dict[str, str]:
    try:
        return {str(key): str(value) for key, value in dict(page.headers or {}).items()}
    except Exception:
        # Some Patchright responses release the backing object before all_headers
        # is materialized. Headers are useful metadata, but not worth losing a
        # successfully rendered policy page.
        return {}


def _content(page: Any) -> bytes:
    rendered = getattr(page, "html_content", "")
    if rendered:
        return str(rendered).encode("utf-8", errors="replace")
    body = getattr(page, "body", b"")
    return body if isinstance(body, bytes) else str(body).encode("utf-8", errors="replace")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("dynamic", "stealth"), required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--timeout-ms", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--solve-cloudflare",
        action="store_true",
        help="Allow Scrapling to handle a detected Cloudflare challenge.",
    )
    parser.add_argument(
        "--action", choices=("none", "reveal-gptmail-public-key"), default="none"
    )
    args = parser.parse_args()

    args.output.mkdir(parents=True, exist_ok=True)
    try:
        if args.mode == "stealth":
            page = StealthyFetcher.fetch(
                args.url,
                headless=True,
                solve_cloudflare=args.solve_cloudflare,
                timeout=args.timeout_ms,
                network_idle=True,
                hide_canvas=True,
                block_webrtc=True,
                block_ads=True,
                disable_resources=True,
                retries=1,
            )
        else:
            page_action = (
                _reveal_gptmail_public_key
                if args.action == "reveal-gptmail-public-key"
                else None
            )
            page = DynamicFetcher.fetch(
                args.url,
                headless=True,
                timeout=args.timeout_ms,
                network_idle=True,
                block_ads=True,
                disable_resources=True,
                retries=1,
                page_action=page_action,
            )

        headers = _headers(page)
        content = _content(page)
        if args.mode != "static" and content:
            headers["Content-Type"] = "text/html; charset=utf-8"
        (args.output / "content.bin").write_bytes(content)
        metadata = {
            "status_code": int(getattr(page, "status", 0) or 0),
            "url": str(getattr(page, "url", "") or args.url),
            "headers": headers,
        }
        (args.output / "result.json").write_text(
            json.dumps(metadata, ensure_ascii=False), encoding="utf-8"
        )
        return 0
    except Exception as exc:
        (args.output / "error.txt").write_text(
            f"{type(exc).__name__}: {exc}"[:4000], encoding="utf-8"
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
