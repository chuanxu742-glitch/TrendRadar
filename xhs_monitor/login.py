from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from typing import Any, Callable

from xhs_monitor.fetcher import load_spider_xhs_login_api, spider_xhs_runtime


def qrcode_login(
    cookie_file: Path,
    timeout_seconds: int = 180,
    *,
    api_loader: Callable[[], Any] = load_spider_xhs_login_api,
) -> bool:
    """Run the standalone QR login and persist the session only to a file."""

    with spider_xhs_runtime():
        login_api = api_loader()
        print("[小红书登录] 正在生成二维码", flush=True)
        cookies = login_api.generate_init_cookies()
        success, message, qr_data = login_api.generate_qrcode(cookies)
        if not success:
            print(f"[小红书登录] 二维码生成失败: {message}", flush=True)
            return False

        cookies = qr_data["cookies"]
        print("[小红书登录] 请使用小红书 App 扫描并确认：", flush=True)
        login_api.show_qrcode_terminal(qr_data["qr_url"])

        deadline = time.monotonic() + timeout_seconds
        last_message = ""
        while time.monotonic() < deadline:
            success, message, cookies = login_api.check_qrcode_status(
                qr_data["qr_id"],
                qr_data["code"],
                cookies,
            )
            if success:
                break
            if message != last_message:
                print(f"[小红书登录] {message}", flush=True)
                last_message = message
            if message == "二维码已过期":
                return False
            time.sleep(2)
        else:
            print("[小红书登录] 等待扫码超时", flush=True)
            return False

        valid, user_info, cookies = login_api.get_user_info(cookies)
        if not valid:
            print("[小红书登录] 登录状态验证失败，未保存 Cookie", flush=True)
            return False
        cookie_value = login_api.cookies_to_str(cookies)

    cookie_file.parent.mkdir(parents=True, exist_ok=True)
    cookie_file.write_text(cookie_value, encoding="utf-8")
    if os.name != "nt":
        cookie_file.chmod(0o600)
    print(
        f"[小红书登录] 登录成功: {user_info.get('nickname', '未知用户')}",
        flush=True,
    )
    print("[小红书登录] 登录状态已安全保存", flush=True)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="独立小红书二维码登录")
    parser.add_argument(
        "--cookie-file",
        default=os.getenv("XHS_COOKIE_FILE", "/app/config/xhs_cookie.txt"),
        help="登录状态文件路径",
    )
    parser.add_argument("--timeout", type=int, default=180, help="扫码超时秒数")
    args = parser.parse_args()
    if not qrcode_login(Path(args.cookie_file), max(30, args.timeout)):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
