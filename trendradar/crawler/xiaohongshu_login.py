# coding=utf-8
"""使用 Spider_XHS PC 登录模块生成并保存小红书 Cookie。"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

from trendradar.crawler.xiaohongshu import (
    load_spider_xhs_login_api,
    spider_xhs_runtime,
)


def qrcode_login(cookie_file: Path, timeout_seconds: int = 180) -> bool:
    """二维码登录；Cookie 只写入本地文件，不打印到终端。"""
    with spider_xhs_runtime():
        login_api = load_spider_xhs_login_api()
        print("[小红书登录] 正在生成安全参数和二维码")
        cookies = login_api.generate_init_cookies()
        success, message, qr_data = login_api.generate_qrcode(cookies)
        if not success:
            print(f"[小红书登录] 二维码生成失败: {message}")
            return False

        cookies = qr_data["cookies"]
        print("[小红书登录] 请使用小红书 App 扫描并确认：")
        login_api.show_qrcode_terminal(qr_data["qr_url"])

        deadline = time.monotonic() + timeout_seconds
        last_message = ""
        while time.monotonic() < deadline:
            success, message, cookies = login_api.check_qrcode_status(
                qr_data["qr_id"], qr_data["code"], cookies
            )
            if success:
                break
            if message != last_message:
                print(f"[小红书登录] {message}")
                last_message = message
            if message == "二维码已过期":
                return False
            time.sleep(2)
        else:
            print("[小红书登录] 等待扫码超时")
            return False

        valid, user_info, cookies = login_api.get_user_info(cookies)
        if not valid:
            print("[小红书登录] 登录状态验证失败，未保存 Cookie")
            return False

        cookie_value = login_api.cookies_to_str(cookies)

    cookie_file.parent.mkdir(parents=True, exist_ok=True)
    cookie_file.write_text(cookie_value, encoding="utf-8")
    if os.name != "nt":
        cookie_file.chmod(0o600)
    nickname = user_info.get("nickname", "未知用户")
    print(f"[小红书登录] 登录成功: {nickname}")
    print(f"[小红书登录] Cookie 已保存: {cookie_file.resolve()}")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Spider_XHS 小红书二维码登录")
    parser.add_argument(
        "--cookie-file",
        default=os.getenv("XHS_COOKIE_FILE", "config/xhs_cookie.txt"),
        help="Cookie 保存路径（默认 config/xhs_cookie.txt）",
    )
    parser.add_argument("--timeout", type=int, default=180, help="扫码超时秒数")
    args = parser.parse_args()
    if not qrcode_login(Path(args.cookie_file), max(30, args.timeout)):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
