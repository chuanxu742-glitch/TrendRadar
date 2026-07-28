from __future__ import annotations

import base64
import html
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable

import qrcode

from xhs_monitor.fetcher import load_spider_xhs_login_api, spider_xhs_runtime


PENDING_STATUSES = {"creating", "waiting_scan", "waiting_confirm"}


def qr_svg_data_url(value: str) -> str:
    qr = qrcode.QRCode(version=None, box_size=1, border=2)
    qr.add_data(value)
    qr.make(fit=True)
    matrix = qr.get_matrix()
    module_size = 6
    size = len(matrix) * module_size
    rectangles = []
    for row_index, row in enumerate(matrix):
        for column_index, filled in enumerate(row):
            if filled:
                rectangles.append(
                    f'<rect x="{column_index * module_size}" '
                    f'y="{row_index * module_size}" width="{module_size}" '
                    f'height="{module_size}"/>'
                )
    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {size} {size}" '
        f'width="{size}" height="{size}" shape-rendering="crispEdges">'
        '<rect width="100%" height="100%" fill="white"/>'
        f'<g fill="black">{"".join(rectangles)}</g>'
        f'<title>{html.escape("小红书登录二维码")}</title></svg>'
    )
    encoded = base64.b64encode(svg.encode("utf-8")).decode("ascii")
    return f"data:image/svg+xml;base64,{encoded}"


class LoginSessionManager:
    def __init__(
        self,
        cookie_file: Path,
        *,
        api_loader: Callable[[], Any] = load_spider_xhs_login_api,
        timeout_seconds: int = 180,
        poll_seconds: float = 2,
    ) -> None:
        self.cookie_file = cookie_file
        self.api_loader = api_loader
        self.timeout_seconds = max(int(timeout_seconds), 30)
        self.poll_seconds = max(float(poll_seconds), 0.1)
        self._lock = threading.RLock()
        self._state: dict[str, Any] = {
            "status": "idle",
            "status_label": "尚未开始登录",
            "qr_image": "",
            "expires_at": "",
        }

    def status(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._state)

    def _replace_state(self, **values: Any) -> None:
        with self._lock:
            self._state = {
                "status": str(values.get("status") or "failed"),
                "status_label": str(values.get("status_label") or "登录暂时失败"),
                "qr_image": str(values.get("qr_image") or ""),
                "expires_at": str(values.get("expires_at") or ""),
            }

    def start(self) -> dict[str, Any]:
        with self._lock:
            if self._state["status"] in PENDING_STATUSES:
                return dict(self._state)
            self._state = {
                "status": "creating",
                "status_label": "正在生成二维码",
                "qr_image": "",
                "expires_at": "",
            }
        try:
            with spider_xhs_runtime():
                api = self.api_loader()
                cookies = api.generate_init_cookies()
                success, _message, qr_data = api.generate_qrcode(cookies)
            if not success:
                self._replace_state(
                    status="failed",
                    status_label="二维码生成失败，请重试",
                )
                return self.status()
            deadline = time.monotonic() + self.timeout_seconds
            expires_at = str(int(time.time() + self.timeout_seconds))
            self._replace_state(
                status="waiting_scan",
                status_label="请使用小红书 App 扫码确认",
                qr_image=qr_svg_data_url(str(qr_data["qr_url"])),
                expires_at=expires_at,
            )
            threading.Thread(
                target=self._poll,
                args=(api, qr_data, deadline),
                daemon=True,
            ).start()
        except Exception:
            self._replace_state(
                status="failed",
                status_label="登录服务暂时不可用，请重试",
            )
        return self.status()

    def _poll(self, api: Any, qr_data: dict[str, Any], deadline: float) -> None:
        cookies = qr_data["cookies"]
        while time.monotonic() < deadline:
            try:
                with spider_xhs_runtime():
                    success, message, cookies = api.check_qrcode_status(
                        qr_data["qr_id"],
                        qr_data["code"],
                        cookies,
                    )
                if success:
                    self._finish(api, cookies)
                    return
                if message == "二维码已过期":
                    self._replace_state(
                        status="expired",
                        status_label="二维码已过期，请重新生成",
                    )
                    return
                if "确认" in str(message):
                    self._replace_state(
                        status="waiting_confirm",
                        status_label="已扫码，请在小红书 App 中确认",
                        qr_image=self.status().get("qr_image", ""),
                        expires_at=self.status().get("expires_at", ""),
                    )
            except Exception:
                self._replace_state(
                    status="failed",
                    status_label="登录状态检查失败，请重试",
                )
                return
            time.sleep(self.poll_seconds)
        self._replace_state(
            status="expired",
            status_label="二维码已过期，请重新生成",
        )

    def _finish(self, api: Any, cookies: dict[str, Any]) -> None:
        try:
            with spider_xhs_runtime():
                valid, user_info, cookies = api.get_user_info(cookies)
                if not valid:
                    raise ValueError("invalid login state")
                cookie_value = api.cookies_to_str(cookies)
            self.cookie_file.parent.mkdir(parents=True, exist_ok=True)
            temporary_path = self.cookie_file.with_suffix(
                self.cookie_file.suffix + ".tmp"
            )
            temporary_path.write_text(cookie_value, encoding="utf-8")
            if os.name != "nt":
                temporary_path.chmod(0o600)
            temporary_path.replace(self.cookie_file)
            nickname = str(user_info.get("nickname") or "").strip()
            self._replace_state(
                status="success",
                status_label=f"登录成功{f'：{nickname}' if nickname else ''}",
            )
        except Exception:
            self._replace_state(
                status="failed",
                status_label="登录验证失败，请重新扫码",
            )
