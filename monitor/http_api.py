# coding=utf-8
"""
官方监控 HTTP API

从 official_monitor.py 拆分出来的 HTTP 请求处理器：仪表盘页面、REST 查询接口、
审核/知识库操作接口。业务逻辑仍由 official_monitor 模块提供，这里只负责路由与协议细节。
"""

import hmac
import json
import os
import re
import importlib
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from types import ModuleType
from typing import Any
from urllib.parse import parse_qsl, unquote, urlsplit


class _OfficialMonitorProxy:
    def __init__(self) -> None:
        self.module: ModuleType | None = None

    def bind(self, module: ModuleType) -> None:
        self.module = module

    def __getattr__(self, name: str) -> Any:
        if self.module is None:
            main_module = sys.modules.get("__main__")
            if main_module is not None and hasattr(main_module, "STATE_DIR"):
                self.module = main_module
            else:
                package = __package__
                module_name = f"{package}.official_monitor" if package else "official_monitor"
                self.module = importlib.import_module(module_name)
        return getattr(self.module, name)


om = _OfficialMonitorProxy()


def bind_monitor_module(module: ModuleType) -> None:
    om.bind(module)


class MonitorRequestHandler(SimpleHTTPRequestHandler):
    def send_bytes(self, content: bytes, content_type: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(content)

    def send_json(self, payload: Any, status: int = 200) -> None:
        self.send_bytes(
            json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            "application/json; charset=utf-8",
            status,
        )

    def do_GET(self) -> None:  # noqa: N802
        path = urlsplit(self.path).path
        if path in {"/", "/dashboard", "/dashboard.html"}:
            try:
                self.send_bytes(om.DASHBOARD_PATH.read_bytes(), "text/html; charset=utf-8")
            except OSError as exc:
                self.send_error(500, f"dashboard unavailable: {exc}")
            return
        if path == "/api/overview.json":
            try:
                content = json.dumps(om.dashboard_payload(), ensure_ascii=False).encode("utf-8")
                self.send_bytes(content, "application/json; charset=utf-8")
            except Exception as exc:
                self.send_error(500, f"overview unavailable: {exc}")
            return
        if path == "/api/brief.json":
            try:
                content = json.dumps(om.business_brief_payload(), ensure_ascii=False).encode("utf-8")
                self.send_bytes(content, "application/json; charset=utf-8")
            except Exception as exc:
                self.send_error(500, f"brief unavailable: {exc}")
            return
        if path == "/api/v1/social-intelligence":
            try:
                query = dict(parse_qsl(urlsplit(self.path).query, keep_blank_values=True))
                limit = min(max(int(query.get("limit", "100") or 100), 1), 500)
                self.send_json(om.social_intelligence_payload(limit=limit))
            except (TypeError, ValueError) as exc:
                self.send_json({"error": str(exc)}, 400)
            return
        if path == "/api/v1/xhs/settings":
            try:
                self.send_json(om.xiaohongshu_settings_payload())
            except ValueError as exc:
                self.send_json({"error": str(exc)}, 503)
            return
        if path == "/api/v1/xhs/login/status":
            try:
                self.send_json(om.xiaohongshu_login_status_payload())
            except ValueError as exc:
                self.send_json({"error": str(exc)}, 503)
            return
        if path == "/api/v1/policy-change-digest":
            try:
                query = dict(parse_qsl(urlsplit(self.path).query, keep_blank_values=True))
                limit = min(max(int(query.get("limit", "10000") or 10000), 1), 10000)
                output_format = str(query.get("format") or "json").strip().lower()
                if output_format not in {"json", "text", "markdown", "md"}:
                    raise ValueError("format must be json, text, markdown, or md")
                digest = om.policy_change_digest_payload(
                    start_date=query.get("from", ""),
                    end_date=query.get("to", ""),
                    entity_kind=query.get("kind", ""),
                    period=query.get("period", ""),
                    limit=limit,
                )
                if output_format == "json":
                    self.send_json(digest)
                elif output_format == "text":
                    self.send_bytes(
                        str(digest["text"]).encode("utf-8"),
                        "text/plain; charset=utf-8",
                    )
                else:
                    self.send_bytes(
                        str(digest["markdown"]).encode("utf-8"),
                        "text/markdown; charset=utf-8",
                    )
            except (TypeError, ValueError) as exc:
                self.send_json({"error": str(exc)}, 400)
            return
        if path == "/api/v1/policy-changes":
            try:
                query = dict(parse_qsl(urlsplit(self.path).query, keep_blank_values=True))
                after = max(int(query.get("after", "0") or 0), 0)
                limit = min(max(int(query.get("limit", "100") or 100), 1), 500)
                store = om.monitor_store()
                current_cursor = store.get_change_cursor()
                if query.get("view") == "effective":
                    items = store.list_effective_policy_changes(after_cursor=after, limit=limit)
                    count = store.count_effective_policy_changes()
                    remaining_count = store.count_effective_policy_changes(after_cursor=after)
                else:
                    events = store.read_change_feed(after_cursor=after, limit=limit)
                    items = [
                        {
                            "cursor": event.cursor,
                            "event_type": event.event_type,
                            "occurred_at": event.occurred_at,
                            **dict(event.payload),
                        }
                        for event in events
                    ]
                    count = store.count_change_feed()
                    remaining_count = store.count_change_feed(after_cursor=after)
                has_more = remaining_count > len(items)
                next_cursor = (
                    int(items[-1]["cursor"])
                    if has_more and items
                    else max(after, current_cursor)
                )
                self.send_json({
                    "items": items,
                    "count": count,
                    "page_count": len(items),
                    "remaining_count": remaining_count,
                    "next_cursor": next_cursor,
                    "current_cursor": current_cursor,
                    "has_more": has_more,
                    "limit": limit,
                })
            except (TypeError, ValueError) as exc:
                self.send_json({"error": str(exc)}, 400)
            return
        if path == "/api/v1/site-url-inventory":
            try:
                query = dict(parse_qsl(urlsplit(self.path).query, keep_blank_values=True))
                origin = str(query.get("origin") or "").strip()
                relevance = str(query.get("relevance") or "").strip().lower()
                fetch_status = str(query.get("status") or "").strip().lower()
                if relevance and relevance not in {"high", "medium", "low"}:
                    raise ValueError("relevance must be high, medium, low, or empty")
                limit = min(max(int(query.get("limit", "200") or 200), 1), 5000)
                offset = max(int(query.get("offset", "0") or 0), 0)
                page, count = om.query_site_url_inventory(
                    origin=origin,
                    relevance=relevance,
                    fetch_status=fetch_status,
                    limit=limit,
                    offset=offset,
                )
                next_offset = offset + len(page)
                has_more = next_offset < count
                self.send_json({
                    "summary": om.current_site_url_inventory_summary(),
                    "items": page,
                    "count": count,
                    "page_count": len(page),
                    "offset": offset,
                    "limit": limit,
                    "next_offset": next_offset if has_more else None,
                    "has_more": has_more,
                })
            except (TypeError, ValueError) as exc:
                self.send_json({"error": str(exc)}, 400)
            return
        if path == "/api/v1/manual-sources":
            try:
                query = dict(parse_qsl(urlsplit(self.path).query, keep_blank_values=True))
                limit = min(max(int(query.get("limit", "100") or 100), 1), 500)
                offset = max(int(query.get("offset", "0") or 0), 0)
                items, count = om.list_manual_sources(limit=limit, offset=offset)
                next_offset = offset + len(items)
                has_more = next_offset < count
                self.send_json({
                    "items": [om.source_api_item(item) for item in items],
                    "count": count,
                    "page_count": len(items),
                    "offset": offset,
                    "limit": limit,
                    "next_offset": next_offset if has_more else None,
                    "has_more": has_more,
                    "ai_available": om.source_intake_ai_available(),
                })
            except (TypeError, ValueError) as exc:
                self.send_json({"error": str(exc)}, 400)
            return
        if path == "/api/v1/sources":
            try:
                query = dict(parse_qsl(urlsplit(self.path).query, keep_blank_values=True))
                states = tuple(filter(None, query.get("state", "").split(",")))
                roles = tuple(filter(None, query.get("role", "").split(",")))
                limit = min(max(int(query.get("limit", "500") or 500), 1), 5000)
                offset = max(int(query.get("offset", "0") or 0), 0)
                store = om.monitor_store()
                items = store.list_sources(
                    states=states, roles=roles, limit=limit, offset=offset
                )
                count = store.count_sources(states=states, roles=roles)
                next_offset = offset + len(items)
                has_more = next_offset < count
                self.send_json({
                    "items": [om.source_api_item(item) for item in items],
                    "count": count,
                    "page_count": len(items),
                    "offset": offset,
                    "limit": limit,
                    "next_offset": next_offset if has_more else None,
                    "has_more": has_more,
                })
            except (TypeError, ValueError) as exc:
                self.send_json({"error": str(exc)}, 400)
            return
        if path == "/api/v1/review-tasks":
            try:
                query = dict(parse_qsl(urlsplit(self.path).query, keep_blank_values=True))
                statuses = tuple(filter(None, query.get("status", "open,assigned,in_progress").split(",")))
                limit = min(max(int(query.get("limit", "100") or 100), 1), 500)
                offset = max(int(query.get("offset", "0") or 0), 0)
                store = om.monitor_store()
                items = store.list_review_tasks(
                    statuses=statuses, limit=limit, offset=offset
                )
                count = store.count_review_tasks(statuses=statuses)
                next_offset = offset + len(items)
                has_more = next_offset < count
                self.send_json({
                    "items": [om.review_api_item(item) for item in items],
                    "count": count,
                    "page_count": len(items),
                    "offset": offset,
                    "limit": limit,
                    "next_offset": next_offset if has_more else None,
                    "has_more": has_more,
                })
            except (TypeError, ValueError) as exc:
                self.send_json({"error": str(exc)}, 400)
            return
        if path == "/api/v1/knowledge-updates":
            try:
                query = dict(parse_qsl(urlsplit(self.path).query, keep_blank_values=True))
                statuses = tuple(filter(None, query.get("status", "proposed,approved").split(",")))
                limit = min(max(int(query.get("limit", "100") or 100), 1), 500)
                offset = max(int(query.get("offset", "0") or 0), 0)
                store = om.monitor_store()
                items = store.list_knowledge_update_proposals(
                    statuses=statuses, limit=limit, offset=offset
                )
                count = store.count_knowledge_update_proposals(statuses=statuses)
                next_offset = offset + len(items)
                has_more = next_offset < count
                self.send_json({
                    "items": [om.knowledge_update_api_item(item) for item in items],
                    "count": count,
                    "page_count": len(items),
                    "offset": offset,
                    "limit": limit,
                    "next_offset": next_offset if has_more else None,
                    "has_more": has_more,
                })
            except (TypeError, ValueError) as exc:
                self.send_json({"error": str(exc)}, 400)
            return
        if path == "/api/v1/knowledge-current":
            try:
                query = dict(parse_qsl(urlsplit(self.path).query, keep_blank_values=True))
                limit = min(max(int(query.get("limit", "500") or 500), 1), 5000)
                offset = max(int(query.get("offset", "0") or 0), 0)
                all_items = om.materialized_knowledge_inventory()
                items = all_items[offset:offset + limit]
                count = len(all_items)
                next_offset = offset + len(items)
                has_more = next_offset < count
                self.send_json({
                    "items": items,
                    "count": count,
                    "page_count": len(items),
                    "offset": offset,
                    "limit": limit,
                    "next_offset": next_offset if has_more else None,
                    "has_more": has_more,
                })
            except (TypeError, ValueError) as exc:
                self.send_json({"error": str(exc)}, 400)
            return
        if path in {"/health/live", "/health/ready", "/health/functional"}:
            health = om.load_json(om.STATE_DIR / "status.json", {}).get("functional_health", {})
            if path == "/health/live":
                self.send_json({"status": "ok", "time": om.now_iso()})
            elif path == "/health/ready":
                ready = (om.STATE_DIR / "status.json").exists() and om.monitor_store().journal_mode() in {"wal", "memory"}
                self.send_json({"status": "ready" if ready else "not_ready"}, 200 if ready else 503)
            else:
                status = str(health.get("status") or "unknown")
                self.send_json(health or {"status": status}, 503 if status == "unhealthy" else 200)
            return
        if path in {"/feed.xml", "/ops-feed.xml"}:
            try:
                content = (om.STATE_DIR / path.removeprefix("/")).read_bytes()
                response_status = 200
                if path == "/feed.xml":
                    status_payload = om.load_json(om.STATE_DIR / "status.json", {})
                    functional_health = (
                        status_payload.get("functional_health", {})
                        if isinstance(status_payload, dict)
                        else {}
                    )
                    functional_status = (
                        functional_health.get("status", "")
                        if isinstance(functional_health, dict)
                        else functional_health
                    )
                    if str(functional_status).strip().lower() == "unhealthy":
                        response_status = 503
                self.send_bytes(
                    content,
                    "application/rss+xml; charset=utf-8",
                    response_status,
                )
            except OSError as exc:
                self.send_error(404, f"feed unavailable: {exc}")
            return
        if path == "/inventory.json":
            # 知识库来源清单会出现在 RSS 事件链接里，保留只读访问
            try:
                self.send_bytes(
                    (om.STATE_DIR / "inventory.json").read_bytes(),
                    "application/json; charset=utf-8",
                )
            except OSError as exc:
                self.send_error(404, f"inventory unavailable: {exc}")
            return
        # STATE_DIR 下包含 monitor.db、gptmail 密钥和各类状态文件，
        # 绝不能回落到母类的静态目录服务，未知路径一律 404
        self.send_json({"error": "not found"}, 404)

    def do_HEAD(self) -> None:  # noqa: N802
        # 本服务不提供静态文件浏览：HEAD 一律 404，避免母类回落到文件系统
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _read_json_body(self) -> dict[str, Any]:
        length = min(max(int(self.headers.get("Content-Length", "0") or 0), 0), 64 * 1024)
        if not length:
            return {}
        payload = json.loads(self.rfile.read(length).decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("request body must be a JSON object")
        return payload

    def _require_local_json_write(self) -> None:
        content_type = str(self.headers.get("Content-Type") or "").lower()
        if not content_type.startswith("application/json"):
            raise ValueError("Content-Type must be application/json")
        origin = str(self.headers.get("Origin") or "").strip()
        if not origin:
            return
        origin_host = (urlsplit(origin).hostname or "").lower()
        request_host = str(self.headers.get("Host") or "").split(":", 1)[0].lower()
        if origin_host not in {"localhost", "127.0.0.1", "::1", request_host}:
            raise ValueError("cross-origin source changes are not allowed")

    def _authorize_api_token(self) -> bool:
        """可选 Bearer token 鉴权：未配置 MONITOR_API_TOKEN 时保持原有开放行为。"""
        token = os.getenv("MONITOR_API_TOKEN", "")
        if not token:
            return True
        header = str(self.headers.get("Authorization") or "").strip()
        provided = header[len("Bearer "):].strip() if header.startswith("Bearer ") else ""
        if provided and hmac.compare_digest(provided, token):
            return True
        # 拒绝前先读掉请求体，避免客户端在写入途中收到连接重置
        length = min(max(int(self.headers.get("Content-Length", "0") or 0), 0), 64 * 1024)
        if length:
            try:
                self.rfile.read(length)
            except OSError:
                pass
        self.send_json({"error": "unauthorized"}, 401)
        return False

    def do_POST(self) -> None:  # noqa: N802
        if not self._authorize_api_token():
            return
        path = urlsplit(self.path).path
        try:
            payload = self._read_json_body()
            actor = str(payload.get("actor") or "local-operator")
            if path == "/api/v1/xhs/settings":
                self._require_local_json_write()
                self.send_json(
                    om.update_xiaohongshu_settings(payload.get("keywords"))
                )
                return
            if path == "/api/v1/xhs/login/start":
                self._require_local_json_write()
                self.send_json(om.start_xiaohongshu_login())
                return
            if path == "/api/v1/sources/preview":
                self._require_local_json_write()
                self.send_json(om.preview_manual_source_input(
                    payload.get("input", ""),
                    use_ai=bool(payload.get("use_ai")),
                ))
                return
            if path == "/api/v1/sources":
                self._require_local_json_write()
                items = payload.get("items")
                if not isinstance(items, list):
                    preview = om.preview_manual_source_input(
                        payload.get("input", ""),
                        use_ai=bool(payload.get("use_ai")),
                    )
                    items = [
                        item
                        for item in preview.get("items", [])
                        if item.get("status") == "valid"
                    ]
                result = om.import_manual_sources(items, actor=actor)
                self.send_json(result, 201 if result.get("added") else 200)
                return
            batch_match = re.fullmatch(
                r"/api/v1/source-intake/batches/([^/]+)/actions",
                path,
            )
            if batch_match:
                self._require_local_json_write()
                action = str(payload.get("action") or "").lower()
                if action != "undo":
                    raise ValueError("unsupported source intake batch action")
                self.send_json(om.undo_manual_source_batch(
                    unquote(batch_match.group(1)),
                    actor=actor,
                ))
                return
            review_match = re.fullmatch(r"/api/v1/review-tasks/([^/]+)/actions", path)
            if review_match:
                self._require_local_json_write()
                task_id = unquote(review_match.group(1))
                action = str(payload.get("action") or "").lower()
                task = om.monitor_store().get_review_task(task_id)
                requested_owner = str(payload.get("owner") or "") or None
                owner = requested_owner or task.owner
                resolution = str(payload.get("resolution") or "") or None
                if action == "assign":
                    task = om.monitor_store().transition_review_task(
                        task_id, "assigned", actor=actor, owner=requested_owner or actor,
                    )
                elif action == "start":
                    task = om.monitor_store().transition_review_task(
                        task_id, "in_progress", actor=actor, owner=requested_owner or actor,
                    )
                elif action == "retry":
                    if task.task_type == "source_recovery" and task.source_id:
                        om.schedule_source_revalidation(
                            task.source_id,
                            actor=actor,
                            reason=resolution or "review retry requested",
                        )
                        resume_action = "revalidate_source"
                    elif task.task_type == "change_evidence":
                        om.update_candidate_from_review(
                            task,
                            state="gathering_evidence",
                            actor=actor,
                            reason=resolution or "evidence enrichment requested",
                        )
                        resume_action = "enrich_evidence"
                    else:
                        raise ValueError("review task cannot be retried automatically")
                    task = om.monitor_store().transition_review_task(
                        task_id,
                        "open",
                        actor=actor,
                        owner=owner,
                        resume_action=resume_action,
                        retry_after=om.now_iso(),
                    )
                elif action == "resume":
                    if task.task_type != "source_recovery" or not task.source_id:
                        raise ValueError("resume is only valid for a source recovery task")
                    om.schedule_source_revalidation(
                        task.source_id,
                        actor=actor,
                        reason=resolution or "review resume requested",
                    )
                    task = om.monitor_store().transition_review_task(
                        task_id, "in_progress", actor=actor, owner=requested_owner or actor,
                        resume_action="revalidate_source", retry_after=om.now_iso(),
                    )
                elif action == "reactivate":
                    if task.task_type != "source_recovery" or not task.source_id:
                        raise ValueError("reactivate is only valid for a source recovery task")
                    if not resolution:
                        raise ValueError("reactivate requires an operator resolution")
                    endpoint = om.monitor_store().reactivate_source(
                        task.source_id, actor=actor, reason=resolution, next_due_at=om.now_iso(),
                    )
                    om.mark_source_pending_revalidation(endpoint)
                    task = om.monitor_store().transition_review_task(
                        task_id, "open", actor=actor, owner=owner,
                        resume_action="revalidate_source", retry_after=om.now_iso(),
                    )
                elif action == "retire":
                    if task.task_type != "source_recovery" or not task.source_id:
                        raise ValueError("retire is only valid for a source recovery task")
                    om.monitor_store().transition_source(
                        task.source_id, "retired", reason=resolution or task.reason, force=True,
                    )
                    task = om.monitor_store().transition_review_task(
                        task_id, "resolved", actor=actor, owner=owner,
                        resolution=resolution or "source retired", resume_action="retire_source",
                    )
                elif action in {"resolve", "reject"}:
                    if task.task_type == "change_evidence":
                        candidate_state = str(payload.get("candidate_state") or "").lower()
                        if action == "reject":
                            candidate_state = "rejected"
                        if candidate_state not in {"confirmed", "rejected"}:
                            raise ValueError(
                                "resolving change evidence requires candidate_state confirmed or rejected"
                            )
                        reviewed_candidate = om.update_candidate_from_review(
                            task,
                            state=candidate_state,
                            actor=actor,
                            reason=resolution or action,
                        )
                        if candidate_state == "confirmed":
                            om.publish_confirmed_candidate(reviewed_candidate, actor=actor)
                        else:
                            om.reject_candidate_change(
                                reviewed_candidate,
                                actor=actor,
                                reason=resolution or action,
                            )
                    task = om.monitor_store().transition_review_task(
                        task_id, "resolved", actor=actor, owner=owner,
                        resolution=resolution or action,
                    )
                else:
                    raise ValueError("unsupported review action")
                self.send_json(om.review_api_item(task))
                return
            proposal_match = re.fullmatch(r"/api/v1/knowledge-updates/([^/]+)/actions", path)
            if proposal_match:
                self._require_local_json_write()
                proposal_id = unquote(proposal_match.group(1))
                action = str(payload.get("action") or "").lower()
                owner = str(payload.get("owner") or actor)
                reason = str(payload.get("reason") or "") or None
                if action == "apply":
                    current, materialized_path = om.apply_knowledge_proposal(
                        proposal_id, actor=owner, reason=reason,
                    )
                elif action == "rollback":
                    current, materialized_path = om.rollback_knowledge_proposal(
                        proposal_id, actor=owner, reason=reason,
                    )
                elif action in {"approve", "reject"}:
                    with om.KNOWLEDGE_OPERATION_LOCK:
                        current = om.monitor_store().get_knowledge_update_proposal(proposal_id)
                        if action == "approve":
                            revision = om.monitor_store().get_policy_change_revision(
                                current.policy_change_revision_id
                            )
                            if revision.status != "confirmed":
                                raise ValueError("knowledge update revision is no longer effective")
                        current = om.monitor_store().transition_knowledge_update_proposal(
                            proposal_id,
                            "approved" if action == "approve" else "rejected",
                            owner=owner,
                            reason=reason,
                        )
                    materialized_path = None
                else:
                    raise ValueError("unsupported knowledge update action")
                response = om.knowledge_update_api_item(current)
                if materialized_path is not None:
                    response["materialized_path"] = materialized_path.relative_to(om.STATE_DIR.resolve()).as_posix()
                self.send_json(response)
                return
            self.send_json({"error": "not found"}, 404)
        except KeyError as exc:
            self.send_json({"error": f"not found: {exc}"}, 404)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            self.send_json({"error": str(exc)}, 400)


def serve() -> None:
    # 绑定地址由 MONITOR_BIND_HOST 控制，默认只监听本机；容器部署需显式设为 0.0.0.0
    ThreadingHTTPServer((om.BIND_HOST, om.PORT), MonitorRequestHandler).serve_forever()
