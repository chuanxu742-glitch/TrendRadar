# coding=utf-8
"""
RSS 数据存储 Mixin

从 SQLiteStorageMixin 拆分出来，专注 RSS 抓取数据、官方政策变化追踪的持久化逻辑。
"""

import sqlite3
from typing import Dict, List, Optional

from trendradar.storage.base import RSSItem, RSSData


OFFICIAL_CHANGE_FEED_ID = "official-source-changes"
OFFICIAL_CHANGE_CHECKPOINT_DATE = "__official-change-checkpoint__"
CHANGE_STATUSES = {"confirmed", "retracted", "superseded"}


class RSSStorageMixin:
    """
    RSS 相关的 SQLite 存储操作

    依赖宿主类（最终与 SQLiteStorageMixin 其余部分组合）实现的抽象方法：
    - _get_connection(date, db_type) -> sqlite3.Connection
    - _get_configured_time() -> datetime
    - _format_date_folder(date) -> str
    - _format_time_filename() -> str
    """

    # ========================================
    # RSS 数据存储
    # ========================================

    @staticmethod
    def _split_supersedes(value: str) -> List[str]:
        if not value:
            return []
        normalized = str(value).replace(";", ",").replace("\n", ",")
        return [part.strip() for part in normalized.split(",") if part.strip()]

    def _invalidate_rss_ai_state(
        self,
        date: str,
        rss_item_ids: set[int],
        now_str: str,
    ) -> int:
        """废弃已变化/停用 RSS 条目的 AI 结果，并允许有效修订重新分类。"""
        if not rss_item_ids:
            return 0

        news_conn = self._get_connection(date)
        cursor = news_conn.cursor()
        ids = sorted(rss_item_ids)
        placeholders = ",".join("?" * len(ids))
        cursor.execute(f"""
            UPDATE ai_filter_results
            SET status = 'deprecated', deprecated_at = ?
            WHERE source_type = 'rss'
              AND status = 'active'
              AND news_item_id IN ({placeholders})
        """, [now_str, *ids])
        deprecated_count = cursor.rowcount
        cursor.execute(f"""
            DELETE FROM ai_filter_analyzed_news
            WHERE source_type = 'rss'
              AND news_item_id IN ({placeholders})
        """, ids)
        analyzed_deleted_count = cursor.rowcount
        news_conn.commit()
        return len(ids) if deprecated_count or analyzed_deleted_count else 0

    def _save_rss_data_impl(
        self,
        data: RSSData,
        log_prefix: str = "[存储]",
    ) -> tuple[bool, int, int, int, int]:
        """
        保存 RSS 数据到 SQLite。普通源按 guid/url 去重，结构化变化源按 change_id 去重。

        Args:
            data: RSS 数据
            log_prefix: 日志前缀

        Returns:
            (success, new_count, updated_count, deactivated_count, ai_state_changed_count)
        """
        try:
            conn = self._get_connection(data.date, db_type="rss")
            cursor = conn.cursor()

            now_str = self._get_configured_time().strftime("%Y-%m-%d %H:%M:%S")

            # 同步 RSS 源信息到 rss_feeds 表
            for feed_id, feed_name in data.id_to_name.items():
                cursor.execute("""
                    INSERT INTO rss_feeds (id, name, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        name = excluded.name,
                        updated_at = excluded.updated_at
                """, (feed_id, feed_name, now_str))

            new_count = 0
            updated_count = 0
            deactivated_count = 0
            ai_invalidated_ids: set[int] = set()

            for feed_id, rss_list in data.items.items():
                for item in rss_list:
                    try:
                        item_guid = str(getattr(item, "guid", "") or "").strip()
                        change_id = str(getattr(item, "change_id", "") or "").strip()
                        revision = max(int(getattr(item, "revision", 0) or 0), 0)
                        status = str(getattr(item, "status", "") or "").strip().lower()
                        if status not in CHANGE_STATUSES:
                            status = ""
                        supersedes = str(getattr(item, "supersedes", "") or "").strip()
                        incoming_active = int(status not in {"retracted", "superseded"})
                        existing = None

                        # 去重优先级：change_id > guid > url
                        if change_id:
                            cursor.execute("""
                                SELECT * FROM rss_items
                                WHERE change_id = ? AND feed_id = ?
                            """, (change_id, feed_id))
                            existing = cursor.fetchone()

                        if not existing and item_guid:
                            cursor.execute("""
                                SELECT * FROM rss_items
                                WHERE guid = ? AND feed_id = ?
                            """, (item_guid, feed_id))
                            existing = cursor.fetchone()

                        if not existing and item.url:
                            cursor.execute("""
                                SELECT * FROM rss_items
                                WHERE url = ? AND feed_id = ?
                            """, (item.url, feed_id))
                            existing = cursor.fetchone()

                        if existing:
                            existing_id = int(existing["id"])
                            existing_title = existing["title"]
                            update_title = item.title
                            if (update_title and update_title.strip().startswith(("http://", "https://", "//"))
                                    and existing_title and not existing_title.strip().startswith(("http://", "https://", "//"))):
                                update_title = existing_title

                            existing_revision = int(existing["revision"] or 0)
                            stale_revision = bool(revision and existing_revision > revision)
                            if stale_revision:
                                cursor.execute("""
                                    UPDATE rss_items SET
                                        last_crawl_time = ?,
                                        crawl_count = crawl_count + 1,
                                        updated_at = ?
                                    WHERE id = ?
                                """, (data.crawl_time, now_str, existing_id))
                                updated_count += 1
                                continue

                            revision_changed = bool(revision and revision > existing_revision)
                            substantive_change = any((
                                update_title != existing["title"],
                                bool(item.url) and item.url != (existing["url"] or ""),
                                bool(item_guid) and item_guid != (existing["guid"] or ""),
                                bool(change_id) and change_id != (existing["change_id"] or ""),
                                revision_changed,
                                status != (existing["status"] or ""),
                                supersedes != (existing["supersedes"] or ""),
                                incoming_active != int(existing["is_active"]),
                                item.summary != (existing["summary"] or ""),
                            ))
                            revision_crawl_time = (
                                data.crawl_time
                                if revision_changed or not existing["revision_crawl_time"]
                                else existing["revision_crawl_time"]
                            )
                            needs_ai_sync = int(
                                substantive_change and feed_id == OFFICIAL_CHANGE_FEED_ID
                            )
                            cursor.execute("""
                                UPDATE rss_items SET
                                    title = ?,
                                    url = CASE WHEN ? != '' THEN ? ELSE url END,
                                    guid = CASE WHEN ? != '' THEN ? ELSE guid END,
                                    change_id = CASE WHEN ? != '' THEN ? ELSE change_id END,
                                    revision = CASE WHEN ? > 0 THEN ? ELSE revision END,
                                    status = ?,
                                    supersedes = ?,
                                    is_active = ?,
                                    deactivated_at = CASE WHEN ? = 1 THEN NULL ELSE COALESCE(deactivated_at, ?) END,
                                    revision_crawl_time = ?,
                                    ai_sync_pending = CASE WHEN ? = 1 THEN 1 ELSE ai_sync_pending END,
                                    published_at = ?,
                                    summary = ?,
                                    author = ?,
                                    last_crawl_time = ?,
                                    crawl_count = crawl_count + 1,
                                    updated_at = ?
                                WHERE id = ?
                            """, (update_title,
                                  item.url, item.url,
                                  item_guid, item_guid,
                                  change_id, change_id,
                                  revision, revision,
                                  status, supersedes, incoming_active,
                                  incoming_active, now_str, revision_crawl_time,
                                  needs_ai_sync,
                                  item.published_at, item.summary,
                                  item.author, data.crawl_time, now_str, existing_id))
                            updated_count += 1
                            if substantive_change and feed_id == OFFICIAL_CHANGE_FEED_ID:
                                ai_invalidated_ids.add(existing_id)
                            if (feed_id == OFFICIAL_CHANGE_FEED_ID
                                    and int(existing["is_active"]) and not incoming_active):
                                deactivated_count += 1
                        elif item.url or item_guid or change_id:
                            try:
                                cursor.execute("""
                                    INSERT INTO rss_items
                                    (title, feed_id, url, guid, change_id, revision, status,
                                     supersedes, is_active, deactivated_at, revision_crawl_time,
                                     published_at, summary, author,
                                     first_crawl_time, last_crawl_time, crawl_count,
                                     created_at, updated_at)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                                """, (item.title, feed_id, item.url, item_guid,
                                      change_id, revision, status, supersedes, incoming_active,
                                      None if incoming_active else now_str,
                                      data.crawl_time if revision else "",
                                      item.published_at, item.summary, item.author,
                                      data.crawl_time, data.crawl_time, now_str, now_str))
                                new_count += 1
                            except sqlite3.IntegrityError:
                                if feed_id == OFFICIAL_CHANGE_FEED_ID:
                                    raise

                    except sqlite3.Error as e:
                        print(f"{log_prefix} 保存 RSS 条目失败 [{item.title[:30]}...]: {e}")
                        if feed_id == OFFICIAL_CHANGE_FEED_ID:
                            raise

            # 官方政策变化 feed 是权威有效集合：成功抓取后，缺失项即停用。
            # 抓取失败时 feed_id 不会出现在 data.items，因此不会执行撤销。
            if (
                OFFICIAL_CHANGE_FEED_ID in data.items
                and OFFICIAL_CHANGE_FEED_ID not in data.failed_ids
                and OFFICIAL_CHANGE_FEED_ID in data.authoritative_complete_ids
            ):
                seen_keys: set[str] = set()
                superseding_items: List[RSSItem] = []
                for item in data.items[OFFICIAL_CHANGE_FEED_ID]:
                    stable_key = str(
                        getattr(item, "change_id", "")
                        or getattr(item, "guid", "")
                        or item.url
                    ).strip()
                    if stable_key:
                        seen_keys.add(stable_key)
                    if getattr(item, "supersedes", ""):
                        superseding_items.append(item)

                cursor.execute("""
                    SELECT id, change_id, guid, url, is_active
                    FROM rss_items
                    WHERE feed_id = ?
                """, (OFFICIAL_CHANGE_FEED_ID,))
                for row in cursor.fetchall():
                    stable_key = str(row["change_id"] or row["guid"] or row["url"] or "").strip()
                    if stable_key not in seen_keys and int(row["is_active"]):
                        cursor.execute("""
                            UPDATE rss_items
                            SET status = 'retracted', is_active = 0,
                                deactivated_at = ?, ai_sync_pending = 1, updated_at = ?
                            WHERE id = ?
                        """, (now_str, now_str, row["id"]))
                        deactivated_count += 1
                        ai_invalidated_ids.add(int(row["id"]))

                # confirmed 变更可显式声明替代旧 change_id。
                for item in superseding_items:
                    if str(getattr(item, "status", "") or "").lower() != "confirmed":
                        continue
                    targets = self._split_supersedes(getattr(item, "supersedes", ""))
                    current_change_id = str(getattr(item, "change_id", "") or "").strip()
                    targets = [target for target in targets if target != current_change_id]
                    if not targets:
                        continue
                    placeholders = ",".join("?" * len(targets))
                    cursor.execute(f"""
                        SELECT id, is_active FROM rss_items
                        WHERE feed_id = ? AND change_id IN ({placeholders})
                    """, [OFFICIAL_CHANGE_FEED_ID, *targets])
                    target_rows = cursor.fetchall()
                    for target in target_rows:
                        if int(target["is_active"]):
                            deactivated_count += 1
                        ai_invalidated_ids.add(int(target["id"]))
                    cursor.execute(f"""
                        UPDATE rss_items
                        SET status = 'superseded', is_active = 0,
                            deactivated_at = COALESCE(deactivated_at, ?),
                            ai_sync_pending = 1, updated_at = ?
                        WHERE feed_id = ? AND change_id IN ({placeholders})
                    """, [now_str, now_str, OFFICIAL_CHANGE_FEED_ID, *targets])

                # 持久化待办保证 AI 状态同步失败后，下个成功周期会继续重试。
                cursor.execute("""
                    SELECT id FROM rss_items
                    WHERE feed_id = ? AND ai_sync_pending = 1
                """, (OFFICIAL_CHANGE_FEED_ID,))
                ai_invalidated_ids.update(int(row[0]) for row in cursor.fetchall())

            total_items = new_count + updated_count

            # 记录抓取信息
            cursor.execute("""
                INSERT OR REPLACE INTO rss_crawl_records
                (crawl_time, total_items, created_at)
                VALUES (?, ?, ?)
            """, (data.crawl_time, total_items, now_str))

            # 记录抓取状态
            cursor.execute("""
                SELECT id FROM rss_crawl_records WHERE crawl_time = ?
            """, (data.crawl_time,))
            record_row = cursor.fetchone()
            if record_row:
                crawl_record_id = record_row[0]

                # 记录成功的源
                for feed_id in data.items.keys():
                    cursor.execute("""
                        INSERT OR REPLACE INTO rss_crawl_status
                        (crawl_record_id, feed_id, status)
                        VALUES (?, ?, 'success')
                    """, (crawl_record_id, feed_id))

                # 记录失败的源
                for failed_id in data.failed_ids:
                    cursor.execute("""
                        INSERT OR IGNORE INTO rss_feeds (id, name, updated_at)
                        VALUES (?, ?, ?)
                    """, (failed_id, failed_id, now_str))

                    cursor.execute("""
                        INSERT OR REPLACE INTO rss_crawl_status
                        (crawl_record_id, feed_id, status)
                        VALUES (?, ?, 'failed')
                    """, (crawl_record_id, failed_id))

            conn.commit()

            ai_state_changed_count = self._invalidate_rss_ai_state(
                data.date, ai_invalidated_ids, now_str
            )
            if ai_invalidated_ids:
                placeholders = ",".join("?" * len(ai_invalidated_ids))
                cursor.execute(f"""
                    UPDATE rss_items SET ai_sync_pending = 0
                    WHERE id IN ({placeholders})
                """, sorted(ai_invalidated_ids))
                conn.commit()
            return True, new_count, updated_count, deactivated_count, ai_state_changed_count

        except Exception as e:
            if "conn" in locals():
                conn.rollback()
            print(f"{log_prefix} 保存 RSS 数据失败: {e}")
            return False, 0, 0, 0, 0

    def _get_rss_data_impl(self, date: Optional[str] = None) -> Optional[RSSData]:
        """
        获取指定日期的所有 RSS 数据

        Args:
            date: 日期字符串（YYYY-MM-DD），默认为今天

        Returns:
            RSSData 对象，如果没有数据返回 None
        """
        try:
            conn = self._get_connection(date, db_type="rss")
            cursor = conn.cursor()

            # 获取所有 RSS 数据
            cursor.execute("""
                SELECT i.id, i.title, i.feed_id, f.name as feed_name,
                       i.url, i.guid, i.published_at, i.summary, i.author,
                       i.first_crawl_time, i.last_crawl_time, i.crawl_count,
                       i.change_id, i.revision, i.status, i.supersedes, i.is_active
                FROM rss_items i
                LEFT JOIN rss_feeds f ON i.feed_id = f.id
                WHERE i.is_active = 1
                ORDER BY i.published_at DESC
            """)

            rows = cursor.fetchall()
            if not rows:
                return None

            items: Dict[str, List[RSSItem]] = {}
            id_to_name: Dict[str, str] = {}
            crawl_date = self._format_date_folder(date)

            for row in rows:
                feed_id = row[2]
                feed_name = row[3] or feed_id

                id_to_name[feed_id] = feed_name

                if feed_id not in items:
                    items[feed_id] = []

                items[feed_id].append(RSSItem(
                    title=row[1],
                    feed_id=feed_id,
                    feed_name=feed_name,
                    url=row[4] or "",
                    guid=row[5] or "",
                    published_at=row[6] or "",
                    summary=row[7] or "",
                    author=row[8] or "",
                    crawl_time=row[10],
                    first_time=row[9],
                    last_time=row[10],
                    count=row[11],
                    change_id=row[12] or "",
                    revision=int(row[13] or 0),
                    status=row[14] or "",
                    supersedes=row[15] or "",
                    is_active=bool(row[16]),
                ))

            # 获取最新的抓取时间
            cursor.execute("""
                SELECT crawl_time FROM rss_crawl_records
                ORDER BY crawl_time DESC
                LIMIT 1
            """)
            time_row = cursor.fetchone()
            crawl_time = time_row[0] if time_row else self._format_time_filename()

            # 获取失败的源
            cursor.execute("""
                SELECT DISTINCT cs.feed_id
                FROM rss_crawl_status cs
                JOIN rss_crawl_records cr ON cs.crawl_record_id = cr.id
                WHERE cs.status = 'failed'
            """)
            failed_ids = [row[0] for row in cursor.fetchall()]

            return RSSData(
                date=crawl_date,
                crawl_time=crawl_time,
                items=items,
                id_to_name=id_to_name,
                failed_ids=failed_ids,
            )

        except Exception as e:
            print(f"[存储] 读取 RSS 数据失败: {e}")
            return None

    def _detect_official_change_revisions(
        self,
        current_data: RSSData,
    ) -> List[RSSItem]:
        """Return active policy revisions that have not been delivered successfully."""

        if (
            OFFICIAL_CHANGE_FEED_ID not in current_data.items
            or OFFICIAL_CHANGE_FEED_ID not in current_data.authoritative_complete_ids
        ):
            return []

        items = current_data.items[OFFICIAL_CHANGE_FEED_ID]
        cache_key = (
            current_data.date,
            current_data.crawl_time,
            tuple(
                (
                    str(item.change_id or item.guid or item.url),
                    int(item.revision or 1),
                    str(item.status or "confirmed"),
                )
                for item in items
            ),
        )
        cache = getattr(self, "_official_change_detection_cache", {})
        if cache_key in cache:
            return list(cache[cache_key])

        checkpoint = self._get_connection(
            OFFICIAL_CHANGE_CHECKPOINT_DATE,
            db_type="rss",
        )
        rows = checkpoint.execute("""
            SELECT change_id, revision, status, delivery_state
            FROM official_change_checkpoint
        """).fetchall()
        previous = {
            str(row["change_id"]): (
                int(row["revision"]),
                str(row["status"]),
                str(row["delivery_state"]),
            )
            for row in rows
        }
        now_str = self._get_configured_time().isoformat()
        observed_ids: set[str] = set()
        new_items: List[RSSItem] = []

        with checkpoint:
            for item in items:
                change_id = str(item.change_id or item.guid or item.url or "").strip()
                if not change_id:
                    continue
                observed_ids.add(change_id)
                revision = int(item.revision or 1)
                status = str(item.status or "confirmed").lower()
                old = previous.get(change_id)
                if (
                    bool(item.is_active)
                    and (
                        old is None
                        or revision > old[0]
                        or (
                            revision == old[0]
                            and (
                                old[1] != status
                                or old[2] == "pending"
                            )
                        )
                    )
                ):
                    new_items.append(item)
                initial_delivery_state = (
                    "pending" if bool(item.is_active) else "delivered"
                )
                checkpoint.execute("""
                    INSERT INTO official_change_checkpoint
                    (change_id, revision, status, delivery_state,
                     reported_at, delivered_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, NULL, ?)
                    ON CONFLICT(change_id) DO UPDATE SET
                        revision = excluded.revision,
                        status = excluded.status,
                        delivery_state = CASE
                            WHEN excluded.revision > official_change_checkpoint.revision
                              OR excluded.status != official_change_checkpoint.status
                            THEN excluded.delivery_state
                            ELSE official_change_checkpoint.delivery_state
                        END,
                        reported_at = CASE
                            WHEN excluded.revision > official_change_checkpoint.revision
                              OR excluded.status != official_change_checkpoint.status
                            THEN excluded.reported_at
                            ELSE official_change_checkpoint.reported_at
                        END,
                        delivered_at = CASE
                            WHEN excluded.revision > official_change_checkpoint.revision
                              OR excluded.status != official_change_checkpoint.status
                            THEN NULL
                            ELSE official_change_checkpoint.delivered_at
                        END,
                        updated_at = excluded.updated_at
                    WHERE excluded.revision >= official_change_checkpoint.revision
                """, (
                    change_id,
                    revision,
                    status,
                    initial_delivery_state,
                    now_str,
                    now_str,
                ))

            if observed_ids:
                placeholders = ",".join("?" for _ in observed_ids)
                checkpoint.execute(f"""
                    UPDATE official_change_checkpoint
                    SET status='retracted', delivery_state='delivered',
                        delivered_at=COALESCE(delivered_at, ?), updated_at=?
                    WHERE status='confirmed' AND change_id NOT IN ({placeholders})
                """, [now_str, now_str, *sorted(observed_ids)])
            else:
                checkpoint.execute("""
                    UPDATE official_change_checkpoint
                    SET status='retracted', delivery_state='delivered',
                        delivered_at=COALESCE(delivered_at, ?), updated_at=?
                    WHERE status='confirmed'
                """, (now_str, now_str))

        persist = getattr(self, "_persist_official_change_checkpoint", None)
        if callable(persist) and not persist():
            print("[存储] 权威变化跨日游标持久化失败，本轮不发布以便下轮重试")
            return []
        self._official_change_detection_cache = {cache_key: list(new_items)}
        return new_items

    def _acknowledge_official_changes_impl(
        self,
        change_revisions: List[tuple[str, int]],
    ) -> bool:
        """Mark rendered policy revisions delivered; keep them pending on sync failure."""

        normalized = sorted({
            (str(change_id).strip(), int(revision or 1))
            for change_id, revision in change_revisions
            if str(change_id).strip()
        })
        if not normalized:
            return True

        checkpoint = self._get_connection(
            OFFICIAL_CHANGE_CHECKPOINT_DATE,
            db_type="rss",
        )
        delivered_at = self._get_configured_time().isoformat()
        updated: List[tuple[str, int]] = []
        with checkpoint:
            for change_id, revision in normalized:
                result = checkpoint.execute("""
                    UPDATE official_change_checkpoint
                    SET delivery_state='delivered', delivered_at=?, updated_at=?
                    WHERE change_id=? AND revision=?
                      AND status='confirmed' AND delivery_state='pending'
                """, (delivered_at, delivered_at, change_id, revision))
                if result.rowcount:
                    updated.append((change_id, revision))

        self._official_change_detection_cache = {}
        if not updated:
            return True

        persist = getattr(self, "_persist_official_change_checkpoint", None)
        if not callable(persist):
            return True

        try:
            persisted = bool(persist())
        except Exception as exc:
            print(f"[存储] 权威变化发布确认同步异常: {exc}")
            persisted = False
        if persisted:
            return True

        verify = getattr(self, "_verify_official_change_checkpoint_state", None)
        if callable(verify):
            try:
                if verify(updated, "delivered") is True:
                    print("[存储] 权威变化发布确认已由远端读回验证")
                    return True
            except Exception as exc:
                print(f"[存储] 权威变化发布确认远端读回异常: {exc}")

        # 远端未确认 delivered：先恢复本地 pending，再补偿写回远端。
        # 这样普通上传失败与明确未写入都保持可重放语义。
        with checkpoint:
            for change_id, revision in updated:
                checkpoint.execute("""
                    UPDATE official_change_checkpoint
                    SET delivery_state='pending', delivered_at=NULL, updated_at=?
                    WHERE change_id=? AND revision=?
                      AND delivery_state='delivered' AND delivered_at=?
                """, (delivered_at, change_id, revision, delivered_at))
        print("[存储] 权威变化发布确认同步失败，已恢复为待发布")

        try:
            compensated = bool(persist())
        except Exception as exc:
            print(f"[存储] 权威变化待发布状态补偿同步异常: {exc}")
            compensated = False

        if compensated:
            return False

        if callable(verify):
            try:
                if verify(updated, "pending") is True:
                    print("[存储] 权威变化待发布状态已由远端读回验证")
                    return False
                if verify(updated, "delivered") is True:
                    # 第一次 PUT 可能已成功、仅响应/验证失败。远端 delivered
                    # 与已经成功生成的交付物一致，恢复本地状态并视为成功。
                    with checkpoint:
                        for change_id, revision in updated:
                            checkpoint.execute("""
                                UPDATE official_change_checkpoint
                                SET delivery_state='delivered', delivered_at=?, updated_at=?
                                WHERE change_id=? AND revision=?
                                  AND status='confirmed'
                            """, (
                                delivered_at,
                                delivered_at,
                                change_id,
                                revision,
                            ))
                    print("[存储] 权威变化发布确认经补偿后由远端读回验证")
                    return True
            except Exception as exc:
                print(f"[存储] 权威变化补偿状态远端读回异常: {exc}")

        print("[存储] 权威变化待发布状态远端同步未确认，将在下轮继续重试")
        return False

    def _detect_new_rss_items_impl(self, current_data: RSSData) -> Dict[str, List[RSSItem]]:
        """
        检测新增的 RSS 条目（增量模式）

        该方法比较当前抓取数据与历史数据，找出新增的 RSS 条目。
        关键逻辑：只有在历史批次中从未出现过的 URL 才算新增。

        Args:
            current_data: 当前抓取的 RSS 数据

        Returns:
            新增的 RSS 条目 {feed_id: [RSSItem, ...]}
        """
        try:
            active_current_items = {
                feed_id: [item for item in items if bool(getattr(item, "is_active", True))]
                for feed_id, items in current_data.items.items()
            }
            official_new_items = self._detect_official_change_revisions(current_data)
            active_current_items.pop(OFFICIAL_CHANGE_FEED_ID, None)

            # 获取历史数据
            historical_data = self._get_rss_data_impl(current_data.date)

            if not historical_data:
                # 没有历史数据，所有都是新的
                result = {feed_id: items for feed_id, items in active_current_items.items() if items}
                if official_new_items:
                    result[OFFICIAL_CHANGE_FEED_ID] = official_new_items
                return result

            # 获取当前批次时间
            current_time = current_data.crawl_time

            # 收集历史 URL（first_time < current_time 的条目）
            historical_urls: Dict[str, set] = {}
            for feed_id, rss_list in historical_data.items.items():
                historical_urls[feed_id] = set()
                for item in rss_list:
                    first_time = item.first_time or item.crawl_time
                    if first_time < current_time:
                        if item.url:
                            historical_urls[feed_id].add(item.url)

            # 检查是否有早于当前批次的历史数据
            has_historical_data = any(len(urls) > 0 for urls in historical_urls.values())
            if not has_historical_data:
                # 当天第一次抓取，所有条目都是新增
                result = {feed_id: items for feed_id, items in active_current_items.items() if items}
                if official_new_items:
                    result[OFFICIAL_CHANGE_FEED_ID] = official_new_items
                return result

            # 检测新增
            new_items: Dict[str, List[RSSItem]] = {}
            if official_new_items:
                new_items[OFFICIAL_CHANGE_FEED_ID] = official_new_items
            for feed_id, rss_list in active_current_items.items():
                hist_set = historical_urls.get(feed_id, set())
                for item in rss_list:
                    if item.url and item.url not in hist_set:
                        if feed_id not in new_items:
                            new_items[feed_id] = []
                        new_items[feed_id].append(item)

            return new_items

        except Exception as e:
            print(f"[存储] 检测新 RSS 条目失败: {e}")
            return {}

    def _get_latest_rss_data_impl(self, date: Optional[str] = None) -> Optional[RSSData]:
        """
        获取最新一次抓取的 RSS 数据（当前榜单模式）

        Args:
            date: 日期字符串（YYYY-MM-DD），默认为今天

        Returns:
            最新抓取的 RSS 数据，如果没有数据返回 None
        """
        try:
            conn = self._get_connection(date, db_type="rss")
            cursor = conn.cursor()

            # 获取最新的抓取时间
            cursor.execute("""
                SELECT crawl_time FROM rss_crawl_records
                ORDER BY crawl_time DESC
                LIMIT 1
            """)

            time_row = cursor.fetchone()
            if not time_row:
                return None

            latest_time = time_row[0]

            # 获取该时间的 RSS 数据
            cursor.execute("""
                SELECT i.id, i.title, i.feed_id, f.name as feed_name,
                       i.url, i.guid, i.published_at, i.summary, i.author,
                       i.first_crawl_time, i.last_crawl_time, i.crawl_count,
                       i.change_id, i.revision, i.status, i.supersedes, i.is_active
                FROM rss_items i
                LEFT JOIN rss_feeds f ON i.feed_id = f.id
                WHERE i.last_crawl_time = ? AND i.is_active = 1
                ORDER BY i.published_at DESC
            """, (latest_time,))

            rows = cursor.fetchall()
            if not rows:
                return None

            items: Dict[str, List[RSSItem]] = {}
            id_to_name: Dict[str, str] = {}
            crawl_date = self._format_date_folder(date)

            for row in rows:
                feed_id = row[2]
                feed_name = row[3] or feed_id

                id_to_name[feed_id] = feed_name

                if feed_id not in items:
                    items[feed_id] = []

                items[feed_id].append(RSSItem(
                    title=row[1],
                    feed_id=feed_id,
                    feed_name=feed_name,
                    url=row[4] or "",
                    guid=row[5] or "",
                    published_at=row[6] or "",
                    summary=row[7] or "",
                    author=row[8] or "",
                    crawl_time=row[10],
                    first_time=row[9],
                    last_time=row[10],
                    count=row[11],
                    change_id=row[12] or "",
                    revision=int(row[13] or 0),
                    status=row[14] or "",
                    supersedes=row[15] or "",
                    is_active=bool(row[16]),
                ))

            # 获取失败的源（针对最新一次抓取）
            cursor.execute("""
                SELECT cs.feed_id
                FROM rss_crawl_status cs
                JOIN rss_crawl_records cr ON cs.crawl_record_id = cr.id
                WHERE cr.crawl_time = ? AND cs.status = 'failed'
            """, (latest_time,))

            failed_ids = [row[0] for row in cursor.fetchall()]

            return RSSData(
                date=crawl_date,
                crawl_time=latest_time,
                items=items,
                id_to_name=id_to_name,
                failed_ids=failed_ids,
            )

        except Exception as e:
            print(f"[存储] 获取最新 RSS 数据失败: {e}")
            return None
