# coding=utf-8
"""
AI 智能筛选标签存储 Mixin

从 SQLiteStorageMixin 拆分出来，专注标签管理、AI 分析结果与筛选结果的持久化逻辑。
"""

import sqlite3
from typing import Any, Dict, List, Optional


class TagsStorageMixin:
    """
    AI 智能筛选标签相关的 SQLite 存储操作

    依赖宿主类（最终与 SQLiteStorageMixin 其余部分组合）实现的抽象方法：
    - _get_connection(date, db_type) -> sqlite3.Connection
    - _get_configured_time() -> datetime
    """

    # ========================================
    # AI 智能筛选 - 标签管理
    # ========================================

    def _get_active_tags_impl(self, date: Optional[str] = None, interests_file: str = "ai_interests.txt") -> List[Dict[str, Any]]:
        """获取指定兴趣文件的 active 标签列表"""
        try:
            conn = self._get_connection(date)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT id, tag, description, version, prompt_hash, priority
                FROM ai_filter_tags
                WHERE status = 'active' AND interests_file = ?
                ORDER BY priority ASC, id ASC
            """, (interests_file,))

            return [
                {
                    "id": row[0], "tag": row[1], "description": row[2],
                    "version": row[3], "prompt_hash": row[4], "priority": row[5],
                }
                for row in cursor.fetchall()
            ]
        except Exception as e:
            print(f"[AI筛选] 获取标签失败: {e}")
            return []

    def _get_latest_prompt_hash_impl(self, date: Optional[str] = None, interests_file: str = "ai_interests.txt") -> Optional[str]:
        """获取指定兴趣文件最新版本标签的 prompt_hash"""
        try:
            conn = self._get_connection(date)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT prompt_hash FROM ai_filter_tags
                WHERE status = 'active' AND interests_file = ?
                ORDER BY version DESC
                LIMIT 1
            """, (interests_file,))
            row = cursor.fetchone()
            return row[0] if row else None
        except Exception as e:
            print(f"[AI筛选] 获取 prompt_hash 失败: {e}")
            return None

    def _get_latest_tag_version_impl(self, date: Optional[str] = None) -> int:
        """获取最新版本号"""
        try:
            conn = self._get_connection(date)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT MAX(version) FROM ai_filter_tags
            """)
            row = cursor.fetchone()
            return row[0] if row and row[0] is not None else 0
        except Exception as e:
            print(f"[AI筛选] 获取版本号失败: {e}")
            return 0

    def _deprecate_all_tags_impl(self, date: Optional[str] = None, interests_file: str = "ai_interests.txt") -> int:
        """将指定兴趣文件的 active 标签和关联的分类结果标记为 deprecated"""
        try:
            conn = self._get_connection(date)
            cursor = conn.cursor()
            now_str = self._get_configured_time().strftime("%Y-%m-%d %H:%M:%S")

            # 获取该兴趣文件的 active 标签 id
            cursor.execute(
                "SELECT id FROM ai_filter_tags WHERE status = 'active' AND interests_file = ?",
                (interests_file,)
            )
            tag_ids = [row[0] for row in cursor.fetchall()]

            if not tag_ids:
                return 0

            # 废弃标签
            placeholders = ",".join("?" * len(tag_ids))
            cursor.execute(f"""
                UPDATE ai_filter_tags
                SET status = 'deprecated', deprecated_at = ?
                WHERE id IN ({placeholders})
            """, [now_str] + tag_ids)
            tag_count = cursor.rowcount

            # 废弃关联的分类结果
            placeholders = ",".join("?" * len(tag_ids))
            cursor.execute(f"""
                UPDATE ai_filter_results
                SET status = 'deprecated', deprecated_at = ?
                WHERE tag_id IN ({placeholders}) AND status = 'active'
            """, [now_str] + tag_ids)

            conn.commit()
            print(f"[AI筛选] 已废弃 {tag_count} 个标签及关联分类结果")
            return tag_count
        except Exception as e:
            print(f"[AI筛选] 废弃标签失败: {e}")
            return 0

    def _save_tags_impl(
        self, date: Optional[str], tags: List[Dict], version: int, prompt_hash: str,
        interests_file: str = "ai_interests.txt"
    ) -> int:
        """保存新提取的标签"""
        try:
            conn = self._get_connection(date)
            cursor = conn.cursor()
            now_str = self._get_configured_time().strftime("%Y-%m-%d %H:%M:%S")

            count = 0
            for idx, tag_data in enumerate(tags, start=1):
                priority = tag_data.get("priority", idx)
                try:
                    priority = int(priority)
                except (TypeError, ValueError):
                    priority = idx
                cursor.execute("""
                    INSERT INTO ai_filter_tags
                    (tag, description, priority, version, prompt_hash, interests_file, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    tag_data["tag"],
                    tag_data.get("description", ""),
                    priority,
                    version,
                    prompt_hash,
                    interests_file,
                    now_str,
                ))
                count += 1

            conn.commit()
            return count
        except Exception as e:
            print(f"[AI筛选] 保存标签失败: {e}")
            return 0

    def _deprecate_specific_tags_impl(
        self, date: Optional[str], tag_ids: List[int]
    ) -> int:
        """废弃指定 ID 的标签及其关联分类结果（增量更新时使用）"""
        if not tag_ids:
            return 0
        try:
            conn = self._get_connection(date)
            cursor = conn.cursor()
            now_str = self._get_configured_time().strftime("%Y-%m-%d %H:%M:%S")

            placeholders = ",".join("?" * len(tag_ids))

            cursor.execute(f"""
                UPDATE ai_filter_tags
                SET status = 'deprecated', deprecated_at = ?
                WHERE id IN ({placeholders})
            """, [now_str] + tag_ids)
            tag_count = cursor.rowcount

            cursor.execute(f"""
                UPDATE ai_filter_results
                SET status = 'deprecated', deprecated_at = ?
                WHERE tag_id IN ({placeholders}) AND status = 'active'
            """, [now_str] + tag_ids)

            conn.commit()
            return tag_count
        except Exception as e:
            print(f"[AI筛选] 废弃指定标签失败: {e}")
            return 0

    def _update_tags_hash_impl(
        self, date: Optional[str], interests_file: str, new_hash: str
    ) -> int:
        """更新指定兴趣文件所有 active 标签的 prompt_hash（增量更新时使用）"""
        try:
            conn = self._get_connection(date)
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE ai_filter_tags
                SET prompt_hash = ?
                WHERE interests_file = ? AND status = 'active'
            """, (new_hash, interests_file))
            count = cursor.rowcount

            conn.commit()
            return count
        except Exception as e:
            print(f"[AI筛选] 更新标签 hash 失败: {e}")
            return 0

    # ========================================
    # AI 智能筛选 - 分类结果管理
    # ========================================

    def _update_tag_descriptions_impl(
        self, date: Optional[str], tag_updates: List[Dict],
        interests_file: str = "ai_interests.txt"
    ) -> int:
        """按 tag 名匹配，更新 active 标签的 description 字段"""
        try:
            conn = self._get_connection(date)
            cursor = conn.cursor()

            count = 0
            for t in tag_updates:
                tag_name = t.get("tag", "")
                description = t.get("description", "")
                if not tag_name:
                    continue
                cursor.execute("""
                    UPDATE ai_filter_tags
                    SET description = ?
                    WHERE tag = ? AND interests_file = ? AND status = 'active'
                """, (description, tag_name, interests_file))
                count += cursor.rowcount

            conn.commit()
            return count
        except Exception as e:
            print(f"[AI筛选] 更新标签描述失败: {e}")
            return 0

    def _update_tag_priorities_impl(
        self, date: Optional[str], tag_priorities: List[Dict],
        interests_file: str = "ai_interests.txt"
    ) -> int:
        """按 tag 名匹配，更新 active 标签的 priority 字段"""
        try:
            conn = self._get_connection(date)
            cursor = conn.cursor()

            count = 0
            for t in tag_priorities:
                tag_name = t.get("tag", "")
                priority = t.get("priority")
                if not tag_name:
                    continue
                try:
                    priority = int(priority)
                except (TypeError, ValueError):
                    continue
                cursor.execute("""
                    UPDATE ai_filter_tags
                    SET priority = ?
                    WHERE tag = ? AND interests_file = ? AND status = 'active'
                """, (priority, tag_name, interests_file))
                count += cursor.rowcount

            conn.commit()
            return count
        except Exception as e:
            print(f"[AI筛选] 更新标签优先级失败: {e}")
            return 0

    # ========================================
    # AI 智能筛选 - 已分析新闻追踪
    # ========================================

    def _save_analyzed_news_impl(
        self, date: Optional[str], news_ids: List[int], source_type: str,
        interests_file: str, prompt_hash: str, matched_ids: set
    ) -> int:
        """批量记录已分析的新闻（匹配与不匹配都记录）"""
        try:
            conn = self._get_connection(date)
            cursor = conn.cursor()
            now_str = self._get_configured_time().strftime("%Y-%m-%d %H:%M:%S")

            count = 0
            for nid in news_ids:
                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO ai_filter_analyzed_news
                        (news_item_id, source_type, interests_file, prompt_hash, matched, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        nid, source_type, interests_file, prompt_hash,
                        1 if nid in matched_ids else 0,
                        now_str,
                    ))
                    count += 1
                except Exception:
                    pass

            conn.commit()
            return count
        except Exception as e:
            print(f"[AI筛选] 保存已分析记录失败: {e}")
            return 0

    def _get_analyzed_news_ids_impl(
        self, date: Optional[str] = None, source_type: str = "hotlist",
        interests_file: str = "ai_interests.txt"
    ) -> set:
        """获取已分析过的新闻 ID 集合（用于去重）"""
        try:
            conn = self._get_connection(date)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT news_item_id FROM ai_filter_analyzed_news
                WHERE source_type = ? AND interests_file = ?
            """, (source_type, interests_file))

            return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            print(f"[AI筛选] 获取已分析ID失败: {e}")
            return set()

    def _clear_analyzed_news_impl(
        self, date: Optional[str] = None, interests_file: str = "ai_interests.txt"
    ) -> int:
        """清除指定兴趣文件的所有已分析记录（全量重分类时使用）"""
        try:
            conn = self._get_connection(date)
            cursor = conn.cursor()

            cursor.execute("""
                DELETE FROM ai_filter_analyzed_news
                WHERE interests_file = ?
            """, (interests_file,))

            count = cursor.rowcount
            conn.commit()
            return count
        except Exception as e:
            print(f"[AI筛选] 清除已分析记录失败: {e}")
            return 0

    def _clear_unmatched_analyzed_news_impl(
        self, date: Optional[str] = None, interests_file: str = "ai_interests.txt"
    ) -> int:
        """清除不匹配的已分析记录，让这些新闻有机会被新标签重新分析"""
        try:
            conn = self._get_connection(date)
            cursor = conn.cursor()

            cursor.execute("""
                DELETE FROM ai_filter_analyzed_news
                WHERE interests_file = ? AND matched = 0
            """, (interests_file,))

            count = cursor.rowcount
            conn.commit()
            return count
        except Exception as e:
            print(f"[AI筛选] 清除不匹配记录失败: {e}")
            return 0

    # ========================================
    # AI 智能筛选 - 分类结果管理（原有）
    # ========================================

    def _save_filter_results_impl(
        self, date: Optional[str], results: List[Dict]
    ) -> int:
        """批量保存分类结果"""
        try:
            conn = self._get_connection(date)
            cursor = conn.cursor()
            now_str = self._get_configured_time().strftime("%Y-%m-%d %H:%M:%S")

            count = 0
            for r in results:
                try:
                    cursor.execute("""
                        INSERT INTO ai_filter_results
                        (news_item_id, source_type, tag_id, relevance_score, created_at)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        r["news_item_id"],
                        r.get("source_type", "hotlist"),
                        r["tag_id"],
                        r.get("relevance_score", 0.0),
                        now_str,
                    ))
                    count += 1
                except sqlite3.IntegrityError:
                    pass  # 重复记录，跳过

            conn.commit()
            return count
        except Exception as e:
            print(f"[AI筛选] 保存分类结果失败: {e}")
            return 0

    def _get_active_filter_results_impl(self, date: Optional[str] = None, interests_file: str = "ai_interests.txt") -> List[Dict[str, Any]]:
        """获取指定兴趣文件的 active 分类结果，JOIN news_items 获取新闻详情"""
        try:
            conn = self._get_connection(date)
            cursor = conn.cursor()

            # 热榜结果
            cursor.execute("""
                SELECT
                    r.news_item_id, r.source_type, r.tag_id, r.relevance_score,
                    t.tag, t.description as tag_description, t.priority,
                    n.title, n.platform_id as source_id, p.name as source_name,
                    n.url, n.mobile_url, n.rank,
                    n.first_crawl_time, n.last_crawl_time, n.crawl_count
                FROM ai_filter_results r
                JOIN ai_filter_tags t ON r.tag_id = t.id
                JOIN news_items n ON r.news_item_id = n.id
                LEFT JOIN platforms p ON n.platform_id = p.id
                WHERE r.status = 'active' AND r.source_type = 'hotlist'
                    AND t.status = 'active' AND t.interests_file = ?
                ORDER BY t.priority ASC, t.id ASC, r.relevance_score DESC
            """, (interests_file,))

            results = []
            hotlist_news_ids = []
            for row in cursor.fetchall():
                results.append({
                    "news_item_id": row[0], "source_type": row[1],
                    "tag_id": row[2], "relevance_score": row[3],
                    "tag": row[4], "tag_description": row[5], "tag_priority": row[6],
                    "title": row[7], "source_id": row[8],
                    "source_name": row[9] or row[8],
                    "url": row[10] or "", "mobile_url": row[11] or "",
                    "rank": row[12],
                    "first_time": row[13], "last_time": row[14],
                    "count": row[15],
                })
                hotlist_news_ids.append(row[0])

            # 批量查排名历史（热榜）
            ranks_map: Dict[int, List[int]] = {}
            rank_timeline_map: Dict[int, List[Dict[str, Any]]] = {}
            if hotlist_news_ids:
                unique_ids = list(set(hotlist_news_ids))
                placeholders = ",".join("?" * len(unique_ids))
                cursor.execute(f"""
                    SELECT news_item_id, rank, crawl_time FROM rank_history
                    WHERE news_item_id IN ({placeholders})
                    ORDER BY news_item_id, crawl_time
                """, unique_ids)
                for rh_row in cursor.fetchall():
                    nid, rank, crawl_time = rh_row[0], rh_row[1], rh_row[2]

                    if not crawl_time:
                        continue

                    if nid not in ranks_map:
                        ranks_map[nid] = []
                    if rank != 0 and rank not in ranks_map[nid]:
                        ranks_map[nid].append(rank)

                    if nid not in rank_timeline_map:
                        rank_timeline_map[nid] = []
                    try:
                        time_part = crawl_time.split()[1][:5] if ' ' in crawl_time else crawl_time[:5]
                    except (IndexError, AttributeError):
                        time_part = "??:??"
                    rank_timeline_map[nid].append({
                        "time": time_part,
                        "rank": rank if rank != 0 else None
                    })

            for item in results:
                item["ranks"] = ranks_map.get(item["news_item_id"], [item["rank"]])
                item["rank_timeline"] = rank_timeline_map.get(item["news_item_id"], [])

            # RSS 结果（如果有 rss 库）
            try:
                rss_conn = self._get_connection(date, db_type="rss")
                rss_cursor = rss_conn.cursor()

                # 从 news 库获取 rss 类型的分类结果 ID
                cursor.execute("""
                    SELECT r.news_item_id, r.tag_id, r.relevance_score,
                           t.tag, t.description, t.priority
                    FROM ai_filter_results r
                    JOIN ai_filter_tags t ON r.tag_id = t.id
                    WHERE r.status = 'active' AND r.source_type = 'rss'
                        AND t.status = 'active' AND t.interests_file = ?
                    ORDER BY t.priority ASC, t.id ASC, r.relevance_score DESC
                """, (interests_file,))

                rss_filter_rows = cursor.fetchall()
                if rss_filter_rows:
                    rss_ids = [row[0] for row in rss_filter_rows]
                    placeholders = ",".join("?" * len(rss_ids))
                    rss_cursor.execute(f"""
                        SELECT i.id, i.title, i.feed_id, f.name as feed_name,
                               i.url, i.published_at, i.summary, i.author,
                               i.change_id, i.revision, i.status, i.supersedes
                        FROM rss_items i
                        LEFT JOIN rss_feeds f ON i.feed_id = f.id
                        WHERE i.id IN ({placeholders}) AND i.is_active = 1
                    """, rss_ids)

                    rss_info = {row[0]: row for row in rss_cursor.fetchall()}

                    for fr_row in rss_filter_rows:
                        rss_id = fr_row[0]
                        info = rss_info.get(rss_id)
                        if info:
                            results.append({
                                "news_item_id": rss_id,
                                "source_type": "rss",
                                "tag_id": fr_row[1],
                                "relevance_score": fr_row[2],
                                "tag": fr_row[3],
                                "tag_description": fr_row[4],
                                "tag_priority": fr_row[5],
                                "title": info[1],
                                "source_id": info[2],
                                "source_name": info[3] or info[2],
                                "url": info[4] or "",
                                "mobile_url": "",
                                "rank": 0,
                                "ranks": [],
                                "first_time": info[5] or "",
                                "last_time": info[5] or "",
                                "summary": info[6] or "",
                                "author": info[7] or "",
                                "change_id": info[8] or "",
                                "revision": int(info[9] or 0),
                                "status": info[10] or "",
                                "supersedes": info[11] or "",
                                "count": 1,
                            })
            except Exception:
                pass  # RSS 库不存在时静默跳过

            return results
        except Exception as e:
            print(f"[AI筛选] 获取分类结果失败: {e}")
            return []
