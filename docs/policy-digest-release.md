# 政策汇总发布门禁

## 发布前

1. 运行全部自动化测试、Python/JavaScript 语法检查和报告 DOM 冒烟测试。
2. 运行 `python -m monitor.production_validation --url http://127.0.0.1:8090`。
3. 阻断条件：健康接口失败、汇总出现无证据修订、日期或官方原因缺少来源、P95 超过 2 秒。

## 分阶段发布

1. 先部署 `official-monitor`，保持 `MONITOR_POLICY_DIGEST_ENABLED=true`。
2. 合成检查通过后部署 `trendradar` 和 `trendradar-mcp`。
3. 验证日报 HTML、通知文本、JSON/纯文本/Markdown 导出。
4. 观察一个完整扫描周期的健康状态、证据 Agent 积压、错误率和接口 P95。

## 回滚

1. 将 `MONITOR_POLICY_DIGEST_ENABLED=false` 并重启 `official-monitor`，停止生成新汇总文件。
2. 回滚到前一镜像版本；证据包和政策修订为追加式数据，不删除数据库或快照。
3. 重新运行生产验证，确认原 RSS、健康接口和知识库仍正常。
