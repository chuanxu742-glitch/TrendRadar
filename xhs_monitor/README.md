# 独立小红书监控服务

- 独立容器：`xhs-monitor`
- 独立配置：`config/xhs-monitor.yaml`
- 独立数据：`output/xhs-monitor/xhs-monitor.db`
- 内部摘要接口：`http://xhs-monitor:8091/api/v1/summary`
- 健康接口：`/health/live`、`/health/ready`
- 老板入口仍只有 `http://127.0.0.1:8090/`

关键词只在 `config/xhs-monitor.yaml` 的 `keywords` 中维护。Cookie 仅通过
`XHS_COOKIE` 或 `XHS_COOKIE_FILE` 传入，不写入配置、数据库或摘要接口。

登录或续期：

```powershell
docker compose -f docker/docker-compose.yml run --rm xhs-login
```

终端显示二维码后，使用小红书 App 扫码确认。登录状态会写入被 Git 忽略的
`config/xhs_cookie.txt`，独立采集服务会在 10 秒内发现更新并自动重新采集，
不需要手工复制 Cookie 或重启容器。

启动：

```powershell
docker compose -f docker/docker-compose.yml up -d --build xhs-monitor official-monitor
```

小红书内容只作为客户与市场情报，不会自动修改官网政策知识库。
