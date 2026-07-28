# 宠物托运知识库来源监控

## 数据流

1. `sync_knowledge_inventory.ps1` 从 IPATA 知识库生成去重来源清单。
2. `official_monitor.py` 每 15 分钟从到期队列选择75个来源，默认使用 Scrapling Fetcher。
3. 抓取 Agent 根据静态空壳、WAF、超时和站点历史，在有界循环内选择 Fetcher、DynamicFetcher 或 StealthyFetcher。
4. 首次有效抓取及内容变化时保存不可变快照。
5. Katana按官网域名读取 `robots.txt`、sitemap、HTML和JavaScript链接，并进行有范围的整站扫描。
6. 重定向、可信 canonical 和多语言宠物运输相关链接进入候选队列。
7. 候选页面验证通过后进入来源注册表；无关页面和静态资源自动退出监控。
8. 变化事件写入 RSS，供 TrendRadar 消费；业务页面只展示确认后的政策条款变化。

## 通用站点发现

- 默认使用Katana v1.6.1作为URL发现引擎；Katana缺失、超时或未返回URL时自动降级到内置发现器。
- 不绑定具体航司或国家，依据来源的官网域名和业务实体自动建立发现范围。
- 优先从 `robots.txt` 和 sitemap 获取全站 URL；无 sitemap 时扫描首页、帮助、行李、特殊服务和货运等栏目。
- 支持中、英、法、德、西、意、葡、荷、俄、日、韩、泰、阿拉伯等常见页面或 URL 关键词。
- 每个官网按周期、sitemap 数、页面数、深度和候选数设置独立预算，避免拖慢现有巡检。
- 图片、字体、脚本、样式和媒体资源不会进入候选；PDF 政策文件保留。
- 新 URL 先进入候选并验证主题，验证通过后才参与政策内容监控。

可通过以下环境变量调整：

- `MONITOR_SITE_DISCOVERY_ENABLED`：是否启用，默认 `true`。
- `MONITOR_SITE_DISCOVERY_SITES_PER_CYCLE`：每轮发现的官网数，默认 `40`。
- `MONITOR_SITE_DISCOVERY_CONCURRENCY`：官网发现并发数，默认 `4`，最大 `8`；内置回退抓取保持串行，避免共享浏览器状态冲突。
- `MONITOR_SITE_DISCOVERY_CYCLE_INTERVAL`：发现队列仍有待处理官网时的循环间隔，默认 `30` 秒。
- `MONITOR_SITE_DISCOVERY_CIRCUIT_SECONDS`：发现阶段遇到 401、403 或 429 后暂停该官网的时间，默认 `21600` 秒（6小时）。
- `MONITOR_SCAN_CONCURRENCY`：已登记政策页面的并发数，默认 `8`；同一域名始终串行。
- `MONITOR_SCAN_DYNAMIC_CONCURRENCY`：动态浏览器并发上限，默认 `2`。
- `MONITOR_SCAN_STEALTH_CONCURRENCY`：隐身浏览器并发上限，默认 `1`。
- `MONITOR_CLOUDFLARE_SOLVER_ENABLED`：检测到 Cloudflare 挑战后是否允许 Scrapling 自动处理，默认 `true`。
- `MONITOR_CLOUDFLARE_TIMEOUT`：Cloudflare 单次浏览器处理超时，默认 `60` 秒；外层硬超时会自动保留 15 秒清理余量。
- `MONITOR_SITE_DISCOVERY_INTERVAL`：同一官网再次发现的间隔秒数，默认 `86400`。
- `MONITOR_SITE_DISCOVERY_MAX_SITEMAPS`：单官网每轮最多读取的 sitemap 数，默认 `12`。
- `MONITOR_SITE_DISCOVERY_MAX_URLS`：单官网每轮最多新增候选数，默认 `150`。
- `MONITOR_SITE_DISCOVERY_MAX_PAGES`：无 sitemap 时最多扫描的栏目页数，默认 `6`。
- `MONITOR_SITE_DISCOVERY_MAX_DEPTH`：站内栏目扫描深度，默认 `2`。
- `MONITOR_KATANA_ENABLED`：是否使用Katana，默认 `true`。
- `MONITOR_KATANA_DEPTH`：Katana爬取深度，默认 `3`。
- `MONITOR_KATANA_MAX_PAGES`：单官网Katana最多处理页面数，默认 `150`。
- `MONITOR_KATANA_CRAWL_DURATION`：Katana单官网运行秒数，默认 `30`。
- `MONITOR_KATANA_PROCESS_TIMEOUT`：Katana进程硬超时秒数，默认 `45`。

## 抓取 Agent 循环

- 循环状态为：执行策略 → 校验结果 → 分类失败 → 选择下一策略 → 验收或转人工。
- 每次验收同时检查正文长度、政策主题覆盖、HTML主体结构、结束结构和模板指纹；HTTP 200本身不代表成功。
- 默认最多尝试3种策略、总时长180秒；不会无限循环或自行执行任意代码。
- JavaScript空壳优先升级动态浏览器；Cloudflare Turnstile/挑战页升级到启用求解器的隐身浏览器；通用验证码和登录验证仍转人工；DNS和证书错误直接停止无效重试。
- 每个域名及首级路径会记住最近验证成功的策略，后续优先复用，失败时自动回退到未尝试策略。
- 新的动态或隐身策略先作为候选连续验证2次，之后才晋升为活跃适配器；活跃策略连续失败2次自动回滚上一版本。
- 站点档案保存版本、置信度、模板指纹、完整性分数、候选策略和最近10个历史版本。
- Agent 将相同失败、登录或人机处理停顿写入`manual-queue.json`作为抓取审计；只有绑定来源且带重试/恢复控制的记录才进入正式复核任务。浏览器容量耗尽只进入延后记录。
- 尝试过程只记录策略、失败分类和指标，不保存页面正文、Cookie、验证码或API Key。
- `robots.txt`、sitemap 和发现首页使用纯静态探测通道，不进入浏览器 Agent。
- 人工任务按“域名 + 失败类型”合并并累计出现次数，避免同一官网生成重复记录。
- 多语言 URL 保留为候选别名，同一政策族只选择一个代表页面进入持续监控。
- HTML 内容先提取费用、重量、尺寸、品种、证书、疫苗、检疫、禁运和时限等政策字段；只有事实性字段变化才进入 AI 摘要。
- `MONITOR_AGENT_MAX_ATTEMPTS`控制单页最大策略数，默认`3`。
- `MONITOR_AGENT_MAX_DURATION`控制单页循环总时长，默认`180`秒。

## 稳定性与恢复

- 来源按价值分层调度：官方/主政策页默认6小时，普通政策来源24小时，目录和历史参考来源7天。
- 失败来源采用6小时至7天的指数退避，避免DNS、SSL、WAF和失效页面反复占满巡检队列。
- 正式政策页优先巡检，URL发现随后运行；两者使用独立的动态浏览器预算，慢站和发现失败不会阻塞或耗尽政策页抓取额度。
- `status.json`只保存轻量统计，完整逐来源状态保留在`state.json`，避免前端和健康检查反复传输大文件。
- 静态页面继续使用进程内 `Fetcher`，动态页面和 WAF 页面使用独立浏览器子进程。
- 浏览器子进程有硬超时；无论成功、失败或超时都会清理整个进程组，避免 Chromium 泄漏。
- 扫描进度写入 `scan-progress.json`，状态增量写入 `state-journal.jsonl`。
- 默认每 10 个来源建立一次持久化检查点；进程重启后从最后检查点继续，最多重复 9 个来源。
- 全量扫描看门狗在进度长时间不变时重启容器，超过重试上限后恢复普通监控。
- 政策语义复核默认每批20条、4路并发；有积压时每轮最多处理80条并立即开始下一轮。
- `MONITOR_AI_SUMMARY_CONCURRENCY`控制并发数，默认4、最大8；`MONITOR_AI_SUMMARY_INTERVAL`控制无满载时的轮询间隔，默认5秒。
- 每个复核批次在独立子进程中运行；模型请求默认90秒超时、整批120秒硬超时且不自动重试，单批卡死不会阻塞其他批次或后续轮次。
- `MONITOR_AI_SUMMARY_REQUEST_TIMEOUT`、`MONITOR_AI_SUMMARY_HARD_TIMEOUT`和`MONITOR_AI_SUMMARY_RETRIES`可分别调整请求超时、硬超时和重试次数。

## 输出

- `status.json`：巡检状态和覆盖率。
- `inventory.json`：知识库来源清单。
- `source_registry.json`：业务实体、航司当前官网页面、国家可信来源列表和候选评分。
- `discovered_sources.json`：自动发现、尚待巡检的页面。
- `site-discovery.json`：每个官网最近一次发现结果。
- `site-discovery-summary.json`：最近一轮官网发现统计。
- `events.json` / `feed.xml`：内容变化、失效、恢复和页面迁移事件。
- `/api/v1/policy-change-digest`：按国家、地区或航司汇总通过完整证据链校验的当前有效修订；支持`from`、`to`、`kind`、`period=daily|weekly|monthly`、`format=json|text|markdown`和`limit`查询参数。
- `/api/brief.json`中的`policy_change_digest`：仪表盘“逐条变化 / 国家汇总”视图使用的结构化汇总及可复制中文文本。
- `policy-digests/`：日报、周报、月报和 latest 的 JSON、纯文本、Markdown 原子快照；TrendRadar HTML 报告和通知渠道直接读取这些文件。
- `snapshots/<source-id>/<timestamp>-<hash>/`：压缩原文、完整正文、元数据和差异。
- `current/<source-id>.json`：每个来源最近一次验证通过的快照指针。
- `scan-progress.json`：正在执行批次的心跳、当前来源和可恢复位置；批次完成后自动删除。
- `state-journal.jsonl`：尚未合并进 `state.json` 的检查点记录；批次完成后自动清理。
- `scraping-agent/site-profiles.json`：已验证的站点抓取策略记忆。
- `scraping-agent/runs.jsonl`：不含正文和密钥的 Agent 尝试日志。
- `scraping-agent/manual-queue.json`：Agent 抓取暂停审计记录，不是正式复核任务；正式任务保存在 MonitorStore 的 `review_tasks`。

## 选择原则

- HTTP 200 不是有效证据；空壳、软 404、拦截页和无关页面不能成为当前版本。
- 航司当前页面必须来自知识库明确标注的官网/Official Page 上下文。
- 国家政策允许保留多个政府或监管机构可信来源，不强行选一个页面代表全部规则。
- 新页面不会直接覆盖旧页面；旧快照永久保留，注册表只移动当前指针。
- 第三方聚合页可以作为历史证据监控，但不能替代航司官网当前页面。
- 每批动态浏览器与隐身抓取都有硬限额；抓取 Agent 只在预定义策略中选择，语义 Agent 只总结确认后的变化。
- 汇总中的发现时间、公告时间和生效时间保持独立；官网未明确说明的生效日期或官方原因显示为“未说明”，不会由模型推测。
- 公告时间、生效时间和官方原因必须同时保存官网快照原句及来源地址；缺少来源字段时不会进入业务输出。
- `MONITOR_POLICY_DIGEST_ENABLED=false`可关闭周期汇总生成作为发布回滚开关；生产合成门禁见`docs/policy-digest-release.md`。
