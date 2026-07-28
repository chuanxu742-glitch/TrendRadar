# 宠物托运知识库来源监控

## 数据流

1. `sync_knowledge_inventory.ps1` 从 IPATA 知识库生成去重来源清单。
2. `official_monitor.py` 每 15 分钟从到期队列选择75个来源，默认使用 Scrapling Fetcher。
3. 抓取 Agent 根据静态空壳、WAF、超时和站点历史，在有界循环内选择 Fetcher、DynamicFetcher 或 StealthyFetcher。
4. 首次有效抓取及内容变化时保存不可变快照。
5. Katana按官网域名读取 `robots.txt`、sitemap、HTML和JavaScript链接，建立全站稳定 URL 台账。
6. 所有稳定 URL 记录来源、链接上下文、相关度、读取状态和未读取原因。
7. 高相关页面完整抓取，中相关页面按预算验证，低相关页面轮换抽检。
8. 候选页面连续验证通过后进入来源注册表；无关页面保留审计记录但退出持续监控。
9. 变化事件写入 RSS，供 TrendRadar 消费；业务页面只展示确认后的政策条款变化。

## 老板统一面板与知识更新权限

- `http://127.0.0.1:8090/` 是统一业务入口，同时展示官网政策变化、小红书业务情报和知识库更新结果。
- 页面不展示积压、并发、熔断或 `degraded` 等运维状态；这些指标仍保留在健康接口供内部诊断。
- `current-primary` 和 `trusted-secondary` 官网来源在快照、差异和证据链全部验证通过后，可以自动形成并应用版本化知识更新。
- 候选来源、手工新增但尚未验证的来源和小红书内容只能形成情报或待确认提案，不能自动改写政策事实。
- 小红书由独立 `xhs-monitor` 服务采集并写入自己的 SQLite；8090 只读取其摘要 API，不接触 Cookie、代理或采集错误。
- `xhs-monitor` 不提供第二个业务面板；服务暂时不可用时，8090 只显示“今日数据暂未更新”，不影响官网监控。
- 每次官网知识更新都保留来源、修订、证据和历史版本，可以回滚。
- `/api/v1/social-intelligence`：查询统一面板使用的小红书业务情报。
- `MONITOR_XHS_SUMMARY_URL`：独立服务摘要接口，容器默认 `http://xhs-monitor:8091/api/v1/summary`。

## 手工与批量添加数据源

- 仪表盘右上角“添加数据源”只要求填写 URL，支持单个 URL、每行一个、Excel 复制内容和夹杂说明文字的批量输入。
- 固定规则先提取、规范化、去重并拦截本机、内网、登录、搜索和静态资源地址；每批最多处理 200 个 URL。
- “AI 整理”只为已提取 URL 补充显示名称，模型返回的新 URL 不会被接受；未配置 AI 或模型失败时自动使用确定性结果。
- 新增来源以 `candidate/discovered` 状态写入 `monitor.db`，同时登记为高相关全站 URL，并立即加入到期队列。
- 每次成功导入返回批次编号，页面可将该批新增来源一次性软退役；URL 审计记录继续保留。
- 手工来源默认执行“重点监控该页面 + 同域全站发现”，不会因为下一次知识库来源同步而被删除。
- `/api/v1/sources/preview`：预览批量输入和 AI 整理结果。
- `POST /api/v1/sources`：批量登记验证通过的 URL。
- `/api/v1/manual-sources`：查询手工添加来源及生命周期状态。
- `MONITOR_SOURCE_INTAKE_AI_ENABLED`控制格式整理助手，默认`true`。
- `MONITOR_SOURCE_INTAKE_AI_TIMEOUT`和`MONITOR_SOURCE_INTAKE_AI_HARD_TIMEOUT`默认分别为30秒和45秒。

## 通用站点发现

- 默认使用Katana v1.6.1作为URL发现引擎；Katana缺失、超时或未返回URL时自动降级到内置发现器。
- 不绑定具体航司或国家，依据来源的官网域名和业务实体自动建立发现范围。
- 优先从 `robots.txt` 和 sitemap 获取全站 URL；无 sitemap 时扫描首页、帮助、行李、特殊服务和货运等栏目。
- 支持中、英、法、德、西、意、葡、荷、俄、日、韩、泰、阿拉伯等常见页面或 URL 关键词。
- 每个官网按周期、sitemap 数、页面数和深度设置独立预算；每周执行更大范围的深度扫描。
- 图片、字体、脚本、样式和媒体资源不会进入候选；PDF 政策文件保留。
- 新 URL 先写入全站台账。高相关页面直接验证，中相关页面按预算验证，低相关页面每30天轮换抽检。
- 每个 URL 都保留首次/最近发现时间、发现方式、父页面、链接文字、相关度、读取状态和跳过原因。
- 新 URL 验证通过后才参与政策内容监控。

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
- `MONITOR_SITE_DISCOVERY_DEEP_INTERVAL`：同一官网深度扫描间隔，默认 `604800` 秒（7天）。
- `MONITOR_SITE_DISCOVERY_DEEP_MAX_URLS`：深度扫描单官网最多登记稳定 URL 数，默认 `5000`。
- `MONITOR_SITE_DISCOVERY_DEEP_MAX_PAGES`：深度扫描备用爬取页数，默认 `50`。
- `MONITOR_SITE_INVENTORY_MEDIUM_FETCH_PER_SITE`：每个官网每轮验证的中相关 URL 数，默认 `20`。
- `MONITOR_SITE_INVENTORY_LOW_SAMPLE_PER_SITE`：每个官网每轮抽检的低相关 URL 数，默认 `5`。
- `MONITOR_SITE_INVENTORY_SAMPLE_INTERVAL`：低相关 URL 再次抽检间隔，默认 `2592000` 秒（30天）。
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
- `site-url-inventory.db`：可扩展的全站稳定 URL、相关度、读取状态和未读取原因台账；旧版 JSON 会自动迁移。
- `site-url-inventory-summary.json`：URL 总量、读取覆盖率、相关度分层和跳过原因汇总。
- `events.json` / `feed.xml`：内容变化、失效、恢复和页面迁移事件。
- `/api/v1/policy-change-digest`：按国家、地区或航司汇总通过完整证据链校验的当前有效修订；支持`from`、`to`、`kind`、`period=daily|weekly|monthly`、`format=json|text|markdown`和`limit`查询参数。
- `/api/v1/site-url-inventory`：查询全站 URL 台账，支持按`origin`、`relevance`和`status`筛选。
- `/api/v1/sources/preview`、`POST /api/v1/sources`和`/api/v1/manual-sources`：批量 URL 整理、手工来源登记和状态查询。
- `/api/v1/social-intelligence`：老板面板使用的小红书公开业务情报和数据可用状态。
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
