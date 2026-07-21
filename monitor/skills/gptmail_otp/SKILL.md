---
id: gptmail-otp
scope: project
runtime: python
---

# GPTMail 临时邮箱与验证码

这是 TrendRadar 项目智能体的能力模块，不是 Codex 全局技能。

## 使用场景

- 为已获授权的测试流程生成临时邮箱。
- 查询收件箱、读取邮件并等待邮箱验证码。
- 每日从 GPTMail API 文档页更新公共测试 Key。

## 执行规则

1. 调用 API 前执行 `PublicKeyProvider.ensure_fresh()`。
2. Key 在每日 08:00（Asia/Shanghai）之后自动刷新；进程启动时也检查。
3. 遇到 401 或 403 时强制刷新一次并重试一次，不无限循环。
4. 新 Key 只有通过格式检查和 API 验证后才原子替换旧 Key。
5. 刷新失败时保留最后一个可用 Key，并输出不含 Key 的错误记录。
6. 日志、异常和业务结果不得包含完整 Key。
7. 删除邮件和清空收件箱不是默认能力，必须由调用方显式授权。
8. 仅用于用户拥有或明确授权的账号和测试流程，不规避访问控制。

## 项目入口

```python
from monitor.skills.gptmail_otp import GPTMailClient

client = GPTMailClient()
address = client.generate_email(prefix="monitor-test")
code = client.wait_for_code(address, timeout=180)
```

后台刷新：

```powershell
python -m monitor.skills.gptmail_otp.provider watch
```
