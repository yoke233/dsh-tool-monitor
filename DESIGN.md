# DSH Job Monitor 设计

## 目标

为 DeepSeek Harness 的现有后台 Job 增加运行中事件监听，而不重复执行原始 Bash 或 PowerShell 命令。

模型侧工作流：

```text
pwsh(..., run_in_background: true)
→ pwsh-1

job_monitor(job_id: "pwsh-1", pattern: "error|fatal|exception")
→ monitor-1
```

`monitor-1` 是对 `pwsh-1` 输出的订阅，不是第二个业务进程。

## 装配结构

```text
原 Bash / Pwsh 工具
        ↓ ctx.jobs.start()
MonitorableJobRegistry
  └─ 继承官方 LocalJobRegistry
        ↓
原 job_list / job_output / job_kill
新增 job_monitor
```

- 不修改 DeepSeek Harness 源码。
- 不替换 `@deepseek-ai/dsh-tool-jobs`，保留原来的 `job_list`、`job_output`、`job_kill`。
- 不修改 Bash、PowerShell 或其他 Job 生产者。
- 只用兼容实现替换 Host 上的具体 `@deepseek-ai/dsh-jobs-local` 服务。
- 兼容实现继承官方 `LocalJobRegistry`，复用其所有权隔离、等待、取消、完成通知、容量限制和清理语义。

DSH 补丁不能直接修改已有条目的包名，因此插件会禁用原 `jobs` 条目并插入一个新的 Registry 条目；运行时仍然只有一个 `ctx.jobs` 服务。

## 输出路径

未监听时：

```text
Job producer readOutput() → 原 job_output
```

监听后：

```text
Job producer readOutput()
        ↓（Host 只读取一次）
      惰性 tee
       ↙    ↘
job_output  job_monitor subscriber(s)
```

- 只有首次订阅某个 Job 后才启动 Host 本地轮询。
- 激活 tee 时，订阅前尚未消费的输出只保留给 `job_output`，不作为新的 Monitor 事件回放。
- 后续输出写入有界的 `job_output` 缓冲，同时按完整 LF/CRLF 行广播给多个 Monitor。
- 每个 Monitor 可以独立过滤和批处理，不会争抢原 Job 的输出游标。
- Host 轮询不调用模型；只有匹配事件才通过 Agent inbox 投递。

## 生命周期

- `job_monitor` 返回普通的 `monitor-N` Job id。
- `job_list` 可看到 Monitor，`job_output` 可读取其匹配事件，`job_kill` 可停止订阅。
- 停止 Monitor 只取消订阅，不影响目标 Job。
- 目标 Job 结束时，关联的 Monitor 自动结束并刷新最后一条不完整输出行。
- Agent、Registry 或 Host dispose 时继续沿用官方 Job 生命周期清理。

## 兼容性要求

- Bash（LF）和 PowerShell（CRLF）输出均按完整行处理。
- 目标 Job 的所有权检查通过官方 `ctx.jobs.get()` 完成，不能跨会话监听。
- 非流式 Job 没有 `readOutput()`，应明确拒绝监听。
- 输出缓冲必须有 UTF-8 字节上限，并在丢弃旧输出时给出提示。
- 读取、通知和取消中的异常必须被隔离，不能破坏目标 Job。
- 插件必须针对当前 DSH `JobRegistry` 契约运行兼容性测试。

## 非目标

- 不监听插件加载前、由另一个 Registry 创建的历史 Job。
- 不持久化跨 Host 重启的 Job 或 Monitor。
- 不改变原 Job 的完成状态、取消方式或 Web 展示结构。
