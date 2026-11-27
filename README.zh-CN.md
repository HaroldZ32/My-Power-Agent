# My-Power-Agent 本地消息通道

[English](README.md)

同一台机器上的多个 agent harness 可以通过 Unix 域套接字或 CLI 使用通道。运行方为每个 Agent 实例分配固定编号和独立令牌，实例重启后沿用相同编号即可继续访问自己的收件箱。

实现参考 My-Power-Dsh 的 [邮件面](https://github.com/HaroldZ32/My-Power-Dsh/blob/master/docs/mail.zh-CN.md)，沿用持久化后唤醒、逐收件人广播和收件人确认。通用 JSON 载荷可承载任务、问题、回答及框架自定义字段。通道独立管理 SQLite 数据库，接入 Dsh 现有 JSONL 邮箱需要适配层。

## 启动

需要 Linux 或 macOS、Rust 工具链和 C 编译器。构建会编译内置 SQLite。下面使用 OpenSSL 生成编号 1、2、3 的令牌，仅在首次部署时生成 roster。运行方分别向各实例注入自己的令牌。

```bash
cargo build --release
(umask 077
 printf '{"1":"%s","2":"%s","3":"%s"}\n' \
   "$(openssl rand -hex 32)" "$(openssl rand -hex 32)" "$(openssl rand -hex 32)" > roster.json
)
target/release/agent-channel serve --roster roster.json
```

默认监听 `.agent-channel/channel.sock`。在子命令前加 `--state-dir /path/to/private-dir` 可为不同调度组隔离状态。客户端须使用相同目录，状态目录归当前用户所有且权限为 `0700`。编号是 1–18 位的正十进制数字字符串，每组支持 1–1024 个编号。服务启动时加载 roster。

## 按编号发送、查询和确认

每个 harness 设置 `AGENT_CHANNEL_ID` 和 `AGENT_CHANNEL_TOKEN`。示例中由编号 1 执行发送，编号 2 执行后续命令。确认时填入实际收到的消息 ID。

```bash
target/release/agent-channel call '{"op":"send","to":"2","request_id":"dispatch-17","payload":{"type":"task","text":"汇总检查结果"}}'
target/release/agent-channel call '{"op":"inbox"}'
target/release/agent-channel call '{"op":"receive","wait_ms":30000}'
target/release/agent-channel call '{"op":"ack","ids":[1]}'
```

| 操作 | 请求字段 | 成功返回 |
| --- | --- | --- |
| `whoami` | 无 | `agent_id` |
| `send` | `to`、`request_id`、`payload`，可选 `reply_to` | `thread_id` 和包含 `id`、`recipient` 的 `messages` |
| `inbox` | 无 | 自己的 `agent_id`、未确认数 `pending`、容量 `capacity` |
| `receive` | 可选 `wait_ms`、`limit`、`after_id` | 按 ID 升序排列的未确认 `messages` |
| `ack` | `ids`，1–32 个消息 ID | `acked` |

- `receive` 返回 `id`、`sender`、`recipient`、`thread_id`、`reply_to`、`payload` 和 `created_ms`。每页默认 16 条，最多 32 条，用上一页最后一条的 ID 作为 `after_id` 查询后续消息。处理结束再确认，未确认消息在重启后仍可查询。同一编号有多个并发消费者时，可能读到同一条消息。
- `to:"*"` 向 roster 内除发送者外的所有 Agent 广播。任一目标收件箱已满时，整批返回 `inbox_full:<编号>`。通过 `serve --capacity` 配置每个编号的待确认上限，默认 256。
- `request_id` 在发送方下唯一，最长 128 个 UTF-8 字节。如果发送结果不明确，使用同一个 ID 重发相同内容以取得原始消息 ID。确认也可重试，批量确认使用同一个事务。
- 回复时将 `reply_to` 设为自己收到的原消息 ID，目的编号设为原发送方。回复继承原线程 ID，广播的所有副本也共享同一个线程 ID。

## Meeting 模式

调度器调用 `meeting_create` 并把同一个 `meeting_id` 提供给参会者，例如通过普通消息队列。默认包含 roster 中的所有 Agent，也可在创建时显式传入 `participants`。每轮至少两名参会者，创建后名单固定。创建者也要报告 ready。

| 操作 | 请求字段 | 行为 |
| --- | --- | --- |
| `meeting_create` | `meeting_id`、可选 `participants` | 冻结该轮参会名单 |
| `meeting_ready` | `meeting_id` | 以当前编号报告 ready，可重复调用 |
| `meeting_status` | `meeting_id`、可选 `until` 与 `wait_ms` | 查询当前阶段，可等待 `exchange` 或 `complete` |
| `meeting_summary` | `meeting_id`、`payload` | 全员 ready 后提交自己的总结，可用相同内容重试 |
| `meeting_summaries` | `meeting_id`、可选 `limit` 与 `after_agent_id` | 读取已提交的总结，按编号字符串分页 |

每个 Agent 按以下顺序执行。服务返回 `phase:"exchange"` 或 `"complete"` 才表示 ready 屏障已打开。在此之前提交总结会收到 `meeting_not_ready`。

```bash
target/release/agent-channel call '{"op":"meeting_ready","meeting_id":"review-17"}'
target/release/agent-channel call '{"op":"meeting_status","meeting_id":"review-17","until":"exchange","wait_ms":60000}'
target/release/agent-channel call '{"op":"meeting_summary","meeting_id":"review-17","payload":{"text":"我的检查已完成"}}'
target/release/agent-channel call '{"op":"meeting_status","meeting_id":"review-17","until":"complete","wait_ms":60000}'
target/release/agent-channel call '{"op":"meeting_summaries","meeting_id":"review-17"}'
```

`meeting_status` 立即查询时返回每位参会者的 `ready` 和 `submitted` 状态。指定 `until` 时返回精简阶段快照，超时也返回当前阶段，调用方可按需继续等待。所有总结提交后阶段为 `complete`。总结一经提交即可由所有参会者读取，每页默认 16 条、最多 32 条，使用返回的 `next_after_agent_id` 翻页。SQLite 持久化 ready、提交和阶段数据，服务重启后可以继续同一轮。成员长期缺席时，调用会按超时返回，阶段继续等待该成员。

## 直连协议与调度

每个请求是一行 UTF-8 JSON，以 LF 结尾，包含 `agent_id`、`token`、`op` 和操作字段。请求上限为 64 KiB（含 LF），载荷上限为 32 KiB（按紧凑 JSON 编码计算）。响应同样是一行 JSON，包含 `"ok":true` 或 `{"ok":false,"error":"…"}`。CLI 支持用 `call -` 从标准输入构造请求。每条连接串行处理，可复用连接，同时发送与等待时使用不同连接。等待默认 0 毫秒，上限 60000 毫秒。

Tokio 处理套接字和事件等待，单独的 SQLite 写入线程使用 WAL 和完整同步。收件箱有按编号和消息 ID 建立的待确认索引，广播载荷仅存一份。持久化提交后按收件人发出通知，meeting 等待只在相应阶段推进时收到通知，空闲等待期间无需轮询数据库。已确认消息与会议记录保存在数据库中，供幂等重试和线程引用使用。

## 工程与插件

- [DSH 和 Codex 插件](docs/harnesses.md)
- [GitHub 分支与发布流程](docs/github.md)
- [Agent 工程指南](AGENT.md)
