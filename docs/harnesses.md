# DSH 与 Codex 插件

两个插件调用同一个本地 `agent-channel mcp` 服务进程入口，访问同一个 Unix 套接字。每个 harness 进程绑定一个固定 Agent ID。为同一次协作启动两个实例时，分配不同编号即可互通。

## 准备

先按 [通道说明](../README.zh-CN.md) 启动守护进程。将发布包中的二进制加入 `PATH`，或从源码运行 `cargo install --path . --locked`。在启动各 harness 前设置绝对路径：

```sh
export AGENT_CHANNEL_STATE_DIR=/absolute/path/to/.agent-channel
export AGENT_CHANNEL_CREDENTIALS_FILE=/absolute/path/to/agent-2.json
```

凭据文件采用 `{"agent_id":"2","token":"对应编号的令牌"}`，归当前用户所有，文件权限为 `0600`。每个实例只传自己的文件路径。Codex 也可在环境中同时提供 `AGENT_CHANNEL_ID` 和 `AGENT_CHANNEL_TOKEN`。守护进程使用完整 roster，插件只使用本 Agent 凭据。

## DSH

通过 MPD 使用的 `@deepseek-ai/dsh-mcp-client` 挂载 [Cordis overlay](../plugins/dsh/cordis.patch.yml)。在已含 MCP 客户端的 DSH profile 上执行：

```sh
dsh tui --patch /absolute/path/to/plugins/dsh/cordis.patch.yml
```

可设置 `AGENT_CHANNEL_BIN` 指向绝对二进制路径。若 profile 缺少 MCP 客户端，先通过 `dsh plugin --profile tui add @deepseek-ai/dsh-mcp-client` 安装与当前 DSH 兼容的版本。模型侧工具名以 `mcp__agent_channel__` 开头，例如 `mcp__agent_channel__whoami`。

## Codex

从仓库根目录注册本地 marketplace，再安装插件：

```sh
codex plugin marketplace add "$PWD"
codex plugin add agent-channel@my-power-agent
```

在有上述环境变量的终端启动新会话，用 `/mcp` 检查 `agent_channel`。[插件清单](../plugins/codex/.codex-plugin/plugin.json)随源码提供。也可直接使用 [MCP 配置示例](../plugins/codex/config.toml.example)，将对应表加入本地 Codex 配置。选择其中一种安装方式即可。参见 [Codex MCP 配置](https://learn.chatgpt.com/docs/extend/mcp?surface=cli)和[插件打包文档](https://developers.openai.com/plugins/build/plugins)。

## 跨 harness 验证

在两个实例各调用一次 `whoami`，核对编号。由一个实例用 `send` 发给另一个，后者用 `receive` 读取并在处理后 `ack`。Meeting 流程依次调用 `meeting_create`、各实例的 `meeting_ready`、`meeting_status`、`meeting_summary` 和 `meeting_summaries`。工具参数和等待语义见 [通道说明](../README.zh-CN.md#meeting-模式)。
