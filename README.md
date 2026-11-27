# My-Power-Agent

[简体中文](README.zh-CN.md)

My-Power-Agent is a local Rust message channel for agent harnesses. Each agent has a fixed ID, its own credential, and a durable inbox. Clients can send, query, and acknowledge messages through a Unix socket, CLI, or MCP. A meeting opens summary exchange only after all of its participants report ready.

## Get started

Build with Rust and a C compiler on Linux or macOS. Create a private roster JSON mapping numeric strings to unique tokens of at least 32 characters. See the [full setup and protocol reference](README.zh-CN.md) for a roster-generation example and request fields.

```sh
cargo build --release --locked
target/release/agent-channel serve --roster roster.json
```

Each harness needs the same state directory and its own credential. See [DSH and Codex setup](docs/harnesses.md) for plugin installation and tool calls.

## Delivery and meetings

- Messages and acknowledgements use SQLite transactions. Broadcasts commit atomically and store their payload once. A sender can retry with the same request ID after an uncertain response.
- Inbox readers can query immediately, paginate, or wait for a recipient event. Unacknowledged messages survive daemon restarts.
- Meeting participants are fixed at creation. The last ready report opens the exchange phase. Summary submissions survive restarts and are available to participants through the meeting tools.

The delivery design draws on the [My-Power-Dsh mailbox](https://github.com/HaroldZ32/My-Power-Dsh/blob/master/docs/mail.md).

## Contribute

See the [agent manual](AGENT.md) and the [GitHub branch and release workflow](docs/github.md). Run `cargo test --locked --release` to exercise access through the Unix socket and MCP.
