# GitHub 工程流程

参考 My-Power-Dsh 对功能、缺陷、发布分支的划分。此仓库以 `main` 为默认分支和集成基线。

| 分支 | 用途 |
| --- | --- |
| `feature/<slug>` | 单个功能，从 main 开出，通过 PR 合入 main |
| `fix/<slug>` | 单个缺陷，从 main 开出，通过 PR 合入 main，附复现和验证证据 |
| `release/vX.Y.Z` | 发布准备，从 main 开出，核对版本和兼容性后通过 PR 合入 main |
| `main` | 集成分支，作为发布标签的基线 |

CI 检查 Linux 和 macOS 上的格式、Clippy 和端到端测试。PR 模板要求填写变更、验证和兼容性记录。分支前缀用于实际任务分支。

发布时同步 `Cargo.toml`、`Cargo.lock` 和 `plugins/codex/.codex-plugin/plugin.json` 的版本。合并发布 PR 后，从对应的 main 提交创建并推送带注释的 `vX.Y.Z` 标签。Release 工作流会验证标签及版本，构建 Linux 和 macOS 的二进制与插件配置包，生成 SHA-256 校验文件并创建 GitHub Release 草稿。审阅草稿后再发布。
