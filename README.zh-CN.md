<div align="center">

# HPCPort

**面向 AI Agent 的通用算力服务器执行网关、Slurm 作业管理器、预算审批门禁与本地 Web 控制台。**

[English](README.md) | **简体中文**

</div>

---

HPCPort 运行在任意支持 MCP 的 AI 编程 Agent 与远端 **Slurm 超算集群** / **普通 SSH GPU·CPU 服务器**之间。

你无需把裸 SSH 权限直接交给 Agent，也不必忍受脆弱的轮询脚本；HPCPort 提供：

- **全局单守护进程管理多项目**：本地监听 `127.0.0.1:8765`，所有服务器预设、项目配置、SQLite 状态与 YAML 版本历史均集中在 `~/.hpcport/`，对业务代码目录零侵入。
- **同时支持 Slurm 集群与无 Slurm 的普通 SSH 服务器**：Slurm 集群（`scheduler: slurm`）支持完整的 `sbatch`/`squeue`/`sacct` 批次托管；普通 SSH 服务器（`scheduler: none`）支持基于 `remote_exec(..., background=True)` 的后台长任务管理与实时日志查看。
- **目录安全边界与算力预算门禁**：强制限制远端读写在项目 `root` 之内，拦截越界删除与绕过 `sbatch` 等危险命令；各项目独立维护并发上限（`max_jobs`）与常设预算额度（单次与累计 `core_hours`、`gpu_hours`、`total_jobs`）。
- **防 Agent 自审批的本地 Web 面板**：苹果极简设计风格，支持浅色 / 深色双主题与 **EN / 中文** 实时切换。Agent 可主动调用 `open_dashboard` 帮你唤起浏览器页面，但会话认证采用仅驻留内存的密钥与 60 秒一次性兑换码，Agent 永远无法获取凭据；支持开启 `web_only_approval`（禁用 CLI 审批）以及 SSH 端口转发场景下的 6 位 PIN 码解锁。
- **更稳健的异步等待机制**：
  - 对支持 **MCP Tasks 扩展（`io.modelcontextprotocol/tasks`）** 的客户端（如 Claude Code）原生返回异步 Task 句柄；
  - 对暂不支持 Tasks 的客户端（Codex、OpenCode、Antigravity、DSH）默认提供最长 180 秒短等（带进度心跳），超时自动转入后台持久监控，事件触发后在后续工具返回中自动附带 `[HPCPort notices]` 通知。
- **Agent 提案改进集群预设（`propose_preset`）**：Agent 发现集群环境变更或使用经验时，可发起字段级预设修改提案，带 `[SAFETY]` 安全标记、Diff 对比与保留注释的 YAML 版本回滚。

---

## 零代码安装：直接交给 AI Agent 自动装好

如果你不想手动敲命令，直接把本仓库的 GitHub 链接（或下载后的本地文件夹路径）发给你的 AI Agent，告诉它：

> 请根据 `<仓库链接或本地路径>` 中 README 的「Agent 自动安装协议」帮我安装并配置 HPCPort，接入你的 MCP 配置，然后帮我在浏览器里打开设置页面。

<details>
<summary><strong>Agent 自动安装协议（给 AI Agent 执行的标准化步骤）</strong></summary>

当用户要求你从本地路径或 Git 链接安装 HPCPort 时，请按以下顺序执行：

1. **全局安装 `hpcport` 包**（需 Python >= 3.10）：
   - 优先使用 `uv`（如果已安装）：
     ```bash
     uv tool install "<仓库路径或 git+https 链接>"
     ```
   - 或使用 `pipx`：
     ```bash
     pipx install "<仓库路径或 git+https 链接>"
     ```
   - 通用兜底方案（标准 Python 均可运行）：
     ```bash
     python3 -m venv ~/.hpcport/venv
     ~/.hpcport/venv/bin/pip install "<仓库路径或 git+https 链接>"
     ```
2. **获取绝对可执行路径并自检环境**：
   ```bash
   HPCPORT_BIN="$(command -v hpcport || echo "$HOME/.hpcport/venv/bin/hpcport")"
   "$HPCPORT_BIN" doctor
   ```
3. **将 HPCPort 写入你自己的 MCP 配置文件**，请务必使用上面的**绝对路径** `$HPCPORT_BIN`（避免图形界面客户端因 `PATH` 不含 `~/.local/bin` 而找不到命令）：
   - 通用 stdio 配置（自动拉起全局守护进程，并按当前工作目录 `cwd` 自动匹配项目）：
     ```json
     {
       "mcpServers": {
         "hpcport": {
           "command": "/ABSOLUTE/PATH/TO/hpcport",
           "args": ["mcp"]
         }
       }
     }
     ```
   - 若用户指定了项目名，可设为 `"args": ["mcp", "--project", "<项目名>"]`；原生 HTTP 客户端可直接连接 `http://127.0.0.1:8765/p/<项目名>/mcp`。
4. **拉起守护进程并在用户浏览器打开设置页**：
   ```bash
   "$HPCPORT_BIN" web --tab settings
   ```
5. **向用户交接**：
   - 告知用户浏览器已打开 HPCPort 设置页面，可直接点击**新增服务器**和**新建项目**（或让用户提供 SSH 主机别名由你代填）。
   - 提醒用户**重新加载 MCP 或开启一个新对话**，使刚写入的 HPCPort 工具生效。
   - **安全红线**：绝不要关闭 `web_only_approval`，也绝不要代替用户执行 `hpcport approve`；所有审批与授额必须由用户亲自操作。

</details>

> **说明：仍有哪 2 步需要用户点一下？**
> 1. 同意 Agent 执行安装命令，并在写入 MCP 配置后**重载 MCP / 新开对话**让工具列表刷新。
> 2. 确保本机已配置好目标服务器的 SSH 免密登录（SSH Key 或 `~/.ssh/config`）。

---

## 手动快速开始

### 1. 安装

```bash
# 推荐：使用 uv
uv tool install .
# 或直接从 GitHub 安装：
# uv tool install git+https://github.com/yckbz/HPCPort.git

# 或使用标准 Python 虚拟环境
python3 -m venv ~/.hpcport/venv
~/.hpcport/venv/bin/pip install .
```

### 2. 打开 Web 控制台

```bash
# 自动在后台拉起守护进程，并免密打开浏览器中的设置页
hpcport ui --tab settings

# 或在前台运行守护进程并自动打开页面
hpcport daemon --open

# 一键自检 Python、命令路径、ssh/rsync 与守护进程状态
hpcport doctor
```

如果你通过无桌面环境的 SSH 端口转发（`ssh -L 8765:127.0.0.1:8765 ...`）访问，可打印 6 位配对 PIN 码用于网页解锁：

```bash
hpcport pin
```

### 3. 添加服务器预设与项目（网页或命令行均可）

推荐直接在浏览器的 **设置（Settings）** 页面图形化操作，也支持 CLI（配置命令会自动拉起守护进程）：

```bash
# 添加 Slurm 集群预设
hpcport cluster add gpu01 \
  --host login.cluster.edu \
  --projects-root /scratch/alice/hpcport \
  --partitions a100,cpu \
  --max-jobs 10

# 或添加无 Slurm 的普通 SSH GPU/CPU 服务器
hpcport cluster add lab-4090 \
  --host lab-gpu.local \
  --scheduler none \
  --projects-root /data/alice/hpcport

# 测试 SSH 连通性与 Slurm 检测
hpcport cluster test gpu01

# 注册项目（绑定本地目录）并挂载/解绑服务器
hpcport project init my-research --dir /path/to/my-research
hpcport cluster attach my-research gpu01

# 打印各大客户端的 MCP 接入配置片段
hpcport snippets my-research
```

---

## 各大 AI Agent 接入方式

HPCPort 同时支持**自动拉起守护进程的 stdio 桥接器**（`hpcport mcp`，可根据 `cwd` 自动识别项目）与**直连 Streamable HTTP**（`http://127.0.0.1:8765/p/<project>/mcp`）。

| Agent 客户端 | 推荐接入方式 | 异步等待机制 |
|---|---|---|
| **Codex CLI** | stdio（`hpcport mcp`）或 HTTP | 180 秒短等 + `[HPCPort notices]` |
| **OpenCode** | Streamable HTTP 或 stdio | 180 秒短等 + `[HPCPort notices]` |
| **Antigravity** | stdio（`hpcport mcp`） | 180 秒短等 + `[HPCPort notices]` |
| **DeepSeek Harness (DSH)** | Streamable HTTP 或 stdio | 180 秒短等 + `[HPCPort notices]` |
| **Claude Code** | Streamable HTTP 或 stdio | 原生 **MCP Tasks**（`io.modelcontextprotocol/tasks`） |

在 Web 控制台的 **设置 → 项目管理与 MCP 接入配置** 中可直接切换上述 5 种客户端并一键复制完整配置。

通用 stdio 配置示例（跨项目自动按当前目录匹配）：

```json
{
  "mcpServers": {
    "hpcport": {
      "command": "hpcport",
      "args": ["mcp"]
    }
  }
}
```

---

## 提供给 Agent 的 MCP 工具一览

| 工具名 | 功能说明 |
|---|---|
| `info` | 查看当前项目挂载的服务器清单、远端 `root`、分区、集群指南与常设预算状态 |
| `remote_exec` | 在项目远端 `root` 内执行 Shell 命令（经过守卫检查，支持 `background=True` 运行无 Slurm 后台长任务） |
| `transfer` | 通过 `rsync` 在本地工作区与远端服务器之间双向传输文件（大文件需审批） |
| `remote_stop` | 终止正在运行的后台命令或文件传输（`op:<id>`） |
| `submit` | 解析 `#SBATCH` 脚本、估算核时/卡时、走预算与审批门禁并托管提交 Slurm 批次 |
| `jobs` | 查询 Slurm 批次与作业明细、后台任务 `ops` 以及未登记的外部 Slurm 作业 |
| `cancel` | 取消 Slurm 批次、单个作业或尚未批准的提交 |
| `wait_for` | 等待 Slurm 批次或后台任务完成（支持 Tasks 的客户端直接返回异步 Task，其余短等 180 秒后挂后台 + notices） |
| `propose_preset` | 向用户提议更新共享服务器预设，进入 Web 审批队列并展示字段 Diff 与 `[SAFETY]` 标记 |
| `open_dashboard` | 通知守护进程在用户的系统浏览器中免密打开 Web 控制台（绝不向 Agent 泄露认证凭据） |

---

## 安全模型与目录结构

- **集中存储**：全部状态存放在 `~/.hpcport/`（可通过 `HPCPORT_HOME` 自定义）：
  - `config.yaml` — 端口、`auto_open_dashboard`、`web_only_approval` 与默认界面语言
  - `clusters/<name>.yaml` — 全局共享服务器预设（带 `.history/` 历史版本）
  - `projects/<name>.yaml` — 项目远端目录、并发上限覆盖与独立策略
  - `state.db` — SQLite WAL 数据库，记录批次、作业、后台任务、审批、等待、通知与审计日志
- **防 Agent 自审批**：Web Cookie 仅由守护进程内存密钥通过 60 秒一次性浏览器重定向码（`/auth/exchange?code=...`）或 6 位 PIN 码签发；所有写操作校验 `X-HPCPort-UI: 1` 防 CSRF；在设置页开启 **仅允许网页审批** 后将彻底禁用 CLI 审批。

---

## 开发与测试

```bash
uv venv --python 3.12
uv pip install -e ".[dev]"
PYTHONPATH=src .venv/bin/python -m pytest tests -v
```
