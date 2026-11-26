<div align="center">

# HPCPort

**Agent-agnostic execution gateway, Slurm job manager, compute-budget approval gate, and local Web dashboard for AI coding agents working on remote compute servers.**

**English** | [简体中文](README.zh-CN.md)

</div>

---

HPCPort sits safely between your MCP-compatible AI coding agents and your remote **Slurm clusters** or **bare SSH GPU/CPU servers**.

Instead of giving an agent raw, unguarded SSH access or polling loops, HPCPort provides:

- **One global daemon for all projects**: Listens on `127.0.0.1:8765`, stores all presets, projects, SQLite state, and YAML version history cleanly under `~/.hpcport/` — zero clutter inside your code repositories.
- **Slurm clusters & bare SSH servers**: Full `sbatch`/`squeue`/`sacct` batch lifecycle on Slurm clusters (`scheduler: slurm`) plus detached background execution (`remote_exec(..., background=True)`) on standard GPU/CPU servers without Slurm (`scheduler: none`).
- **Workspace & command safety boundaries**: Confines remote operations inside each project's remote `root`, guards destructive or out-of-bounds shell commands, and enforces per-project concurrency limits (`max_jobs`) and standing compute budgets (`core_hours`, `gpu_hours`, `total_jobs`).
- **Anti-self-approval local Web UI**: Modern, Apple-minimalist light/dark dashboard with **EN / 中文** toggle. Agents can open the dashboard for you (`open_dashboard`), but authentication uses an in-memory secret and single-use 60-second browser exchange codes that are never exposed to the agent — plus optional `web_only_approval` lock and 6-digit PIN unlock for headless SSH port forwarding.
- **Smart waiting without brittle push hacks**:
  - Protocol-level **MCP Tasks (`io.modelcontextprotocol/tasks`)** support for Tasks-capable clients (e.g. Claude Code).
  - 180-second short-wait with progress heartbeats + persistent background monitoring and automatic `[HPCPort notices]` prepended to subsequent MCP tool results for all other clients (Codex, OpenCode, Antigravity, DSH).
- **Agent-proposed preset improvements (`propose_preset`)**: Agents can suggest cluster preset updates (like login-node usage notes or module guides) with field-level diffs, `[SAFETY]` flags, and comment-preserving YAML version rollback.

---

## Zero-Code Setup: Let Your AI Agent Install It

If you prefer not to run terminal commands manually, simply copy and send the following prompt to your AI agent (if you already downloaded the repository locally, replace the URL with your local folder path):

> Install HPCPort from `https://github.com/yckbz/HPCPort` following its README "Agent Self-Install Protocol", connect it to your MCP config, and open the Settings Web dashboard for me.

<details>
<summary><strong>Agent Self-Install Protocol (instructions for AI agents)</strong></summary>

When a user asks you to install HPCPort (from GitHub or a local folder path), perform these exact steps:

1. **Install the `hpcport` package globally** (requires Python >= 3.10; replace the URL with the local path if the user provided one):
   - Preferred if `uv` is installed:
     ```bash
     uv tool install git+https://github.com/yckbz/HPCPort.git
     ```
   - Or if `pipx` is installed:
     ```bash
     pipx install git+https://github.com/yckbz/HPCPort.git
     ```
   - Fallback (always works with standard Python):
     ```bash
     python3 -m venv ~/.hpcport/venv
     ~/.hpcport/venv/bin/pip install git+https://github.com/yckbz/HPCPort.git
     ```
2. **Resolve the absolute executable path and verify the environment**:
   ```bash
   HPCPORT_BIN="$(command -v hpcport || echo "$HOME/.hpcport/venv/bin/hpcport")"
   "$HPCPORT_BIN" doctor
   ```
3. **Register `hpcport` in your own MCP configuration** using the **absolute path** `$HPCPORT_BIN` (important for GUI clients whose default `PATH` omits `~/.local/bin`):
   - Generic stdio MCP config (auto-starts the global daemon and automatically matches the active project by working directory `cwd`):
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
   - Or, if the user already named a project, pass `"args": ["mcp", "--project", "<project-name>"]` or point HTTP-native clients to `http://127.0.0.1:8765/p/<project-name>/mcp`.
4. **Start the daemon and open the Settings dashboard in the user's browser**:
   ```bash
   "$HPCPORT_BIN" ui --tab settings
   ```
5. **Hand off to the user**:
   - Tell the user the Settings dashboard is open in their browser where they can click **Add Server** and **New Project** (or offer to add presets/projects for them if they share their SSH host / alias).
   - Remind the user to **reload MCP / start a new chat session** once so your client loads the newly registered HPCPort MCP tools.
   - **Security rule**: Never disable `web_only_approval` and never run `hpcport approve` on the user's behalf; approvals belong strictly to the user.

</details>

> **What still needs one quick human action?**
> 1. Your agent may ask you to approve the install command and **reload MCP / open a new chat** so the newly added MCP tools become active.
> 2. Ensure your machine can reach your remote server via passwordless SSH (SSH key or `~/.ssh/config`).

---

## Manual Quick Start

### 1. Install

```bash
# Recommended: install directly from GitHub with uv
uv tool install git+https://github.com/yckbz/HPCPort.git

# Or install from a local clone
uv tool install .

# Or using standard Python venv + pip
python3 -m venv ~/.hpcport/venv
~/.hpcport/venv/bin/pip install git+https://github.com/yckbz/HPCPort.git
```

### 2. Open the Web Dashboard

```bash
# Auto-starts the background daemon and opens the authenticated Web UI in your browser
hpcport ui --tab settings

# Or run the daemon in the foreground and auto-open the dashboard
hpcport daemon --open

# Check environment, binary path, ssh/rsync, and daemon status
hpcport doctor
```

On a headless remote machine over SSH port forwarding (`ssh -L 8765:127.0.0.1:8765 ...`), print the 6-digit pairing PIN:

```bash
hpcport pin
```

### 3. Add a Server & Project (Web UI or CLI)

You can configure everything visually in the **Settings** tab, or from the CLI (all setup commands auto-start the daemon if needed):

```bash
# Add a Slurm cluster preset
hpcport cluster add gpu01 \
  --host login.cluster.edu \
  --projects-root /scratch/alice/hpcport \
  --partitions a100,cpu \
  --max-jobs 10

# Or add a bare SSH GPU/CPU server without Slurm
hpcport cluster add lab-4090 \
  --host lab-gpu.local \
  --scheduler none \
  --projects-root /data/alice/hpcport

# Test SSH connectivity and Slurm detection
hpcport cluster test gpu01

# Register a project bound to your local folder and attach/detach servers
hpcport project init my-research --dir /path/to/my-research
hpcport cluster attach my-research gpu01

# Print ready-to-copy MCP configs for supported clients
hpcport snippets my-research
```

---

## Connecting AI Agents (MCP)

HPCPort supports **both** a zero-daemon-setup stdio bridge (`hpcport mcp`, which auto-starts the daemon and auto-matches the project by `cwd`) and direct **Streamable HTTP** (`http://127.0.0.1:8765/p/<project>/mcp`).

| Agent Client | Transport | Wait Behavior |
|---|---|---|
| **Codex CLI** | stdio (`hpcport mcp`) or HTTP | 180s short-wait + `[HPCPort notices]` |
| **OpenCode** | Streamable HTTP or stdio | 180s short-wait + `[HPCPort notices]` |
| **Antigravity** | stdio (`hpcport mcp`) | 180s short-wait + `[HPCPort notices]` |
| **DeepSeek Harness (DSH)** | Streamable HTTP or stdio | 180s short-wait + `[HPCPort notices]` |
| **Claude Code** | Streamable HTTP or stdio | Native **MCP Tasks** (`io.modelcontextprotocol/tasks`) |

You can copy exact, pre-filled config snippets for all 5 clients inside **Settings → Projects → Connect Your Agent (MCP)** in the Web dashboard.

Example stdio config (works across projects automatically via `cwd`):

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

## MCP Tools Exposed to Agents

| Tool | Purpose |
|---|---|
| `info` | Inspect the current project's attached compute servers, remote roots, partitions, usage guides, and standing budget |
| `remote_exec` | Run shell commands inside a project's remote `root` (with guard checks & `background=True` support for bare SSH tasks) |
| `transfer` | Transfer files/directories between local workspace and remote server via `rsync` (size-gated) |
| `remote_stop` | Stop a running background exec or transfer operation (`op:<id>`) |
| `submit` | Validate `#SBATCH` headers, estimate core/GPU-hours, enforce budget approvals, and submit Slurm batches |
| `jobs` | Inspect batch summaries, individual job rows, background `ops`, and unregistered Slurm jobs |
| `cancel` | Cancel Slurm batches, individual jobs, or pending unapproved submissions |
| `wait_for` | Wait for Slurm batches or background `ops` (upgrades to native MCP Task when supported; otherwise 180s short-wait + notices) |
| `propose_preset` | Propose field-level updates to a shared server preset for human review on the Web UI |
| `open_dashboard` | Ask the daemon to open the authenticated Web dashboard in the user's browser (never exposes auth secrets to the agent) |

---

## Security & Architecture

- **Data directory**: Everything lives under `~/.hpcport/` (override with `HPCPORT_HOME`):
  - `config.yaml` — global port, `auto_open_dashboard`, `web_only_approval`, UI language
  - `clusters/<name>.yaml` — shared compute server presets (with `clusters/history/<name>/` versions)
  - `projects/<name>.yaml` — per-project remote roots, `max_jobs` overrides, and policies (with `projects/history/<name>/` versions)
  - `state.db` — SQLite WAL state for batches, jobs, background ops, approvals, waits, notices, and audit log
- **Anti-self-approval**: Web cookies are minted only via an in-memory daemon secret redeemed through a 60s single-use browser redirect (`/auth/exchange?code=...`) or the 6-digit pairing PIN. All mutating Web endpoints require `X-HPCPort-UI: 1` (CSRF protection), and enabling `web_only_approval` in Settings disables CLI approvals completely.

---

## Development

```bash
uv venv --python 3.12
uv pip install -e ".[dev]"
PYTHONPATH=src .venv/bin/python -m pytest tests -v
```
