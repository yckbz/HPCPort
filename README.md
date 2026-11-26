# HPCPort

Agent-agnostic execution gateway, Slurm job manager, resource-budget approval gate, and local Web dashboard for AI coding agents working on remote compute servers.

## Key Capabilities

- **Works with any MCP agent**: Codex CLI, OpenCode, Antigravity, DeepSeek Harness (DSH), Claude Code, and any Streamable HTTP or stdio MCP client.
- **Slurm clusters & bare SSH servers**: Manage `sbatch`/`squeue`/`sacct` job batches on Slurm clusters (`scheduler: slurm`) and detached background tasks on standard SSH GPU/CPU servers (`scheduler: none`).
- **Safety boundaries & approvals**:
  - Remote workspace confinement inside each project's remote `root`
  - Shell command guard (prevents `rm -rf` outside root, stray `sbatch`, destructive system commands, etc.)
  - Per-project concurrency limits (`max_jobs`) and standing compute budgets (`core_hours`, `gpu_hours`, `total_jobs`, cumulative limits)
  - Anti-self-approval Web UI: the daemon opens an authenticated browser session using a 60s single-use exchange code (never revealed to agents), with an optional `web_only_approval` lock and 6-digit PIN fallback for headless SSH forwarding.
- **Smart waiting without brittle push hacks**:
  - Protocol-level **MCP Tasks (`io.modelcontextprotocol/tasks`)** support for Tasks-capable clients
  - 180-second short-wait with progress heartbeats + persistent background monitoring and automatic `[HPCPort notices]` prepended to subsequent tool results for all other clients
- **Single global daemon, zero workspace clutter**: All presets, project configs, SQLite state, and YAML version histories live under `~/.hpcport/` (configurable via `$HPCPORT_HOME`).
- **Apple-minimalist bilingual Web UI**: Light & dark modes, English / Chinese toggle, live SSE updates, approval diff inspection, job script/log viewer, preset editor with connection test and version restore, and one-click MCP setup snippets.

## Quick Start

```bash
# 1. Start the global daemon (127.0.0.1:8765) and open the Web dashboard
hpcport daemon --open

# Or open the dashboard anytime when the daemon is already running:
hpcport ui
```

In the **Settings** tab of the Web dashboard (or via CLI):

```bash
# Add a Slurm cluster preset (or bare SSH server with --scheduler none)
hpcport preset add gpu01 --host login.cluster.edu --projects-root /scratch/alice/hpcport

# Create a project bound to your local workspace and attach the server
hpcport project add my-research --local-dir /path/to/my-research
hpcport project attach my-research gpu01

# Print MCP configuration snippets for Codex, OpenCode, Antigravity, DSH, Claude Code
hpcport snippets my-research
```

## Connecting Agents

### Stdio Bridge (auto-starts daemon & resolves project from cwd)

```json
{
  "mcpServers": {
    "hpcport": {
      "command": "hpcport",
      "args": ["mcp", "--project", "my-research"]
    }
  }
}
```

### Direct Streamable HTTP

Point your MCP client to:

```text
http://127.0.0.1:8765/p/<project>/mcp
```

## Development

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests -v
```
