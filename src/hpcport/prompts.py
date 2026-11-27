"""All agent-visible English texts for HPCPort.

This file is the single source of truth for MCP server instructions, tool
descriptions, parameter descriptions, tool responses, notices, and error texts.
"""

# === 1. Server instructions

SERVER_INSTRUCTIONS = """\
You are connected to HPCPort, a compute-server execution gateway and job manager for project `{project}`{local_part}.

## Compute servers
{clusters}
## Operating rules
- Access remote compute servers exclusively through HPCPort tools (`remote_exec`, `transfer`, `remote_stop`, `submit`, `jobs`, `cancel`, `wait_for`). Do not run `ssh`, `scp`, or `rsync` directly from a local shell.
- All remote operations are confined to each server's project remote root. Do not attempt to delete or modify files outside it.
- On Slurm clusters (`scheduler: slurm`), always submit batch workloads with `submit`; never invoke `sbatch`, `srun`, or `salloc` inside `remote_exec`. Keep login-node commands lightweight.
- On bare SSH servers (`scheduler: none`), run long workloads with `remote_exec(..., background=True)` and monitor them with `jobs` or `wait_for`.
- Approvals (job budgets, dangerous commands, large transfers, preset edits) are decided solely by the user on the HPCPort Web dashboard. Never attempt to call HPCPort's internal HTTP API or run `hpcport` approval commands. If the user needs to view jobs or approve a request, call `open_dashboard`.
- When you learn a durable server rule or environment note (modules, conda env, GPU quirks, queue conventions) not yet in a preset, propose updating the preset with `propose_preset`.
"""

SERVER_INSTRUCTIONS_UNBOUND = """\
You are connected to HPCPort, but the current workspace is not yet registered as a project.
Call `open_dashboard(tab="settings")` to open the HPCPort Web dashboard so the user can register this project and attach a compute server, or ask the user to run `hpcport project init`.
"""

CLUSTER_LINE_SLURM = (
    "- {name} (slurm): remote root `{root}`; partitions {partitions}; "
    "max {max_jobs} concurrent jobs; max exec timeout {exec_timeout}s"
)
CLUSTER_LINE_NONE = (
    "- {name} (bare SSH, no scheduler): remote root `{root}`; "
    "max exec timeout {exec_timeout}s"
)
CLUSTER_TEMPLATE_PART = "; job template `{template}`"
NO_CLUSTERS = "- (no compute servers attached to this project yet; use `open_dashboard` -> Settings to attach one)"

# === 2. MCP tool descriptions

TOOL_REMOTE_EXEC = """\
Run a shell command on a compute server inside the project's remote root.

- Runs detached in a login shell inside `cwd` (pipes, heredocs, `module load`, `conda activate` all work).
- `upload`: optional local files/directories (inside the project's local directory) copied to `cwd` before running.
- Waits up to {wait_s}s by default; if still running, continues in the background as `op:<id>`. Set `background=True` for long-running commands so the call returns immediately.
- Enforces `timeout_s` by terminating the entire process group (and cleans up leftover child processes on return).
- On Slurm clusters, `sbatch`/`srun`/`salloc` are refused here (use `submit`). Risky operations (deleting outside root, editing dotfiles/`~/.ssh`, `crontab`, `scancel`, nested `ssh`, `pkill`) require user approval on the Web dashboard."""

ARG_CLUSTER = "Name of the compute server preset attached to this project."
ARG_EXEC_COMMAND = "Shell command to execute on the server."
ARG_EXEC_CWD = "Remote working directory (absolute path inside remote root, or relative to remote root). Defaults to remote root."
ARG_EXEC_UPLOAD = "Local files or directories inside the project's local directory to upload to `cwd` before running."
ARG_EXEC_TIMEOUT = "Time limit in seconds (up to the server's `exec_timeout_s`). Defaults to 120s in foreground or `exec_timeout_s` in background."
ARG_BACKGROUND = "If true, start in the background and return `op:<id>` immediately without waiting."

TOOL_TRANSFER = """\
Copy files or directories via `rsync -a --partial` between local project directory and a server, or between two servers (relayed through this machine).

- Remote endpoints use `<server>:<path>` (relative paths resolve from remote root; `<server>:` means remote root). Local endpoints must lie inside the project's local directory.
- Interrupted transfers resume automatically when the same call is repeated.
- Transfers exceeding the project's size limit require user approval on the Web dashboard."""

ARG_SRC = "Source endpoint: `<server>:<path>` or a local path."
ARG_DST = "Destination endpoint: `<server>:<path>` or a local path (parent directory must exist)."
ARG_EXCLUDE = "Optional rsync `--exclude` glob patterns."
ARG_TRANSFER_TIMEOUT = "Time limit in seconds (max 3600; default 1800)."

TOOL_REMOTE_STOP = "Stop a running background command or transfer (`op:<id>`) and return its final status."
ARG_OP_ID = "Numeric operation ID (e.g. 12 for `op:12`)."

TOOL_SUBMIT = """\
Submit one or more Slurm job scripts on a Slurm cluster and register them under a batch.

- `workdir` can be any directory inside the project's remote root (absolute or relative to remote root); scripts must already exist there and declare `#SBATCH` directives (including `#SBATCH -t` and CPU/GPU counts so HPCPort can calculate resource limits).
- `batch` is an optional batch label; omit it to auto-generate a short ID (`b001`, `b002`, ...), or reuse an existing label to aggregate multiple submissions into one batch.
- Jobs exceeding the project's `max_jobs` concurrency limit are queued as `held` and submitted automatically as slots free up.
- If user approval is required (no standing budget, unknown resources, or over limit), `submit` waits up to `wait_s` (default 180s) for approval on the Web dashboard; if still pending, it returns the approval ID and submits automatically once approved."""

ARG_WORKDIR = "Working directory on the cluster (inside remote root; absolute or relative to remote root)."
ARG_SCRIPTS = "Job script paths relative to `workdir`, or glob patterns such as `run_*.sh`."
ARG_DESC = "Short one-line description (<= 60 chars) shown on the dashboard and approval cards."
ARG_BATCH = "Optional batch label (alphanumeric, '.', '_', '-'). Omit to auto-assign `b001`, `b002`, etc."
ARG_TAG = "Optional short free-form tag (e.g. `train`, `eval`, `sim`, `sweep`)."
ARG_WAIT_S = "Maximum seconds to wait synchronously (0 to 180; default from project config)."

TOOL_JOBS = """\
Inspect registered Slurm batches, individual jobs, unregistered jobs, and background operations (`ops`) for this project.
Use this tool proactively to check live progress whenever you are not blocked in `wait_for`."""

ARG_JOBS_ID = "Optional batch label (e.g. `b001`) or `op:<id>` for detailed inspection. Omit for project overview."
ARG_JOBS_CLUSTER = "Optional server name filter."

TOOL_CANCEL = """\
Cancel Slurm jobs in this project. Specify exactly one of `batch`, `job_ids`, or `approval`.
Queued jobs are confirmed cancelled immediately via `scontrol` so scripts can be edited and resubmitted right away."""

ARG_CANCEL_BATCH = "Batch label: cancel all unfinished jobs in that batch."
ARG_CANCEL_IDS = "List of Slurm job IDs (optionally `cluster:slurm_id`, array elements like `12345_0`, or internal `#id`) to cancel."
ARG_CANCEL_APPROVAL = "Pending submission approval ID to withdraw without submitting."
ARG_CANCEL_CLUSTER = "Optional cluster name to disambiguate Slurm job IDs when a project uses multiple Slurm clusters."

TOOL_WAIT_FOR = """\
Wait for Slurm batches (`b001`, ...) and/or background gateway operations (`op:12`, ...) until a condition triggers.

- Items are grouped by priority: `2` triggers when any item in the group ends; `1` (default) triggers when every item in the group ends; `0` never triggers alone (piggybacks results onto the next trigger).
- Also triggers if a batch's failure rate exceeds `fail_frac` (once per batch) or `timeout_h` elapses.
- On clients supporting MCP Tasks (`io.modelcontextprotocol/tasks`), returns a durable async task automatically polled by the client host. On other clients, waits synchronously up to `wait_s` (default 180s) and continues monitoring in the background with automatic `notices` delivery."""

ARG_GROUPS = "Priority groups to watch. Omit to watch all unfinished batches and running ops in one priority-1 group."
ARG_GROUP_ITEMS = "Batch labels (e.g. `b001`) and/or background operation refs (e.g. `op:12`)."
ARG_GROUP_PRIORITY = "2 = wake on any; 1 = wake on all (default); 0 = piggyback only."
ARG_FAIL_FRAC = "Failure rate threshold (0 to 1; 1 disables). Defaults to project setting."
ARG_TIMEOUT_H = "Overall monitoring timeout in hours. Defaults to project setting."
ARG_NOTE = "Optional short reminder note returned unchanged when the wait triggers."

TOOL_PROPOSE_PRESET = """\
Propose a field-level patch to a shared compute server preset (for example adding usage notes to `guide`, updating partitions, or adjusting `exec_timeout_s`).
Enters the user's Web approval queue with a field-by-field diff; takes effect only after approval."""

ARG_CHANGES = "Mapping of preset fields to new values (`null` removes a field). Only include fields you intend to change."
ARG_PROPOSAL_SUMMARY = "Concise one-line explanation of why this preset change is proposed."

TOOL_INFO = """\
Return the current project's compute servers (Slurm clusters and bare SSH servers), remote root directories, allowed partitions, concurrency limits, usage guides, and standing budget status.
Call this once at the start of a session before running commands or submitting jobs."""

TOOL_OPEN_DASHBOARD = """\
Open the authenticated HPCPort Web dashboard directly in the user's browser so the user can review jobs, inspect logs, manage presets, or approve pending requests.
Use this whenever approval is pending or the user asks to open the dashboard."""

ARG_TAB = "Optional tab to focus: `approvals`, `jobs`, `audit`, or `settings`."

# === 3. Tool outputs & notices

NOTICES_HEADER = "[HPCPort notices]"
NOTICES_SEP = "---"

NOTICE_TEXTS = {
    "batch_done": "Batch `{batch}` on `{cluster}` finished ({summary}).",
    "jobs_cancelled": "User cancelled job(s) in {target} ({items}).{comment}",
    "op_ended": "Background {kind} `op:{op}` on `{cluster}` {outcome}.",
    "submit_approved": "Approval #{approval} was approved ({batches}); jobs queued for submission.{comment}",
    "submit_refused": "Approval #{approval} was rejected ({batches}); jobs cancelled.{comment}",
    "permission": "Approval #{approval} for `{tool}` was {verdict}.{grant}{comment}",
    "preset_updated": "Cluster preset `{cluster}` updated to version {version}.",
    "cluster_attached": "Server `{cluster}` ({scheduler}) attached with remote root `{root}`.",
    "proposal_rejected": "Preset proposal #{proposal} ({doc}) was rejected.{comment}",
    "proposal_stale": "Preset proposal #{proposal} ({doc}) became stale (changed: {moved}).",
    "wait_done": "Background wait #{wait_id} completed ({finished}).",
    "wait_fail": "Background wait #{wait_id} alert: batch `{batch}` failure rate {pct}% exceeded {thresh}%.",
    "wait_timeout": "Background wait #{wait_id} timed out after {timeout_h}h.",
}

SUBMIT_OK = "Submitted batch `{batch}` on `{cluster}` ({n} job(s): {sent_part}{held_part}{res_part})."
SUBMIT_PENDING = (
    "Batch `{batch}` registered ({n} job(s){res_part}) and is waiting for user approval "
    "(approval #{approval}) on the HPCPort Web dashboard. Call `open_dashboard(tab=\"approvals\")` "
    "if the user has not opened it yet; jobs will submit automatically once approved."
)
SUBMIT_REFUSED_INLINE = "Submission for batch `{batch}` was rejected by the user.{comment}"
SUBMIT_CANCELLED_INLINE = "Submission for batch `{batch}` was cancelled while waiting for approval.{comment}"
SUBMIT_APPROVED_INLINE = "User approved submission (#{approval}); submitted batch `{batch}` on `{cluster}` ({n} job(s): {sent_part}{held_part})."

WAIT_STILL_RUNNING = (
    "Still running after {waited:.0f}s (wait #{wait_id}). Current status: {progress}. "
    "Monitoring continues in the background; check progress anytime with `jobs`, and state "
    "changes will also be delivered automatically as notices on your next tool call."
)

WAIT_FIRED_DONE = "Wait #{wait_id} completed: {finished}."
WAIT_FIRED_FAIL = "Wait #{wait_id} failure-rate alert: batch `{batch}` reached {pct:.1f}% failures (threshold {thresh:.1f}%)."
WAIT_FIRED_TIMEOUT = "Wait #{wait_id} timed out after {timeout_h:g}h."

GATE_PENDING = (
    "Operation blocked pending user approval (approval #{approval}: {reasons}). "
    "Ask the user to approve it on the HPCPort Web dashboard (you may call `open_dashboard`), "
    "then retry the exact same tool call once approved."
)
GATE_REFUSED = "Operation denied by the user (approval #{approval}: {reasons}).{comment}"

GATE_WHY = {
    "scheduler": "`{what}` is not allowed in `remote_exec` on a Slurm cluster (use `submit`)",
    "scancel": "cancelling Slurm jobs via `{what}`",
    "outside": "destructive file operation outside remote root (`{what}`)",
    "root": "destructive operation targeting the remote root itself (`{what}`)",
    "startup": "modifying shell startup file or `~/.ssh` (`{what}`)",
    "crontab": "modifying crontab",
    "remote": "connecting to another remote host via `{what}`",
    "killall": "killing processes by name via `{what}`",
    "size": "transfer size {size:.2f} GB exceeds limit {limit:g} GB",
}

SUBMIT_WHY = {
    "no_standing": "no standing budget approval for this project",
    "unknown": "missing resource/time directives in: {scripts}",
    "over": "requested {what} ({asked}) exceeds single-submission limit ({limit})",
    "total": "cumulative {what} ({reached}) exceeds standing limit ({limit})",
}

DASHBOARD_OPENED = "Opened the HPCPort Web dashboard in the user's browser ({url})."
DASHBOARD_HEADLESS = (
    "Could not launch a local browser automatically (headless environment). "
    "Tell the user to open {url} in their browser and enter the 6-digit pairing PIN "
    "printed by the HPCPort daemon."
)

PROPOSE_OK = "Proposed preset update for `{cluster}` (proposal #{id}, approval #{approval}): {fields}. Waiting for user approval on the Web dashboard."

# === 4. Error texts

ERRORS = {
    "unknown_project": "Unknown project `{project}`. Use `open_dashboard(tab=\"settings\")` to register it.",
    "unbound_project": "Current directory is not bound to any registered HPCPort project. Call `open_dashboard(tab=\"settings\")` to register a project.",
    "unknown_cluster": "Server `{cluster}` is not attached to this project (attached: {clusters}).",
    "not_slurm": "Server `{cluster}` has `scheduler: none` (no Slurm). Run commands there with `remote_exec` (use `background=True` for long tasks).",
    "empty_arg": "Argument `{arg}` must not be empty.",
    "bad_batch": "Invalid batch label `{batch}` (use letters, digits, '.', '_', '-').",
    "bad_pattern": "Invalid script path or pattern `{pattern}`.",
    "bad_script": "Script `{script}` must begin with a `#!` shebang line.",
    "no_partition": "Script `{script}` must declare a `#SBATCH -p/--partition` allowed on `{cluster}` ({allowed}).",
    "partition_denied": "Partition `{partition}` in `{script}` is not allowed on `{cluster}` (allowed: {allowed}).",
    "no_root": "Remote root `{root}` does not exist on `{cluster}`.",
    "workdir_outside": "Working directory `{path}` lies outside remote root `{root}` on `{cluster}`.",
    "missing_scripts": "Script(s) not found in `{workdir}`: {scripts}.",
    "duplicate": "Script `{script}` already has an active job ({jobid}, state `{state}`).",
    "unreachable": "Server `{cluster}` could not be reached: {error}. State was left unchanged.",
    "unreachable_dispatch": (
        "Connection to `{cluster}` dropped during submission of batch `{batch}`: {error} "
        "({n_sent} confirmed sent, {n_held} kept as `held` to prevent duplicates and will "
        "auto-reconcile once reachable; check `jobs` before retrying)."
    ),
    "sbatch_rejected": "sbatch rejected `{script}` on `{cluster}`: {error} ({n_sent} submitted: {ids}; {n_unsent} dropped).",
    "cancel_args": "Provide exactly one of `batch`, `job_ids`, or `approval`.",
    "no_unapproved": "No unapproved jobs found for approval #{approval}.",
    "unknown_batch": "Unknown batch `{batch}` in this project.",
    "unknown_jobs": "Unknown Slurm job ID(s): {ids}.",
    "ambiguous_job_id": "Slurm job ID `{id}` exists on multiple clusters ({clusters}); specify `cluster` or `{id}` prefixed as `<cluster>:{id}`.",
    "unknown_op": "Unknown background operation `op:{id}`.",
    "op_ended": "Operation `op:{id}` ({kind}) has already ended.",
    "bad_timeout_s": "`timeout_s` must be between 1 and {max} on `{cluster}`.",
    "bad_transfer_timeout": "`timeout_s` for transfer must be between 1 and {max}.",
    "bad_cwd": "Directory `{path}` lies outside remote root `{root}` on `{cluster}`.",
    "no_cwd": "Directory `{path}` does not exist on `{cluster}`.",
    "remote_outside": "Remote path `{path}` lies outside remote root `{root}` on `{cluster}`.",
    "local_outside": "Local path `{path}` lies outside project local directory `{project_dir}`.",
    "local_protected": "Writing to protected local path `{path}` is not allowed.",
    "no_local_dir": "Project `{project}` has no `local_dir` configured; local file uploads/transfers are disabled.",
    "bad_upload": "Local upload file `{path}` does not exist.",
    "upload_failed": "Failed to upload files to `{cluster}`: {error}.",
    "exec_failed": "Failed to start remote command on `{cluster}`: {error}.",
    "use_submit": "`{command}` is disabled in `remote_exec` on Slurm clusters; use the `submit` tool instead.",
    "too_many_background": "Too many running background operations ({n}/{max}); wait for one to end or stop it with `remote_stop`.",
    "bad_path": "Remote path `{path}` contains unsupported characters.",
    "both_local": "At least one endpoint of `transfer` must be a remote `<server>:<path>`.",
    "no_source": "Transfer source `{path}` does not exist.",
    "bad_priority": "Invalid group priority `{priority}` (use 0, 1, or 2).",
    "bad_groups": "Invalid `groups` argument (expected a list of `{items, priority}` objects).",
    "empty_group": "Each wait group must contain at least one item.",
    "item_twice": "Item `{item}` appears in more than one wait group.",
    "all_finished": "All specified items have already finished.",
    "group_ended": "Group (priority {priority}) is already satisfied.",
    "nothing_to_wait": "Nothing unfinished to wait for (provide items or an explicit `timeout_h`).",
    "bad_fail_frac": "`fail_frac` must be between 0 and 1.",
    "bad_timeout": "`timeout_h` must be positive.",
    "empty_changes": "`changes` must be a non-empty mapping of preset fields.",
    "no_effect": "The proposed changes match the current approved preset (no effective change).",
    "unknown_setting": "Unknown setting `{key}`.",
    "undecided_file_edit": "Setting `{key}` has an undecided disk edit (proposal #{proposal}); resolve it first.",
    "preset_exists": "Server preset `{cluster}` already exists.",
    "preset_in_use": "Server preset `{cluster}` is still attached to project(s): {projects}. Detach it first.",
    "project_exists": "Project `{project}` already exists.",
    "project_retired": "Project `{project}` has been retired. Its name is reserved for its history.",
    "unknown_version": "Version v{version} does not exist for `{key}`.",
    "unknown_approval": "Unknown approval #{id}.",
    "already_decided": "Approval #{id} was already {state}.",
    "bad_option": "{msg}",
    "bad_limit": "Invalid budget limit for `{name}` (must be a non-negative number or null).",
    "limits_without_always": "Budget limits ({names}) can only be supplied together with standing approval (`always=true`).",
    "invalid_setting": "Invalid setting in approval #{id}: {msg}",
    "missing_setting": "Setting file missing for approval #{id}: {msg}",
    "stale": "Proposal in approval #{id} is stale: {msg}",
    "changed": "Disk file changed since approval #{id} was created: {msg}",
    "ambiguous_batch": "Batch `{batch}` exists in multiple projects ({projects}); specify `project`.",
    "batch_cluster_mismatch": "Batch `{batch}` already belongs to cluster `{existing}` and cannot mix jobs from `{cluster}`; use a different batch label.",
    "cluster_busy": "Cannot detach `{cluster}` from `{project}` while active jobs or background operations are running.",
}
