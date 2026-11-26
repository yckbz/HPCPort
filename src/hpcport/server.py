"""HTTP front of the global HPCPort daemon (127.0.0.1).

Routes:
  /p/{project}/mcp   Project-scoped stateful Streamable HTTP MCP endpoint
  /mcp               Unbound MCP fallback (guides setup & open_dashboard)
  /auth/exchange     Single-use 60s code -> HttpOnly cookie -> redirect to Web UI
  /api/auth/*        Auth status & headless 6-digit PIN unlock
  /api/events        Server-Sent Events push stream for live Web UI updates
  /api/*             Authenticated JSON API for Web UI and CLI
  /, /static/*       Apple-minimalist bilingual local Web dashboard
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

from mcp.server.mcpserver import Context, MCPServer
from mcp.types import CallToolResult, TextContent
from pydantic import BaseModel, Field
from starlette.applications import Starlette
from starlette.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    StreamingResponse,
)
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

from . import config, messages
from . import prompts as P
from .approvals import ApprovalError
from .attach import AttachError
from .config import ConfigError
from .gateway import GatewayError, NeedsApproval
from .jobs import JobError
from .settings import SettingError
from .tasks_ext import TasksExtension
from .waits import WaitError, parse_op_item

log = logging.getLogger("hpcport.server")
ToolErrors = (JobError, WaitError, GatewayError, SettingError, ConfigError)
COOKIE_NAME = "hpcport_session"
WEB_DIR = Path(__file__).resolve().parent / "web"


def arg(description: str):
    return Field(description=description)


class WaitGroup(BaseModel):
    items: Annotated[list[str], arg(P.ARG_GROUP_ITEMS)]
    priority: Annotated[int, arg(P.ARG_GROUP_PRIORITY)] = 1


def _ctx_header(ctx: Context, name: str) -> str | None:
    rc = getattr(ctx, "request_context", None)
    req = getattr(rc, "request", None) if rc is not None else None
    headers = getattr(req, "headers", None) if req is not None else None
    if headers is None:
        return None
    return headers.get(name) or headers.get(name.lower())


def _project_and_session(ctx: Context, daemon=None) -> tuple[str | None, str]:
    project = _ctx_header(ctx, "x-hpcport-project") or None
    if project is None and daemon is not None:
        cwd = _ctx_header(ctx, "x-hpcport-cwd")
        if cwd:
            with daemon.lock:
                project = config.resolve_project_by_cwd(cwd, daemon.projects)
    session_key = _ctx_header(ctx, "mcp-session-id") or "default"
    return project, session_key


def build_mcp(daemon) -> MCPServer:
    mcp = MCPServer(
        "hpcport",
        instructions=(
            "HPCPort compute-server gateway and Slurm job manager. "
            "Call `info` first to inspect this project's attached compute servers, remote roots, "
            "partitions, usage guides, and standing budget. "
            "Use HPCPort tools for all remote commands, file transfers, and Slurm job submissions. "
            "Call `jobs` to check status, `open_dashboard` to open the user's Web approval dashboard, "
            "and `propose_preset` to suggest updates to server presets."
        ),
        extensions=[TasksExtension(daemon)],
    )

    def audit(project: str | None, session_key: str, tool: str, args: dict, result: str) -> None:
        daemon.db.insert(
            "audit",
            ts=daemon.now(),
            project=project,
            session_key=session_key,
            tool=tool,
            args=json.dumps(args, ensure_ascii=False),
            result=result,
        )

    def require_project(ctx: Context) -> tuple[str, str]:
        proj, skey = _project_and_session(ctx, daemon)
        if not proj:
            raise JobError("unbound_project")
        if proj not in daemon.projects:
            raise JobError("unknown_project", project=proj)
        daemon.waits.init_cursor(proj, skey)
        return proj, skey

    def make_progress_cb(ctx: Context, loop: asyncio.AbstractEventLoop):
        def on_tick(elapsed: float, total: float, msg: str) -> None:
            try:
                fut = asyncio.run_coroutine_threadsafe(
                    ctx.report_progress(progress=round(elapsed, 1), total=total, message=msg),
                    loop,
                )
                fut.result(timeout=2.0)
            except Exception:
                pass
        return on_tick

    def wrap_notices(project: str | None, skey: str, text: str) -> str:
        if not project or project not in daemon.projects:
            return text
        unread = daemon.waits.pop_notices(project, skey)
        return messages.with_notices(text, unread)

    async def run_tool(ctx: Context, tool: str, args: dict, fn,
                       locked: bool = False, needs_project: bool = True) -> CallToolResult:
        proj, skey = _project_and_session(ctx, daemon)
        loop = asyncio.get_running_loop()
        on_tick = make_progress_cb(ctx, loop)
        try:
            if needs_project:
                proj, skey = require_project(ctx)
            if locked:
                def _call():
                    with daemon.lock:
                        return fn(proj, skey, on_tick)
                text = await asyncio.to_thread(_call)
            else:
                text = await asyncio.to_thread(fn, proj, skey, on_tick)
            is_err, res_tag = False, "ok"
        except NeedsApproval as e:
            text = messages.gate_result(e)
            is_err = True
            res_tag = "%s (approval %s)" % (
                "pending" if e.answer.get("pending") else "refused",
                e.answer.get("approval"),
            )
        except ToolErrors as e:
            text, is_err, res_tag = messages.error(e), True, "error: " + getattr(e, "code", str(e))
        with daemon.lock:
            audit(proj, skey, tool, args, res_tag)
        if not is_err and needs_project:
            text = wrap_notices(proj, skey, text)
        return CallToolResult(content=[TextContent(type="text", text=text)], is_error=is_err)

    def tool(name: str, description: str):
        return mcp.tool(name=name, description=description, structured_output=False)

    # ---------------------------------------------------------------- gateway tools
    @tool("remote_exec", P.TOOL_REMOTE_EXEC.format(wait_s=180))
    async def remote_exec(
        cluster: Annotated[str, arg(P.ARG_CLUSTER)],
        command: Annotated[str, arg(P.ARG_EXEC_COMMAND)],
        ctx: Context,
        cwd: Annotated[str | None, arg(P.ARG_EXEC_CWD)] = None,
        upload: Annotated[list[str] | None, arg(P.ARG_EXEC_UPLOAD)] = None,
        timeout_s: Annotated[int | None, arg(P.ARG_EXEC_TIMEOUT)] = None,
        background: Annotated[bool, arg(P.ARG_BACKGROUND)] = False,
    ) -> CallToolResult:
        args = dict(cluster=cluster, command=command, cwd=cwd, upload=upload,
                    timeout_s=timeout_s, background=background)
        return await run_tool(
            ctx, "remote_exec", args,
            lambda p, s, tick: messages.gateway_ok(
                daemon.gateway.exec(p, cluster, command, cwd, upload or (), timeout_s,
                                    background, on_tick=tick)
            ),
        )

    @tool("transfer", P.TOOL_TRANSFER)
    async def transfer(
        src: Annotated[str, arg(P.ARG_SRC)],
        dst: Annotated[str, arg(P.ARG_DST)],
        ctx: Context,
        exclude: Annotated[list[str] | None, arg(P.ARG_EXCLUDE)] = None,
        timeout_s: Annotated[int | None, arg(P.ARG_TRANSFER_TIMEOUT)] = None,
        background: Annotated[bool, arg(P.ARG_BACKGROUND)] = False,
    ) -> CallToolResult:
        args = dict(src=src, dst=dst, exclude=exclude, timeout_s=timeout_s, background=background)
        return await run_tool(
            ctx, "transfer", args,
            lambda p, s, tick: messages.gateway_ok(
                daemon.gateway.transfer(p, src, dst, exclude or (), timeout_s, background,
                                        on_tick=tick)
            ),
        )

    @tool("remote_stop", P.TOOL_REMOTE_STOP)
    async def remote_stop(
        id: Annotated[int, arg(P.ARG_OP_ID)],
        ctx: Context,
    ) -> CallToolResult:
        return await run_tool(
            ctx, "remote_stop", {"id": id},
            lambda p, s, tick: messages.gateway_ok(daemon.gateway.stop(p, id, by="agent")),
        )

    # ---------------------------------------------------------------- Slurm job tools
    @tool("submit", P.TOOL_SUBMIT)
    async def submit(
        cluster: Annotated[str, arg(P.ARG_CLUSTER)],
        workdir: Annotated[str, arg(P.ARG_WORKDIR)],
        scripts: Annotated[list[str], arg(P.ARG_SCRIPTS)],
        desc: Annotated[str, arg(P.ARG_DESC)],
        ctx: Context,
        batch: Annotated[str | None, arg(P.ARG_BATCH)] = None,
        tag: Annotated[str, arg(P.ARG_TAG)] = "",
        wait_s: Annotated[float | None, arg(P.ARG_WAIT_S)] = None,
    ) -> CallToolResult:
        args = dict(cluster=cluster, workdir=workdir, scripts=scripts, desc=desc,
                    batch=batch, tag=tag, wait_s=wait_s)
        return await run_tool(
            ctx, "submit", args,
            lambda p, s, tick: messages.submit_ok(
                daemon.jobs.submit(p, cluster, workdir, scripts, desc, batch=batch, tag=tag,
                                   wait_s=wait_s, on_tick=tick)
            ),
        )

    @tool("jobs", P.TOOL_JOBS)
    async def jobs(
        ctx: Context,
        id: Annotated[str | None, arg(P.ARG_JOBS_ID)] = None,
        cluster: Annotated[str | None, arg(P.ARG_JOBS_CLUSTER)] = None,
    ) -> CallToolResult:
        def fn(p, s, tick):
            if id:
                oid = parse_op_item(id)
                if oid is not None:
                    op = daemon.gateway.op(oid)
                    if op is None or op["project"] != p:
                        raise GatewayError("unknown_op", id=oid)
                    return messages.gateway_ok({"status": op["state"], "op": op, "waited": 0})
                summ = daemon.jobs.summary(p, id)
                if summ is None:
                    raise JobError("unknown_batch", batch=id)
                return messages.batch_detail(summ, daemon.jobs.job_rows(p, id))
            batches = daemon.jobs.overview(project=p, recent=15, cluster=cluster)
            ops = daemon.gateway.list(project=p, every=True)
            if cluster:
                ops = [o for o in ops if cluster in (o["cluster"] or "").split(",")]
            unreg = daemon.jobs.unregistered(project=p)
            return messages.jobs_overview(batches, ops, unreg)

        return await run_tool(ctx, "jobs", {"id": id, "cluster": cluster}, fn, locked=True)

    @tool("cancel", P.TOOL_CANCEL)
    async def cancel(
        ctx: Context,
        batch: Annotated[str | None, arg(P.ARG_CANCEL_BATCH)] = None,
        job_ids: Annotated[list[str] | None, arg(P.ARG_CANCEL_IDS)] = None,
        approval: Annotated[int | None, arg(P.ARG_CANCEL_APPROVAL)] = None,
        cluster: Annotated[str | None, arg(P.ARG_CANCEL_CLUSTER)] = None,
    ) -> CallToolResult:
        def then(r):
            daemon.submits.sweep()
            return messages.cancel_ok(r)

        return await run_tool(
            ctx, "cancel", {"batch": batch, "job_ids": job_ids, "approval": approval,
                            "cluster": cluster},
            lambda p, s, tick: daemon.jobs.cancel(p, batch=batch, job_ids=job_ids,
                                                  approval=approval, by="agent", then=then,
                                                  cluster=cluster),
        )

    @tool("wait_for", P.TOOL_WAIT_FOR)
    async def wait_for(
        ctx: Context,
        groups: Annotated[list[WaitGroup] | None, arg(P.ARG_GROUPS)] = None,
        fail_frac: Annotated[float | None, arg(P.ARG_FAIL_FRAC)] = None,
        timeout_h: Annotated[float | None, arg(P.ARG_TIMEOUT_H)] = None,
        wait_s: Annotated[float | None, arg(P.ARG_WAIT_S)] = None,
        note: Annotated[str, arg(P.ARG_NOTE)] = "",
    ) -> CallToolResult:
        """Non-Tasks fallback: register wait, short-wait up to 180s with progress heartbeats,
        then leave background monitoring + notices active."""
        plain = None if groups is None else [g.model_dump() for g in groups]
        args = dict(groups=plain, fail_frac=fail_frac, timeout_h=timeout_h,
                    wait_s=wait_s, note=note)

        def fn(p, s, tick):
            with daemon.lock:
                w = daemon.waits.register(p, s, groups=plain, fail_frac=fail_frac,
                                          timeout_h=timeout_h, note=note,
                                          replace_existing=True)
                daemon.waits.arm_sync_wait(w["id"])
                eff_wait = (daemon.projects[p].wake.default_wait_s if wait_s is None
                            else max(0.0, min(float(wait_s), 180.0)))
            fired = daemon.waits.wait_sync(w["id"], eff_wait, on_tick=tick)
            if fired is not None:
                return messages.wait_fired(fired, daemon)
            with daemon.lock:
                row = daemon.db.one("SELECT * FROM waits WHERE id = ?", (w["id"],))
                prog = daemon.waits.progress_message(row) if row else "running"
            return messages.wait_still_running(w["id"], eff_wait, prog)

        return await run_tool(ctx, "wait_for", args, fn)

    # ---------------------------------------------------------------- info, preset & dashboard
    @tool("info", P.TOOL_INFO)
    async def info(ctx: Context) -> CallToolResult:
        def fn(p, s, tick):
            proj = daemon.projects.get(p) if p else None
            text = messages.server_instructions(proj)
            if proj is not None:
                daemon.waits.init_cursor(proj.name, s)
                st = daemon.submits.status(proj.name)
                if not st["approval"]:
                    text += "\n## Compute budget\n- Auto-approval enabled.\n"
                elif st["standing"]:
                    lims = st["standing"]["limits"]
                    used = st["used"] or {}
                    text += (
                        "\n## Standing budget\n"
                        "- Used: %s jobs, %s core-h, %s GPU-h\n"
                        "- Limits: %s\n"
                        % (used.get("jobs", 0), messages.fmt_qty(used.get("core_hours", 0.0)),
                           messages.fmt_qty(used.get("gpu_hours", 0.0)), json.dumps(lims))
                    )
                else:
                    text += "\n## Compute budget\n- No standing budget active (Slurm submissions require approval).\n"
            return text

        return await run_tool(ctx, "info", {}, fn, locked=True, needs_project=False)

    @tool("propose_preset", P.TOOL_PROPOSE_PRESET)
    async def propose_preset(
        cluster: Annotated[str, arg(P.ARG_CLUSTER)],
        changes: Annotated[dict, arg(P.ARG_CHANGES)],
        summary: Annotated[str, arg(P.ARG_PROPOSAL_SUMMARY)],
        ctx: Context,
    ) -> CallToolResult:
        args = dict(cluster=cluster, changes=changes, summary=summary)
        return await run_tool(
            ctx, "propose_preset", args,
            lambda p, s, tick: messages.propose_ok(
                daemon.settings.propose_preset(p, cluster, changes, summary)
            ),
            locked=True,
        )

    @tool("open_dashboard", P.TOOL_OPEN_DASHBOARD)
    async def open_dashboard(
        ctx: Context,
        tab: Annotated[str, arg(P.ARG_TAB)] = "approvals",
    ) -> CallToolResult:
        return await run_tool(
            ctx, "open_dashboard", {"tab": tab},
            lambda p, s, tick: messages.dashboard_result(daemon.open_dashboard(tab=tab)),
            needs_project=False,
        )

    return mcp


def _mcp_snippets(project_name: str, port: int) -> dict[str, dict]:
    http_url = "http://127.0.0.1:%d/p/%s/mcp" % (port, project_name)
    return {
        "codex": {
            "title": "OpenAI Codex CLI (~/.codex/config.toml)",
            "code": (
                "[mcp_servers.hpcport]\n"
                'command = "hpcport"\n'
                'args = ["mcp", "--project", "%s"]\n' % project_name
            ),
            "http_note": 'Or HTTP: url = "%s"' % http_url,
        },
        "opencode": {
            "title": "OpenCode (opencode.jsonc)",
            "code": json.dumps({
                "mcp": {
                    "hpcport": {
                        "type": "remote",
                        "url": http_url,
                    }
                }
            }, indent=2),
        },
        "antigravity": {
            "title": "Antigravity (mcp_config.json)",
            "code": json.dumps({
                "mcpServers": {
                    "hpcport": {
                        "command": "hpcport",
                        "args": ["mcp", "--project", project_name],
                    }
                }
            }, indent=2),
        },
        "dsh": {
            "title": "DeepSeek Harness / DSH (dsh-mcp-client)",
            "code": json.dumps({
                "mcpServers": {
                    "hpcport": {
                        "type": "http",
                        "url": http_url,
                    }
                }
            }, indent=2),
        },
        "claude_code": {
            "title": "Claude Code (.mcp.json — native MCP Tasks)",
            "code": json.dumps({
                "mcpServers": {
                    "hpcport": {
                        "type": "http",
                        "url": http_url,
                    }
                }
            }, indent=2),
        },
    }


def _bearer(headers) -> str | None:
    auth = headers.get("authorization", "")
    return auth[7:] if auth.startswith("Bearer ") else None


def build_app(daemon, start_loop: bool = True) -> Starlette:
    mcp = build_mcp(daemon)
    mcp_http = mcp.streamable_http_app(
        streamable_http_path="/mcp",
        stateless_http=False,
        json_response=True,
    )

    async def body(request) -> dict:
        try:
            data = await request.json()
            return data if isinstance(data, dict) else {}
        except ValueError:
            return {}

    def fail(status: int, msg: str) -> JSONResponse:
        return JSONResponse({"error": msg}, status_code=status)

    def auth_kind(request) -> str | None:
        """Return 'web', 'cli', or None."""
        cookie = request.cookies.get(COOKIE_NAME)
        if daemon.verify_cookie(cookie):
            return "web"
        tok = _bearer(request.headers)
        if daemon.cli_token is None:
            # Test mode without cli_token configured: allow CLI calls if explicit header or cookie
            if tok == "test-cli":
                return "cli"
        elif tok and tok == daemon.cli_token:
            return "cli"
        return None

    def guarded(handler, mutating: bool = False, approval_action: bool = False,
                web_only_when_locked: bool = False):
        async def wrapper(request):
            kind = auth_kind(request)
            if kind is None:
                return fail(401, "authentication required")
            if mutating and kind == "web" and request.headers.get("x-hpcport-ui") != "1":
                return fail(403, "missing X-HPCPort-UI header")
            if approval_action and kind == "cli" and daemon.global_cfg.web_only_approval:
                return fail(403, "CLI approvals are disabled (web_only_approval is enabled)")
            if web_only_when_locked and kind == "cli" and daemon.global_cfg.web_only_approval:
                return fail(
                    403,
                    "Configuration and policy changes via CLI are disabled while "
                    "web_only_approval is enabled",
                )
            try:
                return await handler(request)
            except (ApprovalError, AttachError, ConfigError) + ToolErrors as e:
                msg = messages.error(e) if hasattr(e, "code") and e.code in P.ERRORS else str(e)
                return fail(400, msg)
            except KeyError as e:
                return fail(404, "not found: %s" % e)
        return wrapper

    async def locked(fn, *args):
        def run():
            with daemon.lock:
                return fn(*args)
        r = await asyncio.to_thread(run)
        daemon.dispatch_soon()
        return r

    # ---------------------------------------------------------------- public & auth endpoints
    async def health(request):
        return JSONResponse({"ok": True, "version": "0.1.0"})

    async def auth_status(request):
        kind = auth_kind(request)
        return JSONResponse({
            "authenticated": kind is not None,
            "kind": kind,
            "language": daemon.global_cfg.language,
            "auto_open_dashboard": daemon.global_cfg.auto_open_dashboard,
            "web_only_approval": daemon.global_cfg.web_only_approval,
        })

    async def auth_exchange(request):
        code = request.query_params.get("code", "")
        tab = request.query_params.get("tab", "approvals")
        res = daemon.redeem_exchange_code(code)
        if res is None:
            return RedirectResponse(url="/#unlock", status_code=302)
        cookie_val, stored_tab = res
        target_tab = tab or stored_tab or "approvals"
        resp = RedirectResponse(url="/#%s" % target_tab, status_code=302)
        resp.set_cookie(COOKIE_NAME, cookie_val, httponly=True, samesite="strict", path="/")
        return resp

    async def auth_pin_unlock(request):
        if request.headers.get("x-hpcport-ui") != "1":
            return fail(403, "missing X-HPCPort-UI header")
        d = await body(request)
        cookie_val, retry_after = daemon.attempt_pin(str(d.get("pin", "")))
        if cookie_val is None:
            if retry_after > 0:
                return JSONResponse(
                    {
                        "error": "too many failed PIN attempts; try again in %ds" % retry_after,
                        "retry_after": retry_after,
                    },
                    status_code=429,
                    headers={"Retry-After": str(retry_after)},
                )
            return fail(401, "invalid pairing PIN")
        resp = JSONResponse({"authenticated": True})
        resp.set_cookie(COOKIE_NAME, cookie_val, httponly=True, samesite="strict", path="/")
        return resp

    async def auth_pin_get(request):
        if auth_kind(request) != "cli":
            return fail(403, "CLI authentication required")
        if daemon.global_cfg.web_only_approval:
            return fail(
                403,
                "PIN retrieval via CLI is disabled while web_only_approval is enabled. "
                "On a headless host, run `hpcport daemon` in a foreground terminal (e.g. tmux/screen) "
                "to view the startup pairing PIN, or open the dashboard from a local desktop.",
            )
        return JSONResponse({"pin": daemon.pairing_pin})

    async def dashboard_open_api(request):
        d = await body(request)
        return JSONResponse(daemon.open_dashboard(tab=d.get("tab") or "approvals"))

    # ---------------------------------------------------------------- SSE stream
    async def sse_events(request):
        if auth_kind(request) is None:
            return fail(401, "authentication required")
        q = daemon.subscribe_sse()

        async def gen():
            try:
                yield "event: connected\ndata: {}\n\n"
                while True:
                    try:
                        ev, data = await asyncio.wait_for(q.get(), timeout=20.0)
                        yield "event: %s\ndata: %s\n\n" % (ev, json.dumps(data))
                    except asyncio.TimeoutError:
                        yield ": keepalive\n\n"
            finally:
                daemon.unsubscribe_sse(q)

        return StreamingResponse(gen(), media_type="text/event-stream", headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        })

    # ---------------------------------------------------------------- combined state
    async def state_snapshot(request):
        def build():
            with daemon.lock:
                daemon.settings.scan()
                projs = [daemon.settings.project_status(n) for n in sorted(daemon.projects)]
                presets = [daemon.settings.preset_status(n) for n in sorted(daemon.presets)]
                return {
                    "config": {
                        "port": daemon.global_cfg.port,
                        "auto_open_dashboard": daemon.global_cfg.auto_open_dashboard,
                        "web_only_approval": daemon.global_cfg.web_only_approval,
                        "language": daemon.global_cfg.language,
                    },
                    "projects": projs,
                    "presets": presets,
                    "health": daemon.cluster_health(),
                    "pending_approvals": daemon.approvals.pending_count(),
                }
        return JSONResponse(await asyncio.to_thread(build))

    # ---------------------------------------------------------------- projects & presets
    async def projects_list(request):
        with daemon.lock:
            out = [config.project_to_dict(daemon.projects[n]) for n in sorted(daemon.projects)]
        return JSONResponse({"projects": out})

    async def projects_create(request):
        d = await body(request)
        if not d.get("name"):
            return fail(400, "name is required")
        r = await locked(daemon.settings.create_project, d["name"], d.get("local_dir"),
                         d.get("data"))
        return JSONResponse(r)

    async def project_get(request):
        name = request.path_params["name"]
        return JSONResponse(await locked(daemon.settings.project_status, name))

    async def project_patch(request):
        name = request.path_params["name"]
        d = await body(request)
        changes = d.get("changes") if isinstance(d.get("changes"), dict) else d
        if not changes:
            return fail(400, "changes mapping is required")
        return JSONResponse(await locked(daemon.settings.update_project, name, changes,
                                         d.get("note") or ""))

    async def project_attach(request):
        name = request.path_params["name"]
        d = await body(request)
        if not d.get("cluster"):
            return fail(400, "cluster is required")
        r = await asyncio.to_thread(daemon.attach_cluster, name, d["cluster"],
                                    d.get("root"), d.get("max_jobs"))
        return JSONResponse(r)

    async def project_detach(request):
        name = request.path_params["name"]
        cluster = request.path_params["cluster"]
        r = await asyncio.to_thread(daemon.detach_cluster, name, cluster)
        return JSONResponse(r)

    async def project_snippets(request):
        name = request.path_params["name"]
        if name not in daemon.projects:
            return fail(404, "unknown project %s" % name)
        return JSONResponse({"project": name,
                             "snippets": _mcp_snippets(name, daemon.global_cfg.port)})

    async def presets_list(request):
        with daemon.lock:
            out = [daemon.settings.preset_status(n) for n in sorted(daemon.presets)]
        return JSONResponse({"presets": out})

    async def presets_create(request):
        d = await body(request)
        if not d.get("name") or not isinstance(d.get("data"), dict):
            return fail(400, "name and data mapping are required")
        return JSONResponse(await locked(daemon.settings.create_preset, d["name"], d["data"]))

    async def preset_get(request):
        name = request.path_params["name"]
        return JSONResponse(await locked(daemon.settings.preset_status, name))

    async def preset_patch(request):
        name = request.path_params["name"]
        d = await body(request)
        changes = d.get("changes") if isinstance(d.get("changes"), dict) else d
        if not changes:
            return fail(400, "changes mapping is required")
        return JSONResponse(await locked(daemon.settings.update_preset, name, changes,
                                         d.get("note") or ""))

    async def preset_delete(request):
        name = request.path_params["name"]
        await locked(daemon.settings.delete_preset, name)
        return JSONResponse({"deleted": name})

    async def preset_test(request):
        name = request.path_params["name"]
        r = await asyncio.to_thread(daemon.test_cluster, name)
        return JSONResponse(r)

    async def preset_restore(request):
        name = request.path_params["name"]
        d = await body(request)
        v = d.get("version")
        if not isinstance(v, int):
            return fail(400, "integer version is required")
        return JSONResponse(await locked(daemon.settings.restore_version,
                                         "cluster_preset:" + name, v))

    # ---------------------------------------------------------------- jobs & ops
    async def jobs_list(request):
        project = request.query_params.get("project") or None
        cluster = request.query_params.get("cluster") or None
        with daemon.lock:
            batches = daemon.jobs.overview(project=project, recent=30, cluster=cluster)
            ops = daemon.gateway.list(project=project, every=True)
            unreg = daemon.jobs.unregistered(project=project)
            usage = {}
            if project and project in daemon.projects:
                for cname in daemon.projects[project].clusters:
                    usage[cname] = daemon.jobs.cluster_usage(project, cname)
            standing = daemon.submits.status(project) if project else None
        return JSONResponse({
            "text": messages.jobs_overview(batches, ops, unreg),
            "batches": batches,
            "ops": ops,
            "unregistered": unreg,
            "usage": usage,
            "standing": standing,
        })

    async def batch_detail(request):
        proj, batch = request.path_params["project"], request.path_params["batch"]
        with daemon.lock:
            s = daemon.jobs.summary(proj, batch)
            if s is None:
                return fail(404, "unknown batch %s in %s" % (batch, proj))
            rows = daemon.jobs.job_rows(proj, batch)
        return JSONResponse({"summary": s, "jobs": rows, "text": messages.batch_detail(s, rows)})

    async def job_files(request):
        jid = request.path_params["id"]
        r = await asyncio.to_thread(daemon.jobs.read_job_files, jid)
        return JSONResponse(r)

    async def cancel_api(request):
        d = await body(request)
        comment = (d.get("comment") or "").strip()

        def then(r):
            daemon.submits.sweep()
            for proj, info in (r.get("affected") or {}).items():
                items = info["cancelled"] + info["ending"] + info["elements"]
                if not items:
                    continue
                subject = "cancel:" + ",".join(info["batches"] or items)
                daemon.waits.notify(
                    proj,
                    "jobs_cancelled",
                    subject,
                    {
                        "batches": info["batches"],
                        "cancelled": info["cancelled"],
                        "ending": info["ending"],
                        "elements": info["elements"],
                        "comment": comment,
                    },
                )
            daemon.waits.evaluate(render_result=lambda b: messages.wait_fired(b, daemon))
            return r

        r = await asyncio.to_thread(
            daemon.jobs.cancel, d.get("project"), d.get("batch"), d.get("job_ids"),
            d.get("approval"), by="user", note=comment, then=then,
            cluster=d.get("cluster"),
        )
        return JSONResponse({"text": messages.cancel_ok(r), "result": r})

    async def poll_api(request):
        await asyncio.to_thread(daemon.cycle, True)
        await asyncio.to_thread(daemon.gateway.poll)
        return JSONResponse({"ok": True})

    async def ops_list(request):
        project = request.query_params.get("project") or None
        every = request.query_params.get("all") == "1"
        return JSONResponse({"ops": daemon.gateway.list(project=project, every=every)})

    async def op_output(request):
        oid = request.path_params["id"]
        return JSONResponse(await asyncio.to_thread(daemon.gateway.read_output, oid))

    async def op_stop(request):
        oid = request.path_params["id"]
        r = await asyncio.to_thread(daemon.gateway.stop, None, oid, by="user")

        def _eval():
            with daemon.lock:
                daemon.waits.evaluate(render_result=lambda b: messages.wait_fired(b, daemon))

        await asyncio.to_thread(_eval)
        return JSONResponse({"status": r["status"], "op": r["op"],
                             "text": messages.gateway_ok(r)})

    # ---------------------------------------------------------------- approvals, standing, audit
    async def approvals_list(request):
        project = request.query_params.get("project") or None
        every = request.query_params.get("all") == "1"
        return JSONResponse({"approvals": daemon.approvals.list(pending_only=not every,
                                                                project=project)})

    async def approval_decide(request):
        d = await body(request)
        action = request.path_params["action"]
        if action not in ("approve", "reject"):
            return fail(404, "unknown action")
        options = {k: v for k, v in d.items() if k != "comment"}
        r = await locked(
            lambda: daemon.approvals.decide(
                request.path_params["id"], action == "approve",
                d.get("comment") or "", **options,
            )
        )
        return JSONResponse(r)

    async def standing_get(request):
        proj = request.path_params["project"]
        return JSONResponse(daemon.submits.status(proj))

    async def standing_revoke(request):
        proj = request.path_params["project"]
        return JSONResponse({"revoked": await locked(daemon.submits.revoke, proj)})

    async def audit_list(request):
        q = request.query_params
        conds, args = [], []
        for key in ("project", "tool"):
            if q.get(key):
                conds.append("%s = ?" % key)
                args.append(q[key])
        where = "WHERE " + " AND ".join(conds) if conds else ""
        lim = min(int(q.get("n") or 50), 1000)
        rows = daemon.db.all(
            "SELECT id, ts, project, session_key, tool, args, result FROM audit %s "
            "ORDER BY id DESC LIMIT ?" % where,
            tuple(args) + (lim,),
        )
        return JSONResponse({"audit": rows})

    async def config_get(request):
        c = daemon.global_cfg
        return JSONResponse({
            "port": c.port,
            "auto_open_dashboard": c.auto_open_dashboard,
            "web_only_approval": c.web_only_approval,
            "language": c.language,
        })

    async def config_patch(request):
        d = await body(request)
        c = await locked(daemon.update_global_config, d)
        return JSONResponse({
            "port": c.port,
            "auto_open_dashboard": c.auto_open_dashboard,
            "web_only_approval": c.web_only_approval,
            "language": c.language,
        })

    # ---------------------------------------------------------------- Web UI static
    async def index_page(request):
        idx = WEB_DIR / "index.html"
        if idx.is_file():
            return FileResponse(idx)
        return HTMLResponse("<h1>HPCPort</h1><p>Web UI assets not installed.</p>")

    # ---------------------------------------------------------------- project MCP router
    class _ProjectMCPASGI:
        async def __call__(self, scope, receive, send):
            """Rewrite /p/{project}/mcp -> /mcp while injecting x-hpcport-project header."""
            project = scope["path_params"]["project"]
            headers = list(scope.get("headers") or [])
            headers = [(k, v) for k, v in headers if k.lower() != b"x-hpcport-project"]
            headers.append((b"x-hpcport-project", project.encode("utf-8")))
            new_scope = dict(scope, path="/mcp", raw_path=b"/mcp", headers=headers)
            await mcp_http(new_scope, receive, send)

    project_mcp_asgi = _ProjectMCPASGI()

    @asynccontextmanager
    async def lifespan(app):
        async with mcp.session_manager.run():
            loop_task = asyncio.create_task(daemon.run()) if start_loop else None
            try:
                yield
            finally:
                if loop_task is not None:
                    loop_task.cancel()
                    await asyncio.gather(loop_task, return_exceptions=True)

    def r_get(path, fn):
        return Route(path, guarded(fn, mutating=False), methods=["GET"])

    def r_mut(path, fn, method="POST", approval_action=False, web_only_when_locked=False):
        return Route(
            path,
            guarded(fn, mutating=True, approval_action=approval_action,
                    web_only_when_locked=web_only_when_locked),
            methods=[method],
        )

    routes = [
        Route("/", index_page, methods=["GET"]),
        Route("/auth/exchange", auth_exchange, methods=["GET"]),
        Route("/api/health", health, methods=["GET"]),
        Route("/api/auth/status", auth_status, methods=["GET"]),
        Route("/api/auth/pin", auth_pin_unlock, methods=["POST"]),
        Route("/api/auth/pin", auth_pin_get, methods=["GET"]),
        Route("/api/events", sse_events, methods=["GET"]),
        r_mut("/api/dashboard/open", dashboard_open_api, "POST"),
        r_get("/api/state", state_snapshot),
        r_get("/api/config", config_get),
        r_mut("/api/config", config_patch, "PATCH", web_only_when_locked=True),
        r_get("/api/projects", projects_list),
        r_mut("/api/projects", projects_create, "POST", web_only_when_locked=True),
        r_get("/api/projects/{name}", project_get),
        r_mut("/api/projects/{name}", project_patch, "PATCH", web_only_when_locked=True),
        r_mut("/api/projects/{name}/clusters", project_attach, "POST", web_only_when_locked=True),
        r_mut("/api/projects/{name}/clusters/{cluster}", project_detach, "DELETE",
              web_only_when_locked=True),
        r_get("/api/projects/{name}/mcp-snippets", project_snippets),
        r_get("/api/presets", presets_list),
        r_mut("/api/presets", presets_create, "POST", web_only_when_locked=True),
        r_get("/api/presets/{name}", preset_get),
        r_mut("/api/presets/{name}", preset_patch, "PATCH", web_only_when_locked=True),
        r_mut("/api/presets/{name}", preset_delete, "DELETE", web_only_when_locked=True),
        r_mut("/api/presets/{name}/test", preset_test, "POST"),
        r_mut("/api/presets/{name}/restore", preset_restore, "POST", web_only_when_locked=True),
        r_get("/api/jobs", jobs_list),
        r_get("/api/jobs/{project}/{batch}", batch_detail),
        r_get("/api/job/{id:int}/files", job_files),
        r_mut("/api/cancel", cancel_api, "POST"),
        r_mut("/api/poll", poll_api, "POST"),
        r_get("/api/ops", ops_list),
        r_get("/api/ops/{id:int}/output", op_output),
        r_mut("/api/ops/{id:int}/stop", op_stop, "POST"),
        r_get("/api/approvals", approvals_list),
        r_mut("/api/approvals/{id:int}/{action}", approval_decide, "POST", approval_action=True),
        r_get("/api/standing/{project}", standing_get),
        r_mut("/api/standing/{project}", standing_revoke, "DELETE", approval_action=True),
        r_get("/api/audit", audit_list),
        Route("/p/{project}/mcp", endpoint=project_mcp_asgi, methods=["GET", "POST", "DELETE"]),
    ]
    if WEB_DIR.is_dir():
        routes.append(Mount("/static", app=StaticFiles(directory=str(WEB_DIR)), name="static"))
    routes.append(Mount("/", app=mcp_http))
    return Starlette(lifespan=lifespan, routes=routes)
