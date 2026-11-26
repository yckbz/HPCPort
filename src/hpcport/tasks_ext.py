"""MCP Tasks extension (`io.modelcontextprotocol/tasks`, SEP-2663 / 2026-07-28).

Implements server-side async task execution for `wait_for` using the official
`mcp.server.extension.Extension` hook in mcp 2.3.x:
  - Advertises `io.modelcontextprotocol/tasks` under ServerCapabilities.extensions.
  - Inspects per-request client capabilities (`_meta["io.modelcontextprotocol/clientCapabilities"]`
    or legacy 2025-11-25 initialize capabilities). Clients that do not advertise
    Tasks pass straight through to `wait_for`'s normal short-wait + notices path.
  - Serves `tasks/get`, `tasks/cancel`, and legacy `tasks/result` backed by the
    persistent `mcp_tasks` SQLite table in WaitService.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from typing import Any, Literal

from mcp.server.context import CallNext, HandlerResult, ServerRequestContext
from mcp.server.extension import Extension, MethodBinding
from mcp.server.runner import MODERN_PROTOCOL_VERSIONS
from mcp_types import (
    CLIENT_CAPABILITIES_META_KEY,
    CallToolRequestParams,
    CallToolResult,
    RequestParams,
    Result,
    TextContent,
)

from . import config, messages
from .waits import WaitError

EXTENSION_ID = "io.modelcontextprotocol/tasks"


def _iso(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class TaskIdParams(RequestParams):
    task_id: str


class TaskHandleResult(Result):
    """Returned from tools/call when upgraded to an async MCP Task."""
    result_type: Literal["task"] = "task"
    task_id: str
    status: Literal["working", "input_required", "completed", "failed", "cancelled"]
    status_message: str | None = None
    created_at: str
    last_updated_at: str
    ttl: int | None = None
    poll_interval_ms: int | None = None


class TaskGetResult(Result):
    """Returned from tasks/get with current state and terminal CallToolResult when done."""
    task_id: str
    status: Literal["working", "input_required", "completed", "failed", "cancelled"]
    status_message: str | None = None
    created_at: str
    last_updated_at: str
    ttl: int | None = None
    poll_interval_ms: int | None = None
    result: dict[str, Any] | None = None


def client_supports_tasks(ctx: ServerRequestContext[Any, Any]) -> bool:
    """Return True if the calling MCP client is on a 2026-era wire and advertised
    `io.modelcontextprotocol/tasks`.

    Legacy connections (<= 2025-11-25) strictly sieve `tools/call` responses to
    `CallToolResult` and cannot carry extension `resultType: "task"` payloads,
    so they transparently fall through to `wait_for`'s short-wait + notices path.
    """
    if getattr(ctx, "protocol_version", None) not in MODERN_PROTOCOL_VERSIONS:
        return False
    meta = ctx.meta or {}
    if isinstance(meta, dict):
        cc = meta.get(CLIENT_CAPABILITIES_META_KEY)
        if isinstance(cc, dict):
            exts = cc.get("extensions")
            if isinstance(exts, dict) and EXTENSION_ID in exts:
                return True
            if cc.get("tasks") is not None:
                return True
    session = getattr(ctx, "session", None)
    client_params = getattr(session, "client_params", None) if session else None
    caps = getattr(client_params, "capabilities", None) if client_params else None
    if caps is not None:
        exts = getattr(caps, "extensions", None)
        if isinstance(exts, dict) and EXTENSION_ID in exts:
            return True
        if getattr(caps, "tasks", None) is not None:
            return True
    return False


def _header(ctx: ServerRequestContext[Any, Any], name: str) -> str | None:
    req = getattr(ctx, "request", None)
    headers = getattr(req, "headers", None) if req is not None else None
    if headers is None:
        return None
    return headers.get(name) or headers.get(name.lower())


class TasksExtension(Extension):
    identifier = EXTENSION_ID

    def __init__(self, daemon):
        self.daemon = daemon

    def settings(self) -> dict[str, Any]:
        return {"requests": {"tools": {"call": {}}}, "cancel": {}}

    def methods(self) -> tuple[MethodBinding, ...]:
        return (
            MethodBinding("tasks/get", TaskIdParams, self._handle_get),
            MethodBinding("tasks/cancel", TaskIdParams, self._handle_cancel),
            MethodBinding("tasks/result", TaskIdParams, self._handle_result),
        )

    def _resolve_project(self, ctx: ServerRequestContext[Any, Any]) -> str | None:
        project = _header(ctx, "x-hpcport-project")
        if not project:
            cwd = _header(ctx, "x-hpcport-cwd")
            if cwd:
                with self.daemon.lock:
                    project = config.resolve_project_by_cwd(cwd, self.daemon.projects)
        return project or None

    async def intercept_tool_call(
        self,
        params: CallToolRequestParams,
        ctx: ServerRequestContext[Any, Any],
        call_next: CallNext,
    ) -> HandlerResult:
        if params.name != "wait_for" or not client_supports_tasks(ctx):
            return await call_next(ctx)

        session_key = _header(ctx, "mcp-session-id") or "default"
        args = dict(params.arguments or {})

        def _register():
            project = self._resolve_project(ctx)
            with self.daemon.lock:
                if not project or project not in self.daemon.projects:
                    raise WaitError("unknown_project" if project else "unbound_project",
                                    project=project or "")
                self.daemon.waits.init_cursor(project, session_key)
                wait = self.daemon.waits.register(
                    project,
                    session_key,
                    groups=args.get("groups"),
                    fail_frac=args.get("fail_frac"),
                    timeout_h=args.get("timeout_h"),
                    note=args.get("note") if args.get("note") is not None else "",
                )
                task = self.daemon.waits.create_task(project, wait["id"])
                self.daemon.db.insert(
                    "audit",
                    ts=self.daemon.now(),
                    project=project,
                    session_key=session_key,
                    tool="wait_for",
                    args=json.dumps(args, ensure_ascii=False),
                    result="task:" + task["task_id"],
                )
                return task

        try:
            task = await asyncio.to_thread(_register)
        except WaitError as e:
            return CallToolResult(
                content=[TextContent(type="text", text=messages.error(e))],
                is_error=True,
            )

        return TaskHandleResult(
            task_id=task["task_id"],
            status=task["status"],
            status_message=task["status_message"],
            created_at=_iso(task["created"]),
            last_updated_at=_iso(task["updated"]),
            ttl=task["ttl_ms"],
            poll_interval_ms=task["poll_interval_ms"],
        )

    async def _handle_get(self, ctx: ServerRequestContext[Any, Any],
                          params: TaskIdParams) -> TaskGetResult:
        def _load():
            project = self._resolve_project(ctx)
            with self.daemon.lock:
                self.daemon.waits.evaluate(
                    render_result=lambda b: messages.wait_fired(b, self.daemon)
                )
                if not project:
                    return None
                task = self.daemon.waits.get_task(params.task_id)
                if task is None or task["project"] != project:
                    return None
                return task

        task = await asyncio.to_thread(_load)
        if task is None:
            return TaskGetResult(
                task_id=params.task_id,
                status="failed",
                status_message="Unknown or expired task ID",
                created_at=_iso(self.daemon.now()),
                last_updated_at=_iso(self.daemon.now()),
            )
        payload = None
        if task["status"] == "completed" and task.get("result"):
            text = task["result"].get("text") or ""
            payload = {
                "content": [{"type": "text", "text": text}],
                "isError": False,
                "resultType": "complete",
            }
        return TaskGetResult(
            task_id=task["task_id"],
            status=task["status"],
            status_message=task["status_message"],
            created_at=_iso(task["created"]),
            last_updated_at=_iso(task["updated"]),
            ttl=task["ttl_ms"],
            poll_interval_ms=task["poll_interval_ms"],
            result=payload,
        )

    async def _handle_result(self, ctx: ServerRequestContext[Any, Any],
                             params: TaskIdParams) -> CallToolResult:
        def _load():
            project = self._resolve_project(ctx)
            with self.daemon.lock:
                self.daemon.waits.evaluate(
                    render_result=lambda b: messages.wait_fired(b, self.daemon)
                )
                if not project:
                    return None
                task = self.daemon.waits.get_task(params.task_id)
                if task is None or task["project"] != project:
                    return None
                return task

        task = await asyncio.to_thread(_load)
        if task is None:
            return CallToolResult(
                content=[TextContent(type="text", text="Unknown or expired task ID.")],
                is_error=True,
            )
        if task["status"] == "completed" and task.get("result"):
            return CallToolResult(
                content=[TextContent(type="text", text=task["result"].get("text") or "")]
            )
        return CallToolResult(
            content=[TextContent(
                type="text",
                text="Task %s is currently %s (%s)."
                % (task["task_id"], task["status"], task["status_message"] or ""),
            )]
        )

    async def _handle_cancel(self, ctx: ServerRequestContext[Any, Any],
                             params: TaskIdParams) -> TaskGetResult:
        def _cancel():
            project = self._resolve_project(ctx)
            with self.daemon.lock:
                if not project:
                    return None
                existing = self.daemon.waits.get_task(params.task_id)
                if existing is None or existing["project"] != project:
                    return None
                return self.daemon.waits.cancel_task(params.task_id)

        task = await asyncio.to_thread(_cancel)
        if task is None:
            return TaskGetResult(
                task_id=params.task_id,
                status="failed",
                status_message="Unknown or expired task ID",
                created_at=_iso(self.daemon.now()),
                last_updated_at=_iso(self.daemon.now()),
            )
        return TaskGetResult(
            task_id=task["task_id"],
            status=task["status"],
            status_message=task["status_message"],
            created_at=_iso(task["created"]),
            last_updated_at=_iso(task["updated"]),
            ttl=task["ttl_ms"],
            poll_interval_ms=task["poll_interval_ms"],
        )
