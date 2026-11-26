"""Thin stdio-to-HTTP MCP bridge (`hpcport mcp`).

Allows any stdio-based MCP client (Codex, OpenCode, Antigravity, DSH, Claude
Code) to connect to the global HPCPort daemon without manually starting it:
  1. Ensures the global daemon is running (auto-starts in background under an
     flock lockfile if not yet reachable);
  2. Resolves the project from `--project` or by longest-prefix match of `cwd`
     against registered projects' `local_dir` (falls back to `/mcp` unbound
     mode so the agent can still call `open_dashboard` for setup);
  3. Proxies newline-delimited JSON-RPC between stdin/stdout and the daemon's
     Streamable HTTP endpoint, forwarding `Mcp-Session-Id` and SSE notifications.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx

from . import config


def _daemon_alive(port: int) -> bool:
    try:
        with httpx.Client(trust_env=False, timeout=1.5) as client:
            r = client.get("http://127.0.0.1:%d/api/health" % port)
            return r.status_code == 200
    except Exception:
        return False


def ensure_daemon(port: int | None = None, wait_s: float = 10.0) -> int:
    """Start the global daemon in the background if it is not already listening."""
    cfg = config.load_global_config()
    port = cfg.port if port is None else port
    if _daemon_alive(port):
        return port
    lock_path = config.daemon_lock_file()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        if _daemon_alive(port):
            return port
        log_path = config.run_dir() / "daemon.log"
        env = dict(os.environ)
        env.pop("HPCPORT_AGENT", None)
        with open(log_path, "ab") as logf:
            subprocess.Popen(
                [sys.executable, "-m", "hpcport.cli", "daemon", "--port", str(port)],
                stdin=subprocess.DEVNULL,
                stdout=logf,
                stderr=logf,
                env=env,
                start_new_session=True,
            )
        deadline = time.monotonic() + wait_s
        while time.monotonic() < deadline:
            if _daemon_alive(port):
                return port
            time.sleep(0.15)
    raise RuntimeError("HPCPort daemon did not start on port %d within %.1fs" % (port, wait_s))


def resolve_target(project: str | None = None, cwd: str | None = None) -> str | None:
    if project:
        return project.strip()
    presets = {}
    for name in config.list_preset_names():
        try:
            presets[name] = config.load_preset(name)
        except config.ConfigError:
            pass
    projects = {}
    for name in config.list_project_names():
        try:
            projects[name] = config.load_project(name, presets=presets)
        except config.ConfigError:
            pass
    return config.resolve_project_by_cwd(Path(cwd or os.getcwd()), projects)


def build_forward_headers(
    raw_json: str,
    session_id: str | None = None,
    protocol_version: str | None = None,
) -> tuple[dict[str, str], str | None]:
    """Build HTTP headers for forwarding a stdio JSON-RPC line to Streamable HTTP."""
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    if session_id:
        headers["Mcp-Session-Id"] = session_id
    detected_version = protocol_version
    method: str | None = None
    try:
        payload = json.loads(raw_json)
    except ValueError:
        payload = None
    _NAME_KEYS = {"tools/call": "name", "prompts/get": "name", "resources/read": "uri"}
    name_val: str | None = None
    if isinstance(payload, dict):
        m = payload.get("method")
        if isinstance(m, str) and m:
            method = m
        params = payload.get("params")
        if isinstance(params, dict):
            if method in _NAME_KEYS:
                nv = params.get(_NAME_KEYS[method])
                if isinstance(nv, str) and nv:
                    name_val = nv
            meta = params.get("_meta")
            if isinstance(meta, dict):
                pv = meta.get("io.modelcontextprotocol/protocolVersion")
                if isinstance(pv, str) and pv:
                    detected_version = pv
    if detected_version == "2026-07-28":
        headers["MCP-Protocol-Version"] = detected_version
        if method:
            headers["MCP-Method"] = method
        if name_val:
            headers["MCP-Name"] = name_val
    elif detected_version:
        headers["MCP-Protocol-Version"] = detected_version
    return headers, detected_version


async def _run_bridge(url: str) -> None:
    session_id: str | None = None
    protocol_version: str | None = None
    write_lock = asyncio.Lock()

    async def emit(payload: str) -> None:
        async with write_lock:
            sys.stdout.write(payload.strip() + "\n")
            sys.stdout.flush()

    async with httpx.AsyncClient(trust_env=False, timeout=None) as client:
        loop = asyncio.get_running_loop()

        def read_line() -> str:
            return sys.stdin.readline()

        while True:
            line = await loop.run_in_executor(None, read_line)
            if not line:
                break
            raw = line.strip()
            if not raw:
                continue
            headers, protocol_version = build_forward_headers(
                raw, session_id=session_id, protocol_version=protocol_version
            )
            try:
                async with client.stream("POST", url, content=raw.encode("utf-8"),
                                         headers=headers) as resp:
                    sid = resp.headers.get("mcp-session-id")
                    if sid:
                        session_id = sid
                    pv_hdr = resp.headers.get("mcp-protocol-version")
                    if pv_hdr:
                        protocol_version = pv_hdr
                    ctype = resp.headers.get("content-type", "")
                    if "text/event-stream" in ctype:
                        data_lines: list[str] = []
                        async for sse_line in resp.aiter_lines():
                            if sse_line.startswith("data:"):
                                data_lines.append(sse_line[5:].lstrip())
                            elif sse_line == "" and data_lines:
                                msg = "\n".join(data_lines).strip()
                                data_lines.clear()
                                if msg:
                                    await emit(msg)
                        if data_lines:
                            msg = "\n".join(data_lines).strip()
                            if msg:
                                await emit(msg)
                    else:
                        body = (await resp.aread()).decode("utf-8", errors="replace").strip()
                        if body:
                            await emit(body)
            except Exception as e:
                # Return a JSON-RPC internal error if this was a request with an id
                try:
                    req = json.loads(raw)
                except ValueError:
                    req = {}
                if isinstance(req, dict) and "id" in req:
                    await emit(json.dumps({
                        "jsonrpc": "2.0",
                        "id": req["id"],
                        "error": {"code": -32603, "message": "HPCPort bridge error: %s" % e},
                    }))


def serve_stdio(project: str | None = None, port: int | None = None) -> int:
    actual_port = ensure_daemon(port=port)
    proj = resolve_target(project=project)
    if proj:
        url = "http://127.0.0.1:%d/p/%s/mcp" % (actual_port, proj)
    else:
        url = "http://127.0.0.1:%d/mcp" % actual_port
    try:
        asyncio.run(_run_bridge(url))
    except KeyboardInterrupt:
        pass
    return 0
