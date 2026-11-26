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
import secrets
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
        log_fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(log_fd, "ab") as logf:
            proc = subprocess.Popen(
                [sys.executable, "-m", "hpcport.cli", "--port", str(port), "daemon"],
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
            if proc.poll() is not None:
                break
            time.sleep(0.15)
    tail = ""
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        if lines:
            tail = " (log: %s)" % " | ".join(lines[-3:])[-300:]
    except OSError:
        pass
    raise RuntimeError("HPCPort daemon did not start on port %d within %.1fs%s"
                       % (port, wait_s, tail))


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
    cwd: str | None = None,
    client_id: str | None = None,
) -> tuple[dict[str, str], str | None]:
    """Build HTTP headers for forwarding a stdio JSON-RPC line to Streamable HTTP."""
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    if session_id:
        headers["Mcp-Session-Id"] = session_id
    if cwd:
        headers["X-HPCPort-Cwd"] = cwd
    if client_id:
        headers["X-HPCPort-Client"] = client_id
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


async def _run_bridge(url: str, cwd: str | None = None) -> None:
    session_id: str | None = None
    protocol_version: str | None = None
    client_id = "stdio-" + secrets.token_hex(8)
    seen_any = False
    write_lock = asyncio.Lock()
    in_flight: set[asyncio.Task] = set()

    async def emit(payload: str) -> None:
        async with write_lock:
            sys.stdout.write(payload.strip() + "\n")
            sys.stdout.flush()

    def is_handshake(raw: str) -> bool:
        try:
            payload = json.loads(raw)
        except ValueError:
            return False
        if not isinstance(payload, dict):
            return False
        return payload.get("method") in (
            "initialize",
            "notifications/initialized",
            "server/discover",
        )

    async def forward_one(client: httpx.AsyncClient, raw: str) -> None:
        nonlocal session_id, protocol_version, seen_any
        try:
            req_obj = json.loads(raw)
        except ValueError:
            req_obj = None
        has_id = isinstance(req_obj, dict) and "id" in req_obj
        req_id = req_obj["id"] if has_id else None

        headers, det_ver = build_forward_headers(
            raw, session_id=session_id, protocol_version=protocol_version,
            cwd=cwd, client_id=client_id,
        )
        if det_ver:
            protocol_version = det_ver
        try:
            async with client.stream("POST", url, content=raw.encode("utf-8"),
                                     headers=headers) as resp:
                if resp.status_code == 404:
                    session_id = None
                    seen_any = False
                sid = resp.headers.get("mcp-session-id")
                if sid:
                    session_id = sid
                pv_hdr = resp.headers.get("mcp-protocol-version")
                if pv_hdr:
                    protocol_version = pv_hdr
                if resp.status_code >= 400:
                    body = (await resp.aread()).decode("utf-8", errors="replace").strip()
                    if not has_id:
                        return
                    err_obj = None
                    if body:
                        try:
                            parsed = json.loads(body)
                            if isinstance(parsed, dict) and isinstance(parsed.get("error"), dict):
                                err_obj = parsed["error"]
                        except ValueError:
                            pass
                    if err_obj is None:
                        err_obj = {
                            "code": -32000,
                            "message": "HTTP %d: %s" % (
                                resp.status_code, (body or resp.reason_phrase or "error")[:300],
                            ),
                        }
                    await emit(json.dumps({
                        "jsonrpc": "2.0",
                        "id": req_id,
                        "error": err_obj,
                    }))
                    return
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
                        if has_id and req_id is not None:
                            try:
                                parsed = json.loads(body)
                                if (isinstance(parsed, dict) and parsed.get("id") is None
                                        and "error" in parsed):
                                    parsed["id"] = req_id
                                    body = json.dumps(parsed)
                            except ValueError:
                                pass
                        await emit(body)
        except Exception as e:
            # Return a JSON-RPC internal error if this was a request with an id
            if has_id:
                await emit(json.dumps({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32603, "message": "HPCPort bridge error: %s" % e},
                }))

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
            if is_handshake(raw) or (session_id is None and not seen_any):
                seen_any = True
                await forward_one(client, raw)
            else:
                seen_any = True
                task = asyncio.create_task(forward_one(client, raw))
                in_flight.add(task)
                task.add_done_callback(in_flight.discard)

        if in_flight:
            await asyncio.gather(*in_flight, return_exceptions=True)


def serve_stdio(project: str | None = None, port: int | None = None) -> int:
    actual_port = ensure_daemon(port=port)
    cwd = os.getcwd()
    proj = resolve_target(project=project, cwd=cwd)
    if proj:
        url = "http://127.0.0.1:%d/p/%s/mcp" % (actual_port, proj)
        bridge_cwd = None
    else:
        url = "http://127.0.0.1:%d/mcp" % actual_port
        bridge_cwd = cwd
    try:
        asyncio.run(_run_bridge(url, cwd=bridge_cwd))
    except KeyboardInterrupt:
        pass
    return 0
