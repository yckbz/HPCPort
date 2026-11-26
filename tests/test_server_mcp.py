"""Integration tests for MCP HTTP routing, MCP Tasks extension, notices, auth, and REST API."""

from __future__ import annotations

import json
import subprocess
import sys

from starlette.testclient import TestClient

from hpcport.daemon import Daemon
from hpcport.server import build_app
from tests.fake_slurm import FakeSlurmExecutor


def _make_app(hpc_home, tmp_path):
    remote = tmp_path / "remote"
    remote.mkdir()
    local_dir = tmp_path / "local"
    local_dir.mkdir()
    ex = FakeSlurmExecutor(tmp_path / "bin")
    opened = []
    d = Daemon(executors={"gpu01": ex}, browser_opener=lambda url: opened.append(url) or True)
    d.cli_token = "secret-cli-token"
    d.settings.create_preset("gpu01", {
        "connect": "local", "scheduler": "slurm", "partitions": ["a100"],
        "max_jobs": 4, "projects_root": str(remote),
    })
    d.settings.create_preset("bare1", {
        "connect": "local", "scheduler": "none", "projects_root": str(remote),
    })
    d.settings.create_project("demo", local_dir=str(local_dir))
    d.attach_cluster("demo", "gpu01", root=str(remote / "demo_slurm"))
    d.attach_cluster("demo", "bare1", root=str(remote / "demo_bare"))
    app = build_app(d, start_loop=False)
    return d, ex, app, opened, remote, local_dir


def _mcp_init_legacy(client: TestClient, path: str) -> str:
    r = client.post(
        path,
        headers={"Accept": "application/json, text/event-stream"},
        json={
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "test-legacy-client", "version": "1.0"},
            },
        },
    )
    assert r.status_code == 200, r.text
    sid = r.headers.get("mcp-session-id")
    assert sid
    r2 = client.post(
        path,
        headers={"Accept": "application/json, text/event-stream", "Mcp-Session-Id": sid},
        json={"jsonrpc": "2.0", "method": "notifications/initialized"},
    )
    assert r2.status_code in (200, 202)
    return sid


def _mcp_call_modern(client: TestClient, path: str, req_id: int, method: str,
                     params: dict, with_tasks: bool = True, sid: str | None = None):
    meta = {
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientInfo": {"name": "test-modern", "version": "1.0"},
    }
    if with_tasks:
        meta["io.modelcontextprotocol/clientCapabilities"] = {
            "extensions": {"io.modelcontextprotocol/tasks": {}}
        }
    p = dict(params)
    p["_meta"] = meta
    headers = {
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": "2026-07-28",
        "MCP-Method": method,
    }
    if method == "tools/call" and "name" in p:
        headers["MCP-Name"] = p["name"]
    if sid:
        headers["Mcp-Session-Id"] = sid
    r = client.post(
        path,
        headers=headers,
        json={"jsonrpc": "2.0", "id": req_id, "method": method, "params": p},
    )
    assert r.status_code == 200, r.text
    return r.json(), r.headers.get("mcp-session-id") or sid


def test_mcp_discover_advertises_tasks_extension(hpc_home, tmp_path):
    _, _, app, _, _, _ = _make_app(hpc_home, tmp_path)
    with TestClient(app, base_url="http://127.0.0.1:8765") as client:
        body, _ = _mcp_call_modern(client, "/p/demo/mcp", 1, "server/discover", {})
        caps = body["result"]["capabilities"]
        assert "io.modelcontextprotocol/tasks" in (caps.get("extensions") or {})


def _mcp_call(client: TestClient, path: str, sid: str, req_id: int, method: str,
              params: dict):
    r = client.post(
        path,
        headers={"Accept": "application/json, text/event-stream", "Mcp-Session-Id": sid},
        json={"jsonrpc": "2.0", "id": req_id, "method": method, "params": params},
    )
    assert r.status_code == 200, r.text
    return r.json()


def test_mcp_tools_tasks_extension_and_notices(hpc_home, tmp_path):
    d, ex, app, opened, remote, _ = _make_app(hpc_home, tmp_path)
    with TestClient(app, base_url="http://127.0.0.1:8765") as client:
        # 1. Modern 2026-07-28 Tasks-capable client
        _, sid_tasks = _mcp_call_modern(client, "/p/demo/mcp", 1, "server/discover", {})

        # Run a background command on bare1
        r_exec, sid_tasks = _mcp_call_modern(client, "/p/demo/mcp", 2, "tools/call", {
            "name": "remote_exec",
            "arguments": {"cluster": "bare1", "command": "sleep 20", "background": True},
        }, sid=sid_tasks)
        text = r_exec["result"]["content"][0]["text"]
        assert "op:1" in text

        # wait_for on Tasks-capable client returns durable task handle!
        r_wait, sid_tasks = _mcp_call_modern(client, "/p/demo/mcp", 3, "tools/call", {
            "name": "wait_for",
            "arguments": {"groups": [{"items": ["op:1"], "priority": 1}]},
        }, sid=sid_tasks)
        res = r_wait["result"]
        assert res["resultType"] == "task"
        assert res["status"] == "working"
        tid = res["taskId"]

        # Poll tasks/get -> still working
        r_get, _ = _mcp_call_modern(client, "/p/demo/mcp", 4, "tasks/get",
                                    {"taskId": tid}, sid=sid_tasks)
        assert r_get["result"]["status"] == "working"

        # Stop op:1 -> evaluate -> tasks/get transitions to completed with text payload!
        d.gateway.stop("demo", 1, by="user")
        r_get2, _ = _mcp_call_modern(client, "/p/demo/mcp", 5, "tasks/get",
                                     {"taskId": tid}, sid=sid_tasks)
        assert r_get2["result"]["status"] == "completed"
        assert "Wait #" in r_get2["result"]["result"]["content"][0]["text"]

        # Test tasks/cancel on another background op
        r_exec_c, sid_tasks = _mcp_call_modern(client, "/p/demo/mcp", 6, "tools/call", {
            "name": "remote_exec",
            "arguments": {"cluster": "bare1", "command": "sleep 20", "background": True},
        }, sid=sid_tasks)
        assert "op:2" in r_exec_c["result"]["content"][0]["text"]
        r_wait_c, sid_tasks = _mcp_call_modern(client, "/p/demo/mcp", 7, "tools/call", {
            "name": "wait_for",
            "arguments": {"groups": [{"items": ["op:2"], "priority": 1}]},
        }, sid=sid_tasks)
        tid_c = r_wait_c["result"]["taskId"]
        r_cancel, _ = _mcp_call_modern(client, "/p/demo/mcp", 8, "tasks/cancel",
                                       {"taskId": tid_c}, sid=sid_tasks)
        assert r_cancel["result"]["status"] == "cancelled"
        d.gateway.stop("demo", 2, by="user")

        # 2. Legacy / Non-Tasks client (Codex/OpenCode/DSH style): short-wait falls back + notices
        sid_plain = _mcp_init_legacy(client, "/p/demo/mcp")
        r_bg3 = _mcp_call(client, "/p/demo/mcp", sid_plain, 10, "tools/call", {
            "name": "remote_exec",
            "arguments": {"cluster": "bare1", "command": "sleep 20", "background": True},
        })
        assert "op:3" in r_bg3["result"]["content"][0]["text"]

        r_sw = _mcp_call(client, "/p/demo/mcp", sid_plain, 11, "tools/call", {
            "name": "wait_for",
            "arguments": {"groups": [{"items": ["op:3"], "priority": 1}], "wait_s": 0.05},
        })
        assert "Still running" in r_sw["result"]["content"][0]["text"]

        # User stops op:3 -> next tool call automatically prepends [HPCPort notices]!
        d.gateway.stop("demo", 3, by="user")
        r_jobs = _mcp_call(client, "/p/demo/mcp", sid_plain, 12, "tools/call", {
            "name": "jobs", "arguments": {},
        })
        jobs_text = r_jobs["result"]["content"][0]["text"]
        assert "[HPCPort notices]" in jobs_text
        assert "op:3" in jobs_text

        # 3. info tool returns attached servers, roots, and budget status
        r_info = _mcp_call(client, "/p/demo/mcp", sid_plain, 121, "tools/call", {
            "name": "info", "arguments": {},
        })
        info_text = r_info["result"]["content"][0]["text"]
        assert "gpu01 (slurm)" in info_text
        assert "bare1 (bare SSH" in info_text

        # 4. propose_preset via MCP queues a preset approval
        r_prop = _mcp_call(client, "/p/demo/mcp", sid_plain, 13, "tools/call", {
            "name": "propose_preset",
            "arguments": {
                "cluster": "gpu01",
                "changes": {"guide": "Use module load cuda/12.2"},
                "summary": "verified on login node",
            },
        })
        assert "Proposed preset update" in r_prop["result"]["content"][0]["text"]

        # 4. open_dashboard opens authenticated URL in daemon without leaking code to agent
        r_dash = _mcp_call(client, "/p/demo/mcp", sid_plain, 14, "tools/call", {
            "name": "open_dashboard", "arguments": {"tab": "approvals"},
        })
        dash_text = r_dash["result"]["content"][0]["text"]
        assert "Opened the HPCPort Web dashboard" in dash_text
        assert "code=" not in dash_text
        assert len(opened) >= 1 and "code=" in opened[-1]


def test_stdio_bridge_resolve_and_headers(hpc_home, tmp_path):
    from hpcport.stdio_bridge import build_forward_headers, resolve_target

    d, _, _, _, _, local_dir = _make_app(hpc_home, tmp_path)
    sub = local_dir / "nested" / "subdir"
    sub.mkdir(parents=True)
    assert resolve_target(cwd=str(sub)) == "demo"
    assert resolve_target(project="explicit", cwd=str(sub)) == "explicit"
    assert resolve_target(cwd=str(tmp_path)) is None

    hdrs, ver = build_forward_headers(json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {
            "name": "wait_for",
            "arguments": {},
            "_meta": {"io.modelcontextprotocol/protocolVersion": "2026-07-28"},
        },
    }))
    assert ver == "2026-07-28"
    assert hdrs["MCP-Protocol-Version"] == "2026-07-28"
    assert hdrs["MCP-Method"] == "tools/call"
    assert hdrs["MCP-Name"] == "wait_for"


def test_web_auth_csrf_and_web_only_approval(hpc_home, tmp_path):
    d, ex, app, opened, remote, _ = _make_app(hpc_home, tmp_path)
    with TestClient(app, base_url="http://127.0.0.1:8765") as client:
        # Unauthenticated API denied
        assert client.get("/api/state").status_code == 401

        # Daemon opens browser with single-use code -> exchange sets HttpOnly cookie
        d.open_dashboard(tab="settings")
        exchange_path = opened[-1].split("127.0.0.1:8765")[1]
        r_ex = client.get(exchange_path, follow_redirects=False)
        assert r_ex.status_code == 302
        assert r_ex.headers["location"] == "/#settings"
        assert "hpcport_session" in client.cookies

        # Authenticated GET succeeds
        st = client.get("/api/state")
        assert st.status_code == 200
        assert len(st.json()["projects"]) == 1

        # Mutating Web POST without X-HPCPort-UI header is blocked (CSRF protection)
        assert client.post("/api/poll", json={}).status_code == 403
        assert client.post("/api/poll", json={}, headers={"X-HPCPort-UI": "1"}).status_code == 200

        # MCP config snippets endpoint works for all target agents
        snips = client.get("/api/projects/demo/mcp-snippets").json()["snippets"]
        for key in ("codex", "opencode", "antigravity", "dsh", "claude_code"):
            assert key in snips

        # Enable web_only_approval -> CLI token cannot approve, read PIN, OR weaken config/policies!
        client.patch("/api/config", json={"web_only_approval": True},
                     headers={"X-HPCPort-UI": "1"})
        aid = d.settings.propose_preset("demo", "gpu01", {"guide": "new"}, "add note")["approval"]
        cli_client = TestClient(app)
        cli_hdr = {"Authorization": "Bearer secret-cli-token"}
        r_cli_pin = cli_client.get("/api/auth/pin", headers=cli_hdr)
        assert r_cli_pin.status_code == 403
        assert "hpcport daemon" in r_cli_pin.json()["error"]
        # CLI cannot disable web_only_approval or mutate project/preset policies
        assert cli_client.patch(
            "/api/config", json={"web_only_approval": False}, headers=cli_hdr,
        ).status_code == 403
        assert d.global_cfg.web_only_approval is True
        assert cli_client.patch(
            "/api/projects/demo", json={"changes": {"submit": {"approval": False}}}, headers=cli_hdr,
        ).status_code == 403
        assert cli_client.patch(
            "/api/presets/gpu01", json={"changes": {"gateway_allow": ["outside"]}}, headers=cli_hdr,
        ).status_code == 403
        r_cli = cli_client.post(
            "/api/approvals/%d/approve" % aid, json={}, headers=cli_hdr,
        )
        assert r_cli.status_code == 403
        r_web = client.post(
            "/api/approvals/%d/approve" % aid,
            json={"comment": "ok"},
            headers={"X-HPCPort-UI": "1"},
        )
        assert r_web.status_code == 200
        assert r_web.json()["state"] == "approved"
        # Web session CAN still toggle web_only_approval back off
        assert client.patch(
            "/api/config", json={"web_only_approval": False}, headers={"X-HPCPort-UI": "1"},
        ).status_code == 200
        assert d.global_cfg.web_only_approval is False


def test_cli_blocks_agent_env(hpc_home):
    p = subprocess.run(
        [sys.executable, "-m", "hpcport.cli", "approvals"],
        env={"PATH": "/usr/bin:/bin", "PYTHONPATH": "src", "HPCPORT_AGENT": "1",
             "HPCPORT_HOME": str(hpc_home)},
        cwd="/Users/liuce/Desktop/HPCPort",
        capture_output=True, text=True,
    )
    assert p.returncode != 0
    assert "not for agents" in p.stderr


def test_cli_doctor_ports_and_aliases(hpc_home):
    from hpcport.cli import build_parser

    p = build_parser()
    # --port accepted both before and after subcommand
    assert p.parse_args(["--port", "9001", "daemon", "--open"]).port == 9001
    assert p.parse_args(["daemon", "--port", "9002", "--open"]).port == 9002
    assert p.parse_args(["daemon", "--open"]).open is True
    assert p.parse_args(["ui", "--tab", "settings"]).tab == "settings"
    assert p.parse_args(["pin", "--port", "9003"]).port == 9003
    assert p.parse_args(["snippets", "demo", "--agent", "codex"]).agent == "codex"
    assert p.parse_args(["project", "add", "demo", "--local-dir", "/tmp"]).dir == "/tmp"
    assert p.parse_args(["preset", "list"]).sub == "list"
    assert p.parse_args(["cluster", "detach", "demo", "gpu01"]).sub == "detach"

    r = subprocess.run(
        [sys.executable, "-m", "hpcport.cli", "doctor", "--json"],
        env={"PATH": "/usr/bin:/bin", "PYTHONPATH": "src", "HPCPORT_HOME": str(hpc_home)},
        cwd="/Users/liuce/Desktop/HPCPort",
        capture_output=True, text=True,
    )
    assert r.returncode == 0, r.stderr
    info = json.loads(r.stdout)
    assert info["home"] == str(hpc_home)
    assert "python" in info and "port" in info


def test_unbound_dynamic_cwd_detach_and_internal_job_cancel(hpc_home, tmp_path):
    d, ex, app, opened, remote, local_dir = _make_app(hpc_home, tmp_path)
    with TestClient(app, base_url="http://127.0.0.1:8765") as client:
        # 1. Dynamic X-HPCPort-Cwd resolution on unbound /mcp endpoint
        r_init = client.post(
            "/mcp",
            headers={"Accept": "application/json, text/event-stream",
                     "X-HPCPort-Cwd": str(local_dir)},
            json={"jsonrpc": "2.0", "id": 1, "method": "initialize",
                  "params": {"protocolVersion": "2025-11-25", "capabilities": {},
                             "clientInfo": {"name": "cwd-test", "version": "1.0"}}},
        )
        assert r_init.status_code == 200
        sid = r_init.headers["mcp-session-id"]
        client.post(
            "/mcp",
            headers={"Accept": "application/json, text/event-stream", "Mcp-Session-Id": sid},
            json={"jsonrpc": "2.0", "method": "notifications/initialized"},
        )
        r_info = client.post(
            "/mcp",
            headers={"Accept": "application/json, text/event-stream",
                     "Mcp-Session-Id": sid, "X-HPCPort-Cwd": str(local_dir)},
            json={"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                  "params": {"name": "info", "arguments": {}}},
        )
        assert r_info.status_code == 200
        assert "project `demo`" in r_info.json()["result"]["content"][0]["text"]

        # 2. Authenticate Web client
        client.post("/api/auth/pin", json={"pin": d.pairing_pin}, headers={"X-HPCPort-UI": "1"})

        # 3. Trying to delete an in-use preset returns human-readable error message
        r_inuse = client.delete("/api/presets/bare1", headers={"X-HPCPort-UI": "1"})
        assert r_inuse.status_code == 400
        assert "still attached to project" in r_inuse.json()["error"]

        # 4. Detaching a cluster with an active unapproved job is blocked (cluster_busy),
        #    cancelling that job via #<internal_id> works, then detach succeeds!
        wd = remote / "demo_slurm" / "run2"
        wd.mkdir(parents=True, exist_ok=True)
        (wd / "job.sh").write_text(
            "#!/bin/bash\n#SBATCH --partition=a100\n#SBATCH --cpus-per-task=2\n"
            "#SBATCH --time=00:30:00\necho ok\n",
            encoding="utf-8",
        )
        d.jobs.submit("demo", "gpu01", str(wd), ["job.sh"], "pending job", wait_s=0)
        jrow = d.db.one("SELECT * FROM jobs ORDER BY id DESC LIMIT 1")
        assert jrow["slurm_id"] is None

        r_busy = client.delete("/api/projects/demo/clusters/gpu01", headers={"X-HPCPort-UI": "1"})
        assert r_busy.status_code == 400
        assert "Cannot detach" in r_busy.json()["error"]

        # Cancel unapproved job by #<internal_id>
        r_can = client.post(
            "/api/cancel",
            json={"project": "demo", "job_ids": ["#%d" % jrow["id"]], "comment": "cancel test"},
            headers={"X-HPCPort-UI": "1"},
        )
        assert r_can.status_code == 200
        assert "#%d" % jrow["id"] in r_can.json()["result"]["cancelled"]

        # Now detach gpu01 & bare1 cleanly!
        r_det = client.delete("/api/projects/demo/clusters/gpu01", headers={"X-HPCPort-UI": "1"})
        assert r_det.status_code == 200
        r_det2 = client.delete("/api/projects/demo/clusters/bare1", headers={"X-HPCPort-UI": "1"})
        assert r_det2.status_code == 200
        assert client.delete("/api/presets/bare1", headers={"X-HPCPort-UI": "1"}).status_code == 200


def test_web_ui_static_assets_and_rest_lifecycle(hpc_home, tmp_path):
    d, ex, app, opened, remote, _ = _make_app(hpc_home, tmp_path)
    with TestClient(app, base_url="http://127.0.0.1:8765") as client:
        # Static assets served publicly (auth is enforced on /api/*)
        r_idx = client.get("/")
        assert r_idx.status_code == 200
        assert "HPCPort" in r_idx.text
        for asset in ("/static/app.css", "/static/app.js", "/static/i18n.js"):
            ra = client.get(asset)
            assert ra.status_code == 200, asset

        # Authenticate via PIN
        r_pin = client.post("/api/auth/pin", json={"pin": d.pairing_pin},
                            headers={"X-HPCPort-UI": "1"})
        assert r_pin.status_code == 200
        assert client.get("/api/auth/status").json()["authenticated"] is True

        # Create, test, update, restore, and delete a bare SSH preset via Web API
        r_cp = client.post(
            "/api/presets",
            json={"name": "lab4090", "data": {
                "connect": "local", "scheduler": "none", "projects_root": str(remote),
            }},
            headers={"X-HPCPort-UI": "1"},
        )
        assert r_cp.status_code == 200
        r_test = client.post("/api/presets/lab4090/test", json={},
                             headers={"X-HPCPort-UI": "1"})
        assert r_test.status_code == 200
        assert r_test.json()["ok"] is True

        r_up = client.patch(
            "/api/presets/lab4090",
            json={"changes": {"guide": "4x RTX 4090 workstation"}},
            headers={"X-HPCPort-UI": "1"},
        )
        assert r_up.status_code == 200
        assert r_up.json()["version"] == 2

        r_rest = client.post(
            "/api/presets/lab4090/restore",
            json={"version": 1},
            headers={"X-HPCPort-UI": "1"},
        )
        assert r_rest.status_code == 200
        assert r_rest.json()["version"] == 3

        r_del = client.delete("/api/presets/lab4090", headers={"X-HPCPort-UI": "1"})
        assert r_del.status_code == 200

        # Submit a job, approve it, write fake stdout, and verify /api/job/{id}/files
        wd = remote / "demo_slurm" / "run1"
        wd.mkdir(parents=True, exist_ok=True)
        (wd / "job.sh").write_text(
            "#!/bin/bash\n#SBATCH --partition=a100\n#SBATCH --cpus-per-task=4\n"
            "#SBATCH --time=01:00:00\necho hi\n",
            encoding="utf-8",
        )
        r_sub = d.jobs.submit("demo", "gpu01", str(wd), ["job.sh"], "test run", wait_s=0)
        client.post(
            "/api/approvals/%d/approve" % r_sub["approval"],
            json={}, headers={"X-HPCPort-UI": "1"},
        )
        d.cycle(force=True)
        jrow = d.db.one("SELECT * FROM jobs ORDER BY id DESC LIMIT 1")
        assert jrow and jrow["slurm_id"]
        (wd / ("slurm-%s.out" % jrow["slurm_id"])).write_text("hello from slurm\n", encoding="utf-8")
        r_files = client.get("/api/job/%d/files" % jrow["id"])
        assert r_files.status_code == 200
        jf = r_files.json()
        assert "#SBATCH --cpus-per-task=4" in jf["script"]
        assert "hello from slurm" in jf["stdout"]

        # Jobs & Audit endpoints respond cleanly
        assert client.get("/api/jobs?project=demo").status_code == 200
        assert client.get("/api/audit?project=demo").status_code == 200


def test_stdio_bridge_concurrent_ping_not_blocked_by_wait_for(hpc_home, tmp_path):
    """Real `hpcport mcp` stdio bridge must answer `ping` immediately while `wait_for` runs."""
    import os
    import socket
    import threading
    import time
    import uvicorn

    d, ex, app, _, _, _ = _make_app(hpc_home, tmp_path)
    # Find an ephemeral free local port
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    cfg = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(cfg)
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    try:
        # Wait until live HTTP daemon is reachable
        import httpx
        deadline = time.monotonic() + 5.0
        with httpx.Client(trust_env=False, timeout=0.5) as hc:
            while time.monotonic() < deadline:
                try:
                    if hc.get("http://127.0.0.1:%d/api/health" % port).status_code == 200:
                        break
                except Exception:
                    time.sleep(0.05)

        # Start a background op so wait_for(wait_s=2.0) blocks for ~2s
        op = d.gateway.exec("demo", "bare1", "sleep 20", background=True)
        oid = op["op"]["id"]

        env = dict(os.environ, PYTHONPATH="src", HPCPORT_HOME=str(hpc_home))
        proc = subprocess.Popen(
            [sys.executable, "-m", "hpcport.cli", "mcp", "--project", "demo", "--port", str(port)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1, env=env, cwd="/Users/liuce/Desktop/HPCPort",
        )
        try:
            def send(obj):
                proc.stdin.write(json.dumps(obj) + "\n")
                proc.stdin.flush()

            send({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25", "capabilities": {},
                    "clientInfo": {"name": "stdio-conc", "version": "1.0"},
                },
            })
            r1 = json.loads(proc.stdout.readline())
            assert r1["id"] == 1
            send({"jsonrpc": "2.0", "method": "notifications/initialized"})

            # Fire a 2.0s wait_for, then immediately fire ping
            t0 = time.monotonic()
            send({
                "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {
                    "name": "wait_for",
                    "arguments": {"groups": [{"items": ["op:%d" % oid], "priority": 1}], "wait_s": 2.0},
                },
            })
            time.sleep(0.05)
            t_ping = time.monotonic()
            send({"jsonrpc": "2.0", "id": 3, "method": "ping", "params": {}})

            first = json.loads(proc.stdout.readline())
            ping_dt = time.monotonic() - t_ping
            second = json.loads(proc.stdout.readline())
            total_dt = time.monotonic() - t0

            assert first["id"] == 3, "ping was blocked behind wait_for: first=%r" % (first,)
            assert ping_dt < 0.8, "ping took %.2fs (head-of-line blocked)" % ping_dt
            assert second["id"] == 2
            assert total_dt >= 1.8
        finally:
            proc.stdin.close()
            proc.wait(timeout=5)
            d.gateway.stop("demo", oid, by="user")
    finally:
        server.should_exit = True
        t.join(timeout=5)


def test_pin_auth_requires_ui_header_and_rate_limits(hpc_home, tmp_path):
    now = [1000.0]
    d = Daemon(clock=lambda: now[0], browser_opener=lambda url: True)
    app = build_app(d, start_loop=False)

    with TestClient(app, base_url="http://127.0.0.1:8765") as client:
        # PIN unlock without X-HPCPort-UI header is rejected (403)
        r_no_hdr = client.post("/api/auth/pin", json={"pin": d.pairing_pin})
        assert r_no_hdr.status_code == 403

        # Five wrong PIN attempts trigger 429 cooldown that blocks even the real PIN
        wrong = "000000" if d.pairing_pin != "000000" else "999999"
        for _ in range(4):
            assert client.post(
                "/api/auth/pin", json={"pin": wrong}, headers={"X-HPCPort-UI": "1"},
            ).status_code == 401
        r_fifth = client.post(
            "/api/auth/pin", json={"pin": wrong}, headers={"X-HPCPort-UI": "1"},
        )
        assert r_fifth.status_code == 429
        assert int(r_fifth.headers["retry-after"]) >= 1
        assert client.post(
            "/api/auth/pin", json={"pin": d.pairing_pin}, headers={"X-HPCPort-UI": "1"},
        ).status_code == 429

        # After cooldown elapses, valid PIN succeeds
        now[0] += 61.0
        r_ok = client.post(
            "/api/auth/pin", json={"pin": d.pairing_pin}, headers={"X-HPCPort-UI": "1"},
        )
        assert r_ok.status_code == 200


def test_user_cancel_delivers_notice_and_shows_note(hpc_home, tmp_path):
    now = [1000.0]
    remote = tmp_path / "remote"
    remote.mkdir()
    local_dir = tmp_path / "local"
    local_dir.mkdir()
    ex = FakeSlurmExecutor(tmp_path / "bin")
    d = Daemon(executors={"gpu01": ex}, clock=lambda: now[0], browser_opener=lambda url: True)
    d.cli_token = "secret-cli-token"
    d.settings.create_preset("gpu01", {
        "connect": "local", "scheduler": "slurm", "partitions": ["a100"],
        "max_jobs": 4, "projects_root": str(remote),
    })
    d.settings.create_project("demo", local_dir=str(local_dir), data={"submit": {"approval": False}})
    d.attach_cluster("demo", "gpu01", root=str(remote / "demo_slurm"))
    app = build_app(d, start_loop=False)

    with TestClient(app, base_url="http://127.0.0.1:8765") as client:
        assert client.post(
            "/api/auth/pin", json={"pin": d.pairing_pin}, headers={"X-HPCPort-UI": "1"},
        ).status_code == 200

        sid = _mcp_init_legacy(client, "/p/demo/mcp")
        wd = remote / "demo_slurm" / "run_cancel"
        wd.mkdir(parents=True, exist_ok=True)
        (wd / "train.sh").write_text(
            "#!/bin/bash\n#SBATCH -p a100\n#SBATCH -n 2\n#SBATCH -t 01:00:00\necho go\n",
            encoding="utf-8",
        )
        sub = d.jobs.submit("demo", "gpu01", str(wd), ["train.sh"], "train run", batch="b_note", wait_s=0)
        assert sub["n_sent"] == 1
        _mcp_call(client, "/p/demo/mcp", sid, 2, "tools/call", {"name": "jobs", "arguments": {}})

        with d.lock:
            w = d.waits.register("demo", sid, groups=[{"items": ["b_note"], "priority": 1}])

        r_can = client.post(
            "/api/cancel",
            json={"project": "demo", "batch": "b_note", "comment": "lr too high, use 1e-4"},
            headers={"X-HPCPort-UI": "1"},
        )
        assert r_can.status_code == 200
        wrow = d.db.one("SELECT state FROM waits WHERE id = ?", (w["id"],))
        assert wrow["state"] == "fired"

        r_det = _mcp_call(client, "/p/demo/mcp", sid, 3, "tools/call", {
            "name": "jobs", "arguments": {"id": "b_note"},
        })
        txt = r_det["result"]["content"][0]["text"]
        assert "[HPCPort notices]" in txt
        assert "lr too high, use 1e-4" in txt
        assert "note=lr too high, use 1e-4" in txt
        assert "cancel note: lr too high, use 1e-4" in txt


def test_wait_for_replace_and_tasks_validation_isolation(hpc_home, tmp_path):
    d, ex, app, opened, remote, local_dir = _make_app(hpc_home, tmp_path)
    d.settings.create_project("other")
    with TestClient(app, base_url="http://127.0.0.1:8765") as client:
        # 1. Non-Tasks wait_for replaces prior active non-Tasks wait (no notice storm)
        sid = _mcp_init_legacy(client, "/p/demo/mcp")
        op = d.gateway.exec("demo", "bare1", "sleep 20", background=True)
        oid = op["op"]["id"]
        _mcp_call(client, "/p/demo/mcp", sid, 2, "tools/call", {
            "name": "wait_for",
            "arguments": {"groups": [{"items": ["op:%d" % oid], "priority": 1}], "wait_s": 0.02},
        })
        _mcp_call(client, "/p/demo/mcp", sid, 3, "tools/call", {
            "name": "wait_for",
            "arguments": {"groups": [{"items": ["op:%d" % oid], "priority": 1}], "wait_s": 0.02},
        })
        # Invalid call must NOT cancel the active wait
        r_bad = _mcp_call(client, "/p/demo/mcp", sid, 4, "tools/call", {
            "name": "wait_for",
            "arguments": {"groups": [{"items": ["nonexistent"], "priority": 1}], "wait_s": 0.02},
        })
        assert r_bad["result"]["isError"] is True
        active_waits = d.db.all("SELECT id FROM waits WHERE project = 'demo' AND state = 'active'")
        assert len(active_waits) == 1

        # 2. TasksExtension invalid argument types return tool isError (not 500)
        _, sid_t = _mcp_call_modern(client, "/p/demo/mcp", 10, "server/discover", {})
        r_bad_g, sid_t = _mcp_call_modern(client, "/p/demo/mcp", 11, "tools/call", {
            "name": "wait_for",
            "arguments": {"groups": "op:1"},
        }, sid=sid_t)
        assert r_bad_g["result"]["isError"] is True
        r_bad_ff, sid_t = _mcp_call_modern(client, "/p/demo/mcp", 12, "tools/call", {
            "name": "wait_for",
            "arguments": {"groups": [{"items": ["op:%d" % oid]}], "fail_frac": "not_a_num"},
        }, sid=sid_t)
        assert r_bad_ff["result"]["isError"] is True

        # 3. TasksExtension isolates tasks across projects (and rejects unbound /mcp)
        r_task, sid_t = _mcp_call_modern(client, "/p/demo/mcp", 13, "tools/call", {
            "name": "wait_for",
            "arguments": {"groups": [{"items": ["op:%d" % oid], "priority": 1}]},
        }, sid=sid_t)
        tid = r_task["result"]["taskId"]
        _, sid_o = _mcp_call_modern(client, "/p/other/mcp", 20, "server/discover", {})
        r_cross_get, _ = _mcp_call_modern(client, "/p/other/mcp", 21, "tasks/get",
                                          {"taskId": tid}, sid=sid_o)
        assert r_cross_get["result"]["status"] == "failed"
        r_cross_cancel, _ = _mcp_call_modern(client, "/p/other/mcp", 22, "tasks/cancel",
                                             {"taskId": tid}, sid=sid_o)
        assert r_cross_cancel["result"]["status"] == "failed"
        _, sid_u = _mcp_call_modern(client, "/mcp", 23, "server/discover", {})
        r_unbound_get, _ = _mcp_call_modern(client, "/mcp", 24, "tasks/get",
                                            {"taskId": tid}, sid=sid_u)
        assert r_unbound_get["result"]["status"] == "failed"
        # Original project still sees working task
        r_own, _ = _mcp_call_modern(client, "/p/demo/mcp", 14, "tasks/get",
                                    {"taskId": tid}, sid=sid_t)
        assert r_own["result"]["status"] == "working"
        d.gateway.stop("demo", oid, by="user")


def test_stdio_bridge_preserves_id_on_404_and_skips_notifications():
    import asyncio
    import io
    import socket
    import threading
    import time
    import httpx
    import uvicorn
    from starlette.applications import Starlette
    from starlette.responses import JSONResponse
    from starlette.routing import Route
    from hpcport import stdio_bridge

    async def fake_mcp(request):
        return JSONResponse(
            {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Session not found"}},
            status_code=404,
        )

    fake_app = Starlette(routes=[Route("/p/demo/mcp", fake_mcp, methods=["POST"])])
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        fport = s.getsockname()[1]
    srv = uvicorn.Server(uvicorn.Config(fake_app, host="127.0.0.1", port=fport, log_level="error"))
    t = threading.Thread(target=srv.run, daemon=True)
    t.start()
    try:
        deadline = time.monotonic() + 5.0
        with httpx.Client(trust_env=False, timeout=0.5) as hc:
            while time.monotonic() < deadline:
                try:
                    hc.post("http://127.0.0.1:%d/p/demo/mcp" % fport, content=b"{}")
                    break
                except Exception:
                    time.sleep(0.05)

        fake_in = io.StringIO(
            json.dumps({"jsonrpc": "2.0", "id": 42, "method": "tools/list", "params": {}}) + "\n"
            + json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n"
        )
        fake_out = io.StringIO()
        orig_in, orig_out = sys.stdin, sys.stdout
        sys.stdin, sys.stdout = fake_in, fake_out
        try:
            asyncio.run(stdio_bridge._run_bridge("http://127.0.0.1:%d/p/demo/mcp" % fport))
        finally:
            sys.stdin, sys.stdout = orig_in, orig_out
        lines = [ln for ln in fake_out.getvalue().splitlines() if ln.strip()]
        assert len(lines) == 1
        resp = json.loads(lines[0])
        assert resp["id"] == 42
        assert "Session not found" in resp["error"]["message"]
    finally:
        srv.should_exit = True
        t.join(timeout=5)


def test_server_auth_token_exchange_tab_and_audit_n_validation(hpc_home, tmp_path):
    d, ex, app, opened, remote, _ = _make_app(hpc_home, tmp_path)
    # 1. Dead "test-cli" token must never grant auth when cli_token is unset
    d.cli_token = None
    with TestClient(app, base_url="http://127.0.0.1:8765") as client:
        assert client.get("/api/state", headers={"Authorization": "Bearer test-cli"}).status_code == 401

        # 2. /auth/exchange validates tab whitelist and preserves stored_tab when omitted/invalid
        code1 = d.issue_exchange_code(tab="settings")
        r_no_tab = client.get("/auth/exchange?code=%s" % code1, follow_redirects=False)
        assert r_no_tab.status_code == 302
        assert r_no_tab.headers["location"] == "/#settings"

        code2 = d.issue_exchange_code(tab="jobs")
        r_bad_tab = client.get("/auth/exchange?code=%s&tab=evil" % code2, follow_redirects=False)
        assert r_bad_tab.status_code == 302
        assert r_bad_tab.headers["location"] == "/#jobs"

        # 3. /api/audit?n= rejects non-int with 400 and clamps <=0 to 1 (no SQLite LIMIT -1)
        for i in range(3):
            d.db.insert("audit", ts="2026-01-01T00:00:0%dZ" % i, project="demo",
                        session_key="s", tool="jobs", args="{}", result="ok")
        assert client.get("/api/audit?n=abc").status_code == 400
        r_neg = client.get("/api/audit?n=-1")
        assert r_neg.status_code == 200
        assert len(r_neg.json()["audit"]) == 1
        r_zero = client.get("/api/audit?n=0")
        assert r_zero.status_code == 200
        assert len(r_zero.json()["audit"]) == 1


def test_cli_port_flag_consistency_and_zero_cores_per_node(monkeypatch):
    from hpcport import cli

    p = cli.build_parser()
    assert p.parse_args(["jobs", "--port", "9010"]).port == 9010
    assert p.parse_args(["approvals", "--port", "9011"]).port == 9011
    assert p.parse_args(["poll", "--port", "9012"]).port == 9012
    assert p.parse_args(["project", "list", "--port", "9013"]).port == 9013
    assert p.parse_args(["project", "--port", "9014", "init", "demo"]).port == 9014
    assert p.parse_args(["cluster", "add", "c1", "--port", "9015", "--cores-per-node", "0"]).port == 9015
    assert p.parse_args(["cluster", "attach", "demo", "c1", "--port", "9016"]).port == 9016

    captured = {}
    monkeypatch.setattr(cli.stdio_bridge, "ensure_daemon", lambda port=None: port or 8765)

    def fake_api(port, method, path, payload=None):
        captured["port"] = port
        captured["path"] = path
        captured["payload"] = payload
        return {"name": "c1", "version": 1}

    monkeypatch.setattr(cli, "_api", fake_api)
    args = p.parse_args(["cluster", "add", "c1", "--port", "9015", "--cores-per-node", "0"])
    cli._cmd_cluster(args)
    assert captured["port"] == 9015
    assert captured["payload"]["data"]["cores_per_node"] == 0


def test_approval_decide_hardening_and_client_header_notice_isolation(hpc_home, tmp_path):
    from hpcport.stdio_bridge import build_forward_headers

    d, ex, app, opened, remote, _ = _make_app(hpc_home, tmp_path)
    hdrs, _ = build_forward_headers('{"jsonrpc":"2.0","id":1,"method":"ping"}',
                                    client_id="stdio-abc123")
    assert hdrs["X-HPCPort-Client"] == "stdio-abc123"

    with TestClient(app, base_url="http://127.0.0.1:8765", raise_server_exceptions=False) as client:
        assert client.post(
            "/api/auth/pin", json={"pin": d.pairing_pin}, headers={"X-HPCPort-UI": "1"},
        ).status_code == 200

        # 1. Colliding parameter names or non-string comment return 400 (never 500)
        aid = d.settings.propose_preset("demo", "gpu01", {"guide": "x"}, "note")["approval"]
        r_col = client.post(
            "/api/approvals/%d/approve" % aid,
            json={"approved": False},
            headers={"X-HPCPort-UI": "1"},
        )
        assert r_col.status_code == 400
        assert "approved" in r_col.json()["error"]

        r_aid = client.post(
            "/api/approvals/%d/approve" % aid,
            json={"aid": 999},
            headers={"X-HPCPort-UI": "1"},
        )
        assert r_aid.status_code == 400

        r_cmt = client.post(
            "/api/approvals/%d/approve" % aid,
            json={"comment": 12345},
            headers={"X-HPCPort-UI": "1"},
        )
        assert r_cmt.status_code == 400
        assert "comment" in r_cmt.json()["error"]

        # Dashboard open still succeeds cleanly via worker thread
        r_open = client.post("/api/dashboard/open", json={"tab": "jobs"},
                             headers={"X-HPCPort-UI": "1"})
        assert r_open.status_code == 200
        assert r_open.json()["opened"] is True

        # 2. Modern 2026-07-28 clients without Mcp-Session-Id isolate notice cursors via X-HPCPort-Client
        def call_with_client(cid: str, req_id: int, tool_name: str):
            r = client.post(
                "/p/demo/mcp",
                headers={
                    "Accept": "application/json, text/event-stream",
                    "MCP-Protocol-Version": "2026-07-28",
                    "MCP-Method": "tools/call",
                    "MCP-Name": tool_name,
                    "X-HPCPort-Client": cid,
                },
                json={
                    "jsonrpc": "2.0", "id": req_id, "method": "tools/call",
                    "params": {
                        "name": tool_name, "arguments": {},
                        "_meta": {
                            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                            "io.modelcontextprotocol/clientInfo": {"name": "c", "version": "1"},
                            "io.modelcontextprotocol/clientCapabilities": {},
                        },
                    },
                },
            )
            assert r.status_code == 200, r.text
            return r.json()["result"]["content"][0]["text"]

        # Initialize both clients' cursors
        call_with_client("stdio-client-A", 1, "jobs")
        call_with_client("stdio-client-B", 2, "jobs")

        # Emit a project notice; BOTH client A and client B must receive it independently
        d.waits.notify("demo", "preset_updated", "cluster:gpu01", {"cluster": "gpu01", "version": 2})
        txt_a = call_with_client("stdio-client-A", 3, "jobs")
        txt_b = call_with_client("stdio-client-B", 4, "jobs")
        assert "[HPCPort notices]" in txt_a and "gpu01" in txt_a
        assert "[HPCPort notices]" in txt_b and "gpu01" in txt_b
