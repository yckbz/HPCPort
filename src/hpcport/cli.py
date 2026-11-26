"""hpcport: command-line interface for HPCPort.

  hpcport daemon [--port PORT] [--open]         run the global daemon (foreground)
  hpcport mcp [--project NAME] [--port PORT]    stdio MCP bridge (auto-starts daemon)
  hpcport web|ui [--tab TAB] [--pin]            open authenticated Web dashboard
  hpcport pin                                   print headless 6-digit pairing PIN
  hpcport snippets PROJECT                      print MCP setup snippets for all agents
  hpcport doctor                                check environment, binary path, and daemon

  hpcport project init|add NAME [--dir DIR]     register a project
  hpcport project list                          list registered projects
  hpcport cluster|preset add NAME --host H ...  create a server preset (slurm or none)
  hpcport cluster attach PROJECT CLUSTER [...]  attach a server preset to a project
  hpcport cluster detach PROJECT CLUSTER        detach a server preset from a project
  hpcport cluster|preset list                   list server presets
  hpcport cluster|preset test NAME              test SSH connectivity to a server

  hpcport jobs [--project P] [BATCH]            list batches or jobs of a batch
  hpcport cancel --project P (--batch B | JOBID...) [-m NOTE]
  hpcport ops [--project P] [--all] [--stop ID]
  hpcport approvals [--project P] [--all]
  hpcport approve ID [--always [LIMITS]] [-m NOTE]
  hpcport reject ID [-m NOTE]
  hpcport standing PROJECT [--revoke]
  hpcport audit [--project P] [--tool T] [-n N]
  hpcport poll                                  trigger immediate cluster sync
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import secrets
import shutil
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from . import config, stdio_bridge
from .config import SUBMIT_LIMITS, cli_token_file, load_global_config

AGENT_ENV = "HPCPORT_AGENT"


class DaemonDown(Exception):
    pass


def _port(args) -> int:
    if getattr(args, "port", None):
        return int(args.port)
    return load_global_config().port


def _api(port: int, method: str, path: str, data: dict | None = None,
         down_ok: bool = False) -> dict:
    headers = {"Content-Type": "application/json"}
    try:
        headers["Authorization"] = "Bearer " + cli_token_file().read_text().strip()
    except OSError:
        pass
    body = json.dumps(data or {}).encode("utf-8") if method in ("POST", "PATCH", "PUT") else None
    req = urllib.request.Request("http://127.0.0.1:%d%s" % (port, path),
                                 method=method, data=body, headers=headers)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req, timeout=600) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        try:
            msg = json.load(e).get("error", "")
        except ValueError:
            msg = ""
        sys.exit("error: %s" % (msg or e))
    except urllib.error.URLError as e:
        if down_ok and isinstance(e.reason, ConnectionRefusedError):
            raise DaemonDown()
        sys.exit("cannot reach HPCPort daemon on port %d (%s); start it with: hpcport daemon"
                 % (port, e.reason))


def _cmd_daemon(args) -> None:
    import uvicorn
    from .daemon import Daemon
    from .server import build_app

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    port = _port(args)
    daemon = Daemon()
    if args.port:
        daemon.global_cfg = config.GlobalConfig(
            port=port,
            auto_open_dashboard=daemon.global_cfg.auto_open_dashboard,
            web_only_approval=daemon.global_cfg.web_only_approval,
            language=daemon.global_cfg.language,
        )
    token = secrets.token_urlsafe(32)
    daemon.cli_token = token
    tf = cli_token_file()
    tf.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(tf, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(token)
    config.daemon_pid_file().write_text(str(os.getpid()))
    logging.getLogger("hpcport").info(
        "HPCPort daemon listening on http://127.0.0.1:%d", port,
    )
    # Never write the pairing PIN to daemon.log; print only to an interactive terminal.
    if sys.stderr.isatty():
        sys.stderr.write("Headless unlock PIN: %s\n" % daemon.pairing_pin)
        sys.stderr.flush()
    if getattr(args, "open", False):
        tab = getattr(args, "tab", None) or "approvals"

        def _open_when_ready() -> None:
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                if stdio_bridge._daemon_alive(port):
                    daemon.open_dashboard(tab=tab)
                    return
                time.sleep(0.15)

        threading.Thread(target=_open_when_ready, daemon=True).start()
    uvicorn.run(build_app(daemon), host="127.0.0.1", port=port, log_level="warning")


def _cmd_mcp(args) -> None:
    sys.exit(stdio_bridge.serve_stdio(project=args.project, port=getattr(args, "port", None)))


def _cmd_web(args) -> None:
    port = stdio_bridge.ensure_daemon(port=_port(args))
    if getattr(args, "pin", False):
        r = _api(port, "GET", "/api/auth/pin")
        print("Dashboard: http://127.0.0.1:%d/" % port)
        print("Pairing PIN: %s" % r["pin"])
        return
    r = _api(port, "POST", "/api/dashboard/open", {"tab": getattr(args, "tab", None) or "approvals"})
    if r.get("opened"):
        print("Opened %s" % r["url"])
    else:
        st = _api(port, "GET", "/api/auth/status")
        if st.get("web_only_approval"):
            print("Open %s in your browser." % r["url"])
            print(
                "Note: web_only_approval is enabled, so CLI PIN retrieval is disabled. "
                "On a headless host, run `hpcport daemon` in a foreground terminal "
                "(e.g. tmux/screen) to view the startup pairing PIN."
            )
            return
        pin = _api(port, "GET", "/api/auth/pin").get("pin", "")
        print("Open %s in your browser (PIN: %s)" % (r["url"], pin))


def _cmd_pin(args) -> None:
    args.pin = True
    _cmd_web(args)


def _cmd_snippets(args) -> None:
    port = stdio_bridge.ensure_daemon(port=_port(args))
    r = _api(port, "GET", "/api/projects/%s/mcp-snippets" % urllib.parse.quote(args.project))
    snippets = r.get("snippets") or {}
    wanted = [args.agent] if getattr(args, "agent", None) else list(snippets)
    for key in wanted:
        if key not in snippets:
            sys.exit("unknown agent %r (choose from: %s)" % (key, ", ".join(snippets)))
        s = snippets[key]
        print("=== %s ===" % s["title"])
        print(s["code"])
        if s.get("http_note"):
            print(s["http_note"])
        print()


def _cmd_doctor(args) -> None:
    port = _port(args)
    bin_path = shutil.which("hpcport") or sys.executable
    alive = stdio_bridge._daemon_alive(port)
    info = {
        "python": sys.version.split()[0],
        "hpcport_bin": bin_path,
        "home": str(config.home()),
        "port": port,
        "daemon_running": alive,
        "ssh": shutil.which("ssh"),
        "rsync": shutil.which("rsync"),
        "presets": config.list_preset_names(),
        "projects": config.list_project_names(),
    }
    if getattr(args, "json", False):
        print(json.dumps(info, indent=2))
        return
    print("HPCPort Doctor")
    print("  Python:         %s" % info["python"])
    print("  CLI path:       %s" % info["hpcport_bin"])
    print("  Data home:      %s" % info["home"])
    print("  Daemon:         %s (http://127.0.0.1:%d)" % (
        "running" if alive else "stopped (auto-starts on `hpcport ui` or `hpcport mcp`)", port))
    print("  ssh:            %s" % (info["ssh"] or "MISSING"))
    print("  rsync:          %s" % (info["rsync"] or "MISSING (optional, needed for transfer)"))
    print("  Server presets: %s" % (", ".join(info["presets"]) or "(none)"))
    print("  Projects:       %s" % (", ".join(info["projects"]) or "(none)"))


def _cmd_project(args) -> None:
    port = stdio_bridge.ensure_daemon(port=_port(args))
    if args.sub == "list":
        r = _api(port, "GET", "/api/projects")
        for p in r["projects"]:
            clusters = ", ".join("%s:%s" % (k, v["root"]) for k, v in p["clusters"].items()) or "-"
            print("%-18s local=%s  clusters=%s" % (p["project"], p.get("local_dir") or "-", clusters))
        return
    local_dir = str(Path(args.dir).resolve()) if args.dir else str(Path.cwd().resolve())
    r = _api(port, "POST", "/api/projects", {"name": args.name, "local_dir": local_dir})
    print("Project registered: %s (local_dir=%s)" % (r["name"], local_dir))


def _cmd_cluster(args) -> None:
    port = stdio_bridge.ensure_daemon(port=_port(args))
    if args.sub == "list":
        r = _api(port, "GET", "/api/presets")
        for p in r["presets"]:
            d = p["data"]
            print("%-16s %-5s %-6s host=%s root=%s" % (
                p["name"], d.get("scheduler", "slurm"), d.get("connect", "ssh"),
                d.get("host") or "-", d.get("projects_root") or "-",
            ))
        return
    if args.sub == "add":
        data = {
            "connect": args.connect,
            "scheduler": args.scheduler,
            "max_jobs": args.max_jobs,
            "exec_timeout_s": args.exec_timeout,
        }
        if args.host:
            data["host"] = args.host
        if args.projects_root:
            data["projects_root"] = args.projects_root
        if args.partitions:
            data["partitions"] = [x.strip() for x in args.partitions.split(",") if x.strip()]
        if args.cores_per_node:
            data["cores_per_node"] = args.cores_per_node
        if args.gpus_per_node is not None:
            data["gpus_per_node"] = args.gpus_per_node
        r = _api(port, "POST", "/api/presets", {"name": args.name, "data": data})
        print("Preset created: %s (v%d)" % (r["name"], r["version"]))
        return
    if args.sub == "test":
        r = _api(port, "POST", "/api/presets/%s/test" % urllib.parse.quote(args.name), {})
        print("Connected to %s (home=%s, slurm=%s)" % (
            args.name, r.get("home") or "-", "yes" if r.get("has_slurm") else "no"))
        return
    if args.sub == "detach":
        r = _api(port, "DELETE", "/api/projects/%s/clusters/%s" % (
            urllib.parse.quote(args.project), urllib.parse.quote(args.cluster)))
        print("Detached %s from %s (v%d)" % (r["cluster"], r["project"], r["version"]))
        return
    if args.sub == "attach":
        payload = {"cluster": args.cluster}
        if args.root:
            payload["root"] = args.root
        if args.max_jobs is not None:
            payload["max_jobs"] = args.max_jobs
        r = _api(port, "POST", "/api/projects/%s/clusters" % urllib.parse.quote(args.project), payload)
        print("Attached %s to %s (root=%s)" % (r["cluster"], r["project"], r["root"]))


def _cmd_jobs(args) -> None:
    port = _port(args)
    if args.batch:
        if not args.project:
            sys.exit("--project is required when inspecting a specific batch")
        r = _api(port, "GET", "/api/jobs/%s/%s" % (
            urllib.parse.quote(args.project), urllib.parse.quote(args.batch)))
        print(r["text"])
        return
    q = ("?project=" + urllib.parse.quote(args.project)) if args.project else ""
    r = _api(port, "GET", "/api/jobs" + q)
    print(r["text"])


def _cmd_cancel(args) -> None:
    port = _port(args)
    payload = {"project": args.project, "batch": args.batch,
               "cluster": getattr(args, "cluster", None),
               "job_ids": args.job_ids or None, "comment": args.comment or ""}
    r = _api(port, "POST", "/api/cancel", payload)
    print(r["text"])


def _cmd_ops(args) -> None:
    port = _port(args)
    if args.stop is not None:
        r = _api(port, "POST", "/api/ops/%d/stop" % args.stop, {})
        print(r["text"])
        return
    params = []
    if args.project:
        params.append("project=" + urllib.parse.quote(args.project))
    if args.all:
        params.append("all=1")
    q = ("?" + "&".join(params)) if params else ""
    r = _api(port, "GET", "/api/ops" + q)
    for op in r["ops"]:
        print("op:%-4d %-10s %-10s %-8s %s" % (
            op["id"], op["project"], op["cluster"], op["kind"], op["state"]))


def _cmd_approvals(args) -> None:
    port = _port(args)
    params = []
    if args.project:
        params.append("project=" + urllib.parse.quote(args.project))
    if args.all:
        params.append("all=1")
    q = ("?" + "&".join(params)) if params else ""
    r = _api(port, "GET", "/api/approvals" + q)
    if not r["approvals"]:
        print("No approvals.")
        return
    for a in r["approvals"]:
        print("#%-4d [%-8s] %-10s (%s) %s" % (
            a["id"], a["state"], a["kind"], a["project"] or "global", a["summary"]))


def _cmd_decide(args, approve: bool) -> None:
    port = _port(args)
    payload: dict = {"comment": args.comment or ""}
    if approve and getattr(args, "always", False):
        payload["always"] = True
        for k in SUBMIT_LIMITS:
            val = getattr(args, k, None)
            if val is not None:
                payload[k] = None if val < 0 else val
    action = "approve" if approve else "reject"
    r = _api(port, "POST", "/api/approvals/%d/%s" % (args.id, action), payload)
    print("Approval #%d: %s" % (r["id"], r["state"]))


def _cmd_standing(args) -> None:
    port = _port(args)
    p = urllib.parse.quote(args.project)
    if args.revoke:
        r = _api(port, "DELETE", "/api/standing/" + p)
        print("Revoked standing approval for %s." % args.project if r["revoked"]
              else "No standing approval active.")
        return
    r = _api(port, "GET", "/api/standing/" + p)
    print(json.dumps(r, indent=2))


def _cmd_audit(args) -> None:
    port = _port(args)
    params = ["n=%d" % args.n]
    if args.project:
        params.append("project=" + urllib.parse.quote(args.project))
    if args.tool:
        params.append("tool=" + urllib.parse.quote(args.tool))
    r = _api(port, "GET", "/api/audit?" + "&".join(params))
    for row in r["audit"]:
        print("%s  %-12s %-14s %s" % (
            row["ts"], row.get("project") or "-", row["tool"], row["result"]))


def _cmd_poll(args) -> None:
    _api(_port(args), "POST", "/api/poll", {})
    print("Polled clusters.")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="hpcport", description="HPCPort compute-server gateway CLI")
    p.add_argument("--port", type=int, default=None, help="Daemon port override")
    sub = p.add_subparsers(dest="cmd", required=True)

    def add_port(sp):
        sp.add_argument("--port", type=int, default=argparse.SUPPRESS, help="Daemon port override")

    pd = sub.add_parser("daemon", help="Run the global daemon in the foreground")
    add_port(pd)
    pd.add_argument("--open", action="store_true", help="Open the Web dashboard once ready")
    pd.add_argument("--tab", default="approvals", choices=("approvals", "jobs", "audit", "settings"))

    pm = sub.add_parser("mcp", help="Run the stdio MCP bridge (auto-starts daemon)")
    add_port(pm)
    pm.add_argument("--project", default=None, help="Explicit project name")

    pw = sub.add_parser("web", aliases=["ui"], help="Open the authenticated Web dashboard")
    add_port(pw)
    pw.add_argument("--tab", default="approvals", choices=("approvals", "jobs", "audit", "settings"))
    pw.add_argument("--pin", action="store_true", help="Print headless 6-digit pairing PIN")

    ppin = sub.add_parser("pin", help="Print headless 6-digit pairing PIN")
    add_port(ppin)

    psn = sub.add_parser("snippets", help="Print MCP config snippets for a project")
    add_port(psn)
    psn.add_argument("project")
    psn.add_argument("--agent", default=None,
                     choices=("codex", "opencode", "antigravity", "dsh", "claude_code"))

    pdoc = sub.add_parser("doctor", help="Check installation, binary path, and daemon health")
    add_port(pdoc)
    pdoc.add_argument("--json", action="store_true", help="Emit JSON output")

    pp = sub.add_parser("project", help="Manage projects")
    ps = pp.add_subparsers(dest="sub", required=True)
    ps.add_parser("list")
    pi = ps.add_parser("init", aliases=["add"])
    pi.add_argument("name")
    pi.add_argument("--dir", "--local-dir", dest="dir", default=None,
                    help="Local workspace directory (default: cwd)")

    pc = sub.add_parser("cluster", aliases=["preset"],
                        help="Manage server presets and attachments")
    cs = pc.add_subparsers(dest="sub", required=True)
    cs.add_parser("list")
    ca = cs.add_parser("add")
    ca.add_argument("name")
    ca.add_argument("--connect", default="ssh", choices=("ssh", "local"))
    ca.add_argument("--host", default=None)
    ca.add_argument("--scheduler", default="slurm", choices=("slurm", "none"))
    ca.add_argument("--projects-root", default=None)
    ca.add_argument("--partitions", default=None, help="Comma-separated partition names")
    ca.add_argument("--max-jobs", type=int, default=10)
    ca.add_argument("--cores-per-node", type=int, default=None)
    ca.add_argument("--gpus-per-node", type=int, default=None,
                    help="GPUs per node for --exclusive estimates (0 for CPU-only nodes)")
    ca.add_argument("--exec-timeout", type=int, default=3600)
    ct = cs.add_parser("test")
    ct.add_argument("name")
    cat = cs.add_parser("attach")
    cat.add_argument("project")
    cat.add_argument("cluster")
    cat.add_argument("--root", default=None)
    cat.add_argument("--max-jobs", type=int, default=None)
    cdet = cs.add_parser("detach")
    cdet.add_argument("project")
    cdet.add_argument("cluster")

    pj = sub.add_parser("jobs", help="Show batches and jobs")
    pj.add_argument("batch", nargs="?", default=None)
    pj.add_argument("--project", "-p", default=None)

    px = sub.add_parser("cancel", help="Cancel Slurm jobs")
    px.add_argument("--project", "-p", default=None)
    px.add_argument("--cluster", "-c", default=None, help="Disambiguate Slurm IDs by cluster")
    px.add_argument("--batch", default=None)
    px.add_argument("job_ids", nargs="*")
    px.add_argument("-m", "--comment", default="")

    po = sub.add_parser("ops", help="List or stop background gateway operations")
    po.add_argument("--project", "-p", default=None)
    po.add_argument("--all", action="store_true")
    po.add_argument("--stop", type=int, default=None)

    pa = sub.add_parser("approvals", help="List approvals")
    pa.add_argument("--project", "-p", default=None)
    pa.add_argument("--all", action="store_true")

    pap = sub.add_parser("approve", help="Approve a pending request")
    pap.add_argument("id", type=int)
    pap.add_argument("--always", action="store_true")
    pap.add_argument("--core-hours", dest="core_hours", type=float, default=None)
    pap.add_argument("--gpu-hours", dest="gpu_hours", type=float, default=None)
    pap.add_argument("--total-jobs", dest="total_jobs", type=int, default=None)
    pap.add_argument("--total-core-hours", dest="total_core_hours", type=float, default=None)
    pap.add_argument("--total-gpu-hours", dest="total_gpu_hours", type=float, default=None)
    pap.add_argument("-m", "--comment", default="")

    prj = sub.add_parser("reject", help="Reject a pending request")
    prj.add_argument("id", type=int)
    prj.add_argument("-m", "--comment", default="")

    pst = sub.add_parser("standing", help="View or revoke project standing approval")
    pst.add_argument("project")
    pst.add_argument("--revoke", action="store_true")

    pau = sub.add_parser("audit", help="View audit log")
    pau.add_argument("--project", "-p", default=None)
    pau.add_argument("--tool", default=None)
    pau.add_argument("-n", type=int, default=30)

    sub.add_parser("poll", help="Force immediate Slurm sync")
    return p


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if os.environ.get(AGENT_ENV) == "1" and args.cmd != "mcp":
        sys.exit("HPCPort CLI is for the user, not for agents; use MCP tools instead.")
    dispatch = {
        "daemon": _cmd_daemon,
        "mcp": _cmd_mcp,
        "web": _cmd_web,
        "ui": _cmd_web,
        "pin": _cmd_pin,
        "snippets": _cmd_snippets,
        "doctor": _cmd_doctor,
        "project": _cmd_project,
        "cluster": _cmd_cluster,
        "preset": _cmd_cluster,
        "jobs": _cmd_jobs,
        "cancel": _cmd_cancel,
        "ops": _cmd_ops,
        "approvals": _cmd_approvals,
        "approve": lambda a: _cmd_decide(a, True),
        "reject": lambda a: _cmd_decide(a, False),
        "standing": _cmd_standing,
        "audit": _cmd_audit,
        "poll": _cmd_poll,
    }
    dispatch[args.cmd](args)


if __name__ == "__main__":
    main()
