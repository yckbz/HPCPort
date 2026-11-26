"""Multi-project compute server gateway: remote_exec, transfer, remote_stop.

Supports both Slurm login nodes (`scheduler: slurm`) and bare SSH compute
servers (`scheduler: none`). Commands run detached via setsid under
<root>/.hpcport/ops/<op_id>/, with process-group timeout enforcement, leftover
child cleanup, head+tail output truncation, and guard.py safety checks.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import posixpath
import re
import shlex
import shutil
import signal
import subprocess
import threading
import time
from pathlib import Path

from . import guard
from .config import staging_dir
from .executor import TransportError

log = logging.getLogger("hpcport.gateway")

WAIT_S = 180.0
EXEC_DEFAULT_S = 120
TRANSFER_DEFAULT_S = 1800
TRANSFER_MAX_S = 3600
KILL_GRACE_S = 10
OUTPUT_MAX = 20000
OUTPUT_HEAD, OUTPUT_TAIL = 5000, 15000
OPS_DIR = ".hpcport/ops"
KEEP_DAYS = 14
POLL_S = 15.0
POLL_BACKOFF_S = 60.0
EXEC_SLOTS = 4
TRANSFER_SLOTS = 2
UPLOAD_TIMEOUT_S = 300
SAFE_PATH = re.compile(r"^[A-Za-z0-9._/@%+=,-]+$")
LOCAL_PROTECTED = (".git",)

RUNNER = r"""__hpcp_d=%(d)s
echo $$ > "$__hpcp_d/runner"
cd -- %(cwd)s || { echo 127 > "$__hpcp_d/rc"; exit 0; }
export HPCPORT_PROJECT=%(project)s HPCPORT_CLUSTER=%(cluster)s HPCPORT_ROOT=%(root)s
set -m
( . "$__hpcp_d/cmd" ) > "$__hpcp_d/out" 2>&1 < /dev/null &
__hpcp_pid=$!
echo "$__hpcp_pid" > "$__hpcp_d/pid"
date +%%s > "$__hpcp_d/t0"
( sleep %(limit)d; : > "$__hpcp_d/timeout"; kill -TERM -- -$__hpcp_pid; sleep %(grace)d
  kill -KILL -- -$__hpcp_pid ) > /dev/null 2>&1 &
__hpcp_wd=$!
wait $__hpcp_pid
__hpcp_rc=$?
kill -TERM -- -$__hpcp_wd 2> /dev/null
if kill -0 -- -$__hpcp_pid 2> /dev/null; then
  : > "$__hpcp_d/leftover"
  kill -TERM -- -$__hpcp_pid 2> /dev/null; sleep 1; kill -KILL -- -$__hpcp_pid 2> /dev/null
fi
date +%%s > "$__hpcp_d/t1"
echo "$__hpcp_rc" > "$__hpcp_d/rc.tmp" && mv "$__hpcp_d/rc.tmp" "$__hpcp_d/rc"
"""

STATUS = r"""hpcp_status() {
  local d=$1 size f
  if [ -f "$d/rc" ]; then
    echo "HPCP_RC $(cat "$d/rc")"
    for f in timeout leftover stopped; do [ -f "$d/$f" ] && echo "HPCP_FLAG $f"; done
    echo "HPCP_TIMES $(cat "$d/t0" 2> /dev/null) $(cat "$d/t1" 2> /dev/null)"
    size=$(wc -c < "$d/out" 2> /dev/null | tr -d ' '); size=${size:-0}
    echo "HPCP_SIZE $size"
    if [ "$size" -le %(max)d ]; then echo HPCP_ALL; base64 < "$d/out" 2> /dev/null
    else echo HPCP_HEAD; head -c %(head)d "$d/out" | base64
         echo HPCP_TAIL; tail -c %(tail)d "$d/out" | base64; fi
    echo HPCP_DONE
  elif [ -f "$d/runner" ] && kill -0 "$(cat "$d/runner")" 2> /dev/null; then echo HPCP_RUNNING
  else echo HPCP_GONE; fi
}
""" % {"max": OUTPUT_MAX, "head": OUTPUT_HEAD, "tail": OUTPUT_TAIL}


class GatewayError(Exception):
    def __init__(self, code: str, **params):
        super().__init__(code)
        self.code = code
        self.params = params


class NeedsApproval(Exception):
    def __init__(self, answer: dict, why: list[dict]):
        super().__init__("needs approval")
        self.answer = answer
        self.why = why


def parse_status(out: str) -> dict:
    st = {"state": "gone", "flags": []}
    part, blocks = None, {}
    for line in out.splitlines():
        if line.startswith("HPCP_"):
            key, _, val = line.partition(" ")
            part = None
            if key == "HPCP_RC":
                st["rc"] = int(val) if val.strip().lstrip("-").isdigit() else None
            elif key == "HPCP_FLAG":
                st["flags"].append(val.strip())
            elif key == "HPCP_TIMES":
                t = [int(x) for x in val.split() if x.isdigit()]
                st["elapsed"] = float(t[1] - t[0]) if len(t) == 2 else None
            elif key == "HPCP_SIZE":
                st["size"] = int(val) if val.strip().isdigit() else 0
            elif key in ("HPCP_ALL", "HPCP_HEAD", "HPCP_TAIL"):
                part = key[5:].lower()
                blocks[part] = []
            elif key == "HPCP_DONE":
                st["state"] = "ended"
            elif key == "HPCP_RUNNING":
                st["state"] = "running"
            elif key == "HPCP_ERR":
                st["error"] = val.strip()
        elif part is not None:
            blocks[part].append(line.strip())
    for name, lines in blocks.items():
        try:
            st[name] = base64.b64decode("".join(lines)).decode("utf-8", errors="replace")
        except ValueError:
            st[name] = ""
    return st


def exec_result(st: dict, log_path: str) -> dict:
    whole = "all" in st
    return {
        "rc": st.get("rc"),
        "timeout": "timeout" in st["flags"],
        "stopped": "stopped" in st["flags"],
        "leftover": "leftover" in st["flags"],
        "elapsed": st.get("elapsed"),
        "size": st.get("size", 0),
        "head": st.get("all", st.get("head", "")),
        "tail": "" if whole else st.get("tail", ""),
        "truncated": not whole,
        "log": log_path,
    }


class _Transfer:
    def __init__(self, legs: list[list[str]], limit_s: float, cleanup=None):
        self.legs = legs
        self.limit_s = limit_s
        self.cleanup = cleanup
        self.done = threading.Event()
        self.stop = threading.Event()
        self.result: dict | None = None

    def run(self) -> None:
        t0 = time.monotonic()
        res = {"rc": 0, "timeout": False, "stopped": False, "error": "", "leg": None}
        try:
            for k, argv in enumerate(self.legs, 1):
                rc, err = self._leg(argv, t0, res)
                if rc != 0:
                    res.update(rc=rc, error=err.strip()[-1500:],
                               leg=k if len(self.legs) > 1 else None)
                    break
            else:
                if self.cleanup:
                    self.cleanup()
        except Exception as e:
            log.exception("transfer failed")
            res.update(rc=-1, error=str(e))
        res["elapsed"] = time.monotonic() - t0
        self.result = res
        self.done.set()

    def _leg(self, argv, t0, res) -> tuple[int, str]:
        try:
            p = subprocess.Popen(
                argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE, text=True, errors="replace", start_new_session=True,
            )
        except OSError as e:
            return 127, "cannot start rsync: %s" % e
        while True:
            try:
                _, err = p.communicate(timeout=0.2)
                return p.returncode, err or ""
            except subprocess.TimeoutExpired:
                over = time.monotonic() - t0 > self.limit_s
                if over or self.stop.is_set():
                    res["timeout" if over and not self.stop.is_set() else "stopped"] = True
                    for sig in (signal.SIGTERM, signal.SIGKILL):
                        try:
                            os.killpg(p.pid, sig)
                        except (ProcessLookupError, PermissionError):
                            break
                        try:
                            p.wait(KILL_GRACE_S)
                            break
                        except subprocess.TimeoutExpired:
                            pass
                    _, err = p.communicate()
                    return p.returncode or -1, err or ""


class GatewayService:
    def __init__(self, projects: dict, db, executors: dict, notify, permissions, lock,
                 clock=time.time):
        self.projects = projects
        self.db = db
        self.ex = executors
        self.notify = notify
        self.permissions = permissions
        self.lock = lock
        self.now = clock
        self.wait_s = WAIT_S
        self.poll_s = POLL_S
        self._facts: dict[tuple[str, str], dict] = {}   # (project, cluster) -> home, real root
        self._inline: set[int] = set()
        self._transfers: dict[int, _Transfer] = {}
        self._misses: dict[int, int] = {}
        self._next_poll: dict[str, float] = {}
        self._slots: dict[str, threading.Semaphore] = {}
        self._transfer_slots = threading.BoundedSemaphore(TRANSFER_SLOTS)
        self.on_change = lambda project=None: None

    # ------------------------------------------------------------------ helpers
    def _project(self, project: str):
        p = self.projects.get(project)
        if p is None:
            raise GatewayError("unknown_project", project=project)
        return p

    def _cluster(self, project: str, name: str):
        p = self._project(project)
        cl = p.clusters.get(name)
        if cl is None or name not in self.ex:
            raise GatewayError("unknown_cluster", cluster=name,
                               clusters=", ".join(sorted(p.clusters)) or "-")
        return p, cl, self.ex[name]

    def _run(self, cluster: str, script: str, timeout: float):
        slot = self._slots.setdefault(cluster, threading.BoundedSemaphore(EXEC_SLOTS))
        with slot:
            return self.ex[cluster].run("bash -s", input=script, timeout=timeout)

    def clear_facts(self, cluster: str | None = None, project: str | None = None) -> None:
        for k in list(self._facts):
            if (cluster is None or k[1] == cluster) and (project is None or k[0] == project):
                self._facts.pop(k, None)

    def facts(self, project: str, cluster: str) -> dict:
        key = (project, cluster)
        if key not in self._facts:
            _, cl, ex = self._cluster(project, cluster)
            try:
                r = ex.run(
                    "printf 'HOME %%s\\n' \"$HOME\"; printf 'REAL %%s\\n' "
                    "\"$(cd -- %s 2> /dev/null && pwd -P)\"" % shlex.quote(cl.root),
                    timeout=60,
                )
            except TransportError as e:
                raise GatewayError("unreachable", cluster=cluster, error=str(e))
            got = dict(line.partition(" ")[::2] for line in r.out.splitlines())
            if not got.get("REAL"):
                raise GatewayError("no_root", cluster=cluster, root=cl.root)
            self._facts[key] = {"home": got.get("HOME") or None, "real": got["REAL"]}
        return self._facts[key]

    def _remote_path(self, project: str, cl, path: str, what: str = "remote_outside") -> str:
        full = posixpath.normpath(path if path.startswith("/") else posixpath.join(cl.root, path))
        real = self.facts(project, cl.name)["real"]
        if not any(full == r or full.startswith(r.rstrip("/") + "/") for r in (cl.root, real)):
            raise GatewayError(what, path=path, root=cl.root, cluster=cl.name)
        return full

    def _local_path(self, p, path: str, dest: bool = False, name: str | None = None) -> Path:
        if p.local_dir is None:
            raise GatewayError("no_local_dir", project=p.name)
        root = p.local_dir.resolve()
        fp = Path(path).expanduser()
        full = (fp if fp.is_absolute() else root / fp).resolve()
        if full != root and root not in full.parents:
            raise GatewayError("local_outside", path=path, project_dir=str(root))
        if dest:
            for target in [full] + ([full / name] if name else []):
                rel = target.relative_to(root).as_posix()
                if any(rel == b or rel.startswith(b + "/") for b in LOCAL_PROTECTED):
                    raise GatewayError("local_protected", path=path)
        return full

    def _ask(self, project: str, tool: str, args: dict, why: list[dict],
             wait_s: float | None = None, on_tick=None) -> None:
        answer = self.permissions.ask(project, tool, args, why=why, wait_s=wait_s, on_tick=on_tick)
        if not answer["allow"]:
            raise NeedsApproval(answer, why)

    def _room(self, p) -> None:
        rows = self.db.all("SELECT id FROM ops WHERE project = ? AND state = 'running'", (p.name,))
        n = len([r for r in rows if r["id"] not in self._inline])
        if n >= p.max_background:
            raise GatewayError("too_many_background", n=n, max=p.max_background)

    def _rsync(self, sources: list[str], dest: str, rsh: str | None, exclude=()) -> list[str]:
        argv = [shutil.which("rsync") or "rsync", "-a", "--partial"]
        argv += ["--exclude=%s" % e for e in exclude]
        if rsh:
            argv += ["-e", rsh]
        return argv + sources + [dest]

    def op(self, op_id: int) -> dict | None:
        row = self.db.one("SELECT * FROM ops WHERE id = ?", (op_id,))
        if row:
            row["spec"] = json.loads(row["spec"])
            row["result"] = json.loads(row["result"]) if row["result"] else None
            row["log"] = row["dir"] + "/out" if row["dir"] else None
        return row

    def list(self, project: str | None = None, every: bool = False) -> list[dict]:
        conds, args = [], []
        if project is not None:
            conds.append("project = ?")
            args.append(project)
        if not every:
            conds.append("state = 'running'")
        where = "WHERE " + " AND ".join(conds) if conds else ""
        rows = self.db.all("SELECT id FROM ops %s ORDER BY id DESC LIMIT 100" % where, tuple(args))
        return [self.op(r["id"]) for r in rows]

    def running_ops(self, project: str) -> list[int]:
        rows = self.db.all("SELECT id FROM ops WHERE project = ? AND state = 'running' ORDER BY id",
                           (project,))
        return [r["id"] for r in rows]

    def _finish(self, op_id: int, result: dict, told: bool, state: str = "ended") -> dict:
        with self.lock:
            row = self.op(op_id)
            if row is None or row["state"] != "running":
                return row
            keep = row["kind"] == "exec" and state != "lost"
            self.db.update("ops", {"id": op_id}, state=state,
                           result=json.dumps(result, ensure_ascii=False),
                           ended=self.now(), cleaned=0 if keep else 1)
            self._misses.pop(op_id, None)
            self._transfers.pop(op_id, None)
            if not told:
                self.notify(
                    row["project"],
                    "op_ended",
                    "op:%d" % op_id,
                    {"op": op_id, "kind": row["kind"], "cluster": row["cluster"],
                     "state": state, "rc": result.get("rc"), "stopped": result.get("stopped"),
                     "stop_by": row.get("stop_by"), "timeout": result.get("timeout"),
                     "elapsed": result.get("elapsed"), "log": row["log"]},
                )
            self.on_change(row["project"])
            return self.op(op_id)

    # ------------------------------------------------------------------ exec
    def exec(self, project: str, cluster: str, command: str, cwd: str | None = None,
             upload=(), timeout_s: float | None = None, background: bool = False,
             on_tick=None) -> dict:
        p, cl, ex = self._cluster(project, cluster)
        if not command or not command.strip():
            raise GatewayError("empty_arg", arg="command")
        if timeout_s is None:
            limit = cl.exec_timeout_s if background else min(EXEC_DEFAULT_S, cl.exec_timeout_s)
        elif not 1 <= timeout_s <= cl.exec_timeout_s:
            raise GatewayError("bad_timeout_s", max=cl.exec_timeout_s, cluster=cluster)
        else:
            limit = int(timeout_s)
        wd = self._remote_path(project, cl, cwd or cl.root, "bad_cwd")
        files = []
        for f in upload or ():
            lp = self._local_path(p, f)
            if not lp.exists():
                raise GatewayError("bad_upload", path=f)
            files.append(str(lp))
        facts = self.facts(project, cluster)
        found = guard.check(
            command,
            guard.place_for(cl.root, facts["real"], wd, facts["home"], cl.extra_paths),
            cl.gateway_allow,
            scheduler=cl.scheduler,
        )
        refused = [f for f in found if f.action == guard.REFUSE]
        if refused:
            raise GatewayError("use_submit", command=refused[0].what)
        args = dict(cluster=cluster, command=command, cwd=cwd, upload=list(upload or ()),
                    timeout_s=timeout_s, background=bool(background))
        if found:
            self._ask(project, "remote_exec", args,
                      [{"code": f.rule, "what": f.what} for f in found],
                      wait_s=min(self.permissions.wait_s, p.gateway.wait_s), on_tick=on_tick)
        with self.lock:
            self._room(p)
        if files:
            argv = self._rsync(files, ex.rsync_spec(wd.rstrip("/") + "/"), ex.rsync_rsh())
            try:
                proc = subprocess.run(argv, capture_output=True, text=True, errors="replace",
                                      stdin=subprocess.DEVNULL, timeout=UPLOAD_TIMEOUT_S)
                err = proc.stderr if proc.returncode else ""
            except (OSError, subprocess.TimeoutExpired) as e:
                err = str(e) or "failed"
            if err:
                raise GatewayError("upload_failed", cluster=cluster,
                                   error=" ".join(err.split())[-500:])
        with self.lock:
            op_id = self.db.insert(
                "ops", project=project, kind="exec", cluster=cluster,
                spec=json.dumps(args, ensure_ascii=False), limit_s=limit,
                state="running", started=self.now(),
            )
            d = "%s/%s/%d" % (cl.root.rstrip("/"), OPS_DIR, op_id)
            self.db.update("ops", {"id": op_id}, dir=d)
            self._inline.add(op_id)
            done = [
                r["dir"] for r in self.db.all(
                    "SELECT dir, result FROM ops WHERE project = ? AND cluster = ? "
                    "AND kind = 'exec' AND cleaned = 0 AND state = 'ended' AND dir IS NOT NULL",
                    (project, cluster),
                )
                if not json.loads(r["result"] or "{}").get("truncated")
            ]
            self.on_change(project)
        wait = 0.0 if background else min(self.wait_s, p.gateway.wait_s)
        script = self._start_script(project, cl, ex, d, wd, command, limit, wait, done)
        try:
            try:
                r = self._run(cluster, script, timeout=wait + 90)
            except TransportError as e:
                log.warning("command %d on %s: %s", op_id, cluster, e)
                return {"status": "unknown", "op": self.op(op_id)}
            st = parse_status(r.out)
            if st.get("error"):
                with self.lock:
                    self.db.run("DELETE FROM ops WHERE id = ?", (op_id,))
                if st["error"] == "nocwd":
                    raise GatewayError("no_cwd", path=cwd or cl.root, cluster=cluster)
                if st["error"] == "outside":
                    raise GatewayError("bad_cwd", path=cwd or cl.root, root=cl.root,
                                       cluster=cluster)
                raise GatewayError("exec_failed", cluster=cluster,
                                   error=" ".join((r.err or st["error"]).split())[-300:])
            with self.lock:
                for path in done:
                    self.db.run("UPDATE ops SET cleaned = 1 WHERE dir = ?", (path,))
            if st["state"] == "ended":
                return {"status": "ended",
                        "op": self._finish(op_id, exec_result(st, d + "/out"), told=True)}
            return {"status": "background" if background else "running", "op": self.op(op_id),
                    "waited": wait}
        finally:
            with self.lock:
                self._inline.discard(op_id)

    def _start_script(self, project, cl, ex, d, cwd, command, limit, wait, done) -> str:
        q = shlex.quote
        runner = RUNNER % {
            "d": q(d), "cwd": q(cwd), "project": q(project), "cluster": q(cl.name),
            "root": q(cl.root), "limit": limit, "grace": KILL_GRACE_S,
        }
        tag = "HPCP_%s" % hashlib.sha256(os.urandom(16)).hexdigest()[:16]
        b64 = base64.encodebytes(command.encode("utf-8")).decode("ascii").strip()
        ops = "%s/%s" % (cl.root.rstrip("/"), OPS_DIR)
        clean = "".join("rm -rf -- %s\n" % q(p) for p in done if p.startswith(ops + "/"))
        return STATUS + (
            "root=%(root)s; d=%(d)s\n"
            "real=$(cd -- \"$root\" 2> /dev/null && pwd -P)\n"
            "if [ -z \"$real\" ]; then echo 'HPCP_ERR noroot'\n"
            "elif ! cd -- %(cwd)s 2> /dev/null; then echo 'HPCP_ERR nocwd'\n"
            "else case \"$(pwd -P)/\" in \"$real\"/*) ok=1;; *) ok=0;; esac\n"
            "if [ $ok = 0 ]; then echo 'HPCP_ERR outside'\n"
            "elif ! mkdir -p \"$d\"; then echo 'HPCP_ERR mkdir'\n"
            "else\n"
            "%(clean)s"
            "find %(ops)s -mindepth 1 -maxdepth 1 -type d -mtime +%(keep)d "
            "-exec rm -rf {} + 2> /dev/null\n"
            "base64 -d > \"$d/cmd\" <<'%(tag)s'\n%(b64)s\n%(tag)s\n"
            "cat > \"$d/run.sh\" <<'%(tag)s'\n%(runner)s%(tag)s\n"
            "if command -v setsid > /dev/null 2>&1; then\n"
            "  setsid %(shell)s \"$d/run.sh\" < /dev/null > /dev/null 2>&1 &\n"
            "else nohup %(shell)s \"$d/run.sh\" < /dev/null > /dev/null 2>&1 &\nfi\n"
            "SECONDS=0; t=0\n"
            "while [ ! -f \"$d/rc\" ] && [ \"$SECONDS\" -lt %(wait)d ]; do\n"
            "  if [ $t -lt 30 ]; then sleep 0.1; else sleep 1; fi; t=$((t+1))\ndone\n"
            "if [ -f \"$d/rc\" ]; then hpcp_status \"$d\"; else echo HPCP_RUNNING; fi\n"
            "fi\nfi\n"
            % {
                "root": q(cl.root), "d": q(d), "cwd": q(cwd), "clean": clean, "ops": q(ops),
                "keep": KEEP_DAYS, "tag": tag, "b64": b64, "runner": runner,
                "shell": ex.shell, "wait": int(round(wait)),
            }
        )

    def _status(self, op: dict, stop: bool = False) -> dict:
        q = shlex.quote
        script = STATUS + "d=%s\n" % q(op["dir"])
        if stop:
            script += (
                "if [ ! -f \"$d/rc\" ] && [ -f \"$d/pid\" ]; then : > \"$d/stopped\"\n"
                "  kill -TERM -- -\"$(cat \"$d/pid\")\" 2> /dev/null; SECONDS=0\n"
                "  while [ ! -f \"$d/rc\" ] && [ \"$SECONDS\" -lt %d ]; do sleep 0.2; done\n"
                "  [ -f \"$d/rc\" ] || kill -KILL -- -\"$(cat \"$d/pid\")\" 2> /dev/null\n"
                "  SECONDS=0; while [ ! -f \"$d/rc\" ] && [ \"$SECONDS\" -lt 5 ]; do sleep 0.2; done\n"
                "fi\n" % KILL_GRACE_S
            )
        r = self._run(op["cluster"], script + "hpcp_status \"$d\"\n", timeout=60)
        return parse_status(r.out)

    def read_output(self, op_id: int, max_bytes: int = 32768) -> dict:
        """Read latest output tail of a gateway exec op (for Web UI live inspection)."""
        op = self.op(op_id)
        if op is None:
            raise GatewayError("unknown_op", id=op_id)
        if op["kind"] != "exec" or not op["dir"]:
            return {"op": op, "output": ""}
        if op["cluster"] not in self.ex:
            raise GatewayError("unknown_cluster", cluster=op["cluster"], clusters="")
        q = shlex.quote
        try:
            r = self._run(op["cluster"],
                          "tail -c %d -- %s 2>/dev/null || true\n"
                          % (max_bytes, q(op["dir"] + "/out")),
                          timeout=30)
        except TransportError as e:
            raise GatewayError("unreachable", cluster=op["cluster"], error=str(e))
        return {"op": op, "output": r.out}

    # ------------------------------------------------------------------ poll & recover
    def poll(self) -> list[int]:
        ended = []
        with self.lock:
            rows = [self.op(r["id"]) for r in self.db.all(
                "SELECT id FROM ops WHERE kind = 'exec' AND state = 'running' ORDER BY id")]
            rows = [r for r in rows if r and r["id"] not in self._inline]
        mono = time.monotonic()
        skip: set[str] = set()
        for op in rows:
            c = op["cluster"]
            if c in skip or c not in self.ex or self._next_poll.get(c, 0) > mono:
                continue
            try:
                st = self._status(op)
            except TransportError as e:
                log.warning("cluster %s unreachable during op poll: %s", c, e)
                self._next_poll[c] = mono + max(POLL_BACKOFF_S, self.poll_s)
                skip.add(c)
                continue
            if st["state"] == "ended":
                self._finish(op["id"], exec_result(st, op["log"]), told=False)
                ended.append(op["id"])
            elif st["state"] == "gone":
                self._misses[op["id"]] = self._misses.get(op["id"], 0) + 1
                if self._misses[op["id"]] >= 2:
                    self._finish(op["id"], {"lost": True, "log": op["log"]}, told=False,
                                 state="lost")
                    ended.append(op["id"])
            else:
                self._misses.pop(op["id"], None)
        for c in {op["cluster"] for op in rows} - skip:
            if self._next_poll.get(c, 0) <= mono:
                self._next_poll[c] = mono + self.poll_s
        return ended

    def recover(self) -> None:
        with self.lock:
            rows = self.db.all("SELECT id FROM ops WHERE kind = 'transfer' AND state = 'running'")
        for r in rows:
            if r["id"] not in self._transfers:
                self._finish(r["id"], {"rc": None, "interrupted": True, "error": "",
                                       "elapsed": None}, told=False)

    # ------------------------------------------------------------------ transfer
    def _endpoint(self, p, text: str, dest: bool, name: str | None = None) -> dict:
        head, sep, tail = text.partition(":")
        if sep and "/" not in head and head:
            _, cl, ex = self._cluster(p.name, head)
            if tail and not SAFE_PATH.match(tail):
                raise GatewayError("bad_path", path=text)
            path = self._remote_path(p.name, cl, tail or cl.root)
            if tail.endswith("/") and path != "/":
                path += "/"
            return {"cluster": head, "path": path, "spec": ex.rsync_spec(path),
                    "rsh": ex.rsync_rsh(), "text": "%s:%s" % (head, path)}
        if not text.strip():
            raise GatewayError("empty_arg", arg="dst" if dest else "src")
        path = str(self._local_path(p, text, dest, name))
        if text.endswith("/"):
            path += "/"
        return {"cluster": None, "path": path, "spec": path, "rsh": None, "text": path}

    def _size(self, end: dict) -> int:
        if end["cluster"] is None:
            lp = Path(end["path"])
            if not lp.exists():
                raise GatewayError("no_source", path=end["text"])
            if lp.is_file():
                return lp.stat().st_size
            return sum(f.stat().st_size for f in lp.rglob("*") if f.is_file()
                       and not f.is_symlink())
        try:
            q = shlex.quote(end["path"])
            r = self.ex[end["cluster"]].run(
                "s=$(du -sb -- %s 2> /dev/null | cut -f1); if [ -n \"$s\" ]; then echo \"B $s\"; "
                "else echo \"K $(du -sk -- %s 2> /dev/null | cut -f1)\"; fi" % (q, q),
                timeout=120,
            )
        except TransportError as e:
            raise GatewayError("unreachable", cluster=end["cluster"], error=str(e))
        got = r.out.split()
        if len(got) != 2 or not got[1].isdigit():
            raise GatewayError("no_source", path=end["text"])
        return int(got[1]) * (1024 if got[0] == "K" else 1)

    def transfer(self, project: str, src: str, dst: str, exclude=(),
                 timeout_s: float | None = None, background: bool = False,
                 on_tick=None) -> dict:
        p = self._project(project)
        if timeout_s is None:
            limit = TRANSFER_DEFAULT_S
        elif not 1 <= timeout_s <= TRANSFER_MAX_S:
            raise GatewayError("bad_transfer_timeout", max=TRANSFER_MAX_S)
        else:
            limit = int(timeout_s)
        exclude = [str(e) for e in exclude or ()]
        a = self._endpoint(p, src, dest=False)
        base = None if src.endswith("/") else posixpath.basename(a["path"].rstrip("/"))
        b = self._endpoint(p, dst, dest=True, name=base)
        if a["cluster"] is None and b["cluster"] is None:
            raise GatewayError("both_local")
        size = self._size(a)
        args = dict(src=src, dst=dst, exclude=exclude, timeout_s=timeout_s,
                    background=bool(background))
        if size > p.transfer_max_gb * 1024 ** 3:
            self._ask(
                project, "transfer", args,
                [{"code": "size", "size": size / 1024 ** 3, "limit": p.transfer_max_gb}],
                wait_s=min(self.permissions.wait_s, p.gateway.wait_s), on_tick=on_tick,
            )
        cleanup = None
        if a["cluster"] is not None and b["cluster"] is not None:
            tag = hashlib.sha256(("%s\0%s\0%s" % (project, a["text"], b["text"])).encode()).hexdigest()[:12]
            stage = staging_dir() / tag
            stage.mkdir(parents=True, exist_ok=True)
            mid = str(stage) + "/" + ("_/" if base is None else "")
            back = str(stage) + "/" + ("_/" if base is None else base)
            legs = [self._rsync([a["spec"]], mid, a["rsh"], exclude),
                    self._rsync([back], b["spec"], b["rsh"], exclude)]
            cleanup = lambda: shutil.rmtree(stage, ignore_errors=True)
        else:
            legs = [self._rsync([a["spec"]], b["spec"], a["rsh"] or b["rsh"], exclude)]
        with self.lock:
            self._room(p)
            spec = dict(args, src=a["text"], dst=b["text"], size=size)
            op_id = self.db.insert(
                "ops", project=project, kind="transfer",
                spec=json.dumps(spec, ensure_ascii=False), limit_s=limit,
                cluster=",".join(dict.fromkeys(e["cluster"] for e in (a, b) if e["cluster"])),
                state="running", started=self.now(),
            )
            job = _Transfer(legs, limit, cleanup)
            self._transfers[op_id] = job
            self._inline.add(op_id)
            self.on_change(project)
        threading.Thread(target=self._transfer_thread, args=(op_id, job), daemon=True,
                         name="transfer-%d" % op_id).start()
        wait = 0.0 if background else min(self.wait_s, p.gateway.wait_s)
        try:
            job.done.wait(wait)
            with self.lock:
                row = self.op(op_id)
                if job.done.is_set() and row["state"] == "running":
                    return {"status": "ended", "op": self._finish(op_id, job.result, told=True)}
                self._inline.discard(op_id)
            if row["state"] != "running":
                return {"status": "ended", "op": row}
            return {"status": "background" if background else "running", "op": row, "waited": wait}
        finally:
            with self.lock:
                self._inline.discard(op_id)

    def _transfer_thread(self, op_id: int, job: _Transfer) -> None:
        with self._transfer_slots:
            job.run()
        with self.lock:
            if op_id in self._inline:
                return
        self._finish(op_id, job.result, told=False)

    # ------------------------------------------------------------------ stop
    def stop(self, project: str | None, op_id: int, by: str = "agent") -> dict:
        op = self.op(op_id)
        if op is None or (project is not None and op["project"] != project):
            raise GatewayError("unknown_op", id=op_id)
        if op["state"] != "running":
            raise GatewayError("op_ended", id=op_id, kind=op["kind"])
        with self.lock:
            self.db.update("ops", {"id": op_id}, stop_by=by)
        told = by != "user"
        if op["kind"] == "transfer":
            job = self._transfers.get(op_id)
            if job is None:
                return {"status": "ended", "op": self._finish(
                    op_id, {"rc": None, "interrupted": True, "error": "", "elapsed": None},
                    told=told)}
            with self.lock:
                if told:
                    self._inline.add(op_id)
            job.stop.set()
            try:
                if job.done.wait(2 * KILL_GRACE_S + 5):
                    return {"status": "ended", "op": self._finish(op_id, job.result, told=told)}
            finally:
                with self.lock:
                    self._inline.discard(op_id)
            return {"status": "stopping", "op": self.op(op_id)}
        try:
            st = self._status(op, stop=True)
        except TransportError as e:
            raise GatewayError("unreachable", cluster=op["cluster"], error=str(e))
        if st["state"] == "ended":
            return {"status": "ended",
                    "op": self._finish(op_id, exec_result(st, op["log"]), told=told)}
        if st["state"] == "gone":
            return {"status": "ended", "op": self._finish(
                op_id, {"lost": True, "log": op["log"]}, told=told, state="lost")}
        return {"status": "stopping", "op": self.op(op_id)}
