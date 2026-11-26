"""Global multi-project daemon: shared cluster pool, polling, auth, SSE, browser launcher.

One daemon serves all registered projects and compute servers on 127.0.0.1.
Web authentication uses a purely in-memory secret + single-use 60s exchange codes
opened directly by the daemon in the user's browser so agents can open the Web UI
for non-technical users without ever seeing an approval credential.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import math
import os
import platform
import secrets
import subprocess
import threading
import time
import webbrowser

from . import attach, config, messages
from .approvals import ApprovalService
from .config import Cluster, ClusterPreset, GlobalConfig, Project
from .db import DB
from .executor import TransportError, make_executor
from .gateway import GatewayService
from .jobs import JobService
from .permissions import PermissionService
from .settings import SettingsService
from .submits import SubmitApprovals
from .waits import WaitService

log = logging.getLogger("hpcport.daemon")
SYNC_BACKOFF_MAX = 600.0
EXCHANGE_TTL_S = 60.0
AUTO_OPEN_COOLDOWN_S = 30.0
PIN_MAX_FAILURES = 5
PIN_LOCKOUT_S = 60.0


class Daemon:
    def __init__(self, executors: dict | None = None, clock=time.time,
                 browser_opener=None):
        self.now = clock
        self.global_cfg: GlobalConfig = config.load_global_config()
        self.db = DB(config.state_db_path())
        self.lock = threading.RLock()
        self.presets: dict[str, ClusterPreset] = {}
        self.projects: dict[str, Project] = {}
        self.ex: dict = dict(executors or {})
        self.make_executor = make_executor
        self._browser_opener = browser_opener   # testable override

        # In-memory Web auth secrets (never written to disk, never exposed via MCP)
        self._web_secret = secrets.token_urlsafe(32)
        self._exchange_codes: dict[str, tuple[float, str]] = {}   # code -> (expires, tab)
        self._pairing_pin = "%06d" % secrets.randbelow(1_000_000)
        self._pin_failures = 0
        self._pin_locked_until = 0.0
        self.cli_token: str | None = None

        # SSE subscribers (asyncio.Queue per connected Web UI tab)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._sse_subs: set[asyncio.Queue] = set()
        self._last_auto_open = 0.0

        # Services
        self.approvals = ApprovalService(self.db, clock)
        self.jobs = JobService(self.projects, self.presets, self.db, self.ex, clock, self.lock)
        self.gateway = GatewayService(self.projects, self.db, self.ex, self._notify,
                                      None, self.lock, clock)
        self.waits = WaitService(self.projects, self.db, self.jobs, self.gateway, self.lock, clock)
        self.permissions = PermissionService(
            self.db, self.approvals, self.waits.notify, self.lock,
            wait_s=180.0, describe=messages.permission_summary, clock=clock,
        )
        self.gateway.permissions = self.permissions
        self.submits = SubmitApprovals(
            self.projects, self.db, self.approvals, self.waits.notify,
            self.jobs, self.lock, describe=messages.submit_approval_summary, clock=clock,
        )
        self.jobs.wait_approval = self.submits.wait_decision
        self.settings = SettingsService(
            self.presets, self.projects, self.db, self.approvals,
            self.waits.notify, self.lock, clock=clock,
        )
        self.settings.on_preset_changed = self._on_preset_changed
        self.settings.on_project_changed = self._on_project_changed
        self.settings.on_change = lambda: self.broadcast("settings_changed", {})
        self.settings.start()

        # Build executors for loaded presets (unless injected by tests)
        for name, preset in self.presets.items():
            if name not in self.ex:
                self.ex[name] = self.make_executor(preset)

        # Wire live event notifications
        self.approvals.on_request = self._on_approval_requested
        self.approvals.on_change = lambda aid: self.broadcast("approvals_changed", {"id": aid})
        self.jobs.on_change = lambda proj=None: self.broadcast("jobs_changed", {"project": proj})
        self.gateway.on_change = lambda proj=None: self.broadcast("ops_changed", {"project": proj})
        self.gateway.recover()

        # Polling state
        self._last_sync: dict[str, float] = {}
        self._sync_failures: dict[str, int] = {}
        self._sync_errors: dict[str, str] = {}
        self._cycling = threading.Lock()

    def _notify(self, project: str, reason: str, subject: str, body: dict) -> int:
        return self.waits.notify(project, reason, subject, body)

    # ------------------------------------------------------------------ config callbacks
    def _on_preset_changed(self, name: str, old: ClusterPreset | None,
                           new: ClusterPreset | None) -> None:
        self.gateway.clear_facts(cluster=name)
        if new is None:
            self.ex.pop(name, None)
            self._sync_failures.pop(name, None)
            self._sync_errors.pop(name, None)
            return
        if (old is None and name not in self.ex) or (
            old is not None and (old.connect, old.host) != (new.connect, new.host)
        ):
            self.ex[name] = self.make_executor(new)
            self._sync_failures.pop(name, None)
            self._sync_errors.pop(name, None)

    def _on_project_changed(self, name: str, old: Project | None,
                            new: Project | None) -> None:
        self.gateway.clear_facts(project=name)
        if new is not None:
            self.submits.recheck(name)

    def update_global_config(self, patch: dict) -> GlobalConfig:
        cur = {
            "port": self.global_cfg.port,
            "auto_open_dashboard": self.global_cfg.auto_open_dashboard,
            "web_only_approval": self.global_cfg.web_only_approval,
            "language": self.global_cfg.language,
        }
        for k, v in patch.items():
            if v is not None:
                cur[k] = v
        new_cfg = config.parse_global_config(cur)
        config.save_global_config(new_cfg)
        self.global_cfg = new_cfg
        self.broadcast("settings_changed", {})
        return new_cfg

    # ------------------------------------------------------------------ cluster attach/test
    def test_cluster(self, name: str) -> dict:
        with self.lock:
            preset = self.presets.get(name)
            if preset is None:
                raise attach.AttachError("unknown cluster preset %s" % name)
            ex = self.ex.get(name) or self.make_executor(preset)
        return attach.test_connection(ex, name)

    def attach_cluster(self, project: str, cluster_name: str, root: str | None = None,
                       max_jobs: int | None = None) -> dict:
        with self.lock:
            p = self.projects.get(project)
            if p is None:
                raise attach.AttachError("unknown project %s" % project)
            preset = self.presets.get(cluster_name)
            cl, derived = attach.plan(p, cluster_name, root, max_jobs=max_jobs, preset=preset)
            ex = self.ex.get(cluster_name) or self.make_executor(cl)
        real = attach.make_root(ex, cl, derived)
        with self.lock:
            version = attach.record(p, cl, max_jobs=max_jobs)
            self.ex[cluster_name] = ex
            self.jobs.note_root(project, cluster_name, real)
            self.settings.attached(project, cluster_name, cl, max_jobs, version)
            self.waits.notify(project, "cluster_attached", "cluster:" + cluster_name,
                              {"cluster": cluster_name, "root": cl.root,
                               "scheduler": cl.scheduler})
        self.broadcast("settings_changed", {"project": project})
        return {"project": project, "cluster": cluster_name, "root": cl.root, "resolved": real}

    def detach_cluster(self, project: str, cluster_name: str) -> dict:
        with self.lock:
            p = self.projects.get(project)
            if p is None:
                raise attach.AttachError("unknown project %s" % project)
            if cluster_name not in p.clusters:
                raise attach.AttachError("cluster %s is not attached to project %s"
                                         % (cluster_name, project))
            active_jobs = self.db.one(
                "SELECT 1 FROM jobs WHERE project = ? AND cluster = ? "
                "AND state IN ('unapproved', 'held', 'queued', 'running') LIMIT 1",
                (project, cluster_name),
            )
            active_ops = self.db.one(
                "SELECT 1 FROM ops WHERE project = ? AND state = 'running' "
                "AND instr(',' || cluster || ',', ?) > 0 LIMIT 1",
                (project, "," + cluster_name + ","),
            )
            if active_jobs or active_ops:
                from .settings import SettingError
                raise SettingError("cluster_busy", cluster=cluster_name, project=project)
            res = self.settings.update_project(
                project, {"clusters": {cluster_name: None}},
                note="detached cluster %s" % cluster_name,
            )
            return {"project": project, "cluster": cluster_name, "version": res["version"]}

    # ------------------------------------------------------------------ Web auth & dashboard launch
    @property
    def pairing_pin(self) -> str:
        return self._pairing_pin

    def _cookie_value(self) -> str:
        sig = hmac.new(self._web_secret.encode(), b"hpcport-web-session", hashlib.sha256).hexdigest()
        return sig

    def verify_cookie(self, cookie: str | None) -> bool:
        return bool(cookie) and hmac.compare_digest(cookie, self._cookie_value())

    def verify_pin(self, pin: str) -> str | None:
        if pin and hmac.compare_digest(str(pin).strip(), self._pairing_pin):
            return self._cookie_value()
        return None

    def attempt_pin(self, pin: str) -> tuple[str | None, int]:
        """Verify pairing PIN with failure cooldown; returns (cookie_or_None, retry_after_s)."""
        now = self.now()
        with self.lock:
            if now < self._pin_locked_until:
                return None, max(1, int(math.ceil(self._pin_locked_until - now)))
            if pin and hmac.compare_digest(str(pin).strip(), self._pairing_pin):
                self._pin_failures = 0
                self._pin_locked_until = 0.0
                return self._cookie_value(), 0
            self._pin_failures += 1
            if self._pin_failures >= PIN_MAX_FAILURES:
                self._pin_failures = 0
                self._pin_locked_until = now + PIN_LOCKOUT_S
                return None, int(PIN_LOCKOUT_S)
            return None, 0

    def issue_exchange_code(self, tab: str = "approvals") -> str:
        code = secrets.token_urlsafe(24)
        now = self.now()
        with self.lock:
            # Purge expired codes
            for k, (exp, _) in list(self._exchange_codes.items()):
                if exp < now:
                    del self._exchange_codes[k]
            self._exchange_codes[code] = (now + EXCHANGE_TTL_S, tab)
        return code

    def redeem_exchange_code(self, code: str) -> tuple[str, str] | None:
        now = self.now()
        with self.lock:
            entry = self._exchange_codes.pop(code, None)
        if entry is None or entry[0] < now:
            return None
        return self._cookie_value(), entry[1]

    def dashboard_url(self, tab: str = "") -> str:
        base = "http://127.0.0.1:%d/" % self.global_cfg.port
        return base + ("#" + tab if tab else "")

    def open_dashboard(self, tab: str = "approvals") -> dict:
        """Open an authenticated Web UI tab directly from the daemon process.
        Never returns the one-time exchange code to the caller."""
        tab = tab if tab in ("approvals", "jobs", "audit", "settings") else "approvals"
        code = self.issue_exchange_code(tab)
        url = "http://127.0.0.1:%d/auth/exchange?code=%s&tab=%s" % (
            self.global_cfg.port, code, tab
        )
        opened = self._launch_browser(url)
        return {"opened": opened, "url": self.dashboard_url(tab), "tab": tab}

    def _launch_browser(self, url: str) -> bool:
        if self._browser_opener is not None:
            return bool(self._browser_opener(url))
        # Avoid launching browser on headless Linux SSH sessions without DISPLAY
        if platform.system() == "Linux" and not os.environ.get("DISPLAY") and not os.environ.get("WAYLAND_DISPLAY"):
            return False
        try:
            if platform.system() == "Darwin":
                p = subprocess.run(["open", url], capture_output=True, timeout=5)
                return p.returncode == 0
            return bool(webbrowser.open(url))
        except Exception as e:
            log.warning("could not open browser: %s", e)
            return False

    def _on_approval_requested(self, aid: int, project: str | None, kind: str) -> None:
        if not self.global_cfg.auto_open_dashboard:
            return
        if len(self._sse_subs) > 0:
            return
        now = self.now()
        if now - self._last_auto_open < AUTO_OPEN_COOLDOWN_S:
            return
        self._last_auto_open = now
        threading.Thread(target=self.open_dashboard, args=("approvals",),
                         daemon=True, name="auto-open").start()

    # ------------------------------------------------------------------ SSE hub
    def subscribe_sse(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=64)
        self._sse_subs.add(q)
        return q

    def unsubscribe_sse(self, q: asyncio.Queue) -> None:
        self._sse_subs.discard(q)

    def broadcast(self, event: str, data: dict) -> None:
        loop = self._loop
        if loop is None or not loop.is_running() or not self._sse_subs:
            return
        msg = (event, data)

        def _push():
            for q in list(self._sse_subs):
                try:
                    q.put_nowait(msg)
                except asyncio.QueueFull:
                    pass

        loop.call_soon_threadsafe(_push)

    # ------------------------------------------------------------------ polling cycle
    def _sync_interval(self, name: str) -> float:
        preset = self.presets.get(name)
        interval = float(preset.poll_interval_s if preset else 120)
        failures = self._sync_failures.get(name, 0)
        return interval if not failures else max(
            interval, min(SYNC_BACKOFF_MAX, interval * 2 ** min(failures, 10))
        )

    def unreachable(self) -> set[str]:
        with self.lock:
            return set(self._sync_failures)

    def cluster_health(self) -> dict[str, dict]:
        with self.lock:
            return {
                name: {
                    "scheduler": p.scheduler,
                    "connect": p.connect,
                    "host": p.host,
                    "reachable": name not in self._sync_failures,
                    "failures": self._sync_failures.get(name, 0),
                    "error": self._sync_errors.get(name),
                    "last_sync": self._last_sync.get(name),
                }
                for name, p in self.presets.items()
            }

    def dispatch_soon(self) -> None:
        threading.Thread(target=self.jobs.dispatch_wanted, args=(self.unreachable(),),
                         daemon=True, name="dispatch").start()

    def cycle(self, force: bool = False) -> None:
        """Scan settings, sync each due Slurm cluster once, dispatch held jobs, evaluate waits."""
        with self._cycling:
            with self.lock:
                self.settings.scan()
                attached_slurm = {
                    cname for p in self.projects.values()
                    for cname, cl in p.clusters.items() if cl.scheduler == "slurm"
                }
                due = [
                    name for name in sorted(attached_slurm)
                    if force or (self.now() - self._last_sync.get(name, 0)
                                 >= self._sync_interval(name))
                ]
                for name in due:
                    self._last_sync[name] = self.now()

            changed: set[tuple[str, str]] = set()
            for name in due:
                try:
                    changed |= self.jobs.sync(name)
                    ok, error = True, None
                except TransportError as e:
                    ok, error = False, str(e)
                with self.lock:
                    if ok:
                        self._sync_failures.pop(name, None)
                        self._sync_errors.pop(name, None)
                    else:
                        self._sync_failures[name] = self._sync_failures.get(name, 0) + 1
                        self._sync_errors[name] = error
                        log.warning("cluster %s unreachable; retry in %.0fs: %s",
                                    name, self._sync_interval(name), error)

            self.jobs.dispatch_wanted(skip=self.unreachable())

            with self.lock:
                self.submits.sweep()
                self.submits.recheck()
                # Emit batch_done notices for newly finished batches
                for proj, batch in changed:
                    brow = self.db.one(
                        "SELECT done_notified FROM batches WHERE project = ? AND batch = ?",
                        (proj, batch),
                    )
                    if not brow or brow["done_notified"]:
                        continue
                    s = self.jobs.summary(proj, batch)
                    if s and s["finished"]:
                        self.db.update("batches", {"project": proj, "batch": batch},
                                       done_notified=1)
                        self.waits.notify(
                            proj, "batch_done", "batch:" + batch,
                            {"batch": batch, "cluster": s["cluster"], "jobs": s["jobs"],
                             "n_jobs": s["n_jobs"], "fail_rate": s["fail_rate"]},
                        )
                self.waits.evaluate(render_result=lambda b: messages.wait_fired(b, self))
                self.waits.purge_expired_tasks()
            self.jobs.dispatch_wanted(skip=self.unreachable())

    async def run(self, tick: float = 5.0) -> None:
        self._loop = asyncio.get_running_loop()
        while True:
            await asyncio.to_thread(self.cycle)
            await asyncio.to_thread(self.gateway.poll)
            with self.lock:
                self.waits.evaluate(render_result=lambda b: messages.wait_fired(b, self))
            await asyncio.sleep(tick)
