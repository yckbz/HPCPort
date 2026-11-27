"""Per-project job submission approvals, standing budgets, dynamic recheck, and short wait.

Each project has its own standing approval (stored in meta under
`standing:<project>`) with single-submission and cumulative limits on jobs,
core-hours, and GPU-hours. Usage counts from the moment the budget is granted;
adjusting the limits later (from the Web budget editor or while approving a
submission) keeps that starting point, so usage is never silently reset. Every
grant, adjustment, and revocation is recorded in `standing_events`.

When a submission needs approval, gate() registers an approval and arms an
in-memory waiter so submit() can wait up to `submit.wait_s` (default 180s) for
an instant human click in the Web UI; if approved in time, jobs are dispatched
immediately within the same submit() call. Otherwise they remain `unapproved`
until approved later or auto-released by recheck().
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time

from .approvals import ApprovalError
from .config import SUBMIT_LIMITS

log = logging.getLogger("hpcport.submits")
APPROVAL_KIND = "submit"
RELEASED = "released without a decision: the reasons it waited for no longer hold"
LIMITS = {"core_hours": "core_hours", "gpu_hours": "gpu_hours"}
TOTALS = {"total_jobs": "jobs", "total_core_hours": "core_hours", "total_gpu_hours": "gpu_hours"}
TICK_S = 15.0
HISTORY_N = 20
assert set(LIMITS) | set(TOTALS) == set(SUBMIT_LIMITS)


def _limit_ok(key: str, v) -> bool:
    if v is None:
        return True
    if isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0:
        return False
    if isinstance(v, float) and not math.isfinite(v):
        return False
    return key != "total_jobs" or isinstance(v, int)


def _readable(limits: dict | None) -> dict | None:
    """Read stored limits written before non-finite values were refused. Infinity and NaN
    never blocked a submission, so they mean "unlimited"."""
    if limits is None:
        return None
    return {k: None if isinstance(v, float) and not math.isfinite(v) else v
            for k, v in limits.items()}


class SubmitApprovals:
    def __init__(self, projects: dict, db, approvals, notify, jobs, lock,
                 describe=None, clock=time.time, on_change=None):
        self.projects = projects        # project_name -> Project
        self.db = db
        self.approvals = approvals
        self.notify = notify
        self.jobs = jobs
        self.lock = lock
        self.describe = describe or (lambda proj, sub, why: "submit %s" % sub["batch"])
        self.now = clock
        self.on_change = on_change or (lambda project: None)
        self.conditions: list = []
        self._cond = threading.Condition()
        self._waiting: dict[int, dict | None] = {}
        approvals.handlers[APPROVAL_KIND] = self.on_decision
        approvals.options[APPROVAL_KIND] = ("always",) + SUBMIT_LIMITS
        jobs.gate = self.gate
        jobs.abort_approval = self.abort_approval

    @staticmethod
    def _meta_key(project: str) -> str:
        return "standing:" + project

    def standing(self, project: str) -> dict | None:
        raw = self.db.get_meta(self._meta_key(project))
        if not raw:
            return None
        st = json.loads(raw)
        st["limits"] = _readable(st["limits"])
        return st

    def used(self, project: str, standing: dict, apart: tuple[int, ...] = ()) -> dict:
        return self.jobs.used_after(
            project,
            standing["after_job"],
            apart,
            tuple(standing.get("waiting", ())),
        )

    def _pending_ids(self, project: str, but: int | None = None) -> list[int]:
        return [a["id"] for a in self.approvals.list(pending_only=True, project=project)
                if a["kind"] == APPROVAL_KIND and a["project"] == project and a["id"] != but]

    def status(self, project: str, history: bool = False) -> dict:
        p = self.projects.get(project)
        st = self.standing(project)
        out = {
            "project": project,
            "approval": p.submit.approval if p else True,
            "defaults": {k: getattr(p.submit, k) for k in SUBMIT_LIMITS} if p else {},
            "standing": st,
            "used": self.used(project, st) if st else None,
            "pending": len(self._pending_ids(project)),
        }
        if history:
            out["history"] = self.history(project)
        return out

    def history(self, project: str, n: int = HISTORY_N) -> list[dict]:
        rows = self.db.all(
            "SELECT id, ts, action, limits, previous, used, approval, origin "
            "FROM standing_events WHERE project = ? ORDER BY id DESC LIMIT ?",
            (project, n),
        )
        for r in rows:
            for k in ("limits", "previous", "used"):
                r[k] = json.loads(r[k]) if r[k] else None
            r["limits"], r["previous"] = _readable(r["limits"]), _readable(r["previous"])
        return rows

    def _record(self, project: str, action: str, limits: dict | None, previous: dict | None,
                used: dict | None, origin: str, approval: int | None = None,
                notify: bool = True) -> None:
        totals = {k: used[k] for k in ("jobs", "core_hours", "gpu_hours")} if used else None
        self.db.insert(
            "standing_events",
            project=project,
            ts=self.now(),
            action=action,
            limits=json.dumps(limits) if limits is not None else None,
            previous=json.dumps(previous) if previous is not None else None,
            used=json.dumps(totals) if totals is not None else None,
            approval=approval,
            origin=origin,
        )
        if notify:
            self.notify(project, "standing_changed", "standing",
                        {"action": action, "limits": limits})

    @staticmethod
    def _check_limits(limits, complete: bool) -> dict:
        if not isinstance(limits, dict):
            raise ApprovalError("bad_limits", names=", ".join(SUBMIT_LIMITS))
        keys = set(limits)
        if keys - set(SUBMIT_LIMITS) or (complete and keys != set(SUBMIT_LIMITS)):
            raise ApprovalError("bad_limits", names=", ".join(SUBMIT_LIMITS))
        for k, v in limits.items():
            if not _limit_ok(k, v):
                raise ApprovalError("bad_limit", name=k)
        return dict(limits)

    def _grant(self, project: str, limits: dict, origin: str,
               approval: int | None = None) -> dict:
        st = {
            "approval": approval,
            "created": self.now(),
            "after_job": self.jobs.last_job_id(project),
            "waiting": self._pending_ids(project, but=approval),
            "limits": limits,
        }
        # Lock order is always state lock, then database: notify() takes the state lock.
        with self.lock, self.db.transaction():
            self.db.set_meta(self._meta_key(project), json.dumps(st))
            self._record(project, "granted", limits, None, self.used(project, st), origin,
                         approval)
        self.on_change(project)
        return st

    def _adjust(self, project: str, st: dict, limits: dict, origin: str,
                approval: int | None = None) -> bool:
        previous = st["limits"]
        new = dict(previous, **limits)
        if new == previous:
            return False
        st = dict(st, limits=new)
        with self.lock, self.db.transaction():
            self.db.set_meta(self._meta_key(project), json.dumps(st))
            self._record(project, "adjusted", new, previous, self.used(project, st), origin,
                         approval)
        self.on_change(project)
        return True

    def set_standing(self, project: str, mode: str, limits, origin: str) -> dict:
        """Grant a new standing budget or adjust the limits of the current one.

        `mode` states what the user saw: "grant" fails if a budget appeared meanwhile and
        "adjust" fails if it was revoked, so a stale editor never overwrites a change."""
        if project not in self.projects:
            raise ApprovalError("unknown_project", project=project)
        limits = self._check_limits(limits, complete=True)
        st = self.standing(project)
        if mode == "grant":
            if st is not None:
                raise ApprovalError("standing_exists", project=project)
            self._grant(project, limits, origin)
            changed = True
        elif mode == "adjust":
            if st is None:
                raise ApprovalError("standing_missing", project=project)
            changed = self._adjust(project, st, limits, origin)
        else:
            raise ApprovalError("bad_option", msg="mode must be 'grant' or 'adjust'")
        released = [b for _, b in self.recheck(project)] if changed else []
        return dict(self.status(project, history=True), changed=changed, released=released)

    def revoke(self, project: str, origin: str = "web", notify: bool = True) -> bool:
        st = self.standing(project)
        if st is None:
            return False
        used = self.used(project, st)
        with self.lock, self.db.transaction():
            self.db.delete_meta(self._meta_key(project))
            self._record(project, "revoked", None, st["limits"], used, origin, notify=notify)
        self.on_change(project)
        self.recheck(project)
        return True

    # ------------------------------------------------------------------ gate
    def reasons(self, project: str, sub: dict, apart: tuple[int, ...] = ()) -> list[dict]:
        st = self.standing(project)
        why = []
        if st is None:
            why.append({"code": "no_standing"})
        if sub["unknown"]:
            why.append({"code": "unknown", "scripts": [f for f, _ in sub["unknown"]]})
        if st is None:
            return why
        limits = st["limits"]
        for key, figure in LIMITS.items():
            asked = sub[figure]
            if limits[key] is not None and asked is not None and asked > limits[key]:
                why.append({"code": "over", "what": figure, "asked": asked, "limit": limits[key]})
        return why + self._totals(st, sub, self.used(project, st, apart))

    @staticmethod
    def _totals(st: dict, sub: dict, used: dict) -> list[dict]:
        why = []
        for key, figure in TOTALS.items():
            asked = sub["jobs"] if figure == "jobs" else sub[figure]
            limit = st["limits"][key]
            if limit is not None and used[figure] + (asked or 0) > limit:
                why.append({"code": "total", "what": figure,
                            "reached": used[figure] + (asked or 0), "limit": limit})
        return why

    def gate(self, project: str, sub: dict) -> int | None:
        p = self.projects[project]
        sub["conditions"] = [r for cond in self.conditions for r in cond(project, sub)]
        why = (self.reasons(project, sub) if p.submit.approval else []) + sub["conditions"]
        if not why:
            return None
        aid = self.approvals.request(
            project,
            APPROVAL_KIND,
            self.describe(project, sub, why),
            {"submission": sub, "why": why},
        )
        with self._cond:
            self._waiting[aid] = None
        return aid

    def wait_decision(self, aid: int, wait_s: float, on_tick=None) -> dict | None:
        """Wait outside the state lock up to wait_s seconds for a decision on aid.
        Returns {"approved": bool, "comment": str, "auto_released": bool} or None on timeout."""
        limit = max(0.0, min(float(wait_s), 180.0))
        if limit <= 0:
            with self._cond:
                return self._waiting.pop(aid, None)
        t0 = time.monotonic()
        deadline = t0 + limit
        while True:
            with self._cond:
                if aid not in self._waiting or self._waiting[aid] is not None:
                    return self._waiting.pop(aid, None)
                left = deadline - time.monotonic()
                if left <= 0:
                    return self._waiting.pop(aid, None)
                self._cond.wait(min(left, TICK_S))
                if aid not in self._waiting or self._waiting[aid] is not None:
                    return self._waiting.pop(aid, None)
            if on_tick is not None and time.monotonic() < deadline:
                try:
                    on_tick(time.monotonic() - t0, limit,
                            "Waiting for job submission approval #%d on the HPCPort dashboard"
                            % aid)
                except Exception:
                    pass

    def drop_waiter(self, aid: int, reason: str = "") -> None:
        with self._cond:
            if aid in self._waiting:
                self._waiting[aid] = {
                    "approved": False,
                    "cancelled": True,
                    "comment": reason,
                    "auto_released": False,
                }
                self._cond.notify_all()

    def abort_approval(self, aid: int, reason: str) -> None:
        self.approvals.cancel(aid, reason)
        with self._cond:
            self._waiting.pop(aid, None)
            self._cond.notify_all()

    # ------------------------------------------------------------------ recheck
    def recheck(self, project: str | None = None) -> list[tuple[str, str]]:
        """Recompute reasons for waiting submissions; auto-release any with no reasons left."""
        waiting = [row for row in self.approvals.list(pending_only=True, project=project)
                   if row["kind"] == APPROVAL_KIND]
        # Group per project to respect submission order within each project
        by_proj: dict[str, list[dict]] = {}
        for row in waiting:
            if row["project"]:
                by_proj.setdefault(row["project"], []).append(row)

        released: list[tuple[str, str]] = []
        for pname, rows in by_proj.items():
            p = self.projects.get(pname)
            if p is None:
                continue
            for n, row in enumerate(rows):
                aid = row["id"]
                detail = row["detail"]
                sub, why = detail["submission"], detail["why"]
                if not self.jobs.unapproved(aid):
                    continue
                sub["conditions"] = [r for cond in self.conditions for r in cond(pname, sub)]
                now = list(sub["conditions"])
                if p.submit.approval:
                    apart = tuple(r["id"] for r in rows[n:])
                    now = self.reasons(pname, sub, apart) + now
                if not now:
                    batches = self.jobs.release(aid, want=False)
                    released += [(pname, b) for b in batches]
                    self.approvals.cancel(aid, RELEASED)
                    inline = self._wake_waiter(aid, approved=True, comment=RELEASED,
                                               auto_released=True)
                    if not inline:
                        self.jobs.want_approval(aid)
                        self.notify(
                            pname,
                            "submit_approved",
                            "approval:%d" % aid,
                            {"approval": aid, "batches": batches, "comment": RELEASED,
                             "standing": False, "auto_released": True},
                        )
                    log.info("submission approval %d (%s) auto-released", aid, pname)
                    continue
                summary = self.describe(pname, sub, now)
                if now != why or summary != row["summary"]:
                    self.approvals.revise(aid, summary, {"submission": sub, "why": now})
        return sorted(set(released))

    # ------------------------------------------------------------------ decision
    def on_decision(self, approval: dict, approved: bool, comment: str, options: dict) -> dict:
        aid = approval["id"]
        project = approval["project"]
        limits = self._check_limits({k: options[k] for k in SUBMIT_LIMITS if k in options},
                                    complete=False)
        always = bool(options.get("always"))
        if limits and not (approved and always):
            raise ApprovalError("limits_without_always", names=", ".join(limits))

        if not approved:
            batches = self.jobs.refuse(aid, comment=comment)
            inline = self._wake_waiter(aid, approved=False, comment=comment)
            if not inline and project:
                self.notify(
                    project,
                    "submit_refused",
                    "approval:%d" % aid,
                    {"approval": aid, "batches": batches, "comment": comment},
                )
            return {"batches": batches, "jobs": "cancelled"}

        result: dict = {"batches": None, "jobs": "released"}
        if always and project:
            st = self.standing(project)
            if st is None:
                # A new budget counts from here on: this submission is approved on its own.
                p = self.projects.get(project)
                defaults = {k: getattr(p.submit, k) for k in SUBMIT_LIMITS} if p else {}
                st = self._grant(project, dict(defaults, **limits), "approval", aid)
            else:
                # An existing budget keeps its usage; only the limits change.
                self._adjust(project, st, limits, "approval", aid)
                st = self.standing(project)
            result["standing"] = st["limits"]

        batches = self.jobs.release(aid, want=False)
        result["batches"] = batches
        inline = self._wake_waiter(aid, approved=True, comment=comment)
        if not inline:
            self.jobs.want_approval(aid)
            if project:
                self.notify(
                    project,
                    "submit_approved",
                    "approval:%d" % aid,
                    {"approval": aid, "batches": batches, "comment": comment,
                     "standing": bool(always)},
                )
        if always and project:
            result["also_released"] = [b for _, b in self.recheck(project)]
        return result

    def _wake_waiter(self, aid: int, approved: bool, comment: str,
                     auto_released: bool = False) -> bool:
        with self._cond:
            if aid in self._waiting:
                self._waiting[aid] = {"approved": approved, "comment": comment,
                                      "auto_released": auto_released}
                self._cond.notify_all()
                return True
        return False

    def sweep(self) -> None:
        for a in self.approvals.list(pending_only=True):
            if a["kind"] == APPROVAL_KIND and not self.jobs.unapproved(a["id"]):
                note = self.jobs.cancel_note_for_approval(a["id"])
                self.approvals.cancel(a["id"], "its jobs no longer wait for approval")
                self.drop_waiter(a["id"], reason=note)
