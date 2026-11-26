"""Blocking-wait and one-time grant bridge for gateway command/transfer approvals.

When remote_exec or transfer triggers a guard rule or size threshold, ask()
enqueues a `permission` approval and waits up to wait_s (default 180s) for a
decision on the Web UI or CLI:
  - decided in time: returns allow/deny straight back so the tool call continues;
  - timed out: returns pending=True with approval ID; when approved later, a
    single-use SHA256-matched grant is recorded and a notice is queued.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time

APPROVAL_KIND = "permission"
DEFAULT_WAIT_S = 180.0
TICK_S = 15.0


def digest(tool: str, tool_input) -> str:
    return hashlib.sha256(
        json.dumps([tool, tool_input], sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()


class PermissionService:
    def __init__(self, db, approvals, notify, lock, wait_s: float = DEFAULT_WAIT_S,
                 describe=None, clock=time.time):
        self.db = db
        self.approvals = approvals
        self.notify = notify            # fn(project, reason, subject, body)
        self.lock = lock
        self.wait_s = wait_s
        self.describe = describe or (lambda proj, tool, inp, why: "%s (%s)" % (tool, proj))
        self.now = clock
        self._cond = threading.Condition()
        self._waiting: dict[int, dict | None] = {}
        approvals.handlers[APPROVAL_KIND] = self.on_decision
        approvals.options[APPROVAL_KIND] = ()

    def ask(self, project: str, tool: str, tool_input, why=(),
            wait_s: float | None = None, on_tick=None) -> dict:
        """Ask for human approval outside the state lock."""
        sha = digest(tool, tool_input)
        limit = self.wait_s if wait_s is None else max(0.0, min(float(wait_s), 240.0))
        with self.lock:
            g = self.db.one(
                "SELECT id, approval FROM permission_grants WHERE project = ? "
                "AND tool = ? AND input_sha256 = ? AND used IS NULL ORDER BY id LIMIT 1",
                (project, tool, sha),
            )
            if g:
                self.db.update("permission_grants", {"id": g["id"]}, used=self.now())
                return {"allow": True, "approval": g["approval"], "pending": False, "comment": ""}
            aid = self.approvals.request(
                project,
                APPROVAL_KIND,
                self.describe(project, tool, tool_input, why),
                {"tool": tool, "input": tool_input, "sha256": sha, "why": list(why)},
            )
            with self._cond:
                self._waiting[aid] = None

        t0 = time.monotonic()
        deadline = t0 + limit
        decision = None
        while True:
            with self._cond:
                val = self._waiting.get(aid)
                if val is not None:
                    decision = self._waiting.pop(aid, None)
                    break
                left = deadline - time.monotonic()
                if left <= 0:
                    decision = self._waiting.pop(aid, None)
                    break
                self._cond.wait(min(left, TICK_S))
                val = self._waiting.get(aid)
                if val is not None:
                    decision = self._waiting.pop(aid, None)
                    break
            if on_tick is not None and time.monotonic() < deadline:
                try:
                    on_tick(time.monotonic() - t0, limit,
                            "Waiting for approval #%d on the HPCPort dashboard" % aid)
                except Exception:
                    pass

        if decision is None:
            return {"allow": False, "approval": aid, "pending": True, "comment": ""}
        return {"allow": decision["approved"], "approval": aid, "pending": False,
                "comment": decision["comment"]}

    def on_decision(self, approval: dict, approved: bool, comment: str, options: dict) -> dict:
        aid = approval["id"]
        with self._cond:
            if aid in self._waiting:
                self._waiting[aid] = {"approved": approved, "comment": comment}
                self._cond.notify_all()
                return {"delivered": "inline"}
        project = approval["project"]
        if approved and project:
            self.db.insert(
                "permission_grants",
                project=project,
                tool=approval["detail"]["tool"],
                input_sha256=approval["detail"]["sha256"],
                approval=aid,
                created=self.now(),
            )
        if project:
            self.notify(
                project,
                "permission",
                "approval:%d" % aid,
                {"approval": aid, "approved": approved, "tool": approval["detail"]["tool"],
                 "comment": comment},
            )
        return {"delivered": "notice"}
