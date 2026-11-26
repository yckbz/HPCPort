"""Unified multi-project approval queue.

Services enqueue approvals under a `kind` and register handlers and accepted
options. Decisions are stored in SQLite; listeners (SSE hub, auto-open browser)
are notified via `on_request` and `on_change`.
"""

from __future__ import annotations

import json
import time


class ApprovalError(Exception):
    def __init__(self, code: str, **params):
        super().__init__(code)
        self.code = code
        self.params = params


class ApprovalService:
    def __init__(self, db, clock=time.time):
        self.db = db
        self.now = clock
        self.handlers: dict = {}
        self.options: dict[str, tuple[str, ...]] = {}
        self.on_request = lambda aid, project, kind: None
        self.on_change = lambda aid: None

    def request(self, project: str | None, kind: str, summary: str, detail: dict) -> int:
        if kind not in self.handlers:
            raise KeyError("no handler for approval kind %r" % kind)
        aid = self.db.insert(
            "approvals",
            project=project,
            kind=kind,
            summary=summary,
            detail=json.dumps(detail, ensure_ascii=False),
            state="pending",
            created=self.now(),
        )
        self.on_request(aid, project, kind)
        self.on_change(aid)
        return aid

    def get(self, aid: int) -> dict | None:
        row = self.db.one("SELECT * FROM approvals WHERE id = ?", (aid,))
        if row:
            row["detail"] = json.loads(row["detail"])
        return row

    def list(self, pending_only: bool = True, project: str | None = None) -> list[dict]:
        conds, args = [], []
        if pending_only:
            conds.append("state = 'pending'")
        if project is not None:
            conds.append("(project = ? OR project IS NULL)")
            args.append(project)
        where = "WHERE " + " AND ".join(conds) if conds else ""
        rows = self.db.all(
            "SELECT id, project, kind, summary, detail, state, comment, created, decided "
            "FROM approvals %s ORDER BY id DESC" % where,
            tuple(args),
        )
        for r in rows:
            r["detail"] = json.loads(r["detail"])
        return rows if not pending_only else rows[::-1]

    def pending_count(self, project: str | None = None) -> int:
        if project is not None:
            r = self.db.one(
                "SELECT COUNT(*) AS n FROM approvals WHERE state = 'pending' "
                "AND (project = ? OR project IS NULL)", (project,))
        else:
            r = self.db.one("SELECT COUNT(*) AS n FROM approvals WHERE state = 'pending'")
        return r["n"] if r else 0

    def decide(self, aid: int, approved: bool, comment: str = "", **options) -> dict:
        row = self.get(aid)
        if row is None:
            raise ApprovalError("unknown_approval", id=aid)
        if row["state"] != "pending":
            raise ApprovalError("already_decided", id=aid, state=row["state"])
        extra = sorted(set(options) - set(self.options.get(row["kind"], ())))
        if extra:
            raise ApprovalError(
                "bad_option",
                id=aid,
                msg="approval %d (%s) does not accept: %s" % (aid, row["kind"], ", ".join(extra)),
            )
        result = self.handlers[row["kind"]](row, approved, comment.strip(), options)
        state = "approved" if approved else "rejected"
        now = self.now()
        self.db.update("approvals", {"id": aid}, state=state, comment=comment.strip(), decided=now)
        self.on_change(aid)
        return dict(result or {}, id=aid, state=state)

    def revise(self, aid: int, summary: str, detail: dict) -> None:
        self.db.run(
            "UPDATE approvals SET summary = ?, detail = ? WHERE id = ? AND state = 'pending'",
            (summary, json.dumps(detail, ensure_ascii=False), aid),
        )
        self.on_change(aid)

    def cancel(self, aid: int, reason: str) -> None:
        row = self.get(aid)
        if row is None or row["state"] != "pending":
            return
        self.db.update("approvals", {"id": aid}, state="cancelled", comment=reason,
                       decided=self.now())
        self.on_change(aid)
