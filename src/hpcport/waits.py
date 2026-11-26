"""Unified wait engine, persistent notices inbox, and MCP Task backing store.

Supports waiting on both Slurm batches ("b001", "sweep-v1") and background
gateway operations ("op:12") with priority groups:
  2  trigger as soon as any item in the group ends
  1  trigger when every item in the group ends
  0  never trigger alone; included in the result when another condition fires

Also triggers on batch failure rate > fail_frac (once per batch) and timeout_h.

For MCP clients supporting `io.modelcontextprotocol/tasks`, each wait backs a
durable row in `mcp_tasks`. For other clients, wait_sync() short-waits up to
180s (with progress heartbeats) and falls back to background monitoring plus
automatic notice delivery on subsequent tool calls.
"""

from __future__ import annotations

import json
import secrets
import threading
import time

PRIORITIES = (0, 1, 2)
TICK_S = 15.0
DEFAULT_POLL_INTERVAL_MS = 15000
DEFAULT_TTL_MS = 7 * 24 * 3600 * 1000


class WaitError(Exception):
    def __init__(self, code: str, **params):
        super().__init__(code)
        self.code = code
        self.params = params


def parse_op_item(item: str) -> int | None:
    if isinstance(item, str) and item.startswith("op:") and item[3:].isdigit():
        return int(item[3:])
    return None


def wait_items(groups: list[dict]) -> list[str]:
    return [i for g in groups for i in g["items"]]


def _due(groups: list[dict], ended: set[str]) -> bool:
    return any(
        (g["priority"] == 2 and any(i in ended for i in g["items"]))
        or (g["priority"] == 1 and all(i in ended for i in g["items"]))
        for g in groups
    )


class WaitService:
    def __init__(self, projects: dict, db, jobs, gateway, lock, clock=time.time):
        self.projects = projects
        self.db = db
        self.jobs = jobs
        self.gateway = gateway
        self.lock = lock
        self.now = clock
        self._cond = threading.Condition()
        self._sync_waiters: dict[int, dict | None] = {}
        self.on_change = lambda project=None: None

    # ------------------------------------------------------------------ notices
    def notify(self, project: str, reason: str, subject: str, body: dict) -> int:
        """Record an unread notice for a project (called under or outside lock)."""
        with self.lock:
            nid = self.db.insert(
                "notices",
                project=project,
                reason=reason,
                subject=subject or "",
                body=json.dumps(body, ensure_ascii=False),
                created=self.now(),
            )
        return nid

    def pop_notices(self, project: str, session_key: str) -> list[dict]:
        """Return deduplicated unread notices since this session's cursor and advance it."""
        with self.lock:
            cur = self.db.one(
                "SELECT last_notice_id FROM session_cursors WHERE session_key = ? AND project = ?",
                (session_key, project),
            )
            last_id = cur["last_notice_id"] if cur else 0
            rows = self.db.all(
                "SELECT id, reason, subject, body, created FROM notices "
                "WHERE project = ? AND id > ? ORDER BY id",
                (project, last_id),
            )
            if not rows:
                if cur is None:
                    # First contact of a brand-new session: initialize cursor at current head
                    head = self.db.one(
                        "SELECT COALESCE(MAX(id), 0) AS m FROM notices WHERE project = ?",
                        (project,),
                    )["m"]
                    self.db.run(
                        "INSERT INTO session_cursors (session_key, project, last_notice_id, updated) "
                        "VALUES (?, ?, ?, ?) ON CONFLICT(session_key, project) DO NOTHING",
                        (session_key, project, head, self.now()),
                    )
                return []
            newest = rows[-1]["id"]
            self.db.run(
                "INSERT INTO session_cursors (session_key, project, last_notice_id, updated) "
                "VALUES (?, ?, ?, ?) ON CONFLICT(session_key, project) DO UPDATE SET "
                "last_notice_id = excluded.last_notice_id, updated = excluded.updated",
                (session_key, project, newest, self.now()),
            )
        # Keep only the newest notice per (reason, subject)
        dedup: dict[tuple[str, str], dict] = {}
        for r in rows:
            item = {
                "id": r["id"],
                "reason": r["reason"],
                "subject": r["subject"],
                "body": json.loads(r["body"]),
                "created": r["created"],
            }
            dedup[(r["reason"], r["subject"])] = item
        return sorted(dedup.values(), key=lambda x: x["id"])

    def init_cursor(self, project: str, session_key: str) -> None:
        with self.lock:
            cur = self.db.one(
                "SELECT 1 FROM session_cursors WHERE session_key = ? AND project = ?",
                (session_key, project),
            )
            if cur is None:
                head = self.db.one(
                    "SELECT COALESCE(MAX(id), 0) AS m FROM notices WHERE project = ?", (project,)
                )["m"]
                self.db.run(
                    "INSERT INTO session_cursors (session_key, project, last_notice_id, updated) "
                    "VALUES (?, ?, ?, ?)",
                    (session_key, project, head, self.now()),
                )

    # ------------------------------------------------------------------ items
    def _ended(self, project: str, item: str) -> bool:
        op_id = parse_op_item(item)
        if op_id is not None:
            op = self.gateway.op(op_id)
            return op is None or op["project"] != project or op["state"] != "running"
        s = self.jobs.summary(project, item)
        return s is None or s["finished"]

    def _validate_group(self, project: str, g: dict) -> dict:
        priority = g.get("priority", 1)
        if isinstance(priority, bool) or priority not in PRIORITIES:
            raise WaitError("bad_priority", priority=priority)
        items = g.get("items")
        if not items:
            raise WaitError("empty_group")
        out = []
        for raw in items:
            item = str(raw).strip()
            op_id = parse_op_item(item)
            if op_id is not None:
                op = self.gateway.op(op_id)
                if op is None or op["project"] != project:
                    raise WaitError("unknown_op", id=op_id)
            else:
                if self.jobs.summary(project, item) is None:
                    raise WaitError("unknown_batch", batch=item)
            out.append(item)
        return {"items": out, "priority": priority}

    # ------------------------------------------------------------------ register
    def register(self, project: str, session_key: str, groups: list[dict] | None = None,
                 fail_frac: float | None = None, timeout_h: float | None = None,
                 note: str = "") -> dict:
        """Register a wait under state lock; returns wait row info."""
        p = self.projects.get(project)
        if p is None:
            raise WaitError("unknown_project", project=project)
        fail_frac = p.fail_frac if fail_frac is None else float(fail_frac)
        if not 0.0 <= fail_frac <= 1.0:
            raise WaitError("bad_fail_frac")
        explicit_timeout = timeout_h is not None
        timeout_h = p.timeout_h if timeout_h is None else float(timeout_h)
        if not timeout_h > 0:
            raise WaitError("bad_timeout")

        if groups is None:
            items = self.jobs.unfinished_batches(project) + [
                "op:%d" % oid for oid in self.gateway.running_ops(project)
            ]
            groups = [{"items": items, "priority": 1}] if items else []
        else:
            groups = [self._validate_group(project, g) for g in groups]

        seen: list[str] = []
        for i in wait_items(groups):
            if i in seen:
                raise WaitError("item_twice", item=i)
            seen.append(i)
        ended = {i for i in seen if self._ended(project, i)}
        if seen and len(ended) == len(seen):
            raise WaitError(
                "all_finished",
                summaries=[
                    s for s in (self.jobs.summary(project, i) for i in seen
                                if parse_op_item(i) is None) if s
                ],
                ops=[
                    self.gateway.op(parse_op_item(i)) for i in seen
                    if parse_op_item(i) is not None
                ],
            )
        for g in groups:
            if _due([g], ended):
                raise WaitError("group_ended", items=g["items"], priority=g["priority"])
        if not seen and not explicit_timeout:
            raise WaitError("nothing_to_wait")

        now = self.now()
        wid = self.db.insert(
            "waits",
            project=project,
            session_key=session_key,
            groups=json.dumps(groups),
            fail_frac=fail_frac,
            timeout_at=now + timeout_h * 3600.0,
            timeout_h=timeout_h,
            note=(note or "").strip(),
            state="active",
            created=now,
        )
        return {
            "id": wid,
            "project": project,
            "groups": groups,
            "fail_frac": fail_frac,
            "timeout_h": timeout_h,
            "timeout_at": now + timeout_h * 3600.0,
            "note": (note or "").strip(),
        }

    # ------------------------------------------------------------------ evaluate
    def check(self, wait: dict) -> dict | None:
        project = wait["project"]
        groups = json.loads(wait["groups"])
        items = wait_items(groups)
        ended = {i for i in items if self._ended(project, i)}
        batches = [i for i in items if parse_op_item(i) is None]
        ops = [parse_op_item(i) for i in items if parse_op_item(i) is not None]
        body = {
            "wait_id": wait["id"],
            "project": project,
            "note": wait["note"],
            "groups": groups,
            "batches": batches,
            "ops": ops,
        }
        if items and (_due(groups, ended) or all(i in ended for i in items)):
            return dict(body, reason="done", finished=sorted(ended))
        for b in batches:
            s = self.jobs.summary(project, b)
            if s and not s["fail_alerted"] and s["fail_rate"] > wait["fail_frac"]:
                return dict(body, reason="fail", batch=b, rate=s["fail_rate"],
                            threshold=wait["fail_frac"])
        if self.now() >= wait["timeout_at"]:
            return dict(body, reason="timeout", timeout_h=wait["timeout_h"])
        return None

    def progress_message(self, wait: dict) -> str:
        project = wait["project"]
        items = wait_items(json.loads(wait["groups"]))
        parts = []
        for i in items:
            op_id = parse_op_item(i)
            if op_id is not None:
                op = self.gateway.op(op_id)
                parts.append("op:%d %s" % (op_id, op["state"] if op else "unknown"))
            else:
                s = self.jobs.summary(project, i)
                if s:
                    done = s["jobs"].get("done", 0)
                    parts.append("%s %d/%d done" % (i, done, s["n_jobs"]))
        return "; ".join(parts) or "waiting"

    def evaluate(self, render_result=None) -> list[dict]:
        """Evaluate all active waits under state lock; update linked tasks & sync waiters."""
        fired = []
        for w in self.db.all("SELECT * FROM waits WHERE state = 'active' ORDER BY id"):
            body = self.check(w)
            if body is None:
                # Refresh progress message on any linked MCP task
                task = self.db.one("SELECT task_id FROM mcp_tasks WHERE wait_id = ? "
                                   "AND status = 'working'", (w["id"],))
                if task:
                    self.db.update("mcp_tasks", {"task_id": task["task_id"]},
                                   status_message=self.progress_message(w), updated=self.now())
                continue
            if body["reason"] == "fail":
                self.db.update("batches", {"project": w["project"], "batch": body["batch"]},
                               fail_alerted=1)
            self.db.update("waits", {"id": w["id"]}, state="fired",
                           result=json.dumps(body, ensure_ascii=False))
            rendered = render_result(body) if render_result else json.dumps(body)
            task = self.db.one("SELECT task_id FROM mcp_tasks WHERE wait_id = ? "
                               "AND status = 'working'", (w["id"],))
            if task:
                self.db.update(
                    "mcp_tasks", {"task_id": task["task_id"]},
                    status="completed",
                    status_message="Wait condition met (%s)" % body["reason"],
                    result_json=json.dumps({"text": rendered, "body": body}, ensure_ascii=False),
                    updated=self.now(),
                )
            with self._cond:
                inline = w["id"] in self._sync_waiters
                if inline:
                    self._sync_waiters[w["id"]] = body
                    self._cond.notify_all()
            if not inline and task is None:
                self.notify(
                    w["project"],
                    "wait_" + body["reason"],
                    "wait:%d" % w["id"],
                    body,
                )
            fired.append(body)
        return fired

    # ------------------------------------------------------------------ sync short wait
    def arm_sync_wait(self, wait_id: int) -> None:
        with self._cond:
            self._sync_waiters[wait_id] = None

    def wait_sync(self, wait_id: int, wait_s: float, on_tick=None) -> dict | None:
        """Wait outside state lock up to min(wait_s, 180)s for wait_id to fire."""
        limit = max(0.0, min(float(wait_s), 180.0))
        t0 = time.monotonic()
        deadline = t0 + limit
        with self._cond:
            if wait_id not in self._sync_waiters:
                self._sync_waiters[wait_id] = None
            while self._sync_waiters.get(wait_id) is None:
                # Re-check DB in case evaluate() fired before arm_sync_wait
                with self.lock:
                    w = self.db.one("SELECT state, result FROM waits WHERE id = ?", (wait_id,))
                    if w and w["state"] == "fired" and w["result"]:
                        self._sync_waiters.pop(wait_id, None)
                        return json.loads(w["result"])
                left = deadline - time.monotonic()
                if left <= 0:
                    break
                self._cond.wait(min(left, TICK_S))
                if self._sync_waiters.get(wait_id) is None and on_tick is not None:
                    try:
                        with self.lock:
                            w = self.db.one("SELECT * FROM waits WHERE id = ?", (wait_id,))
                            msg = self.progress_message(w) if w else "waiting"
                        on_tick(time.monotonic() - t0, limit, msg)
                    except Exception:
                        pass
            return self._sync_waiters.pop(wait_id, None)

    # ------------------------------------------------------------------ MCP Tasks store
    def create_task(self, project: str, wait_id: int,
                    poll_interval_ms: int = DEFAULT_POLL_INTERVAL_MS,
                    ttl_ms: int = DEFAULT_TTL_MS) -> dict:
        now = self.now()
        tid = "hpcp_" + secrets.token_hex(12)
        with self.lock:
            w = self.db.one("SELECT * FROM waits WHERE id = ?", (wait_id,))
            msg = self.progress_message(w) if w else "waiting"
            self.db.insert(
                "mcp_tasks",
                task_id=tid,
                project=project,
                wait_id=wait_id,
                status="working",
                status_message=msg,
                result_json=None,
                poll_interval_ms=poll_interval_ms,
                ttl_ms=ttl_ms,
                created=now,
                updated=now,
                expires_at=now + ttl_ms / 1000.0,
            )
            return self.get_task(tid)

    def get_task(self, task_id: str) -> dict | None:
        with self.lock:
            row = self.db.one("SELECT * FROM mcp_tasks WHERE task_id = ?", (task_id,))
            if row is None:
                return None
            if row["expires_at"] < self.now():
                return None
            if row["result_json"]:
                row["result"] = json.loads(row["result_json"])
            else:
                row["result"] = None
            return row

    def cancel_task(self, task_id: str) -> dict | None:
        with self.lock:
            row = self.db.one("SELECT * FROM mcp_tasks WHERE task_id = ?", (task_id,))
            if row is None:
                return None
            if row["status"] == "working":
                self.db.update("mcp_tasks", {"task_id": task_id}, status="cancelled",
                               status_message="Cancelled by client", updated=self.now())
                self.db.update("waits", {"id": row["wait_id"]}, state="cancelled")
            return self.get_task(task_id)

    def purge_expired_tasks(self) -> None:
        self.db.run("DELETE FROM mcp_tasks WHERE expires_at < ?", (self.now(),))
