"""Multi-project lock-free Slurm job registry.

One shared state lock guards SQLite reads/writes; clusters are never contacted
while it is held. Per-cluster locks serialize sbatch and squeue sync across all
projects sharing that cluster, while concurrency limits (max_jobs) and standing
budgets remain strictly independent per project.

No rigid directory naming (like <sid>.rNN) is required: any workdir inside the
project's remote root is accepted, and jobs group under an optional `batch`
label (auto-generated b001, b002, ... when omitted).
"""

from __future__ import annotations

import logging
import os
import posixpath
import re
import secrets
import shlex
import threading
import time

from . import slurm
from .executor import Executor, TransportError

log = logging.getLogger("hpcport.jobs")

BATCH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
PATTERN_RE = re.compile(r"^[A-Za-z0-9_.+*?\[\]/-]+$")
NAME_RE = re.compile(r"^[A-Za-z0-9_.+/-]+$")
ELEMENT_RE = re.compile(r"^(\d+)_(\d+)$")
LOST_AFTER = 3
SYNC_TIMEOUT = 30
CANCEL_SETTLE = 3
HOOK_DIR = ".hpcport"
USER = "user"
AGENT = "agent"
SCRIPT_PREVIEW_MAX = 16384


class JobError(Exception):
    def __init__(self, code: str, **params):
        super().__init__(code)
        self.code = code
        self.params = params


def inside(root: str, path: str) -> bool:
    return path == root or path.startswith(root.rstrip("/") + "/")


def under_root(root: str, real_root: str, path: str) -> str:
    if path == real_root:
        return root
    return root.rstrip("/") + "/" + path[len(real_root.rstrip("/")) + 1:]


def counted(prefix: str = "") -> tuple[str, tuple]:
    """SQL predicate for jobs counting against submission budgets."""
    p = prefix
    return (
        "(%sslurm_id IS NOT NULL OR %sstate IN (%s)) AND NOT (%sstate = ? AND "
        "(%st_start IS NULL OR (%st_end IS NOT NULL AND %st_start >= %st_end)))"
        % (p, p, slurm.marks(slurm.NOT_SENT), p, p, p, p, p),
        slurm.NOT_SENT + (slurm.CANCELLED,),
    )


def _label(job: dict) -> str:
    return job["slurm_id"] or "#%d" % job["id"]


def _job_stem(job_id: int, token: str | None) -> str:
    return "job%d_%s" % (job_id, token) if token else "job%d" % job_id


def _total(values) -> float | None:
    values = list(values)
    return None if any(v is None for v in values) else float(sum(values))


class JobService:
    def __init__(self, projects: dict, presets: dict, db, executors: dict[str, Executor],
                 clock=time.time, lock=None):
        self.projects = projects        # project_name -> Project
        self.presets = presets          # cluster_name -> ClusterPreset
        self.db = db
        self.ex = executors             # cluster_name -> Executor
        self.now = clock
        self.lock = lock if lock is not None else threading.RLock()
        self._cluster_locks: dict[str, threading.RLock] = {}
        self._preparing: dict[int, int] = {}
        self._frozen: dict[int, int] = {}
        self._sending: dict[int, threading.Event] = {}
        self._want: set[str] = set()    # clusters with newly released held jobs
        self.gate = None                # fn(project, sub) -> approval_id | None
        self.wait_approval = None       # fn(aid, wait_s, on_tick) -> decision | None
        self.on_change = lambda project=None: None

    # ------------------------------------------------------------------ helpers
    def _project(self, project: str):
        p = self.projects.get(project)
        if p is None:
            raise JobError("unknown_project", project=project)
        return p

    def _cluster(self, project: str, cluster: str):
        p = self._project(project)
        cl = p.clusters.get(cluster)
        if cl is None or cluster not in self.ex:
            raise JobError("unknown_cluster", cluster=cluster,
                           clusters=", ".join(sorted(p.clusters)) or "-")
        return p, cl, self.ex[cluster]

    def _cluster_lock(self, cluster: str) -> threading.RLock:
        with self.lock:
            return self._cluster_locks.setdefault(cluster, threading.RLock())

    def note_root(self, project: str, cluster: str, real: str) -> None:
        self.db.set_meta("root:%s:%s" % (project, cluster), real)

    def _real_root(self, project: str, cluster: str) -> str | None:
        return self.db.get_meta("root:%s:%s" % (project, cluster))

    # ------------------------------------------------------------------ submit
    def submit(self, project: str, cluster: str, workdir: str, scripts: list[str],
               desc: str, batch: str | None = None, tag: str = "",
               wait_s: float | None = None, on_tick=None) -> dict:
        p, cl, ex = self._cluster(project, cluster)
        if cl.scheduler != "slurm":
            raise JobError("not_slurm", cluster=cluster)
        if not desc or not desc.strip():
            raise JobError("empty_arg", arg="desc")
        if not scripts:
            raise JobError("empty_arg", arg="scripts")
        for pat in scripts:
            if not PATTERN_RE.match(pat) or pat.startswith("/") or ".." in pat.split("/"):
                raise JobError("bad_pattern", pattern=pat)
        tag = (tag or "").strip()[:32]
        if batch is not None:
            batch = batch.strip()
            if not batch:
                batch = None
            elif not BATCH_RE.match(batch):
                raise JobError("bad_batch", batch=batch)

        # Allow workdir relative to cluster remote root as well as absolute
        raw_wd = (workdir or "").strip() or cl.root
        cand_wd = raw_wd if raw_wd.startswith("/") else posixpath.normpath(
            posixpath.join(cl.root, raw_wd)
        )
        real, wd, files = self._probe(project, ex, cl, cand_wd, scripts)
        texts = self._read_scripts(ex, cluster, real, files)

        headers = {}
        for f in files:
            h = slurm.parse_header(texts[f])
            if h is None:
                raise JobError("bad_script", script=f)
            if cl.partitions:
                if not h.partitions:
                    raise JobError("no_partition", script=f, cluster=cluster,
                                   allowed=", ".join(cl.partitions))
                bad = [pt for pt in h.partitions if pt not in cl.partitions]
                if bad:
                    raise JobError("partition_denied", script=f, partition=", ".join(bad),
                                   cluster=cluster, allowed=", ".join(cl.partitions))
            headers[f] = h

        asked = {
            f: slurm.resources(headers[f], cl.cores_per_node, cl.gpus_per_node)
            for f in files
        }
        previews = {f: texts[f][:SCRIPT_PREVIEW_MAX] for f in files}

        with self.lock:
            self._check_duplicates(project, cluster, wd, files)
            if batch is None:
                while True:
                    cand = "b%03d" % self.db.next_counter("batch:" + project)
                    if self.db.one(
                        "SELECT 1 FROM batches WHERE project = ? AND batch = ?",
                        (project, cand),
                    ) is None:
                        batch = cand
                        break
            row = self.db.one("SELECT * FROM batches WHERE project = ? AND batch = ?",
                              (project, batch))
            if row is not None and row["cluster"] != cluster:
                raise JobError("batch_cluster_mismatch", batch=batch,
                               existing=row["cluster"], cluster=cluster)
            sub = self._submission(project, batch, cluster, wd, tag, desc.strip(),
                                   asked, previews)
            approval = self.gate(project, sub) if self.gate is not None else None
            first = slurm.UNAPPROVED if approval is not None else slurm.HELD
            now = self.now()
            submit_no = self.db.next_counter("submit_no")
            if row is None:
                self.db.insert("batches", project=project, batch=batch, cluster=cluster,
                               workdir=wd, tag=tag, desc=desc.strip(),
                               done_notified=0, created=now)
            else:
                self.db.update("batches", {"project": project, "batch": batch},
                               done_notified=0)
            env = {
                "HPCPORT_PROJECT": project,
                "HPCPORT_BATCH": batch,
                "HPCPORT_CLUSTER": cluster,
                "HPCPORT_ROOT": cl.root,
            }
            ids, wrappers = [], {}
            for f in files:
                h = headers[f]
                token = secrets.token_hex(6)
                jid = self.db.insert(
                    "jobs",
                    project=project,
                    batch=batch,
                    submit_no=submit_no,
                    cluster=cluster,
                    workdir=wd,
                    script=f,
                    partition=",".join(h.partitions) or None,
                    resources="; ".join(d[len("#SBATCH"):].strip() for d in h.directives),
                    state=first,
                    t_register=now,
                    changed=now,
                    approval=approval,
                    core_hours=asked[f].core_hours,
                    gpu_hours=asked[f].gpu_hours,
                    cores=asked[f].cores,
                    gpus=asked[f].gpus,
                    dispatch_token=token,
                    dispatch_attempted=0,
                )
                ids.append(jid)
                self._preparing[jid] = submit_no
                stem = _job_stem(jid, token)
                wrappers[stem] = slurm.wrapper_script(
                    h, "%s/%s" % (real, f), "%s/%s" % (real, HOOK_DIR), env,
                    sid_file="%s/%s/%s.sid" % (real, HOOK_DIR, stem),
                )

        # Step 2: write wrappers outside lock
        failure = None
        try:
            self._write_wrappers(ex, real, wrappers)
        except TransportError as e:
            failure = {"script": None, "error": str(e)}
        finally:
            with self.lock:
                for jid in ids:
                    self._preparing.pop(jid, None)

        if failure:
            with self.lock:
                self._drop_unsent(submit_no, project, batch, row, cluster, failure)

        # Step 3: handle approval wait (up to submit.wait_s, default 180s) or immediate dispatch
        approved_inline = False
        refused_inline = False
        refuse_comment = ""
        pending_approval = False
        dispatch_transport_err: str | None = None

        if approval is not None:
            eff_wait = p.submit.wait_s if wait_s is None else max(0.0, min(float(wait_s), 180.0))
            decision = (self.wait_approval(approval, eff_wait, on_tick)
                        if self.wait_approval is not None else None)
            if decision is None:
                pending_approval = True
            elif not decision["approved"]:
                refused_inline = True
                refuse_comment = decision.get("comment") or ""
            else:
                approved_inline = True
                try:
                    failure = self._dispatch(project, cluster, only=ids, stop_on_error=True)
                except TransportError as e:
                    dispatch_transport_err = str(e)
        else:
            try:
                failure = self._dispatch(project, cluster, only=ids, stop_on_error=True)
            except TransportError as e:
                dispatch_transport_err = str(e)

        with self.lock:
            if dispatch_transport_err is not None:
                # SSH dropped during sbatch; do NOT delete held rows because Slurm
                # may have accepted the job and written .hpcport/job<id>.sid.
                # Keep rows as held so duplicates are blocked and reconnect reconciles .sid.
                jobs = self.db.all("SELECT * FROM jobs WHERE submit_no = ? ORDER BY id", (submit_no,))
                n_sent = sum(1 for j in jobs if j["slurm_id"])
                n_held = sum(1 for j in jobs if j["state"] == slurm.HELD)
                if n_held:
                    self._want.add(cluster)
                self.on_change(project)
                raise JobError(
                    "unreachable_dispatch",
                    cluster=cluster,
                    batch=batch,
                    error=dispatch_transport_err,
                    n_sent=n_sent,
                    n_held=n_held,
                )
            if failure:
                self._drop_unsent(submit_no, project, batch, row, cluster, failure)
            jobs = self.db.all("SELECT * FROM jobs WHERE submit_no = ? ORDER BY id", (submit_no,))
            if self.db.one("SELECT 1 FROM jobs WHERE submit_no = ? AND state = ?",
                           (submit_no, slurm.HELD)):
                self._want.add(cluster)
            self.on_change(project)

        return {
            "project": project,
            "batch": batch,
            "cluster": cluster,
            "workdir": wd,
            "tag": tag,
            "max_jobs": cl.max_jobs,
            "n": len(jobs),
            "n_sent": sum(1 for j in jobs if j["slurm_id"]),
            "n_held": sum(1 for j in jobs if j["state"] == slurm.HELD),
            "ids": [j["slurm_id"] for j in jobs if j["slurm_id"]],
            "approval": approval if pending_approval else None,
            "approval_id": approval,
            "approved_inline": approved_inline,
            "refused_inline": refused_inline,
            "refuse_comment": refuse_comment,
            "conditions": sub["conditions"] if pending_approval else [],
            "core_hours": _total(a.core_hours for a in asked.values()),
            "gpu_hours": _total(a.gpu_hours for a in asked.values()),
        }

    def _check_duplicates(self, project: str, cluster: str, wd: str, files: list[str]) -> None:
        for f in files:
            dup = self.db.one(
                "SELECT slurm_id, state FROM jobs WHERE project = ? AND cluster = ? "
                "AND workdir = ? AND script = ? AND state IN (%s)" % slurm.marks(slurm.ACTIVE),
                (project, cluster, wd, f) + slurm.ACTIVE,
            )
            if dup:
                raise JobError("duplicate", script=f, jobid=dup["slurm_id"] or "-",
                               state=dup["state"])

    def _drop_unsent(self, submit_no, project, batch, prev_batch_row, cluster, failure) -> None:
        unsent = self.db.all(
            "SELECT id FROM jobs WHERE submit_no = ? AND slurm_id IS NULL", (submit_no,))
        sent = self.db.all(
            "SELECT slurm_id FROM jobs WHERE submit_no = ? AND slurm_id IS NOT NULL", (submit_no,))
        self.db.run("DELETE FROM jobs WHERE submit_no = ? AND slurm_id IS NULL", (submit_no,))
        if not sent and prev_batch_row is None and not self.db.one(
                "SELECT 1 FROM jobs WHERE project = ? AND batch = ?", (project, batch)):
            self.db.run("DELETE FROM batches WHERE project = ? AND batch = ?", (project, batch))
        raise JobError(
            "unreachable" if failure["script"] is None else "sbatch_rejected",
            cluster=cluster,
            script=failure["script"],
            error=failure["error"],
            n_sent=len(sent),
            n_unsent=len(unsent),
            ids=", ".join(s["slurm_id"] for s in sent),
        )

    @staticmethod
    def _submission(project, batch, cluster, wd, tag, desc, asked: dict,
                    script_texts: dict) -> dict:
        return {
            "project": project,
            "batch": batch,
            "cluster": cluster,
            "workdir": wd,
            "tag": tag,
            "conditions": [],
            "desc": desc,
            "scripts": list(asked),
            "script_texts": script_texts,
            "jobs": len(asked),
            "core_hours": _total(a.core_hours for a in asked.values()),
            "gpu_hours": _total(a.gpu_hours for a in asked.values()),
            "unknown": [(f, list(a.unknown)) for f, a in asked.items() if a.unknown],
        }

    def _probe(self, project, ex, cl, workdir, patterns):
        q = shlex.quote
        loops = "\n".join(
            "n=0; for f in %s; do [ -f \"$f\" ] && { printf 'SCRIPT %%s\\n' \"$f\"; n=1; }; "
            "done; [ $n = 1 ] || printf 'MISSING %%s\\n' %s" % (p, q(p)) for p in patterns
        )
        cmd = (
            "root=$(cd -- %s 2>/dev/null && pwd -P); wd=$(cd -- %s 2>/dev/null && pwd -P)\n"
            "printf 'ROOT %%s\\nWD %%s\\n' \"$root\" \"$wd\"\n"
            "if [ -n \"$wd\" ]; then cd -- \"$wd\"\n"
            "shopt -s nullglob\n%s\nfi" % (q(cl.root), q(workdir), loops)
        )
        try:
            r = ex.run(cmd)
        except TransportError as e:
            raise JobError("unreachable", cluster=cl.name, script=None, error=str(e),
                           n_sent=0, n_unsent=len(patterns), ids="")
        info = {"ROOT": "", "WD": ""}
        files, missing = [], []
        for line in r.out.splitlines():
            key, _, val = line.partition(" ")
            if key in info:
                info[key] = val
            elif key == "SCRIPT" and val not in files:
                files.append(val)
            elif key == "MISSING":
                missing.append(val)
        root, wd = info["ROOT"], info["WD"]
        if not root:
            raise JobError("no_root", cluster=cl.name, root=cl.root)
        with self.lock:
            self.note_root(project, cl.name, root)
        if not wd or not inside(root, wd):
            raise JobError("workdir_outside", path=workdir, cluster=cl.name, root=cl.root)
        shown = under_root(cl.root, root, wd)
        bad = [f for f in files if not NAME_RE.match(f)]
        if missing or bad or not files:
            raise JobError("missing_scripts", workdir=shown, scripts=", ".join(missing + bad))
        return wd, shown, files

    def _read_scripts(self, ex, cluster, wd, files) -> dict[str, str]:
        q = shlex.quote
        mark = "@@HPCP_%s" % secrets.token_hex(6)
        cmd = ("cd -- %s && for f in %s; do printf '%s %%s\\n' \"$f\"; head -c 262144 -- \"$f\"; "
               "printf '\\n'; done" % (q(wd), " ".join(q(f) for f in files), mark))
        try:
            r = ex.run(cmd)
        except TransportError as e:
            raise JobError("unreachable", cluster=cluster, script=None, error=str(e),
                           n_sent=0, n_unsent=len(files), ids="")
        texts, cur = {}, None
        for line in r.out.splitlines(keepends=True):
            if line.startswith(mark + " "):
                cur = line[len(mark) + 1:].rstrip("\n")
                texts[cur] = ""
            elif cur is not None:
                texts[cur] += line
        if r.rc != 0 or set(texts) != set(files):
            raise JobError("missing_scripts", workdir=wd, scripts=", ".join(files))
        return texts

    def _write_wrappers(self, ex, wd, wrappers: dict[str, str]) -> None:
        q = shlex.quote
        parts = ["cd -- %s && mkdir -p %s || exit 1" % (q(wd), HOOK_DIR)]
        for stem, text in wrappers.items():
            eof = "HPCP_EOF_%s" % secrets.token_hex(6)
            parts.append("cat > %s/%s.sh <<'%s' || exit 1\n%s%s"
                         % (HOOK_DIR, stem, eof, text, eof))
        r = ex.run("bash -s", input="\n".join(parts) + "\n")
        if r.rc != 0:
            raise TransportError("cannot write job wrappers in %s: %s"
                                 % (wd, r.err.strip()[-300:]))

    # ------------------------------------------------------------------ dispatch
    def _dispatch(self, project: str, cluster: str, only: list[int] | None = None,
                  stop_on_error: bool = False) -> dict | None:
        """Submit held jobs for one project on one cluster while its slots allow."""
        p = self.projects.get(project)
        if p is None or cluster not in p.clusters or cluster not in self.ex:
            return None
        cl = p.clusters[cluster]
        if cl.scheduler != "slurm":
            return None
        ex = self.ex[cluster]
        q = shlex.quote
        with self._cluster_lock(cluster):
            while True:
                with self.lock:
                    j = self._next_held(project, cluster, cl.max_jobs, only)
                    if j is None:
                        return None
                    was_attempted = bool(j.get("dispatch_attempted"))
                    if not was_attempted:
                        self.db.update("jobs", {"id": j["id"]}, dispatch_attempted=1)
                        j["dispatch_attempted"] = 1
                    sent = self._sending[j["id"]] = threading.Event()
                try:
                    stem = _job_stem(j["id"], j.get("dispatch_token"))
                    wrapper_rel = "%s/%s.sh" % (HOOK_DIR, stem)
                    sid_rel = "%s/%s.sid" % (HOOK_DIR, stem)
                    lock_rel = "%s/%s.lock" % (HOOK_DIR, stem)
                    squeue_check = ""
                    if was_attempted:
                        squeue_check = (
                            "__sq=$(squeue -h -u \"$USER\" -o \"%%F|%%o\" 2>/dev/null "
                            "| awk -F'|' -v pat=%s 'index($2, pat) > 0 {print $1; exit}'); "
                            "if [ -n \"$__sq\" ]; then "
                            "printf '%%s\\n' \"$__sq\" > %s.tmp && mv %s.tmp %s && "
                            "printf '%%s\\n' \"$__sq\"; return 0; fi; "
                            % (q(stem + ".sh"), q(sid_rel), q(sid_rel), q(sid_rel))
                        )
                    cmd = (
                        "cd -- %s && ("
                        "trap '' HUP; "
                        "__hpcp_sub() { "
                        "if [ -s %s ]; then cat %s; return 0; fi; "
                        "%s"
                        "__sid=$(sbatch --parsable %s); __rc=$?; "
                        "if [ \"$__rc\" -eq 0 ]; then "
                        "printf '%%s\\n' \"$__sid\" > %s.tmp && mv %s.tmp %s && "
                        "printf '%%s\\n' \"$__sid\"; return 0; fi; "
                        "return \"$__rc\"; "
                        "}; "
                        "if command -v flock >/dev/null 2>&1; then "
                        "( flock -x 9 && __hpcp_sub ) 9>%s; "
                        "else __hpcp_sub; fi"
                        ")"
                        % (q(j["workdir"]), q(sid_rel), q(sid_rel), squeue_check,
                           q(wrapper_rel), q(sid_rel), q(sid_rel), q(sid_rel), q(lock_rel))
                    )
                    r = ex.run(cmd)
                    sid = slurm.parse_sbatch(r.out) if r.rc == 0 else None
                    with self.lock:
                        now = self.now()
                        if sid is not None:
                            self.db.update("jobs", {"id": j["id"]}, state=slurm.QUEUED,
                                           slurm_id=sid, slurm_state="PENDING",
                                           t_submit=now, changed=now)
                            continue
                        err = (r.err.strip() or r.out.strip() or "exit %d" % r.rc)[-400:]
                        self.db.update("jobs", {"id": j["id"]}, state=slurm.SUBMIT_FAILED,
                                       error=err, changed=now, t_end=now)
                    log.warning("sbatch rejected %s (%s): %s", j["script"], project, err)
                    if stop_on_error:
                        return {"script": j["script"], "error": err}
                finally:
                    with self.lock:
                        self._sending.pop(j["id"], None)
                    sent.set()

    def _next_held(self, project: str, cluster: str, max_jobs: int, only) -> dict | None:
        n_active = self.db.one(
            "SELECT COUNT(*) AS n FROM jobs WHERE project = ? AND cluster = ? "
            "AND state IN (?, ?)",
            (project, cluster) + slurm.IN_SLURM,
        )["n"]
        if n_active >= max_jobs:
            return None
        held = [
            j for j in self.db.all(
                "SELECT * FROM jobs WHERE project = ? AND cluster = ? AND state = ? ORDER BY id",
                (project, cluster, slurm.HELD),
            )
            if j["id"] not in self._frozen and j["id"] not in self._sending
            and (j["id"] not in self._preparing or (only is not None and j["id"] in only))
        ]
        if only is not None and any(j["id"] not in only for j in held):
            return None
        return held[0] if held else None

    def dispatch_wanted(self, skip=()) -> None:
        with self.lock:
            todo = sorted(self._want - set(skip))
            self._want -= set(todo)
            proj_names = list(self.projects)
        for cluster in todo:
            if cluster not in self.ex:
                continue
            for pname in proj_names:
                p = self.projects.get(pname)
                if p and cluster in p.clusters:
                    try:
                        self._dispatch(pname, cluster)
                    except TransportError as e:
                        log.warning("could not submit held jobs on %s for %s: %s",
                                    cluster, pname, e)

    # ------------------------------------------------------------------ approval hooks
    def unapproved(self, approval: int) -> list[dict]:
        return self.db.all("SELECT * FROM jobs WHERE approval = ? AND state = ? ORDER BY id",
                           (approval, slurm.UNAPPROVED))

    def release(self, approval: int) -> list[str]:
        jobs = self.unapproved(approval)
        now = self.now()
        for j in jobs:
            self.db.update("jobs", {"id": j["id"]}, state=slurm.HELD, changed=now)
        self._want.update(j["cluster"] for j in jobs)
        return sorted({j["batch"] for j in jobs})

    def refuse(self, approval: int, comment: str = "") -> list[str]:
        jobs = self.unapproved(approval)
        now = self.now()
        note = (comment or "").strip() or None
        for j in jobs:
            self.db.update("jobs", {"id": j["id"]}, state=slurm.CANCELLED, source="registry",
                           changed=now, t_end=now, cancel_by=USER, cancel_at=now,
                           cancel_note=note)
        return sorted({j["batch"] for j in jobs})

    def last_job_id(self, project: str) -> int:
        r = self.db.one("SELECT COALESCE(MAX(id), 0) AS n FROM jobs WHERE project = ?", (project,))
        return r["n"] if r else 0

    def used_after(self, project: str, job_id: int, apart: tuple[int, ...] = (),
                   also: tuple[int, ...] = ()) -> dict:
        cond, args = counted()
        ended_state = "state NOT IN (%s)" % ", ".join("'%s'" % st for st in slurm.ACTIVE)
        ended = "%s AND run_s IS NOT NULL" % ended_state
        running_state = "state IN ('%s', '%s', '%s')" % (slurm.HELD, slurm.QUEUED, slurm.RUNNING)
        cols = [
            "COUNT(*) AS jobs",
            "COALESCE(SUM(CASE WHEN %s THEN 1 ELSE 0 END), 0) AS m_jobs" % ended_state,
            "COALESCE(SUM(CASE WHEN %s THEN 1 ELSE 0 END), 0) AS a_jobs" % running_state,
        ]
        for name, held in (("core_hours", "cores"), ("gpu_hours", "gpus")):
            known, used = "%s AND %s IS NOT NULL" % (ended, held), "%s * run_s / 3600.0" % held
            cols += [
                "COALESCE(SUM(CASE WHEN %s THEN %s ELSE %s END), 0) AS %s"
                % (known, used, name, name),
                "COALESCE(SUM(CASE WHEN %s THEN %s WHEN %s THEN %s END), 0) AS m_%s"
                % (known, used, ended_state, name, name),
                "COALESCE(SUM(CASE WHEN %s THEN %s END), 0) AS a_%s"
                % (running_state, name, name),
                "COALESCE(SUM(CASE WHEN state = '%s' THEN %s END), 0) AS w_%s"
                % (slurm.UNAPPROVED, name, name),
            ]
        cols.append("COALESCE(SUM(CASE WHEN state = '%s' THEN 1 ELSE 0 END), 0) AS w_jobs"
                    % slurm.UNAPPROVED)
        if apart:
            cond += " AND NOT (state = ? AND approval IN (%s))" % slurm.marks(apart)
            args += (slurm.UNAPPROVED,) + tuple(apart)
        since = "(id > ? OR approval IN (%s))" % slurm.marks(also) if also else "id > ?"
        r = self.db.one(
            "SELECT %s FROM jobs WHERE project = ? AND %s AND %s"
            % (", ".join(cols), since, cond),
            (project, job_id) + tuple(also) + args,
        )
        return {
            "jobs": r["jobs"],
            "core_hours": r["core_hours"],
            "gpu_hours": r["gpu_hours"],
            "measured": {"jobs": r["m_jobs"], "core_hours": r["m_core_hours"],
                         "gpu_hours": r["m_gpu_hours"]},
            "active": {"jobs": r["a_jobs"], "core_hours": r["a_core_hours"],
                       "gpu_hours": r["a_gpu_hours"]},
            "waiting": {"jobs": r["w_jobs"], "core_hours": r["w_core_hours"],
                        "gpu_hours": r["w_gpu_hours"]},
        }

    # ------------------------------------------------------------------ cancel
    def cancel(self, project: str | None, batch: str | None = None,
               job_ids: list[str] | None = None, approval: int | None = None,
               by: str = AGENT, note: str = "", then=None,
               cluster: str | None = None):
        """Cancel jobs by batch, Slurm IDs (optionally cluster:id or #internal_id),
        or pending approval ID."""
        if sum((bool(batch), bool(job_ids), approval is not None)) != 1:
            raise JobError("cancel_args")
        while True:
            with self.lock:
                jobs, elements = self._cancel_targets(project, batch, job_ids, approval,
                                                      cluster=cluster)
                waiting = [self._sending[j["id"]] for j in jobs if j["id"] in self._sending]
                if not waiting:
                    whole = [j for j in jobs if (j["cluster"], j["slurm_id"]) not in elements]
                    active = [j for j in whole if j["state"] in slurm.ACTIVE]
                    unsent = [j["id"] for j in active if j["state"] in slurm.NOT_SENT]
                    for i in unsent:
                        self._frozen[i] = self._frozen.get(i, 0) + 1
                    break
            for e in waiting:
                e.wait()
        try:
            r = self._cancel(jobs, elements, active, unsent, approval, by, note, then)
        finally:
            with self.lock:
                for i in unsent:
                    self._frozen[i] -= 1
                    if not self._frozen[i]:
                        del self._frozen[i]
        return r

    def _cancel_targets(self, project, batch, job_ids, approval, cluster: str | None = None):
        elements: dict[tuple[str, str], list[str]] = {}
        if approval is not None:
            jobs = self.unapproved(approval)
            if project is not None:
                jobs = [j for j in jobs if j["project"] == project]
            if cluster is not None:
                jobs = [j for j in jobs if j["cluster"] == cluster]
            if not jobs:
                raise JobError("no_unapproved", approval=approval)
        elif batch:
            if project is None:
                rows = self.db.all("SELECT project FROM batches WHERE batch = ?", (batch,))
                if not rows:
                    raise JobError("unknown_batch", batch=batch)
                if len(rows) > 1:
                    raise JobError("ambiguous_batch", batch=batch,
                                   projects=", ".join(sorted(r["project"] for r in rows)))
                project = rows[0]["project"]
            if not self.db.one("SELECT 1 FROM batches WHERE project = ? AND batch = ?",
                               (project, batch)):
                raise JobError("unknown_batch", batch=batch)
            jobs = self.db.all("SELECT * FROM jobs WHERE project = ? AND batch = ? ORDER BY id",
                               (project, batch))
            if cluster is not None:
                jobs = [j for j in jobs if j["cluster"] == cluster]
        else:
            jobs, elements = self._named(project, job_ids, cluster=cluster)
        return jobs, elements

    def _reconcile_uncertain(self, cluster: str, unc_jobs: list[dict]) -> dict[int, str]:
        """Check remote .sid files and squeue for attempted jobs lacking a recorded slurm_id."""
        ex = self.ex.get(cluster)
        if ex is None:
            raise TransportError("unknown cluster %s" % cluster)
        q = shlex.quote
        mark = "@@HPCP_REC_%s" % secrets.token_hex(6)
        lines: list[str] = []
        for j in unc_jobs:
            stem = _job_stem(j["id"], j.get("dispatch_token"))
            hook_abs = "%s/%s" % (j["workdir"], HOOK_DIR)
            sid_path = "%s/%s.sid" % (hook_abs, stem)
            lock_path = "%s/%s.lock" % (hook_abs, stem)
            lines.append(
                "__check_%d() {\n"
                "  local __s=''\n"
                "  if [ -s %s ]; then\n"
                "    __s=$(cat %s 2>/dev/null)\n"
                "  else\n"
                "    __s=$(squeue -h -u \"$USER\" -o \"%%F|%%o\" 2>/dev/null "
                "| awk -F'|' -v pat=%s 'index($2, pat) > 0 {print $1; exit}')\n"
                "  fi\n"
                "  [ -n \"$__s\" ] && printf '%s %d %%s\\n' \"$__s\"\n"
                "}\n"
                "if command -v flock >/dev/null 2>&1 && [ -d %s ]; then\n"
                "  ( flock -x 9 && __check_%d ) 9>%s\n"
                "else\n"
                "  __check_%d\n"
                "fi"
                % (
                    j["id"], q(sid_path), q(sid_path), q(stem + ".sh"),
                    mark, j["id"], q(hook_abs), j["id"], q(lock_path), j["id"],
                )
            )
        with self._cluster_lock(cluster):
            r = ex.run("bash -s", input="\n".join(lines) + "\n", timeout=SYNC_TIMEOUT)
        found: dict[int, str] = {}
        for line in r.out.splitlines():
            if line.startswith(mark + " "):
                rest = line[len(mark) + 1:].strip().split(None, 1)
                if len(rest) == 2 and rest[0].isdigit():
                    sid = slurm.parse_sbatch(rest[1])
                    if sid is not None:
                        found[int(rest[0])] = sid
        return found

    def _cancel(self, jobs, elements, active, unsent, approval, by, note, then):
        local_unsent = list(unsent)
        uncertain_by_cluster: dict[str, list[dict]] = {}
        for j in active:
            if j["state"] in slurm.NOT_SENT and j.get("dispatch_attempted"):
                uncertain_by_cluster.setdefault(j["cluster"], []).append(j)
        for cluster, unc_jobs in uncertain_by_cluster.items():
            try:
                found_sids = self._reconcile_uncertain(cluster, unc_jobs)
            except TransportError as e:
                raise JobError("unreachable", cluster=cluster, script=None, error=str(e),
                               n_sent=0, n_unsent=0, ids="")
            if found_sids:
                with self.lock:
                    now = self.now()
                    for j in unc_jobs:
                        sid = found_sids.get(j["id"])
                        if sid is not None:
                            self.db.update("jobs", {"id": j["id"]}, state=slurm.QUEUED,
                                           slurm_id=sid, slurm_state="PENDING",
                                           t_submit=now, changed=now)
                            j["slurm_id"] = sid
                            j["state"] = slurm.QUEUED
                            if j["id"] in local_unsent:
                                local_unsent.remove(j["id"])

        ask: dict[str, tuple[list[dict], list[str]]] = {}
        for j in jobs:
            if j["state"] in slurm.IN_SLURM:
                sent, parts = ask.setdefault(j["cluster"], ([], []))
                ckey = (j["cluster"], j["slurm_id"])
                parts.extend(elements[ckey]) if ckey in elements else sent.append(j)
        note_clean = (note or "").strip() or None
        errors: dict[str, str] = {}
        answers = []
        for cluster, (sent, parts) in ask.items():
            try:
                error, found = self._scancel(cluster, [j["slurm_id"] for j in sent], parts)
            except TransportError as e:
                with self.lock:
                    self._record_scancel(answers, by, note_clean)
                raise JobError("unreachable", cluster=cluster, script=None, error=str(e),
                               n_sent=0, n_unsent=0, ids="")
            if error:
                errors[cluster] = error
                log.warning("scancel on %s: %s", cluster, error)
            answers.append((sent, error, found))
        with self.lock:
            self._record_scancel(answers, by, note_clean)
            now = self.now()
            for i in local_unsent:
                cur = self.db.one("SELECT state FROM jobs WHERE id = ?", (i,))
                if cur and cur["state"] in slurm.NOT_SENT:
                    self.db.update("jobs", {"id": i}, state=slurm.CANCELLED, source="registry",
                                   changed=now, t_end=now, cancel_by=by, cancel_at=now,
                                   cancel_note=note_clean)
            state = {}
            for j in active:
                cur = self.db.one("SELECT state FROM jobs WHERE id = ?", (j["id"],))
                state[j["id"]] = cur["state"] if cur else slurm.CANCELLED
            parts = [e for _, es in ask.values() for e in es]
            ended = sorted({
                (j["project"], j["batch"])
                for j in active
                if not self.is_unfinished(j["project"], j["batch"])
            })
            r = {
                "requested": [_label(j) for j in active] + parts,
                "cancelled": [_label(j) for j in active if state[j["id"]] == slurm.CANCELLED],
                "ending": [_label(j) for j in active if state[j["id"]] in slurm.ACTIVE],
                "elements": parts,
                "n_finished": (
                    sum(1 for j in jobs if j["state"] not in slurm.ACTIVE)
                    + sum(1 for j in active
                          if state[j["id"]] not in slurm.ACTIVE + (slurm.CANCELLED,))
                ),
                "ended": [b for _, b in ended],
                "ended_pairs": ended,
                "errors": errors,
                "approval": approval,
            }
            self.on_change(jobs[0]["project"] if jobs else None)
            return then(r) if then is not None else r

    def _record_scancel(self, answers, who, note) -> None:
        now = self.now()
        for sent, error, found in answers:
            for j in sent:
                if not error:
                    self.db.update("jobs", {"id": j["id"]}, cancel_by=who, cancel_at=now,
                                   cancel_note=note)
                ctl = found.get(j["slurm_id"])
                cur = self.db.one("SELECT * FROM jobs WHERE id = ?", (j["id"],))
                if (ctl and ctl["state"] and ctl["state"] not in slurm.IN_SLURM
                        and cur and cur["state"] in slurm.IN_SLURM):
                    self._finish(cur, ctl["state"], ctl["raw"], ctl["exit_code"],
                                 "scontrol", ctl["start"], ctl["end"], ctl["elapsed"])

    def _named(self, project: str | None, job_ids, cluster: str | None = None):
        """Resolve job_ids into (jobs, elements) with cluster disambiguation.

        Accepts:
          - `#<internal_id>` (globally unique DB job ID; works for held/unapproved too)
          - `<cluster>:<slurm_id>` or `<cluster>:<slurm_id>_<elem>`
          - `<slurm_id>` or `<slurm_id>_<elem>` (filtered by optional `cluster`;
            raises `ambiguous_job_id` if an unqualified Slurm ID exists on multiple clusters).
        """
        whole: list[tuple[str | None, str, str]] = []       # (cluster, slurm_id, raw)
        elem_reqs: list[tuple[str | None, str, str, str]] = []  # (cluster, parent_id, elem, raw)
        internal: list[int] = []
        unknown: list[str] = []

        for raw in dict.fromkeys(str(i).strip() for i in job_ids):
            if raw.startswith("#") and raw[1:].isdigit():
                internal.append(int(raw[1:]))
                continue
            cl_spec = cluster
            tok = raw
            if ":" in raw:
                head, _, tail = raw.partition(":")
                if head and tail and "/" not in head:
                    cl_spec = head
                    tok = tail
            m = ELEMENT_RE.match(tok)
            if tok.isdigit():
                whole.append((cl_spec, tok, raw))
            elif m:
                elem_reqs.append((cl_spec, m.group(1), tok, raw))
            else:
                unknown.append(raw)

        def query_slurm(cl_spec: str | None, sid: str) -> list[dict]:
            conds = ["slurm_id = ?"]
            args: list = [sid]
            if project is not None:
                conds.append("project = ?")
                args.append(project)
            if cl_spec is not None:
                conds.append("cluster = ?")
                args.append(cl_spec)
            return self.db.all(
                "SELECT * FROM jobs WHERE %s ORDER BY id" % " AND ".join(conds),
                tuple(args),
            )

        jobs_by_id: dict[int, dict] = {}
        whole_keys: set[tuple[str, str]] = set()
        elements: dict[tuple[str, str], list[str]] = {}

        for cl_spec, sid, raw in whole:
            rows = query_slurm(cl_spec, sid)
            if not rows:
                unknown.append(raw)
                continue
            clusters_hit = sorted({r["cluster"] for r in rows})
            if cl_spec is None and len(clusters_hit) > 1:
                raise JobError("ambiguous_job_id", id=sid, clusters=", ".join(clusters_hit))
            for r in rows:
                jobs_by_id[r["id"]] = r
                whole_keys.add((r["cluster"], r["slurm_id"]))

        for cl_spec, parent_sid, elem_tok, raw in elem_reqs:
            rows = query_slurm(cl_spec, parent_sid)
            if not rows:
                unknown.append(raw)
                continue
            clusters_hit = sorted({r["cluster"] for r in rows})
            if cl_spec is None and len(clusters_hit) > 1:
                raise JobError("ambiguous_job_id", id=raw, clusters=", ".join(clusters_hit))
            for r in rows:
                jobs_by_id[r["id"]] = r
                ckey = (r["cluster"], r["slurm_id"])
                if ckey not in whole_keys:
                    lst = elements.setdefault(ckey, [])
                    if elem_tok not in lst:
                        lst.append(elem_tok)

        if internal:
            conds = ["id IN (%s)" % slurm.marks(internal)]
            args = list(internal)
            if project is not None:
                conds.append("project = ?")
                args.append(project)
            if cluster is not None:
                conds.append("cluster = ?")
                args.append(cluster)
            found_i = self.db.all(
                "SELECT * FROM jobs WHERE %s ORDER BY id" % " AND ".join(conds),
                tuple(args),
            )
            for j in found_i:
                jobs_by_id[j["id"]] = j
            known_int = {j["id"] for j in found_i}
            unknown += ["#%d" % i for i in internal if i not in known_int]

        if unknown:
            raise JobError("unknown_jobs", ids=", ".join(unknown))
        return [jobs_by_id[k] for k in sorted(jobs_by_id)], elements

    def _scancel(self, cluster: str, ids: list[str], elements: list[str]):
        mark = "@@HPCP_%s" % secrets.token_hex(6)
        script = ["scancel %s </dev/null; printf '%s R %%d\\n' $?"
                  % (" ".join(ids + elements), mark)]
        if ids:
            running = "|".join(k for k, v in slurm.STATES.items() if v in slurm.IN_SLURM)
            script += [
                "__q() { for i in %s; do printf '%s J %%s\\n' \"$i\"; %sscontrol show job \"$i\" "
                "2>/dev/null; echo; done; }" % (" ".join(ids), mark, slurm.TIME_ENV),
                "__out=$(__q)",
                "if printf '%%s\\n' \"$__out\" | grep -Eq 'JobState=(%s)'; then sleep %d; "
                "__out=$(__q); fi" % (running, CANCEL_SETTLE),
                "printf '%s\\n' \"$__out\"",
            ]
        r = self.ex[cluster].run("bash -s", input="\n".join(script) + "\n")
        rc, found, cur = None, {}, None
        for line in r.out.splitlines():
            if line.startswith(mark + " "):
                tag, _, val = line[len(mark) + 1:].partition(" ")
                if tag == "R":
                    rc = val.strip()
                else:
                    cur = val.strip()
                    found[cur] = ""
            elif cur is not None:
                found[cur] += line + "\n"
        if rc is None:
            raise TransportError("incomplete output from scancel on %s" % cluster)
        error = None
        if rc != "0":
            error = (r.err.strip().splitlines() or ["scancel: exit %s" % rc])[-1][-300:]
        return error, {i: slurm.parse_scontrol(found.get(i, "")) for i in ids}

    # ------------------------------------------------------------------ single-pass cluster sync
    def sync(self, cluster: str) -> set[tuple[str, str]]:
        """Sync one Slurm cluster across ALL projects in a single squeue round-trip.
        Returns set of (project, batch) pairs whose state changed."""
        preset = self.presets.get(cluster)
        if preset is None or preset.scheduler != "slurm" or cluster not in self.ex:
            return set()
        ex = self.ex[cluster]
        with self._cluster_lock(cluster):
            with self.lock:
                known = self.db.all(
                    "SELECT * FROM jobs WHERE cluster = ? AND state IN (?, ?)",
                    (cluster,) + slurm.IN_SLURM,
                )
            r = ex.run(slurm.SQUEUE, timeout=SYNC_TIMEOUT)
            if r.rc != 0:
                raise TransportError("squeue failed on %s: %s" % (cluster, r.err.strip()[-300:]))
            listed = slurm.parse_squeue(r.out)
            missing = [j for j in known if j["slurm_id"] not in listed]
            acct, sections = self._ask_ended(ex, missing) if missing else ({}, {})
            changed: set[tuple[str, str]] = set()
            with self.lock:
                for j0 in known:
                    j = self.db.one("SELECT * FROM jobs WHERE id = ?", (j0["id"],))
                    if j is None or j["state"] not in slurm.IN_SLURM:
                        continue
                    row = listed.get(j["slurm_id"])
                    if row is None:
                        if self._resolve(j, acct, sections):
                            changed.add((j["project"], j["batch"]))
                        continue
                    upd = {"misses": 0, "slurm_state": row["raw"]}
                    if row["state"] != j["state"]:
                        upd.update(state=row["state"], changed=self.now())
                        if row["state"] == slurm.RUNNING and j["t_start"] is None:
                            upd["t_start"] = self.now()
                        changed.add((j["project"], j["batch"]))
                    self.db.update("jobs", {"id": j["id"]}, **upd)
                self._note_unregistered(cluster, listed)
                attached = [pname for pname, p in self.projects.items() if cluster in p.clusters]
            for pname in attached:
                try:
                    self._dispatch(pname, cluster)
                except TransportError as e:
                    log.warning("could not submit held jobs on %s for %s: %s", cluster, pname, e)
        if changed:
            self.on_change(None)
        return changed

    def _ask_ended(self, ex, jobs: list[dict]) -> tuple[dict, dict]:
        acct = {}
        r = ex.run(slurm.sacct_cmd([j["slurm_id"] for j in jobs]), timeout=SYNC_TIMEOUT)
        if r.rc == 0:
            acct = slurm.parse_sacct(r.out)
        else:
            log.info("sacct unavailable: %s", r.err.strip()[-200:])
        rest = [j for j in jobs if not (acct.get(j["slurm_id"]) or {}).get("state")]
        if not rest:
            return acct, {}
        q = shlex.quote
        mark = "@@HPCP_%s" % secrets.token_hex(6)
        parts = []
        for j in rest:
            base = "%s/%s/%s" % (j["workdir"], HOOK_DIR, j["slurm_id"])
            parts.append(
                "printf '%s J %s\\n'; cat %s %s_*.start 2>/dev/null; printf '\\n%s E\\n'; "
                "cat %s %s_*.end 2>/dev/null; printf '\\n%s C\\n'; "
                "%sscontrol show job %s 2>/dev/null"
                % (mark, j["slurm_id"], q(base + ".start"), q(base), mark,
                   q(base + ".end"), q(base), mark, slurm.TIME_ENV, j["slurm_id"])
            )
        r = ex.run("bash -s", input="\n".join(parts) + "\n", timeout=SYNC_TIMEOUT)
        sections: dict[str, dict[str, str]] = {}
        cur, part = None, None
        for line in r.out.splitlines():
            if line.startswith(mark + " "):
                tag, _, val = line[len(mark) + 1:].partition(" ")
                if tag == "J":
                    cur, part = val, "start"
                    sections[cur] = {"start": "", "end": "", "ctl": ""}
                else:
                    part = {"E": "end", "C": "ctl"}[tag]
            elif cur is not None:
                sections[cur][part] += line + "\n"
        return acct, sections

    def _resolve(self, j: dict, acct: dict, sections: dict) -> bool:
        a = acct.get(j["slurm_id"])
        if a and a["state"] in slurm.IN_SLURM:
            self.db.update("jobs", {"id": j["id"]}, misses=0)
            return False
        if a and a["state"]:
            self._finish(j, a["state"], a["raw"], a["exit_code"], "sacct",
                         a["start"], a["end"], a["elapsed"])
            return True
        sec = sections.get(j["slurm_id"])
        if sec is None:
            return False
        hook = slurm.hook_summary(sec["start"], sec["end"])
        ctl = slurm.parse_scontrol(sec["ctl"])
        t0 = hook["start"]
        bad = slurm.CANCELLED if j["cancel_by"] else slurm.FAILED
        by_code = slurm.DONE if hook["exit_code"] == "0" else bad
        if ctl and ctl["state"] in slurm.IN_SLURM:
            self.db.update("jobs", {"id": j["id"]}, misses=0)
            return False
        if ctl and ctl["state"]:
            self._finish(
                j, ctl["state"], ctl["raw"], ctl["exit_code"], "scontrol",
                ctl["start"] or t0, ctl["end"] or hook["end"],
                ctl["elapsed"] if ctl["elapsed"] is not None else hook["elapsed"],
            )
        elif hook["complete"]:
            self._finish(j, by_code, None, hook["exit_code"], "hook", t0, hook["end"],
                         hook["elapsed"])
        elif j["misses"] + 1 >= LOST_AFTER:
            if hook["end"] is not None:
                self._finish(j, bad, None, hook["exit_code"], "hook", t0, hook["end"])
            elif j["cancel_by"]:
                self._finish(j, slurm.CANCELLED, None, None, "cancel", t0, None)
            else:
                self._finish(j, slurm.LOST, None, None, "none", t0, None)
        else:
            self.db.update("jobs", {"id": j["id"]}, misses=j["misses"] + 1)
            return False
        return True

    def _finish(self, j, state, raw, exit_code, source, start, end, elapsed=None) -> None:
        now = self.now()
        self.db.update(
            "jobs", {"id": j["id"]},
            state=state, slurm_state=raw, exit_code=exit_code, source=source,
            changed=now, misses=0,
            t_start=start if start is not None else j["t_start"],
            t_end=end if end is not None else now,
            run_s=elapsed,
        )

    def _note_unregistered(self, cluster: str, listed: dict) -> None:
        known = {
            j["slurm_id"] for j in self.db.all(
                "SELECT slurm_id FROM jobs WHERE cluster = ? AND slurm_id IS NOT NULL", (cluster,))
        }
        # Match unregistered jobs against every project attached to this cluster
        proj_roots = []
        for pname, p in self.projects.items():
            cl = p.clusters.get(cluster)
            if cl is None:
                continue
            real = self._real_root(pname, cluster)
            proj_roots.append((pname, cl.root, real))
        seen = set()
        for jid, row in listed.items():
            if jid in known:
                continue
            wd = row["workdir"]
            matched = None
            for pname, root, real in proj_roots:
                if inside(root, wd):
                    matched = (pname, wd)
                    break
                if real and inside(real, wd):
                    matched = (pname, under_root(root, real, wd))
                    break
            if matched is None:
                continue
            pname, shown_wd = matched
            seen.add(jid)
            self.db.run(
                "INSERT INTO unregistered (cluster, slurm_id, project, workdir, first_seen) "
                "VALUES (?, ?, ?, ?, ?) ON CONFLICT(cluster, slurm_id) DO UPDATE SET "
                "active = 1, project = excluded.project, workdir = excluded.workdir",
                (cluster, jid, pname, shown_wd, self.now()),
            )
        for u in self.db.all("SELECT slurm_id FROM unregistered WHERE cluster = ? AND active = 1",
                             (cluster,)):
            if u["slurm_id"] not in seen:
                self.db.update("unregistered", {"cluster": cluster, "slurm_id": u["slurm_id"]},
                               active=0)

    # ------------------------------------------------------------------ inspection for Web UI
    def read_job_files(self, job_id: int, max_bytes: int = 32768) -> dict:
        """Read original script and Slurm stdout/stderr tail for a registered job."""
        with self.lock:
            j = self.db.one("SELECT * FROM jobs WHERE id = ?", (job_id,))
        if j is None:
            raise JobError("unknown_jobs", ids=str(job_id))
        ex = self.ex.get(j["cluster"])
        if ex is None:
            raise JobError("unknown_cluster", cluster=j["cluster"], clusters="")
        q = shlex.quote
        sid = j["slurm_id"] or ""
        mark = "@@HPCP_%s" % secrets.token_hex(6)
        script_path = "%s/%s" % (j["workdir"], j["script"])
        cmd = [
            "printf '%s SCRIPT\\n'" % mark,
            "head -c %d -- %s 2>/dev/null || true" % (max_bytes, q(script_path)),
            "printf '\\n%s OUT\\n'" % mark,
        ]
        if sid:
            # Resolve StdOut from scontrol if available, fallback to slurm-<id>.out
            cmd += [
                "cd -- %s 2>/dev/null || true" % q(j["workdir"]),
                "f=$(scontrol show job %s 2>/dev/null | tr ' ' '\\n' | "
                "awk -F= '/^StdOut=/{print $2; exit}')" % q(sid),
                "[ -n \"$f\" ] && [ -f \"$f\" ] || f=\"slurm-%s.out\"" % sid,
                "[ -f \"$f\" ] && tail -c %d -- \"$f\" 2>/dev/null || true" % max_bytes,
                "printf '\\n%s ERR\\n'" % mark,
                "e=$(scontrol show job %s 2>/dev/null | tr ' ' '\\n' | "
                "awk -F= '/^StdErr=/{print $2; exit}')" % q(sid),
                "if [ -n \"$e\" ] && [ \"$e\" != \"$f\" ] && [ -f \"$e\" ]; then "
                "tail -c %d -- \"$e\" 2>/dev/null || true; fi" % max_bytes,
            ]
        else:
            cmd += ["printf '\\n%s ERR\\n'" % mark]
        try:
            r = ex.run("bash -s", input="\n".join(cmd) + "\n", timeout=30)
        except TransportError as e:
            raise JobError("unreachable", cluster=j["cluster"], script=None, error=str(e),
                           n_sent=0, n_unsent=0, ids="")
        parts = {"SCRIPT": "", "OUT": "", "ERR": ""}
        cur = None
        for line in r.out.splitlines(keepends=True):
            if line.startswith(mark + " "):
                cur = line[len(mark) + 1:].strip()
            elif cur in parts:
                parts[cur] += line
        return {"job": j, "script": parts["SCRIPT"], "stdout": parts["OUT"],
                "stderr": parts["ERR"]}

    # ------------------------------------------------------------------ queries
    def is_unfinished(self, project: str, batch: str) -> bool:
        return self.db.one(
            "SELECT 1 FROM jobs WHERE project = ? AND batch = ? AND state IN (%s) LIMIT 1"
            % slurm.marks(slurm.ACTIVE),
            (project, batch) + slurm.ACTIVE,
        ) is not None

    def unfinished_batches(self, project: str) -> list[str]:
        rows = self.db.all(
            "SELECT DISTINCT b.batch FROM batches b JOIN jobs j "
            "ON j.project = b.project AND j.batch = b.batch "
            "WHERE b.project = ? AND j.state IN (%s) ORDER BY b.created"
            % slurm.marks(slurm.ACTIVE),
            (project,) + slurm.ACTIVE,
        )
        return [r["batch"] for r in rows]

    def summary(self, project: str, batch: str) -> dict | None:
        b = self.db.one("SELECT * FROM batches WHERE project = ? AND batch = ?", (project, batch))
        if b is None:
            return None
        counts: dict[str, int] = {}
        for r in self.db.all(
            "SELECT state, COUNT(*) AS n FROM jobs WHERE project = ? AND batch = ? GROUP BY state",
            (project, batch),
        ):
            counts[r["state"]] = r["n"]
        n = sum(counts.values())
        rate = sum(counts.get(s, 0) for s in slurm.FAILURES) / n if n else 0.0
        last = self.db.one(
            "SELECT MAX(changed) AS t FROM jobs WHERE project = ? AND batch = ?",
            (project, batch),
        )
        return {
            "project": project,
            "batch": batch,
            "desc": b["desc"],
            "cluster": b["cluster"],
            "tag": b["tag"],
            "workdir": b["workdir"],
            "jobs": counts,
            "n_jobs": n,
            "cancelled": self._cancelled(project, batch),
            "finished": n > 0 and not any(counts.get(s) for s in slurm.ACTIVE),
            "fail_rate": rate,
            "fail_alerted": bool(b["fail_alerted"]),
            "created": b["created"],
            "changed": last["t"] if last else b["created"],
        }

    def _cancelled(self, project: str, batch: str) -> dict:
        out: dict = {}
        for j in self.db.all(
            "SELECT cancel_by, cancel_note FROM jobs WHERE project = ? AND batch = ? "
            "AND state = ? ORDER BY id",
            (project, batch, slurm.CANCELLED),
        ):
            by = j["cancel_by"]
            key = "outside" if by is None else "user" if by == USER else "agent"
            out[key] = out.get(key, 0) + 1
            if j["cancel_note"] and j["cancel_note"] not in out.setdefault("notes", []):
                out["notes"].append(j["cancel_note"])
        return out

    def overview(self, project: str | None = None, recent: int = 20,
                 cluster: str | None = None) -> list[dict]:
        conds, args = [], []
        if project is not None:
            conds.append("project = ?")
            args.append(project)
        if cluster is not None:
            conds.append("cluster = ?")
            args.append(cluster)
        where = "WHERE " + " AND ".join(conds) if conds else ""
        rows = [
            self.summary(b["project"], b["batch"])
            for b in self.db.all("SELECT project, batch FROM batches %s ORDER BY created" % where,
                                 tuple(args))
        ]
        rows = [r for r in rows if r and r["n_jobs"]]
        open_ = [r for r in rows if not r["finished"]]
        done = sorted((r for r in rows if r["finished"]),
                      key=lambda r: r["changed"] or 0)[-recent:]
        return open_ + done[::-1]

    def job_rows(self, project: str, batch: str) -> list[dict]:
        return self.db.all(
            "SELECT * FROM jobs WHERE project = ? AND batch = ? ORDER BY id",
            (project, batch),
        )

    def unregistered(self, project: str | None = None) -> list[dict]:
        if project is not None:
            return self.db.all(
                "SELECT * FROM unregistered WHERE active = 1 AND project = ? ORDER BY first_seen",
                (project,),
            )
        return self.db.all("SELECT * FROM unregistered WHERE active = 1 ORDER BY first_seen")

    def cluster_usage(self, project: str, cluster: str) -> dict:
        r = self.db.all(
            "SELECT state, COUNT(*) AS n FROM jobs WHERE project = ? AND cluster = ? "
            "AND state IN (%s) GROUP BY state" % slurm.marks(slurm.ACTIVE),
            (project, cluster) + slurm.ACTIVE,
        )
        counts = {row["state"]: row["n"] for row in r}
        p = self.projects.get(project)
        max_jobs = p.clusters[cluster].max_jobs if p and cluster in p.clusters else 0
        return {
            "queued": counts.get(slurm.QUEUED, 0),
            "running": counts.get(slurm.RUNNING, 0),
            "held": counts.get(slurm.HELD, 0),
            "unapproved": counts.get(slurm.UNAPPROVED, 0),
            "in_slurm": counts.get(slurm.QUEUED, 0) + counts.get(slurm.RUNNING, 0),
            "max_jobs": max_jobs,
        }
