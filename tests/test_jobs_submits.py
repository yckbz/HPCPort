"""Tests for multi-project Slurm jobs, per-project quotas, standing budgets, and 180s short wait."""

from __future__ import annotations

import threading
import time

import pytest

from hpcport.daemon import Daemon
from hpcport.jobs import JobError
from tests.fake_slurm import FakeSlurmExecutor


def _setup_two_projects(hpc_home, tmp_path):
    remote_base = tmp_path / "remote"
    remote_base.mkdir()
    ex = FakeSlurmExecutor(tmp_path / "fakebin")
    d = Daemon(executors={"gpu01": ex}, browser_opener=lambda url: True)
    d.settings.create_preset("gpu01", {
        "connect": "local",
        "scheduler": "slurm",
        "partitions": ["a100", "debug"],
        "max_jobs": 2,
        "projects_root": str(remote_base),
    })
    d.settings.create_project("projA", local_dir=str(tmp_path / "localA"))
    d.settings.create_project("projB", local_dir=str(tmp_path / "localB"))
    (tmp_path / "localA").mkdir()
    (tmp_path / "localB").mkdir()
    d.attach_cluster("projA", "gpu01")
    d.attach_cluster("projB", "gpu01", max_jobs=1)
    return d, ex, remote_base


def _write_script(path, partition="a100", time_s="01:00:00", ntasks=4, gpus=1):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "#!/bin/bash\n"
        "#SBATCH -p %s\n"
        "#SBATCH -n %d\n"
        "#SBATCH --gres=gpu:%d\n"
        "#SBATCH -t %s\n"
        "echo run\n" % (partition, ntasks, gpus, time_s)
    )


def test_flexible_dirs_inline_approval_and_per_project_quotas(hpc_home, tmp_path):
    d, ex, remote = _setup_two_projects(hpc_home, tmp_path)
    # Arbitrary subdir name (no <sid>.rNN required!)
    wdA = remote / "projA" / "exp" / "lr_1e-4"
    _write_script(wdA / "s1.sh")
    _write_script(wdA / "s2.sh")
    _write_script(wdA / "s3.sh")

    # Approve inline from a background thread while submit() short-waits!
    def approve_soon():
        for _ in range(50):
            pending = d.approvals.list(pending_only=True, project="projA")
            if pending:
                with d.lock:
                    d.approvals.decide(pending[0]["id"], True, "looks good",
                                       always=True, total_jobs=10)
                return
            time.sleep(0.02)

    t = threading.Thread(target=approve_soon, daemon=True)
    t.start()
    res = d.jobs.submit(
        "projA", "gpu01", "exp/lr_1e-4", ["s1.sh", "s2.sh", "s3.sh"],
        desc="sweep lr", batch="sweep-1", tag="train", wait_s=5.0,
    )
    t.join(timeout=2)
    assert res["approved_inline"] is True
    assert res["batch"] == "sweep-1"
    assert res["n"] == 3
    assert res["n_sent"] == 2          # projA max_jobs=2
    assert res["n_held"] == 1

    # projB has completely independent standing approval & max_jobs=1
    wdB = remote / "projB" / "bench"
    _write_script(wdB / "b1.sh")
    resB = d.jobs.submit("projB", "gpu01", str(wdB), ["b1.sh"], desc="bench", wait_s=0.0)
    assert resB["approval"] is not None
    assert resB["batch"] == "b001"     # auto-generated short ID

    # Single squeue call syncs BOTH projects
    before = ex.squeue_calls
    ex.finish(res["ids"][0], "COMPLETED", "0:0", run_s=120.0)
    d.cycle(force=True)
    assert ex.squeue_calls == before + 1
    # 3rd held job of projA was automatically dispatched into the freed slot!
    rowsA = d.jobs.job_rows("projA", "sweep-1")
    assert sum(1 for j in rowsA if j["slurm_id"]) == 3
    assert sum(1 for j in rowsA if j["state"] == "done") == 1


def test_dynamic_recheck_auto_releases_when_budget_frees(hpc_home, tmp_path):
    d, ex, remote = _setup_two_projects(hpc_home, tmp_path)
    wd = remote / "projA" / "budget_test"
    # Each script asks for 4 core-h (4 cores * 1h)
    _write_script(wd / "a.sh", ntasks=4, gpus=0, time_s="01:00:00")
    _write_script(wd / "b.sh", ntasks=4, gpus=0, time_s="01:00:00")
    _write_script(wd / "c.sh", ntasks=4, gpus=0, time_s="01:00:00")

    r1 = d.jobs.submit("projA", "gpu01", str(wd), ["a.sh"], desc="job a", wait_s=0)
    with d.lock:
        # Standing approval allows total_core_hours=5.0; a.sh doesn't count against new standing
        d.approvals.decide(r1["approval"], True, always=True, total_core_hours=5.0)
    d.jobs.dispatch_wanted()

    # b.sh (4 core-h <= 5.0) goes straight through
    r2 = d.jobs.submit("projA", "gpu01", str(wd), ["b.sh"], desc="job b", wait_s=0)
    assert r2["approval"] is None and r2["n_sent"] == 1

    # c.sh (4 + 4 = 8 > 5.0) waits for approval
    r3 = d.jobs.submit("projA", "gpu01", str(wd), ["c.sh"], desc="job c", wait_s=0)
    assert r3["approval"] is not None

    # b.sh finishes in 36s (0.04 core-h actual!) -> recheck auto-releases c.sh!
    ex.finish(r2["ids"][0], "COMPLETED", "0:0", run_s=36.0)
    d.cycle(force=True)
    rows_c = d.jobs.job_rows("projA", r3["batch"])
    assert rows_c[0]["slurm_id"] is not None
    assert d.approvals.get(r3["approval"])["state"] == "cancelled"


def test_cancel_and_not_slurm_rejection(hpc_home, tmp_path):
    d, ex, remote = _setup_two_projects(hpc_home, tmp_path)
    # Bare SSH server rejects submit cleanly
    d.settings.create_preset("bare1", {
        "connect": "local",
        "scheduler": "none",
        "projects_root": str(remote),
    })
    d.attach_cluster("projA", "bare1")
    wd_bare = remote / "projA"
    _write_script(wd_bare / "x.sh")
    with pytest.raises(JobError) as exc:
        d.jobs.submit("projA", "bare1", str(wd_bare), ["x.sh"], desc="nope", wait_s=0)
    assert exc.value.code == "not_slurm"

    # Withdraw unapproved submission via cancel(approval=...)
    wd = remote / "projA" / "w1"
    _write_script(wd / "j1.sh")
    r = d.jobs.submit("projA", "gpu01", str(wd), ["j1.sh"], desc="to cancel", wait_s=0)
    aid = r["approval"]
    assert aid is not None
    cr = d.jobs.cancel("projA", approval=aid)
    assert len(cr["cancelled"]) == 1
    with d.lock:
        d.submits.sweep()
    assert d.approvals.get(aid)["state"] == "cancelled"


def test_lost_sbatch_receipt_reconciles_without_duplicate(hpc_home, tmp_path):
    d, ex, remote = _setup_two_projects(hpc_home, tmp_path)
    from hpcport import slurm
    # Turn off approval gate so submit dispatches immediately
    d.settings.update_project("projA", {"submit": {"approval": False}})
    wd = remote / "projA" / "drop_run"
    _write_script(wd / "job.sh")

    # Simulate Slurm accepting the job & writing .sid, then SSH dropping before receipt returns
    ex.drop_after_sbatch = True
    with pytest.raises(JobError) as exc:
        d.jobs.submit("projA", "gpu01", str(wd), ["job.sh"], desc="flaky ssh", wait_s=0)
    assert exc.value.code == "unreachable_dispatch"
    assert len(ex.jobs) == 1
    orig_sid = next(iter(ex.jobs))

    # Row was kept as held -> immediate retry is blocked as duplicate (no 2nd Slurm job!)
    with pytest.raises(JobError) as dup_exc:
        d.jobs.submit("projA", "gpu01", str(wd), ["job.sh"], desc="retry", wait_s=0)
    assert dup_exc.value.code == "duplicate"
    assert len(ex.jobs) == 1

    # Reconnect / cycle reconciles .hpcport/job<id>.sid without calling sbatch again!
    d.cycle(force=True)
    assert len(ex.jobs) == 1
    rows = d.db.all("SELECT * FROM jobs WHERE project = 'projA'")
    assert len(rows) == 1
    assert rows[0]["slurm_id"] == orig_sid
    assert rows[0]["state"] == slurm.QUEUED


def test_multicluster_same_slurm_id_cancel_disambiguation(hpc_home, tmp_path):
    remote = tmp_path / "remote"
    remote.mkdir()
    ex1 = FakeSlurmExecutor(tmp_path / "bin1")
    ex2 = FakeSlurmExecutor(tmp_path / "bin2")
    d = Daemon(executors={"c1": ex1, "c2": ex2}, browser_opener=lambda url: True)
    for cname in ("c1", "c2"):
        d.settings.create_preset(cname, {
            "connect": "local", "scheduler": "slurm", "partitions": ["a100"],
            "max_jobs": 4, "projects_root": str(remote),
        })
    d.settings.create_project("multi", data={"submit": {"approval": False}})
    d.attach_cluster("multi", "c1", root=str(remote / "m_c1"))
    d.attach_cluster("multi", "c2", root=str(remote / "m_c2"))

    wd1 = remote / "m_c1" / "r1"
    wd2 = remote / "m_c2" / "r1"
    _write_script(wd1 / "j.sh")
    _write_script(wd2 / "j.sh")
    r1 = d.jobs.submit("multi", "c1", str(wd1), ["j.sh"], desc="on c1", wait_s=0)
    r2 = d.jobs.submit("multi", "c2", str(wd2), ["j.sh"], desc="on c2", wait_s=0)
    # Both fake Slurm clusters start at 1000 -> colliding Slurm ID!
    assert r1["ids"] == ["1000"]
    assert r2["ids"] == ["1000"]

    # Unqualified cancel MUST be rejected as ambiguous and leave BOTH clusters intact
    with pytest.raises(JobError) as exc:
        d.jobs.cancel("multi", job_ids=["1000"])
    assert exc.value.code == "ambiguous_job_id"
    assert ex1.jobs["1000"]["state"] == "PENDING"
    assert ex2.jobs["1000"]["state"] == "PENDING"

    # Explicit cluster parameter cancels ONLY c1
    d.jobs.cancel("multi", job_ids=["1000"], cluster="c1")
    assert ex1.jobs["1000"]["state"] == "CANCELLED"
    assert ex2.jobs["1000"]["state"] == "PENDING"

    # cluster:id syntax cancels c2
    d.jobs.cancel("multi", job_ids=["c2:1000"])
    assert ex2.jobs["1000"]["state"] == "CANCELLED"


def test_real_bash_sbatch_rejection_does_not_retry(hpc_home, tmp_path):
    """Real LocalExecutor + fake sbatch returning invalid account must fail once, never retry."""
    import os
    from hpcport import slurm
    from hpcport.executor import LocalExecutor

    fakebin = tmp_path / "real_fakebin"
    fakebin.mkdir()
    calls_log = tmp_path / "sbatch_calls.log"
    sbatch_bin = fakebin / "sbatch"
    sbatch_bin.write_text(
        "#!/bin/bash\n"
        "echo \"$*\" >> %r\n"
        "echo 'sbatch: error: Invalid account or account/partition combination specified' >&2\n"
        "exit 1\n" % str(calls_log),
        encoding="utf-8",
    )
    sbatch_bin.chmod(0o755)
    squeue_bin = fakebin / "squeue"
    squeue_bin.write_text("#!/bin/bash\nexit 0\n", encoding="utf-8")
    squeue_bin.chmod(0o755)

    env = dict(os.environ, PATH="%s:%s" % (fakebin, os.environ.get("PATH", "")))
    ex = LocalExecutor(env=env)
    remote = tmp_path / "remote_real"
    remote.mkdir()
    d = Daemon(executors={"real1": ex}, browser_opener=lambda url: True)
    d.settings.create_preset("real1", {
        "connect": "local", "scheduler": "slurm", "partitions": ["a100"],
        "max_jobs": 4, "projects_root": str(remote),
    })
    d.settings.create_project("projR", data={"submit": {"approval": False}})
    d.attach_cluster("projR", "real1")
    wd = remote / "projR" / "bad_acct"
    _write_script(wd / "job.sh")

    with pytest.raises(JobError) as exc:
        d.jobs.submit("projR", "real1", str(wd), ["job.sh"], desc="bad account", wait_s=0)
    assert exc.value.code == "sbatch_rejected"
    assert "Invalid account" in exc.value.params["error"]

    # Two sync cycles must NOT re-invoke sbatch; no held rows remain
    d.cycle(force=True)
    d.cycle(force=True)
    assert len(calls_log.read_text(encoding="utf-8").splitlines()) == 1
    assert d.db.all("SELECT * FROM jobs WHERE project = 'projR' AND state = ?", (slurm.HELD,)) == []


def test_real_bash_dispatch_reconcile_cancel_and_fresh_db(hpc_home, tmp_path, monkeypatch):
    """Test real Bash execution for:
    1) drop before .sid written -> squeue %F|%o reconciles without duplicate sbatch;
    2) cancel before recovery -> reconciles .sid and scancels remote job;
    3) unreachable cancel of uncertain job -> raises unreachable (never lies cancelled);
    4) fresh state DB reusing same workdir -> ignores stale .sid and submits new job.
    """
    import json as _json
    import os
    from hpcport import slurm
    from hpcport.executor import LocalExecutor, TransportError

    fakebin = tmp_path / "slurm_bin"
    fakebin.mkdir()
    state_file = tmp_path / "slurm_state.json"
    state_file.write_text(_json.dumps({"next_id": 2000, "calls": 0, "jobs": {}}), encoding="utf-8")

    # Executable fake sbatch / squeue / scancel / scontrol using python3
    import sys as _sys
    py = _sys.executable
    (fakebin / "sbatch").write_text(
        "#!%s\nimport json, os, sys\n"
        "sf = %r\n"
        "d = json.load(open(sf))\n"
        "d['calls'] += 1\n"
        "jid = str(d['next_id']); d['next_id'] += 1\n"
        "cmd = sys.argv[-1]\n"
        "d['jobs'][jid] = {'state': 'PENDING', 'workdir': os.getcwd(), 'cmd': cmd}\n"
        "json.dump(d, open(sf, 'w'))\n"
        "print(jid)\n" % (py, str(state_file)),
        encoding="utf-8",
    )
    (fakebin / "squeue").write_text(
        "#!%s\nimport json, sys\n"
        "d = json.load(open(%r))\n"
        "fmt = ''\n"
        "for i, a in enumerate(sys.argv):\n"
        "    if a == '-o' and i + 1 < len(sys.argv): fmt = sys.argv[i+1]\n"
        "for jid, j in d['jobs'].items():\n"
        "    if j['state'] in ('PENDING', 'RUNNING'):\n"
        "        if '%%o' in fmt: print(f\"{jid}|{j['cmd']}\")\n"
        "        else: print(f\"{jid}|{j['state']}|{j['workdir']}\")\n" % (py, str(state_file)),
        encoding="utf-8",
    )
    (fakebin / "scancel").write_text(
        "#!%s\nimport json, sys\n"
        "sf = %r\n"
        "d = json.load(open(sf))\n"
        "for jid in sys.argv[1:]:\n"
        "    if jid in d['jobs']: d['jobs'][jid]['state'] = 'CANCELLED'\n"
        "json.dump(d, open(sf, 'w'))\n" % (py, str(state_file)),
        encoding="utf-8",
    )
    (fakebin / "scontrol").write_text(
        "#!%s\nimport json, sys\n"
        "d = json.load(open(%r))\n"
        "jid = sys.argv[-1]\n"
        "j = d['jobs'].get(jid)\n"
        "if j: print(f\"JobId={jid} JobState={j['state']} ExitCode=0:0 StartTime=Unknown EndTime=1000\")\n"
        % (py, str(state_file)),
        encoding="utf-8",
    )
    for name in ("sbatch", "squeue", "scancel", "scontrol"):
        (fakebin / name).chmod(0o755)

    env = dict(os.environ, PATH="%s:%s" % (fakebin, os.environ.get("PATH", "")))

    class FlakyLocalExecutor(LocalExecutor):
        def __init__(self, env):
            super().__init__(env=env)
            self.drop_next_sbatch = False
            self.strip_sid_on_drop = False
            self.down = False

        def run(self, cmd, input=None, timeout=120):
            if self.down:
                raise TransportError("cluster offline")
            r = super().run(cmd, input=input, timeout=timeout)
            if self.drop_next_sbatch and "sbatch --parsable" in cmd:
                self.drop_next_sbatch = False
                if self.strip_sid_on_drop:
                    self.strip_sid_on_drop = False
                    # Remove .sid to simulate crash AFTER sbatch accepted, BEFORE .sid hit disk
                    for p in (remote / "p1").rglob("*.sid"):
                        p.unlink()
                raise TransportError("ssh dropped mid-dispatch")
            return r

    ex = FlakyLocalExecutor(env)
    remote = tmp_path / "remote_flaky"
    remote.mkdir()
    d = Daemon(executors={"c1": ex}, browser_opener=lambda url: True)
    d.settings.create_preset("c1", {
        "connect": "local", "scheduler": "slurm", "partitions": ["a100"],
        "max_jobs": 4, "projects_root": str(remote),
    })
    d.settings.create_project("p1", data={"submit": {"approval": False}})
    d.attach_cluster("p1", "c1")
    wd = remote / "p1" / "exp"
    _write_script(wd / "a.sh")
    _write_script(wd / "b.sh")

    # 1) Slurm accepts a.sh, .sid was NOT written before drop -> cycle reconciles via squeue %F|%o!
    ex.drop_next_sbatch = True
    ex.strip_sid_on_drop = True
    with pytest.raises(JobError) as e1:
        d.jobs.submit("p1", "c1", str(wd), ["a.sh"], desc="no sid drop", wait_s=0)
    assert e1.value.code == "unreachable_dispatch"
    assert _json.loads(state_file.read_text())["calls"] == 1
    assert list((wd / ".hpcport").glob("*.sid")) == []

    d.cycle(force=True)
    st = _json.loads(state_file.read_text())
    assert st["calls"] == 1   # NO duplicate sbatch!
    row_a = d.db.one("SELECT * FROM jobs WHERE script = 'a.sh'")
    assert row_a["slurm_id"] == "2000"
    assert row_a["state"] == slurm.QUEUED

    # 2) Slurm accepts b.sh, .sid IS written, drop occurs -> cancel BEFORE recovery!
    ex.drop_next_sbatch = True
    with pytest.raises(JobError) as e2:
        d.jobs.submit("p1", "c1", str(wd), ["b.sh"], desc="cancel before rec", wait_s=0)
    assert e2.value.code == "unreachable_dispatch"
    row_b = d.db.one("SELECT * FROM jobs WHERE script = 'b.sh'")
    assert row_b["state"] == slurm.HELD and row_b["slurm_id"] is None

    # 2a) If cluster is STILL offline, cancel MUST raise unreachable (not lie cancelled!)
    ex.down = True
    with pytest.raises(JobError) as e_down:
        d.jobs.cancel("p1", job_ids=["#%d" % row_b["id"]])
    assert e_down.value.code == "unreachable"
    assert d.db.one("SELECT state FROM jobs WHERE id = ?", (row_b["id"],))["state"] == slurm.HELD

    # 2b) Once reachable, cancel reconciles .sid and actually scancels remote job 2001!
    ex.down = False
    cr = d.jobs.cancel("p1", job_ids=["#%d" % row_b["id"]])
    assert "2001" in cr["cancelled"]
    assert _json.loads(state_file.read_text())["jobs"]["2001"]["state"] == "CANCELLED"
    row_b2 = d.db.one("SELECT * FROM jobs WHERE id = ?", (row_b["id"],))
    assert row_b2["slurm_id"] == "2001" and row_b2["state"] == slurm.CANCELLED

    # 3) Fresh state DB reusing the SAME workdir (still containing old .hpcport/*.sid & job1.sid!)
    (wd / ".hpcport" / "job1.sid").write_text("9999\n", encoding="utf-8")
    new_home = tmp_path / "fresh_hpc_home"
    new_home.mkdir()
    monkeypatch.setenv("HPCPORT_HOME", str(new_home))
    d2 = Daemon(executors={"c1": ex}, browser_opener=lambda url: True)
    d2.settings.create_preset("c1", {
        "connect": "local", "scheduler": "slurm", "partitions": ["a100"],
        "max_jobs": 4, "projects_root": str(remote),
    })
    d2.settings.create_project("p1", data={"submit": {"approval": False}})
    d2.attach_cluster("p1", "c1", root=str(remote / "p1"))
    _write_script(wd / "c.sh")
    before_calls = _json.loads(state_file.read_text())["calls"]
    r_new = d2.jobs.submit("p1", "c1", str(wd), ["c.sh"], desc="fresh db", wait_s=0)
    assert _json.loads(state_file.read_text())["calls"] == before_calls + 1
    assert r_new["ids"] == ["2002"]   # freshly submitted, NOT stale 2000 or 9999!


def test_batch_auto_numbering_renotify_and_cluster_mismatch(hpc_home, tmp_path):
    remote = tmp_path / "remote_batches"
    remote.mkdir()
    ex1 = FakeSlurmExecutor(tmp_path / "b1")
    ex2 = FakeSlurmExecutor(tmp_path / "b2")
    d = Daemon(executors={"c1": ex1, "c2": ex2}, browser_opener=lambda url: True)
    for cname in ("c1", "c2"):
        d.settings.create_preset(cname, {
            "connect": "local", "scheduler": "slurm", "partitions": ["a100"],
            "max_jobs": 4, "projects_root": str(remote),
        })
    d.settings.create_project("pb", data={"submit": {"approval": False}})
    d.attach_cluster("pb", "c1", root=str(remote / "pb_c1"))
    d.attach_cluster("pb", "c2", root=str(remote / "pb_c2"))
    d.waits.init_cursor("pb", "sess1")

    wd1 = remote / "pb_c1" / "r1"
    wd2 = remote / "pb_c2" / "r1"
    _write_script(wd1 / "j1.sh")
    _write_script(wd1 / "j2.sh")
    _write_script(wd1 / "j3.sh")
    _write_script(wd2 / "k1.sh")

    # 1) Manual batch "b001" must NOT be collided with by next auto-numbered batch
    r_man = d.jobs.submit("pb", "c1", str(wd1), ["j1.sh"], desc="manual b001", batch="b001", wait_s=0)
    assert r_man["batch"] == "b001"
    r_auto = d.jobs.submit("pb", "c1", str(wd1), ["j2.sh"], desc="auto next", wait_s=0)
    assert r_auto["batch"] == "b002"

    # 2) Same batch name on second cluster MUST be rejected (prevents disappearing in cluster filter)
    with pytest.raises(JobError) as exc:
        d.jobs.submit("pb", "c2", str(wd2), ["k1.sh"], desc="cross cluster", batch="b001", wait_s=0)
    assert exc.value.code == "batch_cluster_mismatch"

    # 3) Batch finishes -> emits batch_done; append another job -> finishes again -> new batch_done!
    ex1.finish(r_man["ids"][0], "COMPLETED", "0:0", run_s=10.0)
    d.cycle(force=True)
    n1 = d.waits.pop_notices("pb", "sess1")
    assert any(n["reason"] == "batch_done" and n["subject"] == "batch:b001" for n in n1)

    r_app = d.jobs.submit("pb", "c1", str(wd1), ["j3.sh"], desc="append b001", batch="b001", wait_s=0)
    ex1.finish(r_app["ids"][0], "COMPLETED", "0:0", run_s=10.0)
    d.cycle(force=True)
    n2 = d.waits.pop_notices("pb", "sess1")
    assert any(n["reason"] == "batch_done" and n["subject"] == "batch:b001" for n in n2)


def test_db_migration_v1_to_v2(tmp_path):
    import sqlite3
    from hpcport.db import DB, SCHEMA_VERSION

    db_path = tmp_path / "v1.db"
    con = sqlite3.connect(str(db_path))
    con.executescript(
        """
        CREATE TABLE batches (
          project TEXT NOT NULL, batch TEXT NOT NULL, cluster TEXT NOT NULL,
          workdir TEXT NOT NULL, tag TEXT NOT NULL DEFAULT '', desc TEXT NOT NULL,
          fail_alerted INTEGER NOT NULL DEFAULT 0, created REAL NOT NULL,
          PRIMARY KEY (project, batch)
        );
        CREATE TABLE jobs (
          id INTEGER PRIMARY KEY AUTOINCREMENT, project TEXT NOT NULL, batch TEXT NOT NULL,
          submit_no INTEGER NOT NULL, cluster TEXT NOT NULL, workdir TEXT NOT NULL,
          script TEXT NOT NULL, state TEXT NOT NULL, t_register REAL NOT NULL
        );
        INSERT INTO batches VALUES ('p', 'b_done', 'c1', '/w', '', 'done', 0, 1.0);
        INSERT INTO batches VALUES ('p', 'b_open', 'c1', '/w', '', 'open', 0, 2.0);
        INSERT INTO jobs (project, batch, submit_no, cluster, workdir, script, state, t_register)
          VALUES ('p', 'b_done', 1, 'c1', '/w', 'a.sh', 'done', 1.0);
        INSERT INTO jobs (project, batch, submit_no, cluster, workdir, script, state, t_register)
          VALUES ('p', 'b_open', 2, 'c1', '/w', 'b.sh', 'running', 2.0);
        PRAGMA user_version = 1;
        """
    )
    con.close()

    db = DB(db_path)
    try:
        ver = db._con.execute("PRAGMA user_version").fetchone()[0]
        assert ver == SCHEMA_VERSION
        b_done = db.one("SELECT done_notified FROM batches WHERE batch = 'b_done'")
        b_open = db.one("SELECT done_notified FROM batches WHERE batch = 'b_open'")
        assert b_done["done_notified"] == 1
        assert b_open["done_notified"] == 0
        j = db.one("SELECT dispatch_token, dispatch_attempted FROM jobs WHERE batch = 'b_open'")
        assert j["dispatch_token"] is None and j["dispatch_attempted"] == 0
    finally:
        db.close()
