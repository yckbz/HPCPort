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
