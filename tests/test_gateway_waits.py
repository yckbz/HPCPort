"""Tests for gateway (slurm + bare SSH), waits (batches + ops), MCP tasks store, and notices."""

from __future__ import annotations

import threading
import time

import pytest

from hpcport.daemon import Daemon
from hpcport.gateway import GatewayError, NeedsApproval
from tests.fake_slurm import FakeSlurmExecutor


def _setup_gateway(hpc_home, tmp_path):
    remote = tmp_path / "remote"
    remote.mkdir()
    local_dir = tmp_path / "workspace"
    local_dir.mkdir()
    ex_slurm = FakeSlurmExecutor(tmp_path / "bin")
    d = Daemon(executors={"slurm1": ex_slurm}, browser_opener=lambda url: True)
    d.settings.create_preset("slurm1", {
        "connect": "local",
        "scheduler": "slurm",
        "projects_root": str(remote),
        "exec_timeout_s": 60,
    })
    d.settings.create_preset("bare1", {
        "connect": "local",
        "scheduler": "none",
        "projects_root": str(remote),
        "exec_timeout_s": 300,
    })
    d.settings.create_project("p1", local_dir=str(local_dir))
    d.attach_cluster("p1", "slurm1", root=str(remote / "p1_slurm"))
    d.attach_cluster("p1", "bare1", root=str(remote / "p1_bare"))
    return d, ex_slurm, local_dir, remote


def test_bare_server_exec_background_wait_and_notices(hpc_home, tmp_path):
    d, _, local_dir, remote = _setup_gateway(hpc_home, tmp_path)
    # Initialize cursor for session A
    d.waits.init_cursor("p1", "sessA")

    # Upload a local script and run in foreground on bare server
    script = local_dir / "hello.sh"
    script.write_text("echo HI_FROM_SCRIPT\n")
    r = d.gateway.exec("p1", "bare1", "bash hello.sh", upload=["hello.sh"])
    assert r["status"] == "ended"
    assert r["op"]["result"]["rc"] == 0
    assert "HI_FROM_SCRIPT" in r["op"]["result"]["head"]

    # On bare server, sbatch is NOT refused by guard
    r_bg = d.gateway.exec("p1", "bare1", "sleep 0.3 && echo DONE_BG", background=True)
    assert r_bg["status"] == "background"
    oid = r_bg["op"]["id"]

    # Register a wait on op:<id> and sync-wait while daemon polls
    with d.lock:
        w = d.waits.register("p1", "sessA", groups=[{"items": ["op:%d" % oid], "priority": 1}])
        d.waits.arm_sync_wait(w["id"])

    def poller():
        for _ in range(20):
            d.gateway.poll_s = 0.01
            d.gateway.poll()
            with d.lock:
                d.waits.evaluate()
            time.sleep(0.05)

    t = threading.Thread(target=poller, daemon=True)
    t.start()
    fired = d.waits.wait_sync(w["id"], wait_s=3.0)
    t.join(timeout=2)
    assert fired is not None
    assert fired["reason"] == "done"
    assert ("op:%d" % oid) in fired["finished"]

    # Notices cursor captures op_ended cleanly
    notices = d.waits.pop_notices("p1", "sessA")
    assert any(n["reason"] == "op_ended" and n["subject"] == "op:%d" % oid for n in notices)
    # Second pop is empty
    assert d.waits.pop_notices("p1", "sessA") == []


def test_slurm_gateway_guard_and_transfer(hpc_home, tmp_path):
    d, _, local_dir, remote = _setup_gateway(hpc_home, tmp_path)
    # sbatch is refused on slurm1
    with pytest.raises(GatewayError) as exc:
        d.gateway.exec("p1", "slurm1", "sbatch run.sh")
    assert exc.value.code == "use_submit"

    # Destructive outside path requires approval; timeout returns NeedsApproval
    d.permissions.wait_s = 0.05
    with pytest.raises(NeedsApproval) as na:
        d.gateway.exec("p1", "slurm1", "rm -rf /tmp_outside_fake_dir")
    aid = na.value.answer["approval"]
    assert na.value.answer["pending"] is True

    # Approve it -> grants one-time pass on exact retry
    with d.lock:
        d.approvals.decide(aid, True, "ok once")
    r = d.gateway.exec("p1", "slurm1", "rm -rf /tmp_outside_fake_dir")
    assert r["status"] == "ended"
    assert r["op"]["result"]["rc"] == 0

    # Transfer local -> remote -> local
    (local_dir / "data.txt").write_text("payload123")
    tr = d.gateway.transfer("p1", "data.txt", "bare1:data_copy.txt")
    assert tr["status"] == "ended"
    assert (remote / "p1_bare" / "data_copy.txt").read_text() == "payload123"


def test_mcp_task_lifecycle_and_cancel(hpc_home, tmp_path):
    d, _, _, _ = _setup_gateway(hpc_home, tmp_path)
    r_bg = d.gateway.exec("p1", "bare1", "sleep 30", background=True)
    oid = r_bg["op"]["id"]
    with d.lock:
        w = d.waits.register("p1", "sessT", groups=[{"items": ["op:%d" % oid], "priority": 1}])
    task = d.waits.create_task("p1", w["id"])
    assert task["status"] == "working"
    got = d.waits.get_task(task["task_id"])
    assert got["task_id"] == task["task_id"]
    cancelled = d.waits.cancel_task(task["task_id"])
    assert cancelled["status"] == "cancelled"
    # Stop the background sleep cleanly
    d.gateway.stop("p1", oid)
