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


def test_wait_sync_no_abba_deadlock(hpc_home, tmp_path):
    """Stress-test wait_sync() vs concurrent evaluate() under daemon.lock (no AB-BA deadlock)."""
    d, _, _, _ = _setup_gateway(hpc_home, tmp_path)
    r = d.gateway.exec("p1", "bare1", "sleep 20", background=True)
    oid = r["op"]["id"]
    with d.lock:
        w = d.waits.register("p1", "s_dl", groups=[{"items": ["op:%d" % oid], "priority": 1}])
        d.waits.arm_sync_wait(w["id"])

    # Force very short TICK_S so wait_sync loops & acquires self.lock repeatedly while
    # a second thread holds self.lock and calls evaluate() -> self._cond.
    import hpcport.waits as waits_mod
    old_tick = waits_mod.TICK_S
    waits_mod.TICK_S = 0.005
    stop_ev = threading.Event()
    ticks = []
    try:
        def evaluator():
            while not stop_ev.is_set():
                with d.lock:
                    d.waits.evaluate()
                time.sleep(0.002)

        t_eval = threading.Thread(target=evaluator, daemon=True)
        t_eval.start()
        result = []

        def waiter():
            result.append(d.waits.wait_sync(w["id"], 2.0, on_tick=lambda e, tot, m: ticks.append(m)))

        t_wait = threading.Thread(target=waiter, daemon=True)
        t_wait.start()
        time.sleep(0.06)
        d.gateway.stop("p1", oid, by="user")
        t_wait.join(timeout=3.0)
        stop_ev.set()
        t_eval.join(timeout=2.0)
        assert not t_wait.is_alive(), "wait_sync deadlocked with evaluate()!"
        assert len(result) == 1 and result[0] is not None
        assert result[0]["reason"] == "done"
    finally:
        waits_mod.TICK_S = old_tick


def test_upload_and_transfer_block_symlink_escape_and_unsafe_cwd(hpc_home, tmp_path):
    d, _, local_dir, remote = _setup_gateway(hpc_home, tmp_path)
    (local_dir / "payload.txt").write_text("secret_payload", encoding="utf-8")

    outside = tmp_path / "outside_target"
    outside.mkdir()
    (outside / "host_secret.txt").write_text("top_secret", encoding="utf-8")
    root = remote / "p1_bare"
    escape = root / "escape"
    escape.symlink_to(outside)

    # 1. remote_exec upload into symlink escaping root must fail BEFORE rsync writes anything
    with pytest.raises(GatewayError) as exc:
        d.gateway.exec("p1", "bare1", "ls", cwd="escape", upload=["payload.txt"])
    assert exc.value.code == "bad_cwd"
    assert not (outside / "payload.txt").exists()

    # 2. remote_exec upload into non-existent cwd must fail with no_cwd without creating dir
    missing = root / "does_not_exist"
    with pytest.raises(GatewayError) as exc2:
        d.gateway.exec("p1", "bare1", "ls", cwd="does_not_exist", upload=["payload.txt"])
    assert exc2.value.code == "no_cwd"
    assert not missing.exists()

    # 3. remote_exec upload with shell metacharacters / spaces in cwd must be rejected
    with pytest.raises(GatewayError) as exc3:
        d.gateway.exec("p1", "bare1", "ls", cwd="sub$(touch /tmp/nope)", upload=["payload.txt"])
    assert exc3.value.code == "bad_path"

    # 4. transfer into or out of symlink escaping root must be blocked BEFORE rsync runs
    with pytest.raises(GatewayError) as exc4:
        d.gateway.transfer("p1", "payload.txt", "bare1:escape/payload.txt")
    assert exc4.value.code == "remote_outside"
    assert not (outside / "payload.txt").exists()

    with pytest.raises(GatewayError) as exc5:
        d.gateway.transfer("p1", "bare1:escape/host_secret.txt", "stolen.txt")
    assert exc5.value.code == "remote_outside"
    assert not (local_dir / "stolen.txt").exists()

    # Chain of two file symlinks escaping root must also be blocked
    link2 = root / "link_to_secret"
    link2.symlink_to(outside / "host_secret.txt")
    link1 = root / "link_chain"
    link1.symlink_to("link_to_secret")
    with pytest.raises(GatewayError) as exc6:
        d.gateway.transfer("p1", "bare1:link_chain", "stolen2.txt")
    assert exc6.value.code == "remote_outside"
    assert not (local_dir / "stolen2.txt").exists()
