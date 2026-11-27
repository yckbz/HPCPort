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


def test_pop_notices_initializes_head_for_new_session(hpc_home, tmp_path):
    d, _, _, _ = _setup_gateway(hpc_home, tmp_path)
    d.waits.notify("p1", "batch_done", "batch:old", {"batch": "old", "cluster": "slurm1", "jobs": {}})
    assert d.waits.pop_notices("p1", "brand_new_session") == []
    d.waits.notify("p1", "batch_done", "batch:new", {"batch": "new", "cluster": "slurm1", "jobs": {}})
    got = d.waits.pop_notices("p1", "brand_new_session")
    assert len(got) == 1 and got[0]["subject"] == "batch:new"


def test_gateway_foreground_heartbeats_fast_stop_and_poll_lock(hpc_home, tmp_path):
    import hpcport.gateway as gw_mod

    d, _, local_dir, remote = _setup_gateway(hpc_home, tmp_path)
    (local_dir / "small.txt").write_text("hello", encoding="utf-8")

    # 1. max_background=0 blocks background=True but MUST NOT block foreground exec/transfer
    d.settings.update_project("p1", {"gateway": {"max_background": 0}})
    r_fg = d.gateway.exec("p1", "bare1", "echo FG_OK", background=False)
    assert r_fg["status"] == "ended" and "FG_OK" in r_fg["op"]["result"]["head"]
    r_tr = d.gateway.transfer("p1", "small.txt", "bare1:small.txt", background=False)
    assert r_tr["status"] == "ended" and r_tr["op"]["result"]["rc"] == 0
    with pytest.raises(GatewayError) as exc_bg:
        d.gateway.exec("p1", "bare1", "sleep 5", background=True)
    assert exc_bg.value.code == "too_many_background"
    with pytest.raises(GatewayError) as exc_tr_bg:
        d.gateway.transfer("p1", "small.txt", "bare1:small2.txt", background=True)
    assert exc_tr_bg.value.code == "too_many_background"

    # 2. Foreground exec sends progress heartbeats while running
    d.settings.update_project("p1", {"gateway": {"max_background": 4}})
    old_tick = gw_mod.TICK_S
    gw_mod.TICK_S = 0.05
    ticks = []
    try:
        r_hb = d.gateway.exec(
            "p1", "bare1", "sleep 0.25 && echo HB_DONE",
            background=False, on_tick=lambda e, tot, m: ticks.append(m),
        )
        assert r_hb["status"] == "ended" and "HB_DONE" in r_hb["op"]["result"]["head"]
        assert len(ticks) >= 1
    finally:
        gw_mod.TICK_S = old_tick

    # 3. background=True immediately followed by remote_stop cleanly ends (never lost)
    r_bg = d.gateway.exec("p1", "bare1", "sleep 30", background=True)
    oid = r_bg["op"]["id"]
    r_stop = d.gateway.stop("p1", oid, by="agent")
    assert r_stop["status"] == "ended"
    assert r_stop["op"]["state"] == "ended"
    assert r_stop["op"]["result"].get("stopped") is True

    # 4. gateway.poll() serializes concurrent callers
    overlap = []
    in_flight = [0]
    orig_poll_once = d.gateway._poll_once

    def slow_poll_once():
        in_flight[0] += 1
        if in_flight[0] > 1:
            overlap.append(True)
        time.sleep(0.05)
        in_flight[0] -= 1
        return []

    d.gateway._poll_once = slow_poll_once
    try:
        t1 = threading.Thread(target=d.gateway.poll)
        t2 = threading.Thread(target=d.gateway.poll)
        t1.start(); t2.start()
        t1.join(timeout=2); t2.join(timeout=2)
        assert overlap == []
    finally:
        d.gateway._poll_once = orig_poll_once


def test_transfer_protects_local_git_stored_output_fallback_and_staging_lock(hpc_home, tmp_path):
    d, _, local_dir, remote = _setup_gateway(hpc_home, tmp_path)
    (local_dir / ".git" / "hooks").mkdir(parents=True)
    (local_dir / ".git" / "config").write_text("SAFE_GIT_CONFIG", encoding="utf-8")

    # 1. Trailing-slash download into local project root MUST NOT overwrite .git (or .GIT)
    evil = remote / "p1_bare" / "evil"
    (evil / ".git").mkdir(parents=True)
    (evil / ".git" / "config").write_text("PWNED_LOWER", encoding="utf-8")
    (evil / "good.txt").write_text("OK_FILE", encoding="utf-8")
    r_tr = d.gateway.transfer("p1", "bare1:evil/", ".")
    assert r_tr["status"] == "ended" and r_tr["op"]["result"]["rc"] == 0
    assert (local_dir / ".git" / "config").read_text(encoding="utf-8") == "SAFE_GIT_CONFIG"
    assert (local_dir / "good.txt").read_text(encoding="utf-8") == "OK_FILE"

    with pytest.raises(GatewayError) as exc_git:
        d.gateway.transfer("p1", "bare1:evil/good.txt", ".GIT/config")
    assert exc_git.value.code == "local_protected"

    # 2. read_output falls back to stored SQLite output after subsequent exec cleans op dir
    r1 = d.gateway.exec("p1", "bare1", "echo STORED_LOG_LINE")
    oid1 = r1["op"]["id"]
    d.gateway.exec("p1", "bare1", "echo trigger_cleanup")
    out1 = d.gateway.read_output(oid1)
    assert "STORED_LOG_LINE" in out1["output"]

    # 3. Concurrent cluster-to-cluster transfers on same (src, dst) serialize via stage lock
    d.settings.create_preset("bare2", {
        "connect": "local", "scheduler": "none", "projects_root": str(remote),
    })
    d.attach_cluster("p1", "bare2", root=str(remote / "p1_bare2"))
    src_dir = remote / "p1_bare" / "c2c"
    src_dir.mkdir(parents=True)
    (src_dir / "payload.bin").write_text("A" * 4096, encoding="utf-8")

    results = []
    errors = []

    def do_c2c():
        try:
            results.append(d.gateway.transfer("p1", "bare1:c2c/", "bare2:c2c_out/"))
        except Exception as e:
            errors.append(e)

    t1 = threading.Thread(target=do_c2c)
    t2 = threading.Thread(target=do_c2c)
    t1.start(); t2.start()
    t1.join(timeout=15); t2.join(timeout=15)
    assert not errors
    assert len(results) == 2
    assert all(r["status"] == "ended" and r["op"]["result"]["rc"] == 0 for r in results)
    assert (remote / "p1_bare2" / "c2c_out" / "payload.bin").read_text(encoding="utf-8") == "A" * 4096


def test_background_limit_counts_starting_ops_and_precedes_approval(hpc_home, tmp_path):
    d, _, _, _ = _setup_gateway(hpc_home, tmp_path)
    d.settings.update_project("p1", {"gateway": {"max_background": 1, "wait_s": 0}})

    # 1. A background op still inside its start-up call occupies the only slot
    orig_run = d.gateway._run
    started = threading.Event()

    def slow_run(cluster, script, timeout):
        started.set()
        time.sleep(0.5)
        return orig_run(cluster, script, timeout)

    d.gateway._run = slow_run
    first = []
    t = threading.Thread(
        target=lambda: first.append(d.gateway.exec("p1", "bare1", "sleep 30", background=True)))
    t.start()
    try:
        assert started.wait(5)
        with pytest.raises(GatewayError) as exc:
            d.gateway.exec("p1", "bare1", "sleep 30", background=True)
        assert exc.value.code == "too_many_background"
    finally:
        t.join(timeout=10)
        d.gateway._run = orig_run
    assert first and first[0]["status"] == "background"
    d.gateway.stop("p1", first[0]["op"]["id"], by="agent")

    # 2. A full background quota is reported before asking for approval
    d.settings.update_project("p1", {"gateway": {"max_background": 0}})
    with pytest.raises(GatewayError) as exc2:
        d.gateway.exec("p1", "bare1", "rm -rf /opt/elsewhere", background=True)
    assert exc2.value.code == "too_many_background"
    assert d.approvals.pending_count() == 0
