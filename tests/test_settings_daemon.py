"""Tests for settings versioning, preset proposals, disk-edit detection, and web auth."""

from __future__ import annotations

from hpcport import config, versions
from hpcport.daemon import Daemon


def test_propose_preset_and_hot_reload_across_projects(hpc_home, tmp_path):
    remote = tmp_path / "remote"
    remote.mkdir()
    d = Daemon(browser_opener=lambda url: True)
    d.settings.create_preset("c1", {
        "connect": "local",
        "scheduler": "slurm",
        "max_jobs": 4,
        "projects_root": str(remote),
        "guide": "old guide",
    })
    d.settings.create_project("p1")
    d.settings.create_project("p2")
    d.attach_cluster("p1", "c1")
    d.attach_cluster("p2", "c1", max_jobs=2)
    d.waits.init_cursor("p1", "s1")
    d.waits.init_cursor("p2", "s2")

    # Agent proposes updating guide + max_jobs (safety field)
    prop = d.settings.propose_preset(
        "p1", "c1", {"guide": "use module load cuda/12", "max_jobs": 8}, "document cuda module"
    )
    assert any(f["name"] == "max_jobs" and f["safety"] for f in prop["fields"])
    assert any(f["name"] == "guide" and not f["safety"] for f in prop["fields"])

    # Approve -> both projects hot-reload; p2 keeps its max_jobs=2 override!
    with d.lock:
        d.approvals.decide(prop["approval"], True, "approved")
    assert d.presets["c1"].guide == "use module load cuda/12"
    assert d.projects["p1"].clusters["c1"].max_jobs == 8
    assert d.projects["p2"].clusters["c1"].max_jobs == 2
    assert d.projects["p2"].clusters["c1"].guide == "use module load cuda/12"

    # Both projects receive preset_updated notice
    n2 = d.waits.pop_notices("p2", "s2")
    assert any(n["reason"] == "preset_updated" for n in n2)


def test_disk_edit_detection_and_reject_reverts(hpc_home, tmp_path):
    d = Daemon(browser_opener=lambda url: True)
    d.settings.create_preset("c1", {"connect": "local", "scheduler": "none", "exec_timeout_s": 600})
    pf = config.preset_file("c1")
    orig = pf.read()

    # Hand-edit on disk
    pf.write(orig + "\nexec_timeout_s: 900\n")
    # Actually replace cleanly
    pf.write(versions.edit(orig, versions.apply(versions.load(orig), {"exec_timeout_s": 900})))
    d.settings.scan()

    # Approved version stays 600 until decided
    assert d.presets["c1"].exec_timeout_s == 600
    pending = [a for a in d.approvals.list(pending_only=True) if a["kind"] == "proposal"]
    assert len(pending) == 1

    # Reject -> reverts disk file back to 600
    with d.lock:
        d.approvals.decide(pending[0]["id"], False, "nope")
    assert versions.load(pf.read())["exec_timeout_s"] == 600


def test_web_auth_exchange_code_never_leaks_and_pin_works(hpc_home):
    opened_urls = []
    d = Daemon(browser_opener=lambda url: opened_urls.append(url) or True)
    res = d.open_dashboard(tab="approvals")
    assert res["opened"] is True
    assert "code=" not in res["url"]          # agent never sees exchange code!
    assert len(opened_urls) == 1
    assert "code=" in opened_urls[0]

    code = opened_urls[0].split("code=")[1].split("&")[0]
    redeemed = d.redeem_exchange_code(code)
    assert redeemed is not None
    cookie, tab = redeemed
    assert tab == "approvals"
    assert d.verify_cookie(cookie) is True
    # Single-use: cannot redeem twice
    assert d.redeem_exchange_code(code) is None

    # Headless PIN verification
    assert d.verify_pin("000000") is None or d.pairing_pin == "000000"
    pin_cookie = d.verify_pin(d.pairing_pin)
    assert pin_cookie is not None and d.verify_cookie(pin_cookie) is True


def test_settings_stale_proposal_delete_preset_and_offline_disk_edit(hpc_home, tmp_path):
    import pytest
    from hpcport.approvals import ApprovalError

    remote = tmp_path / "remote"
    remote.mkdir()
    d = Daemon(browser_opener=lambda url: True)
    d.settings.create_preset("c1", {
        "connect": "local", "scheduler": "none", "projects_root": str(remote),
        "guide": "v1 guide",
    })
    d.settings.create_project("p1")
    d.attach_cluster("p1", "c1")

    # 1. Stale proposal (moved field) cancels approval too -> no permanent zombie approval
    prop = d.settings.propose_preset("p1", "c1", {"guide": "agent guide"}, "update guide")
    aid = prop["approval"]
    # Simulate out-of-band version advance on the same field without _refresh_pending_proposals
    pf = config.preset_file("c1")
    with pf.lock():
        t2 = versions.edit(pf.read(), versions.apply(versions.load(pf.read()), {"guide": "v2"}))
        pf.write(t2)
        pf.add(t2)
    with pytest.raises(ApprovalError) as exc:
        with d.lock:
            d.approvals.decide(aid, True, "approve stale")
    assert exc.value.code == "stale"
    assert d.approvals.get(aid)["state"] == "cancelled"
    assert d.approvals.pending_count() == 0

    # 2. Deleting a preset cancels pending proposals, cleans history, and prevents resurrection
    prop2 = d.settings.propose_preset("p1", "c1", {"guide": "resurrect me"}, "before delete")
    aid2 = prop2["approval"]
    d.detach_cluster("p1", "c1")
    d.settings.delete_preset("c1")
    assert d.approvals.get(aid2)["state"] == "cancelled"
    assert d.db.one("SELECT state FROM proposals WHERE id = ?", (prop2["id"],))["state"] == "cancelled"
    assert not config.preset_file("c1").history.exists()
    # Recreating same preset name starts cleanly at v1
    created = d.settings.create_preset("c1", {
        "connect": "local", "scheduler": "none", "exec_timeout_s": 500,
    })
    assert created["version"] == 1
    assert config.preset_file("c1").versions() == [1]

    # 3. Offline hand-edit on disk while daemon is stopped is detected on first scan() after restart
    pf = config.preset_file("c1")
    edited = versions.edit(pf.read(), versions.apply(versions.load(pf.read()), {"exec_timeout_s": 777}))
    pf.write(edited)
    d2 = Daemon(browser_opener=lambda url: True)
    assert d2.presets["c1"].exec_timeout_s == 500
    d2.settings.scan()
    pending = [a for a in d2.approvals.list(pending_only=True) if a["kind"] == "proposal"]
    assert len(pending) == 1
    assert "exec_timeout_s" in pending[0]["summary"]


def test_duplicate_daemon_does_not_clobber_token_or_running_daemon(hpc_home, tmp_path):
    import os
    import socket
    import subprocess
    import sys
    import time
    from hpcport import stdio_bridge

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    env = dict(os.environ, PYTHONPATH="src", HPCPORT_HOME=str(hpc_home))
    # First daemon starts via ensure_daemon (verifies no deadlock with daemon.lock!)
    started_port = stdio_bridge.ensure_daemon(port=port, wait_s=8.0)
    assert started_port == port
    orig_token = config.cli_token_file().read_text(encoding="utf-8").strip()
    orig_pid = int(config.daemon_pid_file().read_text(encoding="utf-8").strip())
    assert orig_token and orig_pid > 0

    try:
        # Second manual `hpcport daemon` must exit non-zero WITHOUT overwriting token or pid
        dup = subprocess.run(
            [sys.executable, "-m", "hpcport.cli", "--port", str(port), "daemon"],
            env=env, cwd="/Users/liuce/Desktop/HPCPort",
            capture_output=True, text=True, timeout=10,
        )
        assert dup.returncode != 0
        assert "already running" in (dup.stderr + dup.stdout)
        assert config.cli_token_file().read_text(encoding="utf-8").strip() == orig_token
        assert int(config.daemon_pid_file().read_text(encoding="utf-8").strip()) == orig_pid
        assert stdio_bridge._daemon_alive(port) is True
    finally:
        try:
            os.kill(orig_pid, 15)
        except OSError:
            pass
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and stdio_bridge._daemon_alive(port):
            time.sleep(0.05)


def test_detach_cluster_does_not_treat_underscore_as_like_wildcard(hpc_home, tmp_path):
    remote = tmp_path / "remote"
    remote.mkdir()
    d = Daemon(browser_opener=lambda url: True)
    d.settings.create_preset("gpu_1", {
        "connect": "local", "scheduler": "none", "projects_root": str(remote),
    })
    d.settings.create_preset("gpu-1", {
        "connect": "local", "scheduler": "none", "projects_root": str(remote),
    })
    d.settings.create_project("p1")
    d.attach_cluster("p1", "gpu_1", root=str(remote / "p1_u"))
    d.attach_cluster("p1", "gpu-1", root=str(remote / "p1_d"))

    # Running op on gpu-1 must NOT block detaching gpu_1
    op = d.gateway.exec("p1", "gpu-1", "sleep 20", background=True)
    try:
        res = d.detach_cluster("p1", "gpu_1")
        assert res["cluster"] == "gpu_1"
        assert "gpu_1" not in d.projects["p1"].clusters
    finally:
        d.gateway.stop("p1", op["op"]["id"], by="user")
