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
