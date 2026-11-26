"""Tests for executor.py and attach.py."""

from __future__ import annotations

import pytest

from hpcport import attach, config, versions
from hpcport.executor import LocalExecutor, TransportError, _unwrap, _wrap


def test_executor_markers_strip_banners():
    ex = LocalExecutor()
    r = ex.run("echo 'hello world'")
    assert r.rc == 0
    assert r.out.strip() == "hello world"

    r_fail = ex.run("echo err >&2; false")
    assert r_fail.rc == 1
    assert "err" in r_fail.err

    # Explicit `exit` inside cmd must NOT terminate the framing shell or lose END marker
    r_exit = ex.run("echo rejected >&2; exit 42")
    assert r_exit.rc == 42
    assert "rejected" in r_exit.err

    # Simulated login MOTD banner before and after markers
    wrapped = "WELCOME BANNER\n" + "M_BEGIN\nactual output\nM_END 0\nBYE\n"
    assert _unwrap(wrapped, "M") == (0, "actual output")
    with pytest.raises(TransportError):
        _unwrap("no markers here", "M")


def test_attach_plan_make_root_and_record(hpc_home, tmp_path):
    remote_parent = tmp_path / "remote_parent"
    remote_parent.mkdir()
    pf = config.preset_file("localbox")
    with pf.lock():
        pt = versions.dump({"connect": "local", "scheduler": "none",
                            "projects_root": str(remote_parent)})
        pf.write(pt)
        pf.add(pt)

    proj_f = config.project_file("demo")
    with proj_f.lock():
        jt = versions.dump({"project": "demo", "clusters": {}})
        proj_f.write(jt)
        proj_f.add(jt)

    proj = config.load_project("demo")
    cl, derived = attach.plan(proj, "localbox")
    assert derived is True
    assert cl.root == str(remote_parent / "demo")

    ex = LocalExecutor()
    real = attach.make_root(ex, cl, derived)
    assert real == str((remote_parent / "demo").resolve())
    assert (remote_parent / "demo").is_dir()

    v = attach.record(proj, cl)
    assert v == 2
    reloaded = config.load_project("demo")
    assert "localbox" in reloaded.clusters
    assert reloaded.clusters["localbox"].scheduler == "none"

    # Re-attaching derived non-empty root fails
    (remote_parent / "demo" / "file.txt").write_text("x")
    with pytest.raises(attach.AttachError):
        attach.make_root(ex, cl, derived=True)
    # Explicit root may already hold files
    assert attach.make_root(ex, cl, derived=False) == real


def test_ssh_control_path_stays_short_under_deep_home(tmp_path, monkeypatch):
    from hpcport.executor import SshExecutor

    deep = tmp_path / ("very_long_segment_" * 5) / "hpcport_home"
    deep.mkdir(parents=True)
    monkeypatch.setenv("HPCPORT_HOME", str(deep))
    ex = SshExecutor("my-very-long-cluster-hostname.university.edu")
    ctl = next(opt.split("=", 1)[1] for opt in ex._opts if opt.startswith("ControlPath="))
    # Must stay well below macOS 104-byte sun_path limit (leaving room for OpenSSH temp suffix)
    assert len(ctl.encode("utf-8")) <= 76
    assert "%C" not in ctl
