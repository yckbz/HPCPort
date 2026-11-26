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
