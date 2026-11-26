"""Tests for config.py (global settings, slurm/none presets, multi-project configs)."""

from __future__ import annotations

import pytest

from hpcport import config, versions


def test_global_config_roundtrip(hpc_home):
    cfg = config.load_global_config()
    assert cfg.port == 8765
    assert cfg.auto_open_dashboard is True
    assert cfg.web_only_approval is False

    updated = config.GlobalConfig(port=9001, auto_open_dashboard=False,
                                  web_only_approval=True, language="zh")
    config.save_global_config(updated)
    assert config.load_global_config() == updated


def test_preset_slurm_and_bare_ssh(hpc_home):
    p_slurm = config.parse_preset(
        "gpu01",
        {"connect": "ssh", "host": "gpu-login", "scheduler": "slurm",
         "partitions": ["a100", "debug"], "max_jobs": 8, "cores_per_node": 64,
         "projects_root": "/work/user"},
        "gpu01.yaml",
    )
    assert p_slurm.scheduler == "slurm"
    assert p_slurm.partitions == ("a100", "debug")
    assert p_slurm.cores_per_node == 64

    p_bare = config.parse_preset(
        "box1",
        {"connect": "ssh", "host": "lab-gpu", "scheduler": "none",
         "exec_timeout_s": 86400, "projects_root": "/data/exp"},
        "box1.yaml",
    )
    assert p_bare.scheduler == "none"
    assert p_bare.exec_timeout_s == 86400

    with pytest.raises(config.ConfigError):
        config.parse_preset("bad", {"connect": "ssh", "scheduler": "pbs"}, "bad.yaml")


def test_project_parsing_and_cwd_resolution(hpc_home, tmp_path):
    pf = config.preset_file("c1")
    with pf.lock():
        text = versions.dump({"connect": "local", "scheduler": "slurm",
                              "max_jobs": 10, "projects_root": str(tmp_path / "remote")})
        pf.write(text)
        pf.add(text)

    ws_outer = tmp_path / "ws"
    ws_inner = ws_outer / "subproj"
    ws_inner.mkdir(parents=True)

    p1 = config.parse_project(
        "outer",
        {"project": "outer", "local_dir": str(ws_outer),
         "clusters": {"c1": {"root": str(tmp_path / "remote" / "outer"), "max_jobs": 4}}},
        "outer.yaml",
    )
    p2 = config.parse_project(
        "inner",
        {"project": "inner", "local_dir": str(ws_inner),
         "clusters": {"c1": {"root": str(tmp_path / "remote" / "inner")}}},
        "inner.yaml",
    )
    assert p1.clusters["c1"].max_jobs == 4
    assert p2.clusters["c1"].max_jobs == 10

    deep = ws_inner / "src" / "exp"
    deep.mkdir(parents=True)
    projs = {"outer": p1, "inner": p2}
    assert config.resolve_project_by_cwd(deep, projs) == "inner"
    assert config.resolve_project_by_cwd(ws_outer, projs) == "outer"
    assert config.resolve_project_by_cwd(tmp_path, projs) is None
