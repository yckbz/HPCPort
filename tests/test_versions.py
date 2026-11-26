"""Tests for versions.py (numbered history, patches, comment-preserving edits)."""

from __future__ import annotations

import pytest

from hpcport import versions


def test_versioned_file_lifecycle(tmp_path):
    vf = versions.VersionedFile(tmp_path / "c.yaml", tmp_path / "hist")
    assert vf.read() is None
    assert vf.latest() is None
    with vf.lock():
        v1 = vf.add("connect: local\nmax_jobs: 4\n")
        assert v1 == 1
        assert vf.add("connect: local\nmax_jobs: 4\n") == 1
        v2 = vf.add("connect: local\nmax_jobs: 8\n")
        assert v2 == 2
    assert vf.versions() == [1, 2]
    assert vf.latest() == (2, "connect: local\nmax_jobs: 8\n")
    rej = vf.keep_rejected("bad: [", "p01")
    assert rej.exists()


def test_patch_and_edit_preserves_comments():
    raw = (
        "# Cluster config\n"
        "connect: ssh\n"
        "host: login01\n"
        "# Keep this comment\n"
        "max_jobs: 10\n"
        "guide: |\n"
        "  line 1\n"
    )
    old = versions.load(raw)
    new = versions.apply(old, {"max_jobs": 20, "exec_timeout_s": 1800})
    edited = versions.edit(raw, new)
    assert "# Cluster config" in edited
    assert "# Keep this comment" in edited
    assert versions.load(edited) == new

    diff = versions.changes(old, new)
    assert [versions.name(c["field"]) for c in diff] == ["max_jobs", "exec_timeout_s"]
    assert versions.as_patch(old, new) == {"max_jobs": 20, "exec_timeout_s": 1800}


def test_apply_removal_and_nested():
    old = {"submit": {"approval": True, "core_hours": 5000}, "extra": 1}
    new = versions.apply(old, {"submit": {"core_hours": None}, "extra": None})
    assert new == {"submit": {"approval": True}}


def test_load_rejects_non_mapping():
    with pytest.raises(ValueError):
        versions.load("- a\n- b\n")
    with pytest.raises(ValueError):
        versions.load("a: [unclosed")
