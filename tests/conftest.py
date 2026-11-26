"""Shared pytest fixtures for HPCPort."""

from __future__ import annotations

import pytest


@pytest.fixture
def hpc_home(tmp_path, monkeypatch):
    home = tmp_path / "hpcport_home"
    home.mkdir()
    monkeypatch.setenv("HPCPORT_HOME", str(home))
    return home
