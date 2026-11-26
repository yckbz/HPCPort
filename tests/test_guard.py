"""Tests for guard.py (shell command scanner on slurm and bare servers)."""

from __future__ import annotations

from hpcport import guard


def _place():
    return guard.place_for("/work/proj", "/real/work/proj", "/work/proj/exp1", "/home/alice")


def test_slurm_rules_active_vs_bare_server():
    p = _place()
    f_slurm = guard.check("sbatch run.sh && scancel 123", p, scheduler="slurm")
    assert {(f.rule, f.action) for f in f_slurm} == {("scheduler", "refuse"), ("scancel", "ask")}

    # Bare SSH server skips scheduler and scancel checks
    f_bare = guard.check("sbatch run.sh && scancel 123", p, scheduler="none")
    assert f_bare == []


def test_destructive_paths_and_wrappers():
    p = _place()
    assert guard.check("rm -rf ./tmp_out /tmp/scratch", p) == []
    assert [f.rule for f in guard.check("rm -rf ~/other", p)] == ["outside"]
    assert [f.rule for f in guard.check("rm -rf /work/proj", p)] == ["root"]
    assert [f.rule for f in guard.check("echo hi >> ~/.bashrc", p)] == ["startup"]
    assert [f.rule for f in guard.check("nohup bash -c 'ssh otherhost ls'", p)] == ["remote"]
    assert [f.rule for f in guard.check("crontab -e", p)] == ["crontab"]
    assert guard.check("crontab -l", p) == []
    assert [f.rule for f in guard.check("pkill python", p)] == ["killall"]
    # Inside heredoc or single quotes must not false-positive
    assert guard.check("cat <<'EOF'\nrm -rf /\nsbatch foo.sh\nEOF\n", p) == []
    assert guard.check("echo 'rm -rf /home/alice/x'", p) == []


def test_gateway_allow_overrides_ask_not_refuse():
    p = _place()
    assert guard.check("pkill python", p, allow=("killall",)) == []
    # REFUSE rules cannot be bypassed via allow
    assert [f.rule for f in guard.check("sbatch a.sh", p, allow=("scheduler",))] == ["scheduler"]
