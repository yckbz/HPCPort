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


def test_cd_tracking_and_other_user_tilde():
    p = _place()  # root=/work/proj, cwd=/work/proj/exp1, home=/home/alice
    p_root = guard.place_for("/work/proj", "/real/work/proj", "/work/proj", "/home/alice")

    # Bypass attempts using cd / pushd / ~otheruser must be caught
    assert [f.rule for f in guard.check("cd / && rm -rf etc", p)] == ["outside"]
    assert [f.rule for f in guard.check("cd ~ && rm -rf .", p)] == ["outside"]
    assert [f.rule for f in guard.check("cd && rm -rf .", p)] == ["outside"]
    assert [f.rule for f in guard.check("cd .. && rm -rf other_proj", p_root)] == ["outside"]
    assert [f.rule for f in guard.check("cd .. && rm -rf .", p)] == ["root"]
    assert [f.rule for f in guard.check("cd ~ && echo pwn >> .bashrc", p)] == ["startup"]
    assert [f.rule for f in guard.check("rm -rf ~bob/data", p)] == ["outside"]
    assert [f.rule for f in guard.check("echo pwn >> ~bob/.bashrc", p)] == ["startup"]
    # cd AFTER destructive command must NOT blind earlier commands
    assert [f.rule for f in guard.check("rm -rf ../../outside && cd subdir", p)] == ["outside"]
    # Indeterminate cd target followed by destructive relative op or startup redirect
    assert [f.rule for f in guard.check("cd $TARGET && rm -rf foo", p)] == ["outside"]
    assert [f.rule for f in guard.check("cd $TARGET && echo x >> .bashrc", p)] == ["startup"]
    # pushd / popd tracking
    assert [f.rule for f in guard.check("pushd / && rm -rf etc && popd", p)] == ["outside"]
    # Nested bash -c must catch inner cd bypass without poisoning outer cwd
    f_nested = guard.check("bash -c 'cd / && rm -rf etc'; rm -rf ./local_tmp", p)
    assert [(f.rule, f.what) for f in f_nested] == [("outside", "etc")]

    # Legitimate idioms must NOT false-positive
    assert guard.check("cd subdir && rm -rf tmp_cache", p) == []
    assert guard.check("cd / && ls -la", p) == []
    assert guard.check("cd /tmp && rm -rf scratch_tmp", p) == []
    assert guard.check("cd sub1 && cd sub2 && cd - && rm -rf tmp", p) == []
    assert guard.check("pushd subdir && rm -rf tmp && popd && rm -rf tmp2", p) == []
    assert guard.check("rm -rf $BUILD_DIR", p) == []
    assert guard.check("cd $DIR && echo ok > out.log", p) == []


def test_subshell_pipeline_cmdsubst_and_env_wrappers():
    p = guard.place_for("/work/proj", "/work/proj", "/work/proj", "/home/alice")

    # Subshells must not leak cwd outward or allow cd back inside to mask outer cwd
    assert guard.check("(cd / && ls) && rm -rf ./tmp_out", p) == []
    assert [(f.rule, f.what) for f in guard.check("cd / ; (cd /work/proj && true) ; rm -rf etc", p)] == [
        ("outside", "etc")
    ]
    assert [(f.rule, f.what) for f in guard.check("(cd / && rm -rf etc)", p)] == [
        ("outside", "etc")
    ]
    assert [(f.rule, f.what) for f in guard.check("if (cd / && rm -rf etc); then echo ok; fi", p)] == [
        ("outside", "etc")
    ]

    # Pipeline and background subshells must not mutate outer cwd
    assert [(f.rule, f.what) for f in guard.check("cd / ; echo hi | cd /work/proj ; rm -rf etc", p)] == [
        ("outside", "etc")
    ]
    assert [(f.rule, f.what) for f in guard.check("cd / ; cd /work/proj & rm -rf etc", p)] == [
        ("outside", "etc")
    ]

    # Command substitution evaluates at its lexical cwd without leaking outward
    assert [(f.rule, f.what) for f in guard.check("cd / && echo $(rm -rf etc) && cd /work/proj", p)] == [
        ("outside", "etc")
    ]
    assert guard.check("echo $(cd / && ls) && rm -rf ./tmp_out", p) == []

    # env -C / --chdir and env -S / --split-string
    assert [(f.rule, f.what) for f in guard.check("env -C / rm -rf etc", p)] == [("outside", "etc")]
    assert [(f.rule, f.what) for f in guard.check("env --chdir=/ rm -rf etc", p)] == [("outside", "etc")]
    assert [(f.rule, f.what) for f in guard.check("env -C/ rm -rf etc", p)] == [("outside", "etc")]
    assert guard.check("env -C subdir rm -rf tmp && rm -rf ./local_tmp", p) == []
    assert [(f.rule, f.what) for f in guard.check("env -S 'sbatch run.sh'", p)] == [
        ("scheduler", "sbatch")
    ]
    assert [(f.rule, f.what) for f in guard.check("env -S '-C / rm -rf etc'", p)] == [
        ("outside", "etc")
    ]
