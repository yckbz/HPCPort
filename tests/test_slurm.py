"""Tests for slurm.py (headers, resources, parsers, wrapper script, array hooks)."""

from __future__ import annotations

from hpcport import slurm


def test_parse_header_and_resources():
    script = """#!/bin/bash
#SBATCH -p gpu,debug
#SBATCH -N 2 --ntasks-per-node=4 -c 8
#SBATCH --gres=gpu:a100:2
#SBATCH -t 1-02:30:00
#SBATCH --array=0-9:2%3

echo hello
"""
    h = slurm.parse_header(script)
    assert h is not None
    assert h.partitions == ("gpu", "debug")
    res = slurm.resources(h)
    # 2 nodes * 4 tasks * 8 cpus = 64 cores; 26.5 hours; 5 array elements
    assert res.cores == 64
    assert res.gpus == 4
    assert res.unknown == ()
    assert abs(res.core_hours - 64 * 26.5 * 5) < 1e-6
    assert abs(res.gpu_hours - 4 * 26.5 * 5) < 1e-6


def test_exclusive_and_missing_time():
    script = "#!/bin/bash\n#SBATCH -N 2 --exclusive\n#SBATCH -t 01:00:00\n"
    h = slurm.parse_header(script)
    assert slurm.resources(h).unknown == ("cores",)
    assert slurm.resources(h, cores_per_node=64).core_hours == 128.0

    no_time = slurm.parse_header("#!/bin/bash\n#SBATCH -n 4\n")
    res = slurm.resources(no_time)
    assert res.unknown == ("time",)
    assert res.cores == 4


def test_parse_squeue_sacct_scontrol_and_hooks():
    sq = slurm.parse_squeue("101|RUNNING|/work/a\n102|PENDING|/work/b\n101|PENDING|/work/a\n")
    assert sq["101"]["state"] == slurm.RUNNING
    assert sq["102"]["state"] == slurm.QUEUED

    acct = slurm.parse_sacct(
        "101_0|COMPLETED|0:0|1000|1040\n"
        "101_1|COMPLETED|0:0|1005|1025\n"
        "102|CANCELLED by 1000|0:0|1010|1010\n"
    )
    assert acct["101"]["state"] == slurm.DONE
    assert acct["101"]["elapsed"] == 60.0
    assert acct["102"]["state"] == slurm.CANCELLED
    assert acct["102"]["elapsed"] == 0.0

    ctl = slurm.parse_scontrol(
        "JobId=101_0 JobState=COMPLETED ExitCode=0:0 StartTime=1000 EndTime=1030\n"
        "JobId=101_1 JobState=FAILED ExitCode=1:0 StartTime=1005 EndTime=1020\n"
    )
    assert ctl["state"] == slurm.FAILED
    assert ctl["elapsed"] == 45.0

    hs = slurm.hook_summary("1000 node1\n1005 node2\n", "1020 0\n1050 0\n")
    assert hs["complete"] is True
    assert hs["elapsed"] == 65.0
    assert hs["exit_code"] == "0"


def test_wrapper_script_contents():
    h = slurm.parse_header("#!/bin/bash\n#SBATCH -p a01\n")
    text = slurm.wrapper_script(h, "/work/p/run.sh", "/work/p/.hpcport",
                                {"HPCPORT_PROJECT": "demo", "HPCPORT_BATCH": "b001"})
    assert "HPCPORT_PROJECT=demo" in text
    assert "HPCPORT_BATCH=b001" in text
    assert "hpcport wrapper" in text
    assert "trap " in text
