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
    assert slurm.resources(h).unknown == ("cores", "gpus")
    # cores_per_node alone still leaves exclusive GPU allocation unknown (never 0 GPU-h!)
    # while preserving known cores for measured core-hour accounting after completion
    r_only_cores = slurm.resources(h, cores_per_node=64)
    assert r_only_cores.unknown == ("gpus",)
    assert r_only_cores.core_hours is None
    assert r_only_cores.gpu_hours is None
    assert r_only_cores.cores == 128
    assert r_only_cores.gpus is None
    # CPU-only nodes declare gpus_per_node=0 -> 128 core-h, 0 GPU-h
    r_cpu = slurm.resources(h, cores_per_node=64, gpus_per_node=0)
    assert r_cpu.unknown == ()
    assert r_cpu.core_hours == 128.0
    assert r_cpu.gpu_hours == 0.0
    # GPU nodes declare gpus_per_node=8 -> 2 nodes * 8 GPUs * 1h = 16 GPU-h
    r_gpu = slurm.resources(h, cores_per_node=64, gpus_per_node=8)
    assert r_gpu.unknown == ()
    assert r_gpu.core_hours == 128.0
    assert r_gpu.gpu_hours == 16.0
    assert r_gpu.gpus == 16
    # Explicit smaller --gres=gpu:2 with --exclusive still accounts whole-node GPUs
    h_ex_gres = slurm.parse_header(
        "#!/bin/bash\n#SBATCH -N 2 --exclusive --gres=gpu:2\n#SBATCH -t 01:00:00\n"
    )
    assert slurm.resources(h_ex_gres, cores_per_node=64, gpus_per_node=8).gpu_hours == 16.0
    assert "gpus" in slurm.resources(h_ex_gres, cores_per_node=64).unknown

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
                                {"HPCPORT_PROJECT": "demo", "HPCPORT_BATCH": "b001"},
                                sid_file="/work/p/.hpcport/job1_abc.sid")
    assert "HPCPORT_PROJECT=demo" in text
    assert "HPCPORT_BATCH=b001" in text
    assert "hpcport wrapper" in text
    assert "trap " in text
    assert "job1_abc.sid" in text


def test_gpu_per_socket_tres_and_sub_hour_formatting():
    from hpcport import messages

    # 1 node, 2 sockets, 2 GPUs/socket, 1 hour -> 4.0 GPU-h (NEVER 0!)
    h1 = slurm.parse_header(
        "#!/bin/bash\n"
        "#SBATCH -N 1\n"
        "#SBATCH --sockets-per-node=2\n"
        "#SBATCH --gpus-per-socket=2\n"
        "#SBATCH -t 01:00:00\n"
    )
    r1 = slurm.resources(h1)
    assert r1.unknown == ()
    assert r1.gpus == 4
    assert r1.gpu_hours == 4.0

    # --gpus-per-socket without known sockets -> MUST be unknown (never 0!), while preserving known cores
    h2 = slurm.parse_header(
        "#!/bin/bash\n#SBATCH -N 1 -n 4\n#SBATCH --gpus-per-socket=2\n#SBATCH -t 01:00:00\n"
    )
    r2 = slurm.resources(h2)
    assert "gpus_per_socket" in r2.unknown
    assert r2.gpu_hours is None and r2.core_hours is None
    assert r2.cores == 4 and r2.gpus is None

    # Unparseable -N under --exclusive must invalidate both cores and gpus
    h_bad_n = slurm.parse_header("#!/bin/bash\n#SBATCH -N bad --exclusive -t 01:00:00\n")
    r_bad_n = slurm.resources(h_bad_n, cores_per_node=64, gpus_per_node=8)
    assert "nodes" in r_bad_n.unknown
    assert r_bad_n.cores is None and r_bad_n.gpus is None

    # -B 2:16:1 + --gpus-per-socket=2 -> 4 GPUs; --cpus-per-gpu=6 -> 24 cores
    h3 = slurm.parse_header(
        "#!/bin/bash\n#SBATCH -N 1 -B 2:16:1 --gpus-per-socket=2 --cpus-per-gpu=6 -t 01:00:00\n"
    )
    r3 = slurm.resources(h3)
    assert r3.unknown == ()
    assert r3.gpus == 4
    assert r3.cores == 24

    # --tres-per-node & socket affinity suffix & shard safety
    h4 = slurm.parse_header(
        "#!/bin/bash\n#SBATCH -N 2 --tres-per-node=gres/gpu:a100:2 -t 00:30:00\n"
    )
    r4 = slurm.resources(h4)
    assert r4.unknown == ()
    assert r4.gpus == 4
    assert r4.gpu_hours == 2.0

    h5 = slurm.parse_header(
        "#!/bin/bash\n#SBATCH -N 1 --gres=gpu:a100:2(S:0-1) -t 00:00:45\n"
    )
    r5 = slurm.resources(h5)
    assert r5.unknown == ()
    assert r5.gpus == 2
    assert abs(r5.gpu_hours - (2 * 45 / 3600.0)) < 1e-9

    # GPU shard cannot be accounted as 0 GPUs -> must mark unknown
    h6 = slurm.parse_header(
        "#!/bin/bash\n#SBATCH -N 1 --gres=shard:a100:4 -t 01:00:00\n"
    )
    assert "gres" in slurm.resources(h6).unknown

    # Sub-hour / small quantities must NEVER display as '0' or '0.0'
    assert messages.fmt_qty(45 / 3600.0) == "0.013"
    assert messages.fmt_qty(10 / 3600.0) == "0.0028"
    assert messages.fmt_qty(0.1 / 3600.0) == "<0.0001"
    assert messages.fmt_qty(0.0) == "0"
    assert messages._fmt_dur(0.35) == "<1s"
    summ = messages.submit_approval_summary(
        "demo",
        {"batch": "b001", "cluster": "gpu01", "jobs": 1, "desc": "45s demo",
         "core_hours": 0.05, "gpu_hours": 45 / 3600.0},
        [{"code": "no_standing"}],
    )
    assert "0.05 core-h" in summ
    assert "0.013 GPU-h" in summ
    assert "0.0 GPU-h" not in summ


def test_unparseable_sbatch_directive_marks_unknown():
    # Unclosed quote or bare apostrophe must NOT silently drop GPU/node directives
    h1 = slurm.parse_header(
        "#!/bin/bash\n"
        "#SBATCH -N 4 --gres=gpu:8 -J \"O'Connor\n"
        "#SBATCH -t 01:00:00\n"
    )
    r1 = slurm.resources(h1)
    assert "directive" in r1.unknown
    assert r1.core_hours is None and r1.gpu_hours is None

    h2 = slurm.parse_header(
        "#!/bin/bash\n"
        "#SBATCH -N 2 --gres=gpu:4 --comment=Bob's_run\n"
        "#SBATCH -t 02:00:00\n"
    )
    r2 = slurm.resources(h2)
    assert "directive" in r2.unknown
    assert r2.gpu_hours is None
