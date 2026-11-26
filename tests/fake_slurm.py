"""In-memory fake Slurm executor for fast, deterministic unit & concurrency tests."""

from __future__ import annotations

import os
import re
import subprocess
import threading
import time
from pathlib import Path

from hpcport.executor import Executor, LocalExecutor, Result, TransportError


class FakeSlurmExecutor(Executor):
    """Runs real bash commands locally while intercepting sbatch/squeue/sacct/scancel/scontrol."""

    def __init__(self, bin_dir: Path, state_lock: threading.RLock | None = None):
        self.bin_dir = Path(bin_dir)
        self.bin_dir.mkdir(parents=True, exist_ok=True)
        self._lock = state_lock or threading.RLock()
        self.next_id = 1000
        self.jobs: dict[str, dict] = {}   # slurm_id -> {state, workdir, exit_code, start, end, script}
        self.squeue_calls = 0
        self.fail_transport = False
        self.drop_after_sbatch = False
        self.disable_sacct = False
        self.reject_sbatch: str | None = None
        self._local = LocalExecutor()

    def _argv(self, script: str) -> list[str]:
        return ["bash", "-c", script]

    def run(self, cmd: str, input: str | None = None, timeout: float = 120) -> Result:
        if self.fail_transport:
            raise TransportError("simulated network outage")
        stripped = cmd.strip()

        # Intercept squeue
        if "squeue -h -u" in stripped:
            with self._lock:
                self.squeue_calls += 1
                lines = []
                for jid, j in self.jobs.items():
                    if j["state"] in ("PENDING", "RUNNING"):
                        lines.append("%s|%s|%s" % (jid, j["state"], j["workdir"]))
                return Result(0, "\n".join(lines) + ("\n" if lines else ""), "")

        # Intercept sacct
        if "sacct -X" in stripped:
            if self.disable_sacct:
                return Result(1, "", "sacct: accounting storage is disabled")
            m = re.search(r"-j\s+([0-9,]+)", stripped)
            wanted = set(m.group(1).split(",")) if m else set()
            with self._lock:
                lines = []
                for jid in sorted(wanted):
                    j = self.jobs.get(jid)
                    if j:
                        st_s = str(int(j["start"])) if j.get("start") is not None else "Unknown"
                        en_s = str(int(j["end"])) if j.get("end") is not None else "Unknown"
                        lines.append("%s|%s|%s|%s|%s" % (
                            jid, j["state"], j.get("exit_code", "0:0"), st_s, en_s
                        ))
                return Result(0, "\n".join(lines) + ("\n" if lines else ""), "")

        # Intercept sbatch --parsable (including idempotent .sid wrapper)
        if "sbatch --parsable" in stripped:
            m_cd = re.search(r"cd -- (\S+)", stripped)
            wd = m_cd.group(1).strip("'\"") if m_cd else "/tmp"
            m_sid = re.search(r"\.hpcport/job\d+\.sid", stripped)
            sid_path = Path(wd) / m_sid.group(0) if m_sid else None
            if sid_path is not None and sid_path.is_file():
                existing = sid_path.read_text(encoding="utf-8").strip()
                if existing:
                    return Result(0, existing + "\n", "")
            if self.reject_sbatch:
                return Result(1, "", self.reject_sbatch)
            with self._lock:
                jid = str(self.next_id)
                self.next_id += 1
                self.jobs[jid] = {
                    "state": "PENDING",
                    "workdir": wd,
                    "exit_code": "0:0",
                    "start": None,
                    "end": None,
                }
            if sid_path is not None:
                sid_path.parent.mkdir(parents=True, exist_ok=True)
                sid_path.write_text(jid + "\n", encoding="utf-8")
            if self.drop_after_sbatch:
                self.drop_after_sbatch = False
                raise TransportError("connection reset after sbatch accepted job")
            return Result(0, jid + "\n", "")

        # Intercept scancel + scontrol batch script
        if input and "scancel " in input:
            m_mark = re.search(r"printf '([^ ]+) R %d", input)
            mark = m_mark.group(1) if m_mark else "@@M"
            m_ids = re.search(r"scancel ([^<]+)</dev/null", input)
            toks = m_ids.group(1).strip().split() if m_ids else []
            out_lines = ["%s R 0" % mark]
            with self._lock:
                now = time.time()
                for tok in toks:
                    if "_" in tok:
                        continue
                    j = self.jobs.get(tok)
                    if j and j["state"] in ("PENDING", "RUNNING"):
                        was_pending = j["state"] == "PENDING"
                        j["state"] = "CANCELLED"
                        j["end"] = now
                        if was_pending:
                            j["start"] = now
                    if j:
                        st_s = str(int(j["start"])) if j.get("start") is not None else "Unknown"
                        en_s = str(int(j["end"])) if j.get("end") is not None else "Unknown"
                        out_lines.append("%s J %s" % (mark, tok))
                        out_lines.append(
                            "JobId=%s JobState=%s ExitCode=%s StartTime=%s EndTime=%s"
                            % (tok, j["state"], j.get("exit_code", "0:0"), st_s, en_s)
                        )
            return Result(0, "\n".join(out_lines) + "\n", "")

        # Intercept hook + scontrol fallback script (not read_job_files)
        if input and "scontrol show job" in input and " J " in input:
            out_lines = []
            # Run hook cat commands locally, and synthesize scontrol lines
            for line in input.splitlines():
                m_j = re.search(r"printf '([^ ]+) J (\d+)\\n'", line)
                if m_j:
                    mark, jid = m_j.group(1), m_j.group(2)
                    out_lines.append("%s J %s" % (mark, jid))
                    out_lines.append("%s E" % mark)
                    out_lines.append("%s C" % mark)
                    with self._lock:
                        j = self.jobs.get(jid)
                        if j:
                            st_s = str(int(j["start"])) if j.get("start") is not None else "Unknown"
                            en_s = str(int(j["end"])) if j.get("end") is not None else "Unknown"
                            out_lines.append(
                                "JobId=%s JobState=%s ExitCode=%s StartTime=%s EndTime=%s"
                                % (jid, j["state"], j.get("exit_code", "0:0"), st_s, en_s)
                            )
            return Result(0, "\n".join(out_lines) + "\n", "")

        return self._local.run(cmd, input=input, timeout=timeout)

    def finish(self, jid: str, state: str = "COMPLETED", exit_code: str = "0:0",
               run_s: float = 60.0) -> None:
        with self._lock:
            now = time.time()
            self.jobs[jid]["state"] = state
            self.jobs[jid]["exit_code"] = exit_code
            self.jobs[jid]["start"] = now - run_s
            self.jobs[jid]["end"] = now

    def start(self, jid: str) -> None:
        with self._lock:
            self.jobs[jid]["state"] = "RUNNING"
            self.jobs[jid]["start"] = time.time()
