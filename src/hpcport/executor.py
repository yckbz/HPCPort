"""Run shell commands on a compute server login node (system ssh) or locally.

Every command runs in a login shell because scheduler commands and environment
modules are often only on PATH there. Login shells may print banners (motd,
quota) to stdout, so command output is framed by random marker lines and the
exit code is read from the closing marker. Missing markers indicate a transport
failure; callers must keep their state unchanged.
"""

from __future__ import annotations

import os
import secrets
import shlex
import subprocess
from dataclasses import dataclass

from .config import run_dir


class TransportError(Exception):
    """The command could not be delivered or its output was incomplete."""


@dataclass(frozen=True)
class Result:
    rc: int
    out: str
    err: str


def _wrap(cmd: str, mark: str) -> str:
    return ("printf '%%s\\n' %s_BEGIN\n( %s\n)\n__rc=$?\nprintf '\\n%%s %%d\\n' %s_END \"$__rc\"\n"
            % (mark, cmd, mark))


def _unwrap(stdout: str, mark: str) -> tuple[int, str]:
    begin, end = mark + "_BEGIN\n", "\n" + mark + "_END "
    i = stdout.find(begin)
    j = stdout.rfind(end)
    if i < 0 or j < i:
        raise TransportError("incomplete output from remote shell")
    tail = stdout[j + len(end):].split()
    if not tail or not tail[0].lstrip("-").isdigit():
        raise TransportError("unreadable exit code from remote shell")
    return int(tail[0]), stdout[i + len(begin):j]


class Executor:
    """Base executor; subclasses provide the argv that launches the shell."""

    env: dict[str, str] | None = None
    shell = "bash"

    def _argv(self, script: str) -> list[str]:
        raise NotImplementedError

    def rsync_spec(self, path: str) -> str:
        return path

    def rsync_rsh(self) -> str | None:
        return None

    def run(self, cmd: str, input: str | None = None, timeout: float = 120) -> Result:
        mark = "__HPCPORT_%s" % secrets.token_hex(8)
        try:
            p = subprocess.run(
                self._argv(_wrap(cmd, mark)),
                input=input,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=self.env,
                errors="replace",
                stdin=None if input is not None else subprocess.DEVNULL,
            )
        except subprocess.TimeoutExpired:
            raise TransportError("command timed out after %gs" % timeout)
        except OSError as e:
            raise TransportError("cannot start shell: %s" % e)
        try:
            rc, out = _unwrap(p.stdout, mark)
        except TransportError as e:
            detail = (p.stderr or "").strip().splitlines()[-1:] or [""]
            raise TransportError("%s (exit %s) %s" % (e, p.returncode, detail[0]))
        return Result(rc, out, p.stderr)


class LocalExecutor(Executor):
    def __init__(self, env: dict[str, str] | None = None):
        self.env = env

    def _argv(self, script: str) -> list[str]:
        return ["bash", "-c", script]


class SshExecutor(Executor):
    shell = "bash -l"

    def __init__(self, host: str):
        self.host = host
        ctl_dir = str(run_dir())
        os.makedirs(ctl_dir, mode=0o700, exist_ok=True)
        self._opts = [
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=15",
            "-o", "ServerAliveInterval=30",
            "-o", "ServerAliveCountMax=3",
            "-o", "ControlMaster=auto",
            "-o", "ControlPersist=600",
            "-o", "ControlPath=%s/hpcport-%%C" % ctl_dir,
        ]

    def _argv(self, script: str) -> list[str]:
        return ["ssh"] + self._opts + [self.host, "bash -lc " + shlex.quote(script)]

    def rsync_spec(self, path: str) -> str:
        return "%s:%s" % (self.host, path)

    def rsync_rsh(self) -> str:
        return " ".join(shlex.quote(a) for a in ["ssh"] + self._opts)


def make_executor(preset) -> Executor:
    return LocalExecutor() if preset.connect == "local" else SshExecutor(preset.host)
