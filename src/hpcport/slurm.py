"""Slurm specifics: command lines, output parsing, resource accounting, and wrapper script.

Pure functions only; nothing here touches a cluster or the database.
"""

from __future__ import annotations

import shlex
from dataclasses import dataclass

UNAPPROVED, HELD, QUEUED, RUNNING = "unapproved", "held", "queued", "running"
DONE, FAILED, TIMEOUT, CANCELLED, LOST = "done", "failed", "timeout", "cancelled", "lost"
SUBMIT_FAILED = "submit_failed"
ACTIVE = (UNAPPROVED, HELD, QUEUED, RUNNING)
NOT_SENT = (UNAPPROVED, HELD)
IN_SLURM = (QUEUED, RUNNING)
FAILURES = (FAILED, TIMEOUT, LOST, SUBMIT_FAILED)

STATES = {
    "PENDING": QUEUED, "CONFIGURING": QUEUED, "REQUEUED": QUEUED, "REQUEUE_HOLD": QUEUED,
    "REQUEUE_FED": QUEUED, "RESV_DEL_HOLD": QUEUED, "SPECIAL_EXIT": QUEUED,
    "RUNNING": RUNNING, "SUSPENDED": RUNNING, "STOPPED": RUNNING, "COMPLETING": RUNNING,
    "STAGE_OUT": RUNNING, "SIGNALING": RUNNING, "RESIZING": RUNNING,
    "COMPLETED": DONE,
    "FAILED": FAILED, "NODE_FAIL": FAILED, "BOOT_FAIL": FAILED, "OUT_OF_MEMORY": FAILED,
    "DEADLINE": FAILED, "PREEMPTED": FAILED,
    "TIMEOUT": TIMEOUT,
    "CANCELLED": CANCELLED, "REVOKED": CANCELLED,
}
_ACTIVE_ORDER = (RUNNING, QUEUED)
_FINAL_ORDER = (FAILED, TIMEOUT, CANCELLED, DONE)

TIME_ENV = "SLURM_TIME_FORMAT=%s "
SQUEUE = TIME_ENV + 'squeue -h -u "$USER" -o "%F|%T|%Z"'


def marks(values) -> str:
    return ", ".join("?" for _ in values)


def classify(slurm_state: str) -> str | None:
    word = slurm_state.strip().split()[0].rstrip("+") if slurm_state.strip() else ""
    return STATES.get(word.upper())


def aggregate(states: list[str]) -> str | None:
    for s in _ACTIVE_ORDER + _FINAL_ORDER:
        if s in states:
            return s
    return None


def _epoch(text: str) -> float | None:
    return float(text) if text.strip().isdigit() else None


def elapsed(elements) -> float | None:
    """Total seconds run across all elements of a job (or None if any element is unknown)."""
    total = 0.0
    for state, start, end in elements:
        if start is None and state == CANCELLED:
            continue
        if start is None or end is None:
            return None
        total += max(0.0, end - start)
    return total


@dataclass(frozen=True)
class Header:
    interpreter: str
    directives: tuple[str, ...]
    partitions: tuple[str, ...]


_OPTIONS = {
    "partition": ("-p", "--partition"),
    "nodes": ("-N", "--nodes"),
    "ntasks": ("-n", "--ntasks"),
    "ntasks_per_node": (None, "--ntasks-per-node"),
    "ntasks_per_socket": (None, "--ntasks-per-socket"),
    "ntasks_per_core": (None, "--ntasks-per-core"),
    "sockets_per_node": (None, "--sockets-per-node"),
    "extra_node_info": ("-B", "--extra-node-info"),
    "cpus_per_task": ("-c", "--cpus-per-task"),
    "cpus_per_gpu": (None, "--cpus-per-gpu"),
    "time": ("-t", "--time"),
    "array": ("-a", "--array"),
    "gres": (None, "--gres"),
    "gpus": ("-G", "--gpus"),
    "gpus_per_node": (None, "--gpus-per-node"),
    "gpus_per_socket": (None, "--gpus-per-socket"),
    "gpus_per_task": (None, "--gpus-per-task"),
    "tres_per_job": (None, "--tres-per-job"),
    "tres_per_node": (None, "--tres-per-node"),
    "tres_per_socket": (None, "--tres-per-socket"),
    "tres_per_task": (None, "--tres-per-task"),
}
_SHORT = {short: key for key, (short, _) in _OPTIONS.items() if short}
_LONG = {long: key for key, (_, long) in _OPTIONS.items()}
_FLAGS = {"--exclusive": "exclusive"}


def header_options(directives) -> dict[str, str]:
    opts: dict[str, str] = {}
    for line in directives:
        try:
            toks = shlex.split(line[len("#SBATCH"):], comments=True)
        except ValueError:
            continue
        k = 0
        while k < len(toks):
            tok = toks[k]
            k += 1
            key = val = None
            if tok.startswith("--"):
                name, eq, rest = tok.partition("=")
                if name in _FLAGS:
                    opts[_FLAGS[name]] = ""
                elif name in _LONG:
                    key, val = _LONG[name], rest if eq else None
            elif tok[:2] in _SHORT:
                key, val = _SHORT[tok[:2]], tok[2:] or None
            if key is None:
                continue
            if val is None:
                if k >= len(toks):
                    continue
                val = toks[k]
                k += 1
            opts[key] = val
    return opts


def parse_header(text: str) -> Header | None:
    lines = text.splitlines()
    if not lines or not lines[0].startswith("#!") or not lines[0][2:].strip():
        return None
    directives = []
    for line in lines[1:]:
        if not line.strip():
            continue
        if not line.startswith("#"):
            break
        if line.startswith("#SBATCH"):
            directives.append(line.rstrip())
    partitions = tuple(p for p in header_options(directives).get("partition", "").split(",")
                       if p)
    return Header(lines[0][2:].strip(), tuple(directives), partitions)


@dataclass(frozen=True)
class Resources:
    core_hours: float | None
    gpu_hours: float | None
    unknown: tuple[str, ...] = ()
    cores: int | None = None
    gpus: int | None = None


def _count(text: str) -> int:
    n = int(text.split("-")[-1])
    if n < 1:
        raise ValueError(text)
    return n


def time_limit_hours(text: str) -> float | None:
    text = text.strip()
    if text.upper() in ("UNLIMITED", "INFINITE"):
        return None
    days, dash, rest = text.rpartition("-")
    parts = [int(x) for x in rest.split(":")]
    if any(x < 0 for x in parts) or len(parts) > 3:
        raise ValueError(text)
    if dash:
        d = int(days)
        if d < 0:
            raise ValueError(text)
        h, m, sec = (parts + [0, 0])[:3]
        seconds = d * 86400 + h * 3600 + m * 60 + sec
    elif len(parts) == 3:
        seconds = parts[0] * 3600 + parts[1] * 60 + parts[2]
    else:
        seconds = parts[0] * 60 + (parts[1] if len(parts) == 2 else 0)
    return seconds / 3600.0 if seconds > 0 else None


def array_size(text: str) -> int:
    n = 0
    for part in text.split("%")[0].split(","):
        span, _, step = part.partition(":")
        first, dash, last = span.partition("-")
        size = len(range(int(first), int(last if dash else first) + 1, int(step or 1)))
        if size < 1:
            raise ValueError(text)
        n += size
    return n


def _strip_affinity(tok: str) -> str:
    """Strip optional Slurm socket/core binding suffix such as '2(S:0-1)' -> '2'."""
    if "(" in tok:
        head, _, tail = tok.partition("(")
        if not tail.endswith(")"):
            raise ValueError(tok)
        return head
    return tok


def _gpus(text: str, gres: bool = False, tres: bool = False) -> int:
    """Parse GPU counts from --gres, --gpus*, or --tres-per-* specs.

    Raises ValueError on ambiguous/unaccountable GPU specs (such as GPU shards
    or malformed counts) so callers mark them unknown rather than counting 0.
    """
    n = 0
    for raw in text.split(","):
        item = raw.strip()
        if not item:
            continue
        item = _strip_affinity(item)
        parts = item.split(":")
        if tres:
            head = parts[0].lower()
            if head in ("gres/shard", "shard"):
                raise ValueError(item)
            if head not in ("gpu", "gres/gpu"):
                continue
            parts = parts[1:]
        elif gres:
            head = parts[0].lower()
            if head == "shard":
                raise ValueError(item)
            if head != "gpu":
                continue
            parts = parts[1:]
        if not parts:
            n += 1
            continue
        last = parts[-1]
        if last.isdigit():
            n += int(last)
        elif len(parts) == 1:
            # e.g. --gres=gpu:a100 or --gpus=a100 means 1 GPU of that type
            n += 1
        else:
            raise ValueError(item)
    return n


def _sockets_from_extra_node_info(text: str) -> int:
    # -B sockets[:cores[:threads]] (may have min-max ranges; take upper bound)
    sock = text.split(":")[0].strip()
    if not sock or sock == "*":
        raise ValueError(text)
    return _count(sock)


def resources(header: Header, cores_per_node: int | None = None,
              gpus_per_node: int | None = None) -> Resources:
    o = header_options(header.directives)
    unknown: list[str] = []

    def read(key, conv, default):
        if key not in o:
            return default
        try:
            return conv(o[key])
        except ValueError:
            unknown.append(key)
            return default

    nodes = read("nodes", _count, 1)
    cpus = read("cpus_per_task", _count, 1)
    sockets: int | None = None
    if "sockets_per_node" in o:
        sockets = read("sockets_per_node", _count, None)
    elif "extra_node_info" in o:
        try:
            sockets = _sockets_from_extra_node_info(o["extra_node_info"])
        except ValueError:
            sockets = None

    if "ntasks" in o:
        tasks = read("ntasks", _count, 1)
    elif "ntasks_per_node" in o:
        tasks = nodes * read("ntasks_per_node", _count, 1)
    elif "ntasks_per_socket" in o:
        tps = read("ntasks_per_socket", _count, None)
        if tps is not None and sockets is not None:
            tasks = nodes * sockets * tps
        else:
            unknown.append("ntasks_per_socket")
            tasks = 1
    elif "ntasks_per_core" in o:
        unknown.append("ntasks_per_core")
        tasks = 1
    else:
        tasks = nodes

    # Collect GPU counts across every Slurm GPU / TRES directive
    gpu_candidates = [
        nodes * read("gres", lambda v: _gpus(v, gres=True), 0),
        read("gpus", lambda v: _gpus(v), 0),
        nodes * read("gpus_per_node", lambda v: _gpus(v), 0),
        tasks * read("gpus_per_task", lambda v: _gpus(v), 0),
        read("tres_per_job", lambda v: _gpus(v, tres=True), 0),
        nodes * read("tres_per_node", lambda v: _gpus(v, tres=True), 0),
        tasks * read("tres_per_task", lambda v: _gpus(v, tres=True), 0),
    ]
    if "gpus_per_socket" in o:
        gps = read("gpus_per_socket", lambda v: _gpus(v), None)
        if gps is not None and sockets is not None:
            gpu_candidates.append(nodes * sockets * gps)
        elif gps is None or gps > 0:
            unknown.append("gpus_per_socket")
    if "tres_per_socket" in o:
        tps_g = read("tres_per_socket", lambda v: _gpus(v, tres=True), None)
        if tps_g is not None and tps_g > 0:
            if sockets is not None:
                gpu_candidates.append(nodes * sockets * tps_g)
            else:
                unknown.append("tres_per_socket")

    # Reject unknown CPU/TRES accounting if tres_per_* specifies cpu explicitly
    for tk in ("tres_per_job", "tres_per_node", "tres_per_socket", "tres_per_task"):
        if tk in o and any(p.strip().lower().startswith("cpu:") for p in o[tk].split(",")):
            unknown.append(tk)

    gpus: int | None = max(gpu_candidates)
    cores: int | None = tasks * cpus
    if "cpus_per_gpu" in o:
        cpg = read("cpus_per_gpu", _count, None)
        if cpg is not None and gpus and not unknown:
            cores = max(cores, gpus * cpg)
        elif cpg is not None and not gpus:
            unknown.append("cpus_per_gpu")
    if "exclusive" in o:
        cores = nodes * cores_per_node if cores_per_node else None
        if cores is None:
            unknown.append("cores")
        if gpus_per_node is not None:
            gpus = max(gpus, nodes * gpus_per_node)
        else:
            gpus = None
            unknown.append("gpus")
    if unknown:
        cores = gpus = None
    hours = read("time", time_limit_hours, None)
    if hours is None and "time" not in unknown:
        unknown.append("time")
    elements = read("array", array_size, 1)
    if unknown:
        return Resources(None, None, tuple(dict.fromkeys(unknown)), cores, gpus)
    return Resources(cores * hours * elements, gpus * hours * elements, (), cores, gpus)


def wrapper_script(header: Header, script_abs: str, hook_dir: str,
                   env: dict[str, str]) -> str:
    """Generate a batch wrapper that forwards signals and records start/end/exit hooks."""
    q = shlex.quote
    out = ["#!/bin/bash"]
    out += list(header.directives)
    out += ["# hpcport wrapper (generated); original script: %s" % script_abs]
    out += ["export %s=%s" % (k, q(v)) for k, v in sorted(env.items())]
    out += [
        "__hpcp_dir=%s" % q(hook_dir),
        '__hpcp_id="${SLURM_ARRAY_JOB_ID:+${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}}"',
        '__hpcp_id="${__hpcp_id:-${SLURM_JOB_ID:-nojob}}"',
        'mkdir -p "$__hpcp_dir" && echo "$(date +%s) $(hostname)" > "$__hpcp_dir/$__hpcp_id.start"',
        "__hpcp_pid=",
        "for __s in TERM INT HUP USR1 USR2; do",
        '  trap "[ -n \\"\\$__hpcp_pid\\" ] && kill -$__s \\"\\$__hpcp_pid\\" 2>/dev/null" $__s',
        "done",
        "%s %s &" % (header.interpreter, q(script_abs)),
        "__hpcp_pid=$!",
        'wait "$__hpcp_pid"; __hpcp_rc=$?',
        'while kill -0 "$__hpcp_pid" 2>/dev/null; do wait "$__hpcp_pid"; __hpcp_rc=$?; done',
        'wait "$__hpcp_pid" 2>/dev/null; __s=$?; [ "$__s" -ne 127 ] && __hpcp_rc=$__s',
        'echo "$(date +%s) $__hpcp_rc" > "$__hpcp_dir/$__hpcp_id.end"',
        'exit "$__hpcp_rc"',
    ]
    return "\n".join(out) + "\n"


def parse_sbatch(out: str) -> str | None:
    for line in out.strip().splitlines()[::-1]:
        head = line.strip().split(";")[0]
        if head.isdigit():
            return head
    return None


def parse_squeue(out: str) -> dict[str, dict]:
    rows: dict[str, dict] = {}
    for line in out.splitlines():
        parts = line.strip().split("|", 2)
        if len(parts) != 3 or not parts[0].isdigit():
            continue
        jid, raw, workdir = parts
        r = rows.setdefault(jid, {"states": [], "raws": [], "workdir": workdir})
        r["states"].append(classify(raw) or RUNNING)
        r["raws"].append(raw)
    return {
        jid: {"state": aggregate(r["states"]), "raw": ",".join(sorted(set(r["raws"]))),
              "workdir": r["workdir"]}
        for jid, r in rows.items()
    }


def sacct_cmd(ids: list[str]) -> str:
    return (TIME_ENV + "sacct -X -n -P -j " + ",".join(ids)
            + " -o JobID,State,ExitCode,Start,End")


def parse_sacct(out: str) -> dict[str, dict]:
    rows: dict[str, list] = {}
    for line in out.splitlines():
        parts = line.strip().split("|")
        if len(parts) != 5:
            continue
        jid = parts[0].split("_")[0].split(".")[0]
        if jid.isdigit():
            rows.setdefault(jid, []).append(parts[1:])
    res = {}
    for jid, items in rows.items():
        states = [classify(i[0]) for i in items]
        starts = [t for t in (_epoch(i[2]) for i in items) if t is not None]
        ends = [t for t in (_epoch(i[3]) for i in items) if t is not None]
        codes = sorted(set(i[1] for i in items))
        bad = [c for c in codes if c != "0:0"]
        res[jid] = {
            "state": None if None in states else aggregate(states),
            "raw": ",".join(sorted(set(i[0].split()[0] for i in items if i[0]))),
            "exit_code": (bad or codes or [None])[0],
            "start": min(starts) if starts else None,
            "end": max(ends) if len(ends) == len(items) else None,
            "elapsed": elapsed((s, _epoch(i[2]), _epoch(i[3])) for s, i in zip(states, items)),
        }
    return res


def parse_scontrol(out: str) -> dict | None:
    records: list[dict] = []
    for tok in out.split():
        if "=" not in tok:
            continue
        k, v = tok.split("=", 1)
        if k == "JobId":
            records.append({})
        if records:
            records[-1].setdefault(k, v)
    records = [r for r in records if "JobState" in r]
    if not records:
        return None
    states = [classify(r["JobState"]) for r in records]
    codes = [r["ExitCode"] for r in records if "ExitCode" in r]
    starts = [t for t in (_epoch(r.get("StartTime", "")) for r in records) if t is not None]
    ends = [t for t in (_epoch(r.get("EndTime", "")) for r in records) if t is not None]
    return {
        "state": None if None in states else aggregate(states),
        "raw": ",".join(sorted(set(r["JobState"] for r in records))),
        "exit_code": ([c for c in codes if c != "0:0"] or codes or [None])[0],
        "start": min(starts) if starts else None,
        "end": max(ends) if len(ends) == len(records) else None,
        "elapsed": elapsed(
            (s, _epoch(r.get("StartTime", "")), _epoch(r.get("EndTime", "")))
            for s, r in zip(states, records)
        ),
    }


def parse_hooks(text: str) -> list[tuple[float, str]]:
    out = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[0].isdigit():
            out.append((float(parts[0]), parts[1]))
    return out


def hook_summary(start_text: str, end_text: str) -> dict:
    starts, ends = parse_hooks(start_text), parse_hooks(end_text)
    codes = [c for _, c in ends]
    paired = bool(ends) and len(ends) == len(starts)
    return {
        "start": min(t for t, _ in starts) if starts else None,
        "end": max(t for t, _ in ends) if ends else None,
        "exit_code": next((c for c in codes if c != "0"), "0") if codes else None,
        "complete": bool(ends) and len(ends) >= len(starts),
        "elapsed": max(0.0, sum(t for t, _ in ends) - sum(t for t, _ in starts)) if paired else None,
    }
