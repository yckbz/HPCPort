"""Global configuration, cluster presets, and multi-project configurations.

All persistent HPCPort files live under $HPCPORT_HOME (default ~/.hpcport),
leaving user code repositories completely untouched:

  ~/.hpcport/
    config.yaml                 global daemon settings
    state.db                    SQLite state database
    run/                        daemon pid, lock, cli.token, SSH control sockets
    clusters/<name>.yaml        shared compute server presets (+ history/<name>/)
    projects/<name>.yaml        project configs (+ history/<name>/)
    staging/                    temporary cluster-to-cluster transfer staging
"""

from __future__ import annotations

import dataclasses
import os
import re
from dataclasses import dataclass, field
from pathlib import Path

from . import versions

NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")

SCHEDULERS = ("slurm", "none")

PRESET_KEYS = (
    "connect", "host", "scheduler", "partitions", "max_jobs", "python",
    "cores_per_node", "poll_interval_s", "job_template", "guide",
    "projects_root", "exec_timeout_s", "extra_paths", "gateway_allow",
)
PRESET_SAFETY = (
    "connect", "host", "scheduler", "partitions", "max_jobs", "projects_root",
    "extra_paths", "gateway_allow", "exec_timeout_s", "cores_per_node",
)

PROJECT_KEYS = ("project", "local_dir", "clusters", "wake", "submit", "gateway")
PROJECT_PROPOSABLE = ("wake", "submit", "gateway")
PROJECT_SAFETY = ("local_dir", "clusters", "submit", "gateway")

GLOBAL_KEYS = ("port", "auto_open_dashboard", "web_only_approval", "language")

SUBMIT_LIMITS = (
    "core_hours", "gpu_hours", "total_jobs", "total_core_hours", "total_gpu_hours",
)


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class GlobalConfig:
    port: int = 8765
    auto_open_dashboard: bool = True
    web_only_approval: bool = False
    language: str = "auto"


@dataclass(frozen=True)
class ClusterPreset:
    name: str
    connect: str = "ssh"                # "ssh" | "local"
    host: str | None = None
    scheduler: str = "slurm"            # "slurm" | "none"
    projects_root: str | None = None
    partitions: tuple[str, ...] = ()
    max_jobs: int = 10                  # default per-project concurrency limit
    python: str = "python3"
    cores_per_node: int | None = None
    poll_interval_s: int = 120
    job_template: str | None = None
    guide: str = ""
    exec_timeout_s: int = 3600
    extra_paths: tuple[str, ...] = ()
    gateway_allow: tuple[str, ...] = ()


@dataclass(frozen=True)
class Cluster:
    """A cluster preset attached to a specific project with its remote root."""
    name: str
    root: str
    connect: str = "ssh"
    host: str | None = None
    scheduler: str = "slurm"
    projects_root: str | None = None
    partitions: tuple[str, ...] = ()
    max_jobs: int = 10                  # effective concurrency limit for this project
    python: str = "python3"
    cores_per_node: int | None = None
    poll_interval_s: int = 120
    job_template: str | None = None
    guide: str = ""
    exec_timeout_s: int = 3600
    extra_paths: tuple[str, ...] = ()
    gateway_allow: tuple[str, ...] = ()

    @classmethod
    def from_preset(cls, preset: ClusterPreset, root: str,
                    max_jobs: int | None = None) -> Cluster:
        return cls(
            name=preset.name,
            root=root.rstrip("/") or "/",
            connect=preset.connect,
            host=preset.host,
            scheduler=preset.scheduler,
            projects_root=preset.projects_root,
            partitions=preset.partitions,
            max_jobs=preset.max_jobs if max_jobs is None else max_jobs,
            python=preset.python,
            cores_per_node=preset.cores_per_node,
            poll_interval_s=preset.poll_interval_s,
            job_template=preset.job_template,
            guide=preset.guide,
            exec_timeout_s=preset.exec_timeout_s,
            extra_paths=preset.extra_paths,
            gateway_allow=preset.gateway_allow,
        )


@dataclass(frozen=True)
class SubmitPolicy:
    approval: bool = True
    wait_s: float = 180.0
    core_hours: float | None = 5000.0
    gpu_hours: float | None = 100.0
    total_jobs: int | None = 500
    total_core_hours: float | None = 100000.0
    total_gpu_hours: float | None = 1000.0


@dataclass(frozen=True)
class GatewayPolicy:
    transfer_max_gb: float = 5.0
    max_background: int = 4
    wait_s: float = 180.0


@dataclass(frozen=True)
class WakePolicy:
    fail_frac: float = 0.10
    timeout_h: float = 48.0
    default_wait_s: float = 180.0


@dataclass
class Project:
    """Mutable project configuration held in memory by the global daemon."""
    name: str
    local_dir: Path | None = None
    clusters: dict[str, Cluster] = field(default_factory=dict)
    cluster_overrides: dict[str, int | None] = field(default_factory=dict)
    wake: WakePolicy = field(default_factory=WakePolicy)
    submit: SubmitPolicy = field(default_factory=SubmitPolicy)
    gateway: GatewayPolicy = field(default_factory=GatewayPolicy)

    @property
    def dir(self) -> Path | None:
        return self.local_dir

    @property
    def fail_frac(self) -> float:
        return self.wake.fail_frac

    @property
    def timeout_h(self) -> float:
        return self.wake.timeout_h

    @property
    def transfer_max_gb(self) -> float:
        return self.gateway.transfer_max_gb

    @property
    def max_background(self) -> int:
        return self.gateway.max_background


# ------------------------------------------------------------------ paths
def home() -> Path:
    return Path(os.environ.get("HPCPORT_HOME") or Path.home() / ".hpcport")


def state_db_path() -> Path:
    return home() / "state.db"


def run_dir() -> Path:
    return home() / "run"


def cli_token_file() -> Path:
    return run_dir() / "cli.token"


def daemon_pid_file() -> Path:
    return run_dir() / "daemon.pid"


def daemon_lock_file() -> Path:
    return run_dir() / "daemon.lock"


def staging_dir() -> Path:
    return home() / "staging"


def global_config_file() -> Path:
    return home() / "config.yaml"


def presets_dir() -> Path:
    return home() / "clusters"


def projects_dir() -> Path:
    return home() / "projects"


def preset_file(name: str) -> versions.VersionedFile:
    d = presets_dir()
    return versions.VersionedFile(d / (name + ".yaml"), d / "history" / name)


def project_file(name: str) -> versions.VersionedFile:
    d = projects_dir()
    return versions.VersionedFile(d / (name + ".yaml"), d / "history" / name)


def approved_text(vf: versions.VersionedFile) -> str:
    cur = vf.latest()
    if cur is not None:
        return cur[1]
    text = vf.read()
    if text is None:
        raise ConfigError("cannot read %s: no such file" % vf.file)
    return text


def list_preset_names() -> list[str]:
    d = presets_dir()
    if not d.is_dir():
        return []
    return sorted(p.stem for p in d.glob("*.yaml") if p.is_file() and NAME_RE.match(p.stem))


def list_project_names() -> list[str]:
    d = projects_dir()
    if not d.is_dir():
        return []
    return sorted(p.stem for p in d.glob("*.yaml") if p.is_file() and NAME_RE.match(p.stem))


# ------------------------------------------------------------------ parsing
def _number(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _parse_text(text: str, where: str) -> dict:
    try:
        return versions.load(text)
    except ValueError as e:
        raise ConfigError("%s: %s" % (where, e))


def _only_keys(data: dict, allowed: tuple[str, ...], where: str) -> None:
    extra = sorted(set(data) - set(allowed))
    if extra:
        raise ConfigError("%s: unknown keys %s (allowed: %s)"
                          % (where, ", ".join(extra), ", ".join(allowed)))


def validate_name(name: str, kind: str = "name") -> str:
    if not isinstance(name, str) or not NAME_RE.match(name) or name in (".", ".."):
        raise ConfigError("invalid %s %r (use letters, digits, '.', '_' and '-')" % (kind, name))
    return name


def load_global_config() -> GlobalConfig:
    p = global_config_file()
    if not p.exists():
        return GlobalConfig()
    d = _parse_text(p.read_text(encoding="utf-8"), str(p))
    return parse_global_config(d, str(p))


def parse_global_config(d: dict, path: str = "config.yaml") -> GlobalConfig:
    if not isinstance(d, dict):
        raise ConfigError("%s must contain a mapping" % path)
    _only_keys(d, GLOBAL_KEYS, path)
    port = d.get("port", GlobalConfig.port)
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        raise ConfigError("%s: port must be an integer between 1 and 65535" % path)
    auto_open = d.get("auto_open_dashboard", GlobalConfig.auto_open_dashboard)
    if not isinstance(auto_open, bool):
        raise ConfigError("%s: auto_open_dashboard must be true or false" % path)
    web_only = d.get("web_only_approval", GlobalConfig.web_only_approval)
    if not isinstance(web_only, bool):
        raise ConfigError("%s: web_only_approval must be true or false" % path)
    lang = str(d.get("language", GlobalConfig.language))
    if lang not in ("auto", "en", "zh"):
        raise ConfigError("%s: language must be auto, en or zh" % path)
    return GlobalConfig(port=port, auto_open_dashboard=auto_open,
                        web_only_approval=web_only, language=lang)


def save_global_config(cfg: GlobalConfig) -> None:
    p = global_config_file()
    p.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "port": cfg.port,
        "auto_open_dashboard": cfg.auto_open_dashboard,
        "web_only_approval": cfg.web_only_approval,
        "language": cfg.language,
    }
    text = versions.dump(data)
    tmp = p.with_name(".%s.%d.tmp" % (p.name, os.getpid()))
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, p)


def load_preset(name: str) -> ClusterPreset:
    validate_name(name, "cluster name")
    vf = preset_file(name)
    return parse_preset(name, _parse_text(approved_text(vf), str(vf.file)), str(vf.file))


def parse_preset(name: str, d: dict, path: str) -> ClusterPreset:
    try:
        return _parse_preset(name, d, path)
    except (TypeError, ValueError, AttributeError) as e:
        raise ConfigError("%s: invalid value (%s)" % (path, e))


def _parse_preset(name: str, d: dict, path: str) -> ClusterPreset:
    validate_name(name, "cluster name")
    if not isinstance(d, dict):
        raise ConfigError("%s must contain a mapping" % path)
    _only_keys(d, PRESET_KEYS, str(path))
    connect = d.get("connect", "ssh")
    if connect not in ("ssh", "local"):
        raise ConfigError("%s: connect must be ssh or local" % path)
    host = d.get("host")
    if connect == "ssh" and not host:
        raise ConfigError("%s: host is required for connect: ssh" % path)
    scheduler = d.get("scheduler", "slurm")
    if scheduler not in SCHEDULERS:
        raise ConfigError("%s: scheduler must be one of: %s" % (path, ", ".join(SCHEDULERS)))
    max_jobs = d.get("max_jobs", 10)
    if not isinstance(max_jobs, int) or isinstance(max_jobs, bool) or max_jobs < 1:
        raise ConfigError("%s: max_jobs must be a positive integer" % path)
    interval = d.get("poll_interval_s", 120)
    if not _number(interval) or interval <= 0:
        raise ConfigError("%s: poll_interval_s must be a positive number" % path)
    cpn = d.get("cores_per_node")
    if cpn is not None and (not isinstance(cpn, int) or isinstance(cpn, bool) or cpn < 1):
        raise ConfigError("%s: cores_per_node must be a positive integer" % path)
    base = d.get("projects_root")
    if base is not None and (not isinstance(base, str) or not os.path.isabs(base)
                             or not base.rstrip("/")):
        raise ConfigError("%s: projects_root must be an absolute path other than /" % path)
    parts = d.get("partitions") or []
    if not isinstance(parts, list) or not all(isinstance(p, str) for p in parts):
        raise ConfigError("%s: partitions must be a list of names" % path)
    limit = d.get("exec_timeout_s", ClusterPreset.exec_timeout_s)
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise ConfigError("%s: exec_timeout_s must be a positive integer" % path)
    extra = d.get("extra_paths") or []
    if not isinstance(extra, list) or not all(
            isinstance(p, str) and os.path.isabs(p) and p.rstrip("/") for p in extra):
        raise ConfigError("%s: extra_paths must be a list of absolute paths other than /" % path)
    from .guard import ALLOWABLE
    allow = d.get("gateway_allow") or []
    if not isinstance(allow, list) or not all(a in ALLOWABLE for a in allow):
        raise ConfigError("%s: gateway_allow must be a list of rules among: %s"
                          % (path, ", ".join(ALLOWABLE)))
    return ClusterPreset(
        name=name,
        connect=connect,
        host=str(host) if host else None,
        scheduler=scheduler,
        projects_root=base.rstrip("/") if base else None,
        partitions=tuple(parts),
        max_jobs=max_jobs,
        python=str(d.get("python", "python3")),
        cores_per_node=cpn,
        poll_interval_s=int(interval),
        job_template=d.get("job_template"),
        guide=str(d.get("guide") or "").strip(),
        exec_timeout_s=limit,
        extra_paths=tuple(p.rstrip("/") for p in extra),
        gateway_allow=tuple(allow),
    )


def load_project(name: str, presets: dict[str, ClusterPreset] | None = None) -> Project:
    validate_name(name, "project name")
    vf = project_file(name)
    return parse_project(name, _parse_text(approved_text(vf), str(vf.file)),
                         str(vf.file), presets=presets)


def parse_project(name: str, d: dict, path: str,
                  presets: dict[str, ClusterPreset] | None = None) -> Project:
    try:
        return _parse_project(name, d, path, presets)
    except (TypeError, ValueError, AttributeError) as e:
        raise ConfigError("%s: invalid value (%s)" % (path, e))


def _parse_project(name: str, d: dict, path: str,
                   presets: dict[str, ClusterPreset] | None) -> Project:
    validate_name(name, "project name")
    if not isinstance(d, dict):
        raise ConfigError("%s must contain a mapping" % path)
    _only_keys(d, PROJECT_KEYS, str(path))
    pname = str(d.get("project") or name)
    validate_name(pname, "project name")
    raw_dir = d.get("local_dir")
    local_dir: Path | None = None
    if raw_dir is not None and str(raw_dir).strip():
        p = Path(str(raw_dir)).expanduser()
        if not p.is_absolute():
            raise ConfigError("%s: local_dir must be an absolute path: %r" % (path, raw_dir))
        local_dir = p.resolve()

    given = d.get("clusters") or {}
    if not isinstance(given, dict):
        raise ConfigError("%s: clusters must be a mapping" % path)
    clusters: dict[str, Cluster] = {}
    overrides: dict[str, int | None] = {}
    for cname, spec in given.items():
        if not isinstance(spec, dict) or not spec.get("root"):
            raise ConfigError("%s: clusters.%s needs a 'root'" % (path, cname))
        root = str(spec["root"])
        if not os.path.isabs(root) or root.rstrip("/") == "":
            raise ConfigError("%s: clusters.%s.root must be an absolute path other than /"
                              % (path, cname))
        _only_keys(spec, ("root", "max_jobs"), "%s: clusters.%s" % (path, cname))
        mj = spec.get("max_jobs")
        if mj is not None and (not isinstance(mj, int) or isinstance(mj, bool) or mj < 1):
            raise ConfigError("%s: clusters.%s.max_jobs must be a positive integer" % (path, cname))
        preset = (presets[cname] if presets is not None and cname in presets
                  else load_preset(cname))
        clusters[cname] = Cluster.from_preset(preset, root, max_jobs=mj)
        overrides[cname] = mj

    wake_d = d.get("wake") or {}
    _only_keys(wake_d, ("fail_frac", "timeout_h", "default_wait_s"), "%s: wake" % path)
    fail_frac = float(wake_d.get("fail_frac", WakePolicy.fail_frac))
    timeout_h = float(wake_d.get("timeout_h", WakePolicy.timeout_h))
    default_wait_s = float(wake_d.get("default_wait_s", WakePolicy.default_wait_s))
    if not 0.0 <= fail_frac <= 1.0:
        raise ConfigError("%s: wake.fail_frac must be between 0 and 1" % path)
    if not timeout_h > 0:
        raise ConfigError("%s: wake.timeout_h must be positive" % path)
    if not 0.0 <= default_wait_s <= 180.0:
        raise ConfigError("%s: wake.default_wait_s must be between 0 and 180" % path)
    wake = WakePolicy(fail_frac=fail_frac, timeout_h=timeout_h, default_wait_s=default_wait_s)

    sub_d = d.get("submit") or {}
    _only_keys(sub_d, ("approval", "wait_s") + SUBMIT_LIMITS, "%s: submit" % path)
    if not isinstance(sub_d.get("approval", True), bool):
        raise ConfigError("%s: submit.approval must be true or false" % path)
    swait = sub_d.get("wait_s", SubmitPolicy.wait_s)
    if not _number(swait) or not 0 <= swait <= 180:
        raise ConfigError("%s: submit.wait_s must be between 0 and 180" % path)
    for key in SUBMIT_LIMITS:
        v = sub_d.get(key)
        if v is not None and (not _number(v) or v < 0
                              or (key == "total_jobs" and not isinstance(v, int))):
            raise ConfigError("%s: submit.%s must be a non-negative number or null"
                              % (path, key))
    submit_kw = {k: sub_d[k] for k in ("approval",) + SUBMIT_LIMITS if k in sub_d}
    submit_kw["wait_s"] = float(swait)
    submit = SubmitPolicy(**submit_kw)

    gw_d = d.get("gateway") or {}
    _only_keys(gw_d, ("transfer_max_gb", "max_background", "wait_s"), "%s: gateway" % path)
    max_gb = gw_d.get("transfer_max_gb", GatewayPolicy.transfer_max_gb)
    if not _number(max_gb) or max_gb < 0:
        raise ConfigError("%s: gateway.transfer_max_gb must be a non-negative number" % path)
    max_bg = gw_d.get("max_background", GatewayPolicy.max_background)
    if not isinstance(max_bg, int) or isinstance(max_bg, bool) or max_bg < 0:
        raise ConfigError("%s: gateway.max_background must be a non-negative integer" % path)
    gwait = gw_d.get("wait_s", GatewayPolicy.wait_s)
    if not _number(gwait) or not 0 <= gwait <= 240:
        raise ConfigError("%s: gateway.wait_s must be between 0 and 240" % path)
    gateway = GatewayPolicy(transfer_max_gb=float(max_gb), max_background=max_bg,
                            wait_s=float(gwait))

    return Project(
        name=pname,
        local_dir=local_dir,
        clusters=clusters,
        cluster_overrides=overrides,
        wake=wake,
        submit=submit,
        gateway=gateway,
    )


def resolve_project_by_cwd(cwd: str | Path, projects: dict[str, Project]) -> str | None:
    """Return the registered project whose local_dir best contains cwd (longest prefix)."""
    cur = Path(cwd).resolve()
    best: tuple[int, str] | None = None
    for pname, proj in projects.items():
        if proj.local_dir is None:
            continue
        base = proj.local_dir.resolve()
        if cur == base or base in cur.parents:
            depth = len(base.parts)
            if best is None or depth > best[0]:
                best = (depth, pname)
    return best[1] if best else None


def preset_to_dict(p: ClusterPreset) -> dict:
    d: dict = {"connect": p.connect, "scheduler": p.scheduler}
    if p.host:
        d["host"] = p.host
    if p.projects_root:
        d["projects_root"] = p.projects_root
    if p.partitions:
        d["partitions"] = list(p.partitions)
    d["max_jobs"] = p.max_jobs
    if p.cores_per_node is not None:
        d["cores_per_node"] = p.cores_per_node
    if p.scheduler == "slurm":
        d["poll_interval_s"] = p.poll_interval_s
    if p.python != "python3":
        d["python"] = p.python
    if p.job_template:
        d["job_template"] = p.job_template
    d["exec_timeout_s"] = p.exec_timeout_s
    if p.extra_paths:
        d["extra_paths"] = list(p.extra_paths)
    if p.gateway_allow:
        d["gateway_allow"] = list(p.gateway_allow)
    if p.guide:
        d["guide"] = p.guide
    return d


def project_to_dict(p: Project) -> dict:
    d: dict = {"project": p.name}
    if p.local_dir is not None:
        d["local_dir"] = str(p.local_dir)
    clusters: dict = {}
    for cname, cl in p.clusters.items():
        entry: dict = {"root": cl.root}
        if p.cluster_overrides.get(cname) is not None:
            entry["max_jobs"] = p.cluster_overrides[cname]
        clusters[cname] = entry
    d["clusters"] = clusters
    d["wake"] = dataclasses.asdict(p.wake)
    d["submit"] = dataclasses.asdict(p.submit)
    d["gateway"] = dataclasses.asdict(p.gateway)
    return d
