"""Attach a compute server preset to a project and initialize its remote root.

Steps:
1. Validate preset & derive remote root (<projects_root>/<project> if omitted);
2. Probe connectivity over Executor;
3. Create/validate the remote root directory (only the final component is created;
   a derived root must not exist yet or be empty; an explicit root may hold files);
4. Record the attachment into ~/.hpcport/projects/<project>.yaml as a new version.
"""

from __future__ import annotations

import shlex

from . import versions
from .config import (
    NAME_RE, Cluster, ClusterPreset, ConfigError, Project,
    load_preset, preset_file, project_file,
)
from .executor import Executor, TransportError


class AttachError(Exception):
    pass


def plan(project: Project, cluster_name: str, root: str | None = None,
         max_jobs: int | None = None,
         preset: ClusterPreset | None = None) -> tuple[Cluster, bool]:
    if cluster_name in project.clusters:
        raise AttachError("cluster %s is already attached to project %s (root %s)"
                          % (cluster_name, project.name, project.clusters[cluster_name].root))
    if max_jobs is not None and (not isinstance(max_jobs, int) or isinstance(max_jobs, bool)
                                 or max_jobs < 1):
        raise AttachError("max_jobs must be a positive integer")
    if preset is None:
        try:
            preset = load_preset(cluster_name)
        except ConfigError as e:
            raise AttachError(str(e))
    derived = root is None
    if derived:
        if not preset.projects_root:
            raise AttachError("cluster preset %s has no projects_root; set it in the preset or "
                              "provide an explicit remote root" % cluster_name)
        if not NAME_RE.match(project.name) or project.name in (".", ".."):
            raise AttachError("project name %r cannot be used as a directory name; provide an "
                              "explicit remote root" % project.name)
        root = preset.projects_root + "/" + project.name
    root = root.rstrip("/") if root != "/" else root
    parts = root.split("/")
    if not root.startswith("/") or root == "/" or any(p in ("", ".", "..") for p in parts[1:]):
        raise AttachError("remote root for %s must be an absolute path other than /, "
                          "without '.', '..' or empty components: %r" % (cluster_name, root))
    return Cluster.from_preset(preset, root, max_jobs=max_jobs), derived


def test_connection(ex: Executor, name: str) -> dict:
    """Verify server connectivity and probe home directory and Slurm availability."""
    script = (
        "echo HPCPORT_OK\n"
        "printf 'HOME %s\\n' \"$HOME\"\n"
        "command -v sbatch >/dev/null 2>&1 && echo 'SLURM yes' || echo 'SLURM no'\n"
    )
    try:
        r = ex.run(script, timeout=30)
    except TransportError as e:
        raise AttachError("cannot reach server %s: %s" % (name, e))
    if r.rc != 0 or "HPCPORT_OK" not in r.out.split():
        raise AttachError("server %s test command failed (exit %d): %s"
                          % (name, r.rc, (r.err or r.out).strip()[-300:]))
    info = {"ok": True, "home": None, "has_slurm": False}
    for line in r.out.splitlines():
        k, _, v = line.partition(" ")
        if k == "HOME":
            info["home"] = v.strip() or None
        elif k == "SLURM":
            info["has_slurm"] = v.strip() == "yes"
    return info


def make_root(ex: Executor, cluster: Cluster, derived: bool) -> str:
    """Reach server, verify/create project remote root, return resolved real path."""
    test_connection(ex, cluster.name)
    q = shlex.quote
    parent = cluster.root.rsplit("/", 1)[0] or "/"
    script = (
        "d=%s; p=%s; s=''\n"
        "if [ ! -e \"$d\" ] && [ ! -L \"$d\" ]; then\n"
        "  if [ ! -d \"$p\" ]; then s=noparent\n"
        "  elif err=$(mkdir -- \"$d\" 2>&1); then s=made\n"
        "  else s=\"mkfail $err\"; fi\n"
        "elif [ ! -d \"$d\" ]; then s=notdir\n"
        "elif [ ! -r \"$d\" ] || [ ! -w \"$d\" ] || [ ! -x \"$d\" ]; then s=noaccess\n"
        "elif [ -n \"$(ls -A -- \"$d\")\" ]; then s=nonempty\n"
        "else s=empty; fi\n"
        "printf 'STATE %%s\\n' \"$s\" | head -n 1\n"
        "case $s in made|empty|nonempty) "
        "printf 'REAL %%s\\n' \"$(cd -- \"$d\" && pwd -P)\";; esac\n"
        % (q(cluster.root), q(parent))
    )
    try:
        r = ex.run(script, timeout=60)
    except TransportError as e:
        raise AttachError("cannot reach server %s: %s" % (cluster.name, e))
    state, detail, real = None, "", ""
    for line in r.out.splitlines():
        key, _, val = line.partition(" ")
        if key == "STATE" and state is None:
            state, _, detail = val.partition(" ")
        elif key == "REAL":
            real = val
    where = "%s on %s" % (cluster.root, cluster.name)
    if r.rc != 0 or state is None:
        raise AttachError("cannot create %s (exit %d): %s"
                          % (where, r.rc, (r.err or r.out).strip()[-300:]))
    if state == "noparent":
        raise AttachError("cannot create %s: parent directory %s does not exist" % (where, parent))
    if state == "mkfail":
        raise AttachError("cannot create %s: %s" % (where, detail.strip()))
    if state == "notdir":
        raise AttachError("%s exists and is not a directory" % where)
    if state == "noaccess":
        raise AttachError("%s is not readable, writable and searchable by you" % where)
    if state == "nonempty" and derived:
        raise AttachError("%s already exists and is not empty; specify an explicit remote root"
                          % where)
    if not real:
        raise AttachError("cannot resolve %s" % where)
    return real


def record(project: Project, cluster: Cluster, max_jobs: int | None = None) -> int:
    """Persist cluster attachment into ~/.hpcport/projects/<project>.yaml."""
    vf = project_file(project.name)
    entry: dict = {"root": cluster.root}
    if max_jobs is not None:
        entry["max_jobs"] = max_jobs
    try:
        with vf.lock():
            text = vf.read()
            cur = vf.latest()
            if cur is None:
                if text is None:
                    raise ConfigError("cannot read %s: no such file" % vf.file)
                cur = (vf.add(text), text)
            if text != cur[1]:
                raise ConfigError("%s has undecided disk edits; approve or reject them first"
                                  % vf.file)
            old = versions.load(cur[1])
            if cluster.name in (old.get("clusters") or {}):
                raise AttachError("cluster %s is already attached to project %s"
                                  % (cluster.name, project.name))
            new = versions.apply(old, {"clusters": {cluster.name: entry}})
            new_text = versions.edit(cur[1], new)
            vf.write(new_text)
            v = vf.add(new_text)
    except (ConfigError, ValueError) as e:
        raise AttachError(str(e))
    pf = preset_file(cluster.name)
    with pf.lock():
        ptext = pf.read()
        if pf.latest() is None and ptext is not None:
            pf.add(ptext)
    return v
