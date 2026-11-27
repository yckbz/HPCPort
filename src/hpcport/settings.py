"""Versioned cluster presets and multi-project configurations.

Both global cluster presets (~/.hpcport/clusters/<name>.yaml) and project
configurations (~/.hpcport/projects/<name>.yaml) are managed with numbered
history and comment-preserving YAML patching (versions.py).

Changes happen in three ways:
  1. Direct user edit via Web UI or CLI -> immediately approved new version;
  2. Agent proposal via `propose_preset` -> approval queue (`kind='proposal'`)
     with field-level old -> new diff, [SAFETY] badges, and stale-field checks;
  3. Manual file edit on disk -> detected on poll cycle (`origin='detected'`),
     surfaced to approval queue while the approved version stays in use.
"""

from __future__ import annotations

import difflib
import json
import logging
import shutil
import time

from . import config, versions
from .approvals import ApprovalError
from .config import (
    PRESET_KEYS, PRESET_SAFETY, PROJECT_KEYS, PROJECT_PROPOSABLE, PROJECT_SAFETY,
    SUBMIT_LIMITS, Cluster, ClusterPreset, ConfigError, Project,
    list_preset_names, list_project_names, parse_preset, parse_project,
    preset_file, preset_to_dict, project_file, project_to_dict, validate_name,
)

log = logging.getLogger("hpcport.settings")

PRESET = "cluster_preset"
PROJECT = "project_config"
APPROVAL_KIND = "proposal"


class SettingError(Exception):
    def __init__(self, code: str, **params):
        super().__init__(code)
        self.code = code
        self.params = params


def _fmt_val(v) -> str:
    if v is versions.MISSING:
        return "(unset)"
    if isinstance(v, str) and "\n" in v:
        return v.strip().splitlines()[0][:60] + " ..."
    return json.dumps(v, ensure_ascii=False)


class SettingsService:
    def __init__(self, presets: dict[str, ClusterPreset], projects: dict[str, Project],
                 db, approvals, notify, lock, clock=time.time):
        self.presets = presets
        self.projects = projects
        self.db = db
        self.approvals = approvals
        self.notify = notify
        self.lock = lock
        self.now = clock
        self.retired_projects: set[str] = set()
        self.in_use: dict[str, int] = {}
        self._stamps: dict[str, object] = {}
        self.on_preset_changed = lambda name, old, new: None
        self.on_project_changed = lambda name, old, new: None
        self.on_change = lambda: None
        approvals.handlers[APPROVAL_KIND] = self.on_decision
        approvals.options[APPROVAL_KIND] = ()

    # ------------------------------------------------------------------ startup & loading
    def start(self) -> None:
        """Load all approved presets and projects from ~/.hpcport/."""
        for name in list_preset_names():
            vf = preset_file(name)
            with vf.lock():
                cur = vf.latest()
                if cur is None:
                    text = vf.read()
                    if text is None:
                        continue
                    v = vf.add(text)
                    cur = (v, text)
                data = versions.load(cur[1])
                self.presets[name] = parse_preset(name, data, str(vf.file))
                key = "%s:%s" % (PRESET, name)
                self.in_use[key] = cur[0]
                self._stamps[key] = vf.stamp() if vf.read() == cur[1] else None

        self.retired_projects = {row["name"] for row in self.db.all("SELECT name FROM retired_projects")}
        for name in list_project_names(self.retired_projects):
            vf = project_file(name)
            with vf.lock():
                cur = vf.latest()
                if cur is None:
                    text = vf.read()
                    if text is None:
                        continue
                    v = vf.add(text)
                    cur = (v, text)
                data = versions.load(cur[1])
                self.projects[name] = parse_project(name, data, str(vf.file),
                                                    presets=self.presets)
                key = "%s:%s" % (PROJECT, name)
                self.in_use[key] = cur[0]
                self._stamps[key] = vf.stamp() if vf.read() == cur[1] else None

    def _vf_and_kind(self, key: str) -> tuple[str, str, versions.VersionedFile]:
        kind, sep, name = key.partition(":")
        if not sep or kind not in (PRESET, PROJECT) or not name:
            raise SettingError("unknown_setting", key=key)
        return kind, name, (preset_file(name) if kind == PRESET else project_file(name))

    def _deep(self, kind: str, path: tuple) -> bool:
        return kind == PROJECT

    def _keep_none(self, kind: str, path: tuple) -> bool:
        return kind == PROJECT and len(path) == 2 and path[0] == "submit" and path[1] in SUBMIT_LIMITS

    def _check_project_cluster_changes(self, project: str, old_data: dict, new_data: dict) -> None:
        old_clusters = old_data.get("clusters") or {}
        new_clusters = new_data.get("clusters") or {}
        if not isinstance(old_clusters, dict) or not isinstance(new_clusters, dict):
            return
        for cname, old_spec in old_clusters.items():
            new_spec = new_clusters.get(cname)
            old_root = (old_spec.get("root") or "").rstrip("/") if isinstance(old_spec, dict) else ""
            new_root = (new_spec.get("root") or "").rstrip("/") if isinstance(new_spec, dict) else ""
            if new_spec is None or old_root != new_root:
                active_jobs = self.db.one(
                    "SELECT 1 FROM jobs WHERE project = ? AND cluster = ? "
                    "AND state IN ('unapproved', 'held', 'queued', 'running') LIMIT 1",
                    (project, cname),
                )
                active_ops = self.db.one(
                    "SELECT 1 FROM ops WHERE project = ? AND state = 'running' "
                    "AND instr(',' || cluster || ',', ?) > 0 LIMIT 1",
                    (project, "," + cname + ","),
                )
                if active_jobs or active_ops:
                    raise SettingError("cluster_busy", cluster=cname, project=project)

    def _safety(self, kind: str, path: tuple) -> bool:
        if not path:
            return False
        return path[0] in (PRESET_SAFETY if kind == PRESET else PROJECT_SAFETY)

    def _parse(self, kind: str, name: str, data: dict):
        if kind == PRESET:
            return parse_preset(name, data, "preset %s" % name)
        return parse_project(name, data, "project %s" % name, presets=self.presets)

    # ------------------------------------------------------------------ CRUD (user via Web/CLI)
    def create_preset(self, name: str, data: dict) -> dict:
        validate_name(name, "cluster name")
        if name in self.presets:
            raise SettingError("preset_exists", cluster=name)
        parsed = parse_preset(name, data, "preset %s" % name)
        vf = preset_file(name)
        text = versions.dump(preset_to_dict(parsed))
        with vf.lock():
            vf.write(text)
            v = vf.add(text)
        key = "%s:%s" % (PRESET, name)
        self.presets[name] = parsed
        self.in_use[key] = v
        self._stamps[key] = vf.stamp()
        self.on_preset_changed(name, None, parsed)
        self.on_change()
        return {"name": name, "version": v, "preset": preset_to_dict(parsed)}

    def update_preset(self, name: str, patch: dict, note: str = "") -> dict:
        if name not in self.presets:
            raise SettingError("unknown_cluster", cluster=name)
        return self._apply_user_change("%s:%s" % (PRESET, name), patch, note)

    def delete_preset(self, name: str) -> None:
        if name not in self.presets:
            raise SettingError("unknown_cluster", cluster=name)
        used_by = [p.name for p in self.projects.values() if name in p.clusters]
        if used_by:
            raise SettingError("preset_in_use", cluster=name, projects=", ".join(sorted(used_by)))
        key = "%s:%s" % (PRESET, name)
        for row in self.db.all(
            "SELECT id, approval FROM proposals WHERE doc = ? AND state = 'pending'",
            (key,),
        ):
            self.db.update("proposals", {"id": row["id"]}, state="cancelled",
                           comment="preset deleted", decided=self.now())
            if row["approval"]:
                self.approvals.cancel(row["approval"], "preset %s deleted" % name)
        vf = preset_file(name)
        with vf.lock():
            if vf.file.exists():
                vf.file.unlink()
            for v in vf.versions():
                try:
                    vf.path_of(v).unlink()
                except FileNotFoundError:
                    pass
            shutil.rmtree(vf.history / "rejected", ignore_errors=True)
        shutil.rmtree(vf.history, ignore_errors=True)
        old = self.presets.pop(name)
        self.in_use.pop(key, None)
        self._stamps.pop(key, None)
        self.on_preset_changed(name, old, None)
        self.on_change()

    def create_project(self, name: str, local_dir: str | None = None,
                       data: dict | None = None) -> dict:
        validate_name(name, "project name")
        if name in self.projects:
            raise SettingError("project_exists", project=name)
        if any(name.casefold() == retired.casefold() for retired in self.retired_projects):
            raise SettingError("project_retired", project=name)
        raw = dict(data or {})
        raw["project"] = name
        if local_dir is not None and str(local_dir).strip():
            raw["local_dir"] = str(local_dir).strip()
        raw.setdefault("clusters", {})
        parsed = parse_project(name, raw, "project %s" % name, presets=self.presets)
        vf = project_file(name)
        text = versions.dump(project_to_dict(parsed))
        with vf.lock():
            vf.write(text)
            v = vf.add(text)
        key = "%s:%s" % (PROJECT, name)
        self.projects[name] = parsed
        self.in_use[key] = v
        self._stamps[key] = vf.stamp()
        self.on_project_changed(name, None, parsed)
        self.on_change()
        return {"name": name, "version": v, "project": project_to_dict(parsed)}

    def deactivate_project(self, name: str) -> None:
        """Reflect a committed retirement in the running configuration."""
        self.retired_projects.add(name)
        old = self.projects.pop(name)
        key = "%s:%s" % (PROJECT, name)
        self.in_use.pop(key, None)
        self._stamps.pop(key, None)
        self.on_project_changed(name, old, None)
        self.on_change()

    def update_project(self, name: str, patch: dict, note: str = "") -> dict:
        if name not in self.projects:
            raise SettingError("unknown_project", project=name)
        return self._apply_user_change("%s:%s" % (PROJECT, name), patch, note)

    def _apply_user_change(self, key: str, patch: dict, note: str = "") -> dict:
        kind, name, vf = self._vf_and_kind(key)
        pending_det = self.db.one(
            "SELECT id FROM proposals WHERE doc = ? AND origin = 'detected' AND state = 'pending'",
            (key,),
        )
        if pending_det:
            raise SettingError("undecided_file_edit", key=key, proposal=pending_det["id"])
        with vf.lock():
            cur = vf.latest()
            if cur is None:
                raise SettingError("unknown_setting", key=key)
            old_data = versions.load(cur[1])
            new_data = versions.apply(
                old_data, patch, keep_none=lambda p: self._keep_none(kind, p),
            )
            parsed = self._parse(kind, name, new_data)
            if kind == PROJECT:
                self._check_project_cluster_changes(name, old_data, new_data)
            if new_data == old_data:
                return {"key": key, "version": cur[0], "changed": False}
            new_text = versions.edit(cur[1], new_data)
            vf.write(new_text)
            v = vf.add(new_text)
        self._activate(kind, name, v, parsed, vf.stamp(), proposer_project=None)
        self._refresh_pending_proposals(key, new_data, v)
        return {"key": key, "version": v, "changed": True}

    def restore_version(self, key: str, version: int) -> dict:
        kind, name, vf = self._vf_and_kind(key)
        if kind == PROJECT and name in self.retired_projects:
            raise SettingError("project_retired", project=name)
        if version not in vf.versions():
            raise SettingError("unknown_version", key=key, version=version)
        with vf.lock():
            text = vf.text_of(version)
            data = versions.load(text)
            parsed = self._parse(kind, name, data)
            vf.write(text)
            v = vf.add(text)
        self._activate(kind, name, v, parsed, vf.stamp(), proposer_project=None)
        self._refresh_pending_proposals(key, data, v)
        return {"key": key, "version": v}

    def attached(self, project: str, cluster_name: str, cluster: Cluster,
                 max_jobs_override: int | None, version: int) -> None:
        """Called by Daemon after attach_cluster records a new project config version."""
        p = self.projects[project]
        p.clusters[cluster_name] = cluster
        p.cluster_overrides[cluster_name] = max_jobs_override
        key = "%s:%s" % (PROJECT, project)
        self.in_use[key] = version
        self._stamps[key] = project_file(project).stamp()
        self.on_change()

    # ------------------------------------------------------------------ agent proposals
    def propose_preset(self, project: str, cluster: str, changes: dict, summary: str) -> dict:
        p = self.projects.get(project)
        if p is None:
            raise SettingError("unknown_project", project=project)
        if cluster not in p.clusters or cluster not in self.presets:
            raise SettingError("unknown_cluster", cluster=cluster,
                               clusters=", ".join(sorted(p.clusters)) or "-")
        if not isinstance(changes, dict) or not changes:
            raise SettingError("empty_changes")
        if not summary or not summary.strip():
            raise SettingError("empty_arg", arg="summary")
        key = "%s:%s" % (PRESET, cluster)
        vf = preset_file(cluster)
        with vf.lock():
            cur = vf.latest()
            if cur is None:
                raise SettingError("unknown_cluster", cluster=cluster, clusters="")
            base_v, base_text = cur
            old_data = versions.load(base_text)
            new_data = versions.apply(old_data, changes)
            self._parse(PRESET, cluster, new_data)
            diffs = versions.changes(old_data, new_data, deep=lambda path: False)
            if not diffs:
                raise SettingError("no_effect")
            new_text = versions.edit(base_text, new_data)

        # Supersede any prior pending agent proposal for this cluster from this project
        for old_p in self.db.all(
            "SELECT id, approval FROM proposals WHERE doc = ? AND project = ? "
            "AND origin = 'agent' AND state = 'pending'",
            (key, project),
        ):
            self.db.update("proposals", {"id": old_p["id"]}, state="superseded", decided=self.now())
            if old_p["approval"]:
                self.approvals.cancel(old_p["approval"], "superseded by a newer proposal")

        fields = self._describe_diffs(PRESET, diffs)
        diff_text = "".join(difflib.unified_diff(
            base_text.splitlines(keepends=True),
            new_text.splitlines(keepends=True),
            fromfile="%s v%d" % (cluster, base_v),
            tofile="%s (proposed)" % cluster,
        ))
        sum_line = "Preset %s: %s (%s)" % (
            cluster, summary.strip(),
            ", ".join(f["name"] + (" [SAFETY]" if f["safety"] else "") for f in fields),
        )
        pid = self.db.insert(
            "proposals",
            project=project,
            doc=key,
            origin="agent",
            patch=json.dumps(changes, ensure_ascii=False),
            sha256=versions.sha256(new_text),
            base_version=base_v,
            version=base_v + 1,
            summary=summary.strip(),
            state="pending",
            created=self.now(),
        )
        aid = self.approvals.request(
            project,
            APPROVAL_KIND,
            sum_line,
            {
                "proposal_id": pid,
                "doc": key,
                "kind": PRESET,
                "cluster": cluster,
                "origin": "agent",
                "summary": summary.strip(),
                "fields": fields,
                "diff": diff_text,
                "shared": True,
            },
        )
        self.db.update("proposals", {"id": pid}, approval=aid)
        self.on_change()
        return {"id": pid, "approval": aid, "cluster": cluster, "fields": fields}

    def _describe_diffs(self, kind: str, diffs: list[dict]) -> list[dict]:
        return [
            {
                "name": versions.name(c["field"]),
                "old": _fmt_val(c["old"]),
                "new": _fmt_val(c["new"]),
                "safety": self._safety(kind, c["field"]),
            }
            for c in diffs
        ]

    # ------------------------------------------------------------------ disk edit scan
    def scan(self) -> None:
        """Detect direct edits to preset or project YAML files on disk."""
        for name in list(self.presets):
            self._scan_one("%s:%s" % (PRESET, name))
        for name in list(self.projects):
            self._scan_one("%s:%s" % (PROJECT, name))

    def _scan_one(self, key: str) -> None:
        kind, name, vf = self._vf_and_kind(key)
        st = vf.stamp()
        if st == self._stamps.get(key):
            return
        self._stamps[key] = st
        text = vf.read()
        if text is None:
            return
        with vf.lock():
            cur = vf.latest()
            if cur is None:
                return
            cur_v, cur_text = cur
        if text == cur_text:
            self._cancel_detected(key, "file reverted to approved version")
            return
        old_data = versions.load(cur_text)
        err = None
        new_data = None
        try:
            new_data = versions.load(text)
            self._parse(kind, name, new_data)
        except (ValueError, ConfigError) as e:
            err = str(e)

        if err is None and new_data == old_data:
            # Formatting or comment-only change: record silently as next version
            with vf.lock():
                v = vf.add(text)
            self.in_use[key] = v
            self._stamps[key] = vf.stamp()
            self._cancel_detected(key, "formatting-only change adopted")
            return

        sha = versions.sha256(text)
        existing = self.db.one(
            "SELECT id, sha256, approval FROM proposals WHERE doc = ? AND origin = 'detected' "
            "AND state = 'pending'",
            (key,),
        )
        if existing and existing["sha256"] == sha:
            return
        self._cancel_detected(key, "file edited again on disk")
        fields = (self._describe_diffs(kind, versions.changes(
            old_data, new_data, deep=lambda p: self._deep(kind, p)))
                  if err is None and new_data is not None else [])
        diff_text = "".join(difflib.unified_diff(
            cur_text.splitlines(keepends=True),
            text.splitlines(keepends=True),
            fromfile="%s v%d" % (name, cur_v),
            tofile="%s (on disk)" % name,
        ))
        sum_line = ("Disk edit to %s %s: %s"
                    % ("preset" if kind == PRESET else "project", name,
                       "INVALID (" + err + ")" if err else
                       ", ".join(f["name"] + (" [SAFETY]" if f["safety"] else "") for f in fields)))
        proj = name if kind == PROJECT else None
        pid = self.db.insert(
            "proposals",
            project=proj,
            doc=key,
            origin="detected",
            patch=None,
            sha256=sha,
            base_version=cur_v,
            version=cur_v + 1,
            summary=sum_line,
            state="pending",
            created=self.now(),
        )
        aid = self.approvals.request(
            proj,
            APPROVAL_KIND,
            sum_line,
            {
                "proposal_id": pid,
                "doc": key,
                "kind": kind,
                "cluster": name if kind == PRESET else None,
                "origin": "detected",
                "summary": sum_line,
                "fields": fields,
                "diff": diff_text,
                "error": err,
                "shared": kind == PRESET,
            },
        )
        self.db.update("proposals", {"id": pid}, approval=aid)

    def _cancel_detected(self, key: str, reason: str) -> None:
        for row in self.db.all(
            "SELECT id, approval FROM proposals WHERE doc = ? AND origin = 'detected' "
            "AND state = 'pending'",
            (key,),
        ):
            self.db.update("proposals", {"id": row["id"]}, state="cancelled",
                           comment=reason, decided=self.now())
            if row["approval"]:
                self.approvals.cancel(row["approval"], reason)

    # ------------------------------------------------------------------ approval decision
    def on_decision(self, approval: dict, approved: bool, comment: str, options: dict) -> dict:
        pid = approval["detail"]["proposal_id"]
        prop = self.db.one("SELECT * FROM proposals WHERE id = ?", (pid,))
        if prop is None or prop["state"] != "pending":
            st = prop["state"] if prop else "missing"
            self.approvals.cancel(approval["id"], "proposal already %s" % st)
            raise ApprovalError("already_decided", id=approval["id"], state=st)
        key = prop["doc"]
        kind, name, vf = self._vf_and_kind(key)
        target_exists = (name in self.presets) if kind == PRESET else (name in self.projects)
        if not target_exists:
            self.db.update("proposals", {"id": pid}, state="cancelled",
                           comment="setting no longer exists", decided=self.now())
            self.approvals.cancel(approval["id"], "setting %s no longer exists" % key)
            raise ApprovalError("missing_setting", id=approval["id"], msg=key)

        if not approved:
            if prop["origin"] == "detected":
                with vf.lock():
                    cur = vf.latest()
                    disk = vf.read()
                    if disk is not None:
                        vf.keep_rejected(disk, "p%04d" % pid)
                    if cur is not None:
                        vf.write(cur[1])
                self._stamps[key] = vf.stamp()
            self.db.update("proposals", {"id": pid}, state="rejected", comment=comment,
                           decided=self.now())
            if prop["project"]:
                self.notify(prop["project"], "proposal_rejected", "proposal:%d" % pid,
                            {"proposal": pid, "doc": key, "comment": comment})
            self.on_change()
            return {"proposal": pid, "state": "rejected"}

        # Approve
        if prop["origin"] == "detected":
            with vf.lock():
                disk = vf.read()
                if disk is None or versions.sha256(disk) != prop["sha256"]:
                    raise ApprovalError("stale", id=approval["id"],
                                        msg="file on disk changed since this approval was created")
                try:
                    data = versions.load(disk)
                    parsed = self._parse(kind, name, data)
                except (ValueError, ConfigError) as e:
                    raise ApprovalError("invalid_setting", id=approval["id"], msg=str(e))
                v = vf.add(disk)
            self.db.update("proposals", {"id": pid}, state="approved", version=v,
                           comment=comment, decided=self.now())
            self._activate(kind, name, v, parsed, vf.stamp(), proposer_project=prop["project"])
            self._refresh_pending_proposals(key, data, v)
            return {"proposal": pid, "version": v}

        # Agent patch proposal: rebase onto latest version if touched fields didn't move
        patch = json.loads(prop["patch"])
        with vf.lock():
            cur = vf.latest()
            if cur is None:
                self.db.update("proposals", {"id": pid}, state="cancelled",
                               comment="setting file missing", decided=self.now())
                self.approvals.cancel(approval["id"], "setting file missing: %s" % key)
                raise ApprovalError("missing_setting", id=approval["id"], msg=key)
            cur_v, cur_text = cur
            base_text = vf.text_of(prop["base_version"]) if prop["base_version"] else cur_text
            base_data = versions.load(base_text)
            cur_data = versions.load(cur_text)
            touched = versions.leaves(patch, deep=lambda p: self._deep(kind, p))
            moved = [versions.name(f) for f in touched
                     if versions.get(base_data, f) != versions.get(cur_data, f)]
            if moved:
                reason = "fields changed meanwhile: " + ", ".join(moved)
                self.db.update("proposals", {"id": pid}, state="stale",
                               comment=reason, decided=self.now())
                self.approvals.cancel(approval["id"], "stale: " + reason)
                if prop["project"]:
                    self.notify(prop["project"], "proposal_stale", "proposal:%d" % pid,
                                {"proposal": pid, "doc": key, "moved": moved})
                raise ApprovalError("stale", id=approval["id"],
                                    msg="fields changed since proposal: " + ", ".join(moved))
            new_data = versions.apply(
                cur_data, patch, keep_none=lambda p: self._keep_none(kind, p),
            )
            try:
                parsed = self._parse(kind, name, new_data)
            except ConfigError as e:
                raise ApprovalError("invalid_setting", id=approval["id"], msg=str(e))
            new_text = versions.edit(cur_text, new_data)
            vf.write(new_text)
            v = vf.add(new_text)
        self.db.update("proposals", {"id": pid}, state="approved", version=v,
                       comment=comment, decided=self.now())
        self._activate(kind, name, v, parsed, vf.stamp(), proposer_project=prop["project"])
        self._refresh_pending_proposals(key, new_data, v)
        return {"proposal": pid, "version": v}

    def _activate(self, kind: str, name: str, version: int, parsed, stamp,
                  proposer_project: str | None) -> None:
        key = "%s:%s" % (kind, name)
        self.in_use[key] = version
        self._stamps[key] = stamp
        if kind == PRESET:
            old = self.presets.get(name)
            self.presets[name] = parsed
            # Propagate updated preset to every project using this cluster
            for pname, proj in self.projects.items():
                if name in proj.clusters:
                    prev_cl = proj.clusters[name]
                    proj.clusters[name] = Cluster.from_preset(
                        parsed, prev_cl.root, max_jobs=proj.cluster_overrides.get(name)
                    )
                    self.notify(pname, "preset_updated", "cluster:" + name,
                                {"cluster": name, "version": version})
            self.on_preset_changed(name, old, parsed)
        else:
            old = self.projects.get(name)
            prev_snapshot = (
                Project(
                    name=old.name,
                    local_dir=old.local_dir,
                    clusters=dict(old.clusters),
                    cluster_overrides=dict(old.cluster_overrides),
                    wake=old.wake,
                    submit=old.submit,
                    gateway=old.gateway,
                )
                if old is not None else None
            )
            if old is not None:
                old.local_dir = parsed.local_dir
                old.clusters = parsed.clusters
                old.cluster_overrides = parsed.cluster_overrides
                old.wake = parsed.wake
                old.submit = parsed.submit
                old.gateway = parsed.gateway
            else:
                self.projects[name] = parsed
            self.on_project_changed(name, prev_snapshot, parsed)
        self.on_change()

    def _refresh_pending_proposals(self, key: str, cur_data: dict, cur_v: int) -> None:
        kind, name, vf = self._vf_and_kind(key)
        for row in self.db.all(
            "SELECT * FROM proposals WHERE doc = ? AND origin = 'agent' AND state = 'pending'",
            (key,),
        ):
            patch = json.loads(row["patch"])
            base_data = versions.load(vf.text_of(row["base_version"])) if row["base_version"] else {}
            touched = versions.leaves(patch, deep=lambda p: self._deep(kind, p))
            moved = [versions.name(f) for f in touched
                     if versions.get(base_data, f) != versions.get(cur_data, f)]
            if moved:
                self.db.update("proposals", {"id": row["id"]}, state="stale",
                               comment="fields changed: " + ", ".join(moved), decided=self.now())
                if row["approval"]:
                    self.approvals.cancel(row["approval"],
                                          "stale: fields changed (" + ", ".join(moved) + ")")
                if row["project"]:
                    self.notify(row["project"], "proposal_stale", "proposal:%d" % row["id"],
                                {"proposal": row["id"], "doc": key, "moved": moved})

    # ------------------------------------------------------------------ inspection for Web UI
    def preset_status(self, name: str) -> dict:
        if name not in self.presets:
            raise SettingError("unknown_cluster", cluster=name)
        key = "%s:%s" % (PRESET, name)
        vf = preset_file(name)
        cur = vf.latest()
        return {
            "name": name,
            "key": key,
            "version": self.in_use.get(key),
            "versions": vf.versions(),
            "data": preset_to_dict(self.presets[name]),
            "yaml": cur[1] if cur else "",
            "safety_fields": list(PRESET_SAFETY),
            "attached_projects": sorted(p.name for p in self.projects.values()
                                        if name in p.clusters),
        }

    def project_status(self, name: str) -> dict:
        if name not in self.projects:
            raise SettingError("unknown_project", project=name)
        key = "%s:%s" % (PROJECT, name)
        vf = project_file(name)
        cur = vf.latest()
        return {
            "name": name,
            "key": key,
            "version": self.in_use.get(key),
            "versions": vf.versions(),
            "data": project_to_dict(self.projects[name]),
            "yaml": cur[1] if cur else "",
            "safety_fields": list(PROJECT_SAFETY),
        }
