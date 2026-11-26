"""Versioned setting files, field patches, and comment-preserving YAML edits.

A setting is a YAML mapping kept in one file that people may edit directly or
through the Web UI. Its approved versions are stored as numbered byte copies:

  <history>/0001.yaml, 0002.yaml, ...   version N is file N
  <history>/.lock                       taken while a version is added or written
  <history>/rejected/                   file edits that were rejected by the user

HPCPort always runs on the latest approved version, never on unreviewed disk edits.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import os
import re
import threading
from pathlib import Path

import yaml

MISSING = object()
PATTERN = re.compile(r"^(\d{4,})\.yaml$")
_LOCKS: dict[str, list] = {}
_LOCKS_GUARD = threading.Lock()


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def load(text: str) -> dict:
    """Parse YAML text as a mapping; raise ValueError otherwise."""
    try:
        data = yaml.safe_load(text) if text.strip() else {}
    except yaml.YAMLError as e:
        raise ValueError("invalid YAML: %s" % str(e).replace("\n", " "))
    if data is None:
        data = {}
    if not isinstance(data, dict):
        raise ValueError("the content must be a mapping")
    return data


class VersionedFile:
    """One setting file and its numbered history of approved versions."""

    def __init__(self, file: Path, history: Path):
        self.file = Path(file)
        self.history = Path(history)

    def read(self) -> str | None:
        try:
            return self.file.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None

    def stamp(self):
        try:
            st = self.file.stat()
            return st.st_mtime_ns, st.st_size, st.st_ino
        except FileNotFoundError:
            return None

    def write(self, text: str) -> None:
        """Atomically replace the file."""
        self.file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.file.with_name(".%s.%d.tmp" % (self.file.name, os.getpid()))
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, self.file)

    def versions(self) -> list[int]:
        try:
            names = os.listdir(self.history)
        except FileNotFoundError:
            return []
        return sorted(int(m.group(1)) for m in map(PATTERN.match, names) if m)

    def path_of(self, version: int) -> Path:
        return self.history / ("%04d.yaml" % version)

    def text_of(self, version: int) -> str:
        return self.path_of(version).read_text(encoding="utf-8")

    def latest(self) -> tuple[int, str] | None:
        v = self.versions()
        return (v[-1], self.text_of(v[-1])) if v else None

    @contextlib.contextmanager
    def lock(self):
        """Reentrant file lock across both threads and processes."""
        self.history.mkdir(parents=True, exist_ok=True)
        key = str(self.history.resolve())
        with _LOCKS_GUARD:
            entry = _LOCKS.setdefault(key, [threading.RLock(), 0, None])
        with entry[0]:
            if entry[1] == 0:
                entry[2] = open(self.history / ".lock", "a")
                fcntl.flock(entry[2], fcntl.LOCK_EX)
            entry[1] += 1
            try:
                yield
            finally:
                entry[1] -= 1
                if entry[1] == 0:
                    fcntl.flock(entry[2], fcntl.LOCK_UN)
                    entry[2].close()
                    entry[2] = None

    def add(self, text: str) -> int:
        """Store text as the next numbered version (must be called under lock())."""
        cur = self.latest()
        if cur and cur[1] == text:
            return cur[0]
        v = (cur[0] if cur else 0) + 1
        while True:
            try:
                fd = os.open(self.path_of(v), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            except FileExistsError:
                v += 1
                continue
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(text)
            return v

    def keep_rejected(self, text: str, tag: str) -> Path:
        d = self.history / "rejected"
        d.mkdir(parents=True, exist_ok=True)
        p = d / ("%s.yaml" % tag)
        p.write_text(text, encoding="utf-8")
        return p


def get(data, path: tuple):
    for k in path:
        if not isinstance(data, dict) or k not in data:
            return MISSING
        data = data[k]
    return data


def apply(data: dict, patch: dict) -> dict:
    """Return a copy of data with patch applied (None removes an entry)."""
    out = dict(data)
    for k, v in patch.items():
        if v is None:
            out.pop(k, None)
        elif isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = apply(out[k], v)
        else:
            out[k] = v
    return out


def leaves(patch: dict, deep=lambda path: True, prefix: tuple = ()) -> list[tuple]:
    out = []
    for k, v in patch.items():
        p = prefix + (k,)
        if isinstance(v, dict) and v and deep(p):
            out += leaves(v, deep, p)
        else:
            out.append(p)
    return out


def changes(old: dict, new: dict, deep=lambda path: True, prefix: tuple = ()) -> list[dict]:
    out = []
    for k in list(old) + [k for k in new if k not in old]:
        p = prefix + (k,)
        a, b = old.get(k, MISSING), new.get(k, MISSING)
        if a == b:
            continue
        if isinstance(a, dict) and isinstance(b, dict) and deep(p):
            out += changes(a, b, deep, p)
        else:
            out.append({"field": p, "old": a, "new": b})
    return out


def as_patch(old: dict, new: dict, deep=lambda path: True) -> dict:
    patch: dict = {}
    for c in changes(old, new, deep):
        d = patch
        for k in c["field"][:-1]:
            d = d.setdefault(k, {})
        d[c["field"][-1]] = None if c["new"] is MISSING else c["new"]
    return patch


def name(path: tuple) -> str:
    return ".".join(str(k) for k in path)


class _Dumper(yaml.SafeDumper):
    pass


def _str(dumper, value):
    style = "|" if "\n" in value else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", value, style=style)


_Dumper.add_representer(str, _str)


def dump(data: dict) -> str:
    return yaml.dump(data, Dumper=_Dumper, sort_keys=False, allow_unicode=True,
                     default_flow_style=False) if data else ""


_TOP = re.compile(r"^([^\s#][^:]*?)\s*:(\s|$)")


def _blocks(lines: list[str]) -> dict[str, tuple[int, int]]:
    starts = []
    for i, line in enumerate(lines):
        m = _TOP.match(line)
        if m and not line.startswith(("---", "...")):
            starts.append((m.group(1).strip().strip("'\""), i))
    out = {}
    for n, (key, i) in enumerate(starts):
        j = starts[n + 1][1] if n + 1 < len(starts) else len(lines)
        while j > i + 1 and (not lines[j - 1].strip() or lines[j - 1].startswith("#")):
            j -= 1
        out[key] = (i, j)
    return out


def edit(text: str, new: dict) -> str:
    """Update top-level YAML blocks that changed while preserving comments elsewhere."""
    try:
        old = load(text)
    except ValueError:
        return dump(new)
    lines = text.splitlines(keepends=True)
    if lines and not lines[-1].endswith("\n"):
        lines[-1] += "\n"
    blocks = _blocks(lines)
    if set(blocks) != set(map(str, old)):
        return dump(new)
    for key in sorted((k for k in old if old[k] != new.get(k, MISSING)),
                      key=lambda k: -blocks[str(k)][0]):
        i, j = blocks[str(key)]
        lines[i:j] = [] if key not in new else [dump({key: new[key]})]
    out = "".join(lines)
    added = {k: v for k, v in new.items() if k not in old}
    if added:
        out += ("" if not out or out.endswith("\n") else "\n") + dump(added)
    try:
        if load(out) == new:
            return out
    except ValueError:
        pass
    return dump(new)
