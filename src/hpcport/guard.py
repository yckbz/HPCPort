"""Pre-execution lexical/syntax safety scanner for gateway shell commands.

Checks command text before anything runs on the remote server. On Slurm clusters
(scheduler="slurm"), direct sbatch/srun/salloc are refused (submit must be used)
and scancel requires approval; on bare SSH servers (scheduler="none"), those two
Slurm-specific rules are skipped.
"""

from __future__ import annotations

import os
import posixpath
import re
from dataclasses import dataclass, field

ASK, REFUSE = "ask", "refuse"
RULES = {
    "scheduler": REFUSE,
    "scancel": ASK,
    "outside": ASK,
    "root": ASK,
    "startup": ASK,
    "crontab": ASK,
    "remote": ASK,
    "killall": ASK,
}
ALLOWABLE = tuple(r for r, a in RULES.items() if a == ASK)

SCHEDULER = {"sbatch", "srun", "salloc"}
DESTRUCTIVE = {"rm", "rmdir", "mv", "shred", "truncate", "unlink"}
REMOTE = {"ssh", "scp", "sftp"}
KILL_BY_NAME = {"pkill", "killall"}
SHELLS = {"bash", "sh", "zsh", "dash", "ksh"}
STARTUP_FILES = {
    ".bashrc", ".bash_profile", ".bash_login", ".bash_logout", ".profile",
    ".zshrc", ".zprofile", ".zshenv", ".zlogin", ".cshrc", ".tcshrc", ".login",
}
SHARED = ("/tmp", "/var/tmp", "/dev/shm")
RESERVED = {"if", "then", "else", "elif", "fi", "do", "done", "while", "until", "!", "{", "}",
            "time", "esac"}
WRAPPERS = {
    "nohup": (), "nice": ("-n",), "command": (), "exec": ("-a",), "builtin": (),
    "stdbuf": ("-i", "-o", "-e"), "sudo": ("-u", "-g"), "env": ("-u", "-C", "-S"),
    "timeout": ("-k", "-s"), "ionice": ("-c", "-n"),
    "xargs": ("-n", "-I", "-P", "-L", "-d", "-E", "-s", "-a"),
}
ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\+?=")
HOSTSPEC = re.compile(r"^(rsync://|[^/\s:]+:)")
MAX_DEPTH = 6


@dataclass
class Word:
    text: str = ""
    plain: bool = True
    tilde: bool = False
    quoted: bool = False


@dataclass
class Command:
    words: list[Word] = field(default_factory=list)
    redirects: list[tuple[str, Word]] = field(default_factory=list)


@dataclass(frozen=True)
class Finding:
    rule: str
    what: str

    @property
    def action(self) -> str:
        return RULES[self.rule]


@dataclass(frozen=True)
class Place:
    roots: tuple[str, ...]
    cwd: str
    home: str | None = None
    extra: tuple[str, ...] = ()


def _closing(src: str, i: int) -> int:
    depth, n = 1, len(src)
    while i < n:
        c = src[i]
        if c == "\\":
            i += 2
            continue
        if c == "'":
            j = src.find("'", i + 1)
            i = n if j < 0 else j + 1
            continue
        if c == '"':
            i += 1
            while i < n and src[i] != '"':
                i += 2 if src[i] == "\\" else 1
            i += 1
            continue
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return n


def parse(src: str, depth: int = 0) -> list[Command]:
    out: list[Command] = []
    nested: list[Command] = []
    cur = Command()
    word: Word | None = None
    pending: str | None = None
    heredocs: list[tuple[str, bool]] = []
    i, n = 0, len(src)

    def w() -> Word:
        nonlocal word
        if word is None:
            word = Word()
        return word

    def end_word():
        nonlocal word, pending
        if word is None:
            return
        if pending is not None:
            if pending.startswith("<<") and pending != "<<<":
                heredocs.append((word.text, pending == "<<-"))
            else:
                cur.redirects.append((pending, word))
            pending = None
        else:
            cur.words.append(word)
        word = None

    def end_command():
        nonlocal cur, pending
        end_word()
        pending = None
        if cur.words or cur.redirects:
            out.append(cur)
        cur = Command()

    def substitution(inner: str):
        if depth < MAX_DEPTH:
            nested.extend(parse(inner, depth + 1))
        w().plain = False

    while i < n:
        c = src[i]
        if c == "\\":
            if i + 1 < n and src[i + 1] == "\n":
                i += 2
                continue
            w().text += src[i + 1:i + 2]
            w().quoted = True
            i += 2
        elif c == "'":
            j = src.find("'", i + 1)
            j = n if j < 0 else j
            w().text += src[i + 1:j]
            w().quoted = True
            i = j + 1
        elif c == '"':
            w().quoted = True
            i += 1
            while i < n and src[i] != '"':
                if src[i] == "\\" and i + 1 < n and src[i + 1] in '$`"\\\n':
                    w().text += src[i + 1] if src[i + 1] != "\n" else ""
                    i += 2
                elif src[i] == "$" and src[i + 1:i + 2] == "(":
                    j = _closing(src, i + 2)
                    substitution(src[i + 2:j - 1])
                    w().text += src[i:j]
                    i = j
                elif src[i] == "`":
                    j = src.find("`", i + 1)
                    j = n if j < 0 else j
                    substitution(src[i + 1:j])
                    w().text += src[i:j + 1]
                    i = j + 1
                else:
                    if src[i] == "$":
                        w().plain = False
                    w().text += src[i]
                    i += 1
            i += 1
        elif c == "$" and src[i + 1:i + 2] == "'":
            j = i + 2
            while j < n and src[j] != "'":
                j += 2 if src[j] == "\\" else 1
            w().text += src[i + 2:j]
            w().quoted = True
            i = j + 1
        elif c == "$" and src[i + 1:i + 2] == "(":
            j = _closing(src, i + 2)
            if src[i + 2:i + 3] != "(":
                substitution(src[i + 2:j - 1])
            w().plain = False
            w().text += src[i:j]
            i = j
        elif c == "$" and src[i + 1:i + 2] == "{":
            j = src.find("}", i)
            j = n if j < 0 else j + 1
            w().plain = False
            w().text += src[i:j]
            i = j
        elif c == "$":
            w().plain = False
            w().text += c
            i += 1
        elif c == "`":
            j = src.find("`", i + 1)
            j = n if j < 0 else j
            substitution(src[i + 1:j])
            w().text += src[i:j + 1]
            i = j + 1
        elif c in "<>" and src[i + 1:i + 2] == "(":
            j = _closing(src, i + 2)
            substitution(src[i + 2:j - 1])
            w().text += src[i:j]
            i = j
        elif c in " \t":
            end_word()
            i += 1
        elif c == "#" and word is None:
            j = src.find("\n", i)
            i = n if j < 0 else j
        elif c == "\n":
            end_command()
            i += 1
            for delim, strip in heredocs:
                while i < n:
                    j = src.find("\n", i)
                    j = n if j < 0 else j
                    line = src[i:j]
                    i = min(n, j + 1)
                    if (line.lstrip("\t") if strip else line) == delim:
                        break
            heredocs.clear()
        elif c in "<>":
            if word is not None and word.text.isdigit() and not word.quoted:
                word = None
            end_word()
            for op in ("<<<", "<<-", "&>>", "<<", ">>", "<&", ">&", ">|", "<>", "<", ">"):
                if src.startswith(op, i):
                    break
            i += len(op)
            if op in ("<&", ">&"):
                while i < n and (src[i].isdigit() or src[i] == "-"):
                    i += 1
                if i < n and src[i] not in " \t\n;&|()<>":
                    pending = ">" if op == ">&" else "<"
            else:
                pending = op
        elif c == "&" and src[i + 1:i + 2] == ">":
            end_word()
            i += 3 if src[i + 2:i + 3] == ">" else 2
            pending = ">"
        elif c in ";&|()":
            end_command()
            i += 2 if src[i:i + 2] in ("&&", "||", ";;", "|&") else 1
        else:
            if c == "~" and word is None:
                w().tilde = True
            w().text += c
            i += 1
    end_command()
    return out + nested


def _name(word: Word) -> str:
    return posixpath.basename(word.text) if word.plain else ""


def _strip(words: list[Word]) -> list[Word]:
    words = list(words)
    while words:
        first = words[0]
        if not first.quoted and (first.text in RESERVED or ASSIGNMENT.match(first.text)):
            words.pop(0)
            continue
        name = _name(first)
        if name not in WRAPPERS:
            break
        valued = WRAPPERS[name]
        words.pop(0)
        while words and (words[0].text.startswith("-") or
                         (name == "env" and ASSIGNMENT.match(words[0].text))):
            opt = words.pop(0).text
            if opt in valued and words:
                words.pop(0)
        if name == "timeout" and words:
            words.pop(0)
    return words


def _operands(words: list[Word]) -> list[Word]:
    out, rest = [], False
    for a in words[1:]:
        if rest or not a.text.startswith("-") or a.text == "-":
            out.append(a)
        elif a.text == "--":
            rest = True
    return out


def commands(src: str, depth: int = 0) -> list[Command]:
    out = []
    for cmd in parse(src):
        cmd.words = _strip(cmd.words)
        out.append(cmd)
        if not cmd.words or depth >= MAX_DEPTH:
            continue
        name, args = _name(cmd.words[0]), cmd.words[1:]
        if name in SHELLS:
            for k, a in enumerate(args):
                if re.match(r"^-[a-z]*c$", a.text) and k + 1 < len(args):
                    out += commands(args[k + 1].text, depth + 1)
                    break
        elif name == "eval":
            out += commands(" ".join(a.text for a in args), depth + 1)
    return out


_UNSET = object()
_DIR_INDEX_RE = re.compile(r"^[+-]\d+$")


def _inside(base: str, path: str) -> bool:
    base = base.rstrip("/") or "/"
    return path == base or path.startswith(base + "/" if base != "/" else "/")


def _resolve_dir(word: Word, cur_cwd: str | None, place: Place) -> str | None:
    text = word.text
    if not text:
        return cur_cwd
    if word.tilde:
        if text == "~" or text.startswith("~/"):
            if place.home is None:
                return None
            text = place.home + text[1:]
        elif text == "~+" or text.startswith("~+/"):
            if cur_cwd is None:
                return None
            text = cur_cwd + text[2:]
        else:
            return None
    else:
        for var in ("$HOME", "${HOME}"):
            if text == var or text.startswith(var + "/"):
                if place.home is None:
                    return None
                text = place.home + text[len(var):]
                break
    if "$" in text or "`" in text or any(ch in text for ch in "*?["):
        return None
    if text.startswith("/"):
        return posixpath.normpath(text)
    if cur_cwd is None:
        return None
    return posixpath.normpath(posixpath.join(cur_cwd, text))


def locate(word: Word, place: Place, moved: bool = False, cwd=_UNSET) -> tuple[str, str]:
    cur_cwd = None if moved else (place.cwd if cwd is _UNSET else cwd)
    text = word.text
    if word.tilde:
        if text == "~" or text.startswith("~/"):
            if place.home is None:
                return "outside", text
            text = place.home + text[1:]
        elif text == "~+" or text.startswith("~+/"):
            if cur_cwd is None:
                return "outside", word.text
            text = cur_cwd + text[2:]
        else:
            # ~otheruser/... or ~- cannot lie inside the project root
            return "outside", text
    else:
        for var in ("$HOME", "${HOME}"):
            if text == var or text.startswith(var + "/"):
                if place.home is None:
                    return "outside", text
                text = place.home + text[len(var):]
                break
    fixed = text
    plain = "$" not in text and "`" not in text
    if not plain:
        cut = min(k for k in (text.find("$"), text.find("`")) if k >= 0)
        fixed = text[:cut].rsplit("/", 1)[0] if "/" in text[:cut] else ""
        if not fixed:
            return "unknown", word.text
    parts = fixed.split("/")
    whole = plain
    for k, p in enumerate(parts):
        if any(ch in p for ch in "*?["):
            fixed = "/".join(parts[:k]) or ("/" if fixed.startswith("/") else ".")
            whole = False
            break
    if not fixed.startswith("/"):
        if cur_cwd is None:
            return "outside", word.text
        fixed = posixpath.join(cur_cwd, fixed)
    path = posixpath.normpath(fixed)
    if whole and any(path == r.rstrip("/") for r in place.roots):
        return "root", path
    if any(_inside(b, path) for b in place.roots + SHARED + place.extra) or path == "/dev/null":
        return "inside", path
    return ("outside" if plain else "unknown"), path


def _startup(path: str) -> bool:
    parts = path.split("/")
    return parts[-1] in STARTUP_FILES or ".ssh" in parts


def check(command: str, place: Place, allow=(), scheduler: str = "slurm") -> list[Finding]:
    """Return all rule findings for a command; empty list means allowed to run."""
    found: list[Finding] = []
    use_slurm = scheduler == "slurm"

    def add(rule: str, what: str):
        f = Finding(rule, what)
        if (RULES[rule] == REFUSE or rule not in allow) and f not in found:
            found.append(f)

    def scan_src(src: str, init_cwd: str | None, init_prev: str | None,
                 init_stack: list[str | None], depth: int = 0) -> None:
        cur_cwd: str | None = init_cwd
        prev_cwd: str | None = init_prev
        dir_stack: list[str | None] = list(init_stack)

        def paths(words, startup_only=False):
            for a in words:
                where, path = locate(a, place, cwd=cur_cwd)
                if where == "outside" and _startup(path):
                    add("startup", a.text)
                elif startup_only:
                    continue
                elif where == "outside":
                    add("outside", a.text)
                elif where == "root":
                    add("root", a.text)

        for cmd in parse(src):
            cmd.words = _strip(cmd.words)
            for op, target in cmd.redirects:
                if op.startswith(">") or op == "<>":
                    paths([target], startup_only=True)
            if not cmd.words:
                continue
            name, args = _name(cmd.words[0]), cmd.words[1:]
            ops = _operands(cmd.words)
            texts = [a.text for a in args]
            if name == "cd":
                if not ops:
                    new_cwd = place.home
                elif ops[0].text == "-":
                    new_cwd = prev_cwd
                else:
                    new_cwd = _resolve_dir(ops[0], cur_cwd, place)
                prev_cwd, cur_cwd = cur_cwd, new_cwd
            elif name == "pushd":
                if not ops:
                    if dir_stack:
                        top = dir_stack.pop()
                        dir_stack.append(cur_cwd)
                        prev_cwd, cur_cwd = cur_cwd, top
                    else:
                        prev_cwd, cur_cwd = cur_cwd, None
                elif _DIR_INDEX_RE.match(ops[0].text):
                    dir_stack.append(cur_cwd)
                    prev_cwd, cur_cwd = cur_cwd, None
                else:
                    new_cwd = _resolve_dir(ops[0], cur_cwd, place)
                    dir_stack.append(cur_cwd)
                    prev_cwd, cur_cwd = cur_cwd, new_cwd
            elif name == "popd":
                if ops and _DIR_INDEX_RE.match(ops[0].text):
                    cur_cwd = None
                elif dir_stack:
                    prev_cwd, cur_cwd = cur_cwd, dir_stack.pop()
                else:
                    prev_cwd, cur_cwd = cur_cwd, None
            elif use_slurm and name in SCHEDULER:
                add("scheduler", name)
            elif use_slurm and name == "scancel":
                add("scancel", name)
            elif name in DESTRUCTIVE:
                paths(ops)
            elif name == "find":
                k = next((k for k, t in enumerate(texts) if t.startswith("-") or t in ("(", "!")),
                         len(args))
                acts = [posixpath.basename(texts[j + 1]) for j, t in enumerate(texts[:-1])
                        if t in ("-exec", "-execdir", "-ok")]
                if "-delete" in texts or any(a in DESTRUCTIVE for a in acts):
                    paths(args[:k] or [Word(".")])
            elif name == "crontab":
                if not texts or any(t != "-l" for t in texts):
                    add("crontab", name)
            elif name in REMOTE:
                add("remote", name)
            elif name == "rsync":
                if any(HOSTSPEC.match(a.text) for a in ops):
                    add("remote", name)
            elif name in KILL_BY_NAME:
                add("killall", name)
            elif name == "tee":
                paths(ops, startup_only=True)
            elif name == "sed":
                if any(re.match(r"^(-[a-zA-Z]*i|--in-place)", t) for t in texts):
                    paths(ops, startup_only=True)
            elif name in ("cp", "ln", "install", "chmod", "chown", "touch"):
                paths(ops[-1:] if name in ("cp", "ln", "install") else ops, startup_only=True)

            if depth < MAX_DEPTH:
                if name in SHELLS:
                    for k, a in enumerate(args):
                        if re.match(r"^-[a-z]*c$", a.text) and k + 1 < len(args):
                            scan_src(args[k + 1].text, cur_cwd, prev_cwd, dir_stack, depth + 1)
                            break
                elif name == "eval":
                    scan_src(" ".join(a.text for a in args), cur_cwd, prev_cwd, dir_stack, depth + 1)

    scan_src(command, place.cwd, None, [], 0)
    return found


def place_for(root: str, real_root: str | None, cwd: str, home: str | None, extra=()) -> Place:
    roots = tuple(dict.fromkeys(r.rstrip("/") or "/" for r in (root, real_root) if r))
    return Place(roots=roots, cwd=cwd, home=(home or "").rstrip("/") or None,
                 extra=tuple(os.path.normpath(e) for e in extra))
