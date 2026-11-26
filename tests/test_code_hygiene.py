"""Code hygiene checks: formal source and test files stay strictly English."""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
CJK = re.compile(r"[\u4e00-\u9fff]")


def test_no_chinese_in_code():
    bad = []
    for folder in ("src", "tests"):
        root = REPO / folder
        if not root.exists():
            continue
        for p in sorted(root.rglob("*")):
            if not p.is_file() or "__pycache__" in p.parts:
                continue
            # web/i18n.js intentionally holds the Chinese translation dictionary for the Web UI
            if p.relative_to(REPO).as_posix() == "src/hpcport/web/i18n.js":
                continue
            if p.suffix not in (".py", ".js", ".css", ".html"):
                continue
            text = p.read_text(encoding="utf-8", errors="ignore")
            for lineno, line in enumerate(text.splitlines(), 1):
                if CJK.search(line):
                    bad.append("%s:%d: %s" % (p.relative_to(REPO), lineno, line.strip()[:80]))
    assert not bad, "Chinese characters found in formal code:\n" + "\n".join(bad)
