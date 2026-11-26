#!/usr/bin/env python3
"""Render a readable review copy of src/hpcport/prompts.py to docs/prompts/prompts.md."""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from hpcport import messages, prompts as P  # noqa: E402
from hpcport.config import Cluster, Project  # noqa: E402

SOURCE = REPO / "src" / "hpcport" / "prompts.py"
TARGET = REPO / "docs" / "prompts" / "prompts.md"
ASSIGN = re.compile(r"^([A-Z][A-Z0-9_]*)\s*=")


def cell(text: str) -> str:
    return str(text).replace("|", "\\|").replace("\n", "<br>")


def render() -> str:
    out = [
        "# HPCPort Agent-Visible Texts (Review Copy)",
        "",
        "> Generated from `src/hpcport/prompts.py` by `tools/render_prompts.py`.",
        "",
    ]
    table = []

    def flush():
        if table:
            out.extend(["| Name | Text |", "|---|---|"] + table + [""])
            table.clear()

    for line in SOURCE.read_text(encoding="utf-8").splitlines():
        if line.startswith("# === "):
            flush()
            out.extend(["## " + line[6:].strip(), ""])
            continue
        m = ASSIGN.match(line)
        if not m:
            continue
        name = m.group(1)
        val = getattr(P, name)
        if isinstance(val, str) and "\n" not in val.strip("\n"):
            table.append("| `%s` | %s |" % (name, cell(val.strip("\n"))))
        else:
            flush()
            out.extend(["**`%s`**" % name, ""])
            if isinstance(val, str):
                out.extend(["```text", val.rstrip("\n"), "```", ""])
            elif isinstance(val, dict):
                out.extend(
                    ["| Key | Text |", "|---|---|"]
                    + ["| `%s` | %s |" % (k, cell(v)) for k, v in val.items()]
                    + [""]
                )
    flush()
    sample = Project(
        name="demo",
        local_dir=Path("/home/user/demo"),
        clusters={
            "gpu01": Cluster(
                name="gpu01", root="/work/user/demo", host="gpu-login",
                scheduler="slurm", partitions=("a100", "debug"), max_jobs=8,
                guide="Use `module load cuda/12.4` before running GPU jobs.",
            ),
            "lab-box": Cluster(
                name="lab-box", root="/data/user/demo", host="lab-gpu",
                scheduler="none", exec_timeout_s=86400,
            ),
        },
    )
    out.extend([
        "## Appendix: Sample Server Instructions",
        "",
        "```text",
        messages.server_instructions(sample).rstrip("\n"),
        "```",
        "",
    ])
    return "\n".join(out)


def main() -> int:
    text = render()
    if "--check" in sys.argv:
        if not TARGET.exists() or TARGET.read_text(encoding="utf-8") != text:
            print("%s is out of date" % TARGET)
            return 1
        return 0
    TARGET.parent.mkdir(parents=True, exist_ok=True)
    TARGET.write_text(text, encoding="utf-8")
    print("wrote %s" % TARGET)
    return 0


if __name__ == "__main__":
    sys.exit(main())
