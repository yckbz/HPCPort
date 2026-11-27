"""Static and syntax checks for the zero-build Web dashboard assets."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

WEB_DIR = Path(__file__).resolve().parents[1] / "src" / "hpcport" / "web"


def test_web_js_syntax_and_contracts():
    app_js = (WEB_DIR / "app.js").read_text(encoding="utf-8")
    i18n_js = (WEB_DIR / "i18n.js").read_text(encoding="utf-8")

    # PIN unlock must apply active tab DOM (avoiding blank screen on /#settings or /#jobs)
    assert "applyActiveTabDom(state.tab);" in app_js
    assert 'state.tab = readHashTab();\n      showAppShell();' in app_js
    # boot() must not double-trigger switchTab after showAppShell
    assert "showAppShell();\n      switchTab(" not in app_js
    # Preset modal fallback timeout must match 3600s default
    assert 'fd.get("exec_timeout_s") || "3600"' in app_js
    assert 'fd.get("exec_timeout_s") || "1800"' not in app_js
    # Strict number validation protects standing/policy/preset forms from NaN -> null
    assert "function parseStrictNumber(" in app_js
    assert "invalidNumber:" in i18n_js

    node = shutil.which("node")
    if node:
        for fname in ("app.js", "i18n.js"):
            r = subprocess.run(
                [node, "--check", str(WEB_DIR / fname)],
                capture_output=True, text=True, timeout=10,
            )
            assert r.returncode == 0, "%s syntax error: %s" % (fname, r.stderr)


def test_retirement_binding_translation_preserves_literal_directory():
    node = shutil.which("node")
    if node:
        script = """
import assert from "node:assert/strict";
import { createI18n } from "./i18n.js";
const localDir = "/work/{nextProject}/$&";
const nextProject = "parent";
assert.equal(
  createI18n("en").t("retireBindingProject", { localDir, nextProject }),
  "New sessions started from /work/{nextProject}/$& will automatically use parent. "
  + "Subdirectories with their own bindings keep them.",
);
"""
        subprocess.run([node, "--input-type=module", "-e", script],
                       cwd=WEB_DIR, check=True, timeout=10)
