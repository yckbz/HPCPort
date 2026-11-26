"""Format English messages, tool outputs, notices, and errors from prompts.py."""

from __future__ import annotations

import json
import time

from . import prompts as P


def fmt_qty(v: float | int | None) -> str:
    """Format a resource quantity (core-h, GPU-h, GB) without ever rounding
    a non-zero positive value down to '0' or '0.0'."""
    if v is None:
        return "-"
    n = float(v)
    if n == 0.0:
        return "0"
    a = abs(n)
    if a < 0.0001:
        return "<0.0001" if n > 0 else ">-0.0001"
    if a >= 100.0:
        s = "%.1f" % n
    elif a >= 1.0:
        s = "%.2f" % n
    elif a >= 0.01:
        s = "%.3f" % n
    else:
        s = "%.4f" % n
    return s.rstrip("0").rstrip(".")


def _fmt_dur(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    if 0.0 < seconds < 1.0:
        return "<1s"
    s = int(round(max(0.0, seconds)))
    if s < 60:
        return "%ds" % s
    m, s = divmod(s, 60)
    if m < 60:
        return "%dm%02ds" % (m, s)
    h, m = divmod(m, 60)
    return "%dh%02dm" % (h, m)


def _fmt_ts(ts: float | None) -> str:
    if ts is None:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def cluster_line(cl) -> str:
    if cl.scheduler == "slurm":
        line = P.CLUSTER_LINE_SLURM.format(
            name=cl.name,
            root=cl.root,
            partitions=", ".join(cl.partitions) if cl.partitions else "any",
            max_jobs=cl.max_jobs,
            exec_timeout=cl.exec_timeout_s,
        )
        if cl.job_template:
            line += P.CLUSTER_TEMPLATE_PART.format(template=cl.job_template)
    else:
        line = P.CLUSTER_LINE_NONE.format(
            name=cl.name, root=cl.root, exec_timeout=cl.exec_timeout_s
        )
    if cl.guide:
        indented = "\n".join("  " + ln for ln in cl.guide.splitlines())
        line += "\n" + indented
    return line


def server_instructions(project) -> str:
    if project is None:
        return P.SERVER_INSTRUCTIONS_UNBOUND
    local_part = " (local directory `%s`)" % project.local_dir if project.local_dir else ""
    clusters = (
        "\n".join(cluster_line(c) for c in project.clusters.values())
        if project.clusters else P.NO_CLUSTERS
    )
    return P.SERVER_INSTRUCTIONS.format(
        project=project.name, local_part=local_part, clusters=clusters
    )


def error(exc) -> str:
    tmpl = P.ERRORS.get(getattr(exc, "code", ""), None)
    if tmpl is None:
        return str(exc)
    params = dict(getattr(exc, "params", {}))
    if exc.code == "all_finished":
        lines = [tmpl]
        for s in params.get("summaries") or ():
            lines.append("- " + batch_line(s))
        for op in params.get("ops") or ():
            if op:
                lines.append("- " + op_line(op))
        return "\n".join(lines)
    try:
        return tmpl.format(**params)
    except Exception:
        return "%s (%s)" % (exc.code, json.dumps(params, default=str))


def why_text(items: list[dict], table: dict) -> str:
    out = []
    for w in items:
        code = w.get("code", "")
        tmpl = table.get(code)
        if tmpl:
            p = dict(w)
            if isinstance(p.get("scripts"), list):
                p["scripts"] = ", ".join(p["scripts"])
            for k in ("asked", "reached", "limit"):
                if isinstance(p.get(k), (int, float)):
                    p[k] = fmt_qty(p[k])
            out.append(tmpl.format(**p))
        else:
            out.append(code)
    return "; ".join(out)


def permission_summary(project: str, tool: str, tool_input: dict, why: list[dict]) -> str:
    reasons = why_text(why, P.GATE_WHY) if why else tool
    if tool == "remote_exec":
        cmd = " ".join(str(tool_input.get("command", "")).split())[:80]
        return "[%s] %s on %s: %s (%s)" % (
            project, tool, tool_input.get("cluster", "?"), cmd, reasons
        )
    if tool == "transfer":
        return "[%s] transfer %s -> %s (%s)" % (
            project, tool_input.get("src", "?"), tool_input.get("dst", "?"), reasons
        )
    return "[%s] %s (%s)" % (project, tool, reasons)


def submit_approval_summary(project: str, sub: dict, why: list[dict]) -> str:
    res_bits = []
    if sub.get("core_hours") is not None:
        res_bits.append("%s core-h" % fmt_qty(sub["core_hours"]))
    if sub.get("gpu_hours"):
        res_bits.append("%s GPU-h" % fmt_qty(sub["gpu_hours"]))
    res_s = (", " + " / ".join(res_bits)) if res_bits else ""
    reasons = why_text(why, P.SUBMIT_WHY)
    return "[%s] Submit %s on %s (%d job(s)%s): %s — %s" % (
        project, sub["batch"], sub["cluster"], sub["jobs"], res_s, sub["desc"], reasons
    )


def submit_ok(r: dict) -> str:
    res_bits = []
    if r.get("core_hours") is not None:
        res_bits.append("%s core-h" % fmt_qty(r["core_hours"]))
    if r.get("gpu_hours"):
        res_bits.append("%s GPU-h" % fmt_qty(r["gpu_hours"]))
    res_part = ("; est. " + ", ".join(res_bits)) if res_bits else ""

    if r.get("cancelled_inline"):
        c = (" Reason: " + r["refuse_comment"]) if r.get("refuse_comment") else ""
        return P.SUBMIT_CANCELLED_INLINE.format(batch=r["batch"], comment=c)
    if r.get("refused_inline"):
        c = (" Reason: " + r["refuse_comment"]) if r.get("refuse_comment") else ""
        return P.SUBMIT_REFUSED_INLINE.format(batch=r["batch"], comment=c)
    if r.get("approval") is not None:
        return P.SUBMIT_PENDING.format(
            batch=r["batch"], n=r["n"], res_part=res_part, approval=r["approval"]
        )
    sent_part = "%d sent (%s)" % (r["n_sent"], ", ".join(r["ids"])) if r["n_sent"] else "0 sent"
    held_part = ", %d held (max_jobs=%d)" % (r["n_held"], r["max_jobs"]) if r["n_held"] else ""
    if r.get("approved_inline"):
        return P.SUBMIT_APPROVED_INLINE.format(
            approval=r.get("approval_id") or "-",
            batch=r["batch"], cluster=r["cluster"], n=r["n"],
            sent_part=sent_part, held_part=held_part,
        )
    return P.SUBMIT_OK.format(
        batch=r["batch"], cluster=r["cluster"], n=r["n"],
        sent_part=sent_part, held_part=held_part, res_part=res_part,
    )


def batch_line(s: dict) -> str:
    counts = ", ".join("%s:%d" % (k, v) for k, v in sorted(s["jobs"].items()))
    tag = " [%s]" % s["tag"] if s.get("tag") else ""
    state = "FINISHED" if s["finished"] else "ACTIVE"
    notes = (s.get("cancelled") or {}).get("notes") or []
    note_part = " [cancel note: %s]" % "; ".join(notes) if notes else ""
    return "%s (%s)%s [%s] %s — %s%s" % (
        s["batch"], s["cluster"], tag, state, counts, s["desc"], note_part
    )


def op_line(op: dict) -> str:
    res = op.get("result") or {}
    extra = ""
    if op["state"] == "ended":
        extra = " rc=%s elapsed=%s" % (res.get("rc"), _fmt_dur(res.get("elapsed")))
        if res.get("timeout"):
            extra += " [timeout]"
        if res.get("stopped"):
            extra += " [stopped]"
    return "op:%d (%s on %s) [%s]%s" % (op["id"], op["kind"], op["cluster"], op["state"], extra)


def jobs_overview(batches: list[dict], ops: list[dict], unregistered: list[dict]) -> str:
    lines = []
    if batches:
        lines.append("Slurm batches:")
        for s in batches:
            lines.append("  - " + batch_line(s))
    else:
        lines.append("Slurm batches: none")
    if ops:
        lines.append("Background operations:")
        for op in ops[:15]:
            lines.append("  - " + op_line(op))
    if unregistered:
        lines.append("Unregistered Slurm jobs detected under project root:")
        for u in unregistered:
            lines.append("  - %s on %s (%s)" % (u["slurm_id"], u["cluster"], u["workdir"]))
    return "\n".join(lines)


def batch_detail(s: dict, rows: list[dict]) -> str:
    lines = [batch_line(s), "Workdir: " + s["workdir"]]
    for j in rows:
        jid = j["slurm_id"] or "#%d" % j["id"]
        parts = [jid, j["state"], j["script"]]
        if j["partition"]:
            parts.append("part=" + j["partition"])
        if j["exit_code"]:
            parts.append("exit=" + j["exit_code"])
        if j["run_s"] is not None:
            parts.append("run=" + _fmt_dur(j["run_s"]))
        if j["error"]:
            parts.append("err=" + j["error"][:120])
        if j["cancel_by"]:
            parts.append("cancelled_by=" + j["cancel_by"])
        if j.get("cancel_note"):
            parts.append("note=" + j["cancel_note"][:120])
        lines.append("  - " + " | ".join(parts))
    return "\n".join(lines)


def cancel_ok(r: dict) -> str:
    parts = []
    if r["cancelled"]:
        parts.append("cancelled: " + ", ".join(r["cancelled"]))
    if r["ending"]:
        parts.append("cancelling: " + ", ".join(r["ending"]))
    if r["elements"]:
        parts.append("elements requested: " + ", ".join(r["elements"]))
    if r["n_finished"]:
        parts.append("%d already finished" % r["n_finished"])
    if r["ended"]:
        parts.append("batches finished: " + ", ".join(r["ended"]))
    if r["errors"]:
        parts.append("errors: " + "; ".join("%s: %s" % kv for kv in r["errors"].items()))
    return "Cancel result: " + ("; ".join(parts) or "nothing active to cancel")


def gateway_ok(r: dict) -> str:
    op = r["op"]
    st = r["status"]
    if st in ("background", "running"):
        extra = " (still running after %gs)" % r["waited"] if st == "running" else ""
        lines = ["Started background %s `op:%d` on `%s`%s." % (op["kind"], op["id"], op["cluster"], extra)]
        if op.get("log"):
            lines.append("Remote log: `%s`" % op["log"])
        lines.append("Check with `jobs(id=\"op:%d\")`, wait with `wait_for`, or stop with `remote_stop(%d)`."
                     % (op["id"], op["id"]))
        return "\n".join(lines)
    if st == "unknown":
        return ("Command `op:%d` on `%s` lost connection while starting; check `jobs(id=\"op:%d\")`."
                % (op["id"], op["cluster"], op["id"]))
    if st == "stopping":
        return "Stop signal sent to `op:%d`; still shutting down." % op["id"]
    res = op.get("result") or {}
    if op["kind"] == "transfer":
        if res.get("rc") == 0:
            return "Transfer `op:%d` completed in %s (%s -> %s)." % (
                op["id"], _fmt_dur(res.get("elapsed")),
                op["spec"].get("src"), op["spec"].get("dst"),
            )
        flags = [f for f in ("timeout", "stopped", "interrupted") if res.get(f)]
        return "Transfer `op:%d` ended with rc=%s%s: %s" % (
            op["id"], res.get("rc"), (" [%s]" % ",".join(flags)) if flags else "",
            res.get("error") or "unknown error",
        )
    # exec ended
    flags = [f for f in ("timeout", "stopped", "leftover", "lost") if res.get(f)]
    header = "Command `op:%d` on `%s` exited with rc=%s in %s%s." % (
        op["id"], op["cluster"], res.get("rc"), _fmt_dur(res.get("elapsed")),
        (" [%s]" % ",".join(flags)) if flags else "",
    )
    lines = [header]
    if res.get("truncated"):
        lines.append("Output truncated (%d bytes total; full log at `%s`):"
                     % (res.get("size", 0), res.get("log")))
        lines.append("--- head ---\n" + (res.get("head") or ""))
        lines.append("--- tail ---\n" + (res.get("tail") or ""))
    else:
        body = res.get("head") or ""
        if body:
            lines.append(body.rstrip("\n"))
    return "\n".join(lines)


def gate_result(exc) -> str:
    reasons = why_text(exc.why, P.GATE_WHY)
    ans = exc.answer
    if ans.get("pending"):
        return P.GATE_PENDING.format(approval=ans["approval"], reasons=reasons)
    c = (" Comment: " + ans["comment"]) if ans.get("comment") else ""
    return P.GATE_REFUSED.format(approval=ans["approval"], reasons=reasons, comment=c)


def wait_fired(body: dict, daemon) -> str:
    reason = body["reason"]
    if reason == "done":
        head = P.WAIT_FIRED_DONE.format(
            wait_id=body["wait_id"], finished=", ".join(body.get("finished") or ())
        )
    elif reason == "fail":
        head = P.WAIT_FIRED_FAIL.format(
            wait_id=body["wait_id"], batch=body["batch"],
            pct=body["rate"] * 100.0, thresh=body["threshold"] * 100.0,
        )
    else:
        head = P.WAIT_FIRED_TIMEOUT.format(
            wait_id=body["wait_id"], timeout_h=body["timeout_h"]
        )
    lines = [head]
    if body.get("note"):
        lines.append("Note: " + body["note"])
    proj = body["project"]
    for b in body.get("batches") or ():
        s = daemon.jobs.summary(proj, b)
        if s:
            lines.append("  - " + batch_line(s))
    for oid in body.get("ops") or ():
        op = daemon.gateway.op(oid)
        if op:
            lines.append("  - " + op_line(op))
    return "\n".join(lines)


def wait_still_running(wait_id: int, waited: float, progress: str) -> str:
    return P.WAIT_STILL_RUNNING.format(wait_id=wait_id, waited=waited, progress=progress)


def propose_ok(r: dict) -> str:
    fields = ", ".join(
        "%s (%s -> %s)%s" % (f["name"], f["old"], f["new"], " [SAFETY]" if f["safety"] else "")
        for f in r["fields"]
    )
    return P.PROPOSE_OK.format(
        cluster=r["cluster"], id=r["id"], approval=r["approval"], fields=fields
    )


def dashboard_result(r: dict) -> str:
    if r["opened"]:
        return P.DASHBOARD_OPENED.format(url=r["url"])
    return P.DASHBOARD_HEADLESS.format(url=r["url"])


def format_notice(n: dict) -> str:
    reason = n["reason"]
    b = n["body"]
    tmpl = P.NOTICE_TEXTS.get(reason)
    if not tmpl:
        return "%s: %s" % (reason, json.dumps(b, ensure_ascii=False))
    comment = (" Comment: " + b["comment"]) if b.get("comment") else ""
    if reason == "batch_done":
        summary = ", ".join("%s:%d" % (k, v) for k, v in sorted((b.get("jobs") or {}).items()))
        return tmpl.format(batch=b["batch"], cluster=b["cluster"], summary=summary)
    if reason == "jobs_cancelled":
        batches = b.get("batches") or []
        target = ", ".join("batch `%s`" % x for x in batches) if batches else "this project"
        parts = []
        if b.get("cancelled"):
            parts.append("cancelled: " + ", ".join(b["cancelled"]))
        if b.get("ending"):
            parts.append("cancelling: " + ", ".join(b["ending"]))
        if b.get("elements"):
            parts.append("elements: " + ", ".join(b["elements"]))
        return tmpl.format(target=target, items="; ".join(parts) or "jobs", comment=comment)
    if reason == "op_ended":
        outcome = "stopped by user" if b.get("stop_by") == "user" else (
            "timed out" if b.get("timeout") else "exited with rc=%s" % b.get("rc")
        )
        return tmpl.format(kind=b["kind"], op=b["op"], cluster=b["cluster"], outcome=outcome)
    if reason in ("submit_approved", "submit_refused"):
        return tmpl.format(
            approval=b["approval"], batches=", ".join(b.get("batches") or ()), comment=comment
        )
    if reason == "permission":
        verdict = "approved" if b.get("approved") else "rejected"
        grant = " You may now retry the exact same tool call once." if b.get("approved") else ""
        return tmpl.format(approval=b["approval"], tool=b["tool"], verdict=verdict,
                           grant=grant, comment=comment)
    if reason == "preset_updated":
        return tmpl.format(cluster=b["cluster"], version=b["version"])
    if reason == "cluster_attached":
        return tmpl.format(cluster=b["cluster"], scheduler=b["scheduler"], root=b["root"])
    if reason == "proposal_rejected":
        return tmpl.format(proposal=b["proposal"], doc=b["doc"], comment=comment)
    if reason == "proposal_stale":
        return tmpl.format(proposal=b["proposal"], doc=b["doc"],
                           moved=", ".join(b.get("moved") or ()))
    if reason == "wait_done":
        return tmpl.format(wait_id=b["wait_id"], finished=", ".join(b.get("finished") or ()))
    if reason == "wait_fail":
        return tmpl.format(wait_id=b["wait_id"], batch=b["batch"],
                           pct="%.1f" % (b["rate"] * 100), thresh="%.1f" % (b["threshold"] * 100))
    if reason == "wait_timeout":
        return tmpl.format(wait_id=b["wait_id"], timeout_h=b["timeout_h"])
    return "%s: %s" % (reason, json.dumps(b, ensure_ascii=False))


def with_notices(text: str, notices: list[dict]) -> str:
    if not notices:
        return text
    lines = [P.NOTICES_HEADER] + ["- " + format_notice(n) for n in notices] + [P.NOTICES_SEP, text]
    return "\n".join(lines)
