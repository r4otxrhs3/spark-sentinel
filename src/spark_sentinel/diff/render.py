"""Diff reports: text for the terminal, Markdown for pull-request comments, JSON for tools."""

from __future__ import annotations

import json
import textwrap

import typer

from spark_sentinel import __version__
from spark_sentinel.diff.compare import Change, DiffResult, QueryDiff
from spark_sentinel.findings import Finding, format_bytes, format_duration
from spark_sentinel.report._code import mark_code

DIFF_JSON_SCHEMA_VERSION = 1
INDENT = " " * 8
PLAN_LINES = 20  # longest plan diff shown in text; Markdown shows it all (collapsed)

_STATUS = {
    "failed": ("FAILED", typer.colors.RED),
    "slower": ("SLOWER", typer.colors.RED),
    "faster": ("FASTER", typer.colors.GREEN),
    "changed": ("CHANGED", typer.colors.CYAN),
    "new": ("NEW", typer.colors.CYAN),
    "removed": ("REMOVED", typer.colors.CYAN),
}
_ICON = {
    "failed": "❌",
    "slower": "🔴",
    "faster": "🟢",
    "changed": "🔵",
    "new": "🆕",
    "removed": "⚪",
}


def render_diff(result: DiffResult, fmt: str, *, color: bool = False, width: int = 100) -> str:
    if fmt == "text":
        return _text(result, color=color, width=width)
    if fmt == "md":
        return _markdown(result)
    if fmt == "json":
        return _json(result)
    raise ValueError(f"Unknown format '{fmt}' for diff. Choose from: text, md, json")


def _details(q: QueryDiff) -> list[str]:
    """The numbers behind a query's status: task time, plus data that changed noticeably."""
    shown = []
    for c in q.changes:
        if c.name == "duration_ms" or c.before == c.after:
            continue
        if c.name == "task_time_ms" or (c.unit == "bytes" and c.significant):
            shown.append(c.text())
    return shown


def plan_diff_lines(diff: list[str]) -> list[str]:
    """A plan diff without hunk headers, shifted left: deep plans indent by 30+ columns."""
    lines = [line for line in diff if not line.startswith("@@")]
    content = [line[1:] for line in lines if line[1:].strip()]
    indent = min((len(c) - len(c.lstrip()) for c in content), default=0)
    return [line[:1] + line[1 + indent :] for line in lines]


def _thresholds_text(result: DiffResult) -> str:
    t = result.thresholds
    return (
        f"Changes under {t['min_change_pct']:.0f}% or under {format_duration(t['min_time_ms'])}"
        f" / {format_bytes(t['min_bytes'])} are ignored (profile: {result.profile})."
    )


def _app_line(result: DiffResult) -> str:
    parts = [c.text() for c in result.app_changes]
    return " · ".join(parts + result.environment)


def _title(result: DiffResult) -> str:
    b, n = result.base, result.new
    name = n.app_name or b.app_name or "app"
    if b.app_name and n.app_name and b.app_name != n.app_name:
        name = f"{b.app_name} → {n.app_name}"
    return f"{name}: {b.app_id} → {n.app_id}"


# --- text


def _text(result: DiffResult, *, color: bool, width: int) -> str:
    def style(text: str, **kwargs) -> str:
        return typer.style(text, **kwargs) if color else text

    def wrap(text: str, first: str = INDENT, rest: str = INDENT) -> list[str]:
        return textwrap.wrap(text, width=width, initial_indent=first, subsequent_indent=rest)

    out = [style(_title(result), bold=True), style(_app_line(result), dim=True), ""]
    shown = [q for q in result.queries if q.status != "unchanged"]
    for q in shown:
        word, fg = _STATUS[q.status]
        head = style(f"{word:<7}", fg=fg, bold=True)
        out += wrap(f"{head} {q.headline}", first="", rest=INDENT)
        out.append(INDENT + style(q.what, dim=True))
        if q.status in ("slower", "faster", "changed", "failed"):
            if q.status in ("slower", "faster") and not q.confirmed:
                out += wrap(f"Unconfirmed: {q.reasons[0]}.")
            elif q.status == "failed":
                out += [line for r in q.reasons for line in wrap(r)]
            else:  # the first reason is in the headline
                out += [line for r in q.reasons[1:] for line in wrap(f"Also: {r}.")]
            details = _details(q)
            if details:
                out += wrap(" · ".join(details))
            if q.plan_diff:
                out.append(INDENT + style("Plan (- before, + after):", dim=True))
                lines = plan_diff_lines(q.plan_diff)
                for line in lines[:PLAN_LINES]:
                    fg_line = {"-": typer.colors.RED, "+": typer.colors.GREEN}.get(line[:1])
                    out.append(INDENT + "  " + (style(line, fg=fg_line) if fg_line else line))
                if len(lines) > PLAN_LINES:
                    more = f"  ... {len(lines) - PLAN_LINES} more lines (see -f md or json)"
                    out.append(INDENT + style(more, dim=True))
        out.append("")
    if not shown:
        out += [style("No query changed noticeably.", fg=typer.colors.GREEN), ""]

    if result.settings:
        out.append(style("Settings changed", bold=True))
        out += [f"  {s.text()}" for s in result.settings]
        out.append("")
    for title, findings in (
        ("New problems", result.new_findings),
        ("Fixed problems", result.fixed_findings),
    ):
        if findings:
            out.append(style(title, bold=True))
            out += [f"  {f.headline()}" for f in findings]
            out.append("")

    out.append(style(result.summary_line(), bold=True))
    out.append(style(_thresholds_text(result), dim=True))
    return "\n".join(out) + "\n"


# --- Markdown


def _md(text: object) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def _prose(text: str) -> str:
    return mark_code(text, lambda token: f"`{token}`")


def _markdown(result: DiffResult) -> str:
    regressions = result.confirmed_regressions
    if result.count("failed"):
        verdict = "❌ **A query failed in the new run.**"
    elif regressions:
        are = "query is" if len(regressions) == 1 else "queries are"
        verdict = f"🔴 **{len(regressions)} {are} slower.**"
    elif result.regressions:
        verdict = "🟡 **Slower, but unconfirmed** (no change in plan, data or settings)."
    elif result.new_findings:
        verdict = "🟠 **No slowdown, but new problems were found.**"
    else:
        verdict = "✅ **No performance regression.**"

    out = ["## Spark Sentinel diff", "", verdict, ""]
    out += [f"_{_md(_title(result))}_  ", f"_{_md(_app_line(result))}_", ""]

    shown = [q for q in result.queries if q.status != "unchanged"]
    if shown:
        out += ["| | Query | Time | Why |", "|---|---|---|---|"]
        for q in shown:
            time = q.time
            if time and q.status in ("slower", "faster", "changed"):
                when = f"{format_duration(time.before)} → {format_duration(time.after)}"
            else:
                when = format_duration(q.run.duration_ms)
            why = q.reasons[0] if q.reasons else ""
            if q.status in ("slower", "faster") and not q.confirmed:
                why = "unconfirmed: " + why
            out.append(
                f"| {_icon(q)} {q.status} | {_md(q.label)}<br><sub>{_md(q.what)}</sub> | "
                f"{when} | {_md(_prose(why))} |"
            )
        out.append("")

        for q in shown:
            extra = q.reasons[1:] if q.confirmed else []
            details = _details(q)
            if not (extra or details or q.plan_diff):
                continue
            out += [f"#### {_icon(q)} {_md(q.label)}", ""]
            out += [f"- {_prose(r)}" for r in extra]
            out += [f"- {_md(d)}" for d in details]
            if q.plan_diff:
                out += ["", "<details><summary>Plan changes</summary>", "", "```diff"]
                out += plan_diff_lines(q.plan_diff)
                out += ["```", "", "</details>"]
            out.append("")
    else:
        out += ["No query changed noticeably.", ""]

    if result.settings:
        out += ["**Settings changed**", ""]
        out += [
            f"- `{_md(s.key)}`: {_md(s.before or '(default)')} → {_md(s.after or '(default)')}"
            for s in result.settings
        ]
        out.append("")
    for title, findings in (
        ("New problems", result.new_findings),
        ("Fixed problems", result.fixed_findings),
    ):
        if findings:
            out += [f"**{title}**", ""]
            out += [_finding_md(f) for f in findings]
            out.append("")

    out += [
        f"<sub>{_md(result.summary_line())}. {_md(_thresholds_text(result))} "
        f"Spark Sentinel {__version__}</sub>",
    ]
    return "\n".join(out) + "\n"


def _icon(q: QueryDiff) -> str:
    unconfirmed = q.status in ("slower", "faster") and not q.confirmed
    return "🟡" if unconfirmed else _ICON[q.status]


def _finding_md(f: Finding) -> str:
    return f"- {f.severity.label} `{f.rule_id}` {_md(f.title)}"


# --- JSON


def _change_json(c: Change) -> dict:
    return {
        "metric": c.name,
        "unit": c.unit,
        "before": c.before,
        "after": c.after,
        "significant": c.significant,
    }


def _query_json(q: QueryDiff) -> dict:
    return {
        "status": q.status,
        "confirmed": q.confirmed,
        "headline": q.headline,
        "base_execution_id": q.base.execution_id if q.base else None,
        "new_execution_id": q.new.execution_id if q.new else None,
        "description": q.run.description,
        "target": q.run.plan.target,
        "plan_changed": q.plan_changed,
        "reasons": q.reasons,
        "changes": [_change_json(c) for c in q.changes],
        "plan_diff": q.plan_diff,
    }


def _json(result: DiffResult) -> str:
    data = {
        "schema_version": DIFF_JSON_SCHEMA_VERSION,
        "sentinel_version": __version__,
        "profile": result.profile,
        "thresholds": result.thresholds,
        "base": {"app_id": result.base.app_id, "app_name": result.base.app_name},
        "new": {"app_id": result.new.app_id, "app_name": result.new.app_name},
        "summary": {
            status: result.count(status)
            for status in ("slower", "faster", "changed", "new", "removed", "failed", "unchanged")
        }
        | {
            "confirmed_slower": len(result.confirmed_regressions),
            "new_problems": len(result.new_findings),
            "fixed_problems": len(result.fixed_findings),
        },
        "app": [_change_json(c) for c in result.app_changes],
        "environment": result.environment,
        "queries": [_query_json(q) for q in result.queries],
        "settings": [{"key": s.key, "before": s.before, "after": s.after} for s in result.settings],
        "new_problems": [f.to_dict() for f in result.new_findings],
        "fixed_problems": [f.to_dict() for f in result.fixed_findings],
    }
    return json.dumps(data, indent=2) + "\n"
