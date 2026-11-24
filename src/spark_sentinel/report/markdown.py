"""Markdown report, written to read well as a GitHub pull-request comment (Step 13)."""

from __future__ import annotations

from spark_sentinel.findings import Severity, format_duration
from spark_sentinel.report._code import mark_code

BADGE = {
    Severity.HIGH: "🔴 HIGH",
    Severity.MEDIUM: "🟠 MED",
    Severity.LOW: "🟡 LOW",
    Severity.INFO: "🔵 INFO",
}


def _md(text: object) -> str:
    """Make text safe inside Markdown table cells and inline code."""
    return str(text).replace("|", "\\|").replace("\n", " ")


def _prose(text: str) -> str:
    """Prose with code-like tokens in backticks."""
    return mark_code(text, lambda token: f"`{token}`")


def render_markdown(report) -> str:
    out = ["## Spark Sentinel report", ""]
    worst = report.result.max_severity
    icon = "✅" if worst is None else BADGE[worst].split()[0]
    out += [f"{icon} **{report.summary_line()}**", ""]

    if report.findings:
        several = len(report.apps) > 1
        names = {a.app_id: a.app_name or a.app_id for a in report.apps}
        out += [
            "| Severity | Rule | Problem | " + ("App | " if several else "") + "Where |",
            "|---|---|---|" + ("---|" if several else "") + "---|",
        ]
        for f in report.findings:
            app = f"{_md(names[f.location.app_id])} | " if several else ""
            out.append(
                f"| {BADGE[f.severity]} | `{f.rule_id}` | {_md(f.title)} | {app}"
                f"{_md(f.location.label())} |"
            )
        out.append("")

    for app in report.apps:
        meta = [m for m in (app.spark_version and f"Spark {app.spark_version}", app.master) if m]
        if app.duration_ms is not None:
            meta.append(format_duration(app.duration_ms))
        out += [f"### {_md(app.label)}", ""]
        if meta:
            out += ["_" + " · ".join(_md(m) for m in meta) + "_", ""]
        if not app.completed:
            out += [
                "> ⚠️ This log has no ApplicationEnd event: the app was still running or "
                "crashed, so results may be partial.",
                "",
            ]
        if not app.findings:
            out += ["No problems found.", ""]
        for f in app.findings:
            out += [f"#### {BADGE[f.severity]} `{f.rule_id}` {_md(f.title)}", ""]
            where = f.location.label()
            if f.location.sql_description:
                where += f" — `{_md(f.location.sql_description)}`"
            out += [f"**Where:** {where}", ""]
            out += [f"- {_md(e)}" for e in f.evidence]
            out.append("")
            if f.cause:
                out += [f"**Why:** {_prose(f.cause)}", ""]
            if f.fix:
                out += ["**How to fix:**", ""]
                out += [f"{i}. {_prose(fix)}" for i, fix in enumerate(f.fix, 1)]
                out.append("")
            if f.references:
                links = ", ".join(f"[{_md(r.title)}]({r.url})" for r in f.references)
                out += [f"📖 Spark docs: {links}", ""]

    if report.result.errors:
        out += ["### Rule errors", ""]
        out += [f"- `{e.rule_id}` on `{e.app_id}`: {_md(e.message)}" for e in report.result.errors]
        out.append("")

    settings = f"profile `{report.profile}`"
    if report.overrides:
        changed = ", ".join(
            f"`{rule}.{key}={value}`"
            for rule, values in sorted(report.overrides.items())
            for key, value in values.items()
        )
        settings += f", thresholds {changed}"
    out.append(
        f"<sub>Spark Sentinel {report.sentinel_version} · {settings} · {report.generated_at}</sub>"
    )
    return "\n".join(out) + "\n"
