"""Terminal report: the compact format from the project plan.

HIGH  SKEW001  Stage 4: slowest task is 38x the median
      Query 0 · stage 4 — parquet at NativeMethodAccessorImpl.java:0
      slowest task 6m12s vs median 9.8s · its input + shuffle read 2.1 GB vs median 48 MB
      Data skew: the slowest task read 2.1 GB, 44x the median task's 48 MB. ...
      → Turn on AQE skew-join handling: spark.sql.adaptive.skewJoin.enabled=true ...
"""

from __future__ import annotations

import textwrap

import typer

from spark_sentinel.findings import Finding, Severity, format_duration

INDENT = " " * 6
COLORS = {
    Severity.HIGH: typer.colors.RED,
    Severity.MEDIUM: typer.colors.YELLOW,
    Severity.LOW: typer.colors.CYAN,
    Severity.INFO: typer.colors.WHITE,
}


def render_text(report, *, color: bool = False, width: int = 100) -> str:
    def style(text: str, **kwargs) -> str:
        return typer.style(text, **kwargs) if color else text

    def wrap(text: str, first: str = INDENT, rest: str = INDENT) -> list[str]:
        return textwrap.wrap(text, width=width, initial_indent=first, subsequent_indent=rest)

    out: list[str] = []
    for app in report.apps:
        status = "" if app.completed else "  (incomplete log: app still running or crashed)"
        meta = [p for p in (app.spark_version and f"Spark {app.spark_version}", app.master) if p]
        if app.duration_ms is not None:
            meta.append(format_duration(app.duration_ms))
        out.append(
            style(app.label, bold=True) + (f"  ·  {' · '.join(meta)}" if meta else "") + status
        )
        if not app.findings:
            out += [INDENT[:2] + style("No problems found.", fg=typer.colors.GREEN), ""]
        for finding in app.findings:
            out += _finding_lines(finding, style, wrap)  # each ends with a blank line

    for error in report.result.errors:
        out.append(
            style(
                f"ERROR {error.rule_id} crashed on {error.app_id}: {error.message}",
                fg=typer.colors.RED,
                bold=True,
            )
        )
    out.append(style(report.summary_line(), bold=True))
    return "\n".join(out) + "\n"


def _finding_lines(f: Finding, style, wrap) -> list[str]:
    head = style(f"{f.severity.label:<4}", fg=COLORS[f.severity], bold=True)
    lines = [f"{head}  {style(f.rule_id, bold=True)}  {f.title}"]
    where = f.location.label()
    if f.location.sql_description:
        where += f" — {f.location.sql_description}"
    lines.append(INDENT + style(where, dim=True))
    if f.evidence:
        lines += wrap(" · ".join(str(e) for e in f.evidence))
    if f.cause:
        lines += wrap(f.cause)
    for fix in f.fix:
        lines += wrap(fix, first=INDENT + "→ ", rest=INDENT + "  ")
    for ref in f.references:
        lines.append(INDENT + style(f"Spark docs: {ref.title} {ref.url}", dim=True))
    lines.append("")
    return lines
