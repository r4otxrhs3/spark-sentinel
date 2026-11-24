"""Step 10: reports.

report = build_report(warehouse, result, profile="laptop")
text = render(report, "text")     # terminal: compact, coloured when it's a terminal
text = render(report, "md")       # Markdown: GitHub PR comments, READMEs
text = render(report, "html")     # one self-contained HTML file, no external assets
text = render(report, "json")     # stable machine-readable output for CI and diff mode
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from spark_sentinel import __version__
from spark_sentinel.analysis import AnalysisResult
from spark_sentinel.findings import Finding, Severity
from spark_sentinel.store import Warehouse

FORMATS = ("text", "md", "html", "json")
JSON_SCHEMA_VERSION = 1


@dataclass
class AppSummary:
    app_id: str
    app_name: str | None
    spark_version: str | None
    master: str | None
    duration_ms: int | None
    completed: bool
    findings: list[Finding] = field(default_factory=list)

    @property
    def label(self) -> str:
        return f"{self.app_name} ({self.app_id})" if self.app_name else self.app_id


@dataclass
class Report:
    apps: list[AppSummary]
    result: AnalysisResult
    profile: str
    overrides: dict[str, dict[str, Any]]
    generated_at: str
    sentinel_version: str = __version__

    @property
    def findings(self) -> list[Finding]:
        return self.result.findings

    def counts(self) -> dict[Severity, int]:
        return {sev: self.result.count(sev) for sev in sorted(Severity, reverse=True)}

    def summary_line(self) -> str:
        """'3 findings (1 high, 2 medium) in 2 apps' or 'No problems found in 2 apps'."""
        apps = f"{len(self.apps)} app{'s' if len(self.apps) != 1 else ''}"
        if not self.findings:
            return f"No problems found in {apps}"
        parts = [f"{n} {sev.name.lower()}" for sev, n in self.counts().items() if n]
        n = len(self.findings)
        return f"{n} finding{'s' if n != 1 else ''} ({', '.join(parts)}) in {apps}"


def build_report(
    warehouse: Warehouse,
    result: AnalysisResult,
    *,
    profile: str = "default",
    overrides: dict[str, dict[str, Any]] | None = None,
) -> Report:
    apps = []
    for app_id in result.apps:
        rows = warehouse.query(
            "SELECT app_name, spark_version, master, duration_ms, completed FROM app",
            app_id=app_id,
        )[1]
        name, version, master, duration, completed = rows[0] if rows else (None,) * 5
        apps.append(
            AppSummary(
                app_id=app_id,
                app_name=name,
                spark_version=version,
                master=master,
                duration_ms=duration,
                completed=bool(completed),
                findings=[f for f in result.findings if f.location.app_id == app_id],
            )
        )
    return Report(
        apps=apps,
        result=result,
        profile=profile,
        overrides=overrides or {},
        generated_at=datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
    )


def render(report: Report, fmt: str, *, color: bool = False, width: int = 100) -> str:
    if fmt == "text":
        from spark_sentinel.report.text import render_text

        return render_text(report, color=color, width=width)
    if fmt == "md":
        from spark_sentinel.report.markdown import render_markdown

        return render_markdown(report)
    if fmt == "html":
        from spark_sentinel.report.html import render_html

        return render_html(report)
    if fmt == "json":
        return render_json(report)
    raise ValueError(f"Unknown format '{fmt}'. Choose from: {', '.join(FORMATS)}")


def render_json(report: Report) -> str:
    data = {
        "schema_version": JSON_SCHEMA_VERSION,
        "sentinel_version": report.sentinel_version,
        "generated_at": report.generated_at,
        "profile": report.profile,
        "overrides": report.overrides,
        "summary": {
            "apps": len(report.apps),
            "findings": len(report.findings),
            "by_severity": {sev.name.lower(): n for sev, n in report.counts().items()},
            "max_severity": report.result.max_severity.name.lower()
            if report.result.max_severity is not None
            else None,
        },
        "apps": [
            {
                "app_id": a.app_id,
                "app_name": a.app_name,
                "spark_version": a.spark_version,
                "master": a.master,
                "duration_ms": a.duration_ms,
                "completed": a.completed,
                "findings": len(a.findings),
            }
            for a in report.apps
        ],
        "findings": [f.to_dict() for f in report.findings],
        "errors": [
            {"rule_id": e.rule_id, "app_id": e.app_id, "message": e.message}
            for e in report.result.errors
        ],
    }
    return json.dumps(data, indent=2) + "\n"
