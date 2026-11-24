"""HTML report: one self-contained file (no external CSS, fonts or scripts), light and dark.

Evidence that compares two numbers is drawn as two bars on the same linear scale, so a 31x
skew or a 7 KB file next to a 128 MB target is visible at a glance.
"""

from __future__ import annotations

import re
from importlib import resources

import jinja2
from markupsafe import Markup, escape

from spark_sentinel.findings import Evidence, Severity, Unit, format_duration, format_value
from spark_sentinel.report._code import mark_code

_BAR_UNITS = (Unit.BYTES, Unit.MS, Unit.COUNT)


def _codeify(text: str) -> Markup:
    """Escape text, then wrap code-like tokens in <code>."""
    return Markup(mark_code(str(escape(text)), lambda token: f"<code>{token}</code>"))


def _sentence_case(text: str) -> Markup:
    """Uppercase the first letter only (Jinja's capitalize lowercases the rest).

    Labels that are config keys (spark.sql...) are set as code instead, never capitalized.
    """
    if text.startswith("spark."):
        return Markup(f"<code>{escape(text)}</code>")
    return Markup(escape(text[:1].upper() + text[1:]))


def _where(location) -> str:
    return location.label().replace(" · ", ", ")


def _bars(e: Evidence) -> dict | None:
    numeric = isinstance(e.value, int | float) and isinstance(e.compare_to, int | float)
    if not numeric or e.unit not in _BAR_UNITS or max(e.value, e.compare_to) <= 0:
        return None
    top = max(e.value, e.compare_to)
    ratio = None
    if e.compare_to > 0 and e.value > 0:
        r = e.value / e.compare_to
        ratio = (
            f"{r:.0f}x the {e.compare_label}"
            if r >= 2
            else (f"1/{1 / r:.0f} of the {e.compare_label}" if r <= 0.5 else None)
        )
    return {
        "value": format_value(e.value, e.unit),
        "base": format_value(e.compare_to, e.unit),
        "base_label": e.compare_label or "compared with",
        "value_pct": round(100 * e.value / top, 2),
        "base_pct": round(100 * e.compare_to / top, 2),
        "ratio": ratio,
    }


def _about(app) -> str:
    parts = []
    if app.spark_version:
        parts.append(f"Spark {app.spark_version}")
    if app.master:
        parts.append(f"on {app.master}")
    text = " ".join(parts) or "Spark app"
    if app.duration_ms is not None:
        text += f", ran {format_duration(app.duration_ms)}"
    return f"{text}. App ID {app.app_id}."


def _worst_class(app) -> str:
    if not app.findings:
        return "ok"
    return max(f.severity for f in app.findings).name.lower()


def _problems(n: int) -> str:
    return "no problems" if n == 0 else f"{n} problem{'s' if n != 1 else ''}"


def _anchor(app) -> str:
    return "app-" + re.sub(r"[^A-Za-z0-9_-]", "-", app.app_id)


def render_html(report) -> str:
    env = jinja2.Environment(
        autoescape=True, trim_blocks=True, lstrip_blocks=True, undefined=jinja2.StrictUndefined
    )
    source = resources.files("spark_sentinel.report").joinpath("templates/report.html.j2")
    env.filters["first_upper"] = _sentence_case
    env.filters["code"] = _codeify
    template = env.from_string(source.read_text(encoding="utf-8"))

    n_apps = len(report.apps)
    apps = f"{n_apps} app{'s' if n_apps != 1 else ''}"
    verdict = f"{_problems(len(report.findings)).capitalize()} in {apps}."
    overrides = [
        f"{rule}.{key}={value}"
        for rule, values in sorted(report.overrides.items())
        for key, value in values.items()
    ]
    return template.render(
        report=report,
        verdict=verdict,
        worst=report.findings[0] if report.findings else None,
        app_names={a.app_id: a.app_name or a.app_id for a in report.apps},
        counts=list(report.counts().items()),
        overrides=overrides,
        bars=_bars,
        value=lambda e: format_value(e.value, e.unit),
        about=_about,
        worst_class=_worst_class,
        problems=_problems,
        anchor=_anchor,
        where=_where,
        Severity=Severity,
    )
