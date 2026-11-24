"""Tests for the reports (text, Markdown, HTML, JSON) and the `sentinel analyze` command."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from typer.testing import CliRunner

from conftest import GB, AppBuilder, task
from spark_sentinel import rules as rules_pkg
from spark_sentinel.analysis import analyze
from spark_sentinel.cli import app
from spark_sentinel.findings import Evidence, Finding, Unit
from spark_sentinel.profiles import profile_overrides
from spark_sentinel.report import build_report, render
from spark_sentinel.report.html import _bars, _codeify
from spark_sentinel.rules import REGISTRY, Rule
from spark_sentinel.store import Warehouse, ingest

GALLERY = Path(__file__).parent / "fixtures" / "eventlogs" / "gallery"
runner = CliRunner()


@pytest.fixture(scope="module")
def report(tmp_path_factory):
    root = tmp_path_factory.mktemp("wh")
    ingest(GALLERY, root)
    wh = Warehouse(root)
    result = analyze(wh, profile="laptop")
    return build_report(wh, result, profile="laptop", overrides=profile_overrides("laptop"))


# --- text


def test_text_report_matches_the_planned_format(report):
    text = render(report, "text", width=100)
    lines = text.splitlines()
    head = next(i for i, line in enumerate(lines) if "SKEW001" in line)
    assert lines[head] == "MED   SKEW001  Stage 4: slowest task is 7x the median"
    assert lines[head + 1].startswith("      Query 0 · stage 4 — parquet at")
    assert lines[head + 2].startswith("      slowest task 2.6s vs median 363 ms · its input")
    assert any(line.startswith("      → Turn on AQE skew-join handling") for line in lines)
    assert "gallery-clean" in text and "No problems found." in text
    assert lines[-1] == "18 findings (1 high, 11 medium, 1 low, 5 info) in 11 apps"
    assert "\n\n\n" not in text
    assert all(len(line) <= 100 for line in lines if "https://" not in line)  # URLs can't wrap
    assert "      Spark docs: Optimizing Skew Join https://spark.apache.org/docs/latest/" in text
    assert "\x1b[" not in text  # no colour unless asked


def test_text_colour_only_when_asked(report):
    assert "\x1b[" in render(report, "text", color=True)


# --- markdown


def test_markdown_summary_table_and_code(report):
    md = render(report, "md")
    assert md.startswith(
        "## Spark Sentinel report\n\n🔴 **18 findings (1 high, 11 medium, 1 low, 5 info)"
    )
    assert "| Severity | Rule | Problem | App | Where |" in md
    assert "| 🔴 HIGH | `PLAN002` | Query 0: CartesianProduct join" in md
    assert "`spark.sql.adaptive.skewJoin.enabled=true` (it is off for this app)" in md
    assert "`df.groupBy('customer_id').count().orderBy(F.desc('count')).show(10)`" in md
    assert "profile `laptop`, thresholds `GC001.min_gc_ms=2000`" in md
    assert "`SKEW001.min_time_lost_ms=1000`" in md
    assert "📖 Spark docs: [Optimizing Skew Join](https://spark.apache.org/docs/latest/" in md


def test_markdown_escapes_table_pipes(run_rules, tmp_path):
    result = run_rules(
        AppBuilder(plan="== Physical Plan ==\nX (1)\n\n\n(1) X\n", description="a | b").add(
            *[task(shuffle_read=2 * GB)] * 4
        )
    )
    wh = Warehouse(tmp_path / "wh")
    md = render(build_report(wh, result), "md")
    assert "a \\| b" in md


# --- html


def test_html_is_self_contained_and_complete(report):
    html = render(report, "html")
    assert html.startswith("<!DOCTYPE html>")
    assert "<title>Spark Sentinel report: 18 problems in 11 apps.</title>" in html
    assert not re.search(r"""src=["']?(https?:)?//""", html), "no external assets"
    links = re.findall(r'href="(https?://[^"]+)"', html)
    assert links and all(u.startswith("https://spark.apache.org/docs/latest/") for u in links)
    assert "<script" not in html
    assert html.count('<article class="finding') == 18
    assert "@media (prefers-color-scheme: dark)" in html
    assert "<code>spark.sql.adaptive.skewJoin.enabled=true</code>" in html
    assert 'href="#app-local-' in html


def test_html_escapes_everything_from_the_log(run_rules, tmp_path):
    evil = "<script>alert('x')</script>"
    result = run_rules(
        AppBuilder(plan="== Physical Plan ==\nX (1)\n\n\n(1) X\n", description=evil).add(
            *[task(shuffle_read=2 * GB)] * 4
        )
    )
    html = render(build_report(Warehouse(tmp_path / "wh"), result), "html")
    assert "<script>" not in html
    assert "&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt;" in html


def test_html_for_a_clean_run(run_rules, tmp_path):
    result = run_rules(AppBuilder().add(task()))
    html = render(build_report(Warehouse(tmp_path / "wh"), result), "html")
    assert "No problems in 1 app." in html and "none found a problem" in html


def test_evidence_bars():
    bars = _bars(Evidence("read", 69.8 * 2**20, Unit.BYTES, 2.3 * 2**20, "median"))
    assert bars["value_pct"] == 100 and 3 < bars["base_pct"] < 4
    assert bars["ratio"] == "30x the median"
    small = _bars(Evidence("file", 7_500, Unit.BYTES, 128 * 2**20, "target"))
    assert small["value_pct"] <= 0.01 and small["ratio"] == "1/17896 of the target"
    assert _bars(Evidence("udf", "is_india", Unit.TEXT)) is None
    assert _bars(Evidence("n", 5)) is None  # nothing to compare with


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "set spark.sql.adaptive.enabled=true (now false).",
            "set <code>spark.sql.adaptive.enabled=true</code> (now false).",
        ),
        (
            "(spark.sql.autoBroadcastJoinThreshold=-1).",
            "(<code>spark.sql.autoBroadcastJoinThreshold=-1</code>).",
        ),
        (
            "broadcast it (F.broadcast(df)); then",
            "broadcast it (<code>F.broadcast(df)</code>); then",
        ),
        (
            "use repartition(1) or coalesce(1).",
            "use <code>repartition(1)</code> or <code>coalesce(1)</code>.",
        ),
        ("<b>not code</b>", "&lt;b&gt;not code&lt;/b&gt;"),
    ],
)
def test_code_markup_in_prose(text, expected):
    assert str(_codeify(text)) == expected


# --- json


def test_json_report(report):
    data = json.loads(render(report, "json"))
    assert data["schema_version"] == 1
    assert data["summary"] == {
        "apps": 11,
        "findings": 18,
        "by_severity": {"high": 1, "medium": 11, "low": 1, "info": 5},
        "max_severity": "high",
    }
    assert all(f["references"] for f in data["findings"])
    assert {a["app_name"] for a in data["apps"]} >= {"gallery-clean", "gallery-skew"}
    assert [Finding.from_dict(f) for f in data["findings"]] == report.findings
    assert data["errors"] == []


def test_unknown_format(report):
    with pytest.raises(ValueError, match="Choose from"):
        render(report, "pdf")


# --- sentinel analyze


def analyze_cli(*args: str, wh: Path):
    return runner.invoke(app, ["analyze", *args, "-w", str(wh)])


def test_analyze_ingests_paths_and_prints_text(tmp_path):
    r = analyze_cli(
        str(GALLERY / "skew"), str(GALLERY / "clean"), "--profile", "laptop", wh=tmp_path
    )
    assert r.exit_code == 0, r.output
    assert "MED   SKEW001  Stage 4" in r.stdout
    assert r.stdout.rstrip().endswith("3 findings (2 medium, 1 info) in 2 apps")
    assert "Read 1 log(s)" in r.stderr  # progress goes to stderr, not into the report


def test_analyze_whole_warehouse_and_single_app(tmp_path):
    ingest(GALLERY, tmp_path)
    everything = analyze_cli("--profile", "laptop", "-f", "json", wh=tmp_path)
    assert json.loads(everything.stdout)["summary"]["apps"] == 11

    skew_id = Warehouse(tmp_path).query("SELECT app_id FROM app WHERE app_name='gallery-skew'")[1]
    one = analyze_cli("--app", skew_id[0][0], "--profile", "laptop", "-f", "json", wh=tmp_path)
    data = json.loads(one.stdout)
    assert data["summary"]["apps"] == 1
    assert [f["rule_id"] for f in data["findings"]] == ["SKEW001", "PLAN004", "CONF001"]


@pytest.mark.parametrize(
    ("args", "code"),
    [
        ([], 0),  # findings exist, but --fail-on defaults to never
        (["--fail-on", "high"], 1),  # PLAN002 is HIGH
        (["--fail-on", "high", "--ignore", "PLAN002"], 0),
        (["--fail-on", "medium", "--select", "SPILL"], 0),  # spill is only LOW
        (["--fail-on", "low", "--select", "SPILL"], 1),
        (["--fail-on", "never"], 0),
    ],
)
def test_fail_on_exit_codes(tmp_path, args, code):
    ingest(GALLERY, tmp_path)
    r = analyze_cli("--profile", "laptop", *args, wh=tmp_path)
    assert r.exit_code == code, r.output


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["--threshold", "SKEW001.min_ratio=lots"], "must be a float"),
        (["--threshold", "oops"], "must look like RULE.name=value"),
        (["--threshold", "SKEW001.nope=1"], "has no threshold 'nope'"),
        (["--select", "NOPE"], "No rule matches 'NOPE'"),
        (["--profile", "cluster"], "Unknown profile"),
        (["--format", "pdf"], "Unknown format"),
        (["--fail-on", "urgent"], "Unknown severity"),
    ],
)
def test_bad_options_exit_2_with_a_message(tmp_path, args, message):
    ingest(GALLERY / "clean", tmp_path)
    r = analyze_cli(*args, wh=tmp_path)
    assert r.exit_code == 2
    assert message in r.stderr


def test_unreadable_input_exits_3(tmp_path):
    r = analyze_cli(str(tmp_path / "missing"), wh=tmp_path / "wh")
    assert r.exit_code == 3 and "No such file" in r.stderr
    r = analyze_cli(wh=tmp_path / "empty")
    assert r.exit_code == 3 and "No apps" in r.stderr


def test_rule_crash_exits_3(tmp_path, monkeypatch):
    monkeypatch.setattr(rules_pkg, "REGISTRY", dict(REGISTRY))

    class Boom(Rule):
        id = "BOOM001"
        name = "Crashes"

        def evaluate(self, ctx):
            raise RuntimeError("kaboom")

    ingest(GALLERY / "clean", tmp_path)
    r = analyze_cli(wh=tmp_path)
    assert r.exit_code == 3
    assert "rule BOOM001 crashed" in r.stderr and "kaboom" in r.stderr


def test_output_file_and_summary_line(tmp_path):
    ingest(GALLERY, tmp_path / "wh")
    out = tmp_path / "reports" / "sentinel.html"
    r = analyze_cli("--profile", "laptop", "-f", "html", "-o", str(out), wh=tmp_path / "wh")
    assert r.exit_code == 0, r.output
    assert r.stdout == f"18 findings (1 high, 11 medium, 1 low, 5 info) in 11 apps. Report: {out}\n"
    assert out.read_text().startswith("<!DOCTYPE html>")


def test_config_file_and_cli_precedence(tmp_path, monkeypatch):
    ingest(GALLERY, tmp_path / "wh")
    monkeypatch.chdir(tmp_path)
    (tmp_path / "sentinel.toml").write_text(
        'profile = "laptop"\nfail_on = "medium"\nformat = "json"\nignore = ["PLAN"]\n'
        "[thresholds.SKEW001]\nmin_ratio = 100\n"
    )
    r = analyze_cli(wh=tmp_path / "wh")  # picks up ./sentinel.toml
    data = json.loads(r.stdout)
    rules = {f["rule_id"] for f in data["findings"]}
    assert "SKEW001" not in rules  # min_ratio 100 from the file
    assert not rules & {"PLAN001", "PLAN002"}  # ignored by the file
    assert data["overrides"]["SKEW001"] == {"min_time_lost_ms": 1000, "min_ratio": 100}
    assert r.exit_code == 1  # fail_on = medium from the file

    r = analyze_cli("--fail-on", "never", "--threshold", "SKEW001.min_ratio=5", wh=tmp_path / "wh")
    assert r.exit_code == 0  # the command line wins
    assert "SKEW001" in {f["rule_id"] for f in json.loads(r.stdout)["findings"]}


@pytest.mark.parametrize(
    ("content", "message"),
    [("profile = ", "not valid TOML"), ('colour = "red"', "unknown setting(s) ['colour']")],
)
def test_bad_config_file(tmp_path, content, message):
    cfg = tmp_path / "custom.toml"
    cfg.write_text(content)
    r = analyze_cli("--config", str(cfg), wh=tmp_path / "wh")
    assert r.exit_code == 2 and message in r.stderr
