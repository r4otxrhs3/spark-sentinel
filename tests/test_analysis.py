"""Tests for the rule interface and the engine, using small example rules on real gallery data.

These example rules exist to prove the design end-to-end before the real rules (Step 8).
They use `register=False`, so they never leak into the global rule registry.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from typer.testing import CliRunner

from spark_sentinel import rules as rules_pkg
from spark_sentinel.analysis import (
    SPARK_DEFAULTS,
    AnalysisContext,
    analyze,
    parse_size,
    resolve_rules,
)
from spark_sentinel.cli import app
from spark_sentinel.findings import Evidence, Finding, Severity, Unit
from spark_sentinel.rules import REGISTRY, Rule, load_builtin_rules
from spark_sentinel.store import Warehouse, ingest

GALLERY = Path(__file__).parent / "fixtures" / "eventlogs" / "gallery"


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory) -> Warehouse:
    root = tmp_path_factory.mktemp("wh")
    ingest(GALLERY, root)
    return Warehouse(root)


@pytest.fixture
def clean_registry(monkeypatch):
    """Let a test register rules without leaking them into other tests."""
    monkeypatch.setattr(rules_pkg, "REGISTRY", dict(REGISTRY))
    return rules_pkg


def app_id_of(wh: Warehouse, name: str) -> str:
    return wh.query("SELECT app_id FROM app WHERE app_name = ?", [name])[1][0][0]


def context(wh: Warehouse, name: str) -> AnalysisContext:
    app_id = app_id_of(wh, name)
    return AnalysisContext(wh.connect(app_id), app_id)


# --- example rules


class ExampleSpill(Rule, register=False):
    id = "SPILL900"
    name = "Example: stages that spill to disk"
    category = "spill"
    thresholds = {"min_disk_spill_bytes": 1024 * 1024}

    def evaluate(self, ctx: AnalysisContext) -> Iterator[Finding]:
        for row in ctx.rows(
            """
            SELECT stage_id, stage_attempt, sum(disk_spill_bytes) AS spill,
                   sum(duration_ms) AS task_ms, list(task_id ORDER BY disk_spill_bytes DESC) AS ids
            FROM tasks GROUP BY ALL HAVING sum(disk_spill_bytes) >= ?
            """,
            [self.t("min_disk_spill_bytes")],
        ):
            yield Finding(
                rule_id=self.id,
                severity=Severity.MEDIUM,
                title=f"Stage {row['stage_id']}: tasks spilled to disk",
                location=ctx.locate_stage(
                    row["stage_id"], row["stage_attempt"], task_ids=row["ids"][:3]
                ),
                evidence=(Evidence("spilled to disk", row["spill"], Unit.BYTES),),
                fix=("Give executors more memory or use more partitions.",),
                impact_ms=row["task_ms"],
                category=self.category,
            )


class ExampleSkew(Rule, register=False):
    id = "SKEW900"
    name = "Example: max vs median shuffle read"
    thresholds = {"min_ratio": 5.0, "min_tasks": 3}

    def evaluate(self, ctx: AnalysisContext) -> Iterator[Finding]:
        for row in ctx.rows(
            """
            SELECT stage_id, stage_attempt,
                   max(shuffle_read_bytes) / NULLIF(median(shuffle_read_bytes), 0) AS ratio
            FROM tasks GROUP BY ALL
            HAVING count(*) >= ? AND ratio >= ?
            """,
            [self.t("min_tasks"), self.t("min_ratio")],
        ):
            yield Finding(
                rule_id=self.id,
                severity=Severity.HIGH,
                title=f"Stage {row['stage_id']}: one task read {row['ratio']:.0f}x the median",
                location=ctx.locate_stage(row["stage_id"], row["stage_attempt"]),
                evidence=(Evidence("max / median shuffle read", row["ratio"], Unit.RATIO),),
            )


class Crashes(Rule, register=False):
    id = "BUG900"
    name = "Always crashes"

    def evaluate(self, ctx):
        raise RuntimeError("boom")


# --- the engine on real data


def test_example_rules_find_exactly_the_right_apps(warehouse):
    result = analyze(warehouse, rules=[ExampleSpill(), ExampleSkew()])

    names = dict(warehouse.query("SELECT app_id, app_name FROM app")[1])
    hits = sorted((f.rule_id, names[f.location.app_id]) for f in result.findings)
    assert hits == [
        ("SKEW900", "gallery-skew"),
        ("SPILL900", "gallery-gc_pressure"),  # spills 39 MB: above this example's 1 MB threshold
        ("SPILL900", "gallery-spill"),
    ]
    assert result.errors == []
    assert len(result.apps) == 11
    assert result.rules == ["SPILL900", "SKEW900"]


def test_findings_are_located_down_to_the_sql_query(warehouse):
    result = analyze(warehouse, rules=[ExampleSkew()])
    loc = result.findings[0].location
    assert (loc.stage_id, loc.sql_execution_id) == (4, 0)
    assert loc.sql_description.startswith("parquet at")
    assert loc.job_ids == (2,)  # job 2 planned stages 2, 3, 4
    assert loc.label() == "Query 0 · stage 4"


def test_findings_are_sorted_by_severity(warehouse):
    apps = [app_id_of(warehouse, "gallery-skew"), app_id_of(warehouse, "gallery-spill")]
    result = analyze(warehouse, app_ids=apps, rules=[ExampleSpill(), ExampleSkew()])
    assert [f.severity for f in result.findings] == [Severity.HIGH, Severity.MEDIUM]
    spill = result.findings[1]
    assert len(spill.location.task_ids) == 3
    assert str(spill.evidence[0]).startswith("spilled to disk 2")  # ~246 MB


def test_threshold_override_changes_results(warehouse):
    strict = analyze(warehouse, rules=[ExampleSkew({"min_ratio": 1000})])
    assert strict.findings == []


def test_a_crashing_rule_does_not_stop_the_others(warehouse):
    result = analyze(warehouse, rules=[Crashes(), ExampleSpill()])
    assert len(result.findings) == 2  # spill and gc_pressure
    assert len(result.errors) == 11  # once per app
    assert result.errors[0].message == "RuntimeError: boom"
    assert "Traceback" in result.errors[0].traceback
    assert result.exit_code() == 3


def test_exit_codes(warehouse):
    result = analyze(warehouse, rules=[ExampleSpill()])  # one MEDIUM finding
    assert result.max_severity is Severity.MEDIUM
    assert result.exit_code() == 0
    assert result.exit_code(fail_on=Severity.HIGH) == 0
    assert result.exit_code(fail_on=Severity.MEDIUM) == 1
    assert result.exit_code(fail_on=Severity.LOW) == 1
    assert analyze(warehouse, rules=[]).exit_code(fail_on=Severity.INFO) == 0


def test_analyze_one_app(warehouse):
    skew = app_id_of(warehouse, "gallery-skew")
    result = analyze(warehouse, app_ids=[skew], rules=[ExampleSpill(), ExampleSkew()])
    assert result.apps == [skew]
    assert [f.rule_id for f in result.findings] == ["SKEW900"]


def test_duplicate_findings_are_merged(warehouse):
    result = analyze(warehouse, rules=[ExampleSkew(), ExampleSkew()])
    assert len(result.findings) == 1


# --- context helpers


def test_config_with_spark_defaults(warehouse):
    over = context(warehouse, "gallery-over_partitioned")
    clean = context(warehouse, "gallery-clean")
    skew = context(warehouse, "gallery-skew")

    assert over.conf("spark.sql.shuffle.partitions") == "1000"
    assert not over.conf_is_default("spark.sql.shuffle.partitions")
    assert clean.conf("spark.sql.shuffle.partitions") == "200"  # not in the log: default
    assert clean.conf_is_default("spark.sql.shuffle.partitions")
    assert clean.conf_bool("spark.sql.adaptive.skewJoin.enabled") is True
    assert skew.conf_bool("spark.sql.adaptive.skewJoin.enabled") is False
    assert skew.conf_bytes("spark.sql.autoBroadcastJoinThreshold") == -1
    assert clean.conf_bytes("spark.sql.autoBroadcastJoinThreshold") == 10 * 1024 * 1024
    assert clean.conf("spark.not.a.setting", "fallback") == "fallback"
    assert skew.app["app_name"] == "gallery-skew"


@pytest.mark.parametrize(
    ("text", "unit", "expected"),
    [
        ("10MB", "b", 10 * 1024**2),
        ("10m", "b", 10 * 1024**2),
        ("512k", "b", 512 * 1024),
        ("1g", "b", 1024**3),
        ("2GiB", "b", 2 * 1024**3),
        ("10485760", "b", 10485760),
        ("-1", "b", -1),
        ("4096", "m", 4096 * 1024**2),
        ("1.5g", "b", int(1.5 * 1024**3)),
        ("lots", "b", None),
        (None, "b", None),
    ],
)
def test_parse_size(text, unit, expected):
    assert parse_size(text, unit) == expected


def test_every_spark_default_parses():
    for key, value in SPARK_DEFAULTS.items():
        if value[0].isdigit() and not value.replace(".", "").isdigit():
            assert parse_size(value) is not None, key


# --- rule interface and registry


def test_thresholds_defaults_overrides_and_coercion():
    rule = ExampleSkew({"min_ratio": "7"})  # e.g. from the command line: a string
    assert rule.t("min_ratio") == 7.0 and isinstance(rule.t("min_ratio"), float)
    assert rule.t("min_tasks") == 3
    assert ExampleSkew.thresholds["min_ratio"] == 5.0  # class defaults untouched


def test_unknown_or_bad_threshold_is_rejected():
    with pytest.raises(ValueError, match=r"no threshold 'min_ratoi'\. Valid: min_ratio, min_tasks"):
        ExampleSkew({"min_ratoi": 3})
    with pytest.raises(ValueError, match="must be a float"):
        ExampleSkew({"min_ratio": "lots"})


def test_defining_a_rule_registers_it(clean_registry):
    class NewRule(Rule):
        id = "TEST001"
        name = "A test rule"

        def evaluate(self, ctx):
            return []

    assert clean_registry.REGISTRY["TEST001"] is NewRule
    assert "SPILL900" not in clean_registry.REGISTRY  # register=False rules stay out


def test_registry_rejects_bad_rules(clean_registry):
    with pytest.raises(ValueError, match="must look like SKEW001"):

        class BadId(Rule):
            id = "skew-1"
            name = "x"

            def evaluate(self, ctx):
                return []

    with pytest.raises(TypeError, match="must define a class attribute 'name'"):

        class NoName(Rule):
            id = "TEST002"

            def evaluate(self, ctx):
                return []

    class First(Rule):
        id = "TEST003"
        name = "first"

        def evaluate(self, ctx):
            return []

    with pytest.raises(ValueError, match="used by both First and Second"):

        class Second(Rule):
            id = "TEST003"
            name = "second"

            def evaluate(self, ctx):
                return []


def test_resolve_rules_select_ignore_and_overrides():
    available = {"SKEW900": ExampleSkew, "SPILL900": ExampleSpill, "BUG900": Crashes}

    assert [r.id for r in resolve_rules(available=available)] == ["BUG900", "SKEW900", "SPILL900"]
    assert [r.id for r in resolve_rules(select=["SKEW", "spill900"], available=available)] == [
        "SKEW900",
        "SPILL900",
    ]
    assert [r.id for r in resolve_rules(ignore=["BUG"], available=available)] == [
        "SKEW900",
        "SPILL900",
    ]
    rules = resolve_rules(
        select=["SKEW"], overrides={"SKEW900": {"min_ratio": 9}}, available=available
    )
    assert rules[0].t("min_ratio") == 9.0
    with pytest.raises(ValueError, match="No rule matches 'NOPE'"):
        resolve_rules(select=["NOPE"], available=available)


def test_builtin_rules_load():
    assert isinstance(load_builtin_rules(), dict)


def test_cli_rules_command(clean_registry):
    class Listed(Rule):
        id = "TEST010"
        name = "Listed rule"
        category = "demo"
        description = "Shows up in `sentinel rules`."
        thresholds = {"min_ratio": 5.0}

        def evaluate(self, ctx):
            return []

    result = CliRunner().invoke(app, ["rules"])
    assert result.exit_code == 0, result.output
    assert "TEST010  Listed rule  [demo]" in result.output
    assert "min_ratio = 5.0" in result.output
