"""Tests for the finding format."""

from __future__ import annotations

import json

import pytest

from spark_sentinel.findings import (
    Evidence,
    Finding,
    Location,
    Severity,
    Unit,
    format_bytes,
    format_duration,
    format_value,
)


def make(rule="SKEW001", severity=Severity.HIGH, stage=4, impact=None, **kw) -> Finding:
    return Finding(
        rule_id=rule,
        severity=severity,
        title=f"Stage {stage}: something",
        location=Location(app_id="app-1", stage_id=stage, sql_execution_id=0),
        impact_ms=impact,
        **kw,
    )


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("high", Severity.HIGH),
        ("MED", Severity.MEDIUM),
        ("medium", Severity.MEDIUM),
        ("Low", Severity.LOW),
        ("info", Severity.INFO),
        ("critical", Severity.HIGH),
    ],
)
def test_severity_parse(text, expected):
    assert Severity.parse(text) is expected


def test_severity_order_and_errors():
    assert Severity.HIGH > Severity.MEDIUM > Severity.LOW > Severity.INFO
    assert Severity.MEDIUM.label == "MED"
    with pytest.raises(ValueError, match="Choose from"):
        Severity.parse("urgent")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, "0 B"),
        (900, "900 B"),
        (1536, "1.5 KB"),
        (48 * 1024**2, "48.0 MB"),
        (2.1 * 1024**3, "2.1 GB"),
        (5 * 1024**4, "5.0 TB"),
    ],
)
def test_format_bytes(value, expected):
    assert format_bytes(value) == expected


@pytest.mark.parametrize(
    ("ms", "expected"),
    [
        (85, "85 ms"),
        (9_800, "9.8s"),
        (59_940, "59.9s"),
        (372_000, "6m12s"),
        (3_600_000, "1h00m"),
        (7_500_000, "2h05m"),
    ],
)
def test_format_duration(ms, expected):
    assert format_duration(ms) == expected


def test_format_value_units():
    assert format_value(38.2, Unit.RATIO) == "38x"
    assert format_value(3.24, Unit.RATIO) == "3.2x"
    assert format_value(42.4, Unit.PERCENT) == "42%"
    assert format_value(1000, Unit.COUNT) == "1,000"
    assert format_value(None, Unit.BYTES) == "n/a"


def test_evidence_text():
    e = Evidence("slowest task", 372_000, Unit.MS, compare_to=9_800, compare_label="median")
    assert str(e) == "slowest task 6m12s vs median 9.8s"
    assert str(Evidence("tasks", 1000)) == "tasks 1,000"


def test_location_labels():
    assert Location("a", stage_id=4, sql_execution_id=0).label() == "Query 0 · stage 4"
    assert Location("a", stage_id=4, stage_attempt=1).label() == "Stage 4 (attempt 1)"
    assert Location("a", sql_execution_id=2, plan_node="Scan parquet (1)").label() == (
        "Query 2 · Scan parquet (1)"
    )
    assert Location("a", job_ids=(3, 4)).label() == "Job 3, 4"
    assert Location("a").label() == "App"


def test_headline():
    assert make().headline() == "HIGH  SKEW001  Stage 4: something"
    assert make(severity=Severity.MEDIUM).headline().startswith("MED   SKEW001")


def test_json_round_trip():
    original = make(
        evidence=(Evidence("slowest task", 372_000, Unit.MS, 9_800, "median"),),
        cause="Data skew",
        fix=("Salt the key.", "Enable AQE skew join."),
        impact=362_000,
        category="skew",
        details={"ratio": 38.0},
    )
    data = json.loads(json.dumps(original.to_dict()))
    assert data["severity"] == "high"
    assert data["fingerprint"] == "SKEW001:app-1:0:4:"
    assert Finding.from_dict(data) == original


def test_sorting_most_severe_then_biggest_impact():
    findings = [
        make(rule="PLAN001", severity=Severity.MEDIUM, impact=None),
        make(rule="SKEW001", severity=Severity.HIGH, stage=2, impact=1_000),
        make(rule="SPILL001", severity=Severity.HIGH, stage=3, impact=50_000),
        make(rule="PART001", severity=Severity.LOW, impact=999_999),
    ]
    ordered = [f.rule_id for f in sorted(findings, key=Finding.sort_key)]
    assert ordered == ["SPILL001", "SKEW001", "PLAN001", "PART001"]


def test_finding_needs_rule_and_title():
    with pytest.raises(ValueError):
        Finding(rule_id="", severity=Severity.LOW, title="x", location=Location("a"))
    with pytest.raises(ValueError):
        Finding(rule_id="X001", severity=Severity.LOW, title="", location=Location("a"))


def test_fingerprint_ignores_wording_but_not_place():
    a = make(stage=4)
    b = Finding("SKEW001", Severity.LOW, "Different words", a.location)
    c = make(stage=5)
    assert a.fingerprint() == b.fingerprint() != c.fingerprint()
