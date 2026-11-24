"""Tests for the rules.

Part 1: each rule on the gallery job that should trigger it, checking evidence, cause and fix.
Part 2: synthetic apps (tests/conftest.py) for paths the gallery doesn't cover.
The full "every bad job triggers its rule, the clean job triggers none" matrix is Step 9.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from conftest import GB, AppBuilder, task
from spark_sentinel.analysis import analyze
from spark_sentinel.findings import Finding, Severity
from spark_sentinel.plan import final_node_ids, final_nodes, parse_nodes
from spark_sentinel.rules import load_builtin_rules
from spark_sentinel.store import Warehouse, ingest

GALLERY = Path(__file__).parent / "fixtures" / "eventlogs" / "gallery"
LAPTOP = {"SKEW001": {"min_time_lost_ms": 1000}}


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory) -> Warehouse:
    root = tmp_path_factory.mktemp("wh")
    ingest(GALLERY, root)
    return Warehouse(root)


@pytest.fixture(scope="module")
def findings(warehouse) -> dict[str, list[Finding]]:
    """Gallery job name -> its findings, with laptop thresholds."""
    names = dict(warehouse.query("SELECT app_id, app_name FROM app")[1])
    result = analyze(warehouse, overrides=LAPTOP)
    assert result.errors == []
    by_job: dict[str, list[Finding]] = {n.removeprefix("gallery-"): [] for n in names.values()}
    for f in result.findings:
        by_job[names[f.location.app_id].removeprefix("gallery-")].append(f)
    return by_job


def only(findings: list[Finding], rule_id: str) -> Finding:
    matching = [f for f in findings if f.rule_id == rule_id]
    assert len(matching) == 1, [f.headline() for f in findings]
    return matching[0]


def test_all_rules_are_registered():
    assert set(load_builtin_rules()) == {
        "SKEW001",
        "SPILL001",
        "FILES001",
        "PLAN001",
        "PLAN002",
        "PART001",
        "PART002",
    }


# --- Part 1: gallery


def test_skew_on_the_skew_job(findings):
    f = only(findings["skew"], "SKEW001")
    assert f.severity is Severity.MEDIUM
    assert (f.location.stage_id, f.location.sql_execution_id) == (4, 0)
    assert f.details["kind"] == "data"
    assert f.details["bytes_ratio"] > 20
    assert 1_000 < f.impact_ms < 10_000
    assert f.cause.startswith("Data skew")
    fixes = " ".join(f.fix)
    assert "spark.sql.adaptive.skewJoin.enabled=true (it is off for this app)" in fixes
    assert "join on customer_id" in fixes
    assert "autoBroadcastJoinThreshold=-1" in fixes


def test_skew_needs_laptop_thresholds_on_laptop_data(warehouse):
    result = analyze(warehouse, select=["SKEW001"])  # production default: 30 s lost
    assert result.findings == []


def test_spill_on_the_spill_job(findings):
    f = only(findings["spill"], "SPILL001")
    assert f.severity is Severity.LOW  # ~250 MB is small by production standards
    assert f.location.stage_id == 3
    assert f.details["concentrated"] is False
    assert f.details["suggested_partitions"] > 4
    assert "spark.driver.memory (now 1.0 GB)" in " ".join(f.fix)  # local mode: driver memory
    assert str(f.evidence[2]) == "tasks that spilled 4 of 4"


def test_small_files_on_the_small_files_job(findings):
    f = only(findings["small_files"], "FILES001")
    assert f.severity is Severity.MEDIUM
    assert f.details["repartition"] == 500
    assert f.details["suggested_files"] == 1
    assert "repartition(500)" in f.cause
    assert f.fix[0].startswith("Use repartition(1) or coalesce(1) instead of repartition(500)")


def test_udf_filter_on_the_udf_job(findings):
    f = only(findings["udf_filter"], "PLAN001")
    assert f.severity is Severity.MEDIUM
    assert f.title == "Query 0: Python UDF is_india in a filter blocks pushdown"
    assert f.location.plan_node == "Filter (4)"
    assert f.details["scans_without_pushdown"] == ["Scan parquet (1)"]
    assert "rows read from files, all sent to Python 2,000,000" in [str(e) for e in f.evidence]


def test_cartesian_on_the_cross_join_job(findings):
    f = only(findings["cross_join"], "PLAN002")
    assert f.severity is Severity.HIGH
    assert f.location.plan_node == "CartesianProduct (7)"
    assert f.details["condition"] == "(a_val < (b_val - 80))"
    assert "Broadcasting is disabled" in " ".join(f.fix)


def test_too_many_partitions_explains_why(findings):
    over = only(findings["over_partitioned"], "PART001")
    assert over.details["suggested_partitions"] == 4  # one per core, data is tiny
    assert over.cause.startswith("spark.sql.shuffle.partitions=1000 is set explicitly")
    assert any("AQE partition coalescing" in fix for fix in over.fix)
    assert over.impact_ms > 0

    small = only(findings["small_files"], "PART001")
    assert small.cause.startswith("The code calls repartition(500)")
    assert not any("AQE partition coalescing" in fix for fix in small.fix)  # AQE is on there


def test_warm_up_is_not_reported_as_skew(warehouse):
    # cross_join's first wave of 4 tasks takes ~1.5 s, later waves ~0.15 s: JVM warm-up.
    result = analyze(warehouse, select=["SKEW001"], overrides={"SKEW001": {"min_time_lost_ms": 0}})
    names = dict(warehouse.query("SELECT app_id, app_name FROM app")[1])
    assert {names[f.location.app_id] for f in result.findings} == {"gallery-skew"}


# --- plans


def test_final_nodes_skip_the_initial_plan(warehouse):
    plan = warehouse.query(
        "SELECT final_plan FROM sql JOIN app USING (app_id) WHERE app_name = 'gallery-cross_join'"
    )[1][0][0]
    all_nodes = parse_nodes(plan)
    final = final_nodes(plan)
    assert [n.name for n in all_nodes.values()].count("CartesianProduct") == 2
    assert [n.name for n in final].count("CartesianProduct") == 1
    assert len(final) < len(all_nodes)


def test_plan_without_aqe_uses_every_node():
    plan = (
        "== Physical Plan ==\n"
        "* Project (3)\n"
        "+- BroadcastNestedLoopJoin Inner BuildRight (2)\n"
        "   :- * Range (1)\n\n\n"
        "(1) Range [codegen id : 1]\nOutput [1]: [id#0L]\n\n"
        "(2) BroadcastNestedLoopJoin\nJoin type: Inner\nJoin condition: (id#0L < x#2L)\n\n"
        "(3) Project [codegen id : 2]\nOutput [1]: [id#0L]\n"
    )
    assert final_node_ids(plan) == {1, 2, 3}
    nodes = {n.id: n for n in final_nodes(plan)}
    assert nodes[2].get("Join condition") == "(id#0L < x#2L)"
    assert nodes[1].name == "Range"


# --- Part 2: synthetic apps


def wave(n: int, stage: int = 0, start: int = 0, **kwargs) -> list[dict]:
    return [task(stage=stage, launch_ms=start, **kwargs) for _ in range(n)]


def test_compute_skew_in_a_later_wave(run_rules):
    app = AppBuilder().add(*wave(12), task(launch_ms=5_000, duration_ms=60_000))
    f = only(run_rules(app, select=["SKEW001"]).findings, "SKEW001")
    assert f.details["kind"] == "compute"
    assert f.severity is Severity.HIGH  # 30x
    assert "Check executor driver" in " ".join(f.fix)


def test_gc_pressure_skew(run_rules):
    app = AppBuilder().add(*wave(12), task(launch_ms=5_000, duration_ms=60_000, gc_ms=30_000))
    f = only(run_rules(app, select=["SKEW001"]).findings, "SKEW001")
    assert f.details["kind"] == "gc"
    assert "garbage collection" in f.cause


def test_slow_first_wave_is_warm_up_not_skew(run_rules):
    app = AppBuilder(cores=4).add(*wave(4, duration_ms=40_000), *wave(12, start=40_000))
    assert run_rules(app, select=["SKEW001"]).findings == []


def test_data_skew_in_the_first_wave_is_still_skew(run_rules):
    app = AppBuilder(cores=4).add(
        task(duration_ms=80_000, shuffle_read=2 * GB),
        *wave(3, duration_ms=8_000),
        *wave(12, start=8_000),
    )
    f = only(run_rules(app, select=["SKEW001"]).findings, "SKEW001")
    assert f.details["kind"] == "data"


def test_skew_needs_enough_tasks(run_rules):
    app = AppBuilder().add(*wave(2), task(duration_ms=90_000))
    assert run_rules(app, select=["SKEW001"]).findings == []


def test_too_few_partitions(run_rules):
    app = AppBuilder(conf={"spark.sql.shuffle.partitions": "4"}).add(*wave(4, shuffle_read=2 * GB))
    f = only(run_rules(app, select=["PART"]).findings, "PART002")
    assert f.details["suggested_partitions"] == 64  # 8 GB / 128 MB
    assert "spark.sql.shuffle.partitions (now 4) to about 64" in f.fix[0]


def test_concentrated_spill_points_at_skew(run_rules):
    app = AppBuilder().add(*wave(19), task(disk_spill=3 * GB, mem_spill=6 * GB))
    f = only(run_rules(app, select=["SPILL001"]).findings, "SPILL001")
    assert f.severity is Severity.MEDIUM  # >= 1 GB
    assert f.details["concentrated"] is True
    assert "skew" in f.cause and f.fix[0].startswith("Fix the skew first")


def test_many_small_files_without_repartition(run_rules):
    app = AppBuilder().add(*wave(12_000, output_bytes=200 * 1024, output_records=10))
    f = only(run_rules(app, select=["FILES001"]).findings, "FILES001")
    assert f.severity is Severity.HIGH
    assert f.details["repartition"] is None
    assert f.fix[0].startswith("Reduce partitions before writing: df.coalesce(")


def test_broadcast_nested_loop_join_is_low(run_rules):
    plan = (
        "== Physical Plan ==\nBroadcastNestedLoopJoin Inner BuildRight (2)\n:- Range (1)\n\n\n"
        "(1) Range\nOutput [1]: [id#0L]\n\n"
        "(2) BroadcastNestedLoopJoin\nJoin type: Inner\nJoin condition: (id#0L < x#2L)\n"
    )
    f = only(run_rules(AppBuilder(plan=plan).add(*wave(4))).findings, "PLAN002")
    assert f.severity is Severity.LOW
    assert f.details["condition"] == "(id < x)"


def test_no_rule_crashes_on_an_empty_app(run_rules):
    result = run_rules(AppBuilder())
    assert result.errors == []
    assert result.findings == []


def test_no_rule_crashes_on_failed_tasks_without_metrics(run_rules):
    app = AppBuilder(plan="== Physical Plan ==\nRange (1)\n\n\n(1) Range\n").add(
        *wave(5, failed=True), *wave(5)
    )
    result = run_rules(app)
    assert result.errors == []
