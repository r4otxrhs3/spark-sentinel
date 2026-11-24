"""Tests for the rules built from the Spark/PySpark docs (GC001, PAR001, PLAN003, PLAN004,
PLAN005, CONF001) and for the docs references every rule carries.

The gallery covers the happy path of each rule (tests/test_matrix.py). These tests cover exact
threshold boundaries, the cause each rule diagnoses, and cases the gallery doesn't produce.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from conftest import GB, MB, AppBuilder, task
from spark_sentinel.analysis import analyze
from spark_sentinel.findings import Finding, Severity
from spark_sentinel.plan import final_tree, first_with_stats
from spark_sentinel.rules import load_builtin_rules
from spark_sentinel.store import Warehouse, ingest

GALLERY = Path(__file__).parent / "fixtures" / "eventlogs" / "gallery"


@pytest.fixture(scope="module")
def gallery(tmp_path_factory) -> dict[str, list[Finding]]:
    root = tmp_path_factory.mktemp("wh")
    ingest(GALLERY, root)
    wh = Warehouse(root)
    names = dict(wh.query("SELECT app_id, app_name FROM app")[1])
    by_job: dict[str, list[Finding]] = {n.removeprefix("gallery-"): [] for n in names.values()}
    for f in analyze(wh, profile="laptop").findings:
        by_job[names[f.location.app_id].removeprefix("gallery-")].append(f)
    return by_job


def only(findings: list[Finding], rule_id: str) -> Finding:
    matching = [f for f in findings if f.rule_id == rule_id]
    assert len(matching) == 1, [f.headline() for f in findings]
    return matching[0]


def fires(run_rules, app: AppBuilder, rule: str, overrides=None) -> list[Finding]:
    result = run_rules(app, select=[rule], overrides=overrides)
    assert result.errors == []
    return [f for f in result.findings if f.rule_id == rule]


# --- docs references


@pytest.mark.parametrize("rule_id", sorted(load_builtin_rules()))
def test_every_rule_cites_spark_docs(rule_id):
    docs = load_builtin_rules()[rule_id].docs
    assert docs, f"{rule_id} must cite the Spark/PySpark doc sections its advice follows"
    for ref in docs:
        assert ref.url.startswith("https://spark.apache.org/docs/latest/"), ref.url
        assert "#" in ref.url, f"{ref.url} should link to a section"
        assert ref.title and ref.title[0].isupper()


def test_findings_carry_their_rules_docs(gallery):
    skew = only(gallery["skew"], "SKEW001")
    assert [r.title for r in skew.references] == [
        "Optimizing Skew Join",
        "Automatically Broadcasting Joins",
    ]
    assert Finding.from_dict(skew.to_dict()).references == skew.references


# --- gallery: what each new rule says about its own job


def test_gc_pressure_names_the_tiny_young_generation(gallery):
    findings = [f for f in gallery["gc_pressure"] if f.rule_id == "GC001"]
    assert len(findings) == 2  # both stages thrash
    f = max(findings, key=lambda f: f.details["gc_fraction"])
    assert f.details["gc_fraction"] > 0.3 and f.details["young_gen"] is True
    assert "-Xmn2m in spark.driver.extraJavaOptions" in f.cause
    assert f.fix[0].startswith("Remove -Xmn2m from spark.driver.extraJavaOptions")


def test_few_tasks(gallery):
    f = only(gallery["few_tasks"], "PAR001")
    assert f.title == "Stage 0: 2 tasks for 4 cores"
    assert f.severity is Severity.MEDIUM  # half the cores idle
    assert f.details == {"idle_fraction": 0.5, "suggested_tasks": 8}
    assert "repartition(8)" in f.fix[0]  # a source stage: split the input


def test_window_without_partition(gallery):
    f = only(gallery["window_no_partition"], "PLAN003")
    assert f.details["window"] is True
    assert f.location.plan_node.startswith("Window (")
    assert "has orderBy but no partitionBy" in f.cause
    assert "Window.partitionBy" in f.fix[0]
    assert not [x for x in gallery["window_no_partition"] if x.rule_id == "PAR001"]


def test_missed_broadcast_in_the_skew_job(gallery):
    f = only(gallery["skew"], "PLAN004")
    assert f.details["small_side"] == "right"
    assert 30_000 < f.details["small_bytes"] < 50_000  # the 1,001-row customers table
    assert "spark.sql.autoBroadcastJoinThreshold=-1" in f.cause
    assert "F.broadcast(small_df)" in f.fix[0]
    assert ("join keys", "customer_id") in [(e.label, e.value) for e in f.evidence]
    assert f.location.plan_node == "SortMergeJoin (12)"


def test_row_by_row_python_udf(gallery):
    f = only(gallery["python_udf"], "PLAN005")
    assert f.details["udfs"] == ["size_band"]
    assert "Spark 4.2+ runs Python UDFs with Arrow by default" in f.cause  # 4.3 fixture
    assert "rows read by the query, all sent to Python 2,000,000" in [str(e) for e in f.evidence]
    assert not [x for x in gallery["udf_filter"] if x.rule_id == "PLAN005"]  # that's PLAN001


def test_settings_that_switch_optimizations_off(gallery):
    f = only(gallery["skew"], "CONF001")
    assert f.severity is Severity.INFO
    assert f.title == "App: 3 automatic optimizations are turned off"
    assert set(f.details["settings"]) == {
        "spark.sql.adaptive.skewJoin.enabled",
        "spark.sql.adaptive.coalescePartitions.enabled",
        "spark.sql.autoBroadcastJoinThreshold",
    }
    assert f.location.label() == "App"
    assert not [x for x in gallery["clean"] if x.rule_id == "CONF001"]


# --- GC001


@pytest.mark.parametrize(("gc_ms", "expected"), [(2_000, True), (1_999, False)])
def test_gc_fraction_boundary(run_rules, gc_ms, expected):
    # 4 tasks x 10 s run time; 20% threshold -> 2,000 ms GC per task
    app = AppBuilder().add(*[task(duration_ms=10_000, gc_ms=gc_ms) for _ in range(4)])
    found = fires(run_rules, app, "GC001", {"GC001": {"min_gc_ms": 0}})
    assert bool(found) is expected


def test_gc_needs_enough_total_gc_time(run_rules):
    app = AppBuilder().add(*[task(duration_ms=10_000, gc_ms=5_000) for _ in range(4)])
    assert fires(run_rules, app, "GC001") == []  # 20 s of GC < 60 s default
    (f,) = fires(run_rules, app, "GC001", {"GC001": {"min_gc_ms": 20_000}})
    assert f.severity is Severity.HIGH  # 50% >= 40%
    assert f.details["young_gen"] is False
    assert "spark.memory.fraction" in " ".join(f.fix)


# --- PAR001


@pytest.mark.parametrize(("stage_ms", "expected"), [(60_000, True), (59_999, False)])
def test_few_tasks_duration_boundary(run_rules, stage_ms, expected):
    app = AppBuilder(cores=4).add(*[task(duration_ms=stage_ms, shuffle_read=0) for _ in range(2)])
    assert bool(fires(run_rules, app, "PAR001")) is expected


def test_few_tasks_needs_fewer_tasks_than_cores(run_rules):
    app = AppBuilder(cores=4).add(*[task(duration_ms=90_000) for _ in range(4)])
    assert fires(run_rules, app, "PAR001") == []


def test_few_tasks_severity_follows_idle_share(run_rules):
    app = AppBuilder(cores=8).add(*[task(duration_ms=90_000) for _ in range(6)])
    (f,) = fires(run_rules, app, "PAR001")
    assert f.severity is Severity.LOW  # 25% idle
    assert f.impact_ms == 22_500
    assert "shuffle partitions" in f.fix[0]  # this stage read shuffle data


# --- PLAN003


SINGLE = (
    "== Physical Plan ==\nAdaptiveSparkPlan (5)\n+- == Final Plan ==\n"
    "   Window (4)\n   +- Sort (3)\n      +- ShuffleQueryStage (2)\n"
    "         +- Exchange (1)\n\n\n"
    "(1) Exchange\nArguments: SinglePartition, ENSURE_REQUIREMENTS, [plan_id=1]\n\n"
    "(4) Window\nArguments: [sum(x#1) windowspecdefinition(ts#2 ASC NULLS FIRST, "
    "specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())) AS t#3], "
    "[ts#2 ASC NULLS FIRST]\n"
)


@pytest.mark.parametrize(("read", "expected"), [(512 * MB, True), (512 * MB - 1, False)])
def test_single_partition_boundary(run_rules, read, expected):
    app = AppBuilder(plan=SINGLE).add(task(shuffle_read=read, duration_ms=300_000))
    found = fires(run_rules, app, "PLAN003")
    assert bool(found) is expected
    if found:
        assert found[0].details["window"] is True


def test_single_task_without_single_partition_exchange_is_not_plan003(run_rules):
    plan = "== Physical Plan ==\nSort (1)\n\n\n(1) Sort\nArguments: [x#1 ASC]\n"
    app = AppBuilder(plan=plan).add(task(shuffle_read=2 * GB))
    assert fires(run_rules, app, "PLAN003") == []


def test_repartition_one(run_rules):
    plan = SINGLE.replace("ENSURE_REQUIREMENTS", "REPARTITION_BY_NUM").replace("Window", "Project")
    (f,) = fires(run_rules, AppBuilder(plan=plan).add(task(shuffle_read=GB)), "PLAN003")
    assert "repartition(1)" in f.cause


# --- PLAN004


def join_plan(join_type: str, left: str, right: str) -> str:
    return (
        "== Physical Plan ==\nAdaptiveSparkPlan (6)\n+- == Final Plan ==\n"
        f"   SortMergeJoin {join_type} (5)\n"
        "   :- Sort (2)\n"
        f"   :  +- ShuffleQueryStage (1), Statistics(sizeInBytes={left}, rowCount=10)\n"
        "   +- Sort (4)\n"
        f"      +- ShuffleQueryStage (3), Statistics(sizeInBytes={right}, rowCount=1.00E+9)\n"
        "+- == Initial Plan ==\n   SortMergeJoin (7)\n\n\n"
        f"(5) SortMergeJoin\nLeft keys [1]: [k#1]\nRight keys [1]: [k#2]\n"
        f"Join type: {join_type}\nJoin condition: None\n"
    )


def test_join_tree_and_runtime_sizes():
    tree = final_tree(join_plan("Inner", "1.0 KiB", "5.0 GiB"))
    assert 7 not in tree  # initial plan ignored
    assert first_with_stats(tree, 2).size_bytes == 1024
    assert first_with_stats(tree, 4).size_bytes == 5 * GB
    assert tree[1].parent == 2 and tree[2].parent == 5


@pytest.mark.parametrize(
    ("join_type", "left", "right", "side"),
    [
        ("Inner", "1.0 KiB", "5.0 GiB", "left"),
        ("Inner", "5.0 GiB", "2.0 MiB", "right"),
        ("LeftOuter", "5.0 GiB", "2.0 MiB", "right"),
        ("LeftOuter", "1.0 KiB", "5.0 GiB", None),  # can't broadcast the preserved side
        ("RightOuter", "1.0 KiB", "5.0 GiB", "left"),
        ("FullOuter", "1.0 KiB", "5.0 GiB", None),
        ("Inner", "11.0 MiB", "5.0 GiB", None),  # over the 10 MB limit
    ],
)
def test_missed_broadcast_respects_join_type_and_size(run_rules, join_type, left, right, side):
    app = AppBuilder(plan=join_plan(join_type, left, right)).add(task())
    found = fires(run_rules, app, "PLAN004")
    assert (found[0].details["small_side"] if found else None) == side


def test_missed_broadcast_cause_depends_on_settings(run_rules):
    plan = join_plan("Inner", "1.0 KiB", "5.0 GiB")
    off = AppBuilder(plan=plan, conf={"spark.sql.adaptive.enabled": "false"}).add(task())
    assert "AQE is off" in fires(run_rules, off, "PLAN004")[0].cause
    default = AppBuilder(plan=plan).add(task())
    assert "statistics" in " ".join(fires(run_rules, default, "PLAN004")[0].fix)


# --- PLAN005


def udf_plan(node: str = "BatchEvalPython", used_in_filter: bool = False) -> str:
    consumer = (
        "(2) Filter\nCondition : pythonUDF0#9\n"
        if used_in_filter
        else "(2) Project\nOutput [1]: [pythonUDF0#9 AS band#3]\n"
    )
    return (
        f"== Physical Plan ==\n{'Filter' if used_in_filter else 'Project'} (2)\n"
        f"+- {node} (1)\n\n\n"
        f"(1) {node}\nArguments: [size_band(amount#1)#2], [pythonUDF0#9]\n\n{consumer}"
    )


@pytest.mark.parametrize(
    ("version", "conf", "cause"),
    [
        ("4.2.0", {}, "Spark 4.2+ runs Python UDFs with Arrow by default"),
        ("3.5.6", {}, "Before Spark 4.2, Python UDFs run pickled"),
        ("3.4.1", {}, "Before Spark 3.5"),
        ("4.2.0", {"spark.sql.execution.pythonUDF.arrow.enabled": "false"}, "turned off"),
    ],
)
def test_python_udf_cause_by_version(run_rules, version, conf, cause):
    app = AppBuilder(plan=udf_plan(), spark_version=version, conf=conf).add(task())
    (f,) = fires(run_rules, app, "PLAN005")
    assert cause in f.cause
    assert f.details["udfs"] == ["size_band"]


def test_arrow_udfs_and_filter_only_udfs_are_not_plan005(run_rules):
    arrow = AppBuilder(plan=udf_plan("ArrowEvalPython")).add(task())
    in_filter = AppBuilder(plan=udf_plan(used_in_filter=True)).add(task())
    assert fires(run_rules, arrow, "PLAN005") == []
    assert fires(run_rules, in_filter, "PLAN005") == []


# --- CONF001


def test_conf_reports_only_explicit_settings_that_turn_defaults_off(run_rules):
    app = AppBuilder(
        spark_version="3.5.1",
        conf={
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.autoBroadcastJoinThreshold": "-1",
            "spark.sql.execution.pythonUDF.arrow.enabled": "false",  # already the 3.5 default
            "spark.sql.adaptive.skewJoin.enabled": "true",  # on: fine
        },
    ).add(task())
    (f,) = fires(run_rules, app, "CONF001")
    assert set(f.details["settings"]) == {
        "spark.sql.adaptive.enabled",
        "spark.sql.autoBroadcastJoinThreshold",
    }
    assert f.title == "App: 2 automatic optimizations are turned off"


def test_conf_nothing_to_report(run_rules):
    assert fires(run_rules, AppBuilder().add(task()), "CONF001") == []
