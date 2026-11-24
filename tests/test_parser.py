"""Tests for the parser.

Part 1 runs against the six real gallery logs (Spark 4.3, zstd + rolled) in tests/fixtures.
Part 2 feeds hand-written events to cover cases the gallery never produces: failed tasks and
stages, removed executors, crashed apps, odd values.
"""

from __future__ import annotations

import statistics
from collections import Counter
from functools import cache
from pathlib import Path

import orjson
import pytest
from typer.testing import CliRunner

from spark_sentinel.cli import app
from spark_sentinel.parser import SCHEMA, EventParser, ParsedApp, parse
from spark_sentinel.reader import open_log

GALLERY = Path(__file__).parent / "fixtures" / "eventlogs" / "gallery"
JOBS = ["clean", "skew", "spill", "udf_filter", "small_files", "over_partitioned"]
MB = 1024 * 1024


@cache
def parsed(job: str) -> ParsedApp:
    return parse(open_log(GALLERY / job))


# --- Part 1: real Spark logs ------------------------------------------------------------------


EXPECTED_COUNTS = {
    #                   tasks stages jobs job_stages sql executors
    "clean": (6, 3, 3, 4, 1, 1),
    "skew": (40, 4, 4, 9, 1, 1),
    "spill": (12, 3, 3, 4, 1, 1),
    "udf_filter": (6, 3, 3, 4, 1, 1),
    "small_files": (533, 3, 3, 4, 1, 1),
    "over_partitioned": (1006, 4, 4, 7, 1, 1),
}


@pytest.mark.parametrize("job", JOBS)
def test_row_counts(job):
    p = parsed(job)
    tables = ("tasks", "stages", "jobs", "job_stages", "sql", "executors")
    assert tuple(len(p[t]) for t in tables) == EXPECTED_COUNTS[job]
    assert len(p["app"]) == 1
    assert len(p["config"]) > 10


@pytest.mark.parametrize("job", JOBS)
def test_every_row_has_exactly_the_schema_columns(job):
    for table, rows in parsed(job).tables.items():
        for row in rows:
            assert list(row) == list(SCHEMA[table]), table


@pytest.mark.parametrize("job", JOBS)
def test_every_event_type_is_understood(job):
    p = parsed(job)
    assert p.unhandled_events == Counter()
    assert p.bad_lines == 0


@pytest.mark.parametrize("job", JOBS)
def test_tables_link_up(job):
    p = parsed(job)
    stage_keys = {(s["stage_id"], s["stage_attempt"]) for s in p["stages"]}
    planned_stages = {js["stage_id"] for js in p["job_stages"]}
    sql_ids = {s["execution_id"] for s in p["sql"]}
    tasks_per_stage = Counter((t["stage_id"], t["stage_attempt"]) for t in p["tasks"])

    assert all((t["stage_id"], t["stage_attempt"]) in stage_keys for t in p["tasks"])
    assert all(
        tasks_per_stage[(s["stage_id"], s["stage_attempt"])] == s["num_tasks"] for s in p["stages"]
    )
    assert {s["stage_id"] for s in p["stages"]} <= planned_stages
    assert {j["sql_execution_id"] for j in p["jobs"]} - {None} <= sql_ids


@pytest.mark.parametrize("job", JOBS)
def test_shuffle_bytes_written_equal_bytes_read(job):
    tasks = parsed(job)["tasks"]
    written = sum(t["shuffle_write_bytes"] for t in tasks)
    read = sum(t["shuffle_read_bytes"] for t in tasks)
    assert written == read > 0


@pytest.mark.parametrize("job", JOBS)
def test_times_are_consistent_and_in_milliseconds(job):
    p = parsed(job)
    for t in p["tasks"]:
        assert t["duration_ms"] == t["finish_time_ms"] - t["launch_time_ms"] >= 0
        assert t["run_time_ms"] <= t["duration_ms"]
        # CPU time was logged in nanoseconds; after conversion it can't exceed the run time
        # (+1 ms slack because run time is whole milliseconds).
        assert t["cpu_time_ms"] <= t["run_time_ms"] + 1
    app = p["app"][0]
    assert app["completed"] and app["exit_code"] == 0
    assert 1_000 < app["duration_ms"] < 600_000
    for ex in p["executors"]:
        assert ex["executor_id"] == "driver"  # local mode
        assert ex["removed_time_ms"] is None  # local mode never logs ExecutorRemoved
        assert ex["end_time_ms"] == app["end_time_ms"]
        assert 0 < ex["lifetime_ms"] <= app["duration_ms"]


@pytest.mark.parametrize("job", JOBS)
def test_app_row(job):
    app = parsed(job)["app"][0]
    assert app["app_id"].startswith("local-")
    assert app["app_name"] == f"gallery-{job}"
    assert app["spark_version"] == "4.3.0"
    assert app["master"] == "local[4]"


def test_skipped_stages_are_planned_but_never_run():
    p = parsed("clean")
    ran = {s["stage_id"] for s in p["stages"]}
    planned = {js["stage_id"] for js in p["job_stages"]}
    assert planned - ran == {2}  # Spark reused stage 1's shuffle output


def test_sql_rows_have_initial_and_final_plans():
    sql = parsed("skew")["sql"][0]
    assert "isFinalPlan=false" in sql["initial_plan"]
    assert "== Final Plan ==" in sql["final_plan"]
    assert "SortMergeJoin" in sql["final_plan"]
    assert sql["plan_updates"] >= 1
    assert sql["error"] is None
    assert sql["duration_ms"] > 0


def test_skew_signal():
    tasks = [t for t in parsed("skew")["tasks"] if t["stage_id"] == 4]
    reads = [t["shuffle_read_bytes"] for t in tasks]
    durations = [t["duration_ms"] for t in tasks]
    assert len(tasks) == 16
    assert max(reads) > 60 * MB
    assert max(reads) / statistics.median(reads) > 20  # data skew
    assert max(durations) / statistics.median(durations) > 3


def test_spill_signal():
    tasks = parsed("spill")["tasks"]
    assert sum(t["disk_spill_bytes"] for t in tasks) > 200 * MB
    assert sum(t["mem_spill_bytes"] for t in tasks) > 200 * MB
    for job in ("clean", "skew", "udf_filter", "small_files", "over_partitioned"):
        assert sum(t["disk_spill_bytes"] for t in parsed(job)["tasks"]) == 0


def scan_details(plan: str) -> str:
    """The detail block of the first Parquet scan in a formatted plan."""
    start = plan.index("(1) Scan parquet")
    return plan[start : plan.index("\n\n", start)]


def test_udf_filter_signal():
    udf_plan = parsed("udf_filter")["sql"][0]["final_plan"]
    clean_plan = parsed("clean")["sql"][0]["final_plan"]
    assert "BatchEvalPython" in udf_plan
    assert "EvalPython" not in clean_plan
    # When nothing is pushed down, the formatted plan OMITS the PushedFilters line entirely
    # (it does not print "PushedFilters: []"). Step 8's rule must test for its absence.
    assert "PushedFilters" not in scan_details(udf_plan)
    assert "PushedFilters: [IsNotNull(country), EqualTo(country,IN)]" in scan_details(clean_plan)


def test_small_files_signal():
    writers = [t for t in parsed("small_files")["tasks"] if t["output_records"] > 0]
    assert len(writers) == 500
    assert statistics.median(t["output_bytes"] for t in writers) < 20 * 1024


def test_over_partitioned_signal():
    stages = parsed("over_partitioned")["stages"]
    big = max(stages, key=lambda s: s["num_tasks"])
    assert big["num_tasks"] == 1000
    tasks = [t for t in parsed("over_partitioned")["tasks"] if t["stage_id"] == big["stage_id"]]
    assert statistics.median(t["shuffle_read_bytes"] for t in tasks) < 64 * 1024


def test_config_values():
    config = {row["key"]: row["value"] for row in parsed("over_partitioned")["config"]}
    assert config["spark.sql.shuffle.partitions"] == "1000"
    assert config["spark.sql.adaptive.coalescePartitions.enabled"] == "false"
    assert all(isinstance(v, str) for v in config.values())


# --- Part 2: hand-written events ------------------------------------------------------------------


def run(events: list[dict], app_id="local-test") -> ParsedApp:
    parser = EventParser(default_app_id=app_id)
    tables: dict[str, list] = {name: [] for name in SCHEMA}
    lines = (orjson.dumps(e) for e in events)
    for table, row in parser.iter_rows(lines):
        tables[table].append(row)
    return ParsedApp(
        parser.app_id, tables, parser.event_counts, parser.unhandled_events, parser.bad_lines
    )


def task_end(task_id=0, stage_id=0, **metrics) -> dict:
    return {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage_id,
        "Stage Attempt ID": 0,
        "Task Type": "ResultTask",
        "Task End Reason": {"Reason": "Success"},
        "Task Info": {
            "Task ID": task_id,
            "Executor ID": "1",
            "Launch Time": 1_000,
            "Finish Time": 3_500,
            "Failed": False,
        },
        "Task Metrics": {
            "Executor Run Time": 2_000,
            "Executor CPU Time": 1_500_000_000,
            "Shuffle Read Metrics": {"Local Bytes Read": 10, "Remote Bytes Read": 32},
            "Shuffle Write Metrics": {"Shuffle Bytes Written": 7, "Shuffle Write Time": 2_500_000},
            **metrics,
        },
    }


def test_units_and_derived_columns():
    row = run([task_end()])["tasks"][0]
    assert row["duration_ms"] == 2_500
    assert row["cpu_time_ms"] == 1_500.0  # 1.5e9 ns
    assert row["shuffle_write_ms"] == 2.5  # 2.5e6 ns
    assert row["shuffle_read_bytes"] == 42  # local + remote
    assert row["executor_id"] == "1"


def test_tasks_stream_before_the_log_ends():
    parser = EventParser()
    assert [table for table, _ in parser.feed(task_end())] == ["tasks"]
    assert list(parser.feed({"Event": "SparkListenerJobStart", "Job ID": 0})) == []


def test_failed_task_without_metrics():
    event = task_end()
    del event["Task Metrics"]
    event["Task End Reason"] = {"Reason": "ExceptionFailure", "Description": "boom"}
    event["Task Info"]["Failed"] = True

    row = run([event])["tasks"][0]

    assert row["end_reason"] == "ExceptionFailure"
    assert row["failed"] is True
    assert row["run_time_ms"] is None
    assert row["shuffle_read_bytes"] is None
    assert row["duration_ms"] == 2_500


def test_failed_stage():
    event = {
        "Event": "SparkListenerStageCompleted",
        "Stage Info": {
            "Stage ID": 3,
            "Stage Attempt ID": 1,
            "Number of Tasks": 8,
            "Submission Time": 100,
            "Completion Time": 900,
            "Failure Reason": "FetchFailed(...)",
        },
    }
    row = run([event])["stages"][0]
    assert (row["status"], row["failure_reason"], row["duration_ms"]) == (
        "failed",
        "FetchFailed(...)",
        800,
    )
    assert row["parent_stage_ids"] is None


def test_jobs_link_to_sql_and_explode_stages():
    events = [
        {
            "Event": "SparkListenerJobStart",
            "Job ID": 7,
            "Submission Time": 1_000,
            "Stage IDs": [10, 11],
            "Stage Infos": [
                {"Stage ID": 10, "Number of Tasks": 4},
                {"Stage ID": 11, "Number of Tasks": 200},
            ],
            "Properties": {"spark.sql.execution.id": "5", "spark.job.description": "nightly"},
        },
        {
            "Event": "SparkListenerJobEnd",
            "Job ID": 7,
            "Completion Time": 4_000,
            "Job Result": {"Result": "JobSucceeded"},
        },
    ]
    p = run(events)
    job = p["jobs"][0]
    assert job["sql_execution_id"] == 5  # string "5" in the log
    assert (job["description"], job["duration_ms"], job["num_stages"]) == ("nightly", 3_000, 2)
    assert [(r["stage_id"], r["planned_tasks"]) for r in p["job_stages"]] == [(10, 4), (11, 200)]


def test_non_sql_job_has_no_execution_id():
    p = run([{"Event": "SparkListenerJobStart", "Job ID": 0, "Stage IDs": [0], "Properties": {}}])
    assert p["jobs"][0]["sql_execution_id"] is None


def test_sql_without_aqe_uses_initial_plan_and_records_errors():
    prefix = "org.apache.spark.sql.execution.ui."
    events = [
        {
            "Event": prefix + "SparkListenerSQLExecutionStart",
            "executionId": 2,
            "time": 10,
            "description": "count",
            "physicalPlanDescription": "== Physical Plan ==\nScan",
        },
        {
            "Event": prefix + "SparkListenerSQLExecutionEnd",
            "executionId": 2,
            "time": 50,
            "errorMessage": "Job aborted",
        },
    ]
    sql = run(events)["sql"][0]
    assert sql["final_plan"] == sql["initial_plan"] == "== Physical Plan ==\nScan"
    assert sql["plan_updates"] == 0
    assert (sql["error"], sql["duration_ms"]) == ("Job aborted", 40)


def test_executor_removed_on_a_cluster():
    events = [
        {
            "Event": "SparkListenerExecutorAdded",
            "Timestamp": 1_000,
            "Executor ID": "3",
            "Executor Info": {"Host": "worker-1", "Total Cores": 4},
        },
        {
            "Event": "SparkListenerExecutorRemoved",
            "Timestamp": 61_000,
            "Executor ID": "3",
            "Removed Reason": "Executor killed by driver.",
        },
        {"Event": "SparkListenerApplicationEnd", "Timestamp": 99_000},
    ]
    ex = run(events)["executors"][0]
    assert (ex["cores"], ex["removed_time_ms"], ex["end_time_ms"], ex["lifetime_ms"]) == (
        4,
        61_000,
        61_000,  # removed before the app ended: its own end time, not the app's
        60_000,
    )
    assert ex["removed_reason"] == "Executor killed by driver."


def test_crashed_app_without_application_end():
    events = [
        {"Event": "SparkListenerApplicationStart", "App ID": "app-2026-0001", "Timestamp": 5},
        {
            "Event": "SparkListenerExecutorAdded",
            "Timestamp": 6,
            "Executor ID": "driver",
            "Executor Info": {"Total Cores": 2},
        },
    ]
    p = run(events, app_id="from-filename")
    app = p["app"][0]
    assert p.app_id == app["app_id"] == "app-2026-0001"  # event beats file name
    assert app["completed"] is False
    assert app["end_time_ms"] is None and app["duration_ms"] is None
    assert p["executors"][0]["lifetime_ms"] is None


def test_unknown_events_and_bad_lines_are_counted_not_fatal():
    parser = EventParser()
    lines = [b'{"Event":"SparkListenerSomethingNew"}', b"not json", orjson.dumps(task_end())]
    rows = list(parser.iter_rows(iter(lines)))
    assert parser.unhandled_events == Counter({"SparkListenerSomethingNew": 1})
    assert parser.bad_lines == 1
    assert sum(1 for table, _ in rows if table == "tasks") == 1


def test_ignored_events_are_not_reported_as_unhandled():
    parser = EventParser()
    list(parser.iter_rows(iter([b'{"Event":"SparkListenerTaskStart"}'])))
    assert parser.unhandled_events == Counter()
    assert parser.event_counts["SparkListenerTaskStart"] == 1


# --- CLI


def test_cli_parse_counts():
    result = CliRunner().invoke(app, ["parse", str(GALLERY / "over_partitioned")])
    assert result.exit_code == 0, result.output
    assert "tasks            1006 rows" in result.output
    assert "spark    4.3.0" in result.output


def test_cli_parse_show_table():
    result = CliRunner().invoke(app, ["parse", str(GALLERY / "skew"), "--show", "sql"])
    assert result.exit_code == 0, result.output
    assert "sql: showing 1 of 1 rows" in result.output
    assert "final_plan" in result.output


def test_cli_parse_rejects_unknown_table():
    result = CliRunner().invoke(app, ["parse", str(GALLERY / "skew"), "--show", "nope"])
    assert result.exit_code == 2
