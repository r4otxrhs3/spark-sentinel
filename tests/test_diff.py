"""Step 12: sentinel diff.

The fixtures in tests/fixtures/eventlogs/diff are real runs of lab/diff/sales_report.py:
base and rerun are the same code run twice (warm), cold is the same code run first after a
pause, and the others each make one change. See lab/diff/sales_report.py.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pytest
from typer.testing import CliRunner

from spark_sentinel.cli import app
from spark_sentinel.diff import compare, diff_thresholds, match_queries, setting_changes
from spark_sentinel.diff.compare import QueryRun, _change
from spark_sentinel.diff.plans import PlanInfo, normalize, plan_info
from spark_sentinel.diff.render import plan_diff_lines, render_diff
from spark_sentinel.store import Warehouse, ingest

DIFF = Path(__file__).parent / "fixtures" / "eventlogs" / "diff"
GALLERY = Path(__file__).parent / "fixtures" / "eventlogs" / "gallery"
RUNS = ("base", "rerun", "cold", "no_broadcast", "python_udf", "extra_query")


@pytest.fixture(scope="module")
def runs(tmp_path_factory) -> tuple[Warehouse, dict[str, str]]:
    root = tmp_path_factory.mktemp("diffwh")
    ids = {name: ingest(DIFF / name, root)[0].app_id for name in RUNS}
    return Warehouse(root), ids


def diff(runs, base: str, new: str, **kwargs):
    wh, ids = runs
    kwargs.setdefault("profile", "laptop")
    return compare(wh, ids[base], ids[new], **kwargs)


def by_target(result, target: str):
    (query,) = [q for q in result.queries if q.run.plan.target == f"report/{target}"]
    return query


# --- real runs


def test_identical_runs_report_nothing(runs):
    result = diff(runs, "base", "rerun")
    assert [q.status for q in result.queries] == ["unchanged"] * 3
    assert result.settings == []
    assert result.new_findings == result.fixed_findings == []
    assert result.exit_code(["slower", "info"]) == 0


def test_identical_runs_stay_quiet_with_margin(runs):
    """Noise between the warm runs is well under the laptop thresholds, not just under."""
    result = diff(runs, "base", "rerun")
    limits = result.thresholds
    for q in result.queries:
        time = q.time
        assert abs(time.after - time.before) < 0.6 * limits["min_time_ms"], q.headline


def test_broadcast_join_turned_off_is_caught_and_explained(runs):
    result = diff(runs, "base", "no_broadcast")
    q = by_target(result, "revenue")
    assert q.status == "slower" and q.confirmed and q.plan_changed
    assert q.time.ratio > 1.3
    assert q.reasons[0].startswith("the broadcast join became a sort-merge join")
    assert "adds 2 shuffles" in q.reasons[0]
    assert "spark.sql.autoBroadcastJoinThreshold changed from the default to -1" in q.reasons[0]
    assert q.headline.startswith("Query 0: 1.5x slower (")
    assert any(line.startswith("+") and "SortMergeJoin" in line for line in q.plan_diff)
    assert any(line.startswith("-") and "BroadcastHashJoin" in line for line in q.plan_diff)
    assert [s.key for s in result.settings] == ["spark.sql.autoBroadcastJoinThreshold"]
    assert {f.rule_id for f in result.new_findings} == {"PLAN004", "CONF001"}
    # Only the changed query is flagged.
    assert sorted(q.status for q in result.queries) == ["slower", "unchanged", "unchanged"]
    assert result.exit_code(["slower"]) == 1
    assert result.exit_code(["high"]) == 0  # the new problems are MED and INFO
    assert result.exit_code(["medium"]) == 1


def test_reverting_the_change_reads_as_a_fix(runs):
    result = diff(runs, "no_broadcast", "base")
    q = by_target(result, "revenue")
    assert q.status == "faster" and q.confirmed
    assert q.reasons[0].startswith("the sort-merge join became a broadcast join")
    assert "removes 2 shuffles" in q.reasons[0]
    assert {f.rule_id for f in result.fixed_findings} == {"PLAN004", "CONF001"}
    assert result.new_findings == []
    assert result.exit_code(["slower", "info"]) == 0


def test_row_by_row_python_udf_is_caught(runs):
    result = diff(runs, "base", "python_udf")
    q = by_target(result, "bands")
    assert q.status == "slower" and q.confirmed
    assert q.reasons[0] == "it now runs a row-by-row Python UDF"
    assert q.time.ratio > 3
    assert result.settings == []  # a code change, not a setting
    assert [f.rule_id for f in result.new_findings] == ["PLAN005"]
    assert sorted(q.status for q in result.queries) == ["slower", "unchanged", "unchanged"]


def test_an_added_query_shifts_ids_but_matching_holds(runs):
    result = diff(runs, "base", "extra_query")
    statuses = Counter(q.status for q in result.queries)
    assert statuses == {"unchanged": 3, "new": 1}
    (added,) = [q for q in result.queries if q.status == "new"]
    assert added.run.description.startswith("count at")
    pairs = {q.base.execution_id: q.new.execution_id for q in result.queries if q.base and q.new}
    assert pairs == {0: 1, 1: 2, 2: 3}
    assert result.exit_code(["slower", "info"]) == 0


def test_query_removed(runs):
    result = diff(runs, "extra_query", "base")
    assert Counter(q.status for q in result.queries) == {"unchanged": 3, "removed": 1}
    (gone,) = [q for q in result.queries if q.status == "removed"]
    assert gone.headline.startswith("Query 0: no longer runs")


def test_cold_run_slowdown_is_reported_but_unconfirmed(runs):
    """A cold first run is 1.7x slower with nothing changed: report it, don't fail the build."""
    result = diff(runs, "base", "cold")
    (q,) = result.regressions
    assert q.status == "slower" and not q.confirmed
    assert "may be noise" in q.reasons[0]
    assert ":" not in q.headline.split(")", 1)[1]  # no cause claimed in the headline
    assert result.confirmed_regressions == []
    assert result.exit_code(["slower"]) == 0


def test_cluster_profile_ignores_seconds_but_still_reports_the_plan_change(runs):
    """With the default thresholds (30 s), a 2 s slowdown isn't 'slower', but the join change
    is still worth a mention."""
    result = diff(runs, "base", "no_broadcast", profile="default")
    q = by_target(result, "revenue")
    assert q.status == "changed"
    assert q.headline.startswith("Query 0: same speed, but it changed: the broadcast join")
    assert result.exit_code(["slower"]) == 0
    assert diff(runs, "base", "cold", profile="default").regressions == []


def test_both_thresholds_must_be_crossed(runs):
    assert diff(runs, "base", "python_udf", thresholds={"min_change_pct": 1000}).regressions == []
    assert diff(runs, "base", "python_udf", thresholds={"min_time_ms": 60_000}).regressions == []
    lowered = diff(runs, "base", "python_udf", thresholds={"min_change_pct": 1, "min_time_ms": 1})
    assert len(lowered.regressions) >= 1


def test_diff_thresholds_validation():
    assert diff_thresholds("laptop")["min_time_ms"] == 1000
    assert diff_thresholds("default", {"min_time_ms": "5000"})["min_time_ms"] == 5000
    with pytest.raises(ValueError, match="Unknown diff threshold"):
        diff_thresholds("default", {"min_time": 1})
    with pytest.raises(ValueError, match="must be a number"):
        diff_thresholds("default", {"min_time_ms": "soon"})
    with pytest.raises(ValueError, match="Unknown profile"):
        diff_thresholds("huge")


# --- plan normalization


PLAN_A = """== Physical Plan ==
AdaptiveSparkPlan (5)
+- Execute InsertIntoHadoopFsRelationCommand (4)
   +- * HashAggregate (3) [codegen id : 2]
      +- Exchange (2)
         +- Scan parquet  (1)

(1) Scan parquet [codegen id : 1]
Output [2]: [user_id#12L, amount#14]
Location: InMemoryFileIndex [file:/home/a/lab/data/events]
ReadSchema: struct<user_id:bigint,amount:double>

(2) Exchange
Input [2]: [user_id#12L, amount#14]
Arguments: hashpartitioning(user_id#12L, 16), ENSURE_REQUIREMENTS, [plan_id=83]

(4) Execute InsertIntoHadoopFsRelationCommand
Input: []
Arguments: file:/tmp/pytest-1/out/spent, false, Parquet, [path=/tmp/pytest-1/out/spent], \
Overwrite, [user_id, spent]

(5) AdaptiveSparkPlan
Output: []
Arguments: isFinalPlan=false
"""


def _renumber(plan: str) -> str:
    """The same plan from another run: other ids, other paths, a random seed, sizes."""
    return (
        plan.replace("#12L", "#40L")
        .replace("#14", "#41")
        .replace("plan_id=83", "plan_id=191")
        .replace("[codegen id : 1]", "[codegen id : 3]")
        .replace("/home/a/", "/runner/work/x/")
        .replace("/tmp/pytest-1/", "/tmp/pytest-77/")
        .replace("Exchange (2)", "Exchange (2), Statistics(sizeInBytes=3.8 KiB, rowCount=80)")
    )


def test_normalize_removes_what_changes_between_runs():
    text = normalize(PLAN_A)
    for gone in ("#12L", "#14", "plan_id=83", "codegen id", "/home/a", "/tmp/pytest-1", "(5)"):
        assert gone not in text
    assert "hashpartitioning(user_id, 16)" in text  # the meaning stays
    assert normalize(_renumber(PLAN_A)) == text
    assert normalize("rand(-4419513218353425231) and 1732521600000000") == "rand(<n>) and <n>"
    assert normalize("s3a://bucket/a/b, abfss://c@d.dfs.core.windows.net/e") == "<path>, <path>"


def test_plan_info_keys():
    a, b = plan_info(PLAN_A, None), plan_info(_renumber(PLAN_A), None)
    assert (a.plan_key, a.shape_key, a.sources_key) == (b.plan_key, b.shape_key, b.sources_key)
    assert a.target == "out/spent"
    assert a.written == ("user_id", "spent")

    more_partitions = plan_info(PLAN_A.replace("16)", "200)"), None)
    assert more_partitions.plan_key != a.plan_key  # a different plan...
    assert more_partitions.shape_key == a.shape_key  # ...of the same query

    other_columns = plan_info(PLAN_A.replace("[user_id, spent]", "[user_id, total]"), None)
    assert other_columns.shape_key != a.shape_key
    assert other_columns.sources_key == a.sources_key

    other_input = plan_info(PLAN_A.replace("amount:double", "amount:decimal"), None)
    assert other_input.sources_key != a.sources_key


def test_renamed_output_column_is_named_not_just_a_plan_change():
    from spark_sentinel.diff.compare import _compare_query

    renamed = PLAN_A.replace("[user_id, spent]", "[user_id, total_spent]")
    base = QueryRun(0, "", 1000, None, plan_info(PLAN_A, None), {})
    new = QueryRun(0, "", 1050, None, plan_info(renamed, None), {})
    limits = diff_thresholds("laptop")
    q = _compare_query(base, new, limits, [])
    assert q.status == "changed"
    assert q.reasons == ["the columns it writes changed (spent → total_spent)"]
    assert q.headline == f"Query 0: same speed, but it changed: {q.reasons[0]}"


def test_plan_diff_lines_drop_headers_and_shared_indent():
    lines = ["@@ -1,2 +1,2 @@", "         +- A", "-           +- B", "+           +- C"]
    assert plan_diff_lines(lines) == [" +- A", "-   +- B", "+   +- C"]


# --- matching


def _q(eid: int, plan: str, shape: str = "s", sources: str | None = "src") -> QueryRun:
    info = PlanInfo(plan, shape, sources, (), Counter(), Counter(), 0, None)
    return QueryRun(eid, "", 1000, None, info, {})


def _pairs(base, new) -> list[tuple[int | None, int | None]]:
    return [
        (b.execution_id if b else None, n.execution_id if n else None)
        for b, n in match_queries(base, new)
    ]


def test_matching_prefers_the_same_plan_and_ignores_ids():
    base = [_q(0, "p1", "s1"), _q(1, "p2", "s2"), _q(2, "p3", "s3")]
    new = [_q(10, "p3", "s3"), _q(11, "p1", "s1"), _q(12, "p2", "s2")]  # reordered, new ids
    assert _pairs(base, new) == [(2, 10), (0, 11), (1, 12)]


def test_matching_repeated_queries_pair_in_order():
    base = [_q(0, "p"), _q(1, "p"), _q(2, "p")]
    new = [_q(5, "p"), _q(6, "p")]
    assert _pairs(base, new) == [(0, 5), (1, 6), (2, None)]


def test_matching_falls_back_to_shape_then_sources():
    base = [_q(0, "old-plan", "shape-a", "src-a"), _q(1, "x", "shape-b", "src-b")]
    new = [_q(0, "new-plan", "shape-a", "src-a"), _q(1, "y", "shape-new", "src-b")]
    assert _pairs(base, new) == [(0, 0), (1, 1)]


def test_matching_a_strict_match_wins_over_a_looser_earlier_one():
    base = [_q(0, "p-old", "shape"), _q(1, "p-same", "shape")]
    new = [_q(0, "p-same", "shape")]
    assert _pairs(base, new) == [(1, 0), (0, None)]


def test_matching_unknown_sources_never_match_on_sources_alone():
    base = [_q(0, "a", "sa", None)]
    new = [_q(0, "b", "sb", None)]
    assert _pairs(base, new) == [(None, 0), (0, None)]


# --- thresholds and settings


def test_a_change_needs_both_the_relative_and_the_absolute_threshold():
    limits = {"min_change_pct": 25, "min_time_ms": 1000, "min_bytes": 8}
    assert _change("t", "t", "ms", 1000, 2001, limits).significant  # +100%, +1 s
    assert not _change("t", "t", "ms", 100, 900, limits).significant  # +800%, only 0.8 s
    assert not _change("t", "t", "ms", 60_000, 70_000, limits).significant  # +10 s, only 17%
    assert _change("t", "t", "ms", 60_000, 40_000, limits).significant  # faster counts too
    assert _change("b", "b", "bytes", 0, 8, limits).significant  # from nothing: absolute only
    assert not _change("b", "b", "bytes", 0, 7, limits).significant


def test_setting_changes_skip_per_run_values_and_secrets():
    before = {
        "spark.app.id": "local-1",
        "spark.driver.port": "1",
        "spark.eventLog.dir": "/a",
        "spark.sql.shuffle.partitions": "200",
        "spark.hadoop.fs.s3a.secret.key": "x",
        "spark.hadoop.fs.azure.account.key.acct.dfs.core.windows.net": "k1",
        "spark.executor.memory": "4g",
    }
    after = {
        "spark.app.id": "local-2",
        "spark.driver.port": "2",
        "spark.eventLog.dir": "/b",
        "spark.sql.shuffle.partitions": "16",
        "spark.hadoop.fs.s3a.secret.key": "y",
        "spark.hadoop.fs.azure.account.key.acct.dfs.core.windows.net": "k2",
        "spark.sql.adaptive.enabled": "false",
    }
    changes = {s.key: (s.before, s.after) for s in setting_changes(before, after)}
    assert changes == {
        "spark.executor.memory": ("4g", None),
        "spark.sql.adaptive.enabled": (None, "false"),
        "spark.sql.shuffle.partitions": ("200", "16"),
    }


# --- reports


def test_text_report(runs):
    text = render_diff(diff(runs, "base", "no_broadcast"), "text")
    assert "\x1b[" not in text
    assert "SLOWER  Query 0: 1.5x slower" in text
    assert "writes report/revenue" in text
    assert "Plan (- before, + after):" in text
    assert "spark.sql.autoBroadcastJoinThreshold: (default) → -1" in text
    assert "MED   PLAN004" in text
    assert text.rstrip().endswith("are ignored (profile: laptop).")
    assert all(len(line) <= 100 for line in text.splitlines())

    quiet = render_diff(diff(runs, "base", "rerun"), "text")
    assert "No query changed noticeably." in quiet
    coloured = render_diff(diff(runs, "base", "no_broadcast"), "text", color=True)
    assert "\x1b[" in coloured


def test_markdown_report(runs):
    md = render_diff(diff(runs, "base", "no_broadcast"), "md")
    assert md.startswith("## Spark Sentinel diff\n\n🔴 **1 query is slower.**")
    assert "| 🔴 slower | Query 0<br><sub>writes report/revenue</sub> |" in md
    assert "`spark.sql.autoBroadcastJoinThreshold`" in md
    assert "```diff" in md and "<details>" in md
    assert "- MED `PLAN004`" in md

    assert "✅ **No performance regression.**" in render_diff(diff(runs, "base", "rerun"), "md")
    cold = render_diff(diff(runs, "base", "cold"), "md")
    assert "🟡 **Slower, but unconfirmed**" in cold
    assert "| 🟡 slower |" in cold


def test_json_report(runs):
    data = json.loads(render_diff(diff(runs, "base", "python_udf"), "json"))
    assert data["schema_version"] == 1
    assert data["summary"]["slower"] == data["summary"]["confirmed_slower"] == 1
    assert data["summary"]["new_problems"] == 1
    slower = [q for q in data["queries"] if q["status"] == "slower"]
    assert slower[0]["target"] == "report/bands"
    assert slower[0]["reasons"][0] == "it now runs a row-by-row Python UDF"
    metrics = {c["metric"] for c in slower[0]["changes"]}
    assert {"duration_ms", "task_time_ms", "shuffle_bytes", "spill_bytes"} <= metrics
    assert data["new_problems"][0]["rule_id"] == "PLAN005"


# --- command line


def _diff_cli(*args: str, wh: Path):
    return CliRunner().invoke(app, ["diff", *args, "-w", str(wh)])


def test_cli_diff_from_paths(tmp_path):
    r = _diff_cli(
        str(DIFF / "base"),
        str(DIFF / "no_broadcast"),
        "--profile",
        "laptop",
        "--fail-on",
        "slower",
        wh=tmp_path,
    )
    assert r.exit_code == 1, r.output
    assert "the broadcast join became a sort-merge join" in r.output

    quiet = _diff_cli(
        str(DIFF / "base"),
        str(DIFF / "rerun"),
        "--profile",
        "laptop",
        "--fail-on",
        "slower,info",
        wh=tmp_path,
    )
    assert quiet.exit_code == 0, quiet.output


def test_cli_diff_app_ids_and_output_file(tmp_path):
    base = ingest(DIFF / "base", tmp_path)[0].app_id
    new = ingest(DIFF / "python_udf", tmp_path)[0].app_id
    out = tmp_path / "out" / "diff.md"
    r = _diff_cli(base, new, "--profile", "laptop", "-f", "md", "-o", str(out), wh=tmp_path)
    assert r.exit_code == 0, r.output
    assert "1 slower" in r.output and str(out) in r.output
    assert "row-by-row Python UDF" in out.read_text()


def test_cli_diff_no_problems_skips_the_rules(tmp_path):
    r = _diff_cli(
        str(DIFF / "base"),
        str(DIFF / "python_udf"),
        "--profile",
        "laptop",
        "--no-problems",
        "--fail-on",
        "info",
        wh=tmp_path,
    )
    assert r.exit_code == 0, r.output
    assert "New problems" not in r.output


def test_cli_diff_thresholds_from_option_and_config(tmp_path):
    base, new = str(DIFF / "base"), str(DIFF / "python_udf")
    loose = _diff_cli(
        base,
        new,
        "--profile",
        "laptop",
        "--threshold",
        "min_time_ms=60000",
        "--fail-on",
        "slower",
        wh=tmp_path,
    )
    assert loose.exit_code == 0, loose.output

    config = tmp_path / "sentinel.toml"
    config.write_text('profile = "laptop"\n\n[diff]\nmin_change_pct = 1000\n')
    from_file = _diff_cli(base, new, "--config", str(config), "--fail-on", "slower", wh=tmp_path)
    assert from_file.exit_code == 0, from_file.output
    assert "under 1000%" in from_file.output


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["--fail-on", "slow"], "--fail-on slow"),
        (["--threshold", "min_time=5"], "Unknown diff threshold"),
        (["--threshold", "min_time_ms"], "must look like name=value"),
        (["-f", "html"], "Unknown format 'html'"),
        (["--profile", "huge"], "Unknown profile"),
    ],
)
def test_cli_diff_bad_options_exit_2(tmp_path, args, message):
    r = _diff_cli(str(DIFF / "base"), str(DIFF / "rerun"), *args, wh=tmp_path)
    assert r.exit_code == 2, r.output
    assert message in r.output


def test_cli_diff_needs_one_app_per_side(tmp_path):
    several = _diff_cli(str(GALLERY), str(DIFF / "base"), wh=tmp_path)
    assert several.exit_code == 2
    assert "holds 11 apps" in several.output

    unknown = _diff_cli("local-nope", str(DIFF / "base"), wh=tmp_path)
    assert unknown.exit_code == 2
    assert "is not an event log, a folder, or an app ID" in unknown.output


def test_version_flag():
    for flag in ("--version", "-V"):
        r = CliRunner().invoke(app, [flag])
        assert r.exit_code == 0
        assert r.output.startswith("spark-sentinel ")
