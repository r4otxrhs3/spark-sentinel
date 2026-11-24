"""PAR001: a long stage ran fewer tasks than there were cores.
PLAN003: all of a query's data went through one task (single partition).

They describe the same symptom (idle cores) with different causes, so PAR001 leaves single-task
stages fed by a single-partition exchange to PLAN003, which can name the operation responsible.
"""

from __future__ import annotations

from collections.abc import Iterator

from spark_sentinel import docs_links
from spark_sentinel.analysis import AnalysisContext
from spark_sentinel.findings import Evidence, Finding, Severity, Unit
from spark_sentinel.plan import PlanNode
from spark_sentinel.rules import Rule
from spark_sentinel.rules._common import MB, clean_expr, single_partition_exchange, size


def _is_single_partition_stage(ctx: AnalysisContext, row: dict) -> bool:
    return (
        row["n_tasks"] == 1
        and (row["shuffle_read"] or 0) > 0
        and single_partition_exchange(ctx.plan_for_stage(row["stage_id"])) is not None
    )


_STAGES_SQL = """
    SELECT s.stage_id, s.stage_attempt, s.duration_ms, count(t.task_id) AS n_tasks,
           sum(t.shuffle_read_bytes) AS shuffle_read, sum(t.input_records) AS input_records,
           max(t.duration_ms) AS max_task_ms
    FROM stages s
    JOIN tasks t USING (stage_id, stage_attempt)
    GROUP BY s.stage_id, s.stage_attempt, s.duration_ms
    HAVING {condition}
    ORDER BY s.stage_id, s.stage_attempt
"""


class FewTasks(Rule):
    id = "PAR001"
    name = "Fewer tasks than cores"
    category = "parallelism"
    description = (
        "A long stage ran fewer tasks than the cores available, so some cores sat idle for "
        "the whole stage. The Tuning guide recommends 2-3 tasks per CPU core."
    )
    thresholds = {
        "min_stage_ms": 60_000,  # only stages at least this long are worth spreading out
        "tasks_per_core": 2,  # suggest this many tasks per core
        "medium_idle_fraction": 0.5,  # MEDIUM when at least this share of cores sat idle
    }
    docs = (docs_links.PARALLELISM, docs_links.TUNING_PARTITIONS)

    def evaluate(self, ctx: AnalysisContext) -> Iterator[Finding]:
        cores = ctx.total_cores
        rows = ctx.rows(
            _STAGES_SQL.format(condition="count(t.task_id) < ? AND s.duration_ms >= ?"),
            [cores, self.t("min_stage_ms")],
        )
        for row in rows:
            if _is_single_partition_stage(ctx, row):
                continue  # PLAN003 explains these
            idle = 1 - row["n_tasks"] / cores
            suggested = cores * self.t("tasks_per_core")
            source = (row["shuffle_read"] or 0) == 0
            fixes = (
                (
                    f"Split the input into more partitions: repartition({suggested}) before the "
                    "heavy step, or for files lower spark.sql.files.maxPartitionBytes."
                    if source
                    else f"Raise the number of shuffle partitions to about {suggested} "
                    "(spark.sql.shuffle.partitions, or repartition() before the heavy step)."
                ),
                f"Aim for {self.t('tasks_per_core')}-3 tasks per core: {cores} cores here.",
            )
            yield Finding(
                rule_id=self.id,
                severity=Severity.MEDIUM
                if idle >= self.t("medium_idle_fraction")
                else Severity.LOW,
                title=(
                    f"Stage {row['stage_id']}: {row['n_tasks']} task"
                    f"{'s' if row['n_tasks'] != 1 else ''} for {cores} cores"
                ),
                location=ctx.locate_stage(row["stage_id"], row["stage_attempt"]),
                evidence=(
                    Evidence("tasks", row["n_tasks"], Unit.COUNT, cores, "cores"),
                    Evidence("stage ran", row["duration_ms"], Unit.MS),
                    Evidence("cores idle for the whole stage", idle * 100, Unit.PERCENT),
                ),
                cause=(
                    f"The stage had only {row['n_tasks']} partition"
                    f"{'s' if row['n_tasks'] != 1 else ''}, so {cores - row['n_tasks']} of "
                    f"{cores} cores had nothing to do for {row['duration_ms'] / 1000:.0f}s."
                ),
                fix=fixes,
                impact_ms=int(row["duration_ms"] * idle),
                category=self.category,
                details={"idle_fraction": round(idle, 3), "suggested_tasks": suggested},
            )


def _window_without_partition(nodes: list[PlanNode]) -> PlanNode | None:
    """A Window whose spec starts with an ORDER BY term, i.e. has no PARTITION BY."""
    for node in nodes:
        if node.name == "Window":
            spec = node.get("Arguments", "").split("windowspecdefinition(", 1)
            if len(spec) == 2:
                first_term = spec[1].split(",", 1)[0]
                if " ASC" in first_term or " DESC" in first_term:
                    return node
    return None


class SinglePartition(Rule):
    id = "PLAN003"
    name = "All data through one task"
    category = "plan"
    description = (
        "The query moved all its rows into a single partition, so one task did the work of "
        "the whole cluster. Typical causes: a window function without partitionBy, "
        "repartition(1), or pandas-on-Spark operations such as rank() or the 'sequence' index."
    )
    thresholds = {
        "min_bytes": 512 * MB,  # the single task read at least this much shuffle data
    }
    docs = (docs_links.SINGLE_PARTITION, docs_links.PARALLELISM)

    def evaluate(self, ctx: AnalysisContext) -> Iterator[Finding]:
        rows = ctx.rows(
            _STAGES_SQL.format(condition="count(t.task_id) = 1 AND sum(t.shuffle_read_bytes) >= ?"),
            [self.t("min_bytes")],
        )
        for row in rows:
            nodes = ctx.plan_for_stage(row["stage_id"])
            exchange = single_partition_exchange(nodes)
            if exchange is None:
                continue
            window = _window_without_partition(nodes)
            location = ctx.locate_stage(
                row["stage_id"], row["stage_attempt"], plan_node=(window or exchange).label
            )
            if window is not None:
                expr = clean_expr(window.get("Arguments", "")).split(" windowspecdefinition")[0]
                cause = (
                    f"A window function ({expr.strip('[')}) has orderBy but no partitionBy, so "
                    "Spark sends every row to one partition to process them in order."
                )
                fixes = (
                    "Add partitionBy(...) to the window so each group is processed in parallel, "
                    "e.g. Window.partitionBy('country').orderBy('ts').",
                    "For a global ranking, rank within groups first and combine the small "
                    "results, or use approximate methods (percentile_approx).",
                    "In pandas API on Spark, prefer GroupBy.rank over DataFrame.rank, and set "
                    "compute.default_index_type to 'distributed' or 'distributed-sequence'.",
                )
            elif "REPARTITION_BY_NUM" in exchange.get("Arguments", ""):
                cause = "The code calls repartition(1), which moves every row into one partition."
                fixes = (
                    "Drop repartition(1); if the goal is one output file, coalesce(1) at the very "
                    "end only, or write normally and merge small files later.",
                )
            else:
                cause = (
                    "A global operation (such as a sort with limit, or a global aggregate of "
                    "un-aggregated rows) moved every row into one partition."
                )
                fixes = (
                    "Aggregate or filter before the global step, so only a little data reaches "
                    "the single partition.",
                    "In pandas API on Spark, avoid operations that need a global order (rank, "
                    "the 'sequence' default index).",
                )
            yield Finding(
                rule_id=self.id,
                severity=Severity.MEDIUM,
                title=(
                    f"Stage {row['stage_id']}: one task processed all "
                    f"{size(row['shuffle_read'])} of the query's shuffled data"
                ),
                location=location,
                evidence=(
                    Evidence("data read by the single task", row["shuffle_read"], Unit.BYTES),
                    Evidence("it ran", row["max_task_ms"], Unit.MS),
                    Evidence("cores that could have helped", ctx.total_cores - 1),
                ),
                cause=cause,
                fix=fixes,
                category=self.category,
                details={"window": window is not None, "exchange": exchange.label},
            )
