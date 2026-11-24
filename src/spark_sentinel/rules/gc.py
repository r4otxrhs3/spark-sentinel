"""GC001: tasks spend a large share of their time in JVM garbage collection."""

from __future__ import annotations

import re
from collections.abc import Iterator

from spark_sentinel import docs_links
from spark_sentinel.analysis import AnalysisContext
from spark_sentinel.findings import Evidence, Finding, Severity, Unit
from spark_sentinel.rules import Rule

_XMN = re.compile(r"-Xmn(\S+)")


class GcPressure(Rule):
    id = "GC001"
    name = "Garbage collection pressure"
    category = "memory"
    description = (
        "Tasks in a stage spend a large share of their run time in JVM garbage collection: "
        "the JVM keeps pausing to free memory instead of running the job."
    )
    thresholds = {
        "min_gc_fraction": 0.2,  # GC time / task run time in the stage (Spark's UI warns at 10%)
        "min_gc_ms": 60_000,  # and at least this much GC time in the stage, summed over tasks
        "high_gc_fraction": 0.4,  # HIGH from here
    }
    docs = (docs_links.GC_TUNING, docs_links.MEMORY_OVERVIEW)

    def evaluate(self, ctx: AnalysisContext) -> Iterator[Finding]:
        rows = ctx.rows(
            """
            SELECT t.stage_id, t.stage_attempt, count(*) AS n_tasks,
                   sum(t.gc_time_ms) AS gc_ms, sum(t.run_time_ms) AS run_ms,
                   sum(t.gc_time_ms) / NULLIF(sum(t.run_time_ms), 0) AS fraction,
                   any_value(s.duration_ms) AS stage_ms,
                   list(t.task_id ORDER BY t.gc_time_ms DESC)[1:3] AS worst_tasks
            FROM tasks t
            LEFT JOIN stages s USING (stage_id, stage_attempt)
            GROUP BY t.stage_id, t.stage_attempt
            HAVING sum(t.gc_time_ms) >= ? AND fraction >= ?
            ORDER BY t.stage_id, t.stage_attempt
            """,
            [self.t("min_gc_ms"), self.t("min_gc_fraction")],
        )
        for row in rows:
            yield self._finding(ctx, row)

    def _finding(self, ctx: AnalysisContext, row: dict) -> Finding:
        fraction = row["fraction"]
        java_options_key = (
            "spark.driver.extraJavaOptions" if ctx.is_local else "spark.executor.extraJavaOptions"
        )
        young_gen = _XMN.search(ctx.config.get(java_options_key, ""))
        memory_key = "spark.driver.memory" if ctx.is_local else "spark.executor.memory"

        if young_gen:
            cause = (
                f"The young generation is pinned to {young_gen.group(1)} by "
                f"-Xmn{young_gen.group(1)} in {java_options_key}, so the JVM runs a minor "
                "collection after every few MB of new objects."
            )
            fixes = (
                f"Remove -Xmn{young_gen.group(1)} from {java_options_key} and let the G1 "
                "collector size the young generation itself (Spark's default since 4.0).",
                "If you must size it by hand, the Tuning guide suggests a young generation of "
                "about 4/3 of the Eden space the tasks need.",
            )
        else:
            cause = (
                f"Tasks create and discard objects faster than {ctx.executor_cores} tasks per "
                f"executor can fit in {ctx.executor_memory_bytes / 1024**3:.1f} GB of heap."
            )
            fixes = (
                f"Give each task more memory: raise {memory_key} or lower spark.executor.cores "
                f"(now {ctx.executor_cores}) so fewer tasks share one heap.",
                "Avoid building big per-row or per-group collections (collect_list, collect_set, "
                "large maps); aggregate with built-in functions instead.",
                "If cached data fills the heap, cache less or lower spark.memory.fraction, as the "
                "Tuning guide suggests when the old generation is nearly full.",
            )
        severe = fraction >= self.t("high_gc_fraction")
        return Finding(
            rule_id=self.id,
            severity=Severity.HIGH if severe else Severity.MEDIUM,
            title=f"Stage {row['stage_id']}: {fraction:.0%} of task time was garbage collection",
            location=ctx.locate_stage(
                row["stage_id"], row["stage_attempt"], task_ids=row["worst_tasks"]
            ),
            evidence=(
                Evidence("time in GC", row["gc_ms"], Unit.MS, row["run_ms"], "total task time"),
                Evidence("share of task time", fraction * 100, Unit.PERCENT),
                Evidence("tasks in stage", row["n_tasks"]),
            ),
            cause=cause,
            fix=fixes,
            impact_ms=int((row["stage_ms"] or 0) * fraction) or None,
            category=self.category,
            details={"gc_fraction": round(fraction, 3), "young_gen": bool(young_gen)},
        )
