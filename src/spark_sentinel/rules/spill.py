"""SPILL001: tasks ran out of execution memory and spilled to disk."""

from __future__ import annotations

from collections.abc import Iterator

from spark_sentinel.analysis import AnalysisContext
from spark_sentinel.findings import Evidence, Finding, Severity, Unit
from spark_sentinel.rules import Rule
from spark_sentinel.rules._common import GB, MB, ceil_div, round_partitions, size


class Spill(Rule):
    id = "SPILL001"
    name = "Spill to disk"
    category = "spill"
    description = (
        "Tasks didn't have enough execution memory for their sort, aggregation or join, so "
        "they wrote intermediate data to disk and read it back."
    )
    thresholds = {
        "min_disk_spill_bytes": 100 * MB,  # flag a stage from this much disk spill
        "medium_disk_spill_bytes": GB,  # MEDIUM from here
        "high_disk_spill_bytes": 10 * GB,  # HIGH from here
        "concentrated_fraction": 0.25,  # spill in <= this share of tasks -> probably skew
    }

    def evaluate(self, ctx: AnalysisContext) -> Iterator[Finding]:
        rows = ctx.rows(
            """
            SELECT stage_id, stage_attempt, count(*) AS n_tasks,
                   count(*) FILTER (WHERE disk_spill_bytes > 0) AS spilled_tasks,
                   sum(disk_spill_bytes) AS disk, sum(mem_spill_bytes) AS mem,
                   sum(coalesce(shuffle_read_bytes, 0) + coalesce(input_bytes, 0)) AS read_bytes,
                   median(coalesce(shuffle_read_bytes, 0) + coalesce(input_bytes, 0))
                       AS median_task_read,
                   list(task_id ORDER BY disk_spill_bytes DESC)[1:3] AS worst_tasks
            FROM tasks
            GROUP BY stage_id, stage_attempt
            HAVING sum(disk_spill_bytes) >= ?
            ORDER BY stage_id, stage_attempt
            """,
            [self.t("min_disk_spill_bytes")],
        )
        for row in rows:
            yield self._finding(ctx, row)

    def _finding(self, ctx: AnalysisContext, row: dict) -> Finding:
        disk, mem = row["disk"], row["mem"]
        if disk >= self.t("high_disk_spill_bytes"):
            severity = Severity.HIGH
        elif disk >= self.t("medium_disk_spill_bytes"):
            severity = Severity.MEDIUM
        else:
            severity = Severity.LOW

        per_task_mem = ctx.memory_per_task_bytes
        concentrated = row["spilled_tasks"] <= self.t("concentrated_fraction") * row["n_tasks"]
        if concentrated:
            cause = (
                f"Only {row['spilled_tasks']} of {row['n_tasks']} tasks spilled, so a few tasks "
                "got far more data than the rest: this is usually skew (see SKEW001)."
            )
        else:
            cause = (
                f"{row['spilled_tasks']} of {row['n_tasks']} tasks spilled: each task handled "
                f"about {size(row['median_task_read'])} of input, with roughly "
                f"{size(per_task_mem)} of execution memory per task."
            )

        fixes = []
        if concentrated:
            fixes.append("Fix the skew first: spreading the hot keys removes most of the spill.")
        # Aim for each task's input to be about half its memory: data expands in memory.
        target = max(per_task_mem // 2, 16 * MB)
        suggested = round_partitions(ceil_div(row["read_bytes"], target))
        if suggested > row["n_tasks"]:
            fixes.append(
                f"Use more partitions so each task holds less: about {suggested} instead of "
                f"{row['n_tasks']} (spark.sql.shuffle.partitions, or repartition() before "
                "the wide operation)."
            )
        memory_key = "spark.driver.memory" if ctx.is_local else "spark.executor.memory"
        fixes.append(
            f"Give each task more memory: raise {memory_key} (now "
            f"{size(ctx.executor_memory_bytes)}) or run fewer tasks per executor "
            f"(spark.executor.cores, now {ctx.executor_cores})."
        )
        return Finding(
            rule_id=self.id,
            severity=severity,
            title=f"Stage {row['stage_id']}: tasks spilled {size(disk)} to disk",
            location=ctx.locate_stage(
                row["stage_id"], row["stage_attempt"], task_ids=row["worst_tasks"]
            ),
            evidence=(
                Evidence("spilled to disk", disk, Unit.BYTES),
                Evidence("spilled from memory", mem, Unit.BYTES),
                Evidence(
                    "tasks that spilled", f"{row['spilled_tasks']} of {row['n_tasks']}", Unit.TEXT
                ),
                Evidence("execution memory per task (approx.)", per_task_mem, Unit.BYTES),
            ),
            cause=cause,
            fix=tuple(fixes),
            category=self.category,
            details={"suggested_partitions": suggested, "concentrated": concentrated},
        )
