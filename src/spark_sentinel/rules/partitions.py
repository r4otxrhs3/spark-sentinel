"""PART001: too many shuffle partitions. PART002: too few."""

from __future__ import annotations

from collections.abc import Iterator

from spark_sentinel import docs_links
from spark_sentinel.analysis import AnalysisContext
from spark_sentinel.findings import Evidence, Finding, Severity, Unit
from spark_sentinel.rules import Rule
from spark_sentinel.rules._common import GB, MB, ceil_div, explicit_repartition, size

_STAGE_SHUFFLE_SQL = """
    SELECT stage_id, stage_attempt, count(*) AS n_tasks,
           sum(shuffle_read_bytes) AS total, median(shuffle_read_bytes) AS median_read,
           max(shuffle_read_bytes) AS max_read,
           count(*) FILTER (WHERE shuffle_read_bytes = 0) AS empty_tasks,
           median(duration_ms) AS median_ms
    FROM tasks
    GROUP BY stage_id, stage_attempt
    HAVING sum(shuffle_read_bytes) > 0 AND {condition}
    ORDER BY stage_id, stage_attempt
"""


class TooManyPartitions(Rule):
    id = "PART001"
    name = "Too many shuffle partitions"
    category = "partitions"
    description = (
        "A stage that reads shuffle data ran many tasks that each read very little. Each task "
        "has a fixed cost (scheduling, starting, writing results), so tiny tasks waste time."
    )
    thresholds = {
        "min_tasks": 200,  # a few small tasks are fine; only flag stages this wide
        "min_bytes_per_task": 8 * MB,  # median shuffle read per task below this = too small
        "target_bytes_per_task": 128 * MB,  # aim for this when suggesting a count
        "medium_tasks": 1000,  # MEDIUM from this many tasks ...
        "medium_bytes_per_task": MB,  # ... or when the median task reads less than this
    }
    docs = (docs_links.AQE_COALESCE, docs_links.TUNING_PARTITIONS)

    def evaluate(self, ctx: AnalysisContext) -> Iterator[Finding]:
        rows = ctx.rows(
            _STAGE_SHUFFLE_SQL.format(condition="count(*) >= ? AND median(shuffle_read_bytes) < ?"),
            [self.t("min_tasks"), self.t("min_bytes_per_task")],
        )
        for row in rows:
            yield self._finding(ctx, row)

    def _finding(self, ctx: AnalysisContext, row: dict) -> Finding:
        n, median = row["n_tasks"], row["median_read"]
        severe = n >= self.t("medium_tasks") or median < self.t("medium_bytes_per_task")
        # Enough partitions for the data, but never fewer than one per core (or than now).
        by_size = ceil_div(row["total"], self.t("target_bytes_per_task"))
        suggested = max(by_size, min(ctx.total_cores, n))
        repartition_n = explicit_repartition(ctx.plan_for_stage(row["stage_id"]))
        shuffle_partitions = ctx.conf("spark.sql.shuffle.partitions")
        coalescing = ctx.conf_bool("spark.sql.adaptive.enabled") and ctx.conf_bool(
            "spark.sql.adaptive.coalescePartitions.enabled"
        )

        if repartition_n == n:
            cause = f"The code calls repartition({n}) for only {size(row['total'])} of data."
            fixes = [f"Use repartition({suggested}) instead, or drop the repartition()."]
        elif str(n) == shuffle_partitions and not ctx.conf_is_default(
            "spark.sql.shuffle.partitions"
        ):
            cause = (
                f"spark.sql.shuffle.partitions={n} is set explicitly, and this stage only "
                f"shuffled {size(row['total'])}."
            )
            fixes = [f"Lower spark.sql.shuffle.partitions to about {suggested} for this job."]
        else:
            cause = f"{n} partitions for only {size(row['total'])} of shuffled data."
            fixes = [f"Use about {suggested} partitions for this data volume."]
        if not coalescing:
            fixes.append(
                "Turn on AQE partition coalescing (spark.sql.adaptive.enabled and "
                "spark.sql.adaptive.coalescePartitions.enabled): Spark then merges small "
                "partitions automatically. It is off for this app."
            )
        # Tiny tasks are mostly overhead: estimate the time the extra ones cost.
        extra = max(n - suggested, 0)
        overhead_ms = int(extra * row["median_ms"] / max(ctx.total_cores, 1))

        return Finding(
            rule_id=self.id,
            severity=Severity.MEDIUM if severe else Severity.LOW,
            title=f"Stage {row['stage_id']}: {n} tasks each read only {size(median)} of shuffle",
            location=ctx.locate_stage(row["stage_id"], row["stage_attempt"]),
            evidence=(
                Evidence("tasks", n, Unit.COUNT, suggested, "suggested"),
                Evidence(
                    "median shuffle read per task",
                    median,
                    Unit.BYTES,
                    self.t("target_bytes_per_task"),
                    "target",
                ),
                Evidence("empty tasks (read nothing)", row["empty_tasks"]),
                Evidence("total shuffled", row["total"], Unit.BYTES),
            ),
            cause=cause,
            fix=tuple(fixes),
            impact_ms=overhead_ms or None,
            category=self.category,
            details={"suggested_partitions": suggested, "repartition": repartition_n},
        )


class TooFewPartitions(Rule):
    id = "PART002"
    name = "Too few shuffle partitions"
    category = "partitions"
    description = (
        "Each task of a stage reads a very large amount of shuffle data. Tasks this big run "
        "long, spill, and leave cores idle; more partitions spread the work."
    )
    thresholds = {
        "max_bytes_per_task": GB,  # median shuffle read per task above this = too big
        "target_bytes_per_task": 128 * MB,
    }
    docs = (docs_links.TUNING_PARTITIONS, docs_links.PARALLELISM)

    def evaluate(self, ctx: AnalysisContext) -> Iterator[Finding]:
        rows = ctx.rows(
            _STAGE_SHUFFLE_SQL.format(condition="median(shuffle_read_bytes) > ?"),
            [self.t("max_bytes_per_task")],
        )
        for row in rows:
            suggested = ceil_div(row["total"], self.t("target_bytes_per_task"))
            yield Finding(
                rule_id=self.id,
                severity=Severity.MEDIUM,
                title=(
                    f"Stage {row['stage_id']}: {row['n_tasks']} tasks each read "
                    f"{size(row['median_read'])} of shuffle"
                ),
                location=ctx.locate_stage(row["stage_id"], row["stage_attempt"]),
                evidence=(
                    Evidence("tasks", row["n_tasks"], Unit.COUNT, suggested, "suggested"),
                    Evidence(
                        "median shuffle read per task",
                        row["median_read"],
                        Unit.BYTES,
                        self.t("target_bytes_per_task"),
                        "target",
                    ),
                    Evidence("total shuffled", row["total"], Unit.BYTES),
                ),
                cause=(f"{size(row['total'])} was shuffled into only {row['n_tasks']} partitions."),
                fix=(
                    f"Raise spark.sql.shuffle.partitions (now "
                    f"{ctx.conf('spark.sql.shuffle.partitions')}) to about {suggested}, or "
                    "repartition() before the wide operation.",
                    "With AQE on, set spark.sql.adaptive.coalescePartitions.initialPartitionNum "
                    "high and let Spark merge partitions down.",
                ),
                category=self.category,
                details={"suggested_partitions": suggested},
            )
