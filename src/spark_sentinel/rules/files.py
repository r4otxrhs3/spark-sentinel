"""FILES001: a write produced many small output files."""

from __future__ import annotations

from collections.abc import Iterator

from spark_sentinel.analysis import AnalysisContext
from spark_sentinel.findings import Evidence, Finding, Severity, Unit
from spark_sentinel.rules import Rule
from spark_sentinel.rules._common import MB, ceil_div, explicit_repartition, size


class SmallOutputFiles(Rule):
    id = "FILES001"
    name = "Small output files"
    category = "io"
    description = (
        "A write produced many small files. Every later read pays a per-file cost (listing, "
        "opening, footers), and object stores like S3 are slow with many small objects. "
        "Counts tasks that wrote output: partitioned writes can create even more files."
    )
    thresholds = {
        "min_files": 100,  # only writes with at least this many writing tasks
        "max_median_file_bytes": 32 * MB,  # median bytes per writing task below this = small
        "target_file_bytes": 128 * MB,  # size to aim for when suggesting a file count
        "medium_files": 1000,  # MEDIUM from this many files ...
        "medium_median_file_bytes": MB,  # ... or when the median file is smaller than this
        "high_files": 10_000,  # HIGH from this many files
    }

    def evaluate(self, ctx: AnalysisContext) -> Iterator[Finding]:
        rows = ctx.rows(
            """
            SELECT stage_id, stage_attempt,
                   count(*) FILTER (WHERE output_records > 0) AS writers,
                   sum(output_bytes) AS total_bytes,
                   sum(output_records) AS total_records,
                   median(output_bytes) FILTER (WHERE output_records > 0) AS median_bytes
            FROM tasks
            GROUP BY stage_id, stage_attempt
            HAVING count(*) FILTER (WHERE output_records > 0) >= ?
               AND median(output_bytes) FILTER (WHERE output_records > 0) < ?
            ORDER BY stage_id, stage_attempt
            """,
            [self.t("min_files"), self.t("max_median_file_bytes")],
        )
        for row in rows:
            yield self._finding(ctx, row)

    def _finding(self, ctx: AnalysisContext, row: dict) -> Finding:
        writers, median = row["writers"], row["median_bytes"]
        if writers >= self.t("high_files"):
            severity = Severity.HIGH
        elif writers >= self.t("medium_files") or median < self.t("medium_median_file_bytes"):
            severity = Severity.MEDIUM
        else:
            severity = Severity.LOW

        suggested = ceil_div(row["total_bytes"], self.t("target_file_bytes"))
        repartition_n = explicit_repartition(ctx.plan_for_stage(row["stage_id"]))
        if repartition_n == writers:
            cause = (
                f"The code calls repartition({repartition_n}) right before writing, so Spark "
                f"wrote {repartition_n} files for only {size(row['total_bytes'])} of data."
            )
            fix = (
                f"Use repartition({suggested}) or coalesce({suggested}) instead of "
                f"repartition({repartition_n}): about {size(self.t('target_file_bytes'))} "
                "per file.",
            )
        else:
            cause = (
                f"{writers} tasks each wrote a small slice of the {size(row['total_bytes'])} "
                "output: the write ran with far more partitions than the data needs."
            )
            fix = (
                f"Reduce partitions before writing: df.coalesce({suggested}) for about "
                f"{size(self.t('target_file_bytes'))} per file.",
                "For partitioned tables, repartition by the partition columns first, so each "
                "table partition is written by one task.",
            )
        fix += ("Compact existing small files with a periodic rewrite job.",)

        return Finding(
            rule_id=self.id,
            severity=severity,
            title=f"Stage {row['stage_id']}: wrote {writers} files of {size(median)} (median)",
            location=ctx.locate_stage(row["stage_id"], row["stage_attempt"]),
            evidence=(
                Evidence("files written (one per writing task)", writers),
                Evidence(
                    "median file size", median, Unit.BYTES, self.t("target_file_bytes"), "target"
                ),
                Evidence("total written", row["total_bytes"], Unit.BYTES),
            ),
            cause=cause,
            fix=fix,
            category=self.category,
            details={"suggested_files": suggested, "repartition": repartition_n},
        )
