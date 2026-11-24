"""SKEW001: one task in a stage takes far longer than its siblings."""

from __future__ import annotations

from collections.abc import Iterator

from spark_sentinel.analysis import AnalysisContext
from spark_sentinel.findings import Evidence, Finding, Severity, Unit
from spark_sentinel.rules import Rule
from spark_sentinel.rules._common import clean_expr, size


class TaskSkew(Rule):
    id = "SKEW001"
    name = "Task skew"
    category = "skew"
    description = (
        "The slowest task in a stage takes many times longer than the median task, so the "
        "whole stage waits for it. Tells data skew (the slow task read far more data) from "
        "compute skew (same data, more work)."
    )
    thresholds = {
        "min_ratio": 5.0,  # slowest task >= this x the median task
        "min_time_lost_ms": 30_000,  # and the stage waited at least this long for it
        "min_tasks": 4,  # stages with fewer tasks can't be compared meaningfully
        "data_skew_bytes_ratio": 3.0,  # slow task read >= this x median bytes -> data skew
        "high_ratio": 10.0,  # HIGH severity from this ratio ...
        "high_time_lost_ms": 600_000,  # ... or from this much time lost
        "gc_fraction": 0.3,  # GC >= this share of run time -> GC pressure
    }

    def evaluate(self, ctx: AnalysisContext) -> Iterator[Finding]:
        rows = ctx.rows(
            """
            WITH t AS (
                SELECT stage_id, stage_attempt, task_id, executor_id, duration_ms,
                       coalesce(run_time_ms, 0) AS run_ms, coalesce(gc_time_ms, 0) AS gc_ms,
                       coalesce(shuffle_read_bytes, 0) + coalesce(input_bytes, 0) AS read_bytes
                FROM tasks
                WHERE duration_ms IS NOT NULL AND NOT coalesce(failed, false)
            )
            SELECT stage_id, stage_attempt, count(*) AS n_tasks,
                   median(duration_ms)               AS median_ms,
                   max(duration_ms)                  AS max_ms,
                   arg_max(task_id, duration_ms)     AS slow_task,
                   arg_max(executor_id, duration_ms) AS slow_executor,
                   arg_max(read_bytes, duration_ms)  AS slow_bytes,
                   median(read_bytes)                AS median_bytes,
                   arg_max(gc_ms, duration_ms)       AS slow_gc_ms,
                   arg_max(run_ms, duration_ms)      AS slow_run_ms
            FROM t
            GROUP BY stage_id, stage_attempt
            HAVING count(*) >= ?
               AND max(duration_ms) >= ? * greatest(median(duration_ms), 1)
               AND max(duration_ms) - median(duration_ms) >= ?
            ORDER BY stage_id, stage_attempt
            """,
            [self.t("min_tasks"), self.t("min_ratio"), self.t("min_time_lost_ms")],
        )
        for row in rows:
            if self._is_data_skew(row) or not self._is_warm_up(ctx, row):
                yield self._finding(ctx, row)

    def _is_data_skew(self, row: dict) -> bool:
        median_bytes, slow_bytes = row["median_bytes"] or 0, row["slow_bytes"] or 0
        if not median_bytes:
            return slow_bytes > 0
        return slow_bytes / median_bytes >= self.t("data_skew_bytes_ratio")

    def _is_warm_up(self, ctx: AnalysisContext, row: dict) -> bool:
        """True if the slow task is just part of a slow first wave (JVM/JIT warm-up).

        The first `cores` tasks of a stage start together on a cold JVM and are often all
        several times slower than later waves. A real straggler stands out within its wave.
        """
        wave = ctx.total_cores
        if row["n_tasks"] <= wave:
            return False
        first_wave = ctx.rows(
            """
            SELECT task_id, duration_ms FROM tasks
            WHERE stage_id = ? AND stage_attempt = ?
            ORDER BY launch_time_ms, task_id
            LIMIT ?
            """,
            [row["stage_id"], row["stage_attempt"], wave],
        )
        if row["slow_task"] not in {t["task_id"] for t in first_wave}:
            return False
        durations = sorted(t["duration_ms"] for t in first_wave)
        peer_median = durations[len(durations) // 2]
        return row["max_ms"] < self.t("min_ratio") * max(peer_median, 1)

    def _finding(self, ctx: AnalysisContext, row: dict) -> Finding:
        ratio = row["max_ms"] / max(row["median_ms"], 1)
        lost = int(row["max_ms"] - row["median_ms"])
        median_bytes, slow_bytes = row["median_bytes"] or 0, row["slow_bytes"] or 0
        bytes_ratio = slow_bytes / median_bytes if median_bytes else (999.0 if slow_bytes else 1)
        data_skew = self._is_data_skew(row)
        gc_heavy = row["slow_run_ms"] and row["slow_gc_ms"] / row["slow_run_ms"] >= self.t(
            "gc_fraction"
        )

        if data_skew:
            cause = (
                f"Data skew: the slowest task read {size(slow_bytes)}, "
                f"{bytes_ratio:.0f}x the median task's {size(median_bytes)}. "
                "One key (or a few) holds a large share of the rows."
            )
            fix = self._data_skew_fixes(ctx, row["stage_id"])
        elif gc_heavy:
            cause = (
                f"Compute skew from GC pressure: the slowest task read a normal amount of data "
                f"but spent {row['slow_gc_ms'] / row['slow_run_ms']:.0%} of its run time in "
                "garbage collection."
            )
            fix = (
                "Give tasks more memory: raise spark.executor.memory or lower "
                "spark.executor.cores.",
                "Avoid building large in-memory collections per row (collect_list, big maps).",
            )
        else:
            cause = (
                f"Compute skew: the slowest task read about the same data as the others "
                f"({size(slow_bytes)} vs median {size(median_bytes)}) but did more work with it."
            )
            fix = (
                "Look for a UDF or expression that is much slower for some values "
                "(regexes, JSON parsing, nested loops).",
                f"Check executor {row['slow_executor']}: a slow or overloaded machine looks "
                "like this too.",
                "Turn on speculative execution (spark.speculation=true) to retry stragglers.",
            )

        severe = ratio >= self.t("high_ratio") or lost >= self.t("high_time_lost_ms")
        return Finding(
            rule_id=self.id,
            severity=Severity.HIGH if severe else Severity.MEDIUM,
            title=f"Stage {row['stage_id']}: slowest task is {ratio:.0f}x the median",
            location=ctx.locate_stage(
                row["stage_id"], row["stage_attempt"], task_ids=[row["slow_task"]]
            ),
            evidence=(
                Evidence("slowest task", row["max_ms"], Unit.MS, row["median_ms"], "median"),
                Evidence(
                    "its input + shuffle read", slow_bytes, Unit.BYTES, median_bytes, "median"
                ),
                Evidence("tasks in stage", row["n_tasks"]),
            ),
            cause=cause,
            fix=fix,
            impact_ms=lost,
            category=self.category,
            details={
                "ratio": round(ratio, 2),
                "bytes_ratio": round(bytes_ratio, 2),
                "kind": "data" if data_skew else ("gc" if gc_heavy else "compute"),
            },
        )

    def _data_skew_fixes(self, ctx: AnalysisContext, stage_id: int) -> tuple[str, ...]:
        fixes = []
        joins = [n for n in ctx.plan_for_stage(stage_id) if n.name == "SortMergeJoin"]
        aqe_on = ctx.conf_bool("spark.sql.adaptive.enabled")
        skew_join_on = aqe_on and ctx.conf_bool("spark.sql.adaptive.skewJoin.enabled")
        if joins and not skew_join_on:
            off = (
                "spark.sql.adaptive.enabled"
                if not aqe_on
                else "spark.sql.adaptive.skewJoin.enabled"
            )
            fixes.append(
                f"Turn on AQE skew-join handling: {off}=true (it is off for this app). "
                "Spark then splits the hot partition automatically."
            )
        elif joins:
            threshold = ctx.conf("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes")
            fixes.append(
                "AQE skew-join handling is on but didn't split this partition: lower "
                f"spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes (now {threshold})."
            )
        keys = sorted({clean_expr(n.get("Left keys", "")).strip("[]") for n in joins} - {""})
        if keys:
            key = keys[0].split(",")[0].strip()
            fixes.append(
                f"Find the hot key(s) of the join on {', '.join(keys)}: "
                f"df.groupBy('{key}').count().orderBy(F.desc('count')).show(10)"
            )
        if joins and (ctx.conf_bytes("spark.sql.autoBroadcastJoinThreshold") or 0) < 0:
            fixes.append(
                "If one side of the join is small, broadcast it (F.broadcast(df)); broadcast "
                "joins are disabled for this app (spark.sql.autoBroadcastJoinThreshold=-1)."
            )
        fixes.append(
            "Salt the hot key: add a random suffix 0..N-1 on the big side and "
            "replicate the matching rows N times on the other side."
        )
        return tuple(fixes)
