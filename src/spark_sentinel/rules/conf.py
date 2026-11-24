"""CONF001: settings that switch off Spark's automatic optimizations."""

from __future__ import annotations

from collections.abc import Iterator

from spark_sentinel import docs_links
from spark_sentinel.analysis import AnalysisContext
from spark_sentinel.findings import Evidence, Finding, Severity, Unit
from spark_sentinel.rules import Rule
from spark_sentinel.rules._common import spark_version

# (setting, value that turns the optimization off, first Spark version where it's ON by default,
#  what is lost)
_CHECKS = [
    (
        "spark.sql.adaptive.enabled",
        "false",
        (3, 2),
        "Adaptive Query Execution: no automatic partition coalescing, skew-join handling or "
        "switching to broadcast joins at runtime",
    ),
    (
        "spark.sql.adaptive.coalescePartitions.enabled",
        "false",
        (3, 0),
        "AQE no longer merges small shuffle partitions",
    ),
    (
        "spark.sql.adaptive.skewJoin.enabled",
        "false",
        (3, 0),
        "AQE no longer splits skewed join partitions",
    ),
    (
        "spark.sql.autoBroadcastJoinThreshold",
        "-1",
        (0, 0),
        "small tables are never broadcast in joins",
    ),
    (
        "spark.sql.execution.pythonUDF.arrow.enabled",
        "false",
        (4, 2),
        "Python UDFs run row by row (pickled) instead of in Arrow batches",
    ),
]


class OptimizationsOff(Rule):
    id = "CONF001"
    name = "Automatic optimizations turned off"
    category = "config"
    description = (
        "The app explicitly switches off optimizations that Spark enables by default. That is "
        "sometimes deliberate (a workaround for a past problem), but it is easy to forget and "
        "costs performance on every run. Informational: settings alone don't prove a problem."
    )
    thresholds: dict = {}
    docs = (docs_links.AQE, docs_links.BROADCAST_JOINS, docs_links.ARROW_PYTHON_UDFS)

    def evaluate(self, ctx: AnalysisContext) -> Iterator[Finding]:
        version = spark_version(ctx)
        off = [
            (key, ctx.config[key], lost)
            for key, bad, default_on_since, lost in _CHECKS
            if key in ctx.config
            and ctx.config[key].strip().lower() == bad
            and version >= default_on_since
        ]
        if not off:
            return
        n = len(off)
        yield Finding(
            rule_id=self.id,
            severity=Severity.INFO,
            title=f"App: {n} automatic optimization{'s are' if n != 1 else ' is'} turned off",
            location=ctx.locate_app(),
            evidence=tuple(
                Evidence(key, f"{value} ({lost})", Unit.TEXT) for key, value, lost in off
            ),
            cause=(
                "These settings are explicitly set to values that disable optimizations Spark "
                "turns on by default."
            ),
            fix=(
                "Remove these settings unless they were added on purpose for a known problem; "
                "Spark's defaults turn the optimizations back on.",
                "If one was a workaround, note why next to it, so it can be revisited after "
                "upgrading Spark.",
            ),
            category=self.category,
            details={"settings": {key: value for key, value, _ in off}},
        )
