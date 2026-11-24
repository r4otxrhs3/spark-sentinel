"""Links to the official Spark and PySpark documentation that rules cite.

Every rule names the doc section its advice comes from, and reports show these links next to
the fixes. Anchors were checked against the Spark 4.3.0 docs ("latest" when written).
Paraphrase the docs in rule text and link to them; don't copy their text.
"""

from __future__ import annotations

from spark_sentinel.findings import Reference

_SQL = "https://spark.apache.org/docs/latest/sql-performance-tuning.html"
_TUNING = "https://spark.apache.org/docs/latest/tuning.html"
_ARROW = "https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html"
_PANDAS = (
    "https://spark.apache.org/docs/latest/api/python/tutorial/pandas_on_spark/best_practices.html"
)

# Spark SQL: Performance Tuning
TUNING_PARTITIONS = Reference("Tuning Partitions", f"{_SQL}#tuning-partitions")
COALESCE_HINTS = Reference("Coalesce Hints", f"{_SQL}#coalesce-hints")
BROADCAST_JOINS = Reference(
    "Automatically Broadcasting Joins", f"{_SQL}#automatically-broadcasting-joins"
)
JOIN_HINTS = Reference("Join Strategy Hints", f"{_SQL}#join-strategy-hints")
AQE = Reference("Adaptive Query Execution", f"{_SQL}#adaptive-query-execution")
AQE_COALESCE = Reference(
    "Coalescing Post Shuffle Partitions", f"{_SQL}#coalescing-post-shuffle-partitions"
)
AQE_SMJ_TO_BROADCAST = Reference(
    "Converting sort-merge join to broadcast join",
    f"{_SQL}#converting-sort-merge-join-to-broadcast-join",
)
AQE_SKEW_JOIN = Reference("Optimizing Skew Join", f"{_SQL}#optimizing-skew-join")

# Tuning Spark
MEMORY_OVERVIEW = Reference("Memory Management Overview", f"{_TUNING}#memory-management-overview")
GC_TUNING = Reference("Garbage Collection Tuning", f"{_TUNING}#garbage-collection-tuning")
PARALLELISM = Reference("Level of Parallelism", f"{_TUNING}#level-of-parallelism")
REDUCE_TASK_MEMORY = Reference(
    "Memory Usage of Reduce Tasks", f"{_TUNING}#memory-usage-of-reduce-tasks"
)

# PySpark
PANDAS_UDFS = Reference(
    "Pandas UDFs (a.k.a. Vectorized UDFs)", f"{_ARROW}#pandas-udfs-a-k-a-vectorized-udfs"
)
ARROW_PYTHON_UDFS = Reference("Arrow Python UDFs", f"{_ARROW}#arrow-python-udfs")
SINGLE_PARTITION = Reference(
    "Avoid computation on single partition", f"{_PANDAS}#avoid-computation-on-single-partition"
)
