# Rule catalog: Spark's tuning advice → symptoms in the event log → rules

Spark's documentation lists *techniques*: what to do. Sentinel needs *symptoms*: evidence in an
event log that a technique was needed and not used. This catalog maps one to the other.
Every rule links to the doc sections its advice follows, and reports show those links next to
the fixes.

Sources (Spark 4.3.0 docs, "latest" when written):

- [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Tuning Spark](https://spark.apache.org/docs/latest/tuning.html)
- [Apache Arrow in PySpark](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html)
- [Pandas API on Spark: Best Practices](https://spark.apache.org/docs/latest/api/python/tutorial/pandas_on_spark/best_practices.html)

Status: ✅ shipped · 🔜 planned · ⛔ not detectable from an event log

## Spark SQL Performance Tuning

| Doc section | Symptom in the event log | Rule | Status |
|---|---|---|---|
| Tuning Partitions (`spark.sql.shuffle.partitions`) | Many tiny post-shuffle tasks / few huge ones | PART001 / PART002 | ✅ |
| Tuning Partitions (`files.maxPartitionBytes`, `openCostInBytes`) | Many tiny input files per task | FILES002 | 🔜 needs SQL metrics ("number of files read") |
| Coalesce Hints | Many tiny output files | FILES001 | ✅ |
| Leveraging Statistics · Automatically Broadcasting Joins · Converting sort-merge join to broadcast join | `SortMergeJoin` whose side was ≤ 10 MB at runtime (`Statistics(sizeInBytes=…)`) | PLAN004 | ✅ |
| Join Strategy Hints | Join with no equality key → `CartesianProduct` / `BroadcastNestedLoopJoin` | PLAN002 | ✅ |
| Adaptive Query Execution | AQE, coalescing, skew join or broadcasting explicitly switched off | CONF001 (info) | ✅ |
| Coalescing Post Shuffle Partitions | Tiny partitions with AQE coalescing off | PART001 (cause) | ✅ |
| Optimizing Skew Join | One task ≥ 5× the median (Spark's own `skewedPartitionFactor`) | SKEW001 | ✅ |
| Optimizing the Aggregate (adaptive partial aggregation) | Partial aggregate whose rows out ≈ rows in | PLAN006 | 🔜 needs SQL metrics |
| Caching Data | Same input scanned by several queries | CACHE001 | 🔜 |
| Storage Partition Join | Shuffles on co-partitioned V2 tables | — | ⛔ needs catalog metadata |

## Tuning Spark

| Doc section | Symptom in the event log | Rule | Status |
|---|---|---|---|
| Garbage Collection Tuning | GC ≥ 20% of task time in a stage; `-Xmn` in the JVM options | GC001 | ✅ |
| Level of Parallelism (2–3 tasks per core) | A long stage with fewer tasks than cores | PAR001 | ✅ |
| Memory Usage of Reduce Tasks | Spill to disk; tasks failing with out-of-memory | SPILL001 / FAIL001 | ✅ / 🔜 |
| Memory Management Overview (`spark.memory.fraction`) | Spill, GC (used in their fixes) | SPILL001, GC001 | ✅ |
| Data Locality | Many tasks at `RACK_LOCAL` / `ANY` | LOC001 | 🔜 needs cluster logs |
| Broadcasting Large Variables (tasks > ~20 KiB) | Serialized task size isn't logged; deserialize time is a weak proxy | — | ⛔ |
| Data Serialization (Kryo) | Only visible as a setting; matters for RDD jobs | — | ⛔ (setting only) |
| Tuning Data Structures, `SizeEstimator`, compressed oops | Leaves no trace | — | ⛔ |

## PySpark

| Doc section | Symptom in the event log | Rule | Status |
|---|---|---|---|
| Arrow Python UDFs · Pandas UDFs | `BatchEvalPython` (pickled, row by row) instead of `ArrowEvalPython` | PLAN005 | ✅ |
| (pushdown, via Check execution plans) | Python UDF in a filter; no `PushedFilters` on the scan | PLAN001 | ✅ |
| Avoid computation on single partition · default index | `Exchange SinglePartition` + one task reading lots of data; `Window` without `partitionBy` | PLAN003 | ✅ |
| Enabling for Conversion to/from Pandas | Large results to the driver with Arrow off | RESULT001 | 🔜 |
| Check execution plans | — | — | This is what Sentinel automates |

## Version notes that rules depend on

- **Arrow Python UDFs are on by default from Spark 4.2**, but fall back silently to pickled UDFs
  when pandas or PyArrow is missing (`pip install pyspark` doesn't install pandas). PLAN005 and
  CONF001 check the app's Spark version before deciding what "default" means.
- **AQE is on by default from Spark 3.2**; CONF001 only reports it as switched off on 3.2+.
- **Spark 3.5+ applies a "window group limit"** to `row_number() … <= N`, so that pattern doesn't
  funnel all rows through one task. PLAN003 judges the stage that actually ran, not the code.
