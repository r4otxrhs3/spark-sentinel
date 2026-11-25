# Changelog

## 0.1.0 (2026-11-24)

First release.

**Rules (13)**, each citing the Spark or PySpark doc section its advice follows:
SKEW001 task skew (data vs compute vs GC), SPILL001 spill to disk, GC001 garbage-collection
pressure, FILES001 small output files, PART001 / PART002 too many / too few shuffle partitions,
PAR001 fewer tasks than cores, PLAN001 Python UDF in a filter, PLAN002 cartesian or nested-loop
join, PLAN003 all data through one task, PLAN004 missed broadcast join, PLAN005 row-by-row Python
UDF, CONF001 automatic optimizations turned off.

**Commands:** `analyze` (text, Markdown, HTML and JSON reports; `--profile`, `--select`,
`--ignore`, `--threshold`, `--fail-on`; `sentinel.toml`), `ingest`, `sql`, `apps`, `rules`,
`inspect`, `parse`, `doctor`.

**Event logs:** plain, zstd and gzip; single files and rolled `eventlog_v2_*` folders; logs of
apps still running or that crashed; symlinked log folders. Spark 3.5 and 4.x.

**Testing:** a gallery of 11 Spark jobs (10 with a deliberate problem, 1 clean), with logs from
Spark 4.3 and 3.5. Every job must trigger exactly its rules and the clean job none, under both
threshold profiles.
