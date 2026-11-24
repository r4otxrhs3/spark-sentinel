# Gallery of bad jobs

Small PySpark jobs that each cause one known performance problem on purpose, plus one clean job. Their event logs are Sentinel's test data, demo material and blog examples.

## Run

```bash
pip install -e ".[lab]"                   # pyspark; Java 17/21 must be installed
python lab/gallery/run_all.py             # makes lab/data on first run, then runs all 7 jobs (~3 min)
python lab/gallery/summarize.py           # prints each log's symptom numbers
```

- `python lab/gallery/run_all.py skew spill` reruns only those jobs.
- `--remake-data` rebuilds the input data.
- Each log goes to `lab/eventlogs/gallery/<job>/`, which is cleared first, so it always holds exactly one log: the latest run.
- `SENTINEL_DEFAULT_EVENTLOG=1 python lab/gallery/run_all.py` writes logs in Spark's default format instead (zstd-compressed and rolled into an `eventlog_v2_*` folder). Use this to test the reader.

## Jobs and what they should trigger

This table is the spec for the tests: `tests/test_matrix.py` parses it, so every ✅ must fire and every blank cell must not, on logs from Spark 4.3 and 3.5 (`tests/fixtures/eventlogs/gallery*`), with both the `laptop` and `default` threshold profiles. Change the table and the tests change with it.

| Job | SKEW001 | SPILL001 | FILES001 | PLAN001 | PLAN002 | PART001 | How the problem is caused |
|---|:-:|:-:|:-:|:-:|:-:|:-:|---|
| `clean` | | | | | | | Built-in filter (pushed down), even keys, AQE on |
| `skew` | ✅* | | | | | | 70% of rows share one join key; broadcast join, AQE skew-join and coalescing disabled |
| `spill` | | ✅ | | | | | Sorts 6M wide rows in 4 partitions with 1 GB of memory |
| `udf_filter` | | | | ✅ | | | Same filter as `clean`, but written as a Python UDF, so it can't be pushed down |
| `small_files` | | | ✅ | | | ✅ | Reads 1,000 tiny files; `repartition(500)` before the write makes 500 tiny output files |
| `over_partitioned` | | | | | | ✅ | 1,000 shuffle partitions for ~5 MB of shuffle data; AQE coalescing disabled |
| `cross_join` | | | | | ✅ | | Join on `a < b - 80` (no equality), broadcast disabled → `CartesianProduct` |

\* SKEW001 needs a laptop threshold (`min_time_lost_ms=1000`): its production default is 30 s lost, and the hot task here costs ~2 s. Every other cell fires with the default thresholds. PART002 (too few partitions) has no gallery job; synthetic tests cover it.

`small_files` also triggers the partitions rule, and that's correct: `repartition(500)` on a few MB *is* over-partitioning, and it's the cause of the small output files. Real jobs often have several linked problems.

## Reference numbers (`summarize.py`, Spark 4.3, local[4])

Timings vary by machine; byte counts and task counts should be close to these.

| Job | Signal |
|---|---|
| `clean` | 6 tasks · no spill · `PushedFilters=[IsNotNull(country), EqualTo(country,IN)]` |
| `skew` | join stage: slowest task 2.8 s vs median 0.4 s (**~7×**); shuffle read 70 MB vs median 2.3 MB (**~31×**) → **data** skew |
| `spill` | **~250 MB** spilled to disk, ~300 MB from memory |
| `udf_filter` | no `PushedFilters` line in the scan (nothing pushed) · `BatchEvalPython` in the plan |
| `small_files` | **500** write tasks, median **7 KB** written per task |
| `over_partitioned` | **1,000** post-shuffle tasks, median **5 KB** read per task (target ~128 MB) |
| `cross_join` | `CartesianProduct` in the final plan, condition `(a_val < (b_val - 80))` |

## Lessons for tuning the rules (Step 8)

1. **The plan's skew threshold (> 30 s) never fires on laptop data.** Make every threshold configurable, and use lower ones (e.g. 1 s) in tests.
2. **A ratio alone gives false alarms.** `over_partitioned` shows a 5× max/median ratio, but the slowest task took only 131 ms vs 26 ms, which is noise. Require the ratio **and** a minimum time lost (max − median).
3. **Check bytes too, to name the cause.** In `skew` the slow task also read 31× the median bytes, so it's data skew. A slow task with normal bytes would be compute skew (a heavy UDF or GC pressure).
4. **The partitions rule needs a minimum task count.** `skew` has 16 tasks of about 2 MB each, far below 128 MB, but 16 tasks isn't a problem. Only flag "too many partitions" above a task-count floor (e.g. > 200).
5. **AQE hides problems on small data.** With coalescing on, it merged `skew`'s 16 join partitions into 3, and the hot task's 31× shuffle read looked like only 2.7×. That's why `skew` and `over_partitioned` turn coalescing off. On real clusters, check `spark.sql.adaptive.*` in the config table before blaming the code.
6. **`coalesce(n)` reaches upstream.** A `coalesce(1)` before a write collapses the whole previous stage into one task, unlike `repartition(1)`, which adds a shuffle.
7. **"Nothing pushed down" is a missing line, not an empty list.** In the formatted physical plan,
   a scan with no pushed filters simply has no `PushedFilters:` line. Test for its absence.
8. **The first wave of tasks is slow on a cold JVM.** In `cross_join` the first 4 tasks (one per
   core) took ~1.5 s and all later ones ~0.15 s: that looks like 8x skew but is warm-up. A
   real straggler stands out *within its wave*; SKEW001 checks this when bytes don't explain it.
9. **Bytes beat time for diagnosing skew.** In `skew` the warm-up wave (2.1 s) is nearly as slow
   as the hot task (2.6 s), but the hot task read 31x the median bytes. Data skew is unambiguous.
10. **Parquet input byte counts can be wrong.** Tasks reading 500,000 rows reported ~12 KB of
    input from a 45 MB dataset. Use `input_records` for evidence; they are exact.
11. **A plan with AQE lists every node twice** (final and initial plan, different ids). Rules
    read only the final plan's nodes, or they report each problem twice.
12. **Rolled logs contain non-log files.** An `eventlog_v2_*` folder also holds `appstatus_*` and hidden `.crc` files. The reader must read only `events_<N>_*` parts, in order of N.
