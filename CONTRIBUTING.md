# Contributing to Spark Sentinel

Thanks for helping. The most valuable contribution is a new rule: one more Spark performance
problem that Sentinel can find and explain. This guide covers setup, the layout, how to add a
rule, and how releases work.

## Setup

```bash
git clone https://github.com/r4otxrhs3/spark-sentinel.git
cd spark-sentinel
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,lab]"       # lab = PySpark, only needed to run the gallery jobs

pytest                            # the whole suite runs in ~30 s, no Spark needed
ruff check . && ruff format --check .
```

Running the gallery jobs (to create new event logs) needs Java 17 or 21. On Windows, use WSL.

## Layout

```
src/spark_sentinel/
  reader.py       find logs, decompress (plain, zstd, gzip), stream lines; rolled logs
  parser.py       events -> tables (spec: docs/eventlog-fields.md)
  store.py        tables -> Parquet warehouse, queried with DuckDB
  plan.py         read formatted physical plans: final-plan nodes, tree, runtime sizes
  analysis.py     runs rules; AnalysisContext gives rules SQL access, config, locations
  findings.py     the Finding every rule returns
  rules/          one module per family of rules
  docs_links.py   links to the Spark docs sections rules cite
  report/         text, Markdown, HTML and JSON reports
  diff/           sentinel diff: plan normalization, query matching, comparison
  profiles.py     threshold profiles (default, laptop)
action.yml        the GitHub Action; its logic is action/run_action.py
lab/gallery/      PySpark jobs that cause each problem on purpose, plus a clean job
lab/diff/         a report job and one-line changes to it, for testing sentinel diff
tests/            pytest; tests/fixtures/eventlogs holds the gallery's logs (Spark 4.3, 3.5)
docs/             event-log field spec, rule catalog, images
scripts/          maintenance scripts (the demo GIF)
```

## Adding a rule

A rule is only as good as its false-alarm rate, so every rule goes through the same loop. Look
at an existing rule of the same kind first (`rules/spill.py` for metric rules,
`rules/joins.py` for plan rules).

**1. Reproduce the problem.** Write `lab/gallery/<problem>.py`, a small PySpark job that causes
it on purpose (copy an existing one; `_spark.start()` sets up event logging). Add it to `JOBS`
in `lab/gallery/run_all.py` and run it:

```bash
SENTINEL_DEFAULT_EVENTLOG=1 python lab/gallery/run_all.py <problem>
```

Check the signal is clear (and not merely a side effect of a laptop run). Spark can surprise
you: it may optimize the problem away, or the first wave of tasks may be slow from JVM warm-up.

**2. Find the signal with SQL before writing code.**

```bash
sentinel ingest lab/eventlogs/gallery/<problem>
sentinel sql "SELECT stage_id, count(*), median(duration_ms), max(duration_ms) FROM tasks GROUP BY 1"
sentinel parse lab/eventlogs/gallery/<problem> --show sql      # the physical plans
```

**3. Write the rule** in `src/spark_sentinel/rules/` (new module or an existing one):

```python
class MyRule(Rule):
    id = "AREA001"  # letters + 3 digits, unique
    name = "Short name"
    category = "plan"
    description = "One or two sentences: what's wrong and why it costs time."
    thresholds = {"min_bytes": 512 * MB}  # every number you compare against
    docs = (docs_links.SOME_SECTION,)  # the Spark docs the advice follows

    def evaluate(self, ctx: AnalysisContext) -> Iterator[Finding]:
        for row in ctx.rows("SELECT ... HAVING x >= ?", [self.t("min_bytes")]):
            yield Finding(
                rule_id=self.id,
                severity=Severity.MEDIUM,
                title=f"Stage {row['stage_id']}: ...",
                location=ctx.locate_stage(row["stage_id"], row["stage_attempt"]),
                evidence=(Evidence("...", row["x"], Unit.BYTES, target, "target"),),
                cause="What happened, in one or two sentences.",
                fix=("The most effective fix first, as a full sentence.",),
            )
```

Defining the class registers it. Things the tests will hold you to:

- **Titles** start with `Stage N:`, `Query N:` or `App:`. **Causes and fixes** are full
  sentences. **Evidence** never shows `None`, `nan` or `inf`.
- **Ratios in SQL** need `NULLIF(x, 0)`: in DuckDB `x / 0` is `inf`, not `NULL`.
- **Plan rules** read `ctx.plans` (the final plan after AQE), never the raw text: a plan lists
  every node twice.
- **Settings**: use `ctx.conf(key)`, which fills in Spark's defaults. A missing key means
  "default", not "off".
- **Cite the docs**: add the section to `docs_links.py` if it isn't there, with its anchor.

**4. Tune it on the whole gallery**, not just your job: it must fire on its job and stay silent
on every other one, especially `clean`. If it needs lower thresholds on laptop-sized data, add
them to the `laptop` profile in `profiles.py`.

**5. Add the fixtures and tests.**

- Copy the job's log into `tests/fixtures/eventlogs/gallery/<problem>/` (Spark 4.x) and, from a
  Spark 3.5 run, into `gallery-spark35/` (compress it with zstd; see the existing ones).
- Add a column for your rule to the table in `lab/gallery/README.md`: `✅` fires with default
  thresholds, `✅*` only with the laptop profile, `x2` if it fires on two stages.
  `tests/test_matrix.py` reads that table, so the matrix, quality-contract and false-alarm
  tests now cover your rule.
- Add boundary tests (fires exactly at the threshold, not one below) and tests for each cause
  your rule diagnoses, using the synthetic `AppBuilder` in `tests/conftest.py`.
- Add the rule to `docs/rule-catalog.md` and the table in `README.md`.

**6. Check that your tests can fail.** Break your rule on purpose (flip a `>=`, drop a condition)
and make sure a test goes red.

## Style

- `ruff check . && ruff format .` before committing; CI runs both.
- Write user-facing text (titles, causes, fixes, docs) in plain language: say what happened and
  what to do, with the numbers.

## Releasing (maintainers)

One-time setup, so GitHub can publish without any stored password or token:

1. On [pypi.org](https://pypi.org), go to *Your account → Publishing → Add a new pending
   publisher* and enter: project `spark-sentinel`, owner `r4otxrhs3`, repository
   `spark-sentinel`, workflow `release.yml`, environment `pypi`.
2. On GitHub, create an environment called `pypi` (*Settings → Environments*).

Each release:

1. Update `__version__` in `src/spark_sentinel/__init__.py` and add a section to
   `CHANGELOG.md`.
2. Commit, then tag and push: `git tag v0.1.0 && git push origin main v0.1.0`.
3. The `Release` workflow checks that the tag matches the version, runs the tests, builds,
   checks the package, and publishes to PyPI.

To regenerate the demo GIF after changing the report format: `pip install pillow` then
`python scripts/make_demo_gif.py`.
