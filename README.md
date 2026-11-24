# Spark Sentinel

Find performance problems in Apache Spark jobs (skew, spill, small files, plan smells, bad partition counts) by reading the event logs Spark already writes. You don't have to change any Spark code, and you don't need a cluster to run the analysis.

> **Status:** pre-alpha. The project scaffold is in place; the reader, parser and rules are being built.

## Development setup

Requires Python 3.11+. Java 17/21 is only needed to run the PySpark jobs in `lab/`.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,lab]"

sentinel doctor        # check dependencies
pytest                 # run tests
ruff check . && ruff format --check .   # lint
```

## Layout

```
src/spark_sentinel/
  cli.py        sentinel command (Typer)
  reader.py     open plain / compressed / rolling event logs
  parser.py     events -> table rows (spec: docs/eventlog-fields.md)
  store.py      tables -> Parquet, queried with DuckDB
  findings.py   the Finding record every rule returns
  rules/        one module per rule
  report/       terminal, Markdown and HTML output
lab/jobs/       PySpark jobs that generate event logs
tests/          pytest suite
docs/           event-log field spec, architecture notes
```
