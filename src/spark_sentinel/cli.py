"""Command-line interface: `sentinel <command>`."""

from __future__ import annotations

import platform
import shutil
from collections import Counter
from importlib import metadata
from pathlib import Path
from typing import Annotated

import orjson
import typer

from spark_sentinel import __version__
from spark_sentinel.parser import SCHEMA, parse
from spark_sentinel.reader import EventLogError, open_log

app = typer.Typer(
    name="sentinel",
    help="Find performance problems in Spark jobs from their event logs.",
    no_args_is_help=True,
    add_completion=False,
)

CORE_DEPENDENCIES = ["typer", "orjson", "zstandard", "pyarrow", "duckdb", "jinja2"]
OPTIONAL_DEPENDENCIES = ["pyspark", "pytest", "ruff"]


def _installed_version(package: str) -> str | None:
    try:
        return metadata.version(package)
    except metadata.PackageNotFoundError:
        return None


@app.command()
def version() -> None:
    """Print the Spark Sentinel version."""
    typer.echo(f"spark-sentinel {__version__}")


@app.command()
def doctor() -> None:
    """Check that dependencies are installed and show their versions."""
    typer.echo(f"spark-sentinel  {__version__}")
    typer.echo(f"python          {platform.python_version()}")

    missing = []
    typer.echo("\ncore dependencies:")
    for package in CORE_DEPENDENCIES:
        found = _installed_version(package)
        typer.echo(f"  {package:<12} {found or 'MISSING'}")
        if found is None:
            missing.append(package)

    typer.echo("\noptional (lab jobs and development):")
    for package in OPTIONAL_DEPENDENCIES:
        typer.echo(f"  {package:<12} {_installed_version(package) or '-'}")
    typer.echo(f"  {'java':<12} {shutil.which('java') or '- (only needed to run lab jobs)'}")

    if missing:
        typer.echo(f"\nMissing core dependencies: {', '.join(missing)}", err=True)
        raise typer.Exit(code=1)
    typer.echo("\nAll core dependencies found.")


@app.command()
def inspect(
    path: Annotated[
        Path,
        typer.Argument(help="Event log file, eventlog_v2_* folder, or a folder with one log."),
    ],
    top: Annotated[int, typer.Option(help="Show only the N most common events (0 = all).")] = 0,
) -> None:
    """Show how an event log is stored and count its events by type."""
    try:
        log = open_log(path)
    except EventLogError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    counts: Counter[str] = Counter()
    bad_lines = 0
    try:
        for line in log.lines():
            try:
                counts[orjson.loads(line).get("Event", "<no Event field>")] += 1
            except orjson.JSONDecodeError:
                bad_lines += 1
    except EventLogError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    layout = f"rolled, {len(log.files)} part(s)" if log.rolling else "single file"
    typer.echo(f"app id       {log.app_id}")
    typer.echo(f"path         {log.path}")
    typer.echo(f"layout       {layout}")
    typer.echo(f"compression  {', '.join(sorted(set(log.codecs)))}")
    typer.echo(f"status       {'IN PROGRESS (or crashed)' if log.in_progress else 'complete'}")
    typer.echo(
        f"events       {sum(counts.values())}"
        + (f"  ({bad_lines} unparseable lines)" if bad_lines else "")
    )
    typer.echo("")
    for name, n in counts.most_common(top or None):
        typer.echo(f"{n:>8}  {name.removeprefix('org.apache.spark.sql.execution.ui.')}")


@app.command("parse")
def parse_command(
    path: Annotated[
        Path,
        typer.Argument(help="Event log file, eventlog_v2_* folder, or a folder with one log."),
    ],
    show: Annotated[
        str | None,
        typer.Option(help=f"Print rows of one table: {', '.join(SCHEMA)}."),
    ] = None,
    limit: Annotated[int, typer.Option(help="How many rows --show prints.")] = 5,
) -> None:
    """Parse an event log into tables and show row counts (or rows, with --show)."""
    if show is not None and show not in SCHEMA:
        typer.echo(f"error: unknown table '{show}'. Choose from: {', '.join(SCHEMA)}", err=True)
        raise typer.Exit(code=2)
    try:
        parsed = parse(open_log(path))
    except EventLogError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    if show is not None:
        rows = parsed[show]
        typer.echo(f"{show}: showing {min(limit, len(rows))} of {len(rows)} rows\n")
        for row in rows[:limit]:
            for key, value in row.items():
                text = str(value).replace("\n", " | ")
                typer.echo(f"  {key:<26} {text[:100] + '...' if len(text) > 100 else text}")
            typer.echo("")
        return

    app_row = parsed["app"][0]
    status = "complete" if app_row["completed"] else "INCOMPLETE (no ApplicationEnd event)"
    typer.echo(f"app id   {parsed.app_id}")
    typer.echo(f"spark    {app_row['spark_version']}   master {app_row['master']}   {status}")
    typer.echo("")
    for table in SCHEMA:
        typer.echo(f"  {table:<12} {len(parsed[table]):>8} rows")
    if parsed.unhandled_events:
        typer.echo("\nEvent types the parser doesn't handle yet (skipped):")
        for name, n in parsed.unhandled_events.most_common():
            typer.echo(f"  {n:>8}  {name}")
    if parsed.bad_lines:
        typer.echo(f"\nSkipped {parsed.bad_lines} line(s) that were not valid JSON.")
