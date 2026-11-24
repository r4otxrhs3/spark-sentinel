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
from spark_sentinel.store import DEFAULT_WAREHOUSE, StoreError, Warehouse, ingest

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


WarehouseOption = Annotated[
    Path, typer.Option("--warehouse", "-w", help="Folder holding ingested apps.")
]


@app.command("ingest")
def ingest_command(
    path: Annotated[
        Path, typer.Argument(help="An event log, or a folder: every log inside is ingested.")
    ],
    warehouse: WarehouseOption = DEFAULT_WAREHOUSE,
    force: Annotated[bool, typer.Option(help="Re-ingest logs that haven't changed.")] = False,
) -> None:
    """Parse event logs once and store their tables as Parquet."""
    try:
        results = ingest(path, warehouse, force=force)
    except (EventLogError, StoreError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    rows = [
        (
            r.status,
            r.app_id,
            r.rows["tasks"],
            r.rows["stages"],
            r.rows["sql"],
            "yes" if r.completed else "NO",
            ", ".join(r.unhandled_events) or "-",
        )
        for r in results
    ]
    _print_table(["status", "app_id", "tasks", "stages", "sql", "complete", "unhandled"], rows)
    typer.echo(f"\nWarehouse: {Path(warehouse).resolve()}")


@app.command("apps")
def apps_command(warehouse: WarehouseOption = DEFAULT_WAREHOUSE) -> None:
    """List the apps in the warehouse."""
    wh = Warehouse(warehouse)
    manifests = wh.manifests()
    if not manifests:
        typer.echo(f"No apps in {wh.root}. Run: sentinel ingest <event log>")
        return
    _columns, rows = wh.query(
        "SELECT app_id, app_name, spark_version, master, round(duration_ms / 1000, 1) "
        "FROM app ORDER BY app_id"
    )
    tasks = {m["app_id"]: m["rows"]["tasks"] for m in manifests}
    _print_table(
        ["app_id", "name", "spark", "master", "seconds", "tasks"],
        [(*row, tasks.get(row[0])) for row in rows],
    )
    stale = wh.stale_apps()
    if stale:
        typer.echo(f"\nOld schema version, re-ingest with --force: {', '.join(stale)}")


@app.command("sql")
def sql_command(
    query: Annotated[str, typer.Argument(help="SQL over: " + ", ".join(SCHEMA) + ".")],
    warehouse: WarehouseOption = DEFAULT_WAREHOUSE,
    app_id: Annotated[
        str | None, typer.Option("--app", help="Only this app (default: all apps).")
    ] = None,
    max_rows: Annotated[int, typer.Option(help="Print at most this many rows.")] = 50,
) -> None:
    """Run SQL against the stored tables with DuckDB."""
    try:
        columns, rows = Warehouse(warehouse).query(query, app_id=app_id)
    except StoreError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    except Exception as exc:  # DuckDB syntax/binder errors: show them plainly
        typer.echo(f"SQL error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    _print_table(columns, rows[:max_rows])
    if len(rows) > max_rows:
        typer.echo(f"... {len(rows) - max_rows} more rows (use --max-rows)")


def _print_table(columns: list[str], rows: list[tuple], max_width: int = 48) -> None:
    def cell(value: object) -> str:
        text = "" if value is None else str(value).replace("\n", " ")
        return text if len(text) <= max_width else text[: max_width - 3] + "..."

    cells = [[cell(v) for v in row] for row in rows]
    widths = [max([len(c)] + [len(r[i]) for r in cells]) for i, c in enumerate(columns)]
    typer.echo("  ".join(c.ljust(w) for c, w in zip(columns, widths, strict=True)).rstrip())
    typer.echo("  ".join("-" * w for w in widths))
    for r in cells:
        typer.echo("  ".join(v.ljust(w) for v, w in zip(r, widths, strict=True)).rstrip())
    typer.echo(f"({len(rows)} row{'s' if len(rows) != 1 else ''})")
