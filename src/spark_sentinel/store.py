"""Step 6: the store.

Writes the parser's tables to Parquet (parse once) and queries them with DuckDB (analyse many
times). Layout of a warehouse folder (default: .sentinel/):

    .sentinel/
      apps/
        local-1795525495428/
          tasks.parquet  stages.parquet  jobs.parquet  job_stages.parquet
          sql.parquet    executors.parquet  config.parquet  app.parquet
          _manifest.json        what was ingested, from where, when, and row counts
        app-20261123-0001/
          ...
      .staging/                 work-in-progress folders (an ingest that fails leaves nothing)

- Every table gets an `app_id` column, so one SQL query can span many apps.
- Every file records the schema version in its Parquet metadata (and in _manifest.json).
- Every table file is written even when it has 0 rows: DuckDB raises an error for a glob that
  matches no files, so missing files would break queries.
- Rows are written in batches straight from the parser's stream, so memory stays flat
  however many tasks a log has.
- Re-ingesting an unchanged log is skipped; --force replaces it. Never duplicates rows.

DuckDB 2.0 note: on one connection, `execute(...).fetchone()` followed by `con.sql(...)` fails
("unfinished result"). All queries here go through `run_query()`, which always uses
execute(...).fetchall(), and that is safe in every combination.
"""

from __future__ import annotations

import contextlib
import json
import re
import shutil
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spark_sentinel import __version__
from spark_sentinel.parser import SCHEMA, SCHEMA_VERSION, EventParser
from spark_sentinel.reader import ROLLING_DIR_PREFIX, EventLog, find_logs, open_log

DEFAULT_WAREHOUSE = Path(".sentinel")
DEFAULT_BATCH_ROWS = 50_000
MANIFEST = "_manifest.json"

ARROW_TYPES: dict[str, pa.DataType] = {
    "int": pa.int64(),
    "float": pa.float64(),
    "str": pa.string(),
    "bool": pa.bool_(),
    "list[int]": pa.list_(pa.int64()),
}


def arrow_schema(table: str) -> pa.Schema:
    """Arrow schema for a table: app_id first, then the parser's columns."""
    fields = [pa.field("app_id", pa.string(), nullable=False)]
    fields += [
        pa.field(col, ARROW_TYPES[kind]) for col, kind in SCHEMA[table].items() if col != "app_id"
    ]
    metadata = {
        "sentinel.schema_version": str(SCHEMA_VERSION),
        "sentinel.table": table,
        "sentinel.version": __version__,
    }
    return pa.schema(fields, metadata=metadata)


class StoreError(Exception):
    """The warehouse can't be written or queried."""


@dataclass(frozen=True)
class IngestResult:
    app_id: str
    source: Path
    status: str  # "ingested" | "skipped" (unchanged since the last ingest)
    rows: dict[str, int]
    unhandled_events: dict[str, int]
    completed: bool  # the log has an ApplicationEnd event


# --- writing


def ingest(
    path: str | Path,
    warehouse: str | Path = DEFAULT_WAREHOUSE,
    *,
    force: bool = False,
    batch_rows: int = DEFAULT_BATCH_ROWS,
) -> list[IngestResult]:
    """Ingest one log, or every log found under a folder."""
    path = Path(path)
    if path.is_dir() and not path.name.startswith(ROLLING_DIR_PREFIX):
        logs = find_logs(path)
        if not logs:
            raise StoreError(f"No Spark event logs found in {path}")
    else:
        logs = [open_log(path)]
    return [ingest_log(log, warehouse, force=force, batch_rows=batch_rows) for log in logs]


def ingest_log(
    log: EventLog,
    warehouse: str | Path = DEFAULT_WAREHOUSE,
    *,
    force: bool = False,
    batch_rows: int = DEFAULT_BATCH_ROWS,
) -> IngestResult:
    """Parse one log and write its tables. Atomic: on any error nothing is left behind."""
    root = Path(warehouse)
    fingerprint = _fingerprint(log)
    existing = _read_manifest(root / "apps" / _safe_name(log.app_id))
    if (
        not force
        and existing
        and existing.get("fingerprint") == fingerprint
        and existing.get("schema_version") == SCHEMA_VERSION
    ):
        return IngestResult(
            app_id=existing["app_id"],
            source=log.path,
            status="skipped",
            rows=existing["rows"],
            unhandled_events=existing.get("unhandled_events", {}),
            completed=existing.get("completed", False),
        )

    staging = root / ".staging" / f"{_safe_name(log.app_id)}-{uuid.uuid4().hex[:8]}"
    staging.mkdir(parents=True)
    try:
        parser = EventParser(default_app_id=log.app_id)
        rows = _write_tables(parser, log, staging, batch_rows)
        app_id = parser.app_id  # from the ApplicationStart event, if there was one
        completed = bool(parser.event_counts.get("SparkListenerApplicationEnd"))
        manifest = {
            "app_id": app_id,
            "schema_version": SCHEMA_VERSION,
            "sentinel_version": __version__,
            "ingested_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "source": str(log.path.resolve()),
            "fingerprint": fingerprint,
            "rolling": log.rolling,
            "codecs": list(log.codecs),
            "completed": completed,
            "rows": rows,
            "unhandled_events": dict(parser.unhandled_events),
            "bad_lines": parser.bad_lines,
        }
        (staging / MANIFEST).write_text(json.dumps(manifest, indent=2))
        _replace_dir(staging, root / "apps" / _safe_name(app_id))
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    finally:
        _remove_if_empty(root / ".staging")

    return IngestResult(
        app_id=app_id,
        source=log.path,
        status="ingested",
        rows=rows,
        unhandled_events=dict(parser.unhandled_events),
        completed=completed,
    )


def _write_tables(parser: EventParser, log: EventLog, out: Path, batch_rows: int) -> dict:
    schemas = {table: arrow_schema(table) for table in SCHEMA}
    buffers: dict[str, list[dict]] = {table: [] for table in SCHEMA}
    writers: dict[str, pq.ParquetWriter] = {}
    counts = dict.fromkeys(SCHEMA, 0)
    ids_written: set[str] = set()

    def flush(table: str) -> None:
        if table not in writers:  # create the file even if the table ends up with 0 rows
            writers[table] = pq.ParquetWriter(out / f"{table}.parquet", schemas[table])
        rows = buffers[table]
        if not rows:
            return
        # The real app ID arrives with ApplicationStart, near the top of the log; until then
        # the parser uses the ID from the file name. Stamp the current one on this batch.
        for row in rows:
            row["app_id"] = parser.app_id
        ids_written.add(parser.app_id)
        writers[table].write_table(pa.Table.from_pylist(rows, schema=schemas[table]))
        counts[table] += len(rows)
        rows.clear()

    try:
        for table, row in parser.iter_rows(log.lines()):
            buffers[table].append(row)
            if len(buffers[table]) >= batch_rows:
                flush(table)
        for table in SCHEMA:
            flush(table)
    finally:
        for writer in writers.values():
            writer.close()

    if len(ids_written) > 1:
        raise StoreError(
            f"{log.path}: app ID changed mid-log ({sorted(ids_written)}); "
            "ApplicationStart came after a full batch. Re-run with a larger --batch-rows."
        )
    return counts


def _fingerprint(log: EventLog) -> list[list]:
    """Name, size and modification time of each file: changes whenever the log changes."""
    return [[p.name, p.stat().st_size, p.stat().st_mtime_ns] for p in log.files]


def _safe_name(app_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", app_id) or "unknown"


def _replace_dir(new: Path, final: Path) -> None:
    final.parent.mkdir(parents=True, exist_ok=True)
    if final.exists():
        old = final.with_name(f".old-{final.name}-{uuid.uuid4().hex[:8]}")
        final.rename(old)
        new.rename(final)
        shutil.rmtree(old, ignore_errors=True)
    else:
        new.rename(final)


def _remove_if_empty(folder: Path) -> None:
    with contextlib.suppress(OSError):  # not empty, or already gone
        folder.rmdir()


def _read_manifest(app_dir: Path) -> dict | None:
    try:
        return json.loads((app_dir / MANIFEST).read_text())
    except (OSError, ValueError):
        return None


# --- reading


class Warehouse:
    """A folder of ingested apps, queried with DuckDB.

    wh = Warehouse(".sentinel")
    columns, rows = wh.query("SELECT stage_id, max(duration_ms) FROM tasks GROUP BY 1")
    columns, rows = wh.query("SELECT count(*) FROM tasks", app_id="local-1795525495428")
    """

    def __init__(self, root: str | Path = DEFAULT_WAREHOUSE) -> None:
        self.root = Path(root)
        self.apps_dir = self.root / "apps"

    def manifests(self) -> list[dict]:
        """One manifest per ingested app, oldest app ID first."""
        if not self.apps_dir.is_dir():
            return []
        found = (_read_manifest(d) for d in sorted(self.apps_dir.iterdir()) if d.is_dir())
        return [m for m in found if m]

    def app_ids(self) -> list[str]:
        return [m["app_id"] for m in self.manifests()]

    def stale_apps(self) -> list[str]:
        """Apps written with an older schema version: re-ingest them with --force."""
        return [m["app_id"] for m in self.manifests() if m["schema_version"] != SCHEMA_VERSION]

    def connect(self, app_id: str | None = None) -> duckdb.DuckDBPyConnection:
        """An in-memory DuckDB connection with one view per table (tasks, stages, ...).

        With app_id, the views only see that app; otherwise they span every app.
        """
        if app_id is None:
            if not self.app_ids():
                raise StoreError(f"No apps in {self.root}. Run: sentinel ingest <event log>")
            folder = (self.apps_dir / "*").as_posix()
        else:
            app_dir = self.apps_dir / _safe_name(app_id)
            if _read_manifest(app_dir) is None:
                known = ", ".join(self.app_ids()) or "none"
                raise StoreError(f"App '{app_id}' is not in {self.root}. Ingested: {known}")
            folder = app_dir.as_posix()

        con = duckdb.connect()
        for table in SCHEMA:
            pattern = _sql_string(f"{folder}/{table}.parquet")
            con.execute(
                f"CREATE VIEW {table} AS "
                f"SELECT * FROM read_parquet({pattern}, union_by_name = true)"
            )
        return con

    def query(
        self, sql: str, params: list[Any] | None = None, *, app_id: str | None = None
    ) -> tuple[list[str], list[tuple]]:
        """Run SQL against the table views; return (column names, rows)."""
        con = self.connect(app_id)
        try:
            return run_query(con, sql, params)
        finally:
            con.close()


def run_query(
    con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None
) -> tuple[list[str], list[tuple]]:
    """Run SQL; return (column names, rows). Always execute().fetchall(): see module notes."""
    cursor = con.execute(sql, params or [])
    columns = [d[0] for d in cursor.description] if cursor.description else []
    return columns, cursor.fetchall()


def _sql_string(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"
