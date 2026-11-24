"""Tests for the store: Parquet output, the warehouse layout, and DuckDB queries."""

from __future__ import annotations

import json
import os
from pathlib import Path

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import zstandard
from typer.testing import CliRunner

from spark_sentinel.cli import app
from spark_sentinel.parser import SCHEMA, SCHEMA_VERSION
from spark_sentinel.reader import EventLogError
from spark_sentinel.store import StoreError, Warehouse, arrow_schema, ingest

GALLERY = Path(__file__).parent / "fixtures" / "eventlogs" / "gallery"

EXPECTED_TASKS = {
    "gallery-clean": 6,
    "gallery-skew": 40,
    "gallery-spill": 12,
    "gallery-udf_filter": 6,
    "gallery-small_files": 533,
    "gallery-over_partitioned": 1006,
}


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory) -> Warehouse:
    root = tmp_path_factory.mktemp("wh")
    ingest(GALLERY, root)
    return Warehouse(root)


def app_id_of(wh: Warehouse, name: str) -> str:
    _, rows = wh.query("SELECT app_id FROM app WHERE app_name = ?", [name])
    return rows[0][0]


def write_log(path: Path, events: list[dict]) -> Path:
    path.write_bytes(b"".join(orjson.dumps(e) + b"\n" for e in events))
    return path


def minimal_events(app_id="local-1", tasks=2) -> list[dict]:
    events = [
        {"Event": "SparkListenerLogStart", "Spark Version": "4.0.0"},
        {
            "Event": "SparkListenerApplicationStart",
            "App ID": app_id,
            "App Name": "t",
            "Timestamp": 1,
        },
    ]
    events += [
        {
            "Event": "SparkListenerTaskEnd",
            "Stage ID": 0,
            "Stage Attempt ID": 0,
            "Task Info": {"Task ID": i, "Launch Time": 10, "Finish Time": 20},
        }
        for i in range(tasks)
    ]
    events.append({"Event": "SparkListenerApplicationEnd", "Timestamp": 99})
    return events


# --- layout and files


def test_every_app_has_every_table_file_and_a_manifest(warehouse):
    app_dirs = sorted(p for p in (warehouse.root / "apps").iterdir() if p.is_dir())
    assert len(app_dirs) == 6
    for app_dir in app_dirs:
        assert {p.name for p in app_dir.iterdir()} == {f"{t}.parquet" for t in SCHEMA} | {
            "_manifest.json"
        }
    assert not (warehouse.root / ".staging").exists()


def test_parquet_schema_types_and_version(warehouse):
    app_dir = next((warehouse.root / "apps").iterdir())
    schema = pq.read_schema(app_dir / "tasks.parquet")
    assert schema.names[0] == "app_id"
    assert schema.field("app_id").nullable is False
    assert schema.field("cpu_time_ms").type == pa.float64()
    assert schema.field("disk_spill_bytes").type == pa.int64()
    assert schema.field("failed").type == pa.bool_()
    assert pq.read_schema(app_dir / "stages.parquet").field("parent_stage_ids").type == pa.list_(
        pa.int64()
    )
    assert schema.metadata[b"sentinel.schema_version"] == str(SCHEMA_VERSION).encode()
    assert schema.metadata[b"sentinel.table"] == b"tasks"


@pytest.mark.parametrize("table", list(SCHEMA))
def test_arrow_schema_matches_parser_schema(table):
    names = arrow_schema(table).names
    assert names[0] == "app_id"
    assert [n for n in names if n != "app_id"] == [c for c in SCHEMA[table] if c != "app_id"]


def test_rows_carry_their_app_id(warehouse):
    for manifest in warehouse.manifests():
        app_id = manifest["app_id"]
        app_dir = warehouse.root / "apps" / app_id
        for table in SCHEMA:
            ids = set(
                pq.read_table(app_dir / f"{table}.parquet", columns=["app_id"])[
                    "app_id"
                ].to_pylist()
            )
            assert ids <= {app_id}
        assert manifest["schema_version"] == SCHEMA_VERSION
        assert manifest["completed"] is True
        assert manifest["unhandled_events"] == {}


def test_manifest_counts_match_parquet(warehouse):
    for manifest in warehouse.manifests():
        app_dir = warehouse.root / "apps" / manifest["app_id"]
        for table in SCHEMA:
            rows = pq.ParquetFile(app_dir / f"{table}.parquet").metadata.num_rows
            assert rows == manifest["rows"][table]


# --- querying


def test_query_across_all_apps(warehouse):
    _, rows = warehouse.query(
        "SELECT a.app_name, count(*) FROM tasks t JOIN app a USING (app_id) GROUP BY 1"
    )
    assert dict(rows) == EXPECTED_TASKS


def test_query_one_app(warehouse):
    skew = app_id_of(warehouse, "gallery-skew")
    _, rows = warehouse.query("SELECT count(*), count(DISTINCT app_id) FROM tasks", app_id=skew)
    assert rows == [(40, 1)]


def test_skew_query_in_sql(warehouse):
    """The core of the future skew rule, as plain SQL on the stored tables.

    NULLIF matters: in DuckDB x / 0 is inf (and 0 / 0 is NaN), not NULL, and inf sorts first
    in DESC order, so stages that read no shuffle data would otherwise "win".
    """
    columns, rows = warehouse.query(
        """
        SELECT stage_id,
               max(shuffle_read_bytes) / NULLIF(median(shuffle_read_bytes), 0) AS bytes_ratio,
               max(duration_ms) / NULLIF(median(duration_ms), 0)               AS time_ratio
        FROM tasks
        GROUP BY stage_id
        HAVING count(*) >= 3
        ORDER BY bytes_ratio DESC NULLS LAST
        LIMIT 1
        """,
        app_id=app_id_of(warehouse, "gallery-skew"),
    )
    stage_id, bytes_ratio, time_ratio = rows[0]
    assert columns == ["stage_id", "bytes_ratio", "time_ratio"]
    assert stage_id == 4
    assert bytes_ratio > 20
    assert time_ratio > 3


def test_trace_slowest_task_to_its_sql_query(warehouse):
    """tasks -> job_stages -> jobs -> sql: the link the whole design depends on."""
    _, rows = warehouse.query(
        """
        SELECT t.stage_id, q.execution_id, q.final_plan
        FROM tasks t
        JOIN job_stages js ON js.app_id = t.app_id AND js.stage_id = t.stage_id
        JOIN jobs j        ON j.app_id = js.app_id AND j.job_id = js.job_id
        JOIN sql q         ON q.app_id = j.app_id AND q.execution_id = j.sql_execution_id
        ORDER BY t.duration_ms DESC
        LIMIT 1
        """,
        app_id=app_id_of(warehouse, "gallery-skew"),
    )
    stage_id, execution_id, plan = rows[0]
    assert (stage_id, execution_id) == (4, 0)
    assert "SortMergeJoin" in plan


def test_config_is_queryable(warehouse):
    _, rows = warehouse.query(
        "SELECT value FROM config WHERE key = 'spark.sql.shuffle.partitions'",
        app_id=app_id_of(warehouse, "gallery-over_partitioned"),
    )
    assert rows == [("1000",)]


def test_unknown_app_and_empty_warehouse(tmp_path, warehouse):
    with pytest.raises(StoreError, match="not in"):
        warehouse.query("SELECT 1", app_id="nope")
    with pytest.raises(StoreError, match="No apps"):
        Warehouse(tmp_path / "empty").query("SELECT 1")


# --- ingest behaviour


def test_reingest_is_skipped_and_force_does_not_duplicate(tmp_path):
    first = ingest(GALLERY / "skew", tmp_path)
    again = ingest(GALLERY / "skew", tmp_path)
    forced = ingest(GALLERY / "skew", tmp_path, force=True)

    assert [r.status for r in first + again + forced] == ["ingested", "skipped", "ingested"]
    assert Warehouse(tmp_path).query("SELECT count(*) FROM tasks")[1] == [(40,)]
    assert len(list((tmp_path / "apps").iterdir())) == 1  # no leftover .old-* folders


def test_changed_log_is_reingested(tmp_path):
    log = write_log(tmp_path / "local-1", minimal_events(tasks=2))
    ingest(log, tmp_path / "wh")
    write_log(log, minimal_events(tasks=5))
    os.utime(log, ns=(log.stat().st_atime_ns, log.stat().st_mtime_ns + 10**9))

    result = ingest(log, tmp_path / "wh")[0]

    assert result.status == "ingested"
    assert Warehouse(tmp_path / "wh").query("SELECT count(*) FROM tasks")[1] == [(5,)]


def test_batches_become_row_groups(tmp_path):
    ingest(GALLERY / "over_partitioned", tmp_path, batch_rows=100)
    tasks_file = next((tmp_path / "apps").iterdir()) / "tasks.parquet"
    meta = pq.ParquetFile(tasks_file).metadata
    assert meta.num_rows == 1006
    assert meta.num_row_groups == 11  # 10 full batches of 100 + the last 6


def test_tables_with_no_rows_still_get_a_file(tmp_path):
    log = write_log(tmp_path / "local-1", minimal_events())
    ingest(log, tmp_path / "wh")
    wh = Warehouse(tmp_path / "wh")
    assert wh.query("SELECT count(*) FROM sql")[1] == [(0,)]
    assert wh.query("SELECT count(*) FROM tasks")[1] == [(2,)]


def test_failed_ingest_leaves_nothing_behind(tmp_path):
    data = zstandard.ZstdCompressor().compress(
        b"".join(orjson.dumps(e) + b"\n" for e in minimal_events(tasks=50))
    )
    log = tmp_path / "local-1.zstd"
    log.write_bytes(data[:-9])  # a finished log cut off mid-frame: the reader raises

    with pytest.raises(EventLogError):
        ingest(log, tmp_path / "wh")

    assert not (tmp_path / "wh" / "apps").exists()
    assert not (tmp_path / "wh" / ".staging").exists()


def test_app_id_from_event_is_used_for_the_folder(tmp_path):
    log = write_log(tmp_path / "renamed-log", minimal_events(app_id="app-20261124-0007"))
    result = ingest(log, tmp_path / "wh")[0]
    assert result.app_id == "app-20261124-0007"
    assert (tmp_path / "wh" / "apps" / "app-20261124-0007" / "tasks.parquet").exists()


def test_app_id_changing_after_a_flushed_batch_is_an_error(tmp_path):
    events = minimal_events(app_id="app-real", tasks=3)
    events.insert(
        0,
        events.pop(1)
        | {"Event": "SparkListenerTaskEnd", "Stage ID": 0, "Task Info": {"Task ID": 99}},
    )
    events.insert(1, {"Event": "SparkListenerTaskEnd", "Stage ID": 0, "Task Info": {}})
    events.insert(2, {"Event": "SparkListenerApplicationStart", "App ID": "app-real"})
    log = write_log(tmp_path / "from-file", events)

    with pytest.raises(StoreError, match="app ID changed"):
        ingest(log, tmp_path / "wh", batch_rows=1)
    assert not (tmp_path / "wh" / "apps").exists()


def test_stale_schema_versions_are_reported(tmp_path):
    ingest(GALLERY / "clean", tmp_path)
    manifest_path = next((tmp_path / "apps").iterdir()) / "_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["schema_version"] = 0
    manifest_path.write_text(json.dumps(manifest))

    wh = Warehouse(tmp_path)
    assert wh.stale_apps() == [manifest["app_id"]]
    assert ingest(GALLERY / "clean", tmp_path)[0].status == "ingested"  # not skipped
    assert wh.stale_apps() == []


# --- CLI


def test_cli_ingest_apps_and_sql(tmp_path):
    runner = CliRunner()
    wh = str(tmp_path / "wh")

    result = runner.invoke(app, ["ingest", str(GALLERY), "-w", wh])
    assert result.exit_code == 0, result.output
    assert result.output.count("ingested") == 6

    result = runner.invoke(app, ["apps", "-w", wh])
    assert result.exit_code == 0, result.output
    assert "gallery-over_partitioned" in result.output and "(6 rows)" in result.output

    result = runner.invoke(app, ["sql", "SELECT count(*) AS n FROM tasks", "-w", wh])
    assert result.exit_code == 0, result.output
    assert "1603" in result.output


def test_cli_sql_errors_are_readable(tmp_path):
    runner = CliRunner()
    wh = str(tmp_path / "wh")
    runner.invoke(app, ["ingest", str(GALLERY / "clean"), "-w", wh])

    result = runner.invoke(app, ["sql", "SELEC nope", "-w", wh])
    assert result.exit_code == 1
    assert "SQL error" in result.output

    result = runner.invoke(app, ["apps", "-w", str(tmp_path / "nothing")])
    assert result.exit_code == 0
    assert "No apps" in result.output
