"""Smoke tests for the core pipeline idea: JSON lines -> (zstd) -> Arrow -> Parquet -> DuckDB SQL.

If these pass, every library the later steps depend on is installed and works together.
"""

import duckdb
import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard


def _task_end(task_id: int, launch: int, finish: int) -> bytes:
    info = {"Task ID": task_id, "Launch Time": launch, "Finish Time": finish}
    return orjson.dumps({"Event": "SparkListenerTaskEnd", "Stage ID": 0, "Task Info": info})


EVENT_LINES = [
    _task_end(0, 1000, 1500),
    _task_end(1, 1000, 9000),
    b'{"Event":"SparkListenerLogStart","Spark Version":"4.0.0"}',
]


def test_zstd_round_trip_streams_lines():
    compressed = zstandard.ZstdCompressor().compress(b"\n".join(EVENT_LINES))
    with zstandard.ZstdDecompressor().stream_reader(compressed) as reader:
        lines = reader.read().splitlines()
    assert lines == EVENT_LINES


def test_events_to_parquet_to_duckdb(tmp_path):
    rows = []
    for line in EVENT_LINES:
        event = orjson.loads(line)
        if event["Event"] != "SparkListenerTaskEnd":
            continue
        info = event["Task Info"]
        rows.append(
            {
                "stage_id": event["Stage ID"],
                "task_id": info["Task ID"],
                "duration_ms": info["Finish Time"] - info["Launch Time"],
            }
        )

    path = tmp_path / "tasks.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)

    # Use a connection + execute(): this API is the same in DuckDB 1.x and 2.x
    # (DuckDB 2.0 removed fetchone() from duckdb.sql() relations).
    con = duckdb.connect()
    max_ms, median_ms = con.execute(
        "SELECT max(duration_ms), median(duration_ms) FROM read_parquet(?) WHERE stage_id = 0",
        [str(path)],
    ).fetchone()
    assert max_ms == 8000
    assert median_ms == 4250
