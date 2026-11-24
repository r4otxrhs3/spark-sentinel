"""Tests for the event-log reader.

Most cases build small synthetic logs in a temp folder, so every layout and failure mode is
covered without big files. One test reads a real Spark 4.3 rolled zstd log from tests/fixtures.
"""

from __future__ import annotations

import gzip
from pathlib import Path

import orjson
import pytest
import zstandard
from typer.testing import CliRunner

from spark_sentinel.cli import app
from spark_sentinel.reader import EventLogError, UnsupportedCodecError, find_logs, open_log

FIXTURES = Path(__file__).parent / "fixtures" / "eventlogs"


def make_events(n: int) -> list[bytes]:
    events = [{"Event": "SparkListenerLogStart", "Spark Version": "4.0.0"}]
    events += [{"Event": "SparkListenerTaskEnd", "Stage ID": 0, "Task ID": i} for i in range(n)]
    events.append({"Event": "SparkListenerApplicationEnd", "Timestamp": 1})
    return [orjson.dumps(e) for e in events]


def joined(lines: list[bytes]) -> bytes:
    return b"".join(line + b"\n" for line in lines)


def zstd_frame_per_line(lines: list[bytes]) -> bytes:
    """Mimic Spark: many small zstd frames back to back, here one per line."""
    compressor = zstandard.ZstdCompressor()
    return b"".join(compressor.compress(line + b"\n") for line in lines)


# --- single files


def test_plain_file(tmp_path):
    lines = make_events(5)
    (tmp_path / "local-123").write_bytes(joined(lines))

    log = open_log(tmp_path / "local-123")

    assert list(log.lines()) == lines
    assert (log.app_id, log.rolling, log.in_progress, log.codecs) == (
        "local-123",
        False,
        False,
        ("plain",),
    )


def test_blank_lines_and_crlf_are_cleaned(tmp_path):
    lines = make_events(2)
    (tmp_path / "local-1").write_bytes(b"\r\n".join(lines) + b"\r\n\n\n")
    assert list(open_log(tmp_path / "local-1").lines()) == lines


def test_complete_log_without_final_newline_keeps_last_line(tmp_path):
    lines = make_events(3)
    (tmp_path / "local-1").write_bytes(b"\n".join(lines))
    assert list(open_log(tmp_path / "local-1").lines()) == lines


def test_zstd_with_many_frames(tmp_path):
    lines = make_events(500)
    (tmp_path / "local-9.zstd").write_bytes(zstd_frame_per_line(lines))

    log = open_log(tmp_path / "local-9.zstd")

    assert list(log.lines()) == lines
    assert log.app_id == "local-9"
    assert log.codecs == ("zstd",)


def test_gzip(tmp_path):
    lines = make_events(5)
    (tmp_path / "app-1.gz").write_bytes(gzip.compress(joined(lines)))
    assert list(open_log(tmp_path / "app-1.gz").lines()) == lines


def test_codec_detected_from_content_not_name(tmp_path):
    lines = make_events(3)
    (tmp_path / "renamed-log").write_bytes(zstd_frame_per_line(lines))
    assert list(open_log(tmp_path / "renamed-log").lines()) == lines


def test_lines_is_lazy(tmp_path):
    (tmp_path / "local-1").write_bytes(joined(make_events(3)))
    lines = open_log(tmp_path / "local-1").lines()
    assert next(lines).startswith(b'{"Event":"SparkListenerLogStart"')


# --- in-progress and damaged logs


def test_in_progress_plain_drops_cut_off_last_line(tmp_path):
    lines = make_events(3)
    (tmp_path / "local-5.inprogress").write_bytes(joined(lines) + b'{"Event":"SparkListe')

    log = open_log(tmp_path / "local-5.inprogress")

    assert log.in_progress
    assert log.app_id == "local-5"
    assert list(log.lines()) == lines


def test_in_progress_zstd_cut_mid_frame_stops_cleanly(tmp_path):
    lines = make_events(50)
    data = zstd_frame_per_line(lines)
    (tmp_path / "local-5.zstd.inprogress").write_bytes(data[: len(data) - 7])

    log = open_log(tmp_path / "local-5.zstd.inprogress")
    got = list(log.lines())

    assert log.app_id == "local-5"
    assert log.in_progress
    assert got == lines[: len(got)]  # a clean prefix of the real lines
    assert len(got) >= len(lines) - 2


def test_complete_zstd_that_is_truncated_raises(tmp_path):
    data = zstd_frame_per_line(make_events(50))
    (tmp_path / "local-5.zstd").write_bytes(data[: len(data) - 7])
    with pytest.raises(EventLogError, match="corrupt or truncated"):
        list(open_log(tmp_path / "local-5.zstd").lines())


def test_complete_plain_log_cut_mid_line_raises(tmp_path):
    (tmp_path / "local-5").write_bytes(joined(make_events(3)) + b'{"Event":"Spark')
    with pytest.raises(EventLogError, match="incomplete JSON event"):
        list(open_log(tmp_path / "local-5").lines())


def test_unsupported_codec_gives_clear_error(tmp_path):
    (tmp_path / "local-1.lz4").write_bytes(b"LZ4Block\x00\x01\x02")
    with pytest.raises(UnsupportedCodecError, match=r"compression\.codec=zstd"):
        open_log(tmp_path / "local-1.lz4")


def test_empty_file_raises(tmp_path):
    (tmp_path / "local-1").write_bytes(b"")
    with pytest.raises(EventLogError, match="empty"):
        open_log(tmp_path / "local-1")


def test_missing_path_raises(tmp_path):
    with pytest.raises(EventLogError, match="No such file"):
        open_log(tmp_path / "nope")


# --- rolled logs


def make_rolled(root: Path, app_id: str, chunks: list[list[bytes]], *, in_progress=False) -> Path:
    folder = root / f"eventlog_v2_{app_id}"
    folder.mkdir()
    for n, chunk in enumerate(chunks, start=1):
        name = f"events_{n}_{app_id}.zstd"
        if in_progress and n == len(chunks):
            name += ".inprogress"
        (folder / name).write_bytes(zstd_frame_per_line(chunk))
    status = f"appstatus_{app_id}" + (".inprogress" if in_progress else "")
    (folder / status).write_bytes(b"")
    (folder / f".{status}.crc").write_bytes(b"crc\x00\x01\x02\x03\x04")
    return folder


def test_rolled_parts_read_in_numeric_order(tmp_path):
    lines = make_events(110)
    chunks = [lines[i : i + 10] for i in range(0, len(lines), 10)]  # 12 parts: needs 2 < 10
    folder = make_rolled(tmp_path, "local-7", chunks)

    log = open_log(folder)

    assert log.rolling
    assert len(log.files) == 12
    assert [f.name.split("_")[1] for f in log.files] == [str(n) for n in range(1, 13)]
    assert list(log.lines()) == lines  # .crc and appstatus files ignored
    assert (log.app_id, log.in_progress) == ("local-7", False)


def test_rolled_in_progress(tmp_path):
    lines = make_events(20)
    folder = make_rolled(tmp_path, "local-8", [lines[:10], lines[10:]], in_progress=True)

    log = open_log(folder)

    assert log.in_progress
    assert list(log.lines()) == lines


def test_rolled_missing_part_raises(tmp_path):
    folder = make_rolled(tmp_path, "local-7", [[b"{}"], [b"{}"], [b"{}"]])
    next(folder.glob("events_2_*")).unlink()
    with pytest.raises(EventLogError, match=r"missing part\(s\) \[2\]"):
        open_log(folder)


# --- folders


def test_folder_with_one_log_resolves_to_it(tmp_path):
    job = tmp_path / "skew"
    job.mkdir()
    folder = make_rolled(job, "local-3", [make_events(2)])
    assert open_log(job).path == folder


def test_folder_with_several_logs_lists_them(tmp_path):
    (tmp_path / "local-1").write_bytes(joined(make_events(1)))
    make_rolled(tmp_path, "local-2", [make_events(1)])
    with pytest.raises(EventLogError, match="contains 2 event logs"):
        open_log(tmp_path)


def test_find_logs_skips_non_logs(tmp_path):
    (tmp_path / "local-1").write_bytes(joined(make_events(1)))
    make_rolled(tmp_path, "local-2", [make_events(1), make_events(1)])
    (tmp_path / "notes.md").write_text("# not a log")
    (tmp_path / "part-0000.snappy.parquet").write_bytes(b"PAR1")
    (tmp_path / "_SUCCESS").write_bytes(b"")
    (tmp_path / "random.bin").write_bytes(b"\x00\x01binary")

    found = find_logs(tmp_path)

    assert [log.app_id for log in found] == ["local-1", "local-2"]


def test_folder_without_logs_raises(tmp_path):
    (tmp_path / "notes.md").write_text("hi")
    with pytest.raises(EventLogError, match="No Spark event logs"):
        open_log(tmp_path)


# --- a real Spark log


def test_real_spark_rolled_zstd_log():
    """The gallery's clean job, written by Spark 4.3 with default settings (zstd + rolling)."""
    log = open_log(FIXTURES / "clean_rolled")
    events = [orjson.loads(line)["Event"] for line in log.lines()]

    assert log.rolling and log.codecs == ("zstd",) and not log.in_progress
    assert len(events) == 40
    assert events[0] == "SparkListenerLogStart"
    assert events[-1] == "SparkListenerApplicationEnd"
    assert events.count("SparkListenerTaskEnd") == 6


def test_cli_inspect_on_real_log():
    result = CliRunner().invoke(app, ["inspect", str(FIXTURES / "clean_rolled")])
    assert result.exit_code == 0, result.output
    assert "rolled, 1 part(s)" in result.output
    assert "zstd" in result.output
    assert "6  SparkListenerTaskEnd" in result.output


def test_cli_inspect_reports_errors(tmp_path):
    result = CliRunner().invoke(app, ["inspect", str(tmp_path / "missing")])
    assert result.exit_code == 1
    assert "No such file" in result.output
