"""Step 4: the reader.

Finds Spark event logs on disk and yields their lines one at a time, so a multi-GB log never has
to fit in memory. It handles every layout Spark writes:

    local-1795436663349                       plain JSON lines (one file)
    local-1795436663349.zstd                  one compressed file
    app-20261123-0001.inprogress              an app that is still running (or crashed)
    eventlog_v2_local-1795436663349/          a rolled log (Spark 4 default): a folder holding
        events_1_local-1795436663349.zstd       numbered parts, read in order of the number
        events_2_local-1795436663349.zstd
        appstatus_local-1795436663349           a marker file, NOT part of the log
        .appstatus_local-1795436663349.crc      a Hadoop checksum file, NOT part of the log

Compression is detected from the file's first bytes, not its name, so renamed files still work.
Supported: plain, zstd (Spark's default codec) and gzip. Spark's lz4, lzf and snappy codecs use
Java-specific stream formats, which are reported clearly as unsupported.

Usage:
    log = open_log("lab/eventlogs/gallery/skew")
    for line in log.lines():       # bytes, one JSON event per line, newline stripped
        ...
"""

from __future__ import annotations

import gzip
import io
import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import orjson
import zstandard

IN_PROGRESS = ".inprogress"
ROLLING_DIR_PREFIX = "eventlog_v2_"
UNSUPPORTED_CODECS = {"lz4", "lzf", "snappy"}
KNOWN_SUFFIXES = {"zstd", "zst", "gz", *UNSUPPORTED_CODECS}

ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
GZIP_MAGIC = b"\x1f\x8b"

_PART_RE = re.compile(r"^events_(\d+)_(.+?)(?:\.(zstd|zst|gz|lz4|lzf|snappy))?(?:\.inprogress)?$")


class EventLogError(Exception):
    """The path is not a readable Spark event log."""


class UnsupportedCodecError(EventLogError):
    """The log uses a compression codec Sentinel can't read yet."""


@dataclass(frozen=True)
class EventLog:
    """One Spark application's event log: a single file, or the ordered parts of a rolled log."""

    path: Path  # what the user pointed at: a file or an eventlog_v2_* folder
    files: tuple[Path, ...]  # the actual files holding events, in reading order
    app_id: str
    rolling: bool
    in_progress: bool  # app still running, or it crashed before writing ApplicationEnd
    codecs: tuple[str, ...] = field(default=())  # detected per file: "plain", "zstd", "gzip"

    def lines(self) -> Iterator[bytes]:
        """Yield every non-empty line of the log, in order, without the trailing newline.

        For an in-progress log the last line may have been cut off mid-write (and a compressed
        stream may end mid-frame). That incomplete tail is dropped instead of raising.
        """
        last = len(self.files) - 1
        for i, path in enumerate(self.files):
            tolerate_truncation = self.in_progress and i == last
            yield from _file_lines(path, tolerate_truncation)


def open_log(path: str | Path) -> EventLog:
    """Resolve a path to exactly one event log.

    Accepts a log file, a rolled-log folder, or a folder that contains exactly one log
    (e.g. lab/eventlogs/gallery/skew). Raises EventLogError if there are zero or several.
    """
    path = Path(path)
    if not path.exists():
        raise EventLogError(f"No such file or folder: {path}")
    if path.is_file():
        return _single_file_log(path)
    if path.name.startswith(ROLLING_DIR_PREFIX):
        return _rolling_log(path)

    logs = find_logs(path)
    if not logs:
        raise EventLogError(f"No Spark event logs found in {path}")
    if len(logs) > 1:
        names = "\n  ".join(str(log.path) for log in logs[:10])
        raise EventLogError(f"{path} contains {len(logs)} event logs; pass one of them:\n  {names}")
    return logs[0]


def find_logs(root: str | Path) -> list[EventLog]:
    """Find every event log under a folder (searched recursively).

    Sorted by app ID; Spark app IDs start with a timestamp, so this is usually run order.
    """
    root = Path(root)
    logs: list[EventLog] = []
    for path in sorted(root.rglob("*")):
        if any(parent.name.startswith(ROLLING_DIR_PREFIX) for parent in path.parents):
            continue  # files inside a rolled log belong to that log, handled below
        if path.is_dir() and path.name.startswith(ROLLING_DIR_PREFIX):
            logs.append(_rolling_log(path))
        elif path.is_file() and _looks_like_log_file(path):
            logs.append(_single_file_log(path))
    return sorted(logs, key=lambda log: (log.app_id, str(log.path)))


# --- resolving layouts


def _single_file_log(path: Path) -> EventLog:
    if path.stat().st_size == 0:
        raise EventLogError(f"{path} is empty")
    name = path.name
    in_progress = name.endswith(IN_PROGRESS)
    base = name.removesuffix(IN_PROGRESS)
    stem, _, suffix = base.rpartition(".")
    app_id = stem if stem and suffix in KNOWN_SUFFIXES else base
    return EventLog(
        path=path,
        files=(path,),
        app_id=app_id,
        rolling=False,
        in_progress=in_progress,
        codecs=(_detect_codec(path),),
    )


def _rolling_log(folder: Path) -> EventLog:
    parts: list[tuple[int, Path]] = []
    for child in folder.iterdir():
        match = _PART_RE.match(child.name)
        if child.is_file() and match:
            parts.append((int(match.group(1)), child))
    if not parts:
        raise EventLogError(f"Rolled log folder {folder} has no events_<N>_* files")

    parts.sort()  # numeric order: events_2 before events_10
    numbers = [n for n, _ in parts]
    expected = list(range(numbers[0], numbers[0] + len(numbers)))
    if numbers != expected:
        missing = sorted(set(expected) - set(numbers))
        raise EventLogError(f"Rolled log {folder} is missing part(s) {missing}")

    files = tuple(p for _, p in parts)
    app_id = folder.name.removeprefix(ROLLING_DIR_PREFIX)
    # While the app runs, the status marker (and the newest part) carry an .inprogress suffix.
    in_progress = any(
        c.name.startswith("appstatus_") and c.name.endswith(IN_PROGRESS) for c in folder.iterdir()
    ) or files[-1].name.endswith(IN_PROGRESS)
    return EventLog(
        path=folder,
        files=files,
        app_id=app_id,
        rolling=True,
        in_progress=in_progress,
        codecs=tuple(_detect_codec(p) for p in files),
    )


def _looks_like_log_file(path: Path) -> bool:
    """Cheap filter for find_logs(): skip hidden, checksum and obviously non-log files."""
    name = path.name
    if name.startswith((".", "appstatus_", "_")) or name.endswith(".crc"):
        return False
    if path.suffix in {".parquet", ".py", ".md", ".json", ".txt", ".csv"}:
        return False
    if path.stat().st_size == 0:
        return False
    if name.removesuffix(IN_PROGRESS).rpartition(".")[2] in UNSUPPORTED_CODECS:
        return True  # include it, so opening it gives a clear "unsupported codec" error
    with path.open("rb") as fh:
        head = fh.read(4)
    return head.startswith((ZSTD_MAGIC, GZIP_MAGIC, b"{"))


# --- decoding


def _detect_codec(path: Path) -> str:
    with path.open("rb") as fh:
        head = fh.read(4)
    if head.startswith(ZSTD_MAGIC):
        return "zstd"
    if head.startswith(GZIP_MAGIC):
        return "gzip"
    if head.startswith(b"{"):
        return "plain"

    suffix = path.name.removesuffix(IN_PROGRESS).rpartition(".")[2]
    if suffix in UNSUPPORTED_CODECS:
        raise UnsupportedCodecError(
            f"{path.name} is compressed with Spark's '{suffix}' codec, which Sentinel can't "
            "read yet. Re-run the job with spark.eventLog.compression.codec=zstd (the default), "
            "or set spark.eventLog.compress=false."
        )
    raise EventLogError(f"{path} doesn't look like a Spark event log (unrecognised first bytes)")


def _open_decoded(path: Path) -> io.BufferedIOBase:
    codec = _detect_codec(path)
    if codec == "plain":
        return path.open("rb", buffering=1 << 20)
    if codec == "gzip":
        return gzip.open(path, "rb")
    # Spark writes zstd logs as many small frames (one per flush). Without read_across_frames,
    # python-zstandard may report end-of-stream at the first frame boundary.
    reader = zstandard.ZstdDecompressor().stream_reader(
        path.open("rb"), read_across_frames=True, closefd=True
    )
    return io.BufferedReader(reader, buffer_size=1 << 20)


def _file_lines(path: Path, tolerate_truncation: bool) -> Iterator[bytes]:
    with _open_decoded(path) as stream:
        pending: bytes | None = None
        try:
            for line in stream:
                if pending is not None:
                    yield pending
                    pending = None
                if line.endswith(b"\n"):
                    stripped = line.rstrip(b"\r\n")
                    if stripped.strip():
                        yield stripped
                else:
                    pending = line  # final line with no newline: maybe complete, maybe cut off
        except (zstandard.ZstdError, EOFError, gzip.BadGzipFile) as exc:
            if not tolerate_truncation:
                raise EventLogError(f"{path} is corrupt or truncated: {exc}") from exc
            return  # in-progress compressed stream ended mid-frame: stop at the last full line

    # Spark ends every event with a newline, so a last line without one is suspect. Note that
    # python-zstandard does NOT raise when a stream is cut mid-frame; it silently returns the
    # partial bytes, so this check is what catches truncated compressed logs.
    if pending is None or not pending.strip():
        return
    pending = pending.rstrip(b"\r")
    if _is_json(pending):
        yield pending  # a complete event that just lacks the final newline
    elif not tolerate_truncation:
        raise EventLogError(
            f"{path} is corrupt or truncated: its last line is an incomplete JSON event"
        )
    # in-progress log: the writer was mid-event, so drop the fragment


def _is_json(line: bytes) -> bool:
    try:
        orjson.loads(line)
    except orjson.JSONDecodeError:
        return False
    return True
