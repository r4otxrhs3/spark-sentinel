"""Shared SparkSession setup for every gallery job.

Each job writes its event log to lab/eventlogs/gallery/<job>/ so you always know which log is which.
The folder is emptied first, so it holds exactly one log: the latest run.

Set SENTINEL_DEFAULT_EVENTLOG=1 to keep Spark's default log format (zstd-compressed and rolled)
instead of plain JSON. Useful for testing the reader.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "lab" / "data"
OUT = ROOT / "lab" / "out"
LOGS = ROOT / "lab" / "eventlogs" / "gallery"


def start(
    job: str, conf: dict[str, str] | None = None, *, logs: Path = LOGS, app_name: str = ""
) -> SparkSession:
    log_dir = logs / job
    if log_dir.exists():
        shutil.rmtree(log_dir)
    log_dir.mkdir(parents=True)

    builder = (
        SparkSession.builder.appName(app_name or f"gallery-{job}")
        .master("local[4]")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", log_dir.as_uri())
        .config("spark.ui.showConsoleProgress", "false")
    )
    if os.environ.get("SENTINEL_DEFAULT_EVENTLOG") != "1":
        builder = builder.config("spark.eventLog.compress", "false").config(
            "spark.eventLog.rolling.enabled", "false"
        )
    for key, value in (conf or {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
