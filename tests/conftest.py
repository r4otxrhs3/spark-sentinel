"""Shared test helpers: build small synthetic Spark apps for cases the gallery never produces."""

from __future__ import annotations

import itertools
from collections.abc import Callable
from pathlib import Path

import orjson
import pytest

from spark_sentinel.analysis import AnalysisResult, analyze
from spark_sentinel.rules import load_builtin_rules
from spark_sentinel.store import Warehouse, ingest

SQL_UI = "org.apache.spark.sql.execution.ui."
MB = 1024 * 1024
GB = 1024 * MB


def task(
    stage: int = 0,
    duration_ms: int = 2_000,
    launch_ms: int = 0,
    shuffle_read: int = 10 * MB,
    input_bytes: int = 0,
    output_bytes: int = 0,
    output_records: int = 0,
    disk_spill: int = 0,
    mem_spill: int = 0,
    gc_ms: int = 0,
    failed: bool = False,
) -> dict:
    """Arguments for one SparkListenerTaskEnd; AppBuilder fills in ids and timestamps."""
    return locals()


class AppBuilder:
    """Writes a plausible event log: one SQL query, one job, the given stages and tasks."""

    def __init__(
        self,
        app_id: str = "app-synthetic-0001",
        cores: int = 4,
        master: str = "local[4]",
        conf: dict[str, str] | None = None,
        plan: str | None = None,
    ) -> None:
        self.app_id, self.cores, self.master = app_id, cores, master
        self.conf = conf or {}
        self.plan = plan
        self.tasks: list[dict] = []
        self._ids = itertools.count()

    def add(self, *tasks: dict) -> AppBuilder:
        self.tasks.extend(tasks)
        return self

    def events(self) -> list[dict]:
        base = 1_000_000
        stages = sorted({t["stage"] for t in self.tasks}) or [0]
        props = {"spark.master": self.master, **self.conf}
        events: list[dict] = [
            {"Event": "SparkListenerLogStart", "Spark Version": "4.0.0"},
            {"Event": "SparkListenerEnvironmentUpdate", "Spark Properties": props},
            {
                "Event": "SparkListenerApplicationStart",
                "App ID": self.app_id,
                "App Name": "synthetic",
                "Timestamp": base,
            },
            {
                "Event": "SparkListenerExecutorAdded",
                "Timestamp": base,
                "Executor ID": "driver",
                "Executor Info": {"Host": "localhost", "Total Cores": self.cores},
            },
        ]
        if self.plan is not None:
            events.append(
                {
                    "Event": SQL_UI + "SparkListenerSQLExecutionStart",
                    "executionId": 0,
                    "description": "synthetic query",
                    "time": base,
                    "physicalPlanDescription": self.plan,
                }
            )
        events.append(
            {
                "Event": "SparkListenerJobStart",
                "Job ID": 0,
                "Submission Time": base,
                "Stage IDs": stages,
                "Properties": {"spark.sql.execution.id": "0"} if self.plan is not None else {},
            }
        )
        for t in self.tasks:
            launch = base + t["launch_ms"]
            events.append(
                {
                    "Event": "SparkListenerTaskEnd",
                    "Stage ID": t["stage"],
                    "Stage Attempt ID": 0,
                    "Task Type": "ResultTask",
                    "Task End Reason": {"Reason": "ExceptionFailure" if t["failed"] else "Success"},
                    "Task Info": {
                        "Task ID": next(self._ids),
                        "Executor ID": "driver",
                        "Launch Time": launch,
                        "Finish Time": launch + t["duration_ms"],
                        "Failed": t["failed"],
                    },
                    "Task Metrics": {
                        "Executor Run Time": t["duration_ms"],
                        "JVM GC Time": t["gc_ms"],
                        "Memory Bytes Spilled": t["mem_spill"],
                        "Disk Bytes Spilled": t["disk_spill"],
                        "Input Metrics": {"Bytes Read": t["input_bytes"], "Records Read": 0},
                        "Output Metrics": {
                            "Bytes Written": t["output_bytes"],
                            "Records Written": t["output_records"],
                        },
                        "Shuffle Read Metrics": {
                            "Local Bytes Read": t["shuffle_read"],
                            "Remote Bytes Read": 0,
                        },
                        "Shuffle Write Metrics": {"Shuffle Bytes Written": 0},
                    },
                }
            )
        for stage in stages:
            n = sum(1 for t in self.tasks if t["stage"] == stage)
            events.append(
                {
                    "Event": "SparkListenerStageCompleted",
                    "Stage Info": {
                        "Stage ID": stage,
                        "Stage Attempt ID": 0,
                        "Number of Tasks": n,
                        "Submission Time": base,
                        "Completion Time": base + 1,
                    },
                }
            )
        events.append(
            {
                "Event": "SparkListenerJobEnd",
                "Job ID": 0,
                "Completion Time": base + 2,
                "Job Result": {"Result": "JobSucceeded"},
            }
        )
        events.append({"Event": "SparkListenerApplicationEnd", "Timestamp": base + 10**6})
        return events

    def write(self, folder: Path) -> Path:
        path = folder / self.app_id
        path.write_bytes(b"".join(orjson.dumps(e) + b"\n" for e in self.events()))
        return path


@pytest.fixture
def run_rules(tmp_path) -> Callable[..., AnalysisResult]:
    """Build an app, ingest it, and run the given rules (default: all) on it."""

    def run(builder: AppBuilder, select=None, overrides=None) -> AnalysisResult:
        log = builder.write(tmp_path)
        ingest(log, tmp_path / "wh", force=True)
        load_builtin_rules()
        return analyze(Warehouse(tmp_path / "wh"), select=select, overrides=overrides)

    return run
