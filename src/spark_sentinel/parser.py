"""Step 5: the parser.

Turns event-log lines into rows of eight tables. The field paths, units and column names
follow docs/eventlog-fields.md, which is the spec for this module.

    tasks       one row per finished task attempt          (SparkListenerTaskEnd)
    stages      one row per finished stage attempt         (SparkListenerStageCompleted)
    jobs        one row per job                            (JobStart + JobEnd)
    job_stages  one row per (job, stage) the job planned   (JobStart."Stage IDs")
    sql         one row per SQL query, with its final plan (SQLExecutionStart/End + AQE updates)
    executors   one row per executor                       (ExecutorAdded / ExecutorRemoved)
    config      one row per Spark setting                  (EnvironmentUpdate."Spark Properties")
    app         exactly one row                            (LogStart, ApplicationStart/End, ...)

Design:
- Tasks and stages are the big tables, so they are produced as soon as their event is read
  (iter_rows streams them). The small tables need several events joined, so they are
  produced once the whole log has been read.
- Every row of a table has exactly the columns listed in SCHEMA, in that order. A field that is
  missing from the event (older Spark, failed task, ...) becomes None instead of a KeyError.
- All times are converted to milliseconds. Spark logs a few durations in nanoseconds.
- Unknown event types are counted and skipped, because new Spark versions add events.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import orjson

from spark_sentinel.reader import EventLog

SCHEMA_VERSION = 1

# Column name -> type. Types are simple names that Step 6 maps to Arrow types.
SCHEMA: dict[str, dict[str, str]] = {
    "tasks": {
        "stage_id": "int",
        "stage_attempt": "int",
        "task_id": "int",
        "task_index": "int",
        "task_attempt": "int",
        "partition_id": "int",
        "task_type": "str",
        "end_reason": "str",
        "executor_id": "str",
        "host": "str",
        "locality": "str",
        "speculative": "bool",
        "failed": "bool",
        "killed": "bool",
        "launch_time_ms": "int",
        "finish_time_ms": "int",
        "duration_ms": "int",
        "getting_result_ms": "int",
        "run_time_ms": "int",
        "cpu_time_ms": "float",
        "deser_time_ms": "int",
        "gc_time_ms": "int",
        "result_ser_ms": "int",
        "result_bytes": "int",
        "peak_exec_mem_bytes": "int",
        "mem_spill_bytes": "int",
        "disk_spill_bytes": "int",
        "input_bytes": "int",
        "input_records": "int",
        "output_bytes": "int",
        "output_records": "int",
        "shuffle_write_bytes": "int",
        "shuffle_write_records": "int",
        "shuffle_write_ms": "float",
        "shuffle_read_local_bytes": "int",
        "shuffle_read_remote_bytes": "int",
        "shuffle_read_bytes": "int",
        "shuffle_read_records": "int",
        "fetch_wait_ms": "int",
    },
    "stages": {
        "stage_id": "int",
        "stage_attempt": "int",
        "stage_name": "str",
        "num_tasks": "int",
        "parent_stage_ids": "list[int]",
        "submit_time_ms": "int",
        "complete_time_ms": "int",
        "duration_ms": "int",
        "status": "str",
        "failure_reason": "str",
    },
    "jobs": {
        "job_id": "int",
        "sql_execution_id": "int",
        "description": "str",
        "submit_time_ms": "int",
        "complete_time_ms": "int",
        "duration_ms": "int",
        "result": "str",
        "num_stages": "int",
    },
    "job_stages": {
        "job_id": "int",
        "stage_id": "int",
        "planned_tasks": "int",
    },
    "sql": {
        "execution_id": "int",
        "query_id": "str",
        "description": "str",
        "start_time_ms": "int",
        "end_time_ms": "int",
        "duration_ms": "int",
        "error": "str",
        "initial_plan": "str",
        "final_plan": "str",
        "plan_updates": "int",
    },
    "executors": {
        "executor_id": "str",
        "host": "str",
        "cores": "int",
        "added_time_ms": "int",
        "removed_time_ms": "int",
        "removed_reason": "str",
        "end_time_ms": "int",
        "lifetime_ms": "int",
    },
    "config": {
        "key": "str",
        "value": "str",
    },
    "app": {
        "app_id": "str",
        "app_name": "str",
        "user": "str",
        "spark_version": "str",
        "java_version": "str",
        "master": "str",
        "start_time_ms": "int",
        "end_time_ms": "int",
        "duration_ms": "int",
        "exit_code": "int",
        "completed": "bool",
    },
}

TABLES = tuple(SCHEMA)
SQL_EVENT_PREFIX = "org.apache.spark.sql.execution.ui."
NS_PER_MS = 1_000_000

Row = dict[str, Any]


@dataclass
class ParsedApp:
    """All tables for one application, plus what the parser saw along the way."""

    app_id: str
    tables: dict[str, list[Row]]
    event_counts: Counter[str] = field(default_factory=Counter)
    unhandled_events: Counter[str] = field(default_factory=Counter)
    bad_lines: int = 0

    def __getitem__(self, table: str) -> list[Row]:
        return self.tables[table]


def parse(log: EventLog) -> ParsedApp:
    """Parse a whole log into tables held in memory. Fine for logs with up to ~1M tasks."""
    parser = EventParser(default_app_id=log.app_id)
    tables: dict[str, list[Row]] = {name: [] for name in TABLES}
    for table, row in parser.iter_rows(log.lines()):
        tables[table].append(row)
    return ParsedApp(
        app_id=parser.app_id,
        tables=tables,
        event_counts=parser.event_counts,
        unhandled_events=parser.unhandled_events,
        bad_lines=parser.bad_lines,
    )


class EventParser:
    """Stateful parser: feed it events in log order, then call finish()."""

    def __init__(self, default_app_id: str = "unknown") -> None:
        self.app_id = default_app_id
        self.event_counts: Counter[str] = Counter()
        self.unhandled_events: Counter[str] = Counter()
        self.bad_lines = 0

        self._jobs: dict[int, Row] = {}
        self._job_stages: list[Row] = []
        self._sql: dict[int, Row] = {}
        self._executors: dict[str, Row] = {}
        self._config: dict[str, str] = {}
        self._app: Row = {"app_id": default_app_id, "completed": False}

        self._handlers = {
            "SparkListenerTaskEnd": self._task_end,
            "SparkListenerStageCompleted": self._stage_completed,
            "SparkListenerJobStart": self._job_start,
            "SparkListenerJobEnd": self._job_end,
            "SparkListenerExecutorAdded": self._executor_added,
            "SparkListenerExecutorRemoved": self._executor_removed,
            "SparkListenerEnvironmentUpdate": self._environment_update,
            "SparkListenerLogStart": self._log_start,
            "SparkListenerApplicationStart": self._application_start,
            "SparkListenerApplicationEnd": self._application_end,
            "SparkListenerSQLExecutionStart": self._sql_start,
            "SparkListenerSQLExecutionEnd": self._sql_end,
            "SparkListenerSQLAdaptiveExecutionUpdate": self._sql_adaptive_update,
        }
        # Events we read nothing from, on purpose (see docs/eventlog-fields.md, section 2).
        self._ignored = {
            "SparkListenerTaskStart",
            "SparkListenerStageSubmitted",
            "SparkListenerBlockManagerAdded",
            "SparkListenerBlockManagerRemoved",
            "SparkListenerResourceProfileAdded",
            "SparkListenerTaskGettingResult",
            "SparkListenerExecutorMetricsUpdate",
            "SparkListenerStageExecutorMetrics",
            "SparkListenerUnpersistRDD",
            "SparkListenerDriverAccumUpdates",
        }

    # --- driving -------------------------------------------------------------------------------

    def iter_rows(self, lines: Iterator[bytes]) -> Iterator[tuple[str, Row]]:
        """Yield (table, row) pairs: tasks and stages as they are read, the rest at the end."""
        for line in lines:
            try:
                event = orjson.loads(line)
            except orjson.JSONDecodeError:
                self.bad_lines += 1
                continue
            yield from self.feed(event)
        yield from self.finish()

    def feed(self, event: dict) -> Iterator[tuple[str, Row]]:
        name = event.get("Event", "")
        short = name.removeprefix(SQL_EVENT_PREFIX)
        self.event_counts[short] += 1
        handler = self._handlers.get(short)
        if handler is None:
            if short not in self._ignored:
                self.unhandled_events[short] += 1
            return
        result = handler(event)
        if result is not None:
            yield result

    def finish(self) -> Iterator[tuple[str, Row]]:
        app = self._app
        app["master"] = self._config.get("spark.master")
        app["duration_ms"] = _minus(app.get("end_time_ms"), app.get("start_time_ms"))
        app_end = app.get("end_time_ms")

        for job in sorted(self._jobs.values(), key=lambda j: j["job_id"]):
            job["duration_ms"] = _minus(job.get("complete_time_ms"), job.get("submit_time_ms"))
            yield "jobs", _shape("jobs", job)
        for row in self._job_stages:
            yield "job_stages", _shape("job_stages", row)
        for sql in sorted(self._sql.values(), key=lambda s: s["execution_id"]):
            sql["duration_ms"] = _minus(sql.get("end_time_ms"), sql.get("start_time_ms"))
            sql.setdefault("final_plan", sql.get("initial_plan"))
            yield "sql", _shape("sql", sql)
        for ex in self._executors.values():
            # No ExecutorRemoved event (always the case in local mode): it lived until app end.
            ex["end_time_ms"] = ex.get("removed_time_ms") or app_end
            ex["lifetime_ms"] = _minus(ex["end_time_ms"], ex.get("added_time_ms"))
            yield "executors", _shape("executors", ex)
        for key, value in sorted(self._config.items()):
            yield "config", {"key": key, "value": value}
        yield "app", _shape("app", app)

    # --- big tables: emitted immediately -------------------------------------------------------

    def _task_end(self, ev: dict) -> tuple[str, Row]:
        info = ev.get("Task Info", {})
        m = ev.get("Task Metrics") or {}  # absent for some failed/killed tasks
        sr = m.get("Shuffle Read Metrics", {})
        sw = m.get("Shuffle Write Metrics", {})
        inp = m.get("Input Metrics", {})
        out = m.get("Output Metrics", {})
        local, remote = sr.get("Local Bytes Read"), sr.get("Remote Bytes Read")
        launch, finish = info.get("Launch Time"), info.get("Finish Time")

        row = {
            "stage_id": ev.get("Stage ID"),
            "stage_attempt": ev.get("Stage Attempt ID"),
            "task_id": info.get("Task ID"),
            "task_index": info.get("Index"),
            "task_attempt": info.get("Attempt"),
            "partition_id": info.get("Partition ID"),
            "task_type": ev.get("Task Type"),
            "end_reason": (ev.get("Task End Reason") or {}).get("Reason"),
            "executor_id": info.get("Executor ID"),
            "host": info.get("Host"),
            "locality": info.get("Locality"),
            "speculative": info.get("Speculative"),
            "failed": info.get("Failed"),
            "killed": info.get("Killed"),
            "launch_time_ms": launch,
            "finish_time_ms": finish,
            "duration_ms": _minus(finish, launch),
            "getting_result_ms": info.get("Getting Result Time"),
            "run_time_ms": m.get("Executor Run Time"),
            "cpu_time_ms": _ns_to_ms(m.get("Executor CPU Time")),
            "deser_time_ms": m.get("Executor Deserialize Time"),
            "gc_time_ms": m.get("JVM GC Time"),
            "result_ser_ms": m.get("Result Serialization Time"),
            "result_bytes": m.get("Result Size"),
            "peak_exec_mem_bytes": m.get("Peak Execution Memory"),
            "mem_spill_bytes": m.get("Memory Bytes Spilled"),
            "disk_spill_bytes": m.get("Disk Bytes Spilled"),
            "input_bytes": inp.get("Bytes Read"),
            "input_records": inp.get("Records Read"),
            "output_bytes": out.get("Bytes Written"),
            "output_records": out.get("Records Written"),
            "shuffle_write_bytes": sw.get("Shuffle Bytes Written"),
            "shuffle_write_records": sw.get("Shuffle Records Written"),
            "shuffle_write_ms": _ns_to_ms(sw.get("Shuffle Write Time")),
            "shuffle_read_local_bytes": local,
            "shuffle_read_remote_bytes": remote,
            "shuffle_read_bytes": None
            if local is None and remote is None
            else (local or 0) + (remote or 0),
            "shuffle_read_records": sr.get("Total Records Read"),
            "fetch_wait_ms": sr.get("Fetch Wait Time"),
        }
        return "tasks", row

    def _stage_completed(self, ev: dict) -> tuple[str, Row]:
        s = ev.get("Stage Info", {})
        submit, complete = s.get("Submission Time"), s.get("Completion Time")
        failure = s.get("Failure Reason")
        row = {
            "stage_id": s.get("Stage ID"),
            "stage_attempt": s.get("Stage Attempt ID"),
            "stage_name": s.get("Stage Name"),
            "num_tasks": s.get("Number of Tasks"),
            "parent_stage_ids": s.get("Parent IDs"),
            "submit_time_ms": submit,
            "complete_time_ms": complete,
            "duration_ms": _minus(complete, submit),
            "status": "failed" if failure else "succeeded",
            "failure_reason": failure,
        }
        return "stages", row

    # --- small tables: collected, emitted by finish() ------------------------------------------

    def _job_start(self, ev: dict) -> None:
        job_id = ev.get("Job ID")
        props = ev.get("Properties") or {}
        stage_infos = ev.get("Stage Infos") or []
        planned = {si.get("Stage ID"): si.get("Number of Tasks") for si in stage_infos}
        stage_ids = ev.get("Stage IDs") or list(planned)

        self._jobs.setdefault(job_id, {"job_id": job_id}).update(
            {
                "sql_execution_id": _to_int(props.get("spark.sql.execution.id")),
                "description": props.get("spark.job.description"),
                "submit_time_ms": ev.get("Submission Time"),
                "num_stages": len(stage_ids),
            }
        )
        for stage_id in stage_ids:
            self._job_stages.append(
                {"job_id": job_id, "stage_id": stage_id, "planned_tasks": planned.get(stage_id)}
            )

    def _job_end(self, ev: dict) -> None:
        job_id = ev.get("Job ID")
        self._jobs.setdefault(job_id, {"job_id": job_id}).update(
            {
                "complete_time_ms": ev.get("Completion Time"),
                "result": (ev.get("Job Result") or {}).get("Result"),
            }
        )

    def _sql_start(self, ev: dict) -> None:
        exec_id = ev.get("executionId")
        self._sql.setdefault(exec_id, {"execution_id": exec_id, "plan_updates": 0}).update(
            {
                "query_id": ev.get("queryId"),
                "description": ev.get("description"),
                "start_time_ms": ev.get("time"),
                "initial_plan": ev.get("physicalPlanDescription"),
            }
        )

    def _sql_end(self, ev: dict) -> None:
        exec_id = ev.get("executionId")
        row = self._sql.setdefault(exec_id, {"execution_id": exec_id, "plan_updates": 0})
        row["end_time_ms"] = ev.get("time")
        error = ev.get("errorMessage")
        row["error"] = error or None  # Spark writes "" for success
        if row.get("query_id") is None:
            row["query_id"] = ev.get("queryId")

    def _sql_adaptive_update(self, ev: dict) -> None:
        exec_id = ev.get("executionId")
        row = self._sql.setdefault(exec_id, {"execution_id": exec_id, "plan_updates": 0})
        row["final_plan"] = ev.get("physicalPlanDescription")  # the last update wins
        row["plan_updates"] += 1

    def _executor_added(self, ev: dict) -> None:
        info = ev.get("Executor Info") or {}
        executor_id = ev.get("Executor ID")
        self._executors[executor_id] = {
            "executor_id": executor_id,
            "host": info.get("Host"),
            "cores": info.get("Total Cores"),
            "added_time_ms": ev.get("Timestamp"),
        }

    def _executor_removed(self, ev: dict) -> None:
        executor_id = ev.get("Executor ID")
        row = self._executors.setdefault(executor_id, {"executor_id": executor_id})
        row["removed_time_ms"] = ev.get("Timestamp")
        row["removed_reason"] = ev.get("Removed Reason")

    def _environment_update(self, ev: dict) -> None:
        for key, value in (ev.get("Spark Properties") or {}).items():
            self._config[key] = str(value)
        jvm = ev.get("JVM Information") or {}
        if jvm.get("Java Version"):
            self._app["java_version"] = jvm["Java Version"]

    def _log_start(self, ev: dict) -> None:
        self._app["spark_version"] = ev.get("Spark Version")

    def _application_start(self, ev: dict) -> None:
        if ev.get("App ID"):
            self.app_id = ev["App ID"]
        self._app.update(
            {
                "app_id": self.app_id,
                "app_name": ev.get("App Name"),
                "user": ev.get("User"),
                "start_time_ms": ev.get("Timestamp"),
            }
        )

    def _application_end(self, ev: dict) -> None:
        self._app.update(
            {"end_time_ms": ev.get("Timestamp"), "exit_code": ev.get("ExitCode"), "completed": True}
        )


# --- helpers --------------------------------------------------------------------------------------


def _shape(table: str, row: Row) -> Row:
    """Return the row with exactly the table's columns, in schema order; missing ones are None."""
    return {col: row.get(col) for col in SCHEMA[table]}


def _minus(end: int | None, start: int | None) -> int | None:
    return None if end is None or start is None else end - start


def _ns_to_ms(value: int | None) -> float | None:
    return None if value is None else value / NS_PER_MS


def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
