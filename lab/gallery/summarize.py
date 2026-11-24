"""Print the key symptom numbers from each gallery event log.

This is a quick manual check, not Sentinel itself: it confirms every bad job really produced
its problem (and the clean job didn't) before you build rules on top of these logs.

    python lab/gallery/summarize.py
"""

from __future__ import annotations

import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import orjson
import zstandard

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _spark import LOGS

KB, MB = 1024, 1024 * 1024


def log_files(job_dir: Path) -> list[Path]:
    """Return the event-log file(s) for one app, in order.

    A plain or single compressed log is one file. A rolled log is an eventlog_v2_* folder that
    also holds an appstatus_* marker and hidden Hadoop .crc checksum files, which must be
    skipped: only the events_<N>_* parts are the log, and they must be read in order of N.
    """
    files = [p for p in job_dir.rglob("*") if p.is_file()]
    parts = [p for p in files if p.name.startswith("events_")]
    if parts:
        return sorted(parts, key=lambda p: int(p.name.split("_")[1]))
    return [p for p in files if not p.name.startswith((".", "appstatus")) and p.suffix != ".crc"]


def events(job_dir: Path):
    for path in log_files(job_dir):
        if path.suffix == ".zstd":
            with path.open("rb") as fh, zstandard.ZstdDecompressor().stream_reader(fh) as r:
                lines = r.read().splitlines()
        else:
            lines = path.read_bytes().splitlines()
        for line in lines:
            if line.strip():
                yield orjson.loads(line)


def summarize(job_dir: Path) -> None:
    tasks_by_stage: dict[int, list[dict]] = defaultdict(list)
    plans: dict[int, str] = {}

    for ev in events(job_dir):
        name = ev["Event"]
        if name == "SparkListenerTaskEnd" and "Task Metrics" in ev:
            info, m = ev["Task Info"], ev["Task Metrics"]
            sr = m["Shuffle Read Metrics"]
            tasks_by_stage[ev["Stage ID"]].append(
                {
                    "ms": info["Finish Time"] - info["Launch Time"],
                    "shuffle_read": sr["Local Bytes Read"] + sr["Remote Bytes Read"],
                    "disk_spill": m["Disk Bytes Spilled"],
                    "mem_spill": m["Memory Bytes Spilled"],
                    "out_bytes": m["Output Metrics"]["Bytes Written"],
                    "out_records": m["Output Metrics"]["Records Written"],
                }
            )
        elif name.endswith("SQLExecutionStart") or name.endswith("SQLAdaptiveExecutionUpdate"):
            plans[ev["executionId"]] = ev["physicalPlanDescription"]  # last one wins = final plan

    all_tasks = [t for ts in tasks_by_stage.values() for t in ts]
    print(f"\n=== {job_dir.name}  ({len(all_tasks)} tasks, {len(tasks_by_stage)} stages)")

    # Skew: the stage where the slowest task loses the most time versus the median task.
    # (Ranking by time lost, not by ratio, ignores tiny stages where 700 ms vs 130 ms is noise.)
    worst = None
    for stage, ts in tasks_by_stage.items():
        if len(ts) < 3:
            continue
        durations = [t["ms"] for t in ts]
        lost = max(durations) - statistics.median(durations)
        if worst is None or lost > worst[0]:
            worst = (lost, stage, ts)
    if worst:
        _, stage, ts = worst
        durations = [t["ms"] for t in ts]
        reads = [t["shuffle_read"] for t in ts]
        med_ms = statistics.median(durations)
        print(
            f"  skew      stage {stage}: max task {max(durations)} ms vs median {med_ms:.0f} ms "
            f"({max(durations) / max(med_ms, 1):.1f}x); shuffle read max {max(reads) / MB:.1f} MB "
            f"vs median {statistics.median(reads) / MB:.2f} MB"
        )

    disk = sum(t["disk_spill"] for t in all_tasks)
    mem = sum(t["mem_spill"] for t in all_tasks)
    print(f"  spill     disk {disk / MB:.1f} MB, memory {mem / MB:.1f} MB")

    writers = [t for t in all_tasks if t["out_records"] > 0]
    if writers:
        med = statistics.median(t["out_bytes"] for t in writers)
        print(f"  writes    {len(writers)} write tasks, median {med / KB:.1f} KB per task")

    biggest_stage, ts = max(tasks_by_stage.items(), key=lambda kv: len(kv[1]))
    reads = [t["shuffle_read"] for t in ts]
    if any(reads):
        print(
            f"  partitions stage {biggest_stage}: {len(ts)} tasks, median shuffle read "
            f"{statistics.median(reads) / KB:.1f} KB per task (target ~128 MB)"
        )

    for exec_id, plan in sorted(plans.items()):
        pushed = re.findall(r"PushedFilters: (\[.*?\])", plan)
        python_udf = bool(re.search(r"(Batch|Arrow)EvalPython", plan))
        files_read = re.findall(r"number of files read: (\d+)", plan)
        if pushed or python_udf:
            print(
                f"  plan      query {exec_id}: PushedFilters={pushed[0] if pushed else '-'}"
                f"  PythonUDF={'yes' if python_udf else 'no'}"
                + (f"  files read={files_read[0]}" if files_read else "")
            )


def main() -> None:
    if not LOGS.exists():
        sys.exit(f"No gallery logs in {LOGS}. Run: python lab/gallery/run_all.py")
    for job_dir in sorted(p for p in LOGS.iterdir() if p.is_dir()):
        summarize(job_dir)


if __name__ == "__main__":
    main()
