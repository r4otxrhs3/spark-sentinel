"""Run the whole gallery: make the input data (if missing), then every job, one Spark app each.

    python lab/gallery/run_all.py              # all jobs
    python lab/gallery/run_all.py skew spill   # just these
    python lab/gallery/run_all.py --remake-data

Each job runs in its own Python process, so per-job settings like spark.driver.memory apply.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _spark import DATA, LOGS  # noqa: E402

JOBS = [
    "clean",
    "skew",
    "spill",
    "udf_filter",
    "small_files",
    "over_partitioned",
    "cross_join",
]


def run(script: str) -> bool:
    start = time.perf_counter()
    print(f"-> {script:<18}", end="", flush=True)
    proc = subprocess.run(
        [sys.executable, str(HERE / f"{script}.py")], capture_output=True, text=True
    )
    elapsed = time.perf_counter() - start
    if proc.returncode != 0:
        print(f"FAILED after {elapsed:.0f}s\n{proc.stderr[-3000:]}")
        return False
    print(f"ok  {elapsed:5.1f}s")
    return True


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    unknown = [a for a in args if a not in JOBS]
    if unknown:
        print(f"Unknown job(s): {unknown}. Choose from: {JOBS}")
        return 2

    need_data = not (DATA / "events").exists() or not (DATA / "tiny").exists()
    if ("--remake-data" in sys.argv or need_data) and not run("make_data"):
        return 1

    failed = [job for job in (args or JOBS) if not run(job)]
    print(f"\nEvent logs: {LOGS}")
    print("Summary:    python lab/gallery/summarize.py")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
