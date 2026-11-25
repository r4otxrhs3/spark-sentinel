"""Run the sales report as a baseline, again unchanged (the noise check), and once per change.

The first run after a pause is slow (cold JVM and disk caches) whatever the code, so it runs
first as "cold": compare base with cold to see how diff treats a slowdown with no cause.

    python lab/diff/run_all.py
    sentinel diff lab/eventlogs/diff/base lab/eventlogs/diff/no_broadcast --profile laptop

Each run is a separate Python process (a fresh JVM), like separate CI runs would be.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "gallery"))
from _spark import DATA  # noqa: E402

RUNS = [
    ("cold", ["--name", "cold"]),
    ("base", []),
    ("rerun", ["--name", "rerun"]),
    ("no_broadcast", ["--change", "no_broadcast"]),
    ("python_udf", ["--change", "python_udf"]),
    ("extra_query", ["--change", "extra_query"]),
]


def main() -> int:
    if not (DATA / "events").exists():
        print("No input data yet: run python lab/gallery/run_all.py clean first.")
        return 1
    wanted = sys.argv[1:] or [name for name, _ in RUNS]
    failed = []
    for name, extra in RUNS:
        if name not in wanted:
            continue
        start = time.perf_counter()
        print(f"-> {name:<14}", end="", flush=True)
        proc = subprocess.run(
            [sys.executable, str(HERE / "sales_report.py"), *extra],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            print(f"FAILED\n{proc.stderr[-3000:]}")
            failed.append(name)
        else:
            print(f"ok  {time.perf_counter() - start:5.1f}s")
    print("\nCompare:  sentinel diff lab/eventlogs/diff/base lab/eventlogs/diff/<run> \\")
    print("            --profile laptop")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
