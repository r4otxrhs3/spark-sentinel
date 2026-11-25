"""A small daily sales report, plus one-line "pull requests" that change it. For testing
`sentinel diff`: run the report as it is (the baseline), then with one change, and compare.

    python lab/diff/sales_report.py                         # baseline -> lab/eventlogs/diff/base
    python lab/diff/sales_report.py --change no_broadcast   # -> lab/eventlogs/diff/no_broadcast
    python lab/diff/run_all.py                              # every variant, plus a rerun

The report runs three queries:
  1. revenue by user segment and country: events joined with a small users table
  2. the 100 biggest spenders
  3. orders by size band

Changes (each is what a real pull request might do by accident):
  no_broadcast  turns off broadcast joins: query 1's broadcast join becomes a sort-merge join,
                which shuffles the 2M events instead of sending 100k users to every task
  python_udf    query 3 computes the size band in a row-by-row Python UDF instead of a
                built-in expression
  extra_query   adds a query before the others (a count for a log line), so every query
                after it gets a new execution id: matching must not rely on ids
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "gallery"))
from _spark import DATA, OUT, ROOT, start
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

LOGS = ROOT / "lab" / "eventlogs" / "diff"
CHANGES = ["no_broadcast", "python_udf", "extra_query"]


@F.udf(returnType=StringType(), useArrow=False)
def size_band_udf(amount: float) -> str:
    if amount < 50:
        return "small"
    return "medium" if amount < 250 else "large"


def size_band(col):
    return F.when(col < 50, "small").when(col < 250, "medium").otherwise("large")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--change", choices=CHANGES)
    parser.add_argument("--name", help="Log folder name (default: the change, or 'base').")
    args = parser.parse_args()
    change = args.change

    conf = {"spark.sql.shuffle.partitions": "16"}
    if change == "no_broadcast":
        conf["spark.sql.autoBroadcastJoinThreshold"] = "-1"
    spark = start(args.name or change or "base", conf, logs=LOGS, app_name="sales-report")

    events = spark.read.parquet(str(DATA / "events"))
    users = spark.range(100_000).select(
        F.col("id").alias("user_id"),
        F.element_at(
            F.array(*[F.lit(s) for s in ("new", "regular", "vip")]),
            (F.col("id") % 3 + 1).cast("int"),
        ).alias("segment"),
    )

    if change == "extra_query":
        print(f"{events.count()} events")

    # 1. revenue by segment and country (a broadcast join: users is small)
    (
        events.join(users, "user_id")
        .groupBy("segment", "country")
        .agg(F.sum("amount").alias("revenue"), F.count("*").alias("orders"))
        .write.mode("overwrite")
        .parquet(str(OUT / "report" / "revenue"))
    )

    # 2. the 100 biggest spenders
    (
        events.groupBy("user_id")
        .agg(F.sum("amount").alias("spent"))
        .orderBy(F.desc("spent"))
        .limit(100)
        .write.mode("overwrite")
        .parquet(str(OUT / "report" / "top_users"))
    )

    # 3. orders by size band
    band = size_band_udf(F.col("amount")) if change == "python_udf" else size_band(F.col("amount"))
    (
        events.select(band.alias("band"))
        .groupBy("band")
        .count()
        .write.mode("overwrite")
        .parquet(str(OUT / "report" / "bands"))
    )

    spark.stop()


if __name__ == "__main__":
    main()
