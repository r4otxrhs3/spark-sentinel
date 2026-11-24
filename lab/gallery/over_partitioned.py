"""OVER-PARTITIONED: 1,000 shuffle partitions for a few MB of shuffle data.

Expected signal: the post-shuffle stage has ~1,000 tasks, each reading only a few KB
(target is ~128 MB per task), so scheduling overhead dwarfs the real work.

AQE would normally merge these tiny partitions, so its coalescing is switched off here,
which is what an older Spark version or a hard-coded config would do.
"""

from _spark import DATA, OUT, start
from pyspark.sql import functions as F


def main() -> None:
    spark = start(
        "over_partitioned",
        {
            "spark.sql.shuffle.partitions": "1000",
            "spark.sql.adaptive.coalescePartitions.enabled": "false",
        },
    )

    events = spark.read.parquet(str(DATA / "events"))
    per_user = events.groupBy("user_id").agg(F.sum("amount").alias("spend"))  # 1,000 tiny tasks
    # A second, global aggregation gives a one-row result, so the write itself stays small.
    # (Don't use .coalesce(1) here: it would collapse the 1,000-task stage into a single task.)
    summary = per_user.agg(
        F.count("*").alias("users"), F.avg("spend").alias("avg_spend"), F.max("spend").alias("top")
    )
    summary.write.mode("overwrite").parquet(str(OUT / "over_partitioned"))

    spark.stop()


if __name__ == "__main__":
    main()
