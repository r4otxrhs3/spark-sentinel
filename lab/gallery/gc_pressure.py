"""GC PRESSURE: the JVM's young generation is far too small, so it collects constantly.

The Tuning guide's "too many minor GCs" case: new objects are created in Eden, and when Eden is
tiny every burst of allocation triggers a collection. Here -Xmn2m pins the young generation to
2 MB while collect_list builds per-key arrays of long strings, which allocates a lot.

Expected signal: a stage where JVM GC time is a large share of task run time.
"""

from _spark import OUT, start
from pyspark.sql import functions as F


def main() -> None:
    spark = start(
        "gc_pressure",
        {
            "spark.driver.memory": "1g",
            "spark.driver.extraJavaOptions": "-Xmn2m",
            "spark.sql.shuffle.partitions": "4",
            "spark.sql.adaptive.coalescePartitions.enabled": "false",
        },
    )

    rows = spark.range(4_000_000).select(
        (F.col("id") % 2000).alias("key"),
        F.repeat(F.col("id").cast("string"), 8).alias("text"),
    )
    grouped = rows.groupBy("key").agg(F.collect_list("text").alias("texts"))
    result = grouped.select("key", F.size("texts").alias("n"), F.length(F.concat_ws("", "texts")))
    result.write.mode("overwrite").parquet(str(OUT / "gc_pressure"))

    spark.stop()


if __name__ == "__main__":
    main()
