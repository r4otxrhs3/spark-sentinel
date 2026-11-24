"""SPILL: a big sort squeezed into too little memory, so tasks spill to disk.

Expected signal: Memory Bytes Spilled and Disk Bytes Spilled > 0 in the sort stage.

How it is forced:
- spark.driver.memory=1g  (in local mode the driver IS the executor)
- only 4 shuffle partitions with AQE coalescing off, so each task sorts a large chunk
- wide rows (a padded string column) so the data is big in memory
"""

from _spark import OUT, start
from pyspark.sql import functions as F


def main() -> None:
    spark = start(
        "spill",
        {
            "spark.driver.memory": "1g",
            "spark.sql.shuffle.partitions": "4",
            "spark.sql.adaptive.coalescePartitions.enabled": "false",
        },
    )

    rows = spark.range(6_000_000).select(
        F.col("id"),
        (F.rand(seed=7) * 1e9).cast("long").alias("sort_key"),
        F.sha2(F.col("id").cast("string"), 256).alias("payload"),
    )
    rows.orderBy("sort_key").write.mode("overwrite").parquet(str(OUT / "spill"))

    spark.stop()


if __name__ == "__main__":
    main()
