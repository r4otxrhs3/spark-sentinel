"""FEW TASKS: a long, CPU-heavy stage with fewer tasks than cores, so cores sit idle.

The data is generated in 2 partitions on a 4-core machine: 2 cores do all the hashing while 2
do nothing. The Tuning guide recommends 2-3 tasks per CPU core.

Expected signal: a stage that runs for several seconds with 2 tasks while 4 cores are available.
"""

from _spark import OUT, start
from pyspark.sql import functions as F


def main() -> None:
    spark = start("few_tasks")

    rows = spark.range(0, 6_000_000, 1, numPartitions=2)
    digest = F.col("id").cast("string")
    for _ in range(6):  # deliberately expensive per row: chained SHA-256 hashes
        digest = F.sha2(digest, 256)
    hashed = rows.select((F.conv(F.substring(digest, 1, 6), 16, 10) % 1000).alias("bucket"))
    result = hashed.agg(F.count("*").alias("rows"), F.countDistinct("bucket").alias("buckets"))
    result.write.mode("overwrite").parquet(str(OUT / "few_tasks"))

    spark.stop()


if __name__ == "__main__":
    main()
