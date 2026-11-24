"""SMALL FILES: reads 1,000 tiny files and writes 500 more.

Expected signal:
- read side: the scan reports ~1,000 files read for very few bytes
- write side: ~500 write tasks, each writing only a few KB  (many tiny output files)

This is the classic result of `.repartition(N)` with a big N before a write, or of a
streaming/incremental job that appends a little data very often.
"""

from _spark import DATA, OUT, start
from pyspark.sql import functions as F


def main() -> None:
    spark = start("small_files")

    tiny = spark.read.parquet(str(DATA / "tiny"))
    result = tiny.withColumn("amount_usd", F.round(F.col("amount") / 83.0, 2))
    result.repartition(500).write.mode("overwrite").parquet(str(OUT / "small_files"))

    spark.stop()


if __name__ == "__main__":
    main()
