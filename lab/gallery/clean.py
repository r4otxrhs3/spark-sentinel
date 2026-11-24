"""CLEAN: a healthy job. Sentinel must report NOTHING for this one (Step 9's false-alarm test).

- reads well-sized Parquet files
- filters with a built-in expression, so the filter is pushed down into the Parquet scan
- one shuffle with evenly spread keys; AQE left on to size the partitions
- writes a handful of output files
"""

from _spark import DATA, OUT, start
from pyspark.sql import functions as F


def main() -> None:
    spark = start("clean")

    events = spark.read.parquet(str(DATA / "events"))
    result = (
        events.filter(F.col("country") == "IN")
        .groupBy("user_id")
        .agg(F.count("*").alias("orders"), F.sum("amount").alias("revenue"))
    )
    result.coalesce(4).write.mode("overwrite").parquet(str(OUT / "clean"))

    spark.stop()


if __name__ == "__main__":
    main()
