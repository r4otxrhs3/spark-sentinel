"""Create the input datasets the gallery jobs read. Event logging is OFF here on purpose:
this is setup, not something Sentinel should analyse.

  lab/data/events  ~2M rows of fake sales events in 8 Parquet files (healthy layout)
  lab/data/tiny    the same kind of rows split into 1,000 tiny Parquet files (bad layout)
"""

from _spark import DATA
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

COUNTRIES = ["IN", "US", "GB", "DE", "FR", "BR", "JP", "SG", "AU", "CA"]


def sales(spark: SparkSession, rows: int):
    country = F.element_at(
        F.array(*[F.lit(c) for c in COUNTRIES]), (F.col("id") % len(COUNTRIES) + 1).cast("int")
    )
    return spark.range(rows).select(
        F.col("id").alias("order_id"),
        (F.col("id") * 7919 % 100_000).alias("user_id"),
        country.alias("country"),
        F.round(F.rand(seed=42) * 500, 2).alias("amount"),
        (F.lit(1_700_000_000) + F.col("id")).cast("timestamp").alias("ts"),
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("gallery-make-data")
        .master("local[4]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    sales(spark, 2_000_000).repartition(8).write.mode("overwrite").parquet(str(DATA / "events"))
    sales(spark, 100_000).repartition(1000).write.mode("overwrite").parquet(str(DATA / "tiny"))

    spark.stop()
    print(f"Wrote {DATA / 'events'} and {DATA / 'tiny'}")


if __name__ == "__main__":
    main()
