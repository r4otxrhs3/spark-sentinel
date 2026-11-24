"""SKEW: one join key holds ~70% of the rows, so one task does most of the work.

Expected signal (stage that reads the join shuffle):
- slowest task many times longer than the median task
- that task's shuffle read bytes many times the median   -> DATA skew, not compute skew

Settings that keep the skew visible on purpose:
- autoBroadcastJoinThreshold=-1   forces a sort-merge join (with a shuffle) instead of a broadcast
- adaptive.skewJoin.enabled=false stops AQE from splitting the hot partition for us
- adaptive.coalescePartitions.enabled=false stops AQE from merging the 15 small partitions
  together (on toy data they're tiny; on real data they'd be big enough not to be merged)
"""

from _spark import OUT, start
from pyspark.sql import functions as F


def main() -> None:
    spark = start(
        "skew",
        {
            "spark.sql.autoBroadcastJoinThreshold": "-1",
            "spark.sql.adaptive.skewJoin.enabled": "false",
            "spark.sql.adaptive.coalescePartitions.enabled": "false",
            "spark.sql.shuffle.partitions": "16",
            "spark.driver.memory": "2g",  # enough memory that the hot task doesn't also spill
        },
    )

    # 10M orders: 70% belong to customer 0, the rest spread over 1,000 customers.
    orders = spark.range(10_000_000).select(
        F.col("id").alias("order_id"),
        F.when(F.rand(seed=1) < 0.7, F.lit(0))
        .otherwise((F.rand(seed=2) * 1000).cast("int") + 1)
        .alias("customer_id"),
        (F.rand(seed=3) * 100).alias("amount"),
    )
    customers = spark.range(0, 1001).select(
        F.col("id").cast("int").alias("customer_id"),
        F.concat(F.lit("segment-"), (F.col("id") % 5).cast("string")).alias("segment"),
    )

    result = (
        orders.join(customers, "customer_id")
        .groupBy("segment")
        .agg(F.sum("amount").alias("revenue"), F.count("*").alias("orders"))
    )
    result.write.mode("overwrite").parquet(str(OUT / "skew"))

    spark.stop()


if __name__ == "__main__":
    main()
