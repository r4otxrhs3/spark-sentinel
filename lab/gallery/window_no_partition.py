"""WINDOW WITHOUT PARTITION: a window function over the whole dataset runs in ONE task.

A running total over `Window.orderBy(...)` has no partitionBy, so Spark must put every row in a
single partition to add them up in order (it logs "No Partition Defined for Window operation!").
pandas API on Spark does the same for DataFrame.rank() and the old "sequence" default index.

(A `row_number() ... <= N` filter would NOT show the problem: Spark 3.5+ applies a top-N
"window group limit" before the shuffle, so only N rows per partition reach the single task.)

Expected signal: the final plan has `Exchange SinglePartition` feeding a `Window` with an empty
partition spec, and the stage after it runs 1 task that reads all the shuffled data.
"""

from _spark import DATA, OUT, start
from pyspark.sql import Window
from pyspark.sql import functions as F


def main() -> None:
    spark = start("window_no_partition")

    events = spark.read.parquet(str(DATA / "events"))
    running_total = F.sum("amount").over(Window.orderBy("ts"))
    result = events.select("order_id", "ts", "amount", running_total.alias("running_total"))
    result.write.mode("overwrite").parquet(str(OUT / "window_no_partition"))

    spark.stop()


if __name__ == "__main__":
    main()
