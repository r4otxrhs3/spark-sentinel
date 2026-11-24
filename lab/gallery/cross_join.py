"""CROSS JOIN: a join with no equality condition becomes a cartesian product.

Every row of one side is paired with every row of the other: 3,000 x 3,000 = 9M pairs,
then filtered down. This usually happens by accident (a join condition that compares with
< or >, or forgets the key), and its cost grows with the PRODUCT of the input sizes.

Expected signal: the plan contains a CartesianProduct node (or BroadcastNestedLoopJoin
when one side is small enough to broadcast; broadcasting is disabled here to show the
worst case).
"""

from _spark import OUT, start
from pyspark.sql import functions as F


def main() -> None:
    spark = start("cross_join", {"spark.sql.autoBroadcastJoinThreshold": "-1"})

    a = spark.range(3000).select(F.col("id").alias("a_id"), (F.col("id") % 97).alias("a_val"))
    b = spark.range(3000).select(F.col("id").alias("b_id"), (F.col("id") % 89).alias("b_val"))

    # Non-equi condition: Spark can't hash or sort-merge on it, so it compares all pairs.
    pairs = a.join(b, F.col("a_val") < F.col("b_val") - 80)
    pairs.groupBy("a_val").count().write.mode("overwrite").parquet(str(OUT / "cross_join"))

    spark.stop()


if __name__ == "__main__":
    main()
