"""PYTHON UDF: a row-at-a-time (pickled) Python UDF computes a column.

Every row is pickled, sent to a Python worker, processed one at a time and sent back. Arrow
Python UDFs (useArrow=True, or spark.sql.execution.pythonUDF.arrow.enabled) and pandas UDFs
move data in columnar batches instead; built-in functions avoid Python entirely.

Arrow UDFs are on by default from Spark 4.2 (and silently fall back to pickled UDFs when pandas
or PyArrow is missing), so this UDF opts out with useArrow=False to behave the same on every
version. (Setting spark.sql.execution.pythonUDF.arrow.enabled=false would also work, but that is
already the default before Spark 4.2.)

Expected signal: the final plan has a `BatchEvalPython` node (not `ArrowEvalPython`) that
isn't just a filter (that case is PLAN001).
"""

from _spark import DATA, OUT, start
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


@F.udf(returnType=StringType(), useArrow=False)
def size_band(amount: float) -> str:
    if amount < 50:
        return "small"
    return "medium" if amount < 250 else "large"


def main() -> None:
    spark = start("python_udf")

    events = spark.read.parquet(str(DATA / "events"))
    banded = events.withColumn("band", size_band(F.col("amount")))
    result = banded.groupBy("country", "band").agg(F.count("*").alias("orders"))
    result.write.mode("overwrite").parquet(str(OUT / "python_udf"))

    spark.stop()


if __name__ == "__main__":
    main()
