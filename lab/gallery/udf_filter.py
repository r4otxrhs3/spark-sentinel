"""UDF FILTER: a Python UDF in a filter blocks predicate pushdown.

This does the same thing as clean.py's filter (country == "IN"), but through a Python UDF.
Spark can't look inside the UDF, so:
- the Parquet scan shows `PushedFilters: []` instead of `[IsNotNull(country), EqualTo(country,IN)]`
- every row is read, shipped to a Python worker, and only then filtered
- the plan gains a BatchEvalPython (or ArrowEvalPython) node

Expected signal: plan text contains a *EvalPython node feeding a Filter, and the scan
has no pushed filters.
"""

from _spark import DATA, OUT, start
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType


@F.udf(returnType=BooleanType())
def is_india(country: str) -> bool:
    return country == "IN"


def main() -> None:
    spark = start("udf_filter")

    events = spark.read.parquet(str(DATA / "events"))
    result = (
        events.filter(is_india(F.col("country")))
        .groupBy("user_id")
        .agg(F.count("*").alias("orders"), F.sum("amount").alias("revenue"))
    )
    result.coalesce(4).write.mode("overwrite").parquet(str(OUT / "udf_filter"))

    spark.stop()


if __name__ == "__main__":
    main()
