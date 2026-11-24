from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

log_dir = Path("lab/eventlogs").resolve()
log_dir.mkdir(parents=True, exist_ok=True)  # Spark fails if the directory is missing

spark = (
    SparkSession.builder.appName("hello-sentinel")
    .master("local[4]")  # 4 threads on your machine
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", log_dir.as_uri())  # file:///.../lab/eventlogs
    .config("spark.eventLog.compress", "false")  # see the warning below
    .config("spark.eventLog.rolling.enabled", "false")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

df = spark.range(0, 5_000_000).withColumn("key", F.col("id") % 100)
agg = df.groupBy("key").agg(F.count("*").alias("n"), F.sum("id").alias("total"))
agg.write.mode("overwrite").parquet("lab/out/hello")

spark.stop()  # writes the ApplicationEnd event; without it the log stays ".inprogress"
