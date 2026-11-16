from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("show").config("spark.sql.session.timeZone", "Asia/Seoul").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("/app/data/candles")

print("=== 최근 봉 ===")
df.orderBy(F.desc("minute"), "code").show(12, False)

print("=== 코인별 점검 ===")
(
    df.groupBy("code")
    .agg(
        F.min("minute").alias("first"),
        F.max("minute").alias("last"),
        F.count("*").alias("candles"),
        F.count_distinct("minute").alias("distinct_minutes"),
    )
    .withColumn("expected", ((F.unix_timestamp("last") - F.unix_timestamp("first")) / 60 + 1).cast("int"))
    .withColumn("missing", F.col("expected") - F.col("distinct_minutes"))
    .withColumn("duplicates", F.col("candles") - F.col("distinct_minutes"))
    .orderBy("code")
    .show(truncate=False)
)