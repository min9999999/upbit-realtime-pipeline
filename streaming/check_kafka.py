from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("check-kafka").config("spark.sql.session.timeZone", "Asia/Seoul").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "trades.raw")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

ts = F.get_json_object(F.col("value").cast("string"), "$.trade_timestamp").cast("long")
(
    df.withColumn("minute", F.date_trunc("minute", F.timestamp_millis(ts)))
    .filter(F.col("minute") >= "2026-11-11 16:15:00")
    .groupBy("minute").count()
    .orderBy("minute")
    .show(100, False)
)