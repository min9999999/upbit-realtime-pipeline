import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType

BUCKET = os.environ["S3_BUCKET"]
TRADES_PATH = f"s3a://{BUCKET}/trades"
CANDLES_PATH = f"s3a://{BUCKET}/candles"

spark = (
    SparkSession.builder.appName("upbit-candles")
    .config("spark.sql.session.timeZone", "Asia/Seoul")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION", "ap-northeast-2"))
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("code", StringType()),
    StructField("trade_timestamp", LongType()),
    StructField("trade_price", DecimalType(24, 8)),
    StructField("trade_volume", DecimalType(24, 8)),
    StructField("ask_bid", StringType()),
    StructField("sequential_id", LongType()),
])

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "trades.raw")
    .option("startingOffsets", "earliest")
    .load()
)

trades = (
    raw.select(F.from_json(F.col("value").cast("string"), schema).alias("t"))
    .select("t.*")
    .withColumn("event_time", F.timestamp_millis("trade_timestamp"))
    .withWatermark("event_time", "30 seconds")
    .dropDuplicatesWithinWatermark(["code", "sequential_id"])
)

# ---------- 저장 1: 체결 원본 (날짜·코인별 파티션) ----------
trades_query = (
    trades.withColumn("date", F.to_date("event_time"))
    .coalesce(1)
    .writeStream.outputMode("append")
    .format("parquet")
    .option("path", TRADES_PATH)
    .option("checkpointLocation", "/app/checkpoints/trades")
    .partitionBy("date", "code")
    .trigger(processingTime="1 minute")
    .start()
)

# ---------- 저장 2: 1분봉 ----------
order_key = F.struct("trade_timestamp", "sequential_id")
candles = (
    trades.groupBy(F.window("event_time", "1 minute"), "code")
    .agg(
        F.min_by("trade_price", order_key).alias("open"),
        F.max("trade_price").alias("high"),
        F.min("trade_price").alias("low"),
        F.max_by("trade_price", order_key).alias("close"),
        F.sum("trade_volume").alias("volume"),
        F.count("*").alias("trades"),
    )
    .select(
        F.col("window.start").alias("minute"),
        "code", "open", "high", "low", "close", "volume", "trades",
    )
    .withColumn("date", F.to_date("minute"))
)

candles_query = (
    candles.coalesce(1)
    .writeStream.outputMode("append")
    .format("parquet")
    .option("path", CANDLES_PATH)
    .option("checkpointLocation", "/app/checkpoints/candles")
    .partitionBy("date")
    .trigger(processingTime="30 seconds")
    .start()
)

print(f"스트리밍 시작: 체결은 {TRADES_PATH}, 봉은 {CANDLES_PATH} 에 저장 중")
spark.streams.awaitAnyTermination()