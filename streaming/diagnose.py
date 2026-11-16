import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = os.environ["S3_BUCKET"]

spark = (
    SparkSession.builder.appName("diagnose")
    .config("spark.sql.session.timeZone", "Asia/Seoul")
    .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION", "ap-northeast-2"))
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

v = F.col("value").cast("string")
source = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "trades.raw,trades.backfill")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
    .select(
        "topic",
        F.get_json_object(v, "$.code").alias("code"),
        F.get_json_object(v, "$.trade_timestamp").cast("long").alias("ts"),
        F.get_json_object(v, "$.sequential_id").alias("seq"),
    )
    .filter("code = 'KRW-BTC'")
    .withColumn("minute", F.date_trunc("minute", F.timestamp_millis("ts")))
    .filter(F.col("minute") >= "2026-11-11 15:55:00")
    .groupBy("minute")
    .agg(
        F.count_distinct(F.when(F.col("topic") == "trades.raw", F.col("seq"))).alias("realtime"),
        F.count_distinct(F.when(F.col("topic") == "trades.backfill", F.col("seq"))).alias("backfill"),
        F.count_distinct("seq").alias("source_trades"),
    )
)


def read_candles(folder, column):
    """S3의 봉 폴더를 읽는다. 폴더가 아직 없으면 빈 표를 돌려준다."""
    try:
        return (
            spark.read.parquet(f"s3a://{BUCKET}/{folder}")
            .filter("code = 'KRW-BTC'")
            .select("minute", F.col("trades").alias(column))
        )
    except Exception:
        return spark.createDataFrame([], f"minute timestamp, {column} long")


# 최종 봉: 백필 봉이 있으면 그것을, 없으면 실시간 봉을 사용
stream = read_candles("candles", "stream_trades")
filled = read_candles("candles_backfill", "backfill_trades")
saved = stream.join(filled, "minute", "full").select(
    "minute", F.coalesce("backfill_trades", "stream_trades").alias("saved_trades")
)

joined = source.join(saved, "minute", "left").cache()

print("=== 최근 40분 ===")
joined.orderBy(F.desc("minute")).limit(40).orderBy("minute").show(40, False)

# 가장 최근 2분은 아직 확정 전이라 판정에서 제외
cutoff = F.expr("current_timestamp() - INTERVAL 3 MINUTES")
done = joined.filter(F.col("minute") < cutoff)

span = done.agg(
    ((F.unix_timestamp(F.max("minute")) - F.unix_timestamp(F.min("minute"))) / 60 + 1).cast("int").alias("total_minutes"),
    F.count("*").alias("minutes_with_source"),
    F.sum(F.when(F.col("saved_trades").isNull(), 1).otherwise(0)).alias("not_processed"),
    F.sum(F.when(F.col("saved_trades") != F.col("source_trades"), 1).otherwise(0)).alias("count_mismatch"),
).withColumn("no_source", F.col("total_minutes") - F.col("minutes_with_source"))

print("=== 요약 (KRW-BTC) ===")
span.select("total_minutes", "no_source", "not_processed", "count_mismatch").show()

problems = done.filter(F.col("saved_trades").isNull() | (F.col("saved_trades") != F.col("source_trades")))
if problems.count() > 0:
    print("=== 문제가 있는 분 ===")
    problems.orderBy("minute").show(100, False)
