"""
count_trades.py - 운영 구간의 총 수집 체결 건수를 센다

Kafka의 trades.raw / trades.backfill을 읽어 지정한 기간의 체결 건수를
코인별·전체로 집계한다. Kafka 데이터를 지우기 전에 한 번 돌려 기록을 남긴다.

실행:
    docker exec -it spark /opt/spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2 \
        /app/streaming/count_trades.py
"""

from pyspark.sql import SparkSession, functions as F

# 3일 운영 구간 (KST)
START = "2026-11-13 01:50:00"
END = "2026-11-16 02:30:00"

spark = (
    SparkSession.builder.appName("count-trades")
    .config("spark.sql.session.timeZone", "Asia/Seoul")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

v = F.col("value").cast("string")

df = (
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
    .withColumn("event_time", F.timestamp_millis("ts"))
)

run = df.filter((F.col("event_time") >= START) & (F.col("event_time") < END)).cache()

# sequential_id는 코인별로 매겨지므로 전체 집계에는 (code, seq) 쌍을 쓴다
pair = F.concat_ws("|", F.col("code"), F.col("seq"))

print(f"\n=== 운영 구간: {START} ~ {END} ===")

print("\n--- 코인별 ---")
(
    run.groupBy("code")
    .agg(
        F.count_distinct("seq").alias("체결건수"),
        F.min("event_time").alias("처음"),
        F.max("event_time").alias("마지막"),
    )
    .orderBy("code")
    .show(truncate=False)
)

print("--- 토픽별 ---")
run.groupBy("topic").agg(F.count_distinct(pair).alias("체결건수")).orderBy("topic").show(truncate=False)

print("--- 전체 ---")
(
    run.agg(
        F.count_distinct(pair).alias("총_체결건수"),
        F.count("*").alias("Kafka_메시지수"),
    ).show(truncate=False)
)

print("(총_체결건수 < Kafka_메시지수 이면 그 차이가 중복 전송분이며, 중복 제거로 걸러진 양이다)\n")

spark.stop()
