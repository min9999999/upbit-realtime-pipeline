import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType

from date_plan import parse_kst, plan_days

URL = "https://api.upbit.com/v1/trades/ticks"
CODES = ["KRW-BTC", "KRW-ETH", "KRW-XRP"]
KAFKA = "kafka:29092"
BACKFILL_TOPIC = "trades.backfill"
OUTPUT = f"s3a://{os.environ['S3_BUCKET']}/candles_backfill"


def request(params):
    while True:
        req = urllib.request.Request(
            URL + "?" + urllib.parse.urlencode(params),
            headers={"accept": "application/json", "User-Agent": "backfill-job"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as res:
                return json.load(res)
        except urllib.error.HTTPError as e:
            if e.code == 429:           # 요청 제한에 걸리면 잠시 대기
                time.sleep(1)
                continue
            raise


def fetch(code, start, end):
    """start 이상 end 미만 구간의 체결 수집.

    업비트 API가 UTC 날짜 단위로 동작하므로 날짜별로 나눠 조회한다.
    나누는 계산은 date_plan.plan_days가 맡는다.
    """
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)

    plans = plan_days(start, end)
    if not plans:
        print("조회할 수 있는 날짜가 없습니다. 7일이 지난 구간인지 확인하세요.")
        return []

    trades = []
    for plan in plans:
        params = {"market": code, "count": 500}
        if plan["days_ago"] > 0:
            params["days_ago"] = plan["days_ago"]
        if plan["to"] is not None:      # 구간이 이 날짜 안에서 끝날 때만 끝 시각 지정
            params["to"] = plan["to"]

        while True:
            rows = request(params)
            if not rows:
                break
            trades.extend(t for t in rows if start_ms <= t["timestamp"] < end_ms)
            if rows[-1]["timestamp"] < plan["stop_ms"]:
                break
            params["cursor"] = rows[-1]["sequential_id"]
            time.sleep(0.15)

    return trades


def load_trades(spark, start, end):
    """1단계: 업비트에서 체결을 받아 Kafka 백필 토픽에 적재"""
    messages = []
    for code in CODES:
        trades = fetch(code, start, end)
        for t in trades:
            messages.append((code, json.dumps({
                "type": "trade",
                "code": code,
                "trade_timestamp": t["timestamp"],
                "trade_price": t["trade_price"],
                "trade_volume": t["trade_volume"],
                "ask_bid": t["ask_bid"],
                "sequential_id": t["sequential_id"],
                "stream_type": "BACKFILL",
            })))
        print(f"{code}: {len(trades)}건 수집")

    if not messages:
        print("받아 온 체결이 없습니다.")
        return
    (
        spark.createDataFrame(messages, "key string, value string")
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("topic", BACKFILL_TOPIC)
        .save()
    )
    print(f"총 {len(messages)}건을 {BACKFILL_TOPIC} 토픽에 적재")


def build_candles(spark):
    """2단계: 백필 토픽의 체결로 백필 봉 생성"""
    schema = StructType([
        StructField("code", StringType()),
        StructField("trade_timestamp", LongType()),
        StructField("trade_price", DecimalType(24, 8)),
        StructField("trade_volume", DecimalType(24, 8)),
        StructField("ask_bid", StringType()),
        StructField("sequential_id", LongType()),
    ])

    trades = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", f"trades.raw,{BACKFILL_TOPIC}")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select("topic", F.from_json(F.col("value").cast("string"), schema).alias("t"))
        .select("topic", "t.*")
        .withColumn("minute", F.date_trunc("minute", F.timestamp_millis("trade_timestamp")))
    )

    targets = trades.filter(F.col("topic") == BACKFILL_TOPIC).select("code", "minute").distinct()

    order_key = F.struct("trade_timestamp", "sequential_id")
    candles = (
        trades.dropDuplicates(["code", "sequential_id"])
        .join(targets, ["code", "minute"], "left_semi")
        .groupBy("minute", "code")
        .agg(
            F.min_by("trade_price", order_key).alias("open"),
            F.max("trade_price").alias("high"),
            F.min("trade_price").alias("low"),
            F.max_by("trade_price", order_key).alias("close"),
            F.sum("trade_volume").alias("volume"),
            F.count("*").alias("trades"),
        )
        .withColumn("date", F.to_date("minute"))
    )

    candles.coalesce(1).write.mode("overwrite").partitionBy("date").parquet(OUTPUT)

    print(f"=== 백필로 만든 봉 수 ({OUTPUT}) ===")
    spark.read.parquet(OUTPUT).groupBy("code").count().orderBy("code").show()
    print("백필 완료")


def main():
    if len(sys.argv) not in (1, 3):
        print('사용법 1: backfill.py "2026-11-11 16:22" "2026-11-11 16:40"   (체결 수집 + 봉 생성)')
        print("사용법 2: backfill.py                                        (봉만 다시 생성)")
        sys.exit(1)

    spark = (
        SparkSession.builder.appName("backfill")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION", "ap-northeast-2"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    if len(sys.argv) == 3:
        load_trades(spark, parse_kst(sys.argv[1]), parse_kst(sys.argv[2]))
    build_candles(spark)


if __name__ == "__main__":
    main()
