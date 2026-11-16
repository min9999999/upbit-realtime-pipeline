"""
alerts.py - 1분봉 이상 징후 판정

S3의 candles/ 와 candles_backfill/ 을 읽어 이상 징후를 판정하고
s3://{BUCKET}/alerts/ 에 저장한다.

실행:
    docker exec -it spark /opt/spark/bin/spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.4.1 \
        /app/streaming/alerts.py

판정 규칙
    VOLUME_SPIKE : 직전 20분 대비 거래량 급증 (z-score + 평균 대비 배수, 둘 다 만족)
    PRICE_MOVE   : 직전 분 대비 가격 급변 (z-score + 최소 변동률, 둘 다 만족)

코인마다 변동성이 다르므로 고정 퍼센트가 아니라 직전 구간의 표준편차를 기준으로
판정한다. 다만 조용한 구간에서 표준편차가 작아지면 사소한 변화도 z-score가 커지므로,
최소 배수/최소 변동률을 AND 조건으로 함께 걸어 걸러낸다.
"""

import os
import sys

from pyspark.sql import SparkSession, Window, functions as F

# ---------------------------------------------------------------------------
# 경로
# ---------------------------------------------------------------------------
BUCKET = os.environ["S3_BUCKET"]
CANDLES_PATH = f"s3a://{BUCKET}/candles"
BACKFILL_PATH = f"s3a://{BUCKET}/candles_backfill"
ALERTS_PATH = f"s3a://{BUCKET}/alerts"

# 컨테이너는 UTC로 도는데 봉의 date 파티션과 진단 스크립트는 KST 기준이다.
# 맞추지 않으면 alerts의 date 파티션만 하루씩 어긋난다.
SESSION_TZ = "Asia/Seoul"

# ---------------------------------------------------------------------------
# candles 컬럼 이름 - 실제 스키마와 다르면 여기만 고친다
# ---------------------------------------------------------------------------
COL_CODE = "code"
COL_MINUTE = "minute"
COL_CLOSE = "close"
COL_VOLUME = "volume"

# ---------------------------------------------------------------------------
# 판정 기준
# ---------------------------------------------------------------------------
BASELINE_MINUTES = 20   # 비교 기준이 되는 직전 구간 (분)
MIN_BASELINE = 10       # 기준 구간에 봉이 이만큼은 있어야 판정한다

VOLUME_Z = 3.0          # 거래량 z-score 임계값
VOLUME_RATIO = 3.0      # 거래량 평균 대비 최소 배수

RETURN_Z = 3.0          # 수익률 z-score 임계값
RETURN_PCT = 0.15       # 최소 변동률 (%). 1분봉 수익률 표준편차가 0.05~0.15% 수준이라
                        # 0.3%로 두면 z를 넘겨도 전부 걸러진다.

HIGH_Z = 8.0            # 이 이상이면 심각도 HIGH

SUPPRESS_MINUTES = 10   # 같은 코인·같은 규칙은 이 간격 안에서 1건만 남긴다

# 최근 N일을 다시 판정해 해당 날짜 파티션을 덮어쓴다 (재실행해도 중복되지 않음)
DAYS_BACK = int(os.environ.get("ALERT_DAYS_BACK", "2"))


def read_candles(spark):
    """실시간 봉과 백필 봉을 함께 읽고, 같은 분이 겹치면 실시간을 우선한다."""
    frames = []
    for path, src in ((CANDLES_PATH, "realtime"), (BACKFILL_PATH, "backfill")):
        try:
            frames.append(spark.read.parquet(path).withColumn("_src", F.lit(src)))
            print(f"[read] {path}")
        except Exception:
            print(f"[skip] {path} - 읽을 수 없음 (아직 없을 수 있음)")

    if not frames:
        sys.exit("candles를 찾을 수 없습니다. S3 경로와 자격증명을 확인하세요.")

    df = frames[0]
    for other in frames[1:]:
        df = df.unionByName(other, allowMissingColumns=True)

    pick = Window.partitionBy(COL_CODE, COL_MINUTE).orderBy(
        F.when(F.col("_src") == "realtime", 0).otherwise(1)
    )
    return (
        df.withColumn("_rn", F.row_number().over(pick))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_src")
    )


def check_schema(df):
    required = [COL_CODE, COL_MINUTE, COL_CLOSE, COL_VOLUME]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print("\n실제 candles 컬럼:", df.columns)
        sys.exit(
            f"\n없는 컬럼: {missing}\n"
            f"alerts.py 상단의 COL_CODE / COL_MINUTE / COL_CLOSE / COL_VOLUME 를 "
            f"위 목록에 맞게 고치세요."
        )


def main():
    spark = SparkSession.builder.appName("upbit-alerts").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", SESSION_TZ)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    candles = read_candles(spark)
    check_schema(candles)

    # -----------------------------------------------------------------------
    # 기준선 계산
    # -----------------------------------------------------------------------
    c = candles.withColumn("_ts", F.col(COL_MINUTE).cast("long"))

    w_lag = Window.partitionBy(COL_CODE).orderBy("_ts")

    # 수집 공백이 있으므로 직전 봉이 정확히 1분 전일 때만 변동률을 계산한다.
    # (공백 건너편 봉과 비교하면 20분치 변동이 1분 변동으로 둔갑한다)
    c = (
        c.withColumn("_prev_close", F.lag(COL_CLOSE).over(w_lag))
        .withColumn("_prev_ts", F.lag("_ts").over(w_lag))
        .withColumn(
            "_ret_pct",
            F.when(
                (F.col("_ts") - F.col("_prev_ts") == 60) & (F.col("_prev_close") > 0),
                (F.col(COL_CLOSE) - F.col("_prev_close")) / F.col("_prev_close") * 100,
            ),
        )
    )

    # rowsBetween이 아니라 rangeBetween(초 단위)을 쓴다.
    # 공백이 있어도 "직전 20분"이라는 시간 범위가 유지된다.
    w_base = (
        Window.partitionBy(COL_CODE)
        .orderBy("_ts")
        .rangeBetween(-BASELINE_MINUTES * 60, -60)
    )

    c = (
        c.withColumn("_vol_avg", F.avg(COL_VOLUME).over(w_base))
        .withColumn("_vol_std", F.stddev_samp(COL_VOLUME).over(w_base))
        .withColumn("_ret_std", F.stddev_samp("_ret_pct").over(w_base))
        .withColumn("_base_n", F.count(COL_VOLUME).over(w_base))
    )

    # 1분 수익률의 평균은 0에 가까우므로 평균을 빼지 않고 절대값 / 표준편차로 본다
    c = (
        c.withColumn(
            "_vol_z",
            F.when(
                F.col("_vol_std") > 0,
                (F.col(COL_VOLUME) - F.col("_vol_avg")) / F.col("_vol_std"),
            ),
        )
        .withColumn(
            "_vol_ratio",
            F.when(F.col("_vol_avg") > 0, F.col(COL_VOLUME) / F.col("_vol_avg")),
        )
        .withColumn(
            "_ret_z",
            F.when(F.col("_ret_std") > 0, F.abs(F.col("_ret_pct")) / F.col("_ret_std")),
        )
    )
    c.cache()

    enough = F.col("_base_n") >= MIN_BASELINE

    vol_z_only = enough & (F.col("_vol_z") >= VOLUME_Z)
    vol_hit = vol_z_only & (F.col("_vol_ratio") >= VOLUME_RATIO)

    ret_z_only = enough & (F.col("_ret_z") >= RETURN_Z)
    ret_hit = ret_z_only & (F.abs(F.col("_ret_pct")) >= RETURN_PCT)

    # 필터 단계별 효과 (AND 조건이 얼마나 걸러냈는지)
    n_vol_z = c.filter(vol_z_only).count()
    n_vol_hit = c.filter(vol_hit).count()
    n_ret_z = c.filter(ret_z_only).count()
    n_ret_hit = c.filter(ret_hit).count()

    # -----------------------------------------------------------------------
    # 알림 생성
    # -----------------------------------------------------------------------
    vol_alerts = c.filter(vol_hit).select(
        F.col(COL_MINUTE).alias("minute"),
        F.col(COL_CODE).alias("code"),
        F.lit("VOLUME_SPIKE").alias("rule"),
        F.round(F.col("_vol_z"), 2).alias("score"),
        F.concat(F.lit("평균 대비 "), F.round(F.col("_vol_ratio"), 1), F.lit("배")).alias("detail"),
        F.round(F.col(COL_VOLUME), 4).alias("value"),
        F.round(F.col("_vol_avg"), 4).alias("baseline"),
    )

    ret_alerts = c.filter(ret_hit).select(
        F.col(COL_MINUTE).alias("minute"),
        F.col(COL_CODE).alias("code"),
        F.lit("PRICE_MOVE").alias("rule"),
        F.round(F.col("_ret_z"), 2).alias("score"),
        F.concat(F.lit("1분 "), F.round(F.col("_ret_pct"), 2), F.lit("%")).alias("detail"),
        F.round(F.col(COL_CLOSE), 4).alias("value"),
        F.round(F.col("_prev_close"), 4).alias("baseline"),
    )

    raw_alerts = vol_alerts.unionByName(ret_alerts).withColumn(
        "severity", F.when(F.col("score") >= HIGH_Z, "HIGH").otherwise("MEDIUM")
    )
    raw_alerts.cache()
    raw_count = raw_alerts.count()

    # -----------------------------------------------------------------------
    # 중복 억제: 같은 코인·같은 규칙은 10분 구간당 1건만
    # -----------------------------------------------------------------------
    bucketed = raw_alerts.withColumn(
        "_bucket", F.window("minute", f"{SUPPRESS_MINUTES} minutes").getField("start")
    )
    w_sup = Window.partitionBy("code", "rule", "_bucket")

    alerts = (
        bucketed.withColumn("_rn", F.row_number().over(w_sup.orderBy("minute")))
        .withColumn("suppressed", F.count("*").over(w_sup) - 1)
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_bucket")
        .withColumn("date", F.date_format("minute", "yyyy-MM-dd"))
    )
    alerts.cache()
    kept_count = alerts.count()

    # -----------------------------------------------------------------------
    # 저장: 최근 N일 파티션만 덮어쓴다
    # -----------------------------------------------------------------------
    target = alerts.filter(F.to_date("minute") >= F.date_sub(F.current_date(), DAYS_BACK - 1))
    target_count = target.count()

    if target_count > 0:
        (
            target.coalesce(1)
            .write.mode("overwrite")
            .partitionBy("date")
            .parquet(ALERTS_PATH)
        )
        print(f"\n[write] {ALERTS_PATH} <- {target_count}건 (최근 {DAYS_BACK}일)")
    else:
        print(f"\n[write] 최근 {DAYS_BACK}일에 저장할 알림이 없습니다.")

    # -----------------------------------------------------------------------
    # 요약 출력
    # -----------------------------------------------------------------------
    print("\n=== 필터 단계별 건수 ===")
    print(f"VOLUME_SPIKE   z>={VOLUME_Z} 만: {n_vol_z}건"
          f"  ->  배수>={VOLUME_RATIO} 추가: {n_vol_hit}건")
    print(f"PRICE_MOVE     z>={RETURN_Z} 만: {n_ret_z}건"
          f"  ->  변동률>={RETURN_PCT}% 추가: {n_ret_hit}건")
    print(f"\n전체 판정 {raw_count}건  ->  중복 억제 후 {kept_count}건", end="")
    if raw_count > 0:
        print(f"  (억제율 {(1 - kept_count / raw_count) * 100:.1f}%)")
    else:
        print()

    print("\n=== 규칙·심각도별 ===")
    alerts.groupBy("rule", "severity").count().orderBy("rule", "severity").show()

    print("\n=== 코인별 ===")
    alerts.groupBy("code", "rule").count().orderBy("code", "rule").show()

    print("\n=== 최근 20건 ===")
    (
        alerts.orderBy(F.col("minute").desc())
        .select("minute", "code", "rule", "severity", "score", "detail", "suppressed")
        .show(20, truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()
