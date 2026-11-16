"""
dashboard.py - 업비트 실시간 파이프라인 대시보드

S3에 쌓인 봉과 알림을 읽어 파이프라인 상태, 데이터 품질, 이상 징후를 보여준다.
로컬에서만 실행한다 (AWS 자격증명이 들어가므로 외부에 배포하지 않는다).

설치:
    pip install streamlit pandas pyarrow s3fs python-dotenv

실행:
    cd C:\\realtime-pipeline
    streamlit run dashboard.py
"""

import os
from datetime import datetime

import pandas as pd
import streamlit as st

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------
BUCKET = os.environ.get("S3_BUCKET")

COL_CODE = "code"
COL_MINUTE = "minute"
COL_CLOSE = "close"
COL_VOLUME = "volume"

# Parquet에는 시각이 UTC로 저장되어 있어 KST로 변환해 보여준다.
# 화면의 시각이 9시간 어긋나 보이면 False로 바꾼다.
CONVERT_TZ = True
DISPLAY_TZ = "Asia/Seoul"

# 구조적 지연 2분 (봉 구간 1분 + 워터마크 30초 + 트리거 대기 최대 30초)
# + 대시보드 캐시와 S3 읽기 지연 최대 1분
FRESH_OK = 4       # 위 두 요소의 합. 넘으면 평소보다 느린 상태
FRESH_WARN = 10    # 넘으면 수집 중단으로 본다

COUNT_CANDIDATES = ["trade_count", "trades", "count", "cnt", "n_trades", "trade_cnt"]

st.set_page_config(page_title="업비트 실시간 파이프라인", layout="wide")

if not BUCKET:
    st.error("S3_BUCKET 환경변수가 없습니다. .env 파일이 같은 폴더에 있는지 확인하세요.")
    st.stop()

CANDLES = f"s3://{BUCKET}/candles"
BACKFILL = f"s3://{BUCKET}/candles_backfill"
ALERTS = f"s3://{BUCKET}/alerts"


# ---------------------------------------------------------------------------
# 데이터 읽기
# ---------------------------------------------------------------------------
@st.cache_data(ttl=60, show_spinner=False)
def read_path(path: str, label: str) -> pd.DataFrame:
    try:
        df = pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()
    if not df.empty:
        df["source"] = label
    return df


def to_display_tz(s: pd.Series) -> pd.Series:
    s = pd.to_datetime(s)
    if CONVERT_TZ:
        if s.dt.tz is None:
            s = s.dt.tz_localize("UTC")
        s = s.dt.tz_convert(DISPLAY_TZ).dt.tz_localize(None)
    elif s.dt.tz is not None:
        s = s.dt.tz_localize(None)
    return s


@st.cache_data(ttl=60, show_spinner="S3에서 읽는 중...")
def load_candles() -> pd.DataFrame:
    frames = [
        df
        for df in (read_path(CANDLES, "realtime"), read_path(BACKFILL, "backfill"))
        if not df.empty
    ]
    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df[COL_MINUTE] = to_display_tz(df[COL_MINUTE])

    # 같은 분이 양쪽에 있으면 실시간을 남긴다
    df["_pri"] = (df["source"] != "realtime").astype(int)
    df = (
        df.sort_values("_pri")
        .drop_duplicates([COL_CODE, COL_MINUTE], keep="first")
        .drop(columns="_pri")
        .sort_values(COL_MINUTE)
        .reset_index(drop=True)
    )
    return df


@st.cache_data(ttl=60, show_spinner=False)
def load_alerts() -> pd.DataFrame:
    df = read_path(ALERTS, "alerts")
    if df.empty:
        return df
    df[COL_MINUTE] = to_display_tz(df[COL_MINUTE])
    return df.sort_values(COL_MINUTE, ascending=False).reset_index(drop=True)


def find_gaps(minutes: pd.Series) -> pd.DataFrame:
    m = minutes.drop_duplicates().sort_values().reset_index(drop=True)
    diff = m.diff()
    rows = []
    for i in diff.index[diff > pd.Timedelta(minutes=1)]:
        missing = int(diff.loc[i].total_seconds() // 60) - 1
        rows.append(
            {
                "공백 시작": m.loc[i] - diff.loc[i] + pd.Timedelta(minutes=1),
                "복구 시각": m.loc[i],
                "공백(분)": missing,
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 화면
# ---------------------------------------------------------------------------
st.title("업비트 실시간 체결 파이프라인")

top = st.columns([6, 1])
with top[1]:
    if st.button("새로고침", width="stretch"):
        st.cache_data.clear()
        st.rerun()

candles = load_candles()

if candles.empty:
    st.warning("봉 데이터를 읽지 못했습니다. S3 경로와 자격증명을 확인하세요.")
    st.stop()

count_col = next((c for c in COUNT_CANDIDATES if c in candles.columns), None)
codes = sorted(candles[COL_CODE].unique())
ref_code = codes[0]
ref = candles[candles[COL_CODE] == ref_code]

# ---------------------------------------------------------------------------
# 1. 지금 상태
# ---------------------------------------------------------------------------
st.subheader("지금 상태")

latest = ref[COL_MINUTE].max()
lag_min = (datetime.now() - latest).total_seconds() / 60

if lag_min <= FRESH_OK:
    badge, color = "정상", "normal"
elif lag_min <= FRESH_WARN:
    badge, color = "지연", "off"
else:
    badge, color = "중단", "inverse"

gaps = find_gaps(ref[COL_MINUTE])
if gaps.empty:
    uptime = latest - ref[COL_MINUTE].min()
else:
    uptime = latest - gaps.iloc[-1]["복구 시각"]
uptime_txt = f"{uptime.days}일 {uptime.seconds // 3600}시간 {(uptime.seconds % 3600) // 60}분"

today = candles[candles[COL_MINUTE].dt.date == datetime.now().date()]

m = st.columns(5)
m[0].metric("파이프라인", badge, delta=f"{lag_min:.0f}분 지연", delta_color=color)
m[1].metric("마지막 데이터", latest.strftime("%m-%d %H:%M"))
m[2].metric("연속 가동", uptime_txt)
m[3].metric("오늘 봉", f"{len(today):,}개")
if count_col:
    m[4].metric("오늘 체결", f"{int(today[count_col].sum()):,}건")
else:
    m[4].metric("오늘 거래량", f"{today[COL_VOLUME].sum():,.2f}")

st.divider()

# ---------------------------------------------------------------------------
# 2. 데이터 품질
# ---------------------------------------------------------------------------
st.subheader("데이터 품질")

q1, q2 = st.columns([2, 1])

with q1:
    hours = st.slider("조회 구간 (시간)", 1, 48, 12)
    since = latest - pd.Timedelta(hours=hours)
    window = candles[candles[COL_MINUTE] >= since]

    metric_col = count_col or COL_VOLUME
    pivot = (
        window.pivot_table(
            index=COL_MINUTE, columns=COL_CODE, values=metric_col, aggfunc="sum"
        )
        .sort_index()
    )
    label = "분당 체결 건수" if count_col else "분당 거래량"
    st.caption(f"{label} — 선이 끊긴 구간이 수집 공백")
    st.line_chart(pivot, height=260)

    # 시간대별 수집률: 60분 중 몇 분이 들어왔는지
    rate = (
        window[window[COL_CODE] == ref_code]
        .assign(hour=lambda d: d[COL_MINUTE].dt.floor("h"))
        .groupby("hour")[COL_MINUTE]
        .nunique()
        .div(60)
        .mul(100)
        .clip(upper=100)
        .rename("수집률(%)")
    )
    st.caption("시간대별 수집률 — 100%면 그 시간의 60분이 모두 들어옴")
    st.bar_chart(rate, height=200)

with q2:
    st.caption("실시간 / 백필 구성")
    comp = candles["source"].value_counts().rename("봉 수")
    st.dataframe(comp, width="stretch")

    st.caption(f"수집 공백 구간 ({ref_code} 기준)")
    if gaps.empty:
        st.success("공백 없음")
    else:
        show = gaps.sort_values("공백 시작", ascending=False).head(20).copy()
        show["공백 시작"] = show["공백 시작"].dt.strftime("%m-%d %H:%M")
        show["복구 시각"] = show["복구 시각"].dt.strftime("%m-%d %H:%M")
        st.dataframe(show, width="stretch", hide_index=True)
        st.caption(f"총 {len(gaps)}건 / {int(gaps['공백(분)'].sum()):,}분")

st.divider()

# ---------------------------------------------------------------------------
# 3. 이상 징후
# ---------------------------------------------------------------------------
st.subheader("이상 징후")

alerts = load_alerts()

if alerts.empty:
    st.info("알림이 없습니다. alerts.py를 실행하면 여기에 쌓입니다.")
else:
    f = st.columns(3)
    sel_code = f[0].multiselect("코인", sorted(alerts["code"].unique()))
    sel_rule = f[1].multiselect("규칙", sorted(alerts["rule"].unique()))
    sel_sev = f[2].multiselect("심각도", sorted(alerts["severity"].unique()))

    view = alerts
    if sel_code:
        view = view[view["code"].isin(sel_code)]
    if sel_rule:
        view = view[view["rule"].isin(sel_rule)]
    if sel_sev:
        view = view[view["severity"].isin(sel_sev)]

    s = st.columns(4)
    s[0].metric("전체", f"{len(view):,}건")
    s[1].metric("HIGH", f"{(view['severity'] == 'HIGH').sum():,}건")
    s[2].metric("거래량 급증", f"{(view['rule'] == 'VOLUME_SPIKE').sum():,}건")
    s[3].metric("가격 급변", f"{(view['rule'] == 'PRICE_MOVE').sum():,}건")

    table = view[["minute", "code", "rule", "severity", "score", "detail"]].copy()
    table["minute"] = table["minute"].dt.strftime("%m-%d %H:%M")
    table.columns = ["시각", "코인", "규칙", "심각도", "점수", "내용"]

    st.dataframe(
        table.style.map(
            lambda v: "color: #d93025; font-weight: 600" if v == "HIGH" else "",
            subset=["심각도"],
        ),
        width="stretch",
        hide_index=True,
        height=420,
    )

st.caption(f"데이터 출처: s3://{BUCKET}/  ·  캐시 60초")
