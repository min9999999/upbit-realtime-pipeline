"""
date_plan.py - 업비트 조회 구간을 UTC 날짜별로 나눈다

업비트의 과거 체결 조회(/v1/trades/ticks)는 days_ago가 UTC 날짜 기준이다.
한국시간(UTC+9)은 오전 9시에 UTC 날짜가 바뀌므로, 그 경계를 넘는 구간을
한 번에 요청하면 일부가 조회되지 않는다. 날짜별로 나눠 요청해야 한다.

Spark와 네트워크에 의존하지 않는 순수 로직이라 단위 테스트로 검증한다.
"""

from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))
UTC = timezone.utc

MAX_DAYS_AGO = 7  # 업비트가 제공하는 과거 조회 한도


def parse_kst(text):
    """'2026-11-11 16:22' 형식을 한국시간 datetime으로 바꾼다."""
    return datetime.strptime(text, "%Y-%m-%d %H:%M").replace(tzinfo=KST)


def plan_days(start, end, today=None):
    """[start, end) 구간을 UTC 날짜별 조회 계획으로 나눈다.

    최근 날짜부터 거슬러 올라가는 순서로 반환한다. 업비트가 최신 체결부터
    cursor로 거슬러 주기 때문이다.

    각 계획이 담는 것:
        day       조회할 UTC 날짜
        days_ago  오늘로부터 며칠 전인지 (0이면 오늘이고 API 파라미터에서 생략한다)
        to        구간이 이 날짜 안에서 끝나면 끝 시각 "HH:MM:SS", 아니면 None
        stop_ms   이 날짜에서 더 거슬러 올라가지 않아도 되는 시각 (epoch ms)

    today를 넘기면 그 날짜를 기준으로 계산한다. 테스트에서 쓴다.
    """
    if today is None:
        today = datetime.now(UTC).date()

    start_ms = int(start.timestamp() * 1000)
    first_day = start.astimezone(UTC).date()

    # end는 구간에 포함되지 않는다. 1ms 빼고 날짜를 구하면
    # end가 정확히 UTC 자정일 때 그 날짜를 조회하지 않는다.
    day = (end - timedelta(milliseconds=1)).astimezone(UTC).date()

    plans = []
    while day >= first_day:
        days_ago = (today - day).days
        if days_ago > MAX_DAYS_AGO:
            break

        day_start = datetime(day.year, day.month, day.day, tzinfo=UTC)
        day_end = day_start + timedelta(days=1)

        plans.append(
            {
                "day": day,
                "days_ago": days_ago,
                "to": end.astimezone(UTC).strftime("%H:%M:%S") if end < day_end else None,
                "stop_ms": max(start_ms, int(day_start.timestamp() * 1000)),
            }
        )
        day -= timedelta(days=1)

    return plans
