"""
업비트 조회 구간의 UTC 날짜 분할 테스트

운영 중 실제로 겪은 버그에서 나온 테스트다. 한국시간 기준으로 오전 9시를
넘는 구간을 백필하려 했을 때 일부 체결이 조회되지 않았고, 원인은 업비트
API의 days_ago가 UTC 날짜 기준이라는 점이었다.
"""

from datetime import date

from date_plan import parse_kst, plan_days


def test_하루_안에서_끝나는_구간은_계획이_하나다():
    # KST 18:00~18:30 = UTC 같은 날 09:00~09:30
    plans = plan_days(
        parse_kst("2026-11-11 18:00"),
        parse_kst("2026-11-11 18:30"),
        today=date(2026, 11, 11),
    )

    assert len(plans) == 1
    assert plans[0]["day"] == date(2026, 11, 11)


def test_한국시간_오전9시_경계를_넘으면_날짜가_둘로_나뉜다():
    # 이게 실제로 겪은 버그다.
    # KST 08:30~10:30 = UTC 11-10 23:30 ~ 11-11 01:30 이라 UTC 날짜가 둘이다.
    plans = plan_days(
        parse_kst("2026-11-11 08:30"),
        parse_kst("2026-11-11 10:30"),
        today=date(2026, 11, 11),
    )

    assert [p["day"] for p in plans] == [date(2026, 11, 11), date(2026, 11, 10)]


def test_구간이_UTC_자정에_정확히_끝나면_그_날짜는_조회하지_않는다():
    # KST 08:00~09:00 = UTC 11-10 23:00 ~ 11-11 00:00
    # end는 구간에 포함되지 않으므로 11-11은 조회할 체결이 없다.
    plans = plan_days(
        parse_kst("2026-11-11 08:00"),
        parse_kst("2026-11-11 09:00"),
        today=date(2026, 11, 11),
    )

    assert [p["day"] for p in plans] == [date(2026, 11, 10)]


def test_7일이_지난_구간은_제외된다():
    # 업비트는 7일까지만 과거 체결을 준다.
    plans = plan_days(
        parse_kst("2026-11-10 18:00"),
        parse_kst("2026-11-10 18:30"),
        today=date(2026, 11, 18),
    )

    assert plans == []


def test_끝_시각은_구간이_끝나는_날짜에만_붙는다():
    plans = plan_days(
        parse_kst("2026-11-11 08:30"),
        parse_kst("2026-11-11 10:30"),
        today=date(2026, 11, 11),
    )

    assert plans[0]["to"] == "01:30:00"   # 구간이 끝나는 날 (UTC 11-11)
    assert plans[1]["to"] is None         # 그 전날은 자정까지 전부 조회


def test_오늘_날짜는_days_ago가_0이다():
    plans = plan_days(
        parse_kst("2026-11-11 18:00"),
        parse_kst("2026-11-11 18:30"),
        today=date(2026, 11, 11),
    )

    assert plans[0]["days_ago"] == 0


def test_하루_전_날짜는_days_ago가_1이다():
    plans = plan_days(
        parse_kst("2026-11-11 08:30"),
        parse_kst("2026-11-11 10:30"),
        today=date(2026, 11, 11),
    )

    assert plans[1]["days_ago"] == 1


def test_이전_날짜의_정지_시각은_구간_시작_시각이다():
    # 구간 시작보다 더 거슬러 올라갈 필요가 없다.
    start = parse_kst("2026-11-11 08:30")
    plans = plan_days(start, parse_kst("2026-11-11 10:30"), today=date(2026, 11, 11))

    assert plans[1]["stop_ms"] == int(start.timestamp() * 1000)
