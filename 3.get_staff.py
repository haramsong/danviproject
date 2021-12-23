from collections import Counter
from itertools import chain
from collections import deque
import datetime as dt
import pandas as pd


def get_staff(data: pd.DataFrame, start: int, end: int) -> deque:
    """[summary]
    Spider 설치소 직원들의 MAC_ADDR를 deque의 형태로 반환해주는 함수
    작업 진행 전후
         선택한 설치소의 데이터
         MAC_ADDR : 약 33만 개 -> 4개 추출
         Time : 33 Seconds 소요

    Args:
        start (int): [출근 시각]
        end (int): [퇴근 시각]

    Returns:
        deque: [직원들의 MAC_ADDR]
    """
    data['weekday'] = data['COLLECT_DATE'].dt.weekday
    dt_year = str(data['YEAR'].unique()[0])
    dt_month = str(data['MONTH'].unique()[0]).zfill(2)
    month_weekday = Counter(pd.date_range(dt_year+'-'+dt_month+'-01',
                            dt_year+'-'+str(int(dt_month)+1).zfill(2)+'-01')[:-1].weekday)
    # 100시간 이상 관측된 데이터만 추출
    temp_staff_df = data[data['MAC_ADDR'].isin(data.groupby(['MAC_ADDR'])['day_hour'].nunique()[
                                               data.groupby(['MAC_ADDR'])['day_hour'].nunique() > 100].index)]

    cand_staff = deque()
    # 한달 중 4번 이상 쉰 요일의 데이터 추출
    for mac in temp_staff_df['MAC_ADDR'].unique():
        weekday_counter = Counter(list(chain(
            *temp_staff_df[temp_staff_df['MAC_ADDR'] == mac].groupby('DAY')['weekday'].unique().values)))
        if sum([True for i in (month_weekday - weekday_counter).values() if i >= 4]) == 2 and len(weekday_counter) == 5:
            cand_staff.append(mac)

    st_work, end_work = int(start), int(end)
    for cand in cand_staff:
        ck_time = temp_staff_df[temp_staff_df['MAC_ADDR'] == cand]
        if (ck_time['HOUR'].max() <= end_work+1) and (ck_time['HOUR'].min() >= st_work-1):
            continue
        else:
            cand_staff.remove(cand)

    return cand_staff
