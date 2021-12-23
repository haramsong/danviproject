import pandas as pd
import math


def remove_machine(data: pd.DataFrame, loc: str) -> pd.DataFrame:
    """[summary]
    Spider 설치소의 프린터 및 상주하는 기기 제거 프로세스
    상주기기는 다음과 같이 정의한다.
    1. 법정근로 시간인 주 52시간 초과 관측
    2. 일일 18시간 이상 관측
    3. DURATION이 9시간(3600*9=32400) 초과

    Columns : ['IDX', 'AP_ADDR', 'MAC_ADDR', 'IN_TIME', 'DURATION', 'DISTANCE', 'COLLECT_DATE', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'WEEK', 'WEEKOFYEAR']
    작업 진행 전후
         선택한 설치소의 데이터
         Rows : 약 4천만 row -> 약 9백3십만 row (전체 데이터 77% 감소)
         Memory : 2.4GB -> 650MB (Memory 사용량 70% 감소)
         Time : 35 Seconds 소요
    Args:
         data (pd.DataFrame): [에러가 없는 데이터]
         loc (str) : [확인하고자 하는 Spider 설치소 AP_ADDR]

    Returns:
         pd.DataFrame: [일부 상주기기가 제거되고 남은 유동인구 후보 데이터]
    """
    seablue = data[data['AP_ADDR'] == loc]
    seablue = seablue.copy()
    seablue['DISTANCE'] = seablue['DISTANCE'].astype('float16')
    seablue['YEAR'] = seablue['YEAR'].astype('int16')
    seablue[['MONTH', 'DAY', 'HOUR', 'WEEK', 'WEEKOFYEAR']] = seablue[[
        'MONTH', 'DAY', 'HOUR', 'WEEK', 'WEEKOFYEAR']].astype('int8')
    seablue[['COLLECT_DATE', 'IN_TIME']] = seablue[[
        'COLLECT_DATE', 'IN_TIME']].astype('datetime64')
    seablue['day_hour'] = seablue['DAY'].astype(
        'str')+'_'+seablue['HOUR'].astype('str')
    # 기준 1
    machine_1 = seablue.groupby(['MAC_ADDR'])['day_hour'].nunique()[seablue.groupby(
        ['MAC_ADDR'])['day_hour'].nunique() >= math.ceil(52/7*seablue['DAY'].nunique())].index
    # 기준 2
    machine_2 = seablue.groupby(['MAC_ADDR', 'DAY'])['HOUR'].nunique()[seablue.groupby(
        ['MAC_ADDR', 'DAY'])['HOUR'].nunique() >= 18].reset_index()['MAC_ADDR'].unique()
    # 기준 3
    machine_3 = seablue[seablue['DURATION'] >= 3600*9]['MAC_ADDR'].unique()
    seablue = seablue[~seablue['MAC_ADDR'].isin(
        set(machine_1) | set(machine_2) | set(machine_3))]

    return seablue
