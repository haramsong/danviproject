import pandas as pd


def remove_error(path: str) -> pd.DataFrame:
    """[summary]
    원본 데이터의 경로를 받아 에러 데이터를 제거해주는 프로세스
    에러 데이터는 다음과 같이 정의한다.
    1. COLLECT_DATE가 IN_TIME보다 빠른 데이터 제거
    2. COLLEC_DATE - IN_TIME이 DURATION보다 작은 데이터 제거
    3. MAC_ADDR의 시리얼 넘버(뒷 6자리)가 00:00:00인 데이터 제거

    Columns : ['IDX', 'AP_ADDR', 'MAC_ADDR', 'IN_TIME', 'DURATION', 'DISTANCE', 'COLLECT_DATE', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'WEEK', 'WEEKOFYEAR']
    작업 상황
        Rows : 약 4천 1백만 row -> 약 4천만 row (전체 데이터 4% 감소)
        Memory : 4.9GB -> 2.4GB (Memory 사용량 50% 감소)
        Time : 약 40 Seconds 소요

    Args :
        path ([str]): [Spider를 통해 수집된 원본 데이터(txt)]

    Returns :
        [pd.DataFrame] : [에러 데이터가 제거된 데이터]
    """
    data = pd.read_csv(path, delimiter='|', usecols=range(13))
    data['DISTANCE'] = data['DISTANCE'].astype('float16')
    data['YEAR'] = data['YEAR'].astype('int16')
    data[['MONTH', 'DAY', 'HOUR', 'WEEK', 'WEEKOFYEAR']] = data[[
        'MONTH', 'DAY', 'HOUR', 'WEEK', 'WEEKOFYEAR']].astype('int8')
    data[['COLLECT_DATE', 'IN_TIME']] = data[[
        'COLLECT_DATE', 'IN_TIME']].astype('datetime64')
    # 기준 1
    data = data.loc[data['IN_TIME'] <= data['COLLECT_DATE']]
    # 기준 2
    data['error_sec'] = (data['COLLECT_DATE']-data['IN_TIME']
                         ).dt.total_seconds()-data['DURATION']
    data = data.loc[data['error_sec'] >= 0]
    # 기준 3
    data['is_machine'] = data['MAC_ADDR'].apply(
        lambda x: 1 if x[-8:] == '00:00:00' else 0)
    data = data.loc[data['is_machine'] == 0]
    data.drop(columns=['error_sec', 'is_machine'], inplace=True)

    return data
