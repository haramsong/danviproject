import pandas as pd
import datetime as dt


def duration_max(data: pd.DataFrame, fix_time: int) -> pd.DataFrame:
    """[summary]
    기기적 결함으로 발생하는 데이터 끊김을 방지하고, 연속적으로 계속 쌓이는 데이터를
     IN_TIME을 기준으로 DURATION이 가장 긴 데이터를 뽑아주는 함수

    작업 진행 전후
        선택한 설치소의 데이터
        Rows : 약 9백만 row -> 약 35만 row (전체 데이터 96% 감소)
        Memory : 1.1GB -> 42MB (Memory 사용량 96% 감소)
     #    Time : 32 Seconds 소요

    Args:
        data (pd.DataFrame): [에러, 기기, 직원이 없는 데이터]
        fix_time (int): [기기적 오류로 다시 잡히는 데 걸리는 시간(초)]
                        임의로 설정 가능하지만, 상주 기기들에서 이 시간을 도출하는 것을 추천

    Returns:
        pd.DataFrame: [description]
    """
    data['DISTANCE'] = data['DISTANCE'].astype('float16')
    data['YEAR'] = data['YEAR'].astype('int16')
    data[['MONTH', 'DAY', 'HOUR', 'WEEK', 'WEEKOFYEAR']] = data[[
        'MONTH', 'DAY', 'HOUR', 'WEEK', 'WEEKOFYEAR']].astype('int8')
    data[['COLLECT_DATE', 'IN_TIME']] = data[[
        'COLLECT_DATE', 'IN_TIME']].astype('datetime64')

    data = data.copy()
    data['DURATION'] = (data['COLLECT_DATE'] - data['IN_TIME']
                        ).dt.total_seconds().astype('int')
    max_idx = data.groupby(['MAC_ADDR', 'IN_TIME'])['IDX'].max().values
    data = data[data['IDX'].isin(max_idx)]

    fix_mac = data.groupby('MAC_ADDR')['IN_TIME'].nunique()[
        data.groupby('MAC_ADDR')['IN_TIME'].nunique() > 1].index
    tofix_df = data[data['MAC_ADDR'].isin(fix_mac)]
    nonfix_df = data[~data['MAC_ADDR'].isin(fix_mac)]

    tofix_df['retry'] = tofix_df['IN_TIME'] - \
        tofix_df.groupby(['MAC_ADDR']).shift()['COLLECT_DATE']
    mean_du = dt.timedelta(seconds=int(fix_time))
    tofix_df['del_re'] = tofix_df['retry'].apply(
        lambda x: 1 if x < mean_du else 0)
    tofix_df['com_re'] = tofix_df.groupby('MAC_ADDR').shift(-1)['del_re']
    tofix_df = tofix_df[~((tofix_df['del_re'] == 1) &
                          (tofix_df['com_re'] == 1))]
    tofix_df['replace_collect'] = tofix_df.groupby(
        'MAC_ADDR').shift(-1)['COLLECT_DATE']
    tofix_df['COLLECT_DATE'] = tofix_df.apply(
        lambda x: x['replace_collect'] if x['del_re'] == 0 and x['com_re'] == 1 else x['COLLECT_DATE'], axis=1)
    tofix_df = tofix_df[(tofix_df['del_re'] == 0)]
    tofix_df['DURATION'] = (tofix_df['COLLECT_DATE'] -
                            tofix_df['IN_TIME']).dt.seconds
    tofix_df.drop(columns=['retry', 'del_re', 'com_re',
                  'replace_collect'], inplace=True)
    data = pd.concat([tofix_df, nonfix_df], axis=0).sort_values('IDX')

    return data
