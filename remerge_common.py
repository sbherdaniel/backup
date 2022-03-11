# double precision 처리 필요한 애들
REMERGE_METRIC_DATA = [
    "app_open_rate"
]

# double precision 처리 불필요한 애들
REMERGE_METRIC_NUM_DATA = [
    "cost",
    "conversions",
    "ap_conversions"
    "impressions",
    "clicks",
    "unique_users"
]

REMERGE_COLUMN = [
    "event"
]

# groupby할때 사용할 index 컬럼 리스트
REMERGE_SELECT_DATA = {
    "timestamp" : "timestamp",
    "event.country": "country",
    "event.audience": "audience",
    "event.ad_label_name": "ad_label_name",
    "event.platform": "platform",
    "event.ad_label_id": "ad_label_id",
    "event.campaign": "campaign",
    "event.ad_creative_url": "ad_creative_url",
    "event.campaign_name": "campaign_name",
    "event.cost_currency": "cost_currency",
    "event.ad_id": "ad_id",
    "event.ad_name": "ad_name"
}

# 쿼리에서 select문에 들어갈 select column들 쿼리문으로 작성하는 함수
def get_remerge_column(columns):
    select_column = list()
    for col in columns:
        if col in REMERGE_METRIC_DATA:
            select_column.append(f'sum(cast(event."{col}" as double precision)) "{col}"')
        elif col in REMERGE_METRIC_NUM_DATA:
            select_column.append(f'sum(event."{col}") "{col}"')
        elif col in REMERGE_SELECT_DATA.values():
            for key, val in REMERGE_SELECT_DATA.items():
                if val == col:
                    select_column.append(f'cast({key} as VARCHAR) "{val}"')
    return ', '.join(select_column)

# 최종 쿼리 작성
def get_remerge_query(select_columns, metric_columns, groupby_columns, owner_id, start_date, end_date):
    return f"""
        SELECT
               {select_columns},
               {metric_columns}
            FROM remerge_basic_report
            WHERE owner_id in ('{owner_id}') AND
                  collected_at between '{start_date}' and '{end_date}'
        GROUP BY
                 {groupby_columns}
        ;
        """


# test query print
if __name__ == '__main__':
    columns = REMERGE_SELECT_DATA.values()
    groupby_columns = ['"' + x + '"' for x in columns]
    groupby_columns = ', '.join(groupby_columns)
    owner_id = 'innisfree'
    start_date, end_date = '2022-03-07', '2022-03-07'

    select_columns = get_remerge_column(columns)
    metric_columns = get_remerge_column(REMERGE_METRIC_DATA + REMERGE_METRIC_NUM_DATA)

    test_query = get_remerge_query(select_columns, metric_columns, groupby_columns, owner_id, start_date, end_date)
    print(test_query)
