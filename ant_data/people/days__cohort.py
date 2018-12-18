"""
Active days
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on installed/synced/active days by cohort.

- Create date:  2018-12-11
- Update date:
- Version:      1.0

Notes:
==========================
- v1.0: Initial version
"""
from elasticsearch_dsl import Search, Q, A
from pandas import DataFrame, Series

from ant_data import elastic


def search(country, start=None, end=None, f=None, interval='month'):
    s = Search(using=elastic, index='people') \
        .query('term', country=country)

    if start is not None:
        s = s.query('bool', filter=Q('range', opened={ 'gte': start }))
    if end is not None:
        s = s.query('bool', filter=Q('range', opened={ 'lt': end }))
    if f is not None:
        s = s.query('bool', filter=f)

    s.aggs.bucket('cohort', 'date_histogram', field='opened', interval=interval, min_doc_count=1) \
        .bucket('stats', 'children', type='stat') \
        .bucket('dates', 'date_histogram', field='date', interval=interval, min_doc_count=1)
    s.aggs['cohort']['stats']['dates'].bucket('active','terms',field='active')
    s.aggs['cohort']['stats']['dates'].bucket('sync','terms',field='sync')
    s.aggs['cohort']['stats']['dates'].bucket('update','terms',field='update')

    return s[:0].execute()


def df(country, start=None, end=None, f=None, interval='month'):
    response = search(country, start=start, end=end, f=f, interval=interval)

    obj = {}
    for cohort in response.aggs.cohort.buckets:
        obj[cohort.key_as_string] = {}
        for date in cohort.stats.dates.buckets:
            obj[cohort.key_as_string][date.key_as_string] = {
                'install': date.doc_count,
                'active': 0,
                'update': 0,
                'sync': 0
            }
            for active in date.active.buckets:
                if active.key == 1:
                    obj[cohort.key_as_string][date.key_as_string]['active'] = active.doc_count
            for sync in date.sync.buckets:
                if sync.key == 1:
                    obj[cohort.key_as_string][date.key_as_string]['sync'] = sync.doc_count
            for update in date.update.buckets:
                if update.key == 1:
                    obj[cohort.key_as_string][date.key_as_string]['update'] = update.doc_count

    df = DataFrame.from_dict({(i,j): obj[i][j]
                                    for i in obj.keys()
                                    for j in obj[i].keys()},
                                orient='index')

    if df.empty:
        return df


    df.index = df.index.set_levels(df.index.levels[0].astype('datetime64'), level=0)
    df.index = df.index.set_levels(df.index.levels[1].astype('datetime64'), level=1)
    df = df.fillna(0).astype('int64')
    df.index = df.index.set_names('cohort', level=0)
    df.index = df.index.set_names('date', level=1)
    df = df.reset_index().set_index('date')
    df['month'] = df.index.daysinmonth
    df = df[['cohort','install','update','sync','active','month']]

    return df
