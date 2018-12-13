"""
Active days
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on installed/synced/active days.

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


def search(country, f=None, interval='month'):
    s = Search(using=elastic, index='people') \
        .query('term', country=country)

    if f is not None:
        s = s.query('bool', filter=f)

    s.aggs.bucket('stats', 'children', type='stat') \
        .bucket('dates', 'date_histogram', field='date', interval=interval)
    s.aggs['stats']['dates'].bucket('active','terms',field='active')
    s.aggs['stats']['dates'].bucket('sync','terms',field='sync')
    s.aggs['stats']['dates'].bucket('update','terms',field='update')

    return s[:0].execute()


def df(country, f=None, interval='month'):
    response = search(country, f=f, interval=interval)

    obj = {}
    for date in response.aggs.stats.dates.buckets:
        obj[date.key_as_string] = {
            'install': date.doc_count,
            'active': 0,
            'sync': 0
        }
        for active in date.active.buckets:
            if active.key == 1:
                obj[date.key_as_string]['active'] = active.doc_count
        for sync in date.sync.buckets:
            if sync.key == 1:
                obj[date.key_as_string]['sync'] = sync.doc_count
        for update in date.update.buckets:
            if update.key == 1:
                obj[date.key_as_string]['update'] = update.doc_count


    df = DataFrame(obj).T

    if df.empty:
        return df

    df = df.reindex(df.index.astype('datetime64')).sort_index()
    df = df.fillna(0).astype('int64')
    df.index.name = 'date'
    df['month'] = df.index.daysinmonth
    df = df[['install','update','sync','active','month']]

    return df
