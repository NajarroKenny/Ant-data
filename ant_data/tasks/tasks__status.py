"""
Task Status
============================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on task status.

- Create date:  2018-12-10
- Update date:
- Version:      1.0

Notes:
============================
- v1.0: Initial version
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic
from ant_data.static.GEOGRAPHY import COUNTRY_LIST


def search(country, start=None, end=None, f=None, interval='month'):
    if country not in COUNTRY_LIST:
        raise Exception(f'{country} is not a valid country')

    s = Search(using=elastic, index='tasks') \
        .query('term', country=country) \
        .query('term', doctype='task')

    if start is not None:
        s = s.query('bool', filter=Q('range', due={ 'gte': start }))
    if end is not None:
        s = s.query('bool', filter=Q('range', due={ 'lt': end }))
    if f is not None:
        s = s.query('bool', filter=f)

    s.aggs.bucket('dates', 'date_histogram', field='due', interval=interval, min_doc_count=1) \
        .bucket('status', 'terms', field='status', size=10000)

    return s[:0].execute()


def df(country, start=None, end=None, f=None, interval='month'):
    if country not in COUNTRY_LIST:
        raise Exception(f'{country} is not a valid country')

    response = search(country, start=start, end=end, f=f, interval=interval)

    obj = {}
    for interval in response.aggs.dates.buckets:
        obj[interval.key_as_string] = {}
        for status in interval.status.buckets:
            obj[interval.key_as_string][status.key] = status.doc_count


    df = DataFrame.from_dict(obj, orient='index')

    if df.empty:
        return df

    df.index = df.index.astype('datetime64')
    df.index.name = 'date'
    df = df.fillna(0).astype('int64')
    df['total'] = df.sum(axis=1)

    return df
