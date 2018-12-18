"""
Tasks action list
============================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on task workflows based on a given action list.

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


def search(country, workflows, start=None, end=None, f=None, interval='month'):
    if country not in COUNTRY_LIST:
        raise Exception(f'{country} is not a valid country')

    s = Search(using=elastic, index='tasks') \
        .query('term', country=country) \
        .query('term', doctype='task') \
        .query('has_child', type='history', query=Q('terms', workflow=workflows))

    if start is not None:
        s = s.query('bool', filter=Q('range', due={ 'gte': start }))
    if end is not None:
        s = s.query('bool', filter=Q('range', due={ 'lt': end }))
    if f is not None:
        s = s.query('bool', filter=f)

    s.aggs.bucket(
        'dates', 'date_histogram', field='due', interval=interval, min_doc_count=1
        ).bucket('types', 'terms', field='type', size=10000)

    return s[:0].execute()


def df(country, workflows, start=None, end=None, f=None, interval='month'):
    if country not in COUNTRY_LIST:
        raise Exception(f'{country} is not a valid country')

    must = search(country, workflows, start=start, end=end, f=f, interval=interval)

    obj = {}
    for interval in must.aggs.dates.buckets:
        if not interval.key_as_string in obj:
            obj[interval.key_as_string] = {}
        for task_type in interval.types.buckets:
            obj[interval.key_as_string][task_type.key] = task_type.doc_count

    df = DataFrame.from_dict(obj, orient='index')

    if df.empty:
        return df

    df.index = df.index.astype('datetime64')
    df.index.name = 'date'
    df = df.fillna(0).astype('int64')
    df['total'] = df.sum(axis=1)

    return df
