"""
Tasks Planned
============================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on planned tasks.

- Create date:  2018-12-10
- Update date:  2018-12-26
- Version:      1.1

Notes:
============================
- v1.0: Initial version
- v1.1: Elasticsearch index names as parameters in config.ini
"""
import configparser

from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic, ROOT_DIR
from ant_data.static.GEOGRAPHY import COUNTRY_LIST


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def search(country, start=None, end=None, f=None, interval='month'):
    if country not in COUNTRY_LIST:
        raise Exception(f'{country} is not a valid country')

    s = Search(using=elastic, index=CONFIG['ES']['TASKS']) \
        .query('term', country=country) \
        .query('term', doctype='task')

    if start is not None:
        s = s.query('bool', filter=Q('range', due={ 'gte': start }))
    if end is not None:
        s = s.query('bool', filter=Q('range', due={ 'lt': end }))
    if f is not None:
        s = s.query('bool', filter=f)

    s.aggs.bucket('dates', 'date_histogram', field='due', interval=interval, min_doc_count=1) \
        .bucket('planned', 'terms', field='planned') \

    return s[:0].execute()


def df(country, start=None, end=None, f=None, interval='month'):
    if country not in COUNTRY_LIST:
        raise Exception(f'{country} is not a valid country')

    response = search(country, start=start, end=end, f=f, interval=interval)

    obj = {}
    for interval in response.aggs.dates.buckets:
        obj[interval.key_as_string] = {}
        for planned in interval.planned.buckets:
            key = 'planned' if planned.key else 'unplanned'
            obj[interval.key_as_string][key] = planned.doc_count


    df = DataFrame.from_dict(obj, orient='index')

    if df.empty:
        return df

    df.index = df.index.astype('datetime64')
    df.index.name = 'date'
    df = df.fillna(0).astype('int64')
    df['total'] = df.sum(axis=1)

    return df
