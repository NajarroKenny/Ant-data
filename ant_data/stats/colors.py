"""
Client Colors
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on client colors.

- Create date:  2018-12-11
- Update date:  2018-12-28
- Version:      1.2

Notes:
==========================
- v1.0: Initial version
- v1.1: Elasticsearch index names as parameters in config.ini
- v1.2: Add filter to only include clients with kingo_open = True
"""
import configparser
import datetime as dt

from elasticsearch_dsl import Search, Q, A
from pandas import DataFrame, Series
import numpy as np

from ant_data import elastic, ROOT_DIR


CONFIG = configparser.ConfigParser()
CONFIG.read(ROOT_DIR + '/config.ini')


def search(country, f=None, date=None):
    if date is None:
        s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
            .query('term', country=country) \
            .query('term', doctype='client') \
            .query('term', kingo_open=True)

        if f is not None:
            s = s.query('bool', filter=f)

        s.aggs.bucket('color30', 'terms', field='stats.color30', size=100)
        s.aggs.bucket('color60', 'terms', field='stats.color60', size=100)
        s.aggs.bucket('color90', 'terms', field='stats.color90', size=100)
        s.aggs.bucket('color180', 'terms', field='stats.color180', size=100)
        s.aggs.bucket('color360', 'terms', field='stats.color360', size=100)

    else:
        s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
            .query('term', country=country) \
            .query('term', doctype='stat') \
            .query('term', date=date) \
            .query('has_parent', parent_type='person', query=Q('term', doctype='client')) \
            .query('has_parent', parent_type='person', query=Q('term', kingo_open=True))

        if f is not None:
            s = s.query('has_parent', parent_type='person', query=Q('bool', filter=f))

        s.aggs.bucket('color30', 'terms', field='color30', size=100)

    return s[:0].execute()


def df(country, f=None, **kwargs):
    date = kwargs['date'] if 'date' in kwargs else None
    response = search(country, f=f, date=date)

    if date is None:
        obj = {}
        obj[30] = {}
        for color in response.aggs.color30.buckets:
            obj[30][color.key] = color.doc_count
        obj[60] = {}
        for color in response.aggs.color60.buckets:
            obj[60][color.key] = color.doc_count
        obj[90] = {}
        for color in response.aggs.color90.buckets:
            obj[90][color.key] = color.doc_count
        obj[180] = {}
        for color in response.aggs.color180.buckets:
            obj[180][color.key] = color.doc_count
        obj[360] = {}
        for color in response.aggs.color360.buckets:
            obj[360][color.key] = color.doc_count

    else:
        obj = {}
        obj[30] = {}
        for color in response.aggs.color30.buckets:
            obj[30][color.key] = color.doc_count

    df = DataFrame.from_dict(obj).T

    if df.empty:
        return df

    df.index.name = 'period'
    df = df.fillna(0).astype('int64')
    for x in ['black', 'red', 'yellow', 'blue', 'green']:
        if x not in df:
            df[x] = 0
    df = df.rename(columns={ 'black': 'negro', 'red': 'rojo', 'yellow': 'amarillo', 'blue': 'azul', 'green': 'verde' })
    df = df[['negro','rojo','amarillo','azul','verde']]
    df['total'] = df.sum(axis=1)

    return df
