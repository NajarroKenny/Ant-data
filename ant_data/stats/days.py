"""
Client days
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on client days.

- Create date:  2018-12-11
- Update date:  2018-12-26
- Version:      1.1

Notes:
==========================
- v1.0: Initial version
- v1.1: Elasticsearch index names as parameters in config.ini
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
            .query('term', doctype='client')

        if f is not None:
            s = s.query('bool', filter=f)

        ranges30 = [
            { 'to': 1 },
            { 'from': 1, 'to': 8 },
            { 'from': 8, 'to': 15 },
            { 'from': 15, 'to': 22 },
            { 'from': 22 }
        ]
        ranges60 = [
            { 'to': 1 },
            { 'from': 1, 'to': 16 },
            { 'from': 16, 'to': 31 },
            { 'from': 31, 'to': 46 },
            { 'from': 66 }
        ]
        ranges90 = [
            { 'to': 1 },
            { 'from': 1, 'to': 22 },
            { 'from': 22, 'to': 46 },
            { 'from': 46, 'to': 68 },
            { 'from': 68 }
        ]
        ranges180 = [
            { 'to': 1 },
            { 'from': 1, 'to': 46 },
            { 'from': 46, 'to': 91 },
            { 'from': 91, 'to': 136 },
            { 'from': 136 }
        ]
        ranges360 = [
            { 'to': 1 },
            { 'from': 1, 'to': 91 },
            { 'from': 91, 'to': 181 },
            { 'from': 181, 'to': 271 },
            { 'from': 271 }
        ]

        s.aggs.bucket('days30', 'range', field='stats.days30', ranges=ranges30)
        s.aggs.bucket('days60', 'range', field='stats.days60', ranges=ranges60)
        s.aggs.bucket('days90', 'range', field='stats.days90', ranges=ranges90)
        s.aggs.bucket('days180', 'range', field='stats.days180', ranges=ranges180)
        s.aggs.bucket('days360', 'range', field='stats.days360', ranges=ranges360)

    else:
        s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
            .query('term', country=country) \
            .query('term', doctype='stat') \
            .query('term', date=date) \
            .query('has_parent', parent_type='person', query=Q('term', doctype='client'))

        if f is not None:
            s = s.query('has_parent', parent_type='person', query=Q('bool', filter=f))

        ranges30 = [
            { 'to': 1 },
            { 'from': 1, 'to': 8 },
            { 'from': 8, 'to': 15 },
            { 'from': 15, 'to': 22 },
            { 'from': 22 }
        ]

        s.aggs.bucket('days30', 'range', field='days30', ranges=ranges30)

    return s[:0].execute()


def df(country, f=None, **kwargs):
    date = kwargs['date'] if 'date' in kwargs else None
    response = search(country, f=f, date=date)

    if date is None:
        obj = {}
        obj[30] = {}
        i = 0
        for days in response.aggs.days30.buckets:
            obj[30][i] = days.doc_count
            i += 1
        obj[60] = {}
        i = 0
        for days in response.aggs.days60.buckets:
            obj[60][i] = days.doc_count
            i += 1
        obj[90] = {}
        i = 0
        for days in response.aggs.days90.buckets:
            obj[90][i] = days.doc_count
            i += 1
        obj[180] = {}
        i = 0
        for days in response.aggs.days180.buckets:
            obj[180][i] = days.doc_count
            i += 1
        obj[360] = {}
        i = 0
        for days in response.aggs.days360.buckets:
            obj[360][i] = days.doc_count
            i += 1

    else:
        obj = {}
        obj[30] = {}
        i = 0
        for days in response.aggs.days30.buckets:
            obj[30][i] = days.doc_count
            i += 1

    df = DataFrame.from_dict(obj).T

    if df.empty:
        return df

    df.index.name = 'period'
    df = df.fillna(0).astype('int64')
    df['total'] = df.sum(axis=1)
    df = df.reset_index()

    return df
