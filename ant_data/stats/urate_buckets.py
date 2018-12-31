"""
Client urs
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on client urs.

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

        s.aggs.bucket('ur30', 'histogram', field='stats.ur30', interval=0.05)
        s.aggs.bucket('ur60', 'histogram', field='stats.ur60', interval=0.05)
        s.aggs.bucket('ur90', 'histogram', field='stats.ur90', interval=0.05)
        s.aggs.bucket('ur180', 'histogram', field='stats.ur180', interval=0.05)
        s.aggs.bucket('ur360', 'histogram', field='stats.ur360', interval=0.05)

    else:
        s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
            .query('term', country=country) \
            .query('term', doctype='stat') \
            .query('term', date=date) \
            .query('has_parent', parent_type='person', query=Q('term', doctype='client')) \
            .query('has_parent', parent_type='person', query=Q('term', kingo_open=True))

        if f is not None:
            s = s.query('has_parent', parent_type='person', query=Q('bool', filter=f))

        s.aggs.bucket('ur30', 'histogram', field='ur30', interval=0.05)

    return s[:0].execute()


def df(country, f=None, **kwargs):
    date = kwargs['date'] if 'date' in kwargs else None
    response = search(country, f=f, date=date)

    if date is None:
        obj = {}
        obj[30] = {}
        for ur in response.aggs.ur30.buckets:
            obj[30][round(ur.key,2)] = ur.doc_count
        obj[60] = {}
        for ur in response.aggs.ur60.buckets:
            obj[60][round(ur.key,2)] = ur.doc_count
        obj[90] = {}
        for ur in response.aggs.ur90.buckets:
            obj[90][round(ur.key,2)] = ur.doc_count
        obj[180] = {}
        for ur in response.aggs.ur180.buckets:
            obj[180][round(ur.key,2)] = ur.doc_count
        obj[360] = {}
        for ur in response.aggs.ur360.buckets:
            obj[360][round(ur.key,2)] = ur.doc_count

    else:
        obj = {}
        obj[30] = {}
        for ur in response.aggs.ur30.buckets:
            obj[30][round(ur.key,2)] = ur.doc_count

    df = DataFrame.from_dict(obj).T

    if df.empty:
        return df

    df.index.name = 'period'
    df = df.fillna(0).astype('int64')
    df['total'] = df.sum(axis=1)
    df = df.reset_index()

    return df
