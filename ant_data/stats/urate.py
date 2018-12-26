"""
Client urs
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on client urs.

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

        s.aggs.metric('ur30', 'avg', field='stats.ur30')
        s.aggs.metric('ur60', 'avg', field='stats.ur60')
        s.aggs.metric('ur90', 'avg', field='stats.ur90')
        s.aggs.metric('ur180', 'avg', field='stats.ur180')
        s.aggs.metric('ur360', 'avg', field='stats.ur360')

    else:
        s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
            .query('term', country=country) \
            .query('term', doctype='stat') \
            .query('term', date=date) \
            .query('has_parent', parent_type='person', query=Q('term', doctype='client'))

        if f is not None:
            s = s.query('has_parent', parent_type='person', query=Q('bool', filter=f))

        s.aggs.metric('ur30', 'avg', field='ur30')

    return s[:0].execute()


def df(country, f=None, **kwargs):
    date = kwargs['date'] if 'date' in kwargs else None
    response = search(country, f=f, date=date)

    if date is None:
        obj = {}
        obj[30] = { 'taza': round(response.aggs.ur30.value, 4) }
        obj[60] = { 'taza': round(response.aggs.ur60.value, 4) }
        obj[90] = { 'taza': round(response.aggs.ur90.value, 4) }
        obj[180] = { 'taza': round(response.aggs.ur180.value, 4) }
        obj[360] = { 'taza': round(response.aggs.ur360.value, 4) }
    else:
        obj = {}
        obj[30] = { 'taza': round(response.aggs.ur30.value, 4) }

    df = DataFrame.from_dict(obj).T

    if df.empty:
        return df

    df.index.name = 'period'
    df = df.reset_index()

    return df
