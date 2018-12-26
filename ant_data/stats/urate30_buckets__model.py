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
    s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
        .query('term', country=country) \
        .query('term', doctype='stat') \
        .query('term', date=date) \
        .query('has_parent', parent_type='person', query=Q('term', doctype='client'))

    if f is not None:
        s = s.query('has_parent', parent_type='person', query=Q('bool', filter=f))

    s.aggs.bucket('model', 'terms', field='model') \
        .bucket('ur30', 'histogram', field='ur30', interval=0.05)

    return s[:0].execute()


def df(country, f=None, **kwargs):
    today = dt.datetime.today().strftime('%Y-%m-%d')
    date = kwargs['date'] if 'date' in kwargs else today
    response = search(country, f=f, date=date)

    obj = {}
    for model in response.aggs.model.buckets:
        obj[model.key] = {}
        for ur in model.ur30.buckets:
            obj[model.key][round(ur.key,2)] = ur.doc_count

    df = DataFrame.from_dict(obj).T

    if df.empty:
        return df

    df.index.name = 'modelo'
    df = df.fillna(0).astype('int64')
    df['total'] = df.sum(axis=1)
    df.loc['total'] = df.sum()
    df = df.reset_index()

    return df
