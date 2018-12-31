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
    s = Search(using=elastic, index=CONFIG['ES']['PEOPLE']) \
        .query('term', country=country) \
        .query('term', doctype='stat') \
        .query('term', date=date) \
        .query('has_parent', parent_type='person', query=Q('term', doctype='client')) \
        .query('has_parent', parent_type='person', query=Q('term', kingo_open=True))

    if f is not None:
        s = s.query('has_parent', parent_type='person', query=Q('bool', filter=f))

    s.aggs.bucket('model', 'terms', field='model') \
        .metric('ur30', 'avg', field='ur30')

    return s[:0].execute()


def df(country, f=None, **kwargs):
    today = dt.datetime.today().strftime('%Y-%m-%d')
    date = kwargs['date'] if 'date' in kwargs else today
    response = search(country, f=f, date=date)

    obj = {}
    for model in response.aggs.model.buckets:
        obj[model.key] = { 'taza 30': round(model.ur30.value, 4), 'total': model.doc_count }

    df = DataFrame.from_dict(obj).T

    if df.empty:
        return df

    df.index.name = 'modelo'
    df = df[['taza 30', 'total']]
    df = df.reset_index()

    return df
