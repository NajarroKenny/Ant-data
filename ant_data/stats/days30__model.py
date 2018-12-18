"""
Client dayss
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on client dayss.

- Create date:  2018-12-11
- Update date:
- Version:      1.0

Notes:
==========================
- v1.0: Initial version
"""
import datetime as dt
from elasticsearch_dsl import Search, Q, A
from pandas import DataFrame, Series
import numpy as np

from ant_data import elastic


def search(country, f=None, date=None):
    s = Search(using=elastic, index='people') \
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

    s.aggs.bucket('model', 'terms', field='model') \
        .bucket('days30', 'range', field='days30', ranges=ranges30)

    return s[:0].execute()


def df(country, f=None, **kwargs):
    today = dt.datetime.today().strftime('%Y-%m-%d')
    date = kwargs['date'] if 'date' in kwargs else today
    response = search(country, f=f, date=date)

    obj = {}
    for model in response.aggs.model.buckets:
        obj[model.key] = {}
        i = 0
        for days in model.days30.buckets:
            obj[model.key][i] = days.doc_count
            i += 1

    df = DataFrame.from_dict(obj).T

    if df.empty:
        return df

    df.index.name = 'modelo'
    df = df.fillna(0).astype('int64')
    df['total'] = df.sum(axis=1)
    df.loc['total'] = df.sum()
    df = df.reset_index()

    return df
