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

    ranges = [
        { 'to': 1 },
        { 'from': 1 }
    ]

    if f is not None:
        s = s.query('has_paremt', parent_type='person', query=Q('bool', filter=f))

    s.aggs.bucket('model', 'terms', field='model') \
        .bucket('days', 'range', field='days30', ranges=ranges)

    return s[:0].execute()


def df(country, f=None, **kwargs):
    today = dt.datetime.today().strftime('%Y-%m-%d')
    date = kwargs['date'] if 'date' in kwargs else today
    response = search(country, f=f, date=date)

    obj = {}
    for model in response.aggs.model.buckets:
        obj[model.key] = {}
        i = 0
        for days in model.days.buckets:
            obj[model.key][i] = days.doc_count
            i += 1

    df = DataFrame.from_dict(obj).T

    if df.empty:
        return df

    df.index.name = 'period'
    df = df.rename(columns={ 0: 'inactive', 1: 'active' })
    df['total'] = df.sum(axis=1)
    df = df.fillna(0).astype('int64')
    df['percent'] = df['active'].div(df['total'])
    df = df.replace((np.nan, -np.nan), (0, 0))
    df = df.replace((np.inf, -np.inf), (0, 0))

    return df
