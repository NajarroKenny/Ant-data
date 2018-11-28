from ant_data import elastic
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series
from pandas.io.json import json_normalize
import pandas as pd

def search(f = None, interval='month'):
    s = Search(using=elastic, index='codes') \
        .query('term', doctype='credit')

    if f is not None:
        s = s.query('bool', filter=f)

    s.aggs.bucket('dates', 'date_histogram', field='datetime', interval=interval) \
        .metric('value', 'sum', field='value', missing=0) \
        .metric('commission', 'sum', field='commission', missing=0)

    return s[:0].execute()

def df(f = None, iva = 0, interval='month'):
    response = search(f, interval)

    obj = {}
    for date in response.aggregations.dates.buckets:
        obj[date.key_as_string] = {
            'total': date.value.value,
            'commission': date.commission.value
        }

    df = DataFrame.from_dict(obj, orient='index')
    df.index.name = 'date'
    df = df.reindex(df.index.astype('datetime64')).sort_index()
    df = df.fillna(0).astype('double')
    df['credit'] = df['total'] - df['commission']
    if iva > 0:
        df['iva'] = iva * df['credit']
        df['credit'] = (1-iva) * df['credit']
        df = df[['credit', 'iva', 'commission', 'total']]
    else:
        df = df[['credit', 'commission', 'total']]


    return df