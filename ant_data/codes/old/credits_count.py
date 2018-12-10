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

    s.aggs.bucket('dates', 'date_histogram', field='datetime', interval=interval)

    return s[:0].execute()

def df(f = None, interval='month'):
    response = search(f, interval)

    obj = {}
    for date in response.aggregations.dates.buckets:
        obj[date.key_as_string] = {
            'count': date.doc_count
        }

    df = DataFrame.from_dict(obj, orient='index')
    df.index.name = 'date'
    df = df.reindex(df.index.astype('datetime64')).sort_index()
    df = df.fillna(0).astype('int64')

    return df