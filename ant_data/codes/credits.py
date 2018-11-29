from ant_data import elastic
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series
from pandas.io.json import json_normalize
import pandas as pd
from ..static.FINANCE import IVA


def search(country, f=None, interval='month'):
    s = Search(using=elastic, index='codes') \
        .query('term', doctype='credit') \
        .query('bool', filter=Q('term', country=country))

    if f is not None:
        s = s.query('bool', filter=f)

    s.aggs.bucket('dates', 'date_histogram',
                  field='datetime', interval=interval) \
        .bucket('sales', 'terms', field='sale') \
        .metric('value', 'sum', field='value', missing=0) \
        .metric('commission', 'sum', field='commission', missing=0)

    return s[:0].execute()


def df(country, f=None, interval='month'):
    response = search(country, f=f, interval=interval)

    obj = {}
    for date in response.aggregations.dates.buckets:
        free = 0
        paid = 0
        commission = 0

        for sale in date.sales.buckets:
            if sale.key_as_string == 'true':
                paid += sale.value.value
            elif sale.key_as_string == 'false':
                free += sale.value.value
            commission += sale.commission.value

        obj[date.key_as_string] = {
            'paid': paid,
            'free': free,
            'commission': commission
        }

    df = DataFrame.from_dict(obj, orient='index')
    df.index.name = 'date'
    df = df.reindex(df.index.astype('datetime64')).sort_index()

    if not df.empty:
        df['paid'] = df['paid'] - df['commission']
        df['iva'] = IVA[country] * df['paid']
        df['paid'] = (1-IVA[country]) * df['paid']
        df['total'] = df.sum(axis=1)
        df = df.fillna(0).astype('double')
        df = df[['free', 'paid', 'iva', 'commission', 'total']]

    return df
