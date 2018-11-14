from ant_data import elastic
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series
from pandas.io.json import json_normalize
import pandas as pd

def search(q = None):
    s = Search(using=elastic, index='codes') \
        .query()

    if q is not None:
        s.query(q)

    s.aggs.bucket('cats', 'terms', field='cat', size=1000) \
        .bucket('doctypes', 'terms', field='doctype', size=1000) \
        .bucket('sales', 'terms', field='sale', size=1000) \
        .bucket('plans', 'terms', field='plan', size=1000) \
        .bucket('months', 'date_histogram', field='datetime', interval='month')

    return s[:0].execute()

def df(q = None):
    response = search(q)

    data = []
    for cat in response.aggregations.cats.buckets:
        for doctype in cat.doctypes.buckets:
            for sale in doctype.sales.buckets:
                for plan in sale.plans.buckets:
                    for month in plan.months.buckets:
                        data.append({ 'cat': cat.key, 'doctype': doctype.key, 'sale': sale.key, 'plan': plan.key, 'month': month.key_as_string, 'count': month.doc_count })

    df = DataFrame(json_normalize(data))
    df = df.set_index(['cat', 'doctype', 'sale', 'plan', 'month'])

    # If typecasting is necessary
    # df = df.fillna(0).astype('int64')
    # If no typecasting is necessary
    # df.fillna(0, inplace=True)

    return df

if __name__ == '__main__':
    print('Kingo Installs by Model and Month')
    print(df())