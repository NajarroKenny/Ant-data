from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic


def search(country, f=None, interval='month'):
    s = Search(using=elastic, index='installs') \
        .query(Q('term', doctype='install') & ~Q('term', model='Kingo Shopkeeper'))

    if f is not None:
        s = s.query('bool', fitler=f)

    s.aggs.bucket('dates', 'date_histogram', field='opened', interval=interval) \
        .bucket('models', 'terms', field='model') \
        
    return s[:0].execute()

def df(country, f=None, interval='month'):
    response = search(country, f=f, interval=interval)

    obj = {}
    for date in response.aggregations.dates.buckets:
        obj[date.key_as_string] = { model.key: model.doc_count for model in
                          date.models.buckets }

    df = DataFrame.from_dict(obj, orient='index')
    df = df.reindex(df.index.astype('datetime64')).sort_index()
    df = df.fillna(0).astype('int64')
    df['total'] = df.sum(axis=1)
    df.index.name = 'date'

    return df