from ant_data import elastic
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series


def search(country, type, f=None, interval='month'):
    s = Search(using=elastic, index='people') \
        .query('bool', filter=[Q('term', country=country),
                               Q('term', doctype='client')])

    if f is not None:
        s = s.query('bool', filter=f)

    s.aggs.bucket('dates', 'date_histogram', field=type, interval=interval)

    return s[:0].execute()


def df(country, f=None, interval='month'):
    response = search(country, 'opened', f=f, interval=interval)
    obj = {}

    for date in response.aggregations.dates.buckets:
        obj[date.key_as_string] = {'opened': date.doc_count}

    response = search(country, 'closed', f=f, interval=interval)

    for date in response.aggregations.dates.buckets:
        if not date.key_as_string in obj:
            obj[date.key_as_string] = {}
        obj[date.key_as_string]['closed'] = date.doc_count

    df = DataFrame.from_dict(
        obj, orient='index', dtype='int64', columns=['opened', 'closed'])
    df.index.name = 'date'
    df = df.reindex(df.index.astype('datetime64')).sort_index()
    df = df.fillna(0).astype('int64')
    df['open'] = df['opened'].cumsum() - df['closed'].cumsum()
    df = df[(df.T != 0).any()]

    return df
