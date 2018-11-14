from ant_data import elastic
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

DFINDEX = 'Date'

def search(q = None):
    s = Search(using=elastic, index='installs') \
        .query(Q('term', doctype='install') & ~Q('term', model='Kingo Shopkeeper'))

    if q is not None:
        s.query(q)

    s.aggs.bucket('models', 'terms', field='model') \
        .bucket('by_months', 'date_histogram', field='opened', interval='month')

    return s[:0].execute()

def df(q = None):
    response = search(q)

    obj = {}
    for model in response.aggregations.models.buckets:
        obj[model.key] = {month.key_as_string: month.doc_count for month in
                          model.by_months.buckets}
    df = DataFrame(obj, dtype='int64')
    df = df.reindex(df.index.astype('datetime64'))
    # If typecasting is necessary
    df = df.fillna(0).astype('int64')
    # If no typecasting is necessary
    # df.fillna(0, inplace=True)
    df.index.name = DFINDEX

    return df

if __name__ == '__main__':
    print('Kingo Installs by Model and Month')
    print(df())