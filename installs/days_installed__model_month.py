from ant_data import elastic
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.aggs import Terms, Nested
from pandas import DataFrame, Series

def search(q = None):
    s = Search(using=elastic, index='installs') \
        .query('term', doctype='install')

    if q is not None:
        s.query(q)

    s.aggs.bucket('models', 'terms', field='model') \
        .bucket('stats', Nested(path='stats')) \
        .bucket('by_months', 'date_histogram', field='stats.date', interval='month')

    return s[:0].execute()

def df(q = None):
    response = search(q)
    obj = {}
    for model in response.aggregations.models.buckets:
        obj[model.key] = {month.key_as_string: month.doc_count for month in model.stats.by_months.buckets}
    df = DataFrame(obj)
    df = df.reindex(df.index.astype('datetime64')).sort_index()
    df = df.fillna(0).astype('int64')
    df.index.name = 'date'
    df['days_in_month'] = df.index.daysinmonth

    return df

if __name__ == '__main__':
    print('Days installed by Model and Month')
    print(df())