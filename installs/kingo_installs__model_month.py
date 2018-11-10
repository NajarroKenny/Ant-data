from pandas import DataFrame, Series
from elasticsearch_dsl import Search, Q
from ant_data import elastic

def search():
    s = Search(using=elastic, index='installs') \
        .query(Q('term', doctype='install') & ~Q('term', model='Kingo Shopkeeper'))
    s.aggs.bucket('models', 'terms', field='model') \
        .bucket('by_months', 'date_histogram', field='opened', interval='month')

    s = s[:0]
    return s.execute()

def df():
    response = search()

    obj = {}
    for model in response.aggregations.models.buckets:
        obj[model.key] = { month.key_as_string: month.doc_count for month in model.by_months.buckets }

    df = DataFrame(obj)
    df = df.reindex(df.index.astype('datetime64'))

    return df

if __name__ == '__main__':
    print(df())