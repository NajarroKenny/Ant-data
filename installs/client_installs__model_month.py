from pandas import DataFrame
from elasticsearch_dsl import Search
from ant_data import elastic

def df():
    s = Search(using=elastic, index='installs') \
        .query("term", doctype='install')
    s.aggs.bucket('models', 'terms', field='model.raw') \
        .bucket('by_months', 'date_histogram', field='opened', interval='month')

    s = s[:0]
    response = s.execute()

    obj = {}
    for model in response.aggregations.models.buckets:
        obj[model.key] = {}
        for month in model.by_months.buckets:
            obj[model.key][month.key_as_string] = month.doc_count

    df = DataFrame(obj)
    df = df.reindex(df.index.astype('datetime64'))
    df = df.drop(['Kingo Shopkeeper'], axis=1)
    return df

if __name__ == '__main__':
    print(df())