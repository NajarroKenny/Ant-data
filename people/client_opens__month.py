from pandas import DataFrame, Series
from elasticsearch_dsl import Search, Q
from ant_data import elastic

def search():
    s = Search(using=elastic, index='people') \
        .query('term', doctype='client')
    s.aggs.bucket('by_months', 'date_histogram', field='opened', interval='month')

    s = s[:0]
    return s.execute()

def series():
    response = search()

    obj = { month.key_as_string: month.doc_count for month in response.aggregations.by_months.buckets }

    series = Series(obj)
    series = series.reindex(series.index.astype('datetime64'))

    return series

if __name__ == '__main__':
    print(df())