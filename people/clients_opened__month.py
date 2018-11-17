from ant_data import elastic
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

SNAME = 'count'
SINDEX = 'date'

def search(q = None):
    s = Search(using=elastic, index='people') \
        .query('term', doctype='client')

    if q is not None:
        s.query(q)

    s.aggs.bucket('by_months', 'date_histogram', field='opened', interval='month')

    return s[:0].execute()

def series(q = None):
    response = search(q)

    obj = {month.key_as_string: month.doc_count for month in
           response.aggregations.by_months.buckets}
    series = Series(obj, dtype='int64')
    series = series.reindex(series.index.astype('datetime64')).sort_index()
    if series.hasnans:
        series.fillna(0, inplace=True)
    series.name = SNAME
    series.index.name = SINDEX

    return series

if __name__ == '__main__':
    print('Clients opened by month')
    print('Date         Count')
    print(series())