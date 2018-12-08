from ant_data import elastic
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.aggs import Terms, Nested
from pandas import DataFrame, Series

def search(q = None):
    """Count of days open, by Kingo Model and Month.

    Input: elasticsearch-dsl Q object (optional)
    Ouput: elasticsearch-dsl Search response
    Description: Query days open, by Kingo Model and Month, from the People index."""

    s = Search(using=elastic, index='people') \
        .query('has_parent', parent_type='person', query=Q('term', doctype='client')) \
        .query('term', doctype='stat')

    if q is not None:
        s = s.query(q)

    s.aggs.bucket('models', 'terms', field='model') \
        .bucket('months', 'date_histogram', field='date', interval='month')

    return s[:0].execute()

def df(q = None):
    """Count of days open, by Kingo Model and Month.

    Input: elasticsearch-dsl Q object (optional)
    Ouput: pandas DataFrame
    Description: Count of days open, by Kingo Model and Month, from the People index."""

    response = search(q)
    obj = {}
    for model in response.aggregations.models.buckets:
        obj[model.key] = {month.key_as_string: month.doc_count for month in model.months.buckets}
    df = DataFrame(obj)
    df = df.reindex(df.index.astype('datetime64')).sort_index()
    df = df.fillna(0).astype('int64')
    df.index.name = 'date'
    df['total'] = df.sum(axis=1)
    df['days_in_month'] = df.index.daysinmonth

    return df