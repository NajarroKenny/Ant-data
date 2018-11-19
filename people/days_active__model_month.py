from ant_data import elastic
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.aggs import Terms, Nested
from pandas import DataFrame, Series
from .days_installed__model_month import search

def df(q = None):
    if q is not None:
        activeQ = Q('bool', must=[Q('term', active=True), q])
    else:
        activeQ = Q('term', active=True)

    response = search(activeQ)
    obj = {}
    for model in response.aggregations.models.buckets:
        obj[model.key] = {month.key_as_string: month.doc_count for month in model.months.buckets}
    df = DataFrame(obj)
    df = df.reindex(df.index.astype('datetime64')).sort_index()
    df = df.fillna(0).astype('int64')
    df.index.name = 'date'
    df['days_in_month'] = df.index.daysinmonth

    return df

if __name__ == '__main__':
    print('Days installed by Model and Month')
    print(df())