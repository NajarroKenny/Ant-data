"""
Kingo installs by model and month
========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on kingo installations every month by model
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic


# GLOBAL VARIABLES
DFINDEX = 'date'

def search(q = None):
    """Instantiates and builds a search object to perform the respective query.

    The query is a terms aggregation on the 'model' field chained with a month
    date histogram aggregation on the 'opened' field. The index is 'installs'

    Args:
        q (elasticsearch-dsl Q object, optional): Additional queries to chain.
        
    Returns:
        elasticsearch-dsl search object response.
    """
    s = Search(using=elastic, index='installs') \
        .query(Q('term', doctype='install') & ~Q('term', model='Kingo Shopkeeper'))

    if q is not None:
        s = s.query(q)

    s.aggs.bucket('models', 'terms', field='model') \
        .bucket('by_months', 'date_histogram', field='opened', interval='month')

    return s[:0].execute()

def df(q = None):
    """Returns a dataframe object with kingos installed per model per month.

    Args:
        q (elasticsearch-dsl Q object, optional): Additional queries to chain.
        
    Returns:
        Pandas DataFrame with a datetime index named 'date' corresponding to 
        months of the year and columns = ['Kingo 15', 'Kingo - Basico', 
        'Kingo TV', 'Kingo - Luz', 'Kingo Basico', 'Kingo 10', 'Kingo 100', 
        'Kingo - TV', 'Kingo Hogar', 'no_model', 'total']
    """
    response = search(q)

    obj = {}
    for model in response.aggregations.models.buckets:
        obj[model.key] = {month.key_as_string: month.doc_count for month in
                          model.by_months.buckets}
    df = DataFrame(obj, dtype='int64')
    df = df.reindex(df.index.astype('datetime64')).sort_index()
    # If typecasting is necessary
    df = df.fillna(0).astype('int64')
    df['total'] = df.sum(axis=1)
    # If no typecasting is necessary
    # df.fillna(0, inplace=True)
    df.index.name = DFINDEX

    return df

if __name__ == '__main__':
    print('Kingo Installs by Model and Month')
    print(df())