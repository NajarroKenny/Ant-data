"""
Clients Opened per Month
========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on new clients opened every month
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic

# GLOBAL VARIABLES
SNAME = 'count'
SINDEX = 'date'

def search(q=None):
    """Instantiates and builds a search object to perform the respective query.

    The query is a simple date histogram by month with a single bucket 
    aggregation executed on the 'people' index.

    Args:
        q (elasticsearch-dsl Q object, optional): Additional queries to chain.
        
    Returns:
        elasticsearch-dsl search object response.
    """
    s = Search(using=elastic, index='people') \
        .query('term', doctype='client')

    if q is not None:
        s = s.query(q)

    s.aggs.bucket('by_months', 'date_histogram', field='opened', interval='month')

    return s[:0].execute()

def series(q=None):
    """Returns a series object with clients opened per month.

    Args:
        q (elasticsearch-dsl Q object, optional): Additional queries to chain.
        
    Returns:
        Pandas Series with a datetime index named 'date' corresponding to months 
        of the year and series name = 'count'
    """
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