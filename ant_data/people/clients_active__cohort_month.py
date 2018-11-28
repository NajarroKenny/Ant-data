from ant_data import elastic
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.aggs import Terms, Nested
from pandas import DataFrame, Series
import pandas as pd

def search(q = None):
    """Count of active clients, by cohort and month.

    Input: elasticsearch-dsl Q object (optional)
    Ouput: elasticsearch-dsl Search response
    Description: Count of active clients, by cohort and month, from the people index."""

    s = Search(using=elastic, index='people') \
        .query('term', doctype='client')

    if q is not None:
        s = s.query(q)

    s.aggs \
        .bucket('cohort', 'date_histogram', field='opened', interval='month') \
        .bucket('stats', 'children', type='stat') \
        .bucket('month', 'date_histogram', field='date', interval='month') \
        .bucket('active', 'cardinality', field='person_id')

    return s[:0].execute()

def df(q = None):
    """Count of active clients, by cohort and month.

    Input: elasticsearch-dsl Q object (optional)
    Ouput: pandas DataFrame
    Description: Count of active clients, by cohort and month, from the people index."""

    response = search(q)

    obj = {}
    for cohort in response.aggregations.cohort.buckets:
        obj[cohort.key_as_string] = {}
        for month in cohort.stats.month.buckets:
            obj[cohort.key_as_string][month.key_as_string] = { 'active': month.active.value }

    df = DataFrame.from_dict({(i,j): obj[i][j]
                                    for i in obj.keys()
                                    for j in obj[i].keys()},
                                orient='index')

    df.index = df.index.set_levels(df.index.levels[1].astype('datetime64'), level=1)
    df.index = df.index.set_levels(df.index.levels[0].astype('datetime64'), level=0)
    df.index = df.index.set_names('cohort', level=0)
    df.index = df.index.set_names('month', level=1)
    df = df.fillna(0).astype('int64')

    return df