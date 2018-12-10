"""
Clients Opened
==========================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on task histories/actions.

- Create date:  2018-12-10
- Update date:
- Version:      1.0

Notes:
==========================
- v1.0: Initial version
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic


def search(country, f=None, interval='month'):
    s = Search(using=elastic, index='tasks') \
        .query('term', country=country) \
        .query('term', doctype='history')

    if f is not None:
        s = s.query('bool', filter=f)

    s.aggs.bucket('agents', 'terms', field='agent_id') \
        .bucket(
        'dates', 'date_histogram', field='datetime', interval=interval,
        min_doc_count=0
    ) \
        .bucket('types', 'terms', field='type') \

    return s[:0].execute()


def df(country, f=None, interval='month'):
    response = search(country, f=f, interval=interval)

    obj = {}
    for agent in response.aggregations.agents.buckets:
        obj[agent.key] = {}
        for interval in agent.dates.buckets:
            obj[agent.key][interval.key_as_string] = {}
            for action in interval.types.buckets:
                obj[agent.key][interval.key_as_string][action.key] = action.doc_count

    df = DataFrame.from_dict({(i, j): obj[i][j]
                              for i in obj.keys()
                              for j in obj[i].keys()},
                             orient='index')

    if df.empty:
        return df

    df.index = df.index.set_levels(
        df.index.levels[1].astype('datetime64'), level=1)

    df.index = df.index.set_names('agent', level=0)
    df.index = df.index.set_names('date', level=1)
    df = df.fillna(0).astype('int64')
    df = df.swaplevel(1, 0).reset_index()
    df = df.set_index('date')

    return df
