"""
Tasks workfow list agent type
============================
Provides functions to fetch and parse data from Kingo's ElasticSearch Data
Warehouse to generate a report on tasks based on a given action list per agent
and task type.

- Create date:  2018-12-15
- Update date:
- Version:      1.0

Notes:
============================
- v1.0: Initial version
"""
from elasticsearch_dsl import Search, Q
from pandas import DataFrame, Series

from ant_data import elastic
from ant_data.static.GEOGRAPHY import COUNTRY_LIST


def search(country, workflows, start, end, f=None):
    if country not in COUNTRY_LIST:
        raise Exception(f'{country} is not a valid country')

    s = Search(using=elastic, index='tasks') \
        .query('term', country=country) \
        .query('term', doctype='task') \
        .query('range', due={'gte': start, 'lte': end}) \
        .query('has_child', type='history', query=Q('terms', workflow=workflows))

    if f is not None:
        s = s.query('bool', filter=f)

    s.aggs.bucket(
        'agents', 'terms', field='agent_id', min_doc_count=1, size=10000
        ).bucket('types', 'terms', field='type', size=10000)

    return s[:0].execute()



def df(country, workflows, start, end, f=None):
    if country not in COUNTRY_LIST:
        raise Exception(f'{country} is not a valid country')

    must = search(country, workflows, start, end, f=f)

    obj = {}
    for agent in must.aggs.agents.buckets:
        obj[agent.key] = {}
        for task_type in agent.types.buckets:
            obj[agent.key][task_type.key] = task_type.doc_count

    df = DataFrame.from_dict(obj, orient='index')

    if df.empty:
        return df

    df.index.name = 'agent_id'
    df = df.fillna(0).astype('int64')
    df['total'] = df.sum(axis=1)

    return df
